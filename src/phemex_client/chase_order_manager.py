# src/phemex_client/chase_order_manager.py
# Singleton manager for chase limit orders with real-time bid1/ask1 streaming
# Establishes WS connection on warmup, receives commands via async queue
# RELEVANT FILES: exchange_client.py, models.py, config.py

"""
Chase Order Manager for Phemex.

Singleton service that:
1. Streams bid1/ask1 prices via WebSocket (warmup phase)
2. Receives chase commands via async queue (minimal latency)
3. Updates orders when price changes

Usage:
    manager = ChaseOrderManager.get_instance(client)
    await manager.warmup(["SOL/USDT:USDT"])
    chase_id = await manager.submit_chase(config)
"""

import asyncio
import logging
import time
import uuid
from typing import Optional

from phemex_client.exchange_client import PhemexClient
from phemex_client.models import ChaseOrderConfig, ChaseOrderState, OrderResult


logger = logging.getLogger(__name__)


class ChaseOrderManager:
    """
    Singleton manager for chase limit orders.
    
    Streams bid1/ask1 in background and processes chase commands
    via async queue for minimal latency.
    """
    
    # Singleton instance
    _instance: Optional["ChaseOrderManager"] = None
    
    # Minimum price change to trigger order update (avoids spam)
    MIN_PRICE_CHANGE_PCT = 0.0001  # 0.01%
    
    # Minimum interval between amends (seconds)
    MIN_AMEND_INTERVAL = 1.0
    
    @classmethod
    def get_instance(cls, exchange_client: PhemexClient) -> "ChaseOrderManager":
        """
        Get or create singleton instance.
        
        Args:
            exchange_client: Initialized PhemexClient
        
        Returns:
            ChaseOrderManager singleton
        """
        if cls._instance is None:
            cls._instance = cls(exchange_client)
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
    
    def __init__(self, exchange_client: PhemexClient):
        """
        Initialize chase order manager.
        
        Use get_instance() instead of direct construction.
        """
        self._client = exchange_client
        
        # Price cache: symbol -> {bid1, bid2, ask1, ask2}
        self._prices: dict[str, dict] = {}
        
        # Order updates cache: order_id -> order_info
        # Populated by WebSocket order stream for real-time fill detection
        self._order_updates: dict[str, dict] = {}
        
        # Active chase orders
        self._active_chases: dict[str, ChaseOrderState] = {}
        
        # Background tasks
        self._orderbook_tasks: dict[str, asyncio.Task] = {}
        self._order_stream_tasks: dict[str, asyncio.Task] = {}  # Per-symbol order streams
        self._chase_tasks: dict[str, asyncio.Task] = {}
        
        # Command queue for submitting chase orders
        self._command_queue: asyncio.Queue = asyncio.Queue()
        self._command_processor_task: Optional[asyncio.Task] = None
        
        # State
        self._warmed_up = False
        self._running = False
        self._symbols: list[str] = []  # Symbols we're tracking
    
    @property
    def is_warmed_up(self) -> bool:
        """Check if manager is warmed up and ready."""
        return self._warmed_up
    
    async def warmup(self, symbols: list[str]) -> None:
        """
        Warmup phase: establish WS connections and start price streaming.
        
        Must be called before submitting chase orders.
        
        Args:
            symbols: List of symbols to stream prices for
        """
        if self._warmed_up:
            logger.warning("Already warmed up, skipping")
            return
        
        logger.info(f"Warming up ChaseOrderManager for {len(symbols)} symbols...")
        
        # Start orderbook streaming for each symbol
        for symbol in symbols:
            task = asyncio.create_task(self._stream_orderbook(symbol))
            self._orderbook_tasks[symbol] = task
        
        # Store symbols for order streaming
        self._symbols = symbols
        
        # Start order updates stream for each symbol (for fill detection)
        # Phemex requires symbol for WebSocket order subscriptions
        for symbol in symbols:
            task = asyncio.create_task(self._stream_orders(symbol))
            self._order_stream_tasks[symbol] = task
        
        # Wait for initial prices
        for symbol in symbols:
            for _ in range(50):  # 5 second timeout
                if symbol in self._prices:
                    break
                await asyncio.sleep(0.1)
            else:
                logger.warning(f"Timeout waiting for {symbol} prices")
        
        # Start command processor
        self._running = True
        self._command_processor_task = asyncio.create_task(
            self._process_commands()
        )
        
        self._warmed_up = True
        logger.info(f"ChaseOrderManager warmed up. Streaming: {list(self._prices.keys())}")
    
    async def shutdown(self) -> None:
        """Shutdown manager and cancel all tasks."""
        logger.info("Shutting down ChaseOrderManager...")
        
        self._running = False
        
        # Cancel all chase orders
        for chase_id in list(self._active_chases.keys()):
            await self.cancel_chase(chase_id)
        
        # Cancel orderbook streams
        for task in self._orderbook_tasks.values():
            task.cancel()
        self._orderbook_tasks.clear()
        
        # Cancel order streams
        for task in self._order_stream_tasks.values():
            task.cancel()
        self._order_stream_tasks.clear()
        
        # Cancel command processor
        if self._command_processor_task:
            self._command_processor_task.cancel()
            self._command_processor_task = None
        
        self._warmed_up = False
        logger.info("ChaseOrderManager shutdown complete")
    
    async def submit_chase(self, config: ChaseOrderConfig) -> str:
        """
        Submit a chase order via command queue.
        
        Returns immediately with chase_id. Order processing is async.
        
        Args:
            config: Chase order configuration
        
        Returns:
            chase_id: Unique ID to track this chase
        """
        if not self._warmed_up:
            raise RuntimeError("ChaseOrderManager not warmed up. Call warmup() first.")
        
        chase_id = str(uuid.uuid4())[:8]
        
        # Put command in queue (non-blocking)
        await self._command_queue.put(("start", chase_id, config))
        
        logger.info(f"[{chase_id}] Chase submitted: {config.side} {config.amount} {config.symbol}")
        
        return chase_id
    
    async def submit_chase_and_wait(
        self,
        config: ChaseOrderConfig,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> ChaseOrderState:
        """
        Submit a chase order and wait for it to complete.
        
        Blocks until the chase reaches a terminal state (filled, stopped, 
        canceled, error) or timeout is reached.
        
        Args:
            config: Chase order configuration
            timeout: Max seconds to wait (default 60s)
            poll_interval: Seconds between status checks (default 0.5s)
        
        Returns:
            Final ChaseOrderState with fill details
        
        Raises:
            TimeoutError: If timeout reached before completion
            RuntimeError: If chase manager not warmed up
        """
        chase_id = await self.submit_chase(config)
        
        elapsed = 0.0
        while elapsed < timeout:
            state = self._active_chases.get(chase_id)
            
            if state and state.status != "active":
                # Terminal state reached
                logger.info(
                    f"[{chase_id}] Chase completed: {state.status}, "
                    f"filled={state.total_filled:.4f}"
                )
                return state
            
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        # Timeout - cancel and return current state
        logger.warning(f"[{chase_id}] Timeout after {timeout}s, canceling...")
        await self.cancel_chase(chase_id)
        
        state = self._active_chases.get(chase_id)
        if state:
            return state
        
        raise TimeoutError(f"Chase {chase_id} timed out after {timeout}s")
    
    async def cancel_chase(self, chase_id: str) -> bool:
        """
        Cancel an active chase order.
        
        Args:
            chase_id: ID returned from submit_chase
        
        Returns:
            True if canceled, False if not found
        """
        if chase_id not in self._active_chases:
            logger.warning(f"[{chase_id}] Chase not found")
            return False
        
        state = self._active_chases[chase_id]
        state.status = "canceled"
        
        # Cancel chase task
        if chase_id in self._chase_tasks:
            self._chase_tasks[chase_id].cancel()
            del self._chase_tasks[chase_id]
        
        # Cancel current order if exists
        if state.current_order_id:
            try:
                await self._client.cancel_order(
                    state.current_order_id,
                    state.config.symbol,
                    position_side=state.config.position_side,
                )
                logger.info(f"[{chase_id}] Canceled order {state.current_order_id}")
            except Exception as e:
                logger.debug(f"[{chase_id}] Cancel order failed: {e}")
        
        logger.info(f"[{chase_id}] Chase canceled")
        return True
    
    def get_chase_status(self, chase_id: str) -> Optional[dict]:
        """Get status of a chase order."""
        if chase_id not in self._active_chases:
            return None
        
        state = self._active_chases[chase_id]
        return {
            "chase_id": state.chase_id,
            "symbol": state.config.symbol,
            "side": state.config.side,
            "amount": state.config.amount,
            "status": state.status,
            "initial_price": state.initial_price,
            "current_price": state.current_price,
            "retry_count": state.retry_count,
            "fill_price": state.fill_price,
            "fill_amount": state.fill_amount,
            "total_filled": state.total_filled,
            "remaining_amount": state.remaining_amount,
        }
    
    def get_prices(self, symbol: str) -> Optional[dict]:
        """
        Get cached bid1/ask1 prices for a symbol.
        
        Returns:
            {"bid1": float, "ask1": float} or None
        """
        return self._prices.get(symbol)
    
    def get_all_active_chases(self) -> list[dict]:
        """Get status of all active chase orders."""
        return [
            self.get_chase_status(cid)
            for cid in self._active_chases
            if self._active_chases[cid].status == "active"
        ]
    
    # -------------------------------------------------------------------------
    # Background Tasks
    # -------------------------------------------------------------------------
    
    async def _stream_orderbook(self, symbol: str) -> None:
        """
        Stream orderbook updates and cache bid1/ask1.
        
        Runs continuously in background until shutdown.
        """
        logger.debug(f"Starting orderbook stream for {symbol}")
        
        try:
            while True:
                try:
                    orderbook = await self._client.watch_order_book(symbol)
                    
                    if orderbook["bids"] and orderbook["asks"]:
                        self._prices[symbol] = {
                            "bid1": orderbook["bids"][0][0],
                            "bid2": orderbook["bids"][1][0] if len(orderbook["bids"]) > 1 else orderbook["bids"][0][0],
                            "ask1": orderbook["asks"][0][0],
                            "ask2": orderbook["asks"][1][0] if len(orderbook["asks"]) > 1 else orderbook["asks"][0][0],
                        }
                    
                except asyncio.CancelledError:
                    raise  # Re-raise to exit
                except Exception as e:
                    logger.warning(f"Orderbook stream error for {symbol}: {e}")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        
        logger.debug(f"Orderbook stream stopped for {symbol}")
    
    async def _stream_orders(self, symbol: str) -> None:
        """
        Stream order updates via WebSocket for a specific symbol.
        
        Caches order status for real-time fill detection.
        Phemex requires symbol to be specified for order subscriptions.
        """
        logger.info(f"Starting order stream for {symbol}")
        
        try:
            while True:
                try:
                    orders = await self._client.watch_orders(symbol)
                    
                    # Update order cache with latest status
                    for order in orders:
                        order_id = order.get("id")
                        if order_id:
                            status = order.get("status")
                            filled = order.get("filled", 0.0)
                            
                            self._order_updates[order_id] = {
                                "status": status,
                                "filled": filled,
                                "remaining": order.get("remaining", 0.0),
                                "average": order.get("average"),
                            }
                            
                            # Log ALL order updates (not just fills) for debugging
                            logger.info(
                                f"WS order: {order_id[:8]}... "
                                f"status={status} filled={filled}"
                            )
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Order stream error for {symbol}: {e}")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        
        logger.info(f"Order stream stopped for {symbol}")
    
    async def _process_commands(self) -> None:
        """
        Process commands from the queue.
        
        Runs continuously in background.
        """
        logger.debug("Command processor started")
        
        while self._running:
            try:
                command = await asyncio.wait_for(
                    self._command_queue.get(),
                    timeout=1.0,
                )
                
                cmd_type, chase_id, config = command
                
                if cmd_type == "start":
                    await self._start_chase(chase_id, config)
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Command processor error: {e}")
        
        logger.debug("Command processor stopped")
    
    async def _start_chase(self, chase_id: str, config: ChaseOrderConfig) -> None:
        """Start a new chase order."""
        # Create state
        state = ChaseOrderState(
            chase_id=chase_id,
            config=config,
            current_order_id=None,
            initial_price=0.0,
            current_price=0.0,
        )
        
        self._active_chases[chase_id] = state
        
        # Start chase loop
        task = asyncio.create_task(self._chase_loop(chase_id))
        self._chase_tasks[chase_id] = task
    
    async def _chase_loop(self, chase_id: str) -> None:
        """
        Main chase loop - reads cached prices and updates order.
        
        CRITICAL: Check if filled FIRST, before placing any new orders.
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        try:
            while state.status == "active" and self._running:
                # CRITICAL: Check WebSocket cache for fills FIRST
                # This prevents placing new orders when previous one was filled
                if state.current_order_id:
                    if await self._check_order_filled(chase_id):
                        state.status = "filled"
                        logger.info(f"[{chase_id}] Filled at {state.fill_price}")
                        break
                
                # Also check if fully filled from accumulated partial fills
                if state.is_fully_filled:
                    state.status = "filled"
                    logger.info(f"[{chase_id}] Fully filled (accumulated): {state.total_filled:.4f}")
                    break
                
                # Get cached prices (instant, no WS wait)
                prices = self._prices.get(config.symbol)
                
                if not prices:
                    await asyncio.sleep(0.05)
                    continue
                
                target_price = self._calculate_target_price(config, prices)
                
                # Set initial price on first iteration
                if state.initial_price == 0.0:
                    state.initial_price = target_price
                
                # Check limits
                if self._check_limits(chase_id, target_price):
                    break
                
                # Check if we need to place/update order
                if self._needs_update(state, target_price):
                    await self._update_order(chase_id, target_price)
                    
                    # Re-check fill after update (order might have filled immediately)
                    if state.is_fully_filled:
                        state.status = "filled"
                        logger.info(f"[{chase_id}] Filled immediately!")
                        break
                
                # Chase interval: 1 second between checks
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            logger.debug(f"[{chase_id}] Chase task canceled")
        except Exception as e:
            logger.error(f"[{chase_id}] Chase error: {e}", exc_info=True)
            state.status = "error"
        finally:
            if chase_id in self._chase_tasks:
                del self._chase_tasks[chase_id]
    
    def _check_limits(self, chase_id: str, target_price: float) -> bool:
        """Check if chase limits reached. Returns True to stop."""
        state = self._active_chases[chase_id]
        config = state.config
        
        # Max chase distance
        if config.max_chase_distance > 0:
            distance = abs(target_price - state.initial_price)
            if distance > config.max_chase_distance:
                logger.info(f"[{chase_id}] Max distance: {distance:.4f}")
                state.status = "stopped"
                return True
        
        # Max retries
        if state.retry_count >= config.max_retries:
            logger.info(f"[{chase_id}] Max retries: {state.retry_count}")
            state.status = "stopped"
            return True
        
        return False
    
    def _needs_update(self, state: ChaseOrderState, target_price: float) -> bool:
        """
        Check if order needs to be placed/updated.
        
        Key logic: only chase when price moves AWAY from order.
        If price moves TOWARD order, wait for fill.
        
        For BUY: only update if target goes UP (price moving away)
        For SELL: only update if target goes DOWN (price moving away)
        """
        if state.current_order_id is None:
            return True
        
        if state.current_price == 0.0:
            return True
        
        # Rate limit: minimum 1 second between amends
        if state.last_amend_time > 0:
            elapsed = time.time() - state.last_amend_time
            if elapsed < self.MIN_AMEND_INTERVAL:
                return False
        
        price_diff = target_price - state.current_price
        min_change = state.current_price * self.MIN_PRICE_CHANGE_PCT
        
        # No significant change
        if abs(price_diff) < min_change:
            return False
        
        # BUY order: only chase UP (price moving away from our order)
        # If price drops, we wait for our order to fill
        if state.config.side == "buy":
            return price_diff > 0  # target went UP
        
        # SELL order: only chase DOWN (price moving away from our order)
        # If price rises, we wait for our order to fill
        else:
            return price_diff < 0  # target went DOWN
    
    def _calculate_target_price(
        self,
        config: ChaseOrderConfig,
        prices: dict,
    ) -> float:
        """
        Calculate target order price based on chase mode.
        
        Supported modes:
        - 'bid1': Best bid price
        - 'bid2': Second best bid price (default for buys)
        - 'ask1': Best ask price
        - 'ask2': Second best ask price (default for sells)
        - 'distance': Fixed offset from bid1/ask1
        
        Includes spread protection:
        - BUY orders will never be placed at or above ask1
        - SELL orders will never be placed at or below bid1
        """
        bid1 = prices["bid1"]
        bid2 = prices["bid2"]
        ask1 = prices["ask1"]
        ask2 = prices["ask2"]
        
        # Calculate base price based on mode
        if config.chase_mode == "bid1":
            price = bid1
        elif config.chase_mode == "bid2":
            price = bid2
        elif config.chase_mode == "ask1":
            price = ask1
        elif config.chase_mode == "ask2":
            price = ask2
        elif config.chase_mode == "distance":
            if config.side == "buy":
                price = bid1 - config.price_distance
            else:
                price = ask1 + config.price_distance
        else:
            # Default: bid2 for buys, ask2 for sells (one tick back)
            price = bid2 if config.side == "buy" else ask2
        
        # Spread protection: ensure we never cross the spread
        # This prevents accidental taker orders
        if config.side == "buy":
            # Buy orders must be below ask1 (use bid1 as max)
            price = min(price, bid1)
        else:
            # Sell orders must be above bid1 (use ask1 as min)
            price = max(price, ask1)
        
        return price
    
    async def _update_order(self, chase_id: str, target_price: float) -> None:
        """
        Place or update the chase order.
        
        Uses edit_order (amend) when an order exists to reduce latency.
        Falls back to cancel+place if amend fails.
        Aggregates partial fills from previous orders.
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        # Check for partial fills from WebSocket cache
        if state.current_order_id:
            order_update = self._order_updates.get(state.current_order_id)
            if order_update:
                current_filled = order_update.get("filled", 0.0)
                if current_filled > state.fill_amount:
                    additional_fill = current_filled - state.fill_amount
                    state.total_filled += additional_fill
                    state.fill_amount = current_filled
                    logger.info(
                        f"[{chase_id}] Partial fill: +{additional_fill:.4f}, "
                        f"Total: {state.total_filled:.4f}/{config.amount:.4f}"
                    )
        
        # Check if fully filled
        if state.is_fully_filled:
            state.status = "filled"
            state.fill_price = state.current_price
            logger.info(f"[{chase_id}] Fully filled! Total: {state.total_filled:.4f}")
            return
        
        remaining = state.remaining_amount
        if remaining <= 0:
            state.status = "filled"
            return
        
        # If order exists, try to AMEND it (single API call, lower latency)
        if state.current_order_id:
            try:
                result = await self._client.edit_order(
                    order_id=state.current_order_id,
                    symbol=config.symbol,
                    side=config.side,
                    amount=remaining,
                    price=target_price,
                    position_side=config.position_side,
                )
                
                state.current_price = target_price
                state.last_amend_time = time.time()
                state.retry_count += 1
                
                logger.info(
                    f"[{chase_id}] AMEND #{state.retry_count}: "
                    f"{config.side.upper()} {remaining:.4f} @ {target_price:.4f}"
                )
                return
                
            except Exception as e:
                error_str = str(e)
                
                # ORDER_NOT_FOUND = order was already filled, canceled, or rejected
                # Check WebSocket cache FIRST, then REST API as fallback
                if "ORDER_NOT_FOUND" in error_str or "10002" in error_str:
                    order_id_to_check = state.current_order_id
                    
                    # Step 1: Check WebSocket cache
                    order_update = self._order_updates.get(order_id_to_check)
                    ws_filled = order_update.get("filled", 0.0) if order_update else 0.0
                    ws_status = order_update.get("status", "") if order_update else ""
                    
                    # Step 2: If WS has no info, use REST API as fallback
                    if not order_update or ws_status == "":
                        logger.info(f"[{chase_id}] WS cache empty, checking REST API...")
                        rest_order = await self._client.fetch_order(
                            order_id_to_check,
                            config.symbol,
                        )
                        if rest_order:
                            ws_filled = float(rest_order.get("filled", 0.0))
                            ws_status = rest_order.get("status", "")
                            # Update cache with REST data
                            self._order_updates[order_id_to_check] = {
                                "status": ws_status,
                                "filled": ws_filled,
                                "remaining": rest_order.get("remaining", 0.0),
                                "average": rest_order.get("average"),
                            }
                            logger.info(
                                f"[{chase_id}] REST API: status={ws_status}, filled={ws_filled}"
                            )
                    
                    # Step 3: Process fill info
                    if ws_filled > state.fill_amount:
                        # Confirmed fill
                        additional = ws_filled - state.fill_amount
                        state.total_filled += additional
                        state.fill_amount = ws_filled
                        avg_price = (
                            order_update.get("average") if order_update 
                            else state.current_price
                        )
                        state.fill_price = avg_price or state.current_price
                        logger.info(
                            f"[{chase_id}] Fill confirmed: "
                            f"+{additional:.4f}, Total: {state.total_filled:.4f}"
                        )
                        # Check if fully filled and return immediately
                        if state.is_fully_filled:
                            state.status = "filled"
                            logger.info(f"[{chase_id}] Fully filled! Stopping chase.")
                            return
                    elif ws_status in ("closed", "filled"):
                        # Order is closed but we didn't detect any new fill
                        # This means order was fully filled before we checked
                        logger.info(
                            f"[{chase_id}] Order {order_id_to_check[:8]}... "
                            f"is {ws_status}, assuming filled"
                        )
                        state.total_filled = config.amount
                        state.status = "filled"
                        return
                    else:
                        # No fill detected - order was canceled/rejected
                        logger.warning(
                            f"[{chase_id}] Order {order_id_to_check[:8]}... "
                            f"not found, status={ws_status}, filled={ws_filled}"
                        )
                    
                    # Clear order ID, will place new order below
                    state.current_order_id = None
                
                # Minimum amount precision error = remaining too small, consider filled
                elif "minimum amount precision" in error_str.lower():
                    logger.info(
                        f"[{chase_id}] Remaining {remaining:.4f} below minimum precision, "
                        f"considering filled (total: {state.total_filled:.4f})"
                    )
                    state.status = "filled"
                    state.fill_price = state.current_price
                    return
                
                else:
                    # Other amend errors - try cancel+place
                    logger.warning(f"[{chase_id}] Amend failed, will cancel+place: {e}")
                    try:
                        await self._client.cancel_order(
                            state.current_order_id,
                            config.symbol,
                            position_side=config.position_side,
                        )
                    except Exception:
                        pass
                    state.current_order_id = None
        
        # Check again if fully filled after aggregating
        if state.is_fully_filled:
            state.status = "filled"
            state.fill_price = state.current_price
            return
        
        remaining = state.remaining_amount
        if remaining <= 0:
            state.status = "filled"
            return
        
        # Check minimum order size (Phemex min is ~0.01 SOL or $5 notional)
        # If remaining is too small, consider it filled
        MIN_ORDER_SIZE = 0.01
        if remaining < MIN_ORDER_SIZE:
            logger.info(
                f"[{chase_id}] Remaining {remaining:.4f} below minimum, "
                f"considering filled (total: {state.total_filled:.4f})"
            )
            state.status = "filled"
            state.fill_price = state.current_price
            return
        
        # Place NEW order (no existing order or amend failed)
        try:
            result = await self._client.place_limit_post_only(
                symbol=config.symbol,
                side=config.side,
                amount=remaining,
                price=target_price,
                reduce_only=config.reduce_only,
                position_side=config.position_side,
            )
            
            state.current_order_id = result.order_id
            state.current_price = target_price
            state.fill_amount = 0.0
            state.last_amend_time = time.time()
            state.retry_count += 1
            
            # Check if order was immediately rejected or filled
            # PostOnly orders can be rejected if they would cross the spread
            if result.status in ("rejected", "canceled", "expired"):
                logger.warning(
                    f"[{chase_id}] Order was {result.status} immediately. "
                    f"Price {target_price} may have crossed spread."
                )
                state.current_order_id = None
                return
            
            # Check if order was immediately filled
            if result.status in ("closed", "filled") and result.filled > 0:
                state.total_filled += result.filled
                state.fill_price = result.average or target_price
                logger.info(
                    f"[{chase_id}] Order filled immediately: "
                    f"+{result.filled:.4f}, Total: {state.total_filled:.4f}"
                )
                state.current_order_id = None
                return
            
            logger.info(
                f"[{chase_id}] NEW #{state.retry_count}: "
                f"{config.side.upper()} {remaining:.4f} @ {target_price:.4f}"
            )
            
        except Exception as e:
            error_str = str(e)
            
            # TE_QTY_TOO_SMALL = remaining is below minimum, consider filled
            if "TE_QTY_TOO_SMALL" in error_str or "11058" in error_str:
                logger.info(
                    f"[{chase_id}] Quantity too small ({remaining:.4f}), "
                    f"considering filled (total: {state.total_filled:.4f})"
                )
                state.status = "filled"
                state.fill_price = state.current_price
                return
            
            if "TE_REDUCE_ONLY_ABORT" in error_str or "11011" in error_str:
                logger.info(f"[{chase_id}] Position already closed (reduce_only abort)")
                state.status = "filled"
                state.total_filled = config.amount
                return
            
            logger.warning(f"[{chase_id}] Order failed: {e}")
            await asyncio.sleep(0.5)
    
    async def _check_order_filled(self, chase_id: str) -> bool:
        """
        Check if order is filled via WebSocket cache.
        
        Uses ONLY WebSocket order updates for fill detection.
        The order cache is populated by _stream_orders().
        
        Aggregates partial fills to total_filled.
        Returns True when fully filled.
        """
        state = self._active_chases[chase_id]
        
        if not state.current_order_id:
            return False
        
        # Get order update from WebSocket cache
        order_update = self._order_updates.get(state.current_order_id)
        
        if not order_update:
            # Order not in cache yet - wait for WS update
            return False
        
        status = order_update.get("status", "")
        filled = float(order_update.get("filled", 0.0))
        
        # Aggregate new fills
        if filled > state.fill_amount:
            additional = filled - state.fill_amount
            state.total_filled += additional
            state.fill_amount = filled
            logger.info(
                f"[{chase_id}] Fill detected: +{additional:.4f}, "
                f"Total: {state.total_filled:.4f}/{state.config.amount:.4f}"
            )
        
        # Check if order is closed/filled
        if status in ("closed", "filled"):
            if filled > 0:
                state.fill_price = order_update.get("average") or state.current_price
                logger.info(
                    f"[{chase_id}] Order {state.current_order_id[:8]}... "
                    f"filled: {filled:.4f} @ {state.fill_price:.4f}"
                )
            return state.is_fully_filled
        
        # Order was canceled or rejected
        if status in ("canceled", "rejected", "expired"):
            logger.warning(
                f"[{chase_id}] Order {state.current_order_id[:8]}... "
                f"was {status} with filled={filled:.4f}"
            )
            # Clear order ID so we place a new one
            state.current_order_id = None
            return state.is_fully_filled
        
        return False
