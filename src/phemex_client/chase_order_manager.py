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
        self._order_stream_task: Optional[asyncio.Task] = None
        self._chase_tasks: dict[str, asyncio.Task] = {}
        
        # Command queue for submitting chase orders
        self._command_queue: asyncio.Queue = asyncio.Queue()
        self._command_processor_task: Optional[asyncio.Task] = None
        
        # State
        self._warmed_up = False
        self._running = False
    
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
        
        # Start order updates stream (for fill detection)
        self._order_stream_task = asyncio.create_task(self._stream_orders())
        
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
        
        # Cancel order stream
        if self._order_stream_task:
            self._order_stream_task.cancel()
            self._order_stream_task = None
        
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
    
    async def _stream_orders(self) -> None:
        """
        Stream order updates via WebSocket.
        
        Caches order status for real-time fill detection.
        """
        logger.debug("Starting order stream")
        
        try:
            while True:
                try:
                    orders = await self._client.watch_orders()
                    
                    # Update order cache with latest status
                    for order in orders:
                        order_id = order.get("id")
                        if order_id:
                            self._order_updates[order_id] = {
                                "status": order.get("status"),
                                "filled": order.get("filled", 0.0),
                                "remaining": order.get("remaining", 0.0),
                                "average": order.get("average"),
                            }
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Order stream error: {e}")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        
        logger.debug("Order stream stopped")
    
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
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        try:
            while state.status == "active" and self._running:
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
                
                # Check if filled (uses WebSocket cache, instant)
                if state.current_order_id:
                    if self._check_order_filled(chase_id):
                        state.status = "filled"
                        logger.info(f"[{chase_id}] Filled at {state.fill_price}")
                        break
                
                # Small delay to avoid tight loop
                await asyncio.sleep(0.01)
                
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
        """Check if order needs to be placed/updated."""
        if state.current_order_id is None:
            return True
        
        if state.current_price == 0.0:
            return True
        
        price_change = abs(target_price - state.current_price)
        min_change = state.current_price * self.MIN_PRICE_CHANGE_PCT
        
        return price_change > min_change
    
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
        """Place or update the chase order."""
        state = self._active_chases[chase_id]
        config = state.config
        
        # Cancel existing order
        if state.current_order_id:
            try:
                await self._client.cancel_order(
                    state.current_order_id,
                    config.symbol,
                    position_side=config.position_side,
                )
            except Exception:
                pass  # May already be filled/canceled
        
        # Place new order
        try:
            result = await self._client.place_limit_post_only(
                symbol=config.symbol,
                side=config.side,
                amount=config.amount,
                price=target_price,
                reduce_only=config.reduce_only,
                position_side=config.position_side,
            )
            
            state.current_order_id = result.order_id
            state.current_price = target_price
            state.retry_count += 1
            
            logger.info(
                f"[{chase_id}] #{state.retry_count}: "
                f"{config.side.upper()} @ {target_price:.4f}"
            )
            
        except Exception as e:
            logger.error(f"[{chase_id}] Order failed: {e}")
    
    def _check_order_filled(self, chase_id: str) -> bool:
        """
        Check if current order is filled using WebSocket order cache.
        
        Uses real-time order updates instead of REST polling.
        """
        state = self._active_chases[chase_id]
        
        if not state.current_order_id:
            return False
        
        # Check WebSocket order cache
        order_update = self._order_updates.get(state.current_order_id)
        
        if not order_update:
            return False  # No update yet
        
        status = order_update.get("status", "")
        filled = order_update.get("filled", 0.0)
        
        # Track partial fills
        if filled > 0:
            state.fill_amount = filled
        
        # Check for full fill
        if status == "closed" or status == "filled":
            state.fill_price = order_update.get("average") or state.current_price
            state.fill_amount = order_update.get("filled", state.config.amount)
            return True
        
        # Check if canceled (might have been partially filled)
        if status == "canceled":
            if filled > 0:
                # Partial fill
                state.fill_price = order_update.get("average") or state.current_price
                return True
        
        return False
