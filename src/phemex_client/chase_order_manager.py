# src/phemex_client/chase_order_manager.py
# Singleton manager for chase limit orders with real-time bid1/ask1 streaming
# Establishes WS connection on warmup, receives commands via async queue
# RELEVANT FILES: exchange_client.py, models.py, config.py, websocket_manager.py

"""
Chase Order Manager for Phemex.

Singleton service that:
1. Receives chase commands via async queue (minimal latency)
2. Uses WebsocketManager for prices and order updates
3. Updates orders when price changes

Usage:
    ws_manager = WebsocketManager.get_instance(client)
    manager = ChaseOrderManager.get_instance(client, ws_manager)
    await ws_manager.start(["SOL/USDT:USDT"])
    await manager.start()
    chase_id = await manager.submit_chase(config)
"""

import asyncio
import logging
import time
import uuid
from typing import Optional

from phemex_client.exchange_client import PhemexClient, truncate_to_step_size
from phemex_client.websocket_manager import WebsocketManager
from phemex_client.models import ChaseOrderConfig, ChaseOrderState, OrderResult


logger = logging.getLogger(__name__)


class ChaseOrderManager:
    """
    Singleton manager for chase limit orders.
    
    Processes chase commands via async queue.
    Relies on WebsocketManager for real-time data.
    """
    
    # Singleton instance
    _instance: Optional["ChaseOrderManager"] = None
    
    # Minimum price change to trigger order update (avoids spam)
    MIN_PRICE_CHANGE_PCT = 0.0001  # 0.01%
    
    # Minimum interval between amends (seconds)
    MIN_AMEND_INTERVAL = 1.0
    
    @classmethod
    def get_instance(cls, exchange_client: PhemexClient, ws_manager: Optional[WebsocketManager] = None) -> "ChaseOrderManager":
        """
        Get or create singleton instance.
        
        Args:
            exchange_client: Initialized PhemexClient
            ws_manager: Initialized WebsocketManager (required for first init)
        
        Returns:
            ChaseOrderManager singleton
        """
        if cls._instance is None:
            if ws_manager is None:
                raise ValueError("ws_manager required for initialization")
            cls._instance = cls(exchange_client, ws_manager)
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
    
    def __init__(self, exchange_client: PhemexClient, ws_manager: WebsocketManager):
        """
        Initialize chase order manager.
        
        Use get_instance() instead of direct construction.
        """
        self._client = exchange_client
        self._ws_manager = ws_manager
        
        # Active chase orders
        self._active_chases: dict[str, ChaseOrderState] = {}
        
        # Background tasks
        self._chase_tasks: dict[str, asyncio.Task] = {}
        
        # Command queue for submitting chase orders
        self._command_queue: asyncio.Queue = asyncio.Queue()
        self._command_processor_task: Optional[asyncio.Task] = None
        
        # State
        self._running = False
    
    async def start(self) -> None:
        """
        Start the chase manager (command processor).
        
        Note: WebsocketManager must be started separately.
        """
        if self._running:
            return
            
        self._running = True
        self._command_processor_task = asyncio.create_task(
            self._process_commands()
        )
        logger.info("ChaseOrderManager started")
    
    # Backward compatibility alias
    async def warmup(self, symbols: list[str]) -> None:
        """Alias for start() - symbols are handled by WebsocketManager."""
        await self.start()
    
    async def shutdown(self) -> None:
        """Shutdown manager and cancel all tasks."""
        logger.info("Shutting down ChaseOrderManager...")
        
        self._running = False
        
        # Cancel all chase orders
        for chase_id in list(self._active_chases.keys()):
            await self.cancel_chase(chase_id)
        
        # Cancel command processor
        if self._command_processor_task:
            self._command_processor_task.cancel()
            self._command_processor_task = None
        
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
        if not self._running:
             # Auto-start if not running
             await self.start()
        
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
    
    def get_all_active_chases(self) -> list[dict]:
        """Get status of all active chase orders."""
        return [
            self.get_chase_status(cid)
            for cid in self._active_chases
            if self._active_chases[cid].status == "active"
        ]

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
        # Sanitize amount to be a multiple of step size (0.01)
        safe_amount = truncate_to_step_size(config.amount)
        if safe_amount != config.amount:
            logger.info(f"[{chase_id}] Truncating amount {config.amount} -> {safe_amount}")
            config.amount = safe_amount

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
                
                # Get cached prices from WebsocketManager
                prices = self._ws_manager.get_prices(config.symbol)
                
                if not prices:
                    # Retry getting prices a few times before sleeping
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
        if config.side == "buy":
            price = min(price, bid1)
        else:
            price = max(price, ask1)
        
        return price
    
    async def _update_order(self, chase_id: str, target_price: float) -> None:
        """
        Place or update the chase order.
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        # Check for partial fills from WebSocket cache via WebsocketManager
        if state.current_order_id:
            order_update = self._ws_manager.get_order_update(state.current_order_id)
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
        
        # If order exists, try to AMEND it
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
                if "ORDER_NOT_FOUND" in error_str or "10002" in error_str:
                    order_id_to_check = state.current_order_id
                    
                    # Step 1: Check WebSocket cache
                    order_update = self._ws_manager.get_order_update(order_id_to_check)
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
                    
                    # Step 3: Process fill info
                    if ws_filled > state.fill_amount:
                        additional = ws_filled - state.fill_amount
                        state.total_filled += additional
                        state.fill_amount = ws_filled
                        state.fill_price = state.current_price
                        
                        logger.info(
                            f"[{chase_id}] Fill confirmed: "
                            f"+{additional:.4f}, Total: {state.total_filled:.4f}"
                        )
                        if state.is_fully_filled:
                            state.status = "filled"
                            return
                    elif ws_status in ("closed", "filled"):
                        logger.info(
                            f"[{chase_id}] Order is {ws_status}, assuming filled"
                        )
                        state.total_filled = config.amount
                        state.status = "filled"
                        return
                    else:
                        logger.warning(
                            f"[{chase_id}] Order not found/failed, status={ws_status}"
                        )
                    
                    # Clear order ID
                    state.current_order_id = None
                
                # Minimum amount precision error
                elif "minimum amount precision" in error_str.lower():
                    logger.info(
                        f"[{chase_id}] Remaining {remaining:.4f} below min precision, "
                        f"considering filled"
                    )
                    state.status = "filled"
                    state.fill_price = state.current_price
                    return
                
                else:
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
        
        # Check again if fully filled
        if state.is_fully_filled:
            state.status = "filled"
            state.fill_price = state.current_price
            return
        
        remaining = state.remaining_amount
        if remaining <= 0:
            state.status = "filled"
            return
            
        # Place NEW order
        if state.current_order_id is None:
            # Check minimum order size
            MIN_ORDER_SIZE = 0.01
            if remaining < MIN_ORDER_SIZE:
                logger.info(
                    f"[{chase_id}] Remaining {remaining:.4f} below minimum, "
                    f"considering filled"
                )
                state.status = "filled"
                state.fill_price = state.current_price
                return

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
                
                if result.status in ("rejected", "canceled", "expired"):
                    logger.warning(
                        f"[{chase_id}] Order was {result.status} immediately. "
                        f"Price {target_price} may have crossed spread."
                    )
                    state.current_order_id = None
                    return
                
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
                if "TE_QTY_TOO_SMALL" in error_str or "11058" in error_str:
                    logger.info(
                        f"[{chase_id}] Quantity too small ({remaining:.4f}), "
                        f"considering filled"
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
        """
        state = self._active_chases[chase_id]
        if not state.current_order_id:
            return False
            
        order_update = self._ws_manager.get_order_update(state.current_order_id)
        if not order_update:
            return False
            
        filled = order_update.get("filled", 0.0)
        remaining = order_update.get("remaining", 0.0)
        status = order_update.get("status")
        
        # Update state
        if filled > state.fill_amount:
            added = filled - state.fill_amount
            state.total_filled += added
            state.fill_amount = filled
            logger.info(f"[{chase_id}] Fill detected: +{added:.4f}")
            
        if status == "filled" or remaining == 0:
            state.fill_price = order_update.get("average") or state.current_price
            return True
            
        return False
