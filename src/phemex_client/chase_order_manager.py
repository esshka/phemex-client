# src/phemex_client/chase_order_manager.py
# Manages chase limit orders that follow market bid1/ask1 prices
# Places and updates orders to stay at top of orderbook until filled
# RELEVANT FILES: exchange_client.py, models.py, config.py

"""
Chase Order Manager for Phemex.

A chase order places a limit order at bid1/ask1 and automatically
updates it to follow market price until filled, canceled, or
max chase distance is reached.

Key features:
- Bid1/Ask1 chasing: stay at top of orderbook
- Distance mode: maintain fixed offset from best price
- Max chase distance: stop chasing if price moves too far
- Max retries: limit number of order updates
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
    Manages chase limit orders that follow market price.
    
    Chase orders are placed at bid1/ask1 (or at a distance from them)
    and automatically updated when the price moves.
    """
    
    # Minimum price change to trigger order update (avoids spam)
    MIN_PRICE_CHANGE_PCT = 0.0001  # 0.01%
    
    def __init__(self, exchange_client: PhemexClient):
        """
        Initialize chase order manager.
        
        Args:
            exchange_client: Initialized PhemexClient
        """
        self._client = exchange_client
        self._active_chases: dict[str, ChaseOrderState] = {}
        self._chase_tasks: dict[str, asyncio.Task] = {}
    
    async def start_chase(self, config: ChaseOrderConfig) -> str:
        """
        Start chasing with given configuration.
        
        Places initial order and starts background task to chase price.
        
        Args:
            config: Chase order configuration
        
        Returns:
            chase_id: Unique ID to track this chase
        """
        chase_id = str(uuid.uuid4())[:8]
        
        logger.info(
            f"[{chase_id}] Starting chase: {config.side.upper()} "
            f"{config.amount} {config.symbol} mode={config.chase_mode}"
        )
        
        # Create initial state (will be populated when order is placed)
        state = ChaseOrderState(
            chase_id=chase_id,
            config=config,
            current_order_id=None,
            initial_price=0.0,
            current_price=0.0,
        )
        
        self._active_chases[chase_id] = state
        
        # Start chase loop in background
        task = asyncio.create_task(self._chase_loop(chase_id))
        self._chase_tasks[chase_id] = task
        
        return chase_id
    
    async def cancel_chase(self, chase_id: str) -> bool:
        """
        Cancel an active chase order.
        
        Cancels the current limit order and stops chasing.
        
        Args:
            chase_id: ID returned from start_chase
        
        Returns:
            True if canceled, False if not found
        """
        if chase_id not in self._active_chases:
            logger.warning(f"[{chase_id}] Chase not found")
            return False
        
        state = self._active_chases[chase_id]
        state.status = "canceled"
        
        # Cancel background task
        if chase_id in self._chase_tasks:
            self._chase_tasks[chase_id].cancel()
            del self._chase_tasks[chase_id]
        
        # Cancel current order if exists
        if state.current_order_id:
            try:
                await self._client.cancel_order(
                    state.current_order_id,
                    state.config.symbol,
                )
                logger.info(f"[{chase_id}] Canceled order {state.current_order_id}")
            except Exception as e:
                logger.warning(f"[{chase_id}] Cancel order failed: {e}")
        
        logger.info(f"[{chase_id}] Chase canceled")
        return True
    
    def get_chase_status(self, chase_id: str) -> Optional[dict]:
        """
        Get status of a chase order.
        
        Args:
            chase_id: ID returned from start_chase
        
        Returns:
            Status dict or None if not found
        """
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
    
    def get_all_active_chases(self) -> list[dict]:
        """Get status of all active chase orders."""
        return [
            self.get_chase_status(cid)
            for cid in self._active_chases
            if self._active_chases[cid].status == "active"
        ]
    
    async def _chase_loop(self, chase_id: str) -> None:
        """
        Main chase loop - watches orderbook and updates order.
        
        Runs until order is filled, canceled, or limits reached.
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        try:
            # Watch orderbook for price updates
            while state.status == "active":
                # Get latest orderbook
                orderbook = await self._client.watch_order_book(config.symbol)
                
                if not orderbook["bids"] or not orderbook["asks"]:
                    logger.warning(f"[{chase_id}] Empty orderbook, waiting...")
                    await asyncio.sleep(0.5)
                    continue
                
                # Calculate target price
                bid1 = orderbook["bids"][0][0]
                ask1 = orderbook["asks"][0][0]
                target_price = self._calculate_target_price(config, bid1, ask1)
                
                # Set initial price on first iteration
                if state.initial_price == 0.0:
                    state.initial_price = target_price
                
                # Check if max chase distance reached
                if config.max_chase_distance > 0:
                    distance = abs(target_price - state.initial_price)
                    if distance > config.max_chase_distance:
                        logger.info(
                            f"[{chase_id}] Max chase distance reached: "
                            f"{distance:.4f} > {config.max_chase_distance}"
                        )
                        state.status = "stopped"
                        break
                
                # Check if max retries reached
                if state.retry_count >= config.max_retries:
                    logger.info(
                        f"[{chase_id}] Max retries reached: {state.retry_count}"
                    )
                    state.status = "stopped"
                    break
                
                # Check if we need to place/update order
                needs_update = False
                
                if state.current_order_id is None:
                    # No order yet, place initial
                    needs_update = True
                elif state.current_price != 0.0:
                    # Check if price changed enough to update
                    price_change = abs(target_price - state.current_price)
                    min_change = state.current_price * self.MIN_PRICE_CHANGE_PCT
                    if price_change > min_change:
                        needs_update = True
                
                if needs_update:
                    await self._update_order(chase_id, target_price)
                
                # Check order status
                if state.current_order_id:
                    filled = await self._check_order_filled(chase_id)
                    if filled:
                        state.status = "filled"
                        logger.info(
                            f"[{chase_id}] Order filled at {state.fill_price}"
                        )
                        break
                
        except asyncio.CancelledError:
            logger.info(f"[{chase_id}] Chase task canceled")
        except Exception as e:
            logger.error(f"[{chase_id}] Chase loop error: {e}", exc_info=True)
            state.status = "error"
        finally:
            # Cleanup
            if chase_id in self._chase_tasks:
                del self._chase_tasks[chase_id]
    
    def _calculate_target_price(
        self,
        config: ChaseOrderConfig,
        bid1: float,
        ask1: float,
    ) -> float:
        """
        Calculate target order price based on chase mode.
        
        Args:
            config: Chase configuration
            bid1: Best bid price
            ask1: Best ask price
        
        Returns:
            Target price for the order
        """
        if config.chase_mode == "bid1":
            # Place at best bid
            return bid1
        elif config.chase_mode == "ask1":
            # Place at best ask
            return ask1
        elif config.chase_mode == "distance":
            # Place at distance from best price
            if config.side == "buy":
                # For buys, place below bid1
                return bid1 - config.price_distance
            else:
                # For sells, place above ask1
                return ask1 + config.price_distance
        else:
            # Default to bid1 for buys, ask1 for sells
            return bid1 if config.side == "buy" else ask1
    
    async def _update_order(self, chase_id: str, target_price: float) -> None:
        """
        Place or update the chase order.
        
        Cancels existing order (if any) and places new one at target price.
        """
        state = self._active_chases[chase_id]
        config = state.config
        
        # Cancel existing order if any
        if state.current_order_id:
            try:
                await self._client.cancel_order(
                    state.current_order_id,
                    config.symbol,
                )
                logger.debug(f"[{chase_id}] Canceled order {state.current_order_id}")
            except Exception as e:
                # Order might already be filled or canceled
                logger.debug(f"[{chase_id}] Cancel failed (may be filled): {e}")
        
        # Place new order at target price
        try:
            result = await self._client.place_limit_post_only(
                symbol=config.symbol,
                side=config.side,
                amount=config.amount,
                price=target_price,
                reduce_only=config.reduce_only,
            )
            
            state.current_order_id = result.order_id
            state.current_price = target_price
            state.retry_count += 1
            
            logger.info(
                f"[{chase_id}] Order #{state.retry_count}: "
                f"{config.side.upper()} {config.amount} @ {target_price:.4f}"
            )
            
        except Exception as e:
            logger.error(f"[{chase_id}] Place order failed: {e}")
            # Don't increment retry on failure, let it try again
    
    async def _check_order_filled(self, chase_id: str) -> bool:
        """
        Check if current order is filled.
        
        Returns True if fully filled.
        """
        state = self._active_chases[chase_id]
        
        if not state.current_order_id:
            return False
        
        try:
            orders = await self._client.fetch_open_orders(state.config.symbol)
            
            # Check if our order is still open
            for order in orders:
                if order.get("id") == state.current_order_id:
                    # Order still open, update fill amount
                    state.fill_amount = order.get("filled", 0.0)
                    return False
            
            # Order not in open orders - check if it was filled
            # If order was filled, it won't be in open orders
            # We assume filled if we placed it and it's gone
            if state.retry_count > 0:
                state.fill_price = state.current_price
                state.fill_amount = state.config.amount
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"[{chase_id}] Check fill status error: {e}")
            return False
