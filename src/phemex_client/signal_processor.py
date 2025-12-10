# src/phemex_client/signal_processor.py
# Signal processing logic for ENTRY, EXIT, and PARTIAL_EXIT actions
# Core business logic for converting signals into chase orders
# RELEVANT FILES: exchange_client.py, position_manager.py, chase_order_manager.py

"""
Signal processor for trading signals from ZMQ.

Handles:
- ENTRY: Open new positions with chase orders (follows bid/ask for fill)
- EXIT: Close entire positions with chase orders
- PARTIAL_EXIT: Close fraction of position, optionally move SL to break-even

Uses chase orders for all entries/exits to ensure fills.
Uses market orders only for stop-loss protection (safety net).
"""

import asyncio
import logging
from typing import Optional

from phemex_client.exchange_client import PhemexClient, truncate_to_step_size
from phemex_client.position_manager import PositionManager
from phemex_client.chase_order_manager import ChaseOrderManager
from phemex_client.models import PositionState, Direction, ChaseOrderConfig


logger = logging.getLogger(__name__)


class SignalProcessor:
    """
    Processes trading signals from ZMQ.
    
    Converts signals into appropriate order placements.
    Manages position lifecycle from entry to exit.
    """
    
    def __init__(
        self,
        exchange_client: PhemexClient,
        position_manager: PositionManager,
        chase_manager: ChaseOrderManager,
        deposit_size: float = 1000.0,
        r_percentage: float = 0.01,
        leverage: int = 20,
    ):
        """
        Initialize signal processor.
        
        Args:
            exchange_client: Phemex client for order placement
            position_manager: Position manager for state tracking
            chase_manager: Chase order manager for limit order execution
            deposit_size: Total deposit in USDT
            r_percentage: Risk percentage per R unit (e.g. 0.01 = 1%)
            leverage: Default leverage multiplier
        """
        self.exchange = exchange_client
        self.positions = position_manager
        self.chase_manager = chase_manager
        self.deposit_size = deposit_size
        self.r_percentage = r_percentage
        self.leverage = leverage
    
    def calculate_position_size(
        self,
        entry_price: float,
        stop_loss: Optional[float],
        position_size_r: float,
    ) -> float:
        """
        Calculate position size in contracts based on R units.
        
        R Value = deposit_size * r_percentage
        Target Notional = position_size_r * R Value
        
        Args:
            entry_price: Entry price
            stop_loss: Stop-loss price (used for risk calc, not sizing here)
            position_size_r: Position size in R units
        
        Returns:
            Position size in contracts
        """
        # Calculate R value
        r_value = self.deposit_size * self.r_percentage
        
        # Target notional value
        target_notional = position_size_r * r_value
        
        # Safety cap: max 95% of buying power
        max_notional = self.deposit_size * self.leverage * 0.95
        target_notional = min(target_notional, max_notional)
        
        # Convert to contracts
        # Note: Phemex uses the asset as contract unit, not USD
        contracts = target_notional / entry_price
        
        # Round to reasonable precision
        return truncate_to_step_size(contracts)
    
    async def process_signal(self, message: dict) -> None:
        """
        Route signal to appropriate handler.
        
        Args:
            message: Parsed ZMQ message dictionary
        """
        action = message.get("action", "ENTRY").upper()
        
        if action == "ENTRY":
            await self._handle_entry(message)
        elif action == "EXIT":
            await self._handle_exit(message)
        elif action == "PARTIAL_EXIT":
            await self._handle_partial_exit(message)
        else:
            logger.warning(f"Unknown action: {action}")
    
    async def _handle_entry(self, message: dict) -> None:
        """
        Process ENTRY signal.
        
        Flow:
        1. Check for existing position (same direction = ignore)
        2. If opposite position exists, close it first
        3. Place new entry order (limit post-only)
        4. Place stop-loss (market conditional)
        
        Args:
            message: ENTRY signal data
        """
        symbol = message["symbol"]
        direction = message["direction"].upper()
        price = float(message["price"])
        stop_loss = message.get("stop_loss")
        take_profit = message.get("take_profit")
        position_size_r = float(message.get("position_size_r", 1.0))
        
        logger.info(
            f"Processing ENTRY: {direction} {symbol} @ {price}, "
            f"SL={stop_loss}, size_r={position_size_r}"
        )
        
        current_pos = self.positions.get_position(symbol)
        
        # Check if already have position in same direction
        if current_pos:
            is_same_direction = (
                (current_pos.side == "long" and direction == "LONG") or
                (current_pos.side == "short" and direction == "SHORT")
            )
            
            if is_same_direction:
                logger.info(
                    f"Ignoring {direction} - already have "
                    f"{current_pos.side} position"
                )
                return
            
            # Opposite position exists - close it first
            if current_pos.is_read_only:
                logger.warning(
                    f"Cannot close {symbol} - READ-ONLY position"
                )
                return
            
            logger.info(f"Closing opposite {current_pos.side} position first")
            await self._close_position(current_pos, price)
            
            # Brief delay after close for position to clear
            await asyncio.sleep(0.5)
        
        # Calculate position size
        contracts = self.calculate_position_size(
            entry_price=price,
            stop_loss=stop_loss,
            position_size_r=position_size_r,
        )
        
        if contracts <= 0:
            logger.error("Calculated position size is zero or negative")
            return
        
        # Determine order side
        side = "buy" if direction == "LONG" else "sell"
        
        try:
            # Place entry order using chase (follows bid/ask for fill)
            # position_side is required for hedge mode accounts
            position_side = "long" if direction == "LONG" else "short"
            
            chase_config = ChaseOrderConfig(
                symbol=symbol,
                side=side,
                amount=contracts,
                # Default chase mode: bid2 for buys, ask2 for sells (one tick back)
                max_chase_distance=0,  # No distance limit for entries
                max_retries=100,       # Generous retry limit
                reduce_only=False,
                position_side=position_side,
            )
            
            logger.info(
                f"Submitting chase entry: {direction} {contracts:.4f} {symbol}"
            )
            
            # Wait for chase to fill (up to 60s)
            chase_state = await self.chase_manager.submit_chase_and_wait(
                chase_config, timeout=60.0
            )
            
            if chase_state.status != "filled":
                logger.warning(
                    f"Entry chase did not fill: {chase_state.status}, "
                    f"filled={chase_state.total_filled:.4f}"
                )
                return
            
            # Entry filled - log the result
            fill_price = chase_state.fill_price or price
            filled_amount = chase_state.total_filled
            
            logger.info(
                f"Entry filled: {direction} {filled_amount:.4f} @ {fill_price:.4f}"
            )
            
            # Place stop-loss (market conditional) - SAFETY NET ONLY
            # System sends EXIT signals for internal SL logic.
            # Add 0.2% buffer to prevent premature triggers.
            if stop_loss:
                sl_side = "sell" if direction == "LONG" else "buy"
                
                # Apply 0.2% buffer: move SL slightly further from entry
                # LONG: SL is below entry, so subtract 0.2%
                # SHORT: SL is above entry, so add 0.2%
                sl_buffer = 0.002  # 0.2%
                if direction == "LONG":
                    buffered_sl = stop_loss * (1 - sl_buffer)
                else:
                    buffered_sl = stop_loss * (1 + sl_buffer)
                
                sl_order = await self.exchange.place_stop_loss_market(
                    symbol=symbol,
                    side=sl_side,
                    amount=filled_amount,  # Use actual filled amount
                    trigger_price=buffered_sl,
                )
                
                logger.info(
                    f"Stop-loss placed: {sl_order.order_id} "
                    f"trigger @ {buffered_sl:.4f} (buffered from {stop_loss})"
                )
            
            # Mark position as managed (not read-only) so we can close it later
            self.positions.mark_as_managed(symbol)
            
        except TimeoutError:
            logger.error(f"Entry chase timed out for {symbol}")
        except Exception as e:
            logger.error(f"Entry order failed: {e}")
    
    async def _handle_exit(self, message: dict) -> None:
        """
        Process EXIT signal.
        
        Close entire position using limit post-only order.
        
        Args:
            message: EXIT signal data
        """
        symbol = message["symbol"]
        direction = message["direction"].upper()
        price = float(message["price"])
        reason = message.get("reason", "Signal")
        
        logger.info(
            f"Processing EXIT: {direction} {symbol} @ {price}, "
            f"reason={reason}"
        )
        
        current_pos = self.positions.get_position(symbol)
        
        if not current_pos:
            logger.warning(f"EXIT signal but no position for {symbol}")
            return
        
        # Verify direction matches position
        is_matching = (
            (current_pos.side == "long" and direction == "LONG") or
            (current_pos.side == "short" and direction == "SHORT")
        )
        
        if not is_matching:
            logger.warning(
                f"EXIT direction mismatch: signal={direction}, "
                f"position={current_pos.side}"
            )
            return
        
        if current_pos.is_read_only:
            logger.warning(f"Cannot exit {symbol} - READ-ONLY position")
            return
        
        await self._close_position(current_pos, price)
    
    async def _handle_partial_exit(self, message: dict) -> None:
        """
        Process PARTIAL_EXIT signal.
        
        Close fraction of position using limit post-only order.
        Optionally move stop-loss to break-even.
        
        Args:
            message: PARTIAL_EXIT signal data
        """
        symbol = message["symbol"]
        direction = message["direction"].upper()
        price = float(message["price"])
        exit_pct = float(message.get("exit_pct", 0.5))
        move_sl_to_be = message.get("move_sl_to_be", False)
        tp_level = message.get("tp_level", 1)
        
        logger.info(
            f"Processing PARTIAL_EXIT: {direction} {symbol} @ {price}, "
            f"exit_pct={exit_pct}, tp_level={tp_level}"
        )
        
        current_pos = self.positions.get_position(symbol)
        
        if not current_pos:
            logger.warning(f"PARTIAL_EXIT but no position for {symbol}")
            return
        
        if current_pos.is_read_only:
            logger.warning(
                f"Cannot partial exit {symbol} - READ-ONLY position"
            )
            return
        
        # Calculate contracts to close
        close_contracts = current_pos.contracts * exit_pct
        close_contracts = max(close_contracts, 0.001)  # Minimum
        
        # Round to reasonable precision
        close_contracts = round(close_contracts, 6)
        
        # Determine close side
        side = "sell" if current_pos.side == "long" else "buy"
        
        try:
            # Place partial close order using chase (follows bid/ask for fill)
            # position_side: when closing a LONG, still 'long'; when closing SHORT, still 'short'
            position_side = current_pos.side.lower()
            
            chase_config = ChaseOrderConfig(
                symbol=symbol,
                side=side,
                amount=close_contracts,
                # Default chase mode: bid2 for buys, ask2 for sells
                max_chase_distance=0,  # No distance limit for exits
                max_retries=50,        # Lower limit for partial exits
                reduce_only=True,
                position_side=position_side,
            )
            
            logger.info(
                f"Submitting chase partial exit: {close_contracts:.4f} of "
                f"{current_pos.contracts:.4f} {symbol}"
            )
            
            # Wait for chase to fill (up to 30s for partial exits)
            chase_state = await self.chase_manager.submit_chase_and_wait(
                chase_config, timeout=30.0
            )
            
            if chase_state.status != "filled":
                logger.warning(
                    f"Partial exit chase did not fill: {chase_state.status}, "
                    f"filled={chase_state.total_filled:.4f}"
                )
            else:
                logger.info(
                    f"Partial exit filled: {chase_state.total_filled:.4f} "
                    f"@ {chase_state.fill_price:.4f}"
                )
            
            # Move SL to break-even if requested
            if move_sl_to_be:
                await self._move_sl_to_breakeven(symbol, current_pos)
            
        except TimeoutError:
            logger.error(f"Partial exit chase timed out for {symbol}")
        except Exception as e:
            logger.error(f"Partial exit failed: {e}")
    
    async def _close_position(
        self,
        position: PositionState,
        price: float,
    ) -> None:
        """
        Close entire position with chase order.
        
        Cancels existing orders first (including SL/TP).
        Uses chase order to ensure fill.
        
        Args:
            position: Position to close
            price: Target price (used for logging, chase follows market)
        """
        # Cancel existing orders first (including SL/TP)
        await self.exchange.cancel_all_orders(position.symbol)
        
        # Determine close side
        side = "sell" if position.side == "long" else "buy"
        
        # position_side: when closing a LONG, still 'long'; when closing SHORT, still 'short'
        position_side = position.side.lower()
        
        try:
            chase_config = ChaseOrderConfig(
                symbol=position.symbol,
                side=side,
                amount=position.contracts,
                # Default chase mode: bid2 for buys, ask2 for sells
                max_chase_distance=0,  # No distance limit for closes
                max_retries=100,       # Generous retry limit for close
                reduce_only=True,
                position_side=position_side,
            )
            
            logger.info(
                f"Submitting chase close: {position.contracts:.4f} {position.symbol}"
            )
            
            # Wait for chase to fill (up to 60s for closes)
            chase_state = await self.chase_manager.submit_chase_and_wait(
                chase_config, timeout=60.0
            )
            
            if chase_state.status != "filled":
                logger.warning(
                    f"Close chase did not fill: {chase_state.status}, "
                    f"filled={chase_state.total_filled:.4f}"
                )
            else:
                logger.info(
                    f"Position closed: {chase_state.total_filled:.4f} "
                    f"@ {chase_state.fill_price:.4f}"
                )
            
            # Immediately update local state
            # WebSocket will confirm, but this prevents duplicate signals
            self.positions.clear_position(position.symbol)
            
        except TimeoutError:
            logger.error(f"Close chase timed out for {position.symbol}")
        except Exception as e:
            logger.error(f"Close position failed: {e}")
    
    async def _move_sl_to_breakeven(
        self,
        symbol: str,
        position: PositionState,
    ) -> None:
        """
        Move stop-loss to break-even (entry price).
        
        Cancels existing orders and places new SL at entry.
        
        Args:
            symbol: Trading symbol
            position: Current position state
        """
        # Cancel existing orders
        await self.exchange.cancel_all_orders(symbol)
        
        # Recalculate remaining contracts from position manager
        # (may have been partially filled)
        current = self.positions.get_position(symbol)
        remaining = current.contracts if current else position.contracts
        
        # Determine SL side
        sl_side = "sell" if position.side == "long" else "buy"
        
        try:
            sl_order = await self.exchange.place_stop_loss_market(
                symbol=symbol,
                side=sl_side,
                amount=remaining,
                trigger_price=position.entry_price,
            )
            
            logger.info(
                f"SL moved to BE: {sl_order.order_id} "
                f"{symbol} @ {position.entry_price}"
            )
            
        except Exception as e:
            logger.error(f"Move SL to BE failed: {e}")
