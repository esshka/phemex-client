# src/phemex_client/signal_processor.py
# Signal processing logic for ENTRY, EXIT, and PARTIAL_EXIT actions
# Core business logic for converting signals into chase orders
# RELEVANT FILES: exchange_client.py, position_manager.py, chase_order_manager.py

"""
Signal processor for trading signals from NATS.

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


def normalize_symbol_for_phemex(symbol: str) -> str:
    """
    Convert signal symbol format to Phemex perpetual futures format.
    
    Signal format: SOL_USDT, BTC_USDT, ETH_USDT
    Phemex format: SOL/USDT:USDT, BTC/USDT:USDT, ETH/USDT:USDT
    
    Args:
        symbol: Symbol in signal format (underscore-separated)
    
    Returns:
        Symbol in Phemex perpetual futures format
    """
    # If already in correct format, return as-is
    if "/" in symbol and ":" in symbol:
        return symbol
    
    # Convert underscore to slash and add :USDT suffix for perpetuals
    # Example: SOL_USDT -> SOL/USDT:USDT
    symbol = symbol.replace("_", "/")
    
    # Add :USDT suffix if not present (for perpetual futures)
    if ":" not in symbol:
        symbol = f"{symbol}:USDT"
    
    return symbol


class SignalProcessor:
    """
    Processes trading signals from NATS.
    
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
        use_chase_orders: bool = False,
        chase_mode: str = "bid2",
        max_chase_retries: int = 50,
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
            use_chase_orders: If True, use chase orders. If False, use market in.
            chase_mode: Strategy for chase orders (bid1, bid2, etc)
            max_chase_retries: Max retries for chase orders
        """
        self.exchange = exchange_client
        self.positions = position_manager
        self.chase_manager = chase_manager
        self.deposit_size = deposit_size
        self.r_percentage = r_percentage
        self.leverage = leverage
        self.use_chase_orders = use_chase_orders
        self.chase_mode = chase_mode
        self.max_chase_retries = max_chase_retries
    
    def calculate_position_size(
        self,
        symbol: str,
        entry_price: float,
        stop_loss: Optional[float],
        position_size_r: float,
    ) -> float:
        """
        Calculate position size in contracts based on R units.
        
        R Value = deposit_size * r_percentage
        Target Notional = position_size_r * R Value
        
        Args:
            symbol: Trading symbol (needed for step size lookup)
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
        
        # Get the correct step size for this symbol (e.g., SOL=0.01, AAVE=0.1)
        step_size = self.exchange.get_amount_step_size(symbol)
        truncated = truncate_to_step_size(contracts, step_size)
        
        logger.info(
            f"Position sizing: r_value={r_value:.2f}, "
            f"target_notional={target_notional:.2f}, "
            f"raw_contracts={contracts:.6f}, step_size={step_size}, "
            f"truncated={truncated:.6f}"
        )
        
        return truncated
    
    async def process_signal(self, message: dict) -> None:
        """
        Route signal to appropriate handler.
        
        Args:
            message: Parsed signal message dictionary
        """
        action = message.get("action", "ENTRY").upper()
        
        if action == "ENTRY":
            await self._handle_entry(message)
        elif action == "EXIT":
            await self._handle_exit(message)
        elif action == "PARTIAL_EXIT":
            # Filtered out - TPs are handled via limit orders placed after entry
            logger.debug(f"Ignoring PARTIAL_EXIT signal (TPs use limit orders)")
        else:
            logger.warning(f"Unknown action: {action}")
    
    async def _handle_entry(self, message: dict) -> None:
        """
        Process ENTRY signal.
        
        Flow:
        1. Check for existing position (ignore if exists)
        2. Place market entry order with SL attached
        3. After fill, place limit close orders for each TP level
        
        Args:
            message: ENTRY signal data
        """
        # Normalize symbol from signal format to Phemex format
        # Example: SOL_USDT -> SOL/USDT:USDT
        symbol = normalize_symbol_for_phemex(message["symbol"])
        direction = message["direction"].upper()
        price = float(message["price"])
        stop_loss = message.get("stop_loss")
        position_size_r = float(message.get("position_size_r", 1.0))
        
        # TP levels for limit close orders (placed after entry fills)
        tp_levels = message.get("tp_levels", [])
        multi_tp_enabled = message.get("multi_tp_enabled", False)
        
        logger.info(
            f"Processing ENTRY: {direction} {symbol} @ {price}, "
            f"SL={stop_loss}, size_r={position_size_r}, "
            f"tp_levels={len(tp_levels) if multi_tp_enabled else 0}"
        )
        
        current_pos = self.positions.get_position(symbol)
        
        # If any position exists for this symbol, ignore ENTRY signal
        # This prevents duplicate entries and respects read-only positions
        if current_pos:
            read_only_tag = " [READ-ONLY]" if current_pos.is_read_only else ""
            logger.info(
                f"Ignoring ENTRY {direction} - position already exists: "
                f"{current_pos.side} {current_pos.contracts:.4f}{read_only_tag}"
            )
            return
        
        # Calculate position size
        contracts = self.calculate_position_size(
            symbol=symbol,
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
            # Place entry order with SL only (no conditional TP)
            # position_side is required for hedge mode accounts
            position_side = "long" if direction == "LONG" else "short"
            
            fill_result = await self._place_order(
                symbol=symbol,
                side=side,
                amount=contracts,
                position_side=position_side,
                reduce_only=False,
                is_entry=True,
                stop_loss=stop_loss,
                # Note: No take_profit here - we use limit orders instead
            )
            
            if not fill_result:
                return
                
            fill_price, filled_amount = fill_result
            
            # Mark position as managed (not read-only) so we can close it later
            self.positions.mark_as_managed(symbol)
            
            # Place limit TP orders if tp_levels provided
            if tp_levels and multi_tp_enabled:
                await self._place_limit_tp_orders(
                    symbol=symbol,
                    direction=direction,
                    total_contracts=filled_amount,
                    tp_levels=tp_levels,
                )
            
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
        # Normalize symbol from signal format to Phemex format
        symbol = normalize_symbol_for_phemex(message["symbol"])
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
        # Normalize symbol from signal format to Phemex format
        symbol = normalize_symbol_for_phemex(message["symbol"])
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
            # Place partial close order based on configured strategy
            # position_side: when closing a LONG, still 'long'; when closing SHORT, still 'short'
            position_side = current_pos.side.lower()
            
            fill_result = await self._place_order(
                symbol=symbol,
                side=side,
                amount=close_contracts,
                position_side=position_side,
                reduce_only=True
            )
            
            if fill_result:
                fill_price, filled_amount = fill_result
                logger.info(
                    f"Partial exit filled: {filled_amount:.4f} @ {fill_price:.4f}"
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
        Close entire position with market order.
        
        Cancels existing orders first (including SL/TP limit orders).
        Always uses market order for immediate fill.
        
        Args:
            position: Position to close
            price: Target price (for logging only, uses market)
        """
        # Cancel existing orders first (including SL/TP)
        await self.exchange.cancel_all_orders(position.symbol)
        
        # Determine close side
        side = "sell" if position.side == "long" else "buy"
        
        # position_side: when closing a LONG, still 'long'; when closing SHORT, still 'short'
        position_side = position.side.lower()
        
        try:
            # Always use market order for EXIT (immediate fill)
            step_size = self.exchange.get_amount_step_size(position.symbol)
            safe_amount = truncate_to_step_size(position.contracts, step_size)
            
            params = {"reduceOnly": True}
            if position_side:
                params["posSide"] = position_side.capitalize()
            
            logger.info(
                f"Closing position with MARKET order: {side.upper()} "
                f"{safe_amount:.4f} {position.symbol}"
            )
            
            order = await self.exchange._create_order_with_retry(
                symbol=position.symbol,
                order_type="market",
                side=side,
                amount=safe_amount,
                price=None,
                params=params
            )
            
            fill_price = order.get("average") or order.get("price") or 0.0
            filled = order.get("filled") or safe_amount
            
            logger.info(f"Position closed: {filled:.4f} @ {fill_price:.4f}")
            
            # Immediately update local state
            # WebSocket will confirm, but this prevents duplicate signals
            self.positions.clear_position(position.symbol)
            
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

    async def _place_limit_tp_orders(
        self,
        symbol: str,
        direction: str,
        total_contracts: float,
        tp_levels: list[dict],
    ) -> None:
        """
        Place limit close orders for each TP level.
        
        Called after entry order fills. Places reduce-only limit orders
        at each TP price to automatically close portions of the position.
        
        Args:
            symbol: Trading symbol (Phemex format)
            direction: LONG or SHORT
            total_contracts: Total position size (filled amount)
            tp_levels: List of TP levels with 'price' and 'exit_pct'
        """
        # For LONG: close side is 'sell'
        # For SHORT: close side is 'buy'
        close_side = "sell" if direction == "LONG" else "buy"
        position_side = "long" if direction == "LONG" else "short"
        
        for i, tp in enumerate(tp_levels):
            tp_price = tp.get("price")
            exit_pct = tp.get("exit_pct", 0.0)
            
            if not tp_price or exit_pct <= 0:
                logger.warning(f"Skipping invalid TP level {i+1}: {tp}")
                continue
            
            # Calculate amount for this TP level
            tp_amount = total_contracts * exit_pct
            
            # Truncate to step size
            step_size = self.exchange.get_amount_step_size(symbol)
            tp_amount = truncate_to_step_size(tp_amount, step_size)
            
            if tp_amount <= 0:
                logger.warning(f"TP{i+1} amount too small after truncation")
                continue
            
            try:
                order = await self.exchange.place_limit_post_only(
                    symbol=symbol,
                    side=close_side,
                    amount=tp_amount,
                    price=tp_price,
                    reduce_only=True,
                    position_side=position_side,
                )
                
                logger.info(
                    f"Placed TP{i+1} limit order: {close_side.upper()} "
                    f"{tp_amount:.4f} @ {tp_price} (order_id={order.order_id})"
                )
                
            except Exception as e:
                logger.error(f"Failed to place TP{i+1} limit order: {e}")

    async def _place_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        position_side: str,
        reduce_only: bool = False,
        is_entry: bool = False,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> Optional[tuple[float, float]]:
        """
        Execute order using configured strategy (Market or Chase).
        
        Args:
            symbol: Trading symbol
            side: 'buy' or 'sell'
            amount: Order amount
            position_side: 'long' or 'short' (for hedge mode)
            reduce_only: If True, reduce-only order
            is_entry: If True, this is an entry order (different logging/timeouts)
            
        Returns:
            Tuple (fill_price, filled_amount) if successful, None if failed
        """
        if self.use_chase_orders:
            # --- CHASE LIMIT STRATEGY ---
            timeout = 60.0 if not reduce_only else 30.0
            
            chase_config = ChaseOrderConfig(
                symbol=symbol,
                side=side,
                amount=amount,
                chase_mode=self.chase_mode,
                max_chase_distance=0,  # No distance monitoring for now
                max_retries=self.max_chase_retries,
                reduce_only=reduce_only,
                position_side=position_side,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )
            
            logger.info(
                f"Submitting CHASE {'entry' if is_entry else 'exit'}: "
                f"{side.upper()} {amount:.4f} {symbol}"
            )
            
            try:
                chase_state = await self.chase_manager.submit_chase_and_wait(
                    chase_config, timeout=timeout
                )
                
                if chase_state.status != "filled":
                    logger.warning(
                        f"Chase did not fill: {chase_state.status}, "
                        f"filled={chase_state.total_filled:.4f}"
                    )
                    return None
                
                logger.info(
                    f"Chase filled: {chase_state.total_filled:.4f} @ "
                    f"{chase_state.fill_price:.4f}"
                )
                return chase_state.fill_price, chase_state.total_filled
                
            except TimeoutError:
                logger.error(f"Chase timed out for {symbol}")
                return None
        
        else:
            # --- MARKET ORDER STRATEGY ---
            logger.info(
                f"Placing MARKET {'entry' if is_entry else 'exit'}: "
                f"{side.upper()} {amount:.4f} {symbol}"
            )
            
            try:
                # Need to use create_order directly for market orders
                # Truncate amount first
                step_size = self.exchange.get_amount_step_size(symbol)
                safe_amount = truncate_to_step_size(amount, step_size)
                
                params = {"reduceOnly": reduce_only}
                if position_side:
                    params["posSide"] = position_side.capitalize()
                
                if stop_loss:
                    params["stopLossRp"] = str(stop_loss)
                    params["slTrigger"] = "ByMarkPrice"
                if take_profit:
                    params["takeProfitRp"] = str(take_profit)
                    params["tpTrigger"] = "ByMarkPrice"
                
                order = await self.exchange._create_order_with_retry(
                    symbol=symbol,
                    order_type="market",
                    side=side,
                    amount=safe_amount,
                    price=None,
                    params=params
                )
                
                # Market orders fill immediately usually, but we should parse result
                fill_price = order.get("average") or order.get("price") or 0.0
                filled = order.get("filled") or safe_amount
                
                logger.info(f"Market order filled: {filled:.4f} @ {fill_price:.4f}")
                return float(fill_price), float(filled)
                
            except Exception as e:
                logger.error(f"Market order execution failed: {e}")
                return None
