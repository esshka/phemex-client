# src/phemex_client/position_manager.py
# Real-time position tracking via WebSocket
# Manages position state with read-only protection for preloaded positions
# RELEVANT FILES: exchange_client.py, models.py, signal_processor.py

"""
Position manager for real-time position tracking.

Features:
- Load existing positions at startup (marked read-only)
- Watch positions via WebSocket for live updates
- Thread-safe position state access
"""

import asyncio
import logging
from typing import Optional

from phemex_client.models import PositionState


logger = logging.getLogger(__name__)


class PositionManager:
    """
    Manages real-time position state.
    
    Uses CCXT Pro watch_positions() for live updates.
    Preloaded positions are marked read-only for protection.
    """
    
    def __init__(self, exchange):
        """
        Initialize position manager.
        
        Args:
            exchange: CCXT exchange instance (not PhemexClient wrapper)
        """
        self.exchange = exchange
        self.positions: dict[str, PositionState] = {}
        self._running = False
        self._watch_task: Optional[asyncio.Task] = None
    
    async def load_initial_positions(self) -> None:
        """
        Load existing positions at startup.
        
        All preloaded positions are marked as read-only to prevent
        accidental closing of positions opened externally.
        """
        positions = await self.exchange.fetch_positions()
        
        for pos in positions:
            contracts = pos.get("contracts", 0) or 0
            
            if contracts > 0:
                symbol = pos["symbol"]
                
                self.positions[symbol] = PositionState(
                    symbol=symbol,
                    side=pos.get("side", "long"),
                    contracts=contracts,
                    entry_price=pos.get("entryPrice", 0.0) or 0.0,
                    unrealized_pnl=pos.get("unrealizedPnl", 0.0) or 0.0,
                    leverage=pos.get("leverage", 1) or 1,
                    is_read_only=True,  # Preloaded = protected
                )
                
                logger.info(
                    f"Loaded position: {symbol} {pos['side']} "
                    f"{contracts} @ {pos.get('entryPrice')} [READ-ONLY]"
                )
    
    async def watch_positions(self) -> None:
        """
        Continuously poll position updates via REST API.
        
        Note: Phemex does not support WebSocket watch_positions(),
        so we poll fetch_positions() every 5 seconds instead.
        
        Updates local state as positions change.
        Runs until stop() is called.
        """
        self._running = True
        poll_interval = 5  # seconds
        logger.info(f"Starting position poller (interval: {poll_interval}s)")
        
        while self._running:
            try:
                positions = await self.exchange.fetch_positions()
                
                # Track which symbols we've seen in this update
                seen_symbols = set()
                
                for pos in positions:
                    contracts = pos.get("contracts", 0) or 0
                    if contracts > 0:
                        self._handle_position_update(pos)
                        seen_symbols.add(pos["symbol"])
                
                # Remove positions that are no longer present
                # (position was closed externally)
                for symbol in list(self.positions.keys()):
                    if symbol not in seen_symbols:
                        del self.positions[symbol]
                        logger.info(f"Position closed (external): {symbol}")
                
                # Wait before next poll
                await asyncio.sleep(poll_interval)
                    
            except asyncio.CancelledError:
                logger.info("Position poller cancelled")
                break
                
            except Exception as e:
                logger.error(f"Position poll error: {e}")
                # Brief delay before retry on error
                await asyncio.sleep(poll_interval)
        
        logger.info("Position poller stopped")
    
    def _handle_position_update(self, pos: dict) -> None:
        """
        Handle a single position update from WebSocket.
        
        Args:
            pos: Position data from CCXT
        """
        symbol = pos["symbol"]
        contracts = pos.get("contracts", 0) or 0
        
        if contracts == 0:
            # Position closed
            if symbol in self.positions:
                del self.positions[symbol]
                logger.info(f"Position closed: {symbol}")
        else:
            # Update existing or create new
            existing = self.positions.get(symbol)
            
            # Preserve read-only flag from preloaded positions
            is_read_only = existing.is_read_only if existing else False
            
            self.positions[symbol] = PositionState(
                symbol=symbol,
                side=pos.get("side", "long"),
                contracts=contracts,
                entry_price=pos.get("entryPrice", 0.0) or 0.0,
                unrealized_pnl=pos.get("unrealizedPnl", 0.0) or 0.0,
                leverage=pos.get("leverage", 1) or 1,
                is_read_only=is_read_only,
            )
    
    def get_position(self, symbol: str) -> Optional[PositionState]:
        """
        Get current position for a symbol.
        
        Args:
            symbol: Trading symbol
        
        Returns:
            PositionState if position exists, None otherwise
        """
        return self.positions.get(symbol)
    
    def has_position(self, symbol: str) -> bool:
        """Check if a position exists for symbol."""
        return symbol in self.positions
    
    def clear_position(self, symbol: str) -> None:
        """
        Immediately clear position state.
        
        Called after a close order is placed to update local state
        before the WebSocket update arrives.
        
        Args:
            symbol: Trading symbol
        """
        if symbol in self.positions:
            del self.positions[symbol]
            logger.debug(f"Cleared position state: {symbol}")
    
    def mark_as_managed(self, symbol: str) -> None:
        """
        Mark a position as managed (not read-only).
        
        Call this for positions that were opened by this listener
        to allow them to be closed by signals.
        
        Args:
            symbol: Trading symbol
        """
        if symbol in self.positions:
            pos = self.positions[symbol]
            # Create new state with is_read_only=False
            self.positions[symbol] = PositionState(
                symbol=pos.symbol,
                side=pos.side,
                contracts=pos.contracts,
                entry_price=pos.entry_price,
                unrealized_pnl=pos.unrealized_pnl,
                leverage=pos.leverage,
                is_read_only=False,
            )
            logger.debug(f"Marked position as managed: {symbol}")
    
    def get_all_positions(self) -> list[PositionState]:
        """Get all current positions."""
        return list(self.positions.values())
    
    def stop(self) -> None:
        """Stop watching positions."""
        self._running = False
        if self._watch_task:
            self._watch_task.cancel()
