# tests/test_position_manager.py
# Unit tests for PositionManager class
# Tests position loading, state tracking, and read-only protection
# RELEVANT FILES: position_manager.py, models.py

"""
Tests for PositionManager.

Uses mocked exchange for position data.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.position_manager import PositionManager
from phemex_client.models import PositionState


class TestLoadInitialPositions:
    """Tests for loading initial positions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = PositionManager(self.exchange)
    
    @pytest.mark.asyncio
    async def test_load_empty_positions(self):
        """Test loading when no positions exist."""
        self.exchange.fetch_positions = AsyncMock(return_value=[])
        
        await self.manager.load_initial_positions()
        
        assert len(self.manager.positions) == 0
    
    @pytest.mark.asyncio
    async def test_load_existing_position(self):
        """Test loading an existing position."""
        self.exchange.fetch_positions = AsyncMock(return_value=[
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 0.1,
                "entryPrice": 42000.0,
                "unrealizedPnl": 100.0,
                "leverage": 20,
            }
        ])
        
        await self.manager.load_initial_positions()
        
        assert len(self.manager.positions) == 1
        pos = self.manager.get_position("BTC/USDT:USDT")
        
        assert pos is not None
        assert pos.side == "long"
        assert pos.contracts == 0.1
        assert pos.entry_price == 42000.0
        assert pos.is_read_only is True  # Preloaded = protected
    
    @pytest.mark.asyncio
    async def test_load_multiple_positions(self):
        """Test loading multiple positions."""
        self.exchange.fetch_positions = AsyncMock(return_value=[
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 0.1,
                "entryPrice": 42000.0,
                "unrealizedPnl": 100.0,
                "leverage": 20,
            },
            {
                "symbol": "SOL/USDT:USDT",
                "side": "short",
                "contracts": 5.0,
                "entryPrice": 100.0,
                "unrealizedPnl": -50.0,
                "leverage": 10,
            }
        ])
        
        await self.manager.load_initial_positions()
        
        assert len(self.manager.positions) == 2
        
        btc_pos = self.manager.get_position("BTC/USDT:USDT")
        sol_pos = self.manager.get_position("SOL/USDT:USDT")
        
        assert btc_pos.side == "long"
        assert sol_pos.side == "short"
    
    @pytest.mark.asyncio
    async def test_ignore_zero_contract_positions(self):
        """Test that zero-size positions are ignored."""
        self.exchange.fetch_positions = AsyncMock(return_value=[
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 0,  # No position
                "entryPrice": 42000.0,
                "unrealizedPnl": 0,
                "leverage": 20,
            }
        ])
        
        await self.manager.load_initial_positions()
        
        assert len(self.manager.positions) == 0


class TestPositionState:
    """Tests for position state management."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = PositionManager(self.exchange)
    
    def test_get_position_not_exists(self):
        """Test getting non-existent position."""
        pos = self.manager.get_position("BTC/USDT:USDT")
        assert pos is None
    
    def test_has_position_false(self):
        """Test has_position when no position."""
        assert self.manager.has_position("BTC/USDT:USDT") is False
    
    def test_has_position_true(self):
        """Test has_position when position exists."""
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        assert self.manager.has_position("BTC/USDT:USDT") is True
    
    def test_clear_position(self):
        """Test clearing a position."""
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        self.manager.clear_position("BTC/USDT:USDT")
        
        assert self.manager.has_position("BTC/USDT:USDT") is False
    
    def test_clear_nonexistent_position(self):
        """Test clearing non-existent position doesn't error."""
        # Should not raise
        self.manager.clear_position("BTC/USDT:USDT")
    
    def test_mark_as_managed(self):
        """Test marking position as managed (not read-only)."""
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=True,
        )
        
        self.manager.mark_as_managed("BTC/USDT:USDT")
        
        pos = self.manager.get_position("BTC/USDT:USDT")
        assert pos.is_read_only is False
    
    def test_get_all_positions(self):
        """Test getting all positions."""
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        self.manager.positions["SOL/USDT:USDT"] = PositionState(
            symbol="SOL/USDT:USDT",
            side="short",
            contracts=5.0,
            entry_price=100.0,
        )
        
        all_pos = self.manager.get_all_positions()
        
        assert len(all_pos) == 2


class TestPositionUpdate:
    """Tests for position update handling."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = PositionManager(self.exchange)
    
    def test_handle_new_position(self):
        """Test handling new position from WebSocket."""
        pos_data = {
            "symbol": "BTC/USDT:USDT",
            "side": "long",
            "contracts": 0.1,
            "entryPrice": 42000.0,
            "unrealizedPnl": 50.0,
            "leverage": 20,
        }
        
        self.manager._handle_position_update(pos_data)
        
        pos = self.manager.get_position("BTC/USDT:USDT")
        assert pos is not None
        assert pos.side == "long"
        assert pos.contracts == 0.1
        assert pos.is_read_only is False  # New positions are not read-only
    
    def test_handle_position_close(self):
        """Test handling position close (zero contracts)."""
        # Add existing position
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        # Simulate close (zero contracts)
        pos_data = {
            "symbol": "BTC/USDT:USDT",
            "side": "long",
            "contracts": 0,
            "entryPrice": 42000.0,
            "unrealizedPnl": 0,
            "leverage": 20,
        }
        
        self.manager._handle_position_update(pos_data)
        
        assert self.manager.has_position("BTC/USDT:USDT") is False
    
    def test_preserve_read_only_flag(self):
        """Test that read-only flag is preserved on updates."""
        # Add read-only position
        self.manager.positions["BTC/USDT:USDT"] = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=True,
        )
        
        # Update position (simulates PnL change)
        pos_data = {
            "symbol": "BTC/USDT:USDT",
            "side": "long",
            "contracts": 0.1,
            "entryPrice": 42000.0,
            "unrealizedPnl": 100.0,  # Changed
            "leverage": 20,
        }
        
        self.manager._handle_position_update(pos_data)
        
        pos = self.manager.get_position("BTC/USDT:USDT")
        assert pos.is_read_only is True  # Preserved


class TestPositionStateModel:
    """Tests for PositionState model."""
    
    def test_is_long(self):
        """Test is_long property."""
        pos = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        assert pos.is_long is True
        assert pos.is_short is False
    
    def test_is_short(self):
        """Test is_short property."""
        pos = PositionState(
            symbol="BTC/USDT:USDT",
            side="short",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        assert pos.is_long is False
        assert pos.is_short is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
