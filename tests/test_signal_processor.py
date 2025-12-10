# tests/test_signal_processor.py
# Unit tests for SignalProcessor class
# Tests signal validation, position sizing, and signal handling
# RELEVANT FILES: signal_processor.py, models.py, exchange_client.py

"""
Tests for SignalProcessor.

Uses mocked exchange client and position manager.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.signal_processor import SignalProcessor
from phemex_client.models import PositionState, OrderResult


class TestPositionSizing:
    """Tests for position size calculation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.position_manager = MagicMock()
        
        self.chase_manager = MagicMock()
        self.processor = SignalProcessor(
            exchange_client=self.exchange,
            position_manager=self.position_manager,
            chase_manager=self.chase_manager,
            deposit_size=1000.0,
            r_percentage=0.01,  # 1% = 10 USDT per R
            leverage=20,
        )
    
    def test_basic_position_size(self):
        """Test basic position size calculation."""
        # 10 R = 100 USDT notional
        # At price 100, that's 1.0 contracts
        size = self.processor.calculate_position_size(
            entry_price=100.0,
            stop_loss=99.0,
            position_size_r=10.0,
        )
        
        assert size == 1.0
    
    def test_position_size_different_price(self):
        """Test position size at different price levels."""
        # 10 R = 100 USDT notional
        # At price 50, that's 2.0 contracts
        size = self.processor.calculate_position_size(
            entry_price=50.0,
            stop_loss=49.0,
            position_size_r=10.0,
        )
        
        assert size == 2.0
    
    def test_position_size_capped(self):
        """Test position size is capped at 95% of buying power."""
        # 2000 R = 20000 USDT notional (way over limit)
        # Max is 1000 * 20 * 0.95 = 19000 USDT
        # At price 100, max is 190 contracts
        size = self.processor.calculate_position_size(
            entry_price=100.0,
            stop_loss=99.0,
            position_size_r=2000.0,  # High enough to hit the cap
        )
        
        assert size == 190.0
    
    def test_position_size_small_r(self):
        """Test small R values."""
        # 1 R = 10 USDT notional
        # At price 100, that's 0.1 contracts
        size = self.processor.calculate_position_size(
            entry_price=100.0,
            stop_loss=99.0,
            position_size_r=1.0,
        )
        
        assert size == 0.1


class TestEntrySignal:
    """Tests for ENTRY signal handling."""
    
    def setup_method(self):
        """Set up test fixtures with async mocks."""
        self.exchange = MagicMock()
        self.exchange.place_limit_post_only = AsyncMock(
            return_value=OrderResult(
                order_id="test-123",
                symbol="BTC/USDT:USDT",
                side="buy",
                order_type="limit",
                amount=0.1,
                price=43000.0,
            )
        )
        self.exchange.place_stop_loss_market = AsyncMock(
            return_value=OrderResult(
                order_id="sl-456",
                symbol="BTC/USDT:USDT",
                side="sell",
                order_type="market",
                amount=0.1,
            )
        )
        self.exchange.cancel_all_orders = AsyncMock(return_value=[])
        
        self.position_manager = MagicMock()
        self.position_manager.get_position = MagicMock(return_value=None)
        
        self.chase_manager = MagicMock()
        self.chase_manager.submit_chase_and_wait = AsyncMock(
            return_value=MagicMock(status="filled", total_filled=0.1, fill_price=43000.0)
        )
        
        self.processor = SignalProcessor(
            exchange_client=self.exchange,
            position_manager=self.position_manager,
            chase_manager=self.chase_manager,
            deposit_size=1000.0,
            r_percentage=0.01,
            leverage=20,
        )
    
    @pytest.mark.asyncio
    async def test_entry_long_no_position(self):
        """Test ENTRY LONG with no existing position."""
        message = {
            "action": "ENTRY",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43000.0,
            "stop_loss": 42500.0,
            "position_size_r": 500.0,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify entry order placed
        # Verify entry order placed
        self.chase_manager.submit_chase_and_wait.assert_called_once()
        config = self.chase_manager.submit_chase_and_wait.call_args[0][0]
        
        assert config.symbol == "BTC/USDT:USDT"
        assert config.side == "buy"
        
        # Verify stop-loss placed
        self.exchange.place_stop_loss_market.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_entry_short_no_position(self):
        """Test ENTRY SHORT with no existing position."""
        message = {
            "action": "ENTRY",
            "direction": "SHORT",
            "symbol": "BTC/USDT:USDT",
            "price": 43000.0,
            "stop_loss": 43500.0,
            "position_size_r": 500.0,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify entry order placed with sell side
        # Verify entry order placed with sell side
        self.chase_manager.submit_chase_and_wait.assert_called_once()
        config = self.chase_manager.submit_chase_and_wait.call_args[0][0]
        assert config.side == "sell"
        
        # Verify stop-loss side is buy (to close short)
        sl_kwargs = self.exchange.place_stop_loss_market.call_args.kwargs
        assert sl_kwargs["side"] == "buy"
    
    @pytest.mark.asyncio
    async def test_entry_ignored_same_direction(self):
        """Test ENTRY ignored when position exists in same direction."""
        # Mock existing long position
        self.position_manager.get_position.return_value = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
        )
        
        message = {
            "action": "ENTRY",
            "direction": "LONG",  # Same as existing
            "symbol": "BTC/USDT:USDT",
            "price": 43000.0,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify no orders placed
        self.chase_manager.submit_chase_and_wait.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_entry_closes_opposite_position(self):
        """Test ENTRY closes opposite position first."""
        # Mock existing short position
        self.position_manager.get_position.return_value = PositionState(
            symbol="BTC/USDT:USDT",
            side="short",
            contracts=0.1,
            entry_price=44000.0,
            is_read_only=False,
        )
        
        message = {
            "action": "ENTRY",
            "direction": "LONG",  # Opposite of existing
            "symbol": "BTC/USDT:USDT",
            "price": 43000.0,
            "position_size_r": 500.0,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify cancel and close orders called
        self.exchange.cancel_all_orders.assert_called()
        
        # Should have 2 calls: close + new entry
        assert self.chase_manager.submit_chase_and_wait.call_count == 2


class TestExitSignal:
    """Tests for EXIT signal handling."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.exchange.place_limit_post_only = AsyncMock(
            return_value=OrderResult(
                order_id="close-123",
                symbol="BTC/USDT:USDT",
                side="sell",
                order_type="limit",
                amount=0.1,
            )
        )
        self.exchange.cancel_all_orders = AsyncMock(return_value=[])
        
        self.position_manager = MagicMock()
        self.position_manager.clear_position = MagicMock()
        
        self.chase_manager = MagicMock()
        self.chase_manager.submit_chase_and_wait = AsyncMock(
            return_value=MagicMock(status="filled", total_filled=0.1, fill_price=44000.0)
        )
        
        self.processor = SignalProcessor(
            exchange_client=self.exchange,
            position_manager=self.position_manager,
            chase_manager=self.chase_manager,
        )
    
    @pytest.mark.asyncio
    async def test_exit_closes_position(self):
        """Test EXIT closes existing position."""
        self.position_manager.get_position.return_value = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=False,
        )
        
        message = {
            "action": "EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 44000.0,
            "reason": "Signal",
            "timestamp": "2024-01-15T12:00:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify orders cancelled and close placed
        # Verify orders cancelled and close placed
        self.exchange.cancel_all_orders.assert_called()
        self.chase_manager.submit_chase_and_wait.assert_called_once()
        
        config = self.chase_manager.submit_chase_and_wait.call_args[0][0]
        assert config.side == "sell"
        assert config.reduce_only is True
    
    @pytest.mark.asyncio
    async def test_exit_no_position(self):
        """Test EXIT with no position does nothing."""
        self.position_manager.get_position.return_value = None
        
        message = {
            "action": "EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 44000.0,
            "timestamp": "2024-01-15T12:00:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify no orders placed
        self.chase_manager.submit_chase_and_wait.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_exit_read_only_blocked(self):
        """Test EXIT blocked for read-only positions."""
        self.position_manager.get_position.return_value = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=True,  # Protected
        )
        
        message = {
            "action": "EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 44000.0,
            "timestamp": "2024-01-15T12:00:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify no orders placed
        self.chase_manager.submit_chase_and_wait.assert_not_called()


class TestPartialExitSignal:
    """Tests for PARTIAL_EXIT signal handling."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.exchange.place_limit_post_only = AsyncMock(
            return_value=OrderResult(
                order_id="partial-123",
                symbol="BTC/USDT:USDT",
                side="sell",
                order_type="limit",
                amount=0.033,
            )
        )
        self.exchange.place_stop_loss_market = AsyncMock(
            return_value=OrderResult(
                order_id="sl-new",
                symbol="BTC/USDT:USDT",
                side="sell",
                order_type="market",
                amount=0.067,
            )
        )
        self.exchange.cancel_all_orders = AsyncMock(return_value=[])
        
        self.position_manager = MagicMock()
        
        self.chase_manager = MagicMock()
        self.chase_manager.submit_chase_and_wait = AsyncMock(
            return_value=MagicMock(status="filled", total_filled=0.033, fill_price=43500.0)
        )
        
        self.processor = SignalProcessor(
            exchange_client=self.exchange,
            position_manager=self.position_manager,
            chase_manager=self.chase_manager,
        )
    
    @pytest.mark.asyncio
    async def test_partial_exit_closes_fraction(self):
        """Test PARTIAL_EXIT closes correct fraction."""
        self.position_manager.get_position.return_value = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=False,
        )
        
        message = {
            "action": "PARTIAL_EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.0,
            "exit_pct": 0.33,
            "tp_level": 1,
            "timestamp": "2024-01-15T11:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify partial close order placed
        # Verify partial close order placed
        self.chase_manager.submit_chase_and_wait.assert_called_once()
        
        config = self.chase_manager.submit_chase_and_wait.call_args[0][0]
        assert config.reduce_only is True
        # 0.1 * 0.33 = 0.033
        assert abs(config.amount - 0.033) < 0.001
    
    @pytest.mark.asyncio
    async def test_partial_exit_moves_sl_to_be(self):
        """Test PARTIAL_EXIT moves SL to break-even when requested."""
        position = PositionState(
            symbol="BTC/USDT:USDT",
            side="long",
            contracts=0.1,
            entry_price=42000.0,
            is_read_only=False,
        )
        self.position_manager.get_position.return_value = position
        
        message = {
            "action": "PARTIAL_EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.0,
            "exit_pct": 0.33,
            "move_sl_to_be": True,
            "timestamp": "2024-01-15T11:30:00Z",
        }
        
        await self.processor.process_signal(message)
        
        # Verify new SL placed at entry price
        self.exchange.place_stop_loss_market.assert_called()
        sl_kwargs = self.exchange.place_stop_loss_market.call_args.kwargs
        assert sl_kwargs["trigger_price"] == 42000.0  # Entry price


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
