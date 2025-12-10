# tests/test_chase_order_manager.py
# Unit tests for ChaseOrderManager singleton
# Tests price calculation, singleton pattern, and basic operations
# RELEVANT FILES: chase_order_manager.py, models.py, exchange_client.py

"""
Tests for ChaseOrderManager singleton.

Focuses on unit tests that don't require full async lifecycle.
"""

import asyncio
import pytest
from unittest.mock import MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.chase_order_manager import ChaseOrderManager
from phemex_client.models import ChaseOrderConfig, ChaseOrderState, OrderResult


class TestSingletonPattern:
    """Tests for singleton pattern."""
    
    def teardown_method(self):
        """Reset singleton after each test."""
        ChaseOrderManager.reset_instance()
    
    def test_get_instance_returns_same_object(self):
        """Test get_instance returns same instance."""
        exchange = MagicMock()
        
        instance1 = ChaseOrderManager.get_instance(exchange)
        instance2 = ChaseOrderManager.get_instance(exchange)
        
        assert instance1 is instance2
    
    def test_reset_instance_clears_singleton(self):
        """Test reset_instance allows new instance."""
        exchange1 = MagicMock()
        exchange2 = MagicMock()
        
        instance1 = ChaseOrderManager.get_instance(exchange1)
        ChaseOrderManager.reset_instance()
        instance2 = ChaseOrderManager.get_instance(exchange2)
        
        assert instance1 is not instance2


class TestPriceCalculation:
    """Tests for target price calculation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = ChaseOrderManager(self.exchange)
        # Standard prices dict for tests
        self.prices = {
            "bid1": 100.0,
            "bid2": 99.9,
            "ask1": 100.5,
            "ask2": 100.6,
        }
    
    def teardown_method(self):
        ChaseOrderManager.reset_instance()
    
    def test_bid1_mode(self):
        """Test bid1 mode returns best bid."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="bid1",
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 100.0
    
    def test_bid2_mode(self):
        """Test bid2 mode returns second best bid."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="bid2",
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 99.9
    
    def test_ask1_mode(self):
        """Test ask1 mode returns best ask."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="ask1",
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 100.5
    
    def test_ask2_mode(self):
        """Test ask2 mode returns second best ask."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="ask2",
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 100.6
    
    def test_default_mode_buy(self):
        """Test default mode for buys uses bid2."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            # No chase_mode = default
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 99.9  # bid2
    
    def test_default_mode_sell(self):
        """Test default mode for sells uses ask2."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            # No chase_mode = default
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 100.6  # ask2
    
    def test_distance_mode_buy(self):
        """Test distance mode for buys: bid1 - distance."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="distance",
            price_distance=0.5,
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 99.5  # 100.0 - 0.5
    
    def test_distance_mode_sell(self):
        """Test distance mode for sells: ask1 + distance."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="distance",
            price_distance=0.5,
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        assert price == 101.0  # 100.5 + 0.5
    
    def test_spread_protection_buy_never_crosses(self):
        """Test buy orders are capped at bid1, never cross to ask1."""
        # Even with ask1 mode, buy order should be capped at bid1
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="ask1",  # Trying to place at ask1 as a buy
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        # Should be capped at bid1, not ask1
        assert price == 100.0
    
    def test_spread_protection_sell_never_crosses(self):
        """Test sell orders are floored at ask1, never cross to bid1."""
        # Even with bid1 mode, sell order should be floored at ask1
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="bid1",  # Trying to place at bid1 as a sell
        )
        
        price = self.manager._calculate_target_price(config, self.prices)
        # Should be floored at ask1, not bid1
        assert price == 100.5


class TestChaseState:
    """Tests for chase state management."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = ChaseOrderManager(self.exchange)
    
    def teardown_method(self):
        ChaseOrderManager.reset_instance()
    
    def test_needs_update_no_order(self):
        """Test needs_update returns True when no order exists."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
            ),
            current_order_id=None,
            initial_price=100.0,
            current_price=0.0,
        )
        
        assert self.manager._needs_update(state, 100.0) is True
    
    def test_needs_update_price_changed(self):
        """Test needs_update returns True when price changed significantly."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.0,
        )
        
        # 0.1% change should trigger update (> MIN_PRICE_CHANGE_PCT)
        assert self.manager._needs_update(state, 100.1) is True
    
    def test_needs_update_price_same(self):
        """Test needs_update returns False when price unchanged."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.0,
        )
        
        # Same price should not trigger update
        assert self.manager._needs_update(state, 100.0) is False
    
    def test_check_limits_max_retries(self):
        """Test check_limits returns True at max retries."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
                max_retries=5,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.0,
            retry_count=5,
        )
        self.manager._active_chases["test"] = state
        
        result = self.manager._check_limits("test", 100.0)
        
        assert result is True
        assert state.status == "stopped"
    
    def test_check_limits_max_distance(self):
        """Test check_limits returns True when max distance reached."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
                max_chase_distance=1.0,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.0,
        )
        self.manager._active_chases["test"] = state
        
        # Price moved $1.5 from initial
        result = self.manager._check_limits("test", 101.5)
        
        assert result is True
        assert state.status == "stopped"
    
    def test_check_limits_within_limits(self):
        """Test check_limits returns False when within limits."""
        state = ChaseOrderState(
            chase_id="test",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.0,
                max_chase_distance=2.0,
                max_retries=10,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.0,
            retry_count=3,
        )
        self.manager._active_chases["test"] = state
        
        result = self.manager._check_limits("test", 100.5)
        
        assert result is False
        assert state.status == "active"


class TestGetStatus:
    """Tests for status retrieval."""
    
    def setup_method(self):
        self.exchange = MagicMock()
        self.manager = ChaseOrderManager(self.exchange)
    
    def teardown_method(self):
        ChaseOrderManager.reset_instance()
    
    def test_get_chase_status_returns_info(self):
        """Test get_chase_status returns correct data."""
        state = ChaseOrderState(
            chase_id="abc123",
            config=ChaseOrderConfig(
                symbol="SOL/USDT:USDT",
                side="buy",
                amount=1.5,
            ),
            current_order_id="order-1",
            initial_price=100.0,
            current_price=100.2,
            retry_count=3,
        )
        self.manager._active_chases["abc123"] = state
        
        status = self.manager.get_chase_status("abc123")
        
        assert status["chase_id"] == "abc123"
        assert status["symbol"] == "SOL/USDT:USDT"
        assert status["side"] == "buy"
        assert status["amount"] == 1.5
        assert status["status"] == "active"
        assert status["retry_count"] == 3
    
    def test_get_chase_status_not_found(self):
        """Test get_chase_status returns None for unknown ID."""
        status = self.manager.get_chase_status("unknown")
        assert status is None
    
    def test_get_prices_returns_cached(self):
        """Test get_prices returns cached prices."""
        self.manager._prices["SOL/USDT:USDT"] = {
            "bid1": 100.0,
            "ask1": 100.5,
        }
        
        prices = self.manager.get_prices("SOL/USDT:USDT")
        
        assert prices["bid1"] == 100.0
        assert prices["ask1"] == 100.5
    
    def test_get_prices_not_found(self):
        """Test get_prices returns None for unknown symbol."""
        prices = self.manager.get_prices("UNKNOWN")
        assert prices is None


class TestSubmitWithoutWarmup:
    """Test submit_chase fails without warmup."""
    
    def teardown_method(self):
        ChaseOrderManager.reset_instance()
    
    @pytest.mark.asyncio
    async def test_submit_chase_requires_warmup(self):
        """Test submit_chase fails without warmup."""
        exchange = MagicMock()
        manager = ChaseOrderManager(exchange)
        
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
        )
        
        with pytest.raises(RuntimeError, match="not warmed up"):
            await manager.submit_chase(config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
