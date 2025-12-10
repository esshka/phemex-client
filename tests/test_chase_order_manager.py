# tests/test_chase_order_manager.py
# Unit tests for ChaseOrderManager class
# Tests price calculation, chase limits, and order lifecycle
# RELEVANT FILES: chase_order_manager.py, models.py, exchange_client.py

"""
Tests for ChaseOrderManager.

Uses mocked exchange client to test:
- Target price calculation for different chase modes
- Max retries limit
- Max chase distance limit
- Order fill detection
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.chase_order_manager import ChaseOrderManager
from phemex_client.models import ChaseOrderConfig, OrderResult


class TestPriceCalculation:
    """Tests for target price calculation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        self.manager = ChaseOrderManager(self.exchange)
    
    def test_bid1_mode_buy(self):
        """Test bid1 mode returns best bid for buys."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="bid1",
        )
        
        price = self.manager._calculate_target_price(
            config, bid1=100.0, ask1=100.5
        )
        
        assert price == 100.0  # bids[0]
    
    def test_ask1_mode_sell(self):
        """Test ask1 mode returns best ask for sells."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="ask1",
        )
        
        price = self.manager._calculate_target_price(
            config, bid1=100.0, ask1=100.5
        )
        
        assert price == 100.5  # asks[0]
    
    def test_distance_mode_buy(self):
        """Test distance mode for buys: bid1 - distance."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="distance",
            price_distance=0.5,
        )
        
        price = self.manager._calculate_target_price(
            config, bid1=100.0, ask1=100.5
        )
        
        assert price == 99.5  # bid1 - distance
    
    def test_distance_mode_sell(self):
        """Test distance mode for sells: ask1 + distance."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="sell",
            amount=1.0,
            chase_mode="distance",
            price_distance=0.5,
        )
        
        price = self.manager._calculate_target_price(
            config, bid1=100.0, ask1=100.5
        )
        
        assert price == 101.0  # ask1 + distance


class TestChaseLifecycle:
    """Tests for chase order lifecycle."""
    
    def setup_method(self):
        """Set up test fixtures with async mocks."""
        self.exchange = MagicMock()
        
        # Mock orderbook watching
        self.orderbook_updates = [
            {"bids": [[100.0, 10]], "asks": [[100.5, 10]]},
            {"bids": [[100.1, 10]], "asks": [[100.6, 10]]},
            {"bids": [[100.2, 10]], "asks": [[100.7, 10]]},
        ]
        self.orderbook_index = 0
        
        async def mock_watch_order_book(symbol, limit=5):
            if self.orderbook_index < len(self.orderbook_updates):
                ob = self.orderbook_updates[self.orderbook_index]
                self.orderbook_index += 1
                return ob
            # Keep returning last orderbook
            return self.orderbook_updates[-1]
        
        self.exchange.watch_order_book = mock_watch_order_book
        
        # Mock order placement
        self.order_count = 0
        async def mock_place_limit_post_only(**kwargs):
            self.order_count += 1
            return OrderResult(
                order_id=f"order-{self.order_count}",
                symbol=kwargs["symbol"],
                side=kwargs["side"],
                order_type="limit",
                amount=kwargs["amount"],
                price=kwargs["price"],
                status="open",
            )
        
        self.exchange.place_limit_post_only = mock_place_limit_post_only
        
        # Mock cancel
        async def mock_cancel_order(order_id, symbol):
            return {"id": order_id, "status": "canceled"}
        
        self.exchange.cancel_order = mock_cancel_order
        
        # Mock open orders (returns empty = order filled)
        async def mock_fetch_open_orders(symbol):
            return []  # No open orders = filled
        
        self.exchange.fetch_open_orders = mock_fetch_open_orders
        
        self.manager = ChaseOrderManager(self.exchange)
    
    @pytest.mark.asyncio
    async def test_start_chase_returns_id(self):
        """Test start_chase returns a chase ID."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            max_retries=1,  # Low limit for quick test
        )
        
        chase_id = await self.manager.start_chase(config)
        
        assert chase_id is not None
        assert len(chase_id) == 8  # UUID prefix
        
        # Wait for first iteration
        await asyncio.sleep(0.1)
        
        # Cancel to clean up
        await self.manager.cancel_chase(chase_id)
    
    @pytest.mark.asyncio
    async def test_cancel_chase_stops_order(self):
        """Test cancel_chase stops the chase and cancels order."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            max_retries=100,
        )
        
        chase_id = await self.manager.start_chase(config)
        
        # Wait for order to be placed
        await asyncio.sleep(0.2)
        
        # Cancel
        result = await self.manager.cancel_chase(chase_id)
        
        assert result is True
        
        status = self.manager.get_chase_status(chase_id)
        assert status["status"] == "canceled"
    
    @pytest.mark.asyncio
    async def test_get_chase_status(self):
        """Test get_chase_status returns correct info."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.5,
            max_retries=100,
        )
        
        chase_id = await self.manager.start_chase(config)
        await asyncio.sleep(0.2)
        
        status = self.manager.get_chase_status(chase_id)
        
        assert status["chase_id"] == chase_id
        assert status["symbol"] == "SOL/USDT:USDT"
        assert status["side"] == "buy"
        assert status["amount"] == 1.5
        
        await self.manager.cancel_chase(chase_id)


class TestChaseLimits:
    """Tests for chase limits (max retries, max distance)."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.exchange = MagicMock()
        
        # Moving prices to trigger retries
        self.prices = [100.0, 100.2, 100.4, 100.6, 100.8, 101.0]
        self.price_index = 0
        
        async def mock_watch_order_book(symbol, limit=5):
            if self.price_index < len(self.prices):
                price = self.prices[self.price_index]
                self.price_index += 1
            else:
                price = self.prices[-1]
            return {"bids": [[price, 10]], "asks": [[price + 0.5, 10]]}
        
        self.exchange.watch_order_book = mock_watch_order_book
        
        # Track order placements
        self.placed_orders = []
        async def mock_place_limit_post_only(**kwargs):
            order_id = f"order-{len(self.placed_orders) + 1}"
            self.placed_orders.append((order_id, kwargs["price"]))
            return OrderResult(
                order_id=order_id,
                symbol=kwargs["symbol"],
                side=kwargs["side"],
                order_type="limit",
                amount=kwargs["amount"],
                price=kwargs["price"],
                status="open",
            )
        
        self.exchange.place_limit_post_only = mock_place_limit_post_only
        
        async def mock_cancel_order(order_id, symbol):
            return {"id": order_id}
        
        self.exchange.cancel_order = mock_cancel_order
        
        # Orders stay open (not filled)
        self.open_order_id = None
        async def mock_fetch_open_orders(symbol):
            if self.placed_orders:
                self.open_order_id = self.placed_orders[-1][0]
                return [{"id": self.open_order_id, "filled": 0.0}]
            return []
        
        self.exchange.fetch_open_orders = mock_fetch_open_orders
        
        self.manager = ChaseOrderManager(self.exchange)
    
    @pytest.mark.asyncio
    async def test_max_retries_stops_chase(self):
        """Test chase stops at max retries."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="bid1",
            max_retries=3,  # Stop after 3 updates
        )
        
        chase_id = await self.manager.start_chase(config)
        
        # Wait for chase to complete
        for _ in range(20):
            await asyncio.sleep(0.1)
            status = self.manager.get_chase_status(chase_id)
            if status["status"] != "active":
                break
        
        status = self.manager.get_chase_status(chase_id)
        assert status["status"] == "stopped"
        assert status["retry_count"] == 3
    
    @pytest.mark.asyncio
    async def test_max_distance_stops_chase(self):
        """Test chase stops when max distance reached."""
        config = ChaseOrderConfig(
            symbol="SOL/USDT:USDT",
            side="buy",
            amount=1.0,
            chase_mode="bid1",
            max_chase_distance=0.3,  # Stop if moves $0.30
            max_retries=100,
        )
        
        chase_id = await self.manager.start_chase(config)
        
        # Wait for chase to complete
        for _ in range(20):
            await asyncio.sleep(0.1)
            status = self.manager.get_chase_status(chase_id)
            if status["status"] != "active":
                break
        
        status = self.manager.get_chase_status(chase_id)
        # Should stop due to distance (prices move $1.0 total)
        assert status["status"] == "stopped"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
