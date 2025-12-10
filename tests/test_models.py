# tests/test_models.py
# Unit tests for data models
# Tests ZmqMessage parsing and OrderResult creation
# RELEVANT FILES: models.py

"""
Tests for data models.
"""

import pytest
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.models import (
    ZmqMessage,
    PositionState,
    OrderResult,
    Action,
    Direction,
    TpLevel,
)


class TestZmqMessage:
    """Tests for ZmqMessage parsing."""
    
    def test_parse_entry_message(self):
        """Test parsing ENTRY message."""
        data = {
            "action": "ENTRY",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.50,
            "timestamp": "2024-01-15T10:30:00Z",
            "confidence": 0.85,
            "stop_loss": 43000.00,
            "take_profit": 44500.00,
            "position_size_r": 50.0,
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.action == Action.ENTRY
        assert msg.direction == Direction.LONG
        assert msg.symbol == "BTC/USDT:USDT"
        assert msg.price == 43500.50
        assert msg.confidence == 0.85
        assert msg.stop_loss == 43000.00
        assert msg.take_profit == 44500.00
        assert msg.position_size_r == 50.0
    
    def test_parse_exit_message(self):
        """Test parsing EXIT message."""
        data = {
            "action": "EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 44200.00,
            "timestamp": "2024-01-15T12:00:00Z",
            "reason": "Signal",
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.action == Action.EXIT
        assert msg.reason == "Signal"
    
    def test_parse_partial_exit_message(self):
        """Test parsing PARTIAL_EXIT message."""
        data = {
            "action": "PARTIAL_EXIT",
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 44000.00,
            "timestamp": "2024-01-15T11:30:00Z",
            "tp_level": 1,
            "exit_pct": 0.33,
            "remaining_pct": 0.67,
            "move_sl_to_be": True,
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.action == Action.PARTIAL_EXIT
        assert msg.tp_level == 1
        assert msg.exit_pct == 0.33
        assert msg.remaining_pct == 0.67
        assert msg.move_sl_to_be is True
    
    def test_parse_defaults(self):
        """Test default values for optional fields."""
        data = {
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.00,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.action == Action.ENTRY  # Default
        assert msg.position_size_r == 1.0  # Default
        assert msg.stop_loss is None
        assert msg.take_profit is None
        assert msg.move_sl_to_be is False
    
    def test_parse_short_direction(self):
        """Test parsing SHORT direction."""
        data = {
            "direction": "SHORT",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.00,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.direction == Direction.SHORT
    
    def test_parse_tp_levels(self):
        """Test parsing TP levels array."""
        data = {
            "direction": "LONG",
            "symbol": "BTC/USDT:USDT",
            "price": 43500.00,
            "timestamp": "2024-01-15T10:30:00Z",
            "multi_tp_enabled": True,
            "tp_levels": [
                {"price": 44000.0, "exit_pct": 0.33, "ratio": 1.0},
                {"price": 45000.0, "exit_pct": 0.50, "ratio": 2.0},
            ],
        }
        
        msg = ZmqMessage.from_dict(data)
        
        assert msg.multi_tp_enabled is True
        assert len(msg.tp_levels) == 2
        assert msg.tp_levels[0].price == 44000.0
        assert msg.tp_levels[1].exit_pct == 0.50


class TestOrderResult:
    """Tests for OrderResult model."""
    
    def test_from_ccxt_order(self):
        """Test creating OrderResult from CCXT order response."""
        ccxt_order = {
            "id": "order-123",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "type": "limit",
            "amount": 0.1,
            "price": 43000.0,
            "status": "open",
            "filled": 0.0,
            "remaining": 0.1,
            "average": None,
        }
        
        result = OrderResult.from_ccxt_order(ccxt_order)
        
        assert result.order_id == "order-123"
        assert result.symbol == "BTC/USDT:USDT"
        assert result.side == "buy"
        assert result.order_type == "limit"
        assert result.amount == 0.1
        assert result.price == 43000.0
        assert result.status == "open"
        assert result.filled == 0.0
        assert result.remaining == 0.1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
