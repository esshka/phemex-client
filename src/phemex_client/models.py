# src/phemex_client/models.py
# Data models for ZMQ messages, positions, and order results
# Provides type-safe dataclasses for all core data structures
# RELEVANT FILES: config.py, signal_processor.py, position_manager.py

"""
Data models for Phemex ZMQ Order Listener.

Defines:
- ZmqMessage: Parsed ZMQ trading signal
- PositionState: Real-time position tracking
- OrderResult: Order execution result
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class Action(Enum):
    """Trading signal action types."""
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    PARTIAL_EXIT = "PARTIAL_EXIT"


class Direction(Enum):
    """Trade direction."""
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class TpLevel:
    """Take profit level definition."""
    price: float
    exit_pct: float
    ratio: float = 0.0


@dataclass
class ZmqMessage:
    """
    Parsed ZMQ trading signal message.
    
    Contains all fields from the ZMQ message protocol.
    """
    # Required fields
    direction: Direction
    symbol: str
    price: float
    timestamp: datetime
    
    # Action type (defaults to ENTRY)
    action: Action = Action.ENTRY
    
    # Entry-specific fields
    confidence: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    position_size_r: float = 1.0
    multi_tp_enabled: bool = False
    tp_levels: list[TpLevel] = field(default_factory=list)
    
    # Exit-specific fields
    reason: str = ""
    
    # Partial exit fields
    tp_level: int = 0
    exit_pct: float = 0.0
    remaining_pct: float = 1.0
    move_sl_to_be: bool = False
    
    @classmethod
    def from_dict(cls, data: dict) -> "ZmqMessage":
        """
        Create ZmqMessage from dictionary (parsed JSON).
        
        Handles type conversion and defaults.
        """
        # Parse direction
        direction_str = data.get("direction", "LONG")
        direction = Direction[direction_str.upper()]
        
        # Parse action
        action_str = data.get("action", "ENTRY")
        action = Action[action_str.upper()]
        
        # Parse timestamp
        ts_str = data.get("timestamp", "")
        if ts_str:
            # Handle ISO 8601 format
            ts_str = ts_str.replace("Z", "+00:00")
            timestamp = datetime.fromisoformat(ts_str)
        else:
            timestamp = datetime.now(timezone.utc)
        
        # Parse TP levels if present
        tp_levels = []
        for tp_data in data.get("tp_levels", []):
            tp_levels.append(TpLevel(
                price=tp_data.get("price", 0.0),
                exit_pct=tp_data.get("exit_pct", 0.0),
                ratio=tp_data.get("ratio", 0.0),
            ))
        
        return cls(
            direction=direction,
            symbol=data.get("symbol", ""),
            price=float(data.get("price", 0.0)),
            timestamp=timestamp,
            action=action,
            confidence=float(data.get("confidence", 0.0)),
            stop_loss=data.get("stop_loss"),
            take_profit=data.get("take_profit"),
            position_size_r=float(data.get("position_size_r", 1.0)),
            multi_tp_enabled=data.get("multi_tp_enabled", False),
            tp_levels=tp_levels,
            reason=data.get("reason", ""),
            tp_level=int(data.get("tp_level", 0)),
            exit_pct=float(data.get("exit_pct", 0.0)),
            remaining_pct=float(data.get("remaining_pct", 1.0)),
            move_sl_to_be=data.get("move_sl_to_be", False),
        )


@dataclass
class PositionState:
    """
    Real-time position state.
    
    Tracks current position for a symbol with read-only protection.
    """
    symbol: str
    side: str                   # 'long' or 'short'
    contracts: float            # Current size
    entry_price: float          # Average entry price
    unrealized_pnl: float = 0.0
    leverage: int = 1
    is_read_only: bool = False  # Preloaded positions are protected
    
    @property
    def is_long(self) -> bool:
        """Check if position is long."""
        return self.side.lower() == "long"
    
    @property
    def is_short(self) -> bool:
        """Check if position is short."""
        return self.side.lower() == "short"


@dataclass
class OrderResult:
    """
    Order execution result.
    
    Contains order ID and execution details.
    """
    order_id: str
    symbol: str
    side: str                   # 'buy' or 'sell'
    order_type: str             # 'limit' or 'market'
    amount: float
    price: Optional[float] = None
    status: str = "pending"     # pending, open, filled, canceled
    filled: float = 0.0
    remaining: float = 0.0
    average: Optional[float] = None
    timestamp: Optional[datetime] = None
    
    @classmethod
    def from_ccxt_order(cls, order: dict) -> "OrderResult":
        """Create OrderResult from CCXT order response."""
        return cls(
            order_id=order.get("id", ""),
            symbol=order.get("symbol", ""),
            side=order.get("side", ""),
            order_type=order.get("type", ""),
            amount=order.get("amount", 0.0),
            price=order.get("price"),
            status=order.get("status", "pending"),
            filled=order.get("filled", 0.0),
            remaining=order.get("remaining", 0.0),
            average=order.get("average"),
            timestamp=datetime.now(timezone.utc),
        )
