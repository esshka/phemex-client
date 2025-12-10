# src/phemex_client/models.py
# Data models for ZMQ messages, positions, orders, and chase orders
# Provides type-safe dataclasses for all core data structures
# RELEVANT FILES: config.py, signal_processor.py, chase_order_manager.py

"""
Data models for Phemex ZMQ Order Listener.

Defines:
- ZmqMessage: Parsed ZMQ trading signal
- PositionState: Real-time position tracking
- OrderResult: Order execution result
- ChaseOrderConfig: Chase order parameters
- ChaseOrderState: Active chase order tracking
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


@dataclass
class ChaseOrderConfig:
    """
    Chase limit order configuration.
    
    Defines parameters for a chase order that follows market price.
    """
    symbol: str                      # Trading symbol (e.g., 'SOL/USDT:USDT')
    side: str                        # 'buy' or 'sell'
    amount: float                    # Order size in contracts
    
    # Chase mode: where to place the order relative to orderbook
    # 'bid1' = at best bid (top of book)
    # 'bid2' = at second best bid (one tick back, default for buys)
    # 'ask1' = at best ask (top of book)
    # 'ask2' = at second best ask (one tick back, default for sells)
    # 'distance' = at fixed distance from bid1 (buys) or ask1 (sells)
    # Default (empty): bid2 for buys, ask2 for sells
    chase_mode: str = ""
    
    # Distance from bid1/ask1 in price units (only used in 'distance' mode)
    # Positive = further from mid (e.g., bid1 - distance for buys)
    price_distance: float = 0.0
    
    # Max price move from initial price before stopping chase (0 = no limit)
    # If price moves beyond this, order becomes regular limit at current price
    max_chase_distance: float = 0.0
    
    # Max order updates before stopping chase
    max_retries: int = 100
    
    # If True, order can only reduce position (closing orders)
    reduce_only: bool = False
    
    # Position side for hedge mode accounts: 'long', 'short', or None
    # - Set to 'long' when opening/managing a long position
    # - Set to 'short' when opening/managing a short position
    # - Leave as None for one-way mode accounts
    position_side: Optional[str] = None


@dataclass
class ChaseOrderState:
    """
    Tracks state of an active chase order.
    
    Used internally by ChaseOrderManager to track progress.
    """
    chase_id: str                    # Unique ID for this chase
    config: ChaseOrderConfig         # Original config
    current_order_id: Optional[str]  # Current limit order ID (may change)
    initial_price: float             # Price when chase started
    current_price: float             # Current order price
    retry_count: int = 0             # Number of order updates so far
    status: str = "active"           # 'active', 'filled', 'stopped', 'canceled'
    fill_price: Optional[float] = None  # Final fill price if filled
    fill_amount: float = 0.0         # Amount filled so far

