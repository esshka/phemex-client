# src/phemex_client/__init__.py
# Package initialization for phemex_client
# Exports all main components for external use
# RELEVANT FILES: config.py, models.py, exchange_client.py, chase_order_manager.py

"""
Phemex NATS Order Listener Package

Listens for trading signals via NATS and executes trades on Phemex Futures.
Uses limit post-only orders for entries/exits, market orders for stop-loss.
"""

from phemex_client.config import (
    Config,
    load_config,
    PhemexConfig,
    NatsConfig,
    PositionSizingConfig,
    TradingConfig,
)
from phemex_client.models import (
    SignalMessage,
    PositionState,
    OrderResult,
    ChaseOrderConfig,
    ChaseOrderState,
)
from phemex_client.exchange_client import PhemexClient
from phemex_client.position_manager import PositionManager
from phemex_client.signal_processor import SignalProcessor
from phemex_client.nats_listener import NatsListener
from phemex_client.chase_order_manager import ChaseOrderManager

__version__ = "0.1.0"

__all__ = [
    "Config",
    "load_config",
    "PhemexConfig",
    "NatsConfig",
    "PositionSizingConfig",
    "TradingConfig",
    "SignalMessage",
    "PositionState",
    "OrderResult",
    "PhemexClient",
    "PositionManager",
    "SignalProcessor",
    "NatsListener",
    "ChaseOrderManager",
    "ChaseOrderConfig",
    "ChaseOrderState",
]
