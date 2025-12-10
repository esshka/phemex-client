# src/phemex_client/__init__.py
# Package initialization for phemex_client
# Exports all main components for external use
# RELEVANT FILES: config.py, models.py, exchange_client.py, chase_order_manager.py

"""
Phemex ZMQ Order Listener Package

Listens for trading signals via ZeroMQ and executes trades on Phemex Futures.
Uses limit post-only orders for entries/exits, market orders for stop-loss.
"""

from phemex_client.config import (
    Config,
    load_config,
    PhemexConfig,
    ZmqConfig,
    PositionSizingConfig,
    TradingConfig,
)
from phemex_client.models import (
    ZmqMessage,
    PositionState,
    OrderResult,
    ChaseOrderConfig,
    ChaseOrderState,
)
from phemex_client.exchange_client import PhemexClient
from phemex_client.position_manager import PositionManager
from phemex_client.signal_processor import SignalProcessor
from phemex_client.zmq_listener import ZmqListener
from phemex_client.chase_order_manager import ChaseOrderManager

__version__ = "0.1.0"

__all__ = [
    "Config",
    "load_config",
    "PhemexConfig",
    "ZmqConfig",
    "PositionSizingConfig",
    "TradingConfig",
    "ZmqMessage",
    "PositionState",
    "OrderResult",
    "PhemexClient",
    "PositionManager",
    "SignalProcessor",
    "ZmqListener",
    "ChaseOrderManager",
    "ChaseOrderConfig",
    "ChaseOrderState",
]
