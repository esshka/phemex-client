# src/phemex_client/websocket_manager.py
# Singleton manager for WebSocket streams (prices, orders)
# Centralizes WS connections to ensure 1 connection per app
# RELEVANT FILES: exchange_client.py, chase_order_manager.py

"""
Websocket Manager for Phemex.

Singleton service that:
1. Streams bid1/ask1 prices via WebSocket
2. Streams order updates for fill detection
3. Centralizes all WS tasks to ensure efficient connection usage

Usage:
    ws_manager = WebsocketManager.get_instance(client)
    await ws_manager.start(["SOL/USDT:USDT"])
    prices = ws_manager.get_prices("SOL/USDT:USDT")
"""

import asyncio
import logging
from typing import Optional

from phemex_client.exchange_client import PhemexClient


logger = logging.getLogger(__name__)


class WebsocketManager:
    """
    Singleton manager for WebSocket streams.
    
    Centralizes price and order streams to be reused by multiple components
    (ChaseOrderManager, SignalProcessor, etc).
    """
    
    # Singleton instance
    _instance: Optional["WebsocketManager"] = None
    
    @classmethod
    def get_instance(cls, exchange_client: PhemexClient) -> "WebsocketManager":
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = cls(exchange_client)
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
    
    def __init__(self, exchange_client: PhemexClient):
        """
        Initialize websocket manager.
        
        Use get_instance() instead.
        """
        self._client = exchange_client
        
        # Price cache: symbol -> {bid1, bid2, ask1, ask2}
        self._prices: dict[str, dict] = {}
        
        # Order updates cache: order_id -> order_info
        self._order_updates: dict[str, dict] = {}
        
        # Background tasks
        self._tasks: list[asyncio.Task] = []
        
        # State
        self._running = False
        self._symbols: list[str] = []
        
        # Connection lock to prevent race conditions during start
        self._start_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        """Check if manager is running."""
        return self._running
    
    async def start(self, symbols: list[str]) -> None:
        """
        Start WebSocket streams for symbols.
        
        Args:
            symbols: List of symbols to stream
        """
        async with self._start_lock:
            if self._running:
                # If already running with same symbols, do nothing
                if set(symbols) == set(self._symbols):
                    logger.debug("WebsocketManager already running with same symbols")
                    return
                # If symbols changed, we might need to restart or add (simple: restart)
                logger.warning("WebsocketManager restarting with new symbols")
                await self.stop()
            
            self._symbols = symbols
            self._running = True
            
            logger.info(f"Starting WebsocketManager for {len(symbols)} symbols...")
            
            # Start streams for each symbol
            for symbol in symbols:
                # Orderbook stream
                self._tasks.append(
                    asyncio.create_task(self._stream_orderbook(symbol))
                )
                # Order update stream
                self._tasks.append(
                    asyncio.create_task(self._stream_orders(symbol))
                )
            
            # Wait for initial prices (warmup)
            logger.info("Waiting for initial prices...")
            for symbol in symbols:
                for _ in range(50):  # 5 second timeout
                    if symbol in self._prices:
                        break
                    await asyncio.sleep(0.1)
                else:
                    logger.warning(f"Timeout waiting for {symbol} prices")
            
            logger.info("WebsocketManager started")

    async def stop(self) -> None:
        """Stop all streams and clear tasks."""
        logger.info("Stopping WebsocketManager...")
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        self._tasks.clear()
        self._prices.clear()
        # We don't clear order updates as they might be needed for post-mortem
        logger.info("WebsocketManager stopped")

    def get_prices(self, symbol: str) -> Optional[dict]:
        """
        Get cached prices for a symbol.
        
        Returns:
            {"bid1": float, "bid2": float, "ask1": float, "ask2": float}
        """
        return self._prices.get(symbol)
    
    def get_order_update(self, order_id: str) -> Optional[dict]:
        """Get cached order update by order ID."""
        return self._order_updates.get(order_id)
        
    async def _stream_orderbook(self, symbol: str) -> None:
        """Stream orderbook updates."""
        logger.debug(f"Starting orderbook stream for {symbol}")
        try:
            while self._running:
                try:
                    orderbook = await self._client.watch_order_book(symbol)
                    
                    if orderbook["bids"] and orderbook["asks"]:
                        self._prices[symbol] = {
                            "bid1": orderbook["bids"][0][0],
                            "bid2": orderbook["bids"][1][0] if len(orderbook["bids"]) > 1 else orderbook["bids"][0][0],
                            "ask1": orderbook["asks"][0][0],
                            "ask2": orderbook["asks"][1][0] if len(orderbook["asks"]) > 1 else orderbook["asks"][0][0],
                        }
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Orderbook stream error for {symbol}: {e}")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        logger.debug(f"Orderbook stream stopped for {symbol}")

    async def _stream_orders(self, symbol: str) -> None:
        """Stream order updates."""
        logger.info(f"Starting order stream for {symbol}")
        try:
            while self._running:
                try:
                    orders = await self._client.watch_orders(symbol)
                    
                    for order in orders:
                        order_id = order.get("id")
                        if order_id:
                            status = order.get("status")
                            filled = order.get("filled", 0.0)
                            
                            self._order_updates[order_id] = {
                                "status": status,
                                "filled": filled,
                                "remaining": order.get("remaining", 0.0),
                                "average": order.get("average"),
                            }
                            
                            logger.debug(
                                f"WS order: {order_id[:8]}... "
                                f"status={status} filled={filled}"
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Order stream error for {symbol}: {e}")
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        logger.info(f"Order stream stopped for {symbol}")
