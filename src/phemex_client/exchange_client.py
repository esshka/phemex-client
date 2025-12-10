# src/phemex_client/exchange_client.py
# CCXT Pro wrapper for Phemex Futures exchange operations
# Handles connection, authentication, leverage, and order execution
# RELEVANT FILES: config.py, models.py, position_manager.py, signal_processor.py

"""
Exchange client for Phemex Futures using CCXT Pro.

Provides:
- Exchange initialization and authentication
- Leverage and margin mode configuration
- Post-only limit order placement
- Market stop-loss order placement
- Order cancellation
"""

import asyncio
import logging
import math
from typing import Any, Optional

import ccxt.pro as ccxt

from phemex_client.models import OrderResult


logger = logging.getLogger(__name__)


def truncate_to_step_size(amount: float, step_size: float = 0.01) -> float:
    """
    Truncate amount to the nearest step size (floor).
    
    Phemex requires order sizes to be multiples of the minimum step.
    For SOLUSDT, step size is 0.01.
    
    Args:
        amount: Order amount
        step_size: Minimum step size (default 0.01)
    
    Returns:
        Amount truncated to step size
    """
    return math.floor(amount / step_size) * step_size


class PhemexClient:
    """
    CCXT Pro wrapper for Phemex Futures.
    
    Handles connection, authentication, and order execution.
    Uses post-only limit orders for entries/exits, market for stop-loss.
    """
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    
    def __init__(
        self,
        api_key: str,
        secret: str,
        sandbox: bool = False,
    ):
        """
        Initialize Phemex exchange client.
        
        Args:
            api_key: Phemex API key
            secret: Phemex API secret
            sandbox: If True, use testnet
        """
        self.exchange = ccxt.phemex({
            "apiKey": api_key,
            "secret": secret,
            "sandbox": sandbox,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",       # Perpetual futures
                "defaultSubType": "linear",  # USDT-margined
            }
        })
        
        self._initialized = False
        self._symbols: list[str] = []
    
    async def initialize(
        self,
        symbols: list[str],
        leverage: int = 20,
    ) -> None:
        """
        Load markets and configure leverage for trading symbols.
        
        Args:
            symbols: List of trading symbols (e.g., ['BTC/USDT:USDT'])
            leverage: Leverage multiplier to set
        """
        await self.exchange.load_markets()
        
        for symbol in symbols:
            try:
                # Set isolated margin mode
                await self.exchange.set_margin_mode("isolated", symbol)
                logger.info(f"Set isolated margin for {symbol}")
            except Exception as e:
                # May fail if already set, that's okay
                logger.debug(f"Margin mode info for {symbol}: {e}")
            
            try:
                # Set leverage for both position sides (required for Hedge mode)
                # In Hedge mode, Phemex requires setting leverage per side
                for pos_side in ["Long", "Short"]:
                    try:
                        await self.exchange.set_leverage(
                            leverage, symbol, params={"posSide": pos_side}
                        )
                    except Exception:
                        pass  # May fail if already set
                logger.info(f"Set leverage {leverage}x for {symbol}")
            except Exception as e:
                logger.warning(f"Leverage setting for {symbol}: {e}")
        
        self._symbols = symbols
        self._initialized = True
        logger.info(f"Exchange initialized for {len(symbols)} symbols")
    
    async def place_limit_post_only(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> OrderResult:
        """
        Place a limit post-only order.
        
        Post-only ensures the order is a maker order (never taker).
        If it would immediately match, it gets rejected.
        
        Args:
            symbol: Trading symbol
            side: 'buy' or 'sell'
            amount: Order size in contracts
            price: Limit price
            reduce_only: If True, order can only reduce position
            position_side: For hedge mode: 'long' or 'short'. None for one-way mode.
        
        Returns:
            OrderResult with order details
        """
        params = {
            "postOnly": True,
            "reduceOnly": reduce_only,
            "timeInForce": "PostOnly",  # Ensure maker-only, reject if would cross
        }
        
        # Add position side for hedge mode
        if position_side:
            params["posSide"] = position_side.capitalize()  # 'Long' or 'Short'
        
        # Truncate amount to minimum step size (0.01 for SOL)
        truncated_amount = truncate_to_step_size(amount)
        
        order = await self._create_order_with_retry(
            symbol=symbol,
            order_type="limit",
            side=side,
            amount=truncated_amount,
            price=price,
            params=params,
        )
        
        return OrderResult.from_ccxt_order(order)
    
    async def place_stop_loss_market(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        trigger_type: str = "mark",
    ) -> OrderResult:
        """
        Place a stop-loss market order (conditional).
        
        Uses market order type to guarantee fill when triggered.
        
        Args:
            symbol: Trading symbol
            side: 'buy' or 'sell' (opposite of position)
            amount: Order size in contracts
            trigger_price: Price that triggers the stop
            trigger_type: 'mark' or 'last' price trigger
        
        Returns:
            OrderResult with order details
        """
        params = {
            "stopLossPrice": trigger_price,
            "triggerPrice": trigger_price,
            "triggerType": trigger_type,
            "reduceOnly": True,
        }
        
        # Truncate amount to minimum step size
        truncated_amount = truncate_to_step_size(amount)
        
        order = await self._create_order_with_retry(
            symbol=symbol,
            order_type="market",
            side=side,
            amount=truncated_amount,
            price=None,
            params=params,
        )
        
        return OrderResult.from_ccxt_order(order)
    
    async def place_take_profit_limit(
        self,
        symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        limit_price: Optional[float] = None,
    ) -> OrderResult:
        """
        Place a take-profit limit order (conditional).
        
        Args:
            symbol: Trading symbol
            side: 'buy' or 'sell' (opposite of position)
            amount: Order size in contracts
            trigger_price: Price that triggers the TP
            limit_price: Limit price (defaults to trigger_price)
        
        Returns:
            OrderResult with order details
        """
        if limit_price is None:
            limit_price = trigger_price
        
        params = {
            "takeProfitPrice": trigger_price,
            "triggerPrice": trigger_price,
            "reduceOnly": True,
        }
        
        # Truncate amount to minimum step size
        truncated_amount = truncate_to_step_size(amount)
        
        order = await self._create_order_with_retry(
            symbol=symbol,
            order_type="limit",
            side=side,
            amount=truncated_amount,
            price=limit_price,
            params=params,
        )
        
        return OrderResult.from_ccxt_order(order)
    
    async def cancel_all_orders(self, symbol: str) -> list[dict]:
        """
        Cancel all open orders for a symbol.
        
        Args:
            symbol: Trading symbol
        
        Returns:
            List of cancelled order responses
        """
        try:
            result = await self.exchange.cancel_all_orders(symbol)
            logger.info(f"Cancelled all orders for {symbol}")
            return result
        except Exception as e:
            logger.error(f"Cancel all orders failed for {symbol}: {e}")
            return []
    
    async def edit_order(
        self,
        order_id: str,
        symbol: str,
        side: str,
        amount: Optional[float] = None,
        price: Optional[float] = None,
        position_side: Optional[str] = None,
    ) -> OrderResult:
        """
        Amend an existing order (change price and/or amount).
        
        More efficient than cancel+place for chase orders.
        
        Args:
            order_id: ID of order to amend
            symbol: Trading symbol
            side: 'buy' or 'sell' (must match original order)
            amount: New amount (optional, keeps current if None)
            price: New price (optional, keeps current if None)
            position_side: For hedge mode: 'long' or 'short'. None for one-way mode.
        
        Returns:
            OrderResult with updated order details
        """
        try:
            params = {}
            if position_side:
                params["posSide"] = position_side.capitalize()
            
            # Truncate amount to minimum step size if provided
            truncated_amount = truncate_to_step_size(amount) if amount else None
            
            order = await self.exchange.edit_order(
                id=order_id,
                symbol=symbol,
                type="limit",
                side=side,
                amount=truncated_amount,
                price=price,
                params=params,
            )
            logger.debug(f"Amended order {order_id}: amount={amount}, price={price}")
            return OrderResult.from_ccxt_order(order)
        except Exception as e:
            logger.error(f"Edit order {order_id} failed: {e}")
            raise
    
    async def cancel_order(
        self,
        order_id: str,
        symbol: str,
        position_side: Optional[str] = None,
    ) -> dict:
        """
        Cancel a specific order.
        
        Args:
            order_id: Order ID to cancel
            symbol: Trading symbol
            position_side: For hedge mode: 'long' or 'short'. None for one-way mode.
        
        Returns:
            Cancelled order response
        """
        try:
            params = {}
            if position_side:
                params["posSide"] = position_side.capitalize()
            
            result = await self.exchange.cancel_order(order_id, symbol, params)
            logger.info(f"Cancelled order {order_id}")
            return result
        except Exception as e:
            logger.error(f"Cancel order {order_id} failed: {e}")
            raise
    
    async def fetch_positions(self, symbols: Optional[list[str]] = None) -> list[dict]:
        """
        Fetch current positions.
        
        Args:
            symbols: List of symbols to fetch. If None, fetches all.
        
        Returns:
            List of position dictionaries
        """
        return await self.exchange.fetch_positions(symbols)
    
    async def fetch_balance(self) -> dict:
        """Fetch account balance."""
        return await self.exchange.fetch_balance()
    
    async def fetch_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch open orders.
        
        Args:
            symbol: Optional symbol filter
        
        Returns:
            List of open orders
        """
        return await self.exchange.fetch_open_orders(symbol)
    
    async def fetch_order(
        self,
        order_id: str,
        symbol: str,
    ) -> Optional[dict]:
        """
        Fetch a specific order by ID.
        
        Useful for checking order status when WebSocket cache doesn't have it.
        
        Args:
            order_id: Order ID to fetch
            symbol: Trading symbol
        
        Returns:
            Order dict if found, None if not found
        """
        try:
            return await self.exchange.fetch_order(order_id, symbol)
        except Exception as e:
            if "ORDER_NOT_FOUND" in str(e) or "10002" in str(e):
                return None
            logger.warning(f"Fetch order {order_id} failed: {e}")
            return None
    
    async def watch_positions(self):
        """
        Watch positions via WebSocket.
        
        Yields position updates as they occur.
        """
        return await self.exchange.watch_positions()
    
    async def watch_orders(self, symbol: Optional[str] = None):
        """
        Watch orders via WebSocket.
        
        Args:
            symbol: Optional symbol filter
        
        Yields order updates as they occur.
        """
        return await self.exchange.watch_orders(symbol)
    
    async def watch_order_book(self, symbol: str, limit: int = 5):
        """
        Watch orderbook via WebSocket.
        
        Returns current orderbook snapshot with each call.
        The orderbook contains 'bids' and 'asks' arrays.
        
        Args:
            symbol: Trading symbol
            limit: Depth of orderbook to return
        
        Returns:
            Orderbook dict with 'bids' and 'asks'.
            bid1 = orderbook['bids'][0][0] (best bid price)
            ask1 = orderbook['asks'][0][0] (best ask price)
        """
        return await self.exchange.watch_order_book(symbol, limit)
    
    async def watch_balance(self):
        """Watch balance via WebSocket."""
        return await self.exchange.watch_balance()
    
    async def close(self) -> None:
        """Clean up exchange connection."""
        await self.exchange.close()
        logger.info("Exchange connection closed")
    
    async def _create_order_with_retry(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float],
        params: dict[str, Any],
    ) -> dict:
        """
        Create order with retry logic for network errors.
        
        Implements exponential backoff for transient failures.
        """
        last_error = None
        
        for attempt in range(self.MAX_RETRIES):
            try:
                order = await self.exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=side,
                    amount=amount,
                    price=price,
                    params=params,
                )
                return order
                
            except ccxt.NetworkError as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY * (attempt + 1)
                    logger.warning(
                        f"Network error, retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Order failed after {self.MAX_RETRIES} attempts")
                    raise
            
            except ccxt.ExchangeError as e:
                # Exchange errors are not retryable
                logger.error(f"Exchange error: {e}")
                raise
        
        # Should not reach here, but just in case
        raise last_error if last_error else Exception("Order creation failed")
