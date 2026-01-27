#!/usr/bin/env python3
# examples/test_aave_order.py
# Direct test of Phemex API for placing AAVE order
# Bypasses ZMQ to test exchange client directly
# RELEVANT FILES: exchange_client.py, chase_order_manager.py, config.py

"""
Direct API test - Place a small AAVE order to verify connectivity.

Usage:
    poetry run python examples/test_aave_order.py
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    """Test placing a small AAVE order."""
    
    # Load config
    config = load_config()
    
    # Initialize exchange
    exchange = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    symbol = "AAVE/USDT:USDT"
    
    try:
        # Initialize for AAVE
        await exchange.initialize(symbols=[symbol], leverage=20)
        logger.info(f"Exchange initialized for {symbol}")
        
        # Check step size
        step_size = exchange.get_amount_step_size(symbol)
        logger.info(f"AAVE step size: {step_size}")
        
        # Get current balance
        balance = await exchange.fetch_balance()
        usdt_free = balance.get("USDT", {}).get("free", 0)
        logger.info(f"USDT balance: {usdt_free}")
        
        # Fetch current price from orderbook
        orderbook = await exchange.watch_order_book(symbol, limit=5)
        best_bid = orderbook['bids'][0][0] if orderbook['bids'] else None
        best_ask = orderbook['asks'][0][0] if orderbook['asks'] else None
        logger.info(f"BBO: bid={best_bid}, ask={best_ask}")
        
        if not best_bid or not best_ask:
            logger.error("No orderbook data")
            return
        
        # Place a small LONG limit order (0.1 AAVE = minimum)
        # Use bid price - it will sit in the book as a maker order
        amount = 0.1  # Minimum for AAVE
        price = best_bid  # Post at best bid
        
        logger.info(f"Placing order: BUY {amount} AAVE @ {price}")
        
        order = await exchange.place_limit_post_only(
            symbol=symbol,
            side="buy",
            amount=amount,
            price=price,
            reduce_only=False,
            position_side="long",
        )
        
        logger.info(f"Order placed: {order}")
        logger.info(f"Order ID: {order.order_id}")
        
        # Wait a moment then cancel
        await asyncio.sleep(2)
        
        # Cancel the order
        logger.info("Cancelling order...")
        await exchange.cancel_order(order.order_id, symbol, position_side="long")
        logger.info("Order cancelled")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        
    finally:
        await exchange.close()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
