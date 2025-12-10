# examples/place_limit_order.py
# Example script to place a limit BUY order on SOLUSDT perpetual
# Places order 1% below current market price
# RELEVANT FILES: exchange_client.py, config.yml, config.py, models.py

"""
Place a limit BUY order on SOLUSDT perpetual futures.

This example:
1. Fetches current SOL price
2. Calculates limit price (1% below market)
3. Places a post-only limit BUY order
4. Shows order details

Usage:
    poetry run python examples/place_limit_order.py
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def place_limit_buy_order() -> None:
    """
    Place a limit BUY order on SOLUSDT perpetual.
    
    Order will be placed $10 below current market price.
    """
    logger.info("Starting SOLUSDT limit order placement...")
    
    # Configuration
    SYMBOL = "SOL/USDT:USDT"
    PRICE_OFFSET_PCT = 0.01  # 1% below market
    ORDER_SIZE_USDT = 5.0  # Notional size in USDT (minimum on Phemex)
    
    # Load config
    config = load_config("config.yml")
    
    # Initialize client
    client = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    try:
        # Initialize exchange
        await client.initialize(
            symbols=[SYMBOL],
            leverage=config.trading.leverage,
        )
        
        logger.info("=" * 70)
        logger.info("STEP 1: Fetch Current Price")
        logger.info("=" * 70)
        
        # Fetch current price
        ticker = await client.exchange.fetch_ticker(SYMBOL)
        current_price = ticker['last']
        
        logger.info(f"Current SOL Price: ${current_price:.4f}")
        logger.info(f"24h High: ${ticker.get('high', 0):.4f}")
        logger.info(f"24h Low: ${ticker.get('low', 0):.4f}")
        
        # Calculate limit price (1% below market)
        limit_price = current_price * (1 - PRICE_OFFSET_PCT)
        price_diff = current_price - limit_price
        
        logger.info(f"\nLimit Price ({PRICE_OFFSET_PCT*100:.1f}% below): ${limit_price:.4f}")
        logger.info(f"Distance to market: ${price_diff:.2f}")
        
        # Calculate order amount (in SOL contracts)
        # For Phemex perpetuals, amount is in base currency (SOL)
        order_amount = ORDER_SIZE_USDT / limit_price
        
        logger.info(f"Order Size: {order_amount:.4f} SOL (~${ORDER_SIZE_USDT} notional)")
        
        # Check account balance
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: Check Account Balance")
        logger.info("=" * 70)
        
        balance = await client.fetch_balance()
        usdt_balance = balance.get("USDT", {})
        free_balance = usdt_balance.get('free', 0)
        
        logger.info(f"Free USDT Balance: ${free_balance:.2f}")
        logger.info(f"Leverage: {config.trading.leverage}x")
        logger.info(f"Buying Power: ${free_balance * config.trading.leverage:.2f}")
        
        # Calculate required margin
        required_margin = ORDER_SIZE_USDT / config.trading.leverage
        logger.info(f"Required Margin: ${required_margin:.2f}")
        
        if required_margin > free_balance:
            logger.error(f"\n❌ Insufficient balance!")
            logger.error(f"   Required: ${required_margin:.2f}")
            logger.error(f"   Available: ${free_balance:.2f}")
            return
        
        logger.info("✅ Sufficient balance available")
        
        # Place limit order
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: Place Limit BUY Order")
        logger.info("=" * 70)
        
        logger.info(f"\nPlacing order:")
        logger.info(f"  Symbol: {SYMBOL}")
        logger.info(f"  Side: BUY (Long)")
        logger.info(f"  Type: Limit Post-Only")
        logger.info(f"  Price: ${limit_price:.4f}")
        logger.info(f"  Amount: {order_amount:.4f} SOL")
        logger.info(f"  Notional: ~${ORDER_SIZE_USDT:.2f}")
        
        # Confirm before placing
        logger.info("\n⚠️  About to place order on Phemex...")
        logger.info("⚠️  This is a REAL order that will be executed!")
        
        # Place the order
        order_result = await client.place_limit_post_only(
            symbol=SYMBOL,
            side="buy",
            amount=order_amount,
            price=limit_price,
            reduce_only=False,  # Opening a new position
        )
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ ORDER PLACED SUCCESSFULLY!")
        logger.info("=" * 70)
        
        logger.info(f"\nOrder Details:")
        logger.info(f"  Order ID: {order_result.order_id}")
        logger.info(f"  Symbol: {order_result.symbol}")
        logger.info(f"  Side: {order_result.side.upper()}")
        logger.info(f"  Type: {order_result.order_type}")
        logger.info(f"  Status: {order_result.status}")
        logger.info(f"  Price: ${order_result.price:.4f}")
        logger.info(f"  Amount: {order_result.amount:.4f} SOL")
        logger.info(f"  Filled: {order_result.filled:.4f} SOL")
        logger.info(f"  Timestamp: {order_result.timestamp}")
        
        logger.info(f"\n💡 Order is waiting at ${limit_price:.4f}")
        logger.info(f"💡 Current market price: ${current_price:.4f}")
        logger.info(f"💡 Distance to market: ${price_diff:.2f} (-{PRICE_OFFSET_PCT*100:.1f}%)")
        
        # Check open orders
        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: Verify Open Orders")
        logger.info("=" * 70)
        
        open_orders = await client.fetch_open_orders(SYMBOL)
        logger.info(f"\nTotal open orders for {SYMBOL}: {len(open_orders)}")
        
        for order in open_orders:
            logger.info(f"  - {order['side'].upper()} {order['amount']} @ ${order['price']} ({order['status']})")
        
        logger.info("\n" + "=" * 70)
        logger.info("Done!")
        logger.info("=" * 70)
        logger.info("\n💡 To cancel this order, you can:")
        logger.info(f"   - Use Phemex web interface")
        logger.info(f"   - Call client.cancel_order('{order_result.order_id}', '{SYMBOL}')")
        logger.info(f"   - Call client.cancel_all_orders('{SYMBOL}')")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
    
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(place_limit_buy_order())
