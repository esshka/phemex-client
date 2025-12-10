# examples/run_chase_order.py
# Example: Place a chase limit order that follows bid1/ask1
# Demonstrates ChaseOrderManager for market-following orders
# RELEVANT FILES: chase_order_manager.py, models.py, exchange_client.py

"""
Chase Limit Order Example.

Places a BUY order at bid1 (best bid) and automatically
updates it to follow the market price until filled.

Usage:
    poetry run python examples/run_chase_order.py

Press Ctrl+C to cancel the chase and exit.
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient
from phemex_client.chase_order_manager import ChaseOrderManager
from phemex_client.models import ChaseOrderConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def run_chase_order() -> None:
    """
    Run a chase limit order example.
    
    Places a small BUY order that chases bid1 price.
    """
    # Configuration
    SYMBOL = "SOL/USDT:USDT"
    ORDER_SIZE_USDT = 5.0  # Minimum notional on Phemex
    
    logger.info("=" * 70)
    logger.info("CHASE LIMIT ORDER EXAMPLE")
    logger.info("=" * 70)
    
    # Load config
    config = load_config("config.yml")
    
    # Initialize client
    client = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    chase_manager = None
    chase_id = None
    
    try:
        # Initialize exchange
        await client.initialize(
            symbols=[SYMBOL],
            leverage=config.trading.leverage,
        )
        
        # Get current price to calculate order size
        ticker = await client.exchange.fetch_ticker(SYMBOL)
        current_price = ticker['last']
        
        # Get bid1/ask1 from orderbook (ticker may not have them)
        orderbook = await client.exchange.fetch_order_book(SYMBOL, limit=5)
        bid1 = orderbook['bids'][0][0] if orderbook['bids'] else current_price
        ask1 = orderbook['asks'][0][0] if orderbook['asks'] else current_price
        
        logger.info(f"\nCurrent Market:")
        logger.info(f"  Price: ${current_price:.4f}")
        logger.info(f"  Bid1:  ${bid1:.4f}")
        logger.info(f"  Ask1:  ${ask1:.4f}")
        logger.info(f"  Spread: ${ask1 - bid1:.4f}")
        
        # Calculate order amount
        order_amount = ORDER_SIZE_USDT / current_price
        
        # Create chase order config
        chase_config = ChaseOrderConfig(
            symbol=SYMBOL,
            side="buy",
            amount=order_amount,
            chase_mode="bid1",         # Chase at best bid
            max_chase_distance=2.0,    # Stop if price moves $2 from start
            max_retries=50,            # Max 50 order updates
            reduce_only=False,
        )
        
        logger.info(f"\nChase Order Config:")
        logger.info(f"  Symbol: {chase_config.symbol}")
        logger.info(f"  Side: {chase_config.side.upper()}")
        logger.info(f"  Amount: {chase_config.amount:.4f} SOL (~${ORDER_SIZE_USDT})")
        logger.info(f"  Mode: {chase_config.chase_mode}")
        logger.info(f"  Max Chase Distance: ${chase_config.max_chase_distance}")
        logger.info(f"  Max Retries: {chase_config.max_retries}")
        
        # Create chase manager and start
        chase_manager = ChaseOrderManager(client)
        
        logger.info("\n" + "=" * 70)
        logger.info("Starting chase order... Press Ctrl+C to cancel")
        logger.info("=" * 70 + "\n")
        
        chase_id = await chase_manager.start_chase(chase_config)
        
        # Monitor until complete
        while True:
            await asyncio.sleep(2.0)
            
            status = chase_manager.get_chase_status(chase_id)
            if not status:
                break
            
            if status["status"] != "active":
                logger.info(f"\nChase completed: {status['status']}")
                logger.info(f"  Retries: {status['retry_count']}")
                logger.info(f"  Fill Price: {status.get('fill_price', 'N/A')}")
                break
    
    except KeyboardInterrupt:
        logger.info("\n\n⚠️ Ctrl+C received, canceling chase...")
        if chase_manager and chase_id:
            await chase_manager.cancel_chase(chase_id)
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
    
    finally:
        await client.close()
        logger.info("\nDone!")


if __name__ == "__main__":
    asyncio.run(run_chase_order())
