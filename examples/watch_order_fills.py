# examples/watch_order_fills.py
# Simple listener that logs all order updates via WebSocket
# Run this, then place orders manually to see what events arrive
# RELEVANT FILES: exchange_client.py, chase_order_manager.py

"""
Watch and log all order updates via WebSocket.

Run this script, then place/fill orders manually on Phemex.
All order updates will be logged to see what events we receive.

Usage:
    poetry run python examples/watch_order_fills.py
    poetry run python examples/watch_order_fills.py --order-id ac84c300-1912-4847-8cb0-e5dc28469c9a
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def watch_order_fills(filter_order_id: str | None = None) -> None:
    """
    Watch all order updates and log them.
    
    Args:
        filter_order_id: If provided, only show updates for this order ID
    """
    SYMBOL = "SOL/USDT:USDT"
    
    logger.info("=" * 70)
    logger.info("WATCHING ORDER FILLS")
    if filter_order_id:
        logger.info(f"Filtering for order: {filter_order_id}")
    else:
        logger.info("Place orders manually on Phemex and see events here")
    logger.info("=" * 70)
    
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
        await client.initialize([SYMBOL], leverage=20)
        
        logger.info(f"\nListening for order updates on {SYMBOL}...")
        logger.info("Press Ctrl+C to stop\n")
        
        # Watch orders continuously
        while True:
            try:
                orders = await client.watch_orders(SYMBOL)
                
                for order in orders:
                    order_id = order.get("id", "unknown")
                    
                    # Filter by order ID if specified
                    if filter_order_id and order_id != filter_order_id:
                        continue
                    
                    status = order.get("status", "unknown")
                    side = order.get("side", "unknown")
                    order_type = order.get("type", "unknown")
                    price = order.get("price", 0)
                    amount = order.get("amount", 0)
                    filled = order.get("filled", 0)
                    remaining = order.get("remaining", 0)
                    average = order.get("average")
                    
                    logger.info("-" * 50)
                    logger.info(f"ORDER UPDATE RECEIVED:")
                    logger.info(f"  ID: {order_id}")
                    logger.info(f"  Status: {status}")
                    logger.info(f"  Side: {side}")
                    logger.info(f"  Type: {order_type}")
                    logger.info(f"  Price: {price}")
                    logger.info(f"  Amount: {amount}")
                    logger.info(f"  Filled: {filled}")
                    logger.info(f"  Remaining: {remaining}")
                    logger.info(f"  Average: {average}")
                    
                    # Highlight fills
                    if filled > 0:
                        logger.info(f"  >>> FILL DETECTED: {filled} @ {average}")
                    
                    if status in ("closed", "filled"):
                        logger.info(f"  >>> ORDER COMPLETED")
                    elif status == "canceled":
                        logger.info(f"  >>> ORDER CANCELED")
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Stream error: {e}")
                await asyncio.sleep(1.0)
                
    except KeyboardInterrupt:
        logger.info("\n\nStopped by user")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        
    finally:
        await client.close()
        logger.info("Connection closed")


def main():
    parser = argparse.ArgumentParser(description="Watch order fills via WebSocket")
    parser.add_argument(
        "--order-id", "-o",
        help="Filter updates for a specific order ID",
        default=None,
    )
    args = parser.parse_args()
    
    asyncio.run(watch_order_fills(args.order_id))


if __name__ == "__main__":
    main()

