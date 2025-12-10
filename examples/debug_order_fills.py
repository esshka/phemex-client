# examples/debug_order_fills.py
# Debug script to diagnose order fill detection issues
# Tests WebSocket order stream and REST API order fetch
# RELEVANT FILES: exchange_client.py, chase_order_manager.py

"""
Diagnostic script to understand why order fills aren't detected.

Places a small limit order and monitors it via:
1. WebSocket watch_orders stream
2. REST API fetch_order calls
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient


logging.basicConfig(
    level=logging.DEBUG,  # DEBUG level for full visibility
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def debug_order_fills() -> None:
    """
    Debug order fill detection.
    
    1. Place a limit order
    2. Watch for order updates via WS
    3. Periodically fetch order via REST
    """
    SYMBOL = "SOL/USDT:USDT"
    ORDER_SIZE = 0.01  # Tiny size
    
    logger.info("=" * 70)
    logger.info("DEBUG ORDER FILL DETECTION")
    logger.info("=" * 70)
    
    # Load config
    config = load_config("config.yml")
    
    # Initialize client
    client = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    order_id = None
    ws_task = None
    
    try:
        # Initialize exchange
        await client.initialize([SYMBOL], leverage=20)
        
        # Get current price
        orderbook = await client.watch_order_book(SYMBOL)
        bid1 = orderbook["bids"][0][0]
        ask1 = orderbook["asks"][0][0]
        
        logger.info(f"\nMarket: Bid={bid1}, Ask={ask1}")
        
        # Place a SELL limit order AT the ask (should fill immediately if crossed)
        # Actually, let's place it AT bid1 so it might fill
        limit_price = bid1
        
        logger.info(f"\nPlacing SELL order at {limit_price} (bid1)...")
        
        result = await client.place_limit_post_only(
            symbol=SYMBOL,
            side="sell",
            amount=ORDER_SIZE,
            price=limit_price,
            reduce_only=False,  # Not reduce-only, to see what happens
            position_side="short",  # Opening a short
        )
        
        order_id = result.order_id
        logger.info(f"Order placed: ID={order_id}")
        logger.info(f"Initial status: {result.status}, filled: {result.filled}")
        
        # Start WebSocket order watcher
        async def watch_orders_task():
            logger.info("Starting WS order watcher...")
            try:
                while True:
                    orders = await client.watch_orders(SYMBOL)
                    for order in orders:
                        oid = order.get("id", "")[:8]
                        status = order.get("status")
                        filled = order.get("filled", 0)
                        remaining = order.get("remaining", 0)
                        avg = order.get("average")
                        
                        logger.info(
                            f"[WS] Order {oid}... status={status} "
                            f"filled={filled} remaining={remaining} avg={avg}"
                        )
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"WS error: {e}")
        
        ws_task = asyncio.create_task(watch_orders_task())
        
        # Periodically check via REST
        logger.info("\nMonitoring order (Ctrl+C to stop)...")
        for i in range(30):  # 30 seconds
            await asyncio.sleep(1.0)
            
            try:
                # Fetch order by ID via REST
                order = await client._exchange.fetch_order(order_id, SYMBOL)
                
                status = order.get("status")
                filled = order.get("filled", 0)
                remaining = order.get("remaining", 0)
                avg = order.get("average")
                
                logger.info(
                    f"[REST] Status={status} filled={filled} "
                    f"remaining={remaining} avg={avg}"
                )
                
                if status in ("closed", "filled", "canceled"):
                    logger.info(f"\n✅ Order finished: {status}")
                    break
                    
            except Exception as e:
                logger.warning(f"[REST] fetch_order failed: {e}")
        
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        
    finally:
        # Cancel WS task
        if ws_task:
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
        
        # Cancel order if still open
        if order_id:
            try:
                await client.cancel_order(order_id, SYMBOL, position_side="short")
                logger.info(f"Order {order_id[:8]}... canceled")
            except Exception as e:
                logger.debug(f"Cancel failed (probably already closed): {e}")
        
        await client.close()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(debug_order_fills())
