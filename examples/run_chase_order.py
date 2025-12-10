# examples/run_chase_order.py
# Example: Place a chase limit order that follows bid1/ask1
# Demonstrates ChaseOrderManager singleton with warmup and command queue
# RELEVANT FILES: chase_order_manager.py, models.py, exchange_client.py

"""
Chase Limit Order Example.

Shows the singleton pattern:
1. Get ChaseOrderManager instance
2. Warmup (starts WS streaming)
3. Submit chase order via queue
4. Monitor until filled/stopped

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
from phemex_client.websocket_manager import WebsocketManager
from phemex_client.models import ChaseOrderConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def run_chase_order() -> None:
    """
    Run a chase limit order example.
    
    Uses singleton pattern with warmup for low-latency execution.
    """
    # Configuration
    SYMBOL = "SOL/USDT:USDT"
    ORDER_SIZE_USDT = 5.0  # Minimum notional on Phemex
    
    logger.info("=" * 70)
    logger.info("CHASE LIMIT ORDER EXAMPLE (Singleton Pattern)")
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
        
        # Get singleton websocket manager
        ws_manager = WebsocketManager.get_instance(client)
        
        # Start WS streaming
        logger.info("\nStarting WebsocketManager...")
        await ws_manager.start([SYMBOL])
        
        # Get singleton chase manager
        chase_manager = ChaseOrderManager.get_instance(client, ws_manager)
        
        # Start chase manager
        await chase_manager.start()
        
        # Get current cached prices
        prices = ws_manager.get_prices(SYMBOL)
        if prices:
            logger.info(f"\nCached Prices (real-time from WS):")
            logger.info(f"  Bid1: ${prices['bid1']:.4f}")
            logger.info(f"  Ask1: ${prices['ask1']:.4f}")
            logger.info(f"  Spread: ${prices['ask1'] - prices['bid1']:.4f}")
        
        # Calculate order amount
        current_price = prices['bid1'] if prices else 100.0
        order_amount = ORDER_SIZE_USDT / current_price
        
        # Create chase order config
        # NOTE: position_side is required for accounts in hedge mode
        # Set to 'long' for buy orders opening/managing a long position
        # Set to 'short' for sell orders opening/managing a short position
        chase_config = ChaseOrderConfig(
            symbol=SYMBOL,
            side="buy",
            amount=order_amount,
            # chase_mode defaults to bid2 for buys (one tick back from top)
            max_chase_distance=2.0,    # Stop if price moves $2 from start
            max_retries=50,            # Max 50 order updates
            reduce_only=False,
            position_side="long",      # Required for hedge mode accounts
        )
        
        logger.info(f"\nChase Order Config:")
        logger.info(f"  Symbol: {chase_config.symbol}")
        logger.info(f"  Side: {chase_config.side.upper()}")
        logger.info(f"  Amount: {chase_config.amount:.4f} SOL (~${ORDER_SIZE_USDT})")
        logger.info(f"  Mode: {chase_config.chase_mode or 'default (bid2/ask2)'}")
        logger.info(f"  Max Chase Distance: ${chase_config.max_chase_distance}")
        logger.info(f"  Max Retries: {chase_config.max_retries}")
        
        logger.info("\n" + "=" * 70)
        logger.info("Submitting chase order... Press Ctrl+C to cancel")
        logger.info("=" * 70 + "\n")
        
        # Submit chase via command queue (returns immediately)
        chase_id = await chase_manager.submit_chase(chase_config)
        
        # Monitor until complete
        while True:
            await asyncio.sleep(2.0)
            
            status = chase_manager.get_chase_status(chase_id)
            if not status:
                break
            
            # Log current prices
            prices = ws_manager.get_prices(SYMBOL)
            if prices:
                logger.info(
                    f"Status: {status['status']} | "
                    f"Retries: {status['retry_count']} | "
                    f"Filled: {status['total_filled']:.4f}/{status['amount']:.4f} | "
                    f"Price: {status['current_price']:.4f} | "
                    f"Bid1: {prices['bid1']:.4f}"
                )
            
            if status["status"] != "active":
                logger.info(f"\n✅ Chase completed: {status['status']}")
                logger.info(f"   Retries: {status['retry_count']}")
                if status.get('fill_price'):
                    logger.info(f"   Fill Price: ${status['fill_price']:.4f}")
                break
    
    except KeyboardInterrupt:
        logger.info("\n\n⚠️ Ctrl+C received, canceling chase...")
        if chase_manager and chase_id:
            await chase_manager.cancel_chase(chase_id)
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
    
    finally:
        if chase_manager:
            await chase_manager.shutdown()
        if 'ws_manager' in locals() and ws_manager:
            await ws_manager.stop()
        await client.close()
        # Reset singleton for next run
        ChaseOrderManager.reset_instance()
        WebsocketManager.reset_instance()
        logger.info("\nDone!")


if __name__ == "__main__":
    asyncio.run(run_chase_order())
