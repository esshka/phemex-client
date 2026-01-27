# examples/close_position_chase.py
# Example: Close an active position using a chase limit order
# Scans for open position and closes it at bid2/ask2 to avoid taker fees
# RELEVANT FILES: chase_order_manager.py, exchange_client.py, models.py

"""
Close Position with Chase Order.

Scans for an active SOL/USDT position and closes it using a chase
limit order at bid2 (for longs) or ask2 (for shorts).

This avoids taker fees by using post-only maker orders.

Usage:
    poetry run python examples/close_position_chase.py

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


async def close_position_with_chase() -> None:
    """
    Scan for active position and close it using chase order.
    """
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Close active position with chase order")
    parser.add_argument(
        "--side", 
        type=str, 
        choices=["long", "short"], 
        default="long",
        help="Position side to close (long or short)"
    )
    args = parser.parse_args()
    
    target_side = args.side.lower()
    
    SYMBOL = "SOL/USDT:USDT"
    
    logger.info("=" * 70)
    logger.info("CLOSE POSITION WITH CHASE ORDER")
    logger.info(f"Target Side: {target_side.upper()}")
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
        
        # Fetch current positions
        logger.info("\nScanning for active positions...")
        positions = await client.fetch_positions([SYMBOL])
        
        # Find position for our symbol
        position = None
        for pos in positions:
            if pos.get("symbol") == SYMBOL:
                pos_side = pos.get("side", "").lower()
                contracts = float(pos.get("contracts", 0))
                
                # Only select if it matches our target side and has size
                if abs(contracts) > 0 and pos_side == target_side:
                    position = pos
                    break
        
        if not position:
            logger.info(f"✓ No active {target_side.upper()} position found. Nothing to close.")
            return
        
        # Extract position details
        contracts = float(position.get("contracts", 0))
        side = position.get("side")  # 'long' or 'short'
        entry_price = float(position.get("entryPrice", 0))
        unrealized_pnl = float(position.get("unrealizedPnl", 0))
        
        logger.info(f"\n📊 Found Active Position:")
        logger.info(f"   Symbol: {SYMBOL}")
        logger.info(f"   Side: {side.upper()}")
        logger.info(f"   Size: {abs(contracts):.4f} SOL")
        logger.info(f"   Entry: ${entry_price:.4f}")
        logger.info(f"   Unrealized PnL: ${unrealized_pnl:.4f}")
        
        # Determine close order parameters
        # Long position -> SELL to close with position_side='long'
        # Short position -> BUY to close with position_side='short'
        if side == "long":
            close_side = "sell"
            position_side = "long"
        else:
            close_side = "buy"
            position_side = "short"
        
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
            logger.info(f"\nCurrent Market:")
            logger.info(f"   Bid1: ${prices['bid1']:.4f}")
            logger.info(f"   Ask1: ${prices['ask1']:.4f}")
            logger.info(f"   Spread: ${prices['ask1'] - prices['bid1']:.4f}")
        
        # Create chase order config for closing position
        chase_config = ChaseOrderConfig(
            symbol=SYMBOL,
            side=close_side,
            amount=abs(contracts),
            # Uses default mode: ask2 for sells, bid2 for buys
            max_chase_distance=2.0,    # Stop if price moves $2 from start
            max_retries=100,           # More retries for closing
            reduce_only=True,          # IMPORTANT: only reduce position
            position_side=position_side,  # Required for hedge mode
        )
        
        logger.info(f"\nChase Order Config:")
        logger.info(f"   Action: CLOSE {side.upper()} position")
        logger.info(f"   Order Side: {close_side.upper()}")
        logger.info(f"   Amount: {chase_config.amount:.4f} SOL")
        logger.info(f"   Position Side: {position_side}")
        logger.info(f"   Reduce Only: {chase_config.reduce_only}")
        
        logger.info("\n" + "=" * 70)
        logger.info("Starting chase order to close position... Press Ctrl+C to cancel")
        logger.info("=" * 70 + "\n")
        
        # Submit chase via command queue
        chase_id = await chase_manager.submit_chase(chase_config)
        
        # Monitor until complete
        while True:
            await asyncio.sleep(2.0)
            
            status = chase_manager.get_chase_status(chase_id)
            if not status:
                break
            
            # Log current status
            prices = ws_manager.get_prices(SYMBOL)
            if prices:
                ref_price = prices['ask1'] if close_side == 'sell' else prices['bid1']
                logger.info(
                    f"Status: {status['status']} | "
                    f"Retries: {status['retry_count']} | "
                    f"Filled: {status['total_filled']:.4f}/{status['amount']:.4f} | "
                    f"Price: {status['current_price']:.4f} | "
                    f"Market: {ref_price:.4f}"
                )
            
            if status["status"] != "active":
                logger.info(f"\n✅ Position close completed: {status['status']}")
                logger.info(f"   Retries: {status['retry_count']}")
                logger.info(f"   Total Filled: {status['total_filled']:.4f}")
                if status.get('fill_price'):
                    logger.info(f"   Avg Fill Price: ${status['fill_price']:.4f}")
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
        ChaseOrderManager.reset_instance()
        WebsocketManager.reset_instance()
        logger.info("\nDone!")


if __name__ == "__main__":
    asyncio.run(close_position_with_chase())
