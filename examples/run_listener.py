# examples/run_listener.py
# Main entry point for the Phemex ZMQ Order Listener
# Starts WebSocket watchers and ZMQ listener concurrently
# RELEVANT FILES: config.py, exchange_client.py, zmq_listener.py, signal_processor.py

"""
Phemex ZMQ Order Listener - Main Entry Point

Listens for trading signals via ZMQ and executes on Phemex Futures.
Uses chase orders for entries/exits (follows bid/ask for guaranteed fills).
Uses market orders for stop-loss only (safety net).

Usage:
    poetry run python examples/run_listener.py
    poetry run python examples/run_listener.py --config /path/to/config.yml
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient
from phemex_client.position_manager import PositionManager
from phemex_client.chase_order_manager import ChaseOrderManager
from phemex_client.websocket_manager import WebsocketManager
from phemex_client.signal_processor import SignalProcessor
from phemex_client.zmq_listener import ZmqListener


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phemex ZMQ Order Listener"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yml (default: ./config.yml)"
    )
    return parser.parse_args()


def print_startup_banner(config) -> None:
    """Print startup configuration banner."""
    logger.info("=" * 60)
    logger.info("Phemex ZMQ Order Listener")
    logger.info("=" * 60)
    logger.info(f"Testnet mode: {config.phemex.testnet}")
    logger.info(f"Symbols: {config.trading.symbols}")
    logger.info(f"Leverage: {config.trading.leverage}x")
    logger.info(f"Deposit: {config.position_sizing.deposit_size} USDT")
    logger.info(f"R Value: {config.position_sizing.r_value:.2f} USDT "
                f"({config.position_sizing.r_percentage * 100}%)")
    logger.info(f"ZMQ: {config.zmq.url} (topic: {config.zmq.topic})")
    logger.info("=" * 60)


async def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    
    # Validate credentials
    if not config.phemex.api_key or not config.phemex.secret:
        logger.error("Missing api_key or secret in config.yml")
        sys.exit(1)
    
    print_startup_banner(config)
    
    # Initialize exchange client
    exchange = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    try:
        await exchange.initialize(
            symbols=config.trading.symbols,
            leverage=config.trading.leverage,
        )
        logger.info("Exchange initialized")
        
        # Enforce single symbol limit
        if len(config.trading.symbols) > 1:
            logger.error("App supports strictly ONE symbol at a time.")
            logger.error(f"Found {len(config.trading.symbols)}: {config.trading.symbols}")
            await exchange.close()
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Failed to initialize exchange: {e}")
        await exchange.close()
        sys.exit(1)
    
    # Initialize position manager
    position_manager = PositionManager(exchange.exchange)
    
    try:
        await position_manager.load_initial_positions()
        logger.info(f"Loaded {len(position_manager.positions)} existing positions")
        
    except Exception as e:
        logger.error(f"Failed to load positions: {e}")
        await exchange.close()
        sys.exit(1)
    
    # Initialize Websocket Manager (Singleton)
    ws_manager = WebsocketManager.get_instance(exchange)
    
    try:
        await ws_manager.start(config.trading.symbols)
        logger.info("Websocket manager started")
        
    except Exception as e:
        logger.error(f"Failed to start websocket manager: {e}")
        await exchange.close()
        sys.exit(1)
    
    # Initialize chase order manager (singleton)
    # Requires ws_manager for dependency injection
    chase_manager = ChaseOrderManager.get_instance(exchange, ws_manager)
    
    try:
        # Start chase manager (command processor)
        await chase_manager.start()
        logger.info("Chase order manager started")
        
    except Exception as e:
        logger.error(f"Failed to start chase manager: {e}")
        await exchange.close()
        sys.exit(1)
    
    # Initialize signal processor (uses chase orders for entries/exits)
    signal_processor = SignalProcessor(
        exchange_client=exchange,
        position_manager=position_manager,
        chase_manager=chase_manager,
        deposit_size=config.position_sizing.deposit_size,
        r_percentage=config.position_sizing.r_percentage,
        leverage=config.trading.leverage,
    )
    
    # Initialize ZMQ listener
    zmq_listener = ZmqListener(
        signal_processor=signal_processor,
        host=config.zmq.host,
        port=config.zmq.port,
        topic=config.zmq.topic,
    )
    
    # Create background tasks
    tasks = [
        asyncio.create_task(
            position_manager.watch_positions(),
            name="position_watcher"
        ),
        asyncio.create_task(
            zmq_listener.start(),
            name="zmq_listener"
        ),
    ]
    
    logger.info("Listener started. Press Ctrl+C to stop.")
    
    try:
        # Wait for all tasks (they run forever until cancelled)
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    finally:
        # Graceful shutdown
        logger.info("Shutting down...")
        
        position_manager.stop()
        zmq_listener.stop()
        await chase_manager.shutdown()
        await ws_manager.stop()
        ChaseOrderManager.reset_instance()
        WebsocketManager.reset_instance()
        
        # Cancel all tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Wait for cancellation to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close exchange connection
        await exchange.close()
        
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
