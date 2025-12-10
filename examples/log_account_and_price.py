# examples/log_account_and_price.py
# Log account status, SOLUSDT price, active positions, and open orders
# Demonstrates basic Phemex client usage for fetching account and market data
# RELEVANT FILES: exchange_client.py, config.yml, config.py, models.py

"""
Example script that logs:
1. Account balance and status
2. Current SOLUSDT perpetual price
3. Active positions (with PnL)
4. Open orders

Usage:
    poetry run python examples/log_account_and_price.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def log_account_status(client: PhemexClient) -> None:
    """
    Log account balance and status information.
    
    Args:
        client: Initialized PhemexClient instance
    """
    logger.info("=" * 60)
    logger.info("ACCOUNT STATUS")
    logger.info("=" * 60)
    
    try:
        balance = await client.fetch_balance()
        
        # Log USDT balance (futures wallet)
        usdt_info = balance.get("USDT", {})
        
        logger.info(f"Total Balance: {usdt_info.get('total', 0):.2f} USDT")
        logger.info(f"Free Balance: {usdt_info.get('free', 0):.2f} USDT")
        logger.info(f"Used Balance: {usdt_info.get('used', 0):.2f} USDT")
        
        # Log additional info if available
        if "info" in balance:
            info = balance["info"]
            if "data" in info:
                data = info["data"]
                
                # Account equity
                account_margin_map = data.get("accountMarginMap", {})
                if account_margin_map:
                    for account_id, margin_info in account_margin_map.items():
                        account_balance = margin_info.get("accountBalance", 0)
                        total_used_margin = margin_info.get("totalUsedMargin", 0)
                        account_margin_rate = margin_info.get("accountMarginRate", 0)
                        
                        # Convert from Phemex's scaled values (they use 10^8 for USDT)
                        account_balance_scaled = account_balance / 1e8
                        total_used_margin_scaled = total_used_margin / 1e8
                        
                        logger.info(f"\nAccount {account_id}:")
                        logger.info(f"  Account Balance: {account_balance_scaled:.2f} USDT")
                        logger.info(f"  Used Margin: {total_used_margin_scaled:.2f} USDT")
                        logger.info(f"  Margin Rate: {account_margin_rate:.4f}")
        
    except Exception as e:
        logger.error(f"Failed to fetch account status: {e}")


async def log_solusdt_price(client: PhemexClient) -> None:
    """
    Log current SOLUSDT perpetual price.
    
    Args:
        client: Initialized PhemexClient instance
    """
    logger.info("\n" + "=" * 60)
    logger.info("SOLUSDT PERPETUAL PRICE")
    logger.info("=" * 60)
    
    symbol = "SOL/USDT:USDT"
    
    try:
        # Fetch ticker data
        ticker = await client.exchange.fetch_ticker(symbol)
        
        logger.info(f"Symbol: {symbol}")
        
        # Handle potentially None values
        last = ticker.get('last')
        if last is not None:
            logger.info(f"Last Price: {last:.4f} USDT")
        
        bid = ticker.get('bid')
        if bid is not None:
            logger.info(f"Bid: {bid:.4f} USDT")
        
        ask = ticker.get('ask')
        if ask is not None:
            logger.info(f"Ask: {ask:.4f} USDT")
        
        quote_volume = ticker.get('quoteVolume')
        if quote_volume is not None:
            logger.info(f"24h Volume: {quote_volume:.2f} USDT")
        
        percentage = ticker.get('percentage')
        if percentage is not None:
            logger.info(f"24h Change: {percentage:.2f}%")
        
        high = ticker.get('high')
        if high is not None:
            logger.info(f"24h High: {high:.4f} USDT")
        
        low = ticker.get('low')
        if low is not None:
            logger.info(f"24h Low: {low:.4f} USDT")
        
        # Additional info
        if "info" in ticker:
            info = ticker["info"]
            
            # Mark price (important for perpetuals)
            mark_price = info.get("markPrice")
            if mark_price:
                # Phemex uses scaled values
                mark_price_scaled = float(mark_price) / 10000  # SOL uses 10^4 scaling
                logger.info(f"Mark Price: {mark_price_scaled:.4f} USDT")
            
            # Funding rate
            funding_rate = info.get("fundingRate")
            if funding_rate:
                funding_rate_scaled = float(funding_rate) / 1e8
                logger.info(f"Funding Rate: {funding_rate_scaled * 100:.6f}%")
        
    except Exception as e:
        logger.error(f"Failed to fetch SOLUSDT price: {e}")


async def log_active_positions(client: PhemexClient) -> None:
    """
    Log all active positions.
    
    Args:
        client: Initialized PhemexClient instance
    """
    logger.info("\n" + "=" * 60)
    logger.info("ACTIVE POSITIONS")
    logger.info("=" * 60)
    
    try:
        positions = await client.fetch_positions()
        
        # Filter only positions with non-zero size
        active_positions = [
            p for p in positions 
            if p.get('contracts') and float(p.get('contracts', 0)) != 0
        ]
        
        if not active_positions:
            logger.info("No active positions")
            return
        
        logger.info(f"Total active positions: {len(active_positions)}\n")
        
        for pos in active_positions:
            symbol = pos.get('symbol', 'N/A')
            side = pos.get('side', 'N/A')
            contracts = pos.get('contracts', 0)
            notional = pos.get('notional', 0)
            entry_price = pos.get('entryPrice', 0)
            mark_price = pos.get('markPrice', 0)
            unrealized_pnl = pos.get('unrealizedPnl', 0)
            percentage = pos.get('percentage', 0)
            leverage = pos.get('leverage', 0)
            
            logger.info(f"Position: {symbol}")
            logger.info(f"  Side: {side.upper()}")
            logger.info(f"  Contracts: {contracts}")
            logger.info(f"  Notional: ${abs(notional):.2f}")
            logger.info(f"  Entry Price: ${entry_price:.4f}")
            logger.info(f"  Mark Price: ${mark_price:.4f}")
            logger.info(f"  Unrealized PnL: ${unrealized_pnl:.2f} ({percentage:.2f}%)")
            logger.info(f"  Leverage: {leverage}x")
            logger.info("")
        
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")


async def log_open_orders(client: PhemexClient) -> None:
    """
    Log all open orders.
    
    Args:
        client: Initialized PhemexClient instance
    """
    logger.info("\n" + "=" * 60)
    logger.info("OPEN ORDERS")
    logger.info("=" * 60)
    
    try:
        # Fetch open orders for SOL/USDT
        # Phemex requires a symbol argument
        symbol = "SOL/USDT:USDT"
        orders = await client.fetch_open_orders(symbol)
        
        if not orders:
            logger.info(f"No open orders for {symbol}")
            return
        
        logger.info(f"Total open orders for {symbol}: {len(orders)}\n")
        
        for order in orders:
            order_id = order.get('id', 'N/A')
            symbol = order.get('symbol', 'N/A')
            order_type = order.get('type', 'N/A')
            side = order.get('side', 'N/A')
            price = order.get('price', 0)
            amount = order.get('amount', 0)
            filled = order.get('filled', 0)
            remaining = order.get('remaining', 0)
            status = order.get('status', 'N/A')
            
            logger.info(f"Order: {symbol}")
            logger.info(f"  ID: {order_id}")
            logger.info(f"  Type: {order_type.upper()}")
            logger.info(f"  Side: {side.upper()}")
            logger.info(f"  Price: ${price:.4f}")
            logger.info(f"  Amount: {amount:.4f}")
            logger.info(f"  Filled: {filled:.4f}")
            logger.info(f"  Remaining: {remaining:.4f}")
            logger.info(f"  Status: {status}")
            logger.info("")
        
    except Exception as e:
        logger.error(f"Failed to fetch open orders: {e}")



async def main() -> None:
    """
    Main function that initializes the client and logs data.
    """
    logger.info("Starting Phemex account and price logger...")
    
    # Load configuration
    config = load_config("config.yml")
    
    # Initialize client
    client = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    try:
        # Initialize exchange (load markets)
        await client.initialize(
            symbols=["SOL/USDT:USDT"],
            leverage=config.trading.leverage,
        )
        
        # Log account status
        await log_account_status(client)
        
        # Log SOLUSDT price
        await log_solusdt_price(client)
        
        # Log active positions
        await log_active_positions(client)
        
        # Log open orders
        await log_open_orders(client)
        
        logger.info("\n" + "=" * 60)
        logger.info("Done!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    
    finally:
        # Clean up
        await client.close()


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
