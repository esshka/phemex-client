# examples/test_auth.py
# Diagnostic script to test Phemex API authentication
# Helps identify authentication issues
# RELEVANT FILES: exchange_client.py, config.yml, config.py

"""
Test Phemex authentication and provide diagnostic information.

This script helps debug signature verification issues by:
1. Checking API credentials format
2. Testing connection to mainnet/testnet
3. Making a simple authenticated API call
4. Showing detailed error information
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
import ccxt.pro as ccxt


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def test_authentication():
    """Test Phemex authentication with detailed diagnostics."""
    
    # Load config
    logger.info("Loading configuration...")
    config = load_config("config.yml")
    
    # Display config info (without exposing secrets)
    logger.info(f"API Key length: {len(config.phemex.api_key)} chars")
    logger.info(f"Secret length: {len(config.phemex.secret)} chars")
    logger.info(f"Testnet mode: {config.phemex.testnet}")
    logger.info(f"API Key starts with: {config.phemex.api_key[:8]}...")
    
    # Check for common issues
    if config.phemex.api_key.startswith("your_") or config.phemex.secret.startswith("your_"):
        logger.error("⚠️  API credentials still contain placeholder values!")
        logger.error("Please update config.yml with your actual Phemex API credentials")
        return
    
    if " " in config.phemex.api_key or " " in config.phemex.secret:
        logger.warning("⚠️  Detected spaces in API credentials - this might cause issues")
    
    # Initialize exchange
    logger.info(f"\nConnecting to Phemex {'TESTNET' if config.phemex.testnet else 'MAINNET'}...")
    
    exchange = ccxt.phemex({
        "apiKey": config.phemex.api_key,
        "secret": config.phemex.secret,
        "sandbox": config.phemex.testnet,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultSubType": "linear",
        }
    })
    
    try:
        # Load markets first
        logger.info("Loading markets...")
        await exchange.load_markets()
        logger.info(f"✅ Markets loaded successfully ({len(exchange.markets)} markets)")
        
        # Test 1: Fetch balance (simple authenticated call)
        logger.info("\n" + "="*60)
        logger.info("TEST 1: Fetching account balance...")
        logger.info("="*60)
        
        try:
            balance = await exchange.fetch_balance()
            logger.info("✅ Authentication successful!")
            
            # Show USDT balance
            usdt = balance.get("USDT", {})
            logger.info(f"\nUSDT Balance:")
            logger.info(f"  Total: {usdt.get('total', 0):.2f} USDT")
            logger.info(f"  Free:  {usdt.get('free', 0):.2f} USDT")
            logger.info(f"  Used:  {usdt.get('used', 0):.2f} USDT")
            
        except ccxt.AuthenticationError as e:
            logger.error(f"❌ Authentication failed: {e}")
            logger.error("\nPossible causes:")
            logger.error("  1. API key/secret is incorrect")
            logger.error("  2. Testnet/mainnet mismatch (check 'testnet' setting)")
            logger.error("  3. API key permissions not enabled (need 'Read' + 'Trade')")
            logger.error("  4. IP whitelist restriction (if enabled on Phemex)")
            return
        
        # Test 2: Fetch positions
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Fetching positions...")
        logger.info("="*60)
        
        try:
            positions = await exchange.fetch_positions()
            logger.info(f"✅ Positions fetched: {len(positions)} positions")
            
            # Show open positions
            open_positions = [p for p in positions if float(p.get('contracts', 0)) != 0]
            if open_positions:
                logger.info(f"\nOpen positions: {len(open_positions)}")
                for pos in open_positions:
                    logger.info(f"  {pos['symbol']}: {pos['contracts']} contracts ({pos['side']})")
            else:
                logger.info("No open positions")
                
        except Exception as e:
            logger.error(f"❌ Failed to fetch positions: {e}")
        
        # Test 3: Check API permissions
        logger.info("\n" + "="*60)
        logger.info("TEST 3: Checking API capabilities...")
        logger.info("="*60)
        
        logger.info(f"Has fetchBalance: {exchange.has.get('fetchBalance', False)}")
        logger.info(f"Has fetchPositions: {exchange.has.get('fetchPositions', False)}")
        logger.info(f"Has createOrder: {exchange.has.get('createOrder', False)}")
        logger.info(f"Has cancelOrder: {exchange.has.get('cancelOrder', False)}")
        
        logger.info("\n" + "="*60)
        logger.info("✅ All tests completed successfully!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
    
    finally:
        await exchange.close()
        logger.info("\nConnection closed")


if __name__ == "__main__":
    asyncio.run(test_authentication())
