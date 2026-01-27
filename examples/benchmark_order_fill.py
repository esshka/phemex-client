# examples/benchmark_order_fill.py
# Benchmarks order fill latency by measuring time from order send to WS events
# Measures: 1) time to order placement event, 2) time to fill event
# RELEVANT FILES: exchange_client.py, watch_order_fills.py, config.yml

"""
Benchmark Order Fill Latency

This script measures the time from sending an order to receiving:
1. Order placement event via WebSocket (order appears in the system)
2. Order fill event via WebSocket (order is executed)

Uses limit orders at BBO (bid1 for buy, ask1 for sell) for accurate measurement.

Usage:
    poetry run python examples/benchmark_order_fill.py
    poetry run python examples/benchmark_order_fill.py --iterations 10
    poetry run python examples/benchmark_order_fill.py --side sell
"""

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from phemex_client.config import load_config
from phemex_client.exchange_client import PhemexClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Single benchmark measurement result."""
    order_id: str
    send_time_ns: int
    placement_time_ns: int | None = None
    fill_time_ns: int | None = None
    
    @property
    def placement_latency_ms(self) -> float | None:
        """Time from send to placement event in milliseconds."""
        if self.placement_time_ns is None:
            return None
        return (self.placement_time_ns - self.send_time_ns) / 1_000_000
    
    @property
    def fill_latency_ms(self) -> float | None:
        """Time from send to fill event in milliseconds."""
        if self.fill_time_ns is None:
            return None
        return (self.fill_time_ns - self.send_time_ns) / 1_000_000


async def run_single_benchmark(
    client: PhemexClient,
    symbol: str,
    side: str,
    order_size: float,
    timeout_sec: float = 10.0,
) -> BenchmarkResult:
    """
    Run a single benchmark iteration.
    
    Places a limit order at BBO and measures time to placement/fill events.
    - BUY: places at ask1 (immediately fillable)
    - SELL: places at bid1 (immediately fillable)
    
    Args:
        client: Initialized PhemexClient
        symbol: Trading symbol
        side: 'buy' or 'sell'
        order_size: Order size in base currency
        timeout_sec: Max time to wait for events
    
    Returns:
        BenchmarkResult with timing measurements
    """
    result = BenchmarkResult(
        order_id="",
        send_time_ns=0,
    )
    
    order_placed = asyncio.Event()
    order_filled = asyncio.Event()
    
    async def watch_orders_task():
        """Monitor WebSocket for our order events."""
        try:
            while not order_filled.is_set():
                orders = await asyncio.wait_for(
                    client.watch_orders(symbol),
                    timeout=timeout_sec,
                )
                
                for order in orders:
                    order_id = order.get("id", "")
                    
                    # Check if this is our order
                    if order_id != result.order_id:
                        continue
                    
                    status = order.get("status", "")
                    filled = order.get("filled", 0)
                    
                    # Record placement time (first time we see this order)
                    if result.placement_time_ns is None:
                        result.placement_time_ns = time.perf_counter_ns()
                        order_placed.set()
                        logger.debug(f"Order placed: {order_id[:8]}... status={status}")
                    
                    # Record fill time when order is filled
                    if filled > 0 and result.fill_time_ns is None:
                        result.fill_time_ns = time.perf_counter_ns()
                        order_filled.set()
                        logger.debug(f"Order filled: {order_id[:8]}... filled={filled}")
                        return
                        
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for order events")
        except asyncio.CancelledError:
            pass
    
    # Start watching before placing order
    watch_task = asyncio.create_task(watch_orders_task())
    
    # Small delay to ensure WebSocket is ready
    await asyncio.sleep(0.1)
    
    order_id = None
    
    try:
        # Get current orderbook for BBO pricing
        orderbook = await client.watch_order_book(symbol)
        bid1 = orderbook["bids"][0][0]
        ask1 = orderbook["asks"][0][0]
        
        # Place limit order at BBO that will fill immediately
        # BUY at ask1 (crosses the spread, fills immediately)
        # SELL at bid1 (crosses the spread, fills immediately)
        if side == "buy":
            limit_price = ask1
            position_side = "long"
        else:
            limit_price = bid1
            position_side = "short"
        
        logger.debug(f"BBO: bid1={bid1}, ask1={ask1}, placing {side} at {limit_price}")
        
        # Place limit order and record send time
        result.send_time_ns = time.perf_counter_ns()
        
        order_response = await client.exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side,
            amount=order_size,
            price=limit_price,
            params={"posSide": "Long" if side == "buy" else "Short"},
        )
        
        result.order_id = order_response.get("id", "unknown")
        order_id = result.order_id
        logger.debug(f"Order sent: {result.order_id[:8]}... at {limit_price}")
        
        # Wait for fill event or timeout
        try:
            await asyncio.wait_for(order_filled.wait(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for fill event")
        
    finally:
        watch_task.cancel()
        try:
            await watch_task
        except asyncio.CancelledError:
            pass
        
        # Cancel any unfilled order
        if order_id and result.fill_time_ns is None:
            try:
                await client.cancel_order(order_id, symbol)
                logger.debug(f"Cancelled unfilled order {order_id[:8]}...")
            except Exception:
                pass
    
    return result


async def benchmark_order_fills(
    iterations: int = 5,
    side: str = "buy",
    symbol: str = "SOL/USDT:USDT",
    order_size_usdt: float = 5.0,
) -> None:
    """
    Run multiple benchmark iterations and report statistics.
    
    Args:
        iterations: Number of benchmark iterations
        side: Order side ('buy' or 'sell')
        symbol: Trading symbol
        order_size_usdt: Notional order size in USDT
    """
    logger.info("=" * 70)
    logger.info("ORDER FILL LATENCY BENCHMARK")
    logger.info("=" * 70)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Side: {side.upper()}")
    logger.info(f"Iterations: {iterations}")
    logger.info(f"Order Size: ~${order_size_usdt} USDT")
    logger.info(f"Order Type: Limit at BBO (bid1/ask1)")
    logger.info("=" * 70)
    
    # Load config
    config = load_config("config.yml")
    
    # Initialize client
    client = PhemexClient(
        api_key=config.phemex.api_key,
        secret=config.phemex.secret,
        sandbox=config.phemex.testnet,
    )
    
    results: list[BenchmarkResult] = []
    
    try:
        # Initialize exchange
        await client.initialize([symbol], leverage=20)
        
        # Get current price to calculate order size
        ticker = await client.exchange.fetch_ticker(symbol)
        current_price = ticker["last"]
        order_size = order_size_usdt / current_price
        
        # Get step size for symbol and round
        step_size = client.get_amount_step_size(symbol)
        order_size = round(order_size / step_size) * step_size
        
        logger.info(f"Current Price: ${current_price:.4f}")
        logger.info(f"Order Size: {order_size:.4f} ({symbol.split('/')[0]})")
        logger.info("-" * 70)
        
        # Run benchmark iterations
        for i in range(iterations):
            logger.info(f"\n[Iteration {i + 1}/{iterations}]")
            
            result = await run_single_benchmark(
                client=client,
                symbol=symbol,
                side=side,
                order_size=order_size,
            )
            
            results.append(result)
            
            # Log result
            placement_ms = result.placement_latency_ms
            fill_ms = result.fill_latency_ms
            
            logger.info(
                f"  Placement: {placement_ms:.2f}ms" 
                if placement_ms else "  Placement: N/A"
            )
            logger.info(
                f"  Fill:      {fill_ms:.2f}ms"
                if fill_ms else "  Fill: N/A"
            )
            
            # Wait a bit between iterations to avoid rate limiting
            if i < iterations - 1:
                await asyncio.sleep(0.5)
        
        # Calculate and report statistics
        logger.info("\n" + "=" * 70)
        logger.info("RESULTS SUMMARY")
        logger.info("=" * 70)
        
        placement_latencies = [
            r.placement_latency_ms for r in results 
            if r.placement_latency_ms is not None
        ]
        fill_latencies = [
            r.fill_latency_ms for r in results 
            if r.fill_latency_ms is not None
        ]
        
        if placement_latencies:
            logger.info(f"\nPlacement Latency (order sent → placement event):")
            logger.info(f"  Min:    {min(placement_latencies):.2f}ms")
            logger.info(f"  Max:    {max(placement_latencies):.2f}ms")
            logger.info(f"  Mean:   {mean(placement_latencies):.2f}ms")
            if len(placement_latencies) > 1:
                logger.info(f"  StdDev: {stdev(placement_latencies):.2f}ms")
        else:
            logger.warning("No placement latencies recorded")
        
        if fill_latencies:
            logger.info(f"\nFill Latency (order sent → fill event):")
            logger.info(f"  Min:    {min(fill_latencies):.2f}ms")
            logger.info(f"  Max:    {max(fill_latencies):.2f}ms")
            logger.info(f"  Mean:   {mean(fill_latencies):.2f}ms")
            if len(fill_latencies) > 1:
                logger.info(f"  StdDev: {stdev(fill_latencies):.2f}ms")
        else:
            logger.warning("No fill latencies recorded")
        
        logger.info("\n" + "=" * 70)
        logger.info(f"Successful iterations: {len(fill_latencies)}/{iterations}")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}", exc_info=True)
    
    finally:
        await client.close()
        logger.info("Connection closed")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark order fill latency via WebSocket"
    )
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=5,
        help="Number of benchmark iterations (default: 5)",
    )
    parser.add_argument(
        "--side", "-s",
        choices=["buy", "sell"],
        default="buy",
        help="Order side (default: buy)",
    )
    parser.add_argument(
        "--symbol",
        default="SOL/USDT:USDT",
        help="Trading symbol (default: SOL/USDT:USDT)",
    )
    parser.add_argument(
        "--size",
        type=float,
        default=5.0,
        help="Order size in USDT (default: 5.0)",
    )
    args = parser.parse_args()
    
    asyncio.run(
        benchmark_order_fills(
            iterations=args.iterations,
            side=args.side,
            symbol=args.symbol,
            order_size_usdt=args.size,
        )
    )


if __name__ == "__main__":
    main()
