# Phemex ZMQ Order Listener

A Python application that listens for trading signals via ZeroMQ (ZMQ) and executes trades on Phemex Futures using the CCXT Pro library.

## Features

- **ZMQ Signal Listener**: Receives `ENTRY`, `EXIT`, and `PARTIAL_EXIT` signals
- **Limit Post-Only Orders**: All entries and exits use post-only limit orders (maker fees)
- **Market Stop-Loss**: Uses market orders for stop-loss to guarantee fills
- **Real-time Position Tracking**: WebSocket-based position updates
- **Read-Only Protection**: Preloaded positions are protected from accidental closing
- **R-Based Position Sizing**: Calculate position sizes based on risk units

## Installation

```bash
cd phemex-client
poetry install
```

## Configuration

Copy the example config and fill in your values:

```bash
cp config.example.yml config.yml
```

**config.yml:**
```yaml
# Phemex API credentials
phemex:
  api_key: "your_api_key_here"
  secret: "your_api_secret_here"
  testnet: false

# ZeroMQ settings
zmq:
  host: "127.0.0.1"
  port: 5555
  topic: "orders"

# Position sizing
position_sizing:
  deposit_size: 1000.0    # Total deposit in USDT
  r_percentage: 0.01      # Risk per R unit (1% = 0.01)

# Trading settings
trading:
  symbols:
    - "BTC/USDT:USDT"
    - "SOL/USDT:USDT"
  leverage: 20
```

## Usage

### Start the Listener

```bash
poetry run python examples/run_listener.py
```

Or with a custom config path:

```bash
poetry run python examples/run_listener.py --config /path/to/config.yml
```

### Send Test Signals

```bash
# ENTRY signal
poetry run python examples/send_test_signal.py \
    --direction LONG \
    --symbol BTC/USDT:USDT \
    --price 43000 \
    --stop-loss 42500 \
    --position-size-r 10

# EXIT signal
poetry run python examples/send_test_signal.py \
    --action EXIT \
    --direction LONG \
    --symbol BTC/USDT:USDT \
    --price 44000

# PARTIAL_EXIT signal
poetry run python examples/send_test_signal.py \
    --action PARTIAL_EXIT \
    --direction LONG \
    --symbol BTC/USDT:USDT \
    --price 43500 \
    --exit-pct 0.33 \
    --move-sl-to-be
```

## ZMQ Message Protocol

All messages are JSON objects published to the configured topic.

### ENTRY Signal

```json
{
  "action": "ENTRY",
  "direction": "LONG",
  "symbol": "BTC/USDT:USDT",
  "price": 43500.50,
  "timestamp": "2024-01-15T10:30:00Z",
  "stop_loss": 43000.00,
  "take_profit": 44500.00,
  "position_size_r": 50.0
}
```

### EXIT Signal

```json
{
  "action": "EXIT",
  "direction": "LONG",
  "symbol": "BTC/USDT:USDT",
  "price": 44200.00,
  "timestamp": "2024-01-15T12:00:00Z",
  "reason": "Signal"
}
```

### PARTIAL_EXIT Signal

```json
{
  "action": "PARTIAL_EXIT",
  "direction": "LONG",
  "symbol": "BTC/USDT:USDT",
  "price": 44000.00,
  "timestamp": "2024-01-15T11:30:00Z",
  "exit_pct": 0.33,
  "move_sl_to_be": true
}
```

## Project Structure

```
phemex-client/
├── config.example.yml        # Configuration template
├── pyproject.toml            # Dependencies
├── README.md
│
├── src/phemex_client/
│   ├── config.py             # Configuration loader
│   ├── models.py             # Data models
│   ├── exchange_client.py    # CCXT Pro Phemex wrapper
│   ├── position_manager.py   # Position tracking
│   ├── signal_processor.py   # Signal handling
│   └── zmq_listener.py       # ZMQ subscriber
│
├── examples/
│   ├── run_listener.py       # Main entry point
│   └── send_test_signal.py   # Test publisher
│
└── tests/
    └── ...
```

## Running Tests

```bash
poetry run pytest tests/ -v
```

## License

MIT
