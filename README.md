# Phemex Client

Python trading client for Phemex Futures with ZMQ signal processing and chase order execution.

## Features

- **Chase Order Manager** — Post-only limit orders that follow bid/ask prices until filled
- **ZMQ Signal Listener** — Process ENTRY, EXIT, PARTIAL_EXIT signals via ZeroMQ
- **Real-time WebSocket** — Order book streaming and order fill detection
- **R-Based Sizing** — Calculate position sizes from risk units

## Installation

```bash
poetry install
```

## Configuration

```bash
cp config.example.yml config.yml
```

```yaml
phemex:
  api_key: "your_api_key"
  secret: "your_secret"
  testnet: false

zmq:
  host: "127.0.0.1"
  port: 5555
  topic: "orders"

position_sizing:
  deposit_size: 1000.0
  r_percentage: 0.01

trading:
  symbols: ["SOL/USDT:USDT"]
  leverage: 20
```

## Usage

### Chase Order (Low-Latency Fill)

Places limit orders that chase bid2/ask2 until filled:

```bash
poetry run python examples/run_chase_order.py
```

### ZMQ Signal Listener

```bash
poetry run python examples/run_listener.py
```

### Watch Order Fills

Monitor new orders and fills in real-time:

```bash
poetry run python examples/watch_order_fills.py
```

### Account Status

```bash
poetry run python examples/log_account_and_price.py
```

## ZMQ Message Protocol

All messages are JSON published to the configured topic. Required fields: `direction`, `symbol`, `price`, `timestamp`.

### ENTRY — Open a Position

```json
{
  "action": "ENTRY",
  "direction": "LONG",
  "symbol": "SOL/USDT:USDT",
  "price": 140.50,
  "timestamp": "2024-01-15T10:30:00Z",
  "stop_loss": 138.00,
  "take_profit": 145.00,
  "position_size_r": 10.0,
  "confidence": 0.85,
  "multi_tp_enabled": false,
  "tp_levels": []
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string | No | `"ENTRY"` (default) |
| `direction` | string | Yes | `"LONG"` or `"SHORT"` |
| `symbol` | string | Yes | Trading pair (e.g., `"SOL/USDT:USDT"`) |
| `price` | float | Yes | Entry price |
| `timestamp` | string | Yes | ISO 8601 format |
| `stop_loss` | float | No | Stop-loss price |
| `take_profit` | float | No | Take-profit price |
| `position_size_r` | float | No | Position size in R units (default: 1.0) |
| `confidence` | float | No | Signal confidence 0-1 |
| `multi_tp_enabled` | bool | No | Enable multiple TP levels |
| `tp_levels` | array | No | List of `{price, exit_pct, ratio}` |

### EXIT — Close Full Position

```json
{
  "action": "EXIT",
  "direction": "LONG",
  "symbol": "SOL/USDT:USDT",
  "price": 145.00,
  "timestamp": "2024-01-15T12:00:00Z",
  "reason": "TP hit"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string | Yes | `"EXIT"` |
| `direction` | string | Yes | Position direction to close |
| `reason` | string | No | Exit reason for logging |

### PARTIAL_EXIT — Close Partial Position

```json
{
  "action": "PARTIAL_EXIT",
  "direction": "LONG",
  "symbol": "SOL/USDT:USDT",
  "price": 142.00,
  "timestamp": "2024-01-15T11:30:00Z",
  "exit_pct": 0.33,
  "remaining_pct": 0.67,
  "tp_level": 1,
  "move_sl_to_be": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string | Yes | `"PARTIAL_EXIT"` |
| `exit_pct` | float | No | Percentage to exit (0-1) |
| `remaining_pct` | float | No | Remaining after exit |
| `tp_level` | int | No | TP level number (1, 2, 3...) |
| `move_sl_to_be` | bool | No | Move stop-loss to breakeven |

## Project Structure

```
src/phemex_client/
├── exchange_client.py      # CCXT Pro wrapper
├── chase_order_manager.py  # Chase limit order execution
├── signal_processor.py     # ZMQ signal handling
├── position_manager.py     # Position tracking
├── zmq_listener.py         # ZMQ subscriber
├── models.py               # Data models
└── config.py               # Configuration

examples/
├── run_chase_order.py      # Chase order demo
├── run_listener.py         # ZMQ listener
├── watch_order_fills.py    # Order fill monitor
├── log_account_and_price.py
└── send_test_signal.py
```

## Tests

```bash
poetry run pytest tests/ -v
```

## License

MIT
