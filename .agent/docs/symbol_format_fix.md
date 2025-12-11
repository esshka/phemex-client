# Symbol Format Fix

## Problem

The system was failing to match positions when processing ZMQ signals because of a symbol format mismatch:

- **ZMQ Messages**: Use underscore format like `SOL_USDT`, `BTC_USDT`
- **Phemex API**: Returns slash format with perpetual suffix like `SOL/USDT:USDT`, `BTC/USDT:USDT`

This caused the signal processor to not find existing positions, leading to warnings like:
```
EXIT signal but no position for SOL_USDT
```

Even though a position for `SOL/USDT:USDT` actually existed.

## Solution

Added a `normalize_symbol_for_phemex()` function in `signal_processor.py` that:

1. Converts underscore to slash separator
2. Adds `:USDT` suffix for perpetual futures
3. Returns already-normalized symbols unchanged

The function is now called in all signal handlers:
- `_handle_entry()`
- `_handle_exit()`
- `_handle_partial_exit()`

This ensures all symbol lookups and order placements use the correct Phemex format.

## Testing

Added comprehensive tests in `test_signal_processor.py`:

- `TestSymbolNormalization`: Tests the conversion function
- `test_entry_with_zmq_symbol_format`: Integration test verifying end-to-end conversion

All 17 tests pass successfully.

## Example

Before:
```python
symbol = message["symbol"]  # "SOL_USDT"
position = self.positions.get_position(symbol)  # Returns None (not found)
```

After:
```python
symbol = normalize_symbol_for_phemex(message["symbol"])  # "SOL/USDT:USDT"
position = self.positions.get_position(symbol)  # Correctly finds the position
```
