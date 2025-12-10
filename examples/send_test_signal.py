#!/usr/bin/env python3
# examples/send_test_signal.py
# Test signal publisher for development and testing
# Sends ZMQ signals to test the order listener
# RELEVANT FILES: run_listener.py, zmq_listener.py, signal_processor.py

"""
Test Signal Publisher

Sends test trading signals to the ZMQ listener for development and testing.

Usage:
    # Send ENTRY signal
    poetry run python examples/send_test_signal.py \
        --direction LONG \
        --symbol BTC/USDT:USDT \
        --price 43000 \
        --stop-loss 42500
    
    # Send EXIT signal
    poetry run python examples/send_test_signal.py \
        --action EXIT \
        --direction LONG \
        --symbol BTC/USDT:USDT \
        --price 44000
    
    # Send PARTIAL_EXIT signal
    poetry run python examples/send_test_signal.py \
        --action PARTIAL_EXIT \
        --direction LONG \
        --symbol BTC/USDT:USDT \
        --price 43500 \
        --exit-pct 0.33 \
        --move-sl-to-be
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import zmq
import yaml


def load_zmq_config():
    """Load ZMQ settings from config.yml if available."""
    config_path = Path.cwd() / "config.yml"
    
    if config_path.exists():
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        zmq_data = data.get("zmq", {})
        return {
            "host": zmq_data.get("host", "127.0.0.1"),
            "port": zmq_data.get("port", 5555),
            "topic": zmq_data.get("topic", "orders"),
        }
    
    # Defaults if no config
    return {"host": "127.0.0.1", "port": 5555, "topic": "orders"}


def parse_args():
    """Parse command line arguments."""
    zmq_defaults = load_zmq_config()
    
    parser = argparse.ArgumentParser(
        description="Send test trading signals via ZMQ"
    )
    
    # Signal type
    parser.add_argument(
        "--action",
        choices=["ENTRY", "EXIT", "PARTIAL_EXIT"],
        default="ENTRY",
        help="Signal action type (default: ENTRY)"
    )
    
    # Required fields
    parser.add_argument(
        "--direction",
        choices=["LONG", "SHORT"],
        required=True,
        help="Trade direction"
    )
    
    parser.add_argument(
        "--symbol",
        required=True,
        help="Trading symbol (e.g., BTC/USDT:USDT)"
    )
    
    parser.add_argument(
        "--price",
        type=float,
        required=True,
        help="Reference price"
    )
    
    # Entry-specific
    parser.add_argument(
        "--stop-loss",
        type=float,
        help="Stop-loss trigger price"
    )
    
    parser.add_argument(
        "--take-profit",
        type=float,
        help="Take-profit price"
    )
    
    parser.add_argument(
        "--position-size-r",
        type=float,
        default=10.0,
        help="Position size in R units (default: 10.0)"
    )
    
    parser.add_argument(
        "--confidence",
        type=float,
        default=0.85,
        help="Model confidence 0.0-1.0 (default: 0.85)"
    )
    
    # Exit-specific
    parser.add_argument(
        "--reason",
        default="Signal",
        help="Exit reason (default: Signal)"
    )
    
    # Partial exit specific
    parser.add_argument(
        "--exit-pct",
        type=float,
        default=0.5,
        help="Fraction to exit for PARTIAL_EXIT (default: 0.5)"
    )
    
    parser.add_argument(
        "--tp-level",
        type=int,
        default=1,
        help="TP level hit for PARTIAL_EXIT (default: 1)"
    )
    
    parser.add_argument(
        "--move-sl-to-be",
        action="store_true",
        help="Move stop-loss to break-even after partial exit"
    )
    
    # ZMQ settings (defaults from config.yml)
    parser.add_argument(
        "--host",
        default=zmq_defaults["host"],
        help=f"ZMQ host (default: {zmq_defaults['host']})"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=zmq_defaults["port"],
        help=f"ZMQ port (default: {zmq_defaults['port']})"
    )
    
    parser.add_argument(
        "--topic",
        default=zmq_defaults["topic"],
        help=f"ZMQ topic (default: {zmq_defaults['topic']})"
    )
    
    return parser.parse_args()


def build_message(args) -> dict:
    """Build message dictionary from arguments."""
    # Common fields
    message = {
        "action": args.action,
        "direction": args.direction,
        "symbol": args.symbol,
        "price": args.price,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    # Action-specific fields
    if args.action == "ENTRY":
        message["confidence"] = args.confidence
        message["position_size_r"] = args.position_size_r
        
        if args.stop_loss:
            message["stop_loss"] = args.stop_loss
        
        if args.take_profit:
            message["take_profit"] = args.take_profit
    
    elif args.action == "EXIT":
        message["reason"] = args.reason
    
    elif args.action == "PARTIAL_EXIT":
        message["exit_pct"] = args.exit_pct
        message["remaining_pct"] = 1.0 - args.exit_pct
        message["tp_level"] = args.tp_level
        message["move_sl_to_be"] = args.move_sl_to_be
    
    return message


def main():
    """Main entry point."""
    args = parse_args()
    
    # Create ZMQ publisher
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    
    address = f"tcp://{args.host}:{args.port}"
    socket.bind(address)
    
    print(f"Publisher bound to {address}")
    print(f"Topic: {args.topic}")
    
    # Build message
    message = build_message(args)
    
    # Brief delay to allow subscriber to connect
    # (ZMQ PUB/SUB has slow joiner problem)
    print("Waiting 1s for subscribers...")
    time.sleep(1)
    
    # Send message
    payload = json.dumps(message)
    socket.send_multipart([
        args.topic.encode(),
        payload.encode(),
    ])
    
    print(f"\nSent {args.action} signal:")
    print(json.dumps(message, indent=2))
    
    # Brief delay before cleanup
    time.sleep(0.1)
    
    # Cleanup
    socket.close()
    context.term()
    
    print("\nDone!")


if __name__ == "__main__":
    main()
