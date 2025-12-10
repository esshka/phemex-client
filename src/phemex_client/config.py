# src/phemex_client/config.py
# Configuration loader for the single config.yml file
# Loads all settings: API credentials, ZMQ, position sizing, trading
# RELEVANT FILES: __init__.py, models.py, exchange_client.py

"""
Configuration module for Phemex ZMQ Order Listener.

Loads all settings from a single config.yml file.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class PhemexConfig:
    """Phemex API credentials."""
    api_key: str = ""
    secret: str = ""
    testnet: bool = False


@dataclass
class ZmqConfig:
    """ZeroMQ connection settings."""
    host: str = "127.0.0.1"
    port: int = 5555
    topic: str = "orders"
    
    @property
    def url(self) -> str:
        """Get full ZMQ URL."""
        return f"tcp://{self.host}:{self.port}"


@dataclass
class PositionSizingConfig:
    """Position sizing parameters."""
    deposit_size: float = 1000.0
    r_percentage: float = 0.01
    
    @property
    def r_value(self) -> float:
        """Calculate R value in USDT."""
        return self.deposit_size * self.r_percentage


@dataclass
class TradingConfig:
    """Trading settings."""
    symbols: list[str] = field(default_factory=lambda: ["BTC/USDT:USDT"])
    leverage: int = 20


@dataclass
class Config:
    """
    Main configuration container.
    
    Holds all settings from config.yml.
    """
    phemex: PhemexConfig = field(default_factory=PhemexConfig)
    zmq: ZmqConfig = field(default_factory=ZmqConfig)
    position_sizing: PositionSizingConfig = field(default_factory=PositionSizingConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config.yml. Defaults to config.yml in cwd.
    
    Returns:
        Config object with all settings loaded.
    
    Raises:
        FileNotFoundError: If config file doesn't exist.
    """
    if config_path is None:
        config_path = Path.cwd() / "config.yml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Copy config.example.yml to config.yml and fill in your values."
        )
    
    with open(config_path, "r") as f:
        data = yaml.safe_load(f) or {}
    
    # Parse phemex section
    phemex_data = data.get("phemex", {})
    phemex = PhemexConfig(
        api_key=phemex_data.get("api_key", ""),
        secret=phemex_data.get("secret", ""),
        testnet=phemex_data.get("testnet", False),
    )
    
    # Parse zmq section
    zmq_data = data.get("zmq", {})
    zmq = ZmqConfig(
        host=zmq_data.get("host", "127.0.0.1"),
        port=zmq_data.get("port", 5555),
        topic=zmq_data.get("topic", "orders"),
    )
    
    # Parse position_sizing section
    ps_data = data.get("position_sizing", {})
    position_sizing = PositionSizingConfig(
        deposit_size=ps_data.get("deposit_size", 1000.0),
        r_percentage=ps_data.get("r_percentage", 0.01),
    )
    
    # Parse trading section
    trading_data = data.get("trading", {})
    trading = TradingConfig(
        symbols=trading_data.get("symbols", ["BTC/USDT:USDT"]),
        leverage=trading_data.get("leverage", 20),
    )
    
    return Config(
        phemex=phemex,
        zmq=zmq,
        position_sizing=position_sizing,
        trading=trading,
    )
