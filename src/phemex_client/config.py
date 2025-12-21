# src/phemex_client/config.py
# Configuration loader for the single config.yml file
# Loads all settings: API credentials, NATS, position sizing, trading
# RELEVANT FILES: __init__.py, models.py, exchange_client.py

"""
Configuration module for Phemex NATS Order Listener.

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
class NatsConfig:
    """NATS connection settings."""
    url: str = "nats://localhost:4222"
    subject: str = "orders"


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
class ExecutionConfig:
    """Order execution settings."""
    use_chase_orders: bool = False
    chase_mode: str = "bid2"
    max_chase_retries: int = 50


@dataclass
class Config:
    """
    Main configuration container.
    
    Holds all settings from config.yml.
    """
    phemex: PhemexConfig = field(default_factory=PhemexConfig)
    nats: NatsConfig = field(default_factory=NatsConfig)
    position_sizing: PositionSizingConfig = field(default_factory=PositionSizingConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)


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
    
    # Parse nats section
    nats_data = data.get("nats", {})
    nats = NatsConfig(
        url=nats_data.get("url", "nats://localhost:4222"),
        subject=nats_data.get("subject", "orders"),
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
    
    # Parse execution section
    exec_data = data.get("execution", {})
    execution = ExecutionConfig(
        use_chase_orders=exec_data.get("use_chase_orders", False),
        chase_mode=exec_data.get("chase_mode", "bid2"),
        max_chase_retries=exec_data.get("max_chase_retries", 50),
    )
    
    return Config(
        phemex=phemex,
        nats=nats,
        position_sizing=position_sizing,
        trading=trading,
        execution=execution,
    )
