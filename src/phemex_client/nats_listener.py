# src/phemex_client/nats_listener.py
# NATS subscriber for receiving trading signals
# Connects to NATS server, validates messages, and routes to signal processor
# RELEVANT FILES: signal_processor.py, models.py, config.py

"""
NATS listener for trading signals.

Features:
- Subscribe to NATS subject with async message handling
- Parse and validate JSON messages
- Route valid signals to signal processor
- Graceful shutdown handling
"""

import asyncio
import json
import logging

from nats.aio.client import Client as NATS


logger = logging.getLogger(__name__)


class NatsListener:
    """
    NATS subscriber for trading signals.
    
    Connects to a NATS server and routes messages to the signal processor.
    """
    
    # Required fields for all messages
    REQUIRED_FIELDS = ["direction", "symbol", "price", "timestamp"]
    
    # Valid directions
    VALID_DIRECTIONS = ["LONG", "SHORT"]
    
    # Valid actions
    VALID_ACTIONS = ["ENTRY", "EXIT", "PARTIAL_EXIT"]
    
    def __init__(
        self,
        signal_processor,
        url: str = "nats://localhost:4222",
        subject: str = "orders",
    ):
        """
        Initialize NATS listener.
        
        Args:
            signal_processor: SignalProcessor instance
            url: NATS server URL (e.g., nats://localhost:4222)
            subject: NATS subject to subscribe to
        """
        self.processor = signal_processor
        self.url = url
        self.subject = subject
        self._running = False
        self._nc = NATS()
        self._sub = None
    
    async def start(self) -> None:
        """
        Start listening for NATS messages.
        
        Runs until stop() is called or cancelled.
        """
        await self._nc.connect(self.url)
        logger.info(f"Connected to NATS at {self.url}")
        
        # Subscribe to subject with callback
        self._sub = await self._nc.subscribe(self.subject, cb=self._handle_message)
        
        logger.info(
            f"Listening for NATS messages on subject '{self.subject}'"
        )
        self._running = True
        
        try:
            # Keep running until stopped
            while self._running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("NATS listener cancelled")
            
        finally:
            await self._cleanup()
    
    async def _handle_message(self, msg) -> None:
        """
        Handle a received NATS message.
        
        Args:
            msg: NATS message object with 'data' attribute
        """
        try:
            payload_str = msg.data.decode()
            message = json.loads(payload_str)
            action = message.get("action", "ENTRY")
            
            logger.info(f"Received {action} signal on subject '{msg.subject}'")
            logger.debug(f"Message payload: {payload_str[:200]}")
            
            # Validate message
            if not self._validate_message(message):
                logger.warning(f"Invalid message: {payload_str[:100]}")
                return
            
            # Route to signal processor
            await self.processor.process_signal(message)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}, payload: {msg.data.decode()[:100]}")
    
    def _validate_message(self, message: dict) -> bool:
        """
        Validate required message fields.
        
        Args:
            message: Parsed message dictionary
        
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in message:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Validate direction
        direction = message.get("direction", "").upper()
        if direction not in self.VALID_DIRECTIONS:
            logger.error(f"Invalid direction: {direction}")
            return False
        
        # Validate action if provided
        action = message.get("action", "ENTRY").upper()
        if action not in self.VALID_ACTIONS:
            logger.error(f"Invalid action: {action}")
            return False
        
        # Validate price is positive
        try:
            price = float(message.get("price", 0))
            if price <= 0:
                logger.error(f"Invalid price: {price}")
                return False
        except (TypeError, ValueError):
            logger.error(f"Invalid price format: {message.get('price')}")
            return False
        
        return True
    
    async def _cleanup(self) -> None:
        """Clean up NATS resources."""
        if self._sub:
            await self._sub.unsubscribe()
            self._sub = None
        
        if self._nc.is_connected:
            await self._nc.drain()
            await self._nc.close()
        
        logger.debug("NATS listener cleaned up")
    
    def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        logger.info("NATS listener stopping...")
