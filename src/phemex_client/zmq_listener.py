# src/phemex_client/zmq_listener.py
# ZeroMQ subscriber for receiving trading signals
# Connects to publisher, validates messages, and routes to signal processor
# RELEVANT FILES: signal_processor.py, models.py, config.py

"""
ZMQ listener for trading signals.

Features:
- Subscribe to ZMQ publisher with topic filtering
- Parse and validate JSON messages
- Route valid signals to signal processor
- Graceful shutdown handling
"""

import asyncio
import json
import logging

import zmq
import zmq.asyncio


logger = logging.getLogger(__name__)


class ZmqListener:
    """
    ZMQ subscriber for trading signals.
    
    Connects to a publisher and routes messages to the signal processor.
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
        host: str = "127.0.0.1",
        port: int = 5555,
        topic: str = "orders",
    ):
        """
        Initialize ZMQ listener.
        
        Args:
            signal_processor: SignalProcessor instance
            host: ZMQ publisher host
            port: ZMQ publisher port
            topic: ZMQ topic to subscribe to
        """
        self.processor = signal_processor
        self.host = host
        self.port = port
        self.topic = topic
        self._running = False
        self._context = None
        self._socket = None
    
    async def start(self) -> None:
        """
        Start listening for ZMQ messages.
        
        Runs until stop() is called or cancelled.
        """
        self._context = zmq.asyncio.Context()
        self._socket = self._context.socket(zmq.SUB)
        
        address = f"tcp://{self.host}:{self.port}"
        self._socket.connect(address)
        self._socket.subscribe(self.topic.encode())
        
        logger.info(
            f"Listening for ZMQ messages on {address} (topic: {self.topic})"
        )
        self._running = True
        
        try:
            while self._running:
                await self._receive_and_process()
                
        except asyncio.CancelledError:
            logger.info("ZMQ listener cancelled")
            
        finally:
            self._cleanup()
    
    async def _receive_and_process(self) -> None:
        """
        Receive and process a single message.
        
        Handles multipart messages in format: [topic, payload]
        """
        try:
            # Use timeout to allow periodic running check
            if self._socket.poll(1000, zmq.POLLIN):
                msg = await self._socket.recv_multipart()
                await self._handle_message(msg)
                
        except zmq.ZMQError as e:
            logger.error(f"ZMQ error: {e}")
            await asyncio.sleep(0.1)
    
    async def _handle_message(self, msg: list[bytes]) -> None:
        """
        Handle a received multipart message.
        
        Args:
            msg: Multipart message [topic, payload, ...]
        """
        if len(msg) < 2:
            logger.warning(f"Invalid message format: expected 2+ parts, got {len(msg)}")
            return
        
        topic_str = msg[0].decode()
        payload_str = msg[1].decode()
        
        try:
            message = json.loads(payload_str)
            action = message.get("action", "ENTRY")
            
            logger.info(f"Received {action} signal on topic '{topic_str}'")
            logger.debug(f"Message payload: {payload_str[:200]}")
            
            # Validate message
            if not self._validate_message(message):
                logger.warning(f"Invalid message: {payload_str[:100]}")
                return
            
            # Route to signal processor
            await self.processor.process_signal(message)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}, payload: {payload_str[:100]}")
    
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
    
    def _cleanup(self) -> None:
        """Clean up ZMQ resources."""
        if self._socket:
            self._socket.close()
            self._socket = None
        
        if self._context:
            self._context.term()
            self._context = None
        
        logger.debug("ZMQ listener cleaned up")
    
    def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        logger.info("ZMQ listener stopping...")
