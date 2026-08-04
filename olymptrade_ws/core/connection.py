# core/connection.py
import asyncio
import websockets
import logging
from typing import Optional, Callable, Awaitable
from olymptrade_ws.olympconfig import parameters

logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, uri: str, access_token: str, 
                 message_queue: asyncio.Queue, 
                 connection_lost_callback: Optional[Callable[[], Awaitable[None]]] = None):
        self.uri = uri
        self.access_token = access_token
        self.message_queue = message_queue
        self.connection_lost_callback = connection_lost_callback
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._is_connected = False
        self._connect_lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        if not self._is_connected or self.websocket is None:
            return False
        # websockets v10+: .closed; v13+: state / protocol.closed
        ws = self.websocket
        closed = getattr(ws, "closed", None)
        if closed is not None:
            return not closed
        state = getattr(ws, "state", None)
        if state is not None:
            name = getattr(state, "name", str(state))
            return name in ("OPEN", "CONNECTED")
        return True

    async def connect(self):
        async with self._connect_lock:
            if self.is_connected:
                logger.info("Already connected.")
                return

            headers = {
                "Origin": parameters.DEFAULT_ORIGIN,
                "User-Agent": parameters.DEFAULT_USER_AGENT,
                "Cookie": f"access_token={self.access_token}",
            }
            try:
                logger.info(f"Attempting to connect to {self.uri}...")
                # websockets>=14 usa additional_headers; versoes antigas: extra_headers
                connect_kwargs = {
                    "ping_interval": None,
                    "open_timeout": parameters.DEFAULT_CONNECT_TIMEOUT,
                }
                try:
                    import inspect

                    params = inspect.signature(websockets.connect).parameters
                except (TypeError, ValueError):
                    params = {}
                if "additional_headers" in params:
                    connect_kwargs["additional_headers"] = headers
                else:
                    connect_kwargs["extra_headers"] = headers
                if "origin" in params:
                    connect_kwargs["origin"] = parameters.DEFAULT_ORIGIN

                self.websocket = await websockets.connect(self.uri, **connect_kwargs)
                self._is_connected = True
                logger.info("✅ WebSocket connection established.")
                # Start the receiver loop
                if self._receive_task is None or self._receive_task.done():
                     self._receive_task = asyncio.create_task(self._receiver())
                else:
                    logger.warning("Receive task already running.")

            except Exception as e:
                self._is_connected = False
                self.websocket = None
                name = e.__class__.__name__
                if name in ("InvalidStatusCode", "InvalidStatus"):
                    code = getattr(e, "status_code", None) or getattr(e, "response", None)
                    logger.error(
                        f"❌ Connection failed: Invalid status {code}. Check access_token."
                    )
                    raise ConnectionError(f"Invalid status {code}") from e
                logger.error(f"❌ Connection failed: {e}")
                raise ConnectionError(f"Unexpected connection error: {e}") from e


    async def disconnect(self):
        async with self._connect_lock:
            self._is_connected = False # Signal disconnect intention
            if self._receive_task and not self._receive_task.done():
                self._receive_task.cancel()
                try:
                    await self._receive_task
                except asyncio.CancelledError:
                    logger.info("Receiver task cancelled.")
                except Exception as e:
                    logger.error(f"Error during receiver task cancellation: {e}")
                self._receive_task = None

            if self.websocket is not None:
                try:
                    closed = getattr(self.websocket, "closed", False)
                    if not closed:
                        await self.websocket.close()
                    logger.info("🔌 WebSocket connection closed.")
                except Exception as e:
                    logger.error(f"Error closing WebSocket: {e}")
            self.websocket = None


    async def send(self, message: str):
        #logger.debug(f"📤 Sending raw: {message}")  # For debugging, can be removed later
        if not self.is_connected:
            logger.error("⚠️ Cannot send: WebSocket not connected.")
            raise ConnectionError("WebSocket not connected.")
        
        try:
            await self.websocket.send(message)
            # Avoid logging sensitive data directly here, let client handle logging
            # logger.debug(f"📤 Sent raw: {message}") 
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"❌ Send failed: Connection closed unexpectedly. {e}")
            await self._handle_connection_loss()
            raise ConnectionError("Connection closed") from e
        except Exception as e:
            logger.error(f"❌ Send failed: {e}")
            raise

    async def _receiver(self):
        logger.info("Receiver task started.")
        while self._is_connected:
            try:
            
                if not self.is_connected: # Check connection state before receiving
                    logger.warning("Receiver loop: Connection lost, stopping.")
                    break
                message = await self.websocket.recv()
                #logger.debug(f"📥 Received raw: {message}")
                #print(f"📥 Received raw: {message}")  # For debugging, can be removed later
                # logger.debug(f"📥 Received raw: {message}")
                await self.message_queue.put(message)
            except asyncio.CancelledError:
                logger.info("Receiver task cancelled.")
                break # Exit loop cleanly on cancellation
            except websockets.exceptions.ConnectionClosedOK:
                logger.info("Receiver loop: Connection closed gracefully by server.")
                await self._handle_connection_loss()
                break
            except websockets.exceptions.ConnectionClosedError as e:
                logger.error(f"Receiver loop: Connection closed with error: {e}")
                await self._handle_connection_loss()
                break
            except websockets.exceptions.ConnectionClosed as e:
                 logger.error(f"Receiver loop: Connection closed unexpectedly: {e}")
                 await self._handle_connection_loss()
                 break
            except Exception as e:
                logger.error(f"❗ Unexpected error in receiver loop: {e}")
                # Decide if we should break or continue after an unexpected error
                await asyncio.sleep(1) # Avoid tight loop on persistent error

        logger.info("Receiver task finished.")


    async def _handle_connection_loss(self):
        """Internal method to handle cleanup and notify client on connection loss."""
        was_connected = self._is_connected
        self._is_connected = False
        self.websocket = None # Ensure websocket is None
        if self._receive_task and not self._receive_task.done():
             self._receive_task.cancel() # Ensure receiver stops
        
        if was_connected and self.connection_lost_callback:
            logger.info("Notifying client about connection loss.")
            asyncio.create_task(self.connection_lost_callback())
        else:
             logger.info("Connection lost, no callback registered or was already disconnected.")
