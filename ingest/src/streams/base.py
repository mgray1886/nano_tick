import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import AsyncIterator

import websockets

logger = logging.getLogger(__name__)


class WebsocketStream(ABC):
    """Base class for exchange websocket streams.

    Handles reconnect/backoff so subclasses only need to describe the
    venue-specific connection (URL, optional subscribe handshake).
    """

    initial_backoff = 1.0
    max_backoff = 60.0

    @abstractmethod
    def url(self) -> str:
        ...

    async def subscribe(self, _ws: websockets.WebSocketClientProtocol) -> None:
        """Override to send a subscribe message after connecting."""
        return None

    async def messages(self) -> AsyncIterator[dict]:
        backoff = self.initial_backoff
        while True:
            try:
                async with websockets.connect(self.url()) as ws:
                    await self.subscribe(ws)
                    backoff = self.initial_backoff
                    async for raw in ws:
                        try:
                            yield json.loads(raw)
                        except json.JSONDecodeError as exc:
                            logger.warning("dropping unparseable message from %s: %s", self.url(), exc)
            except (websockets.exceptions.WebSocketException, OSError) as exc:
                logger.warning("stream %s disconnected (%s); reconnecting in %.0fs", self.url(), exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
