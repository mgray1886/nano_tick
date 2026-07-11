import asyncio
import json
import logging
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

# Estimated Python object overhead per buffered entry (tuple + int + bytes
# headers), counted against the cap so nominal size reflects real RAM.
# Conservative for 64-bit Pi OS; 32-bit is smaller, which over-reserves safely.
ENTRY_OVERHEAD_BYTES = 120

STATS_INTERVAL_SECONDS = 60.0


class TcpSink:
    """Forwards normalised messages to a downstream TCP consumer (the rpi4B).

    Wire protocol (NDJSON both directions):
      - sink -> consumer: each tick dict with a "seq" field added,
        monotonically increasing from 1 per process lifetime.
      - consumer -> sink: {"ack": N} meaning "received everything up to and
        including seq N" (cumulative; recommended every ~100 msgs or 1s).

    Messages are buffered as encoded bytes until acked, so delivery survives
    connection drops: TCP write()/drain() succeeding proves nothing about
    receipt, only an ack does. On reconnect all unacked messages are resent,
    so the consumer may see duplicates and must dedupe on seq (or trade_id).

    Memory is hard-capped: total buffered bytes never exceed
    max_buffer_bytes (~10+ min of peak btcusdt trade flow at the 16MB
    default). Only at the cap are oldest messages dropped, and loudly.
    """

    def __init__(
        self,
        host: str,
        port: int,
        reconnect_delay: float = 2.0,
        max_buffer_bytes: int = 16 * 1024 * 1024,
    ):
        self.host = host
        self.port = port
        self.reconnect_delay = reconnect_delay
        self.max_buffer_bytes = max_buffer_bytes
        # Entries are (seq, encoded_line). _unsent -> written -> _unacked -> acked (gone).
        self._unsent: deque = deque()
        self._unacked: deque = deque()
        self._buffered_bytes = 0
        self._next_seq = 1
        self._has_data = asyncio.Event()
        self._run_task: Optional[asyncio.Task] = None
        self._stats_task: Optional[asyncio.Task] = None
        self._dropped = 0

    def start(self) -> None:
        self._run_task = asyncio.create_task(self._run())
        self._stats_task = asyncio.create_task(self._stats_loop())

    async def stop(self) -> None:
        for task in (self._run_task, self._stats_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        undelivered = len(self._unsent) + len(self._unacked)
        if undelivered:
            logger.warning("shutting down with %d unacked ticks in buffer", undelivered)

    def send(self, message: dict) -> None:
        line = json.dumps({"seq": self._next_seq, **message}).encode() + b"\n"
        self._unsent.append((self._next_seq, line))
        self._next_seq += 1
        self._buffered_bytes += len(line) + ENTRY_OVERHEAD_BYTES
        while self._buffered_bytes > self.max_buffer_bytes:
            evicted = self._unacked.popleft() if self._unacked else self._unsent.popleft()
            self._buffered_bytes -= len(evicted[1]) + ENTRY_OVERHEAD_BYTES
            self._dropped += 1
            if self._dropped % 1000 == 1:
                logger.warning(
                    "sink buffer full (%d bytes), dropped %d oldest ticks so far",
                    self.max_buffer_bytes,
                    self._dropped,
                )
        self._has_data.set()

    async def _run(self) -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
            except OSError as exc:
                logger.warning("sink connect failed (%s); retrying in %.0fs", exc, self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)
                continue

            # Anything sent on a previous connection but never acked goes
            # back to the front of the queue for resend.
            self._unsent.extendleft(reversed(self._unacked))
            self._unacked.clear()
            if self._unsent:
                self._has_data.set()
            logger.info(
                "connected to sink %s:%s (%d ticks queued for send)",
                self.host, self.port, len(self._unsent),
            )

            send_task = asyncio.create_task(self._send_loop(writer))
            ack_task = asyncio.create_task(self._ack_loop(reader))
            try:
                done, pending = await asyncio.wait(
                    {send_task, ack_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc
            except (OSError, ConnectionError) as exc:
                logger.warning("sink connection lost (%s); retrying in %.0fs", exc, self.reconnect_delay)
            finally:
                send_task.cancel()
                ack_task.cancel()
                await asyncio.gather(send_task, ack_task, return_exceptions=True)
                writer.close()
            await asyncio.sleep(self.reconnect_delay)

    async def _send_loop(self, writer: asyncio.StreamWriter) -> None:
        while True:
            if not self._unsent:
                self._has_data.clear()
                await self._has_data.wait()
                continue
            entry = self._unsent.popleft()
            self._unacked.append(entry)
            writer.write(entry[1])
            await writer.drain()

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(STATS_INTERVAL_SECONDS)
            buffered = len(self._unsent) + len(self._unacked)
            if buffered or self._dropped:
                logger.info(
                    "sink buffer: %d ticks (%d/%d bytes), %d dropped total",
                    buffered, self._buffered_bytes, self.max_buffer_bytes, self._dropped,
                )

    async def _ack_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            raw = await reader.readline()
            if not raw:
                raise ConnectionError("sink closed connection")
            try:
                ack = json.loads(raw)["ack"]
            except (json.JSONDecodeError, KeyError, TypeError):
                logger.warning("ignoring malformed ack from sink: %r", raw)
                continue
            while self._unacked and self._unacked[0][0] <= ack:
                _, line = self._unacked.popleft()
                self._buffered_bytes -= len(line) + ENTRY_OVERHEAD_BYTES
