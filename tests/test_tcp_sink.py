import asyncio
import json
import time

from src.sinks.tcp import ENTRY_OVERHEAD_BYTES, TcpSink


class FakeWriter:
    def __init__(self):
        self.lines = []
        self.closed = False
        self.fail = False

    def write(self, data: bytes) -> None:
        if self.fail:
            raise OSError("broken pipe")
        self.lines.append(data)

    async def drain(self) -> None:
        if self.fail:
            raise OSError("broken pipe")

    def close(self) -> None:
        self.closed = True


class FakeReader:
    """readline() blocks on a queue the test feeds; b'' simulates EOF."""

    def __init__(self):
        self.queue = asyncio.Queue()

    async def readline(self) -> bytes:
        return await self.queue.get()

    def feed_ack(self, seq: int) -> None:
        self.queue.put_nowait(json.dumps({"ack": seq}).encode() + b"\n")

    def feed_eof(self) -> None:
        self.queue.put_nowait(b"")


def install_connections(monkeypatch, pairs):
    """Replace asyncio.open_connection with a scripted sequence of
    (reader, writer) pairs; raises OSError when the script is exhausted."""
    iterator = iter(pairs)

    async def fake_open_connection(host, port):
        try:
            return next(iterator)
        except StopIteration:
            raise OSError("no more scripted connections")

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)


async def wait_for(condition, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out waiting for condition"
        await asyncio.sleep(0.005)


def test_cap_eviction_keeps_buffer_bounded():
    async def scenario():
        sink = TcpSink("h", 1, max_buffer_bytes=2048)  # never started
        for i in range(100):
            sink.send({"trade_id": i})
        return sink

    sink = asyncio.run(scenario())
    assert sink._buffered_bytes <= 2048
    assert sink._dropped > 0
    # Accounting must match reality: payload bytes + per-entry overhead.
    actual = sum(len(line) + ENTRY_OVERHEAD_BYTES for _, line in sink._unsent)
    assert actual == sink._buffered_bytes


def test_send_assigns_monotonic_seq():
    async def scenario():
        sink = TcpSink("h", 1)
        for i in range(3):
            sink.send({"trade_id": i})
        return [json.loads(line)["seq"] for _, line in sink._unsent]

    assert asyncio.run(scenario()) == [1, 2, 3]


def test_acked_messages_are_evicted(monkeypatch):
    async def scenario():
        reader, writer = FakeReader(), FakeWriter()
        install_connections(monkeypatch, [(reader, writer)])

        sink = TcpSink("h", 1, reconnect_delay=0.001)
        sink.start()
        for i in range(5):
            sink.send({"trade_id": i})

        await wait_for(lambda: len(writer.lines) == 5)
        assert len(sink._unacked) == 5  # sent but not yet acknowledged

        reader.feed_ack(5)  # cumulative ack for everything
        await wait_for(lambda: not sink._unacked and not sink._unsent)
        assert sink._buffered_bytes == 0

        await sink.stop()
        return writer

    writer = asyncio.run(scenario())
    assert [json.loads(line)["trade_id"] for line in writer.lines] == list(range(5))


def test_unacked_messages_resend_on_reconnect(monkeypatch):
    async def scenario():
        reader1, writer1 = FakeReader(), FakeWriter()
        reader2, writer2 = FakeReader(), FakeWriter()
        install_connections(monkeypatch, [(reader1, writer1), (reader2, writer2)])

        sink = TcpSink("h", 1, reconnect_delay=0.001)
        sink.start()
        for i in range(5):
            sink.send({"trade_id": i})

        # Everything written on connection 1, but never acked.
        await wait_for(lambda: len(writer1.lines) == 5)
        reader1.feed_eof()  # peer closed: connection dies

        # All five must be requeued and resent on connection 2.
        await wait_for(lambda: len(writer2.lines) == 5)
        reader2.feed_ack(5)
        await wait_for(lambda: not sink._unacked and not sink._unsent)

        await sink.stop()
        return writer1, writer2, sink

    writer1, writer2, sink = asyncio.run(scenario())
    resent = [json.loads(line)["trade_id"] for line in writer2.lines]
    assert resent == [json.loads(line)["trade_id"] for line in writer1.lines]
    assert sink._dropped == 0
    assert writer1.closed


def test_malformed_ack_is_ignored(monkeypatch):
    async def scenario():
        reader, writer = FakeReader(), FakeWriter()
        install_connections(monkeypatch, [(reader, writer)])

        sink = TcpSink("h", 1, reconnect_delay=0.001)
        sink.start()
        sink.send({"trade_id": 1})
        await wait_for(lambda: len(writer.lines) == 1)

        reader.queue.put_nowait(b"garbage\n")
        reader.feed_ack(1)
        await wait_for(lambda: not sink._unacked)

        await sink.stop()
        return sink

    sink = asyncio.run(scenario())
    assert sink._buffered_bytes == 0
