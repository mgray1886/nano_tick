import asyncio
import json

from src.streams import base
from src.streams.binance import BinanceTradeStream


class FakeConnection:
    """Stands in for websockets.connect(): async context manager that yields
    itself and then iterates a fixed list of frames. Exhausting the frames
    either closes cleanly (StopAsyncIteration) or raises `exc`."""

    def __init__(self, frames, exc=None):
        self.frames = list(frames)
        self.exc = exc
        self.subscribed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return self.frames.pop(0)
        if self.exc is not None:
            raise self.exc
        raise StopAsyncIteration


class FakeStream(base.WebsocketStream):
    initial_backoff = 0.001
    max_backoff = 0.002

    def url(self) -> str:
        return "ws://unit-test"


def install_connections(monkeypatch, connections):
    iterator = iter(connections)
    monkeypatch.setattr(base.websockets, "connect", lambda url: next(iterator))


async def take(stream, n):
    received = []
    async for message in stream.messages():
        received.append(message)
        if len(received) == n:
            return received


def test_binance_trade_stream_url_lowercases_symbol():
    assert BinanceTradeStream("BTCUSDT").url() == "wss://stream.binance.com:9443/ws/btcusdt@trade"


def test_messages_yields_parsed_json(monkeypatch):
    install_connections(monkeypatch, [FakeConnection([json.dumps({"n": 1}), json.dumps({"n": 2})])])
    assert asyncio.run(take(FakeStream(), 2)) == [{"n": 1}, {"n": 2}]


def test_malformed_frame_is_skipped_not_fatal(monkeypatch):
    frames = [json.dumps({"n": 1}), "this is not json", json.dumps({"n": 2})]
    install_connections(monkeypatch, [FakeConnection(frames)])
    assert asyncio.run(take(FakeStream(), 2)) == [{"n": 1}, {"n": 2}]


def test_reconnects_after_clean_close(monkeypatch):
    install_connections(monkeypatch, [
        FakeConnection([json.dumps({"n": 1})]),           # closes cleanly
        FakeConnection([json.dumps({"n": 2})], exc=None),
    ])
    assert asyncio.run(take(FakeStream(), 2)) == [{"n": 1}, {"n": 2}]


def test_reconnects_with_backoff_after_drop(monkeypatch):
    install_connections(monkeypatch, [
        FakeConnection([json.dumps({"n": 1})], exc=OSError("connection reset")),
        FakeConnection([json.dumps({"n": 2})]),
    ])
    sleeps = []
    real_sleep = asyncio.sleep
    monkeypatch.setattr(base.asyncio, "sleep", lambda s: sleeps.append(s) or real_sleep(0))

    assert asyncio.run(take(FakeStream(), 2)) == [{"n": 1}, {"n": 2}]
    assert sleeps == [FakeStream.initial_backoff]


def test_subscribe_hook_runs_once_per_connection(monkeypatch):
    connections = [
        FakeConnection([json.dumps({"n": 1})]),
        FakeConnection([json.dumps({"n": 2})]),
    ]
    install_connections(monkeypatch, connections)

    class SubscribingStream(FakeStream):
        async def subscribe(self, ws):
            ws.subscribed = True

    asyncio.run(take(SubscribingStream(), 2))
    assert all(c.subscribed for c in connections)
