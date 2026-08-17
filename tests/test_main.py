import asyncio
from types import SimpleNamespace

import main
from src.config import Config
from src.sinks.mqtt import MqttSink
from src.sinks.tcp import TcpSink


def config(**overrides) -> Config:
    base = dict(
        log_level="INFO", symbol="btcusdt", stream_quotes=True, sink_type="mqtt",
        mqtt_host="example.invalid", mqtt_port=1883, mqtt_max_queued=100,
        sink_host="127.0.0.1", sink_port=1, sink_buffer_mb=1,
    )
    base.update(overrides)
    return Config(**base)


def test_start_sink_factory_selects_by_sink_type(monkeypatch):
    monkeypatch.setattr(TcpSink, "start", lambda self: None)
    monkeypatch.setattr(MqttSink, "start", lambda self: None)

    assert isinstance(main.start_sink(config(sink_type="mqtt")), MqttSink)
    assert isinstance(main.start_sink(config(sink_type="tcp")), TcpSink)


def test_start_sink_passes_buffer_config(monkeypatch):
    monkeypatch.setattr(TcpSink, "start", lambda self: None)
    sink = main.start_sink(config(sink_type="tcp", sink_buffer_mb=8))
    assert sink.max_buffer_bytes == 8 * 1024 * 1024


def test_run_demuxes_trades_and_quotes_and_drops_bad(monkeypatch):
    trade = {"stream": "btcusdt@trade",
             "data": {"E": 2, "s": "BTCUSDT", "t": 1, "p": "1.0", "q": "2.0", "T": 1, "m": False}}
    quote = {"stream": "btcusdt@bookTicker",
             "data": {"s": "BTCUSDT", "u": 9, "b": "1.0", "B": "2.0", "a": "1.1", "A": "3.0"}}
    bad = {"stream": "btcusdt@trade", "data": {"unexpected": "shape"}}  # KeyError

    class FakeStream:
        def __init__(self, symbol, quotes=True):
            pass

        async def messages(self):
            yield trade
            yield bad
            yield quote

    monkeypatch.setattr(main, "BinanceCombinedStream", FakeStream)
    sends = []
    sink = SimpleNamespace(send=lambda m, topic_root="ticks": sends.append((topic_root, m)))

    asyncio.run(main.run(config(), sink))

    assert [root for root, _ in sends] == ["ticks", "quotes"]   # bad trade dropped
    assert sends[0][1]["trade_id"] == 1
    assert sends[1][1]["update_id"] == 9
