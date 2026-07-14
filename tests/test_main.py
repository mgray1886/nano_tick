import asyncio
from types import SimpleNamespace

import main
from src.config import Config
from src.sinks.mqtt import MqttSink
from src.sinks.tcp import TcpSink


def config(**overrides) -> Config:
    base = dict(
        log_level="INFO", symbol="btcusdt", sink_type="mqtt",
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


def test_run_drops_unnormalisable_messages_and_continues(monkeypatch):
    good_raw = {
        "E": 2, "s": "BTCUSDT", "t": 1, "p": "1.0", "q": "2.0", "T": 1, "m": False,
    }

    class FakeStream:
        def __init__(self, symbol):
            pass

        async def messages(self):
            yield {"unexpected": "shape"}  # KeyError inside normalize_trade
            yield good_raw

    monkeypatch.setattr(main, "BinanceTradeStream", FakeStream)
    sent = []
    sink = SimpleNamespace(send=sent.append)

    asyncio.run(main.run(config(), sink))

    assert len(sent) == 1
    assert sent[0]["trade_id"] == 1
