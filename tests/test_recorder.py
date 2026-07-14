import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import recorder as recorder_module
from recorder import Recorder


class InlineThread:
    """Runs the target synchronously so tests never wait on real threads."""

    def __init__(self, target=None, args=(), daemon=None):
        self._target, self._args = target, args

    def start(self):
        self._target(*self._args)


@pytest.fixture(autouse=True)
def inline_threads(monkeypatch):
    monkeypatch.setattr(recorder_module.threading, "Thread", InlineThread)


DAY1_TS = 1783763087212  # 2026-07-11 UTC
DAY2_TS = DAY1_TS + 24 * 3600 * 1000


def tick(trade_id: int, ts: int = DAY1_TS, symbol: str = "BTCUSDT") -> dict:
    return {
        "venue": "binance",
        "symbol": symbol,
        "trade_id": trade_id,
        "price": 1.0,
        "qty": 2.0,
        "event_ts": ts,
        "trade_ts": ts,
        "is_buyer_maker": False,
    }


def mqtt_msg(payload) -> SimpleNamespace:
    if isinstance(payload, dict):
        payload = json.dumps(payload).encode()
    return SimpleNamespace(topic="ticks/binance/btcusdt", payload=payload)


def test_record_writes_ndjson_under_venue_symbol_date(tmp_path):
    recorder = Recorder(tmp_path)
    recorder.record(tick(1))
    recorder.flush()
    path = tmp_path / "binance" / "btcusdt" / "2026-07-11.ndjson"
    assert path.exists()
    assert json.loads(path.read_text().splitlines()[0])["trade_id"] == 1


def test_on_message_dedupes_by_trade_id(tmp_path):
    recorder = Recorder(tmp_path)
    recorder.on_message(None, None, mqtt_msg(tick(5)))
    recorder.on_message(None, None, mqtt_msg(tick(5)))  # duplicate
    recorder.on_message(None, None, mqtt_msg(tick(4)))  # replay of older id
    recorder.on_message(None, None, mqtt_msg(tick(6)))
    recorder.flush()
    assert recorder.received == 2
    assert recorder.duplicates == 2
    path = tmp_path / "binance" / "btcusdt" / "2026-07-11.ndjson"
    ids = [json.loads(line)["trade_id"] for line in path.read_text().splitlines()]
    assert ids == [5, 6]


def test_on_message_ignores_malformed_payloads(tmp_path):
    recorder = Recorder(tmp_path)
    recorder.on_message(None, None, mqtt_msg(b"not json"))
    recorder.on_message(None, None, mqtt_msg({"venue": "binance"}))  # fields missing
    assert recorder.received == 0


def test_symbols_are_tracked_independently(tmp_path):
    recorder = Recorder(tmp_path)
    recorder.on_message(None, None, mqtt_msg(tick(10, symbol="BTCUSDT")))
    recorder.on_message(None, None, mqtt_msg(tick(3, symbol="ETHUSDT")))
    assert recorder.received == 2
    assert recorder.duplicates == 0


def test_date_rollover_gzips_previous_day(tmp_path):
    recorder = Recorder(tmp_path)
    recorder.record(tick(1, ts=DAY1_TS))
    recorder.record(tick(2, ts=DAY2_TS))
    recorder.flush()

    day1 = tmp_path / "binance" / "btcusdt" / "2026-07-11.ndjson"
    day1_gz = Path(f"{day1}.gz")
    assert day1_gz.exists(), "rolled-over file should be gzipped"
    assert not day1.exists(), "plain file should be removed after compression"
    with gzip.open(day1_gz, "rt") as f:
        assert json.loads(f.read().splitlines()[0])["trade_id"] == 1
    day2 = tmp_path / "binance" / "btcusdt" / "2026-07-12.ndjson"
    assert json.loads(day2.read_text().splitlines()[0])["trade_id"] == 2
