import json
from pathlib import Path
from types import SimpleNamespace

from resources import feedhandler
from resources.backfill import BackfillConfig


def _cfg():
    return BackfillConfig(symbol="BTCUSDT", venue="binance", days=30, chunk_size=10,
                          hdb_path=Path("/tmp/x"), schema_q=Path("/tmp/x/schema.q"))


class FakeRoller:
    def __init__(self):
        self.fed = []

    def feed(self, cols):
        self.fed.append(list(cols["tradeID"]))

    def flush(self):
        pass


class FakeWriter:
    def __init__(self, mid=None):
        self._mid = mid

    def max_stored_id(self):
        return self._mid


def tick(tid, ms=1700000000000):
    return {"venue": "binance", "symbol": "BTCUSDT", "trade_id": tid, "price": "1.0",
            "qty": "1.0", "event_ts": ms, "trade_ts": ms, "is_buyer_maker": False}


def _fh(roller, writer=None):
    return feedhandler.FeedHandler(_cfg(), writer=writer, roller=roller)


def test_apply_sorts_dedups_and_tracks_last_id():
    r = FakeRoller()
    fh = _fh(r)
    n = fh._apply([tick(3), tick(1), tick(2), tick(2)])   # out of order + duplicate
    assert n == 3
    assert r.fed == [[1, 2, 3]]
    assert fh._last_id == 3


def test_apply_drops_ids_at_or_below_last():
    r = FakeRoller()
    fh = _fh(r)
    fh._apply([tick(5)])
    n = fh._apply([tick(3), tick(5), tick(6)])   # 3 and 5 <= last(5) -> dropped
    assert n == 1
    assert r.fed[-1] == [6]
    assert fh._last_id == 6


def test_apply_empty_batch_is_noop():
    r = FakeRoller()
    fh = _fh(r)
    assert fh._apply([]) == 0
    assert r.fed == []


def test_on_message_enqueues_valid_and_drops_malformed():
    fh = _fh(FakeRoller())
    fh._on_message(None, None, SimpleNamespace(
        topic="ticks/binance/btcusdt", payload=json.dumps(tick(5)).encode()))
    fh._on_message(None, None, SimpleNamespace(topic="x", payload=b"not json"))
    fh._on_message(None, None, SimpleNamespace(
        topic="x", payload=json.dumps({"no": "id"}).encode()))
    assert fh._queue.qsize() == 1


def test_drain_coalesces_queued_ticks():
    fh = _fh(FakeRoller())
    fh._queue.put(tick(2))
    fh._queue.put(tick(3))
    batch = fh._drain(tick(1))
    assert [t["trade_id"] for t in batch] == [1, 2, 3]


# --- reconnect re-bridge ---------------------------------------------------

def _client():
    return SimpleNamespace(subscribe=lambda *a, **k: None)


def test_on_connect_flags_rebridge_only_on_reconnect():
    fh = _fh(FakeRoller())
    fh._on_connect(_client(), None, None, "ok", None)          # first connect
    assert fh._connected_once is True and fh._needs_rebridge is False
    fh._on_connect(_client(), None, None, "ok", None)          # reconnect
    assert fh._needs_rebridge is True


def test_maybe_rebridge_bridges_and_refreshes_on_gap(monkeypatch):
    fh = _fh(FakeRoller(), writer=FakeWriter(mid=104))
    fh._last_id = 100
    fh._needs_rebridge = True
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge",
                        lambda cfg, w, target_id=None: calls.append(target_id))
    fh._maybe_rebridge([tick(105), tick(106)])   # first new 105 > last+1 -> gap
    assert calls == [105]
    assert fh._last_id == 104                     # refreshed from writer
    assert fh._needs_rebridge is False


def test_maybe_rebridge_no_bridge_when_contiguous(monkeypatch):
    fh = _fh(FakeRoller(), writer=FakeWriter(mid=100))
    fh._last_id = 100
    fh._needs_rebridge = True
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge", lambda *a, **k: calls.append(1))
    fh._maybe_rebridge([tick(101)])               # contiguous -> no gap
    assert calls == []
    assert fh._needs_rebridge is False


def test_maybe_rebridge_keeps_flag_when_only_stale(monkeypatch):
    fh = _fh(FakeRoller(), writer=FakeWriter())
    fh._last_id = 100
    fh._needs_rebridge = True
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge", lambda *a, **k: calls.append(1))
    fh._maybe_rebridge([tick(99), tick(100)])     # all stale -> keep waiting
    assert calls == []
    assert fh._needs_rebridge is True
