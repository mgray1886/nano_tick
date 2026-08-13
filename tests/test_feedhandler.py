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


def tick(tid, ms=1700000000000):
    return {"venue": "binance", "symbol": "BTCUSDT", "trade_id": tid, "price": "1.0",
            "qty": "1.0", "event_ts": ms, "trade_ts": ms, "is_buyer_maker": False}


def _fh(roller):
    return feedhandler.FeedHandler(_cfg(), writer=None, roller=roller)


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
