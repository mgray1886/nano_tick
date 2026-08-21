import json
from pathlib import Path
from types import SimpleNamespace

from resources import feedhandler
from resources.backfill import BackfillConfig


def _cfg(symbols=("BTCUSDT",)):
    return BackfillConfig(symbols=symbols, venue="binance", days=30, chunk_size=10,
                          hdb_path=Path("/tmp/x"), schema_q=Path("/tmp/x/schema.q"))


class FakeRoller:
    def __init__(self):
        self.trades = []
        self.quotes = []

    def feed(self, cols):
        self.trades.append(list(cols["tradeID"]))

    def feed_quote(self, cols):
        self.quotes.append(list(cols["updateID"]))

    def flush(self):
        pass


class FakeWriter:
    def __init__(self, mid=None, mids=None):
        self._mid = mid
        self._mids = mids or {}

    def max_stored_id(self, symbol):
        return self._mids.get(symbol, self._mid)

    def max_stored_quote_id(self, symbol):
        return self._mids.get(symbol, self._mid)


def trade(tid, ms=1700000000000, symbol="BTCUSDT"):
    return {"venue": "binance", "symbol": symbol, "trade_id": tid, "price": "1.0",
            "qty": "1.0", "event_ts": ms, "trade_ts": ms, "is_buyer_maker": False}


def quote(uid, ms=1700000000000, symbol="BTCUSDT"):
    return {"venue": "binance", "symbol": symbol, "update_id": uid, "bid": "1.0",
            "bid_qty": "2.0", "ask": "1.1", "ask_qty": "3.0", "recv_ts": ms}


def _fh(roller=None, writer=None, symbols=("BTCUSDT",)):
    return feedhandler.FeedHandler(_cfg(symbols), writer=writer, roller=roller or FakeRoller())


def msg(topic, payload):
    return SimpleNamespace(topic=topic, payload=json.dumps(payload).encode())


# --- trade path ------------------------------------------------------------

def test_apply_trades_sorts_and_dedups():
    r = FakeRoller()
    fh = _fh(r)
    assert fh._apply_trades([trade(3), trade(1), trade(2), trade(2)]) == 3
    assert r.trades == [[1, 2, 3]]
    assert fh._last_id == {"BTCUSDT": 3}


def test_apply_trades_drops_at_or_below_last():
    r = FakeRoller()
    fh = _fh(r)
    fh._apply_trades([trade(5)])
    assert fh._apply_trades([trade(3), trade(5), trade(6)]) == 1
    assert r.trades[-1] == [6]
    assert fh._last_id == {"BTCUSDT": 6}


def test_apply_trades_dedups_per_symbol():
    # BTC and ETH have INDEPENDENT id sequences; an ETH id below BTC's last id
    # must not be deduped away against BTC's floor
    r = FakeRoller()
    fh = _fh(r, symbols=("BTCUSDT", "ETHUSDT"))
    assert fh._apply_trades([trade(100, symbol="BTCUSDT"), trade(5, symbol="ETHUSDT")]) == 2
    assert fh._last_id == {"BTCUSDT": 100, "ETHUSDT": 5}
    assert sorted(r.trades) == [[5], [100]]        # both kept


# --- quote path ------------------------------------------------------------

def test_apply_quotes_sorts_and_dedups_by_update_id():
    r = FakeRoller()
    fh = _fh(r)
    assert fh._apply_quotes([quote(30), quote(10), quote(20), quote(20)]) == 3
    assert r.quotes == [[10, 20, 30]]
    assert fh._last_quote_id == {"BTCUSDT": 30}


def test_apply_quotes_drops_old_update_ids():
    r = FakeRoller()
    fh = _fh(r)
    fh._apply_quotes([quote(5)])
    assert fh._apply_quotes([quote(4), quote(6)]) == 1
    assert r.quotes[-1] == [6]


# --- ingestion / routing ---------------------------------------------------

def test_on_message_routes_trades_and_quotes_and_drops_bad():
    fh = _fh()
    fh._on_message(None, None, msg("ticks/binance/btcusdt", trade(5)))
    fh._on_message(None, None, msg("quotes/binance/btcusdt", quote(9)))
    fh._on_message(None, None, SimpleNamespace(topic="ticks/x", payload=b"not json"))
    fh._on_message(None, None, msg("ticks/x", {"no": "id"}))
    fh._on_message(None, None, msg("quotes/x", {"no": "id"}))
    items = []
    while not fh._queue.empty():
        items.append(fh._queue.get_nowait())
    assert [k for k, _ in items] == ["trade", "quote"]
    assert items[0][1]["trade_id"] == 5 and items[1][1]["update_id"] == 9


def test_drain_coalesces_queued_records():
    fh = _fh()
    fh._queue.put(("trade", trade(2)))
    fh._queue.put(("quote", quote(3)))
    batch = fh._drain(("trade", trade(1)))
    assert [k for k, _ in batch] == ["trade", "trade", "quote"]


# --- reconnect / startup re-bridge -----------------------------------------

def _client():
    return SimpleNamespace(subscribe=lambda *a, **k: None)


def test_on_connect_flags_rebridge_only_on_reconnect():
    fh = _fh(symbols=("BTCUSDT", "ETHUSDT"))
    fh._on_connect(_client(), None, None, "ok", None)
    assert fh._connected_once is True and fh._pending_rebridge == set()
    fh._on_connect(_client(), None, None, "ok", None)          # reconnect flags all symbols
    assert fh._pending_rebridge == {"BTCUSDT", "ETHUSDT"}


def test_maybe_rebridge_bridges_and_refreshes_on_gap(monkeypatch):
    fh = _fh(writer=FakeWriter(mid=104))
    fh._last_id = {"BTCUSDT": 100}
    fh._pending_rebridge = {"BTCUSDT"}
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge",
                        lambda cfg, w, sym, target_id=None: calls.append((sym, target_id)))
    fh._maybe_rebridge("BTCUSDT", [trade(105), trade(106)])
    assert calls == [("BTCUSDT", 105)]
    assert fh._last_id["BTCUSDT"] == 104
    assert fh._pending_rebridge == set()


def test_maybe_rebridge_no_bridge_when_contiguous(monkeypatch):
    fh = _fh(writer=FakeWriter(mid=100))
    fh._last_id = {"BTCUSDT": 100}
    fh._pending_rebridge = {"BTCUSDT"}
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge", lambda *a, **k: calls.append(1))
    fh._maybe_rebridge("BTCUSDT", [trade(101)])
    assert calls == []
    assert fh._pending_rebridge == set()


def test_maybe_rebridge_keeps_flag_when_only_stale(monkeypatch):
    fh = _fh(writer=FakeWriter())
    fh._last_id = {"BTCUSDT": 100}
    fh._pending_rebridge = {"BTCUSDT"}
    calls = []
    monkeypatch.setattr(feedhandler.backfill, "bridge", lambda *a, **k: calls.append(1))
    fh._maybe_rebridge("BTCUSDT", [trade(99), trade(100)])
    assert calls == []
    assert fh._pending_rebridge == {"BTCUSDT"}      # unchanged; still pending
