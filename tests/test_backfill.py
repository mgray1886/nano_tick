import hashlib
import io
import zipfile
from datetime import date, datetime, timezone

import pytest

from resources import backfill


def csv_row(tid, price, qty, t, maker):
    return f"{tid},{price},{qty},{price * qty},{t},{str(maker).lower()},true"


def make_zip(csv_text, name="BTCUSDT-trades-2026-07-15.csv"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, csv_text)
    return buf.getvalue()


def _cfg(hdb, days=30):
    return backfill.BackfillConfig(symbol="BTCUSDT", venue="binance", days=days,
                                   chunk_size=10, hdb_path=hdb,
                                   schema_q=hdb / "schema.q")


def _mkpart(hdb, name):
    (hdb / name / "trade").mkdir(parents=True)


# --- date planning ---------------------------------------------------------

def test_plan_dates_is_n_complete_days_ending_yesterday():
    dates = backfill.plan_dates(date(2026, 7, 16), 30)
    assert len(dates) == 30
    assert dates[0] == date(2026, 6, 16)
    assert dates[-1] == date(2026, 7, 15)  # yesterday; today excluded
    assert dates == sorted(dates)


# --- HDB inspection --------------------------------------------------------

def test_existing_partition_dates(tmp_path):
    _mkpart(tmp_path, "2026.07.15")
    (tmp_path / "2026.07.14").mkdir()          # no trade dir -> not counted
    (tmp_path / "sym").write_text("")          # enum file -> ignored
    (tmp_path / "notadate" / "trade").mkdir(parents=True)
    assert backfill.existing_partition_dates(tmp_path) == {date(2026, 7, 15)}


def test_existing_partition_dates_missing_hdb(tmp_path):
    assert backfill.existing_partition_dates(tmp_path / "nope") == set()


def test_latest_partition_tradeid_path(tmp_path):
    assert backfill.latest_partition_tradeid_path(tmp_path) is None
    _mkpart(tmp_path, "2026.07.14")
    _mkpart(tmp_path, "2026.07.16")
    _mkpart(tmp_path, "2026.07.15")
    assert backfill.latest_partition_tradeid_path(tmp_path) == (
        tmp_path / "2026.07.16" / "trade" / "tradeID")


# --- fetch composition (client download + data parse) ----------------------

def test_make_fetch_day_downloads_verifies_and_parses(monkeypatch):
    zip_bytes = make_zip(csv_row(1, 1.0, 1.0, 1700000000000, True))
    checksum = f"{hashlib.sha256(zip_bytes).hexdigest()}  name.zip".encode()
    monkeypatch.setattr(backfill.binance_client, "download_archive", lambda s, d: zip_bytes)
    monkeypatch.setattr(backfill.binance_client, "download_checksum", lambda s, d: checksum)
    chunks = list(backfill.make_fetch_day("BTCUSDT", "binance", 1000)(date(2026, 7, 15)))
    assert sum(len(c["tradeID"]) for c in chunks) == 1


def test_make_fetch_day_raises_on_checksum_mismatch(monkeypatch):
    zip_bytes = make_zip(csv_row(1, 1.0, 1.0, 1700000000000, True))
    monkeypatch.setattr(backfill.binance_client, "download_archive", lambda s, d: zip_bytes)
    monkeypatch.setattr(backfill.binance_client, "download_checksum", lambda s, d: b"bad  n.zip")
    with pytest.raises(ValueError):
        list(backfill.make_fetch_day("BTCUSDT", "binance", 1000)(date(2026, 7, 15)))


def test_make_fetch_day_returns_none_when_not_published(monkeypatch):
    monkeypatch.setattr(backfill.binance_client, "download_archive", lambda s, d: None)
    assert backfill.make_fetch_day("BTCUSDT", "binance", 1000)(date(2026, 7, 15)) is None


# --- archive orchestration -------------------------------------------------

class FakeWriter:
    """Records write_day/insert/savedown calls; stands in for HdbWriter."""

    def __init__(self, floor=None):
        self.writes = []
        self.inserted = []
        self.quote_inserted = []
        self.savedowns = []
        self._floor = floor

    def write_day(self, day, chunks):
        n = sum(len(c["tradeID"]) for c in chunks)
        self.writes.append((day, n))
        return n

    def insert(self, cols):
        self.inserted.extend(cols["tradeID"])
        return len(cols["tradeID"])

    def insert_quote(self, cols):
        self.quote_inserted.extend(cols["updateID"])
        return len(cols["updateID"])

    def savedown(self, day):
        self.savedowns.append(day)

    def max_stored_id(self):
        return self._floor


def test_run_backfill_skips_existing_writes_missing_and_records_failures():
    d1, d2, d3, d4 = (date(2026, 7, d) for d in (12, 13, 14, 15))

    def fetch_day(day):
        if day == d2:
            return [{"tradeID": [1, 2, 3]}]
        if day == d3:
            return None            # archive not published
        if day == d4:
            raise RuntimeError("boom")
        raise AssertionError("unexpected fetch")

    writer = FakeWriter()
    summary = backfill.run_backfill([d1, d2, d3, d4], {d1}, fetch_day, writer)

    assert summary == {"written": 1, "rows": 3, "skipped": 1, "missing": 1, "failed": 1}
    assert writer.writes == [(d2, 3)]


# --- config + run() resource -----------------------------------------------

def test_config_from_env_defaults(monkeypatch):
    for v in ("SYMBOL", "VENUE", "BACKFILL_DAYS", "BACKFILL_CHUNK", "HDB_PATH"):
        monkeypatch.delenv(v, raising=False)
    cfg = backfill.BackfillConfig.from_env()
    assert cfg.symbol == "BTCUSDT"
    assert cfg.venue == "binance"
    assert cfg.days == 30
    assert cfg.chunk_size == 500000
    assert cfg.schema_q.name == "schema.q"
    assert cfg.api_key is None


def test_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ethusdt")
    monkeypatch.setenv("BACKFILL_DAYS", "3")
    cfg = backfill.BackfillConfig.from_env()
    assert cfg.symbol == "ETHUSDT"  # upper()d
    assert cfg.days == 3


def test_run_uses_injected_writer_without_touching_kdb(monkeypatch, tmp_path):
    monkeypatch.setattr(backfill, "existing_partition_dates", lambda p: set())
    monkeypatch.setattr(backfill, "make_fetch_day",
                        lambda *a: (lambda day: [{"tradeID": [1]}]))
    writer = FakeWriter()
    summary = backfill.run(_cfg(tmp_path, days=2), writer=writer)
    assert summary["written"] == 2       # 2 planned days, both written
    assert len(writer.writes) == 2


# --- current-day gap bridge ------------------------------------------------

def _fake_universe(monkeypatch, ids):
    def fetch(symbol, from_id, limit, api_key=None):
        avail = [i for i in ids if i >= from_id][:limit]
        return [{"id": i, "price": "1.0", "qty": "1.0", "time": 1, "isBuyerMaker": True}
                for i in avail]
    monkeypatch.setattr(backfill.binance_client, "fetch_trades", fetch)


def test_bridge_pages_from_floor_and_stops_before_target(monkeypatch, tmp_path):
    _fake_universe(monkeypatch, range(6, 13))       # ids 6..12 exist
    writer = FakeWriter(floor=5)
    summary = backfill.bridge(_cfg(tmp_path), writer, target_id=9, page=2)
    assert writer.inserted == [6, 7, 8]             # < target 9
    assert summary["inserted"] == 3
    assert summary["from_id"] == 6


def test_bridge_fills_to_tip_when_no_target(monkeypatch, tmp_path):
    _fake_universe(monkeypatch, range(6, 10))       # ids 6..9 exist
    writer = FakeWriter(floor=5)
    summary = backfill.bridge(_cfg(tmp_path), writer, target_id=None, page=2)
    assert writer.inserted == [6, 7, 8, 9]
    assert summary["inserted"] == 4


def test_bridge_skips_when_no_stored_id(tmp_path):
    writer = FakeWriter(floor=None)
    assert backfill.bridge(_cfg(tmp_path), writer)["inserted"] == 0


# --- live RDB rollover -----------------------------------------------------

def _ms(y, mo, d, h=12):
    return int(datetime(y, mo, d, h, tzinfo=timezone.utc).timestamp() * 1000)


def _chunk(ms, tid):
    return {"time": [ms], "sym": ["BTCUSDT"], "venue": ["binance"], "tradeID": [tid],
            "price": [1.0], "qty": [1.0], "eventTime": [ms], "buyerMaker": [False]}


def _qchunk(ms, uid):
    return {"time": [ms], "sym": ["BTCUSDT"], "venue": ["binance"], "updateID": [uid],
            "bid": [1.0], "bidSize": [2.0], "ask": [1.1], "askSize": [3.0]}


def test_roller_first_feed_does_not_roll():
    w = FakeWriter()
    res = backfill.RdbRoller(w).feed(_chunk(_ms(2026, 7, 15), 1))
    assert res == {"inserted": 1, "day": date(2026, 7, 15), "rolled": None}
    assert w.savedowns == []


def test_roller_no_savedown_within_same_day():
    w = FakeWriter()
    r = backfill.RdbRoller(w)
    r.feed(_chunk(_ms(2026, 7, 15, 1), 1))
    r.feed(_chunk(_ms(2026, 7, 15, 23), 2))
    assert w.savedowns == []
    assert w.inserted == [1, 2]


def test_roller_savedowns_previous_day_on_boundary():
    w = FakeWriter()
    r = backfill.RdbRoller(w)
    r.feed(_chunk(_ms(2026, 7, 15, 23), 1))
    res = r.feed(_chunk(_ms(2026, 7, 16, 0), 2))   # crosses midnight
    assert res["rolled"] == date(2026, 7, 15)
    assert res["day"] == date(2026, 7, 16)
    assert w.savedowns == [date(2026, 7, 15)]       # finished day persisted
    assert w.inserted == [1, 2]


def test_roller_flush_savedowns_current_day():
    w = FakeWriter()
    r = backfill.RdbRoller(w)
    r.feed(_chunk(_ms(2026, 7, 15), 1))
    assert r.flush() == date(2026, 7, 15)
    assert w.savedowns == [date(2026, 7, 15)]
    assert r.flush() is None                         # nothing left to persist


def test_roller_trades_and_quotes_share_one_day_boundary():
    w = FakeWriter()
    r = backfill.RdbRoller(w)
    r.feed(_chunk(_ms(2026, 7, 15), 1))              # trade, day 15
    r.feed_quote(_qchunk(_ms(2026, 7, 15, 23), 10))  # quote, day 15 -> no roll
    assert w.savedowns == []
    res = r.feed_quote(_qchunk(_ms(2026, 7, 16, 0), 11))  # quote crosses midnight
    assert res["rolled"] == date(2026, 7, 15)        # rolls BOTH tables
    assert w.savedowns == [date(2026, 7, 15)]
    assert w.inserted == [1]
    assert w.quote_inserted == [10, 11]


# --- retention prune -------------------------------------------------------

def test_prune_drops_only_partitions_older_than_window(tmp_path):
    _mkpart(tmp_path, "2026.06.01")   # outside 30d window -> remove
    _mkpart(tmp_path, "2026.06.16")   # exactly the cutoff (today-30) -> keep
    _mkpart(tmp_path, "2026.07.15")   # inside window -> keep
    (tmp_path / "sym").write_text("")                        # enum file -> keep
    (tmp_path / "notadate" / "trade").mkdir(parents=True)    # non-date dir -> keep

    summary = backfill.prune(_cfg(tmp_path), today=date(2026, 7, 16))

    assert summary["removed"] == 1
    assert summary["cutoff"] == date(2026, 6, 16)
    assert not (tmp_path / "2026.06.01").exists()
    assert (tmp_path / "2026.06.16" / "trade").exists()      # cutoff day kept
    assert (tmp_path / "2026.07.15" / "trade").exists()
    assert (tmp_path / "sym").exists()
    assert (tmp_path / "notadate" / "trade").exists()


def test_prune_keeps_everything_within_window(tmp_path):
    _mkpart(tmp_path, "2026.07.10")
    _mkpart(tmp_path, "2026.07.15")
    assert backfill.prune(_cfg(tmp_path), today=date(2026, 7, 16))["removed"] == 0


def test_prune_missing_hdb_is_noop(tmp_path):
    assert backfill.prune(_cfg(tmp_path / "nope"), today=date(2026, 7, 16))["removed"] == 0


def test_prune_cutoff_aligns_with_backfill_oldest_no_thrash(tmp_path):
    today = date(2026, 7, 16)
    oldest = backfill.plan_dates(today, 30)[0]           # today-30
    _mkpart(tmp_path, oldest.strftime("%Y.%m.%d"))
    backfill.prune(_cfg(tmp_path, days=30), today=today)
    assert (tmp_path / oldest.strftime("%Y.%m.%d") / "trade").exists()
