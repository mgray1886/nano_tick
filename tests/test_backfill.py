import hashlib
import io
import zipfile
from datetime import date

import pytest

import backfill

HEADER = "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch"


def csv_row(tid, price, qty, t, maker):
    return f"{tid},{price},{qty},{price * qty},{t},{str(maker).lower()},true"


def make_zip(csv_text, name="BTCUSDT-trades-2026-07-15.csv", extra=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, csv_text)
        if extra:
            zf.writestr(extra, csv_text)
    return buf.getvalue()


# --- date planning ---------------------------------------------------------

def test_plan_dates_is_n_complete_days_ending_yesterday():
    dates = backfill.plan_dates(date(2026, 7, 16), 30)
    assert len(dates) == 30
    assert dates[0] == date(2026, 6, 16)
    assert dates[-1] == date(2026, 7, 15)  # yesterday; today excluded
    assert dates == sorted(dates)


# --- URLs ------------------------------------------------------------------

def test_urls():
    d = date(2026, 7, 15)
    assert backfill.archive_url("BTCUSDT", d) == (
        "https://data.binance.vision/data/spot/daily/trades/"
        "BTCUSDT/BTCUSDT-trades-2026-07-15.zip"
    )
    assert backfill.checksum_url("BTCUSDT", d).endswith("2026-07-15.zip.CHECKSUM")


# --- checksum --------------------------------------------------------------

def test_verify_checksum_match_and_mismatch():
    blob = b"some-zip-bytes"
    digest = hashlib.sha256(blob).hexdigest()
    assert backfill.verify_checksum(blob, f"{digest}  BTCUSDT-trades-2026-07-15.zip")
    assert not backfill.verify_checksum(blob, "deadbeef  BTCUSDT-trades-2026-07-15.zip")


# --- http ------------------------------------------------------------------

class FakeResp:
    """Context-manager stand-in for urlopen()'s response."""

    def __init__(self, body):
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._body


def test_http_get_returns_body(monkeypatch):
    monkeypatch.setattr(backfill.urllib.request, "urlopen",
                        lambda url, timeout: FakeResp(b"payload"))
    assert backfill.http_get("http://x") == b"payload"


def test_http_get_404_returns_none(monkeypatch):
    def not_found(url, timeout):
        raise backfill.urllib.error.HTTPError(url, 404, "Not Found", {}, None)
    monkeypatch.setattr(backfill.urllib.request, "urlopen", not_found)
    assert backfill.http_get("http://x") is None


def test_http_get_retries_then_raises(monkeypatch):
    attempts = []

    def flaky(url, timeout):
        attempts.append(url)
        raise backfill.urllib.error.URLError("temporary")

    monkeypatch.setattr(backfill.urllib.request, "urlopen", flaky)
    with pytest.raises(RuntimeError):
        backfill.http_get("http://x", retries=3)
    assert len(attempts) == 3  # exhausted all retries before giving up


# --- parsing ---------------------------------------------------------------

def test_iter_trade_chunks_maps_columns_with_header():
    csv_text = "\n".join([
        HEADER,
        csv_row(100, 42000.5, 0.01, 1700000000000, True),
        csv_row(101, 42001.0, 0.02, 1700000000100, False),
    ])
    chunks = list(backfill.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert len(chunks) == 1
    cols = chunks[0]
    assert cols["tradeID"] == [100, 101]
    assert cols["price"] == [42000.5, 42001.0]
    assert cols["qty"] == [0.01, 0.02]
    assert cols["time"] == [1700000000000, 1700000000100]
    assert cols["eventTime"] == cols["time"]  # archive has no emit time
    assert cols["sym"] == ["BTCUSDT", "BTCUSDT"]
    assert cols["venue"] == ["binance", "binance"]
    assert cols["buyerMaker"] == [True, False]


def test_iter_trade_chunks_without_header():
    csv_text = csv_row(100, 1.0, 2.0, 1700000000000, True)
    chunks = list(backfill.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert chunks[0]["tradeID"] == [100]


def test_iter_trade_chunks_respects_chunk_size():
    rows = [csv_row(i, 1.0, 1.0, 1700000000000 + i, True) for i in range(5)]
    chunks = list(backfill.iter_trade_chunks(
        make_zip("\n".join(rows)), "BTCUSDT", "binance", chunk_size=2))
    assert [len(c["tradeID"]) for c in chunks] == [2, 2, 1]
    assert [tid for c in chunks for tid in c["tradeID"]] == [0, 1, 2, 3, 4]


def test_iter_trade_chunks_skips_blank_lines():
    csv_text = csv_row(1, 1.0, 1.0, 1700000000000, True) + "\n\n"
    chunks = list(backfill.iter_trade_chunks(make_zip(csv_text), "BTCUSDT", "binance"))
    assert chunks[0]["tradeID"] == [1]


def test_iter_trade_chunks_rejects_ambiguous_archive():
    zip_bytes = make_zip("x", extra="BTCUSDT-trades-2026-07-15-extra.csv")
    with pytest.raises(ValueError):
        list(backfill.iter_trade_chunks(zip_bytes, "BTCUSDT", "binance"))


# --- idempotency -----------------------------------------------------------

def test_existing_partition_dates(tmp_path):
    (tmp_path / "2026.07.15" / "trade").mkdir(parents=True)
    (tmp_path / "2026.07.14").mkdir()          # no trade dir -> not counted
    (tmp_path / "sym").write_text("")          # enum file -> ignored
    (tmp_path / "notadate" / "trade").mkdir(parents=True)
    assert backfill.existing_partition_dates(tmp_path) == {date(2026, 7, 15)}


def test_existing_partition_dates_missing_hdb(tmp_path):
    assert backfill.existing_partition_dates(tmp_path / "nope") == set()


# --- real fetch composition (download + checksum + parse) ------------------

def test_make_fetch_day_downloads_verifies_and_parses(monkeypatch):
    csv_text = csv_row(1, 1.0, 1.0, 1700000000000, True)
    zip_bytes = make_zip(csv_text)
    checksum = f"{hashlib.sha256(zip_bytes).hexdigest()}  name.zip".encode()

    monkeypatch.setattr(
        backfill, "http_get",
        lambda url, *a, **k: zip_bytes if url.endswith(".zip") else checksum)
    fetch = backfill.make_fetch_day("BTCUSDT", "binance", 1000)
    chunks = list(fetch(date(2026, 7, 15)))
    assert sum(len(c["tradeID"]) for c in chunks) == 1


def test_make_fetch_day_raises_on_checksum_mismatch(monkeypatch):
    zip_bytes = make_zip(csv_row(1, 1.0, 1.0, 1700000000000, True))
    monkeypatch.setattr(
        backfill, "http_get",
        lambda url, *a, **k: zip_bytes if url.endswith(".zip") else b"bad  name.zip")
    fetch = backfill.make_fetch_day("BTCUSDT", "binance", 1000)
    with pytest.raises(ValueError):
        list(fetch(date(2026, 7, 15)))


def test_make_fetch_day_returns_none_when_not_published(monkeypatch):
    monkeypatch.setattr(backfill, "http_get", lambda url, *a, **k: None)
    fetch = backfill.make_fetch_day("BTCUSDT", "binance", 1000)
    assert fetch(date(2026, 7, 15)) is None


def test_make_fetch_day_parses_when_checksum_absent(monkeypatch):
    # CHECKSUM 404s (None): verification is skipped, the zip still parses.
    zip_bytes = make_zip(csv_row(1, 1.0, 1.0, 1700000000000, True))
    monkeypatch.setattr(
        backfill, "http_get",
        lambda url, *a, **k: zip_bytes if url.endswith(".zip") else None)
    fetch = backfill.make_fetch_day("BTCUSDT", "binance", 1000)
    chunks = list(fetch(date(2026, 7, 15)))
    assert sum(len(c["tradeID"]) for c in chunks) == 1


# --- orchestration ---------------------------------------------------------

class FakeWriter:
    def __init__(self):
        self.writes = []

    def write_day(self, day, chunks):
        n = sum(len(c["tradeID"]) for c in chunks)
        self.writes.append((day, n))
        return n


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
    assert writer.writes == [(d2, 3)]  # only the one available, non-existing day


# --- config + app-facing run() resource ------------------------------------

def test_config_from_env_defaults(monkeypatch):
    for v in ("SYMBOL", "VENUE", "BACKFILL_DAYS", "BACKFILL_CHUNK", "HDB_PATH"):
        monkeypatch.delenv(v, raising=False)
    cfg = backfill.BackfillConfig.from_env()
    assert cfg.symbol == "BTCUSDT"
    assert cfg.venue == "binance"
    assert cfg.days == 30
    assert cfg.chunk_size == 500000
    assert cfg.schema_q.name == "schema.q"


def test_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ethusdt")
    monkeypatch.setenv("BACKFILL_DAYS", "3")
    cfg = backfill.BackfillConfig.from_env()
    assert cfg.symbol == "ETHUSDT"  # upper()d
    assert cfg.days == 3


def test_run_uses_injected_writer_without_touching_kdb(monkeypatch, tmp_path):
    # Inject a fake writer + stub fetch/existing so no pykx or network is needed;
    # proves run() reuses the passed writer rather than building an HdbWriter.
    monkeypatch.setattr(backfill, "existing_partition_dates", lambda p: set())
    monkeypatch.setattr(backfill, "make_fetch_day",
                        lambda *a: (lambda day: [{"tradeID": [1]}]))
    cfg = backfill.BackfillConfig(symbol="BTCUSDT", venue="binance", days=2,
                                  chunk_size=10, hdb_path=tmp_path,
                                  schema_q=tmp_path / "schema.q")
    writer = FakeWriter()
    summary = backfill.run(cfg, writer=writer)
    assert summary["written"] == 2       # 2 planned days, both written
    assert len(writer.writes) == 2


# --- retention prune -------------------------------------------------------

def _cfg(hdb, days=30):
    return backfill.BackfillConfig(symbol="BTCUSDT", venue="binance", days=days,
                                   chunk_size=10, hdb_path=hdb,
                                   schema_q=hdb / "schema.q")


def _mkpart(hdb, name):
    (hdb / name / "trade").mkdir(parents=True)


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
    # run()'s oldest planned day must never be deleted by prune (same window).
    today = date(2026, 7, 16)
    oldest = backfill.plan_dates(today, 30)[0]           # today-30
    _mkpart(tmp_path, oldest.strftime("%Y.%m.%d"))
    backfill.prune(_cfg(tmp_path, days=30), today=today)
    assert (tmp_path / oldest.strftime("%Y.%m.%d") / "trade").exists()
