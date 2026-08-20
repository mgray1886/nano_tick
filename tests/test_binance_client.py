import json
from datetime import date

import pytest

from clients import binance


class FakeResp:
    """Context-manager stand-in for urlopen()'s response."""

    def __init__(self, body, headers=None):
        self._body = body
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._body


def test_urls():
    d = date(2026, 7, 15)
    assert binance.archive_url("BTCUSDT", d) == (
        "https://data.binance.vision/data/spot/daily/trades/"
        "BTCUSDT/BTCUSDT-trades-2026-07-15.zip"
    )
    assert binance.checksum_url("BTCUSDT", d).endswith("2026-07-15.zip.CHECKSUM")


def test_download_returns_body(monkeypatch):
    monkeypatch.setattr(binance.urllib.request, "urlopen",
                        lambda url, timeout: FakeResp(b"payload"))
    assert binance.download_archive("BTCUSDT", date(2026, 7, 15)) == b"payload"


def test_download_404_returns_none(monkeypatch):
    def not_found(url, timeout):
        raise binance.urllib.error.HTTPError(url, 404, "Not Found", {}, None)
    monkeypatch.setattr(binance.urllib.request, "urlopen", not_found)
    assert binance.download_archive("BTCUSDT", date(2026, 7, 15)) is None


def test_download_retries_then_raises(monkeypatch):
    attempts = []

    def flaky(url, timeout):
        attempts.append(url)
        raise binance.urllib.error.URLError("temporary")

    monkeypatch.setattr(binance.urllib.request, "urlopen", flaky)
    with pytest.raises(RuntimeError):
        binance.download_checksum("BTCUSDT", date(2026, 7, 15))
    assert len(attempts) == binance.HTTP_RETRIES  # exhausted retries


def test_fetch_trades_parses_json_and_sends_api_key(monkeypatch):
    payload = json.dumps(
        [{"id": 1, "price": "1.0", "qty": "2.0", "time": 123, "isBuyerMaker": True}]
    ).encode()
    seen = {}

    def fake_urlopen(req, timeout):
        seen["url"] = req.full_url
        seen["key"] = req.get_header("X-mbx-apikey")  # Request title-cases header names
        return FakeResp(payload)

    monkeypatch.setattr(binance.urllib.request, "urlopen", fake_urlopen)
    trades = binance.fetch_trades("BTCUSDT", from_id=1, api_key="KEY123")
    assert trades[0]["id"] == 1
    assert "historicalTrades" in seen["url"]  # not /trades (no fromId support)
    assert seen["key"] == "KEY123"


def test_fetch_trades_empty_when_no_body(monkeypatch):
    monkeypatch.setattr(binance.urllib.request, "urlopen",
                        lambda req, timeout: FakeResp(b""))
    assert binance.fetch_trades("BTCUSDT", from_id=1) == []


def test_get_backs_off_on_429_then_succeeds(monkeypatch):
    calls = []

    def urlopen(req, timeout):
        calls.append(1)
        if len(calls) == 1:
            raise binance.urllib.error.HTTPError(
                req.full_url, 429, "rate", {"Retry-After": "0"}, None)
        return FakeResp(b"ok")

    monkeypatch.setattr(binance.urllib.request, "urlopen", urlopen)
    slept = []
    monkeypatch.setattr(binance.time, "sleep", lambda s: slept.append(s))
    assert binance._get("http://x") == b"ok"
    assert len(calls) == 2      # retried after the 429
    assert slept == [0]         # honoured Retry-After


def test_get_rate_limit_retries_do_not_exhaust_network_budget(monkeypatch):
    # 5 consecutive 429s > HTTP_RETRIES(3): proves throttling uses a separate
    # budget, so a paged backfill rides out the window reset instead of skipping.
    calls = []

    def urlopen(req, timeout):
        calls.append(1)
        if len(calls) <= 5:
            raise binance.urllib.error.HTTPError(
                req.full_url, 429, "rate", {"Retry-After": "0"}, None)
        return FakeResp(b"ok")

    monkeypatch.setattr(binance.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(binance.time, "sleep", lambda s: None)
    assert binance._get("http://x") == b"ok"
    assert len(calls) == 6


def test_get_gives_up_after_persistent_rate_limiting(monkeypatch):
    def always_429(req, timeout):
        raise binance.urllib.error.HTTPError(
            req.full_url, 429, "rate", {"Retry-After": "0"}, None)

    monkeypatch.setattr(binance.urllib.request, "urlopen", always_429)
    monkeypatch.setattr(binance.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):        # backstop: never loops forever
        binance._get("http://x")


def test_get_paces_and_observes_weight(monkeypatch):
    class RecordingPacer:
        def __init__(self):
            self.throttled = 0
            self.observed = []

        def throttle(self):
            self.throttled += 1

        def observe(self, headers):
            self.observed.append(dict(headers))

    monkeypatch.setattr(binance.urllib.request, "urlopen",
                        lambda req, timeout: FakeResp(b"ok", {"X-MBX-USED-WEIGHT-1M": "42"}))
    pacer = RecordingPacer()
    assert binance._get("http://x", pacer=pacer) == b"ok"
    assert pacer.throttled == 1                                  # throttled before the request
    assert pacer.observed == [{"X-MBX-USED-WEIGHT-1M": "42"}]    # observed the response weight


def test_fetch_trades_forwards_pacer(monkeypatch):
    seen = {}

    def fake_get(url, headers=None, pacer=None):
        seen["pacer"] = pacer
        return b"[]"

    monkeypatch.setattr(binance, "_get", fake_get)
    pacer = binance.WeightPacer()
    binance.fetch_trades("BTCUSDT", from_id=1, pacer=pacer)
    assert seen["pacer"] is pacer


# --- WeightPacer -----------------------------------------------------------

def test_weight_pacer_throttles_over_threshold():
    slept = []
    p = binance.WeightPacer(limit=1000, safety=0.8, sleep=slept.append, clock=lambda: 100.0)
    p.observe({"X-MBX-USED-WEIGHT-1M": "850"})       # >= 800 threshold
    p.throttle()
    assert slept == [pytest.approx(60 - (100.0 % 60) + 1)]   # sleeps to next minute + 1s
    assert p.used == 0                                       # counter reset after the wait


def test_weight_pacer_no_throttle_under_threshold():
    slept = []
    p = binance.WeightPacer(limit=1000, safety=0.8, sleep=slept.append, clock=lambda: 0.0)
    p.observe({"X-MBX-USED-WEIGHT-1M": "700"})       # < 800 threshold
    p.throttle()
    assert slept == []


def test_weight_pacer_ignores_missing_or_bad_header():
    p = binance.WeightPacer()
    p.observe({})
    p.observe({"X-MBX-USED-WEIGHT-1M": "not-a-number"})
    p.observe(None)
    assert p.used == 0
