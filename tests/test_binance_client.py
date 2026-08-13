import json
from datetime import date

import pytest

from clients import binance


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
