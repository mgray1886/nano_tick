import json
import logging
import urllib.error
import urllib.request
from datetime import date
from typing import Optional

logger = logging.getLogger("clients.binance")

ARCHIVE_BASE = "https://data.binance.vision/data/spot/daily/trades"
REST_BASE = "https://api.binance.com"
HTTP_TIMEOUT = 60
HTTP_RETRIES = 3
TRADES_LIMIT = 1000  # max trades per /api/v3/trades call


def archive_url(symbol: str, day: date) -> str:
    return f"{ARCHIVE_BASE}/{symbol}/{symbol}-trades-{day:%Y-%m-%d}.zip"


def checksum_url(symbol: str, day: date) -> str:
    return archive_url(symbol, day) + ".CHECKSUM"


def _get(url: str, timeout: int = HTTP_TIMEOUT, retries: int = HTTP_RETRIES,
         headers: Optional[dict] = None) -> Optional[bytes]:
    """GET url. Returns the body, None on 404, or raises after `retries` failures."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        logger.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, last_exc)
    raise RuntimeError(f"GET {url} failed after {retries} attempts") from last_exc


def download_archive(symbol: str, day: date) -> Optional[bytes]:
    """The day's trades zip, or None if the archive hasn't published it yet."""
    return _get(archive_url(symbol, day))


def download_checksum(symbol: str, day: date) -> Optional[bytes]:
    """The day's .CHECKSUM, or None if absent."""
    return _get(checksum_url(symbol, day))


def fetch_trades(symbol: str, from_id: int, limit: int = TRADES_LIMIT,
                 api_key: Optional[str] = None) -> list:
    """REST /api/v3/historicalTrades from `from_id` (ascending). `id` is the
    individual trade id — same space as the @trade stream and the archive
    (unlike aggTrades). `api_key` (X-MBX-APIKEY) is optional: it works keyless
    but a free market-data key raises the rate limit. Returns raw JSON.

    Note: from_id below the endpoint's retained range returns the oldest
    available trades, not an error."""
    url = f"{REST_BASE}/api/v3/historicalTrades?symbol={symbol}&fromId={from_id}&limit={limit}"
    headers = {"X-MBX-APIKEY": api_key} if api_key else {}
    body = _get(url, headers=headers)
    return json.loads(body) if body else []
