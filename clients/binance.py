import json
import logging
import time
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

# Request-weight pacing for api.binance.com. Binance caps request weight per
# minute and reports the running total in the X-MBX-USED-WEIGHT-1M response
# header, resetting it on each clock minute. We pace under the cap to avoid
# tripping a 429/418 (which risks an IP ban) during a large cold-start backfill.
# (The data.binance.vision archive host has no such limit, so only REST is paced.)
WEIGHT_LIMIT_PER_MIN = 6000   # spot REQUEST_WEIGHT / min for a keyless IP; a key raises it
WEIGHT_SAFETY = 0.75          # begin pacing once usage reaches this fraction of the cap
RATE_LIMIT_RETRIES = 10       # 429/418 backoffs before giving up (separate from network retries)


def archive_url(symbol: str, day: date) -> str:
    return f"{ARCHIVE_BASE}/{symbol}/{symbol}-trades-{day:%Y-%m-%d}.zip"


def checksum_url(symbol: str, day: date) -> str:
    return archive_url(symbol, day) + ".CHECKSUM"


class WeightPacer:
    """Proactive request-weight pacing for the Binance REST API.

    Binance reports the current minute's used request weight in the
    ``X-MBX-USED-WEIGHT-1M`` response header and resets it on each clock minute.
    When the reported weight reaches the safety threshold we sleep to just past
    the next minute boundary (where the counter resets), so a large cold-start
    backfill stays under the cap and never has to abandon a page mid-range and
    skip trade ids. The 429 backoff in ``_get`` is the backstop if the estimate
    is ever wrong. One pacer instance should be shared across a run of requests.
    """

    def __init__(self, limit: int = WEIGHT_LIMIT_PER_MIN, safety: float = WEIGHT_SAFETY,
                 sleep=time.sleep, clock=time.time):
        self.limit = limit
        self.threshold = max(1, int(limit * safety))
        self._sleep = sleep
        self._clock = clock
        self.used = 0

    def observe(self, headers) -> None:
        """Record the used weight Binance just reported (authoritative for the
        minute — it reflects every request from this IP/key, not only ours)."""
        raw = headers.get("X-MBX-USED-WEIGHT-1M") if headers else None
        if raw is not None:
            try:
                self.used = int(raw)
            except (TypeError, ValueError):
                pass

    def throttle(self) -> None:
        """Sleep to just past the next clock-minute boundary if usage is at/over
        the safety threshold; otherwise return immediately."""
        if self.used < self.threshold:
            return
        wait = 60 - (self._clock() % 60) + 1        # to the next minute + 1s buffer
        logger.info("request weight %d/%d near cap; pacing %.1fs to window reset",
                    self.used, self.limit, wait)
        self._sleep(wait)
        self.used = 0


def _get(url: str, timeout: int = HTTP_TIMEOUT, retries: int = HTTP_RETRIES,
         headers: Optional[dict] = None, pacer: Optional["WeightPacer"] = None) -> Optional[bytes]:
    """GET url. Returns the body, None on 404, or raises after exhausting retries.

    Rate-limit (429/418) responses are retried with the server's ``Retry-After``
    backoff on a SEPARATE, larger budget (``RATE_LIMIT_RETRIES``) — they succeed
    once the weight window resets, so throttling never consumes the small network
    retry budget and a paged backfill never gives up mid-range. An optional
    ``pacer`` throttles proactively to stay under the cap in the first place.
    """
    last_exc: Optional[Exception] = None
    network = 0      # transient network/timeout attempts (small budget)
    throttled = 0    # 429/418 backoffs (larger budget; succeed on window reset)
    while network < retries and throttled < RATE_LIMIT_RETRIES:
        if pacer is not None:
            pacer.throttle()
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if pacer is not None:
                    pacer.observe(resp.headers)
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if pacer is not None:
                pacer.observe(exc.headers)
            if exc.code in (429, 418):  # rate limited / IP throttled
                wait = min(int(exc.headers.get("Retry-After", "1") or "1"), 120)
                logger.warning("rate limited (%d); backing off %ds", exc.code, wait)
                time.sleep(wait)
                throttled += 1
                continue                 # do NOT consume the network budget
            last_exc = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        network += 1
        logger.warning("GET %s failed (attempt %d/%d): %s", url, network, retries, last_exc)
    raise RuntimeError(
        f"GET {url} failed after {network} network / {throttled} rate-limit attempts") from last_exc


def download_archive(symbol: str, day: date) -> Optional[bytes]:
    """The day's trades zip, or None if the archive hasn't published it yet."""
    return _get(archive_url(symbol, day))


def download_checksum(symbol: str, day: date) -> Optional[bytes]:
    """The day's .CHECKSUM, or None if absent."""
    return _get(checksum_url(symbol, day))


def fetch_trades(symbol: str, from_id: int, limit: int = TRADES_LIMIT,
                 api_key: Optional[str] = None, pacer: Optional["WeightPacer"] = None) -> list:
    """REST /api/v3/historicalTrades from `from_id` (ascending). `id` is the
    individual trade id — same space as the @trade stream and the archive
    (unlike aggTrades). `api_key` (X-MBX-APIKEY) is optional: it works keyless
    but a free market-data key raises the rate limit. `pacer` (shared across a
    run) keeps a large paged backfill under the request-weight cap. Returns raw
    JSON.

    Note: from_id below the endpoint's retained range returns the oldest
    available trades, not an error."""
    url = f"{REST_BASE}/api/v3/historicalTrades?symbol={symbol}&fromId={from_id}&limit={limit}"
    headers = {"X-MBX-APIKEY": api_key} if api_key else {}
    body = _get(url, headers=headers, pacer=pacer)
    return json.loads(body) if body else []
