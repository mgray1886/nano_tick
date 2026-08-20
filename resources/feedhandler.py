"""Live feedhandler (runs on the 4B): MQTT ticks/# + quotes/# -> kdb RDB.

Consumes the normalised trade and quote feeds the 3A+ publishes and inserts
them into the in-memory RDB (trade + quote tables) via RdbRoller, which
savedowns both at the UTC day boundary. On startup it REST-bridges the trade
gap between archived history and the live stream so trades are gap-free; quotes
are live-only (no historical source), so they simply start on connect.

kdb is single-threaded, so paho's network thread only ENQUEUES; all kdb work
(bridge + inserts) happens on the main thread draining the queue.
"""
import json
import logging
import queue
import signal

import paho.mqtt.client as mqtt

from resources import backfill
from resources import binance as binance_data

logger = logging.getLogger("feedhandler")

QUEUE_MAXSIZE = 200_000   # bounded so a slow consumer can't grow memory unbounded
BATCH_MAX = 5_000         # records coalesced into one insert
BATCH_TIMEOUT = 0.5       # seconds to block for the next record before re-checking stop


class FeedHandler:
    def __init__(self, config, writer, roller=None):
        self._config = config
        self._writer = writer
        self._roller = roller if roller is not None else backfill.RdbRoller(writer)
        self._queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
        self._last_id = None          # trade dedup floor
        self._last_quote_id = None    # quote dedup floor
        self._dropped = 0
        self._stop = False
        self._connected_once = False
        self._needs_rebridge = False  # set on (re)connect; the first live trade closes the gap
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="nano_tick_feedhandler",
            clean_session=False,   # broker buffers our QoS 1 messages while we're away
        )
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)

    # --- paho network thread: enqueue only, never touch kdb --------------

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        client.subscribe([("ticks/#", 1), ("quotes/#", 1)])
        if self._connected_once:
            # A reconnect: trades may have been dropped during a long outage.
            # Flag the main thread to REST-bridge the trade gap (kdb is
            # single-threaded, so this callback must not bridge itself).
            logger.warning("reconnected to broker; will re-bridge any trade gap")
            self._needs_rebridge = True
        else:
            self._connected_once = True
            logger.info("connected to broker (%s), subscribing to ticks/# + quotes/#",
                        reason_code)

    def _on_message(self, client, userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload)
            if msg.topic.startswith("quotes/"):
                int(payload["update_id"])
                kind = "quote"
            else:
                int(payload["trade_id"])
                kind = "trade"
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.warning("dropping malformed message on %s", msg.topic)
            return
        try:
            self._queue.put_nowait((kind, payload))
        except queue.Full:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                logger.warning("feed queue full, dropped %d records so far", self._dropped)

    # --- main thread: all kdb access -------------------------------------

    @staticmethod
    def _dedup(items: list, key: str, last):
        """Sort by `key` and keep only ids strictly above `last`."""
        fresh = []
        for it in sorted(items, key=lambda x: x[key]):
            if last is None or it[key] > last:
                fresh.append(it)
                last = it[key]
        return fresh, last

    def _apply_trades(self, trades: list) -> int:
        if not trades:
            return 0
        fresh, self._last_id = self._dedup(trades, "trade_id", self._last_id)
        if fresh:
            self._roller.feed(binance_data.parse_live_ticks(fresh))
        return len(fresh)

    def _apply_quotes(self, quotes: list) -> int:
        if not quotes:
            return 0
        fresh, self._last_quote_id = self._dedup(quotes, "update_id", self._last_quote_id)
        if fresh:
            self._roller.feed_quote(binance_data.parse_live_quotes(fresh))
        return len(fresh)

    def _drain(self, first: tuple) -> list:
        """Coalesce `first` plus whatever else is queued (up to BATCH_MAX)."""
        batch = [first]
        while len(batch) < BATCH_MAX:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _maybe_rebridge(self, trades: list) -> None:
        """After (re)connect, if the first genuinely-new trade jumps past
        last_id+1, the broker dropped messages — REST-bridge [last_id+1 .. it).
        Only clears the flag once a new trade has arrived, so stale backlog
        can't clear it prematurely."""
        new_ids = [t["trade_id"] for t in trades
                   if self._last_id is None or t["trade_id"] > self._last_id]
        if not new_ids:
            return
        first_new = min(new_ids)
        if self._last_id is not None and first_new > self._last_id + 1:
            logger.warning("trade gap [%d .. %d); bridging via REST",
                           self._last_id + 1, first_new)
            backfill.bridge(self._config, self._writer, target_id=first_new)
            self._last_id = self._writer.max_stored_id()
        self._needs_rebridge = False

    def run(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, lambda *_: self._request_stop())
            except ValueError:
                pass  # not the main thread (e.g. tests)

        # 1) REST-catch-up the trade history to the live tip before subscribing.
        backfill.bridge(self._config, self._writer)
        self._last_id = self._writer.max_stored_id()
        self._last_quote_id = self._writer.max_stored_quote_id()
        self._needs_rebridge = True  # first live trade closes the startup gap

        self._client.connect_async(self._config.mqtt_host, self._config.mqtt_port)
        self._client.loop_start()

        logger.info("live; last_id=%s last_quote_id=%s", self._last_id, self._last_quote_id)
        while not self._stop:
            try:
                first = self._queue.get(timeout=BATCH_TIMEOUT)
            except queue.Empty:
                continue
            batch = self._drain(first)
            trades = [m for k, m in batch if k == "trade"]
            quotes = [m for k, m in batch if k == "quote"]
            if self._needs_rebridge and trades:
                self._maybe_rebridge(trades)
            self._apply_trades(trades)
            self._apply_quotes(quotes)

        self._shutdown()

    def _request_stop(self) -> None:
        self._stop = True

    def _shutdown(self) -> None:
        logger.info("shutting down; flushing RDB to HDB")
        self._client.loop_stop()
        self._client.disconnect()
        self._roller.flush()  # savedown the current day (trade + quote)
