"""Live feedhandler (runs on the 4B): MQTT ticks/# -> kdb RDB.

Consumes the normalised tick feed the 3A+ publishes and inserts it into the
in-memory RDB via RdbRoller (which savedowns at the UTC day boundary). On
startup it runs the REST bridge to close the gap between archived history and
the live stream, so the store is gap-free.

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
BATCH_MAX = 5_000         # ticks coalesced into one insert
BATCH_TIMEOUT = 0.5       # seconds to block for the next tick before re-checking stop


class FeedHandler:
    def __init__(self, config, writer, roller=None):
        self._config = config
        self._writer = writer
        self._roller = roller if roller is not None else backfill.RdbRoller(writer)
        self._queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
        self._last_id = None
        self._dropped = 0
        self._stop = False
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
        logger.info("connected to broker (%s), subscribing to ticks/#", reason_code)
        client.subscribe("ticks/#", qos=1)

    def _on_message(self, client, userdata, msg) -> None:
        try:
            tick = json.loads(msg.payload)
            int(tick["trade_id"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.warning("dropping malformed tick on %s", msg.topic)
            return
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                logger.warning("feed queue full, dropped %d ticks so far", self._dropped)

    # --- main thread: all kdb access -------------------------------------

    def _apply(self, ticks: list) -> int:
        """Sort, dedup by trade_id, and feed the roller. Main thread only."""
        fresh = []
        last = self._last_id
        for t in sorted(ticks, key=lambda t: t["trade_id"]):
            if last is None or t["trade_id"] > last:
                fresh.append(t)
                last = t["trade_id"]
        if not fresh:
            return 0
        self._last_id = last
        self._roller.feed(binance_data.parse_live_ticks(fresh))
        return len(fresh)

    def _drain(self, first: dict) -> list:
        """Coalesce `first` plus whatever else is already queued (up to BATCH_MAX)."""
        batch = [first]
        while len(batch) < BATCH_MAX:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def run(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, lambda *_: self._request_stop())
            except ValueError:
                pass  # not the main thread (e.g. tests)

        # 1) REST-catch-up to the live tip before subscribing.
        backfill.bridge(self._config, self._writer)
        self._last_id = self._writer.max_stored_id()

        self._client.connect_async(self._config.mqtt_host, self._config.mqtt_port)
        self._client.loop_start()

        # 2) first live tick fixes F; bridge the small startup gap [last_id+1, F).
        first = self._queue.get()
        target = first["trade_id"]
        if self._last_id is not None and target > self._last_id + 1:
            backfill.bridge(self._config, self._writer, target_id=target)
            self._last_id = self._writer.max_stored_id()

        # 3) live loop.
        self._apply(self._drain(first))
        logger.info("live; last_id=%s", self._last_id)
        while not self._stop:
            try:
                nxt = self._queue.get(timeout=BATCH_TIMEOUT)
            except queue.Empty:
                continue
            self._apply(self._drain(nxt))

        self._shutdown()

    def _request_stop(self) -> None:
        self._stop = True

    def _shutdown(self) -> None:
        logger.info("shutting down; flushing RDB to HDB")
        self._client.loop_stop()
        self._client.disconnect()
        self._roller.flush()  # savedown the current day so it isn't lost
