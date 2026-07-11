"""Tick recorder for the rpi4B: persists the MQTT feed to disk as history
for future training/backtesting.

Subscribes to ticks/# at QoS 1 with a persistent session (broker queues while
this service is down), dedupes on (venue, symbol, trade_id), and appends
NDJSON to <DATA_DIR>/<venue>/<symbol>/<YYYY-MM-DD>.ndjson (UTC date from the
tick's trade_ts). Open files are flushed every FLUSH_INTERVAL seconds to
bound loss on power cut (the 4B records to SSD, so frequent flushes are
cheap); on date rollover the closed file is gzipped in a background thread
(~10x smaller).

Config via env: MQTT_HOST (default 192.168.100.2), MQTT_PORT (1883),
DATA_DIR (default ~/nano_tick_data).
"""
import gzip
import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("recorder")

MQTT_HOST = os.environ.get("MQTT_HOST", "192.168.100.2")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
DATA_DIR = Path(os.environ.get("DATA_DIR", Path.home() / "nano_tick_data"))

FLUSH_INTERVAL = 1.0
STATS_INTERVAL = 10.0


def gzip_file(path: Path) -> None:
    try:
        with open(path, "rb") as src, gzip.open(f"{path}.gz", "wb") as dst:
            shutil.copyfileobj(src, dst)
        path.unlink()
        logger.info("compressed %s", f"{path}.gz")
    except OSError:
        logger.exception("failed to compress %s; leaving uncompressed", path)


class Recorder:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.last_trade_id: dict = {}   # (venue, symbol) -> highest trade_id seen
        self.open_files: dict = {}      # (venue, symbol) -> (date_str, file handle)
        self.lock = threading.Lock()    # paho callbacks + flush timer share files
        self.received = 0
        self.duplicates = 0

    def _file_for(self, venue: str, symbol: str, date_str: str):
        key = (venue, symbol)
        current = self.open_files.get(key)
        if current and current[0] == date_str:
            return current[1]

        if current:  # date rollover: close and compress yesterday's file
            old_handle = current[1]
            old_path = Path(old_handle.name)
            old_handle.close()
            threading.Thread(target=gzip_file, args=(old_path,), daemon=True).start()

        directory = self.data_dir / venue / symbol
        directory.mkdir(parents=True, exist_ok=True)
        handle = open(directory / f"{date_str}.ndjson", "a", encoding="utf-8")
        self.open_files[key] = (date_str, handle)
        return handle

    def record(self, tick: dict) -> None:
        date_str = datetime.fromtimestamp(tick["trade_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        with self.lock:
            handle = self._file_for(tick["venue"], tick["symbol"].lower(), date_str)
            handle.write(json.dumps(tick) + "\n")

    def flush(self) -> None:
        with self.lock:
            for _, handle in self.open_files.values():
                handle.flush()

    # --- mqtt callbacks -------------------------------------------------

    def on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        logger.info("connected to broker (%s), subscribing to ticks/#", reason_code)
        client.subscribe("ticks/#", qos=1)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        logger.warning("disconnected from broker (%s); paho will reconnect", reason_code)

    def on_message(self, client, userdata, msg) -> None:
        try:
            tick = json.loads(msg.payload)
            key = (tick["venue"], tick["symbol"])
            trade_id = tick["trade_id"]
        except (json.JSONDecodeError, KeyError):
            logger.warning("dropping malformed message on %s: %r", msg.topic, msg.payload[:200])
            return

        if trade_id <= self.last_trade_id.get(key, -1):
            self.duplicates += 1
            return
        self.last_trade_id[key] = trade_id
        try:
            self.record(tick)
        except OSError:
            logger.exception("failed to write tick; disk problem?")
            return
        self.received += 1


def main() -> None:
    recorder = Recorder(DATA_DIR)
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="nano_tick_recorder",
        clean_session=False,
    )
    client.on_connect = recorder.on_connect
    client.on_disconnect = recorder.on_disconnect
    client.on_message = recorder.on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    logger.info("recording to %s", DATA_DIR)

    prev = 0
    last_stats = time.monotonic()
    try:
        while True:
            time.sleep(FLUSH_INTERVAL)
            recorder.flush()
            now = time.monotonic()
            if now - last_stats >= STATS_INTERVAL:
                rate = (recorder.received - prev) / (now - last_stats)
                prev = recorder.received
                last_stats = now
                logger.info(
                    "%.1f ticks/s | %d recorded | %d duplicates",
                    rate, recorder.received, recorder.duplicates,
                )
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        client.disconnect()
        client.loop_stop()
        recorder.flush()


if __name__ == "__main__":
    main()
