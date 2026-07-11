"""Reference MQTT subscriber for nano_tick ticks. Runs on the rpi4B.

Pairs with src/sinks/mqtt.py (SINK_TYPE=mqtt, the default): subscribes to
ticks/# at QoS 1 with a persistent session, so the broker queues ticks while
this consumer is down. Delivery is at-least-once - duplicates are dropped
here on (venue, symbol, trade_id), relying on Binance trade ids being
sequential per symbol.

Usage: python3 receiver.py [--host 192.168.100.2] [--port 1883]
Replace `process()` with real handling (write to disk, fan out, etc.).

(For the legacy SINK_TYPE=tcp transport, see git history / src/sinks/tcp.py.)
"""
import argparse
import json
import logging
import time

import paho.mqtt.client as mqtt

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("receiver")

STATS_INTERVAL = 10.0


def process(tick: dict) -> None:
    """Placeholder for real downstream handling."""


class Receiver:
    def __init__(self) -> None:
        self.last_trade_id: dict = {}  # (venue, symbol) -> highest trade_id seen
        self.received = 0
        self.duplicates = 0

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
        self.received += 1
        process(tick)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="192.168.100.2")
    parser.add_argument("--port", type=int, default=1883)
    args = parser.parse_args()

    receiver = Receiver()
    # Fixed client id + clean_session=False: the broker holds QoS 1 messages
    # for this subscriber while it is offline and replays them on reconnect.
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="nano_tick_receiver",
        clean_session=False,
    )
    client.on_connect = receiver.on_connect
    client.on_disconnect = receiver.on_disconnect
    client.on_message = receiver.on_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(args.host, args.port)
    client.loop_start()

    prev = 0
    try:
        while True:
            time.sleep(STATS_INTERVAL)
            rate = (receiver.received - prev) / STATS_INTERVAL
            prev = receiver.received
            logger.info(
                "%.1f ticks/s | %d total | %d duplicates | %d symbols",
                rate, receiver.received, receiver.duplicates, len(receiver.last_trade_id),
            )
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()
