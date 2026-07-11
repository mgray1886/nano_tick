import asyncio
import json
import logging
from typing import Optional

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

STATS_INTERVAL_SECONDS = 60.0


class MqttSink:
    """Publishes normalised ticks to the mosquitto broker on the rpi4B.

    Topic: ticks/<venue>/<symbol> (lowercased), QoS 1 (at-least-once), JSON
    payload. Consumers subscribe to ticks/# (or narrower) and dedupe on
    (venue, trade_id).

    Reliability is delegated to paho/mosquitto: paho reconnects with backoff
    and queues messages published while disconnected, capped at
    max_queued_messages so memory stays bounded on the Pi. Only when that
    queue is full are ticks dropped, and drops are logged.

    Same interface as TcpSink (start / send / stop) so main.py can swap
    between them via SINK_TYPE.
    """

    def __init__(self, host: str, port: int = 1883, max_queued_messages: int = 50_000):
        self.host = host
        self.port = port
        self._dropped = 0
        self._connected = False
        self._stats_task: Optional[asyncio.Task] = None

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="nano_tick")
        self._client.max_queued_messages_set(max_queued_messages)
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

    def start(self) -> None:
        # connect_async + loop_start: paho's network thread owns the
        # connection and retries forever; nothing blocks the asyncio side.
        self._client.connect_async(self.host, self.port)
        self._client.loop_start()
        self._stats_task = asyncio.create_task(self._stats_loop())

    async def stop(self) -> None:
        if self._stats_task:
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass
        self._client.disconnect()
        self._client.loop_stop()

    def send(self, message: dict) -> None:
        topic = f"ticks/{message['venue']}/{message['symbol'].lower()}"
        info = self._client.publish(topic, json.dumps(message), qos=1)
        if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                logger.warning("mqtt queue full, dropped %d ticks so far", self._dropped)

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._connected = True
        logger.info("connected to mqtt broker %s:%s (%s)", self.host, self.port, reason_code)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        self._connected = False
        logger.warning("disconnected from mqtt broker (%s); paho will reconnect", reason_code)

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(STATS_INTERVAL_SECONDS)
            if not self._connected or self._dropped:
                logger.info(
                    "mqtt sink: connected=%s, %d dropped total", self._connected, self._dropped
                )
