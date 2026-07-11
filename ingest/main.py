import asyncio
import logging
import signal

from src.config import Config
from src.normalisers.binance import normalize_trade
from src.sinks.mqtt import MqttSink
from src.sinks.tcp import TcpSink
from src.streams.binance import BinanceTradeStream

Sink = MqttSink | TcpSink

logger = logging.getLogger("nano_tick")


def start_sink(config: Config) -> Sink:
    """Create the sink and open the connection to the 4B; it reconnects
    with backoff for the lifetime of the process."""
    if config.sink_type == "tcp":
        sink: Sink = TcpSink(
            config.sink_host,
            config.sink_port,
            max_buffer_bytes=config.sink_buffer_mb * 1024 * 1024,
        )
    else:
        sink = MqttSink(
            config.mqtt_host,
            config.mqtt_port,
            max_queued_messages=config.mqtt_max_queued,
        )
    sink.start()
    return sink


async def run(config: Config, sink: Sink) -> None:
    stream = BinanceTradeStream(config.symbol)
    async for raw in stream.messages():
        try:
            sink.send(normalize_trade(raw))
        except Exception:
            logger.exception("dropping message that failed to normalise: %r", raw)


async def main() -> None:
    config = Config.from_env()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set_result, None)
    except NotImplementedError:
        # Windows dev box: no loop signal handlers; Ctrl+C raises
        # KeyboardInterrupt out of asyncio.run instead.
        pass

    sink = start_sink(config)
    task = asyncio.create_task(run(config, sink))
    await asyncio.wait({task, stop}, return_when=asyncio.FIRST_COMPLETED)

    if task.done():
        # run() only exits via an unhandled exception; surface it so the
        # process exits non-zero and systemd restarts us, instead of the
        # event loop idling forever with ingestion silently dead.
        await sink.stop()
        task.result()
        return

    logger.info("shutting down")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await sink.stop()


if __name__ == "__main__":
    asyncio.run(main())
