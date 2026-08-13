"""nano_tick KDB-X writer app (runs on the 4B).

Owns the single KDB-X write connection (schema.q + HdbWriter) and sequences the
writer role's lifecycle:

  1. startup: fill the rolling window from the archive (`backfill.run`) and
     prune anything older, using the shared writer;
  2. live: hand the same writer to the feedhandler, which REST-bridges the
     current-day gap then consumes the MQTT `ticks/#` feed into the RDB.

One process sharing one kdb resource mirrors how a small kdb writer is
structured (gap-fill on startup, then go live). Each piece stays a bounded,
separable resource, so they can later split into their own processes — the
kdb-idiomatic scale-out — without reworking this code.

Run on the 4B (needs a licensed q via pykx):  python platform/app.py
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root for resources/
from resources import backfill  # noqa: E402
from resources import feedhandler  # noqa: E402

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app")


def main() -> None:
    config = backfill.BackfillConfig.from_env()
    # The single kdb resource, shared by every role in this process.
    writer = backfill.HdbWriter(config.hdb_path, config.schema_q)

    logger.info("startup: maintaining a %d-day window in %s", config.days, config.hdb_path)
    backfill.run(config, writer=writer)   # fill the window from the archive
    backfill.prune(config)                # trim anything older than the window

    logger.info("starting live feed")
    feedhandler.FeedHandler(config, writer).run()   # bridge the gap, then go live (blocks)


if __name__ == "__main__":
    main()
