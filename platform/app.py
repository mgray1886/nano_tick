"""nano_tick KDB-X writer app (runs on the 4B).

Owns the single KDB-X write connection (schema.q + HdbWriter) and sequences the
writer role's lifecycle:

  1. startup: ensure recent history is present via an idempotent backfill
     (`backfill.run`), using the shared writer;
  2. live (next milestone): consume the MQTT `ticks/#` feed and insertRaw into
     the same HDB.

Running startup backfill and the live feed as one process sharing one kdb
resource mirrors how a small kdb writer is structured (gap-fill on startup,
then go live). Backfill stays a bounded, separable resource, so it can later
split into a standalone loader — the kdb-idiomatic split at a firm's scale —
without reworking this code.

Run on the 4B (needs a licensed q via pykx):  python platform/app.py
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root for resources/
from resources import backfill  # noqa: E402

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app")


def main() -> None:
    config = backfill.BackfillConfig.from_env()
    # The single kdb resource, shared by every role in this process.
    writer = backfill.HdbWriter(config.hdb_path, config.schema_q)

    logger.info("startup: maintaining a %d-day window in %s", config.days, config.hdb_path)
    backfill.run(config, writer=writer)   # fill the window
    backfill.prune(config)                # trim anything older than the window

    # TODO(feedhandler milestone): start the live MQTT -> kdb consumer here,
    # insertRaw-ing into the SAME `writer` so history and live share one HDB.
    # (A daily timer can also call run()+prune() to keep the window current.)
    logger.info("window maintained; live feed not yet implemented — exiting")


if __name__ == "__main__":
    main()
