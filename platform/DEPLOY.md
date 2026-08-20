# nano_tick — deploy runbook (two Pis, go-live)

Bringing the pipeline up on the physical **3A+** (ingest) and **4B** (store /
writer). This is the operational checklist; component setup lives in
[../README.md](../README.md) (ethernet link, broker), [KDBX_SETUP.md](KDBX_SETUP.md)
(KDB-X licensing/install), and [RESEARCH.md](RESEARCH.md) (the query side).

> Validated by an end-to-end dry run (2026-08-20) in the Docker+WSL stack:
> `app.py` booted the full sequence, the feedhandler consumed **~16.7k real
> trades + 38.9k real quotes** from the live Binance feed over MQTT, `savedown`
> materialised all four tables (trade/quote/bar/quoteBar), and the flushed HDB
> read back correctly. One bug found and fixed (`backfill.run` crash on days=0).

## Prerequisites

- **Hardware**: 3A+ (ingest) + USB-ethernet, 4B + SSD, direct ethernet link (README).
- **4B OS is 64-bit** (`uname -m` = `aarch64`) and **KDB-X Community installed** with
  `~/.kx/kc.lic` (KDBX_SETUP.md).
- **Both Pis NTP-synced** — `timedatectl` shows NTP active. *Why it matters:* the
  day-partition rollover keys off record time, and it mixes trade **event time**
  (from Binance) with quote **receive time** (stamped by the 3A+ clock — spot
  bookTicker has no exchange timestamp). If the 3A+ clock drifts across a UTC
  midnight, trades and quotes for the same moment can land in different
  partitions. (The dry run showed exactly this split, caused by a skewed sandbox
  clock — a non-issue on NTP-synced hardware.)
- **HDB on the SSD**: `HDB_PATH` points at the SSD mount, never the SD card.

## 1. 3A+ — ingest

Per README: bring up the point-to-point ethernet link, then `ingest/setup.sh` +
the `ingest.service` unit. It streams trades **and** bookTicker quotes (combined
stream) to MQTT. Confirm from the 4B:

```bash
mosquitto_sub -h 192.168.100.2 -t 'ticks/#' -t 'quotes/#' -v   # both topics should flow
```

## 2. 4B — broker + writer app

1. Install KDB-X + license (KDBX_SETUP.md); install the broker via `platform/setup.sh`.
2. Configure the writer (env or `platform/.env`):
   - `HDB_PATH` → SSD path (e.g. `/mnt/ssd/nano_tick_hdb`)
   - `SYMBOL` (`BTCUSDT`), `VENUE` (`binance`)
   - `BACKFILL_DAYS` (30 to start, 90 later) — sizes **both** the backfill window and retention
   - `MQTT_HOST` (`192.168.100.2`), `MQTT_PORT` (`1883`)
   - `BINANCE_API_KEY` (optional) — raises the REST weight cap; speeds the cold-start bridge
3. Run the writer app (needs the licensed q via pykx):

   ```bash
   HDB_PATH=/mnt/ssd/nano_tick_hdb BACKFILL_DAYS=30 python platform/app.py
   ```

   As a service, mirror `recorder.service` (a `writer.service` unit with
   `Restart=always`, `MemoryMax` sized to the 4B).

`app.py` sequence: build the writer → `backfill.run` (archive) → `prune` →
`feedhandler.run` (REST-bridge to the live tip, then consume MQTT into the RDB;
`savedown` at each UTC-day rollover). It blocks until stopped.

## 3. Bring-up order

1. 4B broker up.
2. 3A+ ingest up → confirm it's publishing (mosquitto_sub above).
3. 4B writer app (`app.py`) → watch the logs (next section).

## 4. Expected first run (cold start)

- **Backfill** downloads the last `BACKFILL_DAYS` complete archived days (the
  archive lags ~1 day). Minutes; ~50–80 MB/day. Each day is written and its
  bars materialised.
- **Bridge**: the feedhandler then REST-pages from the last stored trade id up to
  the live stream's first id. On a fresh mid-day start this gap can be ~a day of
  trades — thousands of paged calls, **weight-aware paced** under the Binance cap
  (you'll see `request weight NNN/6000 near cap; pacing …s` lines). Expect
  **several minutes**; a `BINANCE_API_KEY` raises the cap and shortens it. It
  never skips ids and won't trip a ban.
- Then: `connected to broker … subscribing to ticks/# + quotes/#` and
  `live; last_id=… last_quote_id=…`.

## 5. Verification checklist

- [ ] `mosquitto_sub` shows both `ticks/#` and `quotes/#`.
- [ ] Writer log: `connected to broker … subscribing to ticks/# + quotes/#`.
- [ ] After a UTC-day rollover (or SIGTERM flush): logs
      `saved trade/quote/bar/quoteBar partition YYYY.MM.DD`.
- [ ] Read back in a **separate** q process on the 4B: `q <HDB_PATH>` then
      `select n:count i by date from trade` (repeat for quote / bar / quoteBar).
      Or via Python: `HdbReader(...).bars("BTCUSDT", <day>, 60)`.
- [ ] `bar` and `quoteBar` partitions present → `featureTable` reads the fast path.

## 6. Ongoing

- Each UTC rollover: `savedown` persists trade + quote and materialises bar +
  quoteBar; `prune` trims partitions older than the window.
- The reader / dashboard / experiment CLIs run in a **separate process** from the
  writer (a table is in-memory *or* partitioned, never both) — see RESEARCH.md.

## 7. Restart / recovery

- Stop the writer with **SIGTERM/SIGINT** → the feedhandler logs
  `shutting down; flushing RDB to HDB` and `savedown`s the current day before
  exiting. No loss for the flushed day.
- On restart, `backfill.run` is **idempotent** (existing partitions skipped) and
  the feedhandler **re-bridges** any gap since the last stored id — the store
  stays gap-free across restarts.

## 8. Gotchas (surfaced by the dry run)

- **NTP on both Pis** (§Prerequisites) — the one clock dependency.
- **`HDB_PATH` on the SSD**, not the SD card — frequent writes.
- **Reader must be a separate process** from the writer.
- **`BACKFILL_DAYS`** sizes fill *and* retention; use 30/90 (0 is a valid no-op now, but pointless).
- **Single symbol** today; multi-symbol needs the pipeline generalised.
