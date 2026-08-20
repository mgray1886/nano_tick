# KDB-X on the 4B — licensing & install prep

Prep steps for the KDB+ milestone (the project's main goal: KDB-X as the
primary tick store, queried with q). The schema, feedhandler, backfill,
analytics, and evaluation are now implemented and dev-verified — see
[RESEARCH.md](RESEARCH.md) for the storage → analytics → evaluation runbook.
This doc covers the remaining external prerequisite: licensing and installing
KDB-X on the 4B (plus the backfill/retention design notes below).

Use **KDB-X Community Edition** (GA Nov 2025) — not the legacy "kdb+ Personal
Edition". The Community Edition is free, has no expiry, allows offline and
commercial use, and supports Linux ARM64. The legacy personal edition's
always-on internet license check and patchy ARM support don't apply.

## Steps

1. **Sign up (free) at the KX Developer Center** → you receive a
   base64-encoded `kc.lic` license key. This is the only licensing step.

2. **Confirm the 4B runs a 64-bit OS** — KDB-X is ARM64 (aarch64) only:

   ```bash
   uname -m   # must print: aarch64  (armv7l = 32-bit OS, reflash needed)
   ```

   (Only the 4B needs this; kdb never runs on the 3A+.)

3. **Install on the 4B** — the Developer Center provides a one-line
   `curl`-based install command that takes the license key.
   Prerequisites: `bash`, `curl`, `unzip`. An air-gapped install path exists
   if the 4B is ever offline (download bundle elsewhere, transfer, install).

4. **Verify** — launch `q`; if the interpreter starts, licensing and
   platform are settled.

5. **Skim the Community Edition resource limits** — free tier is
   feature-complete "within defined resource limits" aimed at large servers;
   a 4B sits far below any plausible cap, but confirm before designing.

## When the milestone starts

Natural first slice: kdb+ running on the 4B, a minimal `trade` table schema
in q, and a small feedhandler subscribing to MQTT `ticks/#` and inserting —
live q queries from day one. The NDJSON recorder (`platform/recorder.py`)
becomes bootstrap/fallback; its files can backfill the HDB.

## Rolling 90-day window (backfill + retention)

Decided 2026-07-16. Goal: on startup the store already holds ~3 months of
history with **no gap** between the preloaded data and where the live stream
begins, and storage stays bounded (old data pruned as new arrives). Backfill
and retention are one concept — a single `RETENTION_DAYS` knob (default 90):
backfill fills *up to* it, the prune keeps it *from exceeding* it.

**Runs on the 4B** — the 4B has its own internet (WiFi), so backfill fetches
straight to the SSD/HDB. It does *not* route through the 3A+ or MQTT (that
would shove ~10–25GB through a 512MB WiFi box). The 3A+ ingest is unchanged.

**Gap-free backfill — reconcile by `trade_id`:**
- The live feed/recorder is already dedup-keyed on `trade_id` and records
  from the first live id `F` onward.
- Backfill covers `[90 days ago → F)`, overlapping into the live range;
  dedup drops the overlap. Binance spot `trade_id`s are contiguous per
  symbol, so no-gap ⟺ the id sequence has no holes (verifiable after load).
- Two sources: **data.binance.vision** daily archive for the bulk (fast, lags
  ~1 day) + REST `GET /api/v3/trades?fromId=` paging to bridge the archive's
  end up to `F`.
- **Use individual `trades`, NOT `aggTrades`** — aggTrades use a separate id
  space and would not reconcile with the `@trade` live stream.

**Retention — date-partition prune:** data is date-partitioned, so retention is
just dropping any partition older than the window. Implemented as
`backfill.prune` (see below); a daily timer can drive it. Idempotent,
self-healing if it misses a day, and identical for NDJSON (`rm` old file) and
KDB-X (`rm` old partition dir). Preferred over ad-hoc end-of-day deletion.
(Current single knob is `BACKFILL_DAYS`; the design's `RETENTION_DAYS` folds
into it — one number sizes both fill and retention.)

**Storage-agnostic core:** write fetch → normalise → reconcile once, with the
write target (kdb insert vs NDJSON append) and prune target behind a seam, so
it serves the HDB directly and can fall back to NDJSON.

### Backfill resource — implemented 2026-07-16 (`platform/backfill.py`)

`backfill.py` is a **resource module**, not a script: the writer app
(`platform/app.py`) calls `backfill.run(config, writer=...)` on startup,
passing the app's shared `HdbWriter` so history and the (future) live feed
share one kdb connection. `backfill.run` is idempotent, so it's safe to call
on every start.

Interim window is **30 days** (`BACKFILL_DAYS=30`); raise to 90 later. Run the
writer app on the 4B after the HDB exists (needs a licensed q via pykx):

```bash
HDB_PATH=~/nano_tick_hdb python platform/app.py
```

`app.py` runs startup backfill then (once built) the live MQTT→kdb feed. For a
standalone/manual gap-fill you can also call `backfill.run(BackfillConfig
.from_env())` directly, which creates its own writer when none is passed.

What it does: for each of the last N complete days (archive lags ~1 day, so
`[today-N .. yesterday]`), downloads `data.binance.vision` daily `trades`,
verifies the `.CHECKSUM`, streams the CSV in chunks, and writes each day as an
HDB partition via `schema.q`'s `insertRaw` + `savedown`. **Idempotent** — days
already present (a `<hdb>/YYYY.MM.DD/trade` dir) are skipped, so re-running
only fills gaps. Streams in chunks (`BACKFILL_CHUNK`, default 500k rows) to
stay within the 2GB 4B's memory.

Config (env): `SYMBOL` (BTCUSDT), `VENUE` (binance), `BACKFILL_DAYS` (30),
`HDB_PATH` (~/nano_tick_hdb), `BACKFILL_CHUNK` (500000).

**Retention (`backfill.prune`, added 2026-07-25):** `prune(config)` deletes any
HDB partition older than the window (`today - config.days`), so the store is a
fixed rolling window rather than growing forever. `app.py` calls it right after
startup backfill — run() fills the window, prune() enforces its size. The
cutoff equals backfill's oldest day, so prune never deletes a day run() just
filled (no thrash). Filesystem-only (no pykx), idempotent, and only
date-partition dirs are ever removed (the `sym` enum file and non-date dirs are
untouched). A daily timer can call run()+prune() to keep the window current.
`BACKFILL_DAYS` is the single knob — it sizes both the fill and the retention.

**Not yet done (deliberate):**
- *Gap-free bridge to live.* The archive stops at yesterday; bridging
  yesterday's-end → the live stream's first `trade_id` (via REST `fromId`)
  needs the live feedhandler running to define the cutover id, so it belongs
  with that feedhandler, not here. Backfill only lays down complete days.

**Verification status:** the pure-Python core (config, fetch, checksum, chunked
parse, date planning, idempotency, orchestration) is covered by
`tests/test_backfill.py`. The pykx write path (`HdbWriter`) needs a **licensed
q** and was verified on KDB-X Community in WSL: `insertRaw` → `savedown` writes
a splayed, date-partitioned, `p#`-attributed partition that reads back with
correct types (ms→timestamp conversion confirmed) and a correct vwap. Since
q/pykx is architecture-independent, this gives high confidence for the ARM 4B;
re-confirm there with a fresh session: `q ~/nano_tick_hdb`, then
`select cnt:count i by dt:date from trade`.

Bridge gotchas fixed during that verification (all in `HdbWriter`): `\l` needs
an **absolute** path (pykx q resolves relative to its own cwd); the HDB dir
must exist before `savedown`; and a Python `str` maps to a q **symbol**, so
pass a `SymbolAtom` and `hsym` it rather than casting a string with `` `$ ``.

## Sources

- [KDB-X Community Edition announcement](https://kx.com/news-room/kx-debuts-developer-built-kdb-x-community-edition-transforming-time-series-and-real-time-data-for-the-ai-era/)
- [KDB-X install docs](https://code.kx.com/kdb-x/get_started/kdb-x-install.html)
- [KDB-X GA blog](https://kx.com/blog/kdb-x-ga-built-for-developers/)
- [Legacy kdb+ licensing docs (for contrast)](https://code.kx.com/q/learn/licensing/)
