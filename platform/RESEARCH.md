# nano_tick — research pipeline (storage → analytics → evaluation)

How the 4B turns raw ticks into cost-aware, leakage-safe features for
price-prediction research, and how to run each stage. This is the query/analysis
side; ingest and transport are in the top-level [README](../README.md), and
KDB-X install / backfill / retention design is in [KDBX_SETUP.md](KDBX_SETUP.md).

```
MQTT ticks/# quotes/#        data.binance.vision + REST
        │                            │
   feedhandler.py               backfill.py            ← write path (pykx + schema.q)
        └──────────┬─────────────────┘
                   ▼
          KDB-X HDB  (date-partitioned trade + quote)
                   │
             analytics.q   ← q feature functions (bars, quotes, as-of join, labels)
                   │
             reader.py     ← HdbReader: q → pandas DataFrame
                   │
           evaluation.py   ← purged walk-forward + cost-aware metrics + baseline model
                   │
           experiment.py   ← CLI: one command, symbol × date-range → report / JSON
```

Everything below runs on the 4B (or a dev box with a licensed KDB-X + `pykx`);
none of it touches the 3A+.

## Mental model

**kdb+tick storage.** Today's ticks accumulate in an in-memory **RDB** (`trade`,
`quote` tables); at the UTC day boundary `savedown` flushes the day to the
date-partitioned **HDB** on disk (splayed, `` p# `` attribute on `sym`) and
clears the RDB. A given table name is in-memory **or** partitioned in one q
process, never both — so the **reader always runs in a separate process from the
writer**.

**Feature discipline (why the results are trustworthy).**
- *No lookahead* — every feature uses only bars at or before the current one
  (trailing returns, rolling vol/flow). The **only** forward-looking column is
  the label (`fwdRet` over the next `h` bars); analytics.q drops the unlabelled
  tail so every stored row has a real label.
- *Cost-aware label* — the label is +1/0/−1 for "up / flat / down enough to beat
  the round-trip cost", not raw sign. A signal has to clear costs to score.
- *Purged, embargoed walk-forward* — the label peeks `h` bars forward, so
  training rows within `h` (+ an embargo buffer) of a test block are dropped;
  train is always strictly before test with a gap. This is what stops a model
  from "learning" test-period information.

## 1. Storage layer — `schema.q`, `feedhandler.py`, `backfill.py`

**Schema** ([schema.q](schema.q)) defines the `trade` and `quote` tables and the
write verbs: `insertRaw` / `insertRawQuote` (raw epoch-millis in, kdb timestamps
out), `savedown[date]` (`.Q.dpft` both tables to a partition, clear the RDB).

**Live feed** ([../resources/feedhandler.py](../resources/feedhandler.py))
subscribes to MQTT `ticks/#` and `quotes/#`, dedups on `trade_id` / `update_id`,
and inserts into the RDB, rolling to the HDB at the day boundary.

**Backfill** ([../resources/backfill.py](../resources/backfill.py)) preloads
history from the Binance daily archive + a REST bridge, and `prune` caps the
store to a rolling window. Full design + run instructions in
[KDBX_SETUP.md](KDBX_SETUP.md).

Bring up / refresh the store (needs a licensed q via pykx):

```bash
HDB_PATH=~/nano_tick_hdb python platform/app.py   # startup backfill + prune, then live feed
```

## 2. Analytics — `analytics.q`

Reader-side q functions over the HDB. Load into a q session mapped to the HDB
(`q ~/nano_tick_hdb` then `\l platform/analytics.q`) or call via pykx.

| function | returns |
|----------|---------|
| `bars[sym;date;size]` | OHLCV bars + VWAP, trade count, buy/sell volume, order-flow imbalance |
| `quoteBars[sym;date;size]` | per-bar best bid/ask → `mid`, `spread`, `micro` (size-weighted), `qimb` (quote imbalance) |
| `enrichTrades[t;q]` | as-of join — attach each trade its prevailing (nearest-earlier) quote |
| `featureTable[sym;date;size;h;w;cost]` | the labelled feature table: trailing features + cost-aware forward-return label, warm-up head and unlabelled tail dropped |
| `vwapDay[sym;date]` / `counts[sym]` | whole-day VWAP / trades-per-day coverage check |

`barsOf` / `quoteBarsOf` / `featuresOf` are the table-in / table-out cores the
HDB entry points wrap (used directly by the tests on synthetic tables).

## 3. Reader — `resources/reader.py`

`HdbReader` wraps the analytics functions and returns pandas, ready for
modelling. Run it in a **separate process** from any writer.

```python
from datetime import date
from resources.reader import HdbReader

r = HdbReader("~/nano_tick_hdb", "platform/analytics.q")
bars = r.bars("BTCUSDT", date(2026, 8, 18), 60)                 # 1-min OHLCV, bar-indexed
ft   = r.feature_table("BTCUSDT", date(2026, 8, 18), 60,        # labelled features
                       horizon=1, window=20, cost=0.002)
```

`ReaderConfig.from_env()` + `open_reader(cfg)` attach defaults (symbol, bar size,
horizon, window, cost) so the methods can be called with just symbol/date.
Gotcha baked in: `\l <hdb>` chdirs the whole process, so the reader resolves
`analytics.q` to an absolute path *before* loading the HDB.

## 4. Evaluation — `resources/evaluation.py`

- `walk_forward_splits(n, n_splits, horizon, embargo)` — the leakage-critical
  splitter: expanding-window folds, purge + embargo, train strictly before test.
- `feature_matrix(df)` — pulls X/y/fwdRet, using trailing stationary features
  (excludes price levels and the forward columns).
- `evaluate(df, ...)` — model-agnostic harness (any sklearn-style fit/predict);
  fits a fresh model per fold, pools out-of-sample predictions, reports:
  - **classification**: accuracy, **balanced accuracy** (macro recall — so a lazy
    majority predictor doesn't look good);
  - **cost-aware strategy PnL**: act on the signal (long/short/flat) net of the
    round-trip cost → `total_net_return`, `hit_rate`, `sharpe_per_bar`.
- `make_baseline_model()` — StandardScaler + LogisticRegression (sklearn imported
  lazily, so the splitter/metrics need no ML dependency).

## 5. Experiment CLI — `resources/experiment.py`

One command runs the whole chain over a symbol and date range:

```bash
python -m resources.experiment --symbol BTCUSDT --start 2026-08-18 --end 2026-08-20
```

| flag | meaning (default) |
|------|-------------------|
| `--start` / `--end` | date range, `YYYY-MM-DD` or `YYYY.MM.DD` (`--end` → single day) |
| `--bar-seconds` | bar width (60) |
| `--horizon` | label look-ahead in bars, also the purge distance (1) |
| `--window` | trailing feature window (20) |
| `--cost` | round-trip cost as a return — used for **both** the label and the strategy PnL (0.002) |
| `--n-splits` / `--embargo` | walk-forward folds (5) / extra purge buffer (0) |
| `--features` | comma-separated feature subset (the standard 8) |
| `--hdb` / `--analytics` | paths (env `HDB_PATH` / repo) |
| `--json` | machine-readable output for scripting sweeps |

It loads each day's feature table, skips days absent from the HDB (with a
warning), concatenates (each day is leakage-self-contained), and evaluates.
`--json` stdout is pure JSON — the CLI redirects the KDB-X banner (which pykx
prints to stdout on init) to stderr.

### Reading the output

On a 3-day HDB with a *planted* qimb→return edge, varying only `--cost`:

| data / cost | balanced acc | net PnL | trades | verdict |
|-------------|--------------|---------|--------|---------|
| random walk, cost 0 | 0.50 | ≈0 | — | no signal (and no leakage) |
| planted edge, cost 0.0003 | 0.53 | **+0.32** (every fold +ve) | 723 | real, tradeable edge |
| planted edge, cost 0.003 | 0.33 | 0.00 | 0 | edge real but **eaten by costs** — model goes flat |

The high-cost row is the cautionary case: raw **accuracy 0.97** looks great but
**balanced accuracy 0.33** (chance) exposes it as pure majority-class ("flat")
prediction — no skill, zero trades. Always read balanced accuracy and the
cost-aware PnL together, never raw accuracy alone. On real market data the honest
expectation is much closer to the random-walk row.

## Testing

- **Python unit tests** — `pytest` from the repo root. All external boundaries
  (websockets, MQTT, pykx, sklearn) are mocked; the splitter, metrics, reader
  dispatch, and CLI orchestration are covered directly. Deps: `requirements-dev.txt`.
- **q tests** — [run_q_tests.py](run_q_tests.py) runs `schema_test.q` and
  `analytics_test.q` under pykx (strips q's `exit`, reads each file's `fails`
  counter, one subprocess per file). Run `python platform/run_q_tests.py`; CI
  runs it in the gated `q-tests` job (needs the `KDB_LICENSE_B64` secret).
