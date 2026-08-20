# nano_tick

Market-data pipeline on two Raspberry Pis: a Pi 3A+ streams trades and
bookTicker quotes from Binance over websocket, normalises them, and forwards
them over a dedicated ethernet link to a Pi 4B, which stores them in KDB-X and
turns them into cost-aware, leakage-safe features for price-prediction research.

```
Binance ──wss──> [Pi 3A+ (WiFi)] ──ethernet (point-to-point)──> [Pi 4B] ──> KDB-X HDB
                  nano_tick        MQTT QoS 1                    mosquitto      │
                  trades+quotes                                                 └─> q analytics
                                                                                    → features → eval
```

The transport/infra side is documented below; the 4B storage → analytics →
evaluation pipeline has its own runbook in
[platform/RESEARCH.md](platform/RESEARCH.md).

## Layout

- `ingest/` — everything that deploys to the 3A+:
  - `main.py` — wires stream → normaliser → sink; systemd entry point
  - `src/streams/` — websocket streams (`WebsocketStream` base handles reconnect/backoff)
  - `src/normalisers/` — venue message → flat tick dict
  - `src/sinks/mqtt.py` — MQTT QoS 1 publisher (default transport)
  - `src/sinks/tcp.py` — legacy acked TCP forwarder (`SINK_TYPE=tcp` fallback)
  - `setup.sh` / `ingest.service` — one-shot provisioning + systemd unit
- `platform/` — everything that deploys to the 4B (store / query / research):
  - `schema.q` — `trade` + `quote` tables and the write verbs (`insertRaw`, `savedown`)
  - `analytics.q` — q feature functions (bars, quote features, as-of join, labels)
  - `app.py` — 4B writer entry point (startup backfill + prune, then the live feed)
  - `*_test.q` + `run_q_tests.py` — q unit tests and their pykx runner
  - broker config, recorder service, `setup.sh`
  - `KDBX_SETUP.md` — KDB-X licensing / install / backfill+retention design
  - `RESEARCH.md` — **the storage → analytics → evaluation runbook** (start here for the query side)
- `resources/` — the 4B data-handling modules (imported by `platform/app.py` and the CLIs):
  `backfill.py` (archive + REST backfill, prune, RDB rollover), `feedhandler.py` (MQTT → kdb),
  `recorder.py` (NDJSON fallback), `reader.py` (`HdbReader`: q → pandas),
  `evaluation.py` (purged walk-forward + cost-aware metrics), `experiment.py` (the eval CLI),
  `binance.py` (parsing/normalising)
- `clients/` — external HTTP clients (`binance.py`: archive download + REST `trades`)
- `tools/receiver.py` — reference MQTT subscriber (protocol example, no persistence)
- `tests/` — whole-repo Python unit tests; run `pytest` from the root (deps: `requirements-dev.txt`)
- `.github/workflows/ci.yml` — on every push: flake8 + unit tests (Python 3.13/3.14) + q tests + docker build check

## Ethernet link setup (3A+ ⇄ 4B)

The 3A+ has **no ethernet port** — use a USB-to-ethernet adapter (USB 2.0 /
100Mbps is ample; tick flow peaks well under 1Mbps). Connect the two Pis
directly with a single cable, no switch needed. The 3A+ keeps internet via
WiFi; the wired link is a private point-to-point subnet chosen not to clash
with a typical home LAN (192.168.0.x / 192.168.1.x):

| Host  | Interface           | Address           |
|-------|---------------------|-------------------|
| 3A+   | eth0 (USB adapter)  | 192.168.100.1/24  |
| 4B    | eth0 (built-in)     | 192.168.100.2/24  |

On Raspberry Pi OS Bookworm (NetworkManager):

```bash
# On the 3A+
sudo nmcli con add type ethernet ifname eth0 con-name p2p \
    ipv4.method manual ipv4.addresses 192.168.100.1/24
sudo nmcli con up p2p

# On the 4B
sudo nmcli con add type ethernet ifname eth0 con-name p2p \
    ipv4.method manual ipv4.addresses 192.168.100.2/24
sudo nmcli con up p2p
```

No gateway on this connection — it is deliberately non-routed so ethernet
carries only tick traffic and the 3A+ still reaches Binance via WiFi.
On older Bullseye images use `/etc/dhcpcd.conf` instead:

```
interface eth0
static ip_address=192.168.100.1/24    # .2 on the 4B
```

Verify with `ping 192.168.100.2` from the 3A+, then set up the broker below.

## 4B setup (broker + recorder)

Clone the repo into the home directory on the 4B and run:

```bash
cd nano_tick/platform && ./setup.sh
```

That installs mosquitto with the bundled config (`platform/mosquitto-nano_tick.conf`:
bound to the private link only, anonymous — the p2p subnet is non-routed —
persistence on, queue cap), plus the **recorder service**: the first real
consumer, subscribing to `ticks/#` and appending each tick as NDJSON to

```
~/nano_tick_data/<venue>/<symbol>/<YYYY-MM-DD>.ndjson
```

Files are flushed every second (the 4B records to SSD, so frequent writes
are cheap and the power-cut loss window stays ~1s) and gzipped on date
rollover. Raw volume is ~260MB/day/symbol at average trade rates, ~10x
smaller compressed. Make sure `DATA_DIR` points at the SSD mount, not the SD
card — override it (and `MQTT_HOST`) in `platform/.env` if needed.

Watch live ticks with:

```bash
mosquitto_sub -h 192.168.100.2 -t 'ticks/#' -v
```

Consumers subscribe to `ticks/<venue>/<symbol>` (e.g. `ticks/binance/btcusdt`)
at QoS 1; `tools/receiver.py` is a minimal reference subscriber. Multiple
consumers can subscribe independently — no sink changes needed.

## Configuration (`.env`)

| Var               | Default         | Meaning                                          |
|-------------------|-----------------|--------------------------------------------------|
| `SYMBOL`          | `btcusdt`       | Binance symbol to stream                         |
| `SINK_TYPE`       | `mqtt`          | transport: `mqtt` or `tcp` (legacy fallback)     |
| `MQTT_HOST`       | `192.168.100.2` | broker address on the point-to-point link        |
| `MQTT_PORT`       | `1883`          | broker port                                      |
| `MQTT_MAX_QUEUED` | `50000`         | publisher-side queue cap while broker unreachable |
| `LOG_LEVEL`       | `INFO`          | journald verbosity                               |

(`SINK_HOST` / `SINK_PORT` / `SINK_BUFFER_MB` apply only when `SINK_TYPE=tcp`.)

## Delivery semantics

At-least-once end to end (MQTT QoS 1), so consumers dedupe on
`(venue, symbol, trade_id)`. While the broker is unreachable the publisher
queues up to `MQTT_MAX_QUEUED` ticks in RAM (~8+ min at extreme peak rates,
hours at normal rates) before dropping oldest, logged. While a *consumer* is
down, the broker holds its QoS 1 messages (persistent session + `persistence
true`), surviving broker restarts. Trades occurring while disconnected from
*Binance* are not replayed by the stream — REST backfill via `trade_id` gaps
is future work.

## Local testing (Docker)

`docker compose up --build` runs the full topology on a dev machine: a
`broker` container (mosquitto, standing in for the 4B), `ingest` running
`main.py` against real Binance data, `receiver` running the reference
subscriber, and `recorder` writing NDJSON to `./tmp/recorder/`. Healthy
output: ingest logs "connected to mqtt broker", receiver/recorder log a
non-zero ticks/s stats line every 10s, and dated `.ndjson` files grow under
`./tmp/recorder/binance/btcusdt/`.

Failure drills worth running:

```bash
# Consumer outage: broker must hold ticks and replay them on return
docker compose stop receiver && sleep 60 && docker compose start receiver
# -> receiver's "total" count jumps to catch up after restart

# Broker outage: ingest must queue (bounded) and resume cleanly
docker compose stop broker && sleep 30 && docker compose start broker
# -> ingest logs disconnect then reconnect; no ticks lost within queue cap
```

`docker compose down` tears everything down. Dev-only Python deps live in
`requirements-dev.txt` (`pip install -r requirements-dev.txt`); the Pis
install their own role folder's `requirements.txt` via the setup scripts.

### Hardware-mimicking resource limits

Each service in `docker-compose.yml` sets `deploy.resources.limits.memory` to
**the exact `MemoryMax` from its real systemd unit**, so the stack hits the
same failure modes the Pis would.

| Service    | Stands in for | CPU | Memory cap | Source                                          |
|------------|---------------|-----|------------|-------------------------------------------------|
| `ingest`   | Pi 3A+        | 1.0 | 256M       | `ingest.service` `MemoryMax=256M`               |
| `recorder` | Pi 4B         | 1.0 | 384M       | `recorder.service` `MemoryMax=384M`             |
| `broker`   | Pi 4B         | 1.0 | *(none)*   | stock mosquitto unit has no cap — bounded by 2GB box |
| `receiver` | —             | 1.0 | 256M       | debug tool, not deployed on the 4B; modest cap only |

**There are no memory floors — on the Pis or here — because the Pis don't have
any.** Linux memory is free-for-all: whichever process allocates first gets the
RAM, and if a box fills up the kernel OOM-killer picks a victim by `oom_score`.
The units deliberately set hard `MemoryMax` *ceilings* (never `MemoryMin`/
`MemoryLow` floors) so that **systemd** kills and restarts a runaway service
rather than leaving the kernel to take out something random like sshd or
mosquitto. The compose `restart:` policies mirror each unit's `Restart=always`
so a memory-kill self-heals the same way. (Docker has no clean equivalent of
the units' softer `MemoryHigh` throttle, so only the hard ceiling is modelled.)

**CPU caps are approximate** — a dev-machine core is far faster per-cycle than
a Pi's Cortex-A5x, so `cpus: "1.0"` bounds CPU *time* but a throttled container
still outruns a real Pi core. To surface CPU-bound bottlenecks, lower `cpus`
(e.g. `0.3`–`0.5`), or test on real hardware.

**Known gap — the 4B's shared 2GB isn't modelled as an aggregate.** The real
broker and recorder compete for one 2GB pool (the broker uncapped, the recorder
capped at 384M), and the box OOMs when the *total* is exhausted. Per-service
Docker limits can't express that shared ceiling. If you need to test 4B-wide
memory exhaustion, put the 4B services in a shared cgroup slice capped at 2G
(cgroup v2 is available on this host) via `cgroup_parent:` — left out by
default because that slice needs host setup that doesn't survive a Docker
Desktop restart.

## CI

`.github/workflows/ci.yml` runs on **every push to any branch** (plus manual
trigger via `workflow_dispatch`). Three parallel jobs:

| Job            | What it checks                                                              |
|----------------|-----------------------------------------------------------------------------|
| `lint`         | `flake8` over the whole repo (config in `.flake8`: 110-char lines) and `bash -n` syntax checks on both Pi setup scripts |
| `test`         | `pytest` on Python **3.13 and 3.14** — 3.13 is the canonical version (Raspberry Pi OS Lite/Trixie on the Pis, and the Docker image); 3.14 is an upgrade canary for the next OS bump |
| `q-tests`      | `schema_test.q` + `analytics_test.q` via `platform/run_q_tests.py` (pykx). Needs a KDB-X license in the `KDB_LICENSE_B64` secret; **skips with a warning** when absent, so forks aren't blocked |
| `docker-build` | `docker compose build` — catches Dockerfile/compose drift (e.g. a renamed folder no longer `COPY`ed) without needing Binance access |

Details:

- Tooling versions are pinned in `requirements-dev.txt`, so CI and local runs
  always agree. Run the same checks locally with `flake8` and `pytest` from
  the repo root.
- pip downloads are cached, keyed on both requirements files.
- A `concurrency` group cancels a superseded in-progress run when a newer
  push lands on the same branch.
- The unit tests mock all external boundaries (no network), so CI runs them
  in under a second. The Docker failure drills above stay manual — they need
  live Binance data; if CI coverage is ever wanted there, use a scheduled
  workflow rather than per-push.

## Roadmap

**Built and dev-verified** (against KDB-X Community Edition in WSL; not yet
deployed on the physical Pis — see [platform/RESEARCH.md](platform/RESEARCH.md)):

- [x] 3A+ ingest: trades **and bookTicker quotes** via Binance combined streams
- [x] 4B recorder (NDJSON) — bootstrap / fallback persistence
- [x] **KDB-X tick store on the 4B, queried with q** (the project's main goal):
  - [x] `schema.q` `trade`/`quote` tables + RDB → date-partitioned HDB savedown
  - [x] `feedhandler.py` — MQTT `ticks/#` + `quotes/#` → kdb, day-boundary rollover
  - [x] Rolling-window backfill (bulk archive + REST bridge, reconciled by
        `trade_id`) + date-partition prune; one `BACKFILL_DAYS` knob sizes both
- [x] q analytics (`analytics.q`): OHLCV/flow bars, quote features
      (mid/spread/microprice/imbalance), as-of join, cost-aware no-lookahead labels
- [x] `HdbReader` — q → pandas DataFrames
- [x] Purged/embargoed walk-forward evaluation + baseline model + `experiment` CLI
- [x] Tests: Python `pytest` + q tests, both in CI

**Next:**

- [ ] **Deploy live on the Pis** — collect real quote data (bookTicker is
      live-only), then run the evaluation on real market data. The real milestone;
      everything above is proven only on synthetic / archived-trade data.
- [ ] Harden the REST bridge (API key + weight-aware rate limiting) for large
      cold-start backfills
- [ ] Incremental bar-table precompute (materialise bars to speed repeated queries)
- [ ] Multi-instrument (combined streams already support it; pipeline is single-symbol)
