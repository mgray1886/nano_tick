"""Make the repo importable so one pytest run covers everything.

- Repo root on the path: `clients` and `resources` are packages
  (clients.binance, resources.backfill, resources.recorder, ...).
- ingest/ on the path: its code imports as `src.*` (as main.py runs on the Pi).
- tools/ on the path: `receiver` is a standalone script.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "ingest", ROOT / "tools"):
    sys.path.insert(0, str(p))
