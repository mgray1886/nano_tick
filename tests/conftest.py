"""Make each role folder importable so one pytest run covers the whole repo.

ingest/ code imports as `src.*` (matching how main.py runs on the Pi);
platform/recorder.py and tools/receiver.py are standalone scripts imported
as top-level modules `recorder` and `receiver`.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for role in ("ingest", "platform", "tools"):
    sys.path.insert(0, str(ROOT / role))
