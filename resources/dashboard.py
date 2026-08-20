"""CLI to render a self-contained candlestick dashboard from HdbReader.bars.

    # range mode — a symbol over N days
    python -m resources.dashboard --symbol BTCUSDT --start 2026-08-18 --end 2026-08-20

    # window mode — snapshot ±30 min around an instant, marked (a trade-analysis view)
    python -m resources.dashboard --symbol BTCUSDT --center "2026-08-18 10:30" \\
        --window 30 --center-label "alpha buy" --center-side buy

Reads OHLCV bars and writes a single standalone HTML file (no server, no external
assets — inline Canvas renderer) that opens in any browser. Pairs with
resources.experiment: same reader, a visual view instead of metrics. Window mode
loads the day(s) the window spans, trims to the window, and drops a marker at the
centre — the building block for v2 trade snapshots.

`markers` is an optional overlay of points on the time axis
(``[{"t": <epoch ms>, "label": str, "side": "buy"|"sell"}]``) drawn as pins —
the seam for a future trade-snapshot view (mark where an alpha executed, inspect
the surrounding price action). Unused by default.

The HTML-generation helpers are pure and tested; the reader/HDB path is verified
live. The Canvas template lives in dashboard_template.html.
"""
import argparse
import contextlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from resources.reader import HdbReader, ReaderConfig, coerce_date

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("dashboard")

TEMPLATE_PATH = Path(__file__).resolve().parent / "dashboard_template.html"


# --- pure helpers (tested) -------------------------------------------------

def date_range(start, end) -> list:
    """Inclusive list of dates from start to end (date or date string)."""
    start, end = coerce_date(start), coerce_date(end)
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def parse_instant(value) -> int:
    """Epoch milliseconds (UTC) from either an epoch int-string (ms, or seconds
    which are scaled up) or an ISO-ish datetime — the format v2 would log a trade
    time in. Accepts `2026-08-18`, `2026-08-18 13:30`, `2026-08-18T13:30:00`."""
    s = str(value).strip()
    if s.isdigit():
        n = int(s)
        return n if n >= 10 ** 11 else n * 1000          # tolerate epoch seconds
    norm = s.replace("Z", "").replace("T", " ")
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
              "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M", "%Y.%m.%d"):
        try:
            return int(datetime.strptime(norm, f).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"could not parse instant: {value!r}")


def window_bounds(center_ms: int, window_min: int) -> tuple:
    """(lo, hi) epoch-ms bounds ±window_min minutes around center_ms."""
    w = int(window_min) * 60_000
    return center_ms - w, center_ms + w


def window_days(lo_ms: int, hi_ms: int) -> list:
    """The UTC dates the [lo, hi] window spans (a window can straddle midnight)."""
    lo = datetime.fromtimestamp(lo_ms / 1000, tz=timezone.utc).date()
    hi = datetime.fromtimestamp(hi_ms / 1000, tz=timezone.utc).date()
    return [lo + timedelta(days=n) for n in range((hi - lo).days + 1)]


def filter_window(df, lo_ms: int, hi_ms: int):
    """Rows of a bar-indexed frame whose bar time falls in [lo, hi] (epoch ms)."""
    ms = (df.index - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)
    return df[(ms >= lo_ms) & (ms <= hi_ms)]


def _iso_min(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def ohlcv_records(df, price_dp: int = 2, vol_dp: int = 4) -> list:
    """[[epoch_ms, o, h, l, c, v], ...] from a bar-indexed OHLCV DataFrame
    (the shape HdbReader.bars returns)."""
    # resolution-independent epoch ms (the index may be datetime64[ns] or [ms])
    ms = ((df.index - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)).tolist()
    o, h, ln = df["open"], df["high"], len(df)
    lo, c, v = df["low"], df["close"], df["vol"]
    return [[int(ms[i]), round(float(o.iloc[i]), price_dp), round(float(h.iloc[i]), price_dp),
             round(float(lo.iloc[i]), price_dp), round(float(c.iloc[i]), price_dp),
             round(float(v.iloc[i]), vol_dp)] for i in range(ln)]


def render_html(records: list, *, symbol: str, day_label: str, bar_seconds: int,
                sma: int = 14, markers=None, note=None, template=TEMPLATE_PATH) -> str:
    """Inject an OHLCV payload into the standalone HTML template. `</` is escaped
    so a stray `</script>` inside a marker label can't break out of the script."""
    payload = {
        "meta": {"symbol": symbol, "bar_seconds": bar_seconds, "range": day_label,
                 "n": len(records), "note": note or "candles = tick OHLCV"},
        "ohlcv": records, "markers": markers or [], "sma": sma,
    }
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    return Path(template).read_text(encoding="utf-8").replace("__PAYLOAD__", blob)


# --- reader boundary + orchestration ---------------------------------------

def load_bars(reader, symbol: str, days: list, bar_seconds: int) -> pd.DataFrame:
    """Concatenate per-day OHLCV bars into one time-ordered frame; days with no
    data (not in the HDB) are logged and skipped. Raises if nothing is found."""
    frames = []
    for d in days:
        try:
            b = reader.bars(symbol, d, bar_seconds)
        except Exception as exc:                       # missing partition, q error, ...
            logger.warning("skip %s: %s", d, exc)
            continue
        if len(b):
            frames.append(b)
        else:
            logger.warning("skip %s: no bars", d)
    if not frames:
        raise ValueError(f"no bars for {symbol} over {days[0]}..{days[-1]}")
    return pd.concat(frames)


def build_dashboard(reader, symbol: str, days: list, bar_seconds: int,
                    *, sma: int = 14, markers=None) -> str:
    df = load_bars(reader, symbol, days, bar_seconds)
    label = str(days[0]) if len(days) == 1 else f"{days[0]} → {days[-1]}"
    return render_html(ohlcv_records(df), symbol=symbol, day_label=label,
                       bar_seconds=bar_seconds, sma=sma, markers=markers)


@contextlib.contextmanager
def _banner_to_stderr():
    """Keep the KDB-X banner (pykx prints it to stdout at the C level on init)
    off this process's stdout — matches resources.experiment."""
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        yield
    finally:
        sys.stdout.flush()
        os.dup2(saved, 1)
        os.close(saved)


def build_parser() -> argparse.ArgumentParser:
    cfg = ReaderConfig.from_env()
    p = argparse.ArgumentParser(
        prog="resources.dashboard",
        description="Render a standalone candlestick dashboard from HdbReader.bars.")
    p.add_argument("--symbol", default=cfg.symbol)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--start", help="range mode: first date, YYYY-MM-DD or YYYY.MM.DD")
    src.add_argument("--center", help="window mode: an instant (epoch ms or "
                                      "YYYY-MM-DD[THH:MM]) to snapshot a window around")
    p.add_argument("--end", help="range mode: last date (inclusive); defaults to --start")
    p.add_argument("--window", type=int, default=30, help="window mode: ± minutes around --center")
    p.add_argument("--center-label", default="center", help="window mode: label for the --center marker")
    p.add_argument("--center-side", choices=["buy", "sell"], help="window mode: colour the --center marker")
    p.add_argument("--bar-seconds", type=int, default=cfg.bar_seconds)
    p.add_argument("--sma", type=int, default=14, help="moving-average period (bars)")
    p.add_argument("--out", default="dashboard.html", help="output HTML path")
    p.add_argument("--markers", help="path to JSON: [{t: epoch_ms, label, side}]")
    p.add_argument("--hdb", default=str(cfg.hdb_path))
    p.add_argument("--analytics", default=str(cfg.analytics_q))
    return p


def main(argv: list) -> int:
    args = build_parser().parse_args(argv)
    # Resolve paths BEFORE building the reader: HdbReader's `\l <hdb>` chdirs the
    # whole process, so a relative --out/--markers would resolve against the HDB.
    out_path = Path(args.out).resolve()
    markers = (json.loads(Path(args.markers).resolve().read_text(encoding="utf-8"))
               if args.markers else [])

    window = None
    if args.center:                                    # window mode: snapshot around an instant
        center_ms = parse_instant(args.center)
        window = window_bounds(center_ms, args.window)
        days = window_days(*window)
        marker = {"t": center_ms, "label": args.center_label}
        if args.center_side:
            marker["side"] = args.center_side
        markers = markers + [marker]
    else:                                              # range mode
        days = date_range(args.start, args.end or args.start)

    with _banner_to_stderr():
        reader = HdbReader(args.hdb, args.analytics)
    df = load_bars(reader, args.symbol, days, args.bar_seconds)
    if window:
        df = filter_window(df, *window)
        if not len(df):
            raise ValueError(f"no bars within ±{args.window}min of {args.center}")
        label = f"±{args.window}min · {_iso_min(center_ms)} UTC"
    else:
        label = str(days[0]) if len(days) == 1 else f"{days[0]} → {days[-1]}"

    html = render_html(ohlcv_records(df), symbol=args.symbol, day_label=label,
                       bar_seconds=args.bar_seconds, sma=args.sma, markers=markers)
    out_path.write_text(html, encoding="utf-8")
    print(f"wrote {out_path} ({len(html):,} bytes) for {args.symbol} · {label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
