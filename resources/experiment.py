"""CLI to run a purged walk-forward experiment end to end: read the labelled
feature table for a symbol over a date range, then evaluate a baseline model.

    python -m resources.experiment --symbol BTCUSDT --start 2026-08-11 --end 2026-08-13

It stitches together the two Phase 3/4 pieces — resources.reader (q analytics ->
pandas) and resources.evaluation (purged/embargoed walk-forward + metrics) — so
you can sweep features / horizon / cost / model from the shell without editing
code. Each day's feature table is self-contained (analytics.q drops the warm-up
head and unlabelled tail per day), so concatenating days introduces no cross-day
label leakage.

The reader/model paths need a licensed q + sklearn; the orchestration helpers
(date_range, load_features, format_report, the arg parser) are pure and tested.
"""
import argparse
import contextlib
import json
import logging
import os
import sys
from dataclasses import asdict
from datetime import timedelta

import pandas as pd

from resources.alpha import ImbalanceAlpha
from resources.evaluation import DEFAULT_FEATURES, evaluate
from resources.reader import HdbReader, ReaderConfig, coerce_date

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("experiment")


# --- pure orchestration helpers (tested) -----------------------------------

def date_range(start, end) -> list:
    """Inclusive list of dates from start to end (accepts date or date string)."""
    start, end = coerce_date(start), coerce_date(end)
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def load_features(reader, symbol: str, days: list, bar_seconds: int,
                  horizon: int, window: int, cost: float) -> pd.DataFrame:
    """Concatenate the per-day feature tables into one time-ordered frame.
    Days with no data (not in the HDB, or too few bars to survive warm-up) are
    logged and skipped. Raises if nothing usable is found."""
    frames = []
    for d in days:
        try:
            ft = reader.feature_table(symbol, d, bar_seconds,
                                      horizon=horizon, window=window, cost=cost)
        except Exception as exc:                       # missing partition, q error, ...
            logger.warning("skip %s: %s", d, exc)
            continue
        if len(ft):
            frames.append(ft)
        else:
            logger.warning("skip %s: no feature rows", d)
    if not frames:
        raise ValueError(f"no feature rows for {symbol} over {days[0]}..{days[-1]}")
    return pd.concat(frames, ignore_index=True)


def format_report(result, *, symbol: str, days: list, bar_seconds: int,
                  horizon: int, window: int, cost: float, label_mix: dict,
                  alpha: str = "logistic") -> str:
    """Human-readable summary of an EvaluationResult."""
    lines = [
        "nano_tick walk-forward experiment",
        f"  symbol={symbol}  dates={days[0]}..{days[-1]} "
        f"({len(days)} day(s), {result.n_samples} rows)",
        f"  alpha={alpha}  bar={bar_seconds}s  horizon={horizon}  window={window}  cost={cost}",
        f"  label mix: {label_mix}",
        f"  features: {', '.join(result.feature_cols)}",
        f"  folds={result.n_folds} (skipped {result.n_skipped})",
        "",
        "pooled out-of-sample metrics:",
    ]
    lines += [f"  {k:20s} {_fmt(v)}" for k, v in result.pooled.items()]
    lines += ["", "per fold:"]
    for i, f in enumerate(result.per_fold):
        lines.append(f"  fold{i}: train={f['train']:4d} test={f['test']:4d} "
                     f"acc={f['accuracy']:.3f} bal={f['balanced_accuracy']:.3f} "
                     f"trades={f['n_trades']:4d} net={f['total_net_return']:+.4f}")
    return "\n".join(lines)


def _fmt(v):
    return f"{v:.6f}" if isinstance(v, float) else str(v)


@contextlib.contextmanager
def _banner_to_stderr():
    """Redirect OS-level fd 1 to fd 2 for the duration of the block. pykx/KDB-X
    prints its Community-Edition banner to stdout at the C level on init (a
    Python-level redirect won't catch it); this keeps stdout clean so --json
    output stays machine-parseable. The banner still appears on stderr."""
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
        prog="resources.experiment",
        description="Purged walk-forward evaluation of a baseline model over an HDB date range.")
    p.add_argument("--symbol", default=cfg.symbol)
    p.add_argument("--start", required=True, help="first date, YYYY-MM-DD or YYYY.MM.DD")
    p.add_argument("--end", help="last date (inclusive); defaults to --start")
    p.add_argument("--bar-seconds", type=int, default=cfg.bar_seconds)
    p.add_argument("--horizon", type=int, default=cfg.horizon, help="label horizon in bars")
    p.add_argument("--window", type=int, default=cfg.window, help="trailing feature window")
    p.add_argument("--cost", type=float, default=cfg.cost, help="round-trip cost as a return")
    p.add_argument("--n-splits", type=int, default=5)
    p.add_argument("--embargo", type=int, default=0, help="extra bars purged before each test block")
    p.add_argument("--alpha", choices=["logistic", "imbalance"], default="logistic",
                   help="signal to evaluate: logistic (StandardScaler+LogReg) or "
                        "imbalance (order-flow/quote microstructure rule)")
    p.add_argument("--features", help="comma-separated feature columns (default: the standard set)")
    p.add_argument("--hdb", default=str(cfg.hdb_path), help="HDB path")
    p.add_argument("--analytics", default=str(cfg.analytics_q), help="analytics.q path")
    p.add_argument("--json", action="store_true", help="emit JSON instead of a text report")
    return p


# --- entry point (needs a licensed q + sklearn) ----------------------------

def main(argv: list) -> int:
    args = build_parser().parse_args(argv)
    days = date_range(args.start, args.end or args.start)
    features = [c.strip() for c in args.features.split(",")] if args.features else list(DEFAULT_FEATURES)

    with _banner_to_stderr():                          # keep the KDB-X banner off stdout
        reader = HdbReader(args.hdb, args.analytics)
    df = load_features(reader, args.symbol, days, args.bar_seconds,
                       args.horizon, args.window, args.cost)
    label_mix = {int(k): int(v) for k, v in df["label"].value_counts().sort_index().items()}
    factory = (lambda: ImbalanceAlpha(features)) if args.alpha == "imbalance" else None
    result = evaluate(df, model_factory=factory, n_splits=args.n_splits, horizon=args.horizon,
                      embargo=args.embargo, cost=args.cost, feature_cols=features)

    if args.json:
        print(json.dumps({
            "symbol": args.symbol, "alpha": args.alpha, "start": str(days[0]), "end": str(days[-1]),
            "bar_seconds": args.bar_seconds, "horizon": args.horizon,
            "window": args.window, "cost": args.cost, "embargo": args.embargo,
            "label_mix": label_mix, **asdict(result),
        }, indent=2))
    else:
        print(format_report(result, symbol=args.symbol, days=days,
                            bar_seconds=args.bar_seconds, horizon=args.horizon,
                            window=args.window, cost=args.cost, label_mix=label_mix,
                            alpha=args.alpha))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
