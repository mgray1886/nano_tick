from datetime import date

import pandas as pd
import pytest

from resources import experiment as exp
from resources.evaluation import EvaluationResult


# --- date_range ------------------------------------------------------------

def test_date_range_inclusive_multiday():
    assert exp.date_range("2026-08-11", "2026-08-13") == [
        date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13)]


def test_date_range_single_day_dotted():
    assert exp.date_range("2026.08.11", "2026.08.11") == [date(2026, 8, 11)]


def test_date_range_end_before_start_raises():
    with pytest.raises(ValueError):
        exp.date_range(date(2026, 8, 13), date(2026, 8, 11))


# --- load_features ---------------------------------------------------------

class FakeReader:
    """feature_table returns a preset frame (or raises) per day; records calls."""

    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def feature_table(self, symbol, day, bar_seconds, horizon, window, cost):
        self.calls.append((symbol, day, bar_seconds, horizon, window, cost))
        v = self.per_day.get(day)
        if isinstance(v, Exception):
            raise v
        return v if v is not None else pd.DataFrame()


def _frame(n):
    return pd.DataFrame({"label": [1] * n, "ret": [0.0] * n})


def test_load_features_concatenates_days_in_order():
    days = exp.date_range("2026-08-11", "2026-08-13")
    reader = FakeReader({days[0]: _frame(2), days[1]: _frame(3), days[2]: _frame(4)})
    df = exp.load_features(reader, "BTCUSDT", days, 60, 1, 20, 0.002)
    assert len(df) == 9
    assert [c[1] for c in reader.calls] == days              # asked for every day, in order
    assert reader.calls[0] == ("BTCUSDT", days[0], 60, 1, 20, 0.002)


def test_load_features_skips_empty_and_erroring_days():
    days = exp.date_range("2026-08-11", "2026-08-13")
    reader = FakeReader({days[0]: _frame(2), days[1]: RuntimeError("missing"), days[2]: None})
    df = exp.load_features(reader, "BTCUSDT", days, 60, 1, 20, 0.002)
    assert len(df) == 2                                       # only day 0 contributed


def test_load_features_raises_when_nothing_usable():
    days = exp.date_range("2026-08-11", "2026-08-12")
    reader = FakeReader({})                                   # every day empty
    with pytest.raises(ValueError):
        exp.load_features(reader, "BTCUSDT", days, 60, 1, 20, 0.002)


# --- arg parsing -----------------------------------------------------------

def test_parser_defaults(monkeypatch):
    for v in ("SYMBOL", "BAR_SECONDS", "LABEL_HORIZON", "FEATURE_WINDOW", "ROUNDTRIP_COST"):
        monkeypatch.delenv(v, raising=False)
    args = exp.build_parser().parse_args(["--start", "2026-08-11"])
    assert args.symbol == "BTCUSDT"
    assert args.start == "2026-08-11" and args.end is None
    assert (args.bar_seconds, args.horizon, args.window, args.cost) == (60, 1, 20, 0.002)
    assert args.n_splits == 5 and args.embargo == 0


def test_parser_overrides():
    args = exp.build_parser().parse_args([
        "--symbol", "ethusdt", "--start", "2026-08-01", "--end", "2026-08-05",
        "--horizon", "3", "--embargo", "5", "--features", "ret,qimb", "--json"])
    assert args.symbol == "ethusdt" and args.end == "2026-08-05"
    assert args.horizon == 3 and args.embargo == 5
    assert args.features == "ret,qimb" and args.json is True


def test_parser_requires_start():
    with pytest.raises(SystemExit):
        exp.build_parser().parse_args([])


def test_parser_alpha_default_and_choice():
    assert exp.build_parser().parse_args(["--start", "2026-08-18"]).alpha == "logistic"
    assert exp.build_parser().parse_args(
        ["--start", "2026-08-18", "--alpha", "imbalance"]).alpha == "imbalance"
    with pytest.raises(SystemExit):
        exp.build_parser().parse_args(["--start", "2026-08-18", "--alpha", "nope"])


# --- report formatting -----------------------------------------------------

def test_format_report_contains_key_fields():
    result = EvaluationResult(
        n_samples=10, n_folds=2, n_skipped=1, feature_cols=["ret", "qimb"],
        pooled={"accuracy": 0.5, "balanced_accuracy": 0.5, "n_trades": 5,
                "hit_rate": 0.4, "mean_net_return": -0.0001,
                "total_net_return": -0.0005, "sharpe_per_bar": -0.02},
        per_fold=[{"train": 4, "test": 3, "accuracy": 0.5, "balanced_accuracy": 0.5,
                   "n_trades": 3, "total_net_return": 0.001},
                  {"train": 7, "test": 3, "accuracy": 0.5, "balanced_accuracy": 0.5,
                   "n_trades": 2, "total_net_return": -0.0015}])
    days = exp.date_range("2026-08-11", "2026-08-12")
    text = exp.format_report(result, symbol="BTCUSDT", days=days, bar_seconds=60,
                             horizon=1, window=20, cost=0.002, label_mix={-1: 5, 1: 5})
    assert "BTCUSDT" in text
    assert "balanced_accuracy" in text
    assert "fold0" in text and "fold1" in text
    assert "skipped 1" in text
    assert "alpha=logistic" in text                       # default shown

    text2 = exp.format_report(result, symbol="BTCUSDT", days=days, bar_seconds=60,
                              horizon=1, window=20, cost=0.002, label_mix={-1: 5, 1: 5},
                              alpha="imbalance")
    assert "alpha=imbalance" in text2
