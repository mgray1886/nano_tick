"""Phase 4: purged, embargoed walk-forward evaluation of a baseline model.

Consumes the labelled feature table produced by platform/analytics.q (via
resources.reader.HdbReader.feature_table) and evaluates how well a model
predicts the cost-aware forward-return label WITHOUT leakage.

Two leakage guards matter here:
  * The features are already trailing (analytics.q enforces no-lookahead), and
    the unlabelled tail is already dropped, so every row has a real label.
  * The label at bar i looks `horizon` bars forward, so a training row within
    `horizon` bars of a test block would have a label computed from test-period
    data. `walk_forward_splits` purges those rows and embargoes an extra buffer
    (Lopez de Prado style), keeping train strictly before test with a gap.

The evaluate() harness is model-agnostic (any sklearn-style fit/predict object);
make_baseline_model() is the default (StandardScaler + LogisticRegression).
The splitter and metrics are pure numpy/pandas and fully unit-tested; the model
and the live HDB path are verified against a real HDB.
"""
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

import numpy as np

# Trailing, ~stationary predictors. Deliberately EXCLUDES absolute price levels
# (open/high/low/close/vwap/bid/ask/mid/micro — non-stationary, leak the level),
# the identifier (bar), and the forward columns (fwdRet, label).
DEFAULT_FEATURES = ("ret", "rvol", "imbAvg", "imbalance", "qimb", "spread", "vol", "trades")


# --- purged, embargoed walk-forward splitter (pure) ------------------------

def walk_forward_splits(n_samples: int, n_splits: int, horizon: int,
                        embargo: int = 0, min_train: int = 1):
    """Expanding-window walk-forward splits with purge + embargo.

    The timeline [0, n) is cut into `n_splits + 1` equal blocks; the first block
    seeds training and each later block k is a test fold with train = everything
    before it, minus the last `horizon + embargo` rows (purge + embargo) so no
    training label peeks into the test block. The final fold absorbs the
    remainder. Yields (train_idx, test_idx) numpy arrays, time-ordered, with
    train strictly before test and a gap of > horizon + embargo between them.

    Folds whose training set would be smaller than `min_train` are skipped.
    """
    if n_splits < 1:
        raise ValueError("n_splits must be >= 1")
    if horizon < 0 or embargo < 0:
        raise ValueError("horizon and embargo must be >= 0")
    fold = n_samples // (n_splits + 1)
    if fold == 0:
        raise ValueError(f"n_samples={n_samples} too small for n_splits={n_splits}")

    splits = []
    for k in range(1, n_splits + 1):
        test_start = k * fold
        test_end = n_samples if k == n_splits else (k + 1) * fold
        train_end = test_start - horizon - embargo
        if train_end < min_train:
            continue
        splits.append((np.arange(train_end), np.arange(test_start, test_end)))
    return splits


# --- feature/label extraction ----------------------------------------------

def feature_matrix(df, feature_cols: Optional[Sequence[str]] = None,
                   label_col: str = "label", fwdret_col: str = "fwdRet"):
    """Pull (X, y, fwd_ret) from a feature-table DataFrame, dropping any row with
    a NaN in a used column. X is float64, y is int, fwd_ret is float — all
    row-aligned. Raises if a requested column is missing."""
    cols = list(feature_cols) if feature_cols is not None else list(DEFAULT_FEATURES)
    missing = [c for c in (*cols, label_col, fwdret_col) if c not in df.columns]
    if missing:
        raise KeyError(f"feature table missing columns: {missing}")
    sub = df[[*cols, label_col, fwdret_col]].dropna()
    X = sub[cols].to_numpy(dtype=float)
    y = sub[label_col].to_numpy().astype(int)
    fwd = sub[fwdret_col].to_numpy(dtype=float)
    return X, y, fwd


# --- metrics (pure) --------------------------------------------------------

def classification_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Accuracy and balanced accuracy (macro-average recall over the classes
    present in y_true, so a lazy majority predictor doesn't score well)."""
    y_true, y_pred = np.asarray(y_true), np.asarray(y_pred)
    n = len(y_true)
    acc = float(np.mean(y_true == y_pred)) if n else float("nan")
    recalls = []
    for c in np.unique(y_true):
        mask = y_true == c
        recalls.append(np.mean(y_pred[mask] == c))
    bal = float(np.mean(recalls)) if recalls else float("nan")
    return {"n": int(n), "accuracy": acc, "balanced_accuracy": bal}


def strategy_metrics(y_pred: np.ndarray, fwd_ret: np.ndarray, cost: float) -> dict:
    """Cost-aware PnL of acting on the signal: position = sign of prediction
    (+1 long / -1 short / 0 flat), net return = position*fwd_ret - cost per
    entered trade. This is the real test — the label is cost-aware, so a useful
    signal must clear the round-trip cost.

    NB: with horizon > 1 the forward windows overlap, so per-bar trades are not
    independent; treat total_net as an upper-ish bound, not a tradable backtest.
    """
    y_pred = np.asarray(y_pred, dtype=float)
    fwd_ret = np.asarray(fwd_ret, dtype=float)
    pos = np.sign(y_pred)
    traded = pos != 0
    net = pos * fwd_ret - cost * traded
    n_trades = int(traded.sum())
    wins = net[traded] > 0
    std = float(np.std(net)) if len(net) else 0.0
    return {
        "n_trades": n_trades,
        "hit_rate": float(np.mean(wins)) if n_trades else float("nan"),
        "mean_net_return": float(np.mean(net)) if len(net) else float("nan"),
        "total_net_return": float(np.sum(net)),
        "sharpe_per_bar": float(np.mean(net) / std) if std > 0 else float("nan"),
    }


# --- baseline model --------------------------------------------------------

def make_baseline_model():
    """StandardScaler + multinomial LogisticRegression. sklearn is imported
    lazily so the splitter/metrics/harness plumbing need no ML dependency."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    return Pipeline([("scale", StandardScaler()),
                     ("clf", LogisticRegression(max_iter=1000))])


# --- evaluation harness ----------------------------------------------------

@dataclass(frozen=True)
class EvaluationResult:
    n_samples: int
    n_folds: int
    n_skipped: int
    feature_cols: list
    pooled: dict          # metrics over all out-of-sample test predictions
    per_fold: list = field(default_factory=list)


def evaluate(df, *, model_factory: Optional[Callable] = None, n_splits: int = 5,
             horizon: int = 1, embargo: int = 0, cost: float = 0.002,
             feature_cols: Optional[Sequence[str]] = None,
             label_col: str = "label", fwdret_col: str = "fwdRet") -> EvaluationResult:
    """Purged walk-forward evaluation. For each fold, fit a fresh model on the
    (purged) training rows and predict the test rows; pool the out-of-sample
    predictions and report classification + cost-aware strategy metrics.

    Folds whose training set is single-class are skipped (a classifier can't
    learn from one label) and counted in n_skipped.
    """
    if model_factory is None:
        model_factory = make_baseline_model
    cols = list(feature_cols) if feature_cols is not None else list(DEFAULT_FEATURES)
    X, y, fwd = feature_matrix(df, cols, label_col, fwdret_col)

    oos_true, oos_pred, oos_fwd, per_fold, skipped = [], [], [], [], 0
    for train_idx, test_idx in walk_forward_splits(len(y), n_splits, horizon, embargo):
        if len(np.unique(y[train_idx])) < 2:
            skipped += 1
            continue
        model = model_factory()
        model.fit(X[train_idx], y[train_idx])
        pred = model.predict(X[test_idx])
        oos_true.append(y[test_idx])
        oos_pred.append(pred)
        oos_fwd.append(fwd[test_idx])
        per_fold.append({
            "train": len(train_idx), "test": len(test_idx),
            **classification_metrics(y[test_idx], pred),
            **strategy_metrics(pred, fwd[test_idx], cost),
        })

    if not per_fold:
        raise ValueError("no usable folds (all skipped or too little data)")
    yt = np.concatenate(oos_true)
    yp = np.concatenate(oos_pred)
    fw = np.concatenate(oos_fwd)
    pooled = {**classification_metrics(yt, yp), **strategy_metrics(yp, fw, cost)}
    return EvaluationResult(n_samples=len(y), n_folds=len(per_fold), n_skipped=skipped,
                            feature_cols=cols, pooled=pooled, per_fold=per_fold)
