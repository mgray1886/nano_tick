import numpy as np
import pandas as pd
import pytest

from resources import alpha as al
from resources import evaluation as ev

COLS = ["ret", "rvol", "imbAvg", "imbalance", "qimb", "spread", "vol", "trades"]
FLOW, QUOTE = COLS.index("imbAvg"), COLS.index("qimb")


def _row(flow, quote):
    x = np.zeros(len(COLS))
    x[FLOW], x[QUOTE] = flow, quote
    return x


def test_imbalance_alpha_signals():
    a = al.ImbalanceAlpha(COLS, threshold=0.1)
    X = np.array([
        _row(0.5, 0.5),     # both up      -> long
        _row(-0.5, -0.5),   # both down    -> short
        _row(0.5, -0.5),    # disagree     -> flat
        _row(0.05, 0.5),    # flow < thresh-> flat
    ])
    assert list(a.predict(X)) == [1, -1, 0, 0]


def test_imbalance_alpha_fit_is_noop():
    a = al.ImbalanceAlpha(COLS)
    assert a.fit(np.zeros((2, len(COLS))), np.array([1, -1])) is a


def test_imbalance_alpha_missing_column_raises():
    with pytest.raises(KeyError):
        al.ImbalanceAlpha(["ret", "rvol"])          # no imbAvg / qimb


def test_alpha_plugs_into_evaluate_and_captures_planted_edge():
    # plant a signal the alpha should read: label = sign, and imbAvg/qimb carry it
    n = 48
    rng = np.random.default_rng(0)
    data = {c: rng.normal(0, 0.01, n) for c in COLS}
    sig = np.where(rng.random(n) < 0.5, 1, -1)
    data["imbAvg"] = sig * 0.5
    data["qimb"] = sig * 0.5
    data["label"] = sig.astype(int)
    data["fwdRet"] = sig * 0.01
    df = pd.DataFrame(data)

    res = ev.evaluate(df, model_factory=lambda: al.ImbalanceAlpha(COLS, threshold=0.1),
                      n_splits=3, horizon=1, cost=0.0, feature_cols=COLS)
    assert res.n_folds >= 1
    assert res.pooled["total_net_return"] > 0        # the rule captures the edge
    assert res.pooled["balanced_accuracy"] > 0.9     # ~perfect on the planted signal
