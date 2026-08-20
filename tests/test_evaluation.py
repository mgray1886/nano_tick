import numpy as np
import pandas as pd
import pytest

from resources import evaluation as ev


# --- walk_forward_splits: the leakage-critical core ------------------------

def test_splits_count_and_time_order():
    splits = ev.walk_forward_splits(20, n_splits=4, horizon=1)
    assert len(splits) == 4
    prev_test_start = -1
    for train, test in splits:
        assert train.max() < test.min()               # train strictly before test
        assert set(train).isdisjoint(test)            # disjoint
        assert test.min() > prev_test_start           # folds advance in time
        prev_test_start = test.min()


def test_splits_purge_gap_equals_horizon_plus_embargo():
    for horizon, embargo in [(1, 0), (3, 0), (1, 2), (5, 4)]:
        for train, test in ev.walk_forward_splits(40, n_splits=4, horizon=horizon, embargo=embargo):
            # no training row within horizon+embargo of the test block
            assert test.min() - train.max() == horizon + embargo + 1


def test_splits_expanding_train_and_full_coverage():
    splits = ev.walk_forward_splits(20, n_splits=4, horizon=1)
    train_sizes = [len(tr) for tr, _ in splits]
    assert train_sizes == sorted(train_sizes)          # expanding window
    assert splits[-1][1].max() == 19                   # last fold reaches the end
    # test blocks are contiguous and cover [fold, n)
    covered = np.concatenate([te for _, te in splits])
    assert list(covered) == list(range(4, 20))


def test_splits_min_train_skips_early_folds():
    # train_ends would be 3,7,11,15; min_train=5 drops the first
    splits = ev.walk_forward_splits(20, n_splits=4, horizon=1, min_train=5)
    assert len(splits) == 3
    assert len(splits[0][0]) == 7


def test_splits_embargo_shrinks_training():
    no_emb = ev.walk_forward_splits(40, n_splits=4, horizon=1, embargo=0)
    emb = ev.walk_forward_splits(40, n_splits=4, horizon=1, embargo=3)
    for (tr0, _), (tr1, _) in zip(no_emb, emb):
        assert len(tr1) == len(tr0) - 3


@pytest.mark.parametrize("kwargs", [
    {"n_splits": 0, "horizon": 1},
    {"n_splits": 5, "horizon": 1},   # n=3 too small -> fold size 0
    {"n_splits": 3, "horizon": -1},
    {"n_splits": 3, "horizon": 1, "embargo": -1},
])
def test_splits_validation(kwargs):
    with pytest.raises(ValueError):
        ev.walk_forward_splits(3, **kwargs)


# --- feature_matrix --------------------------------------------------------

def _feature_df(n=10):
    rng = np.random.default_rng(0)
    data = {c: rng.normal(size=n) for c in ev.DEFAULT_FEATURES}
    data["label"] = rng.integers(-1, 2, size=n)
    data["fwdRet"] = rng.normal(0, 0.01, size=n)
    data["mid"] = 100 + rng.normal(size=n)   # extra column that must be ignored
    return pd.DataFrame(data)


def test_feature_matrix_shapes_and_default_cols():
    df = _feature_df(10)
    X, y, fwd = ev.feature_matrix(df)
    assert X.shape == (10, len(ev.DEFAULT_FEATURES))
    assert y.dtype.kind in "iu" and len(y) == 10
    assert fwd.shape == (10,) and fwd.dtype == float


def test_feature_matrix_drops_nan_rows():
    df = _feature_df(10)
    df.loc[3, "ret"] = np.nan
    X, y, fwd = ev.feature_matrix(df)
    assert len(y) == 9                         # the NaN row is dropped from all three


def test_feature_matrix_missing_column_raises():
    df = _feature_df(5).drop(columns=["qimb"])
    with pytest.raises(KeyError):
        ev.feature_matrix(df)


def test_feature_matrix_custom_cols():
    df = _feature_df(6)
    X, y, fwd = ev.feature_matrix(df, feature_cols=["ret", "qimb"])
    assert X.shape == (6, 2)


# --- metrics ---------------------------------------------------------------

def test_classification_metrics_accuracy_and_balanced():
    m = ev.classification_metrics(np.array([1, 1, 0, -1, 0]), np.array([1, 0, 0, -1, 0]))
    assert m["accuracy"] == pytest.approx(0.8)
    assert m["balanced_accuracy"] == pytest.approx((0.5 + 1 + 1) / 3)


def test_balanced_accuracy_penalises_majority_predictor():
    # 3:1 imbalance, predict the majority every time -> accuracy 0.75 but bal 0.5
    m = ev.classification_metrics(np.array([1, 1, 1, 0]), np.array([1, 1, 1, 1]))
    assert m["accuracy"] == pytest.approx(0.75)
    assert m["balanced_accuracy"] == pytest.approx(0.5)


def test_strategy_metrics_cost_and_pnl_math():
    m = ev.strategy_metrics(np.array([1, -1, 0, 1]),
                            np.array([0.01, -0.02, 0.5, 0.001]), cost=0.002)
    # net = [0.008, 0.018, 0.0, -0.001]; only 3 positions are trades
    assert m["n_trades"] == 3
    assert m["total_net_return"] == pytest.approx(0.025)
    assert m["mean_net_return"] == pytest.approx(0.025 / 4)
    assert m["hit_rate"] == pytest.approx(2 / 3)


def test_strategy_metrics_flat_signal_no_trades():
    m = ev.strategy_metrics(np.zeros(5), np.array([0.01, -0.01, 0.02, -0.02, 0.0]), cost=0.002)
    assert m["n_trades"] == 0
    assert np.isnan(m["hit_rate"])
    assert m["total_net_return"] == pytest.approx(0.0)


# --- evaluate harness (fake model; no sklearn needed) ----------------------

class ConstantModel:
    """Predicts a fixed class; records the training labels it saw."""

    def __init__(self, value=1):
        self.value = value

    def fit(self, X, y):
        self.seen_classes = set(np.unique(y))
        return self

    def predict(self, X):
        return np.full(len(X), self.value)


def _labelled_df(labels, seed=0):
    n = len(labels)
    rng = np.random.default_rng(seed)
    data = {c: rng.normal(size=n) for c in ev.DEFAULT_FEATURES}
    data["label"] = np.array(labels)
    data["fwdRet"] = rng.normal(0, 0.01, size=n)
    return pd.DataFrame(data)


def test_evaluate_runs_folds_and_pools_metrics():
    df = _labelled_df([(-1) ** i * (i % 3 - 1) for i in range(24)])  # mixed labels
    res = ev.evaluate(df, model_factory=lambda: ConstantModel(1),
                      n_splits=3, horizon=1, cost=0.002)
    assert res.n_folds >= 1
    assert res.n_samples == 24
    assert set(res.feature_cols) == set(ev.DEFAULT_FEATURES)
    assert "accuracy" in res.pooled and "total_net_return" in res.pooled
    assert len(res.per_fold) == res.n_folds


def test_evaluate_skips_single_class_training_fold():
    # first block (seed train of fold 1) is all one class -> that fold is skipped
    labels = [1, 1, 1, 1] + [0, 1, -1, 1, 0, -1, 1, 0]   # n=12, fold size 3
    df = _labelled_df(labels)
    res = ev.evaluate(df, model_factory=lambda: ConstantModel(1),
                      n_splits=3, horizon=1)
    assert res.n_skipped == 1
    assert res.n_folds == 2


def test_evaluate_raises_when_all_folds_unusable():
    df = _labelled_df([1] * 12)   # every training fold is single-class
    with pytest.raises(ValueError):
        ev.evaluate(df, model_factory=lambda: ConstantModel(1), n_splits=3, horizon=1)
