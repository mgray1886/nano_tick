"""Alphas: functions from trailing features to a discrete trading signal.

An Alpha maps the features known at bar close t to a position in {-1, 0, +1}
(short / flat / long), using ONLY trailing information (analytics.q's featureTable
enforces no-lookahead). Alphas are sklearn-compatible (fit / predict returning
signals), so they drop straight into resources.evaluation.evaluate() and are
judged by the SAME purged walk-forward + cost-aware PnL as any model — the point
being to compare signals honestly on one yardstick.

ImbalanceAlpha is an interpretable microstructure baseline: trade with the flow
when order-flow imbalance and quote imbalance agree. It's the kind of signal
worth trying before reaching for a model — at short horizons the real edge (if
any) lives in order flow and quotes, not in a black box over lagging bars.
"""
import numpy as np


class Alpha:
    """Base alpha with the sklearn fit/predict contract. `predict` returns an
    array of signals in {-1, 0, +1}. Stateless by default (fit is a no-op);
    parametric alphas (e.g. a wrapped model) override fit."""
    name = "alpha"

    def fit(self, X, y):
        return self

    def predict(self, X):
        raise NotImplementedError


class ImbalanceAlpha(Alpha):
    """Trade with the flow: **long** when trailing order-flow imbalance AND quote
    imbalance are both above +threshold, **short** when both below -threshold,
    else **flat**. No fitting — a transparent microstructure rule.

    Needs the ordered `feature_cols` (as passed to evaluate) so it can find its
    two columns by name; both must be present (they are in DEFAULT_FEATURES).
    """
    name = "imbalance"

    def __init__(self, feature_cols, flow_col="imbAvg", quote_col="qimb", threshold=0.1):
        cols = list(feature_cols)
        missing = [c for c in (flow_col, quote_col) if c not in cols]
        if missing:
            raise KeyError(f"ImbalanceAlpha needs feature columns {missing}")
        self._flow = cols.index(flow_col)
        self._quote = cols.index(quote_col)
        self._th = float(threshold)

    def predict(self, X):
        X = np.asarray(X, dtype=float)
        flow, quote = X[:, self._flow], X[:, self._quote]
        long_ = (flow > self._th) & (quote > self._th)
        short = (flow < -self._th) & (quote < -self._th)
        return long_.astype(int) - short.astype(int)
