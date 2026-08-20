import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from resources import dashboard as dash


def _ms(y, mo, d, h=0, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


# --- date_range ------------------------------------------------------------

def test_date_range_inclusive():
    assert dash.date_range("2026-08-18", "2026-08-20") == [
        date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)]


def test_date_range_end_before_start_raises():
    with pytest.raises(ValueError):
        dash.date_range("2026-08-20", "2026-08-18")


# --- ohlcv_records ---------------------------------------------------------

def _bars(n, start_ms=1787043600000):
    idx = pd.to_datetime([start_ms + i * 60000 for i in range(n)], unit="ms")
    return pd.DataFrame({
        "open": [64000.0 + i for i in range(n)], "high": [64050.0 + i for i in range(n)],
        "low": [63950.0 + i for i in range(n)], "close": [64010.0 + i for i in range(n)],
        "vwap": [64005.0 + i for i in range(n)], "vol": [1.23456 + i for i in range(n)],
        "trades": [3] * n,
    }, index=idx)


def test_ohlcv_records_shape_and_rounding():
    recs = dash.ohlcv_records(_bars(3))
    assert len(recs) == 3 and all(len(r) == 6 for r in recs)
    assert recs[0][0] == 1787043600000                 # epoch ms from the index
    assert recs[0][1:5] == [64000.0, 64050.0, 63950.0, 64010.0]
    assert recs[0][5] == 1.2346                         # volume rounded to 4dp


# --- render_html -----------------------------------------------------------

def test_render_html_embeds_payload_and_clears_placeholder():
    html = dash.render_html(dash.ohlcv_records(_bars(4)), symbol="BTCUSDT",
                            day_label="2026-08-18", bar_seconds=60, sma=9)
    assert "__PAYLOAD__" not in html                    # placeholder replaced
    assert '"symbol":"BTCUSDT"' in html
    assert '"sma":9' in html
    assert "1787043600000" in html                      # a bar timestamp made it in
    assert "<canvas" in html and "getContext" in html   # the real template


def test_template_has_interactive_controls():
    html = dash.render_html([[1787043600000, 1, 2, 0.5, 1.5, 1]], symbol="S",
                            day_label="d", bar_seconds=60)
    for needle in ('id="typeSeg"', 'data-type="candles"', 'data-type="hollow"', 'data-type="line"',
                   'id="smaBtn"', 'id="zoomIn"', 'id="zoomOut"', 'id="zoomReset"',
                   "let chartType", "view = { i0", "addEventListener(\"wheel\""):
        assert needle in html, needle


def test_render_html_carries_markers():
    html = dash.render_html(dash.ohlcv_records(_bars(2)), symbol="BTCUSDT",
                            day_label="2026-08-18", bar_seconds=60,
                            markers=[{"t": 1787043600000, "label": "alpha buy", "side": "buy"}])
    assert "alpha buy" in html
    assert '"side":"buy"' in html


def test_render_html_escapes_script_close_in_markers():
    # a hostile/awkward label must not break out of the <script> block
    html = dash.render_html(dash.ohlcv_records(_bars(1)), symbol="BTCUSDT",
                            day_label="d", bar_seconds=60,
                            markers=[{"t": 1787043600000, "label": "</script><b>x"}])
    assert "</script><b>x" not in html                  # raw sequence escaped
    assert "<\\/script>" in html


# --- load_bars + build_dashboard (fake reader) -----------------------------

class FakeReader:
    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def bars(self, symbol, day, bar_seconds):
        self.calls.append((symbol, day, bar_seconds))
        v = self.per_day.get(day)
        if isinstance(v, Exception):
            raise v
        return v if v is not None else pd.DataFrame()


def test_load_bars_concatenates_and_skips_empty():
    days = dash.date_range("2026-08-18", "2026-08-20")
    reader = FakeReader({days[0]: _bars(3), days[1]: RuntimeError("missing"), days[2]: _bars(2)})
    df = dash.load_bars(reader, "BTCUSDT", days, 60)
    assert len(df) == 5                                 # day 1 skipped
    assert [c[1] for c in reader.calls] == days


def test_load_bars_raises_when_empty():
    days = dash.date_range("2026-08-18", "2026-08-18")
    with pytest.raises(ValueError):
        dash.load_bars(FakeReader({}), "BTCUSDT", days, 60)


def test_build_dashboard_single_day_label():
    days = dash.date_range("2026-08-18", "2026-08-18")
    html = dash.build_dashboard(FakeReader({days[0]: _bars(5)}), "BTCUSDT", days, 60)
    assert '"range":"2026-08-18"' in html


def test_build_dashboard_multiday_label():
    days = dash.date_range("2026-08-18", "2026-08-20")
    reader = FakeReader({days[0]: _bars(3), days[2]: _bars(3)})
    html = dash.build_dashboard(reader, "BTCUSDT", days, 60)
    assert "2026-08-18 → 2026-08-20" in html       # "start → end" range label


# --- arg parsing -----------------------------------------------------------

def test_parser_defaults(monkeypatch):
    for v in ("SYMBOL", "BAR_SECONDS"):
        monkeypatch.delenv(v, raising=False)
    args = dash.build_parser().parse_args(["--start", "2026-08-18"])
    assert args.symbol == "BTCUSDT" and args.bar_seconds == 60
    assert args.sma == 14 and args.out == "dashboard.html" and args.end is None


def test_parser_requires_start():
    with pytest.raises(SystemExit):
        dash.build_parser().parse_args([])


# --- window mode helpers ---------------------------------------------------

def test_parse_instant_epoch_ms_and_seconds():
    assert dash.parse_instant("1787043600000") == 1787043600000     # ms as-is
    assert dash.parse_instant("1787043600") == 1787043600000        # seconds scaled up


def test_parse_instant_datetime_forms():
    exp = _ms(2026, 8, 18, 10, 30)
    assert dash.parse_instant("2026-08-18 10:30") == exp
    assert dash.parse_instant("2026-08-18T10:30:00") == exp
    assert dash.parse_instant("2026-08-18") == _ms(2026, 8, 18)


def test_parse_instant_invalid_raises():
    with pytest.raises(ValueError):
        dash.parse_instant("not a time")


def test_window_bounds():
    assert dash.window_bounds(1_000_000_000_000, 30) == (
        1_000_000_000_000 - 30 * 60_000, 1_000_000_000_000 + 30 * 60_000)


def test_window_days_straddles_midnight():
    lo, hi = dash.window_bounds(_ms(2026, 8, 18, 0, 10), 30)   # 00:10 ± 30min crosses midnight
    assert dash.window_days(lo, hi) == [date(2026, 8, 17), date(2026, 8, 18)]


def test_window_days_same_day():
    lo, hi = dash.window_bounds(_ms(2026, 8, 18, 12, 0), 10)
    assert dash.window_days(lo, hi) == [date(2026, 8, 18)]


def test_filter_window_trims_to_bounds():
    df = _bars(10)                                             # 1-min bars from 1787043600000
    base = 1787043600000
    trimmed = dash.filter_window(df, base + 60_000, base + 3 * 60_000)
    assert len(trimmed) == 3                                   # bars at +1, +2, +3 min


def test_parser_center_mode():
    args = dash.build_parser().parse_args(
        ["--center", "2026-08-18 10:30", "--window", "20", "--center-side", "buy"])
    assert args.center == "2026-08-18 10:30" and args.window == 20
    assert args.center_side == "buy" and args.start is None


def test_parser_start_and_center_mutually_exclusive():
    with pytest.raises(SystemExit):
        dash.build_parser().parse_args(["--start", "2026-08-18", "--center", "2026-08-18"])


def test_markers_json_round_trips_through_file(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps([{"t": 1787043600000, "label": "x", "side": "sell"}]))
    markers = json.loads(p.read_text())
    html = dash.render_html([[1787043600000, 1, 2, 0.5, 1.5, 1]], symbol="S",
                            day_label="d", bar_seconds=60, markers=markers)
    assert '"side":"sell"' in html
