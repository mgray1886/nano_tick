from datetime import date, datetime, timedelta

import pytest

from resources import reader


# --- pure helpers ----------------------------------------------------------

def test_to_bar_ns_from_seconds():
    assert reader.to_bar_ns(60) == 60_000_000_000
    assert reader.to_bar_ns(1) == 1_000_000_000
    assert reader.to_bar_ns(0.5) == 500_000_000


def test_to_bar_ns_from_timedelta():
    assert reader.to_bar_ns(timedelta(minutes=1)) == 60_000_000_000
    assert reader.to_bar_ns(timedelta(hours=1)) == 3_600_000_000_000


def test_to_bar_ns_rejects_bool():
    # bool is an int subclass; a stray True must not become 1e9 ns
    with pytest.raises(TypeError):
        reader.to_bar_ns(True)


@pytest.mark.parametrize("bad", [0, -5, timedelta(0), timedelta(seconds=-1)])
def test_to_bar_ns_rejects_non_positive(bad):
    with pytest.raises(ValueError):
        reader.to_bar_ns(bad)


def test_to_bar_ns_rejects_wrong_type():
    with pytest.raises(TypeError):
        reader.to_bar_ns("60")


@pytest.mark.parametrize("value,expected", [
    (date(2026, 8, 18), date(2026, 8, 18)),
    (datetime(2026, 8, 18, 13, 30), date(2026, 8, 18)),
    ("2026.08.18", date(2026, 8, 18)),
    ("2026-08-18", date(2026, 8, 18)),
    ("  2026-08-18 ", date(2026, 8, 18)),
])
def test_coerce_date(value, expected):
    assert reader.coerce_date(value) == expected


def test_coerce_date_rejects_wrong_type():
    with pytest.raises(TypeError):
        reader.coerce_date(20260818)


# --- config ----------------------------------------------------------------

_ENV = ("SYMBOL", "HDB_PATH", "BAR_SECONDS", "LABEL_HORIZON", "FEATURE_WINDOW", "ROUNDTRIP_COST")


def test_config_from_env_defaults(monkeypatch):
    for v in _ENV:
        monkeypatch.delenv(v, raising=False)
    cfg = reader.ReaderConfig.from_env()
    assert cfg.symbol == "BTCUSDT"
    assert cfg.analytics_q.name == "analytics.q"
    assert cfg.bar_seconds == 60
    assert cfg.horizon == 1
    assert cfg.window == 20
    assert cfg.cost == 0.002


def test_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ethusdt")
    monkeypatch.setenv("BAR_SECONDS", "5")
    monkeypatch.setenv("LABEL_HORIZON", "3")
    monkeypatch.setenv("FEATURE_WINDOW", "50")
    monkeypatch.setenv("ROUNDTRIP_COST", "0.001")
    cfg = reader.ReaderConfig.from_env()
    assert cfg.symbol == "ETHUSDT"       # upper()d
    assert (cfg.bar_seconds, cfg.horizon, cfg.window, cfg.cost) == (5, 3, 50, 0.001)


# --- HdbReader dispatch (fake kx; no license needed) -----------------------

class FakeAtom:
    def __init__(self, kind, value):
        self.kind, self.value = kind, value


class FakeResult:
    def __init__(self, value):
        self._v = value

    def pd(self):
        return ("pd", self._v)

    def py(self):
        return self._v


class FakeKx:
    """Records q() calls and mimics the atom constructors the reader uses."""

    def __init__(self, result=42):
        self.calls = []
        self._result = result

    def SymbolAtom(self, v):
        return FakeAtom("sym", v)

    def DateAtom(self, v):
        return FakeAtom("date", v)

    def LongAtom(self, v):
        return FakeAtom("long", v)

    def FloatAtom(self, v):
        return FakeAtom("float", v)

    def q(self, *args):
        self.calls.append(args)
        first = args[0]
        if isinstance(first, str) and first.startswith("\\l"):
            return None                                   # HDB / analytics load
        if first == "`timespan$":
            return FakeAtom("span", args[1].value)        # ns long -> timespan
        return FakeResult(self._result)                   # function call


@pytest.fixture
def hdb(tmp_path):
    (tmp_path / "hdb").mkdir()
    aq = tmp_path / "analytics.q"
    aq.write_text("/ stub\n")
    return tmp_path / "hdb", aq


def _last_call(fake):
    return fake.calls[-1]


def test_init_loads_hdb_then_analytics(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    reader.HdbReader(hdb_dir, aq, kx=fake)
    loads = [c[0] for c in fake.calls if isinstance(c[0], str) and c[0].startswith("\\l")]
    assert len(loads) == 2
    assert hdb_dir.resolve().as_posix() in loads[0]        # HDB first (it chdirs)
    assert aq.resolve().as_posix() in loads[1]             # analytics by abs path second


def test_init_missing_hdb_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        reader.HdbReader(tmp_path / "nope", tmp_path / "analytics.q", kx=FakeKx())


def test_init_missing_analytics_raises(tmp_path):
    (tmp_path / "hdb").mkdir()
    with pytest.raises(FileNotFoundError):
        reader.HdbReader(tmp_path / "hdb", tmp_path / "nope.q", kx=FakeKx())


def test_bars_dispatch_and_coercion(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    r = reader.HdbReader(hdb_dir, aq, kx=fake)
    out = r.bars("BTCUSDT", date(2026, 8, 18), 60)
    assert out == ("pd", 42)
    name, sym, day, span = _last_call(fake)
    assert name == "bars"
    assert (sym.kind, sym.value) == ("sym", "BTCUSDT")
    assert (day.kind, day.value) == ("date", date(2026, 8, 18))
    assert (span.kind, span.value) == ("span", 60_000_000_000)


def test_quote_bars_dispatch(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    reader.HdbReader(hdb_dir, aq, kx=fake).quote_bars("BTCUSDT", "2026.08.18", timedelta(minutes=5))
    name, sym, day, span = _last_call(fake)
    assert name == "quoteBars"
    assert span.value == 300_000_000_000


def test_feature_table_dispatch_with_params(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    r = reader.HdbReader(hdb_dir, aq, kx=fake)
    r.feature_table("BTCUSDT", date(2026, 8, 18), 60, horizon=2, window=7, cost=0.001)
    name, sym, day, span, h, w, c = _last_call(fake)
    assert name == "featureTable"
    assert (h.kind, h.value) == ("long", 2)
    assert (w.kind, w.value) == ("long", 7)
    assert (c.kind, c.value) == ("float", 0.001)


def test_vwap_day_returns_float(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx(result=100.5)
    v = reader.HdbReader(hdb_dir, aq, kx=fake).vwap_day("BTCUSDT", date(2026, 8, 18))
    assert v == 100.5 and isinstance(v, float)
    assert _last_call(fake)[0] == "vwapDay"


def test_counts_dispatch(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    out = reader.HdbReader(hdb_dir, aq, kx=fake).counts("BTCUSDT")
    assert out == ("pd", 42)
    name, sym = _last_call(fake)
    assert name == "counts" and sym.value == "BTCUSDT"


# --- open_reader attaches config as method defaults ------------------------

def test_open_reader_uses_config_defaults(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    cfg = reader.ReaderConfig(symbol="BTCUSDT", hdb_path=hdb_dir, analytics_q=aq,
                              bar_seconds=5, horizon=3, window=50, cost=0.001)
    r = reader.open_reader(cfg, kx=fake)
    r.feature_table("BTCUSDT", date(2026, 8, 18))          # no size/h/w/cost -> from config
    name, sym, day, span, h, w, c = _last_call(fake)
    assert span.value == 5_000_000_000                     # bar_seconds=5
    assert (h.value, w.value, c.value) == (3, 50, 0.001)


def test_reader_without_config_uses_hardcoded_defaults(hdb):
    hdb_dir, aq = hdb
    fake = FakeKx()
    r = reader.HdbReader(hdb_dir, aq, kx=fake)             # no config attached
    r.bars("BTCUSDT", date(2026, 8, 18))                  # no size -> 60s fallback
    assert _last_call(fake)[3].value == 60_000_000_000
