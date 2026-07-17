"""UniverseBuilder (§3.2.4): the pinned universe rule with per-symbol exclusion reasons (auditable
``universe_daily``), the top-N watchlist cap, ``mis_candidates ⊆ F&O`` (C7), the NIFTY200
download → cache → seed ladder (E5), and never-raise fail-closed behavior on total failure."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from engine.broker.instruments import Instrument, InstrumentStore
from engine.core.config import Settings, repo_root
from engine.marketdata.store import DailyBar, MarketStore
from engine.universe.builder import (
    EXCL_CAP,
    EXCL_LOW_VALUE,
    EXCL_NO_DATA,
    EXCL_NOT_MIS,
    UniverseBuilder,
    parse_index_constituents_csv,
)
from engine.universe.leverage import MisLeverageIngest, MisLeverageMap, parse_margins_payload
from engine.universe.surveillance import SurveillanceLists
from tests.conftest import FIXED_NOW

FIXTURES = Path(__file__).parent / "fixtures"
NIFTY200_CSV = (FIXTURES / "nifty200_test.csv").read_text(encoding="utf-8")

D = FIXED_NOW.date()
ALL_SYMBOLS = ["RELIANCE", "TCS", "GSMSTK", "ASMSTK", "T2TSTK", "ESMSTK", "NOMIS", "LOWVAL", "NODATA"]


# --------------------------------------------------------------------------- fakes / helpers
class FakeLeverage:
    """Stands in for MisLeverageIngest — the builder only calls ``current()``."""

    def __init__(self, leverages: dict[str, float], *, degraded: bool = False) -> None:
        self.snapshot = MisLeverageMap(as_of=FIXED_NOW, degraded=degraded, leverages=leverages)

    async def current(self) -> MisLeverageMap:
        return self.snapshot


class FakeSurveillance:
    """Stands in for SurveillanceIngest — the builder only calls ``current()``."""

    def __init__(self, lists: SurveillanceLists) -> None:
        self.snapshot = lists

    async def current(self) -> SurveillanceLists:
        return self.snapshot


def surveillance_lists(**kw) -> SurveillanceLists:
    return SurveillanceLists(as_of=D, **kw)


def make_settings(tmp_path, **data_overrides) -> Settings:
    s = Settings()
    object.__setattr__(s, "_resolved_data_dir", tmp_path / "data")
    for key, value in data_overrides.items():
        setattr(s.data, key, value)
    return s


def make_instruments(clock, fno_symbols: set[str]) -> InstrumentStore:
    store = InstrumentStore(clock)
    store.seed(
        [
            Instrument(
                tradingsymbol=sym, instrument_token=i + 1, exchange="NSE", segment="NSE",
                tick_size=Decimal("0.05"), lot_size=1, instrument_type="EQ",
                is_fno=sym in fno_symbols,
            )
            for i, sym in enumerate(ALL_SYMBOLS)
        ]
    )
    return store


def serving_transport(text: str) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(200, text=text))


def failing_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nse unreachable", request=request)

    return httpx.MockTransport(handler)


def collect_alerts():
    msgs = []

    async def sink(msg):
        msgs.append(msg)

    return msgs, sink


def seed_daily_bars(store: MarketStore, symbol: str, *, close: str, volume: int, days: int = 20) -> None:
    price = Decimal(close)
    store.upsert_bars_1d(
        [
            DailyBar(
                symbol=symbol, d=D - timedelta(days=i), open=price, high=price, low=price,
                close=price, volume=volume,
            )
            for i in range(1, days + 1)
        ]
    )


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def make_builder(
    settings, store, clock, transport, *, fno=frozenset({"RELIANCE"}), leverages=None,
    surveillance=None, notify=None,
) -> UniverseBuilder:
    if leverages is None:
        leverages = {s: 5.0 for s in ALL_SYMBOLS if s != "NOMIS"}
    if surveillance is None:
        surveillance = surveillance_lists(
            gsm={"GSMSTK"}, asm={"ASMSTK"}, t2t={"T2TSTK"}, esm={"ESMSTK"},
        )
    return UniverseBuilder(
        settings,
        store,
        make_instruments(clock, set(fno)),
        FakeLeverage(leverages),
        FakeSurveillance(surveillance),
        clock,
        httpx.AsyncClient(transport=transport),
        notify=notify,
    )


# --------------------------------------------------------------------------- the pinned rule (§3.2.4)
async def test_universe_rule_every_exclusion_reason(tmp_path, store, clock):
    """One symbol per rule leg: NIFTY200 ∩ MIS ∩ not-surveillance ∩ liquidity, all auditable."""
    settings = make_settings(tmp_path)
    for sym in ALL_SYMBOLS:
        if sym == "NODATA":
            continue                                   # no bars ⇒ liquidity unconfirmable
        if sym == "LOWVAL":
            seed_daily_bars(store, sym, close="100.00", volume=100)          # ₹10k << ₹5cr
        else:
            seed_daily_bars(store, sym, close="200.00", volume=500_000)      # ₹10cr ≥ ₹5cr

    builder = make_builder(settings, store, clock, serving_transport(NIFTY200_CSV))
    universe = await builder.build(D)

    assert universe.symbols == ("RELIANCE", "TCS")
    assert universe.eligible == ("RELIANCE", "TCS")
    assert universe.mis_candidates == ("RELIANCE",)    # C7: TCS is not F&O-listed
    assert universe.nifty200_source == "download"
    assert universe.degraded is False
    assert universe.exclusions == {
        "GSMSTK": ("surveillance_gsm",),
        "ASMSTK": ("surveillance_asm",),
        "T2TSTK": ("surveillance_t2t",),
        "ESMSTK": ("surveillance_esm",),
        "NOMIS": (EXCL_NOT_MIS,),
        "LOWVAL": (EXCL_LOW_VALUE,),
        "NODATA": (EXCL_NO_DATA,),
    }
    assert universe.median_traded_value["RELIANCE"] == Decimal("100000000.00")

    # Persisted universe_daily rows are the audit trail (§4.3): every NIFTY200 symbol has a row.
    rows = {r["symbol"]: r for r in store.get_universe_daily(D)}
    assert set(rows) == set(ALL_SYMBOLS)
    assert rows["RELIANCE"]["included"] is True and rows["RELIANCE"]["mis_candidate"] is True
    assert rows["TCS"]["included"] is True and rows["TCS"]["mis_candidate"] is False
    assert rows["GSMSTK"]["included"] is False
    assert rows["GSMSTK"]["exclusion_reasons"] == ["surveillance_gsm"]
    assert rows["NODATA"]["median_traded_value"] is None
    assert rows["LOWVAL"]["median_traded_value"] == Decimal("10000.00")


async def test_symbol_accumulates_multiple_reasons(tmp_path, store, clock):
    csv_text = "Company Name,Industry,Symbol,Series,ISIN Code\nBad Co,Misc,BADCO,EQ,\n"
    settings = make_settings(tmp_path)
    builder = make_builder(
        settings, store, clock, serving_transport(csv_text),
        leverages={},                                   # not MIS-eligible either
        surveillance=surveillance_lists(gsm={"BADCO"}, asm={"BADCO"}),
    )
    universe = await builder.build(D)
    assert universe.symbols == ()
    assert universe.exclusions["BADCO"] == (
        EXCL_NOT_MIS, "surveillance_gsm", "surveillance_asm", EXCL_NO_DATA,
    )


async def test_watchlist_cap_keeps_top_by_traded_value(tmp_path, store, clock):
    """§3.2.4 cap [tunable]: top-N eligible by median traded value; the rest audit as watchlist_cap."""
    settings = make_settings(tmp_path, universe_max_watchlist=1)
    seed_daily_bars(store, "RELIANCE", close="200.00", volume=1_000_000)     # ₹20cr median
    seed_daily_bars(store, "TCS", close="200.00", volume=500_000)            # ₹10cr median
    for sym in ("GSMSTK", "ASMSTK", "T2TSTK", "ESMSTK", "NOMIS", "LOWVAL"):
        seed_daily_bars(store, sym, close="100.00", volume=100)

    builder = make_builder(settings, store, clock, serving_transport(NIFTY200_CSV))
    universe = await builder.build(D)

    assert universe.symbols == ("RELIANCE",)           # higher median wins the focus cap
    assert "TCS" in universe.eligible                  # rule-passing, just past the cap
    assert universe.exclusions["TCS"] == (EXCL_CAP,)
    rows = {r["symbol"]: r for r in store.get_universe_daily(D)}
    assert rows["TCS"]["included"] is False and rows["TCS"]["exclusion_reasons"] == [EXCL_CAP]


# --------------------------------------------------------------------------- NIFTY200 ladder (E5)
async def test_download_failure_falls_back_to_seed_and_alerts(tmp_path, store, clock):
    settings = make_settings(tmp_path)
    seed = tmp_path / "seed.csv"
    seed.write_text("# owner-refreshed seed\n" + NIFTY200_CSV, encoding="utf-8")
    settings.universe.nifty200_seed_path = str(seed)
    seed_daily_bars(store, "RELIANCE", close="200.00", volume=500_000)
    msgs, sink = collect_alerts()

    builder = make_builder(settings, store, clock, failing_transport(), notify=sink)
    universe = await builder.build(D)

    assert universe.nifty200_source == "seed"
    assert universe.degraded is True                   # not freshly downloaded (E5)
    assert "RELIANCE" in universe.symbols
    assert any("fallback" in m.title.lower() or "fallback" in m.body.lower() for m in msgs)


async def test_successful_download_writes_cache_then_cache_is_used(tmp_path, store, clock):
    settings = make_settings(tmp_path)
    settings.universe.nifty200_seed_path = str(tmp_path / "missing_seed.csv")
    seed_daily_bars(store, "RELIANCE", close="200.00", volume=500_000)

    ok = make_builder(settings, store, clock, serving_transport(NIFTY200_CSV))
    assert (await ok.build(D)).nifty200_source == "download"
    cache = settings.resolved_data_dir() / "universe" / "nifty200_cached.csv"
    assert cache.exists()

    msgs, sink = collect_alerts()
    degraded = make_builder(settings, store, clock, failing_transport(), notify=sink)
    universe = await degraded.build(D)
    assert universe.nifty200_source == "cache"
    assert universe.degraded is True and msgs


async def test_total_failure_never_raises_returns_empty_degraded(tmp_path, store, clock):
    """E5: no download, no cache, no seed ⇒ alert + empty degraded universe, NEVER an exception."""
    settings = make_settings(tmp_path)
    settings.universe.nifty200_seed_path = str(tmp_path / "missing_seed.csv")
    msgs, sink = collect_alerts()

    builder = make_builder(settings, store, clock, failing_transport(), notify=sink)
    universe = await builder.build(D)

    assert universe.symbols == () and universe.mis_candidates == ()
    assert universe.degraded is True and universe.nifty200_source == "none"
    assert any(m.severity == "critical" for m in msgs)
    assert store.get_universe_daily(D) == []           # nothing persisted — fail closed


# --------------------------------------------------------------------------- CSV parser
def test_parse_index_constituents_csv_defensive():
    text = (
        "# comment line to skip\n"
        "Company Name,Industry,SYMBOL,Series,ISIN Code\n"   # case-insensitive Symbol column
        "A Co,X,AAA,EQ,\n"
        "B Co,X,BBB,BE,\n"                                  # non-EQ series dropped
        "A Co dup,X,AAA,EQ,\n"                              # de-duplicated
        "C Co,X,ccc,EQ,\n"                                  # uppercased
    )
    assert parse_index_constituents_csv(text) == ["AAA", "CCC"]


def test_parse_index_constituents_csv_requires_symbol_column():
    with pytest.raises(ValueError):
        parse_index_constituents_csv("Company Name,Industry\nA,B\n")


def test_committed_seed_is_real_and_parses():
    """The shipped config/universe/nifty200_seed.csv must always parse as a usable fallback."""
    text = (repo_root() / "config" / "universe" / "nifty200_seed.csv").read_text(encoding="utf-8")
    symbols = parse_index_constituents_csv(text)
    assert len(symbols) >= 100                          # a meaningful subset of the 200
    assert len(symbols) == len(set(symbols))
    # Stable blue-chips only — e.g. TATAMOTORS left the list when the 2025 demerger split it
    # into TMCV/TMPV, so index membership of any single name is never guaranteed forever.
    for well_known in ("RELIANCE", "TCS", "HDFCBANK", "SBIN", "ICICIBANK"):
        assert well_known in symbols


# --------------------------------------------------------------------------- MIS leverage ingest (A8/E5)
MARGINS_JSON = [
    {"tradingsymbol": "RELIANCE", "mis_multiplier": 5.0, "mis_margin": 20.0},
    {"tradingsymbol": "TCS", "mis_margin": 25.0},        # no multiplier: 100/25 = 4×
    {"tradingsymbol": "CNCONLY", "mis_multiplier": 0},   # 0/absent leverage ⇒ not MIS-tradeable
    {"mis_multiplier": 5.0},                             # no symbol: skipped, never fatal
]


def test_parse_margins_payload_defensive():
    parsed = parse_margins_payload(MARGINS_JSON)
    assert parsed == {"RELIANCE": 5.0, "TCS": 4.0}
    assert parse_margins_payload({"data": MARGINS_JSON}) == parsed   # wrapper form too
    assert parse_margins_payload({"unexpected": "shape"}) == {}


async def test_leverage_refresh_then_reuse_cache_on_failure(tmp_path, clock):
    """E5 reuse-yesterday: a fetch failure serves the cached last-good file, degraded + alert."""
    cache = tmp_path / "mis_margins.json"
    ok = MisLeverageIngest(
        clock,
        httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=MARGINS_JSON))
        ),
        cache,
    )
    fresh = await ok.refresh()
    assert fresh.degraded is False and fresh.as_of == FIXED_NOW
    assert fresh.is_mis_eligible("reliance") is True     # case-insensitive
    assert fresh.leverage("TCS") == 4.0
    assert fresh.is_mis_eligible("CNCONLY") is False     # in the file but not leveraged
    assert cache.exists()

    msgs, sink = collect_alerts()
    degraded = await MisLeverageIngest(clock, failing_transport_client(), cache, notify=sink).refresh()
    assert degraded.degraded is True
    assert degraded.leverages == fresh.leverages         # yesterday's file reused
    assert msgs and msgs[0].data["job_id"] == "mis_margins"


async def test_leverage_failure_without_cache_fails_closed(tmp_path, clock):
    """A8 conservative default: no fetch AND no cache ⇒ empty map ⇒ nothing is MIS-eligible."""
    msgs, sink = collect_alerts()
    ingest = MisLeverageIngest(
        clock, failing_transport_client(), tmp_path / "missing.json", notify=sink
    )
    snapshot = await ingest.refresh()                    # must not raise (E5)
    assert snapshot.degraded is True and snapshot.leverages == {}
    assert snapshot.is_mis_eligible("RELIANCE") is False
    assert msgs
    assert await ingest.current() is snapshot            # retained for same-process consumers


def failing_transport_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=failing_transport())
