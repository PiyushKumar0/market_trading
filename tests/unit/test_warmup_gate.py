"""WarmupGate (§2.6 step 6 / §7.1 ``warmup_ready`` + ``regime_data_ready``) — ready / missing /
frozen paths, against a fake MarketStore (the store's own coverage SQL is tested in
test_market_store.py; here the subject is the gate's requirements + the lifecycle consequence)."""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import RiskState
from engine.ops.warmup import (
    CLASS_DAILY,
    CLASS_INTRADAY,
    CLASS_REGIME,
    CLASS_UNKNOWN,
    DAILY_STRATEGY_SCOPE,
    WarmupGate,
    WarmupStatus,
    blocker_class,
    blocker_symbol,
    recent_sessions,
)
from tests.conftest import FIXED_NOW
from tests.unit.test_lifecycle_selftest import OWNER_OK, _build


@pytest.fixture
def temp_config(tmp_path):
    """Hermetic protected-store config dir for the lifecycle-consequence tests (mirrors the fixture
    in test_lifecycle_selftest; a fixture cannot cross module boundaries, so it is defined locally
    rather than imported — importing it and re-using the name as a test parameter trips ruff F811)."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    return cfg

# The 5 completed sessions before Wed 2026-06-17 (13/14 = weekend): newest first.
RECENT_5 = [date(2026, 6, 16), date(2026, 6, 15), date(2026, 6, 12), date(2026, 6, 11), date(2026, 6, 10)]


class FakeStore:
    """Duck-typed MarketStore surface the gate consumes (async wrappers only)."""

    def __init__(self):
        self.gaps: dict[str, list[datetime]] = {}
        self.daily: dict[str, list[date]] = {}
        self.intraday_ranges: list[tuple[str, datetime, datetime]] = []
        # Optional (first_bar, total) override for the young-listing probe. Unset ⇒ derived from
        # ``daily`` (min/len) — i.e. the daily dict IS the symbol's whole history. Set it to model an
        # OLD security whose in-window shortfall is a real gap, not a fresh listing.
        self.spans: dict[str, tuple[date | None, int]] = {}

    async def acoverage_gaps(self, symbol, start, end):
        self.intraday_ranges.append((symbol, start, end))
        return list(self.gaps.get(symbol, []))

    async def aget_bars_1d(self, symbol, start, end):
        return [SimpleNamespace(d=d) for d in self.daily.get(symbol, []) if start <= d <= end]

    async def adaily_bar_span(self, symbol):
        if symbol in self.spans:
            return self.spans[symbol]
        days = self.daily.get(symbol, [])
        return (min(days) if days else None, len(days))


def _gate(store, clock, *, symbols=("RELIANCE",), **kw):
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    kw.setdefault("daily_lookback_sessions", 5)
    kw.setdefault("vix_lookback_sessions", 3)
    return WarmupGate(store, clock, calendar, symbols=list(symbols), **kw)


def _fill_ready(store):
    store.daily["RELIANCE"] = list(RECENT_5)
    store.daily["NIFTY 50"] = list(RECENT_5)
    store.daily["INDIA VIX"] = list(RECENT_5[:3])


# --------------------------------------------------------------------- daily window (2026-09-15:
# the daily_gap repair fetches exactly this window — it must be the SAME window the gate checks, or
# a fill that disagrees with the gate can never clear its blocker).
def test_daily_window_is_the_gates_own_lookback(clock):
    assert _gate(store=FakeStore(), clock=clock).daily_window() == RECENT_5


def test_recent_sessions_matches_the_gate_window(clock):
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    assert recent_sessions(calendar, clock.today(), 5) == RECENT_5


def test_recent_sessions_returns_none_when_calendar_cannot_enumerate(clock):
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    assert recent_sessions(calendar, clock.today(), 100_000) is None


@pytest.mark.asyncio
async def test_ready_when_all_lookbacks_covered(clock):
    store = FakeStore()
    _fill_ready(store)
    gate = _gate(store, clock)
    status = await gate.status()
    assert status == WarmupStatus(ready=True, blockers=[])
    assert await gate.ready() is True
    # The intraday check clamped to the session: [09:15, now) on the fixed trading day.
    sym, start, end = store.intraday_ranges[0]
    assert sym == "RELIANCE"
    assert (start.hour, start.minute) == (9, 15)
    assert end == FIXED_NOW.replace(second=0, microsecond=0)


@pytest.mark.asyncio
async def test_intraday_gap_blocks_orb(clock):
    store = FakeStore()
    _fill_ready(store)
    store.gaps["RELIANCE"] = [FIXED_NOW.replace(hour=9, minute=30, second=0, microsecond=0)]
    gate = _gate(store, clock)
    status = await gate.status()
    assert status.ready is False
    assert any(b.startswith("orb:RELIANCE") for b in status.blockers)


@pytest.mark.asyncio
async def test_daily_shortfall_blocks_daily_strategies(clock):
    store = FakeStore()
    _fill_ready(store)
    store.daily["RELIANCE"] = RECENT_5[:3]   # 3/5 sessions
    store.spans["RELIANCE"] = (date(2007, 1, 1), 4000)  # an OLD stock: 3/5 is a real gap, not a listing
    gate = _gate(store, clock)
    blockers = await gate.missing()
    assert "rsi2/trend/mom:RELIANCE daily bars 3/5" in blockers


@pytest.mark.asyncio
async def test_missing_regime_history_blocks_regime(clock):
    """§7.1 regime_data_ready: NIFTY 50 + India VIX history must be present, not just the watchlist."""
    store = FakeStore()
    _fill_ready(store)
    store.daily["NIFTY 50"] = []
    store.daily["INDIA VIX"] = RECENT_5[:1]  # 1/3
    store.spans["INDIA VIX"] = (date(2008, 3, 1), 4000)  # ancient index: 1/3 is a real gap, not a listing
    gate = _gate(store, clock)
    blockers = await gate.missing()
    assert "regime:NIFTY 50 daily bars 0/5" in blockers
    assert "regime:INDIA VIX daily bars 1/3" in blockers


@pytest.mark.asyncio
async def test_non_trading_day_has_no_intraday_requirement():
    """Sunday start (R6: no session) ⇒ no orb requirement even with zero intraday bars; the daily
    lookbacks still bind."""
    sunday = Clock(time_source=lambda: datetime(2026, 6, 14, 10, 5, tzinfo=IST))
    store = FakeStore()
    week = [date(2026, 6, 12), date(2026, 6, 11), date(2026, 6, 10), date(2026, 6, 9), date(2026, 6, 8)]
    store.daily["RELIANCE"] = list(week)
    store.daily["NIFTY 50"] = list(week)
    store.daily["INDIA VIX"] = week[:3]
    store.gaps["RELIANCE"] = [datetime(2026, 6, 14, 9, 30, tzinfo=IST)]  # would block on a trading day
    gate = _gate(store, sunday)
    assert await gate.missing() == []
    assert store.intraday_ranges == []       # coverage_gaps never queried without a session


@pytest.mark.asyncio
async def test_before_open_accrues_no_intraday_requirement():
    early = Clock(time_source=lambda: datetime(2026, 6, 17, 9, 0, tzinfo=IST))
    store = FakeStore()
    _fill_ready(store)
    gate = _gate(store, early)
    assert await gate.missing() == []
    assert store.intraday_ranges == []


# --------------------------------------------------------------------- young-listing exclusion (F3)
@pytest.mark.asyncio
async def test_young_listing_alone_does_not_block(clock):
    """A fresh listing (a bar for every session since it listed < the lookback ago) can never satisfy
    the 200-session gate, so it is EXCLUDED from blockers and reported in young_excluded — not frozen
    forever. With everything else covered, the gate is READY despite the shortfall."""
    store = FakeStore()
    _fill_ready(store)
    # GROWW listed 3 sessions ago and has a bar for each: full coverage since its first bar (3/5).
    store.daily["GROWW"] = [date(2026, 6, 16), date(2026, 6, 15), date(2026, 6, 12)]
    gate = _gate(store, clock, symbols=("RELIANCE", "GROWW"))
    status = await gate.status()
    assert status.ready is True
    assert status.young_excluded == ["GROWW(3/5)"]
    assert not any(b.startswith("rsi2/trend/mom:GROWW") for b in status.blockers)


@pytest.mark.asyncio
async def test_young_excluded_but_gaps_and_old_shortfalls_still_block(clock):
    """The exclusion must not weaken the gate: only a fresh listing with FULL coverage is excluded.
    A young-aged symbol WITH an interior gap, and an OLD symbol with a gap, both still block."""
    store = FakeStore()
    store.daily["NIFTY 50"] = list(RECENT_5)          # regime covered (5/5)
    store.daily["INDIA VIX"] = list(RECENT_5[:3])     # regime covered (3/3)
    # (a) genuine young listing → EXCLUDED + reported
    store.daily["GROWW"] = [date(2026, 6, 16), date(2026, 6, 15), date(2026, 6, 12)]
    # (b) first bar is recent BUT there is an interior gap (missing 6/15) → NOT young → BLOCKS
    store.daily["GAPYOUNG"] = [date(2026, 6, 16), date(2026, 6, 12)]
    # (c) an OLD security (first bar 2019) with an in-window gap → NOT young → BLOCKS
    store.daily["OLDGAP"] = [date(2026, 6, 16), date(2026, 6, 12), date(2026, 6, 10)]
    store.spans["OLDGAP"] = (date(2019, 5, 1), 1700)
    gate = _gate(store, clock, symbols=("GROWW", "GAPYOUNG", "OLDGAP"))
    status = await gate.status()
    assert status.young_excluded == ["GROWW(3/5)"]
    assert status.ready is False
    assert "rsi2/trend/mom:GAPYOUNG daily bars 2/5" in status.blockers
    assert "rsi2/trend/mom:OLDGAP daily bars 3/5" in status.blockers
    assert not any(b.startswith("rsi2/trend/mom:GROWW") for b in status.blockers)


@pytest.mark.asyncio
async def test_young_listing_survives_todays_bar_landing(clock):
    """Regression (observed GROWW 2026-07-28): the evening daily_bars job writes TODAY'S bar, which
    the session window excludes — the span total then reads sessions-since-listing + 1 and the old
    ``total == since_listing`` young test failed, flipping the listing back to a blocker every
    evening. Young-ness is judged on in-window coverage only."""
    store = FakeStore()
    _fill_ready(store)
    # Full coverage for the 3 sessions since listing; the span carries one EXTRA bar (today's,
    # 2026-06-17, outside the strictly-before-today window).
    store.daily["GROWW"] = [date(2026, 6, 16), date(2026, 6, 15), date(2026, 6, 12)]
    store.spans["GROWW"] = (date(2026, 6, 12), 4)
    gate = _gate(store, clock, symbols=("RELIANCE", "GROWW"))
    status = await gate.status()
    assert status.ready is True
    assert status.young_excluded == ["GROWW(3/5)"]
    assert not any(b.startswith("rsi2/trend/mom:GROWW") for b in status.blockers)


# --------------------------------------------------------------------- coverage classes (2026-09-13)
@pytest.mark.asyncio
async def test_every_rendered_blocker_classifies_from_its_prefix(clock):
    """Classification reads the gate's OWN rendering, never a guess: one live blocker per class,
    produced by the gate itself, must land in its class — a rendering change that breaks this is a
    scoping bug, not a cosmetic one."""
    store = FakeStore()
    _fill_ready(store)
    store.gaps["RELIANCE"] = [FIXED_NOW.replace(hour=9, minute=30, second=0, microsecond=0)]
    store.daily["RELIANCE"] = RECENT_5[:3]
    store.spans["RELIANCE"] = (date(2007, 1, 1), 4000)
    store.daily["NIFTY 50"] = []
    status = await _gate(store, clock).status()

    assert {blocker_class(b) for b in status.blockers} == {CLASS_INTRADAY, CLASS_DAILY, CLASS_REGIME}
    assert status.blockers_by_class[CLASS_INTRADAY] == ["orb:RELIANCE bars 49/50"]
    assert status.blockers_by_class[CLASS_DAILY] == ["rsi2/trend/mom:RELIANCE daily bars 3/5"]
    assert status.blockers_by_class[CLASS_REGIME] == ["regime:NIFTY 50 daily bars 0/5"]
    assert not status.ready_for(CLASS_INTRADAY)
    assert not status.ready_for(CLASS_DAILY)
    assert not status.ready_for(CLASS_REGIME)


def test_blocker_class_falls_back_to_unknown():
    """An unclassifiable line (the composition root's fail-closed sentinel, the lifecycle's
    "warmup check failed", a future scope) is UNKNOWN — and UNKNOWN holds every class down."""
    assert blocker_class("warmup:unrefreshed 0/0") == CLASS_UNKNOWN
    assert blocker_class("warmup check failed") == CLASS_UNKNOWN
    status = WarmupStatus(ready=False, blockers=["warmup:unrefreshed 0/0"])
    assert not any(status.ready_for(c) for c in (CLASS_INTRADAY, CLASS_DAILY, CLASS_REGIME))


def test_ready_for_is_per_class_with_mixed_blockers():
    status = WarmupStatus(ready=False, blockers=[
        "orb:AAA bars 12/50", "orb:BBB bars 40/50", "regime:INDIA VIX daily bars 4/20",
    ])
    assert status.ready is False                      # the whole-gate answer is unchanged
    assert status.ready_for(CLASS_INTRADAY) is False
    assert status.ready_for(CLASS_REGIME) is False
    assert status.ready_for(CLASS_DAILY) is True      # nothing daily is short


def test_ready_for_fails_closed_on_an_unattributable_not_ready_status():
    """Not ready with NOTHING attributed cannot be scoped to a class, so no class is ready; an
    unrecognised class name is never "ready" either (a typo must not open a leg)."""
    assert WarmupStatus(ready=False).ready_for(CLASS_DAILY) is False
    assert WarmupStatus(ready=True).ready_for("intradya") is False
    assert WarmupStatus(ready=True).ready_for(CLASS_DAILY) is True


# ------------------------------------------------------------------ per-symbol readiness (2026-09-17)
def test_ready_for_is_per_symbol():
    """Owner-directed 2026-09-17. One symbol's coverage hole refuses THAT symbol's candidates and no
    others — the 2026-09-16 13:36 PTCIL tradeless minute took the whole intraday book down from
    13:37 to the close, and OLAELEC's daily hole froze the book for ~12 hours on 09-15. Class-wide
    (``symbol=None``) keeps its old meaning: the freeze/lift/notice code asks a global question."""
    status = WarmupStatus(ready=False, blockers=[
        "orb:RELIANCE bars 10/50",
        f"{DAILY_STRATEGY_SCOPE}:RELIANCE daily bars 3/200",
    ])
    assert status.ready_for(CLASS_INTRADAY, "RELIANCE") is False
    assert status.ready_for(CLASS_INTRADAY, "TCS") is True
    assert status.ready_for(CLASS_DAILY, "RELIANCE") is False
    assert status.ready_for(CLASS_DAILY, "TCS") is True
    assert status.ready_for(CLASS_INTRADAY) is False          # class-wide is unchanged
    assert status.ready_for(CLASS_DAILY) is False
    assert status.ready_for(CLASS_REGIME, "TCS") is True      # nothing regime is short

    # A line in the class that attributes to NOBODY refuses every symbol in it (fail closed, R6) —
    # and only in it: the daily class still answers per symbol.
    unattributed = WarmupStatus(ready=False, blockers=[
        "orb:RELIANCE bars 10/50",
        f"{DAILY_STRATEGY_SCOPE}:RELIANCE daily bars 3/200",
        "orb:?? garbage",
    ])
    assert blocker_class("orb:?? garbage") == CLASS_INTRADAY
    assert blocker_symbol("orb:?? garbage") is None
    assert unattributed.ready_for(CLASS_INTRADAY, "TCS") is False
    assert unattributed.ready_for(CLASS_INTRADAY, "RELIANCE") is False
    assert unattributed.ready_for(CLASS_DAILY, "TCS") is True

    # An UNKNOWN-CLASS line holds every class and every symbol down, exactly as before.
    unknown = WarmupStatus(ready=False, blockers=[
        "orb:RELIANCE bars 10/50",
        f"{DAILY_STRATEGY_SCOPE}:RELIANCE daily bars 3/200",
        "warmup check failed",
    ])
    for cls in (CLASS_INTRADAY, CLASS_DAILY, CLASS_REGIME):
        assert unknown.ready_for(cls, "TCS") is False
        assert unknown.ready_for(cls) is False


def test_blocker_symbol_parses_every_rendering():
    """The parse is anchored over EXACTLY the three lines ``WarmupGate._evaluate`` renders, so a
    symbol carrying a space, ``&`` or ``-`` comes back whole. Anything else is UNATTRIBUTABLE
    (None) rather than a guessed symbol — the fail-closed input ``ready_for`` depends on."""
    assert blocker_symbol("orb:PTCIL bars 374/375") == "PTCIL"
    assert blocker_symbol("orb:BAJAJ-AUTO bars 1/50") == "BAJAJ-AUTO"
    assert blocker_symbol(f"{DAILY_STRATEGY_SCOPE}:M&M daily bars 193/200") == "M&M"
    assert blocker_symbol(f"{DAILY_STRATEGY_SCOPE}:GVT&D daily bars 3/200") == "GVT&D"
    assert blocker_symbol("regime:NIFTY 50 daily bars 0/200") == "NIFTY 50"
    assert blocker_symbol("regime:INDIA VIX daily bars 4/20") == "INDIA VIX"
    assert blocker_symbol(f"{DAILY_STRATEGY_SCOPE}:NIFTY 50 calendar horizon < 200 sessions") == "NIFTY 50"
    assert blocker_symbol("regime:INDIA VIX calendar horizon < 20 sessions") == "INDIA VIX"
    assert blocker_symbol("orb:NIFTY 50 bars 10/50") == "NIFTY 50"
    # Unattributable shapes: the fail-closed sentinels and any future rendering.
    assert blocker_symbol("warmup:unrefreshed 0/0") is None
    assert blocker_symbol("warmup check failed") is None
    assert blocker_symbol("orb:?? garbage") is None
    assert blocker_symbol("orb:RELIANCE bars 10") is None


def test_short_symbols_lists_attributed_symbols_once():
    """The owner notice counts SYMBOLS, not blocker lines: order preserved, de-duplicated, and
    unattributable lines omitted (they are not a symbol — ``ready_for`` is what handles them)."""
    status = WarmupStatus(ready=False, blockers=[
        f"{DAILY_STRATEGY_SCOPE}:OLAELEC daily bars 193/200",
        f"{DAILY_STRATEGY_SCOPE}:M&M daily bars 12/200",
        f"{DAILY_STRATEGY_SCOPE}:OLAELEC calendar horizon < 200 sessions",
        f"{DAILY_STRATEGY_SCOPE}:?? garbage",
        "orb:PTCIL bars 374/375",
    ])
    assert status.short_symbols(CLASS_DAILY) == ["OLAELEC", "M&M"]
    assert status.short_symbols(CLASS_INTRADAY) == ["PTCIL"]
    assert status.short_symbols(CLASS_REGIME) == []


# --------------------------------------------------------------------- lifecycle consequence (§2.6)
class _FakeGate:
    def __init__(self, ready: bool, blockers: list[str] | None = None):
        self._status = WarmupStatus(ready=ready, blockers=blockers or [])

    async def status(self) -> WarmupStatus:
        return self._status


@pytest.mark.asyncio
async def test_lifecycle_freezes_entries_when_warmup_not_ready(conn, clock, temp_config, monkeypatch):
    """§2.6 step 6: a REGIME-class shortfall ⇒ FROZEN-for-entries via the risk-state setter +
    WARMUP_FROZEN alert — never trade on thin data. Cold start too close to the window is exactly
    this path (chaos 18). The intraday line rides along and must NOT read as the cause (2026-09-13;
    the freezing blocker is a ``regime:`` one since 2026-09-17, when DAILY stopped freezing)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    blockers = ["orb:RELIANCE bars 12/50", "regime:NIFTY 50 daily bars 0/200"]
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=blockers)

    report = await lifecycle.startup(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons
    assert report.warmup_blockers == blockers
    assert report.warmup_classes_short == ["intraday", "regime"]
    assert mode.risk_state() == RiskState.FROZEN
    frozen_msg = next(m for m in sent if str(m.kind) == "warmup_frozen")
    assert "orb:RELIANCE bars 12/50" in frozen_msg.body
    assert "classes short: intraday, regime — entries FROZEN" in frozen_msg.body
    # The summary rides its OWN field: every element of ``blockers`` stays a rendered
    # "scope: have/need" line, which is the contract catalog.warmup_frozen states and the shape
    # anything re-classifying the payload depends on.
    assert frozen_msg.data["blockers"] == blockers
    assert frozen_msg.data["classes"] == ["intraday", "regime"]


@pytest.mark.asyncio
@pytest.mark.parametrize("blockers", [
    ["regime:NIFTY 50 daily bars 0/200"],                       # REGIME class alone
    ["orb:RELIANCE bars 12/50", "regime:INDIA VIX daily bars 4/20"],
    ["rsi2/trend/mom:AAA daily bars 3/200", "regime:INDIA VIX daily bars 4/20"],
    ["warmup check failed"],                                    # UNATTRIBUTABLE ⇒ freezes (R6)
])
async def test_lifecycle_freezes_on_every_freezing_class(conn, clock, temp_config, monkeypatch, blockers):
    """2026-09-17: the freezing set is REGIME ∪ UNATTRIBUTABLE and nothing else. A non-freezing class
    riding along in the same shortfall never stops the freeze the regime/unknown line owes."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, store, lifecycle = _build(conn, clock, temp_config)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=blockers)

    report = await lifecycle.startup(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_lifecycle_freezes_when_a_not_ready_gate_renders_no_blocker(conn, clock, temp_config, monkeypatch):
    """The class scoping may only ever narrow a shortfall it can NAME. A duck-typed gate answering
    not-ready with an EMPTY blocker list is unattributable, so it must take the freeze — the empty
    list must never read as "nothing outside the intraday class ⇒ leave the risk state alone"."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, _kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=[])

    report = await lifecycle.startup(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons
    assert report.warmup_classes_short == [CLASS_UNKNOWN]
    assert mode.risk_state() == RiskState.FROZEN
    assert any(str(m.kind) == "warmup_frozen" for m in sent)

    # …and the post-login half of the same seam re-freezes rather than lifting.
    res = await lifecycle.reapply_warmup_gate()
    assert res.outcome == "frozen" and res.classes_short == [CLASS_UNKNOWN]


@pytest.mark.asyncio
async def test_lifecycle_does_not_freeze_on_intraday_only_shortfall(conn, clock, temp_config, monkeypatch):
    """2026-09-13 plan change: one symbol's minute hole (09-04 11:09 COROMANDEL) or a market-wide
    one-bar hole after a reconnect (09-09 14:47) no longer freezes the daily-bar legs. Visible, not
    silent: no WARMUP_FROZEN page (nothing froze), so the OWNER-FACING notice is the STARTUP_REPORT
    body itself — `frozen: none` with no coverage line is the silence this whole branch creates."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, _kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:COROMANDEL bars 113/114"])

    report = await lifecycle.startup(check_skew=False)
    assert report.frozen_reasons == []
    assert mode.risk_state() == RiskState.NORMAL
    assert report.warmup_blockers == ["orb:COROMANDEL bars 113/114"]
    assert report.warmup_classes_short == ["intraday"]
    assert "warmup_intraday_not_ready" in report.notes
    assert not any(str(m.kind) == "warmup_frozen" for m in sent)

    # The owner's boot message NAMES the class and the blocker behind it — this is the only
    # Telegram message this state produces, so it may not read as a clean boot.
    startup = next(m for m in sent if str(m.kind) == "startup_report")
    assert "warm-up short: intraday (orb:COROMANDEL bars 113/114)" in startup.body
    assert "frozen: none" in startup.body
    assert startup.data["warmup_classes_short"] == ["intraday"]


@pytest.mark.asyncio
async def test_lifecycle_does_not_freeze_on_daily_only_shortfall(conn, clock, temp_config, monkeypatch):
    """2026-09-17 plan change: DAILY stopped being a freezing class. On 2026-09-15 ONE symbol short
    of the 200-session lookback (``rsi2/trend/mom:OLAELEC daily bars 193/200``) set the GLOBAL
    ``warmup_ready`` FROZEN cause and held it for ~12 hours; its swing/position candidates are now
    refused one by one at the gate instead, and the book keeps trading. Visible, not silent: the
    STARTUP_REPORT names the class, exactly as it does for an intraday-only shortfall."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, _kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    blocker = "rsi2/trend/mom:OLAELEC daily bars 193/200"
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=[blocker])

    report = await lifecycle.startup(check_skew=False)
    assert report.frozen_reasons == []
    assert mode.risk_state() == RiskState.NORMAL
    assert report.warmup_blockers == [blocker]
    assert report.warmup_classes_short == ["daily"]
    assert "warmup_daily_not_ready" in report.notes
    assert not any(str(m.kind) == "warmup_frozen" for m in sent)

    startup = next(m for m in sent if str(m.kind) == "startup_report")
    assert f"warm-up short: daily ({blocker})" in startup.body
    assert "frozen: none" in startup.body
    assert startup.data["warmup_classes_short"] == ["daily"]


@pytest.mark.asyncio
async def test_startup_report_says_nothing_about_warmup_when_every_class_is_covered(
    conn, clock, temp_config, monkeypatch
):
    """The coverage line is conditional: a clean boot must not grow a permanent empty line that the
    owner learns to skip past (which is how the line stops being read at all)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    _mode, _kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=True)

    await lifecycle.startup(check_skew=False)
    startup = next(m for m in sent if str(m.kind) == "startup_report")
    assert "warm-up short" not in startup.body


@pytest.mark.asyncio
async def test_reapply_lifts_with_intraday_still_short(conn, clock, temp_config, monkeypatch):
    """The LIFT is owed to the freezing classes: regime covered ⇒ the ``warmup_ready`` cause clears
    through the latch even though the intraday class is still short. ``ready`` stays False (coverage
    IS missing); the outcome names the state, and the blockers still ride the result so post-login
    detail keeps naming them. The boot freeze is a real regime-class one, so the lift is exercised
    against a cause the same lifecycle set — not a hand-planted latch row."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from tests.unit.test_lifecycle_selftest import _build_with_latch

    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["regime:NIFTY 50 daily bars 0/200"])
    await lifecycle.startup(check_skew=False)
    assert mode.risk_state() == RiskState.FROZEN

    # The regime history lands; the minute hole does not heal.
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:AAA bars 165/182"])
    res = await lifecycle.reapply_warmup_gate()
    assert res.outcome == "short_lifted"
    assert res.ready is False and res.lifted is True and res.froze is False
    assert res.blockers == ["orb:AAA bars 165/182"]
    assert res.classes_short == ["intraday"]
    assert mode.risk_state() == RiskState.NORMAL
    assert all(c != "warmup_ready" for c, _s, _d in latch.active_causes())


@pytest.mark.asyncio
async def test_reapply_lifts_with_daily_still_short(conn, clock, temp_config, monkeypatch):
    """2026-09-17: the DAILY class joined INTRADAY on the non-freezing side, so one symbol's daily
    hole no longer holds the lift. (On 2026-09-15 ``rsi2/trend/mom:OLAELEC daily bars 193/200`` held
    the global FROZEN cause for ~12 hours.) Its candidates are refused per symbol at the gate."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from tests.unit.test_lifecycle_selftest import _build_with_latch

    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["regime:NIFTY 50 daily bars 0/200"])
    await lifecycle.startup(check_skew=False)
    assert mode.risk_state() == RiskState.FROZEN

    lifecycle._warmup_gate = _FakeGate(
        ready=False, blockers=["rsi2/trend/mom:OLAELEC daily bars 193/200"])
    res = await lifecycle.reapply_warmup_gate()
    assert res.outcome == "short_lifted"
    assert res.ready is False and res.lifted is True and res.froze is False
    assert res.classes_short == ["daily"]
    assert mode.risk_state() == RiskState.NORMAL
    assert all(c != "warmup_ready" for c, _s, _d in latch.active_causes())


@pytest.mark.asyncio
async def test_reapply_refreezes_on_a_regime_class_shortfall(conn, clock, temp_config, monkeypatch):
    """The other direction: a regime-class shortfall at post-login re-freezes, never lifts — every
    candidate reads the market context built from NIFTY 50 / India VIX, so there is no per-symbol
    answer to give."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from tests.unit.test_lifecycle_selftest import _build_with_latch

    mode, _kill, _latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["regime:NIFTY 50 daily bars 0/200"])

    res = await lifecycle.reapply_warmup_gate()
    assert res.outcome == "frozen"
    assert res.froze is True and res.lifted is False
    assert res.classes_short == ["regime"]
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_lifecycle_stays_normal_when_warmup_ready(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=True)

    report = await lifecycle.startup(check_skew=False)
    assert "warmup_ready" in report.notes
    assert report.frozen_reasons == []
    assert mode.risk_state() == RiskState.NORMAL
    assert not any(str(m.kind) == "warmup_frozen" for m in sent)


@pytest.mark.asyncio
async def test_selftest_freshness_surfaces_warmup_fail(conn, clock, temp_config, monkeypatch):
    """The §3.2.12 pre-entries self-test rides the same gate, split per class on 2026-09-17 (the
    residue the 09-13 addendum registered): a GLOBAL shortfall (regime / unattributable) is still
    FAIL implying FROZEN, while a per-symbol INTRADAY or DAILY one is a WARN implying nothing — a
    self-test that kept implying FROZEN would re-impose the global freeze the moment the standalone
    ``run()`` (or a dashboard/CLI selftest endpoint) was wired."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)
    lifecycle._selftest._warmup_gate = _FakeGate(ready=False, blockers=["regime:NIFTY 50 daily bars 0/200"])
    report = await lifecycle._selftest.run(check_skew=False)
    check = next(c for c in report.checks if c.name == "warmup_ready")
    assert check.status.value == "FAIL"
    assert "regime:NIFTY 50 daily bars 0/200" in check.detail
    assert "warmup_ready" in report.frozen_reasons

    # A per-symbol DAILY hole: WARN, no frozen reason, and the report still reads OK overall.
    lifecycle._selftest._warmup_gate = _FakeGate(
        ready=False, blockers=["rsi2/trend/mom:OLAELEC daily bars 193/200"])
    report = await lifecycle._selftest.run(check_skew=False)
    check = next(c for c in report.checks if c.name == "warmup_ready")
    assert check.status.value == "WARN"
    assert check.implies.value == "none"
    assert "per-symbol coverage short: rsi2/trend/mom:OLAELEC daily bars 193/200" == check.detail
    assert "warmup_ready" not in report.frozen_reasons

    # An UNATTRIBUTABLE blocker is a global condition (R6) and keeps the FROZEN implication, and so
    # does a duck-typed status with no per-class answer at all (the flat fallback, never looser).
    lifecycle._selftest._warmup_gate = _FakeGate(ready=False, blockers=["warmup check failed"])
    report = await lifecycle._selftest.run(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons

    class _FlatGate:
        async def status(self):
            return SimpleNamespace(ready=False, blockers=["orb:AAA bars 1/50"])

    lifecycle._selftest._warmup_gate = _FlatGate()
    report = await lifecycle._selftest.run(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons
