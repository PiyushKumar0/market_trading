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
from engine.ops.warmup import WarmupGate, WarmupStatus
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

    async def acoverage_gaps(self, symbol, start, end):
        self.intraday_ranges.append((symbol, start, end))
        return list(self.gaps.get(symbol, []))

    async def aget_bars_1d(self, symbol, start, end):
        return [SimpleNamespace(d=d) for d in self.daily.get(symbol, []) if start <= d <= end]


def _gate(store, clock, *, symbols=("RELIANCE",), **kw):
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    kw.setdefault("daily_lookback_sessions", 5)
    kw.setdefault("vix_lookback_sessions", 3)
    return WarmupGate(store, clock, calendar, symbols=list(symbols), **kw)


def _fill_ready(store):
    store.daily["RELIANCE"] = list(RECENT_5)
    store.daily["NIFTY 50"] = list(RECENT_5)
    store.daily["INDIA VIX"] = list(RECENT_5[:3])


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


# --------------------------------------------------------------------- lifecycle consequence (§2.6)
class _FakeGate:
    def __init__(self, ready: bool, blockers: list[str] | None = None):
        self._status = WarmupStatus(ready=ready, blockers=blockers or [])

    async def status(self) -> WarmupStatus:
        return self._status


@pytest.mark.asyncio
async def test_lifecycle_freezes_entries_when_warmup_not_ready(conn, clock, temp_config, monkeypatch):
    """§2.6 step 6: not-ready ⇒ FROZEN-for-entries via the risk-state setter + WARMUP_FROZEN alert —
    never trade on thin data. Cold start too close to the window is exactly this path (chaos 18)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:RELIANCE bars 12/50"])

    report = await lifecycle.startup(check_skew=False)
    assert "warmup_ready" in report.frozen_reasons
    assert report.warmup_blockers == ["orb:RELIANCE bars 12/50"]
    assert mode.risk_state() == RiskState.FROZEN
    frozen_msg = next(m for m in sent if str(m.kind) == "warmup_frozen")
    assert "orb:RELIANCE bars 12/50" in frozen_msg.body


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
    """The §3.2.12 pre-entries self-test rides the same gate: not-ready ⇒ FAIL implying FROZEN."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)
    lifecycle._selftest._warmup_gate = _FakeGate(ready=False, blockers=["regime:NIFTY 50 daily bars 0/200"])
    report = await lifecycle._selftest.run(check_skew=False)
    check = next(c for c in report.checks if c.name == "warmup_ready")
    assert check.status.value == "FAIL"
    assert "warmup_ready" in report.frozen_reasons
