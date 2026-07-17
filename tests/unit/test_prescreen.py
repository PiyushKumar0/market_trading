"""``SignalPreScreen`` dedupe / per-day caps / publication + the scanner registry seam (§3.2.5).

Stub scanners (NOT registered — the global ``SCANNER_REGISTRY`` stays exactly the four §6.1 price
baselines) emit one candidate per scanned bar so the pre-screen's own bookkeeping is what is under
test, isolated from real rule logic (covered in ``test_scanners.py``).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import BaseModel

from engine.core.clock import IST
from engine.core.eventbus import EventBus
from engine.core.types import Bar
from engine.strategy.prescreen import SIGNAL_CANDIDATE_TOPIC, SignalPreScreen
from engine.strategy.scanners import (
    SCANNER_REGISTRY,
    OrbScanner,
    Scanner,
    build_enabled_scanners,
    params_from_envelope,
    register,
)
from engine.strategy.types import ScanContext


def _bar(symbol: str = "TCS", day: int = 17, hh: int = 10, mm: int = 0) -> Bar:
    return Bar(symbol=symbol, ts_minute=datetime(2026, 6, day, hh, mm, tzinfo=IST),
               open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
               close=Decimal("100.50"), volume=1000)


def _stub(sid: str = "stub") -> Scanner:
    """A scanner that always emits one BUY candidate for the scanned bar (never registered)."""

    class _Stub(Scanner):
        strategy_id = sid
        style = "intraday"
        DEFAULT_PARAMS = {}

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            return [self._candidate(bar=bar, ctx=ctx, side="BUY", entry=bar.close, score=1.0)]

    return _Stub()


def _prescreen(scanners=None, bus=None, **kw) -> SignalPreScreen:
    return SignalPreScreen(scanners or [_stub()], lambda bar: ScanContext(), bus, **kw)


# ---------------------------------------------------------------------------- dedupe (§3.2.5)
def test_dedupe_same_symbol_strategy_day():
    ps = _prescreen()
    assert len(ps.on_bar(_bar(mm=0))) == 1
    # The same (symbol, strategy) re-firing a minute later must NOT re-trigger Tier-1 (D5).
    assert ps.on_bar(_bar(mm=1)) == []
    # A different symbol is a different dedupe key.
    assert len(ps.on_bar(_bar(symbol="INFY", mm=2))) == 1


def test_dedupe_resets_on_new_day():
    ps = _prescreen()
    assert len(ps.on_bar(_bar(day=17))) == 1
    assert ps.on_bar(_bar(day=17, mm=5)) == []
    assert len(ps.on_bar(_bar(day=18))) == 1        # per-day state reset (day = bar date, no Clock)


def test_two_strategies_same_symbol_both_pass_dedupe():
    ps = _prescreen([_stub("s1"), _stub("s2")])
    out = ps.on_bar(_bar())
    assert [c.strategy_id for c in out] == ["s1", "s2"]   # scanner order preserved


# ---------------------------------------------------------------------------- per-day caps
def test_daily_cap_suppresses_after_limit():
    ps = _prescreen(max_candidates_per_day=2)
    assert len(ps.on_bar(_bar(symbol="AAA", mm=0))) == 1
    assert len(ps.on_bar(_bar(symbol="BBB", mm=1))) == 1
    assert ps.on_bar(_bar(symbol="CCC", mm=2)) == []      # cap hit — Tier-1 spam bounded (D5)
    assert len(ps.on_bar(_bar(symbol="CCC", day=18))) == 1  # new day, counter reset


def test_per_strategy_cap_binds_before_daily_cap():
    ps = _prescreen([_stub("s1"), _stub("s2")], max_candidates_per_day=20, max_per_strategy_day=1)
    assert len(ps.on_bar(_bar(symbol="AAA"))) == 2        # each strategy takes its 1
    assert ps.on_bar(_bar(symbol="BBB", mm=1)) == []      # both strategies exhausted; day cap untouched


def test_cap_constructor_validation():
    with pytest.raises(ValueError):
        _prescreen(max_candidates_per_day=0)
    with pytest.raises(ValueError):
        _prescreen(max_per_strategy_day=0)


# ---------------------------------------------------------------------------- publication
def test_on_bar_publishes_accepted_candidates():
    bus = EventBus()
    received = []

    async def handler(event):  # noqa: ANN001
        received.append(event)

    bus.subscribe(SIGNAL_CANDIDATE_TOPIC, handler)
    ps = _prescreen(bus=bus)
    out = ps.on_bar(_bar())
    assert len(out) == 1
    assert received == out                                # published on "signal.candidate"
    ps.on_bar(_bar(mm=1))                                 # deduped — nothing new published
    assert len(received) == 1


async def test_handle_bar_bus_adapter_scans_and_publishes():
    bus = EventBus()
    received = []

    async def handler(event):  # noqa: ANN001
        received.append(event)

    bus.subscribe(SIGNAL_CANDIDATE_TOPIC, handler)
    ps = _prescreen(bus=bus)
    await ps.handle_bar(_bar())
    assert len(received) == 1

    class NotABar(BaseModel):
        x: int = 1

    await ps.handle_bar(NotABar())                        # non-Bar events ignored, no crash
    assert len(received) == 1


# ---------------------------------------------------------------------------- determinism (§9.6)
def test_same_bar_same_decision_modulo_signal_id():
    a = _prescreen().on_bar(_bar())
    b = _prescreen().on_bar(_bar())
    assert a[0].model_dump(exclude={"signal_id"}) == b[0].model_dump(exclude={"signal_id"})
    assert a[0].signal_id != b[0].signal_id               # ULIDs are platform-minted, unique


# ---------------------------------------------------------------------------- registry seam
def test_registry_contains_exactly_the_phase1_price_baselines():
    assert set(SCANNER_REGISTRY) == {"mom", "orb", "rsi2", "trend"}
    # The Phase-3 `cat` scanner plugs in here as a peer (§6.1 row 5) — deliberately absent now (§8.2).
    assert "cat" not in SCANNER_REGISTRY


def test_build_enabled_scanners_order_types_and_params():
    scanners = build_enabled_scanners(["trend", "orb"], {"orb": {"vol_mult": 2.0}})
    assert [s.strategy_id for s in scanners] == ["trend", "orb"]
    assert scanners[1].params["vol_mult"] == 2.0
    assert scanners[1].params["rr_target"] == 1.5         # untouched keys keep §6.3 defaults


def test_build_enabled_scanners_unknown_id_fails_loud():
    with pytest.raises(ValueError, match="unknown scanner id"):
        build_enabled_scanners(["orb", "typo"])


def test_unknown_param_key_fails_loud():
    with pytest.raises(ValueError, match="unknown param"):
        OrbScanner({"vol_mult_typo": 2.0})


def test_params_from_envelope_strips_namespace():
    envelope = {"orb.vol_mult": 1.8, "orb.rr_target": 2.0, "rsi2.rsi_entry": 5}
    assert params_from_envelope("orb", envelope) == {"vol_mult": 1.8, "rr_target": 2.0}
    assert params_from_envelope("rsi2", envelope) == {"rsi_entry": 5.0}


def test_register_rejects_duplicate_strategy_id():
    class Impostor(Scanner):
        strategy_id = "orb"
        style = "intraday"

        def scan(self, bar, ctx):  # noqa: ANN001
            return []

    with pytest.raises(ValueError, match="already registered"):
        register(Impostor)
    assert SCANNER_REGISTRY["orb"] is OrbScanner          # registry untouched by the failed attempt
    assert register(OrbScanner) is OrbScanner             # re-registering the same class is idempotent
