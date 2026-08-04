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
from engine.strategy.types import PendingSetup, ScanContext


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


# ---------------------------------------------------------------------------- admit (brk20 sweep leg)
def _ext_cand(symbol: str, strategy_id: str = "brk20"):
    from engine.strategy.types import RawLevels, SignalCandidate
    return SignalCandidate(
        signal_id=f"sig-{symbol}-{strategy_id}", strategy_id=strategy_id, symbol=symbol,
        side="BUY", style="swing",
        raw_levels=RawLevels(entry=Decimal("103.00"), stop=Decimal("100.00")), score=0.65,
    )


def test_admit_applies_the_same_dedupe_and_caps_as_the_bar_path():
    """§3.2.5 (2026-08-04 brk20 addendum): externally-scanned candidates face the identical spine —
    same-day (symbol, strategy) dedupe and the shared daily cap — or the sweep is a cap bypass."""
    from datetime import date as _date
    ps = _prescreen(max_candidates_per_day=3)
    day = _date(2026, 6, 17)
    admitted = ps.admit([_ext_cand("BPCL"), _ext_cand("COALINDIA")], day)
    assert [c.symbol for c in admitted] == ["BPCL", "COALINDIA"]
    # Re-admitting the same pair the same day is a dedupe no-op.
    assert ps.admit([_ext_cand("BPCL")], day) == []
    # The daily cap is SHARED with the bar-driven path: one slot left, then suppression.
    assert len(ps.on_bar(_bar(symbol="TCS"))) == 1
    assert ps.on_bar(_bar(symbol="INFY", mm=1)) == []          # day cap (3) exhausted
    assert ps.admit([_ext_cand("RELIANCE")], day) == []        # and admit is equally bound
    # New day: everything resets.
    assert [c.symbol for c in ps.admit([_ext_cand("BPCL")], _date(2026, 6, 18))] == ["BPCL"]


# ---------------------------------------------------------------------------- hydrate (restart-proof day state)
def test_hydrate_restores_dedupe_and_caps_across_a_restart():
    """2026-08-04: the day state was process memory only, so a restart reset both bounds (observed:
    ~54 publications vs the 20/day cap across two mid-session restarts). A hydrated `seen` pair may
    not re-publish; hydrated `charged` pairs count against the caps."""
    from datetime import date as _date
    day = _date(2026, 6, 17)
    ps = _prescreen(max_candidates_per_day=3)
    ps.hydrate(day, seen=[("BPCL", "brk20"), ("TCS", "stub")],
               charged=[("BPCL", "brk20"), ("TCS", "stub")])
    # Evaluated pairs stay deduped after the "restart".
    assert ps.admit([_ext_cand("BPCL")], day) == []
    assert ps.on_bar(_bar(symbol="TCS")) == []
    # The cap counter survived too: 2 of 3 slots spent, one NEW pair fits, the next is suppressed.
    assert [c.symbol for c in ps.admit([_ext_cand("COALINDIA")], day)] == ["COALINDIA"]
    assert ps.admit([_ext_cand("RELIANCE")], day) == []
    assert ps.seen_today() == 3


def test_hydrate_charged_but_unseen_republishes_within_paid_quota():
    """A pair published but LOST IN FLIGHT (evaluated=0 — the crash/restart shape) re-publishes,
    and within its already-paid cap slot: the 2026-07-29 rearm semantics, now restart-proof."""
    from datetime import date as _date
    day = _date(2026, 6, 17)
    ps = _prescreen(max_candidates_per_day=2)
    ps.hydrate(day, seen=[("TCS", "stub")],
               charged=[("TCS", "stub"), ("BPCL", "brk20")])   # BPCL: published, never evaluated
    # Cap is already FULL (2 charged pairs) — a brand-new pair is suppressed...
    assert ps.admit([_ext_cand("RELIANCE")], day) == []
    # ...but the in-flight-lost pair re-publishes inside its paid quota.
    assert [c.symbol for c in ps.admit([_ext_cand("BPCL")], day)] == ["BPCL"]
    # And only once — the re-publish spends the slot again.
    assert ps.admit([_ext_cand("BPCL")], day) == []


def test_hydrate_day_rolls_forward_normally():
    """Hydrated state belongs to ITS day: the next session's first bar resets everything (§9.6)."""
    from datetime import date as _date
    ps = _prescreen(max_candidates_per_day=1)
    ps.hydrate(_date(2026, 6, 17), seen=[("TCS", "stub")], charged=[("TCS", "stub")])
    assert len(ps.on_bar(_bar(day=18, symbol="TCS"))) == 1


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


# ------------------------------------------------------------------ sweep addendum (2026-07-29)
def test_rearm_gives_back_the_day_slot():
    """A never-evaluated drop re-arms the (symbol, strategy) day slot so a still-true condition
    can re-publish (2026-07-29: six candidates burned by a broken analyst were dedupe-blocked for
    the rest of the day)."""
    ps = _prescreen()
    assert len(ps.on_bar(_bar(mm=0))) == 1
    assert ps.on_bar(_bar(mm=1)) == []                    # slot spent
    assert ps.rearm("TCS", "stub") is True
    assert len(ps.on_bar(_bar(mm=2))) == 1                # re-published after re-arm
    assert ps.rearm("TCS", "nosuch") is False             # nothing to give back


def test_caps_charge_once_per_pair_across_rearm_cycles():
    """2026-07-29 owner decision: caps bind on UNIQUE pairs. A re-armed pair re-publishes within
    its already-paid quota even with the cap full; a NEW pair is still suppressed. Without this,
    an out-of-window re-arm/re-publish cycle would exhaust the day cap with zero evaluations."""
    ps = _prescreen(max_candidates_per_day=1)
    assert len(ps.on_bar(_bar(symbol="AAA", mm=0))) == 1  # cap now full (1/1 unique pair)
    for cycle in range(3):                                # re-arm/re-publish churns freely
        assert ps.rearm("AAA", "stub") is True
        assert len(ps.on_bar(_bar(symbol="AAA", mm=1 + cycle))) == 1, f"cycle {cycle}"
    assert ps.on_bar(_bar(symbol="BBB", mm=9)) == []      # a NEW pair still hits the full cap
    assert ps.seen_today() == 1


def _pending_stub(sid: str = "pend") -> Scanner:
    """Never fires; always reports one pending arm level (never registered)."""

    class _Pending(Scanner):
        strategy_id = sid
        style = "swing"
        DEFAULT_PARAMS = {}

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            return []

        def pending(self, bar, ctx):  # noqa: ANN001 - test stub
            return [PendingSetup(strategy_id=sid, symbol=bar.symbol, side="BUY", style="swing",
                                 trigger_price=Decimal("95"), last_price=bar.close,
                                 condition="dip to the level")]

    return _Pending()


def test_sweep_scans_with_normal_dedupe_and_reports_pending():
    ps = SignalPreScreen([_stub(), _pending_stub()], lambda bar: ScanContext())
    accepted, pending = ps.sweep([_bar(symbol="AAA"), _bar(symbol="BBB", mm=1)])
    assert [c.symbol for c in accepted] == ["AAA", "BBB"]
    assert [p.symbol for p in pending] == ["AAA", "BBB"]
    assert ps.seen_today() == 2
    # Second sweep: the stub's day slots are spent — no re-publication, no double-counting; the
    # never-published pending strategy keeps reporting its level.
    accepted2, pending2 = ps.sweep([_bar(symbol="AAA", mm=2)])
    assert accepted2 == []
    assert [(p.symbol, p.strategy_id) for p in pending2] == [("AAA", "pend")]


def test_sweep_suppresses_pending_for_pairs_that_already_published():
    """Once (symbol, strategy) published today, its pending arm level is noise — the day slot is
    spent and a re-break cannot re-enter the pipeline (once-per-day rule)."""

    class _Both(Scanner):
        strategy_id = "both"
        style = "intraday"
        DEFAULT_PARAMS = {}

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            return [self._candidate(bar=bar, ctx=ctx, side="BUY", entry=bar.close, score=1.0)]

        def pending(self, bar, ctx):  # noqa: ANN001 - test stub
            return [PendingSetup(strategy_id="both", symbol=bar.symbol, side="SELL",
                                 style="intraday", trigger_price=Decimal("90"))]

    ps = SignalPreScreen([_Both()], lambda bar: ScanContext())
    accepted, pending = ps.sweep([_bar(symbol="AAA")])
    assert len(accepted) == 1
    assert pending == []                                  # published this very sweep — pending muted


def test_sweep_never_publishes_itself():
    """sweep() is worker-thread-safe BECAUSE publication is the caller's job (handle_bar's split):
    accepted candidates come back unpublished."""
    bus = EventBus()
    received = []

    async def handler(event):  # noqa: ANN001
        received.append(event)

    bus.subscribe(SIGNAL_CANDIDATE_TOPIC, handler)
    ps = _prescreen(bus=bus)
    accepted, _pending = ps.sweep([_bar()])
    assert len(accepted) == 1
    assert received == []                                 # nothing hit the bus from inside sweep()


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
