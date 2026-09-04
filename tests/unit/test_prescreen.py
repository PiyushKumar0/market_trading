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


def _window(day: int = 17, start=(9, 15), end=(15, 30)) -> tuple[datetime, datetime]:
    return (datetime(2026, 6, day, *start, tzinfo=IST), datetime(2026, 6, day, *end, tzinfo=IST))


def _ctx(bar: Bar) -> ScanContext:
    """Default provider: a normally-OPEN trade window on the scanned bar's own date.

    Every test outside the "trade-window gate" section below is about dedupe/caps/ranking, so the
    default context models an open 09:15-15:30 window — otherwise the 2026-08-18 window gate
    (which fails closed on a ``None`` window) would refuse every candidate and mask what they test.
    """
    day = bar.ts_minute.day
    return ScanContext(
        trade_window=_window(day),
        session_open=datetime(2026, 6, day, 9, 15, tzinfo=IST),
    )


def _prescreen(scanners=None, bus=None, context_provider=None, **kw) -> SignalPreScreen:
    return SignalPreScreen(scanners or [_stub()], context_provider or _ctx, bus, **kw)


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
    ps = SignalPreScreen([_stub(), _pending_stub()], _ctx)
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

    ps = SignalPreScreen([_Both()], _ctx)
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


# ======================================================================== WO-1: ranked admission
def _scored_stub(sid: str, scores: dict[str, float]):
    """A scanner emitting one candidate per bar with a per-SYMBOL score (never registered)."""

    class _Scored(Scanner):
        strategy_id = sid
        style = "intraday"
        DEFAULT_PARAMS = {}

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            if bar.symbol not in scores:
                return []
            return [self._candidate(bar=bar, ctx=ctx, side="BUY", entry=bar.close,
                                    score=scores[bar.symbol])]

    return _Scored()


def _multi_stub(sid: str, scored_symbols: dict[str, float]):
    """A scanner that emits one candidate per entry in ``scored_symbols`` on EVERY bar — the
    batch-burst shape (``admit``/one bar producing many candidates at once)."""

    class _Multi(Scanner):
        strategy_id = sid
        style = "intraday"
        DEFAULT_PARAMS = {}

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            from engine.strategy.types import RawLevels, SignalCandidate
            return [
                SignalCandidate(
                    signal_id=f"sig-{sid}-{sym}", strategy_id=sid, symbol=sym, side="BUY",
                    style="intraday",
                    raw_levels=RawLevels(entry=Decimal("100"), stop=Decimal("99")), score=score,
                )
                for sym, score in scored_symbols.items()
            ]

    return _Multi()


def test_burst_admits_the_top_scored_not_the_earliest():
    """WO-1 (i): under a binding cap the batch is admitted SCORE-DESCENDING, so the best candidate
    of a burst takes the last free slot instead of whichever scanner happened to emit first.
    Pre-WO-1 this admitted LOW/MID (arrival order) and dropped the two best."""
    burst = {"LOW": 0.10, "MID": 0.50, "TOP": 0.95, "HIGH": 0.80}
    ps = _prescreen([_multi_stub("s1", burst)], max_candidates_per_day=2)
    out = ps.on_bar(_bar(symbol="DRIVER"))
    assert [c.symbol for c in out] == ["TOP", "HIGH"]


def test_burst_ties_keep_arrival_order_and_admission_stays_deterministic():
    """Ties in score fall back to emission order (``sorted`` is stable), so §9.6 replay of the same
    bar stream still reproduces the same decisions byte-for-byte."""
    burst = {"A": 0.50, "B": 0.50, "C": 0.90}
    ps = _prescreen([_multi_stub("s1", burst)], max_candidates_per_day=2)
    assert [c.symbol for c in ps.on_bar(_bar(symbol="DRIVER"))] == ["C", "A"]


def test_admission_mode_arrival_is_the_rollback_to_pre_wo1_order():
    """WO-1 risk note: the rollback is a config flag, not a revert. ``arrival`` restores the exact
    pre-WO-1 behaviour — first emitted, first admitted, score ignored."""
    burst = {"LOW": 0.10, "MID": 0.50, "TOP": 0.95}
    ps = _prescreen([_multi_stub("s1", burst)], max_candidates_per_day=2,
                    admission_mode="arrival")
    assert [c.symbol for c in ps.on_bar(_bar(symbol="DRIVER"))] == ["LOW", "MID"]
    with pytest.raises(ValueError, match="admission_mode"):
        _prescreen(admission_mode="whatever")


def test_admit_batch_is_ranked_too():
    """The brk20 daily sweep is the biggest real burst (dozens of candidates in ONE admit call) —
    it must face the same score-descending selection as the bar path."""
    from datetime import date as _date

    from engine.strategy.types import RawLevels, SignalCandidate

    def _c(symbol: str, score: float) -> SignalCandidate:
        return SignalCandidate(
            signal_id=f"sig-{symbol}", strategy_id="brk20", symbol=symbol, side="BUY",
            style="swing", raw_levels=RawLevels(entry=Decimal("103"), stop=Decimal("100")),
            score=score,
        )

    ps = _prescreen([], max_candidates_per_day=2)
    out = ps.admit([_c("AAA", 0.2), _c("BBB", 0.9), _c("CCC", 0.55)], _date(2026, 6, 17))
    assert [c.symbol for c in out] == ["BBB", "CCC"]


# ======================================================================== WO-1: per-strategy caps
def test_per_strategy_cap_mapping_binds_per_strategy():
    """WO-1 (iii): the sub-cap becomes a per-strategy MAP; ``default`` binds any strategy without
    its own line (the structural ≤40%-of-the-day guarantee for strategies added later)."""
    ps = _prescreen(
        [_multi_stub("orb", {"A": 0.9, "B": 0.8, "C": 0.7}),
         _multi_stub("rsi2", {"D": 0.6, "E": 0.5, "F": 0.4}),
         _multi_stub("mom", {"G": 0.3, "H": 0.2})],
        max_candidates_per_day=20,
        max_per_strategy_day={"default": 2, "orb": 1},
    )
    out = ps.on_bar(_bar(symbol="DRIVER"))
    by_strategy: dict[str, list[str]] = {}
    for c in out:
        by_strategy.setdefault(c.strategy_id, []).append(c.symbol)
    assert by_strategy["orb"] == ["A"]                    # explicit cap 1, top-scored takes it
    assert by_strategy["rsi2"] == ["D", "E"]              # `default` 2
    assert by_strategy["mom"] == ["G", "H"]               # `default` 2


def test_an_orb_flood_cannot_starve_rsi2():
    """WO-1 acceptance: an orb flood must not consume slots rsi2 could have used. On 2026-08-11 orb
    took 55% of the day's publications purely by arriving first; with orb capped at 6/20 the flood
    stops at its cap and rsi2's later, lower-scored candidates still publish."""
    orb_flood = {f"ORB{i}": 0.99 - i / 100 for i in range(30)}
    ps = _prescreen(
        [_multi_stub("orb", orb_flood), _multi_stub("rsi2", {"RSI_A": 0.30, "RSI_B": 0.25})],
        max_candidates_per_day=20,
        max_per_strategy_day={"default": 8, "orb": 6},
    )
    out = ps.on_bar(_bar(symbol="DRIVER"))
    ids = [c.strategy_id for c in out]
    assert ids.count("orb") == 6                          # capped, despite 30 higher-scored floods
    assert sorted(c.symbol for c in out if c.strategy_id == "rsi2") == ["RSI_A", "RSI_B"]
    # And orb took its cap with its BEST six, not its first six.
    assert sorted(c.symbol for c in out if c.strategy_id == "orb") == [
        "ORB0", "ORB1", "ORB2", "ORB3", "ORB4", "ORB5"
    ]


def test_per_strategy_cap_mapping_validation():
    with pytest.raises(ValueError):
        _prescreen(max_per_strategy_day={"orb": 0})


def test_scalar_per_strategy_cap_still_works():
    """The scalar form is the ``default``-only mapping — the shipped config used it before WO-1."""
    ps = _prescreen([_stub("s1"), _stub("s2")], max_candidates_per_day=20, max_per_strategy_day=1)
    assert len(ps.on_bar(_bar(symbol="AAA"))) == 2
    assert ps.on_bar(_bar(symbol="BBB", mm=1)) == []


# ======================================================================== WO-9: funnel counters
def test_funnel_counters_track_raw_published_and_suppressed():
    """WO-9: raw (what the scanners produced) vs published (what cleared dedupe/caps) is the top of
    the funnel; without it "the analyst never saw it" and "nothing fired" are indistinguishable."""
    from datetime import date as _date
    ps = _prescreen([_multi_stub("orb", {"A": 0.9, "B": 0.8, "C": 0.7})],
                    max_candidates_per_day=20, max_per_strategy_day={"orb": 2})
    ps.on_bar(_bar(symbol="DRIVER"))
    counters = ps.funnel_counters()
    assert counters["d"] == _date(2026, 6, 17)
    assert counters["raw"]["orb"] == 3
    assert counters["published"]["orb"] == 2
    assert counters["suppressed_cap"]["orb"] == 1
    assert ps.raw_counts(_date(2026, 6, 17)) == {"orb": 3}
    assert ps.raw_counts(_date(2026, 6, 18)) == {}        # another day's counters are not this day's


# ================================= raw counters survive a restart (2026-08-21, `funnel_raw_counts`)
#
# THE BUG this section pins. `raw` was the only WO-9 funnel number with no DB home, so a mid-session
# restart zeroed it and the 22:35 `funnel_utilization` line reported `raw=None` ("unmeasured") for
# the WHOLE day — on 4 of the last 6 trade days. That is WO-9's own "the analyst never saw it" vs
# "nothing fired" ambiguity, re-introduced by the restart. The counters are now seeded on every day
# roll (the first roll of a fresh process included) from what the drain tick flushed.

def test_raw_counters_continue_from_the_loader_instead_of_restarting_at_zero():
    """A fresh process rolls onto today, hydrates the day's persisted total, and counts ON from it."""
    from datetime import date as _date
    day = _date(2026, 6, 17)
    ps = _prescreen([_stub("orb")], raw_counts_loader=lambda d: {"orb": 40} if d == day else {})
    assert ps.raw_counts(day) == {}                       # nothing rolled yet — no day, no counters
    ps.on_bar(_bar(symbol="TCS"))                         # first roll hydrates, THEN counts this bar
    assert ps.raw_counts(day) == {"orb": 41}
    ps.on_bar(_bar(symbol="INFY", mm=1))
    assert ps.raw_counts(day) == {"orb": 42}
    # The published/suppressed counters are deliberately NOT hydrated — they describe what this
    # process's admission spine did, and the day-slot journal is their restart-proof record.
    assert ps.funnel_counters()["published"] == {"orb": 2}


def test_raw_counter_hydration_is_per_day_never_carried_forward():
    """A roll onto a genuinely NEW day loads that day's rows (none yet) and starts at zero — carrying
    yesterday's total into today would be the same lie ``raw_counts`` already refuses to tell."""
    from datetime import date as _date
    ps = _prescreen([_stub("orb")],
                    raw_counts_loader=lambda d: {"orb": 40} if d == _date(2026, 6, 17) else {})
    ps.on_bar(_bar(day=17))
    assert ps.raw_counts(_date(2026, 6, 17)) == {"orb": 41}
    ps.on_bar(_bar(day=18))
    assert ps.raw_counts(_date(2026, 6, 18)) == {"orb": 1}


def test_a_broken_raw_counts_loader_never_costs_a_scan():
    """D7: a telemetry read that cannot be served degrades to the pre-2026-08-21 behaviour (counting
    from zero) — it must never propagate out of the scan path."""
    from datetime import date as _date

    def _boom(_d):
        raise RuntimeError("database is locked")

    ps = _prescreen([_stub("orb")], raw_counts_loader=_boom)
    assert len(ps.on_bar(_bar())) == 1
    assert ps.raw_counts(_date(2026, 6, 17)) == {"orb": 1}


def test_hydrate_seeds_the_raw_counters_too():
    """The boot path (``engine.ops.main._hydrate_prescreen``) rolls the day through the same seam, so
    a restart's dedupe/cap state and its raw counters are restored in one step."""
    from datetime import date as _date
    day = _date(2026, 6, 17)
    ps = _prescreen([_stub("orb")], raw_counts_loader=lambda d: {"orb": 40, "rsi2": 6})
    ps.hydrate(day, seen=[("TCS", "orb")], charged=[("TCS", "orb")])
    assert ps.raw_counts(day) == {"orb": 40, "rsi2": 6}


# ================================================ trade-window gate (§7.1, 2026-08-18 origination fix)
#
# THE BUG this section pins. The caps bind on unique (symbol, strategy) pairs and are never refunded
# ("the spam bound counts attempts, not outcomes" — SignalPreScreen.rearm). Before this gate the
# pre-screen published on every bar from session open, the pipeline dropped each one as
# `signal_candidate_out_of_window` and re-armed it — but the CHARGE stayed. Live 2026-08-18: 15 of
# the day's 20 slots, and all 6 of orb's sub-cap, were spent before the trade window ever opened;
# the cap filled completely 44 s after it did, and the session produced 2,549 refusals and 0
# proposals. The gate refuses BEFORE charging, which is the only place a doomed candidate can be
# stopped — there is deliberately no decrement path for `_charged` anywhere in the codebase.

def _not_yet_open(bar: Bar) -> ScanContext:
    """Window opens LATER than the scanned bar — the live 2026-08-18 shape (owner window 10:10)."""
    return ScanContext(trade_window=_window(bar.ts_minute.day, start=(10, 10), end=(15, 30)),
                       session_open=datetime(2026, 6, bar.ts_minute.day, 9, 15, tzinfo=IST))


def _already_closed(bar: Bar) -> ScanContext:
    """Window already SHUT for the scanned bar (10:10-13:45 — today's early close)."""
    return ScanContext(trade_window=_window(bar.ts_minute.day, start=(10, 10), end=(13, 45)),
                       session_open=datetime(2026, 6, bar.ts_minute.day, 9, 15, tzinfo=IST))


def test_out_of_window_candidate_is_refused_before_the_open():
    assert _prescreen(context_provider=_not_yet_open).on_bar(_bar(hh=9, mm=52)) == []


def test_out_of_window_candidate_is_refused_after_the_close():
    assert _prescreen(context_provider=_already_closed).on_bar(_bar(hh=14, mm=0)) == []


def test_out_of_window_candidate_charges_no_day_slot():
    """THE regression. A pre-window bar must leave the pair able to publish once the window opens —
    that is the whole defect: those early fires permanently consumed the slot."""
    state = {"open": False}

    def provider(bar: Bar) -> ScanContext:
        return _ctx(bar) if state["open"] else _not_yet_open(bar)

    ps = _prescreen(context_provider=provider, max_candidates_per_day=1)
    for minute in range(5):                     # 09:52-09:56, exactly the live pre-window burst
        assert ps.on_bar(_bar(hh=9, mm=52 + minute)) == []
    state["open"] = True
    admitted = ps.on_bar(_bar(hh=10, mm=10))    # the window opens: the slot must still be there
    assert len(admitted) == 1
    assert admitted[0].symbol == "TCS"


def test_out_of_window_does_not_consume_the_per_strategy_sub_cap():
    """orb's 6-slot sub-cap was exhausted at 10:01 against a stale window, nine minutes before the
    real one opened — so the sub-cap must survive a shut window too, not just the day cap."""
    state = {"open": False}

    def provider(bar: Bar) -> ScanContext:
        return _ctx(bar) if state["open"] else _not_yet_open(bar)

    ps = _prescreen([_stub("orb")], context_provider=provider,
                    max_candidates_per_day=20, max_per_strategy_day={"orb": 2})
    for i in range(6):
        assert ps.on_bar(_bar(symbol=f"S{i}", hh=10, mm=0)) == []
    state["open"] = True
    assert len(ps.on_bar(_bar(symbol="A", hh=10, mm=10))) == 1
    assert len(ps.on_bar(_bar(symbol="B", hh=10, mm=11))) == 1
    assert ps.on_bar(_bar(symbol="C", hh=10, mm=12)) == []         # now the sub-cap genuinely binds


def test_absent_window_fails_closed():
    """No window ⇒ nothing originates: the R6/§2.7 fail-to-zero direction every other guard takes.
    A ``None`` here means "not a trading day" or an unreadable window, never "unrestricted"."""
    ps = _prescreen(context_provider=lambda bar: ScanContext())
    assert ps.on_bar(_bar()) == []


def test_window_edges_are_inclusive_on_the_bar_close_instant():
    """The instant tested is ts_minute + 1m — when an entry off this bar could actually be placed —
    matching OrbScanner.scan's `entry_dt`, so orb and the pre-screen agree to the minute."""
    from engine.strategy.prescreen import bar_in_trade_window
    w = _window(17, start=(10, 0), end=(10, 30))
    assert not bar_in_trade_window(_bar(hh=9, mm=58), w)    # closes 09:59 — before the open
    assert bar_in_trade_window(_bar(hh=9, mm=59), w)        # closes 10:00 — exactly the lower edge
    assert bar_in_trade_window(_bar(hh=10, mm=29), w)       # closes 10:30 — exactly the upper edge
    assert not bar_in_trade_window(_bar(hh=10, mm=30), w)   # closes 10:31 — past the close
    assert not bar_in_trade_window(_bar(hh=10, mm=0), None)


def test_admit_batch_path_honours_the_window_flag():
    """brk20/ins/cat carry no bar, so the sweep (which has the Clock) decides. Same property: a
    refused batch charges nothing, so a later in-window sweep still admits."""
    from datetime import date as _date
    ps = _prescreen([], max_candidates_per_day=1)
    day = _date(2026, 6, 17)
    cands = _multi_stub("brk20", {"A": 0.9}).scan(_bar(), _ctx(_bar()))
    assert ps.admit(cands, day, in_window=False) == []
    assert len(ps.admit(cands, day, in_window=True)) == 1


def test_admit_defaults_to_in_window_for_backward_compatibility():
    """The parameter is purely additive: omitting it reproduces pre-2026-08-18 behaviour exactly."""
    from datetime import date as _date
    ps = _prescreen([])
    cands = _multi_stub("brk20", {"A": 0.9}).scan(_bar(), _ctx(_bar()))
    assert len(ps.admit(cands, _date(2026, 6, 17))) == 1


def test_window_suppression_is_counted_apart_from_cap_suppression():
    """A large suppressed_window is healthy budget protection; a large suppressed_cap is starvation.
    Conflating them would make the WO-9 funnel line unreadable. `raw` still counts what fired."""
    ps = _prescreen([_multi_stub("orb", {"A": 0.9, "B": 0.8})], context_provider=_not_yet_open)
    ps.on_bar(_bar(hh=9, mm=52))
    counters = ps.funnel_counters()
    assert counters["raw"]["orb"] == 2                    # the scanners DID produce
    assert counters["suppressed_window"]["orb"] == 2
    assert counters["published"] == {}
    assert counters["suppressed_cap"] == {}


def test_window_gate_uses_bar_time_not_wall_clock():
    """§9.6: the pre-screen takes no Clock. The same bar stream must decide the same way whenever it
    is replayed, so the gate reads bar.ts_minute against ctx.trade_window and nothing else."""
    a = _prescreen(context_provider=_not_yet_open).on_bar(_bar(hh=9, mm=52))
    b = _prescreen(context_provider=_not_yet_open).on_bar(_bar(hh=9, mm=52))
    assert a == b == []
    assert len(_prescreen(context_provider=_ctx).on_bar(_bar(hh=9, mm=52))) == 1


# ============================== one ranked admission across batch legs (2026-08-18, the `cat` loss)
def test_admit_ranks_across_strategies_in_one_batch():
    """Live 2026-08-18: brk20 (0.552, 0.523) took the day's last slots and cat LT (0.82) was
    suppressed 94 ms later, because run_scan_sweep made three SEPARATE admit calls and WO-1 ranks
    only WITHIN a batch. `cat` is single-shot (MAX_EVENT_AGE_SESSIONS = 1), so that loss is
    permanent. Concatenating the legs into ONE admit is what lets the best candidate win."""
    from datetime import date as _date
    ps = _prescreen([], max_candidates_per_day=1)
    ctx, bar = _ctx(_bar()), _bar()
    brk20_leg = _multi_stub("brk20", {"OBEROIRLTY": 0.552}).scan(bar, ctx)
    cat_leg = _multi_stub("cat", {"LT": 0.82}).scan(bar, ctx)
    admitted = ps.admit(brk20_leg + cat_leg, _date(2026, 6, 17))   # brk20 FIRST in source order
    assert [c.strategy_id for c in admitted] == ["cat"]
    assert admitted[0].symbol == "LT"


# ================================ cap displacement (2026-08-27: the KALYANKJIL/TATACONSUM lockout)
def _scored(symbol: str, score: float, strategy_id: str = "orb", **kw):
    """One admissible candidate, built directly so a test can pin its score to the third decimal."""
    from engine.strategy.types import RawLevels, SignalCandidate
    base = {
        "signal_id": f"01{symbol}", "strategy_id": strategy_id, "symbol": symbol,
        "side": "BUY", "style": "intraday",
        "raw_levels": RawLevels(entry=Decimal("100"), stop=Decimal("99"), target=Decimal("103")),
        "score": score, "features_snapshot_id": "01SNAP", "catalyst_ref": None,
    }
    return SignalCandidate(**{**base, **kw})


def _the_day():
    from datetime import date as _date
    return _date(2026, 6, 17)


def test_a_better_later_candidate_displaces_an_unevaluated_slot():
    """THE 2026-08-27 LOCKOUT, in the shape it happened.

    All 7 of ``orb``'s daily admission slots were charged between 09:46:06 and 09:47:05 - inside 90
    seconds of the trade window opening. KALYANKJIL then fired at 12:49 at score **1.0**, which the
    scanner is designed to do (its entry window runs to 14:30; only the RANGE is the first 30
    minutes), and was refused outright with ``prescreen_cap_suppressed cap="strategy_day"``. Nothing
    about its quality could have saved it. Since 2026-08-27 the cap protects the analyst BUDGET
    rather than the morning's arrival order: the slot moves off the worst incumbent nobody looked at.
    """
    ps = _prescreen([], max_per_strategy_day={"orb": 3})
    day = _the_day()
    morning = [_scored("SHRIRAMFIN", 0.55), _scored("POLICYBZR", 0.50), _scored("HDFCAMC", 0.72)]
    assert len(ps.admit(morning, day)) == 3                    # 09:46 - the burst takes every slot

    # 12:49, a separate batch: KALYANKJIL is admitted, and POLICYBZR (the WORST unevaluated
    # incumbent at 0.50, not merely the oldest) is the one that gives up its slot.
    assert [c.symbol for c in ps.admit([_scored("KALYANKJIL", 1.0)], day)] == ["KALYANKJIL"]
    assert ps.take_displaced() == [("POLICYBZR", "orb")]
    assert ps.funnel_counters()["suppressed_cap"] == {}        # never a refusal - a reassignment

    # The SWAP keeps the cap exact: still 3 charged, so the analyst budget is untouched.
    assert ps._count_by_strategy["orb"] == 3
    assert ("POLICYBZR", "orb") not in ps._charged
    assert ("KALYANKJIL", "orb") in ps._charged


def test_an_evaluated_candidate_never_loses_its_slot():
    """INVARIANT #1, now load-bearing across three fixes (2026-07-29 rearm, the 2026-08-27 TTL
    refund, and displacement). A candidate the analyst was actually spent on keeps its slot against
    an arrival of ANY score - including a perfect one."""
    ps = _prescreen([], max_per_strategy_day={"orb": 2})
    day = _the_day()
    assert len(ps.admit([_scored("SHRIRAMFIN", 0.20), _scored("POLICYBZR", 0.21)], day)) == 2
    assert ps.claim_slot("SHRIRAMFIN", "orb") is True          # the pipeline spent an analyst call
    assert ps.claim_slot("POLICYBZR", "orb") is True

    assert ps.admit([_scored("KALYANKJIL", 1.0)], day) == []   # both incumbents are untouchable
    assert ps.take_displaced() == []
    assert ps.funnel_counters()["suppressed_cap"] == {"orb": 1}
    assert ps._charged == {("SHRIRAMFIN", "orb"), ("POLICYBZR", "orb")}
    # …and the refusal log says so: zero displaceable incumbents, the diagnostic the same single
    # scan that picks a victim now also produces.
    assert ps._displacement_scan_locked(_scored("KALYANKJIL", 1.0)).displaceable == 0


def test_a_displaced_pair_is_refused_the_analyst_slot_it_was_queued_for():
    """The other half of invariant #1: a freed slot must never be spent twice.

    The pipeline can be holding a forward-queue pointer to a pair the pre-screen has just displaced.
    ``claim_slot`` is the compare-and-set that tells it so - otherwise the displaced candidate and
    the candidate that took its slot would BOTH reach the analyst, breaching the cap by a real call.
    """
    ps = _prescreen([], max_per_strategy_day={"orb": 1})
    day = _the_day()
    assert len(ps.admit([_scored("POLICYBZR", 0.50)], day)) == 1
    assert len(ps.admit([_scored("KALYANKJIL", 1.0)], day)) == 1
    assert ps.claim_slot("POLICYBZR", "orb") is False          # displaced - the pipeline must skip
    assert ps.claim_slot("KALYANKJIL", "orb") is True          # the slot's new owner may spend it
    # Fail-open on anything this process never charged: refusing wrongly would silently withhold a
    # legitimate analyst call, while allowing wrongly costs at most one extra one.
    assert ps.claim_slot("TATACONSUM", "orb") is True


def test_displacement_needs_a_real_margin_not_a_rounding_error():
    """Sufficiently-better is ``displacement_margin`` (shipped 0.10), not a bare ``>``. Scores are
    clamped floats in [0, 1]; strict inequality would let 0.801 evict 0.800 - a publication and a
    journal write spent on noise, and an oscillation the margin makes structurally impossible."""
    from engine.strategy.prescreen import DEFAULT_DISPLACEMENT_MARGIN
    assert DEFAULT_DISPLACEMENT_MARGIN == 0.10
    day = _the_day()

    near = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(near.admit([_scored("POLICYBZR", 0.80)], day)) == 1
    assert near.admit([_scored("TATACONSUM", 0.89)], day) == []          # +0.09 - not enough
    assert near.funnel_counters()["suppressed_cap"] == {"orb": 1}

    at = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(at.admit([_scored("POLICYBZR", 0.80)], day)) == 1
    assert [c.symbol for c in at.admit([_scored("TATACONSUM", 0.90)], day)] == ["TATACONSUM"]

    # Saturation is the honest limit: orb clamps at 1.0 and fired 883 times at exactly 1.0 on
    # 2026-08-18. Equal incumbents are undisplaceable, which is correct - there is no basis to
    # prefer the later one - but it is why this fix cannot rescue every locked-out candidate.
    flat = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(flat.admit([_scored("SHRIRAMFIN", 1.0)], day)) == 1
    assert flat.admit([_scored("KALYANKJIL", 1.0)], day) == []


def test_displacement_is_budgeted_so_the_spam_bound_survives():
    """This is the first decrement path ``_charged`` has ever had, and the trade-window gate's
    docstring warns that one would erode the spam bound. A SWAP does not - the charged count is
    invariant, so analyst spend is still exactly the cap - but PUBLICATIONS would become unbounded.
    Displacements are budgeted at the strategy's own cap, so admissions per strategy per day are
    bounded by 2x cap with no second owner knob to drift out of sync with ``max_per_strategy_day``.
    """
    ps = _prescreen([], max_per_strategy_day={"orb": 2})
    day = _the_day()
    assert len(ps.admit([_scored("A", 0.10), _scored("B", 0.11)], day)) == 2
    for symbol, score in (("C", 0.30), ("D", 0.40)):            # 2 displacements = the whole budget
        assert [c.symbol for c in ps.admit([_scored(symbol, score)], day)] == [symbol]
    assert ps._displacements["orb"] == 2

    assert ps.admit([_scored("E", 1.0)], day) == []              # budget spent - a flat refusal
    assert ps.funnel_counters()["suppressed_cap"] == {"orb": 1}
    assert ps._count_by_strategy["orb"] == 2                     # the cap never moved
    assert len(ps.funnel_counters()["published_scores"]["orb"]) == 4    # 2x cap, and no more


def test_a_catalyst_candidate_is_never_evicted():
    """2.7 anti-manipulation: ``_catalyst_refs`` is the news-entry budget and it only GROWS.
    Displacing a catalyst-bearing candidate would decrement it and let news-originated entries churn
    the guard, so they are excluded from the victim pool outright (D7 - the carve-out fails to LESS
    activity). An ARRIVAL may still carry one; it simply faces the catalyst cap on its own."""
    ps = _prescreen([], max_per_strategy_day={"cat": 1}, catalyst_cap_fn=lambda: 5)
    day = _the_day()
    news = _scored("LT", 0.20, strategy_id="cat", catalyst_ref="01CATREF")
    assert len(ps.admit([news], day)) == 1
    assert ps._catalyst_refs == {"01CATREF"}

    assert ps.admit([_scored("SIEMENS", 1.0, strategy_id="cat")], day) == []
    assert ps._catalyst_refs == {"01CATREF"}                    # never given back
    assert ps.take_displaced() == []


def test_displacement_never_reaches_inside_one_batch():
    """Displacement decides CROSS-batch competition; WO-1's ranking decides intra-batch competition.
    They must not overlap: the accepted list is what the caller PUBLISHES, so a candidate admitted
    and then evicted by a later member of its own batch would be published holding no slot. Only the
    ``arrival`` rollback can order a batch worst-first, and a rollback flag that quietly changed
    behaviour would not be one."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1}, admission_mode="arrival")
    admitted = ps.admit([_scored("LOW", 0.10), _scored("TOP", 0.95)], _the_day())
    assert [c.symbol for c in admitted] == ["LOW"]              # pre-WO-1 order, cap bites at 1
    assert ps.take_displaced() == []


def test_displacement_replays_byte_for_byte():
    """9.6 DETERMINISM, checked rather than asserted.

    Every input to the decision - the charged set, each pair's admission score and sequence, the
    per-strategy counters and the displacement budget - is written only by the admission spine and
    derived only from the candidate stream in arrival order. No Clock is read on this path, the
    victim tie-break (score, then admission sequence) is TOTAL because two pairs cannot share a
    sequence, and float comparison of identical inputs is exact. So the same stream replays to the
    same admissions AND the same evictions.

    The one live-only input is ``_evaluated``, written by ``claim_slot`` on the pipeline's 3-minute
    drain cadence - the same composition-root-only coupling ``hydrate`` and ``raw_counts_loader``
    already carve out, and a replay never wires it. That is what this test pins: with no pipeline in
    the loop, the mechanism is a pure function of the stream.
    """
    day = _the_day()
    stream = [
        [_scored("A", 0.30), _scored("B", 0.30)],   # a TIE, so the seq tie-break is what decides
        [_scored("C", 0.55)],
        [_scored("D", 0.90)],
        [_scored("E", 0.31)],
    ]

    def replay():
        ps = _prescreen([], max_per_strategy_day={"orb": 2})
        admitted, displaced = [], []
        for batch in stream:
            admitted.append([c.symbol for c in ps.admit(batch, day)])
            displaced.append(ps.take_displaced())
        return admitted, displaced, sorted(ps._charged), dict(ps._count_by_strategy)

    first = replay()
    assert first == replay() == replay()
    # The tie is broken by ADMISSION ORDER, not by dict-iteration luck: A was admitted before B, so
    # A is the one C evicts. E (0.31) clears no incumbent by the margin and is refused.
    assert first[0] == [["A", "B"], ["C"], ["D"], []]
    assert first[1] == [[], [("A", "orb")], [("B", "orb")], []]


def test_displacement_margin_is_an_owner_knob_in_settings():
    """The margin is owner-tunable without a code deploy (2026-08-27), like every other threshold in
    this subsystem. ``config/settings.yaml`` is the authority; the module constant is only the
    fallback for a directly-constructed pre-screen (tests, replay, backtest), and the two must agree
    or the shipped engine and every test in this file would be arguing about different policy."""
    from engine.core.config import load_settings
    from engine.strategy.prescreen import DEFAULT_DISPLACEMENT_MARGIN

    shipped = load_settings().strategy.prescreen.displacement_margin
    assert shipped == DEFAULT_DISPLACEMENT_MARGIN == 0.10

    # And the knob is REAL: a wider margin refuses a displacement the shipped value would allow.
    day = _the_day()
    strict = _prescreen([], max_per_strategy_day={"orb": 1}, displacement_margin=0.50)
    assert len(strict.admit([_scored("POLICYBZR", 0.50)], day)) == 1
    assert strict.admit([_scored("TATACONSUM", 0.90)], day) == []        # +0.40 - under 0.50
    assert [c.symbol for c in strict.admit([_scored("KALYANKJIL", 1.0)], day)] == ["KALYANKJIL"]


def test_displacement_margin_none_is_the_rollback_flag():
    """``null`` in settings.yaml disables displacement entirely, restoring the pre-2026-08-27
    behaviour where whoever fires first keeps the strategy's slots for the whole session. The way
    ``admission_mode='arrival'`` is WO-1's rollback: a flag, not a revert."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1}, displacement_margin=None)
    day = _the_day()
    assert len(ps.admit([_scored("POLICYBZR", 0.10)], day)) == 1
    assert ps.admit([_scored("KALYANKJIL", 1.0)], day) == []             # refused flat, as before
    assert ps.take_displaced() == []
    assert ps.funnel_counters()["suppressed_cap"] == {"orb": 1}


def test_displacement_margin_rejects_a_nonsense_value():
    """A margin outside (0, 1] cannot mean anything against a [0, 1] clamped score: 0 would make
    displacement fire on ties (churn), and >1 could never be met. Fail at construction rather than
    silently never displacing, so a fat-fingered settings.yaml is a boot error, not a quiet regime
    change nobody notices until the funnel goes quiet."""
    for bad in (0.0, -0.1, 1.5):
        with pytest.raises(ValueError, match="displacement_margin"):
            _prescreen([], displacement_margin=bad)


# ================================= declined incumbents are displaceable (2026-09-04, the 09-03 lockout)
def test_a_declined_incumbent_is_displaceable_again():
    """2026-09-03: five analyst no_action verdicts held all five orb slots from 09:52/11:30 to the
    close (``displaceable=0`` on 3,984 refusals, UNITDSPR at 0.967 among them). A declined evaluation
    holds no position, so it must not hold the slot either: it stays SEEN (no re-publication) and
    is evicted by the first materially better arrival, exactly like an unevaluated incumbent."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1})
    day = _the_day()
    assert len(ps.admit([_scored("IREDA", 0.72)], day)) == 1
    assert ps.claim_slot("IREDA", "orb") is True
    assert ps.admit([_scored("UNITDSPR", 0.97)], day) == []      # evaluated, in flight: untouchable
    assert ps.decline("IREDA", "orb") is True                     # the analyst RAN and said no
    assert ps.admit([_scored("KEI", 0.79)], day) == []            # 0.07 better: not materially
    assert [c.symbol for c in ps.admit([_scored("UNITDSPR", 0.97)], day)] == ["UNITDSPR"]
    assert ps.take_displaced() == [("IREDA", "orb")]
    assert ("IREDA", "orb") not in ps._evaluated and ("IREDA", "orb") not in ps._declined
    assert ps._count_by_strategy["orb"] == 1                      # a swap, never a refund
    assert ps.decline("NOBODY", "orb") is False                   # never charged: nothing to decline


def test_a_declined_displacement_spends_the_day_budget_not_the_strategy_budget():
    """Evicting a declined incumbent costs one more analyst call, which the day-level displacement
    budget and the forward cap bound. The per-strategy budget bounds PRE-evaluation churn only — on
    2026-09-03 it was spent (5/5) by 11:30 and would otherwise have re-locked the afternoon."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1})
    day = _the_day()
    assert len(ps.admit([_scored("A", 0.30)], day)) == 1
    assert len(ps.admit([_scored("B", 0.50)], day)) == 1          # unevaluated A evicted: budget 1/1
    assert ps._displacements["orb"] == 1
    assert ps.claim_slot("B", "orb") and ps.decline("B", "orb")
    assert len(ps.admit([_scored("C", 0.70)], day)) == 1          # declined B evicted regardless
    assert ps.take_displaced() == [("A", "orb"), ("B", "orb")]
    assert ps._displacements["orb"] == 1                          # strategy budget untouched
    assert ps._day_displacements == 2                             # the day budget counted it


def test_the_day_displacement_budget_still_binds_declined_displacements():
    ps = _prescreen([], max_candidates_per_day=1, max_per_strategy_day={"orb": 1})
    day = _the_day()
    assert len(ps.admit([_scored("A", 0.30)], day)) == 1
    assert ps.claim_slot("A", "orb") and ps.decline("A", "orb")
    assert len(ps.admit([_scored("B", 0.90)], day)) == 1          # day budget 1/1 spent here
    assert ps.claim_slot("B", "orb") and ps.decline("B", "orb")
    assert ps.admit([_scored("C", 1.0)], day) == []               # budget exhausted: refused
    assert ps.funnel_counters()["suppressed_cap"] == {"orb": 1}


# =========================================== displacement/catalyst accounting audit (2026-08-28)
def test_displacements_are_bounded_by_the_day_cap_too():
    """The per-strategy displacement budget alone does not bound the DAY.

    Displacement fires on a full ``max_candidates_per_day`` as well as on a full per-strategy cap,
    but its budget was charged per strategy only — so N strategies could each spend their own budget
    against the ONE day cap and publish ~sum(caps) beyond it while ``_count_day`` still read the cap.
    A day-level budget (derived from ``max_candidates_per_day``, no new knob) closes it: total
    publications per day are bounded by 2x the day cap, exactly as they are per strategy."""
    ps = _prescreen([], max_candidates_per_day=2, max_per_strategy_day={"orb": 5, "rsi2": 5})
    day = _the_day()
    # The DAY cap is what binds; neither strategy is anywhere near its own.
    assert len(ps.admit([_scored("A", 0.10), _scored("B", 0.10, "rsi2")], day)) == 2
    assert [c.symbol for c in ps.admit([_scored("C", 0.90)], day)] == ["C"]
    assert [c.symbol for c in ps.admit([_scored("D", 0.90, "rsi2")], day)] == ["D"]
    assert ps._day_displacements == 2                       # = max_candidates_per_day, spent

    assert ps.admit([_scored("E", 1.0)], day) == []          # otherwise eligible, budget gone
    assert ps.funnel_counters()["suppressed_cap"] == {"orb": 1}
    assert ps._count_day == 2                                # the cap itself never moved
    published = ps.funnel_counters()["published"]
    assert sum(published.values()) == 4                      # 2x the day cap, and no more


def test_displacement_is_bounded_with_no_per_strategy_cap_at_all():
    """``max_per_strategy_day=None`` (the replay/backtest construction) makes ``_cap_for`` ``None``,
    which made the per-strategy budget test vacuous — every arrival could displace, forever. The
    day-level budget bounds it whether or not a per-strategy cap exists."""
    ps = _prescreen([], max_candidates_per_day=2, max_per_strategy_day=None)
    day = _the_day()
    assert len(ps.admit([_scored("A", 0.10), _scored("B", 0.11)], day)) == 2
    admitted = [
        c.symbol
        for i, score in enumerate((0.30, 0.40, 0.50, 0.60, 0.70, 0.80))
        for c in ps.admit([_scored(f"X{i}", score)], day)
    ]
    assert admitted == ["X0", "X1"]                          # bounded, not one per arrival
    assert ps._day_displacements == 2


def test_a_republished_pair_is_judged_on_its_newest_score():
    """``_admitted`` was written only on the FIRST charge, so a re-armed pair that re-published at a
    better level was still offered up for displacement at its stale score — the arrival was compared
    against a number no longer describing anything. The refresh is a fact about the candidate
    stream, so §9.6 replay is untouched."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1})
    day = _the_day()
    assert len(ps.admit([_scored("SHRIRAMFIN", 0.30)], day)) == 1
    assert ps.rearm("SHRIRAMFIN", "orb") is True
    # Re-publishes inside its already-paid quota, now at 0.95.
    assert len(ps.admit([_scored("SHRIRAMFIN", 0.95)], day)) == 1
    assert ps.admit([_scored("KALYANKJIL", 0.45)], day) == []   # 0.45 vs 0.95, not vs 0.30
    assert ps.take_displaced() == []


def test_rearm_gives_back_displaceability_too():
    """:meth:`claim_slot` marks a pair EVALUATED at dispatch; :meth:`rearm` is the statement that the
    analyst call never actually happened. Discarding from ``_seen`` alone left the pair permanently
    undisplaceable on the strength of a call that did not run — memory and journal disagreeing about
    the same fact. Safe because only a never-completed call ever re-arms."""
    day = _the_day()
    ps = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(ps.admit([_scored("SHRIRAMFIN", 0.20)], day)) == 1
    assert ps.claim_slot("SHRIRAMFIN", "orb") is True        # dispatched...
    assert ps.rearm("SHRIRAMFIN", "orb") is True             # ...and the call never ran
    assert [c.symbol for c in ps.admit([_scored("KALYANKJIL", 1.0)], day)] == ["KALYANKJIL"]
    assert ps.take_displaced() == [("SHRIRAMFIN", "orb")]

    # A REAL evaluation never re-arms, so a claim on its own is still permanent (invariant #1).
    kept = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(kept.admit([_scored("SHRIRAMFIN", 0.20)], day)) == 1
    assert kept.claim_slot("SHRIRAMFIN", "orb") is True
    assert kept.admit([_scored("KALYANKJIL", 1.0)], day) == []


def test_the_displacement_walk_is_skipped_when_displacement_is_off():
    """With ``displacement_margin=None`` no victim can exist, so walking ``_admitted`` bought only a
    log field — and a fabricated ``displaceable=0`` reads exactly like "every incumbent was already
    evaluated", the opposite diagnosis. ``None`` logs as null: disabled, not zero."""
    ps = _prescreen([], max_per_strategy_day={"orb": 1}, displacement_margin=None)
    day = _the_day()
    assert len(ps.admit([_scored("POLICYBZR", 0.10)], day)) == 1
    assert ps._displacement_scan_locked(_scored("KALYANKJIL", 1.0)) == (None, None)
    # And the enabled case still counts, which is what makes the two readings distinguishable.
    on = _prescreen([], max_per_strategy_day={"orb": 1})
    assert len(on.admit([_scored("POLICYBZR", 0.10)], day)) == 1
    assert on._displacement_scan_locked(_scored("KALYANKJIL", 1.0)).displaceable == 1


def test_hydrate_restores_the_catalyst_budget_across_a_restart():
    """Before this, ``hydrate`` deliberately left the §2.7 counter at zero, justified by
    ``max_per_strategy_day['cat']`` being equal to the guard. Adding ``cat_reversal: 2`` broke that
    coincidence — the post-restart bound became 4 against a guard of 2. The journal has no ref
    column, so the set is reconstructed from the SYMBOLS of the charged catalyst-strategy pairs."""
    day = _the_day()
    ps = _prescreen([], max_candidates_per_day=20, max_per_strategy_day={"default": 5},
                    catalyst_cap_fn=lambda: 2)
    ps.hydrate(day, seen=[("HINDZINC", "cat")],
               charged=[("HINDZINC", "cat"), ("TCS", "orb")],
               catalyst_strategies=("cat", "cat_reversal"))
    # The pre-restart STORY is reconstructed by symbol, so its other leg costs no second entry...
    assert len(ps.admit([_scored("HINDZINC", 0.5, "cat_reversal", catalyst_ref="r1")], day)) == 1
    # ...one of the two entries is left for a genuinely new story...
    assert [c.symbol for c in ps.admit([_scored("TITAN", 0.5, "cat", catalyst_ref="r2")], day)] \
        == ["TITAN"]
    # ...and the third is refused, exactly as it would have been without the restart.
    assert ps.admit([_scored("LT", 0.5, "cat", catalyst_ref="r3")], day) == []


# ------------------------------------------ cap release schedule (owner-directed 2026-09-02)
# Three days running (08-27 KALYANKJIL 1.0@12:49; 09-01 PERSISTENT et al.; 09-02 IDEA/VMM/OIL 1.0),
# the orb sub-cap was fully spent within the window-open burst and later, better fires were locked
# out. The schedule releases the cap in cumulative tranches by IST bar time, so afternoon capacity
# is guaranteed; scores (de-saturated the same day) + displacement then allocate WITHIN a tranche.
def _sched_prescreen(**kw):
    defaults = dict(
        scanners=[],
        context_provider=lambda bar: None,
        max_candidates_per_day=48,
        max_per_strategy_day={"orb": 7},
        cap_release_schedule={"orb": {"10:00": 3, "11:30": 5, "13:00": 7}},
    )
    defaults.update(kw)
    return SignalPreScreen(**defaults)


def _orb_cand(symbol: str, score: float = 0.6):
    from engine.strategy.types import RawLevels, SignalCandidate
    return SignalCandidate(
        signal_id=f"sig-orb-{symbol}", strategy_id="orb", symbol=symbol, side="BUY",
        style="intraday",
        raw_levels=RawLevels(entry=Decimal("103.00"), stop=Decimal("100.00")), score=score,
    )


def _at(hh: int, mm: int):
    from datetime import datetime
    from engine.core.clock import IST
    return datetime(2026, 9, 3, hh, mm, tzinfo=IST)


def test_cap_schedule_releases_cumulative_tranches() -> None:
    """Before 11:30 only the first tranche (3) admits; a bar at 11:30 opens tranche 2 (5); 13:00
    opens the full cap (7). The suppression at a full tranche logs the EFFECTIVE cap."""
    from datetime import date as _date

    ps = _sched_prescreen()
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        for i in range(3):
            assert ps._admit_one_locked(_orb_cand(f"AAA{i}"), at=_at(10, 5)) is True
        assert ps._admit_one_locked(_orb_cand("BBB"), at=_at(10, 6)) is False   # tranche 1 full
        assert ps._admit_one_locked(_orb_cand("CCC"), at=_at(11, 30)) is True   # tranche 2 opens
        assert ps._admit_one_locked(_orb_cand("DDD"), at=_at(12, 0)) is True
        assert ps._admit_one_locked(_orb_cand("EEE"), at=_at(12, 1)) is False   # tranche 2 full
        assert ps._admit_one_locked(_orb_cand("FFF"), at=_at(13, 0)) is True    # full cap
        assert ps._admit_one_locked(_orb_cand("GGG"), at=_at(14, 0)) is True
        assert ps._admit_one_locked(_orb_cand("HHH"), at=_at(14, 1)) is False   # 7 = flat cap


def test_cap_schedule_unscheduled_paths_use_flat_cap() -> None:
    """No `at` (the batch admit path) and strategies without a schedule line use the flat cap
    exactly as before — the schedule is purely additive."""
    from datetime import date as _date

    ps = _sched_prescreen(max_per_strategy_day={"orb": 7, "brk20": 2})
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        for i in range(7):                                     # at=None -> flat cap 7
            assert ps._admit_one_locked(_orb_cand(f"AAA{i}")) is True
        assert ps._admit_one_locked(_orb_cand("BBB")) is False
        for i in range(2):                                     # brk20 has no schedule line
            assert ps._admit_one_locked(_ext_cand(f"KKK{i}"), at=_at(10, 5)) is True
        assert ps._admit_one_locked(_ext_cand("KKK9"), at=_at(10, 5)) is False


def test_cap_schedule_is_capped_by_the_flat_cap_and_validated() -> None:
    """The schedule can never RAISE the flat cap (effective = min(flat, scheduled)); a
    non-monotone or malformed schedule is a constructor error, not a silent behavior."""
    import pytest as _pytest
    from datetime import date as _date

    ps = _sched_prescreen(
        max_per_strategy_day={"orb": 4},
        cap_release_schedule={"orb": {"10:00": 3, "11:30": 9}},   # 9 > flat 4 -> min() binds at 4
    )
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        for i in range(3):
            assert ps._admit_one_locked(_orb_cand(f"AAA{i}"), at=_at(10, 5)) is True
        assert ps._admit_one_locked(_orb_cand("BBB"), at=_at(10, 6)) is False
        assert ps._admit_one_locked(_orb_cand("CCC"), at=_at(11, 31)) is True
        assert ps._admit_one_locked(_orb_cand("DDD"), at=_at(11, 32)) is False  # min(4, 9) = 4

    with _pytest.raises(ValueError):
        _sched_prescreen(cap_release_schedule={"orb": {"10:00": 5, "11:30": 3}})  # decreasing
    with _pytest.raises(ValueError):
        _sched_prescreen(cap_release_schedule={"orb": {"nonsense": 3}})
    with _pytest.raises(ValueError):
        _sched_prescreen(cap_release_schedule={"orb": {"10:00": 0}})


def test_cap_schedule_before_first_release_uses_first_tranche() -> None:
    """A bar EARLIER than the first schedule entry admits under the first tranche's value — the
    schedule keys mark release times, and pre-open/early candidates get the opening allocation."""
    from datetime import date as _date

    ps = _sched_prescreen()
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        for i in range(3):
            assert ps._admit_one_locked(_orb_cand(f"AAA{i}"), at=_at(9, 50)) is True
        assert ps._admit_one_locked(_orb_cand("BBB"), at=_at(9, 51)) is False


def test_cap_schedule_tranche_boundary_uses_entry_time_convention() -> None:
    """2026-09-02 review: the tranche instant is ENTRY time (ts_minute + 1m, the file-wide boundary
    convention) - an 11:29-close bar enters AT 11:30 and gets the 11:30 tranche."""
    from datetime import date as _date

    ps = _sched_prescreen()
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        for i in range(3):
            assert ps._admit_one_locked(_orb_cand(f"AAA{i}"), at=_at(10, 5)) is True
        assert ps._admit_one_locked(_orb_cand("BBB"), at=_at(11, 28)) is False  # entry 11:29 - tranche 1
        assert ps._admit_one_locked(_orb_cand("CCC"), at=_at(11, 29)) is True   # entry 11:30 - tranche 2


def test_cap_schedule_bounds_the_displacement_budget_per_tranche() -> None:
    """2026-09-02 review (blocking): the displacement budget derives from the RELEASED tranche, not
    the flat cap - a busy early tranche can no longer burn the whole day's displacement allowance
    and void the afternoon's contested-seat guarantee."""
    from datetime import date as _date

    ps = _sched_prescreen(
        max_per_strategy_day={"orb": 4},
        cap_release_schedule={"orb": {"10:00": 2, "13:00": 4}},
    )
    day = _date(2026, 9, 3)
    with ps._lock:
        ps._roll_day_locked(day)
        assert ps._admit_one_locked(_orb_cand("AAA", 0.40), at=_at(10, 5)) is True
        assert ps._admit_one_locked(_orb_cand("BBB", 0.45), at=_at(10, 6)) is True   # tranche 1 full
        # Churn within tranche 1: budget = released tranche (2), NOT the flat cap (4).
        assert ps._admit_one_locked(_orb_cand("CC1", 0.56), at=_at(10, 10)) is True  # displaces 0.40
        assert ps._admit_one_locked(_orb_cand("CC2", 0.67), at=_at(10, 11)) is True  # displaces 0.45
        assert ps._displacements.get("orb") == 2
        assert ps._admit_one_locked(_orb_cand("CC3", 0.90), at=_at(10, 12)) is False  # budget spent
        # 13:00 tranche opens: direct capacity (count 2 < 4) admits without displacement, and the
        # budget headroom returns (budget 4 > used 2) for genuine afternoon churn.
        assert ps._admit_one_locked(_orb_cand("DDD", 0.30), at=_at(13, 1)) is True
        assert ps._admit_one_locked(_orb_cand("EEE", 0.31), at=_at(13, 2)) is True
        assert ps._admit_one_locked(_orb_cand("FFF", 0.55), at=_at(13, 3)) is True   # displaces 0.30
        assert ps._displacements.get("orb") == 3


def test_cap_schedule_duplicate_release_times_are_a_loud_error() -> None:
    """2026-09-02 review: "09:00" and "9:00" parse to one instant - a silent last-writer-wins on an
    owner-typed YAML block must be a constructor error instead."""
    import pytest as _pytest

    with _pytest.raises(ValueError):
        _sched_prescreen(cap_release_schedule={"orb": {"09:00": 3, "9:00": 5}})
