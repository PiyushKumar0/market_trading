"""§6.1 `ins` EOD job + morning admission — the crossing seam, the pending journal, the once-only bound.

The load-bearing test in this file is :func:`test_golden_live_job_and_validated_study_agree`: the live
job and the WO-16 study must produce the SAME crossings from the same fixture, because the whole
justification for shipping `ins` is evidence measured by that study. A drift between the two would
mean trading a population the evidence never measured. It is pinned two ways — by FUNCTION IDENTITY
(one definition, not two copies that happen to agree today) and by BEHAVIOUR on a shared fixture.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.datafeeds import filings_events, insider_crossings
from engine.datafeeds.ins_crossings import InsCrossingsJob
from engine.marketdata.store import DailyBar

# Load the loose study script by path (the established pattern — test_event_study_filings.py).
_ES_PATH = Path(__file__).resolve().parents[2] / "scripts" / "event_study.py"
if "mt_event_study" in sys.modules:
    es = sys.modules["mt_event_study"]
else:
    _spec = importlib.util.spec_from_file_location("mt_event_study", _ES_PATH)
    es = importlib.util.module_from_spec(_spec)
    sys.modules["mt_event_study"] = es
    _spec.loader.exec_module(es)

THRESHOLD = 10_000_000                     # ₹1cr — the shipped ins.threshold_inr
#: Mon 2026-08-10 .. Fri 2026-08-21 are trading days in config/calendar/2026.yaml; the fixture uses a
#: contiguous weekday run so "the next trading day" is unambiguous.
RUN_DAY = date(2026, 8, 17)                # a Monday
NEXT_SESSION = date(2026, 8, 18)


def _sessions(n: int, end: date = RUN_DAY) -> list[date]:
    """``n`` consecutive fixture sessions ending ON ``end`` (weekday-agnostic: the crossing function
    is calendar-blind, it only needs an ascending session list)."""
    return [end - timedelta(days=n - 1 - i) for i in range(n)]


SESSIONS = _sessions(30)


def _buy(
    session: date, value: int, *, symbol: str = "AAA", hh: int = 10, mode: str = "Market Purchase",
    txn: str = "Buy", ident: str | None = None,
) -> dict[str, Any]:
    return {
        "id": ident or f"bse:{symbol}-{session.isoformat()}-{value}",
        "symbol": symbol,
        "person_name": "A Promoter",
        "person_category": "Promoter",
        "acq_mode": mode,
        "txn_type": txn,
        "qty": 1000,
        "value": Decimal(value),
        "txn_from": session,
        "broadcast_dt": datetime(session.year, session.month, session.day, hh, 0, tzinfo=IST),
    }


# --------------------------------------------------------------------------- doubles
class _FakeStore:
    """The three MarketStore reads :class:`InsCrossingsJob` makes, and nothing else."""

    def __init__(
        self,
        *,
        universe: list[str],
        filings: list[dict[str, Any]],
        closes: dict[str, dict[date, str]],
    ) -> None:
        self.universe = universe
        self.filings = filings
        self.closes = closes
        self.bars_calls: list[str] = []

    def get_universe_daily(self, d: date) -> list[dict[str, Any]]:
        return [
            {"d": d, "symbol": s, "included": True, "exclusion_reasons": []} for s in self.universe
        ]

    def get_insider_trades(self, *, symbol=None, broadcast_from=None, broadcast_to=None):
        out = []
        for row in self.filings:
            bdt = row["broadcast_dt"]
            if symbol is not None and row["symbol"] != symbol:
                continue
            if broadcast_from is not None and bdt < broadcast_from:
                continue
            if broadcast_to is not None and bdt > broadcast_to:
                continue
            out.append(row)
        return sorted(out, key=lambda r: (r["broadcast_dt"], r["id"]))

    def get_bars_1d(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        self.bars_calls.append(symbol)
        per_day = self.closes.get(symbol, {})
        return [
            DailyBar(
                symbol=symbol, d=d, open=Decimal(c), high=Decimal(c), low=Decimal(c),
                close=Decimal(c), volume=1000, src="test",
            )
            for d, c in sorted(per_day.items())
            if start <= d <= end
        ]


class _FrozenClock:
    def now(self) -> datetime:
        return datetime(RUN_DAY.year, RUN_DAY.month, RUN_DAY.day, 19, 15, tzinfo=IST)

    def today(self) -> date:
        return RUN_DAY


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


def _store_with_crossing_today() -> _FakeStore:
    """Two ₹6L... no: two ₹60L open-market buys inside the trailing window, the second landing ON the
    run day, so the trailing sum crosses ₹1cr exactly at ``RUN_DAY``."""
    filings = [
        _buy(SESSIONS[-3], 6_000_000),
        _buy(SESSIONS[-1], 6_000_000),
    ]
    closes = {"AAA": {d: "100.00" for d in SESSIONS}}
    return _FakeStore(universe=["AAA", "BBB"], filings=filings, closes=closes)


def _run(job: InsCrossingsJob, d: date = RUN_DAY):
    return asyncio.run(job.run(d))


def _pending_rows(conn) -> list[dict[str, Any]]:
    return [
        dict(r) for r in conn.execute("SELECT * FROM ins_pending ORDER BY symbol").fetchall()
    ]


# =========================================================================== THE GOLDEN SEAM TEST
def test_golden_shared_crossing_function_is_one_definition_not_a_copy():
    """Identity, not equality: the study, the engine event builder and the live job must all resolve
    to the SAME function object. Two copies that agree today are exactly the drift the §6.1 addendum
    forbids ("the live code MUST consume the same crossing function the validated study runs")."""
    assert es.insider_cluster_events is insider_crossings.insider_cluster_events
    assert filings_events.insider_cluster_events is insider_crossings.insider_cluster_events
    assert es.is_open_market_buy is insider_crossings.is_open_market_buy
    assert es.entry_session_index is insider_crossings.entry_session_index
    assert es.INSIDER_TRAILING_SESSIONS == insider_crossings.INSIDER_TRAILING_SESSIONS == 10
    # ...and no engine module path-loads the loose script any more (the pre-2026-08-17 shim).
    assert not hasattr(filings_events, "_es")


def test_golden_live_job_and_validated_study_agree(conn, calendar):
    """BEHAVIOURAL half of the golden test, on one shared fixture: every crossing session the WO-16
    study reports is a crossing session the live job reports, for the same filings and sessions."""
    # A fixture with three crossings separated by genuine RE-ARMS (each cluster must age out of the
    # 10-session trailing window before the next can fire), plus an ESOP row the §2.8.2 taxonomy must
    # exclude however large. The spacing is the point: it exercises the hysteresis, not just the sum.
    filings = [
        _buy(SESSIONS[2], 6_000_000),
        _buy(SESSIONS[3], 6_000_000),        # crossing #1 at [3] (trailing 12M)
        _buy(SESSIONS[3], 90_000_000, mode="ESOP", ident="bse:esop"),   # excluded, however large
        _buy(SESSIONS[15], 11_000_000),      # [3] has aged out by [13] -> re-armed -> crossing #2
        _buy(SESSIONS[29], 12_000_000),      # [15] aged out by [25] -> re-armed -> crossing #3 TODAY
    ]

    # --- the STUDY's answer (its own entry point, over its own session list) ---
    study_indices = es.insider_buy_events(SESSIONS, filings, THRESHOLD)
    study_sessions = [SESSIONS[i] for i in study_indices]
    assert len(study_sessions) >= 3, study_sessions        # the fixture really does re-arm
    assert study_sessions[-1] == RUN_DAY

    # --- the LIVE JOB's answer, over the same fixture ---
    store = _FakeStore(
        universe=["AAA"], filings=filings, closes={"AAA": {d: "100.00" for d in SESSIONS}}
    )
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.ok

    # The job journals only TODAY's crossing (older ones were journalled on their own run days), so
    # the agreement to assert is: the job's crossing is exactly the study's LAST crossing session.
    rows = _pending_rows(conn)
    assert [r["crossing_session"] for r in rows] == [study_sessions[-1].isoformat()]

    # ...and the full crossing SET agrees when the same window is put to the engine-side builder —
    # the function both sides call.
    engine_sessions = [
        e["event_session"]
        for e in filings_events.insider_net_buy(filings, SESSIONS, min_value_inr=THRESHOLD)
    ]
    assert engine_sessions == study_sessions


# =========================================================================== the EOD job
def test_job_persists_pending_rows_for_the_next_session(conn, calendar):
    store = _store_with_crossing_today()
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))

    assert result.ok is True
    assert result.crossings_found == 1
    assert result.rows_written == 1
    assert result.for_session == NEXT_SESSION

    rows = _pending_rows(conn)
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAA"
    assert row["for_session"] == NEXT_SESSION.isoformat()      # the session whose OPEN is the fill
    assert row["crossing_session"] == RUN_DAY.isoformat()
    assert Decimal(row["trailing_value"]) == Decimal("12000000")
    assert row["contributing_filings_n"] == 2
    assert Decimal(row["reference_close"]) == Decimal("100.00")  # the pre-open entry reference
    assert row["consumed"] == 0


def test_job_only_journals_crossings_dated_the_run_day(conn, calendar):
    """A crossing that happened earlier in the lookback window was journalled on ITS run day.
    Re-emitting it every night would re-originate stale events forever."""
    filings = [_buy(SESSIONS[3], 11_000_000)]          # crosses at SESSIONS[3], not the run day
    store = _FakeStore(
        universe=["AAA"], filings=filings, closes={"AAA": {d: "100.00" for d in SESSIONS}}
    )
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.ok is True
    assert result.crossings_found == 0
    assert _pending_rows(conn) == []


def test_job_is_idempotent_per_day(conn, calendar):
    """A §2.6 catch-up replay / missed-job sweep / manual --once must UPSERT, never queue a duplicate
    candidate: the (for_session, symbol) primary key is the structural bound."""
    store = _store_with_crossing_today()
    job = InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD)
    first = _run(job)
    second = _run(job)
    third = _run(job)

    assert first.rows_written == second.rows_written == third.rows_written == 1
    assert len(_pending_rows(conn)) == 1                      # still ONE row after three runs


def test_a_rerun_never_un_consumes_an_already_admitted_row(conn, calendar):
    """The idempotent upsert must leave ``consumed`` alone — otherwise an evening catch-up pass for
    today would resurrect a candidate the morning sweep already admitted, and it would be admitted a
    second time."""
    store = _store_with_crossing_today()
    job = InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD)
    _run(job)
    with conn:
        conn.execute("UPDATE ins_pending SET consumed = 1, consumed_at = '2026-08-18T09:30:00+05:30'")

    _run(job)
    rows = _pending_rows(conn)
    assert len(rows) == 1
    assert rows[0]["consumed"] == 1                           # NOT reset by the re-run
    assert rows[0]["consumed_at"] == "2026-08-18T09:30:00+05:30"


def test_esop_rows_never_produce_a_crossing(conn, calendar):
    """§2.8.2 taxonomy, end-to-end through the live job: an ESOP allotment is not an open-market buy
    however large, so it can never originate an `ins` candidate."""
    filings = [
        _buy(SESSIONS[-1], 900_000_000, mode="ESOP Allotment"),
        _buy(SESSIONS[-1], 500_000_000, mode="Inter-se Transfer", ident="bse:interse"),
    ]
    store = _FakeStore(
        universe=["AAA"], filings=filings, closes={"AAA": {d: "100.00" for d in SESSIONS}}
    )
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.crossings_found == 0
    assert _pending_rows(conn) == []


def test_out_of_universe_symbols_are_ignored(conn, calendar):
    """BSE serves the whole market; an out-of-universe scrip is EXPECTED, never a candidate."""
    filings = [_buy(SESSIONS[-2], 6_000_000, symbol="ZZZ"), _buy(SESSIONS[-1], 6_000_000, symbol="ZZZ")]
    store = _FakeStore(
        universe=["AAA"], filings=filings, closes={"ZZZ": {d: "100.00" for d in SESSIONS}}
    )
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.crossings_found == 0
    assert store.bars_calls == []                       # never even priced — narrowed before the read


def test_missing_run_day_bar_is_counted_never_invented(conn, calendar):
    """No completed bar for the run day (a bhavcopy gap, or a symbol that did not trade) ⇒ the entry
    reference does not exist and must NOT be back-filled from an older close."""
    closes = {"AAA": {d: "100.00" for d in SESSIONS[:-1]}}      # everything EXCEPT the run day
    store = _FakeStore(
        universe=["AAA"],
        filings=[_buy(SESSIONS[-3], 6_000_000), _buy(SESSIONS[-1], 6_000_000)],
        closes=closes,
    )
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.ok is True
    assert result.symbols_missing_bar == 1
    assert result.crossings_found == 0
    assert _pending_rows(conn) == []


def test_no_universe_for_the_day_sinks_the_watermark(conn, calendar):
    """``ok=False`` = the day could not be EVALUATED, so the §2.6 sweep retries it. Distinct from an
    evaluated day with zero crossings, which is a real answer."""
    store = _FakeStore(universe=[], filings=[], closes={})
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.ok is False
    assert result.reason is not None


def test_zero_crossings_on_an_evaluated_day_is_ok_true(conn, calendar):
    store = _FakeStore(universe=["AAA"], filings=[], closes={"AAA": {d: "100.00" for d in SESSIONS}})
    result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert result.ok is True
    assert result.crossings_found == 0


# =========================================================================== starvation visibility
def test_run_logs_fresh_feed_counts_next_to_crossings_found(conn, calendar, caplog):
    """The plan's binding starvation-visibility requirement: one line must distinguish "the BSE fresh
    feed delivered nothing" (STARVATION — the live-reachability caveat biting) from "a genuinely quiet
    day". A bare crossings=0 cannot."""
    store = _store_with_crossing_today()
    with caplog.at_level("INFO"):
        result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))

    assert "ins_crossings_run" in caplog.text
    # Both BSE-prefixed fixture rows are broadcast in the window; one lands ON the run day.
    assert result.fresh_rows_today == 1
    assert result.fresh_rows_in_universe == 1
    assert result.crossings_found == 1


def test_empty_fresh_feed_raises_the_starvation_warning(conn, calendar, caplog):
    """Zero fresh rows on a run day is the shape of feed starvation and must warn on its own,
    independent of whether any crossing was found."""
    store = _FakeStore(universe=["AAA"], filings=[], closes={"AAA": {d: "100.00" for d in SESSIONS}})
    with caplog.at_level("INFO"):
        result = _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))

    assert result.fresh_rows_today == 0
    assert "ins_crossings_fresh_feed_empty" in caplog.text


# =========================================================================== morning admission
def test_admission_reads_builds_and_consumes_exactly_once(conn, calendar):
    """The restart-safe once-only bound: ``_read_ins_pending`` sees a row once, and after
    ``_consume_ins_pending`` a second sweep (or a sweep after a mid-session restart — the flag is
    PERSISTED, not process memory) sees nothing."""
    from engine.ops.main import _consume_ins_pending, _read_ins_pending

    store = _store_with_crossing_today()
    _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))

    first = _read_ins_pending(conn, NEXT_SESSION)
    assert [c.symbol for c in first] == ["AAA"]
    assert first[0].reference_close == Decimal("100.00")
    assert first[0].trailing_value == Decimal("12000000")
    assert first[0].crossing_session == RUN_DAY

    now = datetime(2026, 8, 18, 9, 30, tzinfo=IST)
    _consume_ins_pending(conn, NEXT_SESSION, [c.symbol for c in first], now=now)

    assert _read_ins_pending(conn, NEXT_SESSION) == []          # second sweep: nothing left
    row = _pending_rows(conn)[0]
    assert row["consumed"] == 1
    assert row["consumed_at"] == now.isoformat()                # audit trail kept, never deleted


def test_admission_ignores_rows_keyed_to_another_session(conn, calendar):
    """``for_session`` is the key: yesterday's unconsumed leftovers must never leak into today."""
    from engine.ops.main import _read_ins_pending

    store = _store_with_crossing_today()
    _run(InsCrossingsJob(store, conn, _FrozenClock(), calendar, threshold_inr=THRESHOLD))
    assert _read_ins_pending(conn, date(2026, 8, 19)) == []
    assert len(_read_ins_pending(conn, NEXT_SESSION)) == 1


def test_admission_survives_a_malformed_row(conn, calendar):
    """A corrupt row costs itself and nothing else — the sweep must not die on one bad journal row."""
    from engine.ops.main import _read_ins_pending

    with conn:
        conn.executemany(
            "INSERT INTO ins_pending (for_session, symbol, crossing_session, trailing_value, "
            "contributing_filings_n, reference_close, consumed, created_at) VALUES (?,?,?,?,?,?,0,?)",
            [
                (NEXT_SESSION.isoformat(), "GOOD", RUN_DAY.isoformat(), "12000000", 2, "100.00", "x"),
                (NEXT_SESSION.isoformat(), "BAD", "not-a-date", "12000000", 2, "100.00", "x"),
            ],
        )
    rows = _read_ins_pending(conn, NEXT_SESSION)
    assert [c.symbol for c in rows] == ["GOOD"]


def test_prescreen_per_strategy_cap_binds_ins(conn, calendar):
    """`ins` goes through ``SignalPreScreen.admit`` like brk20, so the §3.2.5 per-strategy cap binds —
    the batch path is not a cap bypass. Shipped cap: 4/day."""
    from engine.strategy.prescreen import SignalPreScreen
    from engine.strategy.scanners import ins

    crossings = [
        ins.Crossing(
            symbol=f"S{i:02d}", crossing_session=RUN_DAY, trailing_value=Decimal(20_000_000 + i),
            contributing_filings_n=1, reference_close=Decimal("100.00"),
        )
        for i in range(9)
    ]
    prescreen = SignalPreScreen(
        scanners=(), context_provider=lambda bar: None,
        max_candidates_per_day=20, max_per_strategy_day={"default": 8, "ins": 4},
    )
    admitted = prescreen.admit(ins.sweep_crossings(crossings), NEXT_SESSION)
    assert len(admitted) == 4
    assert {c.strategy_id for c in admitted} == {"ins"}

    # ...and a second batch the same day adds nothing (cap already spent).
    assert prescreen.admit(ins.sweep_crossings(crossings), NEXT_SESSION) == []


def test_shipped_settings_cap_ins(conn):
    """``config/settings.yaml`` ships an explicit ``ins`` sub-cap, below the ``default``.

    4→8 on 2026-08-18 when the day cap was rescaled 20→48 (the origination cap had fallen 2.4x below
    the analyst forward cap it exists to protect). The behavioural test above deliberately pins its
    OWN caps rather than reading the shipped file, so a future rescale changes this assertion only.
    What must stay true is the SHAPE: `ins` is a rare EOD-batch event and stays capped under
    `default`, so a cluster day cannot let it crowd out the per-bar strategies.
    """
    from engine.core.config import load_settings

    caps = load_settings().strategy.prescreen.max_per_strategy_day
    assert isinstance(caps, dict)
    assert caps["ins"] == 8
    assert caps["ins"] < caps["default"]


# =========================================================================== the exit path (§7.1)
def test_max_holding_exit_needs_no_ins_specific_code():
    """§6.1: "the EXISTING max-holding machinery IS the exit path" — `ins` ships NO exit code.

    Two things make that true and both are asserted here rather than assumed:

    1. ``RecommendationPipeline.check_aged_positions`` selects its cap by ``position['style']`` alone
       and contains no ``strategy_id`` branch at all, so an `ins` position (style ``swing``) flows
       through the identical path as any other swing.
    2. The swing cap it reads is 20 trading days — exactly ``ins.hold_sessions`` and exactly the
       validated T+20 horizon. If either number moved, the strategy's exit would silently stop
       matching the horizon its evidence was measured over.
    """
    import inspect

    from engine.ops.pipeline import RecommendationPipeline

    source = inspect.getsource(RecommendationPipeline.check_aged_positions)
    assert "strategy_id" not in source, "the max_holding exit must stay strategy-agnostic"
    assert 'position["style"]' in source

    import yaml

    from engine.core.config import load_settings
    from engine.risk.limits import LimitTable

    limits_path = Path(__file__).resolve().parents[2] / "config" / "limits.yaml"
    table = LimitTable.model_validate(yaml.safe_load(limits_path.read_text(encoding="utf-8")))
    assert int(table.limits.max_holding.swing_trading_days) == 20
    assert load_settings().ins.hold_sessions == 20
