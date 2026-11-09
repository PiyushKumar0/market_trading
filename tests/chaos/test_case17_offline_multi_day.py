"""Chaos case 17 — Offline MULTI-DAY (e.g. 4 days incl. a weekend).

Failure injected: the engine is offline for 4 days (a clean stop Thu 15:40, before the EOD window →
restart Mon 09:30). Must hold: startup backfills the candle gap, re-runs each missed trading day's EOD
jobs, re-arms schedules, enforces cold-start ``warmup_ready``, and handles an ex-date in the gap. Gaps:
``weekend`` (Thu 2026-06-11 → Mon 06-15; missed trading days Thu, Fri) and ``holiday_weekend`` (Thu
06-25 → Mon 06-29; Fri 06-26 is an NSE holiday, so only Thu is missed).

Composition (``tests/chaos/_lifecycle_rig.py``): real lifecycle + CatchUpRunner over the real registry
+ Scheduler arming; for the backfill/warm-up clauses also a real ``MarketStore``, ``BackfillJob`` driven
through ``regime_and_warmup_backfill`` (the call main.py's ``backfill_hook`` makes, incl. its
``token_valid()`` guard) and a real ``WarmupGate``. The Kite historical-candle API is the faked
boundary (:class:`FakeKiteCandles`).

Date-keyed jobs run once per missed TRADING day, ascending, never for a weekend/holiday; run-latest jobs
run once for the whole gap. ``tick_compact`` is vetoed in-session at a 09:30 boot and keeps its watermark
debt for the post-session sweep (asserted so the deferral stays visible). A token-less restart cannot
cover the gap: REGIME coverage is short ⇒ FROZEN + ``WARMUP_FROZEN`` until the post-login re-trigger
backfills and lifts it.

Paper GTT clauses: paper never repairs a GTT across an ex-date; an ex-date in the unobserved window VOIDs
the position in session prep, deleting its restored GTT before any tick is forwarded. Without one, the
restored GTT still rests as the position's protection and fires on the first tick through the stop.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import PaperSettings, config_dir
from engine.core.enums import RiskState
from engine.marketdata.backfill import BackfillJob
from engine.notify.catalog import MessageKind
from engine.ops.jobs import (
    JOB_BACKUP,
    JOB_BHAVCOPY,
    JOB_DEALS,
    JOB_NIGHTLY_REVIEW,
    JOB_RECONCILE,
    JOB_SECTOR_MAP,
    JOB_TICK_COMPACT,
    JOB_WEEKLY_SUMMARY,
    JobClass,
)
from engine.ops.main import INDEX_SYMBOL, LANE_JOB_IDS, VIX_SYMBOL, _in_session_window
from engine.ops.paper_runtime import PaperRuntime
from engine.ops.post_login import regime_and_warmup_backfill
from engine.ops.warmup import WarmupGate
from engine.paper.fill_model import FillModelConfig
from tests.chaos._lifecycle_rig import EngineProcess, RigEnv, run_session
from tests.unit.test_protection_manager import Now, Rig, orders, tick

A_BOOT, A_STOP, B_BOOT = time(9, 30), time(15, 40), time(9, 30)
PLAN_NAMED_DATE_KEYED = {JOB_RECONCILE, JOB_BHAVCOPY, JOB_DEALS, JOB_NIGHTLY_REVIEW}

GAPS = {
    # name: (prior evening, stop day, restart day, missed trading days, non-trading days in the gap)
    "weekend": (date(2026, 6, 10), date(2026, 6, 11), date(2026, 6, 15),
                [date(2026, 6, 11), date(2026, 6, 12)], [date(2026, 6, 13), date(2026, 6, 14)]),
    "holiday_weekend": (date(2026, 6, 24), date(2026, 6, 25), date(2026, 6, 29),
                        [date(2026, 6, 25)], [date(2026, 6, 26), date(2026, 6, 27), date(2026, 6, 28)]),
}

WATCH = ["RELIANCE"]
TOKENS = {INDEX_SYMBOL: 256265, VIX_SYMBOL: 264969, "RELIANCE": 738561}


def _t(d: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _at(d: date, tm: time) -> datetime:
    return datetime.combine(d, tm, tzinfo=IST)


async def _gap_restart(tmp_path, monkeypatch, gap: str, **proc_kw):
    """Prior evening (all jobs watermarked) → stop-day session 09:30-15:40 → restart-day 09:30 boot."""
    prior, stop_day, restart_day, _missed, _off = GAPS[gap]
    env = RigEnv(tmp_path, monkeypatch, start=_t(prior, 22, 45))
    await run_session(env, _t(prior, 22, 45), _t(prior, 22, 50))
    await run_session(env, _at(stop_day, A_BOOT), _at(stop_day, A_STOP), **proc_kw)
    env.at(_at(restart_day, B_BOOT))
    calls_b, sent_b = len(env.job_calls), len(env.sent)
    b = EngineProcess(env, **proc_kw)
    # Precondition on the configured schedule: nothing fires inside the stop day's uptime.
    assert not [s.job_id for s in b.registry.specs() if A_BOOT < s.at <= A_STOP]
    b.catch_up.record_run(JOB_WEEKLY_SUMMARY, prior)   # fires on Fridays only: the prior evening never ran it
    await b.boot()
    return env, b, calls_b, sent_b


# --------------------------------------------------------------------------- missed-day EOD replay + re-arm
@pytest.mark.parametrize("gap", sorted(GAPS))
async def test_case17_missed_days_eod_jobs_rerun_once_per_trading_day_and_schedules_rearmed(
    tmp_path, monkeypatch, gap
):
    _prior, stop_day, restart_day, missed, off_days = GAPS[gap]
    env, b, calls_b, _sent_b = await _gap_restart(tmp_path, monkeypatch, gap)
    calls = env.calls(since=calls_b)
    reg = b.registry
    assert _in_session_window(env.clock, b.calendar)           # tick_compact no-run window: no compaction
    date_keyed = {s.job_id for s in reg.specs(JobClass.DATE_KEYED)} - {JOB_TICK_COMPACT, JOB_WEEKLY_SUMMARY}
    assert PLAN_NAMED_DATE_KEYED <= date_keyed
    assert all(s.at > A_STOP for s in reg.specs(JobClass.DATE_KEYED)), "stop day must be a missed EOD"
    run_latest = {s.job_id for s in reg.specs(JobClass.RUN_LATEST)} - LANE_JOB_IDS
    morning_safety = {s.job_id for s in reg.specs(JobClass.SAFETY_CRITICAL) if s.at <= B_BOOT}
    eod_safety = {s.job_id for s in reg.specs(JobClass.SAFETY_CRITICAL) if s.at > A_STOP}

    for job_id in sorted(date_keyed):
        assert [d for j, d in calls if j == job_id] == missed, f"{job_id}: {[c for c in calls if c[0] == job_id]}"
    assert not {d for _j, d in calls if d is not None} & set(off_days), "a weekend/holiday was replayed"
    # Run-latest: ONE run for the whole gap.
    assert JOB_BACKUP in run_latest
    for job_id in sorted(run_latest):
        assert [j for j, _ in calls if j == job_id] == [job_id], f"{job_id} not exactly once for the gap"
    for job_id in sorted(morning_safety):                      # today's deadline jobs: run for today
        assert [j for j, _ in calls if j == job_id] == [job_id]
        assert b.catch_up.was_run(job_id, restart_day)
    # EOD deadline jobs (earnings): today's entries read the LAST missed day's run; one replay, not one per day.
    for job_id in sorted(eod_safety):
        assert [j for j, _ in calls if j == job_id] == [job_id]
        assert b.catch_up.was_run(job_id, missed[-1])
    assert {j for j, _ in calls} == date_keyed | run_latest | morning_safety | eod_safety | {JOB_WEEKLY_SUMMARY}
    # The weekly summary fires only on a week's last session.
    assert [d for j, d in calls if j == JOB_WEEKLY_SUMMARY] == [missed[-1]]
    # Sunday's weekly sector map fell inside the gap: one run, recorded under that Sunday.
    sunday = next(d for d in off_days if d.weekday() == 6)
    assert b.catch_up.was_run(JOB_SECTOR_MAP, sunday)
    # The lane starts nothing in-session; the debt stays on the watermark for the first sweep after the window.
    assert all(not b.catch_up.was_run(job_id, d) for job_id in LANE_JOB_IDS for d in missed)
    assert not {j for j, _ in calls} & LANE_JOB_IDS

    report = b.report
    assert report is not None and report.crash_recovered is False and report.jobs_failed == []
    assert report.off_duration_s == pytest.approx(
        (_at(restart_day, B_BOOT) - _at(stop_day, A_STOP)).total_seconds()
    )
    # The whole registry is armed on a RUNNING scheduler after the boot.
    assert b.scheduler.is_running()
    assert {s.job_id for s in b.registry.specs()} <= b.armed_job_ids()
    await b.stop()


# --------------------------------------------------------------------------- candle backfill + warm-up gate
class FakeKiteCandles:
    """``KiteClient.historical`` boundary: one daily candle per NSE session strictly before today (never
    today's running candle), one minute candle per elapsed continuous-session minute."""

    def __init__(self, env: RigEnv, is_trading_day) -> None:
        self._env = env
        self._trading = is_trading_day
        self.token_valid = True
        self.requests: list[tuple[int, datetime, datetime, str]] = []

    @staticmethod
    def _candle(ts: datetime) -> dict:
        return {"date": ts, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}

    async def historical(self, token, frm, to, interval):
        self.requests.append((token, frm, to, interval))
        now = self._env.clock.now()
        out = []
        if interval == "day":
            d = frm.astimezone(IST).date()
            while d <= to.astimezone(IST).date():
                if d < now.date() and self._trading(d):
                    out.append(self._candle(datetime.combine(d, time(0, 0), tzinfo=IST)))
                d += timedelta(days=1)
            return out
        m = frm.astimezone(IST).replace(second=0, microsecond=0)
        end = min(to.astimezone(IST), now)
        while m < end:
            if self._trading(m.date()) and time(9, 15) <= m.time() < time(15, 30):
                out.append(self._candle(m))
            m += timedelta(minutes=1)
        return out


def _market_wiring(kite_holder: dict):
    """The startup backfill hook and warm-up gate as main.py wires them, over a FakeKiteCandles kept in
    ``kite_holder`` so a scenario can flip its token."""

    def warmup_gate_factory(proc: EngineProcess):
        return WarmupGate(proc.store, proc.env.clock, proc.calendar, symbols=WATCH,
                          index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)

    def backfill_hook_factory(proc: EngineProcess):
        # One exchange for the whole scenario: it outlives every engine process.
        kite = kite_holder.setdefault("kite", FakeKiteCandles(
            proc.env, NSECalendar(config_dir() / "calendar", proc.env.clock, strict=False).is_trading_day,
        ))
        backfill = BackfillJob(proc.store, kite, proc.env.clock, proc.env.settings, proc.conn, TOKENS.get)

        async def backfill_hook() -> None:
            # As main.py: a token-less boot issues no historical calls.
            if not kite.token_valid:
                return
            await regime_and_warmup_backfill(
                backfill, proc.env.clock, proc.calendar, proc.env.settings, lambda: list(WATCH),
                INDEX_SYMBOL, VIX_SYMBOL,
            )

        return backfill_hook

    return {"market_store": True, "warmup_gate_factory": warmup_gate_factory,
            "backfill_hook_factory": backfill_hook_factory}


def _daily_dates(proc: EngineProcess, symbol: str, frm: date, to: date) -> set[date]:
    return {b.d for b in proc.store.get_bars_1d(symbol, frm, to)}


async def test_case17_restart_backfills_candle_gap_then_warmup_ready(tmp_path, monkeypatch):
    kite_holder: dict = {}
    _prior, stop_day, restart_day, missed, off_days = GAPS["weekend"]
    env, b, _calls, sent_b = await _gap_restart(tmp_path, monkeypatch, "weekend", **_market_wiring(kite_holder))
    kite: FakeKiteCandles = kite_holder["kite"]
    report = b.report
    assert report is not None and "data_gap_backfill_ok" in report.notes

    # Bars landed for exactly the gap's trading days.
    for sym in (INDEX_SYMBOL, VIX_SYMBOL, "RELIANCE"):
        have = _daily_dates(b, sym, stop_day - timedelta(days=1), restart_day)
        assert set(missed) <= have, f"{sym}: gap days missing {sorted(set(missed) - have)}"
        assert not have & set(off_days), f"{sym}: bar on a non-trading day"
    gap_reqs = [iv for _tok, f, t, iv in kite.requests
                if iv == "day" and f.date() <= missed[0] and t.date() >= missed[-1]]
    assert gap_reqs, "no daily candle request covered the offline gap"
    session_open = b.calendar.session(restart_day).open
    boot = _at(restart_day, B_BOOT)
    minutes = b.store.get_bars_1m("RELIANCE", session_open, boot)
    assert len(minutes) == (boot - session_open) // timedelta(minutes=1)          # every elapsed minute
    assert {m.src for m in minutes} == {"gap_backfilled"}

    assert "warmup_ready" in report.notes and report.frozen_reasons == []
    assert b.mode.risk_state() == RiskState.NORMAL
    assert MessageKind.WARMUP_FROZEN not in [m.kind for m in env.messages(since=sent_b)]
    await b.stop()


async def test_case17_tokenless_restart_enforces_warmup_until_gap_backfilled(tmp_path, monkeypatch):
    """A restart that cannot reach the candle API must not trade on gapped history: REGIME short ⇒ FROZEN + WARMUP_FROZEN."""
    kite_holder: dict = {}
    wiring = _market_wiring(kite_holder)
    prior, stop_day, restart_day, missed, _off = GAPS["weekend"]
    env = RigEnv(tmp_path, monkeypatch, start=_t(prior, 22, 45))
    await run_session(env, _t(prior, 22, 45), _t(prior, 22, 50))
    await run_session(env, _at(stop_day, A_BOOT), _at(stop_day, A_STOP), **wiring)   # token valid Thu
    kite: FakeKiteCandles = kite_holder["kite"]
    kite.token_valid = False                                   # Mon boot before the daily Kite login

    env.at(_at(restart_day, B_BOOT))
    sent_b = len(env.sent)
    b = EngineProcess(env, **wiring)
    report = await b.boot()
    assert "warmup_ready" in report.frozen_reasons
    assert "regime" in report.warmup_classes_short
    assert b.mode.risk_state() == RiskState.FROZEN
    frozen_msgs = [m for m in env.messages(since=sent_b) if m.kind == MessageKind.WARMUP_FROZEN]
    assert len(frozen_msgs) == 1 and "regime" in frozen_msgs[0].data["classes"]
    assert not set(missed) & _daily_dates(b, INDEX_SYMBOL, missed[0], missed[-1])   # gap still open

    # Post-login re-trigger: the SAME backfill hook, then the gate is re-applied.
    env.at(_at(restart_day, B_BOOT) + timedelta(minutes=10))
    kite.token_valid = True
    await b.backfill_hook()
    reapply = await b.lifecycle.reapply_warmup_gate()
    assert reapply.ready is True and reapply.lifted is True, reapply
    assert set(missed) <= _daily_dates(b, INDEX_SYMBOL, missed[0], missed[-1])
    assert b.mode.risk_state() == RiskState.NORMAL
    await b.stop()


@pytest.mark.parametrize("ex_date", [date(2026, 6, 12), None], ids=["ex-date-in-gap", "no-ex-date"])
async def test_case17_paper_gtt_across_the_gap_voids_on_an_ex_date_else_stays_resting(conn, bus, ex_date) -> None:
    """An ex-date in the unobserved window VOIDs the position and its restored GTT is gone before any tick; without one the GTT rests and fires."""
    _prior, stop_day, restart_day, _missed, _off = GAPS["weekend"]
    now = Now()
    now.value = _t(stop_day, 10, 0)
    before = Rig(conn, bus, now=now)
    pid = await before.enter(ts=_t(stop_day, 10, 0, 1))
    await before.mgr.stop()
    conn.execute("INSERT INTO paper_state (id, enabled, last_observed_at) VALUES (1, 1, ?)",
                 (_t(stop_day, 15, 29).isoformat(),))

    now.value = _t(restart_day, 9, 30)
    clock = Clock(time_source=now)
    holder: dict[str, Rig] = {}

    async def prep(since: datetime | None, until: datetime) -> None:
        """Stand-in for SessionPrep's ex-date rule (covered by tests/unit/test_session_prep.py) so this case isolates the PaperRuntime prep hook."""
        if ex_date is not None and since is not None and since.date() < ex_date <= until.date():
            await holder["rig"].book.void(pid, None, until, "corp_action")

    runtime = PaperRuntime(conn, clock, NSECalendar(config_dir() / "calendar", clock, strict=False), bus,
                           PaperSettings(), capital_base_fn=lambda: Decimal(40000), tick_size_fn=lambda _: Decimal("0.05"),
                           order_guard=lambda _: None, prep=prep, fill_model_fn=FillModelConfig)
    await runtime.start()
    rig = holder["rig"] = Rig(conn, bus, broker=runtime.broker, now=now)
    try:
        [restored] = await rig.gtts()
        for ts in (_t(restart_day, 9, 30), _t(restart_day, 9, 30, 5)):     # gapped below the 95 stop
            now.value = ts
            bus.publish("tick", tick(ts, "90.00"))
            await rig.settle()
        await rig.pm.verify_all()

        assert runtime.prep_ready()
        pos = conn.execute("SELECT * FROM positions WHERE position_id = ?", (pid,)).fetchone()
        if ex_date is not None:
            assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("CLOSED", "void", "corp_action")
            assert await rig.gtts() == [] and orders(conn, "gtt_leg") == []
            assert conn.execute("SELECT state FROM gtts").fetchone()["state"] == "deleted"
        else:
            [fired] = await rig.gtts()
            assert (fired["id"], fired["status"], pos["state"]) == (restored["id"], "triggered", "PENDING_EXIT")
            assert len(orders(conn, "gtt_leg")) == 1
    finally:
        await rig.mgr.stop()
        await runtime.stop()
