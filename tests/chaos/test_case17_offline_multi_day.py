"""§9.4 chaos case 17 — Offline MULTI-DAY (e.g. 4 days incl. a weekend).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 17), "Must hold":

    "startup backfills the bar/candle gap from the Kite candle API; re-runs each missed day's EOD jobs;
    detects any ex-date that fell in the gap, re-derives the adjusted trigger and cancel-and-replaces the
    held-CNC GTT to repair protection, AND FROZEN-for-symbol + high-sev alert if T-1 passed unadjusted;
    verifies open CNC GTTs still resting; re-arms schedules; cold-start warmup_ready enforced"

Two 4-day gaps, each a clean stop Thu 15:40 (before the EOD window) → restart Mon 09:30:

* ``weekend``         Thu 2026-06-11 → Mon 2026-06-15: missed trading days Thu 11, Fri 12.
* ``holiday_weekend`` Thu 2026-06-25 → Mon 2026-06-29: Fri 26 is an NSE holiday (Muharram), so the
  only missed trading day is Thu 25.

Composition (``tests/chaos/_lifecycle_rig.py``): real lifecycle + CatchUpRunner over the real
``build_job_registry`` inventory + Scheduler arming; for the backfill/warm-up clauses additionally a
real ``MarketStore``, ``BackfillJob`` driven through ``regime_and_warmup_backfill`` (the exact §2.6
step-4 call main.py's ``backfill_hook`` makes, incl. its ``token_valid()`` guard) and a real
``WarmupGate``. The Kite historical-candle API is the faked boundary (:class:`FakeKiteCandles`:
official candles for every NSE session — none for weekends/holidays, none for today's running daily
candle, none for minutes that have not happened).

Clauses covered:

* "startup backfills the bar/candle gap from the Kite candle API" — the restart fetches the gap's
  daily candles (regime NIFTY 50 / INDIA VIX + the watchlist's daily window) and today's minutes;
  bars exist for exactly the gap's trading days, none for the weekend.
* "re-runs each missed day's EOD jobs" — every date-keyed job runs once per missed TRADING day,
  ascending, never for a weekend/holiday; run-latest jobs (incl. backup, and the Sunday sector map
  whose fire-day fell in the gap) run exactly once for the whole gap. ``tick_compact`` is vetoed
  in-session at a 09:30 boot (WO-21) and keeps its watermark debt for the post-session sweep —
  asserted so that deferral stays visible.
* "re-arms schedules" — every registry job armed on a running scheduler after the boot.
* "cold-start warmup_ready enforced" — a token-less restart (no candle API) cannot cover the gap:
  REGIME coverage is short ⇒ FROZEN-for-entries + ``WARMUP_FROZEN``; the post-login re-trigger (the
  same backfill hook + ``reapply_warmup_gate``) then fills the gap and lifts the freeze.

Phase-3-gated (skipped, ``test_case17_ex_date_gtt_repair_and_resting_gtt_verify``): the ex-date GTT
repair (re-derive trigger, cancel-and-replace, FROZEN-for-symbol + high-sev alert on a passed T-1) and
"verifies open CNC GTTs still resting" — needs GTTManager + held-CNC protection (WO-P3-6); the
corp-actions ex-date feed into ``NSECalendar.ex_dates`` is also still a stub.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
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
    JobClass,
)
from engine.ops.main import INDEX_SYMBOL, VIX_SYMBOL, post_arm_exclusions
from engine.ops.post_login import regime_and_warmup_backfill
from engine.ops.warmup import WarmupGate
from tests.chaos._lifecycle_rig import EngineProcess, RigEnv, run_session
from tests.chaos.conftest import PHASE3_GATED

A_BOOT, A_STOP, B_BOOT = time(9, 30), time(15, 40), time(9, 30)
#: The EOD jobs the plan names for the per-missed-day replay (§2.6 step 5 date-keyed list).
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
    # Scenario precondition on the configured schedule (settings.yaml jobs.*): nothing fires inside
    # the stop day's uptime — the rig fires no scheduled jobs there.
    assert not [s.job_id for s in b.registry.specs() if A_BOOT < s.at <= A_STOP]
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
    in_session_veto = set(post_arm_exclusions(env.clock, b.calendar))   # WO-21 at a 09:30 boot
    assert in_session_veto == {JOB_TICK_COMPACT}
    date_keyed = {s.job_id for s in reg.specs(JobClass.DATE_KEYED)} - in_session_veto
    assert PLAN_NAMED_DATE_KEYED <= date_keyed
    assert all(s.at > A_STOP for s in reg.specs(JobClass.DATE_KEYED)), "stop day must be a missed EOD"
    run_latest = {s.job_id for s in reg.specs(JobClass.RUN_LATEST)}
    morning_safety = {s.job_id for s in reg.specs(JobClass.SAFETY_CRITICAL) if s.at <= B_BOOT}

    # Each missed day's EOD jobs: once per missed TRADING day, ascending; never a weekend/holiday.
    for job_id in sorted(date_keyed):
        assert [d for j, d in calls if j == job_id] == missed, f"{job_id}: {[c for c in calls if c[0] == job_id]}"
    assert not {d for _j, d in calls if d is not None} & set(off_days), "a weekend/holiday was replayed"
    # Run-latest (backup, corp-actions, universe, the news chain, …): ONE run for the whole gap.
    assert JOB_BACKUP in run_latest
    for job_id in sorted(run_latest):
        assert [j for j, _ in calls if j == job_id] == [job_id], f"{job_id} not exactly once for the gap"
    for job_id in sorted(morning_safety):                      # today's deadline jobs: run for today
        assert [j for j, _ in calls if j == job_id] == [job_id]
        assert b.catch_up.was_run(job_id, restart_day)
    assert {j for j, _ in calls} == date_keyed | run_latest | morning_safety
    # Sunday's weekly sector map fell inside the gap: one run, recorded under that Sunday.
    sunday = next(d for d in off_days if d.weekday() == 6)
    assert b.catch_up.was_run(JOB_SECTOR_MAP, sunday)
    # tick_compact: vetoed in-session (WO-21) — the debt stays on the watermark for the evening sweep.
    assert all(not b.catch_up.was_run(JOB_TICK_COMPACT, d) for d in missed)

    report = b.report
    assert report is not None and report.crash_recovered is False and report.jobs_failed == []
    assert report.off_duration_s == pytest.approx(
        (_at(restart_day, B_BOOT) - _at(stop_day, A_STOP)).total_seconds()
    )
    # Re-arms schedules: the whole registry is armed on a RUNNING scheduler after the boot.
    assert b.scheduler.is_running()
    assert {s.job_id for s in b.registry.specs()} <= b.armed_job_ids()
    await b.stop()


# --------------------------------------------------------------------------- candle backfill + warm-up gate
class FakeKiteCandles:
    """``KiteClient.historical`` boundary: the official candles the exchange published — one daily
    candle per NSE session strictly before today (the day interval never returns today's running
    candle here; the §2.6 step-4 caller clamps to yesterday anyway), one minute candle per elapsed
    continuous-session minute."""

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
    """The composition-root pieces for §2.6 steps 4 + 6 (main.py ``backfill_hook`` + ``WarmupGate``),
    bound to a per-process FakeKiteCandles kept in ``kite_holder`` so a scenario can flip its token."""

    def warmup_gate_factory(proc: EngineProcess):
        return WarmupGate(proc.store, proc.env.clock, proc.calendar, symbols=WATCH,
                          index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)

    def backfill_hook_factory(proc: EngineProcess):
        # One exchange for the whole scenario (it outlives every engine process), on its own calendar.
        kite = kite_holder.setdefault("kite", FakeKiteCandles(
            proc.env, NSECalendar(config_dir() / "calendar", proc.env.clock, strict=False).is_trading_day,
        ))
        backfill = BackfillJob(proc.store, kite, proc.env.clock, proc.env.settings, proc.conn, TOKENS.get)

        async def backfill_hook() -> None:
            # Mirrors engine.ops.main backfill_hook: a token-less boot issues no historical calls.
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

    # The gap was fetched from the candle API at this boot and landed for exactly the trading days.
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

    # With the gap covered, warm-up is satisfied and entries stay open.
    assert "warmup_ready" in report.notes and report.frozen_reasons == []
    assert b.mode.risk_state() == RiskState.NORMAL
    assert MessageKind.WARMUP_FROZEN not in [m.kind for m in env.messages(since=sent_b)]
    await b.stop()


async def test_case17_tokenless_restart_enforces_warmup_until_gap_backfilled(tmp_path, monkeypatch):
    """Cold-start ``warmup_ready`` enforced: a restart that cannot reach the candle API (no Kite
    session yet) must not trade on the gapped history — REGIME short ⇒ FROZEN + WARMUP_FROZEN."""
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

    # Login lands: the post-login re-trigger re-runs the SAME backfill hook and re-applies the gate.
    env.at(_at(restart_day, B_BOOT) + timedelta(minutes=10))
    kite.token_valid = True
    await b.backfill_hook()
    reapply = await b.lifecycle.reapply_warmup_gate()
    assert reapply.ready is True and reapply.lifted is True, reapply
    assert set(missed) <= _daily_dates(b, INDEX_SYMBOL, missed[0], missed[-1])
    assert b.mode.risk_state() == RiskState.NORMAL
    await b.stop()


def test_case17_ex_date_gtt_repair_and_resting_gtt_verify() -> None:
    """Ex-date-in-gap GTT repair (re-derive trigger, cancel-and-replace, FROZEN-for-symbol + high-sev
    alert on a passed T-1) and 'verifies open CNC GTTs still resting'."""
    pytest.skip(f"case 17 GTT clauses: {PHASE3_GATED}; missing: GTTManager + held-CNC protection "
                "(corp-action ex-date GTT repair, resting-GTT verify)")
