"""Prompt integrity (A2): absent is not empty.

Every prompt section renders one of three states and never confuses them: the source is absent
(``unavailable (<reason>)``), present but empty (an explicit ``none``), or present (the value). Rule 14
tells the analyst absent news is neutral, which only works if absence is labelled. Sections with no
empty state (1m bars, the day plan, movers) carry only the other two; the regime line's renderings
are tests/unit/test_context_assembler.py's T6 table.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.datafeeds.news_pipeline import CatalystDigestJob
from engine.intelligence.context import ContextAssembler
from engine.intelligence.harness import AgentResult, load_agent_defs
from engine.marketdata.store import MarketStore
from engine.ops.jobs import JOB_CATALYST_DIGEST, JOB_EARNINGS
from engine.ops.preopen_planner import PreopenPlannerJob
from engine.strategy.scanners import brk20
from tests.unit.test_context_assembler import (
    BAR_TAIL,
    CANDIDATE,
    DAILY_HEADER,
    DAILY_LAST_ROW,
    SIGNAL_KW,
    SWING_CANDIDATE,
    SYMBOL,
    TODAY,
    UNWATCHED,
    seed_bars,
    seed_catalyst,
    seed_daily_bars,
    seed_day_plan,
    seed_features,
)
from tests.unit.test_preopen_planner import (
    AGENTS_CFG,
    PRIOR,
    PRIOR2,
    FakeGovernor,
    FakeHarness,
    _plan,
    _position,
    _seed_bars,
    _seed_universe,
)

AS_OF = datetime(2026, 6, 17, 8, 35, tzinfo=IST)
YESTERDAY_AS_OF = datetime(2026, 6, 16, 18, 0, tzinfo=IST)
NOW = "2026-06-17T10:05:00+05:30"
BARS = f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first):"
HEADROOM = "risk headroom (informational - the gate re-checks everything):"
NO_HEADLINES = "none (no scored headlines in the latest digest)"


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def w(store, conn, clock, monkeypatch):
    """The world one row arranges: the stores, the candidate and kwargs, and the two watermark seams."""
    return SimpleNamespace(
        store=store, conn=conn, clock=clock, mp=monkeypatch,
        calendar=NSECalendar(config_dir() / "calendar", clock, strict=False),
        cand=CANDIDATE, kw=dict(SIGNAL_KW), ran=None, last={},
    )


def _boom(*_args, **_kwargs):
    raise RuntimeError("store is having a day")


def _fails(read: str):
    return lambda w: w.mp.setattr(w.store, read, _boom)


def _set(**attrs):
    return lambda w: vars(w).update(attrs)


def _both(*arrange):
    return lambda w: [a(w) for a in arrange]


def _assert_lines(prompt: str, chunks: tuple[str, ...]) -> None:
    """Each chunk must appear as WHOLE lines (a ``none`` must not match ``none (...)``)."""
    padded = f"\n{prompt}\n"
    assert [c for c in chunks if f"\n{c}\n" not in padded] == []


def _sentiment(*rows: tuple[str, str, float], as_of: datetime = AS_OF):
    return lambda w: w.store.upsert_sentiment_agg(
        [{"scope": s, "scope_key": k, "as_of": as_of, "value": v} for s, k, v in rows]
    )


def _digest_ran_empty(w) -> None:
    """A digest run that scored no cluster, in production's own row shape (the market marker)."""
    job = CatalystDigestJob(w.store, w.clock, w.calendar, None)
    w.store.upsert_sentiment_agg(job._sentiment_rows([], AS_OF, {}, {}, set()))


def _sector(w) -> None:
    w.store.upsert_sector_map(TODAY, [{"symbol": SYMBOL, "sector": "Energy"}])


_SWING = _set(cand=SWING_CANDIDATE)

SIGNAL_CASES = {
    "features/absent": (
        _set(cand=CANDIDATE.model_copy(update={"features_snapshot_id": None})),
        ("features: unavailable (no feature snapshot for this candidate)",),
    ),
    "features/not_found": (lambda w: None, ("features: unavailable (feature snapshot fs-1 not found)",)),
    "features/read_failed": (_fails("get_feature_snapshot"), ("features: unavailable (feature read failed)",)),
    "features/empty": (
        lambda w: w.store.insert_feature_snapshot("fs-1", SYMBOL, AS_OF, 1, "{}"),
        ("features: none (the snapshot carries no features)",),
    ),
    "features/present": (lambda w: seed_features(w.store), ('features: {"atr14":12.5,"rvol":1.8} (as of 10:04)',)),
    "bars/absent": (lambda w: None, (
        f"{BARS} unavailable (no completed 1m bars this session)",
        "session: unavailable (no completed 1m bars this session), 50m elapsed",
        f"last price: unavailable (no completed 1m bars this session; context assembled at {NOW})",
    )),
    "bars/read_failed": (_fails("get_bars_1m"), (
        f"{BARS} unavailable (1m bar read failed)",
        "session: unavailable (1m bar read failed), 50m elapsed",
        f"last price: unavailable (1m bar read failed; context assembled at {NOW})",
    )),
    "bars/present": (lambda w: seed_bars(w.store, count=5), (
        f"{BARS}\n  10:00 1400.00 1401.00 1399.00 1400.50 1000",
        f"last price: 1404.50 at 10:04 (last completed 1m bar; context assembled at {NOW})",
    )),
    "swing_daily/absent": (_SWING, (
        f"{DAILY_HEADER} unavailable (no completed daily bars in the last 45 calendar days)",
    )),
    "swing_daily/read_failed": (
        _both(_SWING, _fails("get_bars_1d")), (f"{DAILY_HEADER} unavailable (daily-bar read failed)",),
    ),
    "swing_daily/present": (_both(_SWING, lambda w: seed_daily_bars(w.store, symbol=UNWATCHED)), (DAILY_LAST_ROW,)),
    "watchlist/digest_not_run": (
        _set(ran=lambda d: False),
        ("  watchlist entry: unavailable (today's catalyst digest has not run)",),
    ),
    "watchlist/read_failed": (
        _fails("get_catalyst_watchlist"), ("  watchlist entry: unavailable (watchlist read failed)",),
    ),
    "watchlist/empty": (_set(ran=lambda d: d == TODAY), ("  watchlist entry: none for this symbol today",)),
    "watchlist/empty_rows_prove_the_run": (_both(
        _set(ran=lambda d: False),
        lambda w: w.store.replace_catalyst_watchlist(TODAY, [{"symbol": "OTHER", "grade": "context"}]),
    ), ("  watchlist entry: none for this symbol today",)),
    "watchlist/unwired": (lambda w: None, ("  watchlist entry: none for this symbol today",)),
    "watchlist/present": (lambda w: seed_catalyst(w.store), (
        '  watchlist entry: {"direction":"long","event_type":"order_win","grade":"context",'
        '"materiality":0.72,"source_domain_count":3}',
    )),
    "sentiment/absent": (lambda w: None, ("catalyst:", "  sentiment: unavailable (no sentiment digest has run)")),
    "sentiment/read_failed": (
        _both(_sentiment(("market", "market", 0.1)), _fails("get_sentiment_agg")),
        ("catalyst:", "  sentiment: unavailable (sentiment read failed)"),
    ),
    "sentiment/empty": (_both(_sector, _digest_ran_empty), (
        "catalyst (sentiment as of 08:35):",
        f"  sentiment symbol {SYMBOL}: {NO_HEADLINES}",
        f"  sentiment sector Energy: {NO_HEADLINES}",
        f"  sentiment market: {NO_HEADLINES}",
    )),
    "sentiment/sector_unknown": (
        _sentiment(("market", "market", 0.1)),
        ("  sentiment sector: unavailable (sector unknown for this symbol)",),
    ),
    "sentiment/yesterday": (
        _sentiment(("market", "market", 0.1), as_of=YESTERDAY_AS_OF),
        ("catalyst (sentiment as of 2026-06-16 18:00):",),
    ),
    "sentiment/present": (lambda w: seed_catalyst(w.store), (
        f"  sentiment symbol {SYMBOL}: +0.420",
        "  sentiment sector Energy: +0.150",
        "  sentiment market: -0.110",
    )),
    "headroom/empty": (lambda w: w.kw.update(headroom_lines=[]), (f"{HEADROOM}\n  - none",)),
    "headroom/present": (lambda w: None, (f"{HEADROOM}\n  - per_trade_risk: 0.6% of 1.0%",)),
    "day_plan/absent": (lambda w: None, ("day plan: unavailable (no day plan for 2026-06-17)",)),
    "day_plan/read_failed": (
        lambda w: w.conn.execute("DROP TABLE day_plans"),
        ("day plan: unavailable (day plan read failed)", "day plan age: unavailable (day plan read failed)"),
    ),
    "day_plan/unreadable": (
        lambda w: seed_day_plan(w.conn, payload="{not json"), ("day plan: unavailable (day plan unreadable)",),
    ),
    "day_plan/present": (
        lambda w: seed_day_plan(w.conn, payload='{"regime": "chop"}'), ('day plan: {"regime":"chop"}',),
    ),
}


@pytest.mark.parametrize(("arrange", "expected"), SIGNAL_CASES.values(), ids=SIGNAL_CASES.keys())
def test_for_signal_sections(w, arrange, expected):
    arrange(w)
    asm = ContextAssembler(w.store, w.conn, w.clock, w.calendar, digest_ran_fn=w.ran)
    _assert_lines(asm.for_signal(w.cand, **w.kw).prompt(), expected)


# --------------------------------------------------------------------------- the pre-open planner
async def _planner_prompt(w, d: date = TODAY) -> str:
    harness = FakeHarness(AgentResult.Ok(call_id="c1", payload=_plan()))
    job = PreopenPlannerJob(
        w.store, w.conn, ContextAssembler(w.store, w.conn, w.clock, w.calendar), harness,
        load_agent_defs(AGENTS_CFG), FakeGovernor(), w.clock, w.calendar,
        last_success=None if w.last is None else w.last.get,
    )
    await job.run(d)
    return harness.calls[0]["context"].prompt()


MOVERS = "overnight movers (bhavcopy):"
BREAKOUTS = "20d-high daily breakouts (brk20, yesterday's close; full eligible universe):"
DIGEST = "catalyst digest:"
WATCHLIST = "catalyst watchlist (binding levels are the scanner's)"
EARNINGS = "earnings today"
SURVEILLANCE = "exchange surveillance changes:"
POSITIONS = "open positions and overnight risk:"
REVIEW = "prior session's post-mortem (HISTORY, may describe already-fixed issues):"


def _aaa_today(w) -> None:
    _seed_universe(w.store, TODAY, ["AAA"])


def _aaa_prior_bar(w) -> None:
    _seed_bars(w.store, PRIOR, {"AAA": "104.10"})


def _review(payload: object):
    return lambda w: w.conn.execute(
        "INSERT INTO nightly_reviews (d, payload, created_at) "
        "VALUES ('2026-06-16', ?, '2026-06-16T21:00:00+05:30')",
        (json.dumps(payload),),
    )


_BREAKOUT = SimpleNamespace(
    symbol="AAA", raw_levels=SimpleNamespace(entry=Decimal("100"), stop=Decimal("98")), score=0.9
)

PLANNER_CASES = {
    "movers/absent": (lambda w: None, (f"{MOVERS}\n  - unavailable (no universe for 2026-06-16)",)),
    "movers/no_bars": (
        lambda w: _seed_universe(w.store, PRIOR, ["AAA"]),
        (f"{MOVERS}\n  - unavailable (no symbol has daily bars for both 2026-06-15 and 2026-06-16)",),
    ),
    "movers/present": (_both(
        lambda w: _seed_universe(w.store, PRIOR, ["AAA"]),
        lambda w: _seed_bars(w.store, PRIOR2, {"AAA": "100.00"}), _aaa_prior_bar,
    ), (f"{MOVERS}\n  - AAA +4.10%",)),
    "breakouts/no_universe": (
        lambda w: None, (f"{BREAKOUTS}\n  - unavailable (no batch universe for 2026-06-17)",),
    ),
    "breakouts/absent": (_aaa_today, (f"{BREAKOUTS}\n  - unavailable (no completed daily bars for 2026-06-16)",)),
    "breakouts/empty": (_both(_aaa_today, _aaa_prior_bar), (f"{BREAKOUTS}\n  - none",)),
    "breakouts/present": (_both(
        _aaa_today, _aaa_prior_bar, lambda w: w.mp.setattr(brk20, "sweep_daily", lambda *a, **k: [_BREAKOUT]),
    ), (f"{BREAKOUTS}\n  - AAA closed 102 above its 20d high 100 (score 0.90; watchlisted: yes)",)),
    "digest/absent": (lambda w: None, (f"{DIGEST}\n  - unavailable (no sentiment digest has run)",)),
    "digest/empty": (
        _digest_ran_empty,
        (f"{DIGEST}\n  - sentiment as of 08:35\n  - none (the latest digest scored no market or sector headlines)",),
    ),
    "digest/yesterday": (
        _sentiment(("market", "market", -0.11), as_of=YESTERDAY_AS_OF),
        (f"{DIGEST}\n  - sentiment as of 2026-06-16 18:00\n  - market: -0.110",),
    ),
    "digest/present": (
        _sentiment(("market", "market", -0.11)), (f"{DIGEST}\n  - sentiment as of 08:35\n  - market: -0.110",),
    ),
    "watchlist/absent": (
        _set(last={JOB_CATALYST_DIGEST: PRIOR}),
        (f"{WATCHLIST}:\n  - unavailable (catalyst digest has not run for 2026-06-17)",),
    ),
    "watchlist/empty": (_set(last={JOB_CATALYST_DIGEST: TODAY}), (f"{WATCHLIST}: none",)),
    "watchlist/unwired": (_set(last=None), (f"{WATCHLIST}: none",)),
    "watchlist/present": (lambda w: seed_catalyst(w.store), (f"{WATCHLIST}:\n  - {SYMBOL} context order_win",)),
    "earnings/never": (
        lambda w: None, (f"{EARNINGS}:\n  - unavailable (earnings calendar not refreshed since never)",),
    ),
    "earnings/stale": (
        _set(last={JOB_EARNINGS: date(2026, 6, 11)}),
        (f"{EARNINGS}:\n  - unavailable (earnings calendar not refreshed since 2026-06-11)",),
    ),
    "earnings/empty": (_set(last={JOB_EARNINGS: date(2026, 6, 12)}), (f"{EARNINGS}: none",)),
    "earnings/unwired": (_set(last=None), (f"{EARNINGS}: none",)),
    "earnings/present": (
        lambda w: w.store.upsert_earnings_calendar([{"symbol": "HDFCBANK", "event_date": TODAY, "kind": "results"}]),
        (f"{EARNINGS}:\n  - HDFCBANK (results)",),
    ),
    "surveillance/absent": (
        lambda w: None, (f"{SURVEILLANCE}\n  - unavailable (universe build has not run for 2026-06-17)",),
    ),
    "surveillance/empty": (_aaa_today, (f"{SURVEILLANCE}\n  - none",)),
    "surveillance/present": (lambda w: w.store.upsert_universe_daily([{
        "d": TODAY, "symbol": "FLAGGED", "included": False, "mis_candidate": False,
        "exclusion_reasons": ["surveillance_asm"], "median_traded_value": None,
    }]), (f"{SURVEILLANCE}\n  - FLAGGED: surveillance_asm",)),
    "positions/read_failed": (
        lambda w: w.conn.execute("DROP TABLE positions"), (f"{POSITIONS} unavailable (positions read failed)",),
    ),
    "positions/empty": (lambda w: None, (f"{POSITIONS} none",)),
    "positions/present": (
        lambda w: _position(w.conn, "pos-1", SYMBOL, 3, stop="90"),
        (f"{POSITIONS} {SYMBOL} BUY 3 @ 100 (CNC, stop 90)",),
    ),
    "review/absent": (lambda w: None, (f"{REVIEW} unavailable (no nightly review recorded)",)),
    "review/unreadable": (_review(["not", "a", "dict"]), (f"{REVIEW} unavailable (the 2026-06-16 review is unreadable)",)),
    "review/empty": (_review({"summary": " "}), (f"{REVIEW} none (the 2026-06-16 review has no summary)",)),
    "review/present": (
        _review({"summary": "all quiet"}), (f"{REVIEW} [review of the 2026-06-16 session] all quiet",),
    ),
}


@pytest.mark.parametrize(("arrange", "expected"), PLANNER_CASES.values(), ids=PLANNER_CASES.keys())
async def test_for_planner_sections(w, arrange, expected):
    arrange(w)
    _assert_lines(await _planner_prompt(w), expected)


async def test_breakouts_count_a_muhurat_bar_after_the_prior_session(w):
    """Live calendar (muhurat closed): on Mon 2026-11-09 the prior session is Fri 11-06, and the
    Sunday 11-08 muhurat bar the brk20 sweep reads is a completed session, not a missing 11-06."""
    monday = date(2026, 11, 9)
    w.calendar = NSECalendar(config_dir() / "calendar", w.clock, strict=True, include_special=False)
    _seed_universe(w.store, monday, ["AAA"])
    _seed_bars(w.store, date(2026, 11, 8), {"AAA": "100.00"})

    _assert_lines(await _planner_prompt(w, monday), (f"{BREAKOUTS}\n  - none",))


# --------------------------------------------------------------------------- the guard
async def test_an_all_absent_context_leaves_no_header_without_content(w):
    """A line ending in ``:`` must have indented content beneath it: an empty header reads as
    "nothing to report" exactly where the source said nothing at all."""
    asm = ContextAssembler(w.store, w.conn, w.clock, w.calendar, digest_ran_fn=lambda d: False)
    no_headroom = {**SIGNAL_KW, "headroom_lines": []}
    news = asm.for_news_batch([], []).prompt()
    prompts = [
        *(asm.for_signal(c, **no_headroom).prompt() for c in (CANDIDATE, SWING_CANDIDATE)),
        await _planner_prompt(w),
        news,
    ]

    _assert_lines(news, ("theme vocabulary (the only themes you may emit): none",))
    for prompt in prompts:
        lines = prompt.splitlines()
        indent = [len(ln) - len(ln.lstrip()) for ln in lines] + [-1]
        assert [ln for i, ln in enumerate(lines) if ln.endswith(":") and indent[i + 1] <= indent[i]] == []
