"""ContextAssembler (§3.2.6 / §5.2–§5.4): cache discipline, determinism, and fail-to-zero rendering.

The load-bearing properties, in order of how expensive they are to get wrong:

1. **Byte-stability (D8).** The same inputs produce the same digest; changing only VOLATILE inputs
   leaves the system prompt and the stable block byte-identical, so the cacheable prefix survives.
2. **Digest sensitivity (R8).** Any block change moves the digest — it is what a decision is replayed
   against.
3. **Fail to zero (D7).** A missing feature snapshot, no bars, no day plan and an un-run sentiment
   digest all render as explicit "unavailable" text instead of raising.
4. **Per-trigger output restrictions (§5.2).** Position-event and heartbeat contexts say in the
   prompt what the caller enforces structurally.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.intelligence.agents import intraday, news_analyst, preopen
from engine.intelligence.context import BAR_TAIL, ContextAssembler
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.types import RawLevels, SignalCandidate

# conftest's frozen clock: Wed 2026-06-17 10:05 IST, a real trading day.
TODAY = date(2026, 6, 17)
SYMBOL = "RELIANCE"

CANDIDATE = SignalCandidate(
    signal_id="sig-1",
    strategy_id="orb",
    symbol=SYMBOL,
    side="BUY",
    style="intraday",
    raw_levels=RawLevels(entry=Decimal("1400.00"), stop=Decimal("1390.00")),
    score=0.7,
    features_snapshot_id="fs-1",
)

OTHER_CANDIDATE = CANDIDATE.model_copy(update={"signal_id": "sig-2", "score": 0.4})

SIGNAL_KW = dict(
    equity=Decimal("500000.00"),
    headroom_lines=["per_trade_risk: 0.6% of 1.0%", "daily_loss: -0.2% of -3.0%"],
    open_positions_summary="1 open: TCS 5 @ 3900 (MIS)",
    cost_line="notional 19600, total cost 41.20, breakeven 0.21%, edge multiple 3.1",
    max_qty_by_risk=14,
    sector_exposure_line="Energy 12% of 25% cap",
)


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def assembler(store, conn, clock, calendar) -> ContextAssembler:
    return ContextAssembler(store, conn, clock, calendar)


def seed_bars(store: MarketStore, count: int = 50) -> None:
    """`count` 1m bars ending 10:04 (the last completed bar before the frozen 10:05 "now")."""
    last = datetime(2026, 6, 17, 10, 4, tzinfo=IST)
    bars = [
        Bar(
            symbol=SYMBOL,
            ts_minute=last - timedelta(minutes=count - 1 - i),
            open=Decimal("1400.00") + i,
            high=Decimal("1401.00") + i,
            low=Decimal("1399.00") + i,
            close=Decimal("1400.50") + i,
            volume=1000 + i,
        )
        for i in range(count)
    ]
    store.insert_bars_1m(bars)


def seed_daily_bars(store: MarketStore, symbol: str = SYMBOL, count: int = 60) -> None:
    """`count` consecutive-calendar-day daily bars ending YESTERDAY (2026-06-16), plus one for TODAY.

    Deliberately wider than the assembler's ~45-day lookback and deliberately including today's row:
    the tail must clip to the window (dropping the oldest bars) AND must never show today's forming
    session — the same "completed sessions only" bound ops/main.py's brk20 sweep reads on.
    Prices ramp with the index so the first/last rendered rows are literally assertable.
    """
    yesterday = TODAY - timedelta(days=1)
    bars = [
        DailyBar(
            symbol=symbol,
            d=yesterday - timedelta(days=count - 1 - i),
            open=Decimal("500.00") + i,
            high=Decimal("510.00") + i,
            low=Decimal("490.00") + i,
            close=Decimal("505.00") + i,
            volume=100_000 + i,
        )
        for i in range(count)
    ]
    bars.append(
        DailyBar(symbol=symbol, d=TODAY, open=Decimal("999.00"), high=Decimal("999.00"),
                 low=Decimal("999.00"), close=Decimal("999.00"), volume=999),
    )
    store.upsert_bars_1d(bars)


def seed_features(store: MarketStore) -> None:
    store.insert_feature_snapshot(
        "fs-1", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1, '{"atr14":12.5,"rvol":1.8}'
    )


def seed_catalyst(store: MarketStore) -> None:
    store.replace_catalyst_watchlist(
        TODAY,
        [{
            "symbol": SYMBOL,
            "grade": "context",
            "direction": "long",
            "event_type": "order_win",
            "materiality": 0.72,
            "source_domain_count": 3,
            "confirm_trigger": Decimal("1405.00"),
            "invalidation": Decimal("1388.00"),
        }],
    )
    as_of = datetime(2026, 6, 17, 8, 35, tzinfo=IST)
    store.upsert_sentiment_agg([
        {"scope": "symbol", "scope_key": SYMBOL, "as_of": as_of, "value": 0.42},
        {"scope": "market", "scope_key": "market", "as_of": as_of, "value": -0.11},
        {"scope": "sector", "scope_key": "Energy", "as_of": as_of, "value": 0.15},
    ])
    store.upsert_sector_map(TODAY, [{"symbol": SYMBOL, "sector": "Energy"}])


def seed_day_plan(
    conn,
    payload: str = '{"regime": "trending up", "no_trade_today": false}',
    created_at: str = "2026-06-17T08:50:00+05:30",
) -> None:
    conn.execute(
        "INSERT INTO day_plans (d, payload, created_at) VALUES (?,?,?)",
        (TODAY.isoformat(), payload, created_at),
    )


# --------------------------------------------------------------------------- byte-stability (D8)
def test_same_inputs_produce_the_same_digest(assembler, store, conn):
    seed_bars(store)
    seed_features(store)
    seed_catalyst(store)
    seed_day_plan(conn)

    first = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    second = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert first.inputs_digest == second.inputs_digest
    assert (first.system_prompt, first.stable_block, first.volatile_block) == (
        second.system_prompt, second.stable_block, second.volatile_block
    )
    assert len(first.inputs_digest) == 64          # sha256 hex


def test_volatile_change_leaves_the_cacheable_prefix_identical(assembler, store, conn):
    """D8: only the tail may move. A stable block that drifts with the candidate is a cache miss
    on every call, which is the whole cost model of §11 for the intraday analyst."""
    seed_bars(store)
    seed_day_plan(conn)

    a = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    b = assembler.for_signal(OTHER_CANDIDATE, **{**SIGNAL_KW, "max_qty_by_risk": 9})

    assert a.system_prompt == b.system_prompt
    assert a.stable_block == b.stable_block        # byte-identical prefix
    assert a.volatile_block != b.volatile_block
    assert a.inputs_digest != b.inputs_digest


def test_every_call_class_shares_the_intraday_system_prompt(assembler, store):
    """One agent, one prompt: signal/position-event/heartbeat differ only in their context blocks."""
    seed_bars(store)
    sig = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    pos = assembler.for_position_event({"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399")
    hb = assembler.for_heartbeat(regime_lines=["breadth negative"], open_positions_summary="none")

    assert sig.system_prompt == pos.system_prompt == hb.system_prompt == intraday.SYSTEM_PROMPT
    assert sig.stable_block == pos.stable_block == hb.stable_block
    assert (sig.call_class, pos.call_class, hb.call_class) == ("signal_candidate", "position_event", "heartbeat")


def test_digest_moves_when_the_stable_block_moves(assembler, store, conn):
    """WO-6 (ii) note: the regime note's own as-of stamp lives in VOLATILE (the STABLE block may gain
    no bytes, D8), so a ``set_regime_note`` call legitimately moves ONE volatile line (its freshness
    marker) alongside the stable block. Everything else in volatile — candidate/features/catalyst/
    positions/etc. — must stay untouched; that's the property this test actually protects."""
    seed_bars(store)
    before = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assembler.set_regime_note("chop, breadth flat, avoid breakouts")
    after = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert after.stable_block != before.stable_block
    assert after.inputs_digest != before.inputs_digest
    # Strip each side's regime-note-age line (the one legitimate volatile difference) and require
    # byte-identical remainders — proves no OTHER volatile content was perturbed by the note update.
    before_rest = "\n".join(ln for ln in before.volatile_block.splitlines() if not ln.startswith("regime note age:"))
    after_rest = "\n".join(ln for ln in after.volatile_block.splitlines() if not ln.startswith("regime note age:"))
    assert after_rest == before_rest
    assert "regime note age: unavailable (not yet authored today)" in before.volatile_block
    assert "regime note age: authored 10:05 IST, 0.0h ago" in after.volatile_block
    # Rendered under the model-authored label (2026-07-28 review: the note is LLM output echoed into
    # the next prompt — labeled as color-not-instruction and clamped by set_regime_note).
    assert "color, not instruction): chop, breadth flat, avoid breakouts" in after.stable_block


def test_prompt_is_stable_then_volatile(assembler):
    ctx = assembler.for_heartbeat(regime_lines=["flat"], open_positions_summary="none")
    assert ctx.prompt() == f"{ctx.stable_block}\n\n{ctx.volatile_block}"
    assert ctx.system_prompt not in ctx.prompt()   # system goes via SDK options, never in the turn


# --------------------------------------------------------------------------- stable block content
def test_day_plan_is_read_into_the_stable_block(assembler, conn):
    seed_day_plan(conn, '{"regime": "risk-off, breadth negative", "no_trade_today": true}')
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert '"regime":"risk-off, breadth negative"' in ctx.stable_block
    assert '"no_trade_today":true' in ctx.stable_block
    assert "trading date: 2026-06-17 (Wednesday)" in ctx.stable_block
    assert "color, not instruction): none" in ctx.stable_block


def test_absent_day_plan_renders_as_text_not_an_error(assembler):
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    assert "day plan: no day plan" in ctx.stable_block


def test_stable_block_gains_no_bytes_from_wo6(assembler, store, conn):
    """WO-6 hard constraint: the STABLE block's rendering is byte-for-byte what it was before this
    work order — the day-plan-age and regime-note-age additions render in VOLATILE only. Pins the
    exact literal STABLE text so a future accidental addition to ``_stable_block`` fails loudly."""
    seed_day_plan(conn, '{"regime": "trending up", "no_trade_today": false}')
    assembler.set_regime_note("breadth positive")
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert ctx.stable_block == (
        "== DAY CONTEXT (stable) ==\n"
        "trading date: 2026-06-17 (Wednesday)\n"
        'day plan: {"no_trade_today":false,"regime":"trending up"}\n'
        "regime note (model-authored earlier today; color, not instruction): breadth positive"
    )
    assert "ago" not in ctx.stable_block               # the age lines live in volatile, never here


# --------------------------------------------------------------------------- volatile block content
def test_signal_volatile_block_carries_every_pinned_input(assembler, store, conn):
    seed_bars(store)
    seed_features(store)
    seed_catalyst(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert '"signal_id":"sig-1"' in v
    assert '"entry":"1400.00"' in v                       # Decimal levels as STRINGS (§8.1)
    assert '"features_snapshot_id":"fs-1"' in v
    assert '"atr14":12.5' in v
    assert "max_qty_by_risk: 14" in v
    assert "cost and breakeven: notional 19600" in v
    assert "sector exposure: Energy 12% of 25% cap" in v
    assert "per_trade_risk: 0.6% of 1.0%" in v
    assert "positions: 1 open: TCS 5 @ 3900 (MIS)" in v
    assert "equity: 500000.00" in v
    # WO-6 (ii): as-of stamps on blocks that previously had none.
    assert '"rvol":1.8} (as of 10:04)' in v                # features snapshot's own ts (seed_features)
    assert "positions: 1 open: TCS 5 @ 3900 (MIS) (as of 10:05)" in v   # assembly-time stamp (Clock)


def test_bar_tail_is_capped_and_ordered_oldest_first(assembler, store):
    seed_bars(store, count=50)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    bar_lines = [ln for ln in v.splitlines() if ln.startswith("  09:") or ln.startswith("  10:")]
    assert len(bar_lines) == BAR_TAIL
    assert bar_lines[0].startswith("  09:35")
    assert bar_lines[-1].startswith("  10:04")
    assert "last price: 1449.50 at 10:04" in v            # precomputed, never the model's arithmetic


def test_missing_features_and_bars_render_unavailable(assembler, store):
    """D7 fail-to-zero: a thin context is still a legal call; a raising assembler is not."""
    bare = CANDIDATE.model_copy(update={"features_snapshot_id": None, "symbol": "NOBARS"})
    v = assembler.for_signal(bare, **SIGNAL_KW).volatile_block

    assert "features: unavailable" in v
    assert f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first): unavailable" in v
    assert "last price: unavailable" in v
    # WO-6 (i): no bars -> session stats unavailable, never fabricated; elapsed minutes still render
    # (it needs only Clock/NSECalendar, not bars, so D7 fail-to-zero applies to the price stats alone).
    assert "session: unavailable (no bars this session yet), 50m elapsed" in v


def test_catalyst_block_renders_watchlist_entry_and_sentiment(assembler, store):
    seed_bars(store)
    seed_catalyst(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert '"grade":"context"' in v
    assert '"event_type":"order_win"' in v
    assert '"materiality":0.72' in v
    assert '"confirm_trigger":"1405.00"' in v
    assert "sentiment symbol RELIANCE: +0.420" in v
    assert "sentiment sector Energy: +0.150" in v
    assert "sentiment market: -0.110" in v
    # WO-6 (ii): the catalyst block's as-of stamp is the sentiment digest's last run time (08:35,
    # seed_catalyst) — the only genuinely-timestamped input this block has.
    assert "catalyst (sentiment as of 08:35):" in v


def test_sentiment_unavailable_when_the_digest_has_not_run(assembler, store):
    """§5.4 failure mode: analyst down ⇒ no scores ⇒ the context says so (never a 0.0 standing in)."""
    seed_bars(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert "sentiment unavailable" in v
    assert "watchlist entry: none for this symbol today" in v


# --------------------------------------------------------------------------- WO-6: session + staleness
def test_session_aggregates_line(assembler, store):
    """WO-6 (i): day H/L, %-from-open, %-from-VWAP (typical-price VWAP, matching features.py's
    ``vwap_dist`` formula §4.3), session elapsed minutes. Hand-computable fixture: 3 round-number bars.

    day H = max(110,120,130) = 130; day L = min(90,95,100) = 90; open = bars[0].open = 100; last
    close = bars[-1].close = 120 -> from_open = 120/100-1 = +20.00%. VWAP (typical price (H+L+C)/3,
    weighted by volume) = (100*100 + 325/3*200 + 350/3*300) / 600 = (200000/3)/600 = 1000/9 ->
    from_vwap = 120/(1000/9)-1 = 0.08 exactly -> +8.00%. Session open 09:15, frozen "now" 10:05 (the
    tests/conftest.py FIXED_NOW) -> 50m elapsed.
    """
    bars = [
        Bar(symbol=SYMBOL, ts_minute=datetime(2026, 6, 17, 9, 15, tzinfo=IST),
            open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("100"), volume=100),
        Bar(symbol=SYMBOL, ts_minute=datetime(2026, 6, 17, 9, 16, tzinfo=IST),
            open=Decimal("100"), high=Decimal("120"), low=Decimal("95"), close=Decimal("110"), volume=200),
        Bar(symbol=SYMBOL, ts_minute=datetime(2026, 6, 17, 9, 17, tzinfo=IST),
            open=Decimal("110"), high=Decimal("130"), low=Decimal("100"), close=Decimal("120"), volume=300),
    ]
    store.insert_bars_1m(bars)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert "session: day H 130.00 / L 90.00, +20.00% from open, +8.00% from VWAP, 50m elapsed" in v


def test_day_plan_age_line_in_volatile(assembler, store, conn):
    """WO-6 (iii): explicit day-plan age line, VOLATILE only (STABLE stays byte-identical — see
    ``test_stable_block_gains_no_bytes_from_wo6``). 90 minutes authored-to-now = a clean 1.5h with no
    banker's-rounding ambiguity (unlike the fixture default 08:50, which is 75min -> 1.25h)."""
    seed_bars(store)
    seed_day_plan(conn, created_at="2026-06-17T08:35:00+05:30")
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert "day plan age: authored 08:35 IST, 1.5h ago" in v


def test_day_plan_age_unavailable_when_no_plan(assembler, store):
    seed_bars(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block
    assert "day plan age: unavailable (no day plan)" in v


def test_regime_note_age_unavailable_before_any_heartbeat(assembler):
    v = assembler.for_heartbeat(regime_lines=["flat"], open_positions_summary="none").volatile_block
    assert "regime note age: unavailable (not yet authored today)" in v


def test_day_plan_and_regime_note_age_shared_across_triggers(assembler, conn):
    """The staleness lines appear wherever the STABLE block (day plan + regime note) is shared —
    for_signal (covered above), for_position_event and for_heartbeat all read it (WO-6 ii/iii)."""
    seed_day_plan(conn, created_at="2026-06-17T08:35:00+05:30")
    assembler.set_regime_note("breadth negative")

    pos_v = assembler.for_position_event(
        {"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399"
    ).volatile_block
    hb_v = assembler.for_heartbeat(regime_lines=["flat"], open_positions_summary="none").volatile_block

    for v in (pos_v, hb_v):
        assert "day plan age: authored 08:35 IST, 1.5h ago" in v
        assert "regime note age: authored 10:05 IST, 0.0h ago" in v


# --------------------------------------------------------------------------- per-trigger restrictions
def test_position_event_context_states_the_output_restriction(assembler):
    ctx = assembler.for_position_event(
        {"position_id": "pos-9", "symbol": SYMBOL, "qty": 10},
        "stop_proximity",
        "price within 0.4 ATR of stop",
        ltp_line="last price: 1391.05 at 10:04",
    )
    v = ctx.volatile_block
    assert ctx.call_class == "position_event"
    assert "exit" in v and "modify-stop" in v and "no_action" in v
    assert "new entry is not a legal answer" in v
    assert '"position_id":"pos-9"' in v
    assert "event: stop_proximity" in v
    assert "detail: price within 0.4 ATR of stop" in v
    assert "last price: 1391.05 at 10:04" in v


def test_heartbeat_context_forbids_entries(assembler):
    ctx = assembler.for_heartbeat(
        regime_lines=["NIFTY -0.6%", "advance/decline 1:3"], open_positions_summary="none"
    )
    assert ctx.call_class == "heartbeat"
    assert "MUST be no_action" in ctx.volatile_block
    assert "Do not propose an entry" in ctx.volatile_block
    assert "  - NIFTY -0.6%" in ctx.volatile_block
    assert "positions: none (as of 10:05)" in ctx.volatile_block   # WO-6 (ii): assembly-time stamp


# --------------------------------------------------------------------------- planner (§5.3)
def test_planner_context_uses_the_preopen_prompt_and_carries_its_inputs(assembler):
    ctx = assembler.for_planner(
        TODAY,
        movers_lines=["TATASTEEL +4.1%"],
        gap_lines=["INFY gap -1.8%"],
        digest_lines=["3 originating clusters"],
        watchlist_lines=["RELIANCE originating order_win"],
        earnings_today=["HDFCBANK"],
        surveillance_changes=["XYZ added to ASM"],
        open_positions_summary="1 swing long in TCS",
        yesterday_review_summary="2 trades, +0.4R",
    )
    assert ctx.call_class == "schedule"
    assert ctx.system_prompt == preopen.SYSTEM_PROMPT
    assert ctx.stable_block == "== DAY CONTEXT (stable) ==\ntrading date: 2026-06-17 (Wednesday)"
    for expected in ("TATASTEEL +4.1%", "INFY gap -1.8%", "3 originating clusters",
                     "RELIANCE originating order_win", "HDFCBANK", "XYZ added to ASM",
                     "1 swing long in TCS", "2 trades, +0.4R"):
        assert expected in ctx.volatile_block
    assert "binding levels are the scanner's" in ctx.volatile_block


def test_planner_empty_sections_render_as_none(assembler):
    ctx = assembler.for_planner(
        TODAY, movers_lines=[], gap_lines=[], digest_lines=[], watchlist_lines=[],
        earnings_today=[], surveillance_changes=[], open_positions_summary="none",
        yesterday_review_summary="none",
    )
    assert "earnings today: none" in ctx.volatile_block


# --------------------------------------------------------------------------- news batch (§5.4)
def _clusters(n: int) -> list[dict]:
    return [
        {
            "cluster_id": f"c{i}",
            "representative": f"Headline number {i}",
            "source_domains": ["a.com", "b.com"],
            "first_seen": datetime(2026, 6, 17, 7, 30, tzinfo=IST),
        }
        for i in range(n)
    ]


def test_news_batch_renders_numbered_clusters_and_the_theme_vocabulary(assembler):
    ctx = assembler.for_news_batch(_clusters(3), ["ev_mobility", "defence"])

    assert ctx.call_class == "batch"
    assert ctx.system_prompt == news_analyst.SYSTEM_PROMPT
    assert "1. cluster_id=c0 | first seen 2026-06-17 | 2 source domain(s)" in ctx.volatile_block
    assert "   headline: Headline number 2" in ctx.volatile_block
    assert "  - ev_mobility" in ctx.volatile_block
    # No date in the stable prefix: this agent runs across the pre-open boundary (D8).
    assert "2026" not in ctx.stable_block


def test_news_batch_rejects_more_than_thirty_clusters(assembler):
    assert len(assembler.for_news_batch(_clusters(30), []).volatile_block) > 0
    with pytest.raises(ValueError, match="30-cluster cap"):
        assembler.for_news_batch(_clusters(31), [])


# ------------------------------------------------- swing daily evidence (2026-08-17 live finding)
# The analyst declined a 0.93 brk20 candidate (LGEINDIA) for "bar_count 0, no 1m bars this session".
# brk20/ins sweep the FULL eligible universe; 1m bars exist only for the tick watchlist. So every
# sub-watchlist swing candidate was judged on nothing at all. These tests pin the fix: swing
# candidates carry a daily tail, an empty 1m tail is labelled structural, and intraday is untouched.

UNWATCHED = "LGEINDIA"            # outside the tick watchlist: daily bars exist, 1m bars never will

SWING_CANDIDATE = SignalCandidate(
    signal_id="sig-brk20-1",
    strategy_id="brk20",
    symbol=UNWATCHED,
    side="BUY",
    style="swing",
    raw_levels=RawLevels(entry=Decimal("564.00"), stop=Decimal("530.00")),
    score=0.93,
)

WATCHED_SWING_CANDIDATE = SWING_CANDIDATE.model_copy(
    update={"signal_id": "sig-rsi2-1", "strategy_id": "rsi2", "symbol": SYMBOL}
)

# seed_daily_bars ramps prices by index off 2026-06-16. The ~45-calendar-day window opens 2026-05-03,
# so the 20-row tail is 2026-05-28 .. 2026-06-16 — both ends asserted literally below.
DAILY_HEADER = (
    "bars_1d last 20 (date o h l c vol, oldest first; completed sessions through 2026-06-16):"
)
DAILY_FIRST_ROW = "  2026-05-28 540.00 550.00 530.00 545.00 100040"
DAILY_LAST_ROW = "  2026-06-16 559.00 569.00 549.00 564.00 100059"
STRUCTURAL_MARKER = "STRUCTURAL ABSENCE, NOT A DATA FAILURE"


def test_unwatched_swing_candidate_gets_a_structural_absence_note_and_a_daily_tail(assembler, store):
    """(a) The exact live failure: swing candidate, zero 1m bars, full daily history available."""
    seed_daily_bars(store, symbol=UNWATCHED)
    v = assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block
    lines = v.splitlines()

    # The 1m tail still says unavailable — but is no longer bare.
    assert f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first): unavailable" in lines
    assert f"  NOTE - {STRUCTURAL_MARKER}: {UNWATCHED} is outside the intraday tick" in lines
    assert "  watchlist, so it has no 1m bars this session and never will. The full-universe batch" in lines
    assert "  rules (brk20, ins) deliberately originate for ANY eligible symbol, watchlist or not." in lines
    assert "  Judge this swing candidate on the daily bars below - that is the series its rule fired" in lines
    assert "  on. An empty 1m tail here is neither missing evidence nor a feed outage." in lines

    # 20 daily rows, oldest first, both ends literal.
    assert DAILY_HEADER in lines
    head = lines.index(DAILY_HEADER)
    rows = lines[head + 1: head + 1 + 20]
    assert len(rows) == 20
    assert rows[0] == DAILY_FIRST_ROW
    assert rows[-1] == DAILY_LAST_ROW
    # ascending: ISO dates sort lexically (strict=False - the offset pairing is one short by design)
    assert all(a < b for a, b in zip(rows, rows[1:], strict=False))

    # Today's forming bar (seeded at 999) is never shown — completed sessions only, ending yesterday.
    assert "2026-06-17" not in "\n".join(rows)
    assert "999" not in "\n".join(rows)
    # Window bound: the oldest seeded bar (2026-04-18) is outside the ~45-day lookback.
    assert "2026-04-18" not in v

    # Ordering: 1m line, then the absence note, then the daily header, then the session line.
    bars_1m_at = next(i for i, ln in enumerate(lines) if ln.startswith("bars_1m last"))
    note_at = next(i for i, ln in enumerate(lines) if STRUCTURAL_MARKER in ln)
    session_at = next(i for i, ln in enumerate(lines) if ln.startswith("session:"))
    assert bars_1m_at < note_at < head < session_at


def test_swing_candidate_without_daily_history_renders_text_not_an_error(assembler, store):
    """(b) D7 fail-to-zero: no 1m bars AND no daily bars is a thin call, never a raised one."""
    v = assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block

    assert f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first): unavailable" in v
    assert STRUCTURAL_MARKER in v
    assert (
        "bars_1d last 20 (date o h l c vol, oldest first; completed sessions through 2026-06-16): "
        "unavailable (no completed daily bars in the last 45 calendar days)"
    ) in v


def test_swing_daily_read_failure_renders_unavailable_and_never_raises(assembler, store, monkeypatch):
    """D7: the daily read is wrapped — a store blow-up costs the call its daily tail, not the call."""
    def boom(*_args, **_kwargs):
        raise RuntimeError("duckdb is having a day")

    monkeypatch.setattr(store, "get_bars_1d", boom)
    v = assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block

    assert f"{DAILY_HEADER} unavailable (daily-bar read failed)" in v


def test_watched_swing_candidate_carries_both_tails_and_no_structural_wording(assembler, store):
    """(c) A swing candidate that IS on the tick watchlist: intraday tail AND daily tail, no note."""
    seed_bars(store, count=5)
    seed_daily_bars(store, symbol=SYMBOL)
    v = assembler.for_signal(WATCHED_SWING_CANDIDATE, **SIGNAL_KW).volatile_block
    lines = v.splitlines()

    assert f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first):" in lines
    assert "  10:04 1404.00 1405.00 1403.00 1404.50 1004" in lines      # 1m tail present
    assert DAILY_HEADER in lines
    assert DAILY_FIRST_ROW in lines and DAILY_LAST_ROW in lines          # daily tail present
    assert STRUCTURAL_MARKER not in v                                    # bars exist: nothing absent
    assert "outside the intraday tick" not in v


# ------------------------------------------------------------------ (d) intraday is byte-untouched
#: The intraday volatile block captured by running ``_intraday_pin_block`` at HEAD (e8ff7db), BEFORE
#: the daily-tail change. The fixture seeds daily bars deliberately: an intraday candidate must not
#: render them even when they are sitting right there in the store.
#:
#: WO-20a (2026-08-20) added exactly ONE block to this pin — the two-line ``orb`` strategy contract,
#: immediately after the candidate line. Nothing else about the intraday rendering moved: the
#: rel_volume legend needs a ``rel_volume`` key this fixture does not carry, and the saturation label
#: needs a railed sentiment value this fixture does not carry either.
#:
#: WO-21 (2026-08-21) left it byte-identical for the same reason — the legend now names
#: ``rel_volume_tod`` too, but this fixture's features carry neither key, so it renders nothing.
INTRADAY_VOLATILE_PIN = """== MARKET STATE (volatile) ==
candidate: {"catalyst_ref":null,"features_snapshot_id":"fs-1","raw_levels":{"entry":"1400.00","stop":"1390.00","target":null},"score":0.7,"side":"BUY","signal_id":"sig-1","strategy_id":"orb","style":"intraday","symbol":"RELIANCE"}
strategy contract (orb):
  class: intraday (MIS), same-day squareoff; the rule supplies a price target. The intraday frame FULLY applies: participation, VWAP, acceptance above the range, the day plan's read — judge exactly as an intraday breakout trade.
  evidence status: exploratory — repeatedly tested gross-negative at retail costs in this book; demand exceptional quality.
features: {"atr14":12.5,"rvol":1.8} (as of 10:04)
bars_1m last 30 (time o h l c vol, oldest first):
  10:00 1400.00 1401.00 1399.00 1400.50 1000
  10:01 1401.00 1402.00 1400.00 1401.50 1001
  10:02 1402.00 1403.00 1401.00 1402.50 1002
  10:03 1403.00 1404.00 1402.00 1403.50 1003
  10:04 1404.00 1405.00 1403.00 1404.50 1004
session: day H 1405.00 / L 1399.00, +0.32% from open, +0.17% from VWAP, 50m elapsed
catalyst (sentiment as of 08:35):
  watchlist entry: {"confirm_trigger":"1405.00","direction":"long","event_type":"order_win","grade":"context","invalidation":"1388.00","materiality":0.72,"source_domain_count":3}
  sentiment symbol RELIANCE: +0.420
  sentiment sector Energy: +0.150
  sentiment market: -0.110
positions: 1 open: TCS 5 @ 3900 (MIS) (as of 10:05)
sector exposure: Energy 12% of 25% cap
cost and breakeven: notional 19600, total cost 41.20, breakeven 0.21%, edge multiple 3.1
equity: 500000.00
max_qty_by_risk: 14
risk headroom (informational - the gate re-checks everything):
  - per_trade_risk: 0.6% of 1.0%
  - daily_loss: -0.2% of -3.0%
last price: 1404.50 at 10:04 (last completed 1m bar; context assembled at 2026-06-17T10:05:00+05:30)
day plan age: authored 08:50 IST, 1.2h ago
regime note age: unavailable (not yet authored today)"""


def _intraday_pin_block(assembler, store, conn) -> str:
    """The exact fixture whose rendering is pinned byte-for-byte by ``INTRADAY_VOLATILE_PIN``."""
    seed_bars(store, count=5)
    seed_features(store)
    seed_catalyst(store)
    seed_daily_bars(store)
    seed_day_plan(conn)
    return assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block


def test_intraday_volatile_block_is_byte_identical_to_pre_change(assembler, store, conn):
    """(d) The orb path pays nothing for the swing fix — not a byte, not a token, not a query."""
    assert _intraday_pin_block(assembler, store, conn) == INTRADAY_VOLATILE_PIN


def test_structural_wording_never_appears_for_an_intraday_candidate(assembler, store, conn):
    """Even with zero 1m bars: an intraday candidate with no bars IS a data problem, not a design one."""
    bare = CANDIDATE.model_copy(update={"symbol": UNWATCHED})
    seed_daily_bars(store, symbol=UNWATCHED)
    v = assembler.for_signal(bare, **SIGNAL_KW).volatile_block

    assert f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first): unavailable" in v
    assert STRUCTURAL_MARKER not in v
    assert "bars_1d" not in v                       # style gates the read, not symbol coverage
    # `position` style (trend) is untouched too.
    pos_v = assembler.for_signal(
        bare.model_copy(update={"style": "position", "strategy_id": "trend"}), **SIGNAL_KW
    ).volatile_block
    assert "bars_1d" not in pos_v and STRUCTURAL_MARKER not in pos_v


def test_daily_tail_token_delta_is_bounded(assembler, store, capsys):
    """(e) Token cost of the addition, chars/4 convention — reported, and capped so it cannot creep.

    Measured as the same candidate rendered swing vs intraday; the two blocks differ only by the new
    lines plus the 3-char ``"swing"``/``"intraday"`` literal in the candidate JSON.
    """
    seed_daily_bars(store, symbol=UNWATCHED)
    swing_v = assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block
    intraday_v = assembler.for_signal(
        SWING_CANDIDATE.model_copy(update={"style": "intraday"}), **SIGNAL_KW
    ).volatile_block

    added_chars = len(swing_v) - len(intraday_v) + 3        # +3: "intraday" -> "swing" in the JSON
    est_tokens = added_chars / 4
    with capsys.disabled():
        print(f"\n[token delta] worst case (absence note + 20 daily rows): "
              f"{added_chars} chars ~= {est_tokens:.0f} tokens (chars/4)")

    assert est_tokens < 400            # worst case; a watched swing candidate pays only the tail


# ============================================ WO-20 (2026-08-20): the contract frame + input honesty
# The funnel autopsy: 63/63 analyst evaluations ended no_action, zero proposals ever created, the
# §7.1 gate never invoked. Two inputs lied (rel_volume's denominator, sentiment_agg's rail) and one
# intraday rubric was applied to seven strategies. These tests pin the render-time half of the fix.

INS_CANDIDATE = SignalCandidate(
    signal_id="sig-ins-1",
    strategy_id="ins",
    symbol=UNWATCHED,
    side="BUY",
    style="swing",
    raw_levels=RawLevels(entry=Decimal("564.00"), stop=Decimal("535.80")),   # target=None BY DESIGN
    score=0.62,
)


def test_every_signal_context_carries_its_strategy_contract(assembler, store):
    """(a) The block exists at all, and sits directly under the candidate line it frames."""
    seed_bars(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block
    lines = v.splitlines()

    assert "strategy contract (orb):" in lines
    assert lines.index("strategy contract (orb):") == lines.index(
        next(ln for ln in lines if ln.startswith("candidate: "))) + 1
    assert "  class: intraday (MIS), same-day squareoff" in v
    assert "  evidence status: exploratory" in v


def test_the_ins_contract_reaches_the_prompt_verbatim(assembler, store):
    """(b) The 2026-08-19 HCLTECH decline, answered in the context itself.

    ``ins`` is the only validated edge; its crossings cluster in drawdowns, so a red tape and a
    below-VWAP price ARE the validated entry population, and its ``target=None`` is design rather
    than a missing reward. All three statements must arrive in the block the analyst reads.
    """
    seed_daily_bars(store, symbol=UNWATCHED)
    v = assembler.for_signal(INS_CANDIDATE, **SIGNAL_KW).volatile_block

    assert "strategy contract (ins):" in v.splitlines()
    assert "EXPECTED entry population" in v
    assert "ONLY validated edge" in v
    assert "target=None BY DESIGN" in v
    assert "never invent one" in v


def test_an_unregistered_strategy_renders_the_unknown_frame_and_warns(assembler, store, caplog):
    """(c) D7: a scanner shipped without a contract costs the call its frame, never the call — and
    the omission is a WARNING line, because silently thinner prompts are how this class of defect
    survives for weeks."""
    import logging

    bare = CANDIDATE.model_copy(update={"strategy_id": "newleg"})
    with caplog.at_level(logging.WARNING, logger="engine.intelligence.context"):
        v = assembler.for_signal(bare, **SIGNAL_KW).volatile_block

    assert "strategy contract (newleg):" in v.splitlines()
    assert "  UNKNOWN (unregistered strategy_id — evaluate conservatively and flag it)" in v
    records = [r for r in caplog.records if r.getMessage() == "strategy_contract_missing"]
    assert records and records[0].strategy_id == "newleg"


# ------------------------------------------------------------------ WO-20c (E2): the sentiment rail
def test_a_railed_sentiment_value_is_labelled_saturated(assembler):
    """``sentiment_agg`` is a CLIPPED SUM: a handful of same-direction headlines reaches ±1.0. The
    analyst read the rail as "the floor of the scale" in 46 of 63 declines, so the semantics are
    stated where the number is. A row with NO WO-21 measures keeps this prose verbatim."""
    line = assembler._sentiment_line("symbol RELIANCE", {"value": -1.0})

    assert "SATURATED" in line
    assert "net negative" in line
    assert "clipped decay-weighted SUM" in line
    assert line.startswith("  sentiment symbol RELIANCE: -1.000 (SATURATED:")

    positive = assembler._sentiment_line("market", {"value": 1.0})
    assert "net positive" in positive


def test_an_unrailed_sentiment_value_renders_exactly_as_before(assembler):
    """The label is for the RAIL only: a normal reading must not grow an explanation it does not
    need (D8 — every extra byte here is on the volatile path of every call). Measures present or
    absent makes no difference off the rail."""
    assert assembler._sentiment_line("market", {"value": 0.5}) == "  sentiment market: +0.500"
    assert assembler._sentiment_line("market", {"value": -0.998}) == "  sentiment market: -0.998"
    assert assembler._sentiment_line("market", None) == "  sentiment market: unavailable"
    measured = {"value": 0.5, "raw_sum": 0.5, "n_clusters": 3}
    assert assembler._sentiment_line("market", measured) == "  sentiment market: +0.500"


# ------------------------------------------------------- WO-21 (2026-08-21): the measured rail
def test_a_railed_sentiment_value_reports_its_measured_saturation(assembler):
    """With the digest's own ``raw_sum``/``n_clusters``, the rail stops being an annotation and
    becomes a reading: how far past the clip the flow ran, and over how many clusters."""
    line = assembler._sentiment_line(
        "market", {"value": -1.0, "raw_sum": -2.3149, "n_clusters": 9}
    )

    assert line == (
        "  sentiment market: -1.000 (clipped SUM saturated: raw -2.31 across 9 clusters, "
        "mean -0.257 per cluster — read as net negative headline flow, not extremity)"
    )
    assert "SATURATED:" not in line          # the measurement REPLACES the WO-20 prose

    positive = assembler._sentiment_line(
        "symbol RELIANCE", {"value": 1.0, "raw_sum": 1.0004, "n_clusters": 1}
    )
    assert "raw +1.00 across 1 cluster, mean +1.000 per cluster —" in positive   # singular, signed
    assert "net positive" in positive


def test_a_railed_sum_over_a_large_corpus_reports_its_near_zero_mean(assembler):
    """2026-09-03: the market row read -1.000 from raw -9.48 across 759 clusters — about -0.01 per
    cluster, i.e. neutral flow — and both the planner and the analyst read the rail as a risk-off
    regime all day. The per-cluster mean is the number that says otherwise, so it is printed."""
    line = assembler._sentiment_line("market", {"value": -1.0, "raw_sum": -9.48, "n_clusters": 759})
    assert "raw -9.48 across 759 clusters, mean -0.012 per cluster" in line


def test_a_half_measured_rail_falls_back_to_the_wo20_prose(assembler):
    """Either measure missing ⇒ no measurement: an unmeasured rail must never borrow the credibility
    of a measured one by printing half of it."""
    for partial in ({"value": -1.0, "raw_sum": -2.31}, {"value": -1.0, "n_clusters": 9}):
        line = assembler._sentiment_line("market", partial)
        assert "SATURATED:" in line and "raw " not in line


def test_the_catalyst_block_renders_the_measured_rail_end_to_end(assembler, store):
    """The seam this WO threads: ``_catalyst_text`` must pass the FULL store row through, not the
    bare value — otherwise the measures never reach the render."""
    as_of = datetime(2026, 6, 17, 8, 35, tzinfo=IST)
    store.upsert_sentiment_agg([
        {"scope": "market", "scope_key": "market", "as_of": as_of, "value": -1.0,
         "raw_sum": -2.31, "n_clusters": 9},
    ])
    text = assembler._catalyst_text(SYMBOL, TODAY)

    assert "  sentiment market: -1.000 (clipped SUM saturated: raw -2.31 across 9 clusters" in text


# --------------------------------------------------------- WO-20c (E1): the rel_volume denominator
def test_rel_volume_carries_its_full_day_denominator(assembler, store):
    """cumulative session volume ÷ 20d median FULL-DAY volume, NOT time-adjusted — so 0.03 at 09:20
    is an ordinary tape, not a dead one. Phantom thinness was cited in 44 of the 63 declines."""
    store.insert_feature_snapshot(
        "fs-rv", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1,
        '{"atr14":12.5,"rel_volume":0.03}',
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-rv"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert "NOT time-of-day adjusted" in v
    assert "20d median FULL-DAY volume" in v
    assert "  note: rel_volume = cumulative session volume / 20d median FULL-DAY volume" in v


# ------------------------------------------------- WO-21: the corrected number ships beside it
def test_both_relative_volume_keys_are_explained(assembler, store):
    """``rel_volume_tod`` is the pace number (1.0 = typical for this time of day) and ``rel_volume``
    is the legacy full-day ratio. Two ratios named alike on one page ⇒ the legend names BOTH
    denominators, and points at the one to read."""
    store.insert_feature_snapshot(
        "fs-rvt", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1,
        '{"atr14":12.5,"rel_volume":0.03,"rel_volume_tod":1.12}',
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-rvt"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert (
        "  note: rel_volume_tod = cumulative session volume / the 20d MEDIAN cumulative volume "
        "at the SAME elapsed time" in v
    )
    assert "1.0 = a typical participation pace" in v and "read this one" in v
    assert "  note: rel_volume = cumulative session volume / 20d median FULL-DAY volume" in v
    assert "NOT time-of-day adjusted" in v


def test_the_legend_renders_when_only_the_tod_key_carries_a_number(assembler, store):
    """Either key alone earns the legend: a lone ``rel_volume_tod`` still needs its denominator
    named, and a null one beside it is itself information (too little 1m history to judge pace)."""
    store.insert_feature_snapshot(
        "fs-tod-only", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1,
        '{"atr14":12.5,"rel_volume":null,"rel_volume_tod":0.87}',
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-tod-only"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert "note: rel_volume_tod = " in v and "note: rel_volume = " in v


@pytest.mark.parametrize(
    "features_json",
    [
        '{"atr14":12.5,"rvol":1.8}',
        '{"atr14":12.5,"rel_volume":null}',
        '{"atr14":12.5,"rel_volume":null,"rel_volume_tod":null}',
    ],
)
def test_no_rel_volume_legend_when_the_feature_is_absent_or_null(assembler, store, features_json):
    """The legend annotates a number that is THERE. Absent or null ⇒ not a byte spent."""
    store.insert_feature_snapshot(
        "fs-norv", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1, features_json,
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-norv"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert "NOT time-of-day adjusted" not in v
    assert "note: rel_volume" not in v
