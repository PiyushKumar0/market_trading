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
from engine.marketdata.store import MarketStore
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
