"""ContextAssembler: byte-stable cacheable prefix, digest sensitivity, fail-to-zero rendering,
and per-trigger output restrictions."""

from __future__ import annotations

import gzip
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.intelligence.agents import intraday, news_analyst, preopen
from engine.intelligence.context import BAR_TAIL, PROMPT_TEMPLATE_VERSION, ContextAssembler
from engine.intelligence.schemas import EXIT_REASON_CODES
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.regime import INDEX_SYMBOL
from engine.strategy.types import RawLevels, SignalCandidate

# conftest's frozen clock: Wed 2026-06-17 10:05 IST, a real trading day.
TODAY = date(2026, 6, 17)
PREV = date(2026, 6, 16)
SYMBOL = "RELIANCE"
GOLDEN = Path(__file__).parent / "golden" / "for_signal_template.txt"

#: The regime line with no index bars and no EW rows seeded.
NO_DATA_REGIME_LINE = (
    "index regime: unavailable (no NIFTY 50 bar for 2026-06-16); "
    "core equal-weight 20-session return: unavailable (no return for 2026-06-16)"
)

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

    Wider than the assembler's ~45-day lookback and including today's row: the tail must clip to
    the window and never show today's forming session. Prices ramp with the index.
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


def _sessions(calendar: NSECalendar, end: date, n: int) -> list[date]:
    """``n`` trading days ending at ``end``, oldest first."""
    days = [end]
    while len(days) < n:
        days.append(calendar.previous_trading_day(days[-1]))
    return days[::-1]


def seed_index(store: MarketStore, calendar: NSECalendar, closes: list[int], end: date = PREV) -> None:
    store.upsert_bars_1d([
        DailyBar(symbol=INDEX_SYMBOL, d=x, open=Decimal(c), high=Decimal(c), low=Decimal(c),
                 close=Decimal(c), volume=0)
        for x, c in zip(_sessions(calendar, end, len(closes)), closes, strict=True)
    ])


def seed_ew(conn, calendar: NSECalendar, rets: list[float | None], end: date = PREV) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO universe_ew_returns (d, ret, n) VALUES (?, ?, 500)",
        [(x.isoformat(), r) for x, r in zip(_sessions(calendar, end, len(rets)), rets, strict=True)],
    )


def _boom(*_args, **_kwargs):
    raise RuntimeError("duckdb is having a day")


# SMA50 is 24500 at the last close in all three; 20 sessions earlier it is 24460 (UP) or 24540 (DOWN).
UP = [24400] * 25 + [24500] * 48 + [23900, 25100]
DOWN = [24600] * 25 + [24500] * 48 + [24900, 24100]
AT = [24500] * 75
EW_OK = [0.0] * 19 + [-0.0342]

UP_LINE = (
    "index regime (deterministic, completed sessions through 2026-06-16): NIFTY 50 close 25100.00 "
    "above SMA50 24500.00; SMA50 rising over 20 sessions"
)
EW_LINE = "; core equal-weight return over the 20 sessions through 2026-06-16: -3.42%"


# --------------------------------------------------------------------------- byte-stability
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
    """Only the tail may move; a stable block that drifts with the candidate is a cache miss every call."""
    seed_bars(store)
    seed_day_plan(conn)

    a = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    b = assembler.for_signal(OTHER_CANDIDATE, **{**SIGNAL_KW, "max_qty_by_risk": 9})

    assert a.system_prompt == b.system_prompt
    assert a.stable_block == b.stable_block        # byte-identical prefix
    assert a.volatile_block != b.volatile_block
    assert a.inputs_digest != b.inputs_digest


def test_every_call_class_shares_the_intraday_system_prompt(assembler, store):
    """One agent, one prompt: signal/position-event differ only in their volatile blocks."""
    seed_bars(store)
    sig = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    pos = assembler.for_position_event({"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399")

    assert sig.system_prompt == pos.system_prompt == intraday.SYSTEM_PROMPT
    assert sig.stable_block == pos.stable_block
    assert (sig.call_class, pos.call_class) == ("signal_candidate", "position_event")


def test_digest_moves_when_the_stable_block_moves(assembler, store, calendar):
    seed_bars(store)
    before = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    seed_index(store, calendar, UP)
    after = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert after.stable_block != before.stable_block
    assert after.inputs_digest != before.inputs_digest
    assert after.volatile_block == before.volatile_block


def test_prompt_is_stable_then_volatile(assembler):
    ctx = assembler.for_position_event({"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399")
    assert ctx.prompt() == f"{ctx.stable_block}\n\n{ctx.volatile_block}"
    assert ctx.system_prompt not in ctx.prompt()   # system goes via SDK options, never in the turn


# --------------------------------------------------------------------------- stable block content
def test_day_plan_is_read_into_the_stable_block(assembler, conn):
    seed_day_plan(conn, '{"regime": "risk-off, breadth negative", "no_trade_today": true}')
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert '"regime":"risk-off, breadth negative"' in ctx.stable_block
    assert '"no_trade_today":true' in ctx.stable_block
    assert "trading date: 2026-06-17 (Wednesday)" in ctx.stable_block


def test_stable_block_gains_no_bytes_from_the_plan_age_line(assembler, store, conn):
    """The day-plan-age line renders in VOLATILE only; pins the exact STABLE text."""
    seed_day_plan(conn, '{"regime": "trending up", "no_trade_today": false}')
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)

    assert ctx.stable_block == (
        "== DAY CONTEXT (stable) ==\n"
        "trading date: 2026-06-17 (Wednesday)\n"
        'day plan: {"no_trade_today":false,"regime":"trending up"}\n'
        f"{NO_DATA_REGIME_LINE}"
    )
    assert "ago" not in ctx.stable_block               # the age lines live in volatile, never here


# --------------------------------------------------------------------------- the regime line
@pytest.mark.parametrize(
    ("closes", "end", "rets", "expected", "warnings"),
    [
        (UP, PREV, EW_OK, UP_LINE + EW_LINE, []),
        (DOWN, PREV, EW_OK,
         "index regime (deterministic, completed sessions through 2026-06-16): NIFTY 50 close 24100.00 "
         "below SMA50 24500.00; SMA50 not rising over 20 sessions" + EW_LINE, []),
        (AT, PREV, EW_OK,
         "index regime (deterministic, completed sessions through 2026-06-16): NIFTY 50 close 24500.00 "
         "at SMA50 24500.00; SMA50 not rising over 20 sessions" + EW_LINE, []),
        (UP, date(2026, 6, 15), EW_OK,
         "index regime: unavailable (no NIFTY 50 bar for 2026-06-16)" + EW_LINE, []),
        ([24500] * 69, PREV, EW_OK, "index regime: unavailable (69 of 70 NIFTY 50 sessions)" + EW_LINE, []),
        (UP, PREV, [0.0] * 18 + [None, -0.0342],
         UP_LINE + "; core equal-weight 20-session return: unavailable (19 of 20 sessions)", []),
        (UP, PREV, [0.0] * 19 + [0.0123],
         UP_LINE + "; core equal-weight return over the 20 sessions through 2026-06-16: +1.23%", []),
        (None, PREV, EW_OK, "index regime: unavailable (index read failed)" + EW_LINE,
         ["index_regime_unavailable"]),
        (UP, PREV, None, UP_LINE + "; core equal-weight 20-session return: unavailable (read failed)",
         ["regime_ew_unavailable"]),
    ],
    ids=["up", "down", "at", "bar_missing", "short_history", "ew_short", "ew_positive", "read_failed",
         "ew_read_failed"],
)
def test_regime_line_renderings(
    assembler, store, conn, calendar, monkeypatch, caplog, closes, end, rets, expected, warnings
):
    if closes is None:
        monkeypatch.setattr(store, "get_bars_1d_frame", _boom)
    else:
        seed_index(store, calendar, closes, end)
    if rets is None:
        conn.execute("DROP TABLE universe_ew_returns")
    else:
        seed_ew(conn, calendar, rets)

    with caplog.at_level(logging.WARNING, logger="engine.intelligence.context"):
        assert assembler._regime_text(TODAY) == expected
    assert [r.getMessage() for r in caplog.records] == warnings


def test_index_closes_with_a_gap_render_unavailable(assembler, store, calendar, monkeypatch):
    seed_index(store, calendar, UP)
    read = store.get_bars_1d_frame

    def with_gap(*args):
        frame = read(*args)
        frame.iloc[-5, frame.columns.get_loc("close")] = float("nan")
        return frame

    monkeypatch.setattr(store, "get_bars_1d_frame", with_gap)
    assert assembler._regime_text(TODAY).startswith("index regime: unavailable (NIFTY 50 closes have gaps);")


def test_index_read_failure_warns_once_per_day(assembler, store, monkeypatch, caplog):
    monkeypatch.setattr(store, "get_bars_1d_frame", _boom)
    with caplog.at_level(logging.WARNING, logger="engine.intelligence.context"):
        assembler._regime_text(TODAY)
        assembler._regime_text(TODAY)

    assert [r.getMessage() for r in caplog.records] == ["index_regime_unavailable"]


def test_ew_line_stale_after_missed_evening_is_unavailable(assembler, store, conn, calendar):
    seed_index(store, calendar, UP)
    seed_ew(conn, calendar, EW_OK, end=date(2026, 6, 15))

    assert assembler._regime_text(TODAY) == (
        UP_LINE + "; core equal-weight 20-session return: unavailable (no return for 2026-06-16)"
    )


def test_stable_block_changes_only_when_inputs_advance(assembler, store, conn, calendar):
    """A corrected close under the same last date keeps the prefix byte-identical; a newly written EW row moves it once."""
    seed_index(store, calendar, UP)
    seed_ew(conn, calendar, [0.0] * 20, end=date(2026, 6, 15))
    first = assembler.for_signal(CANDIDATE, **SIGNAL_KW).stable_block

    seed_index(store, calendar, [25200])
    assert assembler.for_position_event(
        {"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399"
    ).stable_block == first

    seed_ew(conn, calendar, [-0.0342])
    second = assembler.for_signal(CANDIDATE, **SIGNAL_KW).stable_block
    assert second != first and second.endswith(EW_LINE)
    assert assembler.for_signal(CANDIDATE, **SIGNAL_KW).stable_block == second


# --------------------------------------------------------------------------- volatile block content
def test_signal_volatile_block_carries_every_pinned_input(assembler, store, conn):
    seed_bars(store)
    seed_features(store)
    seed_catalyst(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert '"signal_id":"sig-1"' in v
    assert '"entry":"1400.00"' in v                       # Decimal levels as STRINGS
    assert '"features_snapshot_id":"fs-1"' in v
    assert "max_qty_by_risk: 14" in v
    assert "cost and breakeven: notional 19600" in v
    assert "sector exposure: Energy 12% of 25% cap" in v
    assert "equity: 500000.00" in v
    # An assembly-time as-of stamp (Clock) on a block that has none of its own.
    assert "positions: 1 open: TCS 5 @ 3900 (MIS) (as of 10:05)" in v


def test_bar_tail_is_capped_and_ordered_oldest_first(assembler, store):
    seed_bars(store, count=50)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    bar_lines = [ln for ln in v.splitlines() if ln.startswith("  09:") or ln.startswith("  10:")]
    assert len(bar_lines) == BAR_TAIL
    assert bar_lines[0].startswith("  09:35")
    assert bar_lines[-1].startswith("  10:04")
    assert "last price: 1449.50 at 10:04" in v            # precomputed, never the model's arithmetic


def test_catalyst_block_renders_watchlist_entry_and_sentiment(assembler, store):
    seed_bars(store)
    seed_catalyst(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert '"grade":"context"' in v
    assert '"event_type":"order_win"' in v
    assert '"materiality":0.72' in v
    assert "confirm_trigger" not in v and "invalidation" not in v   # retired v1 levels stay out of prompts
    # as-of is the sentiment digest's last run time (08:35, seed_catalyst).
    assert "catalyst (sentiment as of 08:35):" in v


# --------------------------------------------------------------------------- session + staleness
def test_session_aggregates_line(assembler, store):
    """Day H/L, %-from-open, %-from-VWAP (typical-price VWAP, as features.py's ``vwap_dist``), elapsed minutes.

    Fixture arithmetic: from_open = 120/100-1 = +20%; VWAP = (100*100 + 325/3*200 + 350/3*300)/600
    = 1000/9, so from_vwap = 120/(1000/9)-1 = +8%; open 09:15 to frozen now 10:05 = 50m.
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
    """Day-plan age renders in VOLATILE only. 90 minutes gives a clean 1.5h (the default 08:50 is 1.25h, a rounding case)."""
    seed_bars(store)
    seed_day_plan(conn, created_at="2026-06-17T08:35:00+05:30")
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block

    assert "day plan age: authored 08:35 IST, 1.5h ago" in v


def test_day_plan_age_unavailable_when_no_plan(assembler, store):
    seed_bars(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block
    assert "day plan age: unavailable (no day plan)" in v


def test_position_event_carries_the_day_plan_age(assembler, conn):
    seed_day_plan(conn, created_at="2026-06-17T08:35:00+05:30")
    v = assembler.for_position_event(
        {"position_id": "p1"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399"
    ).volatile_block
    assert "day plan age: authored 08:35 IST, 1.5h ago" in v


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


def test_position_event_context_names_the_closed_exit_codes(assembler):
    """The exit codes are named in the VOLATILE block (the system prompt stays byte-stable), derived from the parser's contract."""
    ctx = assembler.for_position_event(
        {"position_id": "pos-9"}, "stop_proximity", "0.4 ATR", ltp_line="ltp 1399"
    )
    v = ctx.volatile_block
    assert "an exit MUST carry exit_reason" in v
    for code in EXIT_REASON_CODES:
        assert code in v, code
    assert "reasoning in thesis" in v
    assert "exit_reason" not in ctx.system_prompt and "exit_reason" not in ctx.stable_block


def test_exit_codes_are_named_only_where_an_exit_is_legal(assembler, store):
    """A signal-candidate call is an entry decision; the exit-code line would be dead prompt bytes there."""
    seed_bars(store)
    assert "exit_reason" not in assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block


# --------------------------------------------------------------------------- planner
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
    assert ctx.stable_block == (
        f"== DAY CONTEXT (stable) ==\ntrading date: 2026-06-17 (Wednesday)\n{NO_DATA_REGIME_LINE}"
    )
    for expected in ("TATASTEEL +4.1%", "INFY gap -1.8%", "3 originating clusters",
                     "RELIANCE originating order_win", "HDFCBANK", "XYZ added to ASM",
                     "1 swing long in TCS", "2 trades, +0.4R"):
        assert expected in ctx.volatile_block
    assert "binding levels are the scanner's" in ctx.volatile_block


# --------------------------------------------------------------------------- news batch
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
    # No date in the stable prefix: this agent runs across the pre-open boundary.
    assert "2026" not in ctx.stable_block


def test_news_batch_rejects_more_than_thirty_clusters(assembler):
    assert len(assembler.for_news_batch(_clusters(30), []).volatile_block) > 0
    with pytest.raises(ValueError, match="30-cluster cap"):
        assembler.for_news_batch(_clusters(31), [])


# ------------------------------------------------- swing daily evidence
# Swing rules sweep the full eligible universe but 1m bars exist only for the tick watchlist: swing
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

# The ~45-calendar-day window opens 2026-05-03, so the 20-row tail is 2026-05-28 .. 2026-06-16.
DAILY_HEADER = (
    "bars_1d last 20 (date o h l c vol, oldest first; completed sessions through 2026-06-16):"
)
DAILY_FIRST_ROW = "  2026-05-28 540.00 550.00 530.00 545.00 100040"
DAILY_LAST_ROW = "  2026-06-16 559.00 569.00 549.00 564.00 100059"
STRUCTURAL_MARKER = "STRUCTURAL ABSENCE, NOT A DATA FAILURE"


def test_unwatched_swing_candidate_gets_a_structural_absence_note_and_a_daily_tail(assembler, store):
    """Swing candidate, zero 1m bars, full daily history available."""
    seed_daily_bars(store, symbol=UNWATCHED)
    v = assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block
    lines = v.splitlines()

    assert (
        f"bars_1m last {BAR_TAIL} (time o h l c vol, oldest first): unavailable "
        "(no completed 1m bars this session)"
    ) in lines
    assert f"  NOTE - {STRUCTURAL_MARKER}: {UNWATCHED} is outside the intraday tick" in lines
    assert "  watchlist, so it has no 1m bars this session and never will. The full-universe batch" in lines
    assert "  rules (brk20, hi52, ins, cat, cat_reversal) deliberately originate for eligible symbols," in lines
    assert "  watchlist or not." in lines
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

    # Today's forming bar (999) is never shown; the oldest seeded bar is outside the lookback.
    assert "2026-06-17" not in "\n".join(rows)
    assert "999" not in "\n".join(rows)
    assert "2026-04-18" not in v

    # Order: 1m line, absence note, daily header, session line.
    bars_1m_at = next(i for i, ln in enumerate(lines) if ln.startswith("bars_1m last"))
    note_at = next(i for i, ln in enumerate(lines) if STRUCTURAL_MARKER in ln)
    session_at = next(i for i, ln in enumerate(lines) if ln.startswith("session:"))
    assert bars_1m_at < note_at < head < session_at


def test_a_failed_1m_read_is_not_labelled_a_structural_absence(assembler, store, monkeypatch):
    """The note asserts "not a data failure"; after a failed read that would be false."""
    monkeypatch.setattr(store, "get_bars_1m", _boom)
    assert STRUCTURAL_MARKER not in assembler.for_signal(SWING_CANDIDATE, **SIGNAL_KW).volatile_block


def test_watched_swing_candidate_carries_both_tails_and_no_structural_wording(assembler, store):
    """A swing candidate that IS on the tick watchlist: intraday tail AND daily tail, no note."""
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
#: Pinned intraday volatile block from ``_intraday_pin_block``. The fixture seeds daily bars on
#: purpose: an intraday candidate must not render them. Its features carry no rel_volume key and no
#: sentiment value is railed, so neither legend nor saturation label appears.
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
  watchlist entry: {"direction":"long","event_type":"order_win","grade":"context","materiality":0.72,"source_domain_count":3}
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
day plan age: authored 08:50 IST, 1.2h ago"""


def _intraday_pin_block(assembler, store, conn) -> str:
    """The exact fixture whose rendering is pinned byte-for-byte by ``INTRADAY_VOLATILE_PIN``."""
    seed_bars(store, count=5)
    seed_features(store)
    seed_catalyst(store)
    seed_daily_bars(store)
    seed_day_plan(conn)
    return assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block


def test_intraday_volatile_block_is_byte_identical_to_pre_change(assembler, store, conn):
    """The intraday path pays nothing for the swing daily tail."""
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
    """Token cost of the daily tail (chars/4), capped. Measured as the same candidate rendered swing vs
    intraday; the blocks differ by the new lines plus the 3-char ``"swing"``/``"intraday"`` JSON literal."""
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


# ============================================ the contract frame + input honesty

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
    """The contract block sits directly under the candidate line it frames."""
    seed_bars(store)
    v = assembler.for_signal(CANDIDATE, **SIGNAL_KW).volatile_block
    lines = v.splitlines()

    assert "strategy contract (orb):" in lines
    assert lines.index("strategy contract (orb):") == lines.index(
        next(ln for ln in lines if ln.startswith("candidate: "))) + 1
    assert "  class: intraday (MIS), same-day squareoff" in v
    assert "  evidence status: exploratory" in v


def test_signal_context_carries_its_contract_as_template(assembler):
    brk20 = CANDIDATE.model_copy(update={"strategy_id": "brk20"})
    other_brk20 = brk20.model_copy(update={"signal_id": "sig-3", "score": 0.4})
    hi52 = brk20.model_copy(update={"strategy_id": "hi52"})

    template = assembler.for_signal(brk20, **SIGNAL_KW).template
    assert template.startswith(f"{PROMPT_TEMPLATE_VERSION}\x00strategy contract (brk20):")
    assert assembler.for_signal(other_brk20, **SIGNAL_KW).template == template
    assert assembler.for_signal(hi52, **SIGNAL_KW).template != template


def test_for_signal_fixed_text_matches_golden(assembler, store, conn, calendar):
    """Wording changes fail until the golden file and ``PROMPT_TEMPLATE_VERSION`` (its first line) are updated together."""
    seed_daily_bars(store, symbol=UNWATCHED)
    seed_day_plan(conn)
    seed_index(store, calendar, UP)
    seed_ew(conn, calendar, EW_OK)
    store.insert_feature_snapshot(
        "fs-golden", UNWATCHED, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1,
        '{"atr14":12.5,"rel_volume":0.03,"rel_volume_tod":1.12}',
    )
    cand = SWING_CANDIDATE.model_copy(update={"features_snapshot_id": "fs-golden"})

    version, _, body = GOLDEN.read_text(encoding="utf-8").partition("\n")
    assert version == PROMPT_TEMPLATE_VERSION
    assert assembler.for_signal(cand, **SIGNAL_KW).prompt() == body


def test_the_nightly_review_reads_back_the_candidate_line(assembler, store):
    """The nightly review finds a DECLINE's candidate by parsing the ``candidate:`` line from the archived
    prompt (``nightly_review._candidate_from_context``); a reformatted line would silently render every decline as "unknown"."""
    from engine.ops.nightly_review import _candidate_from_context

    seed_bars(store)
    ctx = assembler.for_signal(CANDIDATE, **SIGNAL_KW)
    blob = gzip.compress(f"{ctx.system_prompt}\n\n{ctx.prompt()}".encode())
    assert _candidate_from_context(blob) == CANDIDATE.model_dump(mode="json")


def test_the_ins_contract_reaches_the_prompt_verbatim(assembler, store):
    """``ins`` crossings cluster in drawdowns, so a red tape and below-VWAP price are the validated entry
    population, and ``target=None`` is design; all three statements must reach the analyst's block."""
    seed_daily_bars(store, symbol=UNWATCHED)
    v = assembler.for_signal(INS_CANDIDATE, **SIGNAL_KW).volatile_block

    assert "strategy contract (ins):" in v.splitlines()
    assert "EXPECTED entry population" in v
    assert "ONLY validated edge" in v
    assert "target=None BY DESIGN" in v
    assert "never invent one" in v


def test_an_unregistered_strategy_renders_the_unknown_frame_and_warns(assembler, store, caplog):
    """A scanner without a contract costs the call its frame, never the call, and logs a WARNING."""
    import logging

    bare = CANDIDATE.model_copy(update={"strategy_id": "newleg"})
    with caplog.at_level(logging.WARNING, logger="engine.intelligence.context"):
        v = assembler.for_signal(bare, **SIGNAL_KW).volatile_block

    assert "strategy contract (newleg):" in v.splitlines()
    assert "  UNKNOWN (unregistered strategy_id — evaluate conservatively and flag it)" in v
    records = [r for r in caplog.records if r.getMessage() == "strategy_contract_missing"]
    assert records and records[0].strategy_id == "newleg"


# ------------------------------------------------------------------ the sentiment rail
def test_a_railed_sentiment_value_is_labelled_saturated(assembler):
    """``sentiment_agg`` is a clipped sum, so a few same-direction headlines reach ±1.0; the semantics are
    stated beside the number. A row with no saturation measures keeps this prose."""
    line = assembler._sentiment_line("symbol RELIANCE", {"value": -1.0})

    assert "SATURATED" in line
    assert "net negative" in line
    assert "clipped decay-weighted SUM" in line
    assert line.startswith("  sentiment symbol RELIANCE: -1.000 (SATURATED:")

    positive = assembler._sentiment_line("market", {"value": 1.0})
    assert "net positive" in positive


def test_an_unrailed_sentiment_value_renders_exactly_as_before(assembler):
    """The label is for the rail only; off the rail, measures present or absent make no difference."""
    assert assembler._sentiment_line("market", {"value": 0.5}) == "  sentiment market: +0.500"
    assert assembler._sentiment_line("market", {"value": -0.998}) == "  sentiment market: -0.998"
    assert assembler._sentiment_line("market", None) == (
        "  sentiment market: none (no scored headlines in the latest digest)"
    )
    measured = {"value": 0.5, "raw_sum": 0.5, "n_clusters": 3}
    assert assembler._sentiment_line("market", measured) == "  sentiment market: +0.500"


# ------------------------------------------------------- the measured rail
def test_a_railed_sentiment_value_reports_its_measured_saturation(assembler):
    """With ``raw_sum``/``n_clusters`` the rail reports how far past the clip the flow ran, over how many clusters."""
    line = assembler._sentiment_line(
        "market", {"value": -1.0, "raw_sum": -2.3149, "n_clusters": 9}
    )

    assert line == (
        "  sentiment market: -1.000 (clipped SUM saturated: raw -2.31 across 9 clusters, "
        "mean -0.257 per cluster — read as net negative headline flow, not extremity)"
    )
    assert "SATURATED:" not in line          # the measurement REPLACES the saturated-label prose

    positive = assembler._sentiment_line(
        "symbol RELIANCE", {"value": 1.0, "raw_sum": 1.0004, "n_clusters": 1}
    )
    assert "raw +1.00 across 1 cluster, mean +1.000 per cluster —" in positive   # singular, signed
    assert "net positive" in positive


def test_a_railed_sum_over_a_large_corpus_reports_its_near_zero_mean(assembler):
    """A rail over a large corpus is near-neutral flow; the per-cluster mean is printed to show it."""
    line = assembler._sentiment_line("market", {"value": -1.0, "raw_sum": -9.48, "n_clusters": 759})
    assert "raw -9.48 across 759 clusters, mean -0.012 per cluster" in line


def test_a_half_measured_rail_falls_back_to_the_plain_prose(assembler):
    """Either measure missing ⇒ no measurement; never print half of one."""
    for partial in ({"value": -1.0, "raw_sum": -2.31}, {"value": -1.0, "n_clusters": 9}):
        line = assembler._sentiment_line("market", partial)
        assert "SATURATED:" in line and "raw " not in line


def test_the_catalyst_block_renders_the_measured_rail_end_to_end(assembler, store):
    """``_catalyst_text`` must pass the full store row through, not the bare value, or the measures never render."""
    as_of = datetime(2026, 6, 17, 8, 35, tzinfo=IST)
    store.upsert_sentiment_agg([
        {"scope": "market", "scope_key": "market", "as_of": as_of, "value": -1.0,
         "raw_sum": -2.31, "n_clusters": 9},
    ])
    text = assembler._catalyst_text(SYMBOL, TODAY)

    assert "  sentiment market: -1.000 (clipped SUM saturated: raw -2.31 across 9 clusters" in text


# --------------------------------------------------------- the rel_volume denominator
def test_rel_volume_carries_its_full_day_denominator(assembler, store):
    """rel_volume is cumulative session volume ÷ 20d median FULL-DAY volume, not time-adjusted, so 0.03 at 09:20 is ordinary."""
    store.insert_feature_snapshot(
        "fs-rv", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1,
        '{"atr14":12.5,"rel_volume":0.03}',
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-rv"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert "NOT time-of-day adjusted" in v
    assert "20d median FULL-DAY volume" in v
    assert "  note: rel_volume = cumulative session volume / 20d median FULL-DAY volume" in v


# ------------------------------------------------- the corrected number ships beside it
def test_both_relative_volume_keys_are_explained(assembler, store):
    """``rel_volume_tod`` is the pace number and ``rel_volume`` the full-day ratio; the legend names both denominators."""
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
    """Either key alone earns the legend; a null ``rel_volume`` beside a tod number is itself information."""
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
    """The legend annotates a number that is present; absent or null renders nothing."""
    store.insert_feature_snapshot(
        "fs-norv", SYMBOL, datetime(2026, 6, 17, 10, 4, tzinfo=IST), 1, features_json,
    )
    cand = CANDIDATE.model_copy(update={"features_snapshot_id": "fs-norv"})
    v = assembler.for_signal(cand, **SIGNAL_KW).volatile_block

    assert "NOT time-of-day adjusted" not in v
    assert "note: rel_volume" not in v
