"""``brk20`` RETEST re-arm — the durable resting-level book (WO-R, 2026-09-12).

The 2026-09-12 pre-registered entry-mechanism backtest selected ``V2_limit_at_H20_N5`` (a limit
resting at the broken level for five sessions) and the live rule shipped N=1 — the level published
once, on the crossing session. These tests pin the mechanism that closes that gap AND, just as
load-bearing, the four ways it must stay narrow: it re-offers the SAME levels, only inside the
§7.1 ``entry_sanity_band``, only inside the ``(signal_d, expires_d]`` window, and at most once a day.

The window arithmetic is asserted against the REAL NSE calendar rather than a stub, because the one
thing a calendar-blind ``+5 days`` gets wrong is exactly the case that matters: 2026-06-17 + 5
sessions is 06-24 (a weekend intervenes), and 2026-06-22 + 5 sessions is 06-30 (a weekend AND the
06-26 holiday intervene) — both materially later than a naive date add.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.db import connect
from engine.core.migrations import apply_migrations
from engine.strategy.retest import (
    DEFAULT_RETEST_SESSIONS,
    STATUS_EXPIRED,
    STATUS_RESTING,
    RestingLevelBook,
)
from engine.strategy.scanners import brk20
from engine.strategy.types import RawLevels, SignalCandidate

LOGGER = "engine.strategy.retest"

#: Wed 2026-06-17 — the crossing session. The five trading sessions that follow are 18, 19 (Fri),
#: 22 (Mon), 23, 24; a calendar-blind "+5 days" would say 06-22.
SIGNAL_D = date(2026, 6, 17)
EXPIRES_D = date(2026, 6, 24)
#: The first retest session (the day after the crossing) and the first day past the window.
FIRST_RETEST_D = date(2026, 6, 18)
AFTER_EXPIRY_D = date(2026, 6, 25)

BAND = Decimal("2.0")          # the shipped limits.yaml `entry_sanity_band.cnc_pct`


# --------------------------------------------------------------------------- doubles / helpers
class _MovableClock(Clock):
    """A ``Clock`` whose DATE the test moves — the whole mechanism is multi-session, so a frozen
    clock cannot exercise it. 10:05 IST keeps every ``now()`` inside a real session."""

    def __init__(self, day: date) -> None:
        super().__init__(time_source=lambda: datetime.combine(self.day, time(10, 5), tzinfo=IST))
        self.day = day


@pytest.fixture
def rclock() -> _MovableClock:
    return _MovableClock(SIGNAL_D)


@pytest.fixture
def rcalendar(rclock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", rclock, strict=False)


@pytest.fixture
def book(conn, rcalendar, rclock) -> RestingLevelBook:
    return RestingLevelBook(conn, rcalendar, rclock)


def _cand(symbol: str = "TESTSYM", *, entry: str = "100.00", stop: str | None = "94.00",
          target: str | None = "112.00", score: float = 0.6,
          strategy_id: str = brk20.STRATEGY_ID) -> SignalCandidate:
    return SignalCandidate(
        signal_id=f"sig-{symbol}-{entry}", strategy_id=strategy_id, symbol=symbol,
        side="BUY", style="swing",
        raw_levels=RawLevels(
            entry=Decimal(entry),
            stop=None if stop is None else Decimal(stop),
            target=None if target is None else Decimal(target),
        ),
        score=score,
    )


def _ltp(**prices: str):
    """An ``ltp_fn`` over a fixed price map; an unmapped symbol has no tick (None)."""
    return lambda sym: (Decimal(prices[sym]) if sym in prices else None)


def _rows(conn) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM brk20_resting_levels ORDER BY symbol, signal_d"
    ).fetchall()]


# --------------------------------------------------------------------------- record
def test_record_is_idempotent_per_symbol_and_signal_day(book, conn, caplog) -> None:
    """A second sweep the same day (an owner /scan_now, a freeze-lift re-sweep) must not re-write the
    row: the FIRST admission's levels, score, expiry and created_at are the ones the crossing session
    earned, and a later write would silently extend the window or move the level."""
    assert book.record(_cand(entry="100.00", score=0.60)) is True
    first = _rows(conn)[0]

    # Same (symbol, signal_d), DIFFERENT levels/score — the conflict must win, not the newcomer.
    assert book.record(_cand(entry="111.00", score=0.95)) is False
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0] == first
    assert rows[0]["entry"] == "100.00"
    assert rows[0]["score"] == pytest.approx(0.60)
    assert rows[0]["status"] == STATUS_RESTING

    # The book is brk20's; another leg's candidate is refused loudly rather than stored silently.
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.record(_cand(symbol="OTHER", strategy_id="hi52")) is False
    assert len(_rows(conn)) == 1
    assert "brk20_retest_record_rejected" in caplog.text


def test_the_window_is_counted_in_TRADING_sessions_not_calendar_days(book, conn, rclock) -> None:
    """``expires_d`` is resolved through the NSE calendar: a naive ``+N days`` would shorten the
    window across every weekend and holiday, i.e. exactly when a price is most likely to have walked
    away and come back."""
    book.record(_cand("AAA"))
    assert _rows(conn)[0]["expires_d"] == EXPIRES_D.isoformat()
    assert EXPIRES_D != SIGNAL_D + timedelta(days=DEFAULT_RETEST_SESSIONS)

    # Mon 2026-06-22: a weekend AND the 06-26 holiday fall inside the five sessions ⇒ 06-30.
    rclock.day = date(2026, 6, 22)
    book.record(_cand("BBB"))
    bbb = next(r for r in _rows(conn) if r["symbol"] == "BBB")
    assert bbb["signal_d"] == "2026-06-22"
    assert bbb["expires_d"] == "2026-06-30"


def test_a_zero_or_negative_sessions_knob_still_writes_a_window_that_exists(conn, rcalendar,
                                                                           rclock) -> None:
    """A window of 0 sessions is a disabled mechanism expressed as a config value; the book clamps to
    the shortest REAL retest instead of writing rows that are born expired."""
    for bad in (0, -3):
        conn.execute("DELETE FROM brk20_resting_levels")
        RestingLevelBook(conn, rcalendar, rclock, sessions=bad).record(_cand("AAA"))
        row = _rows(conn)[0]
        assert row["expires_d"] == FIRST_RETEST_D.isoformat() > row["signal_d"]


def test_stop_and_target_survive_a_none(book, conn, rclock) -> None:
    """``RawLevels`` permits None; a schema that cannot represent what the type permits would turn a
    future level shape into an IntegrityError inside the sweep."""
    assert book.record(_cand("AAA", stop=None, target=None)) is True
    rclock.day = FIRST_RETEST_D
    (cand,) = book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D)
    assert (cand.raw_levels.stop, cand.raw_levels.target) == (None, None)


# --------------------------------------------------------------------------- due(): the window
def test_due_is_open_at_the_near_end_and_closed_at_the_far_end(book, rclock) -> None:
    """``signal_d < today <= expires_d``. Strictly after the crossing session (the level was already
    published that day — the pre-screen's same-day dedupe owns it) and INCLUSIVE at expiry, or a
    5-session window would only offer four retest sessions."""
    book.record(_cand("AAA", entry="100.00"))
    ltp = _ltp(AAA="100.00")                      # dead-on the level all week

    assert book.due(ltp, BAND, SIGNAL_D) == []               # the crossing session itself: no
    for d in (FIRST_RETEST_D, date(2026, 6, 19), date(2026, 6, 22), date(2026, 6, 23), EXPIRES_D):
        rclock.day = d
        assert [c.symbol for c in book.due(ltp, BAND, d)] == ["AAA"], d
    rclock.day = AFTER_EXPIRY_D
    assert book.due(ltp, BAND, AFTER_EXPIRY_D) == []         # one session past expiry: no


def test_the_republished_candidate_is_the_same_trade_with_a_fresh_identity(book, rclock) -> None:
    """No new entry price: same levels, same score. A FRESH signal_id because a new publication is a
    new signal (the journal, the forward queue and the recommendation all key off it), and NO
    features_snapshot_id — the caller mints that after ``prescreen.admit``, as the sweep does."""
    original = _cand("AAA", entry="100.00", stop="94.00", target="112.00", score=0.73)
    book.record(original)
    rclock.day = FIRST_RETEST_D

    (cand,) = book.due(_ltp(AAA="100.50"), BAND, FIRST_RETEST_D)
    assert cand.raw_levels == original.raw_levels
    assert cand.score == pytest.approx(0.73)
    assert (cand.strategy_id, cand.side, cand.style) == (brk20.STRATEGY_ID, "BUY", "swing")
    assert cand.signal_id != original.signal_id and cand.signal_id
    assert cand.features_snapshot_id is None


# --------------------------------------------------------------------------- due(): the band
def test_the_band_edge_publishes_and_a_hair_beyond_it_does_not(book, rclock, caplog) -> None:
    """The §7.1 ``entry_sanity_band`` formula and its ``<=`` edge, verbatim: ``dev == band`` is a
    PASS in the gate, so it is a pass here. AAA's level sits EXACTLY 2.00% from the live price and
    BBB's 2.05% — the band is a NARROWING, and nothing in this book may loosen it."""
    book.record(_cand("AAA", entry="102.00"))     # |102-100|/100*100 == 2.00 exactly
    book.record(_cand("BBB", entry="102.05"))     # 2.05 > 2.00
    rclock.day = FIRST_RETEST_D

    with caplog.at_level(logging.INFO, logger=LOGGER):
        due = book.due(_ltp(AAA="100.00", BBB="100.00"), BAND, FIRST_RETEST_D)
    assert [c.symbol for c in due] == ["AAA"]
    # `_offered`, never `_republished`: the caller emits the publication line off `offers` AFTER
    # `prescreen.admit` has ruled, so the two funnel stages stay separately greppable (WO-9).
    assert "brk20_retest_offered" in caplog.text
    assert "brk20_retest_republished" not in caplog.text

    # A tighter owner band binds immediately — it is read per tick, never captured at construction.
    rclock.day = date(2026, 6, 19)
    assert book.due(_ltp(AAA="100.00"), Decimal("1.0"), date(2026, 6, 19)) == []


@pytest.mark.parametrize("price", [None, "0", "-5.00", "0.00"])
def test_an_unpriceable_symbol_publishes_nothing(book, rclock, price) -> None:
    """No tick, or a non-positive one, is the one case where the band CANNOT be evaluated. A
    mechanism that spends a scarce analyst slot stays silent there rather than guessing (D7)."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    ltp = (lambda _s: None) if price is None else (lambda _s: Decimal(price))
    assert book.due(ltp, BAND, FIRST_RETEST_D) == []


def test_a_raising_ltp_costs_its_own_row_and_nothing_else(book, rclock, caplog) -> None:
    """One bad row must never take the tick down with it — the whole module is fail-to-zero."""
    book.record(_cand("AAA", entry="100.00"))
    book.record(_cand("BBB", entry="100.00", score=0.9))
    rclock.day = FIRST_RETEST_D

    def ltp(sym: str):
        if sym == "BBB":
            raise RuntimeError("tick cache exploded")
        return Decimal("100.00")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        due = book.due(ltp, BAND, FIRST_RETEST_D)
    assert [c.symbol for c in due] == ["AAA"]
    assert "brk20_retest_row_failed" in caplog.text


# --------------------------------------------------------------------------- due(): the day bound
def test_a_level_is_offered_once_a_day_and_again_the_next_session(book, rclock) -> None:
    """The row STAYS resting: a re-publication that expires unactioned (declined, capped out,
    re-armed by a freeze) may fire again on the next session inside the window. What bounds it is
    per-day — the 60 s cadence must not hand ``prescreen.admit`` the same row 375 times, because
    admit counts every candidate as RAW and the WO-9 funnel would become unreadable."""
    book.record(_cand("AAA", entry="100.00"))
    ltp = _ltp(AAA="100.00")

    rclock.day = FIRST_RETEST_D
    assert len(book.due(ltp, BAND, FIRST_RETEST_D)) == 1
    assert book.due(ltp, BAND, FIRST_RETEST_D) == []          # the same minute-pulse, again
    assert book.due(ltp, BAND, FIRST_RETEST_D) == []

    rclock.day = date(2026, 6, 19)                            # next session, still inside the window
    assert len(book.due(ltp, BAND, date(2026, 6, 19))) == 1
    assert _rows(book._conn)[0]["status"] == STATUS_RESTING    # never consumed by a re-publication


def test_a_level_outside_the_band_this_minute_is_still_offerable_later_today(book, rclock) -> None:
    """The offer bound is spent on an OFFER, never on a look: a level that is out of band at 09:20
    and back inside it at 14:00 is exactly the case this mechanism exists for."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    assert book.due(_ltp(AAA="130.00"), BAND, FIRST_RETEST_D) == []     # far away all morning
    assert len(book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D)) == 1  # …comes back


def test_one_symbol_yields_one_candidate_even_with_two_resting_levels(book, rclock) -> None:
    """A symbol can cross twice inside one window (cross, fall back, cross again). Handing the
    pre-screen two same-symbol candidates in one batch buys nothing — the dedupe admits one — while
    costing a raw count and a suppression line, so the strongest breakout margin goes and the other
    is left un-offered, still available tomorrow."""
    book.record(_cand("AAA", entry="100.00", score=0.55))
    rclock.day = FIRST_RETEST_D
    book.record(_cand("AAA", entry="101.00", score=0.85))
    assert len(_rows(book._conn)) == 2                       # two rows: different signal_d

    rclock.day = date(2026, 6, 19)
    due = book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 19))
    assert [str(c.raw_levels.entry) for c in due] == ["101.00"]   # score desc wins
    # …and the day's offer is spent for the SYMBOL, not merely for that row: the loser must not
    # sneak back in on the next minute-pulse, where the pre-screen could only ever dedupe it away
    # at the cost of a raw count and a suppression line.
    assert book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 19)) == []
    # Tomorrow it is the strongest row's turn again — the other is never consumed, just outranked.
    rclock.day = date(2026, 6, 22)
    again = book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 22))
    assert [str(c.raw_levels.entry) for c in again] == ["101.00"]


# --------------------------------------------------------------------------- due(): tick freshness
def test_a_stale_trigger_price_offers_nothing(book, rclock, caplog) -> None:
    """The tick cache has NO upper bound on age and ``mark_price`` cannot say so, so without this the
    TRIGGER can be a price from hours ago — while the §7.1 gate that judges the resulting proposal
    rejects anything past ``stale_data_guard.max_tick_age_s``. Originating on a stale print spends a
    §3.2.5 day slot, the symbol's one offer of the day and one of the day's analyst calls on a
    certain reject. Worst exactly for the population ``resting_symbols`` exists to reach: sub-cap
    eligible names that go minutes between prints."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    ltp = _ltp(AAA="100.00")

    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert book.due(ltp, BAND, FIRST_RETEST_D,
                        tick_age_fn=lambda _s: 9.0, max_tick_age_s=5.0) == []
    assert "brk20_retest_tick_stale" in caplog.text

    # A symbol the cache has never aged at all is the same refusal — fail to zero, never to a guess.
    assert book.due(ltp, BAND, FIRST_RETEST_D,
                    tick_age_fn=lambda _s: None, max_tick_age_s=5.0) == []
    # …and no offer was spent on either refusal: the tape comes back and so does the level.
    assert book.due(ltp, BAND, FIRST_RETEST_D,
                    tick_age_fn=lambda _s: 5.0, max_tick_age_s=5.0)   # `<=` edge, as in the gate


def test_the_staleness_refusal_is_logged_once_a_day_not_once_a_minute(book, rclock, caplog) -> None:
    """A refusal is re-evaluated on every 60 s pulse; an unbounded line would be ~375 copies a day
    per symbol (the `prescreen_out_of_window` discipline). Bounded per (symbol, day), and the bound
    rolls with the day so each session re-states it once."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    stale = dict(tick_age_fn=lambda _s: 99.0, max_tick_age_s=5.0)

    with caplog.at_level(logging.INFO, logger=LOGGER):
        for _ in range(5):
            book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D, **stale)
    assert sum(1 for r in caplog.records if r.getMessage() == "brk20_retest_tick_stale") == 1

    with caplog.at_level(logging.INFO, logger=LOGGER):
        rclock.day = date(2026, 6, 19)
        book.due(_ltp(AAA="100.00"), BAND, date(2026, 6, 19), **stale)
    assert sum(1 for r in caplog.records if r.getMessage() == "brk20_retest_tick_stale") == 2


# --------------------------------------------------------------------- due(): the not-offerable screen
def test_a_level_already_acted_on_is_never_re_offered(book, rclock, caplog) -> None:
    """The mechanism the backtest measured books ONE trade per signal. A symbol already HELD (or
    carrying a pending entry recommendation) is rejected by `gate._rule_per_stock_exposure` as
    `already held or pending` — a HARD reason no shrink can cure — but only AFTER the day slot and
    one of the day's (DG1-capped) analyst calls are spent. The book refuses first."""
    book.record(_cand("AAA", entry="100.00", score=0.9))
    book.record(_cand("BBB", entry="100.00", score=0.5))
    rclock.day = FIRST_RETEST_D
    ltp = _ltp(AAA="100.00", BBB="100.00")

    with caplog.at_level(logging.INFO, logger=LOGGER):
        due = book.due(ltp, BAND, FIRST_RETEST_D,
                       skip_fn=lambda _syms: {"AAA": "already held"})
    assert [c.symbol for c in due] == ["BBB"]
    assert "brk20_retest_not_offerable" in caplog.text

    # The skip spends NO offer: the reason can lapse inside the day (the position closes, the
    # recommendation expires) and the level has not stopped being a level.
    assert [c.symbol for c in book.due(ltp, BAND, FIRST_RETEST_D, skip_fn=lambda _s: {})] == ["AAA"]


def test_the_screen_sees_every_due_symbol_exactly_once_per_tick(book, rclock) -> None:
    """It reads the positions/recommendations tables, so it is called ONCE per tick with the whole
    due set — never per row, and never on the ordinary minute when nothing is in the window."""
    book.record(_cand("AAA"))
    book.record(_cand("BBB"))
    rclock.day = FIRST_RETEST_D
    book.record(_cand("AAA", entry="101.00"))     # a second row for the same symbol

    seen: list[list[str]] = []

    def skip(symbols):
        seen.append(list(symbols))
        return {}

    rclock.day = date(2026, 6, 19)
    book.due(_ltp(AAA="100.50", BBB="100.00"), BAND, date(2026, 6, 19), skip_fn=skip)
    assert seen == [["AAA", "BBB"]]               # deduped and deterministic

    # …and an EMPTY window never calls it at all (the cadence has to stay one cheap SQL read).
    seen.clear()
    book.due(_ltp(), BAND, date(2026, 7, 1), skip_fn=skip)
    assert seen == []


def test_an_unevaluable_screen_offers_nothing(book, rclock, caplog) -> None:
    """A narrowing that cannot be evaluated fails to ZERO, exactly like an unreadable band: the
    alternative is re-offering a level the platform may already own."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D

    def boom(_symbols):
        raise RuntimeError("positions table locked")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D, skip_fn=boom) == []
    assert "brk20_retest_skip_unreadable" in caplog.text


# --------------------------------------------------------------------------- offer detail for the caller
def test_the_offer_detail_rides_out_for_the_callers_publication_line(book, rclock) -> None:
    """`brk20_retest_republished` belongs at the PUBLICATION step — emitted at offer time it would
    also count everything `prescreen.admit` suppressed. The book fills `offers` in place (the
    `brk20.sweep_daily(veto_counts=...)` convention) so the caller can log the accepted ones."""
    book.record(_cand("AAA", entry="102.00"))
    rclock.day = FIRST_RETEST_D
    offers: dict[str, dict[str, str]] = {}

    (cand,) = book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D, offers=offers)
    assert set(offers) == {cand.signal_id}
    assert offers[cand.signal_id] == {
        "symbol": "AAA", "entry": "102.00", "ltp": "100.00", "dev": "2.00",
        "signal_d": SIGNAL_D.isoformat(),
    }


# --------------------------------------------------------------------------- the feed follows
def test_resting_symbols_is_todays_window_so_the_feed_can_follow_a_level(book, rclock) -> None:
    """Without this the mechanism is DEAD for the population it exists for. ``due`` bands against a
    LIVE price and the engine's only price source is the tick cache; the sweep's batch subscription
    ROLLS AT MIDNIGHT, so a level on a sub-cap symbol (no 1m bars, never on the watchlist) would read
    ``ltp is None`` on every retest day. The sweep feeds this list into the same subscription set it
    builds from today's admissions."""
    book.record(_cand("AAA"))
    book.record(_cand("BBB"))
    assert book.resting_symbols(SIGNAL_D) == []          # today's own admissions are in the batch

    rclock.day = FIRST_RETEST_D
    assert book.resting_symbols(FIRST_RETEST_D) == ["AAA", "BBB"]
    book.record(_cand("AAA", entry="101.00"))            # a second crossing: still ONE subscription
    assert book.resting_symbols(date(2026, 6, 19)) == ["AAA", "BBB"]

    rclock.day = AFTER_EXPIRY_D                          # past the window: the feed may let go
    assert book.resting_symbols(AFTER_EXPIRY_D) == ["AAA"]   # …AAA's 06-18 row still runs to 06-25
    assert book.resting_symbols(date(2026, 7, 1)) == []


def test_resting_symbols_never_breaks_the_sweep(book, conn, caplog) -> None:
    """It is a FEED HINT on the sweep's hot path; an unreadable journal costs a re-offer, not a scan."""
    book.record(_cand("AAA"))
    conn.execute("DROP TABLE brk20_resting_levels")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.resting_symbols(FIRST_RETEST_D) == []
    assert "brk20_retest_symbols_failed" in caplog.text


# --------------------------------------------------------------------------- expire
def test_expire_marks_rows_past_the_window_and_never_on_the_expiry_day(book, conn, rclock) -> None:
    """``expires_d`` is the LAST offerable session (``due`` is inclusive there), so expiring it on
    its own day would silently ship an N-1 window."""
    book.record(_cand("AAA"))

    rclock.day = EXPIRES_D
    assert book.expire(EXPIRES_D) == 0
    assert _rows(conn)[0]["status"] == STATUS_RESTING

    rclock.day = AFTER_EXPIRY_D
    assert book.expire(AFTER_EXPIRY_D) == 1
    row = _rows(conn)[0]
    assert row["status"] == STATUS_EXPIRED
    assert row["updated_at"] > row["created_at"]
    assert book.expire(AFTER_EXPIRY_D) == 0                  # idempotent: nothing left resting
    assert book.due(_ltp(AAA="100.00"), BAND, AFTER_EXPIRY_D) == []


def test_due_does_not_depend_on_expire_having_run(book, rclock) -> None:
    """A day the engine was down, or the window never opened, leaves stale rows ``resting``.
    ``due`` re-tests ``expires_d >= today`` itself, so housekeeping that never ran cannot resurrect
    a level whose window closed."""
    book.record(_cand("AAA", entry="100.00"))
    later = date(2026, 7, 1)
    rclock.day = later
    assert book.due(_ltp(AAA="100.00"), BAND, later) == []
    assert _rows(book._conn)[0]["status"] == STATUS_RESTING   # …and expire() was never called


# --------------------------------------------------------------------------- durability
def test_an_engine_restart_between_the_signal_and_the_retest_loses_nothing(
    db_path, rcalendar, rclock
) -> None:
    """THE reason the state is SQLite. The 2026-09-11 forensics recorded the in-memory forward queue
    being lost on every mid-session reboot; a multi-session mechanism whose state does not survive a
    restart silently degenerates into the single-session one it was built to replace."""
    first = connect(db_path)
    apply_migrations(first)
    RestingLevelBook(first, rcalendar, rclock).record(_cand("AAA", entry="100.00", score=0.71))
    first.close()                                            # …the engine goes down

    rclock.day = EXPIRES_D                                   # …and comes back four sessions later
    second = connect(db_path)
    try:
        reborn = RestingLevelBook(second, rcalendar, rclock)
        (cand,) = reborn.due(_ltp(AAA="100.00"), BAND, EXPIRES_D)
        assert (cand.symbol, str(cand.raw_levels.entry)) == ("AAA", "100.00")
        assert cand.score == pytest.approx(0.71)
        # The per-day OFFER bound is process memory, and losing it in a restart costs exactly one
        # extra offer that day — deduped by the pre-screen, +1 on a raw counter. That is the same
        # cost a restart already imposes on the ordinary sweep, which re-admits its whole batch.
        third = RestingLevelBook(second, rcalendar, rclock)
        assert len(third.due(_ltp(AAA="100.00"), BAND, EXPIRES_D)) == 1
        assert reborn.due(_ltp(AAA="100.00"), BAND, EXPIRES_D) == []   # …but not within one process
    finally:
        second.close()


# --------------------------------------------------------------------------- fail to zero
def test_an_unreadable_journal_re_offers_nothing_and_never_raises(book, conn, caplog) -> None:
    """A journal failure logs and returns [] — it must cost a re-offer, never the drain tick."""
    book.record(_cand("AAA"))
    conn.execute("DROP TABLE brk20_resting_levels")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D) == []
        assert book.expire(FIRST_RETEST_D) == 0
        assert book.record(_cand("BBB")) is False
    for event in ("brk20_retest_due_failed", "brk20_retest_expire_failed",
                  "brk20_retest_record_failed"):
        assert event in caplog.text


def test_a_corrupt_price_column_costs_its_own_row(book, conn, rclock, caplog) -> None:
    """Rows are TEXT decimal strings written by this platform, but a hand-edited or half-migrated
    row must not be able to kill the tick for every other resting level."""
    book.record(_cand("AAA", entry="100.00"))
    book.record(_cand("BBB", entry="100.00", score=0.9))
    conn.execute("UPDATE brk20_resting_levels SET entry = 'not-a-price' WHERE symbol = 'BBB'")
    rclock.day = FIRST_RETEST_D
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        due = book.due(_ltp(AAA="100.00", BBB="100.00"), BAND, FIRST_RETEST_D)
    assert [c.symbol for c in due] == ["AAA"]
    assert "brk20_retest_row_failed" in caplog.text
