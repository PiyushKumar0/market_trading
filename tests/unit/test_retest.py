"""``brk20`` RETEST re-arm: the durable resting-level book (a limit resting at the broken level for five sessions).

Pins the mechanism and the four ways it stays narrow: it re-offers the SAME levels, only inside the
``entry_sanity_band``, only inside the ``(signal_d, expires_d]`` window, and at most once a day.

Window arithmetic runs against the real NSE calendar: 2026-06-17 + 4 sessions is 06-23 (weekend),
and 2026-06-22 + 4 sessions is 06-29 (weekend and the 06-26 holiday), later than a calendar-blind date add.
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

#: Wed 2026-06-17 — signal_d, the publication session y+1 (the crossing y is 06-16). The four
#: re-offer sessions are 18, 19 (Fri), 22 (Mon), 23; a calendar-blind "+4 days" would say 06-21.
SIGNAL_D = date(2026, 6, 17)
EXPIRES_D = date(2026, 6, 23)
#: The first re-offer session and the first day past the window.
FIRST_RETEST_D = date(2026, 6, 18)
AFTER_EXPIRY_D = date(2026, 6, 24)

BAND = Decimal("2.0")          # the shipped limits.yaml `entry_sanity_band.cnc_pct`


# --------------------------------------------------------------------------- doubles / helpers
class _MovableClock(Clock):
    """A ``Clock`` whose date the test moves (the mechanism is multi-session); 10:05 IST keeps ``now()`` inside a session."""

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
    """A second sweep the same day must not re-write the row: the first admission's levels, score and expiry stand."""
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
    """``expires_d`` is resolved through the NSE calendar; a naive ``+N days`` would shorten the window across weekends and holidays."""
    book.record(_cand("AAA"))
    assert _rows(conn)[0]["expires_d"] == EXPIRES_D.isoformat()
    assert EXPIRES_D != SIGNAL_D + timedelta(days=DEFAULT_RETEST_SESSIONS - 1)


def test_expiry_is_n_minus_1_sessions_after_signal_d(book, conn, rcalendar, rclock) -> None:
    """5-session-limit rests the level on y+1..y+5 and ``signal_d`` is y+1, so re-offers fall on y+2..y+5."""
    monday = date(2026, 6, 22)                            # a weekend and the 06-26 holiday follow
    assert book._expiry(monday) == date(2026, 6, 29)
    assert RestingLevelBook(conn, rcalendar, rclock, sessions=1)._expiry(monday) == monday

    book.record(_cand("AAA"))                             # signal_d 06-17 = y+1, crossing y = 06-16
    offered, d = [], SIGNAL_D
    while d <= date(2026, 6, 30):
        rclock.day = d
        offered += [d for _ in book.due(_ltp(AAA="100.00"), BAND, d)]
        d = rcalendar.next_trading_day(d)
    assert offered == [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22), date(2026, 6, 23)]


def test_a_zero_or_negative_sessions_knob_still_writes_a_window_that_exists(conn, rcalendar,
                                                                           rclock) -> None:
    """A window of 0 sessions clamps to N=1, never a window that ends before signal_d."""
    for bad in (0, -3):
        conn.execute("DELETE FROM brk20_resting_levels")
        RestingLevelBook(conn, rcalendar, rclock, sessions=bad).record(_cand("AAA"))
        row = _rows(conn)[0]
        assert row["expires_d"] == row["signal_d"] == SIGNAL_D.isoformat()


def test_stop_and_target_survive_a_none(book, conn, rclock) -> None:
    """``RawLevels`` permits None, so the schema must represent it (no IntegrityError inside the sweep)."""
    assert book.record(_cand("AAA", stop=None, target=None)) is True
    rclock.day = FIRST_RETEST_D
    (cand,) = book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D)
    assert (cand.raw_levels.stop, cand.raw_levels.target) == (None, None)


# --------------------------------------------------------------------------- due(): the window
def test_due_is_open_at_the_near_end_and_closed_at_the_far_end(book, rclock) -> None:
    """``signal_d < today <= expires_d``: not on ``signal_d`` (already published; the pre-screen's same-day dedupe owns it), inclusive at expiry."""
    book.record(_cand("AAA", entry="100.00"))
    ltp = _ltp(AAA="100.00")                      # dead-on the level all week

    assert book.due(ltp, BAND, SIGNAL_D) == []               # the publication session itself: no
    for d in (FIRST_RETEST_D, date(2026, 6, 19), date(2026, 6, 22), EXPIRES_D):
        rclock.day = d
        assert [c.symbol for c in book.due(ltp, BAND, d)] == ["AAA"], d
    rclock.day = AFTER_EXPIRY_D
    assert book.due(ltp, BAND, AFTER_EXPIRY_D) == []         # one session past expiry: no


def test_the_republished_candidate_is_the_same_trade_with_a_fresh_identity(book, rclock) -> None:
    """Same levels and score; a fresh signal_id (a new publication is a new signal) and no
    features_snapshot_id (the caller mints it after ``prescreen.admit``)."""
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
    """The ``entry_sanity_band`` formula and its ``<=`` edge: ``dev == band`` passes, as in the gate.
    AAA sits exactly 2.00% from the live price, BBB 2.05%. The band only narrows."""
    book.record(_cand("AAA", entry="102.00"))     # |102-100|/100*100 == 2.00 exactly
    book.record(_cand("BBB", entry="102.05"))     # 2.05 > 2.00
    rclock.day = FIRST_RETEST_D

    with caplog.at_level(logging.INFO, logger=LOGGER):
        due = book.due(_ltp(AAA="100.00", BBB="100.00"), BAND, FIRST_RETEST_D)
    assert [c.symbol for c in due] == ["AAA"]
    # `_offered`, never `_republished`: the caller emits that line after `prescreen.admit` rules.
    assert "brk20_retest_offered" in caplog.text
    assert "brk20_retest_republished" not in caplog.text

    # The band is read per tick, never captured at construction.
    rclock.day = date(2026, 6, 19)
    assert book.due(_ltp(AAA="100.00"), Decimal("1.0"), date(2026, 6, 19)) == []


@pytest.mark.parametrize("price", [None, "0", "-5.00", "0.00"])
def test_an_unpriceable_symbol_publishes_nothing(book, rclock, price) -> None:
    """No tick, or a non-positive one, cannot be banded: stay silent rather than spend an analyst slot on a guess."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    ltp = (lambda _s: None) if price is None else (lambda _s: Decimal(price))
    assert book.due(ltp, BAND, FIRST_RETEST_D) == []


def test_a_raising_ltp_costs_its_own_row_and_nothing_else(book, rclock, caplog) -> None:
    """One bad row never takes the tick down (the module is fail-to-zero)."""
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
    """The row stays resting and may fire again next session; the bound is per day, so the 60 s cadence
    does not hand ``prescreen.admit`` the same row 375 times (admit counts every candidate as RAW)."""
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
    """The offer bound is spent on an offer, never on a look: out of band at 09:20 and back inside at 14:00 still offers."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    assert book.due(_ltp(AAA="130.00"), BAND, FIRST_RETEST_D) == []     # far away all morning
    assert len(book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D)) == 1  # …comes back


def test_one_symbol_yields_one_candidate_even_with_two_resting_levels(book, rclock) -> None:
    """A symbol that crosses twice in one window yields one candidate (the strongest score); the other stays un-offered, available tomorrow."""
    book.record(_cand("AAA", entry="100.00", score=0.55))
    rclock.day = FIRST_RETEST_D
    book.record(_cand("AAA", entry="101.00", score=0.85))
    assert len(_rows(book._conn)) == 2                       # two rows: different signal_d

    rclock.day = date(2026, 6, 19)
    due = book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 19))
    assert [str(c.raw_levels.entry) for c in due] == ["101.00"]   # score desc wins
    # The day's offer is spent for the symbol, not just that row: the loser does not return on the next pulse.
    assert book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 19)) == []
    # Tomorrow the strongest row is up again; the other is outranked, never consumed.
    rclock.day = date(2026, 6, 22)
    again = book.due(_ltp(AAA="100.50"), BAND, date(2026, 6, 22))
    assert [str(c.raw_levels.entry) for c in again] == ["101.00"]

    # On a later session inside BOTH windows (06-17 -> 06-24, 06-18 -> 06-25), a price inside the
    # outranked level's band but outside the winner's offers it
    # (|100.00 - 98.50| / 98.50 = 1.52% <= 2.0%; 101.00 is 2.54% away).
    rclock.day = date(2026, 6, 23)
    later = book.due(_ltp(AAA="98.50"), BAND, date(2026, 6, 23))
    assert [str(c.raw_levels.entry) for c in later] == ["100.00"]


# --------------------------------------------------------------------------- due(): tick freshness
def test_a_stale_trigger_price_offers_nothing(book, rclock, caplog) -> None:
    """The tick cache has no age bound, but the risk gate rejects anything past ``stale_data_guard.max_tick_age_s``;
    originating on a stale print would spend a day slot, the symbol's offer and an analyst call on a certain reject."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D
    ltp = _ltp(AAA="100.00")

    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert book.due(ltp, BAND, FIRST_RETEST_D,
                        tick_age_fn=lambda _s: 9.0, max_tick_age_s=5.0) == []
    assert "brk20_retest_tick_stale" in caplog.text

    # An unknown age is the same refusal; neither refusal spends the offer.
    assert book.due(ltp, BAND, FIRST_RETEST_D,
                    tick_age_fn=lambda _s: None, max_tick_age_s=5.0) == []
    assert book.due(ltp, BAND, FIRST_RETEST_D,
                    tick_age_fn=lambda _s: 5.0, max_tick_age_s=5.0)   # `<=` edge, as in the gate


def test_the_staleness_refusal_is_logged_once_a_day_not_once_a_minute(book, rclock, caplog) -> None:
    """The refusal is re-evaluated every 60 s pulse; the log line is bounded per (symbol, day) and re-states once each session."""
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
    """One trade per signal: a symbol already held or with a pending entry recommendation is a hard gate
    reject (`gate._rule_per_stock_exposure`) only after the day slot and an analyst call are spent, so the book refuses first."""
    book.record(_cand("AAA", entry="100.00", score=0.9))
    book.record(_cand("BBB", entry="100.00", score=0.5))
    rclock.day = FIRST_RETEST_D
    ltp = _ltp(AAA="100.00", BBB="100.00")

    with caplog.at_level(logging.INFO, logger=LOGGER):
        due = book.due(ltp, BAND, FIRST_RETEST_D,
                       skip_fn=lambda _syms: {"AAA": "already held"})
    assert [c.symbol for c in due] == ["BBB"]
    assert "brk20_retest_not_offerable" in caplog.text

    # The skip spends no offer: the reason can lapse inside the day.
    assert [c.symbol for c in book.due(ltp, BAND, FIRST_RETEST_D, skip_fn=lambda _s: {})] == ["AAA"]


def test_the_screen_sees_every_due_symbol_exactly_once_per_tick(book, rclock) -> None:
    """The screen reads the positions/recommendations tables: called once per tick with the whole due set, never per row or on an empty window."""
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

    seen.clear()
    book.due(_ltp(), BAND, date(2026, 7, 1), skip_fn=skip)
    assert seen == []


def test_an_unevaluable_screen_offers_nothing(book, rclock, caplog) -> None:
    """A screen that cannot be evaluated fails to zero, like an unreadable band."""
    book.record(_cand("AAA", entry="100.00"))
    rclock.day = FIRST_RETEST_D

    def boom(_symbols):
        raise RuntimeError("positions table locked")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.due(_ltp(AAA="100.00"), BAND, FIRST_RETEST_D, skip_fn=boom) == []
    assert "brk20_retest_skip_unreadable" in caplog.text


# --------------------------------------------------------------------------- offer detail for the caller
def test_the_offer_detail_rides_out_for_the_callers_publication_line(book, rclock) -> None:
    """`brk20_retest_republished` belongs at the publication step, so the book fills `offers` in place
    (the `brk20.sweep_daily(veto_counts=...)` convention) for the caller to log the accepted ones."""
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
    """``due`` bands against a live tick-cache price, and the sweep's batch subscription rolls at midnight, so a
    sub-cap symbol's level would read ``ltp is None`` on every retest day; the sweep adds this list to its subscription set."""
    book.record(_cand("AAA"))
    book.record(_cand("BBB"))
    assert book.resting_symbols(SIGNAL_D) == []          # today's own admissions are in the batch

    rclock.day = FIRST_RETEST_D
    assert book.resting_symbols(FIRST_RETEST_D) == ["AAA", "BBB"]
    book.record(_cand("AAA", entry="101.00"))            # a second crossing: still ONE subscription
    assert book.resting_symbols(date(2026, 6, 19)) == ["AAA", "BBB"]

    rclock.day = AFTER_EXPIRY_D                          # past the window: the feed may let go
    assert book.resting_symbols(AFTER_EXPIRY_D) == ["AAA"]   # …AAA's 06-18 row still runs to 06-24
    assert book.resting_symbols(date(2026, 7, 1)) == []


def test_resting_symbols_never_breaks_the_sweep(book, conn, caplog) -> None:
    """A feed hint on the sweep's hot path: an unreadable journal costs a re-offer, not a scan."""
    book.record(_cand("AAA"))
    conn.execute("DROP TABLE brk20_resting_levels")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert book.resting_symbols(FIRST_RETEST_D) == []
    assert "brk20_retest_symbols_failed" in caplog.text


# --------------------------------------------------------------------------- expire
def test_expire_marks_rows_past_the_window_and_never_on_the_expiry_day(book, conn, rclock) -> None:
    """``expires_d`` is the last offerable session (``due`` is inclusive), so expiring it that day would ship an N-1 window."""
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
    """``due`` re-tests ``expires_d >= today`` itself, so housekeeping that never ran cannot resurrect a closed window."""
    book.record(_cand("AAA", entry="100.00"))
    later = date(2026, 7, 1)
    rclock.day = later
    assert book.due(_ltp(AAA="100.00"), BAND, later) == []
    assert _rows(book._conn)[0]["status"] == STATUS_RESTING   # …and expire() was never called


# --------------------------------------------------------------------------- durability
def test_an_engine_restart_between_the_signal_and_the_retest_loses_nothing(
    db_path, rcalendar, rclock
) -> None:
    """The state is SQLite so a multi-session mechanism survives a restart."""
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
        # The per-day offer bound is process memory: a restart costs one extra offer that day (deduped by the pre-screen).
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
    """A hand-edited or half-migrated row must not kill the tick for every other resting level."""
    book.record(_cand("AAA", entry="100.00"))
    book.record(_cand("BBB", entry="100.00", score=0.9))
    conn.execute("UPDATE brk20_resting_levels SET entry = 'not-a-price' WHERE symbol = 'BBB'")
    rclock.day = FIRST_RETEST_D
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        due = book.due(_ltp(AAA="100.00", BBB="100.00"), BAND, FIRST_RETEST_D)
    assert [c.symbol for c in due] == ["AAA"]
    assert "brk20_retest_row_failed" in caplog.text
