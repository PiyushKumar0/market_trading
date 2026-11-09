"""Holdings reconcile for tracked CNC positions: tells "the owner is ignoring the exit" from "the owner
sold and never sent ``/closed``". Also pins the four ways it must stay quiet: a T+1-young position, a
fully-held one, a second run on the same day, and a broker failure (a false "you sold this" page costs
trust in every later alert).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pytest
from kiteconnect.exceptions import TokenException

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops.holdings_reconcile import (
    HoldingsReconcileJob,
    _held_quantities,
    entry_rec_id,
    in_reconcile_window,
    missing_holdings_observations,
    positions_missing_from_holdings,
)

#: Wed 2026-06-17 10:05 IST — a real trading day inside the 09:20–15:30 reconcile window.
NOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)
#: Wed 2026-06-10: sessions strictly between it and today are 11/12/15/16 ⇒ 4 ≥ min_age_sessions.
OLD_OPEN = datetime(2026, 6, 10, 10, 0, tzinfo=IST)
#: Tue 2026-06-16: ZERO completed sessions strictly between it and today ⇒ T+1-young.
YOUNG_OPEN = datetime(2026, 6, 16, 10, 0, tzinfo=IST)

LOGGER = "engine.ops.holdings_reconcile"


# --------------------------------------------------------------------------- doubles
class _FakeKite:
    """Only ``holdings()`` is used by the job — the Kite shape is a list of per-symbol dicts."""

    def __init__(self, rows: list[dict] | None = None, raises: BaseException | None = None) -> None:
        self._rows = rows or []
        self._raises = raises
        self.calls = 0

    async def holdings(self) -> list[dict]:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return list(self._rows)


class _Sink:
    def __init__(self) -> None:
        self.messages: list[CatalogMessage] = []

    async def __call__(self, msg: CatalogMessage) -> None:
        self.messages.append(msg)


def _clock(at: datetime = NOW) -> Clock:
    return Clock(time_source=lambda: at)


def _calendar(clock: Clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


def _job(conn, kite, *, at: datetime = NOW, notify=None) -> HoldingsReconcileJob:
    clock = _clock(at)
    return HoldingsReconcileJob(
        conn, kite, clock, _calendar(clock), notify if notify is not None else _Sink()
    )


# --------------------------------------------------------------------------- seeding
def _position(
    conn, position_id: str, symbol: str, qty: int, *, opened: datetime = OLD_OPEN,
    product: str = "CNC", origin: str = "recommended", state: str = "OPEN",
) -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, state, "
        "is_paper, origin, opened_at) VALUES (?, ?, 'BUY', 'swing', ?, ?, '100', ?, 0, ?, ?)",
        (position_id, symbol, product, qty, state, origin, opened.isoformat()),
    )
    conn.commit()


def _ledger(
    conn, entry_id: str, position_id: str, rec_id: str, *, entry_px: str | None = "100",
    created_at: str = "2026-06-10T10:00:00+05:30",
) -> None:
    conn.execute(
        "INSERT INTO learning_ledger (entry_id, position_id, rec_id, entry_px, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (entry_id, position_id, rec_id, entry_px, created_at),
    )
    conn.commit()


def _holding(symbol: str, quantity: int, t1: int = 0, collateral: int = 0) -> dict:
    return {"tradingsymbol": symbol, "exchange": "NSE", "quantity": quantity, "t1_quantity": t1,
            "collateral_quantity": collateral}


# --------------------------------------------------------------------------- (i) absent symbol
@pytest.mark.asyncio
async def test_absent_symbol_is_flagged_and_names_the_closed_reply(conn, caplog) -> None:
    """A tracked position the broker no longer holds ⇒ one alert naming the entry rec id and the literal ``/closed`` reply."""
    _position(conn, "pos-1", "HDFCAMC", 7)
    _ledger(conn, "led-1", "pos-1", "REC-ENTRY-1")
    sink = _Sink()

    with caplog.at_level(logging.INFO, logger=LOGGER):
        result = await _job(conn, _FakeKite([_holding("RELIANCE", 10)]), notify=sink).run()

    assert result.checked == 1
    assert result.flagged == ["pos-1"]
    assert result.skipped_young == 0
    assert result.error is None

    assert len(sink.messages) == 1
    msg = sink.messages[0]
    assert msg.kind is MessageKind.POSITION_NOT_IN_HOLDINGS
    assert msg.severity == "warning"
    assert "HDFCAMC" in msg.title or "HDFCAMC" in msg.body
    assert "/closed REC-ENTRY-1 <price>" in msg.body
    assert "7" in msg.body and "0" in msg.body            # tracked vs held, both stated
    assert msg.data["entry_rec_id"] == "REC-ENTRY-1"
    assert msg.data["position_id"] == "pos-1"
    assert msg.data["tracked_qty"] == 7
    assert msg.data["held_qty"] == 0

    flagged = [r for r in caplog.records if r.getMessage() == "position_not_in_holdings"]
    assert len(flagged) == 1
    assert flagged[0].levelno == logging.WARNING
    assert flagged[0].symbol == "HDFCAMC"
    assert flagged[0].tracked == 7 and flagged[0].held == 0
    assert flagged[0].entry_rec_id == "REC-ENTRY-1"
    done = [r for r in caplog.records if r.getMessage() == "holdings_reconcile_done"]
    assert len(done) == 1
    assert done[0].checked == 1 and done[0].flagged == 1 and done[0].skipped_young == 0


@pytest.mark.asyncio
async def test_position_without_an_entry_ledger_row_still_alerts_with_the_position_id(conn) -> None:
    """No ledger row ⇒ the alert cannot name a rec id, so it gives the position id rather than going silent."""
    _position(conn, "pos-2", "HINDZINC", 5)
    sink = _Sink()

    result = await _job(conn, _FakeKite([]), notify=sink).run()

    assert result.flagged == ["pos-2"]
    body = sink.messages[0].body
    assert "pos-2" in body
    assert sink.messages[0].data["entry_rec_id"] is None


@pytest.mark.asyncio
async def test_entry_row_wins_over_a_later_exit_ledger_row(conn) -> None:
    """``/closed`` accepts either id, but the entry rec (the row carrying ``entry_px``) is the one the owner saw first, as in ``pipeline.close``."""
    _position(conn, "pos-3", "HDFCAMC", 4)
    _ledger(conn, "led-exit", "pos-3", "REC-EXIT", entry_px=None,
            created_at="2026-06-09T10:00:00+05:30")
    _ledger(conn, "led-entry", "pos-3", "REC-ENTRY", entry_px="100",
            created_at="2026-06-10T10:00:00+05:30")
    sink = _Sink()

    await _job(conn, _FakeKite([]), notify=sink).run()

    assert sink.messages[0].data["entry_rec_id"] == "REC-ENTRY"


@pytest.mark.asyncio
async def test_falls_back_to_any_ledger_row_when_no_entry_px_exists(conn) -> None:
    _position(conn, "pos-4", "HDFCAMC", 4)
    _ledger(conn, "led-only", "pos-4", "REC-ONLY", entry_px=None)
    sink = _Sink()

    await _job(conn, _FakeKite([]), notify=sink).run()

    assert sink.messages[0].data["entry_rec_id"] == "REC-ONLY"


# --------------------------------------------------------------------------- (ii) partial
@pytest.mark.asyncio
async def test_partial_holding_is_flagged(conn) -> None:
    """Held 3 of 7: a partial exit is still an unreported exit."""
    _position(conn, "pos-5", "HDFCAMC", 7)
    _ledger(conn, "led-5", "pos-5", "REC-5")
    sink = _Sink()

    result = await _job(conn, _FakeKite([_holding("HDFCAMC", 3)]), notify=sink).run()

    assert result.flagged == ["pos-5"]
    assert sink.messages[0].data["held_qty"] == 3


# --------------------------------------------------------------------------- (iii) fully held
@pytest.mark.asyncio
async def test_fully_held_position_is_silent(conn, caplog) -> None:
    _position(conn, "pos-6", "HDFCAMC", 7)
    _ledger(conn, "led-6", "pos-6", "REC-6")
    sink = _Sink()

    with caplog.at_level(logging.INFO, logger=LOGGER):
        result = await _job(conn, _FakeKite([_holding("HDFCAMC", 7)]), notify=sink).run()

    assert result.checked == 1 and result.flagged == []
    assert sink.messages == []
    assert not [r for r in caplog.records if r.getMessage() == "position_not_in_holdings"]


@pytest.mark.asyncio
async def test_t1_quantity_counts_as_held(conn) -> None:
    """A buy still in ``t1_quantity`` is held; counting only ``quantity`` would page about every position bought yesterday."""
    _position(conn, "pos-7", "HDFCAMC", 7)
    sink = _Sink()

    result = await _job(conn, _FakeKite([_holding("HDFCAMC", 0, t1=7)]), notify=sink).run()

    assert result.flagged == []
    assert sink.messages == []


@pytest.mark.asyncio
async def test_missing_quantity_keys_read_as_zero(conn) -> None:
    """A holdings row that omits a quantity key must not raise — it reads 0 and flags."""
    _position(conn, "pos-8", "HDFCAMC", 7)
    sink = _Sink()

    result = await _job(
        conn, _FakeKite([{"tradingsymbol": "HDFCAMC", "exchange": "NSE"}]), notify=sink
    ).run()

    assert result.flagged == ["pos-8"]
    assert sink.messages[0].data["held_qty"] == 0


# --------------------------------------------------------------------------- (iv) T+1 young
@pytest.mark.asyncio
async def test_position_opened_yesterday_is_skipped_not_flagged(conn, caplog) -> None:
    """T+1: a fresh buy is not in holdings yet. Under two COMPLETED sessions ⇒ counted, never paged."""
    _position(conn, "pos-9", "HDFCAMC", 7, opened=YOUNG_OPEN)
    sink = _Sink()

    with caplog.at_level(logging.INFO, logger=LOGGER):
        result = await _job(conn, _FakeKite([]), notify=sink).run()

    assert result.skipped_young == 1
    assert result.checked == 0
    assert result.flagged == []
    assert sink.messages == []
    done = [r for r in caplog.records if r.getMessage() == "holdings_reconcile_done"]
    assert done[0].skipped_young == 1 and done[0].checked == 0


# --------------------------------------------------------------------------- (v) once per day
@pytest.mark.asyncio
async def test_second_run_same_day_logs_again_but_alerts_once(conn, caplog) -> None:
    """Hourly cadence, one page per position per trading day; the log stays the evidence trail."""
    _position(conn, "pos-10", "HDFCAMC", 7)
    _ledger(conn, "led-10", "pos-10", "REC-10")
    sink = _Sink()
    job = _job(conn, _FakeKite([]), notify=sink)

    with caplog.at_level(logging.INFO, logger=LOGGER):
        first = await job.run()
        second = await job.run()

    assert first.flagged == ["pos-10"] and second.flagged == ["pos-10"]
    assert len(sink.messages) == 1                              # alerted once
    flagged = [r for r in caplog.records if r.getMessage() == "position_not_in_holdings"]
    assert len(flagged) == 2                                    # logged every run
    assert len([r for r in caplog.records if r.getMessage() == "holdings_reconcile_done"]) == 2


# --------------------------------------------------------------------------- (vi) broker error
@pytest.mark.asyncio
async def test_broker_failure_degrades_to_a_warning_and_flags_nothing(conn, caplog) -> None:
    """Diagnostic, never a gate input: a dead token must not imply every position was sold; it returns ``error`` and pages nobody."""
    _position(conn, "pos-11", "HDFCAMC", 7)
    sink = _Sink()

    with caplog.at_level(logging.INFO, logger=LOGGER):
        result = await _job(
            conn, _FakeKite(raises=TokenException("Incorrect `api_key` or `access_token`.")),
            notify=sink,
        ).run()                                                  # must not raise

    assert result.error is not None and "TokenException" in result.error
    assert result.flagged == [] and result.checked == 0
    assert sink.messages == []
    failed = [r for r in caplog.records if r.getMessage() == "holdings_reconcile_failed"]
    assert len(failed) == 1
    assert failed[0].levelno == logging.WARNING
    assert failed[0].error_type == "TokenException"


@pytest.mark.asyncio
async def test_any_broker_error_is_swallowed_too(conn) -> None:
    _position(conn, "pos-12", "HDFCAMC", 7)

    result = await _job(conn, _FakeKite(raises=RuntimeError("connection reset")), notify=_Sink()).run()

    assert "RuntimeError" in (result.error or "")
    assert result.flagged == []


# --------------------------------------------------------------------------- (vii) scope
@pytest.mark.asyncio
async def test_mis_external_and_closed_positions_are_out_of_scope(conn) -> None:
    """MIS never reaches holdings, ``external`` positions are not the platform's to track, and a CLOSED row is already reconciled."""
    _position(conn, "pos-mis", "HDFCAMC", 7, product="MIS")
    _position(conn, "pos-ext", "HINDZINC", 7, origin="external")
    _position(conn, "pos-closed", "TITAN", 7, state="CLOSED")
    sink = _Sink()

    result = await _job(conn, _FakeKite([]), notify=sink).run()

    assert result.checked == 0
    assert result.flagged == []
    assert sink.messages == []


@pytest.mark.asyncio
async def test_platform_origin_is_in_scope(conn) -> None:
    """Both tracked origins count (``platform`` positions are AUTO-mode fills)."""
    _position(conn, "pos-plat", "HDFCAMC", 7, origin="platform")

    result = await _job(conn, _FakeKite([]), notify=_Sink()).run()

    assert result.checked == 1 and result.flagged == ["pos-plat"]


@pytest.mark.asyncio
async def test_notify_failure_never_breaks_the_run(conn) -> None:
    """A Telegram outage must not raise on the scheduler thread (same posture as ``token_check``)."""
    _position(conn, "pos-13", "HDFCAMC", 7)

    async def _boom(_msg) -> None:
        raise RuntimeError("telegram is down")

    result = await _job(conn, _FakeKite([]), notify=_boom).run()

    assert result.flagged == ["pos-13"]


# --------------------------------------------------------------------------- the in-session window
def test_reconcile_window_is_a_trading_day_between_0920_and_1530() -> None:
    """The hourly tick is in-session only; after the close the EOD path owns the day."""
    clock = _clock()
    cal = _calendar(clock)
    assert in_reconcile_window(NOW, cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 9, 19, tzinfo=IST), cal) is False
    assert in_reconcile_window(datetime(2026, 6, 17, 9, 20, tzinfo=IST), cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 15, 30, tzinfo=IST), cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 15, 31, tzinfo=IST), cal) is False
    assert in_reconcile_window(datetime(2026, 6, 20, 10, 5, tzinfo=IST), cal) is False   # Saturday


# =========================================================================== the journal
#: conftest/NOW's trading date, and the two sessions before it (Mon 15 / Tue 16 / Wed 17 June 2026).
TODAY = NOW.date()
YESTERDAY = date(2026, 6, 16)
DAY_BEFORE = date(2026, 6, 15)


def _observe(conn, position_id: str, d: date, tracked: int, held: int, *, at: str = "10:05:00") -> None:
    """Seed one ``holdings_observations`` row directly (the missing-set reader is pure SQL+Python)."""
    conn.execute(
        "INSERT INTO holdings_observations (position_id, d, tracked_qty, held_qty, observed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (position_id, d.isoformat(), tracked, held, f"{d.isoformat()}T{at}+05:30"),
    )
    conn.commit()


def _observations(conn) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT position_id, d, tracked_qty, held_qty FROM holdings_observations "
            "ORDER BY position_id, d"
        ).fetchall()
    ]


@pytest.mark.asyncio
async def test_every_checked_position_is_journalled_including_the_held_ones(conn) -> None:
    """One row per checked position per run, held ones too: a flagged-only journal could never express "it came back"."""
    _position(conn, "pos-j1", "HDFCAMC", 7)
    _position(conn, "pos-j2", "HINDZINC", 3)
    _position(conn, "pos-young", "TITAN", 5, opened=YOUNG_OPEN)

    result = await _job(conn, _FakeKite([_holding("HINDZINC", 3)]), notify=_Sink()).run()

    assert result.checked == 2 and result.observed == 2
    # The T+1-young position is not observed: "held 0" for an unsettled buy would fabricate a streak the missing rule reads as a sale.
    assert _observations(conn) == [
        ("pos-j1", TODAY.isoformat(), 7, 0),
        ("pos-j2", TODAY.isoformat(), 3, 3),
    ]


@pytest.mark.asyncio
async def test_the_last_run_of_the_day_wins(conn) -> None:
    """One row per day, the latest observation wins: a 09:20 "not held" is superseded by the settled leg at 14:00."""
    _position(conn, "pos-j3", "HDFCAMC", 7)

    await _job(conn, _FakeKite([]), notify=_Sink()).run()
    assert _observations(conn) == [("pos-j3", TODAY.isoformat(), 7, 0)]

    later = _job(conn, _FakeKite([_holding("HDFCAMC", 7)]), at=NOW + timedelta(hours=4), notify=_Sink())
    await later.run()

    assert _observations(conn) == [("pos-j3", TODAY.isoformat(), 7, 7)]
    stamp = conn.execute("SELECT observed_at FROM holdings_observations").fetchone()[0]
    assert stamp == (NOW + timedelta(hours=4)).isoformat()


@pytest.mark.asyncio
async def test_a_broker_failure_journals_nothing(conn) -> None:
    """A dead token must not write "held 0" for every tracked position (two such runs would silence the exit path)."""
    _position(conn, "pos-j4", "HDFCAMC", 7)

    result = await _job(conn, _FakeKite(raises=TokenException("bad token")), notify=_Sink()).run()

    assert result.error is not None and result.observed == 0
    assert _observations(conn) == []


# =========================================================================== the missing set
def test_one_missing_day_is_not_enough(conn) -> None:
    """A single short reading has innocent explanations (settlement edge, rename, truncated payload) and the consequence is silence about a live position, so one day never qualifies."""
    _observe(conn, "pos-m1", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == set()


def test_a_partial_holding_counts_as_short(conn) -> None:
    """Held 3 of 7 on both days is short; the day plan states the quantity the broker showed, not zero."""
    _observe(conn, "pos-m3", YESTERDAY, tracked=7, held=3)
    _observe(conn, "pos-m3", TODAY, tracked=7, held=3)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-m3"}
    seen = missing_holdings_observations(conn, TODAY)["pos-m3"]
    assert (seen.sessions, seen.tracked_qty, seen.held_qty, seen.d) == (2, 7, 3, TODAY.isoformat())


def test_a_recovered_holding_leaves_the_set_immediately(conn) -> None:
    """The streak ends at the first fully-held day: two days short then held today => out, and the platform resumes managing it at once."""
    _observe(conn, "pos-m4", DAY_BEFORE, tracked=7, held=0)
    _observe(conn, "pos-m4", YESTERDAY, tracked=7, held=0)
    _observe(conn, "pos-m4", TODAY, tracked=7, held=7)

    assert set(positions_missing_from_holdings(conn, TODAY)) == set()
    assert "pos-m4" not in missing_holdings_observations(conn, TODAY)


def test_observations_older_than_the_lookback_do_not_qualify(conn) -> None:
    """Two short days a fortnight ago say nothing about today (likely engine downtime); a stale streak must not silence the exit path."""
    _observe(conn, "pos-m5", TODAY - timedelta(days=14), tracked=7, held=0)
    _observe(conn, "pos-m5", TODAY - timedelta(days=13), tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == set()
    # A wide enough lookback qualifies the same rows.
    assert set(positions_missing_from_holdings(conn, TODAY, lookback_days=30)) == {"pos-m5"}


def test_a_gap_inside_the_lookback_still_qualifies_on_the_two_most_recent_days(conn) -> None:
    """Consecutive observation days, not calendar days: a weekend, holiday or downtime does not reset the evidence."""
    _observe(conn, "pos-m6", TODAY - timedelta(days=5), tracked=7, held=0)
    _observe(conn, "pos-m6", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-m6"}


def test_future_dated_observations_are_ignored(conn) -> None:
    """A clock-skewed or replayed write stamped ahead of today is not evidence about today."""
    _observe(conn, "pos-m7", TODAY, tracked=7, held=0)
    _observe(conn, "pos-m7", TODAY + timedelta(days=1), tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == set()


def test_a_position_with_no_observations_is_never_missing(conn) -> None:
    """The load-bearing asymmetry: no evidence => the platform keeps managing the position."""
    _position(conn, "pos-unseen", "INFY", 7)                      # open and tracked, never observed
    _position(conn, "pos-gone", "TCS", 7)
    _observe(conn, "pos-gone", YESTERDAY, tracked=7, held=0)
    _observe(conn, "pos-gone", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-gone"}


def test_zero_sessions_cannot_silence_everything(conn) -> None:
    """A mis-configured ``sessions=0`` degrades to "nothing is missing", never "everything is" (the consequence is the platform going quiet)."""
    _observe(conn, "pos-m8", YESTERDAY, tracked=7, held=0)
    _observe(conn, "pos-m8", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY, sessions=0)) == set()


@pytest.mark.asyncio
async def test_the_job_feeds_its_own_missing_set_across_two_sessions(conn) -> None:
    """End to end: two reconcile runs on two trading days satisfy the missing rule; only the job seeds the journal."""
    _position(conn, "pos-m9", "HDFCAMC", 7)

    await _job(conn, _FakeKite([]), at=datetime(2026, 6, 16, 10, 5, tzinfo=IST), notify=_Sink()).run()
    assert set(positions_missing_from_holdings(conn, YESTERDAY)) == set()

    await _job(conn, _FakeKite([]), notify=_Sink()).run()
    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-m9"}


# ================================================== "short" and "gone" are not the same
def test_a_partial_holding_is_short_but_not_gone(conn) -> None:
    """``require_zero`` separates telling the owner from making the platform go quiet: held 3 of 7 on both days
    is a real unreported exit (alert and day plan say so), but 3 shares of exposure still need their stop watched."""
    _observe(conn, "pos-part", YESTERDAY, tracked=7, held=3)
    _observe(conn, "pos-part", TODAY, tracked=7, held=3)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-part"}
    assert set(positions_missing_from_holdings(conn, TODAY, require_zero=True)) == set()
    seen = missing_holdings_observations(conn, TODAY)["pos-part"]
    assert (seen.sessions, seen.zero_sessions) == (2, 0)


def test_a_partial_exit_reads_short_forever_but_is_never_gone(conn) -> None:
    """Held 2 of 7 is an unreported partial exit: an unbounded streak under the wide predicate (the owner keeps
    hearing of it), but it must never silence the exit path of a position with real residual exposure."""
    for n in range(5):
        _observe(conn, "pos-partial", TODAY - timedelta(days=4 - n), tracked=7, held=2)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-partial"}
    assert missing_holdings_observations(conn, TODAY)["pos-partial"].sessions == 5
    assert set(positions_missing_from_holdings(conn, TODAY, require_zero=True)) == set()


def test_pledged_shares_count_as_held() -> None:
    """Kite moves shares pledged for margin out of ``quantity`` into ``collateral_quantity``; without counting
    that bucket a fully pledged position reads held 0 and, two sessions later, silences its own exit path."""
    rows = [
        _holding("PLEDGED", 0, collateral=7),
        _holding("MIXED", 3, t1=1, collateral=3),
        {"tradingsymbol": "PLEDGED", "exchange": "BSE", "quantity": 0, "collateral_quantity": 2},
    ]
    assert _held_quantities(rows) == {"PLEDGED": 9, "MIXED": 7}


@pytest.mark.asyncio
async def test_a_fully_pledged_position_is_not_flagged(conn) -> None:
    """A tracked 7 reported entirely under ``collateral_quantity`` is held in full: no alert, no short observation."""
    _position(conn, "pos-8", "HDFCAMC", 7)
    job = _job(conn, _FakeKite([_holding("HDFCAMC", 0, collateral=7)]))

    result = await job.run()

    assert result.checked == 1
    assert result.flagged == []
    assert set(positions_missing_from_holdings(conn, TODAY)) == set()


def test_the_zero_run_is_a_prefix_and_needs_its_own_two_days(conn) -> None:
    """Held 3 yesterday, 0 today: only gone today. The two-day rule applies to the zero run in its own right:
    the wide predicate qualifies at once, the narrow one waits a second zero day."""
    _observe(conn, "pos-drain", YESTERDAY, tracked=7, held=3)
    _observe(conn, "pos-drain", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-drain"}
    assert set(positions_missing_from_holdings(conn, TODAY, require_zero=True)) == set()
    seen = missing_holdings_observations(conn, TODAY)["pos-drain"]
    assert (seen.sessions, seen.zero_sessions) == (2, 1)

    _observe(conn, "pos-drain", TODAY + timedelta(days=1), tracked=7, held=0)
    tomorrow = TODAY + timedelta(days=1)
    assert set(positions_missing_from_holdings(conn, tomorrow, require_zero=True)) == {"pos-drain"}


def test_two_zero_days_are_gone_under_both_readings(conn) -> None:
    """The broker holds nothing, twice: both predicates agree, the only state in which the position-event path goes quiet."""
    _observe(conn, "pos-gone", YESTERDAY, tracked=7, held=0)
    _observe(conn, "pos-gone", TODAY, tracked=7, held=0)

    assert set(positions_missing_from_holdings(conn, TODAY)) == {"pos-gone"}
    assert set(positions_missing_from_holdings(conn, TODAY, require_zero=True)) == {"pos-gone"}
    assert missing_holdings_observations(conn, TODAY)["pos-gone"].zero_sessions == 2


# ================================================== the systemic "nothing is held" shape
@pytest.mark.asyncio
async def test_a_whole_book_reading_short_gets_its_own_log_line(conn, caplog) -> None:
    """A successful holdings call returning nothing useful (API shape change, truncated page, wrong-account token)
    looks like "sold everything" and would silence the whole book; this line says "look at the feed"."""
    _position(conn, "pos-a1", "HDFCAMC", 7)
    _position(conn, "pos-a2", "HINDZINC", 3)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        result = await _job(conn, _FakeKite([]), notify=_Sink()).run()

    assert result.checked == 2 and len(result.flagged) == 2
    systemic = [r for r in caplog.records if r.getMessage() == "holdings_reconcile_all_short"]
    assert len(systemic) == 1
    assert systemic[0].levelno == logging.WARNING
    assert systemic[0].checked == 2 and systemic[0].holdings_rows == 0


@pytest.mark.asyncio
async def test_one_position_short_beside_a_held_one_is_not_systemic(conn, caplog) -> None:
    """One name missing while another is held is an owner who sold one position: the per-position alert's job, not this channel's."""
    _position(conn, "pos-a3", "HDFCAMC", 7)
    _position(conn, "pos-a4", "HINDZINC", 3)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        await _job(conn, _FakeKite([_holding("HINDZINC", 3)]), notify=_Sink()).run()

    assert not [r for r in caplog.records if r.getMessage() == "holdings_reconcile_all_short"]


# ================================================== only TRADING days are journalled
@pytest.mark.asyncio
async def test_a_non_trading_day_run_alerts_but_journals_nothing(conn) -> None:
    """``post_login._step_holdings`` runs this job ungated (only the hourly tick applies ``in_reconcile_window``),
    so a weekend run must not supply the second "session" of a streak: no row is written, the alert and log still fire."""
    _position(conn, "pos-sat", "HDFCAMC", 7)
    saturday = datetime(2026, 6, 20, 11, 0, tzinfo=IST)
    sink = _Sink()

    result = await _job(conn, _FakeKite([]), at=saturday, notify=sink).run()

    assert result.checked == 1 and result.flagged == ["pos-sat"] and result.observed == 0
    assert len(sink.messages) == 1                      # the owner is still told
    assert _observations(conn) == []


@pytest.mark.asyncio
async def test_a_weekend_boot_cannot_complete_a_two_session_streak(conn) -> None:
    """Friday's single short reading plus a Saturday redeploy must not add up to "sold outside the ledger" on Monday."""
    _position(conn, "pos-fri", "HDFCAMC", 7)
    friday = datetime(2026, 6, 19, 15, 20, tzinfo=IST)
    saturday = datetime(2026, 6, 20, 11, 0, tzinfo=IST)
    monday = date(2026, 6, 22)

    await _job(conn, _FakeKite([]), at=friday, notify=_Sink()).run()
    await _job(conn, _FakeKite([]), at=saturday, notify=_Sink()).run()

    assert [r[1] for r in _observations(conn)] == ["2026-06-19"]
    assert set(positions_missing_from_holdings(conn, monday)) == set()


def test_entry_rec_id_is_the_one_lookup_both_surfaces_use(conn) -> None:
    """The reconcile alert and the day plan's "sold outside the ledger" block name the same id via this shared function."""
    _position(conn, "pos-m10", "HDFCAMC", 7)
    _ledger(conn, "led-exit", "pos-m10", "REC-EXIT", entry_px=None,
            created_at="2026-06-09T10:00:00+05:30")
    _ledger(conn, "led-entry", "pos-m10", "REC-ENTRY", entry_px="100",
            created_at="2026-06-10T10:00:00+05:30")

    assert entry_rec_id(conn, "pos-m10") == "REC-ENTRY"
    assert entry_rec_id(conn, "pos-unknown") is None
