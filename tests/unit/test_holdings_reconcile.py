"""Holdings reconcile for tracked CNC positions (§3.6, owner-directed 2026-09-07).

HDFCAMC and HINDZINC carried 39 expired exit recommendations across eleven sessions because nothing
could tell "the owner is ignoring the exit" from "the owner sold and never sent ``/closed``". These
tests pin the diagnostic that answers that question, and — just as load-bearing — the four ways it
must stay QUIET: a T+1-young position, a fully-held one, a second run on the same day, and a broker
failure. A false "you sold this" page costs the owner trust in every later alert.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pytest
from kiteconnect.exceptions import TokenException

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops.holdings_reconcile import HoldingsReconcileJob, in_reconcile_window

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


def _holding(symbol: str, quantity: int, t1: int = 0) -> dict:
    return {"tradingsymbol": symbol, "exchange": "NSE", "quantity": quantity, "t1_quantity": t1}


# --------------------------------------------------------------------------- (i) absent symbol
@pytest.mark.asyncio
async def test_absent_symbol_is_flagged_and_names_the_closed_reply(conn, caplog) -> None:
    """The whole product: a tracked position the broker no longer holds ⇒ ONE alert the owner can
    act on from the phone — the entry rec id and the literal ``/closed`` reply."""
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
    """No ledger row ⇒ the alert cannot name a rec id, so it says so and gives the position id —
    silence would put the position right back in the eleven-session limbo this job exists to end."""
    _position(conn, "pos-2", "HINDZINC", 5)
    sink = _Sink()

    result = await _job(conn, _FakeKite([]), notify=sink).run()

    assert result.flagged == ["pos-2"]
    body = sink.messages[0].body
    assert "pos-2" in body
    assert sink.messages[0].data["entry_rec_id"] is None


@pytest.mark.asyncio
async def test_entry_row_wins_over_a_later_exit_ledger_row(conn) -> None:
    """``/closed`` accepts either id, but the ENTRY rec is the one the owner was told about first —
    the row carrying ``entry_px`` is the entry (mirrors ``pipeline.close``'s own preference)."""
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
    """Held 3 of 7: a partial exit is still an unreported exit — the ledger's qty is now fiction."""
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
    """T+1 settlement: a buy still in ``t1_quantity`` IS held. Counting only ``quantity`` would page
    the owner about every position bought yesterday — the exact false positive the age gate and this
    rule both exist to prevent."""
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
    """Hourly cadence, one page per position per trading day: the log stays the evidence trail, the
    owner's phone does not buzz twelve times about one unsold-or-sold HDFCAMC."""
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
    """Diagnostic, never a gate input (§3.6): a dead token cannot be allowed to invent a story that
    every tracked position was sold. It returns ``error`` and pages nobody."""
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
    """MIS never reaches holdings (intraday), ``external`` positions are not the platform's to track
    (R5), and a CLOSED row is already reconciled. None of them may produce a page."""
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
    """Both tracked origins count — ``platform`` positions are the platform's own AUTO-mode fills."""
    _position(conn, "pos-plat", "HDFCAMC", 7, origin="platform")

    result = await _job(conn, _FakeKite([]), notify=_Sink()).run()

    assert result.checked == 1 and result.flagged == ["pos-plat"]


@pytest.mark.asyncio
async def test_notify_failure_never_breaks_the_run(conn) -> None:
    """The check is diagnostic; a Telegram outage must not turn it into an exception on the
    scheduler thread (same posture as ``token_check``)."""
    _position(conn, "pos-13", "HDFCAMC", 7)

    async def _boom(_msg) -> None:
        raise RuntimeError("telegram is down")

    result = await _job(conn, _FakeKite([]), notify=_boom).run()

    assert result.flagged == ["pos-13"]


# --------------------------------------------------------------------------- the in-session window
def test_reconcile_window_is_a_trading_day_between_0920_and_1530() -> None:
    """The hourly tick is in-session only: nothing to reconcile before the open, and after the close
    the EOD path owns the day."""
    clock = _clock()
    cal = _calendar(clock)
    assert in_reconcile_window(NOW, cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 9, 19, tzinfo=IST), cal) is False
    assert in_reconcile_window(datetime(2026, 6, 17, 9, 20, tzinfo=IST), cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 15, 30, tzinfo=IST), cal) is True
    assert in_reconcile_window(datetime(2026, 6, 17, 15, 31, tzinfo=IST), cal) is False
    assert in_reconcile_window(datetime(2026, 6, 20, 10, 5, tzinfo=IST), cal) is False   # Saturday
