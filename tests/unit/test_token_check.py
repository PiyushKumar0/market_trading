"""Pre-open Kite token validity check (WO-21 (iii), R6/A5).

2026-08-20: the daily access token was already stale at the open and NOTHING looked at it — the
first rejection surfaced at 09:50 as a mid-session TokenException → FROZEN, ~108 minutes of the
trade window gone. These tests pin the four outcomes of the 08:40 probe: ok (silent), rejected
(CRITICAL + critical alert naming /token and the login link), any other error (WARNING + warning
alert, and NEVER a raise — the check must not be load-bearing), and a non-trading-day skip.
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
from engine.ops.token_check import TOKEN_CHECK_IST, TokenCheckJob

#: 08:40 IST on Wed 2026-06-17 (a real trading day) — the job's own fire-time.
PRE_OPEN = datetime(2026, 6, 17, 8, 40, tzinfo=IST)
#: Same clock time on Sat 2026-06-20 — a non-trading day.
WEEKEND = datetime(2026, 6, 20, 8, 40, tzinfo=IST)

LOGIN_URL = "https://kite.zerodha.com/connect/login?api_key=abc&v=3"


class _FakeKite:
    """Minimal stand-in for the KiteClient facade: only ``margins()`` is probed."""

    def __init__(self, raises: BaseException | None = None) -> None:
        self._raises = raises
        self.calls = 0

    async def margins(self):
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return {"equity": {"net": 20000}}


class _Sink:
    def __init__(self, *, boom: bool = False) -> None:
        self.messages: list[CatalogMessage] = []
        self._boom = boom

    async def __call__(self, msg: CatalogMessage) -> None:
        self.messages.append(msg)
        if self._boom:
            raise RuntimeError("telegram is down")


def _clock(at: datetime) -> Clock:
    return Clock(time_source=lambda: at)


def _calendar(clock: Clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


def _job(kite, *, at: datetime = PRE_OPEN, notify=None, login_url=lambda: LOGIN_URL) -> TokenCheckJob:
    clock = _clock(at)
    return TokenCheckJob(
        kite=kite, clock=clock, calendar=_calendar(clock),
        notify=notify if notify is not None else _Sink(), login_url=login_url,
    )


def test_fire_time_is_before_the_open() -> None:
    """The whole point is time-to-act: the probe must land ahead of the 09:15 open."""
    assert TOKEN_CHECK_IST < datetime(2026, 6, 17, 9, 15).time()


@pytest.mark.asyncio
async def test_valid_token_logs_ok_and_never_bothers_the_owner(caplog) -> None:
    kite, sink = _FakeKite(), _Sink()

    with caplog.at_level(logging.INFO, logger="engine.ops.token_check"):
        await _job(kite, notify=sink).run()

    assert kite.calls == 1
    assert sink.messages == []                      # a healthy token is silent — no daily noise
    ok = [r for r in caplog.records if r.getMessage() == "token_check_ok"]
    assert len(ok) == 1
    assert ok[0].date == "2026-06-17"
    assert ok[0].at.startswith("2026-06-17T08:40")


@pytest.mark.asyncio
async def test_rejected_token_alerts_the_owner_critically(caplog) -> None:
    kite, sink = _FakeKite(TokenException("Incorrect `api_key` or `access_token`.")), _Sink()

    with caplog.at_level(logging.INFO, logger="engine.ops.token_check"):
        await _job(kite, notify=sink).run()

    failed = [r for r in caplog.records if r.getMessage() == "token_check_failed"]
    assert len(failed) == 1
    assert failed[0].levelno == logging.CRITICAL

    assert len(sink.messages) == 1
    msg = sink.messages[0]
    assert msg.severity == "critical"
    assert msg.kind is MessageKind.LOGIN_PROMPT
    assert "re-login before open" in msg.title.lower()
    assert "(R6)" in msg.title
    # The owner needs the two things that let them ACT from the phone: the link and the command.
    assert LOGIN_URL in msg.body
    assert "/token" in msg.body
    assert msg.data["outcome"] == "rejected"


@pytest.mark.asyncio
async def test_network_error_warns_without_claiming_the_token_is_dead(caplog) -> None:
    """"Cannot verify" is not "invalid" (the same discipline SessionManager.verify_token uses): a
    DNS blip at 08:40 must not send the owner a critical re-login page."""
    kite, sink = _FakeKite(RuntimeError("connection reset by peer")), _Sink()

    with caplog.at_level(logging.INFO, logger="engine.ops.token_check"):
        await _job(kite, notify=sink).run()       # must not raise

    errors = [r for r in caplog.records if r.getMessage() == "token_check_error"]
    assert len(errors) == 1
    assert errors[0].levelno == logging.WARNING
    assert errors[0].error_type == "RuntimeError"

    assert len(sink.messages) == 1
    msg = sink.messages[0]
    assert msg.severity == "warning"
    assert "inconclusive" in msg.title.lower()
    assert "UNVERIFIED" in msg.body
    assert msg.data["outcome"] == "error"
    # And no critical was logged alongside it.
    assert not [r for r in caplog.records if r.getMessage() == "token_check_failed"]


@pytest.mark.asyncio
async def test_non_trading_day_skips_silently(caplog) -> None:
    kite, sink = _FakeKite(), _Sink()

    with caplog.at_level(logging.INFO, logger="engine.ops.token_check"):
        await _job(kite, at=WEEKEND, notify=sink).run()

    assert kite.calls == 0                          # no broker call at all
    assert sink.messages == []
    assert [r.getMessage() for r in caplog.records if r.name == "engine.ops.token_check"] == [
        "token_check_skipped_non_trading_day"
    ]


@pytest.mark.asyncio
async def test_a_failed_notification_never_escapes(caplog) -> None:
    """Alerting is part of the check, and the check is never load-bearing: a dead Telegram must not
    turn the 08:40 job into an exception on the scheduler thread."""
    kite, sink = _FakeKite(TokenException("bad token")), _Sink(boom=True)

    with caplog.at_level(logging.INFO, logger="engine.ops.token_check"):
        await _job(kite, notify=sink).run()

    assert len(sink.messages) == 1
    assert [r for r in caplog.records if r.getMessage() == "token_check_notify_failed"]


@pytest.mark.asyncio
async def test_missing_login_url_degrades_to_the_token_command() -> None:
    """A raising/absent ``login_url`` (no api_key) still produces a usable alert."""
    def boom() -> str:
        raise RuntimeError("no api key")

    sink = _Sink()
    await _job(_FakeKite(TokenException("bad token")), notify=sink, login_url=boom).run()
    assert "/token" in sink.messages[0].body
    assert "http" not in sink.messages[0].body

    sink2 = _Sink()
    await _job(_FakeKite(TokenException("bad token")), notify=sink2, login_url=None).run()
    assert "/token" in sink2.messages[0].body
