"""Notification journal + Telegram retry outbox + the notifications page.

A ``notifications`` row is written BEFORE the wire attempt; it is both the retry outbox and what the owner's page reads.

* **Journal first.** The row exists even when the transport is dead / not started; the split into
  Telegram-sized parts is a transport detail, one notification is one row.
* **A failure is `pending`, not gone.** attempts+1 and ``last_error`` recorded; only expiry may move a
  row to ``failed``, and a CRITICAL row (recommendation, login prompt, freeze) never expires.
* **Journalling never breaks a send.** No store, or a broken store, and the send proceeds.
* **The page is bearer-authed data on an unauthenticated shell.** ``GET /notifications`` 401s without
  the token; the static page carries no data and no off-box dependency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta
from html.parser import HTMLParser

import pytest
from fastapi.testclient import TestClient

from engine.api.app import create_app
from engine.core.clock import IST, Clock
from engine.core.config import repo_root
from engine.core.secrets import DASHBOARD_TOKEN
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.notify.telegram import (
    _CONNECT_TIMEOUT_S,
    _DRAIN_BATCH,
    _DRAIN_SCAN,
    _POOL_TIMEOUT_S,
    _READ_TIMEOUT_S,
    _SEND_TIMEOUT_S,
    _WRITE_TIMEOUT_S,
    CRITICAL_KINDS,
    TelegramBot,
    _build_request,
    _is_critical,
    _wire_text,
)

OWNER_CHAT = 4242
_TOKEN = "s3cret-dash-token"
AUTH = {"Authorization": f"Bearer {_TOKEN}"}
UI_HTML = repo_root() / "web" / "dist" / "index.html"


# --------------------------------------------------------------------------- test doubles
class _MovableClock:
    """A Clock whose "now" the test advances (the drainer's backoff and 6 h expiry are clock-driven)."""

    def __init__(self, start):
        self._now = start
        self.clock = Clock(time_source=lambda: self._now)

    def advance(self, **kw) -> None:
        self._now = self._now + timedelta(**kw)

    def now(self):
        return self._now


class _ScriptedBot:
    """Stand-in for ``app.bot``. ``failures`` sends raise before any succeed; ``fail_forever`` never
    lets one through (the outage case)."""

    def __init__(self, *, failures: int = 0, fail_forever: bool = False) -> None:
        self.sent: list[str] = []
        self.markups: list = []
        self.attempts = 0
        self._failures = failures
        self._fail_forever = fail_forever

    async def send_message(self, chat_id: int, text: str, reply_markup=None) -> None:
        self.attempts += 1
        if self._fail_forever or self.attempts <= self._failures:
            raise ConnectionError("httpx.ConnectTimeout: telegram.org timed out")
        self.sent.append(text)
        self.markups.append(reply_markup)


class _App:
    """Started-transport stand-in: carries the bot, plus the no-op teardown surface ``stop()`` walks."""

    def __init__(self, bot: _ScriptedBot) -> None:
        self.bot = bot
        self.updater = None

    async def stop(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None


class _FakeSecrets:
    def __init__(self, token: str | None) -> None:
        self._token = token

    def get(self, key: str) -> str:
        if key == DASHBOARD_TOKEN and self._token is not None:
            return self._token
        raise KeyError(key)

    def has(self, key: str) -> bool:
        return key == DASHBOARD_TOKEN and self._token is not None


# --------------------------------------------------------------------------- fixtures / helpers
@pytest.fixture
def moving(clock) -> _MovableClock:
    return _MovableClock(clock.now())


def _bot(conn, clock, sender: _ScriptedBot | None = None) -> tuple[TelegramBot, _ScriptedBot]:
    """A journalling bot with a started-transport stand-in (``send`` is otherwise a no-op)."""
    sender = sender if sender is not None else _ScriptedBot()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)
    bot._app = _App(sender)
    return bot, sender


def _rows(conn) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM notifications ORDER BY created_at ASC").fetchall()


def _one(conn) -> sqlite3.Row:
    rows = _rows(conn)
    assert len(rows) == 1, f"expected exactly one journal row, got {len(rows)}"
    return rows[0]


def _insert(conn, notification_id: str, created_at: str, **kw) -> None:
    row = {
        "kind": MessageKind.FEED_DEGRADED.value, "severity": "warning", "title": "T", "body": "B",
        "status": "pending", "attempts": 0, "delivered_at": None, "last_error": None,
        "last_attempt_at": None,
    }
    row.update(kw)
    conn.execute(
        "INSERT INTO notifications (notification_id, created_at, kind, severity, title, body, "
        "status, attempts, delivered_at, last_error, last_attempt_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (notification_id, created_at, *[row[k] for k in (
            "kind", "severity", "title", "body", "status", "attempts", "delivered_at", "last_error",
            "last_attempt_at")]),
    )


# --------------------------------------------------------------------------- journal write
@pytest.mark.asyncio
async def test_send_journals_then_delivers(conn, clock):
    """One row, written pending before the wire, stamped delivered after."""
    bot, sender = _bot(conn, clock)
    msg = catalog.engine_started(mode="RECOMMEND", version="1.2.3")

    await bot.send(msg)

    row = _one(conn)
    assert row["status"] == "delivered"
    assert row["kind"] == MessageKind.ENGINE_STARTED.value and row["severity"] == "info"
    assert row["title"] == msg.title and row["body"] == msg.body
    assert row["attempts"] == 1                       # the successful attempt is counted, not skipped
    assert row["delivered_at"] == clock.now().isoformat()
    assert row["last_error"] is None
    assert sender.sent == [msg.render()]


@pytest.mark.asyncio
async def test_journalled_row_reconstructs_the_exact_wire_text(conn, clock):
    """A retry ships the same bytes as the first attempt: the journal stores render inputs, pinned against ``CatalogMessage.render``."""
    bot, sender = _bot(conn, clock)
    for msg in (
        catalog.login_prompt("kite-login-url"),
        catalog.feed_stale(7.5),
        catalog.catalyst_disabled("digest stale"),
    ):
        await bot.send(msg)
    for row, sent in zip(_rows(conn), sender.sent, strict=True):
        assert _wire_text(row["severity"], row["title"], row["body"]) == sent


@pytest.mark.asyncio
async def test_plain_string_send_is_journalled_and_round_trips(conn, clock):
    """A bare string (the ``alert()`` path): first line is the title, the rest the body; reconstruction is the identity."""
    bot, sender = _bot(conn, clock)
    text = "dashboard/login API failed to bind\nport 8400 already in use"

    await bot.send(text)

    row = _one(conn)
    assert row["kind"] is None and row["severity"] is None
    assert row["title"] == "dashboard/login API failed to bind"
    assert row["body"] == "port 8400 already in use"
    assert _wire_text(row["severity"], row["title"], row["body"]) == text
    assert sender.sent == [text]


@pytest.mark.asyncio
async def test_failed_send_leaves_the_row_pending_with_the_error(conn, clock):
    """A failed send is queued: status stays pending, attempts is 1, last_error names the exception class."""
    bot, sender = _bot(conn, clock, _ScriptedBot(fail_forever=True))

    await bot.send(catalog.catalyst_disabled("feed outage"))

    row = _one(conn)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["delivered_at"] is None
    assert row["last_attempt_at"] == clock.now().isoformat()
    assert row["last_error"].startswith("ConnectionError: httpx.ConnectTimeout")
    assert len(row["last_error"]) <= 200
    assert sender.sent == []


@pytest.mark.asyncio
async def test_send_before_the_transport_starts_is_queued_not_dropped(conn, clock):
    """A not-yet-started bot still logs the drop, but the row is on disk, pending, for the drainer."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)   # no _app assigned

    await bot.send(catalog.login_prompt("kite-login-url"))

    row = _one(conn)
    assert row["status"] == "pending" and row["attempts"] == 1
    assert "NotStarted" in row["last_error"]


@pytest.mark.asyncio
async def test_split_message_is_one_notification_not_five(conn, clock):
    """The 4096-char split is a transport detail: five wire parts, one journal row holding the whole body."""
    bot, sender = _bot(conn, clock)
    body = "X" * 30000
    msg = CatalogMessage(kind=MessageKind.DAILY_SUMMARY, title="Daily summary", body=body)

    await bot.send(msg)

    assert len(sender.sent) == 5                       # split still works, still capped at 5 parts
    assert all(len(part) <= 4096 for part in sender.sent)
    row = _one(conn)
    assert row["status"] == "delivered" and row["attempts"] == 1
    assert row["body"] == body                         # journalled whole, never the truncated wire form


_BUTTONS = [[{"text": "skip: market", "command": "/veto 01M0H8ZXM3PYNAVXF54A5TDGV0 market"}]]


def _with_buttons(body: str = "B", buttons=_BUTTONS) -> CatalogMessage:
    return CatalogMessage(kind=MessageKind.RECOMMENDATION, title="T", body=body, buttons=buttons)


def _callbacks(markup) -> list[list[str]]:
    return [[b.callback_data for b in row] for row in markup.inline_keyboard]


@pytest.mark.asyncio
async def test_send_journals_buttons_and_attaches_markup_to_last_part_only(conn, clock):
    bot, sender = _bot(conn, clock)
    await bot.send(_with_buttons("X" * 9000))

    assert json.loads(_one(conn)["buttons"]) == _BUTTONS
    assert len(sender.markups) > 1 and set(sender.markups[:-1]) == {None}
    assert _callbacks(sender.markups[-1]) == [["/veto 01M0H8ZXM3PYNAVXF54A5TDGV0 market"]]


@pytest.mark.asyncio
async def test_drainer_retry_reattaches_buttons(conn, moving):
    bot, sender = _bot(conn, moving.clock, _ScriptedBot(failures=1))
    await bot.send(_with_buttons())
    moving.advance(seconds=31)
    await bot._drain_outbox_once()

    assert _one(conn)["status"] == "delivered"
    assert _callbacks(sender.markups[0]) == [["/veto 01M0H8ZXM3PYNAVXF54A5TDGV0 market"]]


@pytest.mark.asyncio
async def test_an_oversized_button_ships_the_text_alone_and_the_row_is_delivered(conn, clock):
    bot, sender = _bot(conn, clock)
    await bot.send(_with_buttons(buttons=[[{"text": "x", "command": "/veto " + "A" * 60}]]))
    assert sender.markups == [None] and _one(conn)["status"] == "delivered"


@pytest.mark.asyncio
async def test_a_broken_journal_never_blocks_the_send(conn, clock, caplog):
    """A dropped table costs a WARNING, never the notification."""
    conn.execute("DROP TABLE notifications")
    bot, sender = _bot(conn, clock)

    with caplog.at_level(logging.WARNING):
        await bot.send(catalog.feed_stale(9.0))        # must not raise

    assert len(sender.sent) == 1
    assert any(r.getMessage() == "notification_journal_failed" for r in caplog.records)


@pytest.mark.asyncio
async def test_an_unwired_state_store_sends_exactly_as_before(clock):
    """No conn ⇒ no journal, plain send (the ``wired_bot`` fixture in ``test_telegram_commands`` has this shape)."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    sender = _ScriptedBot()
    bot._app = _App(sender)

    await bot.send("hello")

    assert sender.sent == ["hello"]


# --------------------------------------------------------------------------- outbox drainer
@pytest.mark.asyncio
async def test_drainer_retries_a_failed_row_and_marks_it_delivered(conn, moving):
    """Fail once, wait out the 30 s backoff, and the next drain delivers; attempts counts both tries."""
    sender = _ScriptedBot(failures=1)
    bot, _ = _bot(conn, moving.clock, sender)

    await bot.send(catalog.login_prompt("kite-login-url"))
    assert _one(conn)["status"] == "pending" and _one(conn)["attempts"] == 1

    moving.advance(seconds=10)
    await bot._drain_outbox_once()                     # still inside the 30 s backoff ⇒ untouched
    assert _one(conn)["attempts"] == 1 and sender.attempts == 1

    moving.advance(seconds=25)
    await bot._drain_outbox_once()

    row = _one(conn)
    assert row["status"] == "delivered" and row["attempts"] == 2
    assert row["last_error"] is None                   # cleared on delivery, not left as history
    assert sender.sent == [catalog.login_prompt("kite-login-url").render()]


@pytest.mark.asyncio
async def test_drainer_preserves_order_and_batches_five(conn, moving):
    """Oldest-first, at most five per pass, so a recovered link replays the backlog in order."""
    sender = _ScriptedBot()
    bot, _ = _bot(conn, moving.clock, sender)
    base = moving.now()
    for i in range(7):
        _insert(conn, f"n{i}", (base + timedelta(seconds=i)).isoformat(), title=f"T{i}", body=f"B{i}")

    await bot._drain_outbox_once()
    assert [t.splitlines()[0] for t in sender.sent] == ["⚠️ T0", "⚠️ T1", "⚠️ T2", "⚠️ T3", "⚠️ T4"]

    await bot._drain_outbox_once()
    assert [t.splitlines()[0] for t in sender.sent][5:] == ["⚠️ T5", "⚠️ T6"]
    assert {r["status"] for r in _rows(conn)} == {"delivered"}


@pytest.mark.asyncio
async def test_backoff_doubles_to_a_five_minute_ceiling(conn, moving):
    """min(300, 30 * 2**(attempts-1)) after the last attempt. A row inside its window is skipped, keeping its place."""
    bot, sender = _bot(conn, moving.clock, _ScriptedBot(fail_forever=True))
    now = moving.now()
    _insert(conn, "n1", now.isoformat(), attempts=2, last_attempt_at=now.isoformat())   # due in 60 s

    moving.advance(seconds=59)
    await bot._drain_outbox_once()
    assert sender.attempts == 0

    moving.advance(seconds=2)
    await bot._drain_outbox_once()
    assert sender.attempts == 1 and _one(conn)["attempts"] == 3

    # attempts=20 would be 30 * 2**19 seconds without the ceiling; the ceiling is 300.
    conn.execute("UPDATE notifications SET attempts=20, last_attempt_at=?", (moving.now().isoformat(),))
    moving.advance(seconds=301)
    await bot._drain_outbox_once()
    assert sender.attempts == 2


@pytest.mark.asyncio
async def test_non_critical_row_expires_after_six_hours(conn, moving):
    """A non-critical row still failing after 6 h is marked ``failed``, not retried indefinitely."""
    bot, sender = _bot(conn, moving.clock, _ScriptedBot(fail_forever=True))
    _insert(conn, "n1", moving.now().isoformat(), kind=MessageKind.FEED_DEGRADED.value,
            severity="warning")

    moving.advance(hours=5, minutes=59)
    await bot._drain_outbox_once()
    assert _one(conn)["status"] == "pending"           # not yet — expiry is a 6 h rule, not a 5 h one

    moving.advance(minutes=2)
    await bot._drain_outbox_once()

    assert _one(conn)["status"] == "failed"
    assert sender.attempts == 1                        # attempted while pending, never after expiry


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "severity"),
    [
        (MessageKind.RECOMMENDATION.value, "info"),        # severity understates it — the product itself
        (MessageKind.LOGIN_PROMPT.value, "critical"),
        (MessageKind.WARMUP_FROZEN.value, "warning"),
        (MessageKind.FEED_STALE.value, "critical"),        # not in CRITICAL_KINDS; severity carries it
    ],
)
async def test_critical_rows_never_expire(conn, moving, kind, severity):
    """Critical rows retry until delivered; either critical severity or a critical kind qualifies."""
    bot, sender = _bot(conn, moving.clock, _ScriptedBot(fail_forever=True))
    _insert(conn, "n1", moving.now().isoformat(), kind=kind, severity=severity)

    moving.advance(days=2)
    await bot._drain_outbox_once()

    assert _one(conn)["status"] == "pending"           # still queued, two days on
    assert sender.attempts == 1                        # and still being retried


@pytest.mark.asyncio
async def test_surveillance_migration_dedupes_across_bot_instances(conn, moving):
    """``SurveillanceIngest`` re-raises the alert on every refresh; the unique ``dedupe_key`` makes it once per
    (position, list), across a restart. ``critical`` severity alone keeps it from expiring."""
    def msg(status: str) -> CatalogMessage:
        return catalog.surveillance_migration(
            symbol="HELDX", position_id="P1", qty=10, list_key="asm", as_of="2026-06-17",
            previous_as_of="2026-06-16", status=status, unconfirmed=False)

    first, sender = _bot(conn, moving.clock)
    await first.send(msg("new"))
    moving.advance(days=3)
    restarted, resender = _bot(conn, moving.clock)                  # same journal, new process
    await restarted.send(msg("listed"))                              # same key, different wording

    row = _one(conn)
    assert (row["kind"], row["dedupe_key"]) == ("surveillance_migration", "surveillance_migration:P1:asm")
    assert row["status"] == "delivered" and len(sender.sent) == 1 and resender.sent == []
    assert row["severity"] == "critical" and _is_critical(row["severity"], row["kind"])


def test_critical_kinds_covers_recommendation_and_the_login_freeze_kinds():
    """The set holds kinds whose catalog severity understates the cost of losing them; a kind dropped from it becomes expirable."""
    assert MessageKind.RECOMMENDATION.value in CRITICAL_KINDS
    assert MessageKind.REC_FILL_SUSPECTED.value in CRITICAL_KINDS
    assert MessageKind.LOGIN_PROMPT.value in CRITICAL_KINDS
    assert {MessageKind.WARMUP_FROZEN.value, MessageKind.DATA_FRESHNESS_FROZEN.value} <= CRITICAL_KINDS
    assert MessageKind.DAILY_SUMMARY.value not in CRITICAL_KINDS      # genuinely expirable


@pytest.mark.asyncio
async def test_outage_is_logged_once_per_streak_and_reset_by_a_success(conn, moving, caplog):
    """One ERROR line per outage episode, not per failed attempt."""
    sender = _ScriptedBot(fail_forever=True)
    bot, _ = _bot(conn, moving.clock, sender)
    base = moving.now()
    for i in range(5):
        _insert(conn, f"n{i}", (base + timedelta(seconds=i)).isoformat())

    with caplog.at_level(logging.ERROR):
        for _ in range(4):                             # 4 passes x 5 rows = 20 failed attempts
            moving.advance(minutes=10)
            await bot._drain_outbox_once()

    outages = [r for r in caplog.records if r.getMessage() == "telegram_outage"]
    assert len(outages) == 1 and outages[0].consecutive_failures >= 10

    # A success ends the streak, so the next outage logs again.
    bot._app = _App(_ScriptedBot())
    await bot.send("link is back")
    assert bot._drain_fail_streak == 0 and bot._outage_logged is False


@pytest.mark.asyncio
async def test_drainer_is_started_by_start_and_cancelled_by_stop(conn, clock, monkeypatch):
    """The drainer task comes up with polling and is awaited down by stop(), before the SQLite handle closes."""
    from engine.notify import telegram as tg_mod

    async def _ok(self, app):
        return None

    async def _no_teardown(app):
        return None

    monkeypatch.setattr(TelegramBot, "_start_network", _ok)
    monkeypatch.setattr(TelegramBot, "_teardown_app", staticmethod(_no_teardown))
    monkeypatch.setattr(TelegramBot, "_register_handlers", lambda self, app: None)
    monkeypatch.setattr(tg_mod.ApplicationBuilder, "build", lambda self: _App(_ScriptedBot()))
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)

    await bot.start()
    task = bot._drain_task
    assert task is not None and not task.done()

    await bot.stop()
    assert bot._drain_task is None
    assert task.cancelled() or task.done()


@pytest.mark.asyncio
async def test_no_state_store_means_no_drainer(clock):
    """Without a journal there is nothing to drain, so no task is spun."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    bot._start_outbox_drainer()
    assert bot._drain_task is None
    await bot._stop_outbox_drainer()                   # and stopping a non-existent drainer is a no-op


@pytest.mark.asyncio
async def test_drain_loop_survives_a_bad_pass(conn, clock, monkeypatch):
    """One exploding pass is logged and the loop continues. Event-driven: a fixed real-time budget starves on a loaded Windows host (~15.6 ms timer tick)."""
    from engine.notify import telegram as tg_mod

    monkeypatch.setattr(tg_mod, "_DRAIN_INTERVAL_S", 0.01)
    bot, _ = _bot(conn, clock)
    passes: list[int] = []
    second = asyncio.Event()

    async def _boom(self):
        passes.append(1)
        if len(passes) >= 2:
            second.set()
        raise RuntimeError("drain blew up")

    monkeypatch.setattr(TelegramBot, "_drain_outbox_once", _boom)
    bot._start_outbox_drainer()
    try:
        await asyncio.wait_for(second.wait(), timeout=5)   # a dead loop never sets it: TimeoutError
    finally:
        await bot._stop_outbox_drainer()

    assert len(passes) >= 2                            # it kept going after the first explosion


# ------------------------------------------------- head-of-line blocking
# A row that cannot be attempted yet (inside its backoff window) costs nothing and blocks nobody.
def _stale_backlog(conn, moving, n: int = 20, *, attempts: int = 46) -> None:
    """``n`` old, heavily-retried, non-critical rows — all deep inside their 5-minute backoff."""
    base = moving.now() - timedelta(hours=2)
    for i in range(n):
        _insert(conn, f"old{i:02d}", (base + timedelta(seconds=i)).isoformat(), title=f"OLD{i}",
                attempts=attempts, last_attempt_at=moving.now().isoformat())


@pytest.mark.asyncio
async def test_a_fresh_critical_row_is_not_blocked_by_a_backlog_in_backoff(conn, moving):
    """Twenty older rows none of them due, plus one fresh critical row at the back: the next pass must attempt the critical row."""
    sender = _ScriptedBot()
    bot, _ = _bot(conn, moving.clock, sender)
    _stale_backlog(conn, moving)
    _insert(conn, "fresh", moving.now().isoformat(), kind=MessageKind.RECOMMENDATION.value,
            severity="info", title="RELIANCE long", body="entry 1400 stop 1380")

    await bot._drain_outbox_once()

    assert [t.splitlines()[0] for t in sender.sent] == ["ℹ️ RELIANCE long"]
    assert sender.attempts == 1                        # the backlog consumed no attempt at all
    assert conn.execute(
        "SELECT status FROM notifications WHERE notification_id='fresh'"
    ).fetchone()["status"] == "delivered"


@pytest.mark.asyncio
async def test_rows_inside_their_backoff_window_do_not_consume_the_batch(conn, moving):
    """Twenty rows in backoff plus three due: all three fly in one pass; the batch is spent on eligible rows only."""
    sender = _ScriptedBot()
    bot, _ = _bot(conn, moving.clock, sender)
    _stale_backlog(conn, moving)
    for i in range(3):
        _insert(conn, f"due{i}", (moving.now() + timedelta(seconds=i)).isoformat(), title=f"DUE{i}")

    await bot._drain_outbox_once()

    assert [t.splitlines()[0] for t in sender.sent] == ["⚠️ DUE0", "⚠️ DUE1", "⚠️ DUE2"]
    assert {r["status"] for r in _rows(conn) if r["notification_id"].startswith("old")} == {"pending"}


@pytest.mark.asyncio
async def test_critical_rows_go_first_and_chronology_holds_inside_each_class(conn, moving):
    """Criticals first, and chronological within each class (interleaved input, so only a stable sort passes)."""
    sender = _ScriptedBot()
    bot, _ = _bot(conn, moving.clock, sender)
    base = moving.now()
    plan = [
        ("t0", "T0", MessageKind.FEED_DEGRADED.value, "warning"),
        ("c0", "C0", MessageKind.RECOMMENDATION.value, "info"),        # critical by KIND
        ("t1", "T1", MessageKind.FEED_DEGRADED.value, "warning"),
        ("c1", "C1", MessageKind.FEED_STALE.value, "critical"),        # critical by SEVERITY
        ("t2", "T2", MessageKind.FEED_DEGRADED.value, "warning"),
    ]
    for i, (nid, title, kind, severity) in enumerate(plan):
        _insert(conn, nid, (base + timedelta(seconds=i)).isoformat(), title=title, kind=kind,
                severity=severity)

    await bot._drain_outbox_once()

    assert [t.splitlines()[0].split(" ")[-1] for t in sender.sent] == ["C0", "C1", "T0", "T1", "T2"]


@pytest.mark.asyncio
async def test_a_stale_row_expires_wherever_it_sits_in_the_queue(conn, moving):
    """Expiry runs over the whole scan window: a 6-hour-old row buried behind nineteen others is retired next pass, so the queue shrinks."""
    bot, _ = _bot(conn, moving.clock, _ScriptedBot())
    _stale_backlog(conn, moving)
    _insert(conn, "buried", (moving.now() - timedelta(hours=7)).isoformat(), title="BURIED",
            attempts=46, last_attempt_at=moving.now().isoformat())

    await bot._drain_outbox_once()

    assert conn.execute(
        "SELECT status FROM notifications WHERE notification_id='buried'"
    ).fetchone()["status"] == "failed"


def test_the_scan_window_is_wider_than_the_batch():
    """SCAN is how many rows a pass may consider (and expire), BATCH how many it may send; equal values reintroduce head-of-line blocking."""
    assert _DRAIN_SCAN > _DRAIN_BATCH and _DRAIN_BATCH == 5


# ------------------------------------------------- the HTTP transport
def test_the_transport_timeouts_clear_the_measured_connect_times():
    """First connects to api.telegram.org measured up to 4.84 s against python-telegram-bot's stock 5.0 s
    connect timeout; the budget must sit far enough above that for jitter not to cross it."""
    assert _CONNECT_TIMEOUT_S >= 4 * 4.84
    assert _READ_TIMEOUT_S >= 30.0 and _WRITE_TIMEOUT_S >= 30.0 and _POOL_TIMEOUT_S >= 10.0


def test_the_send_wait_outlasts_the_transports_own_budget():
    """The outer ``asyncio.wait_for`` must not pre-empt httpx, or slow sends journal as an anonymous
    TimeoutError instead of the ConnectTimeout / ReadTimeout that names the broken leg."""
    transport_worst_case = _POOL_TIMEOUT_S + _CONNECT_TIMEOUT_S + _WRITE_TIMEOUT_S + _READ_TIMEOUT_S
    assert _SEND_TIMEOUT_S > transport_worst_case


def test_build_request_carries_the_budget_into_httpx():
    """The budget is threaded into the httpx client (read off ``_client.timeout``, not the kwargs)."""
    timeout = _build_request()._client.timeout
    assert (timeout.connect, timeout.read, timeout.write, timeout.pool) == (
        _CONNECT_TIMEOUT_S, _READ_TIMEOUT_S, _WRITE_TIMEOUT_S, _POOL_TIMEOUT_S
    )
    # The polling role keeps PTB's single-connection pool; only the timeouts differ from stock.
    assert _build_request(get_updates=True)._client_kwargs["limits"].max_connections == 1
    assert _build_request()._client_kwargs["limits"].max_connections == 256


@pytest.mark.asyncio
async def test_start_hands_both_roles_their_own_widened_request(conn, clock, monkeypatch):
    """``start()`` passes an explicit HTTPXRequest for both the bot and the long-poll updater, as DISTINCT
    objects (a BaseRequest is shut down by its owner, so sharing breaks teardown)."""
    from engine.notify import telegram as tg_mod

    seen: list = []

    def _capture_build(self):
        seen.append(self)
        return _App(_ScriptedBot())

    async def _ok(self, app):
        return None

    monkeypatch.setattr(TelegramBot, "_start_network", _ok)
    monkeypatch.setattr(TelegramBot, "_register_handlers", lambda self, app: None)
    monkeypatch.setattr(tg_mod.ApplicationBuilder, "build", _capture_build)
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)

    await bot.start()
    await bot.stop()

    builder = seen[0]
    send_req, poll_req = builder._request, builder._get_updates_request
    assert send_req is not poll_req
    for req in (send_req, poll_req):
        assert req._client.timeout.connect == _CONNECT_TIMEOUT_S
        assert req._client.timeout.read == _READ_TIMEOUT_S


# --------------------------------------------------------------------------- GET /notifications
def _client(**collaborators) -> TestClient:
    return TestClient(create_app(secrets=_FakeSecrets(_TOKEN), **collaborators))


def test_notifications_route_requires_a_bearer_token(conn, clock):
    client = _client(conn=conn, clock=clock)
    assert client.get("/notifications").status_code == 401
    assert client.get("/notifications", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_notifications_route_returns_the_day_ascending(conn, clock):
    """Ascending, day-scoped, all columns."""
    _insert(conn, "b", "2026-06-17T11:30:00+05:30", title="second")
    _insert(conn, "a", "2026-06-17T09:15:00+05:30", title="first", status="delivered", attempts=1,
            delivered_at="2026-06-17T09:15:01+05:30")
    _insert(conn, "z", "2026-06-18T09:00:00+05:30", title="tomorrow")   # other day, must not appear
    _insert(conn, "y", "2026-06-16T23:59:59+05:30", title="yesterday")

    client = _client(conn=conn, clock=clock)
    body = client.get("/notifications?d=2026-06-17", headers=AUTH).json()

    assert body["d"] == "2026-06-17"
    assert [r["title"] for r in body["rows"]] == ["first", "second"]
    assert body["rows"][0]["status"] == "delivered" and body["rows"][0]["attempts"] == 1
    # all columns, so the page can render status/backoff/error without a second round trip
    assert {"notification_id", "kind", "severity", "body", "last_error", "last_attempt_at"} <= set(
        body["rows"][0]
    )


def test_notifications_route_defaults_to_today_ist(conn):
    """No ``?d=`` ⇒ today off the app's clock, never the UTC day (at 02:00 IST the UTC date is still yesterday)."""
    ist_clock = Clock(time_source=lambda: datetime(2026, 6, 18, 2, 0, tzinfo=IST))
    _insert(conn, "a", "2026-06-17T23:00:00+05:30", title="yesterday")
    _insert(conn, "z", "2026-06-18T01:00:00+05:30", title="today")

    body = _client(conn=conn, clock=ist_clock).get("/notifications", headers=AUTH).json()

    assert body["d"] == "2026-06-18"
    assert [r["title"] for r in body["rows"]] == ["today"]


def test_notifications_route_rejects_a_malformed_date(conn, clock):
    """A garbled ``?d=`` is an error, never a silent empty day."""
    r = _client(conn=conn, clock=clock).get("/notifications?d=17-06-2026", headers=AUTH)
    assert r.status_code == 422 and "17-06-2026" in r.text


def test_notifications_route_degrades_without_a_state_store(clock):
    body = _client(clock=clock).get("/notifications", headers=AUTH).json()
    assert body == {"d": "2026-06-17", "rows": []}


@pytest.mark.asyncio
async def test_journal_row_written_by_send_is_what_the_route_serves(conn, clock):
    """The outbox and the dashboard read the same row, so the page never shows a "delivered" the wire never saw."""
    bot, _ = _bot(conn, clock, _ScriptedBot(fail_forever=True))
    await bot.send(catalog.catalyst_disabled("feed outage"))
    await bot.send(catalog.feed_stale(6.0))

    rows = _client(conn=conn, clock=clock).get("/notifications", headers=AUTH).json()["rows"]

    assert len(rows) == 2
    assert all(r["status"] == "pending" and r["attempts"] == 1 for r in rows)
    assert all("ConnectTimeout" in r["last_error"] for r in rows)


# --------------------------------------------------------------------------- the page itself
class _Balance(HTMLParser):
    """Minimal well-formedness check: every non-void start tag is closed, in order."""

    VOID = {"meta", "link", "br", "hr", "img", "input", "source", "area", "base", "col"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.errors: list[str] = []
        self.tags: set[str] = set()

    def handle_starttag(self, tag, attrs):
        self.tags.add(tag)
        if tag not in self.VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if tag in self.VOID:
            return
        if not self.stack or self.stack[-1] != tag:
            self.errors.append(f"</{tag}> closes {self.stack[-1] if self.stack else '(nothing)'}")
            return
        self.stack.pop()


def test_notifications_page_exists_and_parses():
    assert UI_HTML.is_file(), f"missing notifications page at {UI_HTML}"
    parser = _Balance()
    parser.feed(UI_HTML.read_text(encoding="utf-8"))
    parser.close()
    assert parser.errors == []
    assert parser.stack == [], f"unclosed tags: {parser.stack}"
    assert {"html", "head", "body", "style", "script", "input", "button"} <= parser.tags


def test_notifications_page_is_self_contained_and_calls_the_route():
    """The page must render offline (it reports outages): no off-box URL of any kind."""
    html = UI_HTML.read_text(encoding="utf-8")
    assert "/notifications?d=" in html                       # it reads the real route
    assert 'Authorization": "Bearer ' in html                # bearer-authed
    assert "localStorage" in html and "MT Notifications" in html
    external = re.findall(r"https?://[^\s\"'<>]+", html)
    assert external == [], f"external resource reference(s): {external}"
    assert "//cdn" not in html and "integrity=" not in html   # no protocol-relative / SRI'd CDN pull


def test_static_page_is_reachable_without_the_bearer_header(conn, clock):
    """Auth is per-route (no middleware) and a ``StaticFiles`` mount carries no dependency, so a browser
    navigation reaches the shell, which prompts for the token; every row comes from the 401-gated route."""
    client = _client(conn=conn, clock=clock)
    # Both spellings: the catch-all "/" dashboard mount means Starlette never adds the slash redirect.
    for path in ("/notifications-ui", "/notifications-ui/"):
        r = client.get(path)
        assert r.status_code == 200, path
        assert "MT Notifications" in r.text
    # The API route is not shadowed by that route or the "/" mount.
    assert client.get("/notifications").status_code == 401
    assert client.get("/notifications", headers=AUTH).status_code == 200
