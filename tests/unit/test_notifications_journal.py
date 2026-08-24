"""Notification journal + Telegram retry outbox + the notifications page (WO-24d, 2026-08-21).

Before this, ``TelegramBot._send_text`` DROPPED on failure: logged, never queued, never stored. With
223 ConnectTimeouts on 08-20 and 186 on 08-19 that is a recommendation silently lost, and nothing on
the platform able to say it had ever existed. ONE mechanism fixes both halves — a ``notifications``
row written BEFORE the wire attempt, which is the retry outbox AND what the owner's page reads.

The properties pinned here are the ones whose breakage would put the loss back:

* **Journal first.** The row exists even when the transport is dead / not started, and the split into
  Telegram-sized parts is a TRANSPORT detail — one notification is one row, however many parts flew.
* **A failure is `pending`, not gone.** attempts+1 and ``last_error`` recorded; only expiry may move a
  row to ``failed``, and a CRITICAL row (a recommendation, a login prompt, a freeze) never expires.
* **Journalling never breaks a send.** No store, or a broken store, and the send proceeds as before.
* **The page is bearer-authed data on an unauthenticated shell.** ``GET /notifications`` 401s without
  the token; the static page itself carries no data and no off-box dependency.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import timedelta
from html.parser import HTMLParser

import pytest
from fastapi.testclient import TestClient

from engine.api.app import create_app
from engine.core.clock import Clock
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
    _wire_text,
)

OWNER_CHAT = 4242
_TOKEN = "s3cret-dash-token"
AUTH = {"Authorization": f"Bearer {_TOKEN}"}
UI_HTML = repo_root() / "web" / "dist" / "index.html"


# --------------------------------------------------------------------------- test doubles
class _MovableClock:
    """A Clock whose "now" the test advances — the drainer's backoff and the 6 h expiry are both
    clock-driven, and a wall-clock test of a 6 h rule is not a test."""

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
        self.attempts = 0
        self._failures = failures
        self._fail_forever = fail_forever

    async def send_message(self, chat_id: int, text: str) -> None:
        self.attempts += 1
        if self._fail_forever or self.attempts <= self._failures:
            raise ConnectionError("httpx.ConnectTimeout: telegram.org timed out")
        self.sent.append(text)


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
    """The happy path end to end: one row, written pending BEFORE the wire, stamped delivered after."""
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
    """A retry must ship the SAME bytes the first attempt did — the journal stores render inputs, so
    the reconstruction is pinned against ``CatalogMessage.render`` itself, not against a copy of it."""
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
    """The ``alert()``/ad-hoc path sends a bare string: first line is the title, the rest is the body,
    and the reconstruction is the identity (no severity ⇒ no glyph to re-add)."""
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
    """The whole point: a send that fails is QUEUED, not gone. Status stays pending (only expiry may
    say failed), attempts is 1, and last_error names the exception class."""
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
    """A disabled/not-yet-started bot used to log ``telegram_send_dropped`` and lose the message. It
    still logs it — but the row is on disk, pending, for the drainer to pick up once polling is up."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)   # no _app assigned

    await bot.send(catalog.login_prompt("kite-login-url"))

    row = _one(conn)
    assert row["status"] == "pending" and row["attempts"] == 1
    assert "NotStarted" in row["last_error"]


@pytest.mark.asyncio
async def test_split_message_is_one_notification_not_five(conn, clock):
    """WO-22's 4096-char split is unchanged and stays a TRANSPORT detail: five parts on the wire,
    ONE journal row, and the row holds the whole un-split body."""
    bot, sender = _bot(conn, clock)
    body = "X" * 30000
    msg = CatalogMessage(kind=MessageKind.DAILY_SUMMARY, title="Daily summary", body=body)

    await bot.send(msg)

    assert len(sender.sent) == 5                       # split still works, still capped at 5 parts
    assert all(len(part) <= 4096 for part in sender.sent)
    row = _one(conn)
    assert row["status"] == "delivered" and row["attempts"] == 1
    assert row["body"] == body                         # journalled whole, never the truncated wire form


@pytest.mark.asyncio
async def test_a_broken_journal_never_blocks_the_send(conn, clock, caplog):
    """R8, applied to the journal itself: the mechanism that exists to stop messages being lost must
    never become the thing that loses them. A dropped table costs a WARNING, not the notification."""
    conn.execute("DROP TABLE notifications")
    bot, sender = _bot(conn, clock)

    with caplog.at_level(logging.WARNING):
        await bot.send(catalog.feed_stale(9.0))        # must not raise

    assert len(sender.sent) == 1
    assert any(r.getMessage() == "notification_journal_failed" for r in caplog.records)


@pytest.mark.asyncio
async def test_an_unwired_state_store_sends_exactly_as_before(clock):
    """No conn ⇒ no journal ⇒ the pre-WO-24d behaviour, verbatim (the ``wired_bot`` fixture in
    ``test_telegram_commands`` is exactly this shape, so this guards those tests too)."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    sender = _ScriptedBot()
    bot._app = _App(sender)

    await bot.send("hello")

    assert sender.sent == ["hello"]


# --------------------------------------------------------------------------- outbox drainer
@pytest.mark.asyncio
async def test_drainer_retries_a_failed_row_and_marks_it_delivered(conn, moving):
    """Fail once, then succeed: the row survives the failure, waits out its 30 s backoff, and the
    NEXT drain pass delivers it. attempts counts both tries — the dashboard says "delivered on 2"."""
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
    """Oldest-first, at most five per pass: a recovered link replays the backlog in the order the
    engine raised it instead of flooding the chat out of order."""
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
    """min(300, 30 * 2**(attempts-1)) after the last attempt: 30s, 60s, … capped at 300s. A row inside
    its window is SKIPPED, keeping its place — it is never swapped for a younger row."""
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
    """A feed-degraded alert six hours stale is history, not news — the drainer gives up and says so
    (``failed``), rather than retrying it into next week."""
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
    """Late beats never for a recommendation / login / freeze / kill: these retry until delivered or
    the process ends. Two independent arms qualify a row — its severity, or its kind."""
    bot, sender = _bot(conn, moving.clock, _ScriptedBot(fail_forever=True))
    _insert(conn, "n1", moving.now().isoformat(), kind=kind, severity=severity)

    moving.advance(days=2)
    await bot._drain_outbox_once()

    assert _one(conn)["status"] == "pending"           # still queued, two days on
    assert sender.attempts == 1                        # and still being retried


def test_critical_kinds_covers_recommendation_and_the_login_freeze_kinds():
    """The set exists for kinds whose catalog severity understates the cost of losing them; a kind
    dropped from it silently becomes expirable, which is the WO-24d failure mode returning."""
    assert MessageKind.RECOMMENDATION.value in CRITICAL_KINDS
    assert MessageKind.REC_FILL_SUSPECTED.value in CRITICAL_KINDS
    assert MessageKind.LOGIN_PROMPT.value in CRITICAL_KINDS
    assert {MessageKind.WARMUP_FROZEN.value, MessageKind.DATA_FRESHNESS_FROZEN.value} <= CRITICAL_KINDS
    assert MessageKind.DAILY_SUMMARY.value not in CRITICAL_KINDS      # genuinely expirable


@pytest.mark.asyncio
async def test_outage_is_logged_once_per_streak_and_reset_by_a_success(conn, moving, caplog):
    """One loud line per outage EPISODE. With a dead link and a full batch the drainer fails five
    times a pass — an ERROR per failure would bury the log it is trying to make legible."""
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

    # A success ends the streak, so the NEXT outage pages again instead of staying silent forever.
    bot._app = _App(_ScriptedBot())
    await bot.send("link is back")
    assert bot._drain_fail_streak == 0 and bot._outage_logged is False


@pytest.mark.asyncio
async def test_drainer_is_started_by_start_and_cancelled_by_stop(conn, clock, monkeypatch):
    """Lifecycle: the task is owned by the bot, comes up with polling and is awaited down by stop() —
    a drainer outliving the connection would raise into a closed SQLite handle at teardown."""
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
    """Nothing to drain without a journal — the bot must not spin an empty task forever."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    bot._start_outbox_drainer()
    assert bot._drain_task is None
    await bot._stop_outbox_drainer()                   # and stopping a non-existent drainer is a no-op


@pytest.mark.asyncio
async def test_drain_loop_survives_a_bad_pass(conn, clock, monkeypatch):
    """The retry mechanism dying silently would restore exactly the failure it exists to remove, so
    one exploding pass is logged and the loop continues."""
    from engine.notify import telegram as tg_mod

    monkeypatch.setattr(tg_mod, "_DRAIN_INTERVAL_S", 0.01)
    bot, _ = _bot(conn, clock)
    passes: list[int] = []

    async def _boom(self):
        passes.append(1)
        raise RuntimeError("drain blew up")

    monkeypatch.setattr(TelegramBot, "_drain_outbox_once", _boom)
    bot._start_outbox_drainer()
    await asyncio.sleep(0.08)
    await bot._stop_outbox_drainer()

    assert len(passes) >= 2                            # it kept going after the first explosion


# ------------------------------------------------- head-of-line blocking (WO-25b, 2026-08-24)
# The 08-24 incident: 203 rows queued, almost all of them stale alert spam at 46 attempts each (so
# backing off the full five minutes), and the drainer took the FIVE OLDEST every pass and skipped the
# ones inside their window — spending the whole pass on rows it was never going to send. A fresh
# owner-action ack sat at attempt 1 for hours behind them. The rule the fix adds: a row that cannot be
# attempted yet costs nothing and blocks nobody.
def _stale_backlog(conn, moving, n: int = 20, *, attempts: int = 46) -> None:
    """``n`` old, heavily-retried, non-critical rows — all deep inside their 5-minute backoff."""
    base = moving.now() - timedelta(hours=2)
    for i in range(n):
        _insert(conn, f"old{i:02d}", (base + timedelta(seconds=i)).isoformat(), title=f"OLD{i}",
                attempts=attempts, last_attempt_at=moving.now().isoformat())


@pytest.mark.asyncio
async def test_a_fresh_critical_row_is_not_blocked_by_a_backlog_in_backoff(conn, moving):
    """THE regression. Twenty older rows, none of them due; one fresh critical row at the BACK of the
    queue. The next pass must attempt the critical row — under the old oldest-five selection it would
    not have been looked at until the backlog drained, which is hours."""
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
    """The mechanism behind the regression above, isolated: twenty rows in backoff plus three that are
    DUE. All three fly in one pass — the batch is spent on eligible rows, never on skipped ones."""
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
    """Ordering, in full: criticals ahead of the rest, and within EACH class the order the engine
    raised them. Interleaved on purpose — a stable sort is what keeps the second half true."""
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
    """Expiry now runs over the whole scan window, not just the front of it: a 6-hour-old row buried
    behind nineteen others is retired on the next pass. Otherwise a spam backlog is re-scanned
    forever and the queue never shrinks — which is how it reached 203 rows."""
    bot, _ = _bot(conn, moving.clock, _ScriptedBot())
    _stale_backlog(conn, moving)
    _insert(conn, "buried", (moving.now() - timedelta(hours=7)).isoformat(), title="BURIED",
            attempts=46, last_attempt_at=moving.now().isoformat())

    await bot._drain_outbox_once()

    assert conn.execute(
        "SELECT status FROM notifications WHERE notification_id='buried'"
    ).fetchone()["status"] == "failed"


def test_the_scan_window_is_wider_than_the_batch():
    """The two numbers do different jobs and must not be collapsed back into one: the SCAN decides
    what the pass may CONSIDER (and expire), the BATCH decides how many it may SEND. Equal values are
    the old head-of-line bug restored."""
    assert _DRAIN_SCAN > _DRAIN_BATCH and _DRAIN_BATCH == 5


# ------------------------------------------------- the HTTP transport (WO-25b, 2026-08-24)
def test_the_transport_timeouts_clear_the_measured_connect_times():
    """The whole 08-24 finding in one assertion: first connects to api.telegram.org were MEASURED at
    4.25 s and 4.84 s, against python-telegram-bot's stock 5.0 s connect timeout. The budget must sit
    far enough above the measurement that ordinary jitter cannot cross it."""
    assert _CONNECT_TIMEOUT_S >= 4 * 4.84
    assert _READ_TIMEOUT_S >= 30.0 and _WRITE_TIMEOUT_S >= 30.0 and _POOL_TIMEOUT_S >= 10.0


def test_the_send_wait_outlasts_the_transports_own_budget():
    """ORDERING CONSTRAINT. The outer ``asyncio.wait_for`` must not pre-empt httpx: if it fires first,
    every slow send is journalled as an anonymous TimeoutError instead of the ConnectTimeout /
    ReadTimeout that names the broken leg — the exact ambiguity that took three days to resolve."""
    transport_worst_case = _POOL_TIMEOUT_S + _CONNECT_TIMEOUT_S + _WRITE_TIMEOUT_S + _READ_TIMEOUT_S
    assert _SEND_TIMEOUT_S > transport_worst_case


def test_build_request_carries_the_budget_into_httpx():
    """Not just declared — actually threaded into the httpx client the send flies on. Read off
    ``_client.timeout`` (the httpx.Timeout the request builds) rather than trusting the kwargs."""
    timeout = _build_request()._client.timeout
    assert (timeout.connect, timeout.read, timeout.write, timeout.pool) == (
        _CONNECT_TIMEOUT_S, _READ_TIMEOUT_S, _WRITE_TIMEOUT_S, _POOL_TIMEOUT_S
    )
    # The polling role keeps PTB's single-connection pool; only the timeouts differ from stock.
    assert _build_request(get_updates=True)._client_kwargs["limits"].max_connections == 1
    assert _build_request()._client_kwargs["limits"].max_connections == 256


@pytest.mark.asyncio
async def test_start_hands_both_roles_their_own_widened_request(conn, clock, monkeypatch):
    """The wiring itself: ``start()`` must pass an explicit HTTPXRequest for BOTH the bot and the
    long-poll updater, and they must be DISTINCT objects — a BaseRequest is initialized/shut down by
    the bot that owns it, so sharing one instance breaks teardown. Asserted off the builder's own
    slots, which is where the wiring is observable."""
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
    """The day's transcript, read top-to-bottom: ascending, day-scoped, all columns."""
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


def test_notifications_route_defaults_to_today_ist(conn, clock):
    """No ``?d=`` ⇒ today off the app's clock (2026-06-17 in the frozen fixture), never the UTC day."""
    _insert(conn, "a", "2026-06-17T10:00:00+05:30", title="today")
    _insert(conn, "z", "2026-06-18T10:00:00+05:30", title="tomorrow")

    body = _client(conn=conn, clock=clock).get("/notifications", headers=AUTH).json()

    assert body["d"] == "2026-06-17"
    assert [r["title"] for r in body["rows"]] == ["today"]


def test_notifications_route_rejects_a_malformed_date(conn, clock):
    """A garbled ``?d=`` must be an error, never a silent empty day the owner reads as "nothing fired"."""
    r = _client(conn=conn, clock=clock).get("/notifications?d=17-06-2026", headers=AUTH)
    assert r.status_code == 422 and "17-06-2026" in r.text


def test_notifications_route_degrades_without_a_state_store(clock):
    body = _client(clock=clock).get("/notifications", headers=AUTH).json()
    assert body == {"d": "2026-06-17", "rows": []}


@pytest.mark.asyncio
async def test_journal_row_written_by_send_is_what_the_route_serves(conn, clock):
    """The one-mechanism claim, end to end: the retry outbox and the dashboard read the SAME row, so
    the page can never show a "delivered" the wire never saw."""
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
    """LAN/offline by construction: the box's flaky path to the outside world is the very reason this
    page exists, so a page that needed a CDN to render an outage report would be broken exactly when
    it is needed. No off-box URL of any kind may appear in it."""
    html = UI_HTML.read_text(encoding="utf-8")
    assert "/notifications?d=" in html                       # it reads the real route
    assert 'Authorization": "Bearer ' in html                # bearer-authed, per R10
    assert "localStorage" in html and "MT Notifications" in html
    external = re.findall(r"https?://[^\s\"'<>]+", html)
    assert external == [], f"external resource reference(s): {external}"
    assert "//cdn" not in html and "integrity=" not in html   # no protocol-relative / SRI'd CDN pull


def test_static_page_is_reachable_without_the_bearer_header(conn, clock):
    """Confirms the mount posture the page depends on: auth here is PER-ROUTE
    (``Depends(_require_owner)``), there is no auth middleware, and a ``StaticFiles`` mount carries no
    dependency — so a browser navigation (which cannot set a header) reaches the shell, and the shell
    prompts for the token itself. The page holds no data; every row comes from the 401-gated route."""
    client = _client(conn=conn, clock=clock)
    # BOTH spellings: the catch-all dashboard mount at "/" always matches, so Starlette never issues
    # its add-the-slash redirect — the bare path the owner types has to be a real route or it 404s.
    for path in ("/notifications-ui", "/notifications-ui/"):
        r = client.get(path)
        assert r.status_code == 200, path
        assert "MT Notifications" in r.text
    # ...and the API route it calls is NOT shadowed by that route or by the "/" dashboard mount.
    assert client.get("/notifications").status_code == 401
    assert client.get("/notifications", headers=AUTH).status_code == 200
