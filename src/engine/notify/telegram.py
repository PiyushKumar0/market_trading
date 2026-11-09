"""Telegram owner control plane.

The two-way owner I/O surface. Depends on ``core`` plus read-only / command-routing views over
``risk`` (``ModeManager`` / ``KillSwitch``); it **never originates orders itself** — every owner
command routes through those Tier-2 interfaces, exactly as the dashboard does.

Two load-bearing security properties:

* **Owner-ID lock:** :func:`_owner_only` rejects (logs + ignores) any update whose
  ``effective_chat.id`` is not the configured ``owner_chat_id``.
* **Two-step confirmation:** destructive / mode-up commands (``/mode AUTO``, ``/kill_reset``, ...)
  issue a one-time challenge phrase and stash a pending challenge; a follow-up ``/confirm <phrase>``
  completes it. Killing is single-step (fast); only *reset* / mode-up are two-step.

The RECOMMEND-mode data sources arrive as **optional keyword dependencies** — ``reco_book``, ``latch``,
``governor``, ``limits_engine``, ``exposure``, ``session``, ``conn`` — each defaulting to ``None``, in
which case its command replies "… not wired" instead of pretending to act. ``engine.ops.main``
composes them; the bot never constructs a dependency.

The capture commands (``/taken``, ``/closed``, ``/veto``) speak **ticker, not rec_id**: the first
argument is resolved against the recommendation ledger, with the ULID id path kept underneath — see
:meth:`TelegramBot._resolve_rec_arg`.

RECOMMEND-mode honesty rules: the platform places **zero API orders**, so ``/close`` returns guidance
rather than sending anything, and ``/approve`` writes only its ``owner_approvals`` row (a tracked
position's stop is updated only if the injected book exposes ``apply_approval`` — probed, never
required).

The command catalog is the single source of truth for handler registration, ``/help``, and the
Telegram ``/`` autocomplete menu (``setMyCommands``, owner-chat-scoped). Inline-button taps,
owner-guarded the same way, run only the verbs in :data:`_BUTTON_COMMANDS`.
:meth:`TelegramBot.attach_bus` subscribes the outbound owner alerts (mode / risk-state / kill /
trade-window / budget-tier) and renders them through the notification catalog.

All owner-confirmed state changes use :data:`Actor.OWNER`. All times come from :class:`Clock`, never a
bare ``datetime.now()``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets as _secrets
import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from telegram.request import HTTPXRequest
from ulid import ULID

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.money import pct_of
from engine.core.recommendations import recommendation_expired
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import OwnerConfirmation
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged
from engine.notify import catalog
from engine.notify.catalog import _SEVERITY_GLYPH, MessageKind
from engine.oms.state import TIME_ONLY_POLICY
from engine.risk.causes import CAUSE_OWNER_PAUSE, CAUSE_REJECTION_STORM
from engine.risk.events import (
    TOPIC_KILL_STATE,
    TOPIC_MODE_CHANGED,
    TOPIC_RISK_STATE,
    TOPIC_TRADE_WINDOW,
    KillStateChanged,
    ModeChanged,
    RiskStateChanged,
    TradeWindowChanged,
)
from engine.risk.limits import floor_limits_from
from engine.risk.mode import ModeRefused

if TYPE_CHECKING:  # avoid hard import cycles / heavy deps at import time
    from engine.broker.session import SessionManager
    from engine.intelligence.governor import BudgetGovernor
    from engine.ops.paper_control import PaperControl
    from engine.risk.causes import RiskStateLatch
    from engine.risk.exposure import ExposureTracker
    from engine.risk.kill import KillSwitch
    from engine.risk.limits import LimitsEngine
    from engine.risk.mode import ModeManager

_log = get_logger("engine.notify.telegram")


class _Deduped(Exception):
    """The journal's unique ``dedupe_key`` index already holds this message."""

#: HTTP transport budget. python-telegram-bot's default 5 s ``connect_timeout`` is narrower than this
#: LAN box's measured first connect to ``api.telegram.org`` (4.3-4.8 s), so the connect budget is ~4x
#: that worst case and read/write/pool are widened with it: on a slow link a send should be LATE,
#: never lost (the outbox exists for genuinely dead links).
_CONNECT_TIMEOUT_S = 20.0
_READ_TIMEOUT_S = 30.0
_WRITE_TIMEOUT_S = 30.0
_POOL_TIMEOUT_S = 10.0

#: PTB's own per-request pool sizes (256 for the bot, 1 for the long-poll updater), restated because
#: building the request objects by hand bypasses the builder that would supply them.
_POOL_SIZE = 256
_GET_UPDATES_POOL_SIZE = 1

#: Hard bounds on Telegram network awaits: the library's own retry/connect behaviour is not trusted to
#: terminate on this network, and a hanging send must never hold the boot. A send is best-effort (late
#: delivery is worthless), so a bounded drop beats an unbounded wait. Start gets a longer budget and
#: degrades to a DISABLED bot — the engine must boot without Telegram rather than never.
#:
#: ORDERING CONSTRAINT: these must stay STRICTLY LONGER than the transport budget above (worst case
#: for one request: pool 10 + connect 20 + write 30 + read 30 = 90 s). If the outer ``wait_for`` fires
#: first it pre-empts httpx and every slow send is journalled as an anonymous ``TimeoutError`` instead
#: of the ConnectTimeout / ReadTimeout that says WHICH leg is broken. Widen the transport and these
#: move with it; never the reverse.
_SEND_TIMEOUT_S = 95.0
_START_TIMEOUT_S = 120.0

#: Telegram's hard per-message cap — a send past this raises BadRequest "Message is too long".
#: ``_SPLIT_CHUNK_CHARS`` is the raw-content budget per split part, well under the cap so the
#: "(part i/N)" / truncation marker always fits.
_TELEGRAM_MAX_CHARS = 4096
_SPLIT_CHUNK_CHARS = 4000
_SPLIT_MAX_PARTS = 5

# --------------------------------------------------------------------- notification journal
#: Every outbound message is journalled to ``notifications`` (migration 0011) BEFORE the wire attempt:
#: one table is both the retry OUTBOX drained below and the owner dashboard's chronological view. See
#: ``0011_notifications.sql`` for the state machine.

#: Drainer cadence and per-pass batch. FIVE rows per pass, re-attempted SEQUENTIALLY, so a recovered
#: link replays the backlog in the order the engine raised it.
#:
#: :data:`_DRAIN_SCAN` is the head-of-line guard: the pass scans a wide window of the queue and spends
#: its attempts on rows that are actually DUE, critical first, so a row inside its backoff window
#: occupies no slot and blocks nobody (a fresh owner ack must not sit behind rows that are backing off).
_DRAIN_INTERVAL_S = 30.0
_DRAIN_BATCH = 5
_DRAIN_SCAN = 100

#: Per-row exponential backoff: ``min(_BACKOFF_MAX_S, _BACKOFF_BASE_S * 2**(attempts-1))`` seconds
#: after the row's last attempt. A row not yet due is SKIPPED (keeps its place in the queue), so a
#: multi-hour outage does not hammer a dead link.
_BACKOFF_BASE_S = 30
_BACKOFF_MAX_S = 300

#: How long a NON-critical row may stay ``pending`` before the drainer marks it ``failed``: longer than
#: any session's usefulness window. Critical rows are exempt — see :data:`CRITICAL_KINDS`.
_EXPIRY_H = 6

#: Consecutive drainer failures before ONE loud ``telegram_outage`` line is logged. Reset by any
#: successful delivery, so a long outage pages once per episode.
_OUTAGE_STREAK = 10

#: Bound on the journalled ``last_error`` label (exception class + first line of its message).
_ERROR_LABEL_CHARS = 200

#: Kinds that NEVER expire, regardless of catalog severity — late delivery still beats none.
#: ``RECOMMENDATION`` is the load-bearing member: its severity is ``info`` yet losing one is losing the
#: product. The rest are login and freeze/halt kinds: without the login nothing trades, and a
#: freeze/kill the owner never heard about is a position they think is armed when it is not.
CRITICAL_KINDS: frozenset[str] = frozenset(
    {
        MessageKind.RECOMMENDATION.value,
        MessageKind.REC_FILL_SUSPECTED.value,
        # Raised at most ONCE per position per trading day: an expiry during an outage would lose the
        # day's only chance to ask the owner for the missing `/closed`.
        MessageKind.POSITION_NOT_IN_HOLDINGS.value,
        MessageKind.LOGIN_PROMPT.value,
        MessageKind.KILL.value,
        MessageKind.WARMUP_FROZEN.value,
        MessageKind.DATA_FRESHNESS_FROZEN.value,
    }
)

# How long a pending two-step challenge stays valid before it must be re-issued. A short TTL
# keeps a stale confirm phrase from authorising a destructive action long after the owner asked.
_CHALLENGE_TTL = timedelta(minutes=2)

# Reply text for a command registered from the catalog but whose downstream is not yet wired.
_STUB_REPLY = "not yet implemented"

# ------------------------------------------------------------------- ticker → rec_id
#: ``rec_id`` is a 26-char ULID no owner-facing surface prints, so the outcome-capture commands resolve
#: their first argument as a TICKER against the recommendation ledger, falling back to the id path when
#: the argument is ULID-shaped.
#:
#: The namespaces cannot collide: a ULID is exactly 26 Crockford-base32 characters (no I/L/O/U), while
#: NSE tickers are shorter and may carry ``&`` or ``-`` (GVT&D, M&M, BAJAJ-AUTO). That makes
#: "ticker first, id second" safe rather than a heuristic.
_ULID_CHARS = 26
_ULID_ALPHABET = frozenset("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

#: Owner-facing signatures for the capture commands, defined ONCE: :data:`_COMMANDS` (``/help``, the
#: autocomplete menu) and the handlers' ``usage:`` replies both read these, so they never disagree.
_USAGE_TAKEN = "/taken <symbol|rec_id> <qty> <price>"
_USAGE_CLOSED = "/closed <symbol|rec_id> <price>"
VETO_REASONS = ("market", "price", "size", "trust", "away", "other")
_USAGE_VETO = f"/veto <symbol|rec_id> <{'|'.join(VETO_REASONS)}>"
_USAGE_WHY = "/why <symbol>"
_USAGE_PAPER = "/paper <on|off|status|reset>"
_USAGE_PROTECTED ="/protected <symbol|rec_id>"

#: The only verbs an inline-button tap may run. A button's command is its callback data, which
#: Telegram caps at 64 bytes; a tap on anything else is refused, never dispatched.
_BUTTON_COMMANDS = frozenset({"veto"})
_CALLBACK_DATA_MAX_BYTES = 64
#: Telegram's cap on a callback-query answer.
_ANSWER_MAX_CHARS = 200

#: Rows a resolution reply may list before it summarises the rest. ``_reply`` writes straight to
#: ``reply_text`` and does NOT pass through ``send()``'s splitter, so an unbounded list could hit
#: Telegram's 4096-char cap and the owner would get NO reply at all.
_MAX_LIST_ROWS = 12


class RecoBook(Protocol):
    """The RECOMMEND outcome-capture seam — implemented by ``RecommendationBook``.

    Duck-typed on purpose (mirrors the ``FloorLimits`` seam in ``engine.risk.exposure``): the bot
    depends on these methods (take/close/veto/protect + decision_candidates) and nothing else.
    take/close/veto/protect each return an owner-facing summary string and raise :class:`ValueError`
    — with a message meant for the owner — on an unknown ``rec_id`` or an invalid state transition;
    ``decision_candidates`` returns the candidate rows (``list[dict]``) used to resolve a ticker.

    ``apply_approval`` is OPTIONAL and probed with ``hasattr``: in RECOMMEND mode a stop-widen approval has
    no order side effect (the human owns their orders), so a book that cannot apply one is fine.
    """

    async def take(self, rec_id: str, qty: int, price: Decimal) -> str: ...

    async def close(self, rec_id: str, price: Decimal) -> str: ...

    async def veto(self, rec_id: str, reason: str | None) -> str: ...

    async def protect(self, symbol_or_rec: str) -> str: ...

    def decision_candidates(
        self, symbol: str | None, now: datetime, *, include_skipped: bool = False
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class _CommandSpec:
    """One row of the owner-command catalog.

    Single source of truth for handler registration, ``/help``, and the Telegram ``/`` autocomplete
    menu (``setMyCommands``) — so all three can never drift. ``live`` is True for a command with a real
    handler and False for one registered as a logged stub replying ``_STUB_REPLY``. A live command
    whose OPTIONAL dependency is unwired replies "… not wired"; that is not a stub.
    """

    name: str        # command word, no leading slash (Telegram requires ^[a-z0-9_]{1,32}$)
    usage: str       # owner-facing signature, e.g. "/mode <OFF|RECOMMEND|AUTO>"
    summary: str     # one-line description of what it does
    live: bool       # True ⇒ real handler; False ⇒ registered stub


# Ordered so ``/help`` and the menu read top-to-bottom in a sensible sequence (control plane first).
# Editing this tuple is the ONLY place to add/relabel a command — registration + help + menu follow.
# A tap on an inline button is not a command here; it may run only the verbs in _BUTTON_COMMANDS.
_COMMANDS: tuple[_CommandSpec, ...] = (
    # --- sticky control plane (mode / kill / window) ---
    _CommandSpec("status", "/status",
                 "Show sticky control-plane state — mode, order routing, risk_state, kill switch, "
                 "and trade window. Read-only.", True),
    _CommandSpec("kill", "/kill [reason]",
                 "Engage the kill switch now (single-step) — halts trading immediately; optional reason.",
                 True),
    _CommandSpec("kill_reset", "/kill_reset",
                 "Start the two-step kill reset; reply /confirm <phrase> to re-enable trading.", True),
    _CommandSpec("mode", "/mode <OFF|RECOMMEND|AUTO>",
                 "Set engine mode. →AUTO needs /confirm; →OFF or →RECOMMEND applies immediately.", True),
    _CommandSpec("trade_window", "/trade_window [HH:MM HH:MM]",
                 "No args: show the trade window. Two args (HH:MM HH:MM): set start/end (validated).",
                 True),
    _CommandSpec("confirm", "/confirm <phrase>",
                 "Complete a pending two-step challenge (kill reset or →AUTO) with its phrase.", True),
    _CommandSpec("help", "/help", "List every command and what it does.", True),
    # --- entries pause / re-arm (per-cause latch) ---
    _CommandSpec("pause_entries", "/pause_entries",
                 "Pause new entries (risk_state FROZEN); risk-reducing exits continue.", True),
    _CommandSpec("resume_entries", "/resume_entries",
                 "Resume entries: clears the owner pause AND a rejection-storm freeze.", True),
    _CommandSpec("rearm", "/rearm",
                 "Re-arm floor stops (weekly drawdown, equity floor, total-loss floor) the account is back "
                 "above; a stop still breached stays.", True),
    # --- RECOMMEND outcome capture ---
    _CommandSpec("taken", _USAGE_TAKEN,
                 "Confirm you took a recommendation at qty/price (origin=recommended). The symbol is "
                 "enough while exactly one recommendation on it is open.", True),
    _CommandSpec("closed", _USAGE_CLOSED,
                 "Mark a taken recommendation closed at price (records the outcome). The symbol is "
                 "enough while exactly one taken recommendation on it is open.", True),
    _CommandSpec("veto", _USAGE_VETO,
                 "Decline a recommendation with a reason (live, or expired within the last "
                 "sessions); it is recorded as vetoed, never as taken. Exit and adjust "
                 "recommendations need no reason.", True),
    _CommandSpec("protected", _USAGE_PROTECTED,
                 "Confirm you placed the protective stop order for a taken position; stops the "
                 "protection reminders.", True),
    _CommandSpec("close", "/close <position_id>",
                 "Guidance for exiting a position — in RECOMMEND the exit is yours to place.",
                 True),
    # --- read-only reports ---
    _CommandSpec("positions", "/positions",
                 "Show open positions (qty, average entry, stop/target, origin).", True),
    _CommandSpec("pnl", "/pnl", "Show realised P&L today, day MTM and platform equity.", True),
    _CommandSpec("budget", "/budget",
                 "Show quota-week-to-date LLM spend (Thu 14:00 IST window) per agent vs the weekly allocation and the degrade tier.", True),
    _CommandSpec("limits", "/limits",
                 "Show equity, day MTM, open counts, consecutive losses and the key caps.", True),
    # --- approvals + broker session ---
    _CommandSpec("approve", "/approve <approval_id>",
                 "Approve a pending owner-approval request (e.g. a stop-widen).", True),
    _CommandSpec("reject", "/reject <approval_id>", "Reject a pending owner-approval request.", True),
    _CommandSpec("token", "/token <request_token>",
                 "Complete the daily Kite login with the request token (fallback).", True),
    _CommandSpec("scan_now", "/scan_now",
                 "Sweep the scanners on demand: live candidates enter the pipeline; otherwise "
                 "the reply lists the price levels at which today's setups would arm.", True),
    _CommandSpec("why", _USAGE_WHY,
                 "Where one symbol stands: universe status, trigger levels, flags, filings, last "
                 "recommendation, positions, last verdict. Read-only.", True),
    _CommandSpec("paper", _USAGE_PAPER,
                 "Paper autopilot: on and reset need /confirm; off applies now (no new entries; resting "
                 "paper entry orders are cancelled; exits and GTTs continue); status is read-only.", True),
)


def _help_text() -> str:
    """Render the full command catalog as one owner-facing plain-text message (no markdown).

    Any non-live spec is grouped under a "not yet available" heading so the owner is never misled
    into thinking it acts.
    """
    live = [c for c in _COMMANDS if c.live]
    later = [c for c in _COMMANDS if not c.live]
    lines = ["Owner commands (owner-only):", ""]
    for c in live:
        lines.append(c.usage)
        lines.append(f"    {c.summary}")
    if later:
        lines += ["", "Not yet available:"]
        lines += [f"/{c.name} — {c.summary}" for c in later]
    return "\n".join(lines)


def _menu_commands() -> list[BotCommand]:
    """The live commands published to Telegram's ``/`` autocomplete menu (``setMyCommands``).

    Only live commands are advertised. Telegram caps a command description at 256 chars.
    """
    return [BotCommand(c.name, c.summary[:256]) for c in _COMMANDS if c.live]


def _build_request(*, get_updates: bool = False) -> HTTPXRequest:
    """One :class:`~telegram.request.HTTPXRequest` carrying the timeout budget.

    The library's default timeouts (5 s connect) are narrower than this box's first connect. A FRESH
    instance is required per role — a ``BaseRequest`` is ``initialize()``d and ``shutdown()``n by the
    bot that owns it, so the polling and send requests may never be the same object. Pool sizes mirror
    what ``ApplicationBuilder`` would supply (the per-timeout builder setters are mutually exclusive
    with ``.request()`` / ``.get_updates_request()``).
    """
    return HTTPXRequest(
        connection_pool_size=_GET_UPDATES_POOL_SIZE if get_updates else _POOL_SIZE,
        connect_timeout=_CONNECT_TIMEOUT_S,
        read_timeout=_READ_TIMEOUT_S,
        write_timeout=_WRITE_TIMEOUT_S,
        pool_timeout=_POOL_TIMEOUT_S,
    )


@dataclass
class _PendingChallenge:
    """A stashed two-step confirmation awaiting the owner's follow-up ``/confirm <phrase>``.

    ``apply`` is the coroutine that actually performs the destructive action once the phrase matches;
    it receives the proven :class:`OwnerConfirmation` and runs under :data:`Actor.OWNER`.
    """

    action: str                                   # human label, e.g. "mode->AUTO", "kill_reset"
    phrase: str                                   # one-time confirmation phrase
    expires_at: Any                               # tz-aware IST datetime (Clock.now() + TTL)
    apply: Callable[[OwnerConfirmation], Awaitable[str]]


@dataclass(frozen=True)
class _LedgerRec:
    """One ``recommendations`` row flattened for owner-facing lookup and listing.

    Only ``rec_id`` and ``delivered_at`` are actual columns: migration 0001 gives the table
    ``rec_id / payload / delivered_at / human_action / human_fill_price / outcome`` and nothing else,
    so the instrument, side, qty and kind are all parsed out of the ``payload`` JSON blob — the same
    place ``RecommendationBook.expire_stale`` reads ``valid_until`` from.
    """

    rec_id: str
    instrument: str
    side: str
    qty: int
    kind: str
    delivered_at: str | None
    #: A listed row can be from an earlier day (an entry expired inside the veto window, or a taken
    #: position), so the listing must be able to say so.
    valid_until: str | None = None


class TelegramBot:
    """Owner-only two-way Telegram control plane.

    Never originates orders; owner commands route through the injected ``ModeManager`` / ``KillSwitch``
    . Construction is cheap and import-safe; the network application is built in
    :meth:`start`.
    """

    def __init__(
        self,
        token: str,
        owner_chat_id: int,
        clock: Clock,
        *,
        mode_manager: ModeManager | None = None,
        kill_switch: KillSwitch | None = None,
        bus: EventBus | None = None,
        reco_book: RecoBook | None = None,
        latch: RiskStateLatch | None = None,
        governor: BudgetGovernor | None = None,
        limits_engine: LimitsEngine | None = None,
        exposure: ExposureTracker | None = None,
        session: SessionManager | None = None,
        conn: sqlite3.Connection | None = None,
        scan_sweep_fn: Callable[[str], Awaitable[str]] | None = None,
    ) -> None:
        self._token = token
        self._owner_chat_id = int(owner_chat_id)
        self._clock = clock
        self._mode = mode_manager
        self._kill = kill_switch
        self._bus = bus
        # RECOMMEND-mode data sources. Every one is OPTIONAL: composition wires them in a later
        # wave, and until then the owning command replies "not wired" rather than half-acting.
        self._reco_book = reco_book
        self._latch = latch
        self._governor = governor
        self._limits = limits_engine
        self._exposure = exposure
        self._session = session
        self._conn = conn
        #: async (trigger) -> owner-facing sweep verdict text.
        self._scan_sweep_fn = scan_sweep_fn
        self._why_fn: Callable[[str], Awaitable[str]] | None = None
        self._paper: PaperControl | None = None
        self._paper_autopilot_built: Callable[[], bool] = lambda: False
        self._paper_status_fn: Callable[[], dict[str, Any]] | None = None
        self._paper_counters_fn: Callable[[], dict[str, Any]] | None = None
        self._app: Application | None = None
        #: A start() that timed out AFTER start_polling succeeded leaves a LIVE poller behind; retained
        #: so stop() can tear it down (else owner commands run on a bot reported as disabled).
        self._failed_app: Application | None = None
        self._bus_attached = False
        # At most one challenge is pending at a time — a new destructive command supersedes the old.
        self._pending: _PendingChallenge | None = None
        #: Outbox drainer: owned by this bot, started with polling, cancelled by stop().
        self._drain_task: asyncio.Task[None] | None = None
        #: Consecutive FAILED drainer attempts, and whether this streak has already been logged loud.
        self._drain_fail_streak = 0
        self._outage_logged = False

    def set_reco_book(self, book: RecoBook) -> None:
        """Late-wire the recommendation book (built after the bot in the composition root)."""
        self._reco_book = book

    def set_scan_sweep_fn(self, fn: Callable[[str], Awaitable[str]]) -> None:
        """Late-wire the ``/scan_now`` sweep (the closure needs the pre-screen,
        which is built after the bot in the composition root)."""
        self._scan_sweep_fn = fn

    def set_why_fn(self, fn: Callable[[str], Awaitable[str]]) -> None:
        """Late-wire the ``/why`` reader (needs the market store, built after the bot)."""
        self._why_fn = fn

    def set_paper(
        self,
        control: PaperControl,
        *,
        autopilot_built: Callable[[], bool],
        status_fn: Callable[[], dict[str, Any]] | None = None,
        counters_fn: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        """Late-wire ``/paper``. ``status_fn`` returns ``open_positions`` / ``day_pnl``; ``counters_fn``
        returns the reconcile/void/corporate-action counters. Either absent prints ``n/a``."""
        self._paper = control
        self._paper_autopilot_built = autopilot_built
        self._paper_status_fn = status_fn
        self._paper_counters_fn = counters_fn

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Build the application, register the command table, and begin polling.

        Idempotent-ish: a second call while running is a no-op + warning. Uses long-polling (no public
        webhook) so the bot needs no inbound port on the LAN host.
        """
        if self._app is not None:
            _log.warning("telegram_start_noop", reason="already_started")
            return
        # Explicit transport — see :func:`_build_request`. Both roles get their own instance.
        app = (
            ApplicationBuilder()
            .token(self._token)
            .request(_build_request())
            .get_updates_request(_build_request(get_updates=True))
            .build()
        )
        self._register_handlers(app)
        # BOUNDED start: timeout or error degrades to a DISABLED bot (sends drop "not_started") rather
        # than holding the boot. ``self._app`` is assigned only on SUCCESS so a half-started app can
        # never serve sends.
        try:
            await asyncio.wait_for(self._start_network(app), timeout=_START_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - a broken Telegram must never hold or crash the boot
            _log.exception("telegram_start_failed_disabled", timeout_s=_START_TIMEOUT_S)
            # The timeout may have landed AFTER start_polling succeeded: retain the app so stop() can
            # reach it, and best-effort tear it down now.
            self._failed_app = app
            await self._teardown_app(app)
            return
        self._app = app
        # The drainer starts only on a SUCCESSFUL start: a disabled bot has no transport, and its
        # journal rows wait for the next process.
        self._start_outbox_drainer()
        _log.info("telegram_started", owner_chat_id=self._owner_chat_id)

    async def _start_network(self, app: Application) -> None:
        """The network leg of :meth:`start`, isolated so the caller can bound it as one unit."""
        await app.initialize()
        await app.start()
        if app.updater is not None:
            await app.updater.start_polling(drop_pending_updates=True)
        await self._publish_command_menu(app)

    @staticmethod
    async def _teardown_app(app: Application) -> None:
        """Best-effort teardown of a partially-started application; every step individually guarded
        (a timeout may have cancelled the start mid-initialize — any step may legitimately fail)."""
        for step in ("updater", "stop", "shutdown"):
            try:
                if step == "updater":
                    if app.updater is not None:
                        await asyncio.wait_for(app.updater.stop(), timeout=5)
                elif step == "stop":
                    await asyncio.wait_for(app.stop(), timeout=5)
                else:
                    await asyncio.wait_for(app.shutdown(), timeout=5)
            except Exception:  # noqa: BLE001 - teardown of a broken app is best-effort by definition
                pass

    async def stop(self) -> None:
        """Stop polling and shut the application down cleanly — including a partially-started app
        left behind by a degraded start().

        The outbox drainer is cancelled FIRST and unconditionally (before the ``app is None`` early
        return): it must not outlive the SQLite connection it queries."""
        await self._stop_outbox_drainer()
        failed, self._failed_app = self._failed_app, None
        if failed is not None:
            await self._teardown_app(failed)
        app = self._app
        if app is None:
            return
        try:
            if app.updater is not None:
                await app.updater.stop()
            await app.stop()
            await app.shutdown()
        finally:
            self._app = None
            _log.info("telegram_stopped")

    async def _publish_command_menu(self, app: Application) -> None:
        """Publish the live commands so the owner chat shows a ``/`` autocomplete menu (setMyCommands).

        Scoped to the owner chat. Best-effort: a failure is logged, not raised, and must never take
        down the control plane.
        """
        from telegram import BotCommandScopeChat

        try:
            await app.bot.set_my_commands(
                _menu_commands(), scope=BotCommandScopeChat(chat_id=self._owner_chat_id)
            )
            _log.info("telegram_commands_published", count=len(_menu_commands()))
        except Exception:  # noqa: BLE001 - menu publish is best-effort
            _log.exception("telegram_set_commands_failed")

    # ------------------------------------------------------------------ outbound (catalog)
    async def send(self, msg: Any) -> None:
        """Send a catalog message or plain text to the owner chat.

        Accepts a ``CatalogMessage`` (rendered via its ``.render()`` / ``.text`` / ``str()``) or a raw
        string. Never blocks the engine: a send failure is logged, not raised, so an alert path can
        never take down a caller.

        JOURNAL FIRST: the row lands in ``notifications`` BEFORE the wire is touched, so a message
        raised during an outage — or before the transport started — exists regardless of the network.
        That row is both the retry outbox (:meth:`_drain_outbox_once`) and what ``GET /notifications``
        shows the owner. Journalling is best-effort too: with no state store wired, or on a write
        failure, the send proceeds without it (see :meth:`_journal_insert`).

        LONG-MESSAGE GUARD: text over :data:`_TELEGRAM_MAX_CHARS` is split (:func:`_split_message`)
        into numbered parts sent sequentially through :meth:`_send_text`, each with the same
        bounded-timeout / logged-not-raised handling. The split is a TRANSPORT detail: one notification
        is ONE journal row however many parts the wire needed.
        """
        text = self._render(msg)
        try:
            notification_id = self._journal_insert(msg, text)
        except _Deduped:
            _log.info("notification_deduped", dedupe_key=msg.dedupe_key)
            return
        await self._deliver(text, notification_id, getattr(msg, "buttons", None))

    async def _deliver(
        self, text: str, notification_id: str | None, buttons: list[list[dict[str, str]]] | None = None,
    ) -> bool:
        """Put one already-journalled message on the wire and stamp its row with the verdict.

        Returns True when every part reached Telegram. The part loop STOPS at the first failure: the
        retry re-sends from part 1, so shipping later parts would only duplicate them. A ``None``
        transport (bot disabled / not started) is a first-class failure, not a silent drop: the row
        stays ``pending`` for the drainer. ``buttons`` ride on the last part only.
        """
        app = self._app
        if app is None:
            _log.warning("telegram_send_dropped", reason="not_started")
            self._journal_attempt_failed(notification_id, "NotStarted: telegram transport not started")
            return False
        if len(text) <= _TELEGRAM_MAX_CHARS:
            parts = [text]
        else:
            parts = _split_message(text)
            _log.info("telegram_message_split", parts=len(parts), total_chars=len(text))
        markup = _inline_markup(buttons)
        error: str | None = None
        for i, part in enumerate(parts, start=1):
            error = await self._send_text(app, part, markup if i == len(parts) else None)
            if error is not None:
                break
        if error is not None:
            self._journal_attempt_failed(notification_id, error)
            return False
        self._journal_delivered(notification_id)
        # Any success ends an outage streak, whether it came from a live send or a drainer retry.
        self._drain_fail_streak = 0
        self._outage_logged = False
        return True

    async def _send_text(
        self, app: Application, text: str, reply_markup: InlineKeyboardMarkup | None = None,
    ) -> str | None:
        """Put one already-bounded string on the wire (the choke point every outbound send/part
        passes through). Best-effort: a hang or failure is logged, never raised.

        RETURNS the verdict: ``None`` on success, or a bounded ``"ExcClass: first line"`` label for the
        journal's ``last_error`` — the outbox needs to know *whether* a send worked."""
        try:
            # BOUNDED: a hanging send must not wedge the boot; a bounded drop beats an unbounded wait.
            await asyncio.wait_for(
                app.bot.send_message(chat_id=self._owner_chat_id, text=text, reply_markup=reply_markup),
                timeout=_SEND_TIMEOUT_S,
            )
        except TimeoutError:
            _log.error("telegram_send_timeout", timeout_s=_SEND_TIMEOUT_S)
            return f"TimeoutError: send exceeded {_SEND_TIMEOUT_S:g}s"
        except Exception as exc:  # noqa: BLE001 - alerting must never crash the caller
            _log.exception("telegram_send_failed")
            return _error_label(exc)
        return None

    # ------------------------------------------------------------------ notification journal
    def _journal_insert(self, msg: Any, text: str) -> str | None:
        """Write the ``pending`` row for one outbound message; return its id (None if not journalled).

        EVERY journal write in this class is guarded the same way: a broken/locked/missing store logs
        ``notification_journal_failed`` at WARNING and returns — the journal must never become the
        thing that loses a message.
        """
        conn = self._conn
        if conn is None:
            return None
        try:
            kind, severity, title, body = _journal_fields(msg, text)
            buttons = getattr(msg, "buttons", None)
            notification_id = str(ULID())
            now = self._clock.now().isoformat()
            with transaction(conn):
                conn.execute(
                    "INSERT INTO notifications (notification_id, created_at, kind, severity, title, "
                    "body, status, attempts, dedupe_key, buttons) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
                    (notification_id, now, kind, severity, title, body, getattr(msg, "dedupe_key", None),
                     json.dumps(buttons) if buttons else None),
                )
            return notification_id
        except sqlite3.IntegrityError as exc:
            if getattr(msg, "dedupe_key", None) is not None:
                try:   # an expired-undelivered row must not block its key forever
                    with transaction(conn):
                        conn.execute(
                            "UPDATE notifications SET status='pending', attempts=0, last_error=NULL, "
                            "created_at=? WHERE dedupe_key=? AND status='failed'",
                            (now, msg.dedupe_key),
                        )
                except Exception as requeue_exc:  # noqa: BLE001 - see the clause below
                    _log.warning("notification_journal_failed", op="requeue",
                                 error=_error_label(requeue_exc))
                raise _Deduped from exc
            _log.warning("notification_journal_failed", op="insert", error=_error_label(exc))
            return None
        except Exception as exc:  # noqa: BLE001 - journalling must never block a send
            _log.warning("notification_journal_failed", op="insert", error=_error_label(exc))
            return None

    def _journal_delivered(self, notification_id: str | None) -> None:
        """Stamp a row delivered. ``attempts`` counts the attempt that SUCCEEDED too."""
        if notification_id is None or self._conn is None:
            return
        try:
            now = self._clock.now().isoformat()
            with transaction(self._conn):
                self._conn.execute(
                    "UPDATE notifications SET status='delivered', attempts=attempts+1, "
                    "delivered_at=?, last_attempt_at=?, last_error=NULL WHERE notification_id=?",
                    (now, now, notification_id),
                )
        except Exception as exc:  # noqa: BLE001 - see _journal_insert
            _log.warning("notification_journal_failed", op="delivered", error=_error_label(exc))

    def _journal_attempt_failed(self, notification_id: str | None, error: str) -> None:
        """Count a failed attempt against a row and record why. Status is left ``pending`` on purpose —
        only expiry (:meth:`_expire_if_due`) may move a row to ``failed``. The ``status='pending'``
        guard makes the write a no-op on a row some other path already resolved."""
        if notification_id is None or self._conn is None:
            return
        try:
            now = self._clock.now().isoformat()
            with transaction(self._conn):
                self._conn.execute(
                    "UPDATE notifications SET attempts=attempts+1, last_attempt_at=?, last_error=? "
                    "WHERE notification_id=? AND status='pending'",
                    (now, error[:_ERROR_LABEL_CHARS], notification_id),
                )
        except Exception as exc:  # noqa: BLE001 - see _journal_insert
            _log.warning("notification_journal_failed", op="attempt_failed", error=_error_label(exc))

    # ------------------------------------------------------------------ outbox drainer
    def _start_outbox_drainer(self) -> None:
        """Start the background drain loop (idempotent). No state store ⇒ no outbox ⇒ no task."""
        if self._conn is None:
            _log.info("telegram_outbox_disabled", reason="no_state_store")
            return
        if self._drain_task is not None and not self._drain_task.done():
            return
        self._drain_task = asyncio.create_task(self._drain_outbox_forever())
        _log.info("telegram_outbox_started", interval_s=_DRAIN_INTERVAL_S, batch=_DRAIN_BATCH)

    async def _stop_outbox_drainer(self) -> None:
        """Cancel + await the drain loop. Awaiting matters: the task may be mid-``execute`` on the
        shared connection, and the composition root closes that connection right after ``stop()``."""
        task, self._drain_task = self._drain_task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        _log.info("telegram_outbox_stopped")

    async def _drain_outbox_forever(self) -> None:
        """Re-attempt pending journal rows every :data:`_DRAIN_INTERVAL_S` until cancelled.

        Unkillable by a bad pass: any exception out of one drain is logged and the loop continues — the
        retry mechanism dying silently would mean lost notifications nobody knows about."""
        while True:
            try:
                await asyncio.sleep(_DRAIN_INTERVAL_S)
                await self._drain_outbox_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one bad pass must never end the drainer
                _log.exception("telegram_outbox_drain_failed")

    async def _drain_outbox_once(self) -> None:
        """One drain pass: expire what is stale, then retry what is DUE — critical first, else oldest.

        Reads a wide window of the queue (:data:`_DRAIN_SCAN`) and decides eligibility in Python:
        expire the stale, drop anything still inside its backoff window, order what remains
        critical-first / chronological, attempt at most :data:`_DRAIN_BATCH`. A row that cannot be
        attempted yet costs no slot, so it never sits in front of a fresh critical message.
        """
        conn = self._conn
        if conn is None or self._app is None:
            return
        now = self._clock.now()
        rows = conn.execute(
            "SELECT notification_id, created_at, kind, severity, title, body, attempts, "
            "last_attempt_at, buttons FROM notifications WHERE status='pending' "
            "ORDER BY created_at ASC LIMIT ?",
            (_DRAIN_SCAN,),
        ).fetchall()
        for row in self._due_rows(rows, now):
            text = _wire_text(row["severity"], row["title"], row["body"])
            delivered = await self._deliver(text, row["notification_id"], _loads_buttons(row["buttons"]))
            if delivered:
                _log.info(
                    "notification_retry_delivered",
                    notification_id=row["notification_id"],
                    kind=row["kind"],
                    attempts=(row["attempts"] or 0) + 1,
                )
            else:
                self._note_drain_failure()

    def _due_rows(self, rows: Sequence[Any], now: datetime) -> list[Any]:
        """Pick the (at most :data:`_DRAIN_BATCH`) rows this pass will actually attempt.

        In order:

        * **expire first, over the WHOLE scan** — a stale non-critical row is retired wherever it sits
          in the queue, so a backlog is not re-scanned forever.
        * **eligible only** — a row still inside its backoff window is not a candidate, rather than
          one that consumes a slot and declines to use it (the head-of-line guard).
        * **critical first, chronological within a class** — ``CRITICAL_KINDS`` or
          ``severity == 'critical'`` (:func:`_is_critical`, the predicate expiry uses). The enumeration
          index is the tie-breaker, so the SELECT's ``created_at ASC`` order survives within each class
          and the owner reads a backlog in the order the engine raised it.
        """
        eligible = [
            (rank, row)
            for rank, row in enumerate(rows)
            if not self._expire_if_due(row, now) and _backoff_elapsed(row, now)
        ]
        eligible.sort(
            key=lambda item: (
                0 if _is_critical(item[1]["severity"], item[1]["kind"]) else 1,
                item[0],
            )
        )
        return [row for _rank, row in eligible[:_DRAIN_BATCH]]

    def _expire_if_due(self, row: Any, now: datetime) -> bool:
        """Flip a NON-critical row that has been pending past :data:`_EXPIRY_H` to ``failed``.

        Critical rows never expire (see :data:`CRITICAL_KINDS`); they retry until delivered or the
        process ends."""
        if _is_critical(row["severity"], row["kind"]):
            return False
        created = _parse_iso(row["created_at"])
        if created is None or (now - created) < timedelta(hours=_EXPIRY_H):
            return False
        try:
            with transaction(self._conn):
                self._conn.execute(
                    "UPDATE notifications SET status='failed' "
                    "WHERE notification_id=? AND status='pending'",
                    (row["notification_id"],),
                )
        except Exception as exc:  # noqa: BLE001 - see _journal_insert
            _log.warning("notification_journal_failed", op="expire", error=_error_label(exc))
            return False
        _log.warning(
            "notification_expired",
            notification_id=row["notification_id"],
            kind=row["kind"],
            attempts=row["attempts"],
            age_h=_EXPIRY_H,
        )
        return True

    def _note_drain_failure(self) -> None:
        """Count a failed retry and raise ONE loud line per outage episode.

        Per streak, not per failure: on a dead link the drainer fails ten times a minute."""
        self._drain_fail_streak += 1
        if self._drain_fail_streak >= _OUTAGE_STREAK and not self._outage_logged:
            self._outage_logged = True
            _log.error(
                "telegram_outage",
                consecutive_failures=self._drain_fail_streak,
                note="owner notifications are queued in the notifications journal, not lost",
            )

    @staticmethod
    def _render(msg: Any) -> str:
        """Render a catalog message or text into a Telegram string."""
        if isinstance(msg, str):
            return msg
        for attr in ("render", "to_text"):
            fn = getattr(msg, attr, None)
            if callable(fn):
                return str(fn())
        text = getattr(msg, "text", None)
        return str(text) if text is not None else str(msg)

    # ------------------------------------------------------------------ command table
    def _live_handlers(self) -> dict[str, Callable[..., Awaitable[None]]]:
        """Name → bound handler for every command with a real implementation.

        Must cover exactly the ``live`` rows of :data:`_COMMANDS`: a live spec with no handler here
        fails fast at registration (KeyError), and an orphan handler with no spec is never registered —
        :func:`test_live_command_specs_have_handlers` pins both directions so the two never drift.
        """
        return {
            "status": self._cmd_status,
            "kill": self._cmd_kill,
            "kill_reset": self._cmd_kill_reset,
            "mode": self._cmd_mode,
            "trade_window": self._cmd_trade_window,
            "confirm": self._cmd_confirm,
            "help": self._cmd_help,
            "pause_entries": self._cmd_pause_entries,
            "resume_entries": self._cmd_resume_entries,
            "rearm": self._cmd_rearm,
            "taken": self._cmd_taken,
            "closed": self._cmd_closed,
            "veto": self._cmd_veto,
            "protected": self._cmd_protected,
            "close": self._cmd_close,
            "positions": self._cmd_positions,
            "pnl": self._cmd_pnl,
            "budget": self._cmd_budget,
            "limits": self._cmd_limits,
            "approve": self._cmd_approve,
            "reject": self._cmd_reject,
            "token": self._cmd_token,
            "scan_now": self._cmd_scan_now,
            "why": self._cmd_why,
            "paper": self._cmd_paper,
        }

    def _register_handlers(self, app: Application) -> None:
        """Wire every command in :data:`_COMMANDS` to its handler (all owner-guarded).

        Live commands use their bound handler; stubs register a logged not-implemented reply.
        Inline-button taps get one owner-guarded :meth:`_on_button`.
        """
        live = self._live_handlers()
        for spec in _COMMANDS:
            handler = live[spec.name] if spec.live else self._stub(spec.name)
            app.add_handler(CommandHandler(spec.name, _owner_only(self, handler)))
        app.add_handler(CallbackQueryHandler(_owner_only(self, self._on_button)))

    async def _on_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """One inline-button tap runs its command when the verb is allow-listed and dispatched; only
        ``/veto <rec_id> <reason>`` is. The query is always answered, because Telegram spins the
        button until it is: a refusal as an alert the owner must dismiss, a recorded veto before the
        tapped rec's buttons leave the message, so a failing edit or answer never reads as a failure."""
        query = update.callback_query
        command = _button_command(query.data)
        if command is None:
            _log.warning("telegram_button_refused", command=query.data, reason="unlisted")
            await query.answer("this button is not allowed", show_alert=True)
            return
        verb, args = command
        veto = _veto_args(args) if verb == "veto" else None
        if veto is None:
            _log.warning("telegram_button_refused", command=query.data, reason="malformed")
            await query.answer("malformed button", show_alert=True)
            return
        if self._reco_book is None:
            await query.answer("recommendation book not wired", show_alert=True)
            return
        rec_id, reason = veto
        _log.warning("telegram_button_veto", rec_id=rec_id, reason=reason)
        ok, text = await _book_result(self._reco_book.veto(rec_id, reason))
        with contextlib.suppress(Exception):
            await query.answer(text[:_ANSWER_MAX_CHARS], show_alert=None if ok else True)
        if not ok:
            return
        try:
            await query.edit_message_reply_markup(
                reply_markup=_markup_without(query.message.reply_markup, rec_id)
            )
        except Exception:  # noqa: BLE001 - the veto is recorded; stale buttons refuse a second tap
            _log.exception("telegram_button_edit_failed", rec_id=rec_id)

    def _stub(self, name: str) -> Callable[..., Awaitable[None]]:
        """A stub handler that logs the command and replies that it is not yet implemented."""

        async def _handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            _log.info("telegram_cmd_stub", command=name, args=_args(context))
            await _reply(update, f"/{name}: {_STUB_REPLY}.")

        _handler.__qualname__ = f"TelegramBot._stub.{name}"
        return _handler

    # ------------------------------------------------------------------ /help (fully wired)
    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """List every command and what it does (owner-only, read-only). Rendered from :data:`_COMMANDS`,
        so it always matches what is actually registered."""
        _log.info("telegram_cmd_help")
        await _reply(update, _help_text())

    # ------------------------------------------------------------------ /status (fully wired)
    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Report the sticky control-plane state (mode / routing / risk / kill / window). Read-only."""
        lines = [f"now: {self._clock.now().isoformat()}"]
        if self._mode is not None:
            window = self._mode.get_trade_window()
            lines += [
                f"mode: {self._mode.mode().value}",
                f"routing: {self._mode.routing().value if self._mode.routing() else '-'}",
                f"risk_state: {self._mode.risk_state().value}",
                f"trade_window: {window.start}-{window.end}" if window else "trade_window: (unset)",
            ]
        else:
            lines.append("mode: (ModeManager not wired)")
        if self._kill is not None:
            lines.append(
                f"kill: {'ENGAGED ' + (self._kill.reason() or '') if self._kill.is_killed() else 'clear'}"
            )
        if self._pending is not None:
            lines.append(f"pending_confirm: {self._pending.action} (reply /confirm <phrase>)")
        _log.info("telegram_cmd_status")
        await _reply(update, "\n".join(lines))

    # ------------------------------------------------------------------ /scan_now
    async def _cmd_scan_now(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """On-demand scanner sweep: live candidates enter the normal pipeline; the reply is ALWAYS an
        explicit verdict — candidates, pending arm levels, or "nothing to trade right now"."""
        if self._scan_sweep_fn is None:
            await _reply(update, "/scan_now: sweep not wired.")
            return
        _log.info("telegram_cmd_scan_now")
        try:
            text = await self._scan_sweep_fn("scan_now")
        except Exception as exc:  # noqa: BLE001 - a sweep failure is a reply, never a dead command
            _log.exception("telegram_scan_now_failed")
            await _reply(update, f"sweep failed: {type(exc).__name__}: {exc}")
            return
        await _reply(update, text)

    # ------------------------------------------------------------------ /why (read-only)
    async def _cmd_why(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Deterministic per-symbol status report; reads only, never sweeps or enqueues."""
        if self._why_fn is None:
            await _reply(update, "/why: not wired.")
            return
        args = _args(context)
        if len(args) != 1:
            await _reply(update, _USAGE_WHY)
            return
        _log.info("telegram_cmd_why", symbol=args[0])
        try:
            text = await self._why_fn(args[0])
        except Exception as exc:  # noqa: BLE001 - a failed report is a reply, never a dead command
            _log.exception("telegram_why_failed")
            await _reply(update, f"/why failed: {type(exc).__name__}: {exc}")
            return
        await _reply(update, text)

    # ------------------------------------------------------------------ /paper
    async def _cmd_paper(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """``on`` and ``reset`` are two-step and refused while an unexpired challenge is pending;
        ``off`` applies at once; ``status`` reads."""
        if self._paper is None:
            await _reply(update, "/paper: not wired.")
            return
        args = _args(context)
        sub = args[0].lower() if len(args) == 1 else ""
        paper = self._paper
        if sub == "status":
            await _reply(update, self._paper_status_text())
        elif sub == "off":
            paper.set_enabled(False, Actor.OWNER.value, via="telegram")
            _log.warning("telegram_cmd_paper_off")
            await _reply(update, "paper OFF: no new entries; resting paper entry orders are cancelled; "
                                 "exits and GTTs continue.")
        elif sub in ("on", "reset"):
            if sub == "on" and not self._paper_autopilot_built():
                await _reply(update, "/paper on refused: the paper subsystem is not enabled "
                                     "(paper.subsystem_enabled is false or it failed to build).")
                return
            if self._pending is not None and self._clock.now() <= self._pending.expires_at:
                await _reply(update, f"/paper {sub} refused: a confirmation for "
                                     f"{self._pending.action} is pending; /confirm it or let it expire.")
                return

            async def _apply(confirmation: OwnerConfirmation) -> str:
                if sub == "on":
                    paper.set_enabled(True, Actor.OWNER.value, via="telegram")
                    return "paper ON."
                paper.request_reset(Actor.OWNER.value)
                return "paper reset requested: exits all, then a new epoch once the book is flat."

            phrase = self._issue_challenge(f"paper {sub}", _apply)
            await _reply(update, f"Confirm paper {sub}. Reply:\n/confirm {phrase}")
        else:
            await _reply(update, _USAGE_PAPER)

    def _paper_status_text(self) -> str:
        assert self._paper is not None
        st = self._paper.state()
        info = self._paper_status_fn() if self._paper_status_fn else {}
        counters = self._paper_counters_fn() if self._paper_counters_fn else None
        halts = self._paper.active_halts()
        return "\n".join([
            *([] if self._paper_autopilot_built()
              else ["autopilot not built (subsystem disabled or construction failed); flags below are stored only"]),
            f"paper enabled: {st['enabled']} (changed {st['changed_at']} by {st['changed_by']})",
            f"epoch_started_at: {st['epoch_started_at']}",
            f"reset_requested_at: {st['reset_requested_at']}",
            f"open positions: {info.get('open_positions', 'n/a')}",
            f"day P&L: {info.get('day_pnl', 'n/a')}",
            "halts: " + (", ".join(f"{h['cause']}={h['rung']}" for h in halts) if halts else "none"),
            f"counters: {counters if counters is not None else 'n/a'}",
        ])

    # ------------------------------------------------------------------ /kill (single-step, wired)
    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Engage the kill switch — single-step (fast), owner-only."""
        if self._kill is None:
            await _reply(update, "/kill: kill switch not wired.")
            return
        reason = " ".join(context.args) if context.args else "owner /kill via Telegram"
        _log.critical("telegram_cmd_kill", reason=reason)
        await self._kill.trigger(reason, actor=Actor.OWNER)
        await _reply(update, f"KILL SWITCH ENGAGED: {reason}")

    # ------------------------------------------------------------------ /kill_reset (two-step, wired)
    async def _cmd_kill_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Begin the two-step kill reset. Stashes a challenge; /confirm completes it."""
        if self._kill is None:
            await _reply(update, "/kill_reset: kill switch not wired.")
            return

        async def _apply(confirmation: OwnerConfirmation) -> str:
            await self._kill.owner_reset(confirmation)
            return "kill switch reset."

        phrase = self._issue_challenge("kill_reset", _apply)
        await _reply(
            update,
            "Confirm KILL RESET — this re-enables trading. Reply:\n"
            f"/confirm {phrase}",
        )

    # ------------------------------------------------------------------ /mode (two-step for AUTO)
    async def _cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner mode change. →AUTO is two-step; ←OFF/RECOMMEND is single-step."""
        if self._mode is None:
            await _reply(update, "/mode: ModeManager not wired.")
            return
        if not context.args:
            await _reply(update, "usage: /mode <OFF|RECOMMEND|AUTO>")
            return
        try:
            target = Mode(context.args[0].strip().upper())
        except ValueError:
            await _reply(update, f"unknown mode {context.args[0]!r}; use OFF|RECOMMEND|AUTO.")
            return

        if target == Mode.AUTO:
            try:
                self._mode.check_allowed(Mode.AUTO)
            except ModeRefused as exc:
                await _reply(update, str(exc))
                return

            async def _apply(confirmation: OwnerConfirmation) -> str:
                await self._mode.request_transition(Mode.AUTO, Actor.OWNER, confirmation=confirmation)
                return "mode → AUTO."

            phrase = self._issue_challenge("mode->AUTO", _apply)
            _log.warning("telegram_cmd_mode_auto_challenge")
            await _reply(update, f"Confirm mode → AUTO. Reply:\n/confirm {phrase}")
            return

        # Downgrade / RECOMMEND: single-step owner request (no confirmation object needed).
        _log.warning("telegram_cmd_mode", target=target.value)
        await self._mode.request_transition(target, Actor.OWNER)
        await _reply(update, f"mode → {target.value}.")

    # ------------------------------------------------------------------ /trade_window (single-step)
    async def _cmd_trade_window(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the current window, or set it (single-step, owner-ID + validation + audit).

        ``/trade_window`` with no args shows the current value; ``/trade_window HH:MM HH:MM`` sets it.
        The setter validates (start<end, non-empty MIS sub-window, within session) inside
        ``ModeManager.set_trade_window`` and returns False on rejection — we surface that to the owner.
        """
        if self._mode is None:
            await _reply(update, "/trade_window: ModeManager not wired.")
            return
        if not context.args:
            window = self._mode.get_trade_window()
            await _reply(
                update,
                f"trade_window: {window.start}-{window.end} (buffer {window.squareoff_buffer_min}m)"
                if window else "trade_window: (unset)",
            )
            return
        if len(context.args) != 2:
            await _reply(update, "usage: /trade_window <HH:MM> <HH:MM>")
            return
        start = _parse_hhmm(context.args[0])
        end = _parse_hhmm(context.args[1])
        if start is None or end is None:
            await _reply(update, "invalid time; use 24h HH:MM, e.g. /trade_window 09:30 15:00.")
            return
        _log.warning("telegram_cmd_trade_window", start=str(start), end=str(end))
        ok = await self._mode.set_trade_window(start, end, Actor.OWNER)
        await _reply(
            update,
            f"trade_window set to {start}-{end}." if ok
            else "trade_window REJECTED (failed validation); value unchanged.",
        )

    # ------------------------------------------------------------------ /confirm (two-step completion)
    async def _cmd_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Complete a pending two-step challenge. Verifies the one-time phrase + expiry."""
        pending = self._pending
        if pending is None:
            await _reply(update, "nothing to confirm.")
            return
        supplied = context.args[0] if context.args else ""
        now = self._clock.now()
        if now > pending.expires_at:
            self._pending = None
            _log.warning("telegram_confirm_expired", action=pending.action)
            await _reply(update, "confirmation expired; re-issue the command.")
            return
        if not _secrets.compare_digest(supplied, pending.phrase):
            _log.warning("telegram_confirm_bad_phrase", action=pending.action)
            await _reply(update, "phrase did not match; confirmation NOT applied.")
            return

        # Phrase good + fresh: build the proven OwnerConfirmation and run the stashed action.
        self._pending = None
        confirmation = OwnerConfirmation(
            actor=Actor.OWNER,
            confirmed=True,
            phrase=pending.phrase,
            note="two-step via Telegram",
        )
        _log.warning("telegram_confirm_applied", action=pending.action)
        try:
            result = await pending.apply(confirmation)
        except Exception as exc:  # noqa: BLE001 - report the failure to the owner, don't crash the bot
            _log.exception("telegram_confirm_apply_failed", action=pending.action)
            await _reply(update, f"confirmation failed: {exc}")
            return
        await _reply(update, result)

    # ------------------------------------------------------------------ challenge helper
    def _issue_challenge(
        self, action: str, apply: Callable[[OwnerConfirmation], Awaitable[str]]
    ) -> str:
        """Mint a one-time phrase, stash the pending challenge (Clock-stamped TTL), return the phrase."""
        phrase = _secrets.token_hex(3)  # short, owner-typable one-time phrase
        self._pending = _PendingChallenge(
            action=action,
            phrase=phrase,
            expires_at=self._clock.now() + _CHALLENGE_TTL,
            apply=apply,
        )
        _log.info("telegram_challenge_issued", action=action)
        return phrase

    # ------------------------------------------------------------------ /pause_entries /resume_entries
    async def _cmd_pause_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner pause: latch the ``owner_pause`` cause at FROZEN. Exits keep running."""
        if self._latch is None:
            await _reply(update, "/pause_entries: risk-state latch not wired.")
            return
        state = await self._latch.set_cause(
            CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries", Actor.OWNER
        )
        _log.warning("telegram_cmd_pause_entries", risk_state=state.value)
        await _reply(
            update,
            f"entries PAUSED — risk_state {state.value}. Risk-reducing exits/protection continue. "
            "Send /resume_entries to re-arm.",
        )

    async def _cmd_resume_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner re-arm. Clears BOTH the owner pause and a rejection-storm freeze (the one FROZEN cause
        that never auto-recovers; this IS its recovery path). Any other cause still latching keeps
        entries closed, and the reply says so."""
        if self._latch is None:
            await _reply(update, "/resume_entries: risk-state latch not wired.")
            return
        await self._latch.clear_cause(CAUSE_OWNER_PAUSE, Actor.OWNER)
        state = await self._latch.clear_cause(CAUSE_REJECTION_STORM, Actor.OWNER)
        remaining = [cause for cause, _state, _detail in self._latch.active_causes()]
        _log.warning("telegram_cmd_resume_entries", risk_state=state.value, remaining=remaining)
        text = f"owner pause + rejection-storm freeze cleared — risk_state {state.value}."
        if remaining:
            text += " Still latched by: " + ", ".join(remaining) + "."
        await _reply(update, text)

    async def _cmd_rearm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner re-arm of floor stops: clears only the rungs equity is back above."""
        if self._latch is None or self._limits is None or self._exposure is None:
            await _reply(update, "/rearm: exposure, limits or risk latch not wired.")
            return
        cleared, still = await self._exposure.rearm_floors(
            floor_limits_from(self._limits.table()), self._latch, Actor.OWNER
        )
        _log.warning("telegram_cmd_rearm", cleared=cleared, still_breached=still)
        text = (f"re-armed: {', '.join(cleared)}" if cleared else "no floor stop re-armed")
        text += f" — risk_state {self._latch.resolved_state().value}."
        if still:
            text += " Still below the line: " + ", ".join(still) + "."
        await _reply(update, text)

    # ------------------------------------------------------------------ ticker → rec_id
    def _ledger_recs(self, human_action: str | None) -> list[_LedgerRec]:
        """Delivered recommendations in ONE lifecycle state, oldest first (read-only SELECT).

        ``human_action`` carries the whole lifecycle in one column — NULL (open) → ``taken`` →
        ``closed``, or ``dismissed`` / ``expired`` — so the states the capture commands care about are
        ``None`` (open; see :meth:`_open_recs`) and ``'taken'`` (what ``/closed`` may act on).
        ``delivered_at IS NOT NULL`` is the "the owner has actually seen it" half of open.

        EXPIRY IS NOT A COLUMN. A recommendation dies when its ``payload.valid_until`` passes; the
        15:45 ``reco_expire`` sweep only *labels* that afterwards, so between the two a dead row is
        still ``human_action IS NULL``. The filter recomputes it with the SAME shared predicate
        (``core.recommendations.recommendation_expired``) ``RecommendationBook.expire_stale`` uses —
        including its skip: an unparseable or naive ``valid_until`` is KEPT, so the two surfaces never
        disagree about which recommendations exist. Applied to the open set only — a taken
        recommendation is an open position, and positions do not expire.
        """
        if self._conn is None:
            return []
        sql = (
            "SELECT rec_id, payload, delivered_at FROM recommendations "
            "WHERE delivered_at IS NOT NULL AND human_action IS NULL ORDER BY delivered_at"
            if human_action is None else
            "SELECT rec_id, payload, delivered_at FROM recommendations "
            "WHERE delivered_at IS NOT NULL AND human_action=? ORDER BY delivered_at"
        )
        rows = self._conn.execute(sql, () if human_action is None else (human_action,)).fetchall()
        now = self._clock.now()
        recs: list[_LedgerRec] = []
        for row in rows:
            data = _payload_dict(row["payload"])
            if human_action is None and recommendation_expired(data.get("valid_until"), now):
                continue
            recs.append(
                _LedgerRec(
                    rec_id=str(row["rec_id"]),
                    instrument=str(data.get("instrument") or ""),
                    side=str(data.get("side") or "?"),
                    qty=_parse_int(str(data.get("qty") or 0)) or 0,
                    kind=str(data.get("kind") or "entry"),
                    delivered_at=row["delivered_at"],
                    valid_until=str(data["valid_until"]) if data.get("valid_until") else None,
                )
            )
        return recs

    def _open_recs(self, *, include_skipped: bool) -> list[_LedgerRec]:
        """What ``/veto`` and ``/taken`` may act on: the book's entry candidates (live, or expired
        inside the veto window) plus the live exit/adjust recs, which keep the bare veto. ``/taken``
        also sees an expired rec already given a skip reason (the owner executed it while live, and
        recording the fill clears the reason)."""
        entries = [
            _LedgerRec(
                rec_id=str(c["rec_id"]), instrument=str(c["instrument"]), side=str(c["side"]),
                qty=int(c["qty"]), kind=str(c["kind"]), delivered_at=c["delivered_at"],
                valid_until=c["valid_until"],
            )
            for c in self._reco_book.decision_candidates(
                None, self._clock.now(), include_skipped=include_skipped
            )
        ]
        others = [rec for rec in self._ledger_recs(None) if rec.kind != "entry"]
        return sorted(entries + others, key=lambda rec: rec.delivered_at or "")

    def _rec_kind(self, rec_id: str) -> str | None:
        """The ``kind`` of a ledger row, or None when it cannot be read (the book then decides)."""
        if self._conn is None:
            return None
        try:
            row = self._conn.execute(
                "SELECT payload FROM recommendations WHERE rec_id=?", (rec_id,)
            ).fetchone()
        except Exception:  # noqa: BLE001 - a failed lookup must never take down the control plane
            _log.exception("telegram_rec_kind_failed", rec_id=rec_id)
            return None
        return None if row is None else str(_payload_dict(row["payload"]).get("kind") or "entry")

    async def _resolve_rec_arg(
        self, update: Update, arg: str, *, human_action: str | None, include_skipped: bool = False,
    ) -> str | None:
        """Resolve a capture command's first argument to a real ``rec_id`` — or reply and return None.

        The argument is matched as a TICKER first and only then as an id:

        * **ticker, exactly one match** → that row's real ``rec_id``.
        * **ticker, several matches** → the list, with the FULL ids as the tiebreak handle, and NO
          action taken: which of two live recommendations the owner meant is not the bot's guess.
        * **ULID-shaped** → returned verbatim (:func:`_is_ulid`).
        * **neither** → a refusal that names the argument AND lists what is actionable.

        With no state store wired the argument passes straight through to the book, which owns the id
        namespace and answers an unknown id with an owner-facing ``ValueError``.
        """
        if self._conn is None:
            return arg
        try:
            recs = (
                self._open_recs(include_skipped=include_skipped)
                if human_action is None else self._ledger_recs(human_action)
            )
        except Exception:  # noqa: BLE001 - a failed lookup must never take down the control plane
            _log.exception("telegram_rec_lookup_failed", arg=arg)
            if _is_ulid(arg):
                return arg                     # the id path needs no ledger read; don't strand it
            await _reply(
                update,
                f"could not read the recommendation ledger to resolve {arg.strip().upper()}; "
                "re-send with the full rec_id.",
            )
            return None
        wanted = arg.strip().upper()
        # ``rec.instrument`` is "" for an unparseable payload; requiring a non-empty ticker keeps such a
        # row out of the match set (still reachable by id) and stops an empty argument resolving to it.
        matches = [rec for rec in recs if rec.instrument and rec.instrument.strip().upper() == wanted]
        now = self._clock.now()
        if human_action is None and len(matches) > 1:
            live = [rec for rec in matches if not recommendation_expired(rec.valid_until, now)]
            matches = live if len(live) == 1 else matches   # a yesterday's expired rec must not shadow today's
        if len(matches) == 1:
            _log.info(
                "telegram_rec_resolved",
                symbol=matches[0].instrument, rec_id=matches[0].rec_id, state=human_action or "open",
            )
            return matches[0].rec_id
        if matches:
            _log.warning("telegram_rec_ambiguous", symbol=wanted, count=len(matches))
            await _reply(update, _ambiguous_reply(wanted, matches, now.date()))
            return None
        if _is_ulid(arg):
            return arg
        _log.info(
            "telegram_rec_unresolved", arg=wanted, state=human_action or "open", candidates=len(recs)
        )
        await _reply(update, _no_match_reply(wanted, recs, human_action, now.date()))
        return None

    # ------------------------------------------------------------------ RECOMMEND outcome capture
    async def _cmd_taken(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner confirms they took a recommendation — creates the ``origin='recommended'`` position.

        The first argument is a SYMBOL or a ``rec_id``; qty/price are validated BEFORE the ledger is
        touched."""
        if self._reco_book is None:
            await _reply(update, "/taken: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 3:
            await _reply(update, f"usage: {_USAGE_TAKEN}")
            return
        qty, price = _parse_int(args[1]), _parse_decimal(args[2])
        if qty is None or qty <= 0 or price is None or price <= 0:
            await _reply(update, f"invalid qty/price; usage: {_USAGE_TAKEN}")
            return
        rec_id = await self._resolve_rec_arg(update, args[0], human_action=None, include_skipped=True)
        if rec_id is None:
            return                              # unresolvable: _resolve_rec_arg already replied
        _log.warning("telegram_cmd_taken", arg=args[0], rec_id=rec_id, qty=qty, price=str(price))
        _, text = await _book_result(self._reco_book.take(rec_id, qty, price))
        await _reply(update, text)

    async def _cmd_closed(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner marks a taken recommendation closed at ``price`` (outcome capture).

        Resolves the symbol among the TAKEN-not-closed recommendations; the open set holds proposals,
        not positions."""
        if self._reco_book is None:
            await _reply(update, "/closed: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 2:
            await _reply(update, f"usage: {_USAGE_CLOSED}")
            return
        price = _parse_decimal(args[1])
        if price is None or price <= 0:
            await _reply(update, f"invalid price; usage: {_USAGE_CLOSED}")
            return
        rec_id = await self._resolve_rec_arg(update, args[0], human_action="taken")
        if rec_id is None:
            return
        _log.warning("telegram_cmd_closed", arg=args[0], rec_id=rec_id, price=str(price))
        _, text = await _book_result(self._reco_book.close(rec_id, price))
        await _reply(update, text)

    async def _cmd_veto(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner declines a recommendation. Recorded as vetoed, never as a non-fill (a different
        label). An entry rec needs a reason code; exit/adjust recs take the bare form."""
        if self._reco_book is None:
            await _reply(update, "/veto: recommendation book not wired.")
            return
        args = _args(context)
        reason = args[1].lower() if len(args) == 2 else None
        if len(args) not in (1, 2) or (reason is not None and reason not in VETO_REASONS):
            await _reply(update, f"usage: {_USAGE_VETO}")
            return
        rec_id = await self._resolve_rec_arg(update, args[0], human_action=None)
        if rec_id is None:
            return
        if reason is None and self._rec_kind(rec_id) == "entry":
            await _reply(update, f"usage: /veto {args[0].upper()} <{'|'.join(VETO_REASONS)}>")
            return
        _log.warning("telegram_cmd_veto", arg=args[0], rec_id=rec_id, reason=reason)
        _, text = await _book_result(self._reco_book.veto(rec_id, reason))
        await _reply(update, text)

    async def _cmd_protected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner confirms the protective stop order is placed."""
        if self._reco_book is None:
            await _reply(update, "/protected: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 1:
            await _reply(update, f"usage: {_USAGE_PROTECTED}")
            return
        _log.warning("telegram_cmd_protected", arg=args[0])
        _, text = await _book_result(self._reco_book.protect(args[0]))
        await _reply(update, text)

    async def _cmd_close(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Exit guidance: in RECOMMEND the platform places ZERO API orders, so this sends nothing and
        changes no state; the owner exits in their terminal and reports it with ``/closed``."""
        args = _args(context)
        target = args[0] if args else "<position_id>"
        _log.info("telegram_cmd_close", position_id=args[0] if args else None)
        await _reply(
            update,
            f"/close {target}: nothing was sent to the broker. In RECOMMEND the exit decision AND the "
            "order are yours — the platform places zero API orders. Square off in your terminal, "
            f"then send {_USAGE_CLOSED} so the outcome is recorded. Platform-placed "
            "exits arrive with AUTO.",
        )

    # ------------------------------------------------------------------ read-only reports
    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Open positions, straight off the state store (read-only, no broker call). Paper positions
        get their own section, absent while there are none."""
        conn = self._conn
        if conn is None:
            await _reply(update, "/positions: state store not wired.")
            return

        def rows(where: str) -> list[Any]:
            return conn.execute(
                "SELECT position_id, symbol, side, product, qty, avg_entry, stop, target, origin, "
                "protection_state, owner_protected_at, state, strategy_id, exit_session, exit_policy "
                f"FROM positions WHERE {where} ORDER BY opened_at"
            ).fetchall()

        def line(row: Any) -> str:
            if row["protection_state"]:
                protection = row["protection_state"]
            elif row["origin"] == "recommended":
                protection = (f"owner-confirmed protected {row['owner_protected_at']}"
                              if row["owner_protected_at"] else "UNPROTECTED (unconfirmed)")
            else:
                protection = "unprotected"
            return (
                f"{row['symbol']} {row['side']} {row['qty']} @ {row['avg_entry']} · "
                f"{row['product'] or '-'} · stop {row['stop'] or '-'} · target {row['target'] or '-'} · "
                f"{row['origin']} · {protection} · id {row['position_id']}"
            )

        def paper_line(row: Any) -> str:
            exiting = " · EXITING" if row["state"] == "PENDING_EXIT" else ""
            if row["exit_policy"] == TIME_ONLY_POLICY:     # no GTT exists; an exit failing still shows
                protection = "no stop (exit test)" + (" · EXIT FAILED" if row["protection_state"] == "PROTECTION_FAILED"
                                                      else "")
            else:
                protection = f"GTT {row['protection_state'] or 'PENDING'}"
            return (
                f"{row['symbol']} {row['side']} {row['qty']} @ {row['avg_entry']} · "
                f"stop {row['stop'] or '-'} · target {row['target'] or '-'} · {row['strategy_id'] or '-'} · "
                f"exit session {row['exit_session'] or '-'} · {protection}"
                f"{exiting} · id {row['position_id']}"
            )

        real = rows(f"state='OPEN' AND {scope_sql('real')}")
        paper = rows(f"{HELD_STATES_SQL['paper']} AND {scope_sql('paper')}")
        _log.info("telegram_cmd_positions", count=len(real), paper=len(paper))
        lines = [f"open positions: {len(real)}", *map(line, real)] if real else ["no open positions."]
        if paper:
            lines += [f"paper positions: {len(paper)}", *map(paper_line, paper)]
        await _reply(update, "\n".join(lines))

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Today's P&L off :class:`~engine.risk.exposure.ExposureTracker` (rebuilt from the tables).

        ``origin='external'`` is excluded upstream — this is platform P&L, not account P&L. With no
        mark source the open-MTM term would silently read ₹0 (marks fall back to avg entry), so it is
        reported as unavailable.
        """
        if self._exposure is None:
            await _reply(update, "/pnl: exposure tracker not wired.")
            return
        exp = self._exposure
        today = self._clock.today()
        lines = [f"P&L {today.isoformat()} (platform only; external excluded)"]
        base = exp.capital_base

        def money(x: Any) -> str:
            return f"{_inr(x)}{_pct_suffix(pct_of(x, base))}"

        realized_today = _realized_today(exp, today)
        if realized_today is not None:
            lines.append(f"realised today: {money(realized_today)}")
        lines.append(f"day MTM: {money(exp.day_mtm())}")
        lines.append(f"realised all-time: {money(exp.realized_net())}")
        # No mark source ⇒ open_mtm() degrades to zero-at-entry; don't present that as a real MTM.
        if not exp.has_mark_source():
            lines.append("open MTM: n/a offline (no mark source)")
        else:
            lines.append(f"open MTM: {money(exp.open_mtm())}")
        lines.append(f"equity: {_inr(exp.equity())} · open: {exp.open_position_counts().total}")
        _log.info("telegram_cmd_pnl")
        await _reply(update, "\n".join(lines))

    async def _cmd_budget(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Quota-week-to-date LLM spend vs the per-agent allocations + the active degrade tier."""
        if self._governor is None:
            await _reply(update, "/budget: budget governor not wired.")
            return
        gov = self._governor
        credit = gov.credit()
        window = gov.window_key()
        start, end = gov.window_bounds(window)
        allocations: dict[str, Any] = gov.allocations()
        # `None` is UNMEASURABLE, not zero (governor.pro_rata_to_date): $0.0000 would read as 100% over
        # pace while the pace rungs are off.
        pace = gov.pro_rata_to_date(window)
        lines = [
            f"window: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} IST",
            f"window spend: {_usd(gov.window_spend(window))}"
            + (f" / {_usd(credit)} credit" if credit else ""),
            f"pro-rata to date: {_usd(pace)} (trading sessions)" if pace is not None
            else "pro-rata to date: unmeasurable — no trading session elapsed yet (pace rungs off)",
            f"tier: {gov.degrade_tier(window).value} · forward cap: {gov.prescreen_forward_cap()}",
        ]
        # Union of allocated and billed agents: an unallocated agent's spend is inside `window spend`.
        agents = sorted(set(allocations) | set(gov.window_agents(window)))
        if agents:
            lines.append("per agent (spend / allocation):")
            lines += [
                f"  {agent}: {_usd(gov.agent_spend(agent, window))} / "
                + (_usd(allocations[agent]) if agent in allocations else "(unallocated)")
                for agent in agents
            ]
        _log.info("telegram_cmd_budget")
        await _reply(update, "\n".join(lines))

    async def _cmd_limits(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Live headroom against the risk-limit caps: what is used vs what the protected store allows."""
        if self._limits is None or self._exposure is None:
            await _reply(update, "/limits: limits engine / exposure tracker not wired.")
            return
        try:
            table = self._limits.table()
        except Exception as exc:  # noqa: BLE001 - an integrity failure is the gate's call, not the bot's
            _log.exception("telegram_limits_load_failed")
            await _reply(update, f"/limits: limits unavailable ({exc}).")
            return
        lim, exp = table.limits, self._exposure
        counts = exp.open_position_counts()
        dm = exp.day_mtm()
        lines = [
            f"equity: {_inr(exp.equity())} (base {_inr(table.capital_base_inr)})",
            f"day MTM: {_inr(dm)}{_pct_suffix(pct_of(dm, exp.capital_base))} "
            f"(soft {lim.daily_loss_soft.day_mtm_pct}% / hard {lim.daily_loss_hard.day_mtm_pct}%)",
            f"open: {counts.total}/{lim.max_open_positions.total} "
            f"(MIS {counts.mis}/{lim.max_open_positions.max_mis}, "
            f"CNC {counts.cnc}/{lim.max_open_positions.max_cnc})",
            f"consecutive losses: {exp.consecutive_losses()}/{lim.consecutive_losses.max_per_session}",
            f"trades today: {exp.trades_opened_today()}/{lim.max_new_trades_day.count}",
            f"deployed capital: {_inr(exp.deployed_capital())} / "
            f"{_inr(lim.capital_cap.max_deployed_capital_inr)}",
            f"per-trade risk: intraday {lim.per_trade_risk.intraday_pct}% · "
            f"swing/position {lim.per_trade_risk.swing_position_pct}%",
        ]
        _log.info("telegram_cmd_limits")
        await _reply(update, "\n".join(lines))

    # ------------------------------------------------------------------ owner approvals
    async def _cmd_approve(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Approve one ``owner_approvals`` row (e.g. a stop-widen)."""
        await self._resolve_approval(update, context, "approved")

    async def _cmd_reject(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Reject one ``owner_approvals`` row."""
        await self._resolve_approval(update, context, "rejected")

    async def _resolve_approval(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, status: str
    ) -> None:
        """Stamp the approval row and report it. NO side effect beyond the row in RECOMMEND mode. If the
        injected book can push an approved stop onto a tracked position it is offered the approval
        (probe, never a requirement: RecommendationBook has no apply_approval in RECOMMEND mode)."""
        verb = "approve" if status == "approved" else "reject"
        if self._conn is None:
            await _reply(update, f"/{verb}: state store not wired.")
            return
        args = _args(context)
        if len(args) != 1:
            await _reply(update, f"usage: /{verb} <approval_id>")
            return
        approval_id = args[0]
        row = self._conn.execute(
            "SELECT approval_id, kind, payload, status FROM owner_approvals WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        if row is None:
            await _reply(update, f"unknown approval {approval_id}.")
            return
        current = row["status"] or "pending"
        if current != "pending":
            await _reply(update, f"approval {approval_id} already {current}; unchanged.")
            return
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE owner_approvals SET status=?, resolved_at=? WHERE approval_id=?",
                (status, now, approval_id),
            )
        _log.warning("telegram_cmd_approval", approval_id=approval_id, status=status, kind=row["kind"])
        lines = [f"approval {approval_id} {status} ({row['kind'] or 'unknown kind'})"]
        payload = _render_payload(row["payload"])
        if payload:
            lines.append(payload)
        if status == "approved":
            applied = await self._apply_approval(approval_id)
            if applied:
                lines.append(applied)
        await _reply(update, "\n".join(lines))

    async def _apply_approval(self, approval_id: str) -> str | None:
        """Offer an approved approval to the recommendation book, if it implements ``apply_approval``."""
        apply = getattr(self._reco_book, "apply_approval", None) if self._reco_book else None
        if apply is None:
            return None
        try:
            return str(await apply(approval_id))
        except Exception as exc:  # noqa: BLE001 - the row is already stamped; report, never crash
            _log.exception("telegram_apply_approval_failed", approval_id=approval_id)
            return f"(applying the approval failed: {exc})"

    # ------------------------------------------------------------------ /token (fallback path)
    async def _cmd_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Complete the daily Kite login with a pasted ``request_token`` — the fallback for when
        the ``/kite/callback`` redirect cannot reach the engine (off-LAN)."""
        if self._session is None:
            await _reply(update, "/token: session manager not wired.")
            return
        args = _args(context)
        if len(args) != 1:
            await _reply(update, "usage: /token <request_token>")
            return
        try:
            await self._session.complete_login(args[0])
        except Exception as exc:  # noqa: BLE001 - a bad/expired token must not kill the control plane
            _log.exception("telegram_cmd_token_failed")
            await _reply(update, f"login failed: {exc}")
            return
        _log.warning("telegram_cmd_token_ok")
        await _reply(update, "Session live ✅ — daily Kite login complete.")

    # ------------------------------------------------------------------ outbound bus alerts
    def attach_bus(self, bus: EventBus | None = None) -> None:
        """Subscribe the owner-alert handlers to the control-plane topics.

        Every handler renders through the notification catalog and calls :meth:`send` (best-effort), so a
        Telegram outage can never break a publisher (mode change, kill, risk-state latch). A second
        call is a no-op (it would double-send).
        """
        target = bus if bus is not None else self._bus
        if target is None:
            _log.warning("telegram_attach_bus_noop", reason="no_bus")
            return
        if self._bus_attached:
            _log.warning("telegram_attach_bus_noop", reason="already_attached")
            return
        self._bus = target
        target.subscribe(TOPIC_MODE_CHANGED, self._on_mode_changed)
        target.subscribe(TOPIC_RISK_STATE, self._on_risk_state)
        target.subscribe(TOPIC_KILL_STATE, self._on_kill_state)
        target.subscribe(TOPIC_TRADE_WINDOW, self._on_trade_window)
        target.subscribe(TOPIC_BUDGET_STATE, self._on_budget_state)
        self._bus_attached = True
        _log.info("telegram_bus_attached")

    async def _on_mode_changed(self, event: ModeChanged) -> None:
        await self.send(
            catalog.mode_change(
                event.old_mode.value, event.new_mode.value, event.actor.value, event.reason
            )
        )

    async def _on_risk_state(self, event: RiskStateChanged) -> None:
        await self.send(
            catalog.risk_state_change(event.old_state.value, event.new_state.value, event.reason)
        )

    async def _on_kill_state(self, event: KillStateChanged) -> None:
        await self.send(
            catalog.kill_state(killed=event.killed, reason=event.reason, actor=event.actor.value)
        )

    async def _on_trade_window(self, event: TradeWindowChanged) -> None:
        await self.send(
            catalog.trade_window_changed(
                start=event.start_ist,
                end=event.end_ist,
                buffer_min=event.squareoff_buffer_min,
                actor=event.actor.value,
            )
        )

    async def _on_budget_state(self, event: BudgetStateChanged) -> None:
        await self.send(
            catalog.budget_tier(
                event.old_tier.value, event.new_tier.value, event.window_spend_usd, event.window_key
            )
        )


# ---------------------------------------------------------------------- module-level guards/helpers


def _owner_only(
    bot: TelegramBot, handler: Callable[..., Awaitable[None]]
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]:
    """Owner-ID lock: drop (log + ignore) any update not from the owner's chat.

    Wraps a bound :class:`TelegramBot` handler. The check is on ``effective_chat.id`` — the only
    identity Telegram authenticates — and a non-owner gets no reply (the bot never confirms its
    existence to an unknown chat).
    """

    @wraps(handler)
    async def _guarded(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if chat is None or chat.id != bot._owner_chat_id:
            _log.warning(
                "telegram_rejected_non_owner",
                from_chat_id=getattr(chat, "id", None),
                command=getattr(getattr(update, "message", None), "text", None)
                or getattr(getattr(update, "callback_query", None), "data", None),
            )
            return
        try:
            await handler(update, context)
        except Exception as exc:  # noqa: BLE001 - a raising command answers the owner, never goes silent
            _log.exception("telegram_command_failed", handler=getattr(handler, "__name__", None))
            with contextlib.suppress(Exception):
                await _reply(update, f"command failed: {type(exc).__name__}: {exc}"[:400])

    return _guarded


def _inline_markup(buttons: list[list[dict[str, str]]] | None) -> InlineKeyboardMarkup | None:
    """Catalog button rows as Telegram inline buttons, the command as the callback data. A malformed
    or oversized set ships the text without buttons: Telegram would reject the whole message, and a
    critical row would retry forever."""
    if not buttons:
        return None
    try:
        rows = [
            [InlineKeyboardButton(b["text"], callback_data=b["command"]) for b in row] for row in buttons
        ]
        if any(len(b.callback_data.encode()) > _CALLBACK_DATA_MAX_BYTES for row in rows for b in row):
            raise ValueError(f"callback data over {_CALLBACK_DATA_MAX_BYTES} bytes")
        return InlineKeyboardMarkup(rows)
    except Exception as exc:  # noqa: BLE001 - see the docstring
        _log.warning("telegram_buttons_dropped", error=_error_label(exc))
        return None


def _loads_buttons(raw: str | None) -> list[list[dict[str, str]]] | None:
    """A journalled ``buttons`` value; None when absent or unreadable (the text still ships)."""
    try:
        return json.loads(raw) if raw else None
    except ValueError:
        return None


def _button_command(data: str | None) -> tuple[str, list[str]] | None:
    """``(verb, args)`` of a tapped ``/<verb> …`` command whose verb is allow-listed, else None."""
    parts = (data or "").split()
    if not parts or not parts[0].startswith("/") or parts[0][1:] not in _BUTTON_COMMANDS:
        return None
    return parts[0][1:], parts[1:]


def _veto_args(args: list[str]) -> tuple[str, str] | None:
    """``(rec_id, reason)`` of a tapped veto: a rec id, never a ticker a tap could resolve wrongly."""
    if len(args) != 2 or not _is_ulid(args[0]) or args[1] not in VETO_REASONS:
        return None
    return args[0], args[1]


def _markup_without(markup: InlineKeyboardMarkup | None, rec_id: str) -> InlineKeyboardMarkup | None:
    """``markup`` minus every button acting on ``rec_id``; None once nothing is left."""
    rows = [
        [b for b in row if str(b.callback_data or "").split()[1:2] != [rec_id]]
        for row in (markup.inline_keyboard if markup is not None else ())
    ]
    rows = [row for row in rows if row]
    return InlineKeyboardMarkup(rows) if rows else None


async def _reply(update: Update, text: str) -> None:
    """Reply in the originating (owner) chat; tolerate a missing message object."""
    message = getattr(update, "effective_message", None) or getattr(update, "message", None)
    if message is not None:
        await message.reply_text(text)


def _args(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    """Command arguments, or an empty list."""
    return list(context.args) if context.args else []


def _parse_int(raw: str) -> int | None:
    """Parse a whole-number command argument; None on anything malformed."""
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return None


def _parse_decimal(raw: str) -> Decimal | None:
    """Parse a price argument as an exact :class:`Decimal` (money is never a float). Thousands
    separators are tolerated because owners type them; anything else is None."""
    try:
        return Decimal(raw.strip().replace(",", ""))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _inr(value: Any) -> str:
    """Render a rupee amount for the owner. Display-only rounding — the stored value stays Decimal."""
    return f"₹{Decimal(value):,.2f}"


def _pct_suffix(p: str | None) -> str:
    """`` (+2.30%)`` after a rupee figure; nothing when there is no percent, no sign on zero."""
    if p is None:
        return ""
    sign = "+" if Decimal(p) > 0 else ""
    return f" ({sign}{p}%)"


def _usd(value: Any) -> str:
    """Render a USD budget amount. Four places: per-call LLM costs are fractions of a cent."""
    return f"${Decimal(value):,.4f}"


async def _book_result(call: Awaitable[str]) -> tuple[bool, str]:
    """Await a :class:`RecoBook` call: ``(True, summary)``, or ``(False, owner-facing failure line)``.

    ``ValueError`` is the book's contract for "unknown rec / invalid transition" and carries an
    owner-facing message, surfaced verbatim; anything else is logged with a traceback and reported —
    a bad book call must never take down the control plane."""
    try:
        return True, await call
    except ValueError as exc:
        _log.warning("telegram_reco_book_rejected", error=str(exc))
        return False, str(exc)
    except Exception as exc:  # noqa: BLE001 - report, never crash the bot
        _log.exception("telegram_reco_book_failed")
        return False, f"failed: {exc}"


# ---------------------------------------------------------------------- ticker resolution


def _is_ulid(arg: str) -> bool:
    """Is this argument SHAPED like a ``rec_id`` — 26 characters of Crockford base32?

    Shape only, never a claim the row exists (the book is the authority on real ids). It keeps a
    genuine id out of the ticker branch, and is safe because Crockford's alphabet omits I/L/O/U and
    contains no ``&`` or ``-``: a 26-character NSE ticker of only those characters does not exist.
    """
    text = arg.strip().upper()
    return len(text) == _ULID_CHARS and all(char in _ULID_ALPHABET for char in text)


def _payload_dict(raw: str | None) -> dict[str, Any]:
    """The ``recommendations.payload`` blob as a mapping; ``{}`` for anything unreadable.

    Never raises: a garbled payload costs the row its ticker (still actionable by id), not the whole
    listing."""
    try:
        data = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _rec_line(rec: _LedgerRec, today: date) -> str:
    """One ledger row as owner prose, carrying the FULL ``rec_id``.

    The id is the tiebreak handle the owner types back when the ticker is ambiguous; a truncated id is
    not a handle.

    A bare ``HH:MM`` is unambiguous only for a row delivered TODAY. Unless both the delivery and the
    expiry fall on ``today`` (IST), both stamps carry their day."""
    delivered, valid = _parse_iso(rec.delivered_at), _parse_iso(rec.valid_until)
    if delivered is not None and valid is not None and {delivered.date(), valid.date()} != {today}:
        when = f"delivered {_day_hhmm(delivered)} · valid till {_day_hhmm(valid)}"
    else:
        when = f"delivered {_hhmm(rec.delivered_at)}"
    return f"  {rec.instrument} {rec.side} x{rec.qty} · {when} · id {rec.rec_id}"


def _hhmm(raw: str | None) -> str:
    """A journalled IST timestamp as ``HH:MM`` for the owner; ``?`` when it cannot be read."""
    stamp = _parse_iso(raw)
    return stamp.strftime("%H:%M") if stamp is not None else "?"


def _day_hhmm(stamp: datetime) -> str:
    """``Fri 11 Sep 14:47`` — the day-bearing form of :func:`_hhmm` for a row that outlives its day."""
    return stamp.strftime("%a %d %b %H:%M")


def _rec_lines(recs: list[_LedgerRec], today: date) -> list[str]:
    """At most :data:`_MAX_LIST_ROWS` ledger lines, with the overflow ACCOUNTED FOR rather than
    dropped — a silently short list would misinform the owner about what is live."""
    if len(recs) <= _MAX_LIST_ROWS:
        return [_rec_line(rec, today) for rec in recs]
    shown = recs[:_MAX_LIST_ROWS]
    return [*(_rec_line(rec, today) for rec in shown), f"  … and {len(recs) - _MAX_LIST_ROWS} more"]


def _ambiguous_reply(symbol: str, matches: list[_LedgerRec], today: date) -> str:
    """Several live recommendations share a ticker: list them and act on NONE."""
    return "\n".join(
        [
            f"{len(matches)} recommendations match {symbol} — nothing done. "
            "Re-send with the full rec_id:",
            *_rec_lines(matches, today),
        ]
    )


def _no_match_reply(
    symbol: str, recs: list[_LedgerRec], human_action: str | None, today: date
) -> str:
    """No recommendation answers to this argument: say so, then show what the owner CAN act on."""
    label = "open" if human_action is None else human_action
    head = f"no {label} recommendation for {symbol}."
    if not recs:
        return f"{head} none {label} right now."
    return "\n".join([head, f"{label} now:", *_rec_lines(recs, today)])


def _render_payload(raw: str | None) -> str:
    """Render an ``owner_approvals`` payload as owner-facing lines.

    Prose, not the raw JSON blob (the leak ``CatalogMessage.render`` also prevents)."""
    if not raw:
        return ""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return str(raw)
    if isinstance(data, dict):
        return "\n".join(f"  {key}: {value}" for key, value in data.items())
    return str(data)


def _realized_today(exposure: Any, day: date) -> Decimal | None:
    """Net realised P&L of platform/recommended positions CLOSED today, or None if unavailable —
    a read-only report must never be the thing that breaks."""
    try:
        return exposure.realized_net_closed_on(day)
    except Exception:  # noqa: BLE001 - a display line is never worth failing the command over
        _log.exception("telegram_realized_today_failed")
        return None


def _split_message(
    text: str, max_chars: int = _SPLIT_CHUNK_CHARS, max_parts: int = _SPLIT_MAX_PARTS
) -> list[str]:
    """Split an over-cap message into ``<= max_parts`` Telegram-sized pieces.

    Walks ``text`` by index so every chunk is an exact substring and the truncation count is exact.
    Each window prefers the last newline inside it; a single line longer than ``max_chars`` is cut
    hard at ``max_chars``. The boundary newline is dropped, not carried into either chunk.

    Every returned piece carries a ``"(part i/N)"`` suffix. Beyond ``max_parts`` chunks, the remaining
    content is dropped from the wire and accounted for on the final part as
    ``"... [truncated K chars]"`` — an oversized alert must degrade, never vanish silently.
    """
    pieces: list[tuple[str, int]] = []  # (chunk text, index in `text` right after the chunk)
    pos, n = 0, len(text)
    while pos < n:
        end = min(pos + max_chars, n)
        if end < n:
            nl = text.rfind("\n", pos, end)
            if nl > pos:
                pieces.append((text[pos:nl], nl + 1))  # drop the boundary newline itself
                pos = nl + 1
                continue
        pieces.append((text[pos:end], end))
        pos = end

    total = len(pieces)
    if total <= max_parts:
        return [f"{chunk}\n(part {i}/{total})" for i, (chunk, _end) in enumerate(pieces, start=1)]

    kept = pieces[:max_parts]
    remaining = n - kept[-1][1]
    parts = [f"{chunk}\n(part {i}/{max_parts})" for i, (chunk, _end) in enumerate(kept[:-1], start=1)]
    last_chunk = kept[-1][0]
    parts.append(f"{last_chunk}\n(part {max_parts}/{max_parts})\n... [truncated {remaining} chars]")
    return parts


# ---------------------------------------------------------------------- notification journal helpers


def _journal_fields(msg: Any, text: str) -> tuple[str | None, str | None, str, str]:
    """Split one outbound message into the ``(kind, severity, title, body)`` the journal row stores.

    A :class:`~engine.notify.catalog.CatalogMessage` already carries all four. A PLAIN STRING is split
    on its FIRST newline (title, body) — the catalog's own shape — so :func:`_wire_text` reconstructs
    the original string exactly.
    """
    kind = getattr(msg, "kind", None)
    severity = getattr(msg, "severity", None)
    title = getattr(msg, "title", None)
    body = getattr(msg, "body", None)
    if not isinstance(title, str) or not isinstance(body, str):
        title, _, body = text.partition("\n")
    return (
        str(kind) if kind is not None else None,
        str(severity) if severity is not None else None,
        title,
        body,
    )


def _wire_text(severity: str | None, title: str, body: str) -> str:
    """Rebuild the exact wire text of a journalled row — the inverse of :func:`_journal_fields`.

    Mirrors ``CatalogMessage.render`` for a catalog row (severity glyph + title, then body) and is the
    identity for a plain-string row (no severity ⇒ no glyph ⇒ ``title`` + ``\\n`` + ``body``). A retry
    must ship the SAME bytes the first attempt did, not a re-derivation that could drift."""
    glyph = _SEVERITY_GLYPH.get(severity or "", "")
    head = f"{glyph} {title}".strip() if glyph else title
    return f"{head}\n{body}" if body else head


def _is_critical(severity: str | None, kind: str | None) -> bool:
    """True for a row that must never expire: a ``critical`` severity, or a :data:`CRITICAL_KINDS`
    kind whose catalog severity understates how bad losing it would be (``recommendation`` is
    ``info``)."""
    return severity == "critical" or (kind is not None and kind in CRITICAL_KINDS)


def _parse_iso(raw: str | None) -> datetime | None:
    """Parse a journalled IST timestamp back to a tz-aware datetime; None if absent/unparseable.

    Never raises: a garbled timestamp must not stall the drainer, and every caller treats None as
    "cannot judge this row on time" and falls through to the safe branch (retry now, never expire)."""
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else None


def _backoff_elapsed(row: Any, now: datetime) -> bool:
    """Has this row's next-attempt time arrived? ``min(300, 30 * 2**(attempts-1))`` s after the last
    attempt (or after ``created_at`` when the row has never been attempted at all)."""
    attempts = int(row["attempts"] or 0)
    if attempts <= 0:
        return True                      # journalled but never attempted — eligible immediately
    last = _parse_iso(row["last_attempt_at"]) or _parse_iso(row["created_at"])
    if last is None:
        return True                      # no usable clock on the row: retry rather than strand it
    # The exponent is clamped too: a critical row never expires, so ``attempts`` can grow without bound.
    delay = min(_BACKOFF_MAX_S, _BACKOFF_BASE_S * (2 ** min(attempts - 1, 16)))
    return (now - last) >= timedelta(seconds=delay)


def _error_label(exc: BaseException) -> str:
    """``"ExcClass: first line of its message"``, bounded for the journal's ``last_error`` column."""
    detail = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {detail[0] if detail else ''}"[:_ERROR_LABEL_CHARS]


def _parse_hhmm(raw: str):
    """Parse ``HH:MM`` (24h) into a ``datetime.time``; return None on any malformed input."""
    from datetime import time as _time

    parts = raw.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        hh, mm = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return _time(hh, mm)
