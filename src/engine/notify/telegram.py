"""Telegram owner control plane (§3.2.11, O8/R10).

The two-way owner I/O surface. Depends on ``core`` plus read-only / command-routing views over
``risk`` (``ModeManager`` / ``KillSwitch``); it **never originates orders itself** — every owner
command routes through those Tier-2 interfaces, exactly as the dashboard does (§3.2.11).

Two load-bearing security properties ship here in Phase 0:

* **Owner-ID lock (R10):** :func:`_owner_only` rejects (logs + ignores) any update whose
  ``effective_chat.id`` is not the configured ``owner_chat_id``. No other chat can reach a handler.
* **Two-step confirmation (R10):** destructive / mode-up commands (``/mode AUTO``, ``/kill_reset``,
  and later the stop-widening approvals) issue a one-time challenge phrase and stash a pending
  :class:`OwnerConfirmation`-style challenge; a follow-up ``/confirm <phrase>`` completes it. Killing
  is single-step (fast, §7.2); only *reset* / mode-up are two-step.

Phase 2 completes the §3.2.11 command surface: every row of :data:`_COMMANDS` now has a real handler.
The Phase-2 data sources arrive as **optional keyword dependencies** — ``reco_book``, ``latch``,
``governor``, ``limits_engine``, ``exposure``, ``session``, ``conn`` — each defaulting to ``None``, in
which case its command replies "… not wired" instead of pretending to act. Composition (§3.2.12) wires
them in a later wave; the bot itself never constructs a dependency and never originates an order.

Two RECOMMEND-mode honesty rules are encoded here (B7/§3.6): the platform places **zero API orders**,
so ``/close`` returns guidance rather than sending anything, and ``/approve`` writes only its
``owner_approvals`` row (a tracked position's stop is updated only if the injected recommendation book
exposes ``apply_approval`` — probed, never required).

The command catalog is the single source of truth for handler registration, ``/help``, and the
Telegram ``/`` autocomplete menu (``setMyCommands``, owner-chat-scoped), so the three never drift.
:meth:`TelegramBot.attach_bus` subscribes the outbound owner alerts (mode / risk-state / kill /
trade-window / budget-tier) to the §3.2.1 topics, rendering them through the §8 catalog (R8).

All owner-confirmed state changes use :data:`Actor.OWNER`. All times come from :class:`Clock` — the
challenge-expiry clock is the platform ``Clock``, never a bare ``datetime.now()`` (§3.2 convention).
``send()`` formats text from a catalog message (§8/R8) or a plain string and calls
``bot.send_message(owner_chat_id, ...)``.
"""

from __future__ import annotations

import json
import secrets as _secrets
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol

from telegram import BotCommand, Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import OwnerConfirmation
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged
from engine.notify import catalog
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

if TYPE_CHECKING:  # avoid hard import cycles / heavy deps at import time
    from engine.broker.session import SessionManager
    from engine.intelligence.governor import BudgetGovernor
    from engine.risk.causes import RiskStateLatch
    from engine.risk.exposure import ExposureTracker
    from engine.risk.kill import KillSwitch
    from engine.risk.limits import LimitsEngine
    from engine.risk.mode import ModeManager

_log = get_logger("engine.notify.telegram")

# How long a pending two-step challenge stays valid before it must be re-issued (R10). A short TTL
# keeps a stale confirm phrase from authorising a destructive action long after the owner asked.
_CHALLENGE_TTL = timedelta(minutes=2)

# Reply text for a command registered from the catalog but whose downstream is not yet wired.
_PHASE0_STUB = "not yet implemented in Phase 0"


class RecoBook(Protocol):
    """The RECOMMEND outcome-capture seam (§3.6) — implemented by ``RecommendationBook``.

    Duck-typed on purpose (mirrors the ``FloorLimits`` seam in ``engine.risk.exposure``): the bot
    depends on these three coroutines and nothing else, so the book can land independently. Each
    returns an owner-facing summary string and raises :class:`ValueError` — with a message meant for
    the owner — on an unknown ``rec_id`` or an invalid state transition.

    ``apply_approval`` is OPTIONAL and probed with ``hasattr``: in Phase 2 a stop-widen approval has
    no order side effect (the human owns their orders, B7), so a book that cannot apply one is fine.
    """

    async def take(self, rec_id: str, qty: int, price: Decimal) -> str: ...

    async def close(self, rec_id: str, price: Decimal) -> str: ...

    async def veto(self, rec_id: str) -> str: ...


@dataclass(frozen=True)
class _CommandSpec:
    """One row of the §3.2.11 owner-command catalog.

    Single source of truth for handler registration, ``/help``, and the Telegram ``/`` autocomplete
    menu (``setMyCommands``) — so all three can never drift. ``live`` is True for a command with a real
    handler and False for one registered as a logged stub replying ``_PHASE0_STUB``. Every §3.2.11
    command is live as of Phase 2: a live command whose OPTIONAL dependency is unwired replies
    "… not wired", which is a handler's honest answer, not a stub.
    """

    name: str        # command word, no leading slash (Telegram requires ^[a-z0-9_]{1,32}$)
    usage: str       # owner-facing signature, e.g. "/mode <OFF|RECOMMEND|AUTO>"
    summary: str     # one-line description of what it does
    live: bool       # True ⇒ real handler; False ⇒ registered stub


# Ordered so ``/help`` and the menu read top-to-bottom in a sensible sequence (control plane first).
# Editing this tuple is the ONLY place to add/relabel a command — registration + help + menu follow.
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
    # --- entries pause / re-arm (§3.5.3 per-cause latch) ---
    _CommandSpec("pause_entries", "/pause_entries",
                 "Pause new entries (risk_state FROZEN); risk-reducing exits continue.", True),
    _CommandSpec("resume_entries", "/resume_entries",
                 "Resume entries: clears the owner pause AND a rejection-storm freeze.", True),
    # --- RECOMMEND outcome capture (§3.6) ---
    _CommandSpec("taken", "/taken <rec_id> <qty> <price>",
                 "Confirm you took a recommendation at qty/price (origin=recommended).", True),
    _CommandSpec("closed", "/closed <rec_id> <price>",
                 "Mark a taken recommendation closed at price (records the outcome).", True),
    _CommandSpec("veto", "/veto <rec_id>",
                 "Decline an open recommendation; it is recorded as vetoed, never as taken.", True),
    _CommandSpec("close", "/close <position_id>",
                 "Guidance for exiting a position — in RECOMMEND the exit is yours to place (B7).",
                 True),
    # --- read-only reports ---
    _CommandSpec("positions", "/positions",
                 "Show open positions (qty, average entry, stop/target, origin).", True),
    _CommandSpec("pnl", "/pnl", "Show realised P&L today, day MTM and platform equity.", True),
    _CommandSpec("budget", "/budget",
                 "Show month-to-date LLM spend per agent vs allocation and the degrade tier.", True),
    _CommandSpec("limits", "/limits",
                 "Show equity, day MTM, open counts, consecutive losses and the key caps.", True),
    # --- approvals + broker session ---
    _CommandSpec("approve", "/approve <approval_id>",
                 "Approve a pending owner-approval request (e.g. a stop-widen).", True),
    _CommandSpec("reject", "/reject <approval_id>", "Reject a pending owner-approval request.", True),
    _CommandSpec("token", "/token <request_token>",
                 "Complete the daily Kite login with the request token (§10.2 fallback).", True),
)


def _help_text() -> str:
    """Render the full §3.2.11 command catalog as one owner-facing plain-text message (no markdown).

    Live commands list their usage signature + description; any non-live spec is grouped under a
    clearly labelled "not yet available" heading so the owner is never misled into thinking it acts
    (as of Phase 2 every §3.2.11 command is live, so that section is normally empty).
    """
    live = [c for c in _COMMANDS if c.live]
    later = [c for c in _COMMANDS if not c.live]
    lines = ["Owner commands (owner-only):", ""]
    for c in live:
        lines.append(c.usage)
        lines.append(f"    {c.summary}")
    if later:
        lines += ["", "Not yet available (Phase 2+):"]
        lines += [f"/{c.name} — {c.summary}" for c in later]
    return "\n".join(lines)


def _menu_commands() -> list[BotCommand]:
    """The live commands published to Telegram's ``/`` autocomplete menu (``setMyCommands``).

    Only live commands are advertised (stubs would just frustrate the owner). Telegram caps a command
    description at 256 chars, so summaries are truncated defensively even though ours are short.
    """
    return [BotCommand(c.name, c.summary[:256]) for c in _COMMANDS if c.live]


@dataclass
class _PendingChallenge:
    """A stashed two-step confirmation awaiting the owner's follow-up ``/confirm <phrase>`` (R10).

    ``apply`` is the coroutine that actually performs the destructive action once the phrase matches;
    it receives the proven :class:`OwnerConfirmation` and runs under :data:`Actor.OWNER`.
    """

    action: str                                   # human label, e.g. "mode->AUTO", "kill_reset"
    phrase: str                                   # one-time confirmation phrase
    expires_at: Any                               # tz-aware IST datetime (Clock.now() + TTL)
    apply: Callable[[OwnerConfirmation], Awaitable[str]]


class TelegramBot:
    """Owner-only two-way Telegram control plane (O8/R10).

    Never originates orders; owner commands route through the injected ``ModeManager`` / ``KillSwitch``
    (§3.2.11). Construction is cheap and import-safe; the network application is built in
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
    ) -> None:
        self._token = token
        self._owner_chat_id = int(owner_chat_id)
        self._clock = clock
        self._mode = mode_manager
        self._kill = kill_switch
        self._bus = bus
        # Phase-2 data sources. Every one is OPTIONAL: composition (§3.2.12) wires them in a later
        # wave, and until then the owning command replies "not wired" rather than half-acting.
        self._reco_book = reco_book
        self._latch = latch
        self._governor = governor
        self._limits = limits_engine
        self._exposure = exposure
        self._session = session
        self._conn = conn
        self._app: Application | None = None
        self._bus_attached = False
        # At most one challenge is pending at a time — a new destructive command supersedes the old.
        self._pending: _PendingChallenge | None = None

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Build the application, register the command table, and begin polling (R10).

        Idempotent-ish: a second call while running is a no-op + warning. Uses long-polling (no public
        webhook) so the bot needs no inbound port on the LAN host.
        """
        if self._app is not None:
            _log.warning("telegram_start_noop", reason="already_started")
            return
        app = ApplicationBuilder().token(self._token).build()
        self._register_handlers(app)
        self._app = app
        await app.initialize()
        await app.start()
        if app.updater is not None:
            await app.updater.start_polling(drop_pending_updates=True)
        await self._publish_command_menu(app)
        _log.info("telegram_started", owner_chat_id=self._owner_chat_id)

    async def stop(self) -> None:
        """Stop polling and shut the application down cleanly."""
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

        Scoped to the owner chat (the only chat that may command the bot, R10) rather than advertised
        globally. Best-effort: a failure here must never take down the control plane, so it is logged,
        not raised (R8) — the bot still works, the owner just types commands without autocomplete.
        """
        from telegram import BotCommandScopeChat

        try:
            await app.bot.set_my_commands(
                _menu_commands(), scope=BotCommandScopeChat(chat_id=self._owner_chat_id)
            )
            _log.info("telegram_commands_published", count=len(_menu_commands()))
        except Exception:  # noqa: BLE001 - menu publish is best-effort (R8)
            _log.exception("telegram_set_commands_failed")

    # ------------------------------------------------------------------ outbound (§8/R8 catalog)
    async def send(self, msg: Any) -> None:
        """Send a catalog message (§8/R8) or plain text to the owner chat.

        Accepts a ``CatalogMessage`` (rendered via its ``.render()`` / ``.text`` / ``str()``) or a raw
        string — the catalog model itself lands in Phase 2 (§8). Outbound never blocks the engine: a
        send failure is logged, not raised, so an alert path can never take down a caller (R8).
        """
        app = self._app
        if app is None:
            _log.warning("telegram_send_dropped", reason="not_started")
            return
        text = self._render(msg)
        try:
            await app.bot.send_message(chat_id=self._owner_chat_id, text=text)
        except Exception:  # noqa: BLE001 - alerting must never crash the caller (R8)
            _log.exception("telegram_send_failed")

    @staticmethod
    def _render(msg: Any) -> str:
        """Render a catalog message or text into a Telegram string (§8/R8)."""
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
            "taken": self._cmd_taken,
            "closed": self._cmd_closed,
            "veto": self._cmd_veto,
            "close": self._cmd_close,
            "positions": self._cmd_positions,
            "pnl": self._cmd_pnl,
            "budget": self._cmd_budget,
            "limits": self._cmd_limits,
            "approve": self._cmd_approve,
            "reject": self._cmd_reject,
            "token": self._cmd_token,
        }

    def _register_handlers(self, app: Application) -> None:
        """Wire every command in :data:`_COMMANDS` to its handler (all owner-guarded, R10).

        Live commands use their bound handler; Phase-2 stubs register a logged not-implemented reply.
        """
        live = self._live_handlers()
        for spec in _COMMANDS:
            handler = live[spec.name] if spec.live else self._stub(spec.name)
            app.add_handler(CommandHandler(spec.name, _owner_only(self, handler)))

    def _stub(self, name: str) -> Callable[..., Awaitable[None]]:
        """A Phase-0 handler that logs the command and replies that it is not yet implemented."""

        async def _handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            _log.info("telegram_cmd_stub", command=name, args=_args(context))
            await _reply(update, f"/{name}: {_PHASE0_STUB}.")

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

    # ------------------------------------------------------------------ /kill (single-step, wired)
    async def _cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Engage the kill switch — single-step (fast), owner-only (R10/§7.2)."""
        if self._kill is None:
            await _reply(update, "/kill: kill switch not wired.")
            return
        reason = " ".join(context.args) if context.args else "owner /kill via Telegram"
        _log.critical("telegram_cmd_kill", reason=reason)
        await self._kill.trigger(reason, actor=Actor.OWNER)
        await _reply(update, f"KILL SWITCH ENGAGED: {reason}")

    # ------------------------------------------------------------------ /kill_reset (two-step, wired)
    async def _cmd_kill_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Begin the two-step kill reset (R10/§7.2). Stashes a challenge; /confirm completes it."""
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
        """Owner mode change. →AUTO is two-step (R10); ←OFF/RECOMMEND is single-step."""
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
        """Show the current window, or set it (single-step, owner-ID + validation + audit, §3.2.7/§7.1).

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
        """Complete a pending two-step challenge (R10). Verifies the one-time phrase + expiry."""
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

    # ------------------------------------------------------------------ challenge helper (R10)
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
        """Owner pause: latch the ``owner_pause`` cause at FROZEN (§3.5.3). Exits keep running (R3)."""
        if self._latch is None:
            await _reply(update, "/pause_entries: risk-state latch not wired.")
            return
        state = await self._latch.set_cause(
            CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries", Actor.OWNER
        )
        _log.warning("telegram_cmd_pause_entries", risk_state=state.value)
        await _reply(
            update,
            f"entries PAUSED — risk_state {state.value}. Risk-reducing exits/protection continue (R3). "
            "Send /resume_entries to re-arm.",
        )

    async def _cmd_resume_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner re-arm. Clears BOTH the owner pause and a rejection-storm freeze — §3.5.3 makes a
        rejection storm the one FROZEN cause that never auto-recovers; ``/resume_entries`` IS its
        recovery path. Any other cause still latching keeps entries closed, and the reply says so."""
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

    # ------------------------------------------------------------------ RECOMMEND outcome capture (§3.6)
    async def _cmd_taken(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner confirms they took a recommendation — creates the ``origin='recommended'`` position."""
        if self._reco_book is None:
            await _reply(update, "/taken: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 3:
            await _reply(update, "usage: /taken <rec_id> <qty> <price>")
            return
        qty, price = _parse_int(args[1]), _parse_decimal(args[2])
        if qty is None or qty <= 0 or price is None or price <= 0:
            await _reply(update, "invalid qty/price; usage: /taken <rec_id> <qty> <price>")
            return
        _log.warning("telegram_cmd_taken", rec_id=args[0], qty=qty, price=str(price))
        await _reply(update, await _book_result(self._reco_book.take(args[0], qty, price)))

    async def _cmd_closed(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner marks a taken recommendation closed at ``price`` (§3.6 outcome capture)."""
        if self._reco_book is None:
            await _reply(update, "/closed: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 2:
            await _reply(update, "usage: /closed <rec_id> <price>")
            return
        price = _parse_decimal(args[1])
        if price is None or price <= 0:
            await _reply(update, "invalid price; usage: /closed <rec_id> <price>")
            return
        _log.warning("telegram_cmd_closed", rec_id=args[0], price=str(price))
        await _reply(update, await _book_result(self._reco_book.close(args[0], price)))

    async def _cmd_veto(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner declines an open recommendation. Recorded as vetoed — never silently as a non-fill,
        which is a different (unbiased) training label (§6.5)."""
        if self._reco_book is None:
            await _reply(update, "/veto: recommendation book not wired.")
            return
        args = _args(context)
        if len(args) != 1:
            await _reply(update, "usage: /veto <rec_id>")
            return
        _log.warning("telegram_cmd_veto", rec_id=args[0])
        await _reply(update, await _book_result(self._reco_book.veto(args[0])))

    async def _cmd_close(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Exit guidance — the v1 answer, and an honest one: in RECOMMEND the platform places ZERO API
        orders (B7), so there is nothing for this command to send. It marks nothing and changes no
        state; the owner exits in their terminal and reports it with ``/closed`` (§3.6)."""
        args = _args(context)
        target = args[0] if args else "<position_id>"
        _log.info("telegram_cmd_close", position_id=args[0] if args else None)
        await _reply(
            update,
            f"/close {target}: nothing was sent to the broker. In RECOMMEND the exit decision AND the "
            "order are yours — the platform places zero API orders (B7). Square off in your terminal, "
            "then send /closed <rec_id> <price> so the outcome is recorded (§3.6). Platform-placed "
            "exits arrive with AUTO in Phase 3.",
        )

    # ------------------------------------------------------------------ read-only reports
    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Open positions, straight off the state store (read-only, no broker call)."""
        if self._conn is None:
            await _reply(update, "/positions: state store not wired.")
            return
        rows = self._conn.execute(
            "SELECT position_id, symbol, side, product, qty, avg_entry, stop, target, origin, "
            "protection_state FROM positions WHERE state='OPEN' ORDER BY opened_at"
        ).fetchall()
        _log.info("telegram_cmd_positions", count=len(rows))
        if not rows:
            await _reply(update, "no open positions.")
            return
        lines = [f"open positions: {len(rows)}"]
        for row in rows:
            lines.append(
                f"{row['symbol']} {row['side']} {row['qty']} @ {row['avg_entry']} · "
                f"{row['product'] or '-'} · stop {row['stop'] or '-'} · target {row['target'] or '-'} · "
                f"{row['origin']} · {row['protection_state'] or 'unprotected'} · id {row['position_id']}"
            )
        await _reply(update, "\n".join(lines))

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Today's P&L off :class:`~engine.risk.exposure.ExposureTracker` (rebuilt from the tables).

        ``origin='external'`` is excluded upstream (O5) — this is platform P&L, not account P&L. With
        no mark source wired the open-MTM term would silently read ₹0 (marks fall back to avg entry),
        so it is reported as unavailable instead of as a number the owner might trust.
        """
        if self._exposure is None:
            await _reply(update, "/pnl: exposure tracker not wired.")
            return
        exp = self._exposure
        today = self._clock.today()
        lines = [f"P&L {today.isoformat()} (platform only; external excluded, O5)"]
        realized_today = _realized_today(exp, today)
        if realized_today is not None:
            lines.append(f"realised today: {_inr(realized_today)}")
        lines.append(f"day MTM: {_inr(exp.day_mtm())}")
        lines.append(f"realised all-time: {_inr(exp.realized_net())}")
        # No mark source ⇒ open_mtm() degrades to zero-at-entry — say so instead of presenting a
        # zero placeholder as a real mark-to-market.
        if not exp.has_mark_source():
            lines.append("open MTM: n/a offline (no mark source)")
        else:
            lines.append(f"open MTM: {_inr(exp.open_mtm())}")
        lines.append(f"equity: {_inr(exp.equity())} · open: {exp.open_position_counts().total}")
        _log.info("telegram_cmd_pnl")
        await _reply(update, "\n".join(lines))

    async def _cmd_budget(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Month-to-date LLM spend vs the per-agent allocations + the active §5.6 degrade tier."""
        if self._governor is None:
            await _reply(update, "/budget: budget governor not wired.")
            return
        gov = self._governor
        credit = gov.credit()
        allocations: dict[str, Any] = gov.allocations()
        lines = [
            f"month spend: {_usd(gov.month_spend())}" + (f" / {_usd(credit)} credit" if credit else ""),
            f"pro-rata to date: {_usd(gov.pro_rata_to_date())} (trading days, R6)",
            f"tier: {gov.degrade_tier().value}",
        ]
        if allocations:
            lines.append("per agent (spend / allocation):")
            lines += [
                f"  {agent}: {_usd(gov.agent_spend(agent))} / {_usd(alloc)}"
                for agent, alloc in sorted(allocations.items())
            ]
        _log.info("telegram_cmd_budget")
        await _reply(update, "\n".join(lines))

    async def _cmd_limits(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Live headroom against the §7.1 caps: what is used vs what the protected store allows."""
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
        lines = [
            f"equity: {_inr(exp.equity())} (base {_inr(table.capital_base_inr)})",
            f"day MTM: {_inr(exp.day_mtm())} "
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

    # ------------------------------------------------------------------ owner approvals (§3.4)
    async def _cmd_approve(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Approve one ``owner_approvals`` row (e.g. a stop-widen, R1)."""
        await self._resolve_approval(update, context, "approved")

    async def _cmd_reject(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Reject one ``owner_approvals`` row."""
        await self._resolve_approval(update, context, "rejected")

    async def _resolve_approval(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, status: str
    ) -> None:
        """Stamp the approval row and report it. NO side effect beyond the row in Phase 2 — the human
        manages their own orders (B7). If the injected recommendation book knows how to push an
        approved stop onto a tracked position it is offered the approval (probe, never a requirement:
        the book lands in a parallel task and may not implement it yet)."""
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
        except Exception as exc:  # noqa: BLE001 - the row is already stamped; report, never crash (R8)
            _log.exception("telegram_apply_approval_failed", approval_id=approval_id)
            return f"(applying the approval failed: {exc})"

    # ------------------------------------------------------------------ /token (§10.2 fallback path)
    async def _cmd_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Complete the daily Kite login with a pasted ``request_token`` — the §10.2 fallback for when
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

    # ------------------------------------------------------------------ outbound bus alerts (R8)
    def attach_bus(self, bus: EventBus | None = None) -> None:
        """Subscribe the owner-alert handlers to the §3.2.1 control-plane topics (R8).

        Every handler renders through the §8 catalog and calls :meth:`send`, which is best-effort — a
        Telegram outage can never break a publisher (mode change, kill, risk-state latch). Attaching
        twice would double-send, so the second call is a no-op.
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
            catalog.budget_tier(event.old_tier.value, event.new_tier.value, event.month_spend_usd)
        )


# ---------------------------------------------------------------------- module-level guards/helpers


def _owner_only(
    bot: TelegramBot, handler: Callable[..., Awaitable[None]]
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]:
    """Owner-ID lock (R10): drop (log + ignore) any update not from the owner's chat.

    Wraps a bound :class:`TelegramBot` handler. The check is on ``effective_chat.id`` — the only
    identity Telegram authenticates — and there is no reply to a non-owner (silent ignore, so the bot
    never confirms its own existence to an unknown chat).
    """

    @wraps(handler)
    async def _guarded(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if chat is None or chat.id != bot._owner_chat_id:
            _log.warning(
                "telegram_rejected_non_owner",
                from_chat_id=getattr(chat, "id", None),
                command=getattr(getattr(update, "message", None), "text", None),
            )
            return
        await handler(update, context)

    return _guarded


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
    """Parse a price argument as an exact :class:`Decimal` (money is never a float, §8.1). Thousands
    separators are tolerated because owners type them; anything else is None."""
    try:
        return Decimal(raw.strip().replace(",", ""))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _inr(value: Any) -> str:
    """Render a rupee amount for the owner. Display-only rounding — the stored value stays Decimal."""
    return f"₹{Decimal(value):,.2f}"


def _usd(value: Any) -> str:
    """Render a USD budget amount. Four places: per-call LLM costs are fractions of a cent, and a
    2-decimal render would report a real month's early spend as ``$0.00``."""
    return f"${Decimal(value):,.4f}"


async def _book_result(call: Awaitable[str]) -> str:
    """Await a :class:`RecoBook` call and turn any failure into an owner-facing line.

    ``ValueError`` is the book's contract for "unknown rec / invalid transition" and already carries a
    message written for the owner, so it is surfaced verbatim; anything else is a bug, logged with a
    traceback and reported — a bad book call must never take down the control plane (R8)."""
    try:
        return await call
    except ValueError as exc:
        _log.warning("telegram_reco_book_rejected", error=str(exc))
        return str(exc)
    except Exception as exc:  # noqa: BLE001 - report, never crash the bot (R8)
        _log.exception("telegram_reco_book_failed")
        return f"failed: {exc}"


def _render_payload(raw: str | None) -> str:
    """Render an ``owner_approvals`` payload as owner-facing lines.

    Structured data belongs in the audit log, prose belongs in the message (§8/R8) — dumping the raw
    JSON blob into the chat is exactly the leak ``CatalogMessage.render`` exists to prevent."""
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
