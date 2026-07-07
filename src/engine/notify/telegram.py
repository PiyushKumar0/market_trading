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

Phase 0 is a SKELETON: the command table is fully wired, but handlers whose data source is a later
phase (positions, P&L, budget, approvals, RECOMMEND outcome capture) log + reply
"not yet implemented in Phase 0". FULLY wired to their Tier-2 targets are: ``/kill`` and
``/kill_reset`` (two-step) → :class:`KillSwitch`; ``/mode`` (two-step for AUTO) →
:class:`ModeManager`; ``/trade_window`` (single-step, owner-ID + validation) →
``ModeManager.set_trade_window``; ``/status``; ``/confirm`` (completes a two-step challenge); and
``/help`` (renders the :data:`_COMMANDS` catalog).
The command catalog is the single source of truth for handler registration, ``/help``, and the
Telegram ``/`` autocomplete menu (``setMyCommands``, owner-chat-scoped), so the three never drift.

All owner-confirmed state changes use :data:`Actor.OWNER`. All times come from :class:`Clock` — the
challenge-expiry clock is the platform ``Clock``, never a bare ``datetime.now()`` (§3.2 convention).
``send()`` formats text from a catalog message (§8/R8) or a plain string and calls
``bot.send_message(owner_chat_id, ...)``.
"""

from __future__ import annotations

import secrets as _secrets
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from telegram import BotCommand, Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from engine.core.clock import Clock
from engine.core.enums import Actor, Mode
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import OwnerConfirmation

if TYPE_CHECKING:  # avoid hard import cycles / heavy deps at import time
    from engine.risk.kill import KillSwitch
    from engine.risk.mode import ModeManager

_log = get_logger("engine.notify.telegram")

# How long a pending two-step challenge stays valid before it must be re-issued (R10). A short TTL
# keeps a stale confirm phrase from authorising a destructive action long after the owner asked.
_CHALLENGE_TTL = timedelta(minutes=2)

# Commands whose downstream lands in Phase 2+ — wired as logged stubs that reply with this text.
_PHASE0_STUB = "not yet implemented in Phase 0"


@dataclass(frozen=True)
class _CommandSpec:
    """One row of the §3.2.11 owner-command catalog.

    Single source of truth for handler registration, ``/help``, and the Telegram ``/`` autocomplete
    menu (``setMyCommands``) — so all three can never drift. ``live`` is True for commands fully wired
    in Phase 0 and False for the Phase-2+ stubs (which register but reply ``_PHASE0_STUB``).
    """

    name: str        # command word, no leading slash (Telegram requires ^[a-z0-9_]{1,32}$)
    usage: str       # owner-facing signature, e.g. "/mode <OFF|RECOMMEND|AUTO>"
    summary: str     # one-line description of what it does
    live: bool       # True ⇒ wired now; False ⇒ Phase-2 stub


# Ordered so ``/help`` and the menu read top-to-bottom in a sensible sequence (live first). Editing
# this tuple is the ONLY place to add/relabel a command — registration + help + menu follow from it.
_COMMANDS: tuple[_CommandSpec, ...] = (
    # --- fully wired in Phase 0 ---
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
    # --- Phase 2+ data sources — registered but reply that they are not yet implemented ---
    _CommandSpec("positions", "/positions", "Show open positions (qty, average price, P&L).", False),
    _CommandSpec("pnl", "/pnl", "Show today's realised and unrealised P&L.", False),
    _CommandSpec("veto", "/veto", "Veto a pending recommendation before it acts.", False),
    _CommandSpec("close", "/close", "Request a close of an open position.", False),
    _CommandSpec("approve", "/approve", "Approve a pending action (e.g. a stop-widen).", False),
    _CommandSpec("reject", "/reject", "Reject a pending recommendation or action.", False),
    _CommandSpec("token", "/token", "Submit the daily Kite login request token.", False),
    _CommandSpec("budget", "/budget", "Show LLM/API spend vs. cap and the active degrade rung.", False),
    _CommandSpec("limits", "/limits", "Show the active (protected) risk limits.", False),
    _CommandSpec("pause_entries", "/pause_entries", "Pause new entries; risk-reducing exits continue.",
                 False),
    _CommandSpec("resume_entries", "/resume_entries", "Resume new entries after a pause.", False),
    _CommandSpec("taken", "/taken", "Confirm you took a recommended trade (origin=recommended).", False),
    _CommandSpec("closed", "/closed", "Mark a recommended trade as closed.", False),
)


def _help_text() -> str:
    """Render the full §3.2.11 command catalog as one owner-facing plain-text message (no markdown).

    Live commands list their usage signature + description; Phase-2 stubs are grouped under a clearly
    labelled "not yet available" heading so the owner is never misled into thinking they act.
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
        mode_manager: "ModeManager | None" = None,
        kill_switch: "KillSwitch | None" = None,
        bus: EventBus | None = None,
    ) -> None:
        self._token = token
        self._owner_chat_id = int(owner_chat_id)
        self._clock = clock
        self._mode = mode_manager
        self._kill = kill_switch
        self._bus = bus
        self._app: Application | None = None
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
        """Name → bound handler for every command wired in Phase 0.

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
