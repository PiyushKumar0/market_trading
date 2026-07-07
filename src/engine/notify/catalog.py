"""Typed owner-message catalog (§3.2.11 / R8) — the single source of every notification the
``TelegramBot`` sends and ``HealthMonitor``/risk/oms surfaces raise.

Phase-0 skeleton: this module defines the *shapes* (a :class:`MessageKind` enum + a single
:class:`CatalogMessage` Pydantic model) plus tiny pure constructor helpers that stamp the right
``kind``/``severity`` and assemble a human title/body. It deliberately contains NO transport (no
python-telegram-bot, no formatting beyond plain f-strings) and NO ``Clock`` access — callers pass
in already-`Clock`-derived values. ``TelegramBot.send(msg: CatalogMessage)`` (§3.2.11) consumes
these; the catalog enumerated in §8/R8 is: recommendation, fill, limit breach, kill, budget
warning, daily/weekly summary, startup/recovery report, trade-window changed, login prompt,
feed-stale, REC_FILL_SUSPECTED.

Plan citations per kind are documented on the :class:`MessageKind` members and on each helper.
Load-bearing detail captured here (so the bot layer stays dumb): the ``reply_keyboard`` one-tap
confirm hint for the §3.6 ``REC_FILL_SUSPECTED`` prompt is pre-filled with the exact ``/taken``
command (``/taken`` is also the action behind that one-tap confirm — §3.2.11/§3.6), and ``data``
carries the structured fields the bot/audit log persists alongside the rendered text (R8).
"""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# Severity is a closed vocabulary shared by every kind; "critical" maps to the loud/alert path.
Severity = Literal["info", "warning", "critical"]

# Owner-facing glyph prefixed onto the rendered title line (plain-text Telegram; no markdown parse).
_SEVERITY_GLYPH: dict[str, str] = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}


class MessageKind(StrEnum):
    """Every owner-facing message type in the §8/R8 catalog.

    Each member documents its trigger and the plan ID(s) that own it. The value is the stable
    machine key persisted in the audit/notification log (R8) and never localised.
    """

    RECOMMENDATION = "recommendation"
    """A new RECOMMEND-mode recommendation for the owner to act on (§3.6/O8). Carries rec id,
    symbol, side, qty, entry/stop/target and validity; the owner replies ``/taken``/``/closed``."""

    FILL = "fill"
    """An order filled — AUTO-mode platform fill or owner-confirmed RECOMMEND fill (R8 audit).
    Carries symbol, side, qty, avg price, order id."""

    LIMIT_BREACH = "limit_breach"
    """A §7.1 risk rule tripped (FREEZE/FLATTEN/limit), alerting the owner with the tripping rule
    name and the observed-vs-threshold values (§7.1 "alert the owner with the tripping rule +
    values", R8). Severity is ``warning`` for FREEZE, ``critical`` for FLATTEN/equity-floor."""

    KILL = "kill"
    """The kill switch fired (cumulative floor / manual / RMS), sticky in SQLite (R10/§7.2). Always
    ``critical``; reset requires the owner two-step ``/kill_reset`` flow."""

    BUDGET_WARNING = "budget_warning"
    """The LLM/API spend budget governor crossed a degrade rung (§5.6 ladder DG1..DG4). Warns the
    owner which tier is now active and the spend-vs-cap; informational-to-warning severity."""

    DAILY_SUMMARY = "daily_summary"
    """End-of-day owner digest — realised P&L, trades, open positions, FROZEN reasons (O8/R8)."""

    WEEKLY_SUMMARY = "weekly_summary"
    """Weekly owner digest — rolling P&L, drawdown vs the §7.1 weekly rung, learning status (O8)."""

    ENGINE_STARTED = "engine_started"
    """Process-lifecycle alert sent immediately on boot (§2.2), BEFORE the §2.6 recovery/catch-up runs,
    so the owner knows the engine is alive even if catch-up takes a while. Carries mode + build version +
    a ``crash_recovered`` flag when the prior run exited uncleanly (state was RUNNING/STOPPING). The fuller
    STARTUP_REPORT follows on recovery completion. Gated by ``settings.lifecycle.notify_started``."""

    ENGINE_STOPPED = "engine_stopped"
    """Process-lifecycle alert sent as the LAST act of a clean/planned shutdown (§2.2): reason, open-
    position protection status, next expected start. Best-effort — a failed send never blocks exit (the
    watchdog is the backstop). Gated by ``settings.lifecycle.notify_planned_stop``."""

    STARTUP_REPORT = "startup_report"
    """The every-startup recovery & catch-up report (§2.6 step 7): off-duration, what reconciled,
    jobs caught up, MIS squared, FROZEN reasons. Expected on every (manual/scheduled/crash) boot."""

    TRADE_WINDOW_CHANGED = "trade_window_changed"
    """The owner changed the trade window via ``/trade_window`` or the dashboard (§3.2.7/§7.1);
    sticky state written, audited, applied immediately and echoed back to the owner (line §3.2.4)."""

    LOGIN_PROMPT = "login_prompt"
    """The daily Kite login is required — the owner must open the login URL and return the
    ``request_token`` (R6; access token expires ~06:00 IST, §3.2.11 ``/token``). ``critical`` because
    no trading proceeds without it."""

    FEED_STALE = "feed_stale"
    """The live tick feed went silent while RUNNING past the staleness budget (§7.1
    ``stale_data_guard``: tick age 5 s / heartbeat silence 10 s ⇒ FROZEN + ticker respawn, A4/R2).
    NOT raised during WARMING/intentionally-off (those are suppressed upstream, §2.6/§3.2.12)."""

    REC_FILL_SUSPECTED = "rec_fill_suspected"
    """The reconciler matched a broker position to an open RECOMMEND rec (R5/§3.6): exactly one
    one-tap confirm prompt, pre-filled with the observed qty/price, BEFORE any ``positions`` row or
    order call. Owner confirms via ``/taken`` (one tap) ⇒ ``origin='recommended'``; dismiss/expiry ⇒
    ``no_action``. Never auto-adopted."""


class CatalogMessage(BaseModel):
    """A single rendered, typed owner notification consumed by ``TelegramBot.send`` (§3.2.11).

    ``title``/``body`` are presentation text; ``data`` carries the structured fields persisted to the
    notification/audit log (R8) and used by tests, so callers must not bury load-bearing values
    (ids, prices, rule names) in the prose only. ``reply_keyboard`` is an optional one-tap-confirm
    hint: a list of button rows, each button a ``{"text": <label>, "command": <bot command>}`` dict
    the bot layer turns into its reply-keyboard widget (e.g. the §3.6 ``REC_FILL_SUSPECTED`` /taken
    button). Pure data — no Telegram types leak in here.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: MessageKind
    title: str = Field(min_length=1)
    body: str = Field(min_length=1)
    severity: Severity = "info"
    data: dict[str, Any] = Field(default_factory=dict)
    reply_keyboard: list[list[dict[str, str]]] | None = None

    def render(self) -> str:
        """Owner-facing plain-text rendering consumed by ``TelegramBot.send`` (§3.2.11).

        Presentation only: a severity-glyphed ``title`` line followed by ``body``. The structured
        ``data`` / ``reply_keyboard`` are transport & audit concerns and are deliberately NOT dumped
        into the owner's message — leaking the raw model (``kind=… data={…} reply_keyboard=None``) is
        exactly the bug this method exists to prevent (``send`` used to fall back to ``str(model)``).
        """
        glyph = _SEVERITY_GLYPH.get(self.severity, "")
        head = f"{glyph} {self.title}".strip()
        return f"{head}\n{self.body}"


# --------------------------------------------------------------------------- helpers
# Each helper is pure: it takes already-validated/`Clock`-derived primitives and returns a fully
# populated CatalogMessage. Decimal in → str in ``body``/``data`` (prices round-trip as strings so
# nothing is mangled by float formatting; mirrors the §8.1 decimal-as-string convention).


def login_prompt(url: str) -> CatalogMessage:
    """Daily Kite login required (R6). ``url`` is the broker login URL; the owner opens it and
    returns the ``request_token`` via ``/token`` (§3.2.11)."""
    return CatalogMessage(
        kind=MessageKind.LOGIN_PROMPT,
        title="Kite login required",
        body=f"Daily login needed before trading can resume. Open: {url}\nThen send /token <request_token>.",
        severity="critical",
        data={"login_url": url},
    )


def engine_started(*, mode: str, version: str, crash_recovered: bool = False) -> CatalogMessage:
    """Process-lifecycle boot alert (§2.2). Sent before the §2.6 recovery body runs. ``crash_recovered``
    leads the message when the prior run exited uncleanly (belt-and-suspenders to the watchdog's
    real-time ``ENGINE_DOWN``, which a dead process could not send about itself)."""
    lead = "⚠ recovered from an unclean prior exit — " if crash_recovered else ""
    return CatalogMessage(
        kind=MessageKind.ENGINE_STARTED,
        title="Engine started" + (" (crash-recovered)" if crash_recovered else ""),
        body=f"{lead}engine is alive (mode={mode}, build={version}). Running startup recovery…",
        severity="warning" if crash_recovered else "info",
        data={"mode": mode, "version": version, "crash_recovered": crash_recovered},
    )


def engine_stopped(*, reason: str, open_positions: int, next_start: str | None = None) -> CatalogMessage:
    """Process-lifecycle clean-stop alert (§2.2), the last act before exit. ``reason`` ∈
    {owner, window-idle, update, service-stop}; ``open_positions`` are broker-protected throughout (R3)."""
    nxt = f" Next expected start: {next_start}." if next_start else ""
    return CatalogMessage(
        kind=MessageKind.ENGINE_STOPPED,
        title="Engine stopped",
        body=(
            f"Clean shutdown (reason={reason}). Open positions: {open_positions} "
            f"(broker-protected, R3).{nxt}"
        ),
        severity="info",
        data={"reason": reason, "open_positions": open_positions, "next_start": next_start},
    )


def startup_report(
    *,
    mode: str,
    risk_state: str,
    killed: bool,
    needs_login: bool,
    integrity_ok: bool,
    crash_recovered: bool,
    prior_state: str,
    frozen_reasons: list[str],
    deferred_steps: list[str],
) -> CatalogMessage:
    """The every-startup recovery & catch-up report (§2.6 step 7 / STARTUP_REPORT).

    Critical when the book is frozen on an integrity failure, the kill switch is engaged, or the
    prior run crashed; otherwise info. The owner sees a compact status line plus the frozen/deferred
    reasons — the full structured report is preserved in ``data`` (and the ``startup_report`` log),
    never dumped as a raw dict into the prose (R8)."""
    lead = f"⚠ crash-recovered (prior state {prior_state}) — " if crash_recovered else ""
    frozen = ", ".join(frozen_reasons) if frozen_reasons else "none"
    deferred = ", ".join(deferred_steps) if deferred_steps else "none"
    return CatalogMessage(
        kind=MessageKind.STARTUP_REPORT,
        title="Startup recovery complete",
        body=(
            f"{lead}mode={mode} · risk={risk_state} · killed={killed} · "
            f"login_needed={needs_login} · integrity_ok={integrity_ok}\n"
            f"frozen: {frozen}\ndeferred: {deferred}"
        ),
        severity="critical" if (killed or not integrity_ok or crash_recovered) else "info",
        data={
            "mode": mode, "risk_state": risk_state, "killed": killed, "needs_login": needs_login,
            "integrity_ok": integrity_ok, "crash_recovered": crash_recovered,
            "prior_state": prior_state, "frozen_reasons": frozen_reasons,
            "deferred_steps": deferred_steps,
        },
    )


def feed_stale(age_s: float) -> CatalogMessage:
    """Live feed silent for ``age_s`` seconds while running (§7.1 ``stale_data_guard``; A4/R2)."""
    return CatalogMessage(
        kind=MessageKind.FEED_STALE,
        title="Feed stale — entries frozen",
        body=(
            f"No live ticks for {age_s:.1f}s (>budget). Entries FROZEN and ticker respawn requested. "
            "Risk-reducing exits continue."
        ),
        severity="critical",
        data={"age_s": age_s},
    )


def rec_fill_suspected(
    rec_id: str, symbol: str, qty: int, price: Decimal
) -> CatalogMessage:
    """Reconciler matched a broker position to open RECOMMEND rec ``rec_id`` (R5/§3.6).

    Emits a one-tap ``/taken`` confirm pre-filled with the observed ``qty``/``price``. The owner's
    single tap routes through the same ``/taken`` command (§3.2.11) ⇒ ``origin='recommended'``;
    dismissing/expiring leaves it a ``no_action`` non-fill (unbiased training signal, §6.5).
    """
    price_s = str(price)
    return CatalogMessage(
        kind=MessageKind.REC_FILL_SUSPECTED,
        title="Suspected recommendation fill",
        body=(
            f"A broker position matches open recommendation {rec_id} ({symbol}: {qty} @ {price_s}). "
            "Did you take this trade? Tap to confirm — otherwise it is recorded as no-action."
        ),
        severity="warning",
        data={"rec_id": rec_id, "symbol": symbol, "qty": qty, "price": price_s},
        # One-tap confirm: the button command is the literal /taken the bot will execute (§3.6).
        reply_keyboard=[
            [{"text": f"✓ /taken {symbol} {qty}@{price_s}", "command": f"/taken {rec_id} {qty} {price_s}"}],
            [{"text": "✗ No action", "command": f"/reject {rec_id}"}],
        ],
    )
