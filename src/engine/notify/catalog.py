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

from engine.core.contracts import Recommendation

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

    FEED_DEGRADED = "feed_degraded"
    """The live tick feed went silent DURING market hours while the child's 1 s heartbeats keep it
    HEALTHY (2026-07-22 tickless-HEALTHY session, R2/§7.1). Heartbeats are KiteTicker-independent, so a
    feed delivering ZERO ticks used to read HEALTHY all day and write zero self-built bars in silence.
    Distinct from FEED_STALE (heartbeat silence ⇒ kill+respawn+FROZEN, critical): DEGRADED is
    ``warning`` — the child is alive, the FEED is not delivering ticks. Recovers automatically the
    moment ticks resume."""

    FEED_WEDGED = "feed_wedged"
    """The live tick feed is WEDGED: repeated WARMING-timeout respawns never reached HEALTHY
    (2026-07-23 13:41 sleep/resume incident). The supervisor keeps retrying at capped backoff, but
    after ``ticker.max_wedge_respawns`` consecutive attempts the feed has still failed to come up —
    a structural fault (dead child / no network / rejected token) that needs owner attention. Escalated
    ONCE per wedge episode (``warning``, not a per-respawn page); recovers automatically the moment a
    respawn finally heartbeats."""

    REC_FILL_SUSPECTED = "rec_fill_suspected"
    """The reconciler matched a broker position to an open RECOMMEND rec (R5/§3.6): exactly one
    one-tap confirm prompt, pre-filled with the observed qty/price, BEFORE any ``positions`` row or
    order call. Owner confirms via ``/taken`` (one tap) ⇒ ``origin='recommended'``; dismiss/expiry ⇒
    ``no_action``. Never auto-adopted."""

    POSITION_NOT_IN_HOLDINGS = "position_not_in_holdings"
    """The §3.6 holdings reconcile found a tracked OPEN CNC position that is ABSENT or SHORT in the
    broker's holdings (owner-directed 2026-09-07): the owner almost certainly sold it and never sent
    ``/closed``, so the platform is tracking — and blocking new buys on — a position that no longer
    exists. One alert per position per trading day, naming the exact ``/closed <entry_rec_id> <price>``
    reply. Alert-only: the platform never auto-closes a human-owned position (the exit price is the
    owner's fact, §6.5) and never touches risk state on this signal."""

    RECONCILE_DRIFT = "reconcile_drift"
    """Nightly self-built-vs-official bar reconciliation drifted beyond thresholds (A13/§3.2.3:
    |Δvol|>``reconcile.vol_drift_pct`` or |Δclose|>``reconcile.close_drift_ticks`` on more than
    ``reconcile.max_bad_bar_fraction`` of compared bars). Offline spans are excluded from the
    denominator (§2.6) — this alert means genuine self-vs-official divergence, investigate the feed."""

    BACKFILL_REPORT = "backfill_report"
    """A ``BackfillJob`` run (historical or §2.6 warm-up gap-fill) finished — bars written, span,
    failures (§3.2.3/§4.4 jobs 1+3). Warning when any symbol failed; info otherwise."""

    WARMUP_FROZEN = "warmup_frozen"
    """The §2.6 step-6 cold-start warm-up gate: entries stay FROZEN because contiguous bar coverage
    is insufficient for a strategy's feature lookbacks (§7.1 ``warmup_ready``; §10.3
    ``WARMUP_FROZEN(symbol/strategy)``). Never trade on thin data — distinct from feed-stale."""

    CATCHUP_REPORT = "catchup_report"
    """The §2.6 step-5 ``CatchUpRunner`` finished replaying missed jobs after an off period: which
    jobs were caught up, in dependency order, and which failed (feeding the STARTUP_REPORT)."""

    ENGINE_CRASHLOOP = "engine_crashloop"
    """Repeated fast respawns inside ``lifecycle.crashloop_window_s`` coalesced into one loud alert
    (§2.2/§10.7) instead of a page per restart. Always critical — something is structurally wrong."""

    DATA_FRESHNESS_FROZEN = "data_freshness_frozen"
    """A safety/deadline-critical daily job (instruments/tick-size A10, surveillance A8, earnings
    calendar R2, corp-action GTT adjustment A12) could not run or verify before entries open ⇒
    FROZEN-for-entries + this alert (§2.6 step 5). Risk-reducing actions continue (R3)."""

    MODE_CHANGE = "mode_change"
    """The engine mode moved ``OFF ↔ RECOMMEND ↔ AUTO`` (§10.3 ``MODE_CHANGE``). Owner-initiated or a
    risk-forced downgrade — the ``actor`` says which, and a risk-forced one never re-arms on a timer
    (R3/R5). Warning: the owner must always know which mode their capital is running under."""

    RISK_STATE_CHANGE = "risk_state_change"
    """The Tier-2 risk state moved (§10.3 ``RISK_STATE(FROZEN/CLOSE_ONLY)``, §3.5.3). Carries the
    per-cause reason that drove the edge — states are reached by direct per-cause edges composed
    most-restrictive-wins, so the cause is the actionable half of the message (see
    :class:`engine.risk.causes.RiskStateLatch`)."""

    CATALYST_WATCHLIST = "catalyst_watchlist"
    """The daily catalyst watchlist was built (§4.4 job 14 / §10.3 ``CATALYST_WATCHLIST``): how many
    symbols are ORIGINATING candidates (news may start a trade) vs CONTEXT-only (news may veto/size
    but never originate, O11/§2.7)."""

    CATALYST_DISABLED = "catalyst_disabled"
    """The news/catalyst layer disabled itself via the §2.7 fail-safe ladder (stale digest, too few
    source domains, feed outage, …). Warning, not critical: the platform keeps trading its
    deterministic strategies — it just stops originating on news (§10.3 ``CATALYST_DISABLED``)."""

    SCAN_SWEEP = "scan_sweep"
    """An on-demand scanner sweep completed (§3.2.5 sweep addendum, 2026-07-29): fired when the
    trade window becomes active or the owner asks via ``/scan_now``. Carries the explicit verdict —
    candidates published now, plus the deterministic PENDING arm levels ("X arms below ₹N") so the
    owner can decide whether shifting the window is worth it. Never silent: "nothing to trade right
    now" is a first-class answer."""

    POST_LOGIN_RECOVERY = "post_login_recovery"
    """The §2.6 cold-start RE-TRIGGER: after the owner completes the daily Kite login (the LAN
    ``/kite/callback`` route or the Telegram ``/token`` fallback both land in
    ``SessionManager.complete_login``), the post-login recovery re-runs the startup steps a pre-login
    boot could not — instruments load/persist, regime + warm-up-gap backfill, warm-up re-evaluation
    (and freeze-lift once coverage is met), and ticker start — and reports the per-step outcome so a
    BACKGROUND recovery is never silent. Info severity unless a step failed (then warning)."""

    EARLY_HYDRATION = "early_hydration"
    """The §2.6 early-hydration pass ran (owner-directed 2026-09-09): an early Kite login (~06:30) on
    a trading day pulled the pre-open chain — surveillance, universe, news chain, catalyst digest,
    pre-open planner — forward from its 08:20–08:50 clock, so the digest exists before the window
    opens on days the owner is travelling at 08:15. Carries the per-job outcome (ran / already run /
    failed); info severity unless a job failed (then warning)."""


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


def login_instruction(url: str) -> str:
    """The tappable re-login instruction sentence, shared by every login-required alert body
    (``login_prompt`` below and ``ops.token_check``'s pre-open probe)."""
    return f"Open: {url}\nThen send /token <request_token>."


def login_prompt(url: str) -> CatalogMessage:
    """Daily Kite login required (R6). ``url`` is the broker login URL; the owner opens it and
    returns the ``request_token`` via ``/token`` (§3.2.11)."""
    return CatalogMessage(
        kind=MessageKind.LOGIN_PROMPT,
        title="Kite login required",
        body=f"Daily login needed before trading can resume. {login_instruction(url)}",
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
    warmup_classes_short: list[str] | None = None,
    warmup_blockers: list[str] | None = None,
) -> CatalogMessage:
    """The every-startup recovery & catch-up report (§2.6 step 7 / STARTUP_REPORT).

    Critical when the book is frozen on an integrity failure, the kill switch is engaged, or the
    prior run crashed; otherwise info. The owner sees a compact status line plus the frozen/deferred
    reasons — the full structured report is preserved in ``data`` (and the ``startup_report`` log),
    never dumped as a raw dict into the prose (R8).

    ``warmup_classes_short`` renders its own line because since the 2026-09-13 per-class scoping
    (§2.6 step-6 addendum) a short warm-up class no longer implies a ``frozen:`` entry: an INTRADAY
    shortfall refuses intraday candidates per candidate and sends no ``WARMUP_FROZEN`` page, so this
    line is the owner's ONLY boot-time notice that the session is running on incomplete coverage."""
    lead = f"⚠ crash-recovered (prior state {prior_state}) — " if crash_recovered else ""
    frozen = ", ".join(frozen_reasons) if frozen_reasons else "none"
    deferred = ", ".join(deferred_steps) if deferred_steps else "none"
    classes = list(warmup_classes_short or [])
    blockers = list(warmup_blockers or [])
    warm_line = ""
    if classes:
        # The first few blockers only: a market-wide minute hole renders one line per watch symbol,
        # and the whole list is in ``data`` + the structured log for anyone who needs it.
        shown = ", ".join(blockers[:3])
        more = f", +{len(blockers) - 3} more" if len(blockers) > 3 else ""
        detail = f" ({shown}{more})" if shown else ""
        warm_line = f"\nwarm-up short: {', '.join(classes)}{detail}"
    return CatalogMessage(
        kind=MessageKind.STARTUP_REPORT,
        title="Startup recovery complete",
        body=(
            f"{lead}mode={mode} · risk={risk_state} · killed={killed} · "
            f"login_needed={needs_login} · integrity_ok={integrity_ok}\n"
            f"frozen: {frozen}\ndeferred: {deferred}{warm_line}"
        ),
        severity="critical" if (killed or not integrity_ok or crash_recovered) else "info",
        data={
            "mode": mode, "risk_state": risk_state, "killed": killed, "needs_login": needs_login,
            "integrity_ok": integrity_ok, "crash_recovered": crash_recovered,
            "prior_state": prior_state, "frozen_reasons": frozen_reasons,
            "deferred_steps": deferred_steps, "warmup_classes_short": classes,
            "warmup_blockers": blockers,
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


def feed_degraded(*, age_s: float, budget_s: float) -> CatalogMessage:
    """Live feed silent for ``age_s`` s DURING market hours while heartbeats keep the child HEALTHY.

    Warning (not critical): the ticker child is alive and heartbeating, but its KiteTicker is delivering
    no ticks, so NO self-built 1-minute bars are being written. Fires once on the HEALTHY→DEGRADED
    transition (§7.1 in-session tick-silence guard); the state recovers to HEALTHY when ticks resume.
    """
    return CatalogMessage(
        kind=MessageKind.FEED_DEGRADED,
        title="Feed degraded — no ticks in-session",
        body=(
            f"Heartbeats are healthy but NO live ticks for {age_s:.0f}s (>{budget_s:.0f}s budget) "
            "during market hours — the feed is tickless and self-built bars are NOT being written. "
            "Check the ticker child (see ticker_child_output logs)."
        ),
        severity="warning",
        data={"age_s": age_s, "budget_s": budget_s},
    )


def feed_wedged(*, respawns: int, age_s: float) -> CatalogMessage:
    """The ticker feed is WEDGED — repeated WARMING-timeout respawns never reached HEALTHY (§2.6/R2).

    Escalated ONCE after ``ticker.max_wedge_respawns`` consecutive WARMING-timeout respawns (2026-07-23
    sleep/resume wedge): the supervisor keeps retrying at capped backoff, but the child never delivers a
    heartbeat, so the feed is structurally down. Warning (not critical): risk-reducing exits are
    unaffected and the state recovers automatically once a respawn heartbeats.
    """
    return CatalogMessage(
        kind=MessageKind.FEED_WEDGED,
        title="Feed wedged — respawns not recovering",
        body=(
            f"The ticker feed has failed to come up after {respawns} consecutive WARMING-timeout "
            f"respawns (no heartbeat for {age_s:.0f}s on the last attempt). Still retrying at capped "
            "backoff, but the feed is delivering NO data — check the ticker child (ticker_child_output "
            "logs), network, and Kite token. Recovers automatically once a respawn heartbeats."
        ),
        severity="warning",
        data={"respawns": respawns, "age_s": age_s},
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
            # /veto is the rec-dismiss command (§3.6); /reject resolves owner_approvals rows.
            [{"text": "✗ No action", "command": f"/veto {rec_id}"}],
        ],
    )


def position_not_in_holdings(
    *,
    symbol: str,
    position_id: str,
    tracked_qty: int,
    held_qty: int,
    entry_rec_id: str | None,
) -> CatalogMessage:
    """A tracked OPEN CNC position is absent/short in the broker's holdings (§3.6 reconcile).

    The message exists to produce ONE owner action, so the body ends in the literal reply to type:
    ``/closed <entry_rec_id> <price>``. ``entry_rec_id`` is the ENTRY recommendation id from the
    learning ledger (``/closed`` accepts an exit rec's id too, but the entry is the id the ledger row
    is keyed on); when the position has no ledger row at all the alert says so and names the
    ``position_id`` instead — an un-actionable-but-explicit page beats silence, which is the
    eleven-session limbo this check was built to end.

    No ``reply_keyboard``: unlike ``REC_FILL_SUSPECTED``, the platform does not know the exit price —
    it is the owner's fact (§6.5) — so a one-tap button would have to invent one.
    """
    reply = (
        f"/closed {entry_rec_id} <price>" if entry_rec_id
        else f"/closed <rec_id> <price> (no learning-ledger row for position {position_id})"
    )
    return CatalogMessage(
        kind=MessageKind.POSITION_NOT_IN_HOLDINGS,
        title=f"{symbol} tracked but not in holdings",
        body=(
            f"The platform still tracks {symbol} x{tracked_qty}, but your Kite holdings show "
            f"{held_qty}. If you already sold it, report the exit so the ledger and the position "
            f"caps match reality:\n{reply}\nprice = your actual exit price. "
            f"(position {position_id}; if you still hold it, ignore this.)"
        ),
        severity="warning",
        data={
            "symbol": symbol,
            "position_id": position_id,
            "tracked_qty": tracked_qty,
            "held_qty": held_qty,
            "entry_rec_id": entry_rec_id,
        },
    )


def reconcile_drift(
    *,
    d: str,
    symbols_flagged: list[str],
    bars_compared: int,
    bad_bar_fraction: float,
    max_bad_bar_fraction: float,
) -> CatalogMessage:
    """Self-built vs official 1m bars drifted beyond the §3.2.3 thresholds on day ``d`` (A13).

    ``symbols_flagged`` lists the symbols whose bad-bar fraction exceeded
    ``reconcile.max_bad_bar_fraction``; offline (gap-backfilled) spans were already excluded from the
    denominator (§2.6), so this is genuine divergence. Official candles have replaced the drifted
    self-built rows (``src='kite_official'`` canonical, §4.4 job 2) — the alert is for investigation.
    """
    shown = ", ".join(symbols_flagged[:10]) + ("…" if len(symbols_flagged) > 10 else "")
    return CatalogMessage(
        kind=MessageKind.RECONCILE_DRIFT,
        title=f"Bar reconcile drift on {d}",
        body=(
            f"{len(symbols_flagged)} symbol(s) drifted beyond thresholds "
            f"(bad-bar fraction {bad_bar_fraction:.4f} > {max_bad_bar_fraction:.4f} "
            f"over {bars_compared} compared bars): {shown}\n"
            "Official candles are now canonical for the drifted rows; investigate the tick feed."
        ),
        severity="warning",
        data={
            "d": d,
            "symbols_flagged": symbols_flagged,
            "bars_compared": bars_compared,
            "bad_bar_fraction": bad_bar_fraction,
            "max_bad_bar_fraction": max_bad_bar_fraction,
        },
    )


def backfill_report(
    *,
    interval: str,
    symbols: int,
    bars_written: int,
    frm: str,
    to: str,
    duration_s: float,
    failures: list[str],
) -> CatalogMessage:
    """A ``BackfillJob`` run finished (§3.2.3; historical §4.4 job 3 or §2.6 warm-up gap-fill).

    ``frm``/``to`` are already-rendered IST strings (callers pass Clock-derived values; no Clock
    access here). Warning when any symbol failed — those symbols stay behind their checkpoint and the
    next run resumes them (A2 resumable)."""
    fail = f"\nFailed: {', '.join(failures)}" if failures else ""
    return CatalogMessage(
        kind=MessageKind.BACKFILL_REPORT,
        title=f"Backfill {interval} complete" + (" (with failures)" if failures else ""),
        body=(
            f"{bars_written} bars across {symbols} symbol(s) for [{frm} .. {to}] "
            f"in {duration_s:.1f}s (≤3 req/s, A2).{fail}"
        ),
        severity="warning" if failures else "info",
        data={
            "interval": interval,
            "symbols": symbols,
            "bars_written": bars_written,
            "frm": frm,
            "to": to,
            "duration_s": duration_s,
            "failures": failures,
        },
    )


def warmup_frozen(*, blockers: list[str], classes: list[str] | None = None) -> CatalogMessage:
    """Entries FROZEN by the cold-start warm-up gate (§2.6 step 6 / §7.1 ``warmup_ready``).

    Each blocker is a rendered "scope: have/need" line (e.g. ``"orb:RELIANCE bars 12/30"``) — the
    strategies/symbols whose feature lookbacks lack contiguous bar coverage. Entries reopen
    automatically once coverage is met; risk-reducing actions were never gated (R3).

    ``classes`` are the coverage classes short (2026-09-13, §2.6 step-6 addendum). It LEADS the body
    and travels in ``data`` beside the blockers rather than inside them: which class froze entries is
    the first thing the owner needs, an intraday line riding along in the same freeze must not read
    as the cause, and every element of ``blockers`` stays a rendered "scope: have/need" line for the
    consumers that parse it."""
    lead = f"classes short: {', '.join(classes)} — entries FROZEN\n" if classes else ""
    return CatalogMessage(
        kind=MessageKind.WARMUP_FROZEN,
        title="Warm-up incomplete — entries frozen",
        body=(
            f"{lead}"
            "Insufficient contiguous bar coverage for feature lookbacks (never trade on thin data):\n"
            + "\n".join(f"• {b}" for b in blockers)
        ),
        severity="warning",
        data={"blockers": blockers, "classes": list(classes or [])},
    )


def catchup_report(
    *,
    off_duration_s: float,
    jobs_caught_up: list[str],
    jobs_failed: list[str],
) -> CatalogMessage:
    """§2.6 step-5 ``CatchUpRunner`` summary: missed jobs replayed in dependency order after an off
    period. Failures of safety/deadline-critical jobs additionally raise DATA_FRESHNESS_FROZEN."""
    caught = ", ".join(jobs_caught_up) if jobs_caught_up else "none"
    failed = ", ".join(jobs_failed) if jobs_failed else "none"
    hours = off_duration_s / 3600.0
    return CatalogMessage(
        kind=MessageKind.CATCHUP_REPORT,
        title="Missed-job catch-up complete" + (" (with failures)" if jobs_failed else ""),
        body=(
            f"Off for {hours:.1f}h. Jobs caught up: {caught}\nFailed: {failed}"
        ),
        severity="warning" if jobs_failed else "info",
        data={
            "off_duration_s": off_duration_s,
            "jobs_caught_up": jobs_caught_up,
            "jobs_failed": jobs_failed,
        },
    )


def engine_crashloop(*, restarts: int, window_s: int) -> CatalogMessage:
    """Coalesced crash-loop alarm (§2.2/§10.7): ``restarts`` fast respawns inside ``window_s``.

    One loud page instead of one per restart. The engine should be held STOPPED for investigation —
    open positions remain broker-protected throughout (R3)."""
    return CatalogMessage(
        kind=MessageKind.ENGINE_CRASHLOOP,
        title="Engine crash-looping",
        body=(
            f"{restarts} restarts within {window_s}s — holding down for investigation. "
            "Open positions remain broker-protected (R3); check logs before restarting."
        ),
        severity="critical",
        data={"restarts": restarts, "window_s": window_s},
    )


def post_login_recovery(*, steps: list[tuple[str, str, str]]) -> CatalogMessage:
    """Post-login recovery summary (§2.6 cold-start RE-TRIGGER).

    ``steps`` is the ordered ``(name, status, detail)`` list, ``status`` in {ok, skipped, failed}.
    Info severity unless any step failed (then warning). The owner SEES this on Telegram so a
    background recovery (the login HTTP/Telegram path returned immediately) is never silent — it is
    the visible proof the engine re-armed after a boot BEFORE the daily login."""
    any_failed = any(status == "failed" for _name, status, _detail in steps)
    lines = [
        f"- {name}: {status}" + (f" ({detail})" if detail else "")
        for name, status, detail in steps
    ]
    return CatalogMessage(
        kind=MessageKind.POST_LOGIN_RECOVERY,
        title="Post-login recovery" + (" (with failures)" if any_failed else ""),
        body="Login complete - re-ran the startup recovery sequence:\n" + "\n".join(lines),
        severity="warning" if any_failed else "info",
        data={
            "steps": [{"name": n, "status": s, "detail": d} for n, s, d in steps],
            "any_failed": any_failed,
        },
    )


def early_hydration(*, at: str, outcomes: list[tuple[str, str]]) -> CatalogMessage:
    """Early-hydration summary (§2.6 addendum, 2026-09-09) — ONE owner line per early login.

    ``at`` is the already-``Clock``-derived HH:MM of the login (this module never reads a clock);
    ``outcomes`` is the ordered ``(job_id, outcome)`` list from ``CatchUpRunner.hydrate_ahead``,
    outcome in {ran, already_run, failed}. Reads as "Early hydration 06:32: universe_build ran, ...
    (surveillance already run)" — the owner's proof the day's digest exists before the window opens,
    and, on a bad morning, which job did not make it. Warning severity iff something failed."""
    ran = [j for j, o in outcomes if o == "ran"]
    failed = [j for j, o in outcomes if o == "failed"]
    already = [j for j, o in outcomes if o == "already_run"]
    body = f"Early hydration {at}: " + (", ".join(f"{j} ran" for j in ran) or "nothing left to run")
    if failed:
        body += " - FAILED: " + ", ".join(failed)
    if already:
        body += f" ({', '.join(already)} already run)"
    return CatalogMessage(
        kind=MessageKind.EARLY_HYDRATION,
        title="Early hydration" + (" (with failures)" if failed else ""),
        body=body,
        severity="warning" if failed else "info",
        data={
            "at": at,
            "outcomes": [{"job_id": j, "outcome": o} for j, o in outcomes],
            "ran": ran, "already_run": already, "failed": failed,
        },
    )


def mode_change(old: str, new: str, actor: str, reason: str) -> CatalogMessage:
    """Engine mode changed (§10.3 ``MODE_CHANGE``; §3.5.3 machine). Rendered from the ``mode.changed``
    event, so the owner sees every transition — including a gate-forced downgrade they did not ask for
    (which re-arms only on explicit owner action, never a timer; R3/R5)."""
    return CatalogMessage(
        kind=MessageKind.MODE_CHANGE,
        title=f"Mode {old} → {new}",
        body=f"Engine mode changed {old} → {new} by {actor} (reason: {reason}).",
        severity="warning",
        data={"old": old, "new": new, "actor": actor, "reason": reason},
    )


def risk_state_change(old: str, new: str, cause: str) -> CatalogMessage:
    """Risk state changed (§10.3 ``RISK_STATE``; §3.5.3 per-cause edges, most-restrictive-wins).

    ``cause`` is the latching cause (or the re-arm note) from
    :class:`engine.risk.causes.RiskStateLatch` — the actionable half: it names what must clear before
    entries reopen. Risk-reducing actions are never gated by any of these states (R3)."""
    return CatalogMessage(
        kind=MessageKind.RISK_STATE_CHANGE,
        title=f"Risk state {old} → {new}",
        body=(
            f"Risk state changed {old} → {new} (cause: {cause}).\n"
            "Risk-reducing exits/protection continue in every state (R3)."
        ),
        severity="warning",
        data={"old": old, "new": new, "cause": cause},
    )


def kill_state(*, killed: bool, reason: str, actor: str) -> CatalogMessage:
    """Kill switch engaged or reset (§10.3 ``KILL/KILL_RESET``, §7.2). Engaging is loud (``critical``)
    and single-step; the reset is the owner's two-step ``/kill_reset`` + ``/confirm`` flow (R10)."""
    return CatalogMessage(
        kind=MessageKind.KILL,
        title="KILL SWITCH ENGAGED" if killed else "Kill switch reset",
        body=(
            f"Trading halted — {reason} (by {actor}). Reset is owner two-step: /kill_reset then /confirm."
            if killed
            else f"Kill switch cleared by {actor} ({reason}); trading may resume subject to mode + risk state."
        ),
        severity="critical" if killed else "warning",
        data={"killed": killed, "reason": reason, "actor": actor},
    )


def trade_window_changed(*, start: str, end: str, buffer_min: int, actor: str) -> CatalogMessage:
    """The daily trade window was changed (§3.2.7/§10.3 ``TRADE_WINDOW_CHANGED``). Already-rendered
    ``HH:MM`` strings in (no Clock access here); the setter validated + audited before publishing."""
    return CatalogMessage(
        kind=MessageKind.TRADE_WINDOW_CHANGED,
        title=f"Trade window {start}-{end}",
        body=(
            f"Entries are now confined to {start}-{end} IST (MIS square-off buffer {buffer_min}m), "
            f"set by {actor}. Exits/protection are never gated by the window (R3)."
        ),
        severity="info",
        data={"start": start, "end": end, "buffer_min": buffer_min, "actor": actor},
    )


def budget_tier(old: str, new: str, window_spend: Decimal, window_key: str) -> CatalogMessage:
    """The §5.6 degrade ladder moved a rung (§10.3 ``BUDGET_TIER(DGn)``).

    Reuses :data:`MessageKind.BUDGET_WARNING` — the ladder IS the budget warning; a second kind for the
    same event would fork the audit log (R8). The window is named in the prose: the period is the
    subscription's Thursday-14:00-IST quota week, so "$103" alone reads as a month figure to the owner."""
    return CatalogMessage(
        kind=MessageKind.BUDGET_WARNING,
        title=f"Budget tier {old} → {new}",
        body=(
            f"LLM/API degrade ladder moved {old} → {new} (window spend ${window_spend}, "
            f"week from {window_key} 14:00 IST). "
            "Capabilities change per the §5.6 ladder; /budget shows the per-agent split."
        ),
        severity="warning",
        data={
            "old_tier": old,
            "new_tier": new,
            "window_key": window_key,
            "window_spend_usd": str(window_spend),
        },
    )


#: Owner-facing strategy names (2026-07-29 feedback: "orb/rsi2 are not easy to understand").
STRATEGY_LABELS: dict[str, str] = {
    "orb": "intraday breakout",
    "rsi2": "swing dip-buy",
    "trend": "swing trend-follow",
    "mom": "swing momentum",
    "cat": "news catalyst",
}

_STYLE_HEADERS: dict[str, str] = {
    "intraday": "INTRADAY (square off the same day)",
    "swing": "SWING (hold for days)",
    "position": "POSITION (longer hold)",
}


def _sweep_trade_line(row: dict[str, Any], *, live: bool) -> list[str]:
    """Two lines per setup: the WHAT (symbol/side/label/level) and the PLAN (stop/target/exit).

    ``row`` keys: symbol, side, strategy_id, entry|trigger, arms_when, last, stop, target,
    exit_rule, held (all optional except symbol/side/strategy_id).
    """
    label = STRATEGY_LABELS.get(row.get("strategy_id", ""), row.get("strategy_id", "?"))
    held = "  ← you HOLD this" if row.get("held") else ""
    if live:
        head = f"• {row['symbol']} {row['side']} ({label}){held}"
        detail = f"  entry ₹{row.get('entry', '?')}"
    else:
        verb = "breaks above" if row.get("arms_when") == "above" else "dips below"
        now = f" (now ₹{row['last']})" if row.get("last") is not None else ""
        head = f"• {row['symbol']} {row['side']} if price {verb} ₹{row.get('trigger', '?')}{now}{held}"
        detail = f"  entry ₹{row.get('trigger', '?')}"
    if row.get("stop") is not None:
        detail += f" · stop ₹{row['stop']}"
    if row.get("target") is not None:
        detail += f" · target ₹{row['target']}"
    elif row.get("exit_rule"):
        detail += f" · {row['exit_rule']}"
    return [head, detail]


def _sweep_section(rows: list[dict[str, Any]], *, live: bool) -> list[str]:
    """Group rows under their style header, intraday first (2026-07-29: separate the trade types)."""
    out: list[str] = []
    for style in ("intraday", "swing", "position"):
        block = [r for r in rows if r.get("style") == style]
        if not block:
            continue
        out.append(_STYLE_HEADERS[style] + ":")
        for row in block:
            out += _sweep_trade_line(row, live=live)
    return out


def scan_sweep(
    *,
    trigger: str,
    live: list[dict[str, Any]],
    pending: list[dict[str, Any]],
    suppressed_today: int,
) -> CatalogMessage:
    """§3.2.5 sweep verdict (2026-07-29): the owner always hears SOMETHING when a sweep runs.

    Rendering per owner feedback (2026-07-29): trade types grouped intraday/swing, every setup
    shows its full plan (entry + stop + target or exit rule), friendly strategy names, held
    positions flagged. ``live`` rows are candidates entering the pipeline now; ``pending`` rows are
    would-arm levels; ``suppressed_today`` counts day slots already spent.
    """
    if live:
        headline = f"{len(live)} candidate(s) qualify — evaluating now"
    elif pending:
        headline = "Nothing to trade right now — nearest setups below"
    else:
        headline = "Nothing to trade right now — no live or pending setups"
    lines = [headline]
    if live:
        lines += ["", "▶ LIVE — sent to the analyst:"] + _sweep_section(live, live=True)
    if pending:
        lines += ["", "⏳ NOT YET TRIGGERED — would arm at:"] + _sweep_section(pending, live=False)
    if suppressed_today:
        lines += ["", f"({suppressed_today} setup(s) already evaluated today — once-per-day rule)"]
    return CatalogMessage(
        kind=MessageKind.SCAN_SWEEP,
        title="Scan sweep: " + ("candidates found" if live else "nothing to trade right now"),
        body="\n".join(lines),
        severity="info",
        data={
            "trigger": trigger,
            "live": live,
            "pending": pending,
            "suppressed_today": suppressed_today,
        },
    )


def catalyst_watchlist(n_originating: int, n_context: int) -> CatalogMessage:
    """Daily catalyst watchlist built (§4.4 job 14 / §10.3 ``CATALYST_WATCHLIST(n_originating,
    n_context)``). ORIGINATING symbols may start a trade; CONTEXT-only symbols may only veto/size
    one (O11/§2.7) — the split is the whole message."""
    return CatalogMessage(
        kind=MessageKind.CATALYST_WATCHLIST,
        title="Catalyst watchlist built",
        body=(
            f"originating: {n_originating}\ncontext-only: {n_context}\n"
            "Context-only symbols can veto or size a trade, never originate one (O11)."
        ),
        severity="info",
        data={"n_originating": n_originating, "n_context": n_context},
    )


def catalyst_disabled(reason: str) -> CatalogMessage:
    """The news/catalyst layer disabled itself (§2.7 fail-safe ladder / §10.3 ``CATALYST_DISABLED``).
    Deterministic strategies keep running — only news-originated entries stop."""
    return CatalogMessage(
        kind=MessageKind.CATALYST_DISABLED,
        title="Catalyst layer disabled",
        body=(
            f"News-originated entries are OFF: {reason}\n"
            "Deterministic strategies are unaffected; the layer re-enables when the condition clears."
        ),
        severity="warning",
        data={"reason": reason},
    )


#: §3.6 recommendation rendering budgets — a Telegram message must stay readable on a phone.
THESIS_MAX_CHARS = 300
MAX_HEADROOM_LINES = 5

#: Placeholder for the one field the platform must NOT prefill: the price the owner actually paid.
#: Everything else in the footer is known at delivery time; a guessed fill price would be a fabricated
#: number in the owner's own audit trail (§8.1 — the recommendation's entry zone is not a fill).
_PRICE_PLACEHOLDER = "<price>"


def _capture_footer(rec: Recommendation) -> str:
    """The copy-ready outcome-capture line that closes every §3.6 recommendation (WO-29, 2026-08-26).

    The message used to end at the B7 checklist, and the only handle it carried for ``/taken`` was a
    ``rec_id`` printed nowhere — so on the first day the owner actually took a recommendation, the
    capture command was untypable. The footer now states the command verbatim, with the INSTRUMENT as
    the handle (which the owner commands resolve, WO-29(a)) and the recommended qty already filled in.
    Only the fill price stays a placeholder, because that number is the owner's, not the platform's.

    Three kinds, three honest footers. An ``entry`` is a fill to record or decline. An ``exit`` asks
    for an order whose result is reported with ``/closed``. An ``adjust`` places NO order at all (its
    checklist is "move the stop"), so it gets the decline half only — instructing the owner to
    ``/closed`` a position the platform just asked them to KEEP would be a wrong instruction on a
    money surface, and "the exit-kind form" is not one an adjust can honestly carry.
    """
    if rec.kind == "entry":
        return (
            f"record: /taken {rec.instrument} {rec.qty} {_PRICE_PLACEHOLDER} · "
            f"decline: /veto {rec.instrument}"
        )
    if rec.kind == "exit":
        return (
            f"record: /closed {rec.instrument} {_PRICE_PLACEHOLDER} · "
            f"decline: /veto {rec.instrument}"
        )
    return f"decline: /veto {rec.instrument}"


def recommendation_message(rec: Recommendation, *, ltp: Decimal | None = None) -> CatalogMessage:
    """Render a §3.6 :class:`~engine.core.contracts.Recommendation` for the owner (§10.3
    ``RECOMMENDATION``).

    Everything the human needs to act is in the prose — instrument/side/style/product, entry zone,
    stop, targets, size, the gate verdict WITH per-rule headroom (R1: headroom ships in the payload),
    this trade's breakeven math (C3), and the B7/R3 manual protective-order checklist. The platform
    places ZERO API orders in RECOMMEND, so the checklist is the mechanism that transfers protection
    responsibility to the human explicitly — it is never truncated away.

    LEVEL vs CURRENT PRICE (WO-4, 2026-08-13). A degenerate entry zone (``low == high``) is a LIMIT
    proposal: the price is an instruction, and for a ``brk20`` candidate it is specifically the broken
    20-day-high LEVEL, so it is labelled as one rather than shown as a range of itself. ``ltp`` is the
    live price the engine sized against; when supplied, the message states BOTH it and the level plus
    the gap between them, because "buy at 100" is only actionable next to "it is trading at 100.50" —
    the F5 defect was a payload that rendered a price the market had already left. It is optional so a
    caller with no live quote renders exactly the pre-WO-4 message rather than a fabricated one; the
    structured ``data`` carries ``ltp`` either way (``None`` when unknown), so the audit log can always
    tell "no quote" from "quote equal to the level".

    Failed gate checks sort first among the at-most :data:`MAX_HEADROOM_LINES` headroom lines: a
    ``shrink``/``owner_approval_required`` verdict is only meaningful next to the rule that caused it.
    The thesis is truncated at :data:`THESIS_MAX_CHARS`; the full object is on the dashboard.

    CAPTURE FOOTER (WO-29, 2026-08-26). The final line is the command the owner types back, with the
    instrument as its handle — see :func:`_capture_footer`. Before it, the §8.3 capture flow needed a
    ``rec_id`` this message never printed, which is exactly how the first executed recommendation
    ended up unrecordable.
    """
    low, high = rec.entry_zone
    at_level = low == high
    targets = " / ".join(str(t) for t in rec.targets) or "(none)"
    thesis = rec.thesis[:THESIS_MAX_CHARS] + ("…" if len(rec.thesis) > THESIS_MAX_CHARS else "")
    checks = sorted(rec.gate.checks, key=lambda c: c.passed)[:MAX_HEADROOM_LINES]
    approved = rec.gate.approved_qty if rec.gate.approved_qty is not None else rec.qty
    entry_text = f"level {low} (limit-at-level)" if at_level else f"entry {low}-{high}"

    lines = [
        f"{rec.side} {rec.instrument} · {rec.style}/{rec.product} · qty {rec.qty} "
        f"(notional ₹{rec.notional})",
        f"{entry_text} · stop {rec.stop} · targets {targets}",
    ]
    if ltp is not None and ltp > 0:
        gap = (low - ltp) / ltp * Decimal(100)
        lines.append(
            f"current price {ltp} · {'level' if at_level else 'entry'} {low} is {gap:+.2f}% away"
        )
    lines.append(
        f"confidence {rec.confidence:.2f} · valid until {rec.valid_until.isoformat(timespec='minutes')}"
    )
    if rec.short_flag_higher_tail_risk:
        lines.append("SHORT — higher tail risk (C8 shorting policy).")
    lines += [
        f"thesis: {thesis}",
        f"gate: {rec.gate.verdict} (approved qty {approved})",
    ]
    lines += [
        f"  • {c.rule_id}: {c.value} vs {c.limit} — headroom {c.headroom}{'' if c.passed else ' (FAILED)'}"
        for c in checks
    ]
    lines.append(
        f"cost: breakeven {rec.cost.breakeven_pct}% · total ₹{rec.cost.total_cost} · "
        f"edge {rec.cost.edge_multiple}x"
    )
    lines.append("checklist (yours to place — the platform places no orders in RECOMMEND, B7):")
    lines += [f"  • {item}" for item in rec.manual_checklist]
    # WO-29: the last line is the reply the owner types back — the message teaches its own capture.
    lines.append(_capture_footer(rec))

    return CatalogMessage(
        kind=MessageKind.RECOMMENDATION,
        title=f"Recommendation {rec.kind}: {rec.side} {rec.instrument}",
        body="\n".join(lines),
        severity="info",
        data={
            "rec_id": rec.rec_id,
            "kind": rec.kind,
            "instrument": rec.instrument,
            "side": rec.side,
            "style": rec.style,
            "product": rec.product,
            "qty": rec.qty,
            "notional": str(rec.notional),
            "entry_zone": [str(low), str(high)],
            # WO-4: the actionable trigger and the price it must be judged against, as separate
            # machine-readable fields — never only the level, never only the quote.
            "level": str(low) if at_level else None,
            "ltp": None if ltp is None else str(ltp),
            "stop": str(rec.stop),
            "targets": [str(t) for t in rec.targets],
            "confidence": rec.confidence,
            "verdict": rec.gate.verdict,
            "breakeven_pct": str(rec.cost.breakeven_pct),
            "valid_until": rec.valid_until.isoformat(),
        },
        # One-tap outcome capture (§3.6): the owner confirms the fill with the observed qty/price.
        # Keyed on the INSTRUMENT since WO-29, so this hint and the rendered footer prefill the same
        # command — a keyboard that still handed back a rec_id would reintroduce the exact defect the
        # footer exists to close, on the day someone finally wires the widget.
        reply_keyboard=[[{"text": f"✓ /taken {rec.instrument}",
                          "command": f"/taken {rec.instrument} {rec.qty} "}]],
    )


def data_freshness_frozen(*, job_id: str, last_success: str | None, reason: str) -> CatalogMessage:
    """A safety/deadline-critical daily job is not fresh ⇒ entries FROZEN (§2.6 step 5).

    ``last_success`` is a rendered IST timestamp of the job's last good run (or None if never).
    Entries stay frozen until the job runs/verifies; risk-reducing actions continue (R3)."""
    last = last_success or "never"
    return CatalogMessage(
        kind=MessageKind.DATA_FRESHNESS_FROZEN,
        title=f"Entries frozen — {job_id} not fresh",
        body=(
            f"Safety-critical job '{job_id}' could not run/verify before entries open "
            f"(last success: {last}). Reason: {reason}\n"
            "Entries FROZEN until fresh; exits/protection unaffected (R3)."
        ),
        severity="critical",
        data={"job_id": job_id, "last_success": last_success, "reason": reason},
    )
