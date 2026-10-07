"""Plan §9.4 chaos case 4 — Token expiry / mid-day invalidation (A5).

Must hold (§9.4 row 4, verbatim): "order placement fails-safe; FROZEN; protection already resting;
owner alerted with re-login link (R6)".

Scenario: 10:05 IST on a trading day, entries open, the Kite access token dies mid-session (A5 —
revoked / expired early). The next live broker call gets ``TokenException``. Composed exactly as
``engine.ops.main`` wires it: the real ``SessionManager`` (behavioural validity + one-shot circuit
breaker), the real ``KiteClient`` built with ``on_token_rejected=session.on_token_rejected`` and
``make_order_guard`` (predicate (a), §3.5.3; ``orders_enabled=True``, which production never passes,
D8), the §3.5.3 cause ledger, and the composition root's two hooks — ``_on_session_invalidated``
(freeze + critical alert + LOGIN_PROMPT with the login URL) and ``_clear_token_freeze`` (login hook).
Those two hooks are closures inside ``engine.ops.main.run``; they are replicated here line-for-line
and PINNED to the source by ``test_replicated_wiring_matches_the_composition_root`` so the replica
cannot drift silently. Faked: only pykiteconnect's ``KiteConnect`` (the Kite HTTP edge) and the
Telegram sink.

Clauses:
* "FROZEN" + "owner alerted with re-login link (R6)" —
  ``test_midday_token_death_freezes_entries_and_sends_the_login_link``.
* "order placement fails-safe" — ``test_order_placement_fails_safe_on_a_dead_token``: an opening
  order is refused BEFORE the broker (and before the rate budget); a risk-reducing order is never
  gated (R3) and its broker error surfaces instead of passing silently.
* Recovery (the §3.5.3 auto-recovery class "token invalid ⇒ clears on successful re-login"):
  ``test_relogin_clears_the_freeze_and_rearms_the_breaker``.
* "protection already resting" — Phase-3-gated (``test_protection_already_resting``): no platform
  positions or broker-resident SL-M/GTT exist in RECOMMEND; needs ProtectionManager (WO-P3-6).
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import time
from types import SimpleNamespace

import pytest
from kiteconnect.exceptions import TokenException

from engine.broker.kite_client import KiteClient, OrderSurfaceViolation
from engine.broker.rate_limiter import RateLimiter
from engine.broker.session import SessionManager
from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.enums import Actor, Mode, RiskState
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, KITE_API_SECRET
from engine.core.types import OwnerConfirmation, TradeWindow
from engine.marketdata.store import MarketStore
from engine.notify.catalog import MessageKind, login_prompt
from engine.ops import main as opsmain
from engine.risk.causes import RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager
from tests.chaos._entry_gate_rig import build_entry_gate, entry_checks
from tests.chaos.conftest import PHASE3_GATED
from tests.unit.test_session_verify_token import _FakeSecrets

LOGIN_URL = "https://kite.zerodha.com/connect/login?api_key=ak&v=3"
CAUSE = "kite_token_rejected"
WINDOW = TradeWindow(start=time(10, 0), end=time(10, 30))   # the settings.yaml trade-window seed
ENTRY_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "BUY", "quantity": 1,
             "order_type": "LIMIT", "product": "MIS", "price": 100}
EXIT_REQ = {"tradingsymbol": "RELIANCE", "exchange": "NSE", "transaction_type": "SELL", "quantity": 1,
            "order_type": "MARKET", "product": "MIS", "market_protection": -1}


class _FakeKiteConnect:
    """pykiteconnect's ``KiteConnect`` — the Kite REST edge. Every authenticated call raises
    ``TokenException`` once ``token_dead`` is set (what Kite returns for a revoked/expired token); a
    re-login (``generate_session`` → ``set_access_token``) mints a token the broker honours again."""

    def __init__(self) -> None:
        self.token_dead = False
        self.calls: list[str] = []
        self.access_token: str | None = None

    def _auth(self, op: str) -> None:
        self.calls.append(op)
        if self.token_dead:
            raise TokenException("Incorrect `api_key` or `access_token`.")

    def login_url(self) -> str:
        return LOGIN_URL

    def margins(self):
        self._auth("margins")
        return {"equity": {"net": 20000}}

    def orders(self):
        self._auth("orders")
        return []

    def place_order(self, variety: str, **params) -> str:
        self._auth("place_order")
        return "ORDER1"

    def generate_session(self, request_token: str, api_secret: str) -> dict:
        self.calls.append("generate_session")
        return {"access_token": "tok-relogin"}

    def set_access_token(self, token: str) -> None:
        self.access_token = token
        self.token_dead = False


@pytest.fixture
def token_rig(conn, clock, tmp_path):
    """The session / broker / risk slice of engine.ops.main.run, on the conftest clock (10:05 IST,
    Wed 2026-06-17, inside the seeded 10:00–10:30 trade window)."""
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn,
                           window_seed=WINDOW)
    mode = ModeManager(conn, clock, None, calendar, paper_only=False)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    kc = _FakeKiteConnect()
    session = SessionManager(_FakeSecrets({KITE_API_KEY: "ak", KITE_API_SECRET: "as",
                                           KITE_ACCESS_TOKEN: "tok-day1"}), clock)
    session._connect = lambda: kc                    # the api_key-bound KiteConnect (test seam)
    alerts: list[tuple[str, str]] = []
    notified: list = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    async def notify(msg) -> None:
        notified.append(msg)

    # ---- main.py freeze seam + the two session hooks, replicated line-for-line ----
    async def freeze_entries(reason: str) -> None:
        if not kill.is_killed():
            await latch.set_cause(reason, RiskState.FROZEN, reason, Actor.RISK_GATE)

    async def _on_session_invalidated() -> None:
        await freeze_entries("kite_token_rejected")
        await alert(
            "critical",
            "Kite token rejected by broker — entries FROZEN; re-login via the login link or /token (R6)",
        )
        await notify(login_prompt(session.login_url()))
    session.set_invalidation_hook(_on_session_invalidated)

    async def _clear_token_freeze() -> None:
        await latch.clear_cause("kite_token_rejected", Actor.RISK_GATE)
    session.add_login_hook(_clear_token_freeze)

    kite = KiteClient(kc, RateLimiter(clock, burst=100), clock,
                      on_token_rejected=session.on_token_rejected,
                      order_guard=opsmain.make_order_guard(mode, kill, calendar, clock), orders_enabled=True)
    mode.seed_trade_window_if_absent(WINDOW)          # lifecycle step 1b's first-run seed
    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    builder, gate = build_entry_gate(conn=conn, clock=clock, calendar=calendar, mode=mode, kill=kill,
                                     store=store)
    rig = SimpleNamespace(clock=clock, mode=mode, kill=kill, latch=latch, kc=kc, session=session,
                          kite=kite, alerts=alerts, notified=notified, builder=builder, gate=gate)
    yield rig
    store.close()


async def _token_dies_mid_session(rig) -> None:
    """The token dies; the next live broker call is the first to find out (and re-raises)."""
    rig.kc.token_dead = True
    with pytest.raises(TokenException):
        await rig.kite.margins()                     # never swallowed (R5/R8)


# ------------------------------------------------------------------------ FROZEN + login link
async def test_midday_token_death_freezes_entries_and_sends_the_login_link(token_rig):
    rig = token_rig
    await rig.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
    assert rig.session.token_valid() and rig.mode.risk_state() == RiskState.NORMAL
    _v, checks = await entry_checks(rig.builder, rig.gate, rig.clock, "RELIANCE")
    assert checks["mode_risk_state"].passed                   # entries were open before the death

    await _token_dies_mid_session(rig)

    # FROZEN, through its own §3.5.3 cause.
    assert rig.session.token_valid() is False
    assert rig.mode.risk_state() == RiskState.FROZEN
    assert [c for c, _s, _d in rig.latch.active_causes()] == [CAUSE]
    _v, checks = await entry_checks(rig.builder, rig.gate, rig.clock, "RELIANCE")
    assert not checks["mode_risk_state"].passed
    # Owner alerted — critical — with a TAPPABLE re-login link (R6), not just a notice.
    assert [s for s, _m in rig.alerts] == ["critical"] and "re-login" in rig.alerts[0][1]
    prompts = [m for m in rig.notified if m.kind is MessageKind.LOGIN_PROMPT]
    assert len(prompts) == 1
    assert prompts[0].severity == "critical"
    assert prompts[0].data["login_url"] == LOGIN_URL and LOGIN_URL in prompts[0].body

    # A burst of further 403s (every in-flight caller hits the dead token) is ONE page, not N.
    for _ in range(3):
        with pytest.raises(TokenException):
            await rig.kite.orders()
    assert len(rig.alerts) == 1
    assert len([m for m in rig.notified if m.kind is MessageKind.LOGIN_PROMPT]) == 1
    assert rig.mode.risk_state() == RiskState.FROZEN


# ------------------------------------------------------------------- order placement fails-safe
async def test_order_placement_fails_safe_on_a_dead_token(token_rig):
    """AUTO is used deliberately (owner two-step): in RECOMMEND the order surface refuses openings
    anyway, so only AUTO proves it is the token FREEZE that blocks here, not the mode."""
    rig = token_rig
    await rig.mode.request_transition(Mode.AUTO, Actor.OWNER,
                                      OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
    # CONTROL: live token, AUTO ∧ NORMAL ∧ in-window ⇒ the opening order reaches the broker.
    assert await rig.kite.place_order(ENTRY_REQ, intent="entry") == "ORDER1"
    assert rig.kc.calls == ["place_order"]

    await _token_dies_mid_session(rig)
    rig.kc.calls.clear()

    # An opening order is refused by the order surface BEFORE the broker and the rate limiter.
    with pytest.raises(OrderSurfaceViolation, match="risk_state=FROZEN"):
        await rig.kite.place_order(ENTRY_REQ, intent="entry")
    assert rig.kc.calls == []

    # A risk-reducing order is NEVER gated (R3) — it goes to the broker, and the dead token's
    # rejection surfaces to the caller (fail loud, never a silent "placed"); no second page.
    with pytest.raises(TokenException):
        await rig.kite.place_order(EXIT_REQ, intent="risk_reducing")
    assert rig.kc.calls == ["place_order"]
    assert len(rig.alerts) == 1


# ---------------------------------------------------------------------------------- recovery
async def test_relogin_clears_the_freeze_and_rearms_the_breaker(token_rig):
    rig = token_rig
    await rig.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
    await _token_dies_mid_session(rig)
    assert rig.mode.risk_state() == RiskState.FROZEN

    # The owner taps the link; Kite redirects to /kite/callback (or /token) → complete_login, which
    # fires the login hooks fire-and-forget — wait for them the way the loop would run them.
    await rig.session.complete_login("req-token-from-redirect")
    await asyncio.wait_for(asyncio.gather(*list(rig.session._login_tasks)), timeout=5)

    assert rig.session.token_valid() is True
    assert rig.mode.risk_state() == RiskState.NORMAL
    assert rig.latch.active_causes() == []                    # the cause cleared symmetrically
    _v, checks = await entry_checks(rig.builder, rig.gate, rig.clock, "RELIANCE")
    assert checks["mode_risk_state"].passed
    assert await rig.kite.margins() == {"equity": {"net": 20000}}   # the broker honours the new token

    # The breaker re-armed: a second death the same day pages the owner AGAIN (not deduped away).
    await _token_dies_mid_session(rig)
    assert rig.mode.risk_state() == RiskState.FROZEN
    assert len([m for m in rig.notified if m.kind is MessageKind.LOGIN_PROMPT]) == 2


# ------------------------------------------------------------------------------- Phase-3-gated
@pytest.mark.skip(reason=f"'protection already resting' — {PHASE3_GATED}; missing: ProtectionManager "
                         "(broker-resident SL-M/GTT); RECOMMEND holds no platform positions")
async def test_protection_already_resting():
    raise AssertionError("unreachable — skipped")


# ----------------------------------------------------------------------------------- wiring pin
def test_replicated_wiring_matches_the_composition_root():
    """The hooks above are copies of closures inside engine.ops.main.run — pin the lines that carry
    the behaviour, so a change there fails HERE instead of leaving a stale replica."""
    src = inspect.getsource(opsmain.run)
    for line in (
        "await latch.set_cause(reason, RiskState.FROZEN, reason, Actor.RISK_GATE)",
        'await freeze_entries("kite_token_rejected")',
        "await notify(login_prompt(session.login_url()))",
        "session.set_invalidation_hook(_on_session_invalidated)",
        'await latch.clear_cause("kite_token_rejected", Actor.RISK_GATE)',
        "session.add_login_hook(_clear_token_freeze)",
        "on_token_rejected=session.on_token_rejected,",
        "order_guard=make_order_guard(mode, kill, calendar, clock))",
    ):
        assert line in src, f"engine.ops.main.run no longer contains: {line}"
