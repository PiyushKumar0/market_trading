"""Phase-2 owner command surface (§3.2.11) + the §10.3 catalog renderers + the outbound bus alerts.

Three properties are load-bearing and each has a test here:

* **Owner-ID lock (R10)** still guards the NEW commands — a foreign chat gets no reply and causes no
  side effect (not "an error message", *nothing*).
* **Unwired ≠ pretending.** Every Phase-2 dependency is optional; with it absent the command says so
  instead of half-acting (the RECOMMEND-mode honesty rule, B7).
* **No raw model leakage.** Catalog messages render as owner prose; the structured payload stays in
  ``data`` for the audit log (R8) — mirrors ``test_notify_owner``'s render regression.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_yaml
from engine.core.contracts import CheckResult, CostBreakdown, GateVerdict, Recommendation
from engine.core.enums import Actor, DegradeTier, Mode, RiskState, Routing
from engine.core.protected_store import ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.intelligence.events import TOPIC_BUDGET_STATE, BudgetStateChanged
from engine.intelligence.governor import BudgetGovernor
from engine.notify import catalog
from engine.notify.catalog import MessageKind
from engine.notify.telegram import TelegramBot, _owner_only
from engine.risk.causes import CAUSE_OWNER_PAUSE, CAUSE_REJECTION_STORM, RiskStateLatch
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
from engine.risk.exposure import ExposureTracker
from engine.risk.limits import LimitsEngine
from engine.risk.mode import ModeManager

OWNER_CHAT = 4242
FOREIGN_CHAT = 999
REAL_LIMITS_PATH = Path(__file__).resolve().parents[2] / "config" / "limits.yaml"
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="two-step")


# --------------------------------------------------------------------------- test doubles
class _CapturingMessage:
    """A fake Telegram message that records the text a handler replies with (see telegram._reply)."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.sent.append(text)


class _Chat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class _Update:
    """Minimal Update: captures replies AND carries a chat id, so the owner lock is testable on a
    handler that would otherwise reply."""

    def __init__(self, msg: _CapturingMessage, chat_id: int = OWNER_CHAT) -> None:
        self.effective_message = msg
        self.message = msg
        self.effective_chat = _Chat(chat_id)


class _Ctx:
    def __init__(self, *args: str) -> None:
        self.args = list(args)


class _FakeBook:
    """Stand-in for ``RecommendationBook`` (§3.6) — records calls, returns an owner summary."""

    def __init__(self, *, raises: Exception | None = None) -> None:
        self.calls: list[tuple] = []
        self._raises = raises

    async def take(self, rec_id: str, qty: int, price: Decimal) -> str:
        self.calls.append(("take", rec_id, qty, price))
        if self._raises:
            raise self._raises
        return f"recorded: took {rec_id} {qty} @ {price}"

    async def close(self, rec_id: str, price: Decimal) -> str:
        self.calls.append(("close", rec_id, price))
        if self._raises:
            raise self._raises
        return f"recorded: closed {rec_id} @ {price}"

    async def veto(self, rec_id: str) -> str:
        self.calls.append(("veto", rec_id))
        if self._raises:
            raise self._raises
        return f"recorded: vetoed {rec_id}"


class _BookWithApproval(_FakeBook):
    async def apply_approval(self, approval_id: str) -> str:
        self.calls.append(("apply_approval", approval_id))
        return f"stop updated from approval {approval_id}"


class _FakeSession:
    def __init__(self, *, raises: Exception | None = None) -> None:
        self.tokens: list[str] = []
        self._raises = raises

    async def complete_login(self, request_token: str) -> None:
        self.tokens.append(request_token)
        if self._raises:
            raise self._raises


class _SendBot:
    """Stand-in for ``app.bot``: records what the outbound path actually put on the wire."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.sent.append((chat_id, text))


class _SendApp:
    def __init__(self, bot: _SendBot) -> None:
        self.bot = bot


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def bot(clock) -> TelegramBot:
    """A bot with NO Phase-2 dependency wired — the "not wired" baseline."""
    return TelegramBot("dummy-token", owner_chat_id=OWNER_CHAT, clock=clock)


@pytest.fixture
def msg() -> _CapturingMessage:
    return _CapturingMessage()


@pytest.fixture
def latch(conn, clock) -> RiskStateLatch:
    return RiskStateLatch(conn, clock, ModeManager(conn, clock))


@pytest.fixture
def exposure(conn, clock) -> ExposureTracker:
    return ExposureTracker(conn, clock, Decimal("20000"))


@pytest.fixture
def limits_engine(tmp_path, conn, clock) -> LimitsEngine:
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_bytes(REAL_LIMITS_PATH.read_bytes())
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    store = ProtectedStore(cfg, conn, clock)
    store.register_initial("limits.yaml", OWNER_OK)
    return LimitsEngine(store)


@pytest.fixture
def governor(conn, clock) -> BudgetGovernor:
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    return BudgetGovernor(conn, clock, calendar, load_yaml(config_dir() / "agents.yaml"))


def _insert_position(conn, position_id: str, **kw) -> None:
    row = {
        "symbol": "RELIANCE", "side": "BUY", "style": "intraday", "product": "MIS", "qty": 5,
        "avg_entry": "2450.00", "stop": "2400.00", "target": "2550.00", "state": "OPEN",
        "protection_state": "PROTECTED", "origin": "recommended", "opened_at": "2026-06-17T10:00:00+05:30",
        "closed_at": None, "realized_pnl": None, "costs": None,
    }
    row.update(kw)
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, style, product, qty, avg_entry, stop, "
        "target, state, protection_state, origin, opened_at, closed_at, realized_pnl, costs) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (position_id, *[row[k] for k in (
            "symbol", "side", "style", "product", "qty", "avg_entry", "stop", "target", "state",
            "protection_state", "origin", "opened_at", "closed_at", "realized_pnl", "costs")]),
    )


# --------------------------------------------------------------------------- unwired dependencies
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("taken", ("rec-1", "5", "2450")),
        ("closed", ("rec-1", "2500")),
        ("veto", ("rec-1",)),
        ("positions", ()),
        ("pnl", ()),
        ("budget", ()),
        ("limits", ()),
        ("pause_entries", ()),
        ("resume_entries", ()),
        ("approve", ("ap-1",)),
        ("reject", ("ap-1",)),
        ("token", ("tok",)),
    ],
)
async def test_unwired_dependency_says_so(bot, msg, command, args):
    """A command whose Phase-2 dependency is absent must SAY it is unwired — never half-act, and never
    silently succeed (the dependency check precedes argument parsing on purpose)."""
    await bot._live_handlers()[command](_Update(msg), _Ctx(*args))
    assert len(msg.sent) == 1
    assert "not wired" in msg.sent[0]
    assert f"/{command}" in msg.sent[0]


# --------------------------------------------------------------------------- owner-ID lock (R10)
@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["taken", "veto", "pause_entries", "resume_entries", "approve"])
async def test_new_commands_are_owner_locked(clock, conn, latch, msg, command):
    """A foreign chat gets NO reply and causes NO state change — silent drop, not an error message."""
    book = _FakeBook()
    bot = TelegramBot("dummy-token", owner_chat_id=OWNER_CHAT, clock=clock,
                      reco_book=book, latch=latch, conn=conn)
    guarded = _owner_only(bot, bot._live_handlers()[command])

    await guarded(_Update(msg, chat_id=FOREIGN_CHAT), _Ctx("rec-1", "5", "2450"))
    assert msg.sent == []
    assert book.calls == []
    assert latch.active_causes() == []

    await guarded(_Update(msg, chat_id=OWNER_CHAT), _Ctx("rec-1", "5", "2450"))
    assert len(msg.sent) == 1                       # the owner's identical update DOES reach it


# --------------------------------------------------------------------------- outcome capture (§3.6)
@pytest.mark.asyncio
async def test_taken_delegates_to_the_book(clock, msg):
    book = _FakeBook()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, reco_book=book)
    await bot._cmd_taken(_Update(msg), _Ctx("rec-1", "5", "2,450.25"))
    # qty is an int and price an exact Decimal (money is never a float, §8.1); commas tolerated.
    assert book.calls == [("take", "rec-1", 5, Decimal("2450.25"))]
    assert msg.sent == ["recorded: took rec-1 5 @ 2450.25"]


@pytest.mark.asyncio
@pytest.mark.parametrize("args", [("rec-1",), ("rec-1", "5"), ("rec-1", "5", "2450", "extra")])
async def test_taken_rejects_wrong_arity(clock, msg, args):
    book = _FakeBook()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, reco_book=book)
    await bot._cmd_taken(_Update(msg), _Ctx(*args))
    assert msg.sent == ["usage: /taken <rec_id> <qty> <price>"]
    assert book.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("args", [("rec-1", "0", "2450"), ("rec-1", "five", "2450"), ("rec-1", "5", "abc")])
async def test_taken_rejects_bad_qty_or_price(clock, msg, args):
    book = _FakeBook()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, reco_book=book)
    await bot._cmd_taken(_Update(msg), _Ctx(*args))
    assert "invalid qty/price" in msg.sent[0]
    assert book.calls == []


@pytest.mark.asyncio
async def test_book_value_error_is_reported_verbatim(clock, msg):
    """The book raises ValueError carrying an owner-facing message (unknown rec / bad state) — the bot
    surfaces it instead of crashing the control plane."""
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock,
                      reco_book=_FakeBook(raises=ValueError("unknown recommendation rec-9")))
    await bot._cmd_veto(_Update(msg), _Ctx("rec-9"))
    assert msg.sent == ["unknown recommendation rec-9"]


@pytest.mark.asyncio
async def test_closed_and_veto_delegate(clock, msg):
    book = _FakeBook()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, reco_book=book)
    await bot._cmd_closed(_Update(msg), _Ctx("rec-1", "2500.50"))
    await bot._cmd_veto(_Update(msg), _Ctx("rec-2"))
    assert book.calls == [("close", "rec-1", Decimal("2500.50")), ("veto", "rec-2")]
    assert msg.sent == ["recorded: closed rec-1 @ 2500.50", "recorded: vetoed rec-2"]


@pytest.mark.asyncio
async def test_close_is_honest_guidance_and_marks_nothing(clock, conn, msg):
    """RECOMMEND places ZERO API orders (B7): /close must say the exit is owner-side, and must not
    touch the position row it names."""
    _insert_position(conn, "pos-1")
    book = _FakeBook()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn, reco_book=book)
    await bot._cmd_close(_Update(msg), _Ctx("pos-1"))

    text = msg.sent[0]
    assert "pos-1" in text and "nothing was sent to the broker" in text
    assert "/closed" in text                                        # points at the capture command
    assert "not implemented" not in text.lower()                    # honest wording, not a dead stub
    assert book.calls == []
    assert conn.execute("SELECT state FROM positions WHERE position_id='pos-1'").fetchone()["state"] == "OPEN"


# --------------------------------------------------------------------------- read-only reports
@pytest.mark.asyncio
async def test_positions_lists_open_rows(clock, conn, msg):
    _insert_position(conn, "pos-1")
    _insert_position(conn, "pos-2", symbol="TCS", side="SELL", product="CNC", qty=3,
                     avg_entry="3100.00", stop=None, target=None, origin="platform",
                     protection_state=None, opened_at="2026-06-17T10:01:00+05:30")
    _insert_position(conn, "pos-3", state="CLOSED", closed_at="2026-06-17T10:02:00+05:30")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)

    await bot._cmd_positions(_Update(msg), _Ctx())
    text = msg.sent[0]
    assert "open positions: 2" in text and "pos-3" not in text     # closed rows excluded
    assert "RELIANCE BUY 5 @ 2450.00" in text and "recommended" in text
    assert "TCS SELL 3 @ 3100.00" in text and "unprotected" in text
    assert "stop -" in text                                         # NULL renders as a dash, not None
    assert "None" not in text and "sqlite3.Row" not in text


@pytest.mark.asyncio
async def test_positions_empty_book(clock, conn, msg):
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)
    await bot._cmd_positions(_Update(msg), _Ctx())
    assert msg.sent == ["no open positions."]


@pytest.mark.asyncio
async def test_pnl_reports_realised_today_and_flags_a_missing_mark_source(clock, conn, exposure, msg):
    _insert_position(conn, "pos-closed", state="CLOSED", closed_at="2026-06-17T10:02:00+05:30",
                     realized_pnl="500.00", costs="40.00")
    _insert_position(conn, "pos-open")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, exposure=exposure)

    await bot._cmd_pnl(_Update(msg), _Ctx())
    text = msg.sent[0]
    assert "realised today: ₹460.00" in text                # 500 gross - 40 costs, net (§7.1)
    assert "open MTM: n/a offline (no mark source)" in text  # never a zero placeholder posing as a mark
    assert "equity: ₹20,460.00" in text and "open: 1" in text
    assert "2026-06-17" in text


@pytest.mark.asyncio
async def test_limits_renders_usage_against_the_protected_caps(clock, conn, exposure, limits_engine, msg):
    _insert_position(conn, "pos-1")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock,
                      exposure=exposure, limits_engine=limits_engine)

    await bot._cmd_limits(_Update(msg), _Ctx())
    text = msg.sent[0]
    table = limits_engine.table()
    assert f"open: 1/{table.limits.max_open_positions.total}" in text
    assert f"consecutive losses: 0/{table.limits.consecutive_losses.max_per_session}" in text
    assert f"trades today: 1/{table.limits.max_new_trades_day.count}" in text
    assert "deployed capital: ₹12,250.00 / ₹20,000.00" in text
    assert "equity: ₹" in text and "day MTM: ₹" in text
    assert "{" not in text and "LimitTable" not in text


@pytest.mark.asyncio
async def test_limits_survives_an_unreadable_store(clock, exposure, limits_engine, msg):
    """An integrity failure is the gate's call (§2.4), never the bot's — /limits reports and lives."""

    class _Broken:
        def table(self):
            raise RuntimeError("limits.yaml hash mismatch")

    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, exposure=exposure, limits_engine=_Broken())
    await bot._cmd_limits(_Update(msg), _Ctx())
    assert "limits unavailable" in msg.sent[0] and "hash mismatch" in msg.sent[0]


@pytest.mark.asyncio
async def test_budget_reports_spend_pace_tier_and_per_agent_split(clock, governor, msg):
    from engine.intelligence.governor import TokenUsage

    # haiku-4.5 input is $1/MTok => 1 input token == 1 micro-dollar: an exact ledger amount.
    await governor.record("weekly_researcher", "haiku-4.5", TokenUsage(in_tokens=2_500_000, out_tokens=0))
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, governor=governor)

    await bot._cmd_budget(_Update(msg), _Ctx())
    text = msg.sent[0]
    assert "month spend: $2.5000" in text
    assert "pro-rata to date:" in text and "tier: DG" in text
    assert "per agent (spend / allocation):" in text
    assert "weekly_researcher: $2.5000 /" in text
    assert "{" not in text


# --------------------------------------------------------------------------- entries pause / re-arm
@pytest.mark.asyncio
async def test_pause_entries_latches_the_owner_cause(clock, conn, latch, msg):
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, latch=latch)
    await bot._cmd_pause_entries(_Update(msg), _Ctx())

    assert latch.active_causes() == [(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries")]
    assert ModeManager(conn, clock).risk_state() == RiskState.FROZEN
    assert "PAUSED" in msg.sent[0] and "FROZEN" in msg.sent[0]


@pytest.mark.asyncio
async def test_resume_entries_clears_owner_pause_and_rejection_storm(clock, conn, latch, msg):
    """§3.5.3: a rejection-storm freeze has no auto-recovery — /resume_entries IS its recovery path,
    so the command must clear BOTH causes, not just the owner's own pause."""
    await latch.set_cause(CAUSE_REJECTION_STORM, RiskState.FROZEN, "3 rejects/60s", Actor.RISK_GATE)
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, latch=latch)
    await bot._cmd_pause_entries(_Update(msg), _Ctx())
    assert len(latch.active_causes()) == 2

    await bot._cmd_resume_entries(_Update(msg), _Ctx())
    assert latch.active_causes() == []
    assert ModeManager(conn, clock).risk_state() == RiskState.NORMAL
    assert "NORMAL" in msg.sent[-1]


@pytest.mark.asyncio
async def test_resume_entries_reports_a_cause_it_may_not_clear(clock, conn, latch, msg):
    """A stale feed is NOT the owner's to hand-wave away: entries stay frozen and the reply says why."""
    await latch.set_cause("stale_feed", RiskState.FROZEN, "tick age 7s", Actor.RISK_GATE)
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, latch=latch)

    await bot._cmd_resume_entries(_Update(msg), _Ctx())
    assert ModeManager(conn, clock).risk_state() == RiskState.FROZEN
    assert "Still latched by: stale_feed" in msg.sent[0]


# --------------------------------------------------------------------------- owner approvals (§3.4)
def _insert_approval(conn, approval_id: str, payload: str = '{"position_id": "pos-1", "new_stop": "2380.00"}',
                     status: str = "pending") -> None:
    conn.execute(
        "INSERT INTO owner_approvals (approval_id, kind, payload, status, requested_at) "
        "VALUES (?, 'stop_widen', ?, ?, '2026-06-17T10:00:00+05:30')",
        (approval_id, payload, status),
    )


@pytest.mark.asyncio
async def test_approve_stamps_the_row_and_renders_the_payload(clock, conn, msg):
    _insert_approval(conn, "ap-1")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)

    await bot._cmd_approve(_Update(msg), _Ctx("ap-1"))
    row = conn.execute("SELECT status, resolved_at FROM owner_approvals WHERE approval_id='ap-1'").fetchone()
    assert row["status"] == "approved"
    assert row["resolved_at"] == clock.now().isoformat()        # Clock-stamped, tz-aware IST

    text = msg.sent[0]
    assert "ap-1 approved (stop_widen)" in text
    assert "position_id: pos-1" in text and "new_stop: 2380.00" in text
    assert "{" not in text and "'" not in text                 # payload rendered, never dumped raw


@pytest.mark.asyncio
async def test_reject_stamps_rejected(clock, conn, msg):
    _insert_approval(conn, "ap-2")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)
    await bot._cmd_reject(_Update(msg), _Ctx("ap-2"))
    assert conn.execute("SELECT status FROM owner_approvals WHERE approval_id='ap-2'").fetchone()["status"] == "rejected"
    assert "ap-2 rejected" in msg.sent[0]


@pytest.mark.asyncio
async def test_approve_unknown_and_already_resolved(clock, conn, msg):
    _insert_approval(conn, "ap-3", status="approved")
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn)

    await bot._cmd_approve(_Update(msg), _Ctx("nope"))
    assert msg.sent[-1] == "unknown approval nope."
    await bot._cmd_reject(_Update(msg), _Ctx("ap-3"))
    assert "already approved" in msg.sent[-1]
    # The resolved row is untouched by the second command (idempotent, no re-stamp).
    assert conn.execute("SELECT status FROM owner_approvals WHERE approval_id='ap-3'").fetchone()["status"] == "approved"


@pytest.mark.asyncio
async def test_approve_offers_the_approval_to_a_book_that_can_apply_it(clock, conn, msg):
    """Probe, not requirement: a book WITHOUT ``apply_approval`` resolves the row and stops there."""
    _insert_approval(conn, "ap-4")
    _insert_approval(conn, "ap-5")
    smart, plain = _BookWithApproval(), _FakeBook()

    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn, reco_book=smart)
    await bot._cmd_approve(_Update(msg), _Ctx("ap-4"))
    assert ("apply_approval", "ap-4") in smart.calls
    assert "stop updated from approval ap-4" in msg.sent[-1]

    bot_plain = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn, reco_book=plain)
    await bot_plain._cmd_approve(_Update(msg), _Ctx("ap-5"))
    assert plain.calls == []
    assert conn.execute("SELECT status FROM owner_approvals WHERE approval_id='ap-5'").fetchone()["status"] == "approved"


@pytest.mark.asyncio
async def test_reject_never_applies_an_approval(clock, conn, msg):
    _insert_approval(conn, "ap-6")
    smart = _BookWithApproval()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, conn=conn, reco_book=smart)
    await bot._cmd_reject(_Update(msg), _Ctx("ap-6"))
    assert smart.calls == []


# --------------------------------------------------------------------------- /token (§10.2 fallback)
@pytest.mark.asyncio
async def test_token_completes_the_login(clock, msg):
    session = _FakeSession()
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock, session=session)
    await bot._cmd_token(_Update(msg), _Ctx("req-token-123"))
    assert session.tokens == ["req-token-123"]
    assert "Session live" in msg.sent[0]


@pytest.mark.asyncio
async def test_token_reports_a_failed_login(clock, msg):
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock,
                      session=_FakeSession(raises=RuntimeError("Token is invalid or has expired")))
    await bot._cmd_token(_Update(msg), _Ctx("stale"))
    assert msg.sent == ["login failed: Token is invalid or has expired"]

    await bot._cmd_token(_Update(msg), _Ctx())
    assert msg.sent[-1] == "usage: /token <request_token>"


# --------------------------------------------------------------------------- catalog renderers (§10.3)
def _recommendation(clock) -> Recommendation:
    now = clock.now()
    cost = CostBreakdown(
        notional="12250.00", total_cost="18.20", breakeven_pct="0.15",
        expected_edge_pct="0.45", edge_multiple="3.0",
        components={"brokerage": "0.00", "stt": "12.25", "gst": "0.95"},
    )
    checks = [
        CheckResult(rule_id=f"rule_{i}", passed=True, value=f"{i}", limit="10", headroom=f"{10 - i}")
        for i in range(6)
    ]
    checks.append(CheckResult(rule_id="per_trade_risk", passed=False, value="1.4%",
                              limit="1.0%", headroom="-0.4%"))
    return Recommendation(
        rec_id="rec-1", created_at=now, valid_until=now, kind="entry",
        instrument="RELIANCE", side="BUY", style="intraday", product="MIS",
        entry_zone=("2449.00", "2451.00"), stop="2400.00", targets=["2550.00", "2600.00"],
        qty=5, notional="12250.00", thesis="T" * 400, confidence=0.72,
        gate=GateVerdict(
            verdict_id="v-1", proposal_id="p-1", verdict="shrink", original_qty=10, approved_qty=5,
            checks=checks, cost=cost, reasons=["shrunk to fit per_trade_risk"],
            mode=Mode.RECOMMEND, risk_state=RiskState.NORMAL, degrade_tier="DG0", evaluated_at=now,
        ),
        cost=cost,
        manual_checklist=["after entry fills place SL-M at 2400.00", "square off by 10:30"],
    )


def test_recommendation_message_renders_the_full_36_payload(clock):
    rec = _recommendation(clock)
    message = catalog.recommendation_message(rec)
    text = message.render()

    assert message.kind == MessageKind.RECOMMENDATION
    for fragment in (
        "BUY RELIANCE", "intraday/MIS", "qty 5", "₹12250.00",
        "entry 2449.00-2451.00", "stop 2400.00", "targets 2550.00 / 2600.00",
        "confidence 0.72", "gate: shrink (approved qty 5)",
        "breakeven 0.15%", "edge 3.0x",
        "after entry fills place SL-M at 2400.00", "square off by 10:30",
    ):
        assert fragment in text, fragment
    # Thesis truncated (300 chars + ellipsis), never the whole 400-char blob.
    assert "T" * 300 + "…" in text and "T" * 301 not in text
    # At most five headroom lines, FAILED rules first so a shrink verdict is explicable.
    headroom_lines = [ln for ln in text.splitlines() if ln.startswith("  • rule_") or "per_trade_risk" in ln]
    assert len(headroom_lines) == 5
    assert "per_trade_risk: 1.4% vs 1.0% — headroom -0.4% (FAILED)" in text
    # R8: structured payload lives in data, never leaks into the owner's prose.
    assert message.data["rec_id"] == "rec-1" and message.data["verdict"] == "shrink"
    for leak in ("CheckResult(", "rule_id=", "MessageKind.", "data={", "reply_keyboard="):
        assert leak not in text, leak
    # A genuine ZONE (low != high) is not a level, and an unsupplied quote is stated as unknown
    # rather than invented — the pre-WO-4 prose is byte-identical in that case.
    assert message.data["level"] is None and message.data["ltp"] is None
    assert "current price" not in text


# ------------------------------------------------------- WO-4: level + current price (§3.6/F5)
def _limit_recommendation(clock, level: str = "2450.00") -> Recommendation:
    """A LIMIT proposal: the entry zone is the single instructed price (``low == high``), which is
    the shape ``brk20`` now produces — the broken 20-day-high LEVEL."""
    rec = _recommendation(clock)
    return rec.model_copy(update={"entry_zone": (Decimal(level), Decimal(level))})


def test_recommendation_states_both_the_level_and_the_current_price(clock):
    """WO-4/F5: the payload rendered a stale price verbatim and said nothing about where the market
    actually was. It must now state BOTH, in prose and in the structured data."""
    text = catalog.recommendation_message(
        _limit_recommendation(clock), ltp=Decimal("2500.00")
    ).render()
    assert "level 2450.00 (limit-at-level)" in text
    assert "current price 2500.00" in text
    # …and the gap, signed from the owner's point of view: the level is BELOW the live price.
    assert "level 2450.00 is -2.00% away" in text


def test_recommendation_level_and_ltp_are_machine_readable(clock):
    """R8: the owner reads prose, the audit log reads ``data`` — both prices must survive there."""
    message = catalog.recommendation_message(
        _limit_recommendation(clock), ltp=Decimal("2500.00")
    )
    assert message.data["level"] == "2450.00"
    assert message.data["ltp"] == "2500.00"
    assert message.data["entry_zone"] == ["2450.00", "2450.00"]


@pytest.mark.parametrize(
    ("ltp", "expected"),
    [
        (Decimal("2450.00"), "level 2450.00 is +0.00% away"),   # boundary: quote ON the level
        (Decimal("2400.00"), "level 2450.00 is +2.08% away"),   # level above the market
        (Decimal("2500.00"), "level 2450.00 is -2.00% away"),   # market has run past the level
    ],
)
def test_recommendation_gap_direction(clock, ltp, expected):
    text = catalog.recommendation_message(_limit_recommendation(clock), ltp=ltp).render()
    assert expected in text


def test_recommendation_without_a_quote_never_invents_one(clock):
    """No live quote ⇒ no current-price prose at all. Fabricating one (or silently reusing the
    level) is exactly the F5 failure in a new costume."""
    message = catalog.recommendation_message(_limit_recommendation(clock))
    text = message.render()
    assert "level 2450.00 (limit-at-level)" in text
    assert "current price" not in text
    assert message.data["ltp"] is None and message.data["level"] == "2450.00"


@pytest.mark.parametrize(
    ("message", "kind", "severity", "fragments"),
    [
        (catalog.catalyst_watchlist(4, 11), MessageKind.CATALYST_WATCHLIST, "info",
         ("originating: 4", "context-only: 11")),
        (catalog.catalyst_disabled("digest stale > 18h"), MessageKind.CATALYST_DISABLED, "warning",
         ("digest stale > 18h",)),
        (catalog.mode_change("AUTO", "RECOMMEND", "risk_gate", "daily_loss_hard"),
         MessageKind.MODE_CHANGE, "warning", ("AUTO → RECOMMEND", "risk_gate", "daily_loss_hard")),
        (catalog.risk_state_change("NORMAL", "FROZEN", "rejection_storm: 3 rejects/60s"),
         MessageKind.RISK_STATE_CHANGE, "warning", ("NORMAL → FROZEN", "rejection_storm")),
        (catalog.budget_tier("DG0", "DG1", Decimal("42.5")), MessageKind.BUDGET_WARNING, "warning",
         ("DG0 → DG1", "$42.5")),
        (catalog.kill_state(killed=True, reason="cumulative_floor", actor="risk_gate"),
         MessageKind.KILL, "critical", ("KILL SWITCH ENGAGED", "cumulative_floor")),
        (catalog.trade_window_changed(start="09:30", end="10:00", buffer_min=5, actor="owner"),
         MessageKind.TRADE_WINDOW_CHANGED, "info", ("09:30-10:00", "buffer 5m")),
    ],
)
def test_catalog_helpers_render_clean_owner_text(message, kind, severity, fragments):
    """Mirrors the ``test_notify_owner`` render regression: title+body prose only, no model leakage."""
    text = message.render()
    assert message.kind == kind and message.severity == severity
    for fragment in fragments:
        assert fragment in text, fragment
    for leak in ("kind=", "data={", "reply_keyboard=", "MessageKind.", "Decimal("):
        assert leak not in text, leak
    assert message.data                                        # structured payload preserved (R8)


def test_budget_tier_reuses_the_existing_budget_kind():
    """One event, one kind: a second kind for the ladder would fork the notification audit log."""
    assert catalog.budget_tier("DG1", "DG2", Decimal("60")).kind == MessageKind.BUDGET_WARNING


# --------------------------------------------------------------------------- outbound bus alerts (R8)
@pytest.fixture
def wired_bot(clock) -> tuple[TelegramBot, _SendBot]:
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    sender = _SendBot()
    bot._app = _SendApp(sender)          # started-transport stand-in; send() is otherwise a no-op
    return bot, sender


@pytest.mark.asyncio
async def test_attach_bus_publishes_every_control_plane_alert(wired_bot, bus, clock):
    bot, sender = wired_bot
    bot.attach_bus(bus)
    now = clock.now()

    await bus.apublish(TOPIC_MODE_CHANGED, ModeChanged(
        old_mode=Mode.OFF, new_mode=Mode.RECOMMEND, routing=None, actor=Actor.OWNER,
        reason="owner_request", at=now))
    await bus.apublish(TOPIC_RISK_STATE, RiskStateChanged(
        old_state=RiskState.NORMAL, new_state=RiskState.FROZEN, actor=Actor.RISK_GATE,
        reason="stale_feed: tick age 7s", at=now))
    await bus.apublish(TOPIC_KILL_STATE, KillStateChanged(
        killed=True, reason="cumulative_floor", actor=Actor.RISK_GATE, at=now))
    await bus.apublish(TOPIC_TRADE_WINDOW, TradeWindowChanged(
        start_ist="09:30", end_ist="10:00", squareoff_buffer_min=5, actor=Actor.OWNER, at=now))
    await bus.apublish(TOPIC_BUDGET_STATE, BudgetStateChanged(
        old_tier=DegradeTier.DG0, new_tier=DegradeTier.DG1, month_spend_usd=Decimal("42.5"), at=now))

    texts = [text for _chat, text in sender.sent]
    assert len(texts) == 5
    assert all(chat == OWNER_CHAT for chat, _text in sender.sent)
    assert "Mode OFF → RECOMMEND" in texts[0]
    assert "Risk state NORMAL → FROZEN" in texts[1] and "stale_feed" in texts[1]
    assert "KILL SWITCH ENGAGED" in texts[2]
    assert "Trade window 09:30-10:00" in texts[3]
    assert "Budget tier DG0 → DG1" in texts[4]
    for text in texts:                                     # rendered prose, never a raw model
        for leak in ("kind=", "data={", "MessageKind.", "old_mode="):
            assert leak not in text


@pytest.mark.asyncio
async def test_attach_bus_is_idempotent(wired_bot, bus, clock):
    """A second attach must not double-send every alert."""
    bot, sender = wired_bot
    bot.attach_bus(bus)
    bot.attach_bus(bus)
    await bus.apublish(TOPIC_KILL_STATE, KillStateChanged(
        killed=False, reason="owner_reset", actor=Actor.OWNER, at=clock.now()))
    assert len(sender.sent) == 1


@pytest.mark.asyncio
async def test_attach_bus_without_a_bus_is_a_noop(bot):
    bot.attach_bus()                     # no bus injected, none passed — logs and returns
    assert bot._bus_attached is False


@pytest.mark.asyncio
async def test_a_hanging_send_is_bounded_and_dropped(clock, monkeypatch):
    """2026-08-07 boot wedge: a send that neither returns nor raises held the whole boot between
    catch_up_complete and scheduler start. The send seam is now hard-bounded — a hang costs the
    caller at most the timeout, logged as telegram_send_timeout, never an unbounded wait."""
    import asyncio

    from engine.notify import telegram as tg_mod

    class _HangingBot:
        async def send_message(self, chat_id, text):
            await asyncio.sleep(3600)

    monkeypatch.setattr(tg_mod, "_SEND_TIMEOUT_S", 0.05)
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    bot._app = _SendApp(_HangingBot())

    await asyncio.wait_for(bot.send("hello"), timeout=5)       # returns promptly; no exception


@pytest.mark.asyncio
async def test_a_hanging_start_degrades_to_disabled_bot(clock, monkeypatch):
    """2026-08-07: `start()` has four network awaits and used to run UNGUARDED in the boot path —
    a hang or error now degrades to a DISABLED bot (sends drop 'not_started') instead of holding
    or crashing the boot."""
    import asyncio

    from engine.notify import telegram as tg_mod

    async def _hang(self, app):
        await asyncio.sleep(3600)

    async def _no_teardown(app):
        return None

    monkeypatch.setattr(tg_mod, "_START_TIMEOUT_S", 0.05)
    monkeypatch.setattr(TelegramBot, "_start_network", _hang)
    monkeypatch.setattr(TelegramBot, "_teardown_app", staticmethod(_no_teardown))
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)

    await asyncio.wait_for(bot.start(), timeout=5)             # returns; never raises
    assert bot._app is None                                    # disabled, half-started app not kept
    assert bot._failed_app is not None                         # ...but RETAINED for teardown (a live
    await bot.send("dropped")                                  # poller may have survived the timeout)


@pytest.mark.asyncio
async def test_a_partially_started_poller_is_torn_down_by_stop(clock, monkeypatch):
    """Review round: a start() timeout can land AFTER start_polling succeeded — the orphaned live
    poller must remain reachable, and stop() must tear it down (owner commands must not keep
    executing on a bot the engine reports as disabled)."""
    import asyncio

    from engine.notify import telegram as tg_mod

    torn_down: list = []

    async def _partial_start(self, app):
        await asyncio.sleep(3600)                              # timeout lands mid-sequence

    async def _record_teardown(app):
        torn_down.append(app)

    monkeypatch.setattr(tg_mod, "_START_TIMEOUT_S", 0.05)
    monkeypatch.setattr(TelegramBot, "_start_network", _partial_start)
    monkeypatch.setattr(TelegramBot, "_teardown_app", staticmethod(_record_teardown))
    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)

    await asyncio.wait_for(bot.start(), timeout=5)
    assert len(torn_down) == 1                                 # best-effort teardown at degrade time
    assert bot._failed_app is torn_down[0]                     # retained regardless

    await bot.stop()                                           # stop() reaches the orphan again
    assert len(torn_down) == 2 and torn_down[1] is torn_down[0]
    assert bot._failed_app is None


@pytest.mark.asyncio
async def test_teardown_app_runs_every_step_despite_failures():
    """Review round: the teardown loop must attempt shutdown even when stop() raises — each step is
    individually guarded, so a broken step never strands the later ones."""
    class _App:
        def __init__(self):
            self.calls: list[str] = []
            self.updater = None

        async def stop(self):
            self.calls.append("stop")
            raise RuntimeError("stop broke")

        async def shutdown(self):
            self.calls.append("shutdown")

    app = _App()
    await TelegramBot._teardown_app(app)
    assert app.calls == ["stop", "shutdown"]                   # shutdown ran despite stop raising


@pytest.mark.asyncio
async def test_a_send_failure_never_breaks_the_publisher(clock, bus):
    """R8: alerting must never take down the risk publisher that raised the event."""

    class _ExplodingBot:
        async def send_message(self, chat_id, text):
            raise RuntimeError("telegram.error.TimedOut")

    bot = TelegramBot("t", owner_chat_id=OWNER_CHAT, clock=clock)
    bot._app = _SendApp(_ExplodingBot())
    bot.attach_bus(bus)
    await bus.apublish(TOPIC_MODE_CHANGED, ModeChanged(
        old_mode=Mode.AUTO, new_mode=Mode.OFF, routing=Routing.PAPER, actor=Actor.RISK_GATE,
        reason="cumulative_floor", at=clock.now()))    # must not raise
