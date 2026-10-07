"""D8 live-impossibility pins (plan Q3.1 (b), (d), (f)): AST allow-lists, by file, over every
non-test Python root; the Telegram /mode AUTO refusal; the boot downgrade's owner alert."""

from __future__ import annotations

import ast
from types import SimpleNamespace

import pytest

from engine.core.config import repo_root
from engine.notify.telegram import TelegramBot
from engine.risk.mode import ModeManager

_ROOTS = ("src/engine", "scripts", "ticker")
_MUTATING_KITE_METHODS = frozenset({
    "place_order", "modify_order", "cancel_order", "exit_order", "place_gtt", "modify_gtt", "delete_gtt",
    "convert_position", "place_mf_order", "cancel_mf_order", "place_mf_sip", "modify_mf_sip", "cancel_mf_sip",
})


def _trees() -> dict[str, ast.Module]:
    root = repo_root()
    return {
        path.relative_to(root).as_posix(): ast.parse(path.read_text(encoding="utf-8-sig"))
        for base in _ROOTS
        for path in (root / base).rglob("*.py")
    }


_TREES = _trees()


def _callee(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    return func.id if isinstance(func, ast.Name) else None


def _files_calling(names: frozenset[str]) -> set[str]:
    return {
        rel for rel, tree in _TREES.items()
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _callee(node) in names
    }


def test_mutating_kite_methods_are_called_only_in_the_broker_facade_paper_and_oms() -> None:
    files = _files_calling(_MUTATING_KITE_METHODS)
    assert "src/engine/broker/kite_client.py" in files
    allowed = ("src/engine/broker/kite_client.py", "src/engine/paper/", "src/engine/oms/")
    assert sorted(f for f in files if not f.startswith(allowed)) == []


@pytest.mark.parametrize(
    ("callee", "sites"),
    [
        ("KiteConnect", {"src/engine/broker/session.py", "scripts/a11_check.py", "scripts/q15_candle_latency.py"}),
        ("kite_connect", {"src/engine/ops/main.py", "scripts/backfill.py"}),
    ],
)
def test_kite_connect_construction_sites_are_pinned(callee: str, sites: set[str]) -> None:
    assert _files_calling(frozenset({callee})) == sites


def test_orders_enabled_is_never_passed_outside_tests() -> None:
    offenders = sorted(
        rel for rel, tree in _TREES.items()
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword) and node.arg == "orders_enabled"
        and not (isinstance(node.value, ast.Constant) and node.value.value is False)
    )
    assert offenders == []


class _Message:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.sent.append(text)


@pytest.mark.asyncio
@pytest.mark.parametrize("paper_only", [True, False])
async def test_telegram_mode_auto_is_refused_before_any_challenge(conn, clock, paper_only: bool) -> None:
    bot = TelegramBot("t", owner_chat_id=1, clock=clock, mode_manager=ModeManager(conn, clock, paper_only=paper_only))
    message = _Message()

    await bot._cmd_mode(SimpleNamespace(effective_message=message), SimpleNamespace(args=["AUTO"]))

    if paper_only:
        assert bot._pending is None
        assert message.sent == ["AUTO and live routing are refused while feature_flags.paper_only is set (D8)"]
    else:
        assert bot._pending.action == "mode->AUTO"


@pytest.mark.asyncio
async def test_boot_downgrade_journals_a_mode_change_alert_before_telegram_starts(conn, clock, bus) -> None:
    """main.py order: the bot is built and subscribed, not started, when ``enforce_paper_only`` runs."""
    conn.execute("UPDATE mode_state SET mode='AUTO', routing='live' WHERE id=1")
    mode = ModeManager(conn, clock, bus)
    TelegramBot("t", owner_chat_id=1, clock=clock, mode_manager=mode, conn=conn).attach_bus(bus)

    await mode.enforce_paper_only()

    rows = conn.execute("SELECT kind, status FROM notifications").fetchall()
    assert [tuple(r) for r in rows] == [("mode_change", "pending")]
