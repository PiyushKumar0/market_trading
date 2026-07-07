"""Owner I/O security + catalog shapes (§2.4/R10/§8): the Telegram owner-ID lock drops non-owner
updates, and the process-lifecycle catalog helpers produce the right typed messages."""

from __future__ import annotations

import pytest

from engine.notify.catalog import MessageKind, engine_started, engine_stopped, startup_report
from engine.notify.telegram import (
    _COMMANDS,
    _PHASE0_STUB,
    TelegramBot,
    _help_text,
    _menu_commands,
    _owner_only,
)


# --------------------------------------------------------------------------- rendering (regression)
def test_catalog_message_renders_clean_owner_text() -> None:
    """Regression: ``send`` must render a title+body message, never leak the raw Pydantic model
    (``kind=<…> title=… data={…} reply_keyboard=None``) to the owner — the old ``str(model)`` fallback."""
    msg = engine_started(mode="OFF", version="0.0.0")
    rendered = msg.render()
    assert "Engine started" in rendered and msg.body in rendered
    for leak in ("kind=", "data={", "reply_keyboard=", "MessageKind."):
        assert leak not in rendered
    # The bot's transport render path must pick up render(), not fall back to str(model).
    assert TelegramBot._render(msg) == rendered


def test_startup_report_helper_shapes() -> None:
    frozen = ["protected_store:limits.yaml", "protected_store:envelope.yaml"]
    loud = startup_report(
        mode="OFF", risk_state="FROZEN", killed=True, needs_login=False, integrity_ok=False,
        crash_recovered=False, prior_state="STOPPED", frozen_reasons=frozen, deferred_steps=[],
    )
    assert loud.kind == MessageKind.STARTUP_REPORT
    assert loud.severity == "critical"                       # killed / integrity-fail ⇒ loud
    assert loud.data["frozen_reasons"] == frozen
    assert "{" not in loud.body and "model_dump" not in loud.body   # no raw dict dumped into prose

    clean = startup_report(
        mode="AUTO", risk_state="NORMAL", killed=False, needs_login=False, integrity_ok=True,
        crash_recovered=False, prior_state="STOPPED", frozen_reasons=[], deferred_steps=[],
    )
    assert clean.severity == "info" and "frozen: none" in clean.render()


# --------------------------------------------------------------------------- catalog helpers
def test_engine_started_helper_shapes() -> None:
    ok = engine_started(mode="OFF", version="1.2.3", crash_recovered=False)
    assert ok.kind == MessageKind.ENGINE_STARTED
    assert ok.data == {"mode": "OFF", "version": "1.2.3", "crash_recovered": False}
    assert ok.severity == "info"

    crashed = engine_started(mode="RECOMMEND", version="1.2.3", crash_recovered=True)
    assert crashed.severity == "warning"
    assert "crash" in crashed.title.lower() and crashed.data["crash_recovered"] is True


def test_engine_stopped_helper_shapes() -> None:
    msg = engine_stopped(reason="service-stop", open_positions=2, next_start="tomorrow 09:00")
    assert msg.kind == MessageKind.ENGINE_STOPPED
    assert msg.data == {"reason": "service-stop", "open_positions": 2, "next_start": "tomorrow 09:00"}


# --------------------------------------------------------------------------- /help + command catalog
class _CapturingMessage:
    """A fake Telegram message that records the text a handler replies with (see telegram._reply)."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.sent.append(text)


class _MsgUpdate:
    """Minimal Update whose effective_message captures replies."""

    def __init__(self, msg: _CapturingMessage) -> None:
        self.effective_message = msg
        self.message = msg


class _FakeBot:
    """Stand-in for app.bot: records set_my_commands calls; optionally raises to exercise the R8 guard."""

    def __init__(self, *, raises: bool = False) -> None:
        self.calls: list[tuple] = []
        self._raises = raises

    async def set_my_commands(self, commands, scope=None) -> None:   # noqa: ANN001 - test double
        self.calls.append((commands, scope))
        if self._raises:
            raise RuntimeError("simulated telegram.error.TimedOut")


class _FakeApp:
    def __init__(self, bot: _FakeBot) -> None:
        self.bot = bot


class _FakeContext:
    """Minimal ContextTypes stand-in exposing .args (what command handlers read)."""

    def __init__(self, args: list[str] | None = None) -> None:
        self.args = args or []


def test_command_names_are_unique_and_telegram_valid() -> None:
    names = [c.name for c in _COMMANDS]
    assert len(names) == len(set(names))                       # no duplicate registration
    for name in names:
        # Telegram requires command names ^[a-z0-9_]{1,32}$; a bad name would break setMyCommands.
        assert 1 <= len(name) <= 32 and name == name.lower()
        assert all(ch.isalnum() or ch == "_" for ch in name)


def test_live_command_specs_have_handlers(clock) -> None:
    """Every ``live`` spec has a bound handler and vice-versa — guards the two lists against drift."""
    bot = TelegramBot("dummy-token", owner_chat_id=1, clock=clock)
    live_specs = {c.name for c in _COMMANDS if c.live}
    assert set(bot._live_handlers()) == live_specs
    assert "help" in live_specs                                # /help itself is wired


def test_register_handlers_registers_every_command(clock) -> None:
    """Registration is spec-driven and must not raise (no live spec missing a handler)."""
    bot = TelegramBot("dummy-token", owner_chat_id=1, clock=clock)
    registered: list[str] = []

    class _RecordingApp:
        def add_handler(self, handler) -> None:               # noqa: ANN001 - test double
            registered.extend(handler.commands)

    bot._register_handlers(_RecordingApp())
    assert sorted(registered) == sorted(c.name for c in _COMMANDS)


def test_help_text_lists_every_command() -> None:
    """Exact-line membership (not substring): guards the /close ⊂ /closed prefix collision."""
    text = _help_text()
    lines = text.splitlines()
    for c in _COMMANDS:
        if c.live:
            assert c.usage in lines                            # usage on its own line
            assert f"    {c.summary}" in lines                 # indented description line
        else:
            assert f"/{c.name} — {c.summary}" in lines         # exact stub line, no prefix collision
    assert "Not yet available (Phase 2+):" in lines            # stubs clearly separated
    assert len(text) <= 4096                                   # fits a single Telegram message


def test_menu_advertises_only_live_commands() -> None:
    menu = _menu_commands()
    assert {bc.command for bc in menu} == {c.name for c in _COMMANDS if c.live}
    assert all(bc.description for bc in menu)                   # non-empty descriptions
    # Assert the SOURCE summary fits Telegram's 256-char cap so the [:256] slice never silently
    # truncates mid-word — asserting on the already-sliced bc.description would be a tautology.
    for c in _COMMANDS:
        if c.live:
            assert len(c.summary) <= 256


@pytest.mark.asyncio
async def test_cmd_help_replies_with_full_catalog(clock) -> None:
    bot = TelegramBot("dummy-token", owner_chat_id=1, clock=clock)
    msg = _CapturingMessage()
    await bot._cmd_help(_MsgUpdate(msg), None)
    assert msg.sent == [_help_text()]


@pytest.mark.asyncio
async def test_stub_handler_replies_not_implemented(clock) -> None:
    """Phase-2 stubs must reply the not-implemented text — the owner-visible contract of _stub."""
    bot = TelegramBot("dummy-token", owner_chat_id=1, clock=clock)
    msg = _CapturingMessage()
    await bot._stub("positions")(_MsgUpdate(msg), _FakeContext())
    assert msg.sent == [f"/positions: {_PHASE0_STUB}."]


@pytest.mark.asyncio
async def test_publish_command_menu_scopes_to_owner_chat(clock) -> None:
    """The '/' menu is published scoped to the owner chat (R10), advertising only live commands."""
    from telegram import BotCommandScopeChat

    bot = TelegramBot("dummy-token", owner_chat_id=-5586756347, clock=clock)
    fake_bot = _FakeBot()
    await bot._publish_command_menu(_FakeApp(fake_bot))

    assert len(fake_bot.calls) == 1
    commands, scope = fake_bot.calls[0]
    assert {bc.command for bc in commands} == {c.name for c in _COMMANDS if c.live}
    assert isinstance(scope, BotCommandScopeChat) and scope.chat_id == -5586756347


@pytest.mark.asyncio
async def test_publish_command_menu_never_raises_on_api_error(clock) -> None:
    """R8: a setMyCommands failure at startup must be swallowed, never crash the control plane."""
    bot = TelegramBot("dummy-token", owner_chat_id=1, clock=clock)
    await bot._publish_command_menu(_FakeApp(_FakeBot(raises=True)))    # must not raise


# --------------------------------------------------------------------------- owner-ID lock
class _FakeChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class _FakeUpdate:
    def __init__(self, chat_id: int) -> None:
        self.effective_chat = _FakeChat(chat_id)
        self.message = None


@pytest.mark.asyncio
async def test_owner_only_lock_allows_owner_blocks_others(clock) -> None:
    bot = TelegramBot("dummy-token", owner_chat_id=42, clock=clock)
    ran: list[int] = []

    async def handler(update, context) -> None:
        ran.append(update.effective_chat.id)

    guarded = _owner_only(bot, handler)
    await guarded(_FakeUpdate(42), None)     # owner ⇒ handler runs
    await guarded(_FakeUpdate(999), None)    # foreign chat ⇒ silently dropped
    await guarded(_FakeUpdate(-1), None)     # another foreign chat ⇒ dropped
    assert ran == [42]
