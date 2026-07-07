"""Startup self-test + lifecycle recovery (§2.6/§3.2.12, R4/R6/R10).

Hermetic: a temp config dir for the protected store (never touches the real repo files) and a fake
secrets store (never touches the Windows Credential Manager). Clock-skew check disabled (no network).
"""

from __future__ import annotations

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.enums import Actor, Mode, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.core.types import OwnerConfirmation
from engine.ops.lifecycle import CatchUpRunner, SessionLifecycle
from engine.ops.selftest import SelfTest
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True)


class FakeSettings:
    """Minimal settings shim for the lifecycle (only the fields it reads)."""

    class _TW:
        from datetime import time as _t
        start_ist = _t(10, 0)
        end_ist = _t(10, 30)
        squareoff_buffer_min = 5

    class _Clock:
        max_skew_s = 2

    class _LC:
        heartbeat_write_s = 20
        down_stale_s = 90
        watchdog_poll_s = 60
        notify_planned_stop = True
        notify_started = True

    env = "dev"
    trade_window = _TW()
    clock = _Clock()
    lifecycle = _LC()


class FakeSecrets:
    def __init__(self, present=()):
        self._present = set(present)

    def has(self, k):
        return k in self._present

    def get_optional(self, k):
        return "x" if k in self._present else None

    def missing_required(self):
        return [k for k in REQUIRED_AT_STARTUP if k not in self._present]


@pytest.fixture
def temp_config(tmp_path):
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    return cfg


def _build(conn, clock, temp_config, *, secrets_present=REQUIRED_AT_STARTUP, notify=None):
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    secrets = FakeSecrets(secrets_present)
    store = ProtectedStore(temp_config, conn, clock)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=secrets,
                  protected_store=store, kill_switch=kill, mode_manager=mode, session_manager=None)
    catch_up = CatchUpRunner(conn, clock, calendar)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st, catch_up=catch_up,
        notify=notify, build_version="test-0",
    )
    return mode, kill, store, lifecycle


@pytest.mark.asyncio
async def test_startup_frozen_when_protected_store_unregistered(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, kill, store, lifecycle = _build(conn, clock, temp_config)
    report = await lifecycle.startup(check_skew=False)
    assert report.integrity_ok is False
    assert mode.risk_state() == RiskState.FROZEN          # flat book ⇒ FROZEN, not kill (§2.4)
    assert mode.mode() == Mode.OFF
    assert "trade_window_seeded" in report.notes


@pytest.mark.asyncio
async def test_startup_clean_after_seed(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, kill, store, lifecycle = _build(conn, clock, temp_config)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    report = await lifecycle.startup(check_skew=False)
    assert report.integrity_ok is True
    assert report.frozen_reasons == []
    assert mode.risk_state() == RiskState.NORMAL


@pytest.mark.asyncio
async def test_startup_reports_kill_state(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, kill, store, lifecycle = _build(conn, clock, temp_config)
    await kill.trigger("pre-existing kill", actor=Actor.OWNER, flatten=False)
    report = await lifecycle.startup(check_skew=False)
    assert report.killed is True


@pytest.mark.asyncio
async def test_startup_frozen_when_secret_missing(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, kill, store, lifecycle = _build(conn, clock, temp_config, secrets_present=())  # nothing seeded
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    report = await lifecycle.startup(check_skew=False)
    assert "secrets_present" in report.frozen_reasons
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_lifecycle_signals_and_state_writes(conn, clock, temp_config, monkeypatch):
    """ENGINE_STARTED on boot, ENGINE_STOPPED on clean stop, and engine_lifecycle state transitions
    RUNNING → STOPPED with last_clean_stop_at set (§2.2/§8.1 Phase-0 lifecycle deliverable)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    mode, kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)

    report = await lifecycle.startup(check_skew=False)
    assert report.crash_recovered is False                    # seeded STOPPED ⇒ clean prior
    kinds = [str(m.kind) for m in sent]
    assert "engine_started" in kinds
    row = conn.execute("SELECT state, pid, started_at FROM engine_lifecycle WHERE id=1").fetchone()
    assert row["state"] == "RUNNING" and row["pid"] and row["started_at"]

    await lifecycle.shutdown(reason="service-stop")
    assert any(str(m.kind) == "engine_stopped" for m in sent)
    row = conn.execute("SELECT state, last_clean_stop_at FROM engine_lifecycle WHERE id=1").fetchone()
    assert row["state"] == "STOPPED" and row["last_clean_stop_at"]


@pytest.mark.asyncio
async def test_lifecycle_detects_unclean_prior_exit(conn, clock, temp_config, monkeypatch):
    """A prior run left in RUNNING (crash, no clean stop) ⇒ next startup reports crash_recovered (§2.6 step 0)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(msg):
        sent.append(msg)

    # Simulate a crashed prior run: state stuck at RUNNING (never reached the clean STOPPED commit).
    conn.execute("UPDATE engine_lifecycle SET state='RUNNING' WHERE id=1")
    conn.commit()

    mode, kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)

    report = await lifecycle.startup(check_skew=False)
    assert report.crash_recovered is True
    assert report.prior_state == "RUNNING"
    started = next(m for m in sent if str(m.kind) == "engine_started")
    assert started.data["crash_recovered"] is True


@pytest.mark.asyncio
async def test_selftest_frozen_on_stray_anthropic_key(conn, clock, temp_config, monkeypatch):
    """D2 trap (§3.2.12): a stray ANTHROPIC_API_KEY (overflow disabled in the repo agents.yaml) FROZENs
    entries — it silently outranks the Max OAuth token. This exercises the enforcing branch."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-should-not-be-here")
    mode, kill, store, lifecycle = _build(conn, clock, temp_config)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    report = await lifecycle.startup(check_skew=False)
    assert "anthropic_key_absent" in report.frozen_reasons
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_selftest_surfaces_sdk_smoke_skip(conn, clock, temp_config, monkeypatch):
    """The one-cheap-Haiku-call self-test (D11) is Phase-1-deferred but surfaced as a visible SKIP so the
    gate sequence is honest (§3.2.12) — not silently omitted."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)
    report = await lifecycle._selftest.run(check_skew=False)
    sdk = next((c for c in report.checks if c.name == "sdk_smoke"), None)
    assert sdk is not None and sdk.status.value == "SKIP"


def test_catch_up_watermarks(conn, clock):
    from datetime import date

    cal = NSECalendar(config_dir() / "calendar", clock, strict=False)
    runner = CatchUpRunner(conn, clock, cal)
    d = date(2026, 6, 17)
    assert runner.was_run("bhavcopy", d) is False
    runner.record_run("bhavcopy", d, status="success")
    assert runner.was_run("bhavcopy", d) is True
