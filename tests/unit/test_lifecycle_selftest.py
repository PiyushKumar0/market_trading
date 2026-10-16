"""Startup self-test + lifecycle recovery (§2.6/§3.2.12, R4/R6/R10).

Hermetic: a temp config dir for the protected store (never touches the real repo files) and a fake
secrets store (never touches the Windows Credential Manager). Clock-skew check disabled (no network).
"""

from __future__ import annotations

import json

import pytest

from engine.core.calendar import NSECalendar
from engine.core.config import config_dir
from engine.core.enums import Actor, Mode, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.core.types import OwnerConfirmation
from engine.ops.lifecycle import CatchUpResult, CatchUpRunner, SessionLifecycle
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
async def test_selftest_risk_counters_and_ladder_skip_when_unwired(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)
    report = await lifecycle.startup(check_skew=False)
    del report
    # Direct selftest run: the two §2.6 step-2 checks surface as SKIP, never silently pass.
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=ProtectedStore(temp_config, conn, clock),
                  kill_switch=KillSwitch(conn, clock), mode_manager=ModeManager(conn, clock, None, calendar))
    rep = await st.run(check_skew=False, include_freshness=False)
    by_name = {c.name: c for c in rep.checks}
    assert by_name["risk_counters_rebuild"].status.value == "SKIP"
    assert by_name["equity_halt_ladder"].status.value == "SKIP"


@pytest.mark.asyncio
async def test_selftest_equity_ladder_applies_breached_rung_on_startup(conn, clock, temp_config, monkeypatch):
    """§2.6 step 2: an offline-realized loss below the −10% rung trips CLOSE_ONLY on startup —
    behaviorally identical to a live trip (state applied, not merely reported)."""
    from decimal import Decimal

    from engine.risk.causes import RiskStateLatch
    from engine.risk.exposure import ExposureTracker

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    # O16 2026-09-07: base 40000 / caps 6-2-4
    # A closed platform position realizing −₹4,100 net ⇒ equity 35,900 ≤ 36,000 (−10% of 40k).
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin,"
        " opened_at, closed_at, realized_pnl, costs) VALUES ('p1','AAA','BUY','MIS',10,'100','CLOSED',"
        " 'platform', ?, ?, '-4100', '0')",
        (f"{clock.today().isoformat()}T09:30:00+05:30", f"{clock.today().isoformat()}T10:00:00+05:30"),
    )
    exposure = ExposureTracker(conn, clock, Decimal("40000"))

    class _RealLimits:
        """LimitsEngine seam: parse the REAL repo limits.yaml (floor pcts) without the hash store."""

        def load(self):
            from engine.core.config import load_yaml
            from engine.risk.limits import LimitTable
            return LimitTable.model_validate(load_yaml(config_dir() / "limits.yaml"))

    store = ProtectedStore(temp_config, conn, clock)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=store, kill_switch=kill, mode_manager=mode,
                  exposure=exposure, limits_engine=_RealLimits(), latch=latch)
    rep = await st.run(check_skew=False, include_freshness=False)
    by_name = {c.name: c for c in rep.checks}
    assert by_name["risk_counters_rebuild"].status.value == "PASS"
    assert "day_mtm=-4100" in by_name["risk_counters_rebuild"].detail
    assert by_name["equity_halt_ladder"].status.value == "WARN"
    assert "equity_floor_rung" in by_name["equity_halt_ladder"].detail
    assert mode.risk_state() == RiskState.CLOSE_ONLY       # applied, not merely reported
    assert any(c == "floor_equity_floor_rung" for c, _s, _d in latch.active_causes())


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
    """An UNWIRED D11 smoke seam (no harness) surfaces as a visible SKIP so the gate sequence stays
    honest (§3.2.12), never silently omitted; chaos case 13 asserts the non-trading-day SKIP and the
    trading-day PASS."""
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


@pytest.mark.asyncio
async def test_warmup_lift_preserves_standing_owner_pause(conn, clock, temp_config, monkeypatch):
    """2026-07-28 review F0: the post-login warm-up lift must clear ONLY its own causes — a standing
    /pause_entries (owner_pause, FROZEN) survives the lift instead of being erased to NORMAL."""
    from engine.risk.causes import CAUSE_OWNER_PAUSE, RiskStateLatch

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    store = ProtectedStore(temp_config, conn, clock)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=store, kill_switch=kill, mode_manager=mode)

    class ReadyGate:
        async def status(self):
            from engine.ops.warmup import WarmupStatus
            return WarmupStatus(ready=True, blockers=[])

    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st,
        catch_up=CatchUpRunner(conn, clock, calendar), warmup_gate=ReadyGate(), latch=latch,
        build_version="test-0",
    )
    await lifecycle.startup(check_skew=False)
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries", Actor.OWNER)

    reapply = await lifecycle.reapply_warmup_gate()

    assert reapply.lifted is False
    assert mode.risk_state() == RiskState.FROZEN            # the pause survives the lift
    assert any(c == CAUSE_OWNER_PAUSE for c, _s, _d in latch.active_causes())


def _build_with_latch(conn, clock, temp_config, *, catch_up=None):
    """Latch-wired lifecycle with clean protected stores (§2.6 step-5 tests)."""
    from engine.risk.causes import RiskStateLatch

    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    store = ProtectedStore(temp_config, conn, clock)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=store, kill_switch=kill, mode_manager=mode, latch=latch)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=FakeSettings(),
        mode_manager=mode, kill_switch=kill, self_test=st,
        catch_up=catch_up or CatchUpRunner(conn, clock, calendar), latch=latch,
        build_version="test-0",
    )
    return mode, kill, latch, lifecycle


@pytest.mark.asyncio
async def test_startup_clears_stale_catchup_safety_latch(conn, clock, temp_config, monkeypatch):
    """§2.6 step-5 inverse: a CLEAN catch-up pass clears a prior boot's catchup_safety_jobs latch.
    2026-09-01: the cause had a set site (step-5 freeze) but no clear site anywhere, so one transient
    08-31 boot failure kept entries FROZEN for two full sessions after the catch-up had recovered."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    # The stale latch a prior boot left behind (set path: lifecycle step 5 on frozen_reasons).
    await latch.set_cause("catchup_safety_jobs", RiskState.FROZEN,
                          "data_freshness:instruments", Actor.RISK_GATE)
    assert mode.risk_state() == RiskState.FROZEN

    report = await lifecycle.startup(check_skew=False)

    assert report.frozen_reasons == []                       # this boot's pass is clean
    assert all(c != "catchup_safety_jobs" for c, _s, _d in latch.active_causes())
    assert mode.risk_state() == RiskState.NORMAL


@pytest.mark.asyncio
async def test_startup_catchup_clear_respects_other_causes(conn, clock, temp_config, monkeypatch):
    """The step-5 clear is cause-scoped: a standing owner_pause survives it (same clear-only-what-
    was-re-verified rule as the warm-up lift, 2026-07-28 review F0)."""
    from engine.risk.causes import CAUSE_OWNER_PAUSE

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    await latch.set_cause("catchup_safety_jobs", RiskState.FROZEN,
                          "data_freshness:instruments", Actor.RISK_GATE)
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries", Actor.OWNER)

    await lifecycle.startup(check_skew=False)

    assert all(c != "catchup_safety_jobs" for c, _s, _d in latch.active_causes())
    assert any(c == CAUSE_OWNER_PAUSE for c, _s, _d in latch.active_causes())
    assert mode.risk_state() == RiskState.FROZEN             # the pause still holds the state


@pytest.mark.asyncio
async def test_startup_freezes_on_catchup_safety_failure(conn, clock, temp_config, monkeypatch):
    """Pin the existing freeze side (§2.6 step 5): safety-critical catch-up failures still latch
    catchup_safety_jobs — the new clear branch must never weaken the fail-closed path."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    class FailingCatchUp:
        async def catch_up(self, *, off_since=None):
            return CatchUpResult(jobs_failed=["instruments"],
                                 frozen_reasons=["data_freshness:instruments"])

    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config,
                                                      catch_up=FailingCatchUp())
    report = await lifecycle.startup(check_skew=False)

    assert "data_freshness:instruments" in report.frozen_reasons
    assert any(c == "catchup_safety_jobs" for c, _s, _d in latch.active_causes())
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_startup_clears_integrity_latch_once_stores_verify(conn, clock, temp_config, monkeypatch):
    """§2.4 inverse: a boot whose protected stores all verify clears a prior boot's
    protected_store_integrity latch (2026-10-11: the cause had a set site and no clear site, so a
    botched owner reseed would have kept entries FROZEN across restarts until a hand DB edit)."""
    from engine.risk.causes import CAUSE_OWNER_PAUSE

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    await latch.set_cause("protected_store_integrity", RiskState.FROZEN,
                          "protected_store_integrity", Actor.RISK_GATE)
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner /pause_entries", Actor.OWNER)

    report = await lifecycle.startup(check_skew=False)

    assert report.integrity_ok is True
    assert all(c != "protected_store_integrity" for c, _s, _d in latch.active_causes())
    assert any(c == CAUSE_OWNER_PAUSE for c, _s, _d in latch.active_causes())   # cause-scoped


@pytest.mark.asyncio
async def test_startup_still_freezes_on_integrity_failure(conn, clock, temp_config, monkeypatch):
    """Pin the fail-closed side: an unsigned edit on a flat book still latches the cause."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, latch, lifecycle = _build_with_latch(conn, clock, temp_config)
    path = temp_config / "envelope.yaml"
    path.write_text(path.read_text(encoding="utf-8") + "\n# unsigned edit\n", encoding="utf-8")

    report = await lifecycle.startup(check_skew=False)

    assert report.integrity_ok is False
    assert any(c == "protected_store_integrity" for c, _s, _d in latch.active_causes())
    assert mode.risk_state() == RiskState.FROZEN


# --------------------------------------------------------------------------- claude_login (D2/D11)
SENTINEL = "sk-ant-SENTINEL-TOKEN-must-never-surface"


def _creds(tmp_path, clock, *, days: float | None = 20, refresh=SENTINEL, access=SENTINEL,
           sub="team", raw: str | None = None):
    """A .credentials.json shaped like CLI 2.1.288's (claudeAiOauth, epoch-ms expiries)."""
    from datetime import timedelta

    path = tmp_path / ".credentials.json"
    if raw is not None:
        path.write_text(raw, encoding="utf-8")
        return path
    oauth = {"accessToken": access, "refreshToken": refresh, "subscriptionType": sub,
             "expiresAt": int((clock.now() + timedelta(hours=8)).timestamp() * 1000)}
    if days is not None:
        oauth["refreshTokenExpiresAt"] = int((clock.now() + timedelta(days=days)).timestamp() * 1000)
    path.write_text(json.dumps({"claudeAiOauth": oauth}), encoding="utf-8")
    return path


def test_claude_login_pass_when_refresh_token_live(tmp_path, clock):
    from engine.ops.selftest import Implies, check_claude_login

    check = check_claude_login(clock.now(), _creds(tmp_path, clock, days=20))

    assert check.name == "claude_login" and check.status.value == "PASS"
    assert check.implies == Implies.NONE
    assert "20.0 days" in check.detail and "subscription team" in check.detail and "IST" in check.detail


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"days": 3}, "expires in 3.0 days"),
        ({"days": 7}, "expires in 7.0 days"),                       # the boundary is still a WARN
        ({"days": -1}, "EXPIRED"),
        ({"days": None}, "expiry not recorded"),
        ({"refresh": "", "access": ""}, "not logged in"),           # the CLI's invalid_grant clear
        ({"refresh": SENTINEL, "access": ""}, "not logged in"),
        ({"raw": "{not json " + SENTINEL}, "unreadable (JSONDecodeError)"),
        ({"raw": json.dumps({"other": 1})}, "no claudeAiOauth"),
        ({"raw": json.dumps([SENTINEL])}, "no claudeAiOauth"),
    ],
)
def test_claude_login_warns_never_freezes(tmp_path, clock, kwargs, needle):
    from engine.ops.selftest import Implies, check_claude_login

    check = check_claude_login(clock.now(), _creds(tmp_path, clock, **kwargs))

    assert check.status.value == "WARN" and check.implies == Implies.NONE
    assert needle in check.detail
    assert "auth login --claudeai" in check.detail                 # the detail names the fix
    assert "SENTINEL" not in check.detail


@pytest.mark.asyncio
async def test_claude_login_missing_file_warns_and_run_completes(
    tmp_path, conn, clock, temp_config, monkeypatch
):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=ProtectedStore(temp_config, conn, clock),
                  kill_switch=KillSwitch(conn, clock), mode_manager=ModeManager(conn, clock, None, calendar),
                  claude_credentials=tmp_path / "absent" / ".credentials.json")

    report = await st.run(check_skew=False, include_freshness=False)

    login = next(c for c in report.checks if c.name == "claude_login")
    assert login.status.value == "WARN" and "no stored claude.ai login" in login.detail
    assert "claude_login" not in report.frozen_reasons


@pytest.mark.asyncio
async def test_claude_login_unwired_is_skip(conn, clock, temp_config, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)
    report = await lifecycle._selftest.run(check_skew=False)
    login = next(c for c in report.checks if c.name == "claude_login")
    assert login.status.value == "SKIP"                              # the hermetic tier never reads ~/.claude


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [{"days": 20}, {"days": 2}, {"refresh": "", "access": SENTINEL},
                                    {"raw": '{"claudeAiOauth": {"accessToken": "' + SENTINEL}])
async def test_claude_login_never_leaks_token(tmp_path, conn, clock, temp_config, monkeypatch, caplog,
                                              capsys, kwargs):
    """The file holds live OAuth tokens: no detail, alert or log line may carry them."""
    import logging

    from engine.ops.selftest import alert_claude_login

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    caplog.set_level(logging.DEBUG)
    path = _creds(tmp_path, clock, **kwargs)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    st = SelfTest(conn=conn, clock=clock, settings=FakeSettings(), secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                  protected_store=ProtectedStore(temp_config, conn, clock),
                  kill_switch=KillSwitch(conn, clock), mode_manager=ModeManager(conn, clock, None, calendar),
                  claude_credentials=path)
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    report = await st.run(check_skew=False, include_freshness=False)
    await alert_claude_login(clock.now(), path, alert)

    def leaks(text: str) -> bool:   # a fragment counts: any slice of a token is a leak
        return "SENTINEL" in text or "sk-ant-" in text

    assert any(c.name == "claude_login" for c in report.checks)
    assert not any(leaks(c.detail) for c in report.checks)
    assert not any(leaks(m) for _s, m in alerts)
    assert not leaks(caplog.text)
    # structlog kwargs land as record attributes: the detail really is in what is scanned here.
    assert any(getattr(r, "detail", None) for r in caplog.records if r.getMessage() == "claude_login_check")
    assert not any(leaks(str(r.__dict__)) for r in caplog.records)
    captured = capsys.readouterr()
    assert not leaks(captured.out) and not leaks(captured.err)


@pytest.mark.asyncio
async def test_daily_login_job_alerts_once_on_warn_and_never_on_pass(tmp_path, clock):
    from engine.ops.selftest import alert_claude_login

    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    ok = await alert_claude_login(clock.now(), _creds(tmp_path, clock, days=20), alert)
    assert ok.status.value == "PASS" and alerts == []

    near = await alert_claude_login(clock.now(), _creds(tmp_path, clock, days=2), alert)
    assert near.status.value == "WARN"
    assert len(alerts) == 1 and alerts[0][0] == "warning"
    assert "expires in 2.0 days" in alerts[0][1] and "auth login" in alerts[0][1]


@pytest.mark.asyncio
async def test_login_warnings_carry_the_resolved_cli_command(tmp_path, clock):
    """The composition root passes AgentHarness.relogin_hint(): the pinned CLI's real path, the same
    command the auth_failed alert prints, instead of a placeholder the owner has to look up."""
    from engine.ops.selftest import alert_claude_login, check_claude_login

    hint = "re-login the engine's Claude CLI as the service user: & \"C:/x/claude.exe\" auth login --claudeai"
    alerts: list[tuple[str, str]] = []

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    await alert_claude_login(clock.now(), _creds(tmp_path, clock, days=2), alert, hint=hint)
    assert alerts[0][1].endswith(hint)
    assert check_claude_login(clock.now(), tmp_path / "absent.json", hint=hint).detail.endswith(hint)
    assert "<agents.yaml" in check_claude_login(clock.now(), tmp_path / "absent.json").detail


def test_claude_credentials_path_follows_the_cli(tmp_path, monkeypatch):
    from pathlib import Path

    from engine.ops.selftest import claude_credentials_path

    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))
    assert claude_credentials_path() == tmp_path / "cfg" / ".credentials.json"
    monkeypatch.delenv("CLAUDE_CONFIG_DIR")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    assert claude_credentials_path() == tmp_path / "home" / ".claude" / ".credentials.json"
