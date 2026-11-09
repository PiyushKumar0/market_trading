"""Chaos case 14 — Clock skew beyond 2 s.

Failure injected: the engine boots while the host clock is 3.5 s off NTP (or NTP is unreachable). Must
hold: entries FROZEN until resync, and the owner is alerted. The skew limit is 2 s; exactly 2.0 s is
within it.

Composed as ``engine.ops.main`` wires it: ``SessionLifecycle.startup(check_skew=True)`` → ``SelfTest``
clock-skew check → ``RiskStateLatch``; the boot-scoped skew verdict derived from the startup report feeds
the real ``GateContextBuilder`` → ``RiskGate``; the 60 s ``refresh_and_lift_warmup`` runs for real. Faked:
only the NTP server and the Telegram sink. Warm-up is READY throughout, so only the clock stands between
a boot and open entries.

Skipped, an accepted gap: skew is measured at boot only, so mid-session drift detection and in-session
resync are not built.
"""

from __future__ import annotations

from types import SimpleNamespace

import ntplib
import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.enums import Actor, Mode, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.marketdata.store import MarketStore
from engine.ops.jobs import CatchUpRunner
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.main import refresh_and_lift_warmup
from engine.ops.selftest import SelfTest
from engine.ops.warmup import WarmupStatus
from engine.risk.causes import RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager
from tests.chaos._entry_gate_rig import build_entry_gate, entry_checks
from tests.conftest import FIXED_NOW
from tests.unit.test_lifecycle_selftest import OWNER_OK, FakeSecrets

SYMBOL = "RELIANCE"
READY = WarmupStatus(ready=True, blockers=[])


class _FakeNtpServer:
    """The NTP boundary. ``offset_s`` is the signed pool answer; ``reachable=False`` times out like a blocked UDP/123."""

    def __init__(self) -> None:
        self.offset_s = 0.0
        self.reachable = True
        self.requests = 0

    def request(self, host: str, version: int = 3, timeout: int = 3):
        self.requests += 1
        if not self.reachable:
            raise ntplib.NTPException(f"No response received from {host}.")
        return SimpleNamespace(offset=self.offset_s)


class _ReadyWarmupGate:
    async def status(self) -> WarmupStatus:
        return READY


@pytest.fixture
def skew_rig(conn, tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("MT_DATA_DIR", str(tmp_path / "data"))
    settings = load_settings()
    assert settings.clock.max_skew_s == 2                      # the shipped limit

    ntp = _FakeNtpServer()
    monkeypatch.setattr(ntplib.NTPClient, "request",
                        lambda _self, host, version=3, timeout=3: ntp.request(host, version, timeout))

    clock = Clock(time_source=lambda: FIXED_NOW, ntp_servers=["ntp-a.chaos.invalid", "ntp-b.chaos.invalid"])
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    protected = ProtectedStore(cfg, conn, clock)
    protected.register_initial("limits.yaml", OWNER_OK)
    protected.register_initial("envelope.yaml", OWNER_OK)
    self_test = SelfTest(conn=conn, clock=clock, settings=settings, secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                         protected_store=protected, kill_switch=kill, mode_manager=mode, latch=latch,
                         calendar=calendar)
    notified: list = []
    alerts: list[tuple[str, str]] = []

    async def notify(msg) -> None:
        notified.append(msg)

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    def new_lifecycle() -> SessionLifecycle:
        return SessionLifecycle(
            conn=conn, clock=clock, calendar=calendar, settings=settings, mode_manager=mode,
            kill_switch=kill, self_test=self_test, catch_up=CatchUpRunner(conn, clock, calendar),
            alert=alert, notify=notify, build_version="chaos-14", warmup_gate=_ReadyWarmupGate(),
            latch=latch,
        )

    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    skew_holder = {"ok": False}                    # main.py: boot-scoped, fail-closed until the boot
    builder, gate = build_entry_gate(
        conn=conn, clock=clock, calendar=calendar, mode=mode, kill=kill, store=store,
        warmup_status_fn=lambda: READY, clock_skew_ok_fn=lambda: skew_holder["ok"],
    )
    rig = SimpleNamespace(
        ntp=ntp, clock=clock, mode=mode, latch=latch, notified=notified, alerts=alerts,
        new_lifecycle=new_lifecycle, lifecycle=None, skew_holder=skew_holder, builder=builder,
        gate=gate, warmup_holder={"status": None},
    )

    async def boot():
        rig.lifecycle = new_lifecycle()
        await mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
        report = await rig.lifecycle.startup(check_skew=True)
        rig.skew_holder["ok"] = "clock_skew" not in report.frozen_reasons   # as main.py wires it
        return report

    async def warmup_refresh():                  # the 60 s `warmup_status_refresh` job body
        await refresh_and_lift_warmup(_ReadyWarmupGate(), rig.warmup_holder, mode, rig.lifecycle,
                                      alert=alert, clock=clock)

    async def checks():
        return await entry_checks(builder, gate, clock, SYMBOL)

    rig.boot, rig.warmup_refresh, rig.checks = boot, warmup_refresh, checks
    yield rig
    store.close()


# ------------------------------------------------------------------ FROZEN entries at boot + alert
@pytest.mark.parametrize(
    ("offset_s", "reachable", "frozen"),
    [(3.5, True, True), (-3.5, True, True), (0.0, False, True), (2.0, True, False)],
    ids=["clock-behind-3.5s", "clock-ahead-3.5s", "ntp-unreachable", "boundary-2.0s-not-beyond"],
)
async def test_boot_with_skew_beyond_limit_freezes_entries_and_alerts(skew_rig, offset_s, reachable, frozen):
    rig = skew_rig
    rig.ntp.offset_s, rig.ntp.reachable = offset_s, reachable

    report = await rig.boot()
    assert rig.ntp.requests >= 1                              # the boot really asked NTP

    verdict, checks = await rig.checks()
    startup = next(m for m in rig.notified if str(m.kind) == "startup_report")
    if not frozen:                                            # exactly 2 s is "within", not "beyond"
        assert report.frozen_reasons == [] and rig.mode.risk_state() == RiskState.NORMAL
        assert checks["clock_skew"].passed and checks["mode_risk_state"].passed
        assert "frozen: none" in startup.body
        return
    # Frozen through the cause ledger, and refused twice at the gate: risk-state rule AND clock_skew rule.
    assert "clock_skew" in report.frozen_reasons
    assert rig.mode.risk_state() == RiskState.FROZEN
    causes = {c: d for c, _s, d in rig.latch.active_causes()}
    assert "clock_skew" in causes.get("startup_selftest", "")
    assert verdict == "reject"
    assert not checks["mode_risk_state"].passed
    assert not checks["clock_skew"].passed and "no entries until resync" in checks["clock_skew"].headroom
    assert "frozen: clock_skew" in startup.body
    assert startup.data["frozen_reasons"] == ["clock_skew"]


# ------------------------------------------------------------------------ until resync (entries)
async def test_entries_stay_refused_while_skewed_even_after_the_warmup_lift(skew_rig):
    """The gate's backstop, independent of risk state: the boot-scoped skew verdict keeps refusing entries while skewed."""
    rig = skew_rig
    rig.ntp.offset_s = 3.5
    await rig.boot()
    for _ in range(3):                                        # three 60 s warm-up refresh pulses
        await rig.warmup_refresh()
    assert rig.ntp.offset_s == 3.5                            # nothing resynced

    verdict, checks = await rig.checks()
    assert verdict == "reject"
    assert not checks["clock_skew"].passed


async def test_resync_then_restart_reopens_entries(skew_rig):
    rig = skew_rig
    rig.ntp.offset_s = 3.5
    await rig.boot()
    assert rig.mode.risk_state() == RiskState.FROZEN

    # Resync, then the restart the design needs (the skew verdict is measured at boot only).
    await rig.lifecycle.shutdown(reason="owner")
    rig.ntp.offset_s = 0.2
    report = await rig.boot()
    assert "clock_skew" not in report.frozen_reasons and rig.skew_holder["ok"] is True
    await rig.warmup_refresh()                                # the first 60 s pulse after the boot

    assert rig.mode.risk_state() == RiskState.NORMAL
    assert rig.latch.active_causes() == []                    # no latch left behind
    _verdict, checks = await rig.checks()
    assert checks["clock_skew"].passed and checks["mode_risk_state"].passed


# ----------------------------------------------------------------------- until resync (risk state)
async def test_skew_freeze_holds_until_resync(skew_rig):
    """The 60 s warm-up lift must not clear the 'startup_selftest' cause carrying clock_skew (its self-test skips NTP); it holds on the boot's skew verdict."""
    rig = skew_rig
    rig.ntp.offset_s = 3.5
    await rig.boot()
    assert rig.mode.risk_state() == RiskState.FROZEN
    requests_at_boot = rig.ntp.requests

    await rig.warmup_refresh()                                # warm-up is ready; the clock is NOT

    assert rig.ntp.requests == requests_at_boot
    assert rig.mode.risk_state() == RiskState.FROZEN
    assert any(c == "startup_selftest" for c, _s, _d in rig.latch.active_causes())


# ----------------------------------------------------------------------- mid-session (RECOMMEND-mode gap)
@pytest.mark.skip(reason=(
    "Accepted gap, not built: skew is measured at BOOT only — main.py: the clock-skew verdict is "
    "boot-scoped (the startup self-test measures it; the health loop deliberately skips per-minute NTP), "
    "and a mid-day drift is caught at the next startup (skew_holder); the "
    "always-on health pulse runs health.check(check_skew=False). So a drift that STARTS mid-session "
    "is never detected and an in-session resync cannot re-open entries (restart required)."
))
async def test_mid_session_skew_detection_and_in_session_resync():
    raise AssertionError("unreachable — skipped")
