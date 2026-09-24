"""Plan §9.4 chaos case 14 — Clock skew beyond 2 s (R6).

Must hold (§9.4 row 14, verbatim): "FROZEN entries until resync; alert".

Scenario: the engine boots while the host clock is 3.5 s off NTP (or NTP is unreachable). Composed
as ``engine.ops.main`` wires it: the real ``SessionLifecycle.startup(check_skew=True)`` → the real
``SelfTest`` clock-skew check → the §3.5.3 cause ledger (``RiskStateLatch``); the boot-scoped skew
verdict main.py derives from the startup report (main.py ``skew_holder["ok"] = "clock_skew" not in
report.frozen_reasons``) feeding the real ``GateContextBuilder`` → real ``RiskGate``; and the real
60 s warm-up refresh (``engine.ops.main.refresh_and_lift_warmup``, armed as
``warmup_status_refresh``). Faked: ONLY the NTP server (``ntplib.NTPClient.request`` — the external
time source) and the owner's Telegram sink. Warm-up coverage is READY throughout (not this case's
subject), so the only thing between a boot and open entries is the clock.

Clauses:
* "FROZEN entries" at boot (> 2 s, or NTP unverifiable) + "alert" —
  ``test_boot_with_skew_beyond_limit_freezes_entries_and_alerts`` (2.0 s = the boundary control).
* "until resync" — entries stay REFUSED while skewed even across the warm-up lift:
  ``test_entries_stay_refused_while_skewed_even_after_the_warmup_lift``; resync plus the restart the
  Phase-2 design requires re-opens them: ``test_resync_then_restart_reopens_entries``.
* ``test_skew_freeze_holds_until_resync`` — the risk-state FROZEN survives the 60 s warm-up refresh
  while NTP still reports the skew (CD-2, fixed 2026-09-24).
* Skipped (an accepted Phase-2 gap, NOT Phase-3): mid-session skew detection and in-session resync —
  ``test_mid_session_skew_detection_and_in_session_resync``.

Phase-3-gated: none.
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
    """The NTP boundary. ``offset_s`` is what the pool answers (signed, as ntplib reports it);
    ``reachable=False`` makes every server time out like a blocked UDP/123."""

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
    assert settings.clock.max_skew_s == 2                      # the shipped R6 limit this case is about

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
        rig.skew_holder["ok"] = "clock_skew" not in report.frozen_reasons   # verbatim main.py wiring
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
    # FROZEN entries: through the §3.5.3 cause ledger (never a bare risk_state write) ...
    assert "clock_skew" in report.frozen_reasons
    assert rig.mode.risk_state() == RiskState.FROZEN
    causes = {c: d for c, _s, d in rig.latch.active_causes()}
    assert "clock_skew" in causes.get("startup_selftest", "")
    # ... and at the gate, twice over: the risk-state rule AND the §7.1 clock_skew rule refuse.
    assert verdict == "reject"
    assert not checks["mode_risk_state"].passed
    assert not checks["clock_skew"].passed and "no entries until resync" in checks["clock_skew"].headroom
    # Alert: the owner's boot report names the cause.
    assert "frozen: clock_skew" in startup.body
    assert startup.data["frozen_reasons"] == ["clock_skew"]


# ------------------------------------------------------------------------ until resync (entries)
async def test_entries_stay_refused_while_skewed_even_after_the_warmup_lift(skew_rig):
    """The gate's own backstop, independent of the risk state: the boot-scoped skew verdict keeps the
    §7.1 clock_skew rule refusing every entry while skewed."""
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

    # Resync (owner fixes the Windows time service — runbook §10.4 13/14), then the restart the
    # Phase-2 design needs (the skew verdict is measured at boot only).
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
    """CD-2 (fixed 2026-09-24): the 60 s warm-up lift used to clear the 'startup_selftest' cause that
    carries clock_skew — its self-test skips the NTP check. It now holds on the boot's skew verdict."""
    rig = skew_rig
    rig.ntp.offset_s = 3.5
    await rig.boot()
    assert rig.mode.risk_state() == RiskState.FROZEN
    requests_at_boot = rig.ntp.requests

    await rig.warmup_refresh()                                # warm-up is ready; the clock is NOT

    assert rig.ntp.requests == requests_at_boot               # (no resync was even attempted)
    assert rig.mode.risk_state() == RiskState.FROZEN          # §9.4: FROZEN until resync
    assert any(c == "startup_selftest" for c, _s, _d in rig.latch.active_causes())


# ----------------------------------------------------------------------- mid-session (Phase-2 gap)
@pytest.mark.skip(reason=(
    "Accepted Phase-2 gap, not built: skew is measured at BOOT only — main.py 'Clock-skew verdict is "
    "boot-scoped (§3.2.12 self-test measures it; the health loop deliberately skips per-minute NTP). "
    "A mid-day drift is caught at the next startup — accepted for Phase 2.' (skew_holder), and the "
    "always-on health pulse runs health.check(check_skew=False). So a drift that STARTS mid-session "
    "is never detected and an in-session resync cannot re-open entries (restart required)."
))
async def test_mid_session_skew_detection_and_in_session_resync():
    raise AssertionError("unreachable — skipped")
