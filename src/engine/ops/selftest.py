"""Startup self-test (D11, §3.2.12). Runs on EVERY startup (process boot, scheduled or manual).

Asserts the platform's preconditions before entries can open and surfaces what must FROZEN/kill/prompt:
secrets present, ``ANTHROPIC_API_KEY`` absent (D2 trap), protected-store hashes verified (R4), kill-state
honored (R10), clock skew < 2 s (R6), trade window present + valid (§7.1), and (when a session manager is
wired) the daily token's validity (R6).

Phase-1 addition — the §3.2.12 **data-freshness gate before entries**: today-dated
instruments/surveillance/earnings (the safety/deadline-critical §2.6 step-5 jobs) must be recorded
fresh in ``job_runs``; if stale, the catch-up job is triggered (idempotent watermarks); if still
unsatisfiable ⇒ refuse to open entries (FROZEN-for-entries, applied by the lifecycle) + alert. The
warm-up sufficiency check (§7.1 ``warmup_ready``) rides the same surface via the injected
:class:`~engine.ops.warmup.WarmupGate`. ``SessionLifecycle.startup`` runs this self-test with
``include_freshness=False`` because at §2.6 step 1c the backfill/catch-up have not run yet — the
lifecycle enforces freshness after step 5 and warm-up at step 6; a standalone/pre-entries re-run
(``run()`` default) includes them.

Still stubbed with explicit markers (surfaced as SKIP so the gate sequence is honest): the day-scoped
risk-counter rebuild + continuous equity halt-ladder re-evaluation (§2.6 — TODO(Phase 2/3): needs the
ledger + ExposureTracker + broker reconcile) and the one cheap Haiku SDK call (D11 — deduped per
trading day, skipped on non-trading-day starts; TODO(Phase 1 intelligence harness)).

This module REPORTS; the lifecycle applies the consequences (the §2.4 integrity rule: FROZEN-with-flat-book
vs kill, etc.). A failed self-test never trades on thin/unsafe preconditions (§2.6).
"""

from __future__ import annotations

import os
import sqlite3
from enum import StrEnum

from pydantic import BaseModel, Field

from engine import _preload
from engine.core.clock import Clock, ClockSkewUnavailable
from engine.core.config import Settings, config_dir, load_yaml
from engine.core.log import get_logger
from engine.core.protected_store import PROTECTED_NAMES, ProtectedStore
from engine.core.secrets import Secrets
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

_log = get_logger("engine.ops.selftest")


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"


class Implies(StrEnum):
    NONE = "none"
    FROZEN = "frozen_entries"
    KILL = "kill"
    LOGIN_PROMPT = "login_prompt"


class SelfTestCheck(BaseModel):
    name: str
    status: CheckStatus
    detail: str = ""
    implies: Implies = Implies.NONE


class SelfTestReport(BaseModel):
    checks: list[SelfTestCheck] = Field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.status in (CheckStatus.PASS, CheckStatus.SKIP, CheckStatus.WARN) for c in self.checks)

    @property
    def killed(self) -> bool:
        return any(c.implies == Implies.KILL for c in self.checks)

    @property
    def needs_login(self) -> bool:
        return any(c.implies == Implies.LOGIN_PROMPT for c in self.checks)

    @property
    def frozen_reasons(self) -> list[str]:
        return [c.name for c in self.checks if c.implies == Implies.FROZEN]


class SelfTest:
    """The every-startup self-test (D11)."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        clock: Clock,
        settings: Settings,
        secrets: Secrets,
        protected_store: ProtectedStore,
        kill_switch: KillSwitch,
        mode_manager: ModeManager,
        session_manager=None,
        catch_up=None,
        warmup_gate=None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._settings = settings
        self._secrets = secrets
        self._store = protected_store
        self._kill = kill_switch
        self._mode = mode_manager
        self._session = session_manager
        # §3.2.12 data-freshness seams (Phase 1): the CatchUpRunner (job_runs watermarks) and the
        # WarmupGate — both optional; unwired ⇒ the freshness checks surface as SKIP, never silently pass.
        self._catch_up = catch_up
        self._warmup_gate = warmup_gate

    async def run(self, *, check_skew: bool = True, include_freshness: bool = True) -> SelfTestReport:
        report = SelfTestReport()
        report.checks.append(self._check_native_preload())
        report.checks.append(self._check_secrets())
        report.checks.append(self._check_anthropic_key_absent())
        report.checks.extend(self._check_protected_store())
        report.checks.append(self._check_kill_state())
        report.checks.append(await self._check_clock_skew(check_skew))
        report.checks.append(self._check_trade_window())
        report.checks.append(self._check_token())
        report.checks.append(self._stub(
            "risk_counters_rebuild", "§2.6 day-scoped counters — TODO(Phase 2/3): ledger + reconcile"))
        report.checks.append(self._stub(
            "equity_halt_ladder", "§2.6 floor-ladder re-eval — TODO(Phase 2/3): ExposureTracker equity"))
        if include_freshness:
            report.checks.extend(await self.data_freshness_checks())
        report.checks.append(self._stub(
            "sdk_smoke", "one cheap Haiku call — wired with the intelligence harness, Phase 1 (D11)"))

        for c in report.checks:
            level = _log.info if c.status in (CheckStatus.PASS, CheckStatus.SKIP) else _log.warning
            level("selftest_check", check=c.name, status=c.status.value, detail=c.detail, implies=c.implies.value)
        _log.info("selftest_complete", ok=report.ok, killed=report.killed,
                  needs_login=report.needs_login, frozen=report.frozen_reasons)
        return report

    # ----------------------------------------------------------------- checks
    def _check_native_preload(self) -> SelfTestCheck:
        # The vectorbt+skfolio native import-order guard (engine._preload): scikit-learn's OpenMP
        # runtime must be established before numba/vectorbt/cvxpy load, or the process segfaults on
        # Windows (0xC0000005). engine/__init__ imports it first; here we surface that it took hold.
        if _preload.PRELOADED:
            return SelfTestCheck(name="native_preload", status=CheckStatus.PASS,
                                 detail="scikit-learn OpenMP runtime established (vectorbt+skfolio safe)")
        return SelfTestCheck(name="native_preload", status=CheckStatus.WARN,
                             detail="scikit-learn absent — vectorbt+skfolio coexistence unguarded (engine._preload)")

    def _check_secrets(self) -> SelfTestCheck:
        missing = self._secrets.missing_required()
        if missing:
            return SelfTestCheck(name="secrets_present", status=CheckStatus.FAIL,
                                 detail=f"missing: {missing}", implies=Implies.FROZEN)
        return SelfTestCheck(name="secrets_present", status=CheckStatus.PASS)

    def _check_anthropic_key_absent(self) -> SelfTestCheck:
        # D2: a stray ANTHROPIC_API_KEY silently OUTRANKS the OAuth token. Tolerated only if the owner
        # deliberately enabled pay-as-you-go overflow (agents.yaml llm.overflow_enabled).
        present = bool(os.environ.get("ANTHROPIC_API_KEY"))
        overflow = False
        try:
            overflow = bool(load_yaml(config_dir() / "agents.yaml").get("llm", {}).get("overflow_enabled"))
        except FileNotFoundError:
            pass
        if present and not overflow:
            return SelfTestCheck(name="anthropic_key_absent", status=CheckStatus.FAIL,
                                 detail="ANTHROPIC_API_KEY set but overflow disabled — it outranks OAuth (D2)",
                                 implies=Implies.FROZEN)
        if present and overflow:
            return SelfTestCheck(name="anthropic_key_absent", status=CheckStatus.WARN,
                                 detail="ANTHROPIC_API_KEY present; overflow enabled by owner (D6)")
        return SelfTestCheck(name="anthropic_key_absent", status=CheckStatus.PASS)

    def _check_protected_store(self) -> list[SelfTestCheck]:
        out: list[SelfTestCheck] = []
        for name in PROTECTED_NAMES:
            ok = self._store.verify(name)
            # §2.4 single rule: mismatch consequence (FROZEN vs kill) depends on book state — decided by
            # the lifecycle. The self-test surfaces the integrity failure as FROZEN-implying by default.
            out.append(SelfTestCheck(
                name=f"protected_store:{name}",
                status=CheckStatus.PASS if ok else CheckStatus.FAIL,
                detail="" if ok else "hash mismatch or unregistered (R4) — run scripts/seed_protected_config.py",
                implies=Implies.NONE if ok else Implies.FROZEN,
            ))
        return out

    def _check_kill_state(self) -> SelfTestCheck:
        if self._kill.is_killed():
            return SelfTestCheck(name="kill_state", status=CheckStatus.FAIL,
                                 detail=f"kill switch engaged: {self._kill.reason()!r} (R10)", implies=Implies.KILL)
        return SelfTestCheck(name="kill_state", status=CheckStatus.PASS)

    async def _check_clock_skew(self, check_skew: bool) -> SelfTestCheck:
        if not check_skew:
            return SelfTestCheck(name="clock_skew", status=CheckStatus.SKIP, detail="skew check disabled")
        max_skew = self._settings.clock.max_skew_s
        try:
            skew = await self._clock.check_skew()
        except ClockSkewUnavailable as exc:
            # Cannot verify time ⇒ treat conservatively (refuse new entries), never "skew is fine" (R6).
            return SelfTestCheck(name="clock_skew", status=CheckStatus.WARN,
                                 detail=f"NTP unavailable: {exc}; entries stay conservative", implies=Implies.FROZEN)
        if skew.total_seconds() > max_skew:
            return SelfTestCheck(name="clock_skew", status=CheckStatus.FAIL,
                                 detail=f"skew {skew.total_seconds():.2f}s > {max_skew}s (R6)", implies=Implies.FROZEN)
        return SelfTestCheck(name="clock_skew", status=CheckStatus.PASS, detail=f"skew {skew.total_seconds():.2f}s")

    def _check_trade_window(self) -> SelfTestCheck:
        window = self._mode.get_trade_window()
        if window is None:
            return SelfTestCheck(name="trade_window", status=CheckStatus.FAIL,
                                 detail="no trade window set (§7.1)", implies=Implies.FROZEN)
        err = self._mode.validate_window(window)
        if err is not None:
            return SelfTestCheck(name="trade_window", status=CheckStatus.FAIL,
                                 detail=f"invalid window: {err}", implies=Implies.FROZEN)
        return SelfTestCheck(name="trade_window", status=CheckStatus.PASS,
                             detail=f"{window.start}-{window.end} buf {window.squareoff_buffer_min}m")

    def _check_token(self) -> SelfTestCheck:
        if self._session is None:
            return SelfTestCheck(name="token_valid", status=CheckStatus.SKIP, detail="no session manager wired")
        if self._session.token_valid():
            return SelfTestCheck(name="token_valid", status=CheckStatus.PASS)
        # Non-fatal: owner may start before the daily login; entries stay FROZEN until re-login (R6).
        return SelfTestCheck(name="token_valid", status=CheckStatus.WARN,
                             detail="daily token not valid — login required (R6)", implies=Implies.LOGIN_PROMPT)

    # ----------------------------------------------------------------- data freshness (§3.2.12/§2.6)
    async def data_freshness_checks(self) -> list[SelfTestCheck]:
        """The §3.2.12 DATA-FRESHNESS gate before entries: today-dated instruments + surveillance +
        earnings (+ ex-date GTT adjust) recorded fresh in ``job_runs``, and warm-up sufficient.

        Stale safety-critical jobs first TRIGGER the catch-up (idempotent ``job_runs`` watermarks —
        re-running a fresh job is a no-op, §2.6 step 5); only what is STILL stale after that fails the
        check (⇒ FROZEN-for-entries, applied by the lifecycle) + alert. Unwired seams surface as SKIP —
        never a silent pass. The held-CNC ex-date GTT scan itself is the ``corp_actions_gtt_adjust``
        job (TODO(Phase 3) wiring: needs the OMS GTT manager); its freshness rides the same watermark.
        """
        return [await self._check_safety_jobs_fresh(), await self._check_warmup_ready()]

    async def _check_safety_jobs_fresh(self) -> SelfTestCheck:
        if self._catch_up is None or not getattr(self._catch_up, "has_registry", False):
            return self._stub(
                "data_freshness",
                "no CatchUpRunner/job registry wired — Phase-1 jobs registered by the integrator (§2.6)",
            )
        stale = self._catch_up.stale_safety_jobs()
        if stale:
            # Trigger the catch-up job (idempotent watermarks) — §3.2.12 "else run the catch-up job".
            _log.warning("data_freshness_stale_triggering_catchup", stale=stale)
            try:
                await self._catch_up.catch_up()
            except Exception:  # noqa: BLE001 - unsatisfiable catch-up must surface as FAIL, not crash
                _log.exception("data_freshness_catchup_failed")
            stale = self._catch_up.stale_safety_jobs()
        if stale:
            return SelfTestCheck(
                name="data_freshness",
                status=CheckStatus.FAIL,
                detail=f"safety-critical jobs not fresh today after catch-up: {stale} (§2.6 step 5)",
                implies=Implies.FROZEN,
            )
        return SelfTestCheck(name="data_freshness", status=CheckStatus.PASS,
                             detail="today-dated safety-critical jobs fresh (instruments/surveillance/earnings)")

    async def _check_warmup_ready(self) -> SelfTestCheck:
        if self._warmup_gate is None:
            return self._stub(
                "warmup_ready", "no WarmupGate wired — integrator passes engine.ops.warmup.WarmupGate (§2.6)"
            )
        try:
            status = await self._warmup_gate.status()
        except Exception:  # noqa: BLE001 - coverage that cannot be VERIFIED is treated as missing
            _log.exception("warmup_gate_check_failed")
            return SelfTestCheck(name="warmup_ready", status=CheckStatus.FAIL,
                                 detail="warm-up coverage check failed — treated as missing (never trade thin data)",
                                 implies=Implies.FROZEN)
        if status.ready:
            return SelfTestCheck(name="warmup_ready", status=CheckStatus.PASS,
                                 detail="contiguous coverage satisfies every strategy lookback (§7.1)")
        return SelfTestCheck(
            name="warmup_ready",
            status=CheckStatus.FAIL,
            detail="insufficient contiguous coverage: " + "; ".join(status.blockers[:6]),
            implies=Implies.FROZEN,
        )

    @staticmethod
    def _stub(name: str, detail: str) -> SelfTestCheck:
        return SelfTestCheck(name=name, status=CheckStatus.SKIP, detail=detail)
