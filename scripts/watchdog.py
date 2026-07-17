"""Out-of-band engine watchdog (§2.2 / §3.2.12 / §10.7 — pinned spec).

A standalone, NON-wake Windows Scheduled Task run every ``lifecycle.watchdog_poll_s`` (~60 s) as the
mt-engine service account (so the per-user DPAPI Telegram bot token decrypts). It exists to survive
the engine's death, therefore it imports ONLY ``engine.core.secrets`` (bot token) +
``engine.core.config`` (owner chat_id + lifecycle knobs, non-secrets) + ``httpx`` + stdlib — never
``engine.ops``/broker/notify. (Package-init caveat: importing ``engine.core.*`` runs
``engine/__init__`` → ``engine._preload`` → a best-effort ``import sklearn`` — a load-order guard
side effect, not a functional dependency; it adds ~1 s to a 60 s-cadence task.)

Storage discipline (§2.2): opens ``state.db`` READ-ONLY (sqlite URI ``mode=ro``) and keeps its own
debounce in a private file ``data/watchdog_state.json`` — it is NEVER a second writer to state.db
(one-writer invariant; immune to the engine's disk-full/write-lock failure modes).

Per tick, two out-of-band checks (§2.2, full algorithm there):

(a) **missed scheduled start** — on a trading day, now > ``start_grace_s`` past a
    ``lifecycle.active_period_starts`` time with ``started_at`` not ≥ that start (and the engine not
    currently up) ⇒ ``SCHEDULED_START_MISSED``. One alert per (day, start), stamped on send success.
(b) **engine down** — ``state != 'STOPPED'`` AND [ recorded pid NOT alive ⇒ ``reason=crash`` | OR
    ``state == 'RUNNING'`` AND pid alive AND ``COALESCE(last_alive_at, started_at)`` stale >
    ``down_stale_s`` AND ``now − started_at > catchup_grace_s`` ⇒ ``reason=wedged`` — then force-kill
    the recorded pid (ctypes ``TerminateProcess``) so NSSM's exit-triggered restart takes over ] AND
    the re-arm predicate ``last_down_alert_at < COALESCE(last_alive_at, started_at)`` (or unset).
    ``ENGINE_DOWN`` is sent FIRST via the direct Telegram Bot API (httpx — the engine's TelegramBot
    never emits this kind); the debounce is stamped ONLY on a confirmed successful send, so a
    correlated network/Telegram outage retries every tick instead of losing the one real-time alert.
    A fresh boot heartbeat re-arms the alarm automatically. ``state == 'STOPPED'`` ⇒ SILENT
    (intentional off is normal, §2.6). ``STOPPING`` + pid alive ⇒ not-a-crash (intentional teardown).

Spec-silent resolutions (documented):
- The wedge force-kill runs only AFTER the confirmed-successful ``ENGINE_DOWN`` send ("the alert is
  sent first"): killing first and letting NSSM restart would clear the down-condition and lose the
  retried alert during a correlated outage. Remediation is merely delayed while the network is down —
  capital is broker-protected throughout (R3) and a wedged engine cannot act anyway.
- A scheduled start is NOT flagged missed while the engine is already up across the fire-time
  (state != STOPPED with a live pid): the active period is covered; §2.2 likewise does not re-emit
  ENGINE_STARTED when a running process rolls into a later active period.
- Trading-day check: NSECalendar is not importable here (import allowlist above), so the calendar
  YAML (weekday + full-day holidays) is read directly via ``core.config.load_yaml``. Missing year
  file ⇒ treated as non-trading (silent) — "no calendar, no trading"-conservative for an alert-only
  path. Muhurat/shortened special sessions never host a scheduled morning start.

The decision core (:func:`decide` / :func:`run_tick`) is PURE — state rows, pid prober, killer,
sender and trading-day predicate are injected — so ``tests/unit/test_watchdog.py`` exercises crash
vs wedged vs STOPPING vs STOPPED, debounce retry and re-arm without a real process or network.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Bare-script bootstrap: make `engine.core.*` importable when launched as `python scripts/watchdog.py`
# by the Scheduled Task (the venv has the package installed; this covers a source-tree run too).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:  # pragma: no cover - environment shim
    sys.path.insert(0, str(_REPO_ROOT / "src"))

import httpx  # noqa: E402

from engine.core.config import Settings, config_dir, load_settings, load_yaml  # noqa: E402
from engine.core.secrets import TELEGRAM_BOT_TOKEN, Secrets  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")

_STILL_ACTIVE = 259  # Windows GetExitCodeProcess sentinel for a running process

PidAliveFn = Callable[[int | None], bool]
KillFn = Callable[[int], bool]
SendFn = Callable[[str], bool]
TradingDayFn = Callable[[date], bool]


# --------------------------------------------------------------------------- OS probes (stdlib/ctypes)
def pid_alive(pid: int | None) -> bool:
    """True iff ``pid`` names a live OS process. Deliberate stdlib-only duplicate of
    ``engine.ops.heartbeat.pid_alive`` — the watchdog may not import ``engine.ops`` (§3.2.12)."""
    if not pid or pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        process_query_limited_information = 0x1000
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == _STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def force_kill(pid: int) -> bool:
    """Force-kill a wedged engine pid so NSSM's exit-triggered restart takes over (§2.2).

    Windows: ctypes ``OpenProcess(PROCESS_TERMINATE)`` + ``TerminateProcess`` (the §2.2-pinned
    mechanism); POSIX fallback SIGKILL. Returns True iff the terminate call was accepted."""
    if sys.platform == "win32":
        import ctypes

        process_terminate = 0x0001
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(process_terminate, False, int(pid))
        if not handle:
            return False
        try:
            return bool(kernel32.TerminateProcess(handle, 1))
        finally:
            kernel32.CloseHandle(handle)
    import signal

    try:
        os.kill(int(pid), signal.SIGKILL)
    except OSError:
        return False
    return True


# --------------------------------------------------------------------------- pure decision core
@dataclass(frozen=True)
class LifecycleSnapshot:
    """The ``engine_lifecycle`` row as read (read-only) from state.db. ``state=None`` ⇔ no row/db yet
    (fresh install — nothing to watch, silent)."""

    state: str | None = None
    pid: int | None = None
    last_alive_at: datetime | None = None
    started_at: datetime | None = None


@dataclass(frozen=True)
class WatchdogConfig:
    down_stale_s: float = 90.0
    catchup_grace_s: float = 900.0
    start_grace_s: float = 900.0
    active_period_starts: tuple[time, ...] = (time(8, 0),)


@dataclass(frozen=True)
class DebounceState:
    """Private debounce persisted in data/watchdog_state.json (§2.2) — never in state.db."""

    last_down_alert_at: datetime | None = None
    # (day, start) keys "YYYY-MM-DDTHH:MM" already alerted SCHEDULED_START_MISSED for.
    missed_start_alerted: tuple[str, ...] = ()


@dataclass(frozen=True)
class Decision:
    """What this tick should do. ``engine_down_alert`` False with ``down_reason`` set means the
    condition holds but the debounce says the one alert for this outage already went out."""

    down_reason: str | None = None          # "crash" | "wedged" | None
    engine_down_alert: bool = False         # send ENGINE_DOWN this tick (re-arm predicate passed)
    kill_pid: int | None = None             # wedge remediation target (after a successful send)
    since: datetime | None = None           # last known alive (COALESCE(last_alive_at, started_at))
    missed_starts: tuple[datetime, ...] = ()  # SCHEDULED_START_MISSED fire-times to alert


def decide(
    snap: LifecycleSnapshot,
    cfg: WatchdogConfig,
    debounce: DebounceState,
    now: datetime,
    *,
    is_pid_alive: PidAliveFn,
    is_trading_day: TradingDayFn,
) -> Decision:
    """The pure §2.2 per-tick decision (checks (a) + (b)); all probes injected."""
    alive = is_pid_alive(snap.pid)
    engine_up = snap.state in ("RUNNING", "STOPPING") and alive

    # ---- (a) missed scheduled start -------------------------------------------------------------
    missed: list[datetime] = []
    if is_trading_day(now.date()) and not engine_up:
        for t in cfg.active_period_starts:
            start_dt = datetime.combine(now.date(), t, tzinfo=now.tzinfo)
            if now <= start_dt + timedelta(seconds=cfg.start_grace_s):
                continue  # not yet past grace
            if snap.started_at is not None and snap.started_at >= start_dt:
                continue  # a start (manual or scheduled) covered it (§2.2)
            if _missed_key(start_dt) in debounce.missed_start_alerted:
                continue  # already alerted this (day, start)
            missed.append(start_dt)

    # ---- (b) engine down -------------------------------------------------------------------------
    if snap.state not in ("RUNNING", "STOPPING"):
        return Decision(missed_starts=tuple(missed))  # STOPPED / no row ⇒ silent (§2.6 off-is-normal)

    last_known_alive = snap.last_alive_at or snap.started_at
    reason: str | None = None
    kill_pid: int | None = None
    if not alive:
        reason = "crash"  # crash/kill/OOM — or an interrupted STOPPING shutdown (§2.2)
    elif (
        snap.state == "RUNNING"
        and last_known_alive is not None
        and (now - last_known_alive).total_seconds() > cfg.down_stale_s
        and snap.started_at is not None
        and (now - snap.started_at).total_seconds() > cfg.catchup_grace_s
    ):
        reason = "wedged"  # alive-but-stuck: heartbeat starved past any legitimate catch-up (§2.2)
        kill_pid = snap.pid
    if reason is None:
        return Decision(missed_starts=tuple(missed))

    # Re-arm predicate: one alert per outage; a fresh boot heartbeat (last_alive_at newer than the
    # stamp) re-arms automatically (§2.2). Unset stamp ⇒ armed.
    armed = debounce.last_down_alert_at is None or (
        last_known_alive is not None and debounce.last_down_alert_at < last_known_alive
    )
    return Decision(
        down_reason=reason,
        engine_down_alert=armed,
        kill_pid=kill_pid,
        since=last_known_alive,
        missed_starts=tuple(missed),
    )


def run_tick(
    snap: LifecycleSnapshot,
    cfg: WatchdogConfig,
    debounce: DebounceState,
    now: datetime,
    *,
    is_pid_alive: PidAliveFn,
    is_trading_day: TradingDayFn,
    send: SendFn,
    kill: KillFn,
) -> tuple[DebounceState, dict]:
    """One watchdog tick: decide → alert-first → stamp-on-success → remediate. Pure given its
    injected effects; returns the debounce state to persist plus a summary dict (logged by main)."""
    decision = decide(snap, cfg, debounce, now, is_pid_alive=is_pid_alive, is_trading_day=is_trading_day)
    summary: dict = {
        "down_reason": decision.down_reason,
        "engine_down_sent": False,
        "killed_pid": None,
        "missed_start_sent": [],
    }

    if decision.engine_down_alert and decision.down_reason:
        ok = send(_engine_down_text(decision, now))
        summary["engine_down_sent"] = ok
        if ok:
            # Stamp ONLY on confirmed send success — a failed send retries every tick (§2.2).
            debounce = replace(debounce, last_down_alert_at=now)
            if decision.kill_pid is not None:
                # Alert-first honored; now force-kill the wedge so NSSM restarts it (§2.2).
                summary["killed_pid"] = decision.kill_pid if kill(decision.kill_pid) else None

    for start_dt in decision.missed_starts:
        key = _missed_key(start_dt)
        if send(_missed_start_text(start_dt, cfg, now)):
            debounce = replace(debounce, missed_start_alerted=(*debounce.missed_start_alerted, key))
            summary["missed_start_sent"].append(key)

    return debounce, summary


def _missed_key(start_dt: datetime) -> str:
    return start_dt.strftime("%Y-%m-%dT%H:%M")


def _engine_down_text(decision: Decision, now: datetime) -> str:
    since = decision.since.isoformat() if decision.since else "unknown"
    downtime_min = (
        f"{(now - decision.since).total_seconds() / 60.0:.1f}" if decision.since else "?"
    )
    remediation = (
        " Force-killing the wedged pid so the service restarts it." if decision.kill_pid else ""
    )
    return (
        f"🚨 ENGINE_DOWN(reason={decision.down_reason}) — engine last alive {since} "
        f"(~{downtime_min} min ago). Open positions remain broker-protected (SL-M/GTT rest at the "
        f"exchange, R3).{remediation} Runbook: §10.4 case 1/2."
    )


def _missed_start_text(start_dt: datetime, cfg: WatchdogConfig, now: datetime) -> str:
    return (
        f"⚠️ SCHEDULED_START_MISSED — expected active-period start {start_dt.strftime('%H:%M')} on "
        f"{start_dt.date().isoformat()} did not occur within {int(cfg.start_grace_s)}s "
        f"(now {now.strftime('%H:%M')}). Check the Scheduled Task / PC power state; an open MIS "
        f"rides to the broker 15:25 backstop (§10.4 case 19)."
    )


# --------------------------------------------------------------------------- IO shell
def read_snapshot(db_path: Path) -> LifecycleSnapshot:
    """Read the engine_lifecycle row STRICTLY read-only (sqlite URI mode=ro, §2.2). Any failure —
    missing db/table/row (fresh install), or the engine holding an exclusive lock — degrades to an
    empty snapshot (silent tick) rather than ever creating/writing the db."""
    uri = f"file:{db_path.as_posix()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    except sqlite3.Error:
        return LifecycleSnapshot()
    try:
        row = conn.execute(
            "SELECT state, pid, last_alive_at, started_at FROM engine_lifecycle WHERE id=1"
        ).fetchone()
    except sqlite3.Error:
        return LifecycleSnapshot()
    finally:
        conn.close()
    if row is None:
        return LifecycleSnapshot()
    return LifecycleSnapshot(
        state=row[0],
        pid=int(row[1]) if row[1] else None,
        last_alive_at=_parse_dt(row[2]),
        started_at=_parse_dt(row[3]),
    )


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=IST)


def load_debounce(path: Path) -> DebounceState:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return DebounceState()
    return DebounceState(
        last_down_alert_at=_parse_dt(data.get("last_down_alert_at")),
        missed_start_alerted=tuple(
            k for k in data.get("missed_start_alerted", []) if isinstance(k, str)
        )[-64:],  # bounded history; keys are day-scoped so old ones are inert
    )


def save_debounce(path: Path, st: DebounceState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "last_down_alert_at": st.last_down_alert_at.isoformat() if st.last_down_alert_at else None,
                "missed_start_alerted": list(st.missed_start_alerted)[-64:],
            }
        ),
        encoding="utf-8",
    )


def make_trading_day_fn(calendar_dir: Path) -> TradingDayFn:
    """Minimal trading-day predicate from the calendar YAML (weekday + full-day holidays) — the
    watchdog's import allowlist excludes NSECalendar (see module docstring). Missing/unreadable
    year file ⇒ non-trading (silent, conservative for an alert-only path)."""
    cache: dict[int, set[date] | None] = {}

    def _holidays(year: int) -> set[date] | None:
        if year not in cache:
            try:
                raw = load_yaml(calendar_dir / f"{year}.yaml")
                cache[year] = {
                    date.fromisoformat(h["date"]) for h in raw.get("holidays", []) if h.get("date")
                }
            except (OSError, ValueError, KeyError, TypeError):
                cache[year] = None
        return cache[year]

    def _is_trading_day(d: date) -> bool:
        hols = _holidays(d.year)
        if hols is None:
            return False  # no calendar, no expectation — never alert blind
        return d.weekday() < 5 and d not in hols

    return _is_trading_day


def make_telegram_sender(token: str, chat_id: int, timeout_s: float = 10.0) -> SendFn:
    """Direct Telegram Bot API sender (httpx) — deliberately NOT the engine's TelegramBot (§2.2:
    ENGINE_DOWN is watchdog-sent out-of-band; a dead engine can't self-report)."""

    def _send(text: str) -> bool:
        try:
            resp = httpx.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=timeout_s,
            )
            return resp.status_code == 200 and bool(resp.json().get("ok"))
        except Exception:  # noqa: BLE001 - send failure = retry next tick, never crash the task
            return False

    return _send


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="mt-engine out-of-band watchdog (§2.2) — one tick per run")
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    parser.add_argument("--dry-run", action="store_true", help="decide + print, but never send or kill")
    args = parser.parse_args(argv)

    settings: Settings = load_settings(args.config_dir)
    lc = settings.lifecycle
    cfg = WatchdogConfig(
        down_stale_s=float(lc.down_stale_s),
        catchup_grace_s=float(lc.catchup_grace_s),
        start_grace_s=float(lc.start_grace_s),
        active_period_starts=tuple(lc.active_period_starts),
    )
    now = datetime.now(IST)
    snap = read_snapshot(settings.sqlite_path())
    debounce_path = settings.resolved_data_dir() / "watchdog_state.json"
    debounce = load_debounce(debounce_path)
    cal_dir = (Path(args.config_dir) if args.config_dir else config_dir()) / "calendar"

    if args.dry_run:
        decision = decide(
            snap, cfg, debounce, now, is_pid_alive=pid_alive, is_trading_day=make_trading_day_fn(cal_dir)
        )
        print(f"watchdog dry-run: snap={snap} decision={decision}")
        return 0

    token = Secrets().get_optional(TELEGRAM_BOT_TOKEN)
    chat_id = settings.telegram.owner_chat_id
    if not token or not chat_id:
        # Without a sender the watchdog is pointless — exit non-zero so Task Scheduler history shows it.
        print("watchdog: telegram_bot_token secret or telegram.owner_chat_id missing — cannot alert",
              file=sys.stderr)
        return 2

    new_debounce, summary = run_tick(
        snap, cfg, debounce, now,
        is_pid_alive=pid_alive,
        is_trading_day=make_trading_day_fn(cal_dir),
        send=make_telegram_sender(token, chat_id),
        kill=force_kill,
    )
    if new_debounce != debounce:
        save_debounce(debounce_path, new_debounce)
    print(f"watchdog tick: state={snap.state} pid={snap.pid} {summary}")
    return 0


if __name__ == "__main__":  # pragma: no cover - Scheduled Task entrypoint
    raise SystemExit(main())
