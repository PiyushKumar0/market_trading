# Chaos drills

This file holds the chaos suite's run command, the live drills run against the real engine with the soak schedule,
and the evidence log of every live drill. The stop/start set (cases 15–19) is G3 gate evidence
(IMPLEMENTATION_PLAN.md, "Gate G3").

## 1. Automated suite

```powershell
.venv\Scripts\python.exe -m pytest tests\chaos -m chaos -q -rs -rx    # ~90 s
```

Run it weekly and before every gate. Every file under `tests/chaos` gets the `chaos` marker automatically; the
case number is in each file name. The flags list skip reasons (`-rs`) and pinned defects (`-rx`).

- **Passing tests** compose the real components the way `engine/ops/main.py` wires them, over a temp database and
  a movable clock. Only the external edge is faked: Kite, the Claude SDK, feed HTTP, Telegram and NTP.
- **Skips** name what they wait for, with one of the reasons in `tests/chaos/conftest.py` (`PHASE3_GATED`,
  `MIS_OUT_OF_PAPER_SCOPE`, `REAL_BROKER_ONLY`, `BROKER_RESIDENT_R3`, `REAL_SIDE_ONLY`, `PHASE4_LIFECYCLE_HOOKS`).
- **`xfail(strict=True)`** pins a real defect: the test asserts the intended behaviour, so fixing the defect makes it
  XPASS, which fails the run until the marker is removed.

## 2. Live drills

### Drillable now (RECOMMEND; paper positions are simulated)

**Case 15 / 22a: planned stop and restart.** Run outside 09:15–15:30, when no job is in flight;
check the `engine.log` tail first.
```powershell
Get-Date; Stop-Service mt-engine            # or scripts\nssm_install.ps1 -Action stop
Select-String data\logs\engine.log -Pattern 'stop_requested|stop_signal_repeat_ignored|stop_forced|"event": "shutdown"' | Select-Object -Last 5
.venv\Scripts\python.exe -c "import sqlite3;c=sqlite3.connect('file:data/state.db?mode=ro',uri=True);print(c.execute('select state,last_clean_stop_at from engine_lifecycle').fetchone())"
Start-Service mt-engine
```
**Pass** requires all of the following:
- state = STOPPED, with `last_clean_stop_at` at the stop time;
- no `stop_forced` line;
- exactly one ENGINE_STOPPED on Telegram;
- after restart, STARTUP_REPORT has `crash_recovered: false`.

**Cases 16 / 17: offline across EOD / multi-day.** These happen naturally whenever the engine is
off overnight or over a weekend. On the next boot, check that STARTUP_REPORT `jobs_caught_up`
lists one entry per missed trading day.

**Case 22b: crash while running.** `Stop-Process -Id <engine pid> -Force`. NSSM restarts it after
15 s. **Pass** requires STARTUP_REPORT `crash_recovered: true` and exactly one ENGINE_DOWN, and the
ENGINE_DOWN half needs the watchdog scheduled task from `scripts/schedule_tasks.ps1`. Owner-run.

**Case 19** is not drillable until scheduled starts (`scripts/schedule_tasks.ps1`) are in use.

**Cases 3 and 4** can be drilled in market hours at a quiet moment:
- case 3: Wi-Fi off for about 30 s;
- case 4: let the daily token expire before login.

### Soak schedule (30-session paper-AUTO soak, G3)

| Soak week | Cases |
|---|---|
| 1 | 1, 2a, 2b |
| 2 | 3, 4 |
| 3 | 5, 6 |
| 4 | 11, 12 |
| 5 | 13, 14 |
| 6 | 15, 16, 17, 18, 19 (stop/start set, G3-blocking) + the dead-platform drill (RUNBOOK "Dead-platform manual fallback") |
| Quarterly after G3 | dead-platform drill + cases 1, 3, 11, 15, 17 |

## 3. Evidence log

Add a row for every live drill, including failures.

| Date (IST) | Case | Procedure | Result | Evidence |
|---|---|---|---|---|
