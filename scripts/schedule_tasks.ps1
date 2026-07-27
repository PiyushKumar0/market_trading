# scripts/schedule_tasks.ps1 — idempotent Windows Scheduled Task registration (§10.7, E7).
#
# Registers:
#   1. mt-watchdog        — the NON-wake out-of-band watchdog (scripts/watchdog.py) on a 1-minute
#                           repetition (~lifecycle.watchdog_poll_s). Deliberately non-wake: waking
#                           the PC would defeat sleep-between-active-periods (§2.6); it covers
#                           engine-death-while-the-PC-is-awake, not total power loss (§2.2 limitation).
#   2. mt-engine-start-*  — OPTIONAL (-WithEngineStarts) wake-capable tasks, one per
#                           lifecycle.active_period_starts time in config/settings.yaml, that wake
#                           the PC and start the engine for each active period (§2.6/§10.1). Default
#                           start command is `nssm start mt-engine` (install via nssm_install.ps1).
#
# Idempotent: re-running replaces existing tasks of the same name (Register-ScheduledTask -Force).
# Run elevated (Task Scheduler writes) AS THE mt-engine service account, or pass -TaskUser: the
# watchdog must run-as that account so the per-user DPAPI bot token decrypts (§2.2/§10.7).
#
# Usage (elevated PowerShell, from the repo root):
#   .\scripts\schedule_tasks.ps1                        # watchdog only
#   .\scripts\schedule_tasks.ps1 -WithEngineStarts      # + wake-capable engine start tasks
#   .\scripts\schedule_tasks.ps1 -TaskUser "PC\mtsvc"   # explicit run-as account

[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$PythonExe = "",
    [string]$TaskUser = "$env:USERDOMAIN\$env:USERNAME",
    [switch]$WithEngineStarts,
    [string]$EngineStartCommand = "nssm",
    [string]$EngineStartArguments = "start mt-engine",
    [int]$WatchdogEveryMinutes = 1
)

$ErrorActionPreference = "Stop"

if (-not $PythonExe) {
    $PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
}
if (-not (Test-Path $PythonExe)) {
    throw "python not found at '$PythonExe' - pass -PythonExe or create the venv first"
}
$WatchdogScript = Join-Path $RepoRoot "scripts\watchdog.py"
if (-not (Test-Path $WatchdogScript)) {
    throw "watchdog script not found at '$WatchdogScript'"
}

function Register-IdempotentTask {
    param(
        [string]$Name,
        [Microsoft.Management.Infrastructure.CimInstance]$Action,
        [Microsoft.Management.Infrastructure.CimInstance]$Trigger,
        [bool]$WakeToRun
    )
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
    $settings.WakeToRun = $WakeToRun
    # -Force replaces an existing task of the same name => idempotent re-runs.
    Register-ScheduledTask `
        -TaskName $Name `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $settings `
        -User $TaskUser `
        -RunLevel Limited `
        -Force | Out-Null
    Write-Host "registered task '$Name' (wake=$WakeToRun, user=$TaskUser)"
}

# ---------------------------------------------------------------- 1) the non-wake watchdog (§2.2)
$watchdogAction = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$WatchdogScript`"" `
    -WorkingDirectory $RepoRoot
# Start at the next minute boundary and repeat forever while the PC is awake (non-wake task).
$watchdogTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval (New-TimeSpan -Minutes $WatchdogEveryMinutes) `
    -RepetitionDuration ([TimeSpan]::MaxValue)
Register-IdempotentTask -Name "mt-watchdog" -Action $watchdogAction -Trigger $watchdogTrigger -WakeToRun $false

# ------------------------------------------- 2) optional wake-capable engine-start tasks (§2.6/§10.7)
if ($WithEngineStarts) {
    # Parse lifecycle.active_period_starts from config/settings.yaml (flow-style list of "HH:MM").
    $settingsYaml = Join-Path $RepoRoot "config\settings.yaml"
    $starts = @()
    if (Test-Path $settingsYaml) {
        $line = Select-String -Path $settingsYaml -Pattern '^\s*active_period_starts:' |
            Select-Object -First 1
        if ($line) {
            $matches2 = [regex]::Matches($line.Line, '\d{1,2}:\d{2}')
            foreach ($m in $matches2) { $starts += $m.Value }
        }
    }
    if ($starts.Count -eq 0) {
        Write-Warning "no lifecycle.active_period_starts found in settings.yaml - defaulting to 08:00"
        $starts = @("08:00")
    }
    foreach ($hhmm in $starts) {
        $safe = $hhmm.Replace(":", "")
        $action = New-ScheduledTaskAction `
            -Execute $EngineStartCommand `
            -Argument $EngineStartArguments `
            -WorkingDirectory $RepoRoot
        $trigger = New-ScheduledTaskTrigger -Daily -At $hhmm
        # Wake-capable: "wake the computer to run this task" so a sleeping PC still starts the
        # engine for the active period (§2.6). The engine's own §2.6 startup sequence + NSECalendar
        # guard make a holiday/weekend fire harmless (it recovers, finds no session, idles/stops).
        Register-IdempotentTask -Name "mt-engine-start-$safe" -Action $action -Trigger $trigger -WakeToRun $true
    }
}

Write-Host "done. Verify in Task Scheduler; watchdog history shows one tick/minute while awake."
