<#
.SYNOPSIS
    Install / remove / control the `mt-engine` NSSM service for the AI trading platform.

.DESCRIPTION
    Plan refs: E7 (Windows ops via NSSM, SetThreadExecutionState, power-plan), D11 (service
    account + DPAPI secrets, no secrets in repo/env), IMPLEMENTATION_PLAN.md §2.2 (process model:
    one supervised asyncio engine; manual/demand start; restart-on-failure gated to a crash during
    an active period; the engine spawns the pinned Claude Code CLI (agents.yaml llm.cli_path), which
    authenticates with its own stored login; no engine code injects an OAuth token), §2.6 (operating model: the engine is up
    only during ACTIVE PERIODS and may be intentionally stopped between them; off outside an active
    period is NORMAL, not a fault, so a clean self-initiated stop MUST NOT be auto-restarted) and the
    authoritative service-setup text §10.7.

    This script is the §10.7 `scripts/nssm_install.ps1` and is intended to be idempotent: re-running
    `-Action install` reconfigures the existing service rather than failing.

    DESIGN DECISIONS BAKED IN (read before changing):

    1. START TYPE = MANUAL / DEMAND (NSSM `Start = SERVICE_DEMAND_START`), NOT automatic.
       Per §2.2/§2.6 the engine intends to be up only during active periods (morning prep, the
       trade window + square-off, EOD jobs) and is deliberately stopped (PC slept) in between.
       Auto-start at boot would fight that model. Active periods are launched manually by the owner
       or by the wake-capable Windows Scheduled Tasks registered by `scripts/schedule_tasks.ps1`
       (a separate optional helper, §10.7) — NOT by the service control manager.

    2. RESTART-ON-FAILURE IS GATED BY THE `AppExit` MAPPING.
       NSSM's own exit handling distinguishes two outcomes via `AppExit`:
         - exit code 0  -> `AppExit 0 Exit`     : a CLEAN, self-initiated stop. NSSM does NOT
                                                   restart. This is the §2.6 "intentional stop ==
                                                   normal, not a crash" path (planned stop / shutdown
                                                   guard exit code, §10.8).
         - any other    -> `AppExit Default Restart` : a CRASH. NSSM restarts after the 15 s
                                                   `AppRestartDelay`, throttled by the crash-loop
                                                   guard below.
       NSSM cannot read a file or a database row as a restart predicate, so the exit code is the
       only gate at the NSSM level. "I intend to run" is the engine's sticky `engine_lifecycle.state`
       (migration 0001; intend-to-run == state != 'STOPPED', §2.6 step 0/1): the engine uses it on
       the next boot to decide whether the previous exit was intended. There is no sentinel file and
       no `AppEvents` hook (this script sets no `AppEvents` key).

    3. RESTART THROTTLE (crash-loop guard).
       `AppThrottle 60000`  : if the process exits inside 60 s of starting, NSSM treats it as a
                              failed start and applies the delay/backoff below instead of hot-looping.
       `AppRestartDelay 15000` : wait 15 s before each restart attempt so a reconnect storm or a
                              transient broker/Windows-Update hiccup does not spin the CPU. The 15 s
                              delay also gives the §2.6 every-startup reconcile/catch-up room to run
                              cleanly rather than racing a half-dead predecessor. A persistent crash
                              loop surfaces as repeated startup reports / SELFTEST_FAIL alerts (§10.3),
                              and the missed-start watchdog (§10.4 case 19) covers a service that
                              never comes up at all.

    4. NO SECRETS IN THE SERVICE ENVIRONMENT (D11/§2.2/§2.4).
       The engine loads its secrets from DPAPI (Windows Credential Manager via keyring) at startup.
       Injecting CLAUDE_CODE_OAUTH_TOKEN into the CLI child is NOT implemented: no engine code reads
       that secret, and the pinned Claude Code CLI (agents.yaml llm.cli_path; the SDK's bundled CLI
       only if that key is unset) authenticates with its own stored login. NSSM
       `AppEnvironmentExtra` for secrets is a DOCUMENTED FALLBACK ONLY (see the commented block in Install-Service) and is deliberately NOT
       set by default — putting a token in the service env would violate R10 (secrets must live in an
       encrypted store) and would let a stray ANTHROPIC_API_KEY silently outrank the OAuth token (D2).

    5. STDOUT / STDERR -> rotating logs under data\logs (E7).
       `AppStdout` / `AppStderr` point at data\logs\service.out.log / service.err.log with NSSM's
       online rotation enabled. These capture pre-logger bootstrap output and any hard crash before
       structlog is up; normal structured logs are written by core.log to data\logs\engine.log.

.PARAMETER Action
    install | remove | status | start | stop  (default: status).

.PARAMETER NssmPath
    Full path to nssm.exe. If omitted, the script looks on PATH.

.PARAMETER ServiceName
    Service name. Default 'mt-engine' (§2.2). Override only for side-by-side test installs.

.PARAMETER RepoRoot
    Repo root (AppDirectory / working directory). Defaults to the parent of this script's folder.

.PARAMETER Confirm
    Required for the DESTRUCTIVE actions (install reconfigures/creates a service; remove deletes it).
    Without -Confirm those actions only print what they WOULD do (dry run). start/stop/status do not
    require it.

.EXAMPLE
    .\nssm_install.ps1 -Action install -Confirm
    Install (or reconfigure) the mt-engine service using .\.venv and this repo as the working dir.

.EXAMPLE
    .\nssm_install.ps1 -Action status
    Print the service config + current state (read-only; no -Confirm needed).

.EXAMPLE
    .\nssm_install.ps1 -Action remove -Confirm
    Stop and delete the mt-engine service.

.NOTES
    PowerShell 5.1-safe: no '&&'/'||' chaining, no ternary, no null-coalescing. Run from an elevated
    (Administrator) prompt — service create/remove and `nssm set` require it.
#>

[CmdletBinding()]
param(
    [ValidateSet('install', 'remove', 'status', 'start', 'stop')]
    [string]$Action = 'status',

    [string]$NssmPath,

    [string]$ServiceName = 'mt-engine',

    [string]$RepoRoot,

    # Windows account the service logs on as (e.g. '.\piyush.kumar_atari'). REQUIRED in practice:
    # DPAPI secrets decrypt only under the owner's profile (D11); LocalSystem cannot read them.
    [string]$ServiceUser,

    [switch]$Confirm
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --------------------------------------------------------------------------------------------------
# Path resolution
# --------------------------------------------------------------------------------------------------

# Repo root defaults to the parent of the scripts\ directory that holds this file.
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path

$PythonExe   = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogsDir     = Join-Path $RepoRoot 'data\logs'           # core.log + service stdout/stderr (E7)
$RunDir      = Join-Path $RepoRoot 'data\run'
$StdoutLog   = Join-Path $LogsDir 'service.out.log'
$StderrLog   = Join-Path $LogsDir 'service.err.log'

# Service control (install/remove/start/stop) needs an ELEVATED shell; without it nssm fails with
# the cryptic "OpenService(): Access is denied" (2026-07-23 owner-hit). Fail fast + say how.
$script:IsElevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
                     ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $script:IsElevated -and $Action -in @('install', 'remove', 'start', 'stop') -and $Confirm) {
    throw ("This action needs an ELEVATED PowerShell (Run as administrator). From this shell: " +
           "Start-Process powershell -Verb RunAs -ArgumentList '-NoExit','-Command'," +
           "`"cd '$RepoRoot'; scripts\nssm_install.ps1 -Action $Action -Confirm ...`"")
}

# The engine is an installed package (pyproject: hatchling wheel of src/engine, [tool.uv] package),
# so `python -m engine.ops.main` resolves from the venv without PYTHONPATH juggling. AppDirectory =
# repo root so the engine's relative config/ and data/ paths resolve (config.Settings._abs).
$AppParameters = '-m engine.ops.main'

function Resolve-Nssm {
    param([string]$Explicit)

    if (-not [string]::IsNullOrWhiteSpace($Explicit)) {
        if (-not (Test-Path -LiteralPath $Explicit)) {
            throw "nssm.exe not found at -NssmPath '$Explicit'."
        }
        return (Resolve-Path -LiteralPath $Explicit).Path
    }

    $cmd = Get-Command 'nssm.exe' -ErrorAction SilentlyContinue
    if ($null -ne $cmd) {
        return $cmd.Source
    }

    throw "nssm.exe not found. Pass -NssmPath C:\path\to\nssm.exe or add NSSM to PATH. (E7: install NSSM first.)"
}

function Test-ServiceExists {
    param([string]$Name)
    $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
    return ($null -ne $svc)
}

# Thin wrapper so every nssm call is logged and a non-zero exit is surfaced (no '&&' chaining).
function Invoke-Nssm {
    param(
        [string]$Nssm,
        [string[]]$NssmArgs
    )
    Write-Host ("    nssm " + ($NssmArgs -join ' '))
    & $Nssm @NssmArgs
    if ($LASTEXITCODE -ne 0) {
        throw "nssm $($NssmArgs -join ' ') exited with code $LASTEXITCODE"
    }
}

# --------------------------------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------------------------------

function Install-Service {
    param([string]$Nssm)

    Write-Host "== install '$ServiceName' (E7/D11; §2.2/§2.6/§10.7) =="
    Write-Host "    RepoRoot      : $RepoRoot"
    Write-Host "    Application   : $PythonExe"
    Write-Host "    AppParameters : $AppParameters"
    Write-Host "    AppDirectory  : $RepoRoot"
    Write-Host "    Stdout/Stderr : $StdoutLog | $StderrLog"
    Write-Host "    Start type    : SERVICE_DEMAND_START (manual; NOT auto-start)"
    Write-Host "    Restart       : clean exit(0)=no restart; crash=restart, throttled"

    # Preflight: the venv python must exist (owner runs `uv sync` first).
    if (-not (Test-Path -LiteralPath $PythonExe)) {
        throw "venv python not found at '$PythonExe'. Create the venv first (e.g. `uv sync`)."
    }

    if (-not $Confirm) {
        Write-Warning "Dry run: -Confirm not supplied. The above would be applied. Re-run with -Confirm to write."
        return
    }

    # Ensure log + run dirs exist (NSSM will not create them).
    foreach ($d in @($LogsDir, $RunDir)) {
        if (-not (Test-Path -LiteralPath $d)) {
            New-Item -ItemType Directory -Path $d -Force | Out-Null
        }
    }

    $exists = Test-ServiceExists -Name $ServiceName
    if ($exists) {
        Write-Host "    Service exists -> reconfiguring (idempotent)."
        # Make sure it is stopped before we rewrite its config.
        try { Invoke-Nssm -Nssm $Nssm -NssmArgs @('stop', $ServiceName) } catch { Write-Host "    (already stopped)" }
    }
    else {
        Invoke-Nssm -Nssm $Nssm -NssmArgs @('install', $ServiceName, $PythonExe, $AppParameters)
    }

    # Core program config (re-applied every run so install is idempotent).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'Application',   $PythonExe)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppParameters', $AppParameters)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppDirectory',  $RepoRoot)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'DisplayName',   'MT Engine (AI trading platform)')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'Description',   'Single-user AI trading engine (NSE/Kite). Manual/demand start per §2.6; secrets via DPAPI per D11.')

    # (1) MANUAL / DEMAND start -- never auto (§2.2/§2.6).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'Start', 'SERVICE_DEMAND_START')

    # (2) Exit handling: clean exit(0) => stay stopped; anything else => restart (crash path).
    #     NSSM cannot read a predicate, so the AppExit codes are the gate; the engine's sticky
    #     engine_lifecycle.state is its own "intend to run" record (§2.6 step 1).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppExit', 'Default', 'Restart')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppExit', '0', 'Exit')

    # (3) Restart throttle / backoff (crash-loop guard).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppThrottle',     '60000')   # <60s up == failed start
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRestartDelay', '15000')   # 15s between restart tries

    # (3b) Stop grace. NSSM's default waits only 1500 ms after the console Ctrl-C, then closes the
    #      console window -> CTRL_CLOSE_EVENT (SIGBREAK) -> Windows terminates the process. The clean
    #      shutdown (guard steps + snapshot backup + STOPPED commit) takes several seconds, so at 1.5 s
    #      every stop since 2026-09-02 died mid-backup and was recorded as a crash (observed 09-23).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStopMethodConsole', '30000')

    # (5) Rotating stdout/stderr under data\logs (E7).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStdout', $StdoutLog)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStderr', $StderrLog)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStdoutCreationDisposition', '4')  # append
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStderrCreationDisposition', '4')  # append
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateFiles',   '1')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateOnline',  '1')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateBytes',   '10485760')        # 10 MiB

    # (6) Service account (D11 -- REQUIRED, not optional): the DPAPI secrets (Kite keys, Telegram
    #     bot token, Claude OAuth) were sealed under the OWNER's Windows profile. A service left on
    #     the LocalSystem default CANNOT decrypt them -- the engine boots secret-less and freezes.
    #     NSSM grants the 'Log on as a service' right automatically when ObjectName is set.
    if ($ServiceUser) {
        $secure = Read-Host -Prompt "Windows password for $ServiceUser (used once for 'nssm set ObjectName'; not stored)" -AsSecureString
        $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                      [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'ObjectName', $ServiceUser, $plain)
        $plain = $null
    } else {
        Write-Warning ("-ServiceUser not given: service stays on LocalSystem, which CANNOT decrypt " +
                       "the owner's DPAPI secrets (D11). Pass -ServiceUser '.\<owner-account>' or run: " +
                       "nssm set $ServiceName ObjectName '.\<owner-account>' <password>")
    }

    # ----------------------------------------------------------------------------------------------
    # DOCUMENTED FALLBACK ONLY -- DO NOT UNCOMMENT BY DEFAULT (D11/§2.2/§2.4, R10/D2).
    #
    # If, and only if, the CLI's own stored login is ever unusable on this box (token injection from
    # DPAPI is not implemented), the OAuth token MAY be injected via the service environment as a
    # stopgap. This is the
    # NSSM-AppEnvironmentExtra path that D11 originally suggested and that §2.2 explicitly demotes to
    # a fallback. It is INSECURE relative to the default (the token sits in plaintext in the service
    # registry config) and risks a stray ANTHROPIC_API_KEY silently outranking it (D2). Prefer fixing
    # DPAPI. If you must use it, set it OUT-OF-BAND (read from DPAPI/keyring at install time) so the
    # literal never lands in this file or in source control:
    #
    #   # $tok = (& "$PythonExe" -c "import keyring; print(keyring.get_password('market_trading','claude_code_oauth_token'))").Trim()
    #   # Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppEnvironmentExtra', "CLAUDE_CODE_OAUTH_TOKEN=$tok")
    #
    # Note: never set ANTHROPIC_API_KEY here unless the owner has deliberately enabled pay-as-you-go
    # overflow (D6) -- the startup self-test asserts it is absent (§2.2).
    # ----------------------------------------------------------------------------------------------

    Write-Host ""
    Write-Host "Installed/updated '$ServiceName'. It will NOT auto-start (manual/demand, §2.6)."
    Write-Host "Start an active period with:  .\nssm_install.ps1 -Action start"
    Write-Host "or let scripts/schedule_tasks.ps1 wake the PC + start it for each active period (§10.7)."
}

function Remove-Service {
    param([string]$Nssm)

    Write-Host "== remove '$ServiceName' =="

    if (-not (Test-ServiceExists -Name $ServiceName)) {
        Write-Host "    Service '$ServiceName' does not exist. Nothing to do."
        return
    }

    if (-not $Confirm) {
        Write-Warning "Dry run: -Confirm not supplied. Would stop and delete '$ServiceName'. Re-run with -Confirm."
        return
    }

    # Stop first (ignore failure if already stopped), then remove with NSSM's non-interactive confirm.
    try { Invoke-Nssm -Nssm $Nssm -NssmArgs @('stop', $ServiceName) } catch { Write-Host "    (already stopped)" }
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('remove', $ServiceName, 'confirm')
    Write-Host "Removed '$ServiceName'. (DPAPI secrets are untouched -- they are not part of the service.)"
}

function Start-Engine {
    param([string]$Nssm)

    Write-Host "== start '$ServiceName' (begin an active period, §2.6) =="
    if (-not (Test-ServiceExists -Name $ServiceName)) {
        throw "Service '$ServiceName' is not installed. Run: .\nssm_install.ps1 -Action install -Confirm"
    }
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('start', $ServiceName)
    Write-Host "Started. The engine runs the §2.6 every-startup reconcile + catch-up and sends a STARTUP_REPORT."
}

function Stop-Engine {
    param([string]$Nssm)

    Write-Host "== stop '$ServiceName' (planned clean stop, §10.8) =="
    if (-not (Test-ServiceExists -Name $ServiceName)) {
        Write-Host "    Service '$ServiceName' is not installed. Nothing to stop."
        return
    }
    # A clean `nssm stop` lets the engine run its shutdown guard (flatten/verify protection,
    # snapshot backup if due) and exit 0 -> AppExit 0 Exit -> NOT auto-restarted (§2.6).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('stop', $ServiceName)
    Write-Host "Stopped cleanly. Being off between active periods is NORMAL -- no auto-restart (§2.6)."
}

function Show-Status {
    param([string]$Nssm)

    Write-Host "== status '$ServiceName' =="
    if (-not (Test-ServiceExists -Name $ServiceName)) {
        Write-Host "    NOT INSTALLED. Run: .\nssm_install.ps1 -Action install -Confirm"
        return
    }

    $svc = Get-Service -Name $ServiceName
    Write-Host ("    State        : {0}" -f $svc.Status)
    Write-Host ("    StartType    : {0}  (expected: Manual / Demand per §2.6)" -f $svc.StartType)

    # NSSM's own view of the key program settings (read-only `get`).
    foreach ($key in @('Application', 'AppParameters', 'AppDirectory', 'Start', 'AppExit', 'AppThrottle', 'AppRestartDelay', 'AppStopMethodConsole', 'AppStdout', 'AppStderr')) {
        if ($key -eq 'AppExit') {
            Write-Host "    AppExit(0)   : $(& $Nssm get $ServiceName AppExit 0)"
            Write-Host "    AppExit(*)   : $(& $Nssm get $ServiceName AppExit Default)"
        }
        else {
            Write-Host ("    {0,-12} : {1}" -f $key, (& $Nssm get $ServiceName $key))
        }
    }
}

# --------------------------------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------------------------------

$nssm = Resolve-Nssm -Explicit $NssmPath
Write-Host "nssm.exe: $nssm"
Write-Host ""

switch ($Action) {
    'install' { Install-Service -Nssm $nssm }
    'remove'  { Remove-Service  -Nssm $nssm }
    'start'   { Start-Engine    -Nssm $nssm }
    'stop'    { Stop-Engine     -Nssm $nssm }
    'status'  { Show-Status     -Nssm $nssm }
    default   { throw "Unknown action '$Action'." }
}
