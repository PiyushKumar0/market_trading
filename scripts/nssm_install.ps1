<#
.SYNOPSIS
    Install / remove / control the `mt-engine` NSSM service for the AI trading platform.

.DESCRIPTION
    Plan refs: E7 (Windows ops via NSSM, SetThreadExecutionState, power-plan), D11 (service
    account + DPAPI secrets, no secrets in repo/env), IMPLEMENTATION_PLAN.md §2.2 (process model:
    one supervised asyncio engine; manual/demand start; restart-on-failure gated to a crash during
    an active period; the engine itself loads CLAUDE_CODE_OAUTH_TOKEN from DPAPI and injects it into
    its own process env before spawning the SDK CLI child), §2.6 (operating model: the engine is up
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

    2. RESTART-ON-FAILURE IS GATED TO AN "I INTEND TO RUN" SENTINEL.
       NSSM's own exit handling distinguishes two outcomes via `AppExit`:
         - exit code 0  -> `AppExit 0 Exit`     : a CLEAN, self-initiated stop. NSSM does NOT
                                                   restart. This is the §2.6 "intentional stop ==
                                                   normal, not a crash" path (planned stop / shutdown
                                                   guard exit code, §10.8).
         - any other    -> `AppExit Default Restart` : a CRASH. NSSM restarts ONLY IF the sentinel
                                                   file (data\run\intend_to_run.flag) exists — the
                                                   engine writes it when it begins an active period
                                                   and DELETES it as the last step of a clean planned
                                                   stop. Restart is therefore gated to "a crash while
                                                   we intended to be running" (a crash during an
                                                   active period), exactly per §2.2.
       NSSM cannot read a file as a restart predicate, so the gate is EXIT-CODE-based at the NSSM
       level: `AppExit 0 Exit` (a clean self-initiated stop is honoured, not restarted) and
       `AppExit Default Restart` (a crash restarts), throttled by the crash-loop guard below. The
       `MT_RESTART_SENTINEL` path is passed via `AppEnvironmentExtra` purely so the ENGINE can read it
       on the *next* boot and, together with the sticky `engine_lifecycle` state (§2.6 step 0/1),
       decide whether the previous exit was intended — it is NOT an NSSM-level AppEvents hook (this
       script sets no `AppEvents` key). The AppExit codes are the always-present gate; the sentinel +
       sticky state are the engine-side confirmation that a restart was wanted.

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
       The engine loads CLAUDE_CODE_OAUTH_TOKEN and every other secret from DPAPI (Windows Credential
       Manager via keyring) at startup and injects the OAuth token into its OWN process env before
       spawning the SDK's bundled Claude Code CLI child. NSSM `AppEnvironmentExtra` for secrets is a
       DOCUMENTED FALLBACK ONLY (see the commented block in Install-Service) and is deliberately NOT
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
$RunDir      = Join-Path $RepoRoot 'data\run'            # sentinel lives here (§2.2 restart gate)
$Sentinel    = Join-Path $RunDir 'intend_to_run.flag'    # "I intend to run" flag (engine-managed)
$StdoutLog   = Join-Path $LogsDir 'service.out.log'
$StderrLog   = Join-Path $LogsDir 'service.err.log'

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
    Write-Host "    Restart       : clean exit(0)=no restart; crash=restart, throttled, sentinel-gated"

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
    #     The sentinel is the engine-side "I intend to run" confirmation (§2.2); NSSM cannot read a
    #     file as a predicate, so the AppExit codes are the always-present gate and the engine
    #     consumes the sentinel on next boot (§2.6 step 1).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppExit', 'Default', 'Restart')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppExit', '0', 'Exit')

    # (3) Restart throttle / backoff (crash-loop guard).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppThrottle',     '60000')   # <60s up == failed start
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRestartDelay', '15000')   # 15s between restart tries

    # (5) Rotating stdout/stderr under data\logs (E7).
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStdout', $StdoutLog)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStderr', $StderrLog)
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStdoutCreationDisposition', '4')  # append
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppStderrCreationDisposition', '4')  # append
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateFiles',   '1')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateOnline',  '1')
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppRotateBytes',   '10485760')        # 10 MiB

    # (4) Restart sentinel path is exported so the engine knows where to write/delete its flag.
    #     This is the ONLY env var we set -- it is a path, NOT a secret. Secrets stay in DPAPI.
    Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppEnvironmentExtra', "MT_RESTART_SENTINEL=$Sentinel")

    # ----------------------------------------------------------------------------------------------
    # DOCUMENTED FALLBACK ONLY -- DO NOT UNCOMMENT BY DEFAULT (D11/§2.2/§2.4, R10/D2).
    #
    # If, and only if, the DPAPI-at-startup token injection (§2.2) is ever unavailable on this box,
    # the OAuth token MAY be injected via the service environment as a stopgap. This is the
    # NSSM-AppEnvironmentExtra path that D11 originally suggested and that §2.2 explicitly demotes to
    # a fallback. It is INSECURE relative to the default (the token sits in plaintext in the service
    # registry config) and risks a stray ANTHROPIC_API_KEY silently outranking it (D2). Prefer fixing
    # DPAPI. If you must use it, set it OUT-OF-BAND (read from DPAPI/keyring at install time) so the
    # literal never lands in this file or in source control:
    #
    #   # $tok = (& "$PythonExe" -c "import keyring; print(keyring.get_password('market_trading','claude_code_oauth_token'))").Trim()
    #   # Invoke-Nssm -Nssm $Nssm -NssmArgs @('set', $ServiceName, 'AppEnvironmentExtra', "MT_RESTART_SENTINEL=$Sentinel", "CLAUDE_CODE_OAUTH_TOKEN=$tok")
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
    # A clean `nssm stop` lets the engine run its shutdown guard (flatten/verify protection, delete
    # the sentinel, snapshot backup if due) and exit 0 -> AppExit 0 Exit -> NOT auto-restarted (§2.6).
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
    foreach ($key in @('Application', 'AppParameters', 'AppDirectory', 'Start', 'AppExit', 'AppThrottle', 'AppRestartDelay', 'AppStdout', 'AppStderr')) {
        if ($key -eq 'AppExit') {
            Write-Host "    AppExit(0)   : $(& $Nssm get $ServiceName AppExit 0)"
            Write-Host "    AppExit(*)   : $(& $Nssm get $ServiceName AppExit Default)"
        }
        else {
            Write-Host ("    {0,-12} : {1}" -f $key, (& $Nssm get $ServiceName $key))
        }
    }

    if (Test-Path -LiteralPath $Sentinel) {
        Write-Host "    Sentinel     : PRESENT ($Sentinel) -> engine intends to run (crash would be restarted)"
    }
    else {
        Write-Host "    Sentinel     : absent -> a stop is treated as intentional (no restart, §2.6)"
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
