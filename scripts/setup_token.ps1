#Requires -Version 5.1
# =============================================================================
# scripts/setup_token.ps1  -  Claude Agent SDK OAuth token runbook (manual helper)
#
# Plan refs: D2 (OAuth-token validity / ANTHROPIC_API_KEY outranks the token),
#            D11 (token minted by `claude setup-token` as the SERVICE user, via
#            the CLI bundled with claude-agent-sdk; one-off global install + remove
#            fallback, D1 re-verified), and §2.2 (the engine loads the token from
#            DPAPI at startup and injects it into the SDK's CLI child's process
#            environment -- the token is NEVER persisted in a user/service env var).
#
# WHAT THIS IS: a documented, manual runbook you (the owner) run ONCE as the
# Windows SERVICE account that NSSM will use for `mt-engine`, plus on the monthly
# re-mint (D2: re-mint via `claude setup-token` once the token is older than
# ~330 days; 1-year validity). It is mostly comments + a few commands. It is NOT
# called by the engine. Run it interactively, in an elevated/`runas` shell that
# is logged in as the SERVICE user, from the repo root.
#
# Windows PowerShell 5.1 safe: no `&&`, no `||`, no ternary, no `??`. Sequencing
# is `;` and conditional follow-on is `if ($?) { ... }`.
# =============================================================================

# Stop on the first hard error so a half-finished mint never looks like success.
$ErrorActionPreference = 'Stop'

Write-Host ''
Write-Host '=== Claude Agent SDK OAuth token setup (D2 / D11 / Plan section 2.2) ===' -ForegroundColor Cyan
Write-Host 'Run this AS THE SERVICE USER that NSSM uses for mt-engine. The token is'
Write-Host 'account-scoped; minting it under your interactive login will NOT help the'
Write-Host 'service. Confirm the account before continuing:' -ForegroundColor Yellow
Write-Host ("  Current user : {0}\{1}" -f $env:USERDOMAIN, $env:USERNAME)
Write-Host ''

# Resolve the repo root (this script lives in <repo>/scripts/).
$RepoRoot = Split-Path -Parent $PSScriptRoot
Write-Host ("Repo root    : {0}" -f $RepoRoot)
Write-Host ''


# -----------------------------------------------------------------------------
# STEP 1 - Ensure the venv exists (uv sync, or fall back to .venv).
# -----------------------------------------------------------------------------
# The claude-agent-sdk (and its BUNDLED Claude Code CLI -- D1 re-verified: the CLI
# ships with the package, no separate Node install) lives in the service account's
# venv (D11). `uv sync` installs from the lockfile (pins the SDK version, which
# pins the bundled CLI; auto-update disabled). If uv is unavailable, a plain
# `.venv` is acceptable for this manual mint as long as claude-agent-sdk is present.
Write-Host '--- Step 1: ensure the venv exists -------------------------------------' -ForegroundColor Green

$VenvDir    = Join-Path $RepoRoot '.venv'
$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'

$uv = $null
try { $uv = (Get-Command uv -ErrorAction Stop).Source } catch { $uv = $null }

if ($uv) {
    Write-Host ("uv found at {0}; syncing from the lockfile..." -f $uv)
    # Run uv in the repo root; do NOT prefix with cd (the working dir is already
    # the location you launched this from -- pass --directory instead).
    & $uv sync --directory $RepoRoot
    if (-not $?) { throw 'uv sync failed - resolve dependency errors before minting.' }
} else {
    Write-Host 'uv not found on PATH.' -ForegroundColor Yellow
    if (Test-Path $VenvPython) {
        Write-Host ("Using existing venv at {0}" -f $VenvDir)
    } else {
        Write-Host 'No .venv present either. Create one and install deps, e.g.:' -ForegroundColor Yellow
        Write-Host '  py -3.12 -m venv .venv'
        Write-Host '  .\.venv\Scripts\python.exe -m pip install -e .   # or: pip install claude-agent-sdk'
        throw 'No venv available - create .venv (with claude-agent-sdk) and re-run.'
    }
}
Write-Host ''


# -----------------------------------------------------------------------------
# STEP 2 - Mint the token with the Claude Code CLI bundled in claude-agent-sdk.
# -----------------------------------------------------------------------------
# The mint command is:   claude setup-token
# It opens a browser for the Anthropic OAuth consent flow and then PRINTS a
# long-lived token (CLAUDE_CODE_OAUTH_TOKEN). Do this while logged in as the
# SERVICE user so the token is bound to the account the service runs under (D11).
#
# RECORD THE INVOCATION PATH: the exact path/way the bundled CLI is invoked is
# captured during the Phase-0 smoke test (scripts/smoke_test.py asserts the
# bundled CLI works under the service account -- D1 re-verified). Use the SAME
# invocation here. The CLI ships inside the installed claude-agent-sdk package;
# below we try to locate it from the venv before falling back.
Write-Host '--- Step 2: mint the OAuth token (claude setup-token) -------------------' -ForegroundColor Green

# Choose the interpreter for locating the bundled CLI.
if ($uv) {
    # Prefer the venv python uv just synced; fall back to `uv run python`.
    if (Test-Path $VenvPython) { $Python = $VenvPython } else { $Python = $null }
} else {
    $Python = $VenvPython
}

# Try to discover the bundled CLI entry point shipped inside claude-agent-sdk.
# This mirrors what the smoke test records; the exact attribute/name is verified
# there. We only use it to print guidance + run the mint; we do not hardcode a
# brittle path.
$cliCmd = $null
if ($Python) {
    Write-Host 'Locating the bundled Claude Code CLI inside claude-agent-sdk...'
    # The package exposes a `claude` console-script in the venv Scripts dir when
    # installed; check there first (the recorded smoke-test invocation).
    $VenvClaude = Join-Path $VenvDir 'Scripts\claude.exe'
    if (Test-Path $VenvClaude) {
        $cliCmd = $VenvClaude
        Write-Host ("Found bundled CLI: {0}" -f $cliCmd)
    }
}

if (-not $cliCmd) {
    # Next: a `claude` already on PATH (e.g. a prior global install).
    try { $cliCmd = (Get-Command claude -ErrorAction Stop).Source } catch { $cliCmd = $null }
    if ($cliCmd) { Write-Host ("Found `claude` on PATH: {0}" -f $cliCmd) }
}

$DidGlobalInstall = $false
if (-not $cliCmd) {
    # FALLBACK (D1 re-verified path): the package exposed no callable CLI entry
    # point on this box. A one-off GLOBAL CLI install is used SOLELY for minting,
    # then REMOVED below in Step 2b. This keeps the service venv pinned and clean.
    Write-Host 'No bundled/`claude` CLI entry point found.' -ForegroundColor Yellow
    Write-Host 'Falling back to a ONE-OFF global CLI install (solely to mint, then removed).' -ForegroundColor Yellow
    Write-Host 'Performing: npm install -g @anthropic-ai/claude-code'
    & npm install -g '@anthropic-ai/claude-code'
    if (-not $?) { throw 'Global CLI install failed - cannot mint the token.' }
    $DidGlobalInstall = $true
    try { $cliCmd = (Get-Command claude -ErrorAction Stop).Source } catch { $cliCmd = $null }
    if (-not $cliCmd) { throw 'Global install completed but `claude` is still not on PATH.' }
    Write-Host ("Global CLI installed at: {0}" -f $cliCmd)
}

Write-Host ''
Write-Host 'Running: claude setup-token' -ForegroundColor Cyan
Write-Host '  - A browser window opens for the Anthropic OAuth consent flow.'
Write-Host '  - On success the CLI PRINTS a token beginning with CLAUDE_CODE_OAUTH_TOKEN.'
Write-Host '  - COPY the printed token value; you will paste it in Step 3.'
Write-Host '  - Do NOT commit it, log it, or set it as an env var.'
Write-Host ''
& $cliCmd setup-token

Write-Host ''
Write-Host 'If the token did not print (headless box, no browser), re-run the line'
Write-Host 'above in a desktop session as the service user. Mint must happen on a'
Write-Host 'machine where the OAuth browser flow can complete.' -ForegroundColor Yellow
Write-Host ''

# --- Step 2b: remove the one-off global CLI if we installed it (D1 re-verified) ---
if ($DidGlobalInstall) {
    Write-Host '--- Step 2b: removing the one-off global CLI (mint-only) ----------------' -ForegroundColor Green
    Write-Host 'Performing: npm uninstall -g @anthropic-ai/claude-code'
    try {
        & npm uninstall -g '@anthropic-ai/claude-code'
        if ($?) {
            Write-Host 'One-off global CLI removed. The pinned SDK-bundled CLI remains the only CLI.'
        } else {
            Write-Host 'npm uninstall reported a non-zero exit - remove it manually:' -ForegroundColor Yellow
            Write-Host '  npm uninstall -g @anthropic-ai/claude-code'
        }
    } catch {
        Write-Host 'npm uninstall failed - remove it manually:' -ForegroundColor Yellow
        Write-Host '  npm uninstall -g @anthropic-ai/claude-code'
    }
    Write-Host ''
}


# -----------------------------------------------------------------------------
# STEP 3 - Store the token in the DPAPI-encrypted protected store.
# -----------------------------------------------------------------------------
# The engine loads it from DPAPI at startup and injects it into the SDK CLI
# child's process environment (Plan section 2.2). It is NEVER written to a
# user/service env var, the registry, or NSSM AppEnvironmentExtra. The DPAPI
# blob is bound to THIS service account, which is exactly why the mint and this
# store step must run as the service user.
#
# scripts/dpapi_set.py prompts for the secret value (so the token does not land
# in shell history or the process command line) and writes the DPAPI-protected
# entry under the key `claude_code_oauth_token` (mapped to the Secrets key
# KITE_ACCESS_TOKEN-style constant CLAUDE_CODE_OAUTH_TOKEN in engine.core.secrets).
Write-Host '--- Step 3: store the token in DPAPI (do NOT use an env var) ------------' -ForegroundColor Green

if ($Python -and (Test-Path $Python)) {
    $StoreRunner = $Python
} elseif ($uv) {
    # Run through uv so the venv interpreter is used even without an explicit path.
    $StoreRunner = $uv
} else {
    throw 'No interpreter available to run scripts/dpapi_set.py - check the venv.'
}

$DpapiScript = Join-Path $RepoRoot 'scripts\dpapi_set.py'
Write-Host 'When prompted, PASTE the token value printed in Step 2, then press Enter.'
Write-Host ('Running: {0} {1} claude_code_oauth_token' -f (Split-Path -Leaf $StoreRunner), 'scripts\dpapi_set.py')
Write-Host ''

if ($StoreRunner -eq $uv) {
    & $uv run --directory $RepoRoot python $DpapiScript claude_code_oauth_token
} else {
    & $StoreRunner $DpapiScript claude_code_oauth_token
}
if ($?) {
    Write-Host ''
    Write-Host 'Token stored in DPAPI under key: claude_code_oauth_token' -ForegroundColor Green
} else {
    throw 'dpapi_set.py failed - token NOT stored. Re-run Step 3 after fixing the error.'
}
Write-Host ''


# -----------------------------------------------------------------------------
# STEP 4 - WARN: a stray ANTHROPIC_API_KEY silently OUTRANKS the OAuth token (D2).
# -----------------------------------------------------------------------------
# CRITICAL precedence note: when ANTHROPIC_API_KEY is set, the Agent SDK / CLI
# use it INSTEAD of the OAuth token -- silently, and billed as metered
# pay-as-you-go rather than against the Max-plan credit. The startup self-test
# (Plan section 3.2.12) asserts ANTHROPIC_API_KEY is ABSENT unless the owner has
# deliberately configured pay-as-you-go overflow (D6). An EMPTY value
# (ANTHROPIC_API_KEY="") still occupies the precedence slot and can break auth --
# the variable must be truly UNSET, not blanked.
#
# We check both the current process and the persisted User/Machine scopes (the
# ones a service inherits). We only WARN/diagnose here; we do not silently mutate
# machine-wide environment in a runbook.
Write-Host '--- Step 4: check for a stray ANTHROPIC_API_KEY (D2) --------------------' -ForegroundColor Green

$found = $false

# Process scope (this shell).
if ($null -ne $env:ANTHROPIC_API_KEY) {
    $found = $true
    if ($env:ANTHROPIC_API_KEY -eq '') {
        Write-Host '[Process scope] ANTHROPIC_API_KEY is set to an EMPTY string - still wins its slot.' -ForegroundColor Red
    } else {
        Write-Host '[Process scope] ANTHROPIC_API_KEY is SET - it will OUTRANK the OAuth token.' -ForegroundColor Red
    }
    Write-Host '  Unset for this shell:  Remove-Item Env:\ANTHROPIC_API_KEY'
}

# Persisted User scope.
$userKey = [Environment]::GetEnvironmentVariable('ANTHROPIC_API_KEY', 'User')
if ($null -ne $userKey) {
    $found = $true
    Write-Host '[User scope]    ANTHROPIC_API_KEY is persisted for this user.' -ForegroundColor Red
    Write-Host '  Unset (User):  [Environment]::SetEnvironmentVariable(''ANTHROPIC_API_KEY'', $null, ''User'')'
}

# Persisted Machine scope (requires elevation to clear).
$machineKey = [Environment]::GetEnvironmentVariable('ANTHROPIC_API_KEY', 'Machine')
if ($null -ne $machineKey) {
    $found = $true
    Write-Host '[Machine scope] ANTHROPIC_API_KEY is persisted machine-wide.' -ForegroundColor Red
    Write-Host '  Unset (Machine, elevated):  [Environment]::SetEnvironmentVariable(''ANTHROPIC_API_KEY'', $null, ''Machine'')'
}

if ($found) {
    Write-Host ''
    Write-Host 'ACTION REQUIRED (D2): unset ANTHROPIC_API_KEY in every scope above UNLESS you' -ForegroundColor Yellow
    Write-Host 'deliberately want metered pay-as-you-go overflow (D6). After unsetting, open a' -ForegroundColor Yellow
    Write-Host 'NEW shell so the change takes effect, then re-run this step to confirm it is gone.' -ForegroundColor Yellow
} else {
    Write-Host 'OK: ANTHROPIC_API_KEY is not set in process, User, or Machine scope.' -ForegroundColor Green
    Write-Host '    The OAuth token (CLAUDE_CODE_OAUTH_TOKEN) will be used as intended.'
}
Write-Host ''

Write-Host '=== Done. Next, as the service user: run `scripts/smoke_test.py` to verify the ===' -ForegroundColor Cyan
Write-Host '=== bundled CLI (`claude --version`, D1); add `--live-llm` for the one cheap  ===' -ForegroundColor Cyan
Write-Host '=== Haiku SDK round-trip that also spends credit + needs network (D11).       ===' -ForegroundColor Cyan
Write-Host ''
