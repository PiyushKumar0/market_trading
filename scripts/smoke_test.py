#!/usr/bin/env python
"""Phase-0 install smoke test (A4, §8.1).

A CI-style runnable script that, in ONE venv on the pinned interpreter, imports and
minimally exercises every heavy third-party dependency the platform relies on, and
reports PASS/FAIL per item with a final summary and a nonzero exit on any failure.

This is the install-tier gate referenced by §8.1 / Gate G0 ("install smoke test green
on pinned Python"). It is a *script*, so it uses print() (engine modules must not — they
use engine.core.log). Each heavy third-party import runs in its OWN subprocess so that even a
NATIVE crash — a C-level segfault (e.g. an OpenMP runtime conflict), which NO Python try/except
can catch — is reported as that check's FAIL instead of hard-killing the run and masking every
check after it. A try/except per check additionally accumulates ordinary failures so one broken
dep does not mask the rest.

What it covers (plan IDs in parentheses):
  - pinned interpreter: assert Python is 3.12.x (A4 pins ONE interpreter; .python-version/pyproject).
  - pykiteconnect: KiteConnect (REST) + KiteTicker importable; version startswith 5.2 (A4).
  - KiteTicker reactor: construct KiteTicker + start/stop a bare Twisted reactor in a subprocess (A4 —
    proves the reactor-in-a-child pattern works offline; the live connect is the ticker's Phase-1 job).
  - bundled Claude Code CLI: `claude --version` from the venv/PATH exits 0 (D1 — the exact evidence
    that token-minting + SDK subprocess calls work under the service account).
  - vectorbt+skfolio COEXISTENCE: both imported into ONE process (the real backtest+optimize
    pattern) followed by a cvxpy solve — guards the native OpenMP load-order conflict that
    segfaults the interpreter on Windows (0xC0000005) unless scikit-learn is imported first
    (enforced engine-side by engine._preload). Isolated in its own subprocess.
  - claude-agent-sdk: importable; the model / max-output-token / turn / setting-source knobs
    used for cost control are present on the options surface if introspectable (D9). A REAL
    one-Haiku-call test is a SEPARATE owner step needing the OAuth token + network — it is
    GUARDED behind --live-llm (default OFF) so this script never spends credit by default.
  - river, duckdb, vectorbt, skfolio, pydantic, yaml, keyring, msgpack, structlog, ntplib,
    fastapi, uvicorn, telegram (python-telegram-bot): importable, version printed.
  - zoneinfo.ZoneInfo("Asia/Kolkata"): resolves — asserts the tzdata wheel is present, which
    Windows REQUIRES because zoneinfo has no system IANA DB (R6). The engine's only "now" is
    core.Clock/IST, which depends on this; we check it via the same sys.path the engine uses.
  - DuckDB trivial query + a River incremental learn step actually EXECUTE (A4 "minimal run",
    not just import).
  - D2 trap: ANTHROPIC_API_KEY must be ABSENT from the environment (a stray key silently bills
    pay-as-you-go alongside the Max-plan OAuth credit — T7/§3.2.12). Guarded by --allow-api-key.

Usage:
    python scripts/smoke_test.py [--live-llm] [--allow-api-key]

Exit code 0 iff every check passed; 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from types import ModuleType

# --- Make `import engine.core...` work the same way the engine does (src/ layout) -----------
# The ZoneInfo("Asia/Kolkata") check below is the exact dependency engine.core.clock relies on;
# we insert src/ so a future variant of this check could import engine.core.clock directly, and
# so the script is runnable from any working directory (the service runs from elsewhere — D10).
_SRC = Path(__file__).resolve().parent.parent / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


# ---------------------------------------------------------------------------------------------
# Tiny check harness — one row per check, accumulate failures, never abort early.
# ---------------------------------------------------------------------------------------------
class Results:
    """Accumulates PASS/FAIL lines and prints a structured report."""

    def __init__(self) -> None:
        self._rows: list[tuple[str, bool, str]] = []

    def record(self, name: str, ok: bool, detail: str = "") -> None:
        self._rows.append((name, ok, detail))
        status = "PASS" if ok else "FAIL"
        suffix = f"  {detail}" if detail else ""
        print(f"[{status}] {name}{suffix}", flush=True)

    def check(self, name: str, fn) -> bool:
        """Run fn(); PASS if it returns without raising. fn may return a detail string."""
        try:
            detail = fn()
            self.record(name, True, detail if isinstance(detail, str) else "")
            return True
        except Exception as exc:  # noqa: BLE001 — smoke test must catch everything per-check
            self.record(name, False, f"{type(exc).__name__}: {exc}")
            return False

    @property
    def failed(self) -> list[str]:
        return [name for name, ok, _ in self._rows if not ok]

    def summary(self) -> int:
        total = len(self._rows)
        n_failed = len(self.failed)
        n_passed = total - n_failed
        print("-" * 72, flush=True)
        print(f"SUMMARY: {n_passed}/{total} passed, {n_failed} failed", flush=True)
        if n_failed:
            print("FAILED CHECKS: " + ", ".join(self.failed), flush=True)
            print("RESULT: FAIL", flush=True)
        else:
            print("RESULT: PASS", flush=True)
        return 1 if n_failed else 0


def _ver(mod) -> str:
    """Best-effort version string for a module.

    Handles the pykiteconnect quirk where ``kiteconnect.__version__`` is a *submodule*
    (kiteconnect/__version__.py), not a string: unwrap it to the string it carries so the
    version assertion sees "5.2.0" rather than the module's repr.
    """
    ver = getattr(mod, "__version__", None)
    if isinstance(ver, ModuleType):
        ver = getattr(ver, "__version__", None) or getattr(ver, "VERSION", None)
    return str(ver or getattr(mod, "VERSION", "?"))


# ---------------------------------------------------------------------------------------------
# Subprocess isolation — the load-bearing part of "one broken dep must not mask the rest".
# A native crash (C-level segfault) in a compiled dependency bypasses Python exception handling
# entirely and kills the whole interpreter. The per-check try/except CANNOT catch that. So every
# heavy import below runs in its own subprocess: an isolated crash comes back as a nonzero exit and
# is reported as that check's FAIL, with the crash code decoded, leaving the rest of the run intact.
# ---------------------------------------------------------------------------------------------

# Windows STATUS_* crash codes (normalized to unsigned 32-bit). A subprocess killed by an access
# violation returns 0xC0000005 (Python surfaces it as -1073741819; & 0xFFFFFFFF normalizes it).
_NATIVE_CRASH: dict[int, str] = {
    0xC0000005: "ACCESS_VIOLATION 0xC0000005 (segfault - often a native-library/OpenMP conflict)",
    0xC000001D: "ILLEGAL_INSTRUCTION 0xC000001D",
    0xC00000FD: "STACK_OVERFLOW 0xC00000FD",
    0xC0000374: "HEAP_CORRUPTION 0xC0000374",
}

# Self-contained version extractor injected into every subprocess (mirrors _ver, including the
# pykiteconnect submodule-__version__ unwrap) so versions print consistently across isolated checks.
_VER_SNIPPET = """
from types import ModuleType as _ModuleType
def _ver(mod):
    v = getattr(mod, "__version__", None)
    if isinstance(v, _ModuleType):
        v = getattr(v, "__version__", None) or getattr(v, "VERSION", None)
    return str(v or getattr(mod, "VERSION", "?"))
"""


def _run_isolated(snippet: str, *, timeout: float = 180.0) -> str:
    """Run ``snippet`` in a fresh subprocess on THIS interpreter; return its stdout or raise.

    The snippet is prepended with a ``_ver()`` helper and dedented, then executed via ``-c``. A zero
    exit returns the trimmed stdout (used as the check's detail line). Any nonzero exit raises with a
    decoded reason — native crash code, POSIX signal, timeout, or the tail of the child's traceback —
    so the parent records a FAIL for THIS dep without the crash taking down the whole smoke test.
    """
    code = _VER_SNIPPET + "\n" + textwrap.dedent(snippet)
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"timed out after {timeout:.0f}s (possible native hang)") from None
    if proc.returncode == 0:
        return proc.stdout.strip()
    tail = (proc.stderr or proc.stdout).strip().splitlines()
    tail_s = " | ".join(tail[-3:]) if tail else "(no output)"
    crash = _NATIVE_CRASH.get(proc.returncode & 0xFFFFFFFF)
    if crash is not None:
        raise RuntimeError(f"NATIVE CRASH {crash}; last output: {tail_s}")
    if proc.returncode < 0:  # POSIX: terminated by signal N
        raise RuntimeError(f"killed by signal {-proc.returncode}; last output: {tail_s}")
    raise RuntimeError(f"exit {proc.returncode}: {tail_s}")


# ---------------------------------------------------------------------------------------------
# Individual checks. Each is a zero-arg callable that raises on failure and may return a detail.
# ---------------------------------------------------------------------------------------------
def check_python_version() -> str:
    """A4: ASSERT the pinned interpreter is 3.12.x (not merely print it).

    A4 pins ONE tested interpreter (.python-version / pyproject ``requires-python``). Running the smoke
    test — the install-tier gate — under a stray non-3.12 interpreter must FAIL, not silently pass.
    """
    major, minor = sys.version_info[:2]
    assert (major, minor) == (3, 12), (
        f"interpreter is {major}.{minor}.x; A4 pins Python 3.12.x (.python-version / pyproject). "
        "Run via `uv run` so the pinned venv interpreter is used."
    )
    return f"Python {sys.version.split()[0]} (3.12.x as pinned, A4)"


def check_pykiteconnect() -> str:
    """Import KiteConnect + KiteTicker; assert version startswith 5.2 (A4 pins 5.2.0).

    Isolated subprocess (see _run_isolated). Note ``kiteconnect.__version__`` is a *submodule*, not a
    string — the injected ``_ver`` unwraps it, so the assertion sees "5.2.0" not the module's repr.
    """
    return _run_isolated(
        """
        import kiteconnect
        from kiteconnect import KiteConnect, KiteTicker  # noqa: F401 — import is the test
        version = _ver(kiteconnect)
        assert version.startswith("5.2"), f"pykiteconnect version {version!r} does not start with 5.2 (A4)"
        print(f"v{version} (KiteConnect + KiteTicker importable)")
        """
    )


def check_kiteticker_reactor() -> str:
    """A4: prove the Twisted-reactor-in-a-subprocess pattern works — OFFLINE, no live connect.

    KiteTicker is Twisted/autobahn-based and its reactor cannot run inside the engine's asyncio loop
    (A4 forces the separate ``mt-ticker`` child). A bare import does not exercise that; this constructs
    a KiteTicker and starts+stops a bare Twisted reactor inside an isolated subprocess, proving the
    reactor loads and shuts down cleanly in a child interpreter. The authenticated connect needs a live
    token + network and is the ticker's Phase-1 responsibility.
    """
    return _run_isolated(
        """
        from kiteconnect import KiteTicker
        from twisted.internet import reactor
        kws = KiteTicker("dummy_api_key", "dummy_access_token")   # construction only — never connect()
        assert kws is not None
        reactor.callLater(0, reactor.stop)                        # stop from within, proving start/stop
        reactor.run()                                             # returns after the scheduled stop
        print("KiteTicker constructed + Twisted reactor start/stop OK (subprocess, A4)")
        """
    )


def check_bundled_cli() -> str:
    """D1: the Claude Code CLI bundled with ``claude-agent-sdk`` is invocable under this account.

    Locate the CLI the way ``setup_token.ps1`` does (venv Scripts dir, then PATH) and run ``--version``,
    asserting a clean exit — the exact D1 evidence that ``claude setup-token`` and every SDK subprocess
    call will work under the service account. Records the resolved path so the Phase-0 invocation path
    is documented. Offline (no network/credit), so it always runs.
    """
    import shutil

    venv_bin = Path(sys.executable).parent            # .venv/Scripts (win) or .venv/bin (posix)
    candidates = [venv_bin / "claude.exe", venv_bin / "claude"]
    on_path = shutil.which("claude")
    if on_path:
        candidates.append(Path(on_path))
    cli = next((c for c in candidates if c.exists()), None)
    if cli is None:
        raise RuntimeError(
            "bundled Claude Code CLI not found (looked in the venv Scripts dir + PATH). It ships with "
            "claude-agent-sdk — ensure the SDK is installed in this venv (D1)."
        )
    proc = subprocess.run([str(cli), "--version"], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, (
        f"`{cli.name} --version` exit {proc.returncode}: {(proc.stderr or proc.stdout).strip()[:120]}"
    )
    return f"{cli.name} --version OK ({proc.stdout.strip()[:60]!r}) @ {cli}"


def check_claude_agent_sdk(results: Results, live_llm: bool) -> None:
    """Import the SDK and introspect the cost-control knobs (D9).

    A real Haiku call is a SEPARATE owner step (§8.1) requiring the OAuth token + network and
    spending credit — so it is OFF by default and only attempted under --live-llm. By default we
    assert importability and report which of the model / max-output-token / max-turn / setting-source
    knobs are present on the options surface, so Phase 0 can confirm the agents.yaml knobs (D9) map
    to real SDK parameters before any agent is wired up.
    """

    def _import_and_introspect() -> str:
        import claude_agent_sdk as sdk

        version = _ver(sdk)
        exported = {n for n in dir(sdk) if not n.startswith("_")}

        # The options dataclass has been named ClaudeAgentOptions (current) and ClaudeCodeOptions
        # (older) across SDK releases; accept either so the check is resilient to the pinned version.
        options_cls = None
        for cand in ("ClaudeAgentOptions", "ClaudeCodeOptions"):
            options_cls = getattr(sdk, cand, None)
            if options_cls is not None:
                break

        has_query = "query" in exported
        assert has_query, "claude_agent_sdk.query() not found — single-shot agent invocation (D5) unavailable"

        if options_cls is None:
            # Importable + query present, but no recognizable options class. Don't fail the import
            # check on that alone — record what we saw so the owner can verify the knobs manually.
            return f"v{version} (query present; options class NOT found — verify D9 knobs manually)"

        # Cost-control knobs from D9/§5.1: explicit model, output-token cap, turn cap, and the
        # setting_sources control that prevents loading stray CLAUDE.md/settings (D10).
        knob_fields = {
            "model": ("model",),
            "max_output_tokens": ("max_output_tokens", "max_tokens"),
            "max_turns": ("max_turns",),
            "setting_sources": ("setting_sources",),
        }
        annotations = getattr(options_cls, "__annotations__", {}) or {}
        # dataclass fields land in __annotations__; also probe a default instance for attributes.
        try:
            probe = options_cls()
        except Exception:  # noqa: BLE001 — some versions require args; fall back to annotations only
            probe = None

        found: list[str] = []
        missing: list[str] = []
        for label, names in knob_fields.items():
            present = any(n in annotations for n in names) or (
                probe is not None and any(hasattr(probe, n) for n in names)
            )
            (found if present else missing).append(label)

        detail = f"v{version} ({options_cls.__name__}); knobs found: {', '.join(found) or 'none'}"
        if missing:
            detail += f"; NOT introspectable: {', '.join(missing)} (verify D9 manually)"
        return detail

    results.check("claude_agent_sdk import + options surface (D9)", _import_and_introspect)

    # The actual paid Haiku round-trip — separate owner step, opt-in only.
    if live_llm:
        results.check("claude_agent_sdk live Haiku call (--live-llm)", _live_haiku_call)
    else:
        results.record(
            "claude_agent_sdk live Haiku call",
            True,
            "SKIPPED (default; pass --live-llm + OAuth token + network to run the real call)",
        )


def _live_haiku_call() -> str:
    """Opt-in: one minimal Haiku call via the SDK (spends credit; owner step, §8.1).

    Runs the SDK's async query() to completion against Haiku with a trivial prompt. Imported and
    executed lazily so the default (no --live-llm) path never touches the network or credit.
    """
    import asyncio

    import claude_agent_sdk as sdk

    options_cls = getattr(sdk, "ClaudeAgentOptions", None) or getattr(sdk, "ClaudeCodeOptions", None)
    if options_cls is None:
        raise RuntimeError("no ClaudeAgentOptions/ClaudeCodeOptions to configure a live call")

    # Minimal, byte-stable prompt; cheapest model; no tools; load nothing from disk (D5/D10).
    try:
        options = options_cls(model="claude-haiku-4-5", setting_sources=[])
    except TypeError:
        options = options_cls(model="claude-haiku-4-5")

    async def _run() -> str:
        chunks: list[str] = []
        async for message in sdk.query(prompt="Reply with the single word: ok", options=options):
            chunks.append(str(message))
        return "".join(chunks)[:80]

    out = asyncio.run(_run())
    assert out, "live Haiku call returned no content"
    return f"live call OK ({out!r})"


def check_zoneinfo_ist() -> str:
    """Resolve ZoneInfo('Asia/Kolkata') — asserts tzdata is installed (Windows has no system DB, R6)."""
    return _run_isolated(
        """
        import sys
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Kolkata")
        # tzdata is the wheel that supplies the IANA DB on Windows; confirm it's importable too.
        try:
            import tzdata  # noqa: F401
            tzdata_ver = _ver(sys.modules["tzdata"])
        except Exception:  # noqa: BLE001 — ZoneInfo may resolve via a system DB on non-Windows
            tzdata_ver = "not-installed (ZoneInfo resolved via system DB)"
        print(f"ZoneInfo('Asia/Kolkata') -> {tz.key}; tzdata {tzdata_ver}")
        """
    )


def check_duckdb_run() -> str:
    """Import DuckDB AND execute a trivial query (A4 minimal run, not just import)."""
    return _run_isolated(
        """
        import duckdb
        con = duckdb.connect(database=":memory:")
        try:
            (value,) = con.execute("SELECT 40 + 2").fetchone()
            assert value == 42, f"DuckDB returned {value!r}, expected 42"
        finally:
            con.close()
        print(f"v{_ver(duckdb)} (SELECT 40+2 -> 42)")
        """
    )


def check_river_run() -> str:
    """Import River AND perform one incremental learn_one/predict_one step (A4 minimal run)."""
    return _run_isolated(
        """
        import river
        from river import linear_model, preprocessing
        model = preprocessing.StandardScaler() | linear_model.LinearRegression()
        # A handful of incremental updates, then a prediction — proves the online-learning path executes.
        for x_val, y_val in [({"x": 1.0}, 2.0), ({"x": 2.0}, 4.0), ({"x": 3.0}, 6.0)]:
            model.learn_one(x_val, y_val)
        pred = model.predict_one({"x": 4.0})
        assert isinstance(pred, (int, float)), f"River predict_one returned non-numeric {pred!r}"
        print(f"v{_ver(river)} (learn_one x3 + predict_one -> {pred:.3f})")
        """
    )


def check_vectorbt_skfolio_coexistence() -> str:
    """vectorbt (numba/llvmlite) + skfolio (cvxpy/native _cvxcore) in ONE process — the real engine pattern.

    These two segfault the interpreter (0xC0000005) on Windows when their native OpenMP runtimes
    initialize in the wrong order — a C-level crash no try/except can catch. Importing scikit-learn
    FIRST establishes the OpenMP runtime and makes them coexist; the engine enforces this via
    engine._preload. This check reproduces the full backtest+optimize pattern (import both, then run a
    cvxpy solve) in an isolated subprocess and asserts it survives — so a regression in the dependency
    stack, or in the sklearn-first workaround, is caught at Gate G0 instead of at runtime.
    """
    return _run_isolated(
        """
        import sklearn        # noqa: F401 — THE GUARD: establish the OpenMP runtime FIRST (engine._preload)
        import vectorbt       # noqa: F401 — numba / llvmlite (LLVM)
        import skfolio        # noqa: F401 — pulls in cvxpy / native _cvxcore
        import cvxpy
        x = cvxpy.Variable()
        cvxpy.Problem(cvxpy.Minimize((x - 3) ** 2)).solve()
        assert abs(float(x.value) - 3.0) < 1e-6, f"cvxpy solve gave {x.value!r}, expected ~3.0"
        print(f"sklearn->vectorbt->skfolio coexist + cvxpy solve OK (vbt {_ver(vectorbt)}, skf {_ver(skfolio)})")
        """
    )


def check_anthropic_api_key_absent(allow_api_key: bool) -> str:
    """D2/T7 trap: ANTHROPIC_API_KEY must be ABSENT (a stray key silently bills pay-as-you-go).

    The platform authenticates the Agent SDK via the Max-plan OAuth token (CLAUDE_CODE_OAUTH_TOKEN);
    a co-present ANTHROPIC_API_KEY would route calls to metered billing instead. The startup self-test
    (§3.2.12) enforces this in production; the smoke test enforces it here too. --allow-api-key is the
    deliberate escape hatch for the metered-key fallback configuration (§5.6/§13).
    """
    present = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if allow_api_key:
        return "ANTHROPIC_API_KEY present" if present else "absent (allow-api-key set; either is OK)"
    assert not present, (
        "ANTHROPIC_API_KEY is set in the environment (D2/T7): it would silently bill pay-as-you-go "
        "alongside the Max-plan OAuth credit. Unset it, or pass --allow-api-key if using the metered "
        "fallback deliberately."
    )
    return "absent (as required by D2)"


# Simple importable deps: (display name, module name). Version printed; import is the test.
_SIMPLE_IMPORTS: list[tuple[str, str]] = [
    ("pydantic", "pydantic"),
    ("pyyaml", "yaml"),
    ("keyring", "keyring"),
    ("msgpack", "msgpack"),
    ("structlog", "structlog"),
    ("ntplib", "ntplib"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("vectorbt", "vectorbt"),
    ("skfolio", "skfolio"),
    ("python-telegram-bot (telegram)", "telegram"),
]


def _make_import_check(module_name: str):
    def _check() -> str:
        return _run_isolated(
            f"import importlib\nprint('v' + _ver(importlib.import_module({module_name!r})))"
        )

    return _check


# ---------------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="smoke_test",
        description="Phase-0 install smoke test (A4, §8.1): import + minimally run every heavy dep.",
    )
    parser.add_argument(
        "--live-llm",
        action="store_true",
        help="Make a real one-Haiku-call SDK round-trip (spends credit; needs OAuth token + network). "
        "Default OFF — importability + knob introspection only.",
    )
    parser.add_argument(
        "--allow-api-key",
        action="store_true",
        help="Do not fail when ANTHROPIC_API_KEY is present (metered-key fallback config; §5.6/§13). "
        "Default OFF — a stray key fails the D2/T7 trap.",
    )
    args = parser.parse_args(argv)

    print("=" * 72, flush=True)
    print("Phase-0 install smoke test (A4, §8.1)", flush=True)
    print(f"interpreter: {sys.version.split()[0]}  ({sys.executable})", flush=True)
    print(f"flags: live_llm={args.live_llm} allow_api_key={args.allow_api_key}", flush=True)
    print("=" * 72, flush=True)

    results = Results()

    # Pinned interpreter — the install-tier gate must run on Python 3.12.x (A4), asserted not just printed.
    results.check("python 3.12.x pinned interpreter (A4)", check_python_version)

    # Broker / market data — the load-bearing dep; version pin is asserted (A4).
    results.check("pykiteconnect import + version (A4)", check_pykiteconnect)

    # KiteTicker Twisted-reactor-in-a-subprocess pattern (A4) — offline construct + reactor start/stop.
    results.check("KiteTicker + Twisted reactor subprocess (A4)", check_kiteticker_reactor)

    # Bundled Claude Code CLI invocable under this account (D1) — `claude --version` exits 0.
    results.check("bundled Claude Code CLI --version (D1)", check_bundled_cli)

    # Time discipline — tzdata presence on Windows is mandatory for core.Clock/IST (R6).
    results.check("zoneinfo Asia/Kolkata (tzdata present, R6)", check_zoneinfo_ist)

    # Simple importable deps (version printed; import is the check).
    for display, module_name in _SIMPLE_IMPORTS:
        results.check(f"{display} import", _make_import_check(module_name))

    # Minimal-run deps (A4: actually execute, not just import).
    results.check("duckdb import + trivial query (A4 run)", check_duckdb_run)
    results.check("river import + incremental learn step (A4 run)", check_river_run)

    # Native OpenMP load-order landmine: vectorbt + skfolio in one process segfault unless scikit-learn
    # is imported first (engine._preload enforces this engine-side). Isolated subprocess; see the check.
    results.check("vectorbt+skfolio coexistence (native order, G0)", check_vectorbt_skfolio_coexistence)

    # LLM SDK — import + knob introspection (D9); live call guarded by --live-llm.
    check_claude_agent_sdk(results, live_llm=args.live_llm)

    # D2/T7 trap — stray pay-as-you-go key.
    results.check(
        "ANTHROPIC_API_KEY absent (D2 trap)",
        lambda: check_anthropic_api_key_absent(allow_api_key=args.allow_api_key),
    )

    return results.summary()


if __name__ == "__main__":
    raise SystemExit(main())
