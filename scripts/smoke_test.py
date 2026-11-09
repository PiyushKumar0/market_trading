#!/usr/bin/env python
"""Install smoke test.

The install-tier gate: in ONE venv on the pinned interpreter, imports and minimally exercises every
heavy third-party dependency, reports PASS/FAIL per item, and exits nonzero on any failure. It is a
script, so it uses print(). Each heavy import runs in its OWN subprocess so a NATIVE crash (a
C-level segfault, e.g. an OpenMP runtime conflict, which no try/except can catch) is reported as
that check's FAIL instead of killing the run; a try/except per check accumulates ordinary failures.

What it covers:
  - pinned interpreter: Python is 3.12.x (.python-version/pyproject).
  - pykiteconnect: KiteConnect + KiteTicker importable; version startswith 5.2.
  - KiteTicker reactor: construct KiteTicker + start/stop a bare Twisted reactor in a subprocess
    (offline; the live connect is the ticker's own job).
  - engine Claude Code CLI (agents.yaml llm.cli_path, else the SDK's _bundled one): `--version` exits 0.
  - vectorbt+skfolio COEXISTENCE in ONE process plus a cvxpy solve: guards the native OpenMP
    load-order conflict (0xC0000005 on Windows unless scikit-learn is imported first, enforced
    engine-side by engine._preload).
  - claude-agent-sdk: importable; the harness's knobs (model / turn / effort / allowed-tools /
    setting-source / cli_path / env / strict-MCP) are present on the options surface if
    introspectable. A REAL one-Haiku-call test needs the pinned CLI's stored login + network and is
    GUARDED behind --live-llm (default OFF) so the script never spends credit by default.
  - river, duckdb, vectorbt, skfolio, pydantic, yaml, keyring, msgpack, structlog, ntplib,
    fastapi, uvicorn, telegram (python-telegram-bot): importable, version printed.
  - zoneinfo.ZoneInfo("Asia/Kolkata"): resolves - asserts the tzdata wheel (Windows has no system
    IANA DB; core.Clock/IST depends on it).
  - DuckDB trivial query + a River incremental learn step actually EXECUTE.
  - API-key trap: ANTHROPIC_API_KEY must be ABSENT (a stray key silently bills pay-as-you-go
    alongside the Max-plan OAuth credit). Guarded by --allow-api-key.

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

# --- src/ layout shim: runnable from any working directory (the service runs from elsewhere) ---
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
    """Best-effort version string. pykiteconnect's ``__version__`` is a *submodule*, not a string:
    unwrapped here so the assertion sees "5.2.0"."""
    ver = getattr(mod, "__version__", None)
    if isinstance(ver, ModuleType):
        ver = getattr(ver, "__version__", None) or getattr(ver, "VERSION", None)
    return str(ver or getattr(mod, "VERSION", "?"))


# ---------------------------------------------------------------------------------------------
# Subprocess isolation — a native crash kills the whole interpreter and no try/except catches it,
# so every heavy import runs in its own subprocess: a crash comes back as a nonzero exit, reported
# as that check's FAIL with the crash code decoded.
# ---------------------------------------------------------------------------------------------

# Windows STATUS_* crash codes (normalized to unsigned 32-bit: Python surfaces 0xC0000005 as
# -1073741819; & 0xFFFFFFFF normalizes it).
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
    """ASSERT the pinned interpreter is 3.12.x (a stray interpreter must FAIL, not pass silently)."""
    major, minor = sys.version_info[:2]
    assert (major, minor) == (3, 12), (
        f"interpreter is {major}.{minor}.x; the project pins Python 3.12.x (.python-version / pyproject). "
        "Run via `uv run` so the pinned venv interpreter is used."
    )
    return f"Python {sys.version.split()[0]} (3.12.x as pinned)"


def check_pykiteconnect() -> str:
    """Import KiteConnect + KiteTicker; assert version startswith 5.2 (pinned 5.2.0). Isolated."""
    return _run_isolated(
        """
        import kiteconnect
        from kiteconnect import KiteConnect, KiteTicker  # noqa: F401 — import is the test
        version = _ver(kiteconnect)
        assert version.startswith("5.2"), f"pykiteconnect version {version!r} does not start with 5.2"
        print(f"v{version} (KiteConnect + KiteTicker importable)")
        """
    )


def check_kiteticker_reactor() -> str:
    """Prove the Twisted-reactor-in-a-subprocess pattern works — OFFLINE, no live connect.

    KiteTicker's Twisted reactor cannot run inside the engine's asyncio loop (hence the separate
    ``mt-ticker`` child); this constructs a KiteTicker and starts+stops a bare reactor in an isolated
    subprocess. The authenticated connect is the ticker's own responsibility.
    """
    return _run_isolated(
        """
        from kiteconnect import KiteTicker
        from twisted.internet import reactor
        kws = KiteTicker("dummy_api_key", "dummy_access_token")   # construction only — never connect()
        assert kws is not None
        reactor.callLater(0, reactor.stop)                        # stop from within, proving start/stop
        reactor.run()                                             # returns after the scheduled stop
        print("KiteTicker constructed + Twisted reactor start/stop OK (subprocess)")
        """
    )


# mirrors engine.intelligence.harness.ISOLATION_ENV (a literal: importing engine here would break the
# one-heavy-import-per-subprocess rule; tests/unit/test_agent_harness.py pins the two equal).
_ISOLATION_ENV = {
    "DISABLE_AUTOUPDATER": "1",
    "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    "ENABLE_CLAUDEAI_MCP_SERVERS": "false",
}


def _resolve_engine_cli() -> Path:
    """The CLI the engine spawns: agents.yaml ``llm.cli_path`` (the pin; a relative value is taken from
    the repo root, as ``harness.resolve_cli_path`` does), else the SDK's ``_bundled`` one. Plain yaml and
    ``find_spec`` only: importing ``engine`` (sklearn preload) or the SDK here would break the
    one-heavy-import-per-subprocess rule above."""
    import importlib.util

    import yaml

    root = Path(__file__).resolve().parent.parent
    cfg_dir = Path(os.environ["MT_CONFIG_DIR"]) if os.environ.get("MT_CONFIG_DIR") else root / "config"
    cfg = yaml.safe_load((cfg_dir / "agents.yaml").read_text(encoding="utf-8")) or {}
    raw = (cfg.get("llm") or {}).get("cli_path")
    if raw:
        path = Path(str(raw))
        return path if path.is_absolute() else root / path
    spec = importlib.util.find_spec("claude_agent_sdk")
    if spec is None or spec.origin is None:
        raise RuntimeError("claude-agent-sdk is not installed and agents.yaml sets no llm.cli_path")
    return Path(spec.origin).parent / "_bundled" / ("claude.exe" if os.name == "nt" else "claude")


def check_engine_cli() -> str:
    """The Claude Code CLI the engine spawns is invocable under this account.

    Resolved as the harness does (agents.yaml ``llm.cli_path``; the SDK's ``_bundled`` CLI only when
    that key is unset - the SDK's PATH fallback is not checked). FAILs if the file is missing or
    ``--version`` does not exit cleanly. Offline (no network/credit), so it always runs.
    """
    cli = _resolve_engine_cli()
    if not cli.is_file():
        raise RuntimeError(
            f"engine Claude Code CLI missing at {cli} (agents.yaml llm.cli_path, else the SDK's "
            "_bundled CLI) — the harness refuses to call without it."
        )
    proc = subprocess.run([str(cli), "--version"], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, (
        f"`{cli.name} --version` exit {proc.returncode}: {(proc.stderr or proc.stdout).strip()[:120]}"
    )
    return f"{cli.name} --version OK ({proc.stdout.strip()[:60]!r}) @ {cli}"


def check_claude_agent_sdk(results: Results, live_llm: bool) -> None:
    """Import the SDK and introspect the cost-control knobs.

    The real Haiku call spends credit and is attempted only under --live-llm. By default: assert
    importability and report which knobs are present on the options surface, so agents.yaml knobs can
    be confirmed to map to real SDK parameters. The SDK has no output-token cap option (agents.yaml
    ``max_output_tokens`` is not sent). Missing setting_sources, cli_path, env, a strict-MCP knob or
    an allowed-tools knob FAILS, matching the harness refusals.
    """

    def _import_and_introspect() -> str:
        import claude_agent_sdk as sdk

        version = _ver(sdk)
        exported = {n for n in dir(sdk) if not n.startswith("_")}

        # The options class is ClaudeAgentOptions (current) or ClaudeCodeOptions (older); accept either.
        options_cls = None
        for cand in ("ClaudeAgentOptions", "ClaudeCodeOptions"):
            options_cls = getattr(sdk, cand, None)
            if options_cls is not None:
                break

        has_query = "query" in exported
        assert has_query, "claude_agent_sdk.query() not found — single-shot agent invocation unavailable"

        if options_cls is None:
            # Not a failure on its own: record it so the owner can verify the knobs manually.
            return f"v{version} (query present; options class NOT found — verify knobs manually)"

        # Knobs the harness probes: setting_sources (no stray CLAUDE.md/settings), cli_path/env
        # (pinned CLI, isolation env), strict MCP (drops the account's claude.ai connectors).
        knob_fields = {
            "model": ("model",),
            "max_turns": ("max_turns",),
            "setting_sources": ("setting_sources",),
            "cli_path": ("cli_path",),
            "env": ("env",),
            "strict_mcp_config": ("strict_mcp_config", "extra_args"),
            "effort": ("effort",),
            "allowed_tools": ("allowed_tools", "tools"),
        }
        required = ("setting_sources", "cli_path", "env", "strict_mcp_config", "allowed_tools")
        annotations = getattr(options_cls, "__annotations__", {}) or {}
        # Dataclass fields land in __annotations__; also probe a default instance.
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

        absent = [k for k in required if k in missing]
        assert not absent, f"{options_cls.__name__} exposes no {', '.join(absent)} knob — the harness refuses to call"
        detail = f"v{version} ({options_cls.__name__}); knobs found: {', '.join(found) or 'none'}"
        if missing:
            detail += f"; NOT introspectable: {', '.join(missing)} (verify manually)"
        return detail

    results.check("claude_agent_sdk import + options surface", _import_and_introspect)

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
    """Opt-in: one minimal Haiku call via the SDK (spends credit); lazy so the default path never touches the network."""
    import asyncio

    import claude_agent_sdk as sdk

    options_cls = getattr(sdk, "ClaudeAgentOptions", None) or getattr(sdk, "ClaudeCodeOptions", None)
    if options_cls is None:
        raise RuntimeError("no ClaudeAgentOptions/ClaudeCodeOptions to configure a live call")

    # Minimal prompt, cheapest model, nothing loaded from disk; CLI and isolation env as harness._build_options.
    cli = _resolve_engine_cli()
    cli_kwargs: dict = {}
    fields = set(getattr(options_cls, "__dataclass_fields__", {}) or {}) | set(
        getattr(options_cls, "__annotations__", {}) or {}
    )
    if "env" in fields:
        cli_kwargs["env"] = dict(_ISOLATION_ENV)
        if "cli_path" in fields:
            cli_kwargs["cli_path"] = str(cli)
    if "strict_mcp_config" in fields:
        cli_kwargs["strict_mcp_config"] = True
    try:
        options = options_cls(model="claude-haiku-5-5", setting_sources=[], **cli_kwargs)
    except TypeError:
        options = options_cls(model="claude-haiku-5-5", **cli_kwargs)

    async def _run() -> str:
        chunks: list[str] = []
        async for message in sdk.query(prompt="Reply with the single word: ok", options=options):
            chunks.append(str(message))
        return "".join(chunks)[:80]

    out = asyncio.run(_run())
    assert out, "live Haiku call returned no content"
    return f"live call OK ({out!r}) via {cli}"


def check_zoneinfo_ist() -> str:
    """Resolve ZoneInfo('Asia/Kolkata') — asserts tzdata is installed (Windows has no system DB)."""
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
    """Import DuckDB AND execute a trivial query (minimal run, not just import)."""
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
    """Import River AND perform one incremental learn_one/predict_one step (minimal run)."""
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

    They segfault (0xC0000005) on Windows when their native OpenMP runtimes initialize in the wrong
    order; importing scikit-learn FIRST makes them coexist (engine._preload). Runs import-both plus
    a cvxpy solve in a subprocess, so a regression in the stack or the workaround shows at install time.
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
    """API-key trap: ANTHROPIC_API_KEY must be ABSENT (a stray key silently bills pay-as-you-go).

    The SDK authenticates via the Max-plan OAuth token; a co-present ANTHROPIC_API_KEY would route
    calls to metered billing. The startup self-test enforces this in production too.
    --allow-api-key is the deliberate escape hatch for the metered-key fallback.
    """
    present = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if allow_api_key:
        return "ANTHROPIC_API_KEY present" if present else "absent (allow-api-key set; either is OK)"
    assert not present, (
        "ANTHROPIC_API_KEY is set in the environment: it would silently bill pay-as-you-go "
        "alongside the Max-plan OAuth credit. Unset it, or pass --allow-api-key if using the metered "
        "fallback deliberately."
    )
    return "absent (as required)"


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
        description="Install smoke test: import + minimally run every heavy dep.",
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
        help="Do not fail when ANTHROPIC_API_KEY is present (metered-key fallback config). "
        "Default OFF — a stray key fails the check.",
    )
    args = parser.parse_args(argv)

    print("=" * 72, flush=True)
    print("Install smoke test", flush=True)
    print(f"interpreter: {sys.version.split()[0]}  ({sys.executable})", flush=True)
    print(f"flags: live_llm={args.live_llm} allow_api_key={args.allow_api_key}", flush=True)
    print("=" * 72, flush=True)

    results = Results()

    # Pinned interpreter — the install-tier gate must run on Python 3.12.x, asserted not just printed.
    results.check("python 3.12.x pinned interpreter", check_python_version)

    # Broker / market data — the load-bearing dep; version pin is asserted.
    results.check("pykiteconnect import + version", check_pykiteconnect)

    # KiteTicker Twisted-reactor-in-a-subprocess pattern — offline construct + reactor start/stop.
    results.check("KiteTicker + Twisted reactor subprocess", check_kiteticker_reactor)

    # Engine Claude Code CLI (llm.cli_path pin) invocable under this account — `--version` exits 0.
    results.check("engine Claude Code CLI --version", check_engine_cli)

    # Time discipline — tzdata presence on Windows is mandatory for core.Clock/IST.
    results.check("zoneinfo Asia/Kolkata (tzdata present)", check_zoneinfo_ist)

    # Simple importable deps (version printed; import is the check).
    for display, module_name in _SIMPLE_IMPORTS:
        results.check(f"{display} import", _make_import_check(module_name))

    # Minimal-run deps (actually execute, not just import).
    results.check("duckdb import + trivial query", check_duckdb_run)
    results.check("river import + incremental learn step", check_river_run)

    # Native OpenMP load-order landmine: vectorbt + skfolio in one process segfault unless scikit-learn
    # is imported first (engine._preload enforces this engine-side). Isolated subprocess; see the check.
    results.check("vectorbt+skfolio coexistence (native order)", check_vectorbt_skfolio_coexistence)

    # LLM SDK — import + knob introspection; live call guarded by --live-llm.
    check_claude_agent_sdk(results, live_llm=args.live_llm)

    # API-key trap — stray pay-as-you-go key.
    results.check(
        "ANTHROPIC_API_KEY absent",
        lambda: check_anthropic_api_key_absent(allow_api_key=args.allow_api_key),
    )

    return results.summary()


if __name__ == "__main__":
    raise SystemExit(main())
