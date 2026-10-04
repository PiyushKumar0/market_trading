#!/usr/bin/env python
"""Build + persist the §3.2.4 universe for one date OUTSIDE the engine (owner tool, 2026-09-06).

Why it exists: the daily ``universe_build`` job is calendar-guarded at 08:30 and its output is what
the swing/batch legs, the news resolver and the risk gate read for that date. When the configured
index changes (O15: NIFTY 200 → NIFTY 500 on 2026-09-04) the first build under the new index only
happens at the next trading day's 08:30 — or, on a late boot, inside the §2.6 catch-up chain. This
script performs exactly that build now, for an explicit ``--date``, wired the way the composition
root wires it (``engine.ops.main`` ``job_universe``): MIS margins refreshed, surveillance lists
current, the instruments token map hydrated from the persisted ``instruments_daily`` snapshot (the
F&O membership check needs it — an empty map would mark every name non-F&O), then
``UniverseBuilder.build(date)``, which replace-writes the day's ``universe_daily`` rows and the
runtime index cache.

The engine must be OFF (``market.duckdb`` has a single writer). The scheduled 08:30 job on
``--date`` still runs if the engine is up then — it rebuilds the same rows (replace-write,
idempotent); nothing here touches ``job_runs``, so the catch-up machinery keeps its own view.
No Kite session is needed: the index list, margins and surveillance are public downloads and the
token map comes from the stored snapshot.

    python scripts/build_universe.py --date 2026-09-07

Exit codes: 0 = built, not degraded; 1 = built but DEGRADED (a source fell back to cache/seed, or
the build itself failed closed — read the log line); 2 = setup error (engine holds the store, no
instruments snapshot to hydrate from, bad date).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import httpx  # noqa: E402

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.broker.instruments import InstrumentStore  # noqa: E402
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.log import configure_logging  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.ops.post_login import hydrate_instruments_at_startup  # noqa: E402
from engine.universe.builder import UniverseBuilder  # noqa: E402
from engine.universe.leverage import MisLeverageIngest  # noqa: E402
from engine.universe.surveillance import SurveillanceIngest  # noqa: E402


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


async def _run(settings, clock: Clock, d: date) -> int:
    data_dir = Path(settings.resolved_data_dir())
    db_path = data_dir / "market.duckdb"
    if not db_path.exists():
        print(f"build_universe: {db_path} does not exist - wrong --config-dir?", file=sys.stderr)
        return 2
    try:
        store = MarketStore.from_settings(settings, clock).open()
    except Exception as exc:  # noqa: BLE001 - most likely the live-engine single-writer lock
        print(
            "build_universe: could not open market.duckdb - most likely locked by a running engine "
            f"(stop it first) (underlying: {type(exc).__name__}: {exc})",
            file=sys.stderr,
        )
        return 2
    try:
        instruments = InstrumentStore(clock)
        source = await hydrate_instruments_at_startup(
            instruments, store, None, session_valid=False, clock=clock
        )
        if source != "hydrated":
            print(
                f"build_universe: instruments token map not available ({source}) - the F&O check "
                "would mark every name non-F&O; run after an engine boot has persisted a snapshot",
                file=sys.stderr,
            )
            return 2
        async with httpx.AsyncClient(follow_redirects=True) as http:
            leverage = MisLeverageIngest(clock, http, data_dir / "universe" / "mis_margins.json")
            surveillance = SurveillanceIngest(clock, http, data_dir / "universe" / "surveillance.json")
            builder = UniverseBuilder(settings, store, instruments, leverage, surveillance, clock, http)
            await leverage.refresh()            # same order as job_universe (engine.ops.main)
            await surveillance.current()
            universe = await builder.build(d)
    finally:
        store.close()

    print(
        f"build_universe: {d.isoformat()} index={settings.universe.index_name} source={universe.index_source} "
        f"eligible={len(universe.eligible)} watchlist={len(universe.symbols)} "
        f"mis_candidates={len(universe.mis_candidates)} extended={len(universe.extended)} "
        f"degraded={universe.degraded}"
    )
    return 1 if universe.degraded else 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Build the universe for one date (engine OFF).")
    parser.add_argument("--date", type=_parse_date, required=True, help="YYYY-MM-DD (the session the build is for)")
    parser.add_argument("--config-dir", default=None, help="config dir override (default: repo config/)")
    args = parser.parse_args(argv)
    settings = load_settings(args.config_dir)
    return asyncio.run(_run(settings, Clock(), args.date))


if __name__ == "__main__":
    raise SystemExit(main())
