"""Rebuild today's ``sector_map`` snapshot outside the engine (plan section 4.4 job 13).

The job is Sunday-only in the scheduler, so a classification change (a new override, the
2026-09-21 industry fallback) otherwise waits up to a week. This runs the SAME ``SectorMapJob`` the
engine wires, against the same store, cache and industry sources.

The market store is single-writer: **stop mt-engine first** (``Stop-Service mt-engine``), run this,
start it again. Usage::

    .venv\\Scripts\\python.exe scripts\\run_sector_map.py            # as_of today
    .venv\\Scripts\\python.exe scripts\\run_sector_map.py --dry-run  # classify + report, write nothing
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import timedelta

import httpx

from engine.core.clock import Clock
from engine.core.config import load_settings, repo_root
from engine.datafeeds.sector_map import SectorMapJob
from engine.marketdata.store import MarketStore


async def _main(dry_run: bool) -> int:
    settings = load_settings()
    data_dir = settings.resolved_data_dir()
    clock = Clock()
    today = clock.today()
    store = MarketStore(data_dir / "market.duckdb", data_dir / "parquet", clock).open()
    try:
        symbols: list[str] = []
        d = today
        for _ in range(10):                      # Sunday/holiday has no universe row of its own
            symbols = store.get_batch_universe_symbols(d)
            if symbols:
                break
            d -= timedelta(days=1)
        before = store.get_sector_map(as_of=today)
        print(f"batch universe as of {d}: {len(symbols)} symbols; current snapshot {len(before)} rows, "
              f"unclassified={sum(1 for r in before if r['sector'] == 'UNCLASSIFIED')}")
        if dry_run:
            return 0
        async with httpx.AsyncClient() as http:
            job = SectorMapJob(
                store, clock, http, data_dir / "datafeeds" / "sector_lists.json",
                industry_paths=(
                    data_dir / "universe" / "index_cached.csv",
                    repo_root() / settings.universe.index_seed_path,
                ),
            )
            result = await job.run(today, universe_symbols=symbols)
        print(result.model_dump())
        after = store.get_sector_map(as_of=today)
        for sector, n in Counter(r["sector"] for r in after).most_common():
            print(f"  {n:4d} {sector}")
        return 0 if result.ok else 1
    finally:
        store.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dry-run", action="store_true", help="report the current snapshot, write nothing")
    args = ap.parse_args()
    sys.exit(asyncio.run(_main(args.dry_run)))
