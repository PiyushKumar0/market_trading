"""Plan Q5.1: copy the tables the research studies read into ``data/research/market_<date>.duckdb``.

Run only while mt-engine is stopped: DuckDB holds a file lock, and the source is attached READ_ONLY
(``MarketStore.open()`` would run DDL against the live store). Exit 2 on refusal, 1 on a count mismatch.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import duckdb

# bars_1d carries the "NIFTY 50" / "INDIA VIX" index rows. symbol_isin and sector_map are the mapping
# tables the insider and sector helpers join against.
TABLES = (
    "bars_1d", "corp_actions", "universe_daily", "insider_trades", "earnings_calendar",
    "results_filings", "shp_quarterly", "symbol_isin", "sector_map",
)


def snapshot(src: Path, out: Path) -> dict[str, tuple[int, int]]:
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(out))
    try:
        con.execute(f"ATTACH '{src.as_posix()}' AS src (READ_ONLY)")
        counts = {}
        for table in TABLES:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM src.main.{table}")
            n_src = con.execute(f"SELECT count(*) FROM src.main.{table}").fetchone()[0]
            n_out = con.execute(f"SELECT count(*) FROM main.{table}").fetchone()[0]
            counts[table] = (n_src, n_out)
        con.execute("DETACH src")
        con.execute("CHECKPOINT")
        return counts
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, default=Path("data/market.duckdb"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/research"))
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args(argv)
    out = args.out_dir / f"market_{args.date}.duckdb"
    if not args.src.exists():
        print(f"REFUSED: source {args.src} not found")
        return 2
    if out.exists():
        print(f"REFUSED: {out} already exists")
        return 2
    try:
        counts = snapshot(args.src, out)
    except duckdb.Error as exc:
        out.unlink(missing_ok=True)
        print(f"REFUSED: {type(exc).__name__}: {exc} (is mt-engine stopped?)")
        return 2
    mismatched = False
    for table, (n_src, n_out) in counts.items():
        flag = "" if n_src == n_out else "  MISMATCH"
        mismatched |= n_src != n_out
        print(f"{table:<20} src={n_src:>12,} out={n_out:>12,}{flag}")
    print(f"snapshot: {out}")
    return 1 if mismatched else 0


if __name__ == "__main__":
    sys.exit(main())
