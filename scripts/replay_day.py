#!/usr/bin/env python
"""Replay one recorded trading day through the real bar path (WO-P3-3, §3.2.9/§8.4/§9.6).

    python scripts/replay_day.py --day YYYY-MM-DD [--symbols A,B] [--parquet-root data/parquet] \
                                 [--scratch DIR] [--out report.json]

**Safe to run beside the live engine.** It reads the §4.3 tick Parquet dataset through
:class:`~engine.paper.replay.ReplayHarness`, which uses its OWN in-memory DuckDB — ``market.duckdb``
is never opened, so the single-writer lock the trading session holds is never contended (§4.1). It
places no order, touches no broker and imports nothing from the RECOMMEND pipeline. Everything it
writes goes into a scratch directory (a temp dir by default, deleted on exit) that the harness itself
refuses to place inside the archive.

Output is the :class:`~engine.paper.replay.ReplayReport`: tick counts (read / delivered / pre-open /
post-close), bars built per day, the order-postback counts, wall time, and the §9.6 ``digest`` — the
golden-day artefact. Two runs over the same partitions must print the same digest; that equality IS
the regression test, so the digest is what to record when pinning a day.

The postback/order/fill counts are 0 from this CLI: it attaches no broker (WO-P3-3 v1 has no order
origination). They are printed anyway because a zero there is the honest statement that the digest
covered bars only — the failure this replaces was a harness that reported a bars-only digest while
looking as though it had checked orders.

Memory is O(chunk), not O(day): the harness streams the day out of DuckDB in bounded chunks, so a
full 300-symbol session-day replays in the same resident footprint as a single-symbol one.

``--symbols`` narrows the read to those partitions, which is the difference between seconds and
minutes on a full archive day (the live dataset holds ~10⁵–10⁶ per-flush fragments per day until
``compact_tick_partitions`` has run).

Exit codes: 0 = replayed (even if the day had no partitions — an empty day is an answer, not an
error); 2 = bad arguments / unreadable archive.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

from engine.paper.replay import ReplayHarness, ReplayReport


def _parse_day(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--day must be YYYY-MM-DD, got {raw!r}") from exc


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="replay_day.py",
        description="Replay one recorded tick day through BarBuilder into a scratch store.",
    )
    p.add_argument("--day", required=True, type=_parse_day, help="trading day to replay (YYYY-MM-DD)")
    p.add_argument("--symbols", default=None, help="comma-separated tradingsymbols (default: all)")
    p.add_argument(
        "--parquet-root", default="data/parquet", type=Path,
        # ASCII in --help on purpose: this console is cp1252 and renders a non-ASCII ellipsis as '?'.
        help="root of the Parquet datasets; ticks are read from <root>/ticks/date=<d>/symbol=<sym> "
             "(default: data/parquet)",
    )
    p.add_argument(
        "--scratch", default=None, type=Path,
        help="scratch dir for this run's throwaway store (default: a temp dir, removed on exit)",
    )
    p.add_argument("--out", default=None, type=Path, help="write the report as JSON to this path")
    return p


def _print_report(report: ReplayReport) -> None:
    print(f"day(s)            : {', '.join(d.isoformat() for d in report.days)}")
    print(f"symbols           : {', '.join(report.symbols) if report.symbols else 'all'}")
    print(f"ticks read        : {report.ticks_read:,}")
    print(f"  delivered       : {report.ticks_delivered:,}  (in-session)")
    print(f"  pre-open        : {report.pre_open_excluded:,}  (A14: no bar, carries the auction open)")
    print(f"  post-close      : {report.post_close_dropped:,}  (WO-5: no bar)")
    for day, count in report.bars_built.items():
        print(f"bars built {day}: {count:,}")
    # Zero across the board until a broker is attached (v1 has no order origination) - the counts are
    # here so a wired-broker replay reports what the digest actually covers (WO-P3-3 fix, 2026-09-10).
    print(f"postbacks         : {report.postbacks:,}  (order.update frames collected)")
    print(f"  orders          : {report.orders:,}")
    print(f"  fills           : {report.fills:,}")
    print(f"elapsed           : {report.elapsed_s:.2f}s")
    print(f"digest            : {report.digest}")


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None

    root: Path = args.parquet_root
    if not root.is_dir():
        print(f"replay_day: parquet root not found: {root}", file=sys.stderr)
        return 2

    # A caller-supplied --scratch is used as given (the harness rejects one inside the archive); the
    # default is a temp dir removed on exit, so a routine replay leaves nothing behind.
    with tempfile.TemporaryDirectory(prefix="mt-replay-") as tmp:
        scratch = args.scratch if args.scratch is not None else Path(tmp)
        try:
            harness = ReplayHarness(root, scratch)
        except ValueError as exc:
            print(f"replay_day: {exc}", file=sys.stderr)
            return 2
        try:
            report = asyncio.run(harness.run([args.day], symbols=symbols))
        finally:
            harness.close()

    _print_report(report)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(report.model_dump_json(indent=2), encoding="utf-8")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
