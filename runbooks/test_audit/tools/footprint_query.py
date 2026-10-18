"""Query the per-test footprints written by footprint.py.

    python runbooks/test_audit/tools/footprint_query.py <db> stats
    python runbooks/test_audit/tools/footprint_query.py <db> uniques --out <csv>
    python runbooks/test_audit/tools/footprint_query.py <db> subsets --out <csv> [--file <test file>]
    python runbooks/test_audit/tools/footprint_query.py <db> check --removed <nodeid> --survivors <nodeid> ...

``uniques``: per test FUNCTION (parametrized cases unioned), the engine lines no other function covers,
globally and within its own file. Zero unique lines is the R2 necessary condition; it is not sufficient.
``subsets``: function pairs in one file where footprint(A) <= footprint(B) (A non-empty).
``check``: the R2 necessary-condition check for one removal: lines of the removed test outside the
survivors' combined footprint, with the source text of each line.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from array import array
from collections import Counter, defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]


def _load(db: Path):
    con = sqlite3.connect(db)
    paths = dict(con.execute("SELECT idx, path FROM files"))
    # The recorder can assign two indices to one path under a thread race; canonicalise by path.
    canon: dict[int, int] = {}
    first: dict[str, int] = {}
    for idx, p in sorted(paths.items()):
        canon[idx] = first.setdefault(p, idx)
    out: dict[str, frozenset[int]] = {}
    for nodeid, blob in con.execute("SELECT nodeid, lines FROM footprints"):
        a = array("Q")
        a.frombytes(blob)
        out[nodeid] = frozenset(((canon[v >> 20]) << 20) | (v & 0xFFFFF) for v in a)
    con.close()
    return paths, out


def _func(nodeid: str) -> str:
    head, sep, _ = nodeid.partition("[")
    return head if sep else nodeid


def _file(nodeid: str) -> str:
    return nodeid.split("::", 1)[0]


def _by_function(fps: dict[str, frozenset[int]]) -> dict[str, frozenset[int]]:
    acc: dict[str, set[int]] = defaultdict(set)
    for nid, s in fps.items():
        acc[_func(nid)] |= s
    return {k: frozenset(v) for k, v in acc.items()}


def _fmt(paths: dict[int, str], v: int) -> str:
    return f"{paths[v >> 20]}:{v & 0xFFFFF}"


def _src(paths: dict[int, str], v: int, cache: dict[str, list[str]]) -> str:
    rel = paths[v >> 20]
    if rel not in cache:
        try:
            cache[rel] = (_ROOT / rel).read_text(encoding="utf-8-sig").splitlines()
        except OSError:
            cache[rel] = []
    lines = cache[rel]
    n = v & 0xFFFFF
    return lines[n - 1].strip() if 0 < n <= len(lines) else "?"


def cmd_stats(paths, fps, args) -> int:
    sizes = sorted(len(s) for s in fps.values())
    empty = [nid for nid, s in fps.items() if not s]
    print(f"tests={len(fps)} files_seen={len(set(paths.values()))} empty={len(empty)}")
    if sizes:
        print(f"lines per test: min={sizes[0]} median={sizes[len(sizes) // 2]} max={sizes[-1]}")
    allcov = frozenset().union(*fps.values()) if fps else frozenset()
    print(f"engine + script lines covered by the call phases: {len(allcov)}")
    return 0


def cmd_uniques(paths, fps, args) -> int:
    funcs = _by_function(fps)
    glob = Counter(v for s in funcs.values() for v in s)
    per_file: dict[str, Counter] = defaultdict(Counter)
    for f, s in funcs.items():
        per_file[_file(f)].update(s)
    rows = []
    for f, s in funcs.items():
        u_glob = sorted(v for v in s if glob[v] == 1)
        u_file = sorted(v for v in s if per_file[_file(f)][v] == 1)
        cases = sum(1 for nid in fps if _func(nid) == f)
        rows.append((f, cases, len(s), len(u_file), len(u_glob),
                     "; ".join(_fmt(paths, v) for v in u_glob[:8])))
    rows.sort()
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["function", "cases", "lines", "unique_in_file", "unique_global", "unique_global_sample"])
        w.writerows(rows)
    print(f"functions={len(rows)} zero_unique_in_file={sum(1 for r in rows if r[3] == 0)} "
          f"zero_unique_global={sum(1 for r in rows if r[4] == 0)} empty={sum(1 for r in rows if r[2] == 0)}")
    return 0


def cmd_subsets(paths, fps, args) -> int:
    funcs = _by_function(fps)
    by_file: dict[str, list[str]] = defaultdict(list)
    for f in funcs:
        if args.file is None or _file(f) == args.file:
            by_file[_file(f)].append(f)
    rows = []
    for fname, members in by_file.items():
        for a in members:
            sa = funcs[a]
            if not sa:
                continue
            for b in members:
                if a == b:
                    continue
                sb = funcs[b]
                if sa <= sb:
                    rows.append((fname, a.split("::", 1)[1], b.split("::", 1)[1], len(sa), len(sb), sa == sb))
    rows.sort()
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["file", "subset_function", "superset_function", "lines_a", "lines_b", "equal"])
        w.writerows(rows)
    print(f"subset pairs={len(rows)} equal pairs={sum(1 for r in rows if r[5])}")
    return 0


def cmd_check(paths, fps, args) -> int:
    missing = [n for n in [args.removed, *args.survivors] if n not in fps]
    if missing:
        print("not in the footprint db:", *missing, sep="\n  ")
        return 2
    removed = fps[args.removed]
    union = frozenset().union(*(fps[s] for s in args.survivors))
    outside = sorted(removed - union)
    print(f"removed={args.removed} lines={len(removed)} survivors_union={len(union)} outside={len(outside)}")
    cache: dict[str, list[str]] = {}
    for v in outside:
        print(f"  {_fmt(paths, v)}  {_src(paths, v, cache)}")
    return 0 if not outside else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db", type=Path)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats")
    p = sub.add_parser("uniques")
    p.add_argument("--out", required=True)
    p = sub.add_parser("subsets")
    p.add_argument("--out", required=True)
    p.add_argument("--file", default=None)
    p = sub.add_parser("check")
    p.add_argument("--removed", required=True)
    p.add_argument("--survivors", nargs="+", required=True)
    args = ap.parse_args(argv)
    paths, fps = _load(args.db)
    return {"stats": cmd_stats, "uniques": cmd_uniques, "subsets": cmd_subsets, "check": cmd_check}[args.cmd](
        paths, fps, args)


if __name__ == "__main__":
    sys.exit(main())
