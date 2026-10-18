"""Parallelism experiment without pytest-xdist: split the test FILES over N independent pytest processes.

    python runbooks/test_audit/tools/shard_run.py -n 8 --weights <inventory.csv> --out-dir <dir> [-- <pytest args>]

Files are packed longest-first onto the least-loaded shard using the baseline per-test durations in
``inventory.csv`` (``file``, ``dur_total_s``). Each shard is a plain ``python -m pytest <files>`` with its
own junit file, run concurrently from the repo root; every process pins its own config copy (the
root conftest) and gets its own pytest basetemp, so the experiment surfaces exactly the shared state
that xdist workers would also trip over. Prints per-shard wall-clock and outcome counts.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PER_FILE_OVERHEAD_S = 0.3


def pack(weights: dict[str, float], n: int) -> list[list[str]]:
    shards: list[list[str]] = [[] for _ in range(n)]
    load = [0.0] * n
    for f, w in sorted(weights.items(), key=lambda kv: (-kv[1], kv[0])):
        i = min(range(n), key=lambda k: load[k])
        shards[i].append(f)
        load[i] += w
    return shards


def junit_counts(path: Path) -> dict[str, int]:
    out = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    if not path.exists():
        return out
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    for s in suites:
        for k in out:
            out[k] += int(s.get(k, 0))
    return out


def main() -> int:
    argv = sys.argv[1:]
    extra: list[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-n", type=int, required=True)
    ap.add_argument("--weights", required=True, help="inventory.csv with file and dur_total_s columns")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--python", default=sys.executable)
    a = ap.parse_args(argv)

    weights: dict[str, float] = defaultdict(float)
    with open(a.weights, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            weights[r["file"]] += float(r["dur_total_s"] or 0) or 0.0
    for f in weights:
        weights[f] += PER_FILE_OVERHEAD_S
    missing = [f for f in weights if not (ROOT / f).exists()]
    for f in missing:
        del weights[f]
    shards = pack(dict(weights), a.n)

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    procs = []
    t0 = time.perf_counter()
    for i, files in enumerate(shards):
        log = open(out / f"shard_{i}.out.txt", "w", encoding="utf-8")
        cmd = [a.python, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               f"--junitxml={out / f'shard_{i}.xml'}", *extra, *files]
        procs.append((i, files, log, time.perf_counter(),
                      subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                       env={**os.environ, "PYTHONIOENCODING": "utf-8"})))
    results = []
    for i, files, log, started, p in procs:
        rc = p.wait()
        log.close()
        results.append((i, rc, time.perf_counter() - started, files))
    wall = time.perf_counter() - t0

    total = defaultdict(int)
    ok = True
    for i, rc, secs, files in sorted(results):
        c = junit_counts(out / f"shard_{i}.xml")
        for k, v in c.items():
            total[k] += v
        ok = ok and rc == 0
        print(f"shard {i}: exit={rc} wall={secs:6.1f}s files={len(files):3d} est={sum(weights[f] for f in files):6.1f}s "
              f"tests={c['tests']} failures={c['failures']} errors={c['errors']} skipped={c['skipped']}")
    print(f"TOTAL wall={wall:.1f}s shards={a.n} tests={total['tests']} failures={total['failures']} "
          f"errors={total['errors']} skipped={total['skipped']} all_exit_0={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
