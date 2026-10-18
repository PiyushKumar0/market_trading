"""Phase 5 reconciliation: the baseline nodeids, with the ledger rows applied in row-number order, must equal the
final collection. Anything else is a SILENT change and fails the check.

    python runbooks/test_audit/tools/reconcile_nodeids.py --baseline runbooks/test_audit/baseline_nodeids.txt \
        --final <file with the final `pytest --collect-only -q` nodeids> --ledger runbooks/test_audit/LEDGER.md

The final file must hold BOTH collections: the default run and ``pytest tests/offline --collect-only -q``.

Ledger conventions (the `nodeid(s)` column of the Verdicts table):
- every nodeid is in backticks and written in full (``tests/...::name[id]``); backticked tokens without ``::``
  (fixture names, bare case ids) are prose and ignored;
- MV rows (the old test file → the new one, both backticked): the file moved; its baseline nodeids, and every
  nodeid the ledger writes with the old path, are read under the new path;
- R-codes: the listed nodeids are REMOVED; a function nodeid without ``[...]`` stands for all of its cases at that
  row, a backticked test FILE for all of its nodeids;
- a row whose cell contains ``→``: nodeids (or a file) before it are the old ones, read like an R row; nodeids
  after it are the new ones, LITERALLY — a parametrized test lists every case, so a lost or extra case cannot
  hide behind a bare function name. P1 rows map one-for-one; MS rows keep every test name;
- any other row: nodeids already expected are repairs (a bare function name covers its cases); the rest are
  ADDED, literally. Citing a nodeid an earlier row removed is a ledger error, not a re-addition.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_TICKED = re.compile(r"`([^`]+)`")
_TEST_FILE = re.compile(r"tests/[\w/]+\.py")


def _nodeids(text: str) -> set[str]:
    """The backticked NODEIDS in a ledger cell; other backticked tokens (fixtures, files, ids) are prose."""
    return {t for t in _TICKED.findall(text) if "::" in t}


def _files(text: str) -> set[str]:
    return {t for t in _TICKED.findall(text) if _TEST_FILE.fullmatch(t)}


def _moved(nid: str, moves: dict[str, str]) -> str:
    path, sep, rest = nid.partition("::")
    return moves.get(path, path) + sep + rest


def _in_files(files: set[str], universe: set[str]) -> set[str]:
    return {u for u in universe if u.partition("::")[0] in files}


def _read_ids(path: Path) -> set[str]:
    return {line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if "::" in line}


def _expand(ids: set[str], universe: set[str]) -> set[str]:
    """A function nodeid (no ``[``) stands for every parametrized case of it in ``universe``."""
    out: set[str] = set()
    for nid in ids:
        cases = {u for u in universe if u == nid or ("[" not in nid and u.startswith(nid + "["))}
        out |= cases or {nid}
    return out


def _name(nid: str) -> str:
    return nid.partition("::")[2]


def _ledger_rows(path: Path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or line.startswith("| #") or line.startswith("|---"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 4 and cells[0].isdigit():
            rows.append((int(cells[0]), cells[1], cells[3]))
    return sorted(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--final", type=Path, required=True)
    ap.add_argument("--ledger", type=Path, required=True)
    a = ap.parse_args()
    rows = _ledger_rows(a.ledger)
    base_raw = _read_ids(a.baseline)
    problems: list[str] = []
    moves: dict[str, str] = {}
    for row, cell, code in rows:
        if code == "MV":
            old_part, new_part = cell.split("→", 1)
            (old_file,), (new_file,) = _files(old_part), _files(new_part)
            if new_file in moves.values() or _in_files({new_file}, base_raw):
                problems.append(f"row {row}: MV target {new_file} is already taken")
            moves[old_file] = new_file
    baseline = {_moved(n, moves) for n in base_raw}
    final = _read_ids(a.final)
    expected = set(baseline)
    removed: set[str] = set()
    added: set[str] = set()
    for row, cell, code in rows:
        if code == "MV":
            continue
        if "→" in cell:
            old_part, new_part = cell.split("→", 1)
            old_files = {moves.get(f, f) for f in _files(old_part)}
            old = _expand({_moved(n, moves) for n in _nodeids(old_part)}, expected) | _in_files(old_files, expected)
            new = {_moved(n, moves) for n in _nodeids(new_part)}
            if code == "P1" and len(new) != len(old):
                problems.append(f"row {row}: P1 maps {len(old)} nodeids to {len(new)}")
            if code == "MS" and {_name(n) for n in old} != {_name(n) for n in new}:
                problems.append(f"row {row}: MS renames {sorted({_name(n) for n in old} ^ {_name(n) for n in new})}")
        else:
            ids = {_moved(n, moves) for n in _nodeids(cell)}
            if code.startswith("R"):
                old = _expand(ids, expected) | _in_files({moves.get(f, f) for f in _files(cell)}, expected)
                new = set()
            else:
                old = set()
                new = {n for n in ids if not _expand({n}, expected) & expected}
                if new & removed:
                    problems.append(f"row {row}: cites nodeids an earlier row removed: {sorted(new & removed)}")
        if old - expected:
            problems.append(f"row {row}: removed nodeids not expected at that row: {sorted(old - expected)}")
        expected = (expected - old) | new
        removed, added = (removed | old) - new, (added | new) - old
    silent_removals = sorted(expected - final)
    silent_additions = sorted(final - expected)
    still_present = sorted(removed & final)
    print(f"baseline={len(baseline)} final={len(final)} ledger_removed={len(removed)} ledger_added={len(added)} "
          f"expected={len(expected)}")
    for title, items in (("missing from the final collection (silent removal or a rename not in the ledger)",
                          silent_removals),
                         ("in the final collection but not expected (an addition or rename not in the ledger)",
                          silent_additions),
                         ("ledgered as removed but still collected", still_present),
                         ("ledger problems", problems)):
        if items:
            print(f"\n{title}: {len(items)}")
            for item in items:
                print(f"  {item}")
    ok = not (silent_removals or silent_additions or still_present or problems)
    print("\nRECONCILED" if ok else "\nNOT RECONCILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
