"""Phase 5 reconciliation: baseline nodeids, minus ledger removals, with ledger renames applied, must equal the
final collection. Anything else is a SILENT change and fails the check.

    python runbooks/test_audit/tools/reconcile_nodeids.py --baseline runbooks/test_audit/baseline_nodeids.txt \
        --final <file with the final `pytest --collect-only -q` nodeids> --ledger runbooks/test_audit/LEDGER.md

Ledger conventions (the `nodeid(s)` column of the Verdicts table):
- every nodeid is in backticks; a nodeid without ``[...]`` stands for all of that function's cases;
- R-codes: the listed nodeids are REMOVED;
- a row whose cell contains ``→``: nodeids before it are the old ones (removed or renamed away), nodeids after it
  the new ones (must exist) — used by P1 merges and renames;
- any other row whose nodeids are not in the baseline lists ADDED tests (must exist); rows whose nodeids are in
  the baseline and carry no ``→`` (P2-P5 repairs) change nothing in the collection.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_TICKED = re.compile(r"`([^`]+)`")


def _read_ids(path: Path) -> set[str]:
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if "::" in line}


def _expand(ids: set[str], universe: set[str]) -> set[str]:
    """A function nodeid (no ``[``) stands for every parametrized case of it in ``universe``."""
    out: set[str] = set()
    for nid in ids:
        if "[" in nid:
            out.add(nid)
            continue
        cases = {u for u in universe if u == nid or u.startswith(nid + "[")}
        out |= cases or {nid}
    return out


def _ledger_rows(path: Path):
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or line.startswith("| #") or line.startswith("|---"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4 or not cells[0].isdigit():
            continue
        yield cells[0], cells[1], cells[3]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--final", type=Path, required=True)
    ap.add_argument("--ledger", type=Path, required=True)
    a = ap.parse_args()
    baseline, final = _read_ids(a.baseline), _read_ids(a.final)
    removed: set[str] = set()
    added: set[str] = set()
    problems: list[str] = []
    for row, cell, code in _ledger_rows(a.ledger):
        if "→" in cell:
            old_part, new_part = cell.split("→", 1)
            old = _expand(set(_TICKED.findall(old_part)), baseline)
            new = _expand(set(_TICKED.findall(new_part)), final)
            removed |= old
            added |= new
            missing_new = new - final
            if missing_new:
                problems.append(f"row {row}: new nodeids not collected: {sorted(missing_new)}")
            continue
        ids = set(_TICKED.findall(cell))
        if code.startswith("R"):
            exp = _expand(ids, baseline)
            removed |= exp
            not_in_base = exp - baseline
            if not_in_base:
                problems.append(f"row {row}: removed nodeids not in the baseline: {sorted(not_in_base)}")
        else:
            new_ids = ids - baseline
            added |= _expand(new_ids, final)
    expected = (baseline - removed) | added
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
