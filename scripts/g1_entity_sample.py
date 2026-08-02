"""G1 entity-resolution precision sample (§8.2 / §9.1): 50 live headlines for owner review.

Pulls a stratified random sample from the live news corpus and renders a markdown checklist of
headline → resolver verdict, for the human precision audit the gate requires (≥95% of resolved
symbols correct; ambiguous names correctly UNmatched). Deterministic under ``--seed`` so the same
sample can be regenerated for scoring.

MUST run while the engine is OFF — the market store is single-writer (COMMANDS.md) and this
script opens it read-only, which still conflicts with a live writer.

Usage (engine stopped):
    uv run python scripts/g1_entity_sample.py            # writes data/reports/g1_entity_sample_<ts>.md
    uv run python scripts/g1_entity_sample.py --seed 7   # a different (still reproducible) draw
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import duckdb

from engine.core.clock import Clock
from engine.core.config import load_settings

RESOLVED_N = 25
NO_MATCH_N = 15
UNRESOLVED_N = 10


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1, help="sample seed (reproducible draws)")
    args = parser.parse_args()
    rng = random.Random(args.seed)

    settings = load_settings()
    clock = Clock()
    # Direct read-only attach (MarketStore always opens read-write; this script must never be a
    # second writer). DuckDB still takes a lock — hence the engine-off requirement above.
    conn = duckdb.connect(str(settings.duckdb_path()), read_only=True)
    try:
        # Clusters with at least one resolved in-universe symbol, joined back to one headline each.
        resolved = conn.execute(
            """
            SELECT n.title, n.source_domain, n.published_at, c.cluster_id, c.symbols, c.entities
            FROM news_clusters c JOIN news n ON n.cluster_id = c.cluster_id
            WHERE len(c.symbols) > 0
            """
        ).fetchall()
        no_match = conn.execute(
            """
            SELECT n.title, n.source_domain, n.published_at, c.cluster_id
            FROM news_clusters c JOIN news n ON n.cluster_id = c.cluster_id
            WHERE len(c.symbols) = 0 AND len(c.entities) = 0
            """
        ).fetchall()
        unresolved = conn.execute(
            """
            SELECT u.entity_text, u.reason, u.candidate_symbols, n.title, n.source_domain
            FROM unresolved_entities u
            LEFT JOIN news n ON n.cluster_id = u.cluster_id
            WHERE n.title IS NOT NULL
            """
        ).fetchall()
    finally:
        conn.close()

    def draw(rows: list, n: int) -> list:
        rows = list(rows)
        rng.shuffle(rows)
        # One headline per cluster: dedupe on the cluster/title key after the shuffle.
        seen: set = set()
        out = []
        for r in rows:
            key = r[3] if len(r) > 3 else r[0]
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            if len(out) == n:
                break
        return out

    resolved_s = draw(resolved, RESOLVED_N)
    no_match_s = draw(no_match, NO_MATCH_N)
    unresolved_s = draw(unresolved, UNRESOLVED_N)
    total = len(resolved_s) + len(no_match_s) + len(unresolved_s)

    ts = clock.now().strftime("%Y%m%dT%H%M%S")
    out_path = Path("data/reports") / f"g1_entity_sample_{ts}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"# G1 entity-resolution precision sample — {clock.now():%Y-%m-%d} (seed {args.seed}, n={total})",
        "",
        "§8.2 gate check: mark any row whose resolver verdict is WRONG. Pass bar: ≥95% correct",
        "(resolved symbols genuinely the company in the headline; ambiguous names correctly UNmatched).",
        "Reply with the row numbers that are wrong (or 'all correct').",
        "",
        "## A. Resolved — is the symbol really the company this headline is about?",
        "",
        "| # | headline | source | resolver verdict |",
        "|---|---|---|---|",
    ]
    i = 0
    for r in resolved_s:
        i += 1
        syms = ", ".join(r[4]) if r[4] else "?"
        lines.append(f"| {i} | {r[0][:110]} | {r[1]} | **{syms}** |")
    lines += [
        "",
        "## B. No match — should any of these have resolved to a universe symbol?",
        "",
        "| # | headline | source | resolver verdict |",
        "|---|---|---|---|",
    ]
    for r in no_match_s:
        i += 1
        lines.append(f"| {i} | {r[0][:110]} | {r[1]} | no match |")
    lines += [
        "",
        "## C. Recorded unresolved — was refusing to guess correct here?",
        "",
        "| # | entity text | reason | candidates | headline |",
        "|---|---|---|---|---|",
    ]
    for r in unresolved_s:
        i += 1
        cands = ", ".join(r[2]) if r[2] else "-"
        lines.append(f"| {i} | {r[0][:40]} | {r[1]} | {cands} | {(r[3] or '')[:90]} |")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {out_path}  (A resolved={len(resolved_s)}, B no-match={len(no_match_s)}, "
          f"C unresolved={len(unresolved_s)})")
    if total < 50:
        print(f"NOTE: corpus yielded only {total} rows — gate wants 50; rerun after more sessions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
