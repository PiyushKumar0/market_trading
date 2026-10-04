#!/usr/bin/env python
"""Delete the duplicate ``news`` rows the damaged url index let in (found 2026-09-24). Engine OFF.

    python scripts/dedupe_news.py

Per url it keeps the earliest copy in each cluster, and an unclustered copy only when no clustered
copy exists. A later copy in a DIFFERENT cluster stays: it may be the only member of that cluster.
Opening the store drops the secondary indexes first (``MarketStore.init_schema``), because a DELETE
through an index that is missing the row can fail inside DuckDB. One transaction; it rolls back if
any url or any cluster with members would disappear. Re-running is a no-op.
"""

from __future__ import annotations

import os
import sys

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import duckdb  # noqa: E402

from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402

_DUPLICATES = """
    SELECT headline_id FROM (
      SELECT headline_id, cluster_id,
             row_number() OVER (PARTITION BY url, cluster_id ORDER BY ingested_at, headline_id) AS k,
             count(cluster_id) OVER (PARTITION BY url) AS clustered_copies
      FROM news)
    WHERE k > 1 OR (cluster_id IS NULL AND clustered_copies > 0)
"""


def dedupe_news(con: duckdb.DuckDBPyConnection) -> int:
    """Delete the duplicates; returns how many. Raises (after rolling back) if a check fails."""
    urls = "SELECT count(DISTINCT url) FROM news"
    clusters = "SELECT DISTINCT cluster_id FROM news WHERE cluster_id IS NOT NULL"
    urls_before = con.execute(urls).fetchone()[0]
    clusters_before = {r[0] for r in con.execute(clusters).fetchall()}
    con.execute("BEGIN")
    try:
        deleted = con.execute(f"DELETE FROM news WHERE headline_id IN ({_DUPLICATES})").fetchone()[0]
        if con.execute(urls).fetchone()[0] != urls_before:
            raise RuntimeError("a url would disappear")
        if {r[0] for r in con.execute(clusters).fetchall()} != clusters_before:
            raise RuntimeError("a cluster would lose every member")
    except Exception:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")
    return int(deleted)


def main() -> int:
    store = MarketStore.from_settings(load_settings(), Clock()).open()
    try:
        con = store._require_con()
        before = con.execute("SELECT count(*) FROM news").fetchone()[0]
        deleted = dedupe_news(con)
        con.execute("CHECKPOINT")
        spread = con.execute(
            "SELECT count(*) FROM (SELECT url FROM news GROUP BY url HAVING count(*) > 1)"
        ).fetchone()[0]
    finally:
        store.close()
    print(f"news rows {before} -> {before - deleted} (deleted {deleted}); "
          f"urls still stored more than once, one copy per cluster: {spread}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
