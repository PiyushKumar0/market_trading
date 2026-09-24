"""``scripts/dedupe_news.py`` keep rules: the earliest copy per (url, cluster); an unclustered copy
only when no clustered copy exists; a copy in another cluster stays."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from engine.marketdata.store import MarketStore

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "dedupe_news.py"
_spec = importlib.util.spec_from_file_location("_dedupe_news_under_test", _PATH)
dedupe = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = dedupe
_spec.loader.exec_module(dedupe)


def test_dedupe_news_keep_rules(tmp_path, clock):
    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    try:
        con = store._require_con()
        con.execute("""
            INSERT INTO news (headline_id, title, source_domain, url, published_at, cluster_id, ingested_at)
            SELECT id, 't', 'et.com', url, now(), cluster, CAST(ing AS TIMESTAMPTZ) FROM (VALUES
              ('a1', 'u1', NULL, '2026-09-01'), ('a2', 'u1', 'cA', '2026-09-02'),
              ('b1', 'u2', NULL, '2026-09-01'), ('b2', 'u2', NULL, '2026-09-02'),
              ('c1', 'u3', 'cC', '2026-09-01'), ('c2', 'u3', 'cC', '2026-09-02'), ('c3', 'u3', NULL, '2026-09-03'),
              ('d1', 'u4', 'cD', '2026-09-01'), ('d2', 'u4', 'cE', '2026-09-02')
            ) v(id, url, cluster, ing)""")
        assert dedupe.dedupe_news(con) == 4
        assert sorted(r[0] for r in con.execute("SELECT headline_id FROM news").fetchall()) == [
            "a2", "b1", "c1", "d1", "d2"]
        assert dedupe.dedupe_news(con) == 0
    finally:
        store.close()
