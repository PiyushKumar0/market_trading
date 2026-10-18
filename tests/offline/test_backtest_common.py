"""scripts/backtest_common.py — the read-only DuckDB attach and the purged K-fold fallback shared by
the brk20, hi52, tdc, candles and fade backtests."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backtest_common.py"
_spec = importlib.util.spec_from_file_location("mt_backtest_common", _SCRIPT)
bc = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest_common"] = bc
_spec.loader.exec_module(bc)

from engine.marketdata.store import MarketStore  # noqa: E402


def test_open_readonly_refuses_a_missing_or_locked_database_file(tmp_path, clock):
    with pytest.raises(bc.DbUnopenable, match="no such database file"):
        bc.open_readonly(tmp_path / "nope.duckdb")
    # A running engine holds market.duckdb read-write; DuckDB then refuses even a read-only attach.
    db = tmp_path / "market.duckdb"
    holder = MarketStore(db, tmp_path / "parquet", clock).open()
    try:
        with pytest.raises(bc.DbUnopenable, match="cannot open read-only") as exc:
            bc.open_readonly(db)
    finally:
        holder.close()
    assert "mt-engine" in str(exc.value)


def test_purged_kfold_fallback_honours_purge_and_embargo():
    """The self-implemented fallback keeps train observations clear of every test block."""
    splits = bc._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)
