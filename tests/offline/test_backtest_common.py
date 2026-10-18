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


@pytest.mark.parametrize("script", ["backtest_brk20", "backtest_hi52", "backtest_tdc", "backtest_candles",
                                    "backtest_fade"])
def test_every_backtest_cli_refuses_a_missing_db_and_writes_nothing(script, tmp_path, capsys, monkeypatch):
    spec = importlib.util.spec_from_file_location(f"mt_refusal_{script}", _SCRIPT.parent / f"{script}.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    out = tmp_path / "out" / "results.json"
    assert mod.main(["--db", str(tmp_path / "absent.duckdb"), "--out", str(out)]) == 2
    assert "REFUSING TO RUN" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_purged_kfold_fallback_honours_purge_and_embargo():
    """The self-implemented fallback keeps train observations clear of every test block."""
    splits = bc._purged_kfold_splits(120, n_folds=6, purge=5, embargo=5)
    assert len(splits) == 6
    for train, test in splits:
        lo, hi = int(test.min()), int(test.max())
        assert not set(train) & set(test)
        assert all(j < lo - 5 or j > hi + 5 for j in train)
