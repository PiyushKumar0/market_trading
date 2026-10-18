"""Helpers shared by the loose backtest scripts (``backtest_brk20``, ``backtest_hi52``, ``backtest_tdc``,
``backtest_candles``; ``backtest_fade`` through ``backtest_tdc``).

A leaf module: it imports no sibling script, because ``research_common`` loads ``backtest_brk20`` and
``backtest_hi52`` at import time and those two import from here.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np


class DbUnopenable(RuntimeError):
    """The DuckDB file is missing, locked by the engine, or otherwise not readable."""


def open_readonly(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Attach ``db_path`` READ-ONLY, or raise :class:`DbUnopenable` with an actionable message.

    ``MarketStore.open()`` runs schema DDL and is a writer, so it is deliberately not used here.
    DuckDB takes a file lock even for a read-only attach, so a running engine makes this fail — which
    is exactly the refusal the plan asks for, rather than a half-run against a moving store.
    """
    p = Path(db_path)
    if not p.exists():
        raise DbUnopenable(
            f"no such database file: {p}\n"
            "  Pass --db with the path to market.duckdb (default: <repo>/data/market.duckdb)."
        )
    if p.is_dir():
        raise DbUnopenable(f"--db points at a directory, not a DuckDB file: {p}")
    try:
        return duckdb.connect(str(p), read_only=True)
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one clear refusal
        raise DbUnopenable(
            f"cannot open read-only: {p}\n"
            f"  reason: {type(exc).__name__}: {exc}\n"
            "  DuckDB is single-writer and takes a file lock: if the mt-engine service is running it\n"
            "  owns market.duckdb and nothing else can attach, even read-only. Stop the engine (or\n"
            "  run this when it is stopped) and retry. This script NEVER opens the store read-write."
        ) from exc


def _purged_kfold_splits(
    n_obs: int, *, n_folds: int = 6, purge: int = 5, embargo: int = 5
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Fallback for when skfolio is unavailable or its precondition cannot be met: contiguous K-fold
    test blocks with the same purge/embargo guarantee ``cpcv_splits`` enforces (train observations
    within ``purge`` before or ``embargo`` after any test block are dropped). Not combinatorial —
    K folds, not C(K, 2) splits — and labelled as such wherever it is used."""
    if n_obs < n_folds * 2:
        return []
    out: list[tuple[np.ndarray, np.ndarray]] = []
    for test in np.array_split(np.arange(n_obs), n_folds):
        if test.size == 0:
            continue
        lo, hi = int(test.min()), int(test.max())
        all_idx = np.arange(n_obs)
        train = all_idx[(all_idx < lo - purge) | (all_idx > hi + embargo)]
        if train.size == 0:
            continue
        out.append((train, test))
    return out
