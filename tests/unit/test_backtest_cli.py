"""scripts/backtest.py argument defaults (§6.1/§8.2 G1).

The ``rsi2`` baseline is pinned to the "above a rising 50-DMA index" regime filter (§6.1 row 2); the
live ``Rsi2Scanner`` always applies it. The backtest CLI must therefore validate that SAME rule by
DEFAULT — a ``--index-symbol`` that defaults to nothing silently disables the sweep's regime gate
(``SweepRunner(index_symbol=None)``), promoting params measured on an all-regime strategy the live
scanner never runs. These tests pin the canonical default and the explicit all-regime opt-out.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_BACKTEST_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backtest.py"
_spec = importlib.util.spec_from_file_location("mt_backtest", _BACKTEST_PATH)
bt = importlib.util.module_from_spec(_spec)
sys.modules["mt_backtest"] = bt
_spec.loader.exec_module(bt)

_BASE_ARGV = ["rsi2", "--from", "2024-01-01", "--to", "2025-12-31"]


def test_default_index_symbol_engages_the_rsi2_regime_filter():
    """Default invocation resolves a real reference index, so the sweep applies the pinned
    'rising 50-DMA index' gate (SweepRunner receives a non-None index_symbol)."""
    args = bt._build_parser().parse_args(_BASE_ARGV)
    assert args.index_symbol == "NIFTY 50"
    assert args.index_symbol == bt._DEFAULT_INDEX_SYMBOL
    assert (args.index_symbol or None) is not None            # main() forwards it (filter engaged)


def test_empty_index_symbol_is_the_explicit_all_regime_optout():
    """``--index-symbol ""`` is the deliberate opt-out: main() normalizes it to None so the sweep
    runs all-regime (disclosed in the report notes), never silently by default."""
    args = bt._build_parser().parse_args([*_BASE_ARGV, "--index-symbol", ""])
    assert args.index_symbol == ""
    assert (args.index_symbol or None) is None                # the all-regime opt-out


def test_explicit_index_symbol_override_is_honored():
    args = bt._build_parser().parse_args([*_BASE_ARGV, "--index-symbol", "NIFTY BANK"])
    assert args.index_symbol == "NIFTY BANK"
