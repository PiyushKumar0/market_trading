"""Fixtures for the replay tier.

The synthetic day is written ONCE per session: every test replays the SAME bytes, so any
difference between two runs is the harness's, never the fixture's.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from engine.core.clock import Clock
from engine.marketdata.store import MarketStore
from tests.replay.fixtures.synthetic_day import SyntheticDay, write_synthetic_day, write_tie_day

#: The replayed day: a real trading day (Wed 2026-06-17, the one ``tests/conftest.py`` freezes on).
#: The harness never consults the calendar: the tick stream IS the day.
REPLAY_DAY = date(2026, 6, 17)

#: The second day of the multi-day fixture (the next session).
REPLAY_DAY_2 = date(2026, 6, 18)


@pytest.fixture(scope="session")
def synthetic_day(tmp_path_factory: pytest.TempPathFactory) -> SyntheticDay:
    root: Path = tmp_path_factory.mktemp("replay_archive")
    return write_synthetic_day(root, REPLAY_DAY)


@pytest.fixture(scope="session")
def two_day_archive(tmp_path_factory: pytest.TempPathFactory) -> tuple[SyntheticDay, SyntheticDay]:
    """ONE archive root holding two consecutive synthetic days.

    Its own root: single-day tests assert ``ticks_read`` against the whole partition set, so an extra
    date under their root would make them order-dependent.
    """
    root: Path = tmp_path_factory.mktemp("replay_archive_2d")
    return write_synthetic_day(root, REPLAY_DAY), write_synthetic_day(root, REPLAY_DAY_2)


@pytest.fixture(scope="session")
def tie_day(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, date]:
    """A day whose 09:15 minute holds rows tying on ``(exchange_ts, tradingsymbol, volume, ltp)``."""
    root: Path = tmp_path_factory.mktemp("replay_archive_ties")
    root, day, _ = write_tie_day(root, REPLAY_DAY)
    return root, day


@pytest.fixture(scope="session", autouse=True)
def _warm_duckdb(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Pay DuckDB's one-off per-process costs (catalog load, schema DDL) before any test times
    anything, so the perf smoke measures the replay loop and not interpreter start-up."""
    warm = MarketStore(
        tmp_path_factory.mktemp("replay_warm") / "market.duckdb",
        tmp_path_factory.mktemp("replay_warm_pq"),
        Clock(),
    )
    warm.open()
    warm.close()
