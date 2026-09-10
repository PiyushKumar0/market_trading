"""Fixtures for the replay tier (WO-P3-3, 2026-09-10).

The synthetic day is written ONCE per test session (``tmp_path_factory`` at session scope): it is
~6.8 k rows through a DuckDB ``COPY`` and every test in the module replays the SAME bytes — which is
also what makes the "two runs, one digest" assertion meaningful (both runs read one identical corpus,
so any difference is the harness's, never the fixture's).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from engine.core.clock import Clock
from engine.marketdata.store import MarketStore
from tests.replay.fixtures.synthetic_day import SyntheticDay, write_synthetic_day, write_tie_day

#: The replayed day. A real 2026 trading day (Wed 2026-06-17, the same one ``tests/conftest.py``
#: freezes on) so nothing here depends on a holiday-calendar quirk — though the harness itself never
#: consults the calendar: the tick stream IS the day.
REPLAY_DAY = date(2026, 6, 17)

#: The second day of the multi-day fixture (Thu 2026-06-18 — the next session).
REPLAY_DAY_2 = date(2026, 6, 18)


@pytest.fixture(scope="session")
def synthetic_day(tmp_path_factory: pytest.TempPathFactory) -> SyntheticDay:
    root: Path = tmp_path_factory.mktemp("replay_archive")
    return write_synthetic_day(root, REPLAY_DAY)


@pytest.fixture(scope="session")
def two_day_archive(tmp_path_factory: pytest.TempPathFactory) -> tuple[SyntheticDay, SyntheticDay]:
    """ONE archive root holding two consecutive synthetic days (multi-day replay coverage).

    Its own root rather than a second day added to :func:`synthetic_day`: the single-day tests assert
    ``ticks_read`` against the whole partition set, and a stray extra date under the same root would
    make those assertions depend on which fixture ran first.
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
    """Pay DuckDB's one-off per-process costs BEFORE any test times anything (§8.4 perf smoke).

    The first ``duckdb.connect`` in a process loads the extension/catalog machinery, and the first
    ``MarketStore.open`` runs the whole §4.2 schema DDL — measured at seconds on this box, entirely
    once. Folding that into a perf assertion would make the bound a statement about interpreter
    start-up rather than about the replay loop, so it is burned here instead and the smoke test
    asserts the WARM number.
    """
    warm = MarketStore(
        tmp_path_factory.mktemp("replay_warm") / "market.duckdb",
        tmp_path_factory.mktemp("replay_warm_pq"),
        Clock(),
    )
    warm.open()
    warm.close()
