"""Shared test fixtures: a frozen IST clock, a migrated temp SQLite db, and an event bus."""

from __future__ import annotations

from datetime import datetime

import pytest

from engine.core.clock import IST, Clock
from engine.core.db import connect
from engine.core.eventbus import EventBus
from engine.core.migrations import apply_migrations

# A fixed "now" on a real 2026 trading day (Wed 2026-06-17, not a holiday in config/calendar/2026.yaml),
# inside the seeded 10:00–10:30 trade window. Frozen so the golden decision log is deterministic (§9.1).
FIXED_NOW = datetime(2026, 6, 17, 10, 5, tzinfo=IST)


@pytest.fixture
def clock() -> Clock:
    return Clock(time_source=lambda: FIXED_NOW)


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "state.db")


@pytest.fixture
def conn(db_path):
    c = connect(db_path)
    apply_migrations(c)
    yield c
    c.close()


@pytest.fixture
def bus() -> EventBus:
    return EventBus()
