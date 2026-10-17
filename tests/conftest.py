"""Shared test fixtures: a frozen IST clock, a migrated temp SQLite db, and an event bus."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from engine.core.clock import IST, Clock
from engine.core.config import repo_root
from engine.core.db import connect
from engine.core.eventbus import EventBus
from engine.core.migrations import apply_migrations

# Since O19 the owner changes the capital from the dashboard, which rewrites these three value lines
# in the live config/limits.yaml. The suite is written against the registration values, so a
# session-scoped copy of config/ is pinned to them and MT_CONFIG_DIR points at it: no test may
# depend on the live capital.
PINNED_CAPITAL = {"capital_base_inr": 40000, "max_deployed_capital_inr": 40000, "cnc_notional_inr": 12000}

_PIN_STATE: dict = {}


def _pin_limits_text(text: str) -> str:
    for key, value in PINNED_CAPITAL.items():
        text, n = re.subn(rf"(?m)^(\s*{key}:[ \t]*)\d+(?=[ \t]*(?:#|\r?$))", lambda m, v=value: f"{m.group(1)}{v}", text)
        assert n == 1, f"limits.yaml: expected exactly one `{key}:` line, found {n}"
    return text


def pytest_configure(config) -> None:
    """Build the pinned config copy before collection, so module-level ``config_dir()`` sees it.

    An MT_CONFIG_DIR already set by the caller is respected (nothing is pinned). Test-infrastructure
    hook: MT_PIN_SOURCE, when set, is the directory copied instead of ``repo_root()/config`` (used to
    prove the suite does not depend on the live capital, with a scratch limits.yaml at other values).
    """
    _PIN_STATE["prior_env"] = os.environ.get("MT_CONFIG_DIR")
    _PIN_STATE["tmp"] = None
    if _PIN_STATE["prior_env"]:
        return
    source = Path(os.environ.get("MT_PIN_SOURCE") or repo_root() / "config")
    tmp = tempfile.mkdtemp(prefix="mt_cfg_")
    _PIN_STATE["tmp"] = tmp
    dest = Path(tmp) / "config"
    shutil.copytree(source, dest)
    limits = dest / "limits.yaml"
    pinned = _pin_limits_text(limits.read_bytes().decode("utf-8"))
    limits.write_bytes(pinned.encode("utf-8"))
    os.environ["MT_CONFIG_DIR"] = str(dest)


def pytest_unconfigure(config) -> None:
    tmp = _PIN_STATE.get("tmp")
    if tmp is None:
        return
    prior = _PIN_STATE.get("prior_env")
    if prior is None:
        os.environ.pop("MT_CONFIG_DIR", None)
    else:
        os.environ["MT_CONFIG_DIR"] = prior
    shutil.rmtree(tmp, ignore_errors=True)

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
