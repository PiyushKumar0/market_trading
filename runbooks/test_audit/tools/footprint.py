"""Per-test line footprints of ``src/engine`` and ``scripts/`` via ``sys.monitoring`` (Python 3.12+).

The no-install stand-in for ``pytest-cov --cov=engine --cov-context=test``: records which engine and
script lines each test's CALL phase executes (fixture setup and teardown are excluded, so session
fixtures do not skew the footprints). Every thread is observed; subprocesses are not. Paths are stored
relative to the repo root (``src/engine/...``, ``scripts/...``).

    python -m pytest -p runbooks.test_audit.tools.footprint --footprint-out=<footprints.sqlite> [args]

Mechanism: one LINE callback that records the location when a call phase is active and always returns
``DISABLE``, so a location fires at most once until ``restart_events()`` re-arms everything at the start
of the next call phase. Output: SQLite with ``files(idx, path)`` and ``footprints(nodeid, n, lines)``,
where ``lines`` is a sorted ``array('Q')`` of ``file_idx << 20 | lineno``. Query with footprint_query.py.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from array import array
from pathlib import Path

import pytest

_TOOL_NAME = "mt-test-audit-footprint"
_CANDIDATE_TOOL_IDS = (1, 3, 4)   # COVERAGE_ID first; 3 and 4 are unassigned by CPython


class _Recorder:
    def __init__(self, root: Path, out: Path) -> None:
        self.root_prefix = os.path.normcase(str(root.resolve())) + os.sep
        self.prefixes = tuple(self.root_prefix + p + os.sep for p in (os.path.join("src", "engine"), "scripts"))
        self.out = out
        self.file_idx: dict[str, int | None] = {}
        self.paths: list[str] = []
        self.recording = False
        self.current: set[int] = set()
        self.results: dict[str, array] = {}
        self.tool_id: int | None = None

    def _idx(self, filename: str) -> int | None:
        norm = os.path.normcase(os.path.abspath(filename))
        idx: int | None = None
        if norm.startswith(self.prefixes):
            idx = len(self.paths)
            self.paths.append(norm[len(self.root_prefix):].replace(os.sep, "/"))
        self.file_idx[filename] = idx
        return idx

    def on_line(self, code, line):  # sys.monitoring LINE callback
        if self.recording:
            fn = code.co_filename
            idx = self.file_idx[fn] if fn in self.file_idx else self._idx(fn)
            if idx is not None:
                self.current.add((idx << 20) | line)
        return sys.monitoring.DISABLE

    def start(self) -> None:
        mon = sys.monitoring
        for tid in _CANDIDATE_TOOL_IDS:
            try:
                mon.use_tool_id(tid, _TOOL_NAME)
            except ValueError:
                continue
            self.tool_id = tid
            break
        if self.tool_id is None:
            raise RuntimeError("footprint: no free sys.monitoring tool id")
        mon.register_callback(self.tool_id, mon.events.LINE, self.on_line)
        mon.set_events(self.tool_id, mon.events.LINE)

    def stop(self) -> None:
        if self.tool_id is None:
            return
        mon = sys.monitoring
        mon.set_events(self.tool_id, 0)
        mon.register_callback(self.tool_id, mon.events.LINE, None)
        mon.free_tool_id(self.tool_id)
        self.tool_id = None

    def write(self) -> None:
        self.out.parent.mkdir(parents=True, exist_ok=True)
        if self.out.exists():
            self.out.unlink()
        con = sqlite3.connect(self.out)
        with con:
            con.execute("CREATE TABLE files (idx INTEGER PRIMARY KEY, path TEXT NOT NULL)")
            con.execute("CREATE TABLE footprints (nodeid TEXT PRIMARY KEY, n INTEGER NOT NULL, lines BLOB NOT NULL)")
            con.executemany("INSERT INTO files VALUES (?, ?)", list(enumerate(self.paths)))
            con.executemany(
                "INSERT INTO footprints VALUES (?, ?, ?)",
                [(nid, len(a), a.tobytes()) for nid, a in self.results.items()],
            )
        con.close()


_REC: _Recorder | None = None


def pytest_addoption(parser):
    parser.addoption("--footprint-out", default=None, help="write per-test engine line footprints here")


def pytest_configure(config):
    global _REC
    out = config.getoption("--footprint-out")
    if not out:
        return
    _REC = _Recorder(Path(config.rootpath), Path(out))
    _REC.start()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    rec = _REC
    if rec is None:
        yield
        return
    sys.monitoring.restart_events()
    rec.current = set()
    rec.recording = True
    try:
        yield
    finally:
        rec.recording = False
        rec.results[item.nodeid] = array("Q", sorted(rec.current))


def pytest_unconfigure(config):
    global _REC
    rec = _REC
    if rec is None:
        return
    rec.stop()
    rec.write()
    _REC = None
