"""The composition root, composed for a test: :func:`engine.ops.main.compose` over a tmp data dir.

``composed_engine`` builds the REAL object graph the engine boots with — same settings file (a copy of
the session's pinned config), same constructors, same closures — and never boots it: nothing is
started, bound, spawned or scheduled, and the owner's credential store is never read (``FakeSecrets``).
Facts about the wiring are observed through the returned :class:`~engine.ops.main.Engine`, the job
registry, or a constructor spy (``monkeypatch.setattr(opsmain, "RiskGate", spy)`` BEFORE composing —
``compose`` looks every name up at call time).
"""

from __future__ import annotations

import asyncio
import shutil
import socket
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.db import connect
from engine.core.enums import Actor
from engine.core.migrations import apply_migrations
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import KITE_API_KEY, REQUIRED_AT_STARTUP, MissingSecretError
from engine.core.types import OwnerConfirmation
from engine.ops import main as opsmain
from tests.chaos._lifecycle_rig import MovableTime
from tests.conftest import FIXED_NOW

#: Bound on the teardown's bus drain (the engine's own is 5 s; a composed graph has nothing in flight).
_DRAIN_S = 1.0

_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})


class FakeSecrets:
    """The Credential-Manager boundary over a dict. ``engine.core.secrets.Secrets`` reads the owner's
    LIVE Kite and Telegram credentials and must never be constructed in a test."""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})

    def has(self, key: str) -> bool:
        return key in self.values

    def get(self, key: str) -> str:
        if key not in self.values:
            raise MissingSecretError(f"secret '{key}' not found in the test's FakeSecrets")
        return self.values[key]

    def get_optional(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def missing_required(self) -> list[str]:
        return [k for k in REQUIRED_AT_STARTUP if k not in self.values]


@dataclass
class Composed:
    engine: opsmain.Engine
    time: MovableTime              # the engine clock's source: ``composed.time.set(later)`` moves it
    secrets: FakeSecrets
    boot_state: dict[str, Any]
    stop_event: asyncio.Event
    #: Every non-loopback host something tried to resolve — the composition must never reach the network.
    resolved: list[str]


@asynccontextmanager
async def composed_engine(
    tmp_path: Path,
    monkeypatch: Any,
    *,
    db_template: Path | None = None,
    now: datetime = FIXED_NOW,
    kite: bool = False,
    paper: bool | None = None,
    registered: bool = True,
) -> AsyncIterator[Composed]:
    """Compose the engine over ``tmp_path/"data"`` and release everything ``compose`` opened on exit.

    ``db_template``: the session ``migrated_db_template`` fixture — copied in as the state DB so
    ``apply_migrations`` has nothing to do (``None`` migrates from scratch). ``kite``: seed a fake
    ``KITE_API_KEY`` so the broker plane (KiteClient, backfill, reconcile, holdings reconcile, the
    08:40 token check) is built; no access token, so nothing is ever authenticated. ``paper``:
    override ``paper.subsystem_enabled`` (``None`` keeps the shipped value). ``registered``: sign
    ``limits.yaml``/``envelope.yaml`` in the tmp state DB as the owner's seeding does, so the limit
    table verifies (``False``: the unregistered store a fresh install boots FROZEN on — and the paper
    stack, which prices off the verified table, then degrades to ``None``).
    """
    data_dir = tmp_path / "data"
    monkeypatch.setenv("MT_DATA_DIR", str(data_dir))
    # Registration rewrites the files it signs: a private copy of the session's pinned config.
    cfg = tmp_path / "composed_config"
    shutil.copytree(config_dir(), cfg)
    monkeypatch.setenv("MT_CONFIG_DIR", str(cfg))
    settings = load_settings()
    for p in (settings.sqlite_path(), settings.duckdb_path(), settings.backups_dir()):
        assert data_dir in p.parents, f"composed path escaped tmp: {p}"
    if paper is not None:
        settings.paper.subsystem_enabled = paper
    settings.sqlite_path().parent.mkdir(parents=True, exist_ok=True)
    if db_template is not None:
        shutil.copyfile(db_template, settings.sqlite_path())
    time = MovableTime(now)
    if registered:
        conn = connect(settings.sqlite_path())
        try:
            apply_migrations(conn)
            protected = ProtectedStore(cfg, conn, Clock(time_source=time))
            for name in ("limits.yaml", "envelope.yaml"):
                protected.register_initial(name, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))
        finally:
            conn.close()

    resolved: list[str] = []
    real_getaddrinfo = socket.getaddrinfo

    def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        name = host.decode() if isinstance(host, bytes) else str(host)
        if name not in _LOOPBACK:
            resolved.append(name)
            raise OSError(f"network access from a composed test engine: {name}")
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)

    secrets = FakeSecrets({KITE_API_KEY: "test-api-key"} if kite else None)
    boot_state: dict[str, Any] = {"engine_ready": False, "scheduler": None, "alert": None}
    stop_event = asyncio.Event()
    engine = await opsmain.compose(
        settings, secrets=secrets, clock=Clock(time_source=time), stop_event=stop_event,
        boot_state=boot_state,
    )
    async with AsyncExitStack() as stack:
        stack.callback(engine.conn.close)
        stack.callback(engine.store.close)
        stack.push_async_callback(engine.bus.drain, _DRAIN_S)
        stack.push_async_callback(engine.http.aclose)
        stack.push_async_callback(_cancel, engine.freeze_lift_tasks)
        if engine.paper is not None:
            stack.push_async_callback(engine.paper.stop)
        yield Composed(engine, time, secrets, boot_state, stop_event, resolved)
        assert resolved == [], f"the composed engine tried to reach the network: {resolved}"


async def _cancel(tasks: set[asyncio.Task]) -> None:
    for task in list(tasks):
        await opsmain.cancel_post_arm(task)
