"""Ticker subprocess supervisor.

``TickerSupervisor`` owns the **mt-ticker** Twisted child process (``ticker/main.py``) and the localhost TCP
link it ships data on. The Twisted reactor cannot be restarted in-process, so the ticker is a separate
process and a feed respawn kills and re-launches the child. This module is the only thing that spawns /
terminates it and the single owner of its health signal.

Data path:
    KiteTicker → mt-ticker child → length-prefixed msgpack frames on 127.0.0.1:<tcp_port> → here
(tick → :class:`~engine.core.types.Tick` → ``bus.publish("tick", …)``; order → ``order.update``; heartbeat
→ health). Ticks, order updates and a 1 s heartbeat share one socket. Order updates drive the OMS state
machine, so a fabricated frame must not be able to inject a phantom fill — hence the trust boundary.

Trust boundary:
    * the TCP socket binds ``127.0.0.1`` only;
    * a per-spawn **shared-secret handshake**: a fresh random secret handed to the child out-of-band
      (env/stdin), which must echo it on its first frame before any tick/order frame is accepted;
    * a **parent-PID check**: the child exits if orphaned, and the supervisor closes the child's stdin on
      stop so an orphaned child dies on its own.

Health / stale-data guard:
    ``health()`` reports a :class:`FeedHealth` whose ``state`` drives the risk gate's ``stale_data_guard``:
        * ``STOPPED``  — no child running.
        * ``WARMING``  — child just spawned / reconnecting + warm-up backfilling; the HEALTHY-path
          heartbeat-silence kill is **suppressed** on a fresh start. The suppression is **bounded**: WARMING
          with no heartbeat past ``settings.ticker.warming_timeout_s`` ⇒ kill + respawn with capped
          exponential backoff, else the state machine could freeze forever. This is also the generic
          system-resume recovery: whatever state the machine froze in, the HEALTHY-path stale kill or this
          WARMING timeout fires and respawns.
        * ``HEALTHY``  — FRAMES arriving within the silence budget (a tick counts).
        * ``STALE``    — frame silence exceeded ``settings.ticker.heartbeat_silence_kill_s`` (10 s) while
          *running* (not WARMING) ⇒ kill + respawn and publish a ``feed.health`` transition to ``STALE``.
          Recoverable: the very next frame restores HEALTHY.

Liveness truth:
    Liveness is ``_last_frame_at``, stamped on **every** frame in :meth:`_handle_frame` — tick, order,
    heartbeat, even an undecodable one. Bytes off the wire ARE the proof the child is alive; the heartbeat
    is a *supplement* for a quiet tape. Stamping the heartbeat alone produced multi-hour false-STALE wedges
    while ticks kept flowing and bars kept being written. Two further guards:
      * **Loop-starvation discount.** The monitor shares the event loop with the frame reader. A long
        synchronous stretch elsewhere in the engine blocks the loop, so NOTHING is read and a wall-clock
        "silence" across it is evidence about the ENGINE, not the child. The monitor accumulates its own
        wake-up overshoot (``loop_starved_s``, reset whenever a frame lands) and subtracts it before
        judging. Ages stay on the wall clock; only the verdict is discounted.
      * **The kill is real.** :meth:`_respawn` must never self-deadlock (see its docstring): a deadlock
        there silently turns "kill after 10 s" into no restart at all.

Subscription set:
    universe watchlist + held symbols + **NIFTY 50 index** + **India VIX** tokens (the index/VIX feed powers
    market-context features and the stale-data guard). Changes go over a **control frame** on the live TCP
    link via :meth:`update_subscriptions`, not a respawn.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets as _secrets
import struct
import sys
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgpack
from pydantic import BaseModel

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings

# The order-postback wire contract lives in ``engine.core.contracts`` (the paper broker emits it too and
# depends on ``core`` alone); re-exported here under the same names.
from engine.core.contracts import ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Session, Tick
from engine.notify.catalog import CatalogMessage, feed_degraded, feed_silent_unrecovered, feed_wedged

#: Owner-notification sink type: async, consumes a typed :class:`CatalogMessage`.
NotifyFn = Callable[[CatalogMessage], Awaitable[None]]

_log = get_logger("engine.broker.ticker_supervisor")

#: Canonical event bus topic for feed-health transitions.
FEED_HEALTH_TOPIC = "feed.health"
#: Canonical event bus topic for parsed live ticks — consumed by ``BarBuilder``.
TICK_TOPIC = "tick"

#: Length prefix on every frame: 4-byte big-endian unsigned int (mirror of ticker/main.py).
_LEN_PREFIX = struct.Struct(">I")
#: Wire-protocol version we accept from the child's ``hello`` (ticker/main.py ``_PROTOCOL_VERSION``).
PROTOCOL_VERSION = 1

#: Floor for a believable ``exchange_timestamp``. A zeroed/absent broker value arrives as Unix epoch 0
#: (kiteconnect → ``datetime.fromtimestamp(0)``) and would otherwise pass every layer (the Tick validator
#: only checks tz-awareness) into a ``date=1970-01-01`` tick partition. Anything below this is dropped via
#: its OWN ``implausible_timestamp`` path, distinct from a MISSING timestamp, with one WARNING and no
#: traceback (pre-market, not-yet-traded instruments send this on every resubscribe). Far below any real
#: data and far above epoch, so it only catches garbage.
_MIN_PLAUSIBLE_EXCHANGE_TS = datetime(2020, 1, 1, 0, 0, tzinfo=IST)

#: Monitor-cycle overshoot below which a late wake-up is scheduler jitter, not starvation; only overshoot
#: beyond this is credited against a frame silence. Well under ``heartbeat_silence_kill_s`` (10 s) so jitter
#: can never accumulate into a free pass.
_MONITOR_OVERSHOOT_FLOOR_S = 0.5

#: Cap on ``server.wait_closed()`` during read-loop teardown. On CPython ≥3.12 it waits for every accepted
#: connection to drop, so an unbounded await hangs the respawn behind a child that has not exited. Callers
#: terminate the child first, so this only fires for a child that ignored SIGTERM and SIGKILL (and logs).
_SERVER_CLOSE_TIMEOUT_S = 5.0


class _ImplausibleTimestamp(ValueError):
    """A wire ``exchange_timestamp`` below :data:`_MIN_PLAUSIBLE_EXCHANGE_TS` (epoch-0 zeroed field), NOT a
    missing one. Caught in :meth:`TickerSupervisor._parse_tick` BEFORE the generic parse-error handler so
    this expected drop gets its own reason/counter (``implausible_timestamp``) and no ERROR traceback; the
    WARNING already fired in :func:`_wire_timestamp`."""


def _wire_decimal(value: Any) -> Decimal | None:
    """A wire decimal-string (or number) → ``Decimal``; None/empty passes through."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _wire_timestamp(value: Any, symbol: str | None = None) -> datetime | None:
    """A wire ISO-8601 NAIVE-IST timestamp → tz-aware IST ``datetime``.

    We attach ``Asia/Kolkata`` to KiteTicker's naive IST wall time; a tz-aware value is converted, not
    re-stamped. A value below :data:`_MIN_PLAUSIBLE_EXCHANGE_TS` raises :class:`_ImplausibleTimestamp`;
    a missing/empty value returns None.
    """
    if value is None or value == "":
        return None
    ts = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(ts, datetime):
        return None
    ts = ts.replace(tzinfo=IST) if ts.tzinfo is None else ts.astimezone(IST)
    if ts < _MIN_PLAUSIBLE_EXCHANGE_TS:
        _log.warning("tick_timestamp_implausible", symbol=symbol, parsed=ts.isoformat())
        raise _ImplausibleTimestamp(f"tick exchange_timestamp implausible: {ts.isoformat()}")
    return ts


def parse_tick_frame(frame: dict[str, Any], tradingsymbol: str) -> Tick:
    """Parse one PINNED wire tick frame (ticker/main.py ``_frame_tick``) into a core ``Tick``.

    The exact mirror of the child's serializer: prices re-wrap to ``Decimal`` exactly; ``volume_traded`` is
    the CUMULATIVE day volume, verbatim; ``tradingsymbol`` is resolved by the caller (the wire carries only
    the token). Raises ``ValueError`` on a frame missing a load-bearing field, and
    :class:`_ImplausibleTimestamp` (a ``ValueError`` subclass, its OWN drop path) on an epoch-0 timestamp.
    Either way the caller drops that single tick and never crashes the read loop.
    """
    ltp = _wire_decimal(frame.get("last_price"))
    if ltp is None:
        raise ValueError("tick frame missing last_price")
    exchange_ts = _wire_timestamp(frame.get("exchange_timestamp"), tradingsymbol)  # may raise _ImplausibleTimestamp
    if exchange_ts is None:
        raise ValueError("tick frame missing exchange_timestamp")
    token = frame.get("instrument_token")
    if token is None:
        raise ValueError("tick frame missing instrument_token")
    ohlc = frame.get("ohlc") or {}
    depth = frame.get("depth") or {}
    buy = depth.get("buy") or []
    sell = depth.get("sell") or []
    return Tick(
        instrument_token=int(token),
        tradingsymbol=tradingsymbol,
        ltp=ltp,
        volume_traded=int(frame.get("volume_traded") or 0),
        exchange_ts=exchange_ts,
        ohlc_open=_wire_decimal(ohlc.get("open")),
        ohlc_high=_wire_decimal(ohlc.get("high")),
        ohlc_low=_wire_decimal(ohlc.get("low")),
        ohlc_close=_wire_decimal(ohlc.get("close")),
        avg_price=_wire_decimal(frame.get("average_traded_price")),
        bid=_wire_decimal(buy[0].get("price")) if buy else None,
        ask=_wire_decimal(sell[0].get("price")) if sell else None,
    )

# WIRE CONTRACT (ticker/main.py implements exactly this):
#   * TOPOLOGY: the ENGINE is the TCP SERVER (asyncio.start_server on 127.0.0.1:<tcp_port>); the child is
#     the CLIENT that connects on spawn.
#   * SECRETS CHANNEL: shared_secret + parent_pid travel via ENV (below); the Kite api_key + access_token
#     travel via STDIN as the first framed line, never env, so they never land in the process table.
#: Env var carrying the per-spawn shared secret to the child (handshake).
_ENV_SHARED_SECRET = "MT_TICKER_SHARED_SECRET"
#: Env var carrying the parent PID so the child can detect orphaning.
_ENV_PARENT_PID = "MT_TICKER_PARENT_PID"
#: Env vars carrying the loopback TCP endpoint the ENGINE listens on and the child CONNECTS to.
_ENV_TCP_HOST = "MT_TICKER_TCP_HOST"
_ENV_TCP_PORT = "MT_TICKER_TCP_PORT"


class FeedHealth(BaseModel):
    """Health snapshot of the ticker feed; drives the risk gate's ``stale_data_guard``.

    Attributes
    ----------
    last_tick_age_s:
        Seconds since the last tick frame, or ``None`` if none seen yet.
    heartbeat_age_s:
        Seconds since the last heartbeat frame, or ``None``. Diagnostic only — the kill reads
        ``last_frame_age_s``.
    last_frame_age_s:
        Seconds since ANY frame (tick / order / heartbeat), or ``None``. **This** is the liveness signal:
        silence beyond ``heartbeat_silence_kill_s`` (10 s) while running ⇒ kill + respawn.
    state:
        One of ``{"STOPPED", "WARMING", "HEALTHY", "DEGRADED", "STALE"}``. ``DEGRADED`` is the in-session
        tick-silence state: heartbeats are fine but NO ticks arrive during market hours, so no bars are
        written. Alerting, distinct from HEALTHY, and recovers to HEALTHY the moment ticks resume.
    """

    last_tick_age_s: float | None = None
    heartbeat_age_s: float | None = None
    last_frame_age_s: float | None = None
    state: str = "STOPPED"


class TickerSupervisor:
    """Owns the mt-ticker Twisted subprocess + its localhost TCP link.

    Parameters
    ----------
    settings:
        ``ticker.tcp_host`` / ``tcp_port`` / ``heartbeat_silence_kill_s`` / ``max_instruments_per_conn``.
    clock:
        The single source of "now"; every age in :meth:`health` derives from it.
    bus:
        Event bus for ``feed.health``, ``tick`` and ``order.update``. ``None`` in bare harnesses skips
        publishing.
    symbol_for_token:
        Token → tradingsymbol resolver (the wire carries only the token; wired to ``InstrumentStore``). A tick
        whose token cannot be resolved is dropped (logged once per token).
    api_key:
        Kite api_key handed to the child over stdin with the access token (never env).
    calendar:
        NSE calendar gating the in-session tick-silence guard to market hours. ``None`` disables the guard
        (bare harness): the feed keeps heartbeat-only health.
    notify:
        Async, best-effort owner sink for the once-per-episode HEALTHY→DEGRADED alert and the tick-silence
        respawn escalation. ``None`` skips it (the WARNING log + ``feed.health`` transition still fire).
    """

    def __init__(
        self,
        settings: Settings,
        clock: Clock,
        bus: EventBus | None,
        *,
        symbol_for_token: Callable[[int], str | None] | None = None,
        api_key: str = "",
        calendar: NSECalendar | None = None,
        notify: NotifyFn | None = None,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._bus = bus
        self._symbol_for_token = symbol_for_token
        self._api_key = api_key
        # No calendar ⇒ cannot know it is in-session ⇒ the tick-silence guard is off (HEALTHY semantics).
        self._calendar = calendar
        self._notify = notify

        # --- child process state ---
        self._proc: asyncio.subprocess.Process | None = None
        self._shared_secret: str | None = None
        self._access_token: str | None = None
        self._tokens: list[int] = []

        # --- the loopback server + the (single) authenticated child link ---
        self._server: asyncio.AbstractServer | None = None
        self._child_writer: asyncio.StreamWriter | None = None
        self._unresolved_tokens_logged: set[int] = set()

        # --- supervision / read-loop tasks ---
        self._read_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        # Drain tasks for the child's stdout/stderr: un-drained, its diagnostics are lost and a full pipe
        # buffer wedges the child mid-write. Both are pumped into the engine log.
        self._output_tasks: list[asyncio.Task[None]] = []

        # --- health signal (timestamps as tz-aware IST via clock.now()) ---
        self._state: str = "STOPPED"
        self._last_tick_at = None  # type: ignore[var-annotated]
        self._last_heartbeat_at = None  # type: ignore[var-annotated]
        # THE liveness stamp: refreshed by EVERY frame. ``_last_heartbeat_at`` is diagnostic only.
        self._last_frame_at = None  # type: ignore[var-annotated]
        # In-child reconnect trackers (see :meth:`_handle_frame`). ``_last_connect_seq`` is THE signal: a
        # CHANGE of the child's per-connect counter is a reconnect. ``_last_ws_connected`` is the fallback for
        # a heartbeat with no ``connect_seq``. ``None`` = nothing reported yet by this spawn; both are
        # cleared per spawn by :meth:`_reset_spawn_stamps`.
        self._last_connect_seq: int | None = None
        self._last_ws_connected: bool | None = None
        # The child's OWN age since its last Kite tick, off the heartbeat. Diagnostic only: small while the
        # supervisor sees silence names a forwarding fault, not Kite.
        self._child_tick_age_s: float | None = None
        self._started_at = None  # type: ignore[var-annotated]
        self._healthy_since = None  # type: ignore[var-annotated]  # entry into HEALTHY (tick-silence ref)

        # --- silence-episode logging (one line per episode, not one per monitor tick) ---
        self._silence_logged = False        # ticker_heartbeat_silence fired for the current episode
        self._starvation_logged = False     # kill discounted as loop starvation, this episode

        # --- WARMING-wedge backoff: consecutive WARMING-timeout respawns that never reach HEALTHY; drives
        #     the capped exponential backoff and the one-shot owner escalation. Reset on a WARMING->HEALTHY
        #     promotion. ---
        self._wedge_respawns = 0
        self._wedge_escalated = False
        #: Consecutive respawns whose spawn raised; reset by a spawn that returns.
        self._spawn_failures = 0

        # --- tick-silence episode (silent connected feed). NOT per-spawn state: the cap counts the
        #     respawns this episode makes, so a respawn must not clear it. A tick clears it
        #     (_handle_frame); a new session date replaces it (_open_silence_episode). ``None`` date =
        #     no episode open. ---
        self._silence_episode_date: date | None = None
        self._silence_episode_since: datetime | None = None  # last tick / HEALTHY entry, ≥ session open
        self._silence_respawns = 0
        self._silence_escalated = False
        self._silence_notified = False                       # feed_degraded sent for this episode

        # --- feed_stats counters (observability): zero-cost increments, drained by stats_snapshot() ---
        self._ticks_received = 0
        self._frames_dropped: dict[str, int] = {}

        self._lock = asyncio.Lock()  # serialize start/stop/respawn

    # ------------------------------------------------------------------ public API

    async def start(self, tokens: list[int], access_token: str) -> None:
        """Spawn ``ticker/main.py`` as a child and enter the WARMING state.

        The access token + per-spawn shared secret cross the trust boundary out-of-band (env + stdin).
        """
        if not self._api_key:
            # Fail LOUD, never dial: an empty api_key in the WS URL is a permanent 400 upgrade-reject loop
            # that the heartbeat would hide behind a HEALTHY-looking feed.
            _log.error("ticker_start_refused_no_api_key")
            return
        async with self._lock:
            if self._proc is not None and self._proc.returncode is None:
                _log.info("ticker_start_noop_already_running", pid=self._proc.pid)
                return
            self._tokens = list(tokens)
            self._access_token = access_token
            await self._cancel_monitor_task()  # a pending spawn retry must not outlive (or race) this start
            try:
                await self._spawn_child()
            except Exception as exc:  # noqa: BLE001 - a failed spawn must never end supervision
                await self._spawn_failed(self._spawn_failure_logged("start", exc))
                return
            self._spawn_failures = 0

    async def update_subscriptions(self, tokens: list[int]) -> None:
        """Update the live subscription set via a control frame on the TCP link (no respawn).

        Writes a ``{"type":"subscribe","tokens":[…]}`` frame; the child diffs the set and re-asserts FULL
        mode. Over ``settings.ticker.max_instruments_per_conn`` (≤3,000/conn) only warns.
        """
        cap = self._settings.ticker.max_instruments_per_conn
        if len(tokens) > cap:
            _log.warning("ticker_subscription_over_cap", requested=len(tokens), cap=cap)
        self._tokens = list(tokens)
        if self._proc is None or self._proc.returncode is not None:
            _log.info("ticker_update_subscriptions_deferred_not_running", count=len(self._tokens))
            return
        await self._write_control_frame({"type": "subscribe", "tokens": self._tokens})
        _log.info("ticker_update_subscriptions", count=len(self._tokens))

    async def _write_control_frame(self, obj: dict[str, Any]) -> None:
        """Send one control frame to the connected child (deferred+logged if the link is down)."""
        writer = self._child_writer
        if writer is None:
            _log.info("ticker_control_frame_deferred_no_link", frame_type=obj.get("type"))
            return
        try:
            body = msgpack.packb(obj, use_bin_type=True)
            writer.write(_LEN_PREFIX.pack(len(body)) + body)
            await writer.drain()
        except (ConnectionError, RuntimeError) as exc:
            _log.warning("ticker_control_frame_write_failed", error=str(exc))

    async def stop(self) -> None:
        """Stop the child: close stdin (orphan protection) then terminate.

        Teardown order, as in :meth:`_respawn`: the **monitor** first (it must not respawn the child we are
        about to kill); the **child** next (its exit closes the link, which lets a cancelled
        :meth:`_read_loop`'s ``server.wait_closed()`` return on CPython ≥3.12); the **read loop** last.
        """
        async with self._lock:
            await self._cancel_monitor_task()
            await self._terminate_child()
            await self._cancel_read_task()
            self._set_state("STOPPED")
            _log.info("ticker_stopped")

    def health(self) -> FeedHealth:
        """Return the current :class:`FeedHealth`; ages computed from ``clock.now()``."""
        now = self._clock.now()
        last_tick_age = (
            (now - self._last_tick_at).total_seconds() if self._last_tick_at is not None else None
        )
        heartbeat_age = (
            (now - self._last_heartbeat_at).total_seconds()
            if self._last_heartbeat_at is not None
            else None
        )
        frame_age = (
            (now - self._last_frame_at).total_seconds() if self._last_frame_at is not None else None
        )
        return FeedHealth(
            last_tick_age_s=last_tick_age,
            heartbeat_age_s=heartbeat_age,
            last_frame_age_s=frame_age,
            state=self._state,
        )

    # ------------------------------------------------------------------ spawn / terminate (real)

    def _ticker_entrypoint(self) -> Path:
        """Resolve ``ticker/main.py`` at the REPO ROOT (outside ``src/engine``: it must never import ``engine.*``)."""
        repo_root = Path(__file__).resolve().parents[3]
        return repo_root / "ticker" / "main.py"

    async def _spawn_child(self) -> None:
        """Launch a fresh child with a new per-spawn secret; arm the read + monitor loops.

        Every (re)spawn is a brand-new OS process with a new shared secret, so a frame minted against a
        previous child's secret is rejected.
        """
        self._shared_secret = _secrets.token_hex(32)
        entrypoint = self._ticker_entrypoint()

        # Bind the loopback listener BEFORE spawning the child, or its immediate connect is refused.
        self._read_task = asyncio.create_task(self._read_loop(), name="ticker-read-loop")
        await self._wait_server_ready()

        env = {
            _ENV_SHARED_SECRET: self._shared_secret,
            _ENV_PARENT_PID: str(_current_pid()),
            _ENV_TCP_HOST: self._settings.ticker.tcp_host,
            _ENV_TCP_PORT: str(self._settings.ticker.tcp_port),
        }
        full_env = {**_os_environ(), **env}

        self._proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(entrypoint),
            stdin=asyncio.subprocess.PIPE,  # kept open = liveness; closing it orphan-kills the child
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=full_env,
        )

        # Drain stdout+stderr IMMEDIATELY, before the handshake (the child logs to stderr while it reads
        # credentials): un-drained, the pipes hide the child's own explanation of a dead feed and fill up,
        # wedging the child on its next write.
        self._output_tasks = [
            asyncio.create_task(
                self._drain_child_stream(self._proc.stderr, "stderr", "warning"),
                name="ticker-stderr-drain",
            ),
            asyncio.create_task(
                self._drain_child_stream(self._proc.stdout, "stdout", "info"),
                name="ticker-stdout-drain",
            ),
        ]

        await self._send_startup_handshake()

        self._reset_spawn_stamps()
        self._set_state("WARMING")  # suppress false feed-stale alarms until first ticks

        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="ticker-monitor-loop")

        _log.info(
            "ticker_spawned",
            pid=self._proc.pid,
            entrypoint=str(entrypoint),
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
            tokens=len(self._tokens),
        )

    def _reset_spawn_stamps(self) -> None:
        """Clear every per-spawn stamp so a fresh child is judged only on ITS OWN evidence.

        A new child has produced no frames, so the liveness/tick/heartbeat stamps go to ``None`` and
        ``_started_at`` re-stamps so each WARMING episode is timed afresh. The reconnect trackers must reset
        too: a fresh child restarts ``connect_seq`` at 1, so its FIRST heartbeat is a spawn (whose
        WARMING→HEALTHY promotion already publishes), not a reconnect; carrying the dead child's values
        across would publish a phantom reconnect on top of it. Any new per-spawn field belongs here.
        """
        self._started_at = self._clock.now()
        self._last_tick_at = None
        self._last_heartbeat_at = None
        self._last_frame_at = None
        self._last_connect_seq = None
        self._last_ws_connected = None
        self._child_tick_age_s = None
        self._healthy_since = None

    async def _send_startup_handshake(self) -> None:
        """Deliver the credentials + initial subscriptions to the child over stdin.

        One length-prefixed msgpack frame ``{"api_key", "access_token", "tokens"}`` (the wire contract's
        SECRETS CHANNEL; ticker/main.py ``_read_stdin_credentials``). Stdin then stays OPEN: closing it is
        the orphan-protection kill.
        """
        proc = self._proc
        if proc is None or proc.stdin is None:
            return
        payload = msgpack.packb(
            {
                "api_key": self._api_key,
                "access_token": self._access_token or "",
                "tokens": list(self._tokens),
            },
            use_bin_type=True,
        )
        try:
            proc.stdin.write(_LEN_PREFIX.pack(len(payload)) + payload)
            await proc.stdin.drain()
        except (ConnectionError, RuntimeError) as exc:  # pragma: no cover - child died mid-spawn
            _log.warning("ticker_stdin_handshake_failed", error=str(exc))

    async def _drain_child_stream(self, stream: Any, name: str, level: str) -> None:
        """Pump one child pipe line-by-line into the engine log until EOF.

        Child stderr is its error channel ⇒ WARNING; stdout is unexpected ⇒ INFO. Never raises out: a drain
        failure must never take down supervision.
        """
        if stream is None:
            return
        log_fn = getattr(_log, level, _log.warning)
        try:
            while True:
                line = await stream.readline()
                if not line:
                    return  # EOF: the child closed the stream (exited)
                text = line.decode("utf-8", "replace").rstrip()
                if text:
                    log_fn("ticker_child_output", stream=name, line=text)
        except asyncio.CancelledError:  # pragma: no cover - normal on stop/respawn
            raise
        except Exception:  # noqa: BLE001 - a drain error must never kill supervision
            _log.exception("ticker_child_stream_drain_error", stream=name)

    async def _cancel_output_tasks(self) -> None:
        """Cancel + await the stdout/stderr drain tasks (idempotent); leak-cleanup for a killed/wedged child
        whose streams never closed."""
        tasks = self._output_tasks
        self._output_tasks = []
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _terminate_child(self) -> None:
        """Close stdin (orphan protection) then terminate, escalating to kill if it does not exit."""
        proc = self._proc
        self._proc = None
        if proc is None:
            await self._cancel_output_tasks()
            return
        if proc.returncode is not None:
            await self._cancel_output_tasks()
            return

        # 1) Close stdin: a well-behaved child treats stdin EOF as "parent gone" and exits.
        if proc.stdin is not None and not proc.stdin.is_closing():
            try:
                proc.stdin.close()
            except (OSError, RuntimeError):  # pragma: no cover - stdin already torn down
                pass

        # 2) Graceful terminate, then hard kill if it ignores us (a wedged reactor must still die).
        #    Windows can deny TerminateProcess (e.g. in Modern Standby). Stdin is already closed, so the
        #    child exits on its own; a denial is logged, never raised into a respawn or stop().
        try:
            proc.terminate()
        except ProcessLookupError:  # pragma: no cover - exited between checks
            await self._cancel_output_tasks()
            return
        except PermissionError:
            _log.error("ticker_terminate_denied", pid=proc.pid)
            await self._cancel_output_tasks()
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            _log.warning("ticker_terminate_timeout_killing", pid=proc.pid)
            try:
                proc.kill()
            except ProcessLookupError:  # pragma: no cover
                pass
            except PermissionError:
                _log.error("ticker_kill_denied", pid=proc.pid)
                await self._cancel_output_tasks()
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:  # pragma: no cover - OS-level wedge
                _log.error("ticker_kill_timeout", pid=proc.pid)
        await self._cancel_output_tasks()

    # ------------------------------------------------------------------ read loop (the loopback server)

    async def _wait_server_ready(self, timeout_s: float = 5.0) -> None:
        """Poll until :meth:`_read_loop` has bound its listener (or it died / timed out).

        Uses real short sleeps (not Clock) — this is I/O readiness, not trading time.
        """
        for _ in range(int(timeout_s / 0.01)):
            if self._server is not None:
                return
            task = self._read_task
            if task is not None and task.done():
                exc = task.exception() if not task.cancelled() else None
                _log.error("ticker_server_bind_failed", error=str(exc))
                return
            await asyncio.sleep(0.01)
        _log.error("ticker_server_bind_timeout", timeout_s=timeout_s)

    async def _read_loop(self) -> None:
        """The loopback TCP server owning the child's data link (WIRE CONTRACT at module top); frames are
        handled in :meth:`_handle_child_connection`. Cancellation (stop/respawn) closes the listener so a
        respawn can rebind the port.

        The cleanup wait is BOUNDED. ``server.close()`` releases the socket synchronously (all a rebind
        needs), but ``server.wait_closed()`` on CPython ≥3.12 also blocks until every accepted connection
        drops; awaiting it unbounded in a ``finally`` during cancellation makes this task an un-cancellable
        hang (:meth:`_respawn` docstring). Callers terminate the child first, so the wait is normally
        instant; the timeout is the logged backstop for a child that will not let go.
        """
        server = await asyncio.start_server(
            self._handle_child_connection,
            host=self._settings.ticker.tcp_host,
            port=int(self._settings.ticker.tcp_port),
        )
        self._server = server
        _log.info(
            "ticker_server_listening",
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
        )
        try:
            await asyncio.Event().wait()  # serve until cancelled (stop/respawn)
        finally:
            self._server = None
            # Close the link, not just forget it: server.close() keeps accepted connections, so a child that
            # outlived its terminate would keep stamping liveness for the next spawn.
            writer, self._child_writer = self._child_writer, None
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()
            server.close()  # releases the listening socket immediately (rebind-safe on its own)
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=_SERVER_CLOSE_TIMEOUT_S)
            except TimeoutError:  # asyncio.TimeoutError is this builtin since 3.11
                _log.warning("ticker_server_close_timeout", timeout_s=_SERVER_CLOSE_TIMEOUT_S)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - cleanup must never mask the original teardown
                _log.exception("ticker_server_close_error")

    async def _handle_child_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Authenticate one inbound child connection and pump its frames.

        The FIRST frame must be a ``hello`` echoing this spawn's shared secret, else the connection is
        dropped before any tick/order frame is honoured.
        """
        peer = writer.get_extra_info("peername")
        try:
            hello = await self._read_frame(reader)
            if (
                hello is None
                or hello.get("type") != "hello"
                or not self._shared_secret
                or hello.get("secret") != self._shared_secret
            ):
                _log.warning(
                    "ticker_handshake_rejected",
                    peer=str(peer),
                    frame_type=None if hello is None else hello.get("type"),
                )
                return
            _log.info(
                "ticker_handshake_ok",
                child_pid=hello.get("pid"),
                protocol_v=hello.get("v"),
                tokens=hello.get("tokens"),
            )
            self._child_writer = writer
            while True:
                frame = await self._read_frame(reader)
                if frame is None:
                    _log.info("ticker_link_closed", peer=str(peer))
                    return
                await self._handle_frame(frame)
        except asyncio.CancelledError:  # pragma: no cover - normal on stop/respawn
            raise
        except Exception:  # noqa: BLE001 - never let a link error kill the server silently
            _log.exception("ticker_link_error", peer=str(peer))
        finally:
            if self._child_writer is writer:
                self._child_writer = None
            with contextlib.suppress(Exception):
                writer.close()

    async def _read_frame(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
        """Read one length-prefixed msgpack frame; ``None`` on EOF/connection loss.

        An undecodable body yields ``{}`` (logged + counted) so one corrupt frame never tears the link
        down — the heartbeat-silence guard is the backstop for a systematically broken stream.
        """
        try:
            header = await reader.readexactly(_LEN_PREFIX.size)
            (length,) = _LEN_PREFIX.unpack(header)
            body = await reader.readexactly(length)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None
        try:
            frame = msgpack.unpackb(body, raw=False)
        except Exception as exc:  # noqa: BLE001 - corrupt frame is dropped, not fatal
            _log.warning("ticker_frame_decode_error", error=str(exc))
            self._drop("decode_error")
            return {}
        return frame if isinstance(frame, dict) else {}

    async def _handle_frame(self, frame: dict[str, Any]) -> None:
        """Dispatch one authenticated frame by ``type`` (WIRE CONTRACT at module top).

        This is also the ONLY place liveness is stamped. Every frame — tick, order, heartbeat, even an
        undecodable one that :meth:`_read_frame` reduced to ``{}`` — refreshes ``_last_frame_at`` before
        dispatch, so liveness cannot be starved separately from the data it guards.

        **In-child reconnect signal.** KiteTicker reconnects INSIDE the child while it keeps heartbeating,
        so no supervisor state transition happens and ``BarBuilder``'s reconnect grace would never arm; Kite
        then re-delivers its connect-time snapshot of last-trade stamps, which reads as lag. The signal is
        ``connect_seq``, the child's per-connect counter, shipped on a heartbeat emitted BEFORE
        ``ws.subscribe`` so it precedes the snapshot ticks in the same ordered pipe. A CHANGE republishes
        health exactly once — not its FIRST value of a spawn (that frame is the spawn's own
        WARMING→HEALTHY publish) and not per heartbeat (that would spam the bus and hold the grace open).

        ``ws_connected``'s False→True edge is only the FALLBACK for a heartbeat with no ``connect_seq``: it
        is observable no earlier than the next 1 s beat (too late for the snapshot) and a sub-second drop
        shows no edge at all.

        **One frame, at most one publish.** :meth:`_mark_alive` may already have published a STALE→HEALTHY
        recovery for this very frame (the reconnect heartbeat that ends a silence), hence the ``published``
        ledger. This only republishes what ``health()`` already says; the state machine and kill logic are
        untouched.
        """
        published = await self._mark_alive()
        ftype = frame.get("type")
        if ftype == "heartbeat":
            self._last_heartbeat_at = self._clock.now()
            # ``connect_seq`` (primary) / ``ws_connected`` (fallback): an absent field leaves its tracked
            # value alone — absence is neither a reconnect nor a disconnect.
            connect_seq = frame.get("connect_seq")
            ws_connected = frame.get("ws_connected")
            seq_reconnect = (
                connect_seq is not None
                and self._last_connect_seq is not None
                and connect_seq != self._last_connect_seq
            )
            if connect_seq is not None:
                self._last_connect_seq = connect_seq
            ws_reconnect = ws_connected is True and self._last_ws_connected is False
            if ws_connected is not None:
                self._last_ws_connected = bool(ws_connected)
            if "last_tick_age_s" in frame:
                # Present-but-None is the child's own "no tick yet on this connection": kept as None.
                child_age = frame["last_tick_age_s"]
                self._child_tick_age_s = float(child_age) if isinstance(child_age, int | float) else None
            if self._state == "WARMING":
                # The first heartbeat proves link + child are live (feed health only; the warm-up ENTRY
                # gate in ops.warmup is separate).
                self._set_state("HEALTHY")
                await self._publish_health()
                published = True
            elif seq_reconnect or (ws_reconnect and connect_seq is None):
                _log.info(
                    "ticker_ws_reconnected",
                    state=self._state,
                    seq=connect_seq,
                    via="connect_seq" if seq_reconnect else "ws_connected",
                )
                if not published:
                    await self._publish_health()
                    published = True
        elif ftype == "tick":
            self._ticks_received += 1
            self._last_tick_at = self._clock.now()
            if self._silence_episode_date is not None:
                # A tick ends the silence episode, whatever the state: after a tick-silence respawn the
                # fresh child is usually HEALTHY (heartbeat-promoted) when its first tick lands.
                self._reset_silence_episode()
            if self._state == "WARMING":
                # A TICK proves life as well as a heartbeat, so it promotes out of warm-up too; else a
                # child whose heartbeat never started but whose ticks flow would be respawned every
                # warming_timeout_s, discarding a working feed.
                self._set_state("HEALTHY")
                await self._publish_health()
            elif self._state == "DEGRADED":
                # Ticks resumed after an in-session silence: recover DEGRADED → HEALTHY (visible).
                self._set_state("HEALTHY")
                _log.info("feed_tick_recovered")
                await self._publish_health()
            tick = self._parse_tick(frame)
            if tick is not None and self._bus is not None:
                self._bus.publish(TICK_TOPIC, tick)
        elif ftype == "order":
            # Verbatim Kite postback; the OMS correlates it.
            if self._bus is not None:
                self._bus.publish(ORDER_UPDATE_TOPIC, OrderUpdateFrame(data=frame.get("data") or {}))
        else:
            _log.warning("ticker_unknown_frame", frame_type=str(ftype))
            self._drop("unknown_frame")

    async def _mark_alive(self) -> bool:
        """Refresh liveness from an arriving frame and recover a STALE feed.

        Returns **whether it published** a ``feed.health`` event, so :meth:`_handle_frame` can honour
        one-publish-per-frame (the frame that ends a silence is usually the reconnect heartbeat).

        Called for every frame. Two jobs: stamp ``_last_frame_at`` (the age the kill decision reads), and
        **recover** STALE → HEALTHY. Recovery must not depend on a reconnect that may never come: frames are
        present, so the feed is healthy, and it is published because the stale-data guard can FREEZE entries
        off this state.
        """
        self._last_frame_at = self._clock.now()
        if self._state == "STALE":
            self._set_state("HEALTHY")
            _log.info("feed_stale_recovered", frames_resumed=True)
            await self._publish_health()
            return True
        return False

    def _parse_tick(self, frame: dict[str, Any]) -> Tick | None:
        """Wire tick frame → core ``Tick`` (symbol resolved via the injected resolver); None = drop."""
        token = frame.get("instrument_token")
        symbol: str | None = None
        if token is not None and self._symbol_for_token is not None:
            symbol = self._symbol_for_token(int(token))
        if symbol is None:
            tok = -1 if token is None else int(token)
            self._drop("unresolved_symbol")
            if tok not in self._unresolved_tokens_logged:   # log once per token, not per tick
                self._unresolved_tokens_logged.add(tok)
                _log.warning("ticker_tick_symbol_unresolved", instrument_token=tok)
            return None
        try:
            return parse_tick_frame(frame, symbol)
        except _ImplausibleTimestamp:
            # Expected drop: own reason/counter, no ERROR traceback (the WARNING fired in _wire_timestamp).
            self._drop("implausible_timestamp")
            return None
        except Exception:  # noqa: BLE001 - malformed frame is dropped, never crashes the read loop
            _log.exception("ticker_tick_parse_error", instrument_token=token)
            self._drop("parse_error")
            return None

    async def _monitor_loop(self) -> None:
        """Frame-silence watchdog: kill + respawn on >``heartbeat_silence_kill_s``.

        Runs while a child is alive. Each cycle it re-derives health from ``clock.now()``; STALE while
        *running* (not WARMING) triggers a respawn and publishes the STALE transition. WARMING is bounded by
        ``_check_warming_timeout`` (an unbounded WARMING is itself a wedge). Also reaps an unexpectedly dead
        child.

        Loop-starvation discount: this coroutine and the frame reader share one event loop, so a long
        synchronous stretch blocks BOTH while the wall clock runs, and on the late wake-up the silence looks
        like a dead child although its frames are queued in TCP and land in a burst moments later. The loop
        measures its OWN wake-up overshoot, accumulates it while liveness is un-refreshed, and subtracts it
        before pulling the trigger. Ages stay on the wall clock; only the verdict is discounted, and the
        discount is logged.
        """
        kill_after = float(self._settings.ticker.heartbeat_silence_kill_s)
        tick_silence_budget = float(self._settings.ticker.tick_silence_degrade_s)
        warming_timeout = float(self._settings.ticker.warming_timeout_s)
        warming_cap = float(self._settings.ticker.warming_backoff_cap_s)
        max_wedge = int(self._settings.ticker.max_wedge_respawns)
        tick_silence_respawn = float(self._settings.ticker.tick_silence_respawn_s)
        max_silence_respawns = int(self._settings.ticker.max_tick_silence_respawns)
        #: Wall instant of the previous cycle + starvation accrued since liveness was last refreshed.
        last_cycle_at = self._clock.now()
        last_frame_seen = self._last_frame_at
        starved_s = 0.0
        try:
            while True:
                await _monitor_sleep(1.0)

                # --- measure this loop's own starvation before judging anyone else's silence ---
                now = self._clock.now()
                overshoot = (now - last_cycle_at).total_seconds() - 1.0
                last_cycle_at = now
                if self._last_frame_at != last_frame_seen:
                    # A frame landed: liveness is fresh, so is the measurement.
                    last_frame_seen = self._last_frame_at
                    starved_s = 0.0
                    self._starvation_logged = False
                elif overshoot > _MONITOR_OVERSHOOT_FLOOR_S:
                    starved_s += overshoot

                proc = self._proc
                if proc is None:
                    return

                # Child died on its own (KiteTicker crash) — respawn.
                if proc.returncode is not None:
                    _log.warning("ticker_child_exited", pid=proc.pid, code=proc.returncode)
                    await self._respawn(reason="child_exited")
                    return

                heartbeat_age = self._heartbeat_age_s()
                if self._state == "WARMING":
                    # The silence kill below is suppressed during warm-up (a fresh spawn has no frames yet),
                    # but WARMING must not be unbounded: a child that dies/hangs before its first
                    # heartbeat, or a system resume mid-WARMING, would wedge here forever. Bound it with a
                    # timeout + capped backoff; a respawn returns (``_spawn_child`` installs a fresh monitor).
                    if await self._check_warming_timeout(warming_timeout, warming_cap, max_wedge):
                        return
                    continue
                # THE liveness test: ANY frame counts, not just the dedicated heartbeat.
                silence = self._liveness_age_s()
                if silence is not None and silence > kill_after:
                    if silence - starved_s <= kill_after:
                        # Our silence, not the child's: log once per episode and let the queued frames
                        # land; a genuinely dead child fires on the next un-starved cycle.
                        if not self._starvation_logged:
                            self._starvation_logged = True
                            _log.warning(
                                "ticker_silence_discounted_loop_starved",
                                frame_silence_s=round(silence, 3),
                                loop_starved_s=round(starved_s, 3),
                                kill_after_s=kill_after,
                            )
                        continue
                    if not self._silence_logged:
                        self._silence_logged = True
                        _log.error(
                            "ticker_heartbeat_silence",
                            frame_silence_s=round(silence, 3),
                            heartbeat_age_s=None if heartbeat_age is None else round(heartbeat_age, 3),
                            loop_starved_s=round(starved_s, 3),
                            kill_after_s=kill_after,
                        )
                    self._set_state("STALE")
                    await self._publish_health()  # STALE transition for the stale-data guard
                    await self._respawn(reason="heartbeat_silence")
                    return

                # Heartbeats are fine, but is the FEED delivering ticks? The child heartbeats regardless, so
                # in market hours tick silence past the budget ⇒ DEGRADED.
                await self._check_tick_silence(tick_silence_budget)
                # A connected-but-silent feed heartbeats fine, so the kill above never fires for it; only a
                # fresh connection ends one. A respawn returns, as above.
                if await self._check_tick_silence_respawn(tick_silence_respawn, max_silence_respawns):
                    return
        except asyncio.CancelledError:  # pragma: no cover - normal on stop
            raise

    async def _check_tick_silence(self, budget_s: float) -> None:
        """In-session tick-silence guard — called each monitor tick.

        HEALTHY + inside market hours + effective tick age past ``budget_s`` ⇒ DEGRADED (a tickless feed must
        never present HEALTHY in session), with a WARNING, a ``feed.health`` publish and the owner notify.
        Off-hours or any non-HEALTHY state: no-op. Recovery to HEALTHY is on the next tick in
        :meth:`_handle_frame`.

        The notify is one-shot per silence EPISODE, not per entry: each respawn's fresh child re-enters
        DEGRADED later and must not page again. The WARNING and the publish fire on every entry.
        """
        if self._state != "HEALTHY":
            return
        now = self._clock.now()
        session = self._open_session(now)
        if session is None:
            return
        silence = self._effective_tick_silence_s()
        if silence is None or silence <= budget_s:
            return
        self._open_silence_episode(session, now)
        self._set_state("DEGRADED")
        _log.warning(
            "feed_tick_silence_degraded",
            tick_silence_s=round(silence, 1),
            budget_s=budget_s,
            silence_respawns=self._silence_respawns,
            ws_connected=self._last_ws_connected,
            child_last_tick_age_s=self._child_tick_age_s,
        )
        await self._publish_health()
        if not self._silence_notified:
            self._silence_notified = True
            await self._notify_degraded(silence, budget_s)

    async def _check_tick_silence_respawn(self, respawn_after_s: float, max_respawns: int) -> bool:
        """Silent-connected-feed respawn — called each monitor cycle after the DEGRADED guard.

        In session, DEGRADED silence past ``respawn_after_s`` ⇒ kill + respawn, at most ``max_respawns``
        times per episode; then escalate ONCE and stay DEGRADED. Returns ``True`` when a respawn ran (the
        monitor loop then returns; ``_spawn_child`` installs a fresh monitor task).

        Silence runs from the LATEST of the last tick, the last HEALTHY entry and the session open — not
        :meth:`_effective_tick_silence_s`, which prefers an old tick over a newer HEALTHY entry. Each fresh
        connection therefore gets the full budget, and a feed left DEGRADED overnight is timed from today's
        open, not yesterday's silence.
        """
        if self._state != "DEGRADED":
            return False
        now = self._clock.now()
        session = self._open_session(now)
        if session is None:
            return False
        self._open_silence_episode(session, now)
        ref: datetime = session.open
        for stamp in (self._last_tick_at, self._healthy_since):
            if stamp is not None and stamp > ref:
                ref = stamp
        silence = (now - ref).total_seconds()
        if silence <= respawn_after_s:
            return False
        if self._silence_respawns < max_respawns:
            # Counted before the respawn: one that raises (or finds no token) still spends an attempt.
            self._silence_respawns += 1
            _log.warning(
                "ticker_tick_silence_respawn",
                attempt=self._silence_respawns,
                max_respawns=max_respawns,
                tick_silence_s=round(silence, 1),
                ws_connected=self._last_ws_connected,
                child_last_tick_age_s=self._child_tick_age_s,
            )
            await self._respawn(reason="tick_silence")
            return True
        if not self._silence_escalated:
            self._silence_escalated = True
            since = self._silence_episode_since or session.open
            episode_silence = (now - since).total_seconds()
            _log.error(
                "ticker_tick_silence_respawns_exhausted",
                tick_silence_s=round(episode_silence, 1),
                since=since.isoformat(),
                respawns=self._silence_respawns,
            )
            await self._notify_silent_unrecovered(self._silence_respawns, since, episode_silence)
        return False

    def _open_silence_episode(self, session: Session, now: datetime) -> None:
        """Open the silence episode for ``now``'s session unless one is already open for it.

        An episode from an earlier session date is replaced, not continued (a fresh cap and alert each
        day). ``since`` is clamped to the session open, so escalation never reports yesterday's last tick
        as today's silence."""
        today = now.date()
        if self._silence_episode_date == today:
            return
        self._reset_silence_episode()
        self._silence_episode_date = today
        ref = self._last_tick_at or self._healthy_since
        self._silence_episode_since = ref if ref is not None and ref > session.open else session.open

    def _reset_silence_episode(self) -> None:
        """Close the silence episode (a tick arrived, or a new session date replaces it)."""
        self._silence_episode_date = None
        self._silence_episode_since = None
        self._silence_respawns = 0
        self._silence_escalated = False
        self._silence_notified = False

    async def _notify_silent_unrecovered(self, respawns: int, since: datetime, silence_s: float) -> None:
        """One-shot owner escalation once the episode's respawns are spent (best-effort)."""
        if self._notify is None:
            return
        try:
            await self._notify(feed_silent_unrecovered(respawns=respawns, since=since, silence_s=silence_s))
        except Exception:  # noqa: BLE001 - a failed notify must never crash the monitor loop
            _log.exception("feed_silent_unrecovered_notify_failed")

    async def _check_warming_timeout(
        self, timeout_s: float, cap_s: float, max_respawns: int
    ) -> bool:
        """WARMING-wedge guard — called each monitor cycle while WARMING.

        No heartbeat within the backoff-scaled timeout means the child is wedged (dead/hung, or the machine
        slept mid-WARMING): kill + respawn. Returns ``True`` when a respawn ran (the monitor loop then
        returns), ``False`` while inside the budget.

        Consecutive wedge-respawns back off exponentially (``timeout_s × 2ⁿ``) capped at ``cap_s``; past
        ``max_respawns`` escalate ONCE to the owner. A WARMING→HEALTHY promotion resets the counter and flag
        (see :meth:`_set_state`).
        """
        age = self._warming_age_s()
        effective = _backoff(timeout_s, self._wedge_respawns, cap_s)
        if age is None or age <= effective:
            return False
        self._wedge_respawns += 1
        _log.error(
            "ticker_warming_timeout",
            warming_age_s=round(age, 1),
            timeout_s=round(effective, 1),
            wedge_respawns=self._wedge_respawns,
        )
        if self._wedge_respawns >= max_respawns and not self._wedge_escalated:
            self._wedge_escalated = True
            await self._notify_wedged(self._wedge_respawns, age)
        # A wedged feed is a feed-lost incident: publish STALE (the stale-data guard can FREEZE entries)
        # before the respawn re-enters WARMING.
        self._set_state("STALE")
        await self._publish_health()  # STALE transition for the stale-data guard
        await self._respawn(reason="warming_timeout")
        return True

    async def _notify_wedged(self, respawns: int, age_s: float) -> None:
        """One-shot owner escalation for a WEDGED feed (best-effort; never breaks supervision)."""
        if self._notify is None:
            return
        try:
            await self._notify(feed_wedged(respawns=respawns, age_s=age_s))
        except Exception:  # noqa: BLE001 - a failed notify must never crash the monitor loop
            _log.exception("feed_wedged_notify_failed")

    def in_market_hours(self, now: datetime | None = None) -> bool:
        """True iff ``now`` is inside today's NSE continuous session (calendar+clock). Without a
        calendar (bare harness) the feed cannot know it is in-session ⇒ False (guard disabled)."""
        return self._open_session(now or self._clock.now()) is not None

    def _open_session(self, now: datetime) -> Session | None:
        """Today's NSE session if ``now`` is inside it, else ``None`` (see :meth:`in_market_hours`)."""
        if self._calendar is None:
            return None
        session = self._calendar.session(now.date())
        if session is None:  # holiday / weekend / unverified horizon
            return None
        return session if session.open <= now <= session.close else None

    def _effective_tick_silence_s(self) -> float | None:
        """Seconds since the last tick — or, if NO tick has EVER been seen, since we entered HEALTHY
        (a never-ticking feed MUST count toward the silence budget, not be excused as 'no tick yet')."""
        ref = self._last_tick_at or self._healthy_since
        if ref is None:
            return None
        return (self._clock.now() - ref).total_seconds()

    async def _notify_degraded(self, age_s: float, budget_s: float) -> None:
        """One-shot owner alert on HEALTHY→DEGRADED (best-effort; never breaks supervision)."""
        if self._notify is None:
            return
        try:
            await self._notify(feed_degraded(age_s=age_s, budget_s=budget_s))
        except Exception:  # noqa: BLE001 - a failed notify must never crash the monitor loop
            _log.exception("feed_degraded_notify_failed")

    def stats_snapshot(self) -> dict[str, Any]:
        """Return + reset the since-last-call feed counters for the periodic ``feed_stats`` line.

        ``ticks_received`` counts tick frames off the wire; ``frames_dropped`` maps drop-reason →
        count (unresolved_symbol / parse_error / implausible_timestamp / unknown_frame /
        decode_error). Reset-on-read gives the composition-root emitter clean per-interval deltas."""
        snap: dict[str, Any] = {
            "ticks_received": self._ticks_received,
            "frames_dropped": dict(self._frames_dropped),
        }
        self._ticks_received = 0
        self._frames_dropped = {}
        return snap

    def _drop(self, reason: str) -> None:
        """Increment the drop counter for ``reason`` (feed_stats; zero-cost, no hot-path logging)."""
        self._frames_dropped[reason] = self._frames_dropped.get(reason, 0) + 1

    async def _respawn(self, *, reason: str) -> None:
        """Kill the current child and launch a fresh one (the reactor cannot restart in-process).

        Called from the monitor loop. The old read loop is cancelled before :meth:`_spawn_child` overwrites
        ``self._read_task``, but **after** the child is terminated. The monitor task is NOT cancelled here:
        the caller *is* the monitor task and returns right after, and ``_spawn_child`` installs a fresh one
        (cancelling self would abort the respawn mid-flight).

        ORDER IS LOAD-BEARING. Cancelling the read task first self-deadlocks: :meth:`_read_loop`'s
        ``finally`` awaits ``server.wait_closed()``, which on CPython ≥3.12 waits for every accepted
        connection to drop, including the live child link, whose handler only exits when the child goes
        away — and ``_terminate_child()`` is what makes it go away, behind the same ``self._lock``. The
        supervisor would log ``ticker_respawn`` and then freeze for hours. Terminating first breaks the
        cycle; :meth:`_read_loop` also bounds the wait as a second line of defence.
        """
        async with self._lock:
            _log.warning("ticker_respawn", reason=reason)
            try:
                await self._terminate_child()
                await self._cancel_read_task()
                if self._access_token is None:
                    _log.error("ticker_respawn_no_access_token")
                    self._set_state("STOPPED")
                    return
                await self._spawn_child()
            except Exception as exc:  # noqa: BLE001 - a failed respawn must never end supervision
                await self._spawn_failed(self._spawn_failure_logged(reason, exc))
                return
            self._spawn_failures = 0

    def _spawn_failure_logged(self, where: str, exc: Exception) -> float:
        """Count a failed (re)spawn and log it; returns the retry delay. Call it inside the ``except`` so
        the first failure of a streak keeps its traceback."""
        self._spawn_failures += 1
        cfg = self._settings.ticker
        delay = _backoff(float(cfg.warming_timeout_s), self._spawn_failures - 1, float(cfg.warming_backoff_cap_s))
        fields = {"where": where, "failures": self._spawn_failures, "retry_in_s": delay, "error": repr(exc)}
        if self._spawn_failures == 1:
            _log.exception("ticker_spawn_failed", **fields)
        else:
            _log.warning("ticker_spawn_failed", **fields)
        return delay

    async def _spawn_failed(self, delay_s: float) -> None:
        """A (re)spawn that raised leaves no child and no monitor task (CreateProcess can be denied in
        Modern Standby), which must not end supervision silently with feed health still HEALTHY. The feed
        goes STALE (the stale-data guard, and in session its page, see it), the half-made spawn is cleaned
        up, and a retry task stands in for the monitor (``stop()`` cancels it), backing off like a WARMING
        wedge. A cancellation during the cleanup propagates without arming a retry."""
        self._set_state("STALE")
        try:
            await self._terminate_child()
            await self._cancel_read_task()
            await self._publish_health()  # STALE transition for the stale-data guard
        except Exception:  # noqa: BLE001 - cleanup must never cost the retry
            _log.exception("ticker_spawn_failed_cleanup_error")
        self._monitor_task = asyncio.create_task(self._retry_spawn(delay_s), name="ticker-spawn-retry")

    async def _retry_spawn(self, delay_s: float) -> None:
        await _monitor_sleep(delay_s)
        await self._respawn(reason="spawn_retry")

    async def _cancel_read_task(self) -> None:
        """Cancel and await only the read loop (idempotent). Used on respawn, where cancelling the
        monitor task — the current caller — would abort the respawn itself."""
        task = self._read_task
        self._read_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _cancel_monitor_task(self) -> None:
        """Cancel and await only the monitor loop (idempotent). Used by :meth:`stop`, NEVER from inside the
        monitor loop itself. Teardown interleaves ``_terminate_child()`` between the two cancels."""
        task = self._monitor_task
        self._monitor_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------ health helpers

    def _heartbeat_age_s(self) -> float | None:
        if self._last_heartbeat_at is None:
            return None
        return (self._clock.now() - self._last_heartbeat_at).total_seconds()

    def _liveness_age_s(self) -> float | None:
        """Seconds since ANY frame arrived — the signal the kill decision reads.

        ``None`` before the first frame of this spawn (WARMING owns that window via
        :meth:`_warming_age_s`); stamped once, in :meth:`_mark_alive`."""
        if self._last_frame_at is None:
            return None
        return (self._clock.now() - self._last_frame_at).total_seconds()

    def _warming_age_s(self) -> float | None:
        """Seconds this spawn has been WARMING with no heartbeat yet — the WARMING-wedge clock.

        Reference is the last heartbeat if one arrived without promoting (defensive), else this spawn's
        ``_started_at``. ``None`` before the first spawn."""
        ref = self._last_heartbeat_at or self._started_at
        if ref is None:
            return None
        return (self._clock.now() - ref).total_seconds()

    def _set_state(self, state: str) -> None:
        if state != self._state:
            _log.info("feed_health_transition", frm=self._state, to=state)
        if state == "HEALTHY" and self._state != "HEALTHY":
            # Reference instant for the tick-silence budget when NO tick has been seen yet.
            self._healthy_since = self._clock.now()
            # A successful (re)connect clears the WARMING-wedge backoff + escalation: the next wedge
            # starts at the base timeout and can escalate again.
            self._wedge_respawns = 0
            self._wedge_escalated = False
            # A new silence/starvation episode may log again once the feed has been healthy.
            self._silence_logged = False
            self._starvation_logged = False
        self._state = state

    async def _publish_health(self) -> None:
        """Publish the current :class:`FeedHealth` on ``feed.health``. No-op without a bus."""
        if self._bus is None:
            return
        await self._bus.apublish(FEED_HEALTH_TOPIC, self.health())


# --------------------------------------------------------------------------- tiny stdlib indirections
# Wrapped so tests can monkeypatch the process identity / environment.

def _backoff(base_s: float, n: int, cap_s: float) -> float:
    """``base_s × 2ⁿ`` capped at ``cap_s``; the exponent is bounded so a days-long streak cannot overflow
    the float conversion (``60.0 * 2**1100`` raises, which would end supervision like a failed spawn)."""
    return min(base_s * (2 ** min(n, 32)), cap_s)


async def _monitor_sleep(seconds: float) -> None:
    """One :meth:`TickerSupervisor._monitor_loop` cycle delay.

    A test seam: the watchdog is *about* wall-clock time versus scheduling lag, so a test patches this to
    advance the fake clock (1 s, or 30 s to simulate a starved loop) and return immediately.
    """
    await asyncio.sleep(seconds)


def _current_pid() -> int:
    import os

    return os.getpid()


def _os_environ() -> dict[str, str]:
    import os

    return dict(os.environ)
