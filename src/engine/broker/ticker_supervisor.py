"""Ticker subprocess supervisor (§3.2.2, A4/A3/R2, §2.2, §2.4, §2.6).

``TickerSupervisor`` owns the **mt-ticker** Twisted child process (``ticker/main.py``) and the
localhost TCP link it ships data on. The Twisted reactor cannot be restarted in-process, so A4 forces
the ticker into a *separate process*: when the feed has to be respawned (heartbeat silence, A3 KiteTicker
death), the supervisor kills and re-launches a fresh child rather than trying to restart a reactor inside
the engine. This module is the only thing that spawns / terminates that child and the single owner of its
health signal.

Data path (A3/§2.5):
    KiteTicker → mt-ticker child → length-prefixed msgpack frames on 127.0.0.1:<tcp_port> → here.
The child forwards both **ticks** and **order updates** (``type:"order"``, A3) on the same socket, plus a
**heartbeat** frame every 1 s. Because order updates drive the OMS state machine, a fabricated frame must
not be able to inject a phantom fill — hence the §2.4 trust boundary below.

Trust boundary (§2.4):
    * the TCP socket binds ``127.0.0.1`` only (loopback, not routable);
    * a per-spawn **shared-secret handshake** — a fresh random secret is generated for each spawn and
      handed to the child out-of-band (env/stdin), and the child must echo it on its first frame before
      any tick/order frame is accepted;
    * a **parent-PID check** — the child verifies it is still owned by this engine process and exits if
      orphaned, and the supervisor closes the child's stdin on stop so an orphaned child dies on its own.

Health / stale-data guard (R2, §2.6):
    ``health()`` reports a :class:`FeedHealth` whose ``state`` drives the §7.1 ``stale_data_guard``:
        * ``STOPPED``  — no child running.
        * ``WARMING``  — child just spawned / reconnecting + warm-up backfilling; the HEALTHY-path
          heartbeat-silence kill is **suppressed** (§2.6/§3.2.12) to avoid false alarms on a fresh
          startup, distinct from feed-lost-while-running. The suppression is **bounded**: WARMING with
          no heartbeat past ``settings.ticker.warming_timeout_s`` ⇒ kill + respawn with capped
          exponential backoff (2026-07-23 13:41 sleep/resume wedge — an unbounded WARMING froze the
          state machine forever). This is also the generic system-resume recovery: after a resume,
          whatever state the machine froze in, either the HEALTHY-path stale kill or this WARMING
          timeout fires and respawns.
        * ``HEALTHY``  — FRAMES arriving within the silence budget (see below: a tick counts).
        * ``STALE``    — frame silence exceeded ``settings.ticker.heartbeat_silence_kill_s`` (10 s)
          while *running* (not WARMING) ⇒ the supervisor kills + respawns the child and publishes a
          ``feed.health`` transition to ``STALE`` (R2/A4). STALE is **recoverable**: the very next
          frame off the wire restores HEALTHY (WO-26b).

Liveness truth (WO-26b, 2026-08-25 — the recurring false-STALE wedge):
    Liveness is ``_last_frame_at``, stamped on **every** frame in :meth:`_handle_frame` — tick, order,
    heartbeat, even an undecodable one. Bytes off the wire ARE the proof the child is alive; the 1 s
    heartbeat frame is a *supplement* for a quiet tape, not the sole evidence. Stamping the dedicated
    heartbeat alone produced three multi-hour false-STALE wedges (08-20, 08-24 10:01, 08-25 11:28)
    while 46k ticks/5 min kept flowing and every bar kept being written.

    Two further guards close that loop:
      * **Loop-starvation discount.** The monitor is a task on the same event loop as the frame
        reader. When a long synchronous stretch elsewhere in the engine blocks the loop (08-24: the
        feature engine blocked it ~30 s per minute; ``ticks_flushed`` went silent then flooded 3,124
        ticks at once), NOTHING is read — so a wall-clock "silence" measured across that stretch is
        evidence about the ENGINE, not about the child. The monitor accumulates its own wake-up
        overshoot (``loop_starved_s``, reset whenever a frame lands) and subtracts it before judging.
        Ages themselves stay on the wall clock (R6); only the verdict is discounted.
      * **The kill is real.** :meth:`_respawn` used to self-deadlock (see its docstring), so the
        advertised kill-after-10 s never happened — the log said ``ticker_respawn`` and then nothing
        for 40 min / 2 h 14 min / 4 h 53 min. Fixed; genuine silence now actually restarts the child.

Subscription set (§3.2.2):
    universe watchlist + held symbols + **NIFTY 50 index** + **India VIX** tokens — the index/VIX feed
    powers §6.2 market-context features and the §7.1 stale-data guard; advance-decline is derived from
    the universe ticks. Changes go over a **control frame** on the live TCP link via
    :meth:`update_subscriptions`, not a respawn.

Phase 1 scope: real subprocess spawn/terminate (``asyncio.create_subprocess_exec``), per-spawn
secret + stdin orphan-protection, a real :meth:`health` computed from ``clock.now()``, AND the real
framing: the loopback ``asyncio`` server, the §2.4 handshake validation, length-prefixed msgpack
frame parsing (tick → :class:`~engine.core.types.Tick` → ``bus.publish("tick", …)``; order →
``order.update``; heartbeat → health), the ``subscribe`` control frame, and the stdin credentials
frame.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets as _secrets
import struct
import sys
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgpack
from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Tick
from engine.notify.catalog import CatalogMessage, feed_degraded, feed_wedged

#: Owner-notification sink type (§3.2.11): async, consumes a typed :class:`CatalogMessage`.
NotifyFn = Callable[[CatalogMessage], Awaitable[None]]

_log = get_logger("engine.broker.ticker_supervisor")

#: Canonical event bus topic for feed-health transitions (§3.2.1).
FEED_HEALTH_TOPIC = "feed.health"
#: Canonical event bus topic for parsed live ticks (§3.2.1) — consumed by ``BarBuilder`` (§3.2.3).
TICK_TOPIC = "tick"
#: Canonical event bus topic for broker order postbacks (§3.2.1, A3) — drives the OMS (§3.5.1).
ORDER_UPDATE_TOPIC = "order.update"

#: Length prefix on every frame: 4-byte big-endian unsigned int (mirror of ticker/main.py).
_LEN_PREFIX = struct.Struct(">I")
#: Wire-protocol version we accept from the child's ``hello`` (ticker/main.py ``_PROTOCOL_VERSION``).
PROTOCOL_VERSION = 1

#: Floor for a believable ``exchange_timestamp`` (2026-08-20 investigation). A tick whose wire field
#: is a zeroed/absent broker value arrives as Unix epoch 0 — kiteconnect turns it into
#: ``datetime.fromtimestamp(0)``, the child ISO-serializes it, and it used to sail through every
#: layer (the Tick validator only checks tz-awareness) into a ``date=1970-01-01`` tick partition;
#: 103 such rows accumulated before they were quarantined by hand. That is a zeroed field, not a
#: timestamp, so anything below this is dropped — via its OWN ``implausible_timestamp`` path
#: (WO-24e, 2026-08-21), distinct from a genuinely MISSING timestamp, with a single WARNING and no
#: exception traceback (pre-market ~16 not-yet-traded instruments send this on every resubscribe).
#: The floor is far below any real data (this platform's ticks start 2026-08) and far above epoch,
#: so it can only catch garbage.
_MIN_PLAUSIBLE_EXCHANGE_TS = datetime(2020, 1, 1, 0, 0, tzinfo=IST)

#: Monitor-cycle overshoot below which a late wake-up is just scheduler jitter, not starvation
#: (WO-26b). ``asyncio.sleep(1.0)`` on a loaded loop routinely lands a few hundred ms late; only
#: overshoot beyond this floor is credited against an observed frame silence. Deliberately well under
#: ``heartbeat_silence_kill_s`` (10 s) so ordinary jitter can never accumulate into a free pass.
_MONITOR_OVERSHOOT_FLOOR_S = 0.5

#: Cap on ``server.wait_closed()`` during read-loop teardown (WO-26b). Under CPython ≥3.12 that call
#: waits for every accepted connection to drop, so an unbounded await there hangs the whole respawn
#: behind a child that has not exited yet. Callers terminate the child first, so this only ever fires
#: for a child that ignored both SIGTERM and SIGKILL — and it logs when it does.
_SERVER_CLOSE_TIMEOUT_S = 5.0


class _ImplausibleTimestamp(ValueError):
    """A wire ``exchange_timestamp`` below :data:`_MIN_PLAUSIBLE_EXCHANGE_TS` (epoch-0 zeroed
    field) — NOT a missing one. Raised by :func:`_wire_timestamp`, propagated verbatim by
    :func:`parse_tick_frame`, and caught in :meth:`TickerSupervisor._parse_tick` BEFORE the
    generic parse-error handler so this working-as-designed drop gets its own reason/counter
    (``implausible_timestamp``) and skips the ERROR-level traceback — the WARNING already fired
    inside :func:`_wire_timestamp` (WO-24e, 2026-08-21)."""


class OrderUpdateFrame(BaseModel):
    """A verbatim Kite order postback (A3) as forwarded by the mt-ticker child.

    ``data`` is the raw ``on_order_update`` payload, untouched — the OMS correlates it against
    platform orders on the broker's own field names (§3.5.1). Published on ``order.update``.
    """

    data: dict[str, Any] = Field(default_factory=dict)


def _wire_decimal(value: Any) -> Decimal | None:
    """A wire decimal-string (or number) → ``Decimal``; None/empty passes through (§3.2 money)."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _wire_timestamp(value: Any, symbol: str | None = None) -> datetime | None:
    """A wire ISO-8601 NAIVE-IST timestamp → tz-aware IST ``datetime`` (§3.2 convention).

    ticker/main.py forwards KiteTicker's naive IST wall time as an ISO string; we attach
    ``Asia/Kolkata`` here. A tz-aware value (defensive) is converted, not re-stamped.

    A value older than :data:`_MIN_PLAUSIBLE_EXCHANGE_TS` raises :class:`_ImplausibleTimestamp`
    (own drop path, WO-24e) — see that constant: an epoch-0 wire field is not a timestamp, it is a
    zeroed field, not a MISSING one. A genuinely missing/empty value still returns None.
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

    The exact mirror of the child's serializer: prices arrive as decimal strings and re-wrap to
    ``Decimal`` exactly; ``volume_traded`` is the broker's CUMULATIVE day volume, verbatim (A13);
    ``exchange_timestamp`` is naive-IST ISO and becomes tz-aware IST. ``tradingsymbol`` is resolved
    by the caller (the wire carries only the instrument token). Raises ``ValueError`` on a frame
    missing its load-bearing fields; raises :class:`_ImplausibleTimestamp` (a ``ValueError``
    subclass, propagated verbatim from :func:`_wire_timestamp`) when ``exchange_timestamp`` is
    below :data:`_MIN_PLAUSIBLE_EXCHANGE_TS` (epoch-0) — its OWN drop path, distinct from a missing
    field (WO-24e). Either way the caller drops that single tick and never crashes the read loop.
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

# WIRE CONTRACT (single source of truth — ticker/main.py implements exactly this):
#   * TOPOLOGY: the ENGINE is the TCP SERVER — it listens on 127.0.0.1:<tcp_port> (loopback-only, §2.4)
#     and the child is the CLIENT that connects on spawn (ticker/main.py: reactor.connectTCP). Phase 1
#     runs an asyncio server here (asyncio.start_server) that accepts the child's connection.
#   * SECRETS CHANNEL: shared_secret + parent_pid travel via ENV (below); the Kite api_key + access_token
#     travel via STDIN as the first framed line (§2.4 — never env, never a routable frame — so they never
#     land in the process table). The child reads that one-time credentials line before connecting KiteTicker.
#: Env var carrying the per-spawn shared secret to the child (§2.4 handshake).
_ENV_SHARED_SECRET = "MT_TICKER_SHARED_SECRET"
#: Env var carrying the parent PID so the child can detect orphaning (§2.4).
_ENV_PARENT_PID = "MT_TICKER_PARENT_PID"
#: Env vars carrying the loopback TCP endpoint the ENGINE listens on and the child CONNECTS to (§2.4).
_ENV_TCP_HOST = "MT_TICKER_TCP_HOST"
_ENV_TCP_PORT = "MT_TICKER_TCP_PORT"


class FeedHealth(BaseModel):
    """Health snapshot of the ticker feed; drives the §7.1 ``stale_data_guard`` (R2).

    Attributes
    ----------
    last_tick_age_s:
        Seconds since the last tick frame, or ``None`` if no tick has been seen yet (e.g. WARMING /
        STOPPED). The §7.1 per-symbol max-tick-age check reads this alongside per-symbol ages.
    heartbeat_age_s:
        Seconds since the last 1 s heartbeat frame, or ``None`` if none seen. Diagnostic only since
        WO-26b — the kill decision reads ``last_frame_age_s`` (a tick proves liveness just as well).
    last_frame_age_s:
        Seconds since ANY frame (tick / order / heartbeat) was read off the link, or ``None`` if none
        seen. **This** is the liveness signal: silence beyond ``heartbeat_silence_kill_s`` (10 s)
        while running ⇒ kill + respawn (WO-26b).
    state:
        One of ``{"STOPPED", "WARMING", "HEALTHY", "DEGRADED", "STALE"}``. ``WARMING`` suppresses false
        feed-stale alarms on startup (§2.6). ``DEGRADED`` is the in-session tick-silence state (2026-07-22
        tickless-HEALTHY session): heartbeats are fine but NO ticks are arriving during market hours, so
        no self-built bars are being written — a visible, alerting state distinct from HEALTHY, that
        recovers to HEALTHY the moment ticks resume.
    """

    last_tick_age_s: float | None = None
    heartbeat_age_s: float | None = None
    last_frame_age_s: float | None = None
    state: str = "STOPPED"


class TickerSupervisor:
    """Owns the mt-ticker Twisted subprocess + its localhost TCP link (A4/A3/R2).

    Parameters
    ----------
    settings:
        Provides ``ticker.tcp_host`` / ``ticker.tcp_port`` / ``ticker.heartbeat_silence_kill_s`` /
        ``ticker.max_instruments_per_conn`` (A3 cap).
    clock:
        The single source of "now" — every age in :meth:`health` is derived from ``clock.now()``
        (never a bare ``datetime.now()``; §3.2 convention / R6).
    bus:
        Event bus for ``feed.health`` transitions and the parsed ``tick`` / ``order.update``
        streams. May be ``None`` in bare harnesses/tests — publishing is then skipped.
    symbol_for_token:
        Resolver from instrument token → tradingsymbol (the wire carries only the token; the core
        ``Tick`` requires the symbol). The composition root wires ``InstrumentStore``. A tick whose
        token cannot be resolved is dropped (logged once per token) — downstream consumers are
        keyed by symbol, so an unresolvable tick is unusable.
    api_key:
        Kite api_key handed to the child over stdin together with the access token (§2.4 — never
        env, never a routable frame).
    calendar:
        NSE calendar for the in-session tick-silence guard (§7.1): the guard only fires during market
        hours. ``None`` disables the guard (bare harness) — the feed then keeps heartbeat-only health.
    notify:
        Async owner-notification sink (§3.2.11) for the one-shot HEALTHY→DEGRADED alert. Best-effort;
        ``None`` skips the owner notify (the WARNING log + ``feed.health`` transition still fire).
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
        # Calendar + notify power the in-session tick-silence guard (§7.1): market-hours gating +
        # the one-shot owner alert on HEALTHY→DEGRADED. ``None`` (bare harnesses/tests) disables the
        # guard — a feed with no calendar cannot know it is in-session, so it keeps HEALTHY semantics.
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
        # Drain tasks for the child's stdout/stderr. Un-drained (the 2026-07-22 defect) the child's OWN
        # diagnostics — including the reason a feed goes tickless — are discarded, and a full pipe buffer
        # eventually WEDGES the child mid-write. Fail toward visibility: pump both into the engine log.
        self._output_tasks: list[asyncio.Task[None]] = []

        # --- health signal (timestamps as tz-aware IST via clock.now(), R6) ---
        self._state: str = "STOPPED"
        self._last_tick_at = None  # type: ignore[var-annotated]
        self._last_heartbeat_at = None  # type: ignore[var-annotated]
        # THE liveness stamp (WO-26b): refreshed by EVERY frame on the frame path, so it can never be
        # starved separately from the data it guards. ``_last_heartbeat_at`` is now diagnostic only.
        self._last_frame_at = None  # type: ignore[var-annotated]
        self._started_at = None  # type: ignore[var-annotated]
        self._healthy_since = None  # type: ignore[var-annotated]  # entry into HEALTHY (tick-silence ref)

        # --- silence-episode logging (one line per episode, not one per monitor tick) ---
        self._silence_logged = False        # ticker_heartbeat_silence fired for the current episode
        self._starvation_logged = False     # kill discounted as loop starvation, this episode

        # --- WARMING-wedge backoff (2026-07-23 13:41 sleep/resume). Consecutive WARMING-timeout
        #     respawns that never reach HEALTHY; drives the capped exponential backoff and the
        #     one-shot owner escalation. Both reset on a successful WARMING->HEALTHY promotion. ---
        self._wedge_respawns = 0
        self._wedge_escalated = False

        # --- feed_stats counters (R8 observability): zero-cost increments, drained by stats_snapshot() ---
        self._ticks_received = 0
        self._frames_dropped: dict[str, int] = {}

        self._lock = asyncio.Lock()  # serialize start/stop/respawn

    # ------------------------------------------------------------------ public API

    async def start(self, tokens: list[int], access_token: str) -> None:
        """Spawn ``ticker/main.py`` as a child and enter the WARMING state (§2.6).

        The subscription ``tokens`` (universe watchlist + held symbols + NIFTY 50 + India VIX) and the
        Kite ``access_token`` are handed to the child; the access token + the per-spawn shared secret
        cross the §2.4 trust boundary out-of-band (env + stdin), never as a routable frame.
        """
        if not self._api_key:
            # Fail LOUD, never dial: an empty api_key in the WS URL is a guaranteed 400-BadRequest
            # upgrade-reject loop (2026-07-23 root cause — the child reconnected forever while the
            # heartbeat kept the feed looking HEALTHY; zero ticks were ever captured).
            _log.error("ticker_start_refused_no_api_key")
            return
        async with self._lock:
            if self._proc is not None and self._proc.returncode is None:
                _log.info("ticker_start_noop_already_running", pid=self._proc.pid)
                return
            self._tokens = list(tokens)
            self._access_token = access_token
            await self._spawn_child()

    async def update_subscriptions(self, tokens: list[int]) -> None:
        """Update the live subscription set via a control frame on the TCP link (no respawn).

        Writes a length-prefixed msgpack ``{"type":"subscribe","tokens":[…]}`` frame; the child
        diffs the set, (un)subscribes, and re-asserts FULL mode (ticker/main.py). The set is capped
        at ``settings.ticker.max_instruments_per_conn`` (A3, ≤3,000/conn).
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
        """Stop the child: close stdin (orphan protection, §2.4) then terminate (A4).

        Teardown order carries the same WO-26b constraint as :meth:`_respawn`, plus one of its own:

        1. the **monitor** first — it must not respawn the child we are about to kill;
        2. the **child** next — its exit closes the link, which is what lets a cancelled
           :meth:`_read_loop`'s ``server.wait_closed()`` return at all under CPython ≥3.12;
        3. the **read loop** last, so it is not leaked.
        """
        async with self._lock:
            await self._cancel_monitor_task()
            await self._terminate_child()
            await self._cancel_read_task()
            self._set_state("STOPPED")
            _log.info("ticker_stopped")

    def health(self) -> FeedHealth:
        """Return the current :class:`FeedHealth`; ages computed from ``clock.now()`` (R2/R6)."""
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
        """Resolve ``ticker/main.py`` — the separate Twisted program at the REPO ROOT (§3.2.2).

        ``ticker/`` deliberately lives outside ``src/engine`` (it must never import ``engine.*``,
        §2.2): src/engine/broker/ticker_supervisor.py → parents[3] == the repo root.
        """
        repo_root = Path(__file__).resolve().parents[3]
        return repo_root / "ticker" / "main.py"

    async def _spawn_child(self) -> None:
        """Launch a fresh child with a new per-spawn secret; arm the read + monitor loops.

        The reactor cannot restart in-process (A4), so every (re)spawn is a brand-new OS process. A new
        shared secret per spawn (§2.4) means a frame minted against a previous child's secret is rejected.
        """
        self._shared_secret = _secrets.token_hex(32)
        entrypoint = self._ticker_entrypoint()

        # The ENGINE is the TCP server (WIRE CONTRACT at module top): bind the loopback listener
        # BEFORE spawning the child, or the child's immediate connect would be refused.
        self._read_task = asyncio.create_task(self._read_loop(), name="ticker-read-loop")
        await self._wait_server_ready()

        env = {
            _ENV_SHARED_SECRET: self._shared_secret,
            _ENV_PARENT_PID: str(_current_pid()),
            _ENV_TCP_HOST: self._settings.ticker.tcp_host,
            _ENV_TCP_PORT: str(self._settings.ticker.tcp_port),
        }
        # Inherit the parent environment (PATH, credential-manager access, OAuth token injected at
        # startup per §2.4) and overlay the handshake/loopback vars.
        full_env = {**_os_environ(), **env}

        self._proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(entrypoint),
            stdin=asyncio.subprocess.PIPE,  # kept open = liveness; closing it orphan-kills the child (§2.4)
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=full_env,
        )

        # Drain stdout+stderr into the engine log IMMEDIATELY (before the handshake — the child logs to
        # stderr while it reads its stdin credentials and connects KiteTicker). Un-drained, those pipes
        # (a) hide the child's own explanation of a tickless/dead feed and (b) fill their OS buffer and
        # wedge the child on its next stderr write. Both are the 2026-07-22 defect's blind spot.
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

        # The access token crosses the trust boundary on stdin (not env, not a frame) so it never lands
        # in the process table; Phase 1 hands the token + initial subscription set here.
        await self._send_startup_handshake()

        self._started_at = self._clock.now()
        self._last_tick_at = None
        self._last_heartbeat_at = None
        self._last_frame_at = None
        self._healthy_since = None
        self._set_state("WARMING")  # suppress false feed-stale alarms until first ticks (§2.6)

        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="ticker-monitor-loop")

        _log.info(
            "ticker_spawned",
            pid=self._proc.pid,
            entrypoint=str(entrypoint),
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
            tokens=len(self._tokens),
        )

    async def _send_startup_handshake(self) -> None:
        """Deliver the credentials + initial subscriptions to the child over stdin (§2.4).

        One length-prefixed msgpack frame ``{"api_key", "access_token", "tokens"}`` — the SECRETS
        CHANNEL of the wire contract: never env, never a routable frame, so credentials never land
        in the process table. The child reads it before connecting KiteTicker
        (ticker/main.py ``_read_stdin_credentials``). Stdin then stays OPEN as the liveness signal
        (closing it is the orphan-protection kill, §2.4) — the child ignores further stdin bytes.
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
        # Leave stdin OPEN (do NOT close) so the child stays adopted (§2.4 orphan protection).

    async def _drain_child_stream(self, stream: Any, name: str, level: str) -> None:
        """Pump one child pipe (stdout/stderr) line-by-line into the engine log until EOF (R8).

        This is the 2026-07-22 fix: the supervisor captured the child's stderr into a PIPE nothing ever
        read, so the child's own diagnostics (KiteTicker connect/close/reconnect/noreconnect, the reason
        a feed went tickless) were discarded AND a full pipe buffer would eventually wedge the child on
        its next write. Child stderr is its error channel ⇒ WARNING; stdout is unexpected ⇒ INFO. Never
        raises out — a drain failure must never take down supervision.
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
        """Cancel + await the stdout/stderr drain tasks (idempotent). The child is already dead by the
        time this runs, so the pipes are at EOF; this is leak-cleanup for a killed/wedged child whose
        streams never closed."""
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

        # 1) Close stdin: a well-behaved child treats stdin EOF as "parent gone" and exits (§2.4).
        if proc.stdin is not None and not proc.stdin.is_closing():
            try:
                proc.stdin.close()
            except (OSError, RuntimeError):  # pragma: no cover - stdin already torn down
                pass

        # 2) Graceful terminate, then hard kill if it ignores us (A4 — a wedged reactor must still die).
        try:
            proc.terminate()
        except ProcessLookupError:  # pragma: no cover - exited between checks
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
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:  # pragma: no cover - OS-level wedge
                _log.error("ticker_kill_timeout", pid=proc.pid)
        # The child is dead ⇒ its stdout/stderr are at EOF; reap the drain tasks (leak-cleanup).
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
        """The loopback TCP server owning the child's data link (WIRE CONTRACT at module top).

        The ENGINE is the server: ``asyncio.start_server`` on ``127.0.0.1:<tcp_port>`` accepts the
        child's inbound connection (the child is the client, ``reactor.connectTCP``). Frame parsing
        and dispatch live in :meth:`_handle_child_connection`. Cancellation (stop/respawn) closes
        the listener so a respawn can rebind the port.

        The cleanup wait is BOUNDED (WO-26b). ``server.close()`` releases the listening socket
        synchronously — that alone is what a rebind needs — but ``server.wait_closed()`` under
        CPython ≥3.12 additionally blocks until every accepted connection has dropped. Awaiting it
        unbounded inside a ``finally`` that runs during cancellation is how this task became an
        un-cancellable hang (:meth:`_respawn` docstring). Callers now terminate the child first, so
        the wait normally returns in milliseconds; the timeout is the backstop for a child that will
        not let go, and it is logged rather than swallowed.
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
            self._child_writer = None
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
        """Authenticate one inbound child connection (§2.4) and pump its frames.

        The FIRST frame must be a ``hello`` echoing this spawn's shared secret — else the
        connection is dropped before any tick/order frame is honoured (a fabricated local frame
        must not be able to inject a phantom fill; loopback bind + per-spawn secret + REST
        reconciliation backstop, §2.4).
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

        This is also the ONLY place liveness is stamped (WO-26b). Every frame — tick, order,
        heartbeat, even an undecodable one that :meth:`_read_frame` reduced to ``{}`` — is proof the
        child is alive and the link is carrying bytes, so it refreshes ``_last_frame_at`` before any
        type dispatch. Putting the stamp here (rather than on the heartbeat branch alone) means
        liveness CANNOT be starved separately from the data it guards: whatever stalls the frame path
        stalls both, and whatever revives it revives both.
        """
        await self._mark_alive()
        ftype = frame.get("type")
        if ftype == "heartbeat":
            self._last_heartbeat_at = self._clock.now()
            if self._state == "WARMING":
                # First heartbeat proves the link + child are live: promote WARMING → HEALTHY (§2.6).
                # The §7.1 warm-up ENTRY gate (ops.warmup) is separate — this is feed health only.
                self._set_state("HEALTHY")
                await self._publish_health()
        elif ftype == "tick":
            self._ticks_received += 1
            self._last_tick_at = self._clock.now()
            if self._state == "WARMING":
                # Same principle as the liveness stamp (WO-26b): a TICK is at least as good a proof
                # of life as a heartbeat, so it promotes out of warm-up too. Otherwise a child whose
                # heartbeat LoopingCall never started — but whose ticks flow perfectly — would sit in
                # WARMING being respawned every warming_timeout_s forever, discarding a working feed.
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
            # Verbatim Kite postback (A3); the OMS correlates it (§3.5.1).
            if self._bus is not None:
                self._bus.publish(ORDER_UPDATE_TOPIC, OrderUpdateFrame(data=frame.get("data") or {}))
        else:
            _log.warning("ticker_unknown_frame", frame_type=str(ftype))
            self._drop("unknown_frame")

    async def _mark_alive(self) -> None:
        """Refresh liveness from an arriving frame and recover a STALE feed (WO-26b).

        Called from :meth:`_handle_frame` for every frame. Two jobs:

        1. Stamp ``_last_frame_at`` — the age the monitor's kill decision reads.
        2. **Recover.** STALE used to be a one-way door: only the heartbeat branch could promote, and
           only out of WARMING, so a feed that went STALE while the child was fine stayed STALE until
           something respawned it. On 08-24 and 08-25 that respawn was itself deadlocked, so STALE
           held for 40 min / 2 h 14 min with ~46,000 ticks per 5 min still flowing through this very
           method. Recovery must not depend on a reconnect that will never come: frames are present,
           therefore the feed is healthy, therefore say so — and publish it, because the §7.1
           stale-data guard can FREEZE entries off this state.
        """
        self._last_frame_at = self._clock.now()
        if self._state == "STALE":
            self._set_state("HEALTHY")
            _log.info("feed_stale_recovered", frames_resumed=True)
            await self._publish_health()

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
            # Working-as-designed drop (epoch-0 zeroed field, e.g. a not-yet-traded instrument's
            # pre-market resubscribe snapshot) — own reason/counter, no ERROR traceback: the
            # WARNING already fired inside _wire_timestamp (WO-24e, 2026-08-21).
            self._drop("implausible_timestamp")
            return None
        except Exception:  # noqa: BLE001 - malformed frame is dropped, never crashes the read loop
            _log.exception("ticker_tick_parse_error", instrument_token=token)
            self._drop("parse_error")
            return None

    async def _monitor_loop(self) -> None:
        """Frame-silence watchdog: kill + respawn on >``heartbeat_silence_kill_s`` (R2/A4).

        Runs while a child is alive. Each tick it re-derives the health state from ``clock.now()``; when
        the feed goes STALE while *running* (not WARMING — §2.6 suppression) it triggers a respawn and
        publishes the ``feed.health`` STALE transition. WARMING is no longer exempt from ALL timeouts:
        an unbounded WARMING is itself a wedge (2026-07-23), so ``_check_warming_timeout`` bounds it.
        Also reaps an unexpectedly dead child.

        WO-26b, the loop-starvation discount. This coroutine and the frame reader share one event
        loop. A long synchronous stretch anywhere in the engine blocks BOTH — the reader stops
        stamping liveness and this loop stops observing — yet the wall clock keeps running, so on the
        late wake-up the silence looks like a dead child. It is not: the child's frames are queued in
        TCP and land in a burst moments later (08-24: no ``ticks_flushed`` for ~34 s, then 3,124 ticks
        in one flush, with ``store_stalled seconds_since_last_success=900.154`` naming the same
        starvation). So the loop measures its OWN wake-up overshoot, accumulates it while liveness is
        un-refreshed, and subtracts it before pulling the trigger. Ages stay on the wall clock (R6);
        only the verdict is discounted, and the discount is logged.
        """
        kill_after = float(self._settings.ticker.heartbeat_silence_kill_s)
        tick_silence_budget = float(self._settings.ticker.tick_silence_degrade_s)
        warming_timeout = float(self._settings.ticker.warming_timeout_s)
        warming_cap = float(self._settings.ticker.warming_backoff_cap_s)
        max_wedge = int(self._settings.ticker.max_wedge_respawns)
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
                    # A frame landed since the last cycle: liveness is fresh, so is the measurement.
                    last_frame_seen = self._last_frame_at
                    starved_s = 0.0
                    self._starvation_logged = False
                elif overshoot > _MONITOR_OVERSHOOT_FLOOR_S:
                    starved_s += overshoot

                proc = self._proc
                if proc is None:
                    return

                # Child died on its own (KiteTicker crash, A3) — respawn.
                if proc.returncode is not None:
                    _log.warning("ticker_child_exited", pid=proc.pid, code=proc.returncode)
                    await self._respawn(reason="child_exited")
                    return

                heartbeat_age = self._heartbeat_age_s()
                if self._state == "WARMING":
                    # The HEALTHY-path heartbeat-silence kill below is suppressed during warm-up (a
                    # fresh spawn legitimately has no ticks/heartbeat yet, §2.6) — promotion to HEALTHY
                    # happens on the first heartbeat in the read loop. But WARMING must NOT be
                    # unbounded: a child that dies/hangs before its first heartbeat, or a system-resume
                    # that froze the machine mid-WARMING (2026-07-23 13:41), would otherwise wedge here
                    # forever (zero respawns, feed dead). Bound it with a timeout + capped backoff; a
                    # respawn returns (``_spawn_child`` installs a fresh monitor task).
                    if await self._check_warming_timeout(warming_timeout, warming_cap, max_wedge):
                        return
                    continue
                # THE liveness test (WO-26b): ANY frame counts, not just the dedicated heartbeat.
                silence = self._liveness_age_s()
                if silence is not None and silence > kill_after:
                    if silence - starved_s <= kill_after:
                        # Not the child's silence — ours. Say so honestly (once per episode) and let
                        # the queued frames land; a genuinely dead child stays silent and the next
                        # un-starved cycle fires for real.
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
                    await self._publish_health()  # R2 — STALE transition for the stale-data guard
                    # The kill_after contract, honoured: the SAME restart machinery the child's own
                    # ``tcp_connection_lost``/exit takes (``_respawn`` → terminate + fresh spawn).
                    await self._respawn(reason="heartbeat_silence")
                    return

                # Heartbeats fine, but is the FEED actually delivering ticks? A child heartbeats every
                # 1 s regardless of ticks, so a tickless feed used to read HEALTHY all session (the
                # 2026-07-22 defect). During market hours, tick silence past the budget ⇒ DEGRADED.
                await self._check_tick_silence(tick_silence_budget)
        except asyncio.CancelledError:  # pragma: no cover - normal on stop
            raise

    async def _check_tick_silence(self, budget_s: float) -> None:
        """In-session tick-silence guard (§7.1) — called each monitor tick.

        HEALTHY + inside market hours + effective tick age past ``budget_s`` ⇒ transition to the visible
        DEGRADED state (a tickless feed must NEVER present HEALTHY through a session again), emit a
        WARNING, publish the ``feed.health`` transition, and fire the one-shot owner notify. Off-hours or
        any non-HEALTHY state: no-op (heartbeat-only semantics — no false night alarms). Recovery to
        HEALTHY happens on the next tick in :meth:`_handle_frame`.
        """
        if self._state != "HEALTHY" or not self._in_market_hours():
            return
        silence = self._effective_tick_silence_s()
        if silence is None or silence <= budget_s:
            return
        self._set_state("DEGRADED")
        _log.warning("feed_tick_silence_degraded", tick_silence_s=round(silence, 1), budget_s=budget_s)
        await self._publish_health()
        await self._notify_degraded(silence, budget_s)

    async def _check_warming_timeout(
        self, timeout_s: float, cap_s: float, max_respawns: int
    ) -> bool:
        """WARMING-wedge guard (2026-07-23 13:41 sleep/resume) — called each monitor tick while WARMING.

        If no heartbeat has arrived within the backoff-scaled timeout, the child is wedged (dead/hung
        before its first heartbeat, or the machine froze mid-WARMING on OS sleep): kill + respawn.
        Returns ``True`` when a respawn was triggered so the monitor loop returns (``_spawn_child``
        installs a fresh monitor task); ``False`` (no-op) while still inside the budget.

        Consecutive wedge-respawns that never reach HEALTHY back off exponentially
        (``timeout_s × 2ⁿ``) capped at ``cap_s``, and past ``max_respawns`` escalate ONCE to the owner
        (the feed is structurally down — dead child / no network / rejected token). A successful
        WARMING→HEALTHY promotion resets the counter + escalation flag (see :meth:`_set_state`).
        """
        age = self._warming_age_s()
        effective = min(timeout_s * (2 ** self._wedge_respawns), cap_s)
        if age is None or age <= effective:
            return False
        self._wedge_respawns += 1
        _log.error(
            "ticker_warming_timeout",
            warming_age_s=round(age, 1),
            timeout_s=round(effective, 1),
            wedge_respawns=self._wedge_respawns,
        )
        # One-shot owner escalation once the feed has failed to come up max_respawns times in a row.
        if self._wedge_respawns >= max_respawns and not self._wedge_escalated:
            self._wedge_escalated = True
            await self._notify_wedged(self._wedge_respawns, age)
        # A wedged feed is a real feed-lost incident: publish STALE (fail toward visibility — the §7.1
        # stale-data guard can FREEZE entries) before the respawn re-enters WARMING.
        self._set_state("STALE")
        await self._publish_health()  # R2 — STALE transition for the stale-data guard
        await self._respawn(reason="warming_timeout")
        return True

    async def _notify_wedged(self, respawns: int, age_s: float) -> None:
        """One-shot owner escalation for a WEDGED feed (best-effort; never breaks supervision, §3.2.11)."""
        if self._notify is None:
            return
        try:
            await self._notify(feed_wedged(respawns=respawns, age_s=age_s))
        except Exception:  # noqa: BLE001 - a failed notify must never crash the monitor loop
            _log.exception("feed_wedged_notify_failed")

    def _in_market_hours(self, now: datetime | None = None) -> bool:
        """True iff ``now`` is inside today's NSE continuous session (calendar+clock). Without a
        calendar (bare harness) the feed cannot know it is in-session ⇒ False (guard disabled)."""
        if self._calendar is None:
            return False
        now = now or self._clock.now()
        session = self._calendar.session(now.date())
        if session is None:  # holiday / weekend / unverified horizon (R6)
            return False
        return session.open <= now <= session.close

    def _effective_tick_silence_s(self) -> float | None:
        """Seconds since the last tick — or, if NO tick has EVER been seen, since we entered HEALTHY.

        The 'never a single tick' case is exactly the 2026-07-22 outage, so it MUST count toward the
        silence budget rather than being excused as 'no tick yet' (which is what left it HEALTHY)."""
        ref = self._last_tick_at or self._healthy_since
        if ref is None:
            return None
        return (self._clock.now() - ref).total_seconds()

    async def _notify_degraded(self, age_s: float, budget_s: float) -> None:
        """One-shot owner alert on HEALTHY→DEGRADED (best-effort; never breaks supervision, §3.2.11)."""
        if self._notify is None:
            return
        try:
            await self._notify(feed_degraded(age_s=age_s, budget_s=budget_s))
        except Exception:  # noqa: BLE001 - a failed notify must never crash the monitor loop
            _log.exception("feed_degraded_notify_failed")

    def stats_snapshot(self) -> dict[str, Any]:
        """Return + reset the since-last-call feed counters for the periodic ``feed_stats`` line (R8).

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
        """Kill the current child and launch a fresh one (A4 — reactor cannot restart in-process).

        Called from the monitor loop. The old read loop is still cancelled before
        :meth:`_spawn_child` overwrites ``self._read_task`` (otherwise the parked read task runs
        forever), but it is cancelled **after** the child is terminated — see below. The monitor task
        is NOT cancelled here: the caller *is* the monitor task and returns immediately after this
        awaits, and ``_spawn_child`` installs a fresh monitor task (cancelling self would abort the
        respawn mid-flight).

        ORDER IS LOAD-BEARING (WO-26b — the bug that made ``kill_after_s`` a lie). Cancelling the
        read task first self-deadlocked: :meth:`_read_loop`'s ``finally`` awaits
        ``server.wait_closed()``, and since CPython 3.12 (this engine runs 3.12.13) that call waits
        for the server to close *and* every accepted connection to drop — including the live child
        link, whose handler only exits when the child goes away. The thing that makes the child go
        away is ``_terminate_child()``, two lines below, behind the same ``self._lock``. So the
        supervisor logged ``ticker_respawn`` and then froze: 40 min on 08-24 10:01, 2 h 14 min on
        08-25 11:28, 4 h 53 min on 08-25 03:37 — every wedge ending at the exact moment an unrelated
        ``ticker_link_closed`` finally released ``wait_closed()``. Terminating first breaks the
        cycle at the source; :meth:`_read_loop` also bounds the wait as a second line of defence.
        """
        async with self._lock:
            _log.warning("ticker_respawn", reason=reason)
            await self._terminate_child()
            await self._cancel_read_task()
            if self._access_token is None:
                _log.error("ticker_respawn_no_access_token")
                self._set_state("STOPPED")
                return
            await self._spawn_child()

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
        """Cancel and await only the monitor loop (idempotent). Used by :meth:`stop`, NEVER from
        inside the monitor loop itself — that is what :meth:`_cancel_read_task` is for.

        Split out of the old ``_cancel_supervision`` (WO-26b): teardown now interleaves a
        ``_terminate_child()`` between the two cancels, so they cannot be done in one sweep."""
        task = self._monitor_task
        self._monitor_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._read_task = None
        self._monitor_task = None

    # ------------------------------------------------------------------ health helpers

    def _heartbeat_age_s(self) -> float | None:
        if self._last_heartbeat_at is None:
            return None
        return (self._clock.now() - self._last_heartbeat_at).total_seconds()

    def _liveness_age_s(self) -> float | None:
        """Seconds since ANY frame arrived — the signal the kill decision reads (WO-26b).

        ``None`` before the first frame of this spawn (WARMING owns that window via
        :meth:`_warming_age_s`). A tick is as good a proof of life as a heartbeat, so the two can no
        longer disagree: this is stamped once, on the frame path, in :meth:`_mark_alive`."""
        if self._last_frame_at is None:
            return None
        return (self._clock.now() - self._last_frame_at).total_seconds()

    def _warming_age_s(self) -> float | None:
        """Seconds this spawn has been WARMING with no heartbeat yet — the WARMING-wedge clock.

        Reference is the last heartbeat if one somehow arrived without promoting (defensive; in WARMING
        the first heartbeat promotes to HEALTHY), else this spawn's ``_started_at``. ``None`` before the
        first spawn. Each (re)spawn resets ``_started_at``, so every WARMING episode is timed afresh."""
        ref = self._last_heartbeat_at or self._started_at
        if ref is None:
            return None
        return (self._clock.now() - ref).total_seconds()

    def _set_state(self, state: str) -> None:
        if state != self._state:
            _log.info("feed_health_transition", frm=self._state, to=state)
        if state == "HEALTHY" and self._state != "HEALTHY":
            # Reference instant for the tick-silence budget when NO tick has been seen yet (the
            # 2026-07-22 'never a single tick' case must still count as silence, §7.1).
            self._healthy_since = self._clock.now()
            # A successful (re)connect clears the WARMING-wedge backoff + one-shot escalation: the next
            # wedge episode starts fresh at the base timeout and can escalate again (2026-07-23).
            self._wedge_respawns = 0
            self._wedge_escalated = False
            # A new silence/starvation episode may log again once the feed has been healthy (WO-26b).
            self._silence_logged = False
            self._starvation_logged = False
        self._state = state

    async def _publish_health(self) -> None:
        """Publish the current :class:`FeedHealth` on ``feed.health`` (R2). No-op without a bus."""
        if self._bus is None:
            return
        await self._bus.apublish(FEED_HEALTH_TOPIC, self.health())


# --------------------------------------------------------------------------- tiny stdlib indirections
# Wrapped so the rest of the module reads cleanly and so tests can monkeypatch the process identity /
# environment without importing ``os`` at call sites.

async def _monitor_sleep(seconds: float) -> None:
    """One :meth:`TickerSupervisor._monitor_loop` cycle delay.

    A module-level indirection for the same reason as :func:`_current_pid` below: it gives a test a
    seam. The WO-26b watchdog logic is *about* wall-clock time versus the loop's own scheduling lag,
    which is untestable if a cycle really has to take a second — a test patches this to advance the
    fake clock (by exactly 1 s, or by 30 s to simulate a starved loop) and returns immediately.
    """
    await asyncio.sleep(seconds)


def _current_pid() -> int:
    import os

    return os.getpid()


def _os_environ() -> dict[str, str]:
    import os

    return dict(os.environ)
