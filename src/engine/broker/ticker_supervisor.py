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
        * ``WARMING``  — child just spawned / reconnecting + warm-up backfilling; feed-stale alarm and
          respawn are **suppressed** (§2.6/§3.2.12) to avoid false alarms on a fresh startup, distinct
          from feed-lost-while-running.
        * ``HEALTHY``  — heartbeats arriving within the silence budget.
        * ``STALE``    — heartbeat silence exceeded ``settings.ticker.heartbeat_silence_kill_s`` (10 s)
          while *running* (not WARMING) ⇒ the supervisor kills + respawns the child and publishes a
          ``feed.health`` transition to ``STALE`` (R2/A4).

Subscription set (§3.2.2):
    universe watchlist + held symbols + **NIFTY 50 index** + **India VIX** tokens — the index/VIX feed
    powers §6.2 market-context features and the §7.1 stale-data guard; advance-decline is derived from
    the universe ticks. Changes go over a **control frame** on the live TCP link via
    :meth:`update_subscriptions`, not a respawn.

Phase 0 scope: real subprocess spawn/terminate (``asyncio.create_subprocess_exec``), real per-spawn
secret + stdin orphan-protection, and a real :meth:`health` that computes ages from ``clock.now()``. The
connection read loop + msgpack frame parsing + control-frame writing are documented **skeletons** —
real framing is Phase 1.
"""

from __future__ import annotations

import asyncio
import secrets as _secrets
import sys
from pathlib import Path

from pydantic import BaseModel

from engine.core.clock import Clock
from engine.core.config import Settings
from engine.core.eventbus import EventBus
from engine.core.log import get_logger

_log = get_logger("engine.broker.ticker_supervisor")

#: Canonical event bus topic for feed-health transitions (§3.2.1).
FEED_HEALTH_TOPIC = "feed.health"

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
        Seconds since the last 1 s heartbeat frame, or ``None`` if none seen. Silence beyond
        ``heartbeat_silence_kill_s`` (10 s) while running ⇒ kill + respawn.
    state:
        One of ``{"STOPPED", "WARMING", "HEALTHY", "STALE"}``. ``WARMING`` suppresses false feed-stale
        alarms on startup (§2.6).
    """

    last_tick_age_s: float | None = None
    heartbeat_age_s: float | None = None
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
        Event bus for publishing ``feed.health`` transitions.
    """

    def __init__(self, settings: Settings, clock: Clock, bus: EventBus) -> None:
        self._settings = settings
        self._clock = clock
        self._bus = bus

        # --- child process state ---
        self._proc: asyncio.subprocess.Process | None = None
        self._shared_secret: str | None = None
        self._access_token: str | None = None
        self._tokens: list[int] = []

        # --- supervision / read-loop tasks (Phase-1 frame parsing lives behind these) ---
        self._read_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None

        # --- health signal (timestamps as tz-aware IST via clock.now(), R6) ---
        self._state: str = "STOPPED"
        self._last_tick_at = None  # type: ignore[var-annotated]
        self._last_heartbeat_at = None  # type: ignore[var-annotated]
        self._started_at = None  # type: ignore[var-annotated]

        self._lock = asyncio.Lock()  # serialize start/stop/respawn

    # ------------------------------------------------------------------ public API

    async def start(self, tokens: list[int], access_token: str) -> None:
        """Spawn ``ticker/main.py`` as a child and enter the WARMING state (§2.6).

        The subscription ``tokens`` (universe watchlist + held symbols + NIFTY 50 + India VIX) and the
        Kite ``access_token`` are handed to the child; the access token + the per-spawn shared secret
        cross the §2.4 trust boundary out-of-band (env + stdin), never as a routable frame.
        """
        async with self._lock:
            if self._proc is not None and self._proc.returncode is None:
                _log.info("ticker_start_noop_already_running", pid=self._proc.pid)
                return
            self._tokens = list(tokens)
            self._access_token = access_token
            await self._spawn_child()

    async def update_subscriptions(self, tokens: list[int]) -> None:
        """Update the live subscription set via a control frame on the TCP link (no respawn).

        Phase-0 skeleton: records the desired set and (Phase 1) writes a length-prefixed msgpack control
        frame instructing the child to (un)subscribe. The set is capped at
        ``settings.ticker.max_instruments_per_conn`` (A3, ≤3,000/conn).
        """
        cap = self._settings.ticker.max_instruments_per_conn
        if len(tokens) > cap:
            _log.warning("ticker_subscription_over_cap", requested=len(tokens), cap=cap)
        self._tokens = list(tokens)
        if self._proc is None or self._proc.returncode is not None:
            _log.info("ticker_update_subscriptions_deferred_not_running", count=len(self._tokens))
            return
        # Phase 1: await self._write_control_frame({"type": "subscribe", "tokens": self._tokens})
        _log.info("ticker_update_subscriptions", count=len(self._tokens))

    async def stop(self) -> None:
        """Stop the child: close stdin (orphan protection, §2.4) then terminate (A4)."""
        async with self._lock:
            await self._cancel_supervision()
            await self._terminate_child()
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
        return FeedHealth(
            last_tick_age_s=last_tick_age,
            heartbeat_age_s=heartbeat_age,
            state=self._state,
        )

    # ------------------------------------------------------------------ spawn / terminate (real)

    def _ticker_entrypoint(self) -> Path:
        """Resolve ``ticker/main.py`` — the separate Twisted program (§3.2.2)."""
        # src/engine/broker/ticker_supervisor.py -> parents[2] == src/
        src_root = Path(__file__).resolve().parents[2]
        return src_root / "ticker" / "main.py"

    async def _spawn_child(self) -> None:
        """Launch a fresh child with a new per-spawn secret; arm the read + monitor loops.

        The reactor cannot restart in-process (A4), so every (re)spawn is a brand-new OS process. A new
        shared secret per spawn (§2.4) means a frame minted against a previous child's secret is rejected.
        """
        self._shared_secret = _secrets.token_hex(32)
        entrypoint = self._ticker_entrypoint()

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

        # The access token crosses the trust boundary on stdin (not env, not a frame) so it never lands
        # in the process table; Phase 1 hands the token + initial subscription set here.
        await self._send_startup_handshake()

        self._started_at = self._clock.now()
        self._last_tick_at = None
        self._last_heartbeat_at = None
        self._set_state("WARMING")  # suppress false feed-stale alarms until first ticks (§2.6)

        self._read_task = asyncio.create_task(self._read_loop(), name="ticker-read-loop")
        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="ticker-monitor-loop")

        _log.info(
            "ticker_spawned",
            pid=self._proc.pid,
            entrypoint=str(entrypoint),
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
            tokens=len(self._tokens),
        )

    async def _send_startup_handshake(self) -> None:
        """Phase-0 skeleton: deliver the access token + initial subscriptions to the child over stdin.

        Phase 1 writes the access token and the initial subscription set as the first framed message;
        the child must present the shared secret back on the TCP link before any tick/order frame is
        honoured (§2.4). Here we only document the channel and keep stdin open as the liveness signal.
        """
        proc = self._proc
        if proc is None or proc.stdin is None:
            return
        # Phase 1:
        #   payload = msgpack.packb({"access_token": self._access_token, "tokens": self._tokens})
        #   proc.stdin.write(_length_prefix(payload)); await proc.stdin.drain()
        # Phase 0: leave stdin open (do NOT close) so the child stays adopted (§2.4 orphan protection).
        return

    async def _terminate_child(self) -> None:
        """Close stdin (orphan protection) then terminate, escalating to kill if it does not exit."""
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.returncode is not None:
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
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            _log.warning("ticker_terminate_timeout_killing", pid=proc.pid)
            try:
                proc.kill()
            except ProcessLookupError:  # pragma: no cover
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:  # pragma: no cover - OS-level wedge
                _log.error("ticker_kill_timeout", pid=proc.pid)

    # ------------------------------------------------------------------ read + monitor loops (skeleton)

    async def _read_loop(self) -> None:
        """Connection read loop over 127.0.0.1:<tcp_port> (documented skeleton; Phase 1 parses frames).

        Phase 1 responsibilities (the ENGINE is the server — see the WIRE CONTRACT at module top):
            * run an ``asyncio`` server (``asyncio.start_server``) on the loopback endpoint and ACCEPT
              the child's inbound connection (the child is the client, ``reactor.connectTCP``);
            * validate the §2.4 shared-secret handshake on the child's first frame, else drop it;
            * read **length-prefixed msgpack** frames and dispatch by type:
                - ``tick``  → update ``_last_tick_at``; publish ``tick`` on the bus;
                - ``order`` → publish ``order.update`` (drives the OMS, A3) — a fabricated frame is
                  blocked by the handshake + loopback bind + reconciliation backstop (§2.4);
                - ``heartbeat`` (1 s) → update ``_last_heartbeat_at``; first heartbeat + warm-up done
                  promotes WARMING → HEALTHY (§2.6).

        Phase 0: park until cancelled so the task structure (and its cancellation on stop/respawn) is
        real and tested, without opening a real socket.
        """
        try:
            await asyncio.Event().wait()  # replaced by the real connect+read in Phase 1
        except asyncio.CancelledError:  # pragma: no cover - normal on stop/respawn
            raise

    async def _monitor_loop(self) -> None:
        """Heartbeat-silence watchdog: kill + respawn on >``heartbeat_silence_kill_s`` (R2/A4).

        Runs while a child is alive. Each tick it re-derives the health state from ``clock.now()``; when
        the feed goes STALE while *running* (not WARMING — §2.6 suppression) it triggers a respawn and
        publishes the ``feed.health`` STALE transition. Also reaps an unexpectedly dead child.
        """
        kill_after = float(self._settings.ticker.heartbeat_silence_kill_s)
        try:
            while True:
                await asyncio.sleep(1.0)
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
                    # Suppress feed-stale during warm-up; promotion to HEALTHY happens on first
                    # heartbeat in the Phase-1 read loop. Nothing to enforce here yet.
                    continue
                if heartbeat_age is not None and heartbeat_age > kill_after:
                    _log.error(
                        "ticker_heartbeat_silence",
                        heartbeat_age_s=round(heartbeat_age, 3),
                        kill_after_s=kill_after,
                    )
                    self._set_state("STALE")
                    await self._publish_health()  # R2 — STALE transition for the stale-data guard
                    await self._respawn(reason="heartbeat_silence")
                    return
        except asyncio.CancelledError:  # pragma: no cover - normal on stop
            raise

    async def _respawn(self, *, reason: str) -> None:
        """Kill the current child and launch a fresh one (A4 — reactor cannot restart in-process).

        Called from the monitor loop. Cancel the OLD read loop FIRST so it is not leaked when
        :meth:`_spawn_child` overwrites ``self._read_task`` — otherwise the parked read task runs
        forever. The monitor task is NOT cancelled here: the caller *is* the monitor task and returns
        immediately after this awaits, and ``_spawn_child`` installs a fresh monitor task (cancelling
        self would abort the respawn mid-flight).
        """
        async with self._lock:
            _log.warning("ticker_respawn", reason=reason)
            await self._cancel_read_task()
            await self._terminate_child()
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

    async def _cancel_supervision(self) -> None:
        """Cancel and await the read + monitor tasks (idempotent). Used by :meth:`stop`, never from
        inside the monitor loop (see :meth:`_cancel_read_task`)."""
        for task in (self._read_task, self._monitor_task):
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

    def _set_state(self, state: str) -> None:
        if state != self._state:
            _log.info("feed_health_transition", frm=self._state, to=state)
        self._state = state

    async def _publish_health(self) -> None:
        """Publish the current :class:`FeedHealth` on ``feed.health`` (R2)."""
        await self._bus.apublish(FEED_HEALTH_TOPIC, self.health())


# --------------------------------------------------------------------------- tiny stdlib indirections
# Wrapped so the rest of the module reads cleanly and so tests can monkeypatch the process identity /
# environment without importing ``os`` at call sites.

def _current_pid() -> int:
    import os

    return os.getpid()


def _os_environ() -> dict[str, str]:
    import os

    return dict(os.environ)
