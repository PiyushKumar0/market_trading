"""Daily Kite login lifecycle — ``SessionManager``.

The Kite access token rotates daily: it dies at ~06:00 IST and a fresh one must be minted
through the Kite Connect login flow before the engine may place any entry. This module owns that
lifecycle and is the *only* place an access token is created and stored.

Device-flow (the sanctioned path)
------------------------------------
1. ~08:30 IST the scheduler asks for :meth:`login_url` and the owner is sent the URL over Telegram.
2. The owner taps the link on their phone, logs into Kite, and Kite redirects to the LAN callback
   ``GET /kite/callback?request_token=...`` (UNAUTHENTICATED endpoint) which calls
   :meth:`complete_login`.
3. :meth:`complete_login` performs the checksum exchange (SHA-256 of
   ``api_key + request_token + api_secret`` — done for us by ``kc.generate_session``), extracts the
   ``access_token``, persists it via :class:`Secrets` (DPAPI), and confirms on Telegram.
4. Fallbacks: off-LAN, the owner copies the ``request_token`` from the address bar and sends
   ``/token <value>`` on Telegram; both land in :meth:`complete_login`.

Token validity is behavioural, NOT a hard clock: ``_rejected`` is set by :meth:`on_token_rejected` when a
live call comes back 403/TokenException, and :meth:`token_valid` is "token present and not rejected", so a
token the broker still honours past a nominal expiry stays usable and a rejected one is dead at once (fail
toward freezing entries). Because a boot on yesterday's expired token would otherwise read as valid,
:meth:`verify_token` probes it live ONCE at startup (``kc.profile()``) so ``token_valid`` is truthful before
hydrate/warm-up run — WITHOUT firing the mid-day invalidation hook (the boot self-test's
needs_login/login-prompt path owns owner comms then).

Dependencies: ``core`` only. ``KiteConnect`` is sync ``requests``-based, so network calls run in a thread
executor.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, KITE_API_SECRET, Secrets

_log = get_logger("engine.broker.session")

# Async hook fired on mid-day invalidation (:meth:`on_token_rejected`): the wiring layer publishes a
# risk/feed event so the gate freezes entries and the owner is alerted. Injected so ``broker`` need not
# import ``risk``.
InvalidationHook = Callable[[], Awaitable[None]]

# Async hook fired when :meth:`complete_login` mints a valid token (both login paths land there): the wiring
# layer registers the post-login recovery so a boot before the daily login re-arms the instant a token
# arrives. Fire-and-forget (see :meth:`_fire_login_hooks`).
LoginHook = Callable[[], Awaitable[None]]


class SessionManager:
    """Owns the daily Kite token: minting it (login flow) and tracking its validity."""

    def __init__(
        self,
        secrets: Secrets,
        clock: Clock,
        *,
        redirect_path: str = "/kite/callback",
    ) -> None:
        self._secrets = secrets
        self._clock = clock
        self._redirect_path = redirect_path

        # A lazily-built KiteConnect bound to the api_key, reused for login_url + the exchange.
        self._kc: KiteConnect | None = None

        # Behavioural validity state (NOT a hard clock — see module docstring).
        self._access_token: str | None = secrets.get_optional(KITE_ACCESS_TOKEN)
        self._rejected: bool = False
        #: Dedup flag for the MID-DAY circuit breaker ONLY — set when :meth:`on_token_rejected` fires
        #: ``_on_invalidated``, cleared by :meth:`complete_login`. Separate from ``_rejected`` because the
        #: boot probe also sets ``_rejected`` (silently); deduping on it would swallow the first genuine
        #: live rejection's alert.
        self._breaker_fired: bool = False
        self._last_success: datetime | None = None

        # Optional async hook fired on mid-day invalidation; wired by the caller.
        self._on_invalidated: InvalidationHook | None = None

        # Post-login re-trigger hooks fired fire-and-forget when a token becomes valid, plus the
        # set of in-flight hook tasks held so the loop cannot GC a running recovery mid-flight.
        self._login_hooks: list[LoginHook] = []
        self._login_tasks: set[asyncio.Task[None]] = set()

    # -- wiring ---------------------------------------------------------------------------------

    def set_invalidation_hook(self, hook: InvalidationHook | None) -> None:
        """Register the coroutine fired by :meth:`on_token_rejected` (publishes risk/feed event)."""
        self._on_invalidated = hook

    def add_login_hook(self, hook: LoginHook) -> None:
        """Register a coroutine fired (fire-and-forget) whenever :meth:`complete_login` mints a valid token."""
        self._login_hooks.append(hook)

    # -- login flow -----------------------------------------------------------------------------

    def login_url(self) -> str:
        """Return the Kite login URL the owner taps to start the daily flow.

        The redirect is configured in the Kite developer console; ``login_url()`` only needs the api_key.
        ``redirect_path`` is stored but read by nothing: the callback route is fixed at ``/kite/callback``.
        """
        return self._connect().login_url()

    async def complete_login(self, request_token: str) -> None:
        """Exchange ``request_token`` for an access token (in a thread executor) and persist it.

        Stores it via ``secrets.set`` (DPAPI), arms it on the KiteConnect instance, clears ``_rejected`` AND
        ``_breaker_fired`` (so a token that dies again later in the day re-alerts the owner), stamps
        ``_last_success``, then fires the login hooks.
        """
        api_secret = self._secrets.get(KITE_API_SECRET)
        kc = self._connect()

        loop = asyncio.get_running_loop()
        session = await loop.run_in_executor(
            None,
            lambda: kc.generate_session(request_token, api_secret=api_secret),
        )

        token = session["access_token"]
        self._secrets.set(KITE_ACCESS_TOKEN, token)
        kc.set_access_token(token)

        self._access_token = token
        self._rejected = False
        self._breaker_fired = False   # a fresh token re-arms the mid-day breaker's one-shot alert
        self._last_success = self._clock.now()
        _log.info("session_live", at=self._last_success.isoformat())
        self._fire_login_hooks()

    def _fire_login_hooks(self) -> None:
        """Fire every post-login hook as a task on the running loop (like :meth:`EventBus.publish`).

        Unlike :meth:`on_token_rejected` this does not await: recovery (instruments / backfill / warm-up /
        ticker) can take minutes and the login callback and ``/token`` command must return at once. A hook
        that raises is logged and affects neither the login result nor a sibling hook. Tasks are held in
        ``_login_tasks`` so the loop cannot GC a recovery mid-flight."""
        if not self._login_hooks:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        for hook in list(self._login_hooks):
            if loop is not None:
                task = loop.create_task(self._run_login_hook(hook))
                self._login_tasks.add(task)
                task.add_done_callback(self._login_tasks.discard)
            else:  # pragma: no cover - complete_login is always awaited inside a running loop
                asyncio.run(self._run_login_hook(hook))

    async def _run_login_hook(self, hook: LoginHook) -> None:
        try:
            await hook()
        except Exception:  # noqa: BLE001 - a post-login hook must never affect the login result
            _log.exception("login_hook_failed")

    # -- startup liveness probe -----------------------------------------------------------------

    async def verify_token(self) -> str:
        """Live-probe the persisted access token against the broker once at startup.

        One authenticated ``kc.profile()`` call (default executor). Returns a tag:

        * ``"no_api_key"`` — no ``KITE_API_KEY`` seeded yet (fresh install); nothing to probe.
        * ``"absent"`` — api_key present but no access token loaded (pre-login boot).
        * ``"rejected"`` — :class:`TokenException`: sets ``_rejected`` so :meth:`token_valid` is False.
          Deliberately does NOT fire ``_on_invalidated`` (mid-day freeze + alert only; at boot the
          self-test's login-prompt path owns owner comms) and never touches ``_breaker_fired``, so it
          cannot suppress a later genuine live rejection's alert.
        * ``"inconclusive"`` — any other error (network/DNS/5xx): cannot verify is NOT invalid, so ALL
          state is left untouched and the circuit breaker catches a dead token on the first real call.
        * ``"valid"`` — profile succeeded: :meth:`mark_success` stamps freshness.
        * ``"superseded"`` — a :meth:`complete_login` replaced the token while the probe was in flight
          (the callback route binds before this probe runs): the verdict is discarded, else an old
          token's rejection would mark the fresh token dead.
        """
        if not self._secrets.has(KITE_API_KEY):
            return "no_api_key"
        probed = self._access_token
        if probed is None:
            return "absent"
        kc = self._connect()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, lambda: kc.profile())
        except TokenException as exc:
            if self._access_token != probed:
                _log.warning("token_probe_superseded", outcome="rejected", error=str(exc))
                return "superseded"
            self._rejected = True
            _log.warning("token_probe_rejected", error=str(exc))
            return "rejected"
        except Exception as exc:  # noqa: BLE001 - cannot verify != invalid; leave all state untouched
            _log.warning("token_probe_inconclusive", error=str(exc), error_type=type(exc).__name__)
            return "inconclusive"
        if self._access_token != probed:
            _log.info("token_probe_superseded", outcome="valid")
            return "superseded"
        self.mark_success()
        _log.info("token_probe_ok")
        return "valid"

    # -- validity tracking ----------------------------------------------------------------------

    def token_valid(self) -> bool:
        """True iff a token is present and has not been rejected.

        Deliberately NOT a hard clock: the gate must keep trading on a token the broker still honours
        and must stop the instant the broker rejects one — both signalled behaviourally
        (``_rejected``), never by comparing ``now()`` to a nominal 06:00 expiry.
        """
        return self._access_token is not None and not self._rejected

    def mark_success(self) -> None:
        """Stamp a successful authenticated call. Only the boot probe (:meth:`verify_token`) calls
        this; ``_last_success`` is informational — :meth:`token_valid` ignores it."""
        self._last_success = self._clock.now()

    async def on_token_rejected(self, token: str | None) -> None:
        """Mid-day invalidation: a live call returned 403/TokenException.

        ``token`` is the access token the failing request was sent with; a rejection of a token that is no
        longer current (a re-login landed mid-call) is ignored, the same superseded rule as
        :meth:`verify_token`.

        Marks the token invalid and fires the invalidation hook (freeze entries + alert the owner).
        Idempotent: a burst of 403s fires the hook once. The dedup is ``_breaker_fired``, not ``_rejected``
        (which the boot probe also sets): a boot-probe rejection costs at most one redundant alert, whereas
        deduping against it would leave a frozen, unexplained day.
        """
        if token != self._access_token:
            _log.warning("token_rejected_superseded")
            return
        already = self._breaker_fired
        self._rejected = True
        _log.warning("token_rejected", at=self._clock.now().isoformat(), first=not already)
        if not already and self._on_invalidated is not None:
            # Set BEFORE awaiting: a burst's second rejection can arrive while the hook is still
            # in flight, and it must not fire a second alert.
            self._breaker_fired = True
            await self._on_invalidated()

    def access_token(self) -> str | None:
        """The current access token, or ``None`` if none has been minted/loaded yet."""
        return self._access_token

    def kite_connect(self) -> KiteConnect | None:
        """The api_key-bound :class:`KiteConnect` (access token armed if present), or ``None`` when no
        api_key is configured yet — so the composition root can build a :class:`KiteClient` ONLY once
        credentials exist (a fresh install stays runnable with entries FROZEN until login).

        The same instance backs the login flow, so a later :meth:`complete_login` arms the token on the
        very object the KiteClient already holds — no rewiring needed on daily re-login.
        """
        if not self._secrets.has(KITE_API_KEY):
            return None
        return self._connect()

    def api_key(self) -> str | None:
        """The configured Kite api_key, or None when not seeded. The ticker child needs it for the
        WEBSOCKET URL: an empty api_key is a permanent 400 upgrade-reject loop (REST is unaffected)."""
        return self._secrets.get(KITE_API_KEY) if self._secrets.has(KITE_API_KEY) else None

    # -- internals ------------------------------------------------------------------------------

    def _connect(self) -> KiteConnect:
        """Lazily build (and cache) the api_key-bound ``KiteConnect`` used for login + exchange."""
        if self._kc is None:
            self._kc = KiteConnect(api_key=self._secrets.get(KITE_API_KEY))
            if self._access_token is not None:
                self._kc.set_access_token(self._access_token)
        return self._kc

    # -- scripted-TOTP auto-login (owner-accepted policy violation, default OFF) -----------------
    #
    # Violates Kite's terms (automated credential entry) and stores TOTP seed material; the owner accepts
    # it ONLY behind an explicit config flag defaulting OFF. Deliberately NOT implemented.
    #
    #   async def _scripted_totp_login(self) -> None:
    #       raise NotImplementedError(
    #           "scripted-TOTP auto-login is an owner-accepted policy violation; gated behind a "
    #           "config flag (default OFF) and intentionally not implemented"
    #       )
