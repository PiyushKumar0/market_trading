"""Daily Kite login lifecycle — ``SessionManager`` (§3.2.2, R6/A5).

The Kite access token rotates daily: it dies at ~06:00 IST (A5) and a fresh one must be minted
through the Kite Connect login flow before the engine may place any entry. This module owns that
lifecycle and is the *only* place an access token is created and stored.

R6 device-flow (the sanctioned path)
------------------------------------
1. ~08:30 IST the scheduler asks for :meth:`login_url` and the owner is sent the URL over Telegram.
2. The owner taps the link on their phone, logs into Kite, and Kite redirects to the LAN callback
   ``GET /kite/callback?request_token=...`` (UNAUTHENTICATED endpoint, §3.2.11) which calls
   :meth:`complete_login`.
3. :meth:`complete_login` performs the checksum exchange (SHA-256 of
   ``api_key + request_token + api_secret`` — done for us by ``kc.generate_session``), extracts the
   ``access_token``, persists it via :class:`Secrets` (DPAPI, R10), and confirms on Telegram.
4. Fallbacks (§13 runbook): off-LAN, the owner copies the ``request_token`` from the browser address
   bar and sends ``/token <value>`` on Telegram (or pastes it into the dashboard) — both routes land
   in :meth:`complete_login`. PC-side login in any browser works identically.

Token validity is tracked behaviourally, NOT off a hard clock (A5 note): we keep a ``_last_success``
stamp (set on a successful login) and a ``_rejected`` flag (set by :meth:`on_token_rejected` when a
live call comes back 403/TokenException). :meth:`token_valid` returns "token present and not rejected"
so a token the broker still honours past a nominal expiry stays usable, and one the broker rejects
*before* the nominal expiry is treated as dead immediately (R6 — fail toward freezing entries).

Validity is now behavioural AND probed live ONCE at startup (:meth:`verify_token`, added after the
2026-07-21 cold-start lockout): a boot on yesterday's expired token had ``_rejected=False`` and a
token present, so behavioural :meth:`token_valid` answered True and the engine ground the warm-up
backfill on a dead token before any live call surfaced the rejection. The startup probe calls
``kc.profile()`` and marks the token rejected (or confirms it live) so ``token_valid`` is truthful
before hydrate/warm-up run — WITHOUT firing the mid-day invalidation hook (the boot self-test's
needs_login/login-prompt path owns owner comms at that stage).

Dependencies: ``core`` only (Secrets, Clock, log). pykiteconnect's ``KiteConnect`` is sync
``requests``-based, so the network exchange runs in a thread executor to avoid blocking the loop.
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

# Async hook invoked when the session is invalidated mid-day (:meth:`on_token_rejected`). The caller
# (wiring layer) sets it to a coroutine that publishes a risk/feed event so the gate freezes entries
# and the owner is alerted (R6). Kept as an injectable hook — not a hard EventBus dependency — so the
# constructor signature stays the one the plan mandates and ``broker`` need not import ``risk``.
InvalidationHook = Callable[[], Awaitable[None]]

# Async hook invoked when the session transitions to a VALID token via :meth:`complete_login` — the
# convergence point of BOTH login paths (the LAN ``/kite/callback`` route and the Telegram ``/token``
# fallback both land there, §3.2.11). The wiring layer registers the §2.6 post-login recovery here so
# a boot BEFORE the daily login (token-less ⇒ ticker never started, warm-up frozen forever) is
# RE-TRIGGERED the instant a token arrives. Fired fire-and-forget (see :meth:`_fire_login_hooks`).
LoginHook = Callable[[], Awaitable[None]]


class SessionManager:
    """Owns the daily Kite token: minting it (login flow) and tracking its validity (R6/A5)."""

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
        self._last_success: datetime | None = None

        # Optional async hook fired on mid-day invalidation; wired by the caller (R6).
        self._on_invalidated: InvalidationHook | None = None

        # Post-login re-trigger hooks (§2.6) fired fire-and-forget when a token becomes valid, plus the
        # set of in-flight hook tasks held so the loop cannot GC a running recovery mid-flight.
        self._login_hooks: list[LoginHook] = []
        self._login_tasks: set[asyncio.Task[None]] = set()

    # -- wiring ---------------------------------------------------------------------------------

    def set_invalidation_hook(self, hook: InvalidationHook | None) -> None:
        """Register the coroutine fired by :meth:`on_token_rejected` (publishes risk/feed event, R6)."""
        self._on_invalidated = hook

    def add_login_hook(self, hook: LoginHook) -> None:
        """Register a coroutine fired (fire-and-forget) whenever :meth:`complete_login` mints a valid
        token — BOTH login paths land there (§2.6 re-trigger). The wiring layer registers the
        post-login recovery so a boot before the daily login re-arms the moment the owner logs in."""
        self._login_hooks.append(hook)

    # -- login flow -----------------------------------------------------------------------------

    def login_url(self) -> str:
        """Return the Kite login URL the owner taps to start the daily flow (R6).

        Kite appends ``?request_token=...&action=login&status=success`` to the app's configured
        redirect (``redirect_path``) on success; the redirect itself is configured in the Kite
        developer console, not passed here — ``KiteConnect.login_url()`` only needs the api_key.
        """
        return self._connect().login_url()

    async def complete_login(self, request_token: str) -> None:
        """Exchange ``request_token`` for an access token and persist it (R6/A5).

        Runs ``kc.generate_session(request_token, api_secret=...)`` in a thread executor (the
        underlying call is blocking ``requests`` I/O). On success: extracts ``access_token``, stores
        it via ``secrets.set(KITE_ACCESS_TOKEN, token)`` (DPAPI, R10), arms it on the KiteConnect
        instance, clears ``_rejected``, and stamps ``_last_success`` from the Clock.
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
        self._last_success = self._clock.now()
        _log.info("session_live", at=self._last_success.isoformat())
        # §2.6 RE-TRIGGER: a token just became valid — fire the post-login recovery hooks. Both login
        # paths reach here, so this is the single place that catches "the token is now good".
        self._fire_login_hooks()

    def _fire_login_hooks(self) -> None:
        """Fire every registered post-login hook fire-and-forget (§2.6 re-trigger).

        Deliberately unlike :meth:`on_token_rejected` (which awaits its hook inline): the recovery a
        login hook runs — instruments / backfill / warm-up / ticker — can take seconds to minutes, and
        the login HTTP callback (the owner's phone) plus the Telegram ``/token`` command must return at
        once. So this mirrors :meth:`EventBus.publish` semantics — schedule each hook as a task on the
        running loop. A hook that raises is isolated + logged; it can never affect the login result or a
        sibling hook. Tasks are held in ``_login_tasks`` so the loop cannot GC a recovery mid-flight."""
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
        """Live-probe the persisted access token against the broker once at startup (R6/A5).

        Behavioural :meth:`token_valid` cannot tell a still-honoured token from an expired one that
        no live call has yet hit — the 2026-07-21 lockout (a boot on yesterday's token passed the
        self-test and ground the warm-up backfill on a dead token). This probe closes that gap by
        making one authenticated ``kc.profile()`` call. Returns a tag:

        * ``"no_api_key"`` — no ``KITE_API_KEY`` seeded yet (fresh install); nothing to probe.
        * ``"absent"`` — api_key present but no access token loaded (pre-login boot).
        * ``"rejected"`` — the broker returned :class:`TokenException`: sets ``_rejected`` so
          :meth:`token_valid` is now False. Deliberately does NOT fire the ``_on_invalidated`` hook
          — at boot the self-test's needs_login / login-prompt path owns owner comms; that hook is
          for MID-DAY invalidation (freeze + alert) only.
        * ``"inconclusive"`` — any other error (network/DNS/5xx): cannot verify is NOT the same as
          invalid, so ALL state is left untouched and the Fix-3 circuit breaker catches a truly dead
          token on the first real call.
        * ``"valid"`` — profile succeeded: :meth:`mark_success` stamps freshness.

        ``kc.profile()`` is sync ``requests`` I/O (pykiteconnect), so it runs in the default executor
        — the same pattern as :meth:`complete_login`.
        """
        if not self._secrets.has(KITE_API_KEY):
            return "no_api_key"
        if self._access_token is None:
            return "absent"
        kc = self._connect()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, lambda: kc.profile())
        except TokenException as exc:
            self._rejected = True
            _log.warning("token_probe_rejected", error=str(exc))
            return "rejected"
        except Exception as exc:  # noqa: BLE001 - cannot verify != invalid; leave all state untouched
            _log.warning("token_probe_inconclusive", error=str(exc), error_type=type(exc).__name__)
            return "inconclusive"
        self.mark_success()
        _log.info("token_probe_ok")
        return "valid"

    # -- validity tracking ----------------------------------------------------------------------

    def token_valid(self) -> bool:
        """True iff a token is present and has not been rejected (R6/A5).

        Deliberately NOT a hard clock: the gate must keep trading on a token the broker still honours
        and must stop the instant the broker rejects one — both signalled behaviourally
        (``_last_success`` / ``_rejected``), never by comparing ``now()`` to a nominal 06:00 expiry.
        """
        return self._access_token is not None and not self._rejected

    def mark_success(self) -> None:
        """Stamp a successful authenticated call (KiteClient calls this — keeps the token 'fresh')."""
        self._last_success = self._clock.now()

    async def on_token_rejected(self) -> None:
        """Mid-day invalidation: a live call returned 403/TokenException (R6).

        Marks the token invalid and fires the invalidation hook (which publishes a risk/feed event);
        the caller freezes entries and alerts the owner. Idempotent — repeated 403s in a burst only
        fire the hook the first time so we don't spam the owner.
        """
        already = self._rejected
        self._rejected = True
        _log.warning("token_rejected", at=self._clock.now().isoformat(), first=not already)
        if not already and self._on_invalidated is not None:
            await self._on_invalidated()

    def access_token(self) -> str | None:
        """The current access token, or ``None`` if none has been minted/loaded yet."""
        return self._access_token

    def kite_connect(self) -> KiteConnect | None:
        """The api_key-bound :class:`KiteConnect` (access token armed if present), or ``None`` when no
        api_key is configured yet — so the composition root can build a :class:`KiteClient` ONLY once
        credentials exist (a fresh install stays runnable with entries FROZEN until login, §2.6/R6).

        The same instance backs the login flow, so a later :meth:`complete_login` arms the token on the
        very object the KiteClient already holds — no rewiring needed on daily re-login.
        """
        if not self._secrets.has(KITE_API_KEY):
            return None
        return self._connect()

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
    # A5/§7.3: a scripted-TOTP headless auto-login would remove the daily manual tap, but it violates
    # Kite's terms (automating the credential entry) and stores TOTP seed material. The owner has
    # accepted this trade-off ONLY behind an explicit config flag that defaults OFF. This is left as a
    # clearly-marked stub on purpose — it is NOT implemented in Phase 0.
    #
    #   async def _scripted_totp_login(self) -> None:
    #       raise NotImplementedError(
    #           "scripted-TOTP auto-login is an owner-accepted policy violation; gated behind a "
    #           "config flag (default OFF, A5/§7.3) and intentionally not implemented"
    #       )
