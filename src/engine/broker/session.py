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

Dependencies: ``core`` only (Secrets, Clock, log). pykiteconnect's ``KiteConnect`` is sync
``requests``-based, so the network exchange runs in a thread executor to avoid blocking the loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

from kiteconnect import KiteConnect

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, KITE_API_SECRET, Secrets

_log = get_logger("engine.broker.session")

# Async hook invoked when the session is invalidated mid-day (:meth:`on_token_rejected`). The caller
# (wiring layer) sets it to a coroutine that publishes a risk/feed event so the gate freezes entries
# and the owner is alerted (R6). Kept as an injectable hook — not a hard EventBus dependency — so the
# constructor signature stays the one the plan mandates and ``broker`` need not import ``risk``.
InvalidationHook = Callable[[], Awaitable[None]]


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

    # -- wiring ---------------------------------------------------------------------------------

    def set_invalidation_hook(self, hook: InvalidationHook | None) -> None:
        """Register the coroutine fired by :meth:`on_token_rejected` (publishes risk/feed event, R6)."""
        self._on_invalidated = hook

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
