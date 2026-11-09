"""Pre-open Kite access-token validity check.

The Kite access token is minted daily and dies at ~06:00 IST. The boot probe
(:meth:`engine.broker.session.SessionManager.verify_token`) only fires at boot, so an engine up since
the previous evening would otherwise find a stale token mid-session.

One authenticated REST call at 08:40 IST, with a loud owner alert if it fails. Deliberately NOT a
registry job and carries NO ``job_runs`` watermark: a pre-open check replayed by a catch-up pass at
14:00 answers a question nobody is asking. The boot probe and the mid-day circuit breaker remain the
other two layers.

**Never load-bearing.** :meth:`TokenCheckJob.run` does not raise, does not freeze, does not gate
anything. Anything that is not an authentication failure degrades to a WARNING + a warning-severity
notification saying the check was inconclusive — "cannot verify != is invalid", as in ``verify_token``.

Log events: ``token_check_ok`` (INFO), ``token_check_failed`` (CRITICAL), ``token_check_error``
(WARNING), ``token_check_skipped_non_trading_day`` (INFO).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import time
from typing import Any

from kiteconnect.exceptions import TokenException

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.notify.catalog import CatalogMessage, MessageKind, login_instruction

_log = get_logger("engine.ops.token_check")

#: Fire-time (IST): early enough for the owner to log in before the open, late enough that a token
#: minted on the normal ~08:30 prompt is already in.
TOKEN_CHECK_IST = time(8, 40)

#: Typed owner-notification sink (``engine.ops.main.notify``).
NotifyFn = Callable[[CatalogMessage], Awaitable[None]]


class TokenCheckJob:
    """Probe the broker with one authenticated, side-effect-free REST call before the open.

    Parameters
    ----------
    kite:
        The :class:`~engine.broker.kite_client.KiteClient` facade. ``margins()`` (``/user/margins``)
        is the probe: cheapest authenticated endpoint, read-only. It routes through
        ``KiteClient._call``, so a ``TokenException`` ALSO fires the token-rejection circuit breaker
        (``SessionManager.on_token_rejected`` → freeze entries + login link), idempotent with this
        job's own alert.
    clock, calendar:
        Trading-day gate + the alert timestamp. A non-trading day skips silently.
    notify:
        Typed owner sink; delivery failures are swallowed.
    login_url:
        Zero-arg callable returning the broker login URL (``SessionManager.login_url``). ``None`` (or
        a raising call) degrades to the ``/token`` instruction alone.
    """

    def __init__(
        self,
        *,
        kite: Any,
        clock: Clock,
        calendar: NSECalendar,
        notify: NotifyFn,
        login_url: Callable[[], str] | None = None,
    ) -> None:
        self._kite = kite
        self._clock = clock
        self._calendar = calendar
        self._notify = notify
        self._login_url = login_url

    async def run(self) -> None:
        """Run the pre-open probe. NEVER raises — see the module docstring."""
        today = self._clock.today()
        if not self._calendar.is_trading_day(today):
            _log.info("token_check_skipped_non_trading_day", date=today.isoformat())
            return
        try:
            await self._kite.margins()
        except TokenException as exc:
            _log.critical(
                "token_check_failed", date=today.isoformat(), error=str(exc),
                error_type=type(exc).__name__,
                hint="daily Kite token is invalid — re-login before the open",
            )
            await self._send(self._invalid_message(str(exc)))
            return
        except Exception as exc:  # noqa: BLE001 - cannot verify != invalid; never load-bearing
            _log.warning(
                "token_check_error", date=today.isoformat(), error=str(exc),
                error_type=type(exc).__name__,
            )
            await self._send(self._inconclusive_message(exc))
            return
        _log.info("token_check_ok", date=today.isoformat(), at=self._clock.now().isoformat())

    # ------------------------------------------------------------------ messages
    def _login_hint(self) -> str:
        """The re-login instruction, with the tappable URL when one can be resolved."""
        url = None
        if self._login_url is not None:
            try:
                url = self._login_url()
            except Exception:  # noqa: BLE001 - a missing api_key must not break the alert
                url = None
        if url:
            return login_instruction(url)
        return "Send /token <request_token> after logging in to Kite."

    def _invalid_message(self, error: str) -> CatalogMessage:
        # LOGIN_PROMPT is the kind for "the daily Kite login is required"; title/body carry the pre-open framing.
        return CatalogMessage(
            kind=MessageKind.LOGIN_PROMPT,
            title="Kite token invalid — re-login before open",
            body=(
                "The pre-open token check was REJECTED by the broker — today's access token is dead "
                "(it expires ~06:00 IST). Entries will be FROZEN at the open until you re-login.\n"
                f"{self._login_hint()}"
            ),
            severity="critical",
            data={"check": "token_check", "outcome": "rejected", "error": error[:200]},
        )

    def _inconclusive_message(self, exc: BaseException) -> CatalogMessage:
        return CatalogMessage(
            kind=MessageKind.LOGIN_PROMPT,
            title="Kite token check inconclusive — verify before open",
            body=(
                "The pre-open token check could not reach the broker "
                f"({type(exc).__name__}: {str(exc)[:120]}). The token is UNVERIFIED, not known bad. "
                "If the engine reports a token rejection at the open, re-login.\n"
                f"{self._login_hint()}"
            ),
            severity="warning",
            data={"check": "token_check", "outcome": "error", "error_type": type(exc).__name__},
        )

    async def _send(self, msg: CatalogMessage) -> None:
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - the check must never be load-bearing, alerting included
            _log.exception("token_check_notify_failed", title=msg.title)
