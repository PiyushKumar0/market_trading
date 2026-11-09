"""Thin async wrapper over pykiteconnect 5.2.0 ``KiteConnect``.

``KiteClient`` is the single typed surface the OMS / risk / marketdata tiers use to talk to Kite REST. On
*every* call it enforces:

* **Client-side rate limiting** — each method first ``await``s the injected
  :class:`~engine.broker.rate_limiter.RateLimiter` for its ``endpoint_class`` (``"orders"`` / ``"quote"`` /
  ``"historical"``). Order place/modify/cancel pass the caller's ``intent``: ``"entry"`` hard-stops at the
  daily cap, ``"risk_reducing"`` (protective / exit / square-off) is paced but never budget-rejected.
  Reads pass ``intent="read"``: paced, never capped or counted.
* **Structured logging** — every call logs intent/result; pykiteconnect errors are logged and re-raised,
  never swallowed.

pykiteconnect is synchronous, so each call runs in the default executor. This module is the ONLY place that
uses the raw ``KiteConnect`` order surface (the REST orderbook is the source of truth).

**Market protection:** market and SL-M orders must carry ``market_protection=-1`` (Kite's default band).
The client is a pass-through; the *OMS* sets it and the request fields are forwarded verbatim.

Production builds this client with ``orders_enabled=False``, so only the read surface is live. Request
validation lives in the OMS and gate; the pre-place margin check (``GateContextBuilder`` ``margins_fn``) is
unwired until live AUTO.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from kiteconnect.exceptions import TokenException

from engine.core.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import datetime

    from engine.broker.rate_limiter import RateLimiter
    from engine.core.clock import Clock

_log = get_logger("engine.broker.kite_client")

# A request/modify payload is whatever the OMS builds: a plain dict of Kite order params, or a small
# object exposing the same fields. The client stays permissive and forwards fields verbatim — a typed
# ``OrderRequest`` is intentionally optional at this layer (validation is the OMS/gate's job).
ReqLike = Any


class OrderSurfaceViolation(RuntimeError):
    """Raised to block a broker order call.

    The order guard is a plain callback wired by ops (``KiteClient`` does not import ``engine.risk``);
    ``KiteClient`` raises this itself only when built without ``orders_enabled``.
    """


class KiteClient:
    """Async, rate-limited, logged facade over a synchronous pykiteconnect ``KiteConnect``.

    Parameters
    ----------
    kc:
        A configured (synchronous) pykiteconnect ``KiteConnect`` (api_key + access_token set by
        :class:`~engine.broker.session.SessionManager`); calls run in an executor.
    rate_limiter:
        The shared :class:`~engine.broker.rate_limiter.RateLimiter`, acquired before *every* broker call.
    clock:
        The single IST :class:`~engine.core.clock.Clock`; this client never reads ``datetime.now()``.
    on_token_rejected:
        Optional async circuit-breaker fired on the first :class:`~kiteconnect.exceptions.TokenException`
        from ANY broker call (wired to :meth:`~engine.broker.session.SessionManager.on_token_rejected`:
        freeze entries + alert). The original error is ALWAYS re-raised; a hook failure is logged and never
        masks it. The hook receives the access token the failing request was sent with, so the session can
        ignore a rejection of a token a re-login has since superseded.
    order_guard:
        Optional guard called synchronously with the caller's ``intent`` BEFORE the rate limiter, on every
        order-placing/modifying/cancelling path (orders and GTTs). It raises :class:`OrderSurfaceViolation`
        to block or returns ``None`` to allow; the policy (mode, risk state, window, kill switch) lives in
        the guard the wiring layer injects, since ``engine.broker`` may not import ``engine.risk``.
        ``None`` means no gating (bare clients in tests / scripts).
    orders_enabled:
        While False (the default, and the only value production passes) every order/GTT method raises
        :class:`OrderSurfaceViolation` before the guard and the limiter. Only tests pass True.
    """

    def __init__(
        self,
        kc: Any,
        rate_limiter: RateLimiter,
        clock: Clock,
        on_token_rejected: Callable[[str | None], Awaitable[None]] | None = None,
        *,
        order_guard: Callable[[str], None] | None = None,
        orders_enabled: bool = False,
    ) -> None:
        self._kc = kc
        self._rl = rate_limiter
        self._clock = clock
        self._on_token_rejected = on_token_rejected
        self._order_guard = order_guard
        self._orders_enabled = orders_enabled

    async def _fire_token_rejected(self, exc: BaseException, token: str | None) -> None:
        """Fire the circuit breaker if ``exc`` is a :class:`TokenException` and it is wired; a hook failure
        never masks the broker error. ``token`` is the access token the request was sent with. The caller
        re-raises ``exc`` unconditionally."""
        if isinstance(exc, TokenException) and self._on_token_rejected is not None:
            try:
                await self._on_token_rejected(token)
            except Exception:  # noqa: BLE001 - a hook failure must never mask the broker error
                _log.exception("token_rejected_hook_failed")

    # -- internal call-through --------------------------------------------------------------------

    async def _call(self, endpoint_class: str, op: str, fn: Any, **log_fields: Any) -> Any:
        """Acquire the limiter, run the sync pykiteconnect call in an executor, log, re-raise.

        ``fn`` is a zero-arg callable wrapping the pykiteconnect invocation. Errors are logged and re-raised,
        never swallowed. Every caller is a non-placing read, so the limiter is acquired with
        ``intent="read"`` (paced, never counted against the entry budget).
        """
        await self._rl.acquire(endpoint_class=endpoint_class, intent="read")
        loop = asyncio.get_running_loop()
        _log.info("kite.call", op=op, endpoint_class=endpoint_class, **log_fields)
        token = getattr(self._kc, "access_token", None)  # sent-with token; no await before the executor
        try:
            result = await loop.run_in_executor(None, fn)
        except Exception as exc:  # noqa: BLE001 - log + re-raise; do not swallow broker errors
            _log.error(
                "kite.error",
                op=op,
                endpoint_class=endpoint_class,
                error=str(exc),
                error_type=type(exc).__name__,
                **log_fields,
            )
            await self._fire_token_rejected(exc, token)  # TokenException → freeze entries once
            raise
        _log.info("kite.ok", op=op, endpoint_class=endpoint_class, **log_fields)
        return result

    async def _order_call(self, op: str, intent: str, fn: Any, **log_fields: Any) -> Any:
        """Like :meth:`_call` but acquires the ``orders`` bucket with the caller's ``intent``.

        The intent split is enforced inside :meth:`RateLimiter.acquire`. The order-surface guard, if wired,
        runs FIRST, so a blocked call never consumes limiter budget; every order/GTT method routes through
        here, so this one site covers all six.
        """
        if not self._orders_enabled:
            raise OrderSurfaceViolation(f"{op} refused: KiteClient built without orders_enabled")
        if self._order_guard is not None:
            self._order_guard(intent)  # raises OrderSurfaceViolation to block; None return = allowed
        await self._rl.acquire(endpoint_class="orders", intent=intent)
        loop = asyncio.get_running_loop()
        _log.info("kite.call", op=op, endpoint_class="orders", intent=intent, **log_fields)
        token = getattr(self._kc, "access_token", None)  # sent-with token; no await before the executor
        try:
            result = await loop.run_in_executor(None, fn)
        except Exception as exc:  # noqa: BLE001 - log + re-raise; do not swallow broker errors
            _log.error(
                "kite.error",
                op=op,
                endpoint_class="orders",
                intent=intent,
                error=str(exc),
                error_type=type(exc).__name__,
                **log_fields,
            )
            await self._fire_token_rejected(exc, token)  # TokenException → freeze entries once
            raise
        _log.info("kite.ok", op=op, endpoint_class="orders", intent=intent, **log_fields)
        return result

    @staticmethod
    def _as_params(req: ReqLike) -> dict[str, Any]:
        """Coerce an OMS-built request (dict OR object) into a Kite kwargs dict, verbatim.

        The OMS owns the field set Kite expects (including ``market_protection=-1`` for market / SL-M);
        this client adds and mutates nothing.
        """
        if isinstance(req, dict):
            return dict(req)
        # Permissive object → dict: pydantic v2 model, dataclass-like, or plain attrs holder.
        for attr in ("model_dump", "dict"):
            dump = getattr(req, attr, None)
            if callable(dump):
                return dict(dump())
        return dict(vars(req))

    # -- orders (endpoint_class="orders") ---------------------------------------------------------

    async def place_order(self, req: ReqLike, intent: str = "entry") -> str:
        """Place an order; returns the broker ``order_id`` (the orderbook is the source of truth).

        ``intent`` routes the limiter acquire (``"entry"`` default is capped; ``"risk_reducing"`` never is).
        For ``MARKET`` / ``SL-M`` the request MUST already carry ``market_protection=-1`` (set by the OMS).
        """
        params = self._as_params(req)
        variety = params.pop("variety", "regular")
        result = await self._order_call(
            "place_order",
            intent,
            lambda: self._kc.place_order(variety=variety, **params),
            variety=variety,
        )
        return str(result)

    async def modify_order(self, order_id: str, req: ReqLike, intent: str = "risk_reducing") -> str:
        """Modify a live order; returns the broker ``order_id``.

        Modifies are predominantly protective (tighten/exit), so ``intent`` defaults to
        ``"risk_reducing"``; entry-repricing callers may override with ``intent="entry"``.
        """
        params = self._as_params(req)
        variety = params.pop("variety", "regular")
        result = await self._order_call(
            "modify_order",
            intent,
            lambda: self._kc.modify_order(variety=variety, order_id=order_id, **params),
            variety=variety,
            order_id=order_id,
        )
        return str(result)

    async def cancel_order(
        self, order_id: str, variety: str = "regular", intent: str = "risk_reducing"
    ) -> str:
        """Cancel a live order; returns the broker ``order_id``. Cancels default to risk-reducing."""
        result = await self._order_call(
            "cancel_order",
            intent,
            lambda: self._kc.cancel_order(variety=variety, order_id=order_id),
            variety=variety,
            order_id=order_id,
        )
        return str(result)

    async def orders(self) -> list:
        """Return the full REST orderbook — the authoritative order state."""
        return await self._call("orders", "orders", lambda: self._kc.orders())

    async def positions(self) -> Any:
        """Return net/day positions (Kite ``positions()`` shape: ``{"net": [...], "day": [...]}``)."""
        return await self._call("orders", "positions", lambda: self._kc.positions())

    async def holdings(self) -> list:
        """Return long-term holdings (T+1 settled equity)."""
        return await self._call("orders", "holdings", lambda: self._kc.holdings())

    async def margins(self) -> Any:
        """Return account margins. Only the daily token probe calls this today."""
        return await self._call("orders", "margins", lambda: self._kc.margins())

    # -- GTT (endpoint_class="orders") ------------------------------------------------------------

    async def place_gtt(self, req: ReqLike) -> int:
        """Create a GTT trigger; returns the Kite ``trigger_id``.

        Kite caps active GTTs per account, shared with the owner's manual GTTs; the OMS reserves headroom and
        surfaces a cap rejection as a distinct ``PROTECTION_FAILED`` reason.
        """
        params = self._as_params(req)
        result = await self._order_call(
            "place_gtt", "risk_reducing", lambda: self._kc.place_gtt(**params)
        )
        return int(result)

    async def modify_gtt(self, gtt_id: int, req: ReqLike) -> int:
        """Modify an existing GTT; returns the Kite ``trigger_id``."""
        params = self._as_params(req)
        result = await self._order_call(
            "modify_gtt",
            "risk_reducing",
            lambda: self._kc.modify_gtt(trigger_id=gtt_id, **params),
            gtt_id=gtt_id,
        )
        return int(result)

    async def delete_gtt(self, gtt_id: int) -> None:
        """Delete a GTT by ``trigger_id``."""
        await self._order_call(
            "delete_gtt",
            "risk_reducing",
            lambda: self._kc.delete_gtt(trigger_id=gtt_id),
            gtt_id=gtt_id,
        )

    async def gtts(self) -> list:
        """Return all active GTT triggers for the account."""
        return await self._call("orders", "gtts", lambda: self._kc.get_gtts())

    # -- market data (endpoint_class="historical"/"quote") ----------------------------------------

    async def historical(
        self, token: int, frm: datetime, to: datetime, interval: str
    ) -> list:
        """Fetch historical candles for ``token`` in ``[frm, to]`` at ``interval`` (≤3 req/s).

        ``frm``/``to`` are tz-aware IST datetimes supplied by the caller (sourced from
        :class:`~engine.core.clock.Clock`); pykiteconnect formats them for the API.
        """
        return await self._call(
            "historical",
            "historical",
            lambda: self._kc.historical_data(
                instrument_token=token, from_date=frm, to_date=to, interval=interval
            ),
            token=token,
            interval=interval,
        )

    async def ltp(self, tokens: list[int]) -> dict[int, Decimal]:
        """Last traded price per instrument token (``quote`` budget).

        Kite keys ``ltp()`` by ``"<exchange>:<symbol>"``; we re-key by integer instrument token and coerce
        the price to :class:`~decimal.Decimal` (prices are never floats here).
        """
        result = await self._call(
            "quote", "ltp", lambda: self._kc.ltp([str(t) for t in tokens]), n=len(tokens)
        )
        out: dict[int, Decimal] = {}
        for value in (result or {}).values():
            tok = value.get("instrument_token")
            price = value.get("last_price")
            if tok is None or price is None:
                continue
            out[int(tok)] = Decimal(str(price))
        return out

    async def instruments(self, exchange: str | None = None) -> list:
        """Full daily instruments dump for ``exchange`` (all exchanges if None).

        Once-per-day bulk CSV dump consumed by InstrumentStore.refresh; it still goes through the limiter
        on the "quote" bucket (a once-a-day acquire never stalls).
        """
        return await self._call(
            "quote", "instruments", lambda: self._kc.instruments(exchange=exchange)
        )
