"""Thin async wrapper over pykiteconnect 5.2.0 ``KiteConnect`` (§3.2.2, A2/A9/A10).

``KiteClient`` is the single typed surface the OMS / risk / marketdata tiers use to talk to Kite
REST. It exists to enforce two cross-cutting invariants on *every* call, with no exceptions:

* **A2 client-side rate limiting** — every method first ``await``s the injected
  :class:`~engine.broker.rate_limiter.RateLimiter` for the right ``endpoint_class``
  (``"orders"`` / ``"quote"`` / ``"historical"``). Order placement/modify/cancel additionally route
  the caller's ``intent`` through the limiter so that the §7.1 ``order_rate`` split is honoured:
  ``intent="entry"`` calls hard-stop at the 70/day cap (B3/R3) while protective / exit / square-off
  callers pass ``intent="risk_reducing"`` to draw from the uncapped-but-paced reserved pool that is
  *never* budget-rejected.
* **R8 structured logging** — every call logs an intent/result event (and re-raises, never swallows,
  pykiteconnect errors after logging them).

pykiteconnect is synchronous (``requests``-based), so each broker call is offloaded to the default
executor via ``loop.run_in_executor`` to keep the asyncio event loop responsive. This module is the
ONLY place that imports/uses the raw ``KiteConnect`` order surface (R5: the REST orderbook is the
source of truth; reconciliation lives above this layer).

**A9 (market protection):** market and SL-M orders must always carry ``market_protection=-1`` so Kite
applies its default protection band rather than letting a market order print at an arbitrary price.
This client is a pass-through — the *OMS* is responsible for setting ``market_protection`` on the
request it hands to :meth:`KiteClient.place_order`; the client forwards the request fields verbatim.

This is a Phase-0 skeleton: correct signatures, rate-limit/log wiring, and real call-through to
pykiteconnect. Request validation / typed models / margin pre-checks (C6) live in the OMS and gate.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from engine.core.log import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from engine.broker.rate_limiter import RateLimiter
    from engine.core.clock import Clock

_log = get_logger("engine.broker.kite_client")

# A request/modify payload is whatever the OMS builds: a plain dict of Kite order params, or a small
# object exposing the same fields. The client stays permissive and forwards fields verbatim — a typed
# ``OrderRequest`` is intentionally optional at this layer (validation is the OMS/gate's job).
ReqLike = Any


class KiteClient:
    """Async, rate-limited, logged facade over a synchronous pykiteconnect ``KiteConnect``.

    Parameters
    ----------
    kc:
        A configured pykiteconnect ``KiteConnect`` instance (api_key + access_token already set by
        :class:`~engine.broker.session.SessionManager`). Synchronous; all calls are run in an
        executor.
    rate_limiter:
        The shared :class:`~engine.broker.rate_limiter.RateLimiter` (A2). Acquired before *every*
        broker call. Order calls pass ``endpoint_class="orders"`` plus the caller's ``intent``;
        quote/ltp use ``"quote"``; historical uses ``"historical"``.
    clock:
        The single IST :class:`~engine.core.clock.Clock` (R6). Held for tz-aware timestamping of
        log/audit events; this client never reads ``datetime.now()`` directly.
    """

    def __init__(self, kc: Any, rate_limiter: RateLimiter, clock: Clock) -> None:
        self._kc = kc
        self._rl = rate_limiter
        self._clock = clock

    # -- internal call-through --------------------------------------------------------------------

    async def _call(self, endpoint_class: str, op: str, fn: Any, **log_fields: Any) -> Any:
        """Acquire the limiter, run the sync pykiteconnect call in an executor, log, re-raise.

        ``fn`` is a zero-arg callable wrapping the pykiteconnect method invocation. Any exception is
        logged (with context) and re-raised — pykiteconnect errors are NEVER swallowed (R5/R8); the
        OMS/session layers above decide how to react (e.g. token-rejected → freeze entries).
        """
        await self._rl.acquire(endpoint_class=endpoint_class)
        loop = asyncio.get_running_loop()
        _log.info("kite.call", op=op, endpoint_class=endpoint_class, **log_fields)
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
            raise
        _log.info("kite.ok", op=op, endpoint_class=endpoint_class, **log_fields)
        return result

    async def _order_call(self, op: str, intent: str, fn: Any, **log_fields: Any) -> Any:
        """Like :meth:`_call` but acquires the ``orders`` bucket with the caller's ``intent`` (R3).

        ``intent="entry"`` is the default and is hard-capped at 70/day; protective / exit / square-off
        callers pass ``intent="risk_reducing"`` for the uncapped-but-paced reserved pool (never
        budget-rejected). The split is enforced inside :meth:`RateLimiter.acquire`.
        """
        await self._rl.acquire(endpoint_class="orders", intent=intent)
        loop = asyncio.get_running_loop()
        _log.info("kite.call", op=op, endpoint_class="orders", intent=intent, **log_fields)
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
            raise
        _log.info("kite.ok", op=op, endpoint_class="orders", intent=intent, **log_fields)
        return result

    @staticmethod
    def _as_params(req: ReqLike) -> dict[str, Any]:
        """Coerce an OMS-built request (dict OR object) into a Kite kwargs dict, verbatim.

        The OMS is responsible for the field set Kite expects (``variety``, ``exchange``,
        ``tradingsymbol``, ``transaction_type``, ``quantity``, ``order_type``, ``product``,
        ``price``, ``trigger_price``, and — for market / SL-M orders — ``market_protection=-1``
        per A9). This client forwards whatever it is given without adding or mutating fields.
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
        """Place an order; returns the broker ``order_id`` (R5 source of truth is the orderbook).

        ``intent`` routes the limiter acquire (``"entry"`` default → 70/day cap; ``"risk_reducing"``
        for protective/exit/square-off → reserved pool, never budget-rejected, R3).

        **A9:** for ``MARKET`` / ``SL-M`` orders the request MUST already carry
        ``market_protection=-1`` — the *OMS* sets this; the client passes the request fields through
        unchanged (see :meth:`_as_params`).
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
        """Return the full REST orderbook — the authoritative order state (R5)."""
        return await self._call("orders", "orders", lambda: self._kc.orders())

    async def positions(self) -> Any:
        """Return net/day positions (Kite ``positions()`` shape: ``{"net": [...], "day": [...]}``)."""
        return await self._call("orders", "positions", lambda: self._kc.positions())

    async def holdings(self) -> list:
        """Return long-term holdings (T+1 settled equity)."""
        return await self._call("orders", "holdings", lambda: self._kc.holdings())

    async def margins(self) -> Any:
        """Return account margins. Checked before EVERY order (C6) — the OMS calls this pre-place."""
        return await self._call("orders", "margins", lambda: self._kc.margins())

    # -- GTT (endpoint_class="orders") ------------------------------------------------------------

    async def place_gtt(self, req: ReqLike) -> int:
        """Create a GTT trigger; returns the Kite ``trigger_id``.

        Kite caps active GTTs per account and the pool is SHARED with the owner's manual GTTs — the
        OMS reserves headroom and surfaces a cap rejection as a distinct ``PROTECTION_FAILED`` reason
        (§3.2.8). This client forwards the request verbatim.
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
        """Fetch historical candles for ``token`` in ``[frm, to]`` at ``interval`` (≤3 req/s, A2).

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
        """Last traded price per instrument token (``quote`` budget, A2).

        Returns a ``{token: Decimal}`` map. Kite's ``ltp()`` keys results by an
        ``"<exchange>:<symbol>"`` style string and nests ``last_price``; we re-key by the integer
        instrument token and coerce the price to :class:`~decimal.Decimal` (prices are never floats
        in this platform).
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

        Once-per-day reference download consumed by InstrumentStore.refresh (§3.2.2, A10).
        pykiteconnect's instruments() is a bulk CSV dump, not an order/quote/historical API
        call, but it still funnels through the single limiter/logging chokepoint using the
        conservative "quote" bucket - a once-a-day acquire never stalls.
        """
        return await self._call(
            "quote", "instruments", lambda: self._kc.instruments(exchange=exchange)
        )
