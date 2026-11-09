"""The broker order / GTT / account surface shared by live and paper routing.

Routing is a sticky flag: the OMS talks to :class:`~engine.broker.kite_client.KiteClient` or
:class:`~engine.paper.broker.PaperBroker` per ``routing: paper | live`` and must not know which.
:class:`BrokerSurface` makes that structural; ``tests/unit/test_broker_surface.py`` asserts BOTH
implementations against it with :func:`inspect.signature` (names, kinds, defaults).

Pinned here:

* Exactly the twelve order/GTT/account methods the OMS calls. ``KiteClient.historical`` and
  ``KiteClient.instruments`` are OUT: market-data endpoints owned by ``engine.marketdata`` /
  ``InstrumentStore``, never routed per mode.
* ``intent`` defaults (``"entry"`` on place, ``"risk_reducing"`` on modify/cancel) are contract:
  they select the ``order_rate`` pool on the live side and are what the injected ``order_guard``
  sees.
* Every method is ``async`` (paper fills are computed synchronously in ``on_tick``) so the OMS has
  one call shape.

Imports nothing from ``engine.broker`` (keeps ``engine.paper`` free of the pykiteconnect chain).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

#: An OMS-built order/GTT payload: a plain dict of Kite params, or an object exposing the same
#: fields (``model_dump``/``dict``/``vars``). Mirrors ``engine.broker.kite_client.ReqLike`` — the
#: broker layer forwards fields verbatim and validation belongs to the OMS/gate.
ReqLike = Any


@runtime_checkable
class BrokerSurface(Protocol):
    """The routable broker surface. Implemented by ``KiteClient`` (live) and ``PaperBroker``.

    ``runtime_checkable`` gives an ``isinstance`` smoke check (attribute presence only — the real
    guarantee is the signature test, which a runtime protocol check cannot express).
    """

    # -- orders ------------------------------------------------------------------------------
    async def place_order(self, req: ReqLike, intent: str = "entry") -> str:
        """Place an order; returns the broker ``order_id`` (the orderbook is the truth)."""
        ...

    async def modify_order(self, order_id: str, req: ReqLike, intent: str = "risk_reducing") -> str:
        """Modify a live order; returns the broker ``order_id``."""
        ...

    async def cancel_order(
        self, order_id: str, variety: str = "regular", intent: str = "risk_reducing"
    ) -> str:
        """Cancel a live order; returns the broker ``order_id``."""
        ...

    async def orders(self) -> list:
        """The full orderbook, Kite-shaped."""
        ...

    async def positions(self) -> Any:
        """Net/day positions (Kite shape: ``{"net": [...], "day": [...]}``)."""
        ...

    async def holdings(self) -> list:
        """Long-term holdings (settled equity / the CNC book)."""
        ...

    async def margins(self) -> Any:
        """Account margins. Checked before every order."""
        ...

    # -- GTT ---------------------------------------------------------------------------------
    async def place_gtt(self, req: ReqLike) -> int:
        """Create a GTT trigger; returns the ``trigger_id``."""
        ...

    async def modify_gtt(self, gtt_id: int, req: ReqLike) -> int:
        """Modify an existing GTT; returns the ``trigger_id``."""
        ...

    async def delete_gtt(self, gtt_id: int) -> None:
        """Delete a GTT by ``trigger_id``."""
        ...

    async def gtts(self) -> list:
        """All GTT triggers for the account."""
        ...

    # -- quote -------------------------------------------------------------------------------
    async def ltp(self, tokens: list[int]) -> dict[int, Decimal]:
        """Last traded price per instrument token (``{token: Decimal}``; prices are never floats)."""
        ...
