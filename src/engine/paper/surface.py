"""The broker order / GTT / account surface shared by live and paper routing (§3.2.9, R9).

WO-P3-2, 2026-09-10 (§8.4 Phase-3 decomposition addendum).

**Why this exists.** §3.5.3 makes routing a sticky flag: in AUTO the OMS talks to
:class:`~engine.broker.kite_client.KiteClient` or to :class:`~engine.paper.broker.PaperBroker`
depending on ``routing: paper | live``, and it must not know which. Duck typing alone makes that a
convention; :class:`BrokerSurface` makes it structural, and ``tests/unit/test_broker_surface.py``
asserts BOTH implementations against it with :func:`inspect.signature` (identical parameter names,
kinds and defaults) so a signature drift is a CI failure rather than a runtime surprise on the
first paper-AUTO session.

**What is pinned here, and why:**

* Exactly the twelve order/GTT/account methods the OMS calls — the count §8.4's WO-P3-2 entry
  pins. ``KiteClient.historical`` and ``KiteClient.instruments`` are deliberately OUT of the
  surface: they are reference/market-data endpoints owned by ``engine.marketdata`` /
  ``InstrumentStore``, never routed per mode, and a PaperBroker implementation of them would be
  meaningless.
* ``intent`` defaults (``"entry"`` on place, ``"risk_reducing"`` on modify/cancel) are part of the
  contract, not an implementation detail: they select the §7.1 ``order_rate`` pool on the live side
  and are what the injected ``order_guard`` (§3.5.3 order-surface predicate) sees.
* Every method is ``async``. Paper fills are computed synchronously inside ``on_tick``, but the
  surface stays awaitable so the OMS has one call shape.

This module imports nothing from ``engine.broker`` — a Protocol needs no implementation — which
keeps ``engine.paper`` free of the pykiteconnect import chain. Its engine dependency is
``engine.core`` alone (in fact it needs nothing but stdlib), asserted per module by
``tests/unit/test_import_graph.py``.
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
        """Place an order; returns the broker ``order_id`` (R5: the orderbook is the truth)."""
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
        """Account margins. Checked before every order (C6)."""
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
