"""The routing-agnostic broker surface (WO-P3-2, 2026-09-10; plan 3.2.9 / 3.5.3).

The OMS must not know whether it is talking to Kite or to the PaperBroker -- ``ModeManager.routing()``
picks one and hands it over (WO-P3-5). That is only safe if the two are structurally
interchangeable, so this test asserts it MECHANICALLY rather than by convention: every method of
:class:`~engine.paper.surface.BrokerSurface` must exist on BOTH ``KiteClient`` and ``PaperBroker``,
be a coroutine function, and carry identical parameter names / kinds / defaults.

Annotations are deliberately NOT compared: ``KiteClient`` types its request payload as its own
``ReqLike`` alias and returns broker-shaped ``Any``; what has to match is the CALL shape, which is
what a routing-agnostic caller depends on.
"""

from __future__ import annotations

import inspect
from datetime import datetime
from decimal import Decimal

import pytest

from engine.broker.kite_client import KiteClient
from engine.core.clock import IST, Clock
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from engine.paper.surface import BrokerSurface

#: The surface, pinned by hand. A method added to (or dropped from) the Protocol without a
#: deliberate edit here is a drift the OMS would discover at runtime instead of in CI.
EXPECTED_SURFACE = {
    "place_order",
    "modify_order",
    "cancel_order",
    "orders",
    "positions",
    "holdings",
    "margins",
    "place_gtt",
    "modify_gtt",
    "delete_gtt",
    "gtts",
    "ltp",
}


def _surface_methods() -> dict[str, inspect.Signature]:
    out: dict[str, inspect.Signature] = {}
    for name, member in inspect.getmembers(BrokerSurface, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        out[name] = inspect.signature(member)
    return out


def _param_shape(sig: inspect.Signature) -> list[tuple[str, inspect._ParameterKind, object]]:
    """Parameter names/kinds/defaults, minus ``self`` and minus annotations."""
    return [
        (p.name, p.kind, p.default)
        for p in sig.parameters.values()
        if p.name != "self"
    ]


@pytest.fixture
def paper() -> PaperBroker:
    clock = Clock(time_source=lambda: datetime(2026, 6, 17, 10, 5, tzinfo=IST))
    return PaperBroker(
        clock=clock,
        publish=lambda topic, frame: None,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: Decimal("0.05"),
        rng_seed=7,
    )


def test_protocol_is_the_pinned_surface() -> None:
    assert set(_surface_methods()) == EXPECTED_SURFACE


@pytest.mark.parametrize("impl", [KiteClient, PaperBroker], ids=["kite", "paper"])
def test_impl_exposes_every_surface_method_with_the_same_call_shape(impl: type) -> None:
    missing: list[str] = []
    mismatched: list[str] = []
    not_async: list[str] = []
    for name, proto_sig in _surface_methods().items():
        impl_fn = getattr(impl, name, None)
        if impl_fn is None:
            missing.append(name)
            continue
        if not inspect.iscoroutinefunction(impl_fn):
            not_async.append(name)
            continue
        impl_shape = _param_shape(inspect.signature(impl_fn))
        proto_shape = _param_shape(proto_sig)
        if impl_shape != proto_shape:
            mismatched.append(f"{name}: protocol={proto_shape} impl={impl_shape}")
    assert not missing, f"{impl.__name__} is missing BrokerSurface methods: {missing}"
    assert not not_async, f"{impl.__name__} exposes non-async surface methods: {not_async}"
    assert not mismatched, f"{impl.__name__} call-shape drift: " + "; ".join(mismatched)


def test_paper_broker_satisfies_the_protocol_at_runtime(paper: PaperBroker) -> None:
    assert isinstance(paper, BrokerSurface)


def test_order_intent_defaults_match_kite(paper: PaperBroker) -> None:
    # The intent defaults are load-bearing: they route the rate-limiter split (7.1 order_rate) and
    # the order-surface guard (3.5.3). Paper must not quietly default an entry to risk_reducing.
    assert inspect.signature(PaperBroker.place_order).parameters["intent"].default == "entry"
    assert inspect.signature(KiteClient.place_order).parameters["intent"].default == "entry"
    for meth in ("modify_order", "cancel_order"):
        assert (
            inspect.signature(getattr(PaperBroker, meth)).parameters["intent"].default
            == inspect.signature(getattr(KiteClient, meth)).parameters["intent"].default
            == "risk_reducing"
        )
