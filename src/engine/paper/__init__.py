"""Conservative paper-broker fill model + same-code-path replay harness (R9). Phase 3.

WO-P3-2 (2026-09-10, §8.4 addendum) lands the broker half: :class:`~engine.paper.surface.BrokerSurface`
(the routable surface ``KiteClient`` and ``PaperBroker`` both satisfy, §3.2.9/§3.5.3),
:mod:`engine.paper.fill_model` (latency, time-of-day k buckets, never-zero half-spread) and
:class:`~engine.paper.broker.PaperBroker`. ``ReplayHarness`` is WO-P3-3.

**Dependency budget, per module** (§3.2.9; ``tests/unit/test_import_graph.py`` enforces it
structurally, module by module, since round 3 on 2026-09-10):

* this ``__init__``, :mod:`engine.paper.broker`, :mod:`engine.paper.fill_model` and
  :mod:`engine.paper.surface` depend on ``engine.core`` ALONE — which is what the order-postback
  wire contract both brokers emit (``ORDER_UPDATE_TOPIC`` / ``OrderUpdateFrame``) living in
  :mod:`engine.core.contracts` buys, and what keeps a PaperBroker constructible without the
  market-data or pykiteconnect import chains;
* :mod:`engine.paper.replay` additionally depends on ``engine.marketdata``, because the §8.4
  WO-P3-3 harness drives the REAL ``BarBuilder`` into a scratch ``MarketStore`` — "one code path"
  is the point of it. The allowance is one module wide and deliberately not re-exported here, so
  importing this package never pulls the market-data tier in.

No module in the package may import ``engine.broker`` or ``engine.intelligence`` (R1).

Exports are re-exported here so callers write ``from engine.paper import PaperBroker`` rather than
reaching into module paths that may be reorganised as the replay half lands.
"""

from engine.paper.broker import PaperBroker, PaperOrderError, SigmaEstimator
from engine.paper.fill_model import FillModelConfig, load_fill_model
from engine.paper.surface import BrokerSurface

__all__ = [
    "BrokerSurface",
    "FillModelConfig",
    "PaperBroker",
    "PaperOrderError",
    "SigmaEstimator",
    "load_fill_model",
]
