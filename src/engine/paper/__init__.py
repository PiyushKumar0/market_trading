"""Conservative paper-broker fill model + same-code-path replay harness.

Pieces: :class:`~engine.paper.surface.BrokerSurface` (the routable surface ``KiteClient`` and
``PaperBroker`` both satisfy), :mod:`engine.paper.fill_model` (latency, time-of-day k buckets,
never-zero half-spread), :class:`~engine.paper.broker.PaperBroker` and ``ReplayHarness``.

**Dependency budget, per module** (``tests/unit/test_import_graph.py`` enforces it):

* this ``__init__``, :mod:`engine.paper.broker`, :mod:`engine.paper.fill_model` and
  :mod:`engine.paper.surface` depend on ``engine.core`` ALONE (the order-postback wire contract
  both brokers emit lives in :mod:`engine.core.contracts`), so a PaperBroker is constructible
  without the market-data or pykiteconnect import chains;
* :mod:`engine.paper.replay` additionally depends on ``engine.marketdata``: it drives the REAL
  ``BarBuilder`` into a scratch ``MarketStore`` ("one code path"). Not re-exported here, so
  importing this package never pulls the market-data tier in.

No module in the package may import ``engine.broker`` or ``engine.intelligence``.
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
