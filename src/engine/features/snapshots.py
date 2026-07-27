"""Feature snapshots + canonical serialization (§3.2.5 / §4.3 ``feature_snapshots``, §6.2).

``FeatureVector`` is the value returned by ``FeatureEngine.intraday_snapshot`` and persisted to the
DuckDB ``feature_snapshots`` table keyed by ``features_snapshot_id`` (a platform-minted ULID, §3.2
convention 6). Proposals and ledger rows reference that id (§4.3) — the snapshot is the frozen "what
the features looked like at decision time" record for audit/replay (R8).

Serialization contract (load-bearing for the §9.6 determinism tests):

* :func:`features_json` is CANONICAL — keys sorted, compact separators, ``allow_nan=False``. The same
  feature mapping always serializes to the same bytes, so a re-run writes byte-identical rows.
* Values are plain JSON scalars only (``bool | int | float | str | None``). numpy scalars are
  unwrapped via ``.item()``; non-finite floats (NaN/inf — indicator warm-up positions) become
  ``None``; ``Decimal`` values are serialized as strings per the §4.3 JSON-column convention
  (features are statistics and normally floats — Decimals appear only if a caller passes a price
  level through unconverted).

``FEATURE_SET_VERSION`` is the v1 stamp (§6.2) written on every ``features_daily`` row and snapshot;
it bumps to 2 when the §2.7 sentiment layer lands (§8.3 — trial windows reset per version).
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict
from ulid import ULID

from engine.marketdata.store import MarketStore

#: §6.2 feature-set version stamped on every features_daily row + snapshot. Bumps to 2 with §2.7.
FEATURE_SET_VERSION = 1

#: The only value types a feature may hold once cleaned (JSON scalars; never NaN — §6.2).
FeatureValue = bool | int | float | str | None


class FeatureVector(BaseModel):
    """An intraday feature snapshot (§3.2.5) — persisted, then referenced by proposals/ledger (§4.3).

    ``ts`` is the Clock-stamped snapshot time (tz-aware IST, §3.2). ``features`` holds cleaned JSON
    scalars only (see :func:`clean_features`).
    """

    model_config = ConfigDict(frozen=True)

    features_snapshot_id: str                 # platform-minted ULID (§3.2 convention 6)
    symbol: str
    ts: AwareDatetime                         # tz-aware IST only (§9.1 no-naive-datetime invariant)
    feature_set_version: int = FEATURE_SET_VERSION
    features: dict[str, FeatureValue]


def clean_features(features: Mapping[str, Any]) -> dict[str, FeatureValue]:
    """Normalize a raw feature mapping to plain JSON scalars (deterministic, never NaN).

    numpy scalars are unwrapped (``.item()``), non-finite floats become ``None`` (an indicator that
    lacks warm-up history is "unavailable", not a NaN leaking into JSON), Decimals become strings
    (§4.3 JSON-column convention). Any other type is a hard ``TypeError`` — features are scalars.
    """
    out: dict[str, FeatureValue] = {}
    for key in sorted(features):
        v: Any = features[key]
        if hasattr(v, "item") and type(v) not in (bool, int, float, str):
            v = v.item()                       # numpy scalar (float64/bool_/int64) -> python scalar
        if isinstance(v, Decimal):
            v = str(v)                         # Decimals as strings in JSON columns (§4.3)
        if isinstance(v, float) and not math.isfinite(v):
            v = None                           # warm-up NaN/inf -> unavailable, never NaN (§6.2)
        if v is not None and not isinstance(v, (bool, int, float, str)):
            raise TypeError(f"feature {key!r} has non-scalar value of type {type(v).__name__}")
        out[key] = v
    return out


def features_json(features: Mapping[str, Any]) -> str:
    """CANONICAL JSON for a feature mapping: cleaned, key-sorted, compact, ``allow_nan=False``.

    Same mapping in ⇒ same bytes out — the determinism property the §9.1 feature tests assert.
    """
    return json.dumps(clean_features(features), sort_keys=True, separators=(",", ":"), allow_nan=False)


def new_feature_vector(
    symbol: str,
    ts: AwareDatetime,
    features: Mapping[str, Any],
    *,
    feature_set_version: int = FEATURE_SET_VERSION,
) -> FeatureVector:
    """Build a :class:`FeatureVector`, minting its ``features_snapshot_id`` (ULID) and cleaning values."""
    return FeatureVector(
        features_snapshot_id=str(ULID()),
        symbol=symbol,
        ts=ts,
        feature_set_version=feature_set_version,
        features=clean_features(features),
    )


def persist_snapshot(store: MarketStore, vector: FeatureVector) -> str:
    """Write ``vector`` to the DuckDB ``feature_snapshots`` table (§4.3); returns the snapshot id."""
    store.insert_feature_snapshot(
        vector.features_snapshot_id,
        vector.symbol,
        vector.ts,
        vector.feature_set_version,
        features_json(vector.features),
    )
    return vector.features_snapshot_id


def load_snapshot(store: MarketStore, snapshot_id: str) -> FeatureVector | None:
    """Read a persisted snapshot back as a :class:`FeatureVector` (``None`` if unknown id)."""
    row = store.get_feature_snapshot(snapshot_id)
    if row is None:
        return None
    return FeatureVector(
        features_snapshot_id=row["snapshot_id"],
        symbol=row["symbol"],
        ts=row["ts"],
        feature_set_version=row["feature_set_version"],
        features=json.loads(row["features"]),
    )
