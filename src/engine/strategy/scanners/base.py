"""Scanner base class + registry (§3.2.5/§6.1) — the seam every baseline (and Phase-3 ``cat``) plugs into.

A ``Scanner`` is a PURE function of ``(bar, ScanContext, params)``: no Clock, no I/O, no mutable
state — same inputs ⇒ same candidates (§9.6 determinism, modulo the platform-minted ``signal_id``
ULID). Anything a scanner needs beyond the bar arrives via :class:`~engine.strategy.types.ScanContext`
(assembled by the pre-screen's context provider); missing/thin context means FAIL TO ZERO (return
``[]``), never raise — thin data is a warm-up condition (§7.1 ``warmup_ready``), not an error.

Params are injected as a plain dict whose keys mirror the §6.3 envelope rows for the scanner's
``strategy_id`` (bare, un-namespaced: ``orb.vol_mult`` → ``vol_mult``); ``DEFAULT_PARAMS`` carries the
§6.3 defaults so a scanner built with no params trades the envelope defaults. Unknown keys are a hard
``ValueError`` (typo guard — a silently-ignored param would quietly detune a strategy).

Registry: modules self-register via :func:`register` at import time; ``SCANNER_REGISTRY`` maps
``strategy_id`` → class. The Phase-3 ``cat`` scanner (§6.1 row 5, §2.7) registers here as a PEER of
the price baselines — same base class, same ``scan`` signature, ``catalyst_ref`` set to the
``catalyst_watchlist.entry_id`` — with its per-day entry cap enforced in the pre-screen (§3.2.5).
No ``cat`` code exists in Phase 1 (deliberately deferred, §8.2).
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from decimal import Decimal
from typing import ClassVar

from ulid import ULID

from engine.core.types import Bar
from engine.strategy.types import RawLevels, ScanContext, Side, SignalCandidate, Style


class Scanner(abc.ABC):
    """One §6.1 baseline rule. Subclasses pin ``strategy_id``/``style`` and implement :meth:`scan`."""

    strategy_id: ClassVar[str]
    style: ClassVar[Style]

    #: §6.3 envelope defaults for this scanner (bare keys). Overridden per-key via the constructor.
    DEFAULT_PARAMS: ClassVar[Mapping[str, float]] = {}

    def __init__(self, params: Mapping[str, float] | None = None) -> None:
        merged: dict[str, float] = {k: float(v) for k, v in self.DEFAULT_PARAMS.items()}
        if params:
            unknown = sorted(set(params) - set(merged))
            if unknown:
                raise ValueError(
                    f"{self.strategy_id}: unknown param(s) {unknown} — only §6.3 envelope keys "
                    f"{sorted(merged)} are injectable"
                )
            merged.update({k: float(v) for k, v in params.items()})
        self.params: dict[str, float] = merged

    @abc.abstractmethod
    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        """Return 0..n candidates for this bar. Pure; fail to zero on missing context."""

    # ------------------------------------------------------------------ candidate factory
    def _candidate(
        self,
        *,
        bar: Bar,
        ctx: ScanContext,
        side: Side,
        entry: Decimal,
        stop: Decimal | None = None,
        target: Decimal | None = None,
        score: float,
        catalyst_ref: str | None = None,
    ) -> SignalCandidate:
        """Mint a §3.2.5 candidate (ULID ``signal_id``; score clamped to [0, 1]).

        ``catalyst_ref`` stays ``None`` for every price baseline; only the Phase-3 ``cat`` peer
        passes the watchlist entry id (§2.7 audit chain).
        """
        return SignalCandidate(
            signal_id=str(ULID()),
            strategy_id=self.strategy_id,
            symbol=bar.symbol,
            side=side,
            style=self.style,
            raw_levels=RawLevels(entry=entry, stop=stop, target=target),
            score=min(1.0, max(0.0, float(score))),
            features_snapshot_id=ctx.features_snapshot_id,
            catalyst_ref=catalyst_ref,
        )


#: strategy_id → Scanner class. Populated by :func:`register` at module import time
#: (``engine.strategy.scanners.__init__`` imports every scanner module).
SCANNER_REGISTRY: dict[str, type[Scanner]] = {}


def register(cls: type[Scanner]) -> type[Scanner]:
    """Class decorator: add ``cls`` to :data:`SCANNER_REGISTRY` under its ``strategy_id``."""
    sid = cls.strategy_id
    existing = SCANNER_REGISTRY.get(sid)
    if existing is not None and existing is not cls:
        raise ValueError(f"scanner id {sid!r} already registered by {existing.__qualname__}")
    SCANNER_REGISTRY[sid] = cls
    return cls


def params_from_envelope(strategy_id: str, envelope: Mapping[str, float]) -> dict[str, float]:
    """Strip a namespaced §6.3 envelope mapping (``orb.vol_mult`` …) down to one scanner's bare params.

    The integrator reads live values from SQLite ``envelope_state`` (§6.5) keyed exactly like
    ``config/envelope.yaml`` and feeds each scanner through this helper.
    """
    prefix = strategy_id + "."
    return {k[len(prefix):]: float(v) for k, v in envelope.items() if k.startswith(prefix)}
