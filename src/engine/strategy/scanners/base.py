"""Scanner base class + registry — the seam every per-bar baseline plugs into.

A ``Scanner`` is a PURE function of ``(bar, ScanContext, params)``: no Clock, no I/O, no mutable
state — same inputs ⇒ same candidates (determinism, modulo the platform-minted ``signal_id``
ULID). ONE carve-out (2026-09-12): LOG-ONLY state that cannot reach the returned candidates — a
once-per-session latch so a shut filter says so without one line per bar per symbol. It must be
keyed on ``bar.ts_minute.date()``, never a Clock, so a replay logs once per replayed session too;
:attr:`~engine.strategy.scanners.rsi2.Rsi2Scanner._regime_logged_on` is the pattern to copy. State
that a ``scan``/``pending`` RESULT depends on is still forbidden outright.
Anything a scanner needs beyond the bar arrives via :class:`~engine.strategy.types.ScanContext`
(assembled by the pre-screen's context provider); missing/thin context means FAIL TO ZERO (return
``[]``), never raise — thin data is a warm-up condition (the risk gate's ``warmup_ready``), not an error.

Params are injected as a plain dict whose keys mirror the envelope rows for the scanner's
``strategy_id`` (bare, un-namespaced: ``orb.vol_mult`` → ``vol_mult``); ``DEFAULT_PARAMS`` carries the
envelope defaults so a scanner built with no params trades the envelope defaults. Unknown keys are a hard
``ValueError`` (typo guard — a silently-ignored param would quietly detune a strategy).

Registry: modules self-register via :func:`register` at import time; ``SCANNER_REGISTRY`` maps
``strategy_id`` → class. The four per-bar price baselines (orb/rsi2/trend/mom) self-register here.
The EOD-batch rules (brk20, ins, hi52, cat, cat_reversal) are deliberately NOT Scanners; they enter
via ``SignalPreScreen.admit``, where the catalyst cap for ``catalyst_ref`` candidates is enforced.
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from decimal import Decimal
from typing import ClassVar

from ulid import ULID

from engine.core.types import Bar
from engine.strategy.types import PendingSetup, RawLevels, ScanContext, Side, SignalCandidate, Style


class Scanner(abc.ABC):
    """One baseline rule. Subclasses pin ``strategy_id``/``style`` and implement :meth:`scan`."""

    strategy_id: ClassVar[str]
    style: ClassVar[Style]

    #: envelope defaults for this scanner (bare keys). Overridden per-key via the constructor.
    DEFAULT_PARAMS: ClassVar[Mapping[str, float]] = {}

    def __init__(self, params: Mapping[str, float] | None = None) -> None:
        merged: dict[str, float] = {k: float(v) for k, v in self.DEFAULT_PARAMS.items()}
        if params:
            unknown = sorted(set(params) - set(merged))
            if unknown:
                raise ValueError(
                    f"{self.strategy_id}: unknown param(s) {unknown} — only parameter-envelope keys "
                    f"{sorted(merged)} are injectable"
                )
            merged.update({k: float(v) for k, v in params.items()})
        self.params: dict[str, float] = merged

    @abc.abstractmethod
    def scan(self, bar: Bar, ctx: ScanContext) -> list[SignalCandidate]:
        """Return 0..n candidates for this bar. Pure; fail to zero on missing context."""

    def pending(self, bar: Bar, ctx: ScanContext) -> list[PendingSetup]:
        """Setups NOT currently true, with the deterministic level that would arm them (2026-07-29). Same purity/fail-to-zero rules as :meth:`scan`; informational only —
        pendings never enter the pipeline. Default: a scanner that cannot cheaply invert its
        condition reports nothing."""
        return []

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
        """Mint a candidate (ULID ``signal_id``; score clamped to [0, 1]).

        ``catalyst_ref`` is unused by every registered scanner (always ``None``); the batch rules
        that carry a watchlist entry id build their candidates in ``_shadow_catalyst.build_candidate``
        (catalyst audit chain).
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
    """Strip a namespaced envelope mapping (``orb.vol_mult`` …) down to one scanner's bare params.

    The intended online-learning seam for feeding SQLite ``envelope_state`` (keyed like
    ``config/envelope.yaml``) to each scanner; NOT wired today: ``engine.ops.main`` builds
    orb/rsi2/trend/mom on code ``DEFAULT_PARAMS`` and ``envelope_state`` rows for those keys are
    not read (see ``scan_context.py``).
    """
    prefix = strategy_id + "."
    return {k[len(prefix):]: float(v) for k, v in envelope.items() if k.startswith(prefix)}
