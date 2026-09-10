"""Pending-correlation buffer for the postback-before-response race (WO-P3-1, 2026-09-10; §3.5.1/A3).

The race, verbatim from §3.5.1: *"an order update can arrive on the socket BEFORE the synchronous
``place_order()`` call returns the broker order_id. The state machine must tolerate this — an update
for a broker order_id the platform has not yet recorded is held in a pending-correlation buffer and
applied once the place call returns that id, never dropped and never mistaken for a stray/duplicate
order that would trigger a defensive cancel."*

Two failure modes are designed out here:

* **Dropping an update.** Nothing leaves this buffer without being handed to a caller. Eviction and
  expiry RETURN the entries they remove (the caller logs them, §9.2 conservation property:
  ``held == pending + resolved + evicted + expired``) — a silent drop is a lost order.
* **Unbounded growth.** A broker id that never correlates (a stray from another session, a malformed
  frame) would otherwise leak forever. The buffer is capped; the OLDEST entry is evicted first and
  handed back, so the loss is always visible and always the least-recent one.

Pure Tier 3: stdlib + ``core`` only. No clock is held — the caller passes arrival/`now` times from
its own :class:`~engine.core.clock.Clock`, which keeps replay deterministic (§9.6).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from engine.core.log import get_logger

_log = get_logger("engine.oms.correlation")

#: Default cap. A trading day places tens of orders; hundreds of uncorrelated buffered updates means
#: something is structurally wrong, and the cap turns that into visible evictions rather than a leak.
DEFAULT_MAX_ENTRIES = 512


class BufferedUpdate(BaseModel):
    """One held postback: the broker id it arrived for, the verbatim payload, and its arrival time."""

    model_config = ConfigDict(frozen=True)

    broker_order_id: str
    data: dict[str, Any] = Field(default_factory=dict)
    at: datetime
    seq: int                                  # monotonic arrival sequence; preserves order on resolve

    @field_validator("at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("BufferedUpdate.at must be tz-aware IST (§3.2)")
        return v


class CorrelationAccounting(BaseModel):
    """Conservation ledger (§9.2). ``held`` is every update ever offered to :meth:`hold`."""

    model_config = ConfigDict(frozen=True)

    held: int
    pending: int
    resolved: int
    evicted: int
    expired: int


class PendingCorrelation:
    """Bounded, order-preserving buffer of postbacks whose broker order id is not yet known (A3)."""

    def __init__(self, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self._max_entries = max_entries
        self._entries: list[BufferedUpdate] = []      # arrival order; small by construction
        self._seq = 0
        self._held = 0
        self._resolved = 0
        self._evicted = 0
        self._expired = 0

    # ----------------------------------------------------------------- writes
    def hold(self, broker_order_id: str, data: dict[str, Any], *, at: datetime) -> list[BufferedUpdate]:
        """Buffer one update whose broker id the platform has not recorded yet.

        ``at`` is the arrival time (from the caller's ``Clock``) and is what :meth:`sweep` ages
        against — the buffer holds no clock of its own so replay stays deterministic (§9.6).

        Returns the entries evicted to stay within ``max_entries``, OLDEST first. The list is
        normally empty; when it is not, the caller must log every entry (never drop silently).
        """
        self._seq += 1
        self._held += 1
        self._entries.append(
            BufferedUpdate(broker_order_id=str(broker_order_id), data=dict(data), at=at, seq=self._seq)
        )
        evicted: list[BufferedUpdate] = []
        while len(self._entries) > self._max_entries:
            evicted.append(self._entries.pop(0))
        if evicted:
            self._evicted += len(evicted)
            _log.warning(
                "pending_correlation_evicted",
                count=len(evicted),
                max_entries=self._max_entries,
                broker_order_ids=[e.broker_order_id for e in evicted],
            )
        return evicted

    def resolve(self, order_id: str, broker_order_id: str) -> list[BufferedUpdate]:
        """Hand back every buffered update for ``broker_order_id`` in ARRIVAL ORDER, and drain them.

        Called the moment ``place_order()`` returns the id for platform order ``order_id`` (recorded
        for the audit chain). Applying them out of order would let a stale ACK overwrite a fill.

        Returns the ENTRIES, not the bare payloads (2026-09-10 review): each carries the ``at`` the
        platform received it, which the caller passes into
        :func:`engine.oms.updates.apply_update` so the ``order_events`` row records when the broker
        action reached us rather than when the buffer happened to drain (R8). ``seq`` is the arrival
        sequence, preserved for the same reason.
        """
        bid = str(broker_order_id)
        keep: list[BufferedUpdate] = []
        taken: list[BufferedUpdate] = []
        for entry in self._entries:
            (taken if entry.broker_order_id == bid else keep).append(entry)
        self._entries = keep
        if taken:
            self._resolved += len(taken)
            _log.info(
                "pending_correlation_resolved",
                order_id=order_id,
                broker_order_id=bid,
                count=len(taken),
            )
        return sorted(taken, key=lambda e: e.seq)

    def sweep(self, now: datetime, max_age_s: float) -> list[BufferedUpdate]:
        """Expire entries older than ``max_age_s`` and RETURN them, oldest first.

        An entry this old will never correlate (the place call has long returned or failed), but it
        is still evidence of a broker action the platform cannot explain — the caller logs/alerts on
        every returned entry. Never a silent drop.
        """
        cutoff = float(max_age_s)
        stale = [e for e in self._entries if (now - e.at).total_seconds() > cutoff]
        if not stale:
            return []
        stale_seqs = {e.seq for e in stale}
        self._entries = [e for e in self._entries if e.seq not in stale_seqs]
        self._expired += len(stale)
        _log.warning(
            "pending_correlation_expired",
            count=len(stale),
            max_age_s=cutoff,
            broker_order_ids=[e.broker_order_id for e in stale],
        )
        return sorted(stale, key=lambda e: e.seq)

    # ----------------------------------------------------------------- reads
    def pending(self, broker_order_id: str | None = None) -> list[BufferedUpdate]:
        """Currently-buffered entries, arrival order (all, or just one broker id)."""
        if broker_order_id is None:
            return list(self._entries)
        bid = str(broker_order_id)
        return [e for e in self._entries if e.broker_order_id == bid]

    def accounting(self) -> CorrelationAccounting:
        """The §9.2 conservation ledger: ``held == pending + resolved + evicted + expired``."""
        return CorrelationAccounting(
            held=self._held,
            pending=len(self._entries),
            resolved=self._resolved,
            evicted=self._evicted,
            expired=self._expired,
        )

    def __len__(self) -> int:
        return len(self._entries)
