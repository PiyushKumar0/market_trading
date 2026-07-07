"""Client-side order/quote/historical rate limiting (§3.2.2, A2/B3, §7.1 ``order_rate``).

This module is the SINGLE order-API chokepoint. Every call into Kite REST is paced through
:class:`RateLimiter`, which keeps the platform ``<=10`` order-operations-per-second **by construction**
(B3) and well under the broker's 5,000-orders/day ceiling. Because the order bucket sustains only
1 call/s (~<=390 calls even on a fully saturated day), self-learning never crosses into the
algo-registration regime (B3).

Design (load-bearing safety, R3):
  * Per-endpoint-class token buckets (A2). The order bucket counts ``place + modify + cancel`` against
    one **shared, conservative** budget — strictly stricter than the per-API-key reading of the broker
    docs, kept deliberately (§7.1 ``order_rate`` "conservative shared budget kept").
  * The order budget is **split by intent** (§7.1 ``order_rate``):
      - ``intent="entry"``  — entry-related place/modify/cancel calls are hard-capped at
        ``entry_calls_per_day`` (default 70/day, [tunable]). Once the cap is hit, :meth:`acquire`
        raises :class:`EntryBudgetExhausted`. A warning is logged at 80% of the budget.
      - ``intent="risk_reducing"`` — protective placement/tighten, exits, square-off and kill-flatten.
        These are PACED behind the same 1/s bucket but **UNCAPPED and never budget-rejected** (R3): a
        churny entry day must never be able to starve a kill-flatten / square-off / exit. There is
        deliberately **no** single "100/day total" cap; the only hard ceiling is the broker's
        5,000/day limit, made unreachable by the 1/s pace.

Pure-Python + asyncio only (no third-party deps) so the whole limiter is unit-testable at the bare
test tier. ``clock.now()`` (the single sanctioned "now", R6) drives both daily-reset detection and
the monotonic bucket-refill timing.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from engine.core.clock import Clock
from engine.core.log import get_logger

_log = get_logger("engine.broker.rate_limiter")

# --- §7.1 ``order_rate`` / A2 token-bucket constants -------------------------------------------------
# Per-endpoint-class sustained refill rates (requests per second) and burst depths. Each number is the
# §7.1 / A2 value with its citation; do not "tune" these in code — the order budget split is owner-set
# via the constructor (§7.1 ``[tunable]``), but the per-second pacing here is a B3 safety invariant.

# quote: Kite quote/LTP/OHLC endpoints — 1 req/s (A2 conservative; §3.2.3 "within 3 req/s" is historical,
# quotes are paced tighter at 1/s here as the conservative shared reading, A2).
_QUOTE_RATE_PER_S = 1.0
_QUOTE_BURST = 1

# historical: Kite historical-candle endpoint — 3 req/s (A2 / §3.2.3 "historical backfill within 3 req/s").
_HISTORICAL_RATE_PER_S = 3.0
_HISTORICAL_BURST = 3

# orders: place + modify + cancel counted together (A2 shared budget) — 1 order-API call/s sustained with
# burst 2 (§7.1 ``order_rate``: "1 order-API call/s sustained, burst 2"). 1/s keeps the platform <=10 OPS
# by construction (B3); burst 2 absorbs a place-then-protective-SL-M pair without stalling.
_ORDERS_RATE_PER_S = 1.0
_ORDERS_BURST = 2

# Entry budget: hard cap on entry-intent order calls per day (§7.1 ``order_rate``: "entry calls
# hard-capped at 70/day [tunable]"). Alert (log warning) at 80% of this budget (§7.1 "alert at 80%").
_DEFAULT_ENTRY_CALLS_PER_DAY = 70
_ENTRY_BUDGET_ALERT_FRACTION = 0.80

EndpointClass = Literal["quote", "historical", "orders"]
Intent = Literal["entry", "risk_reducing"]


class EntryBudgetExhausted(RuntimeError):
    """Raised by :meth:`RateLimiter.acquire` when the per-day **entry** order budget is exhausted.

    Risk-reducing calls are never rejected (R3); this is raised only for ``intent="entry"`` once the
    ``entry_calls_per_day`` cap (§7.1 ``order_rate``) has been reached for the current session.
    """


@dataclass
class _Bucket:
    """A single token bucket (A2). Monotonic refill driven off an injected ``clock.now()`` epoch.

    Tokens accrue continuously at ``rate_per_s`` up to ``capacity`` (the burst depth). ``_last`` is the
    monotonic seconds value (derived from ``clock.now()``) at which ``tokens`` was last computed, so
    refill is purely a function of elapsed wall time — no busy-loop, no background task.
    """

    rate_per_s: float
    capacity: float
    tokens: float = field(default=0.0)
    _last: float = field(default=0.0)

    def _refill(self, now_monotonic_s: float) -> None:
        elapsed = now_monotonic_s - self._last
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_s)
            self._last = now_monotonic_s

    def wait_seconds(self, now_monotonic_s: float) -> float:
        """Refill against ``now`` and return how long the caller must sleep before a token is free.

        Returns 0.0 when a token is immediately available (and reserves it); otherwise returns the
        time until exactly one token has accrued (the caller sleeps that long, then retries).
        """
        self._refill(now_monotonic_s)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        deficit = 1.0 - self.tokens
        return deficit / self.rate_per_s


class RateLimiter:
    """Token-bucket limiter: the single order-API chokepoint (§3.2.2, B3, §7.1 ``order_rate``).

    Parameters
    ----------
    clock:
        The single source of "now" (R6). Used for daily-reset boundary detection and for the
        monotonic refill epoch — never a bare ``datetime.now()``.
    sustained_per_s:
        Sustained order-bucket refill rate, requests/second (§7.1 ``order_rate`` = 1.0).
    burst:
        Order-bucket burst depth (§7.1 ``order_rate`` = 2).
    entry_calls_per_day:
        Hard daily cap on ``intent="entry"`` order calls (§7.1 ``order_rate`` = 70, [tunable]).
        Risk-reducing calls are uncapped (R3).
    """

    def __init__(
        self,
        clock: Clock,
        *,
        sustained_per_s: float = 1.0,
        burst: int = 2,
        entry_calls_per_day: int = _DEFAULT_ENTRY_CALLS_PER_DAY,
    ) -> None:
        self._clock = clock
        self._entry_calls_per_day = entry_calls_per_day
        # One lock per endpoint class serializes the read-modify-write of each bucket so concurrent
        # acquire() coroutines pace correctly rather than all draining the same token.
        self._locks: dict[str, asyncio.Lock] = {
            "quote": asyncio.Lock(),
            "historical": asyncio.Lock(),
            "orders": asyncio.Lock(),
        }
        now_s = self._monotonic_s()
        # Per-endpoint-class buckets (A2). Buckets start full so the first call(s) up to the burst
        # depth proceed without an artificial cold-start stall.
        self._buckets: dict[str, _Bucket] = {
            "quote": _Bucket(_QUOTE_RATE_PER_S, _QUOTE_BURST, tokens=_QUOTE_BURST, _last=now_s),
            "historical": _Bucket(
                _HISTORICAL_RATE_PER_S, _HISTORICAL_BURST, tokens=_HISTORICAL_BURST, _last=now_s
            ),
            # Order bucket uses the constructor-provided sustained rate / burst (§7.1 ``order_rate``).
            "orders": _Bucket(
                float(sustained_per_s), float(burst), tokens=float(burst), _last=now_s
            ),
        }
        # Per-day order counters (§7.1 ``order_rate`` split), returned by orders_today().
        self._entry_calls = 0
        self._risk_reducing_calls = 0
        self._entry_alert_fired = False
        # Day boundary used to auto-detect a session rollover (defensive; reset_day() is the primary
        # rollover hook). Derived from clock.now() (R6).
        self._day: date = self._clock.now().date()

    # -- internals ----------------------------------------------------------------------------------

    def _monotonic_s(self) -> float:
        """A monotonic-ish seconds epoch derived from ``clock.now()`` (R6).

        Refill only ever uses *differences* of this value, so any fixed epoch is fine; using the
        injected Clock keeps refill timing deterministic under replay (a replay run drives a controlled
        ``time_source``) rather than reading the wall clock independently.
        """
        return self._clock.now().timestamp()

    def _maybe_rollover(self) -> None:
        """Auto-reset counters if ``clock.now()`` has crossed into a new calendar day (§7.1 daily reset).

        ``reset_day()`` (called at the explicit session rollover) is the primary path; this is a
        defensive backstop so a long-lived limiter that is never explicitly reset still re-budgets at
        the day boundary rather than carrying stale counts.
        """
        today = self._clock.now().date()
        if today != self._day:
            self.reset_day()

    # -- public API ---------------------------------------------------------------------------------

    async def acquire(
        self,
        endpoint_class: EndpointClass,
        intent: Intent = "entry",
    ) -> None:
        """Block (paced, never busy-looping) until one token is available, then consume it.

        For ``endpoint_class == "orders"`` the call is additionally accounted against the §7.1
        ``order_rate`` intent split:

          * ``intent="entry"`` is hard-capped at ``entry_calls_per_day``; once the cap is hit this
            raises :class:`EntryBudgetExhausted` (checked **before** pacing, so an exhausted entry
            budget rejects immediately rather than after an unnecessary sleep). A warning is logged the
            first time usage reaches 80% of the budget.
          * ``intent="risk_reducing"`` is uncapped and **never** rejected on budget (R3) — it is only
            paced behind the 1/s order bucket so a churny entry day can never starve a flatten.

        ``intent`` is ignored for ``"quote"``/``"historical"`` (it only governs the order budget split).
        """
        self._maybe_rollover()

        # --- Entry-budget hard cap (orders only), checked before pacing (R3 / §7.1 ``order_rate``).
        if endpoint_class == "orders" and intent == "entry":
            if self._entry_calls >= self._entry_calls_per_day:
                _log.warning(
                    "entry_budget_exhausted",
                    entry_calls=self._entry_calls,
                    entry_calls_per_day=self._entry_calls_per_day,
                )
                raise EntryBudgetExhausted(
                    f"entry order budget exhausted: {self._entry_calls}/"
                    f"{self._entry_calls_per_day} per day (§7.1 order_rate)"
                )

        # --- Pace behind the endpoint-class bucket. Sleep, re-check; tokens may have been taken by a
        # concurrent acquire() while we slept, so loop until we actually reserve one.
        bucket = self._buckets[endpoint_class]
        lock = self._locks[endpoint_class]
        while True:
            async with lock:
                wait = bucket.wait_seconds(self._monotonic_s())
            if wait <= 0.0:
                break
            await asyncio.sleep(wait)

        # --- Account the order call AFTER it has been paced/admitted (orders only).
        if endpoint_class == "orders":
            if intent == "entry":
                self._entry_calls += 1
                threshold = self._entry_calls_per_day * _ENTRY_BUDGET_ALERT_FRACTION
                if not self._entry_alert_fired and self._entry_calls >= threshold:
                    self._entry_alert_fired = True
                    _log.warning(
                        "entry_budget_alert",
                        entry_calls=self._entry_calls,
                        entry_calls_per_day=self._entry_calls_per_day,
                        fraction=_ENTRY_BUDGET_ALERT_FRACTION,
                    )
            else:  # intent == "risk_reducing" — uncapped, never budget-rejected (R3).
                self._risk_reducing_calls += 1

    def orders_today(self) -> tuple[int, int]:
        """Return ``(entry_calls, risk_reducing_calls)`` for the current session (§3.2.2/§7.1).

        ``entry_calls`` is hard-capped at ``entry_calls_per_day`` (B3); ``risk_reducing_calls`` is
        uncapped (R3).
        """
        return (self._entry_calls, self._risk_reducing_calls)

    def reset_day(self) -> None:
        """Zero the per-day counters at session rollover (§7.1 daily reset).

        Token-bucket fill state is intentionally left intact — pacing is continuous across the
        rollover; only the daily entry/risk-reducing budgets reset.
        """
        self._entry_calls = 0
        self._risk_reducing_calls = 0
        self._entry_alert_fired = False
        self._day = self._clock.now().date()
        _log.info("rate_limiter_day_reset", day=self._day.isoformat())
