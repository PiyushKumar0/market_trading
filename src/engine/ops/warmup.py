"""Cold-start warm-up gate (§2.6 step 6 / §7.1 ``warmup_ready`` + ``regime_data_ready``).

Entries stay FROZEN until enough **contiguous** bars (live or startup-backfilled from official
candles) cover each strategy's feature lookbacks — never trade on thin data. This is distinct from
the feed-staleness guard (a live feed that died) and from a feed failure: it is about coverage.

Per-strategy lookback requirements (§6.1/§6.2):

- ``orb`` (intraday): today's 1m bars from the session open (09:15, shortened-session aware) through
  "now", contiguous per watch symbol — the opening range + ATR(14,1m) + last-30-bar features all
  live inside this span, so contiguity from the open covers them. (The ``cat`` scanner's fixed
  09:15–09:45 range is likewise inside this span, §6.1.)
- ``rsi2``/``trend``/``mom`` (daily): 200 completed sessions of ``bars_1d`` per symbol (200-DMA is
  the deepest lookback; trend's EMA/ADX and mom's 4-week rank sit inside it).
- **regime** (§7.1 ``regime_data_ready``): NIFTY 50 index + India VIX daily history present for the
  market-context/regime lookbacks. Missing regime data on a cold/late start freezes regime-dependent
  strategies; in Phase 1 the lifecycle applies the coarser FROZEN-for-entries (the per-strategy
  freeze split is the Phase-2 gate's job — documented deviation-by-phase, not by design).

The gate only ANSWERS (``ready()`` / ``missing()``); the **lifecycle** applies consequences (risk
FROZEN via the injected setter + a ``WARMUP_FROZEN`` alert, §2.6 step 6). If a start is too close to
the window to warm up (backfill already attempted at step 4 and coverage still short), the gate
simply keeps answering not-ready — entries stay FROZEN and the owner is alerted; they reopen only
once coverage is met.

All store scans go through the ``MarketStore`` async wrappers (executor-offloaded — the loop is
never blocked past the §2.2 heartbeat budget).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta

from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.warmup")

#: §6.1 daily-lookback strategies sharing the 200-session bars_1d requirement (200-DMA bound).
DAILY_STRATEGY_SCOPE = "rsi2/trend/mom"


class WarmupStatus(BaseModel):
    ready: bool
    blockers: list[str] = Field(default_factory=list)   # rendered "scope:symbol have/need" lines


class WarmupGate:
    """Contiguous-coverage warm-up gate over ``MarketStore`` (§2.6 step 6).

    Parameters
    ----------
    symbols:
        Today's intraday watch symbols (universe watchlist) — the ``orb`` coverage set.
    daily_symbols:
        Symbols needing the 200-session daily history (defaults to ``symbols``).
    index_symbol / vix_symbol:
        Canonical ``bars_1d`` symbols for NIFTY 50 and India VIX (§7.1 ``regime_data_ready``);
        the integrator passes whatever names the backfill job persists them under.
    daily_lookback_sessions:
        Deepest daily feature lookback (200 = 200-DMA, §6.2).
    vix_lookback_sessions:
        India VIX history needed by the market-context features (level/Δ + 20d realized vol, §6.2).
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        calendar: NSECalendar,
        *,
        symbols: Sequence[str],
        daily_symbols: Sequence[str] | None = None,
        index_symbol: str = "NIFTY 50",
        vix_symbol: str = "INDIA VIX",
        daily_lookback_sessions: int = 200,
        vix_lookback_sessions: int = 20,
    ) -> None:
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._symbols = list(symbols)
        self._daily_symbols = list(daily_symbols) if daily_symbols is not None else list(symbols)
        self._index_symbol = index_symbol
        self._vix_symbol = vix_symbol
        self._daily_n = int(daily_lookback_sessions)
        self._vix_n = int(vix_lookback_sessions)

    # ------------------------------------------------------------------ public surface
    async def ready(self) -> bool:
        return not await self.missing()

    async def status(self) -> WarmupStatus:
        blockers = await self.missing()
        return WarmupStatus(ready=not blockers, blockers=blockers)

    async def missing(self) -> list[str]:
        """Every unmet lookback as a rendered blocker line; empty ⇒ warm-up satisfied."""
        blockers: list[str] = []
        blockers.extend(await self._missing_intraday())
        for sym in self._daily_symbols:
            b = await self._missing_daily(DAILY_STRATEGY_SCOPE, sym, self._daily_n)
            if b:
                blockers.append(b)
        # §7.1 regime_data_ready — NIFTY 50 + India VIX history for market-context features.
        b = await self._missing_daily("regime", self._index_symbol, self._daily_n)
        if b:
            blockers.append(b)
        b = await self._missing_daily("regime", self._vix_symbol, self._vix_n)
        if b:
            blockers.append(b)
        if blockers:
            _log.warning("warmup_not_ready", blockers=blockers)
        return blockers

    # ------------------------------------------------------------------ intraday (orb, today 09:15+)
    async def _missing_intraday(self) -> list[str]:
        today = self._clock.today()
        session = self._calendar.session(today)
        if session is None:
            return []  # non-trading day — no session, no entries, no intraday requirement (R6)
        now = self._clock.now().replace(second=0, microsecond=0)
        start = session.open
        end = min(now, session.close)
        if end <= start:
            return []  # before the open — coverage accrues live; the gate is re-checked later
        need = int((end - start).total_seconds() // 60)
        out: list[str] = []
        for sym in self._symbols:
            gaps = await self._store.acoverage_gaps(sym, start, end)
            if gaps:
                out.append(f"orb:{sym} bars {need - len(gaps)}/{need}")
        return out

    # ------------------------------------------------------------------ daily lookbacks
    async def _missing_daily(self, scope: str, symbol: str, n: int) -> str | None:
        sessions = self._recent_sessions(n)
        if sessions is None:
            # Calendar horizon can't even ENUMERATE n sessions — conservative blocker ("no calendar,
            # no trading", R6): coverage that cannot be verified is treated as missing.
            return f"{scope}:{symbol} calendar horizon < {n} sessions"
        bars = await self._store.aget_bars_1d(symbol, sessions[-1], sessions[0])
        present = {b.d for b in bars}
        have = sum(1 for d in sessions if d in present)
        if have < n:
            return f"{scope}:{symbol} daily bars {have}/{n}"
        return None

    def _recent_sessions(self, n: int) -> list[date] | None:
        """The most recent ``n`` completed trading sessions strictly before today, DESCENDING
        (``[0]`` newest). None if the loaded calendars cannot supply ``n`` sessions (bounded walk)."""
        days: list[date] = []
        probe = self._clock.today() - timedelta(days=1)
        for _ in range(n * 3 + 90):   # bounded: weekends+holidays inflate ~n*1.5; never loop forever
            if len(days) >= n:
                break
            if self._calendar.is_trading_day(probe):
                days.append(probe)
            probe -= timedelta(days=1)
        return days if len(days) >= n else None
