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
  the deepest lookback; trend's EMA/ADX and mom's 4-week rank sit inside it). A YOUNG LISTING — one
  that listed fewer than 200 sessions ago and has a bar for every session since (full coverage, no
  gaps) — can never satisfy this gate, so gating on it would freeze entries forever; such symbols are
  EXCLUDED from the blockers and reported separately (``young_excluded``) instead. A shortfall with
  any gap is NOT a young listing and still blocks.
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
    #: Young LISTINGS short of the daily lookback ONLY because they listed < the lookback ago, with
    #: full coverage since listing (no gaps). Excluded from ``blockers`` (they can never satisfy a
    #: 200-session gate, so gating on them freezes entries forever) but surfaced here for the operator
    #: as "symbol(have/need)". A shortfall WITH gaps is a real blocker, not a young listing.
    young_excluded: list[str] = Field(default_factory=list)


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
    def set_symbols(self, symbols: Sequence[str], daily_symbols: Sequence[str] | None = None) -> None:
        """Refresh the coverage set (2026-07-28 review: it was frozen at boot, so an 08:30 universe
        change left the gate verifying YESTERDAY's watchlist for the rest of the day)."""
        self._symbols = list(symbols)
        self._daily_symbols = list(daily_symbols) if daily_symbols is not None else list(symbols)

    async def ready(self) -> bool:
        return not await self.missing()

    async def status(self) -> WarmupStatus:
        blockers, young = await self._evaluate()
        return WarmupStatus(ready=not blockers, blockers=blockers, young_excluded=young)

    async def missing(self) -> list[str]:
        """Every unmet lookback as a rendered blocker line; empty ⇒ warm-up satisfied (young listings
        excluded — see :meth:`status`.``young_excluded``)."""
        blockers, _young = await self._evaluate()
        return blockers

    async def _evaluate(self) -> tuple[list[str], list[str]]:
        """Single pass over every lookback ⇒ ``(blockers, young_excluded)``.

        Young listings (full coverage since a listing more recent than the lookback) are pulled OUT of
        blockers so they never freeze entries forever, but reported in ``young_excluded`` so the
        exclusion is visible, never silent (§2.6 step 6 — the freeze may only get MORE visible)."""
        blockers: list[str] = []
        young: list[str] = []
        blockers.extend(await self._missing_intraday())
        for sym in self._daily_symbols:
            b, y = await self._classify_daily(DAILY_STRATEGY_SCOPE, sym, self._daily_n)
            if b:
                blockers.append(b)
            if y:
                young.append(y)
        # §7.1 regime_data_ready — NIFTY 50 + India VIX history for market-context features.
        b, y = await self._classify_daily("regime", self._index_symbol, self._daily_n)
        if b:
            blockers.append(b)
        if y:
            young.append(y)
        b, y = await self._classify_daily("regime", self._vix_symbol, self._vix_n)
        if b:
            blockers.append(b)
        if y:
            young.append(y)
        if blockers:
            _log.warning("warmup_not_ready", blockers=blockers, young_excluded=young)
        return blockers, young

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
    async def _classify_daily(self, scope: str, symbol: str, n: int) -> tuple[str | None, str | None]:
        """Classify ``symbol``'s daily coverage ⇒ ``(blocker, young_label)`` (exactly one is non-None,
        or both None when covered). ``young_label`` fires ONLY for a fresh listing (full coverage since
        a first bar more recent than ``n`` sessions); a shortfall with any gap stays a blocker."""
        sessions = self._recent_sessions(n)
        if sessions is None:
            # Calendar horizon can't even ENUMERATE n sessions — conservative blocker ("no calendar,
            # no trading", R6): coverage that cannot be verified is treated as missing.
            return f"{scope}:{symbol} calendar horizon < {n} sessions", None
        bars = await self._store.aget_bars_1d(symbol, sessions[-1], sessions[0])
        present = {b.d for b in bars}
        have = sum(1 for d in sessions if d in present)
        if have >= n:
            return None, None
        # Short of the lookback. Distinguish a YOUNG LISTING from a real gap: a young listing has its
        # FIRST-EVER bar inside the lookback window (so it cannot supply n sessions) AND a bar for every
        # session since that first bar (total available == sessions-since-listing, full coverage). A
        # shortfall that fails EITHER test is a genuine gap and still blocks (never weakened).
        first_bar, total = await self._store.adaily_bar_span(symbol)
        if first_bar is not None and first_bar > sessions[-1]:
            since_listing = sum(1 for d in sessions if d >= first_bar)
            if total == since_listing:
                return None, f"{symbol}({total}/{n})"
        return f"{scope}:{symbol} daily bars {have}/{n}", None

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
