"""Cold-start warm-up gate (``warmup_ready`` + ``regime_data_ready``).

Entries stay FROZEN until enough **contiguous** bars (live or startup-backfilled from official
candles) cover each strategy's feature lookbacks. This is about coverage, distinct from the
feed-staleness guard and from a feed failure.

Per-strategy lookback requirements:

- ``orb`` (intraday): today's 1m bars from the session open (shortened-session aware) through "now",
  contiguous per watch symbol (the opening range, ATR(14,1m) and ``cat``'s 09:15–09:45 range all
  live inside this span).
- ``rsi2``/``trend``/``mom`` (daily): 200 completed sessions of ``bars_1d`` per symbol (the 200-DMA is
  the deepest lookback). A YOUNG LISTING — listed fewer than 200 sessions ago with a bar for every
  session since — can never satisfy this gate, so it is EXCLUDED from the blockers and reported as
  ``young_excluded``. A shortfall with any gap is NOT a young listing and still blocks.
- **regime** (``regime_data_ready``): NIFTY 50 + India VIX daily history for the market-context
  lookbacks.

The gate only ANSWERS (``ready()`` / ``missing()``); the **lifecycle** applies consequences (risk
FROZEN via the injected setter + a ``WARMUP_FROZEN`` alert). A start too close to the window to warm
up keeps answering not-ready; entries reopen only once coverage is met.

Every blocker carries its CLASS in its rendered prefix (:func:`blocker_class`): ``orb:`` ⇒ intraday
1-minute coverage, ``rsi2/trend/mom:`` ⇒ completed daily sessions, ``regime:`` ⇒ NIFTY 50 / India
VIX history — and its SYMBOL in the rest of the line (:func:`blocker_symbol`). The consequence is per
class and per SYMBOL: only the REGIME class (every candidate depends on it) and an UNATTRIBUTABLE
blocker drive the global FROZEN-for-entries, while an INTRADAY or DAILY shortfall refuses THAT
SYMBOL's candidates per-candidate (risk gate + pre-screen). ``ready`` means no blockers at all.

All store scans go through the ``MarketStore`` async wrappers (executor-offloaded).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date, timedelta

from pydantic import BaseModel, Field, model_validator

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.warmup")

#: Daily-lookback strategies sharing the 200-session bars_1d requirement (200-DMA bound).
DAILY_STRATEGY_SCOPE = "rsi2/trend/mom"

#: Deepest daily feature lookback (200-DMA) — the gate's default daily coverage window.
DAILY_LOOKBACK_SESSIONS = 200

#: The three warm-up COVERAGE CLASSES: a property of the missing DATA, not of a strategy.
#: ``intraday`` = today's 1-minute bars, ``daily`` = completed daily sessions, ``regime`` = index/VIX
#: daily history.
CLASS_INTRADAY = "intraday"
CLASS_DAILY = "daily"
CLASS_REGIME = "regime"
#: A blocker whose scope prefix matches no class — a synthesized fail-closed sentinel or a future
#: unclassified scope. Coverage that cannot be ATTRIBUTED blocks EVERY class.
CLASS_UNKNOWN = "unknown"

_REAL_CLASSES = (CLASS_INTRADAY, CLASS_DAILY, CLASS_REGIME)

#: Rendered-prefix ⇒ class. The prefix IS the class label (``orb:``, or the scope passed to
#: ``_classify_daily``). A new scope must be added here AND to the ``warmup_ready`` rows, or it
#: falls to :data:`CLASS_UNKNOWN` and blocks everything.
_CLASS_BY_PREFIX: tuple[tuple[str, str], ...] = (
    ("orb:", CLASS_INTRADAY),
    (f"{DAILY_STRATEGY_SCOPE}:", CLASS_DAILY),
    ("regime:", CLASS_REGIME),
)


def blocker_class(blocker: str) -> str:
    """The coverage class of one rendered blocker line (:data:`CLASS_UNKNOWN` when unattributable)."""
    text = str(blocker)
    for prefix, cls in _CLASS_BY_PREFIX:
        if text.startswith(prefix):
            return cls
    return CLASS_UNKNOWN


#: The SYMBOL inside a rendered blocker line. Anchored over EXACTLY the three renderings
#: :meth:`WarmupGate._evaluate` produces — ``orb:{sym} bars {have}/{need}``,
#: ``{scope}:{sym} daily bars {have}/{n}`` and ``{scope}:{sym} calendar horizon < {n} sessions`` —
#: so names containing spaces, ``&`` or ``-`` (``NIFTY 50``, ``M&M``, ``BAJAJ-AUTO``) are recovered.
#: Anything else is UNATTRIBUTABLE (None), never a guessed symbol.
_BLOCKER_SYMBOL_RE = re.compile(
    r"^(?P<scope>orb|" + re.escape(DAILY_STRATEGY_SCOPE) + r"|regime):"
    r"(?P<sym>.+?) (?:daily bars \d+/\d+|bars \d+/\d+|calendar horizon < \d+ sessions)$"
)


def blocker_symbol(blocker: str) -> str | None:
    """The SYMBOL a rendered blocker line is attributed to, or None. None is fail-closed: a shortfall
    nobody can pin on a symbol refuses EVERY symbol in its class."""
    m = _BLOCKER_SYMBOL_RE.match(str(blocker))
    return m.group("sym") if m is not None else None


def classify_blockers(blockers: Sequence[str]) -> dict[str, list[str]]:
    """Bucket rendered blocker lines by :func:`blocker_class`, order preserved. Empty classes are
    absent from the mapping (a caller asks with ``.get(cls)``, never by membership)."""
    out: dict[str, list[str]] = {}
    for blocker in blockers:
        out.setdefault(blocker_class(blocker), []).append(str(blocker))
    return out


def recent_sessions(calendar: NSECalendar, today: date, n: int) -> list[date] | None:
    """The most recent ``n`` completed trading sessions strictly before ``today``, DESCENDING
    (``[0]`` newest). None if the loaded calendars cannot supply ``n`` sessions (bounded walk).
    Module-level so the daily gap-fill and the gate share ONE definition of the window — a fill that
    disagrees with the gate about which sessions count can never clear the gate's blocker."""
    days: list[date] = []
    probe = today - timedelta(days=1)
    for _ in range(n * 3 + 90):   # bounded: weekends+holidays inflate ~n*1.5; never loop forever
        if len(days) >= n:
            break
        if calendar.is_trading_day(probe):
            days.append(probe)
        probe -= timedelta(days=1)
    return days if len(days) >= n else None


class WarmupStatus(BaseModel):
    ready: bool
    blockers: list[str] = Field(default_factory=list)   # rendered "scope:symbol have/need" lines
    #: Young LISTINGS (full coverage since listing, no gaps) short of the daily lookback. Excluded
    #: from ``blockers`` but surfaced as "symbol(have/need)". A shortfall WITH gaps is a real blocker.
    young_excluded: list[str] = Field(default_factory=list)
    #: ``blockers`` bucketed by class. DERIVED on every construction, never passed in, so no
    #: construction site can hand out a stale or absent bucketing.
    blockers_by_class: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _derive_classes(self) -> WarmupStatus:
        self.blockers_by_class = classify_blockers(self.blockers)
        return self

    def ready_for(self, cls: str, symbol: str | None = None) -> bool:
        """Is the ``cls`` coverage class satisfied — for the whole book, or for one SYMBOL?

        ``symbol=None`` is CLASS-WIDE (False if ANY symbol is short in ``cls``); the freeze / lift /
        owner-notice code asks this. With a ``symbol``: False iff a blocker line in this class is
        attributed to THAT symbol or to no symbol, so one symbol's coverage hole never refuses every
        candidate of its class.

        Fail-closed: an UNKNOWN-class blocker holds every class and symbol down, a not-ready status
        with no blockers is unattributable the same way, an unparseable blocker in this class holds
        every symbol in it down, and an unrecognised ``cls`` is never "ready"."""
        if cls not in _REAL_CLASSES:
            return False
        if not self.ready and not self.blockers:
            return False
        if self.blockers_by_class.get(CLASS_UNKNOWN):
            return False
        lines = self.blockers_by_class.get(cls) or []
        if not lines:
            return True
        if symbol is None:
            return False
        want = str(symbol)
        return not any(blocker_symbol(line) in (None, want) for line in lines)

    def short_symbols(self, cls: str) -> list[str]:
        """The symbols ``cls``'s blockers are attributed to — order preserved, de-duplicated,
        unattributable lines omitted (:meth:`ready_for` handles them)."""
        out: list[str] = []
        for line in self.blockers_by_class.get(cls) or []:
            sym = blocker_symbol(line)
            if sym is not None and sym not in out:
                out.append(sym)
        return out


class WarmupGate:
    """Contiguous-coverage warm-up gate over ``MarketStore``.

    Parameters
    ----------
    symbols:
        Today's intraday watch symbols (universe watchlist) — the ``orb`` coverage set.
    daily_symbols:
        Symbols needing the 200-session daily history (defaults to ``symbols``).
    index_symbol / vix_symbol:
        Canonical ``bars_1d`` symbols for NIFTY 50 and India VIX (``regime_data_ready``);
        the integrator passes whatever names the backfill job persists them under.
    daily_lookback_sessions:
        Deepest daily feature lookback (200 = 200-DMA).
    vix_lookback_sessions:
        India VIX history needed by the market-context features (level/Δ + 20d realized vol).
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
        daily_lookback_sessions: int = DAILY_LOOKBACK_SESSIONS,
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
        """Refresh the coverage set after a universe change (it is not frozen at boot)."""
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

        Young listings are pulled OUT of blockers but reported in ``young_excluded``, never silently."""
        blockers: list[str] = []
        young: list[str] = []
        blockers.extend(await self._missing_intraday())
        for sym in self._daily_symbols:
            b, y = await self._classify_daily(DAILY_STRATEGY_SCOPE, sym, self._daily_n)
            if b:
                blockers.append(b)
            if y:
                young.append(y)
        # regime_data_ready — NIFTY 50 + India VIX history for market-context features.
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
            _log.warning("warmup_not_ready", blockers=blockers, young_excluded=young,
                         classes=sorted(classify_blockers(blockers)))
        return blockers, young

    # ------------------------------------------------------------------ intraday (orb, today 09:15+)
    async def _missing_intraday(self) -> list[str]:
        today = self._clock.today()
        session = self._calendar.session(today)
        if session is None:
            return []  # non-trading day — no session, no entries, no intraday requirement
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
            # Calendar horizon can't ENUMERATE n sessions: unverifiable coverage counts as missing.
            return f"{scope}:{symbol} calendar horizon < {n} sessions", None
        bars = await self._store.aget_bars_1d(symbol, sessions[-1], sessions[0])
        present = {b.d for b in bars}
        have = sum(1 for d in sessions if d in present)
        if have >= n:
            return None, None
        # A young listing has its FIRST-EVER bar inside the lookback window AND a bar for every
        # session since; failing EITHER test is a genuine gap and blocks. Coverage is judged INSIDE the
        # session window only: the span's total includes today's live bar, which the window excludes.
        first_bar, _total = await self._store.adaily_bar_span(symbol)
        if first_bar is not None and first_bar > sessions[-1]:
            since_listing = [d for d in sessions if d >= first_bar]
            if all(d in present for d in since_listing):
                return None, f"{symbol}({len(since_listing)}/{n})"
        return f"{scope}:{symbol} daily bars {have}/{n}", None

    def _recent_sessions(self, n: int) -> list[date] | None:
        """The most recent ``n`` completed trading sessions strictly before today, DESCENDING
        (``[0]`` newest). None if the loaded calendars cannot supply ``n`` sessions (bounded walk)."""
        return recent_sessions(self._calendar, self._clock.today(), n)

    def daily_window(self) -> list[date] | None:
        """The gate's own daily coverage window (see :func:`recent_sessions`) — callers that repair
        daily coverage fetch exactly these sessions, so the repair and the check can never disagree."""
        return self._recent_sessions(self._daily_n)
