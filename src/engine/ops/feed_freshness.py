"""Daily census of every ingested feed's newest data (job ``feed_freshness``, 21:30).

Each feed's own job alerts when a fetch FAILS. What none of them catch is a fetch that succeeds and
delivers nothing new — NSE ``corporates-pit`` (empty after 2026-05-02, when NSE moved insider
filings to a new route) and the results listing (no period after Dec-2024) both ran "green" for
months that way. This census compares each feed's
newest delivered data (:meth:`MarketStore.feed_newest`) with the lag its own history shows it can
have, and alerts the owner when a feed goes past it — once per stale streak, so a feed that stays
dead does not bury the next one in a daily repeat. Not entry-blocking. The results feed carries its
own alarm (``filings_results``) and is not repeated here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta

from pydantic import BaseModel, ConfigDict

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.ops.feed_freshness")

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

#: Allowed lag per feed as ``(limit, unit)``, sized from each feed's own 2026-06..09 history.
#: "trading" counts NSE sessions after the newest date (weekends/holidays never count); the EOD
#: feeds therefore alert the evening after a whole missed session.
MAX_LAG: dict[str, tuple[int, str]] = {
    "bhavcopy": (1, "trading"),
    "deals": (2, "trading"),
    "corp_actions": (2, "trading"),
    "earnings_calendar": (2, "trading"),
    "insider_bse": (5, "trading"),          # sparse: days without a universe filing are normal
    "insider_nse": (5, "trading"),          # the same constituent filings as BSE (PIT V2.0 route)
    "shareholding": (130, "calendar"),      # newest quarter end; filings due 21 days after it
    "news": (2, "calendar"),
    "sector_map": (10, "calendar"),         # weekly rebuild
    "isin_map": (2, "trading"),
}


class StaleFeed(BaseModel):
    model_config = ConfigDict(frozen=True)

    feed: str
    newest: date | None
    lag: int | None
    limit: int
    unit: str


class FeedFreshnessResult(BaseModel):
    """``ok`` is always True: a stale feed is reported, and re-running the census changes nothing."""

    model_config = ConfigDict(frozen=True)

    ok: bool = True
    stale: tuple[StaleFeed, ...] = ()


def feed_lag(newest: date, today: date, unit: str, calendar: NSECalendar) -> int:
    """Days after ``newest`` up to and including ``today`` — NSE sessions for ``"trading"``."""
    if unit != "trading":
        return (today - newest).days
    return sum(
        calendar.is_trading_day(newest + timedelta(days=i))
        for i in range(1, (today - newest).days + 1)
    )


class FeedFreshnessJob:
    def __init__(
        self, store: MarketStore, clock: Clock, calendar: NSECalendar, *, notify: NotifySink | None = None
    ) -> None:
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._notify = notify
        #: Feeds already reported in their current stale streak; a recovered feed re-arms.
        self._alerted: set[str] = set()

    async def run(self) -> FeedFreshnessResult:
        today = self._clock.today()
        newest = await self._store.arun(self._store.feed_newest)
        stale: list[StaleFeed] = []
        for feed, (limit, unit) in MAX_LAG.items():
            value = newest.get(feed)
            day = value.astimezone(IST).date() if isinstance(value, datetime) else value
            lag = None if day is None else feed_lag(day, today, unit, self._calendar)
            if lag is None or lag > limit:
                stale.append(StaleFeed(feed=feed, newest=day, lag=lag, limit=limit, unit=unit))
        _log.info("feed_freshness", stale=[s.feed for s in stale],
                  newest={f: str(v) if v is not None else None for f, v in newest.items()})
        stale_now = {s.feed for s in stale}
        self._alerted &= stale_now
        if stale_now - self._alerted:
            await self._alert(stale)
            self._alerted |= stale_now
        return FeedFreshnessResult(stale=tuple(stale))

    async def _alert(self, stale: list[StaleFeed]) -> None:
        if self._notify is None:
            return
        lines = [
            f"{s.feed}: never delivered" if s.newest is None
            else f"{s.feed}: newest {s.newest.isoformat()} — {s.lag} {s.unit} days behind (limit {s.limit})"
            for s in stale
        ]
        msg = CatalogMessage(
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Data feeds stale",
            body=(
                "These feeds have delivered nothing new within their usual lag (a feed whose fetch "
                "fails also alerts from its own job):\n"
                + "\n".join(lines)
                + "\nNot entry-blocking; the data the platform holds for them is getting old."
            ),
            severity="warning",
            data={"job_id": "feed_freshness", "stale": [s.model_dump(mode="json") for s in stale]},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("feed_freshness_notify_failed")
