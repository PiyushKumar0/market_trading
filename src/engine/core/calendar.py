"""NSE trading calendar (R6).

Loads ``config/calendar/<year>.yaml`` files and answers trading-day / session / next-trading-day
questions, plus the owner-set ``trade_window`` (§7.1) clamped to the day's session. Consumed by the
scheduler, lifecycle, square-off timing, and the budget governor — without it ~15 days/year misbehave
and the LLM loop burns credit on closed markets (R6).

FAIL-SAFE (R6): "no calendar, no trading." In ``strict`` mode (prod), a date is a trading day only if
its year's calendar is loaded, the calendar is ``verified: true``, and the date is at/before
``verified_through``. ``ex_dates`` returns ``[]`` until the corp-actions feed lands (Phase 1).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path

import yaml

from engine.core.clock import Clock
from engine.core.types import CorpAction, Session, TradeWindow

_WEEKEND = {5, 6}  # Saturday, Sunday


def _parse_time(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


class _YearCalendar:
    """Parsed single-year calendar file."""

    def __init__(self, data: dict) -> None:
        self.year: int = int(data["year"])
        self.verified: bool = bool(data.get("verified", False))
        vt = data.get("verified_through")
        self.verified_through: date | None = date.fromisoformat(vt) if vt else None
        sessions = data.get("sessions", {})
        self.pre_open = self._span(sessions.get("pre_open"), default=("09:00", "09:15"))
        self.continuous = self._span(sessions.get("continuous"), default=("09:15", "15:30"))
        self.post_close = self._span(sessions.get("post_close"), default=None)
        self.holidays: dict[date, str] = {
            date.fromisoformat(h["date"]): h.get("name", "") for h in data.get("holidays", [])
        }
        self.muhurat: dict[date, dict] = {
            date.fromisoformat(s["date"]): s for s in data.get("special_sessions", [])
        }
        self.shortened: dict[date, dict] = {
            date.fromisoformat(s["date"]): s for s in data.get("shortened_sessions", [])
        }
        # Convention (R6): a muhurat day is closed for regular daytime trading and modelled ONLY in
        # special_sessions (session() returns its evening times); it must never ALSO appear in
        # ``holidays`` (a full-day closure), because is_trading_day checks holidays first — the overlap
        # would silently swallow the muhurat session. Fail loud at load rather than lose it at runtime.
        overlap = set(self.holidays) & set(self.muhurat)
        if overlap:
            dates = ", ".join(sorted(d.isoformat() for d in overlap))
            raise ValueError(
                f"calendar {self.year}: {dates} listed in BOTH holidays and special_sessions — a muhurat "
                "day belongs only in special_sessions (never holidays), else its session is lost (R6)."
            )

    @staticmethod
    def _span(raw: dict | None, default: tuple[str, str] | None) -> tuple[time, time] | None:
        if raw:
            return _parse_time(raw["start"]), _parse_time(raw["end"])
        if default:
            return _parse_time(default[0]), _parse_time(default[1])
        return None


class NSECalendar:
    """The NSE trading calendar (R6)."""

    def __init__(
        self,
        calendar_dir: str | Path,
        clock: Clock,
        *,
        strict: bool = False,
        sqlite_conn: sqlite3.Connection | None = None,
        window_seed: TradeWindow | None = None,
    ) -> None:
        self._dir = Path(calendar_dir)
        self._clock = clock
        self._strict = strict
        self._conn = sqlite_conn
        self._window_seed = window_seed or TradeWindow(start=time(10, 0), end=time(10, 30))
        self._years: dict[int, _YearCalendar] = {}
        self._load()

    def _load(self) -> None:
        if not self._dir.exists():
            return
        for path in sorted(self._dir.glob("*.yaml")):
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if "year" in data:
                cal = _YearCalendar(data)
                self._years[cal.year] = cal

    # ----------------------------------------------------------------- trading day
    def is_trading_day(self, d: date) -> bool:
        cal = self._years.get(d.year)
        if cal is None:
            return False  # no calendar, no trading (R6)
        if d.weekday() in _WEEKEND:
            return d in cal.muhurat  # muhurat sessions can fall on a weekend
        if d in cal.holidays:
            return False
        if self._strict:
            if not cal.verified:
                return False
            if cal.verified_through is not None and d > cal.verified_through:
                return False
        return True

    def session(self, d: date) -> Session | None:
        """Session times for ``d`` (muhurat/shortened-aware), or ``None`` if not a trading day."""
        if not self.is_trading_day(d):
            return None
        cal = self._years[d.year]
        pre = cal.pre_open or (time(9, 0), time(9, 15))
        cont = cal.continuous or (time(9, 15), time(15, 30))
        is_muhurat = d in cal.muhurat
        is_shortened = d in cal.shortened
        if is_muhurat:
            spec = cal.muhurat[d]
            open_t, close_t = _parse_time(spec["start"]), _parse_time(spec["end"])
        elif is_shortened:
            spec = cal.shortened[d]
            open_t, close_t = _parse_time(spec["start"]), _parse_time(spec["end"])
        else:
            open_t, close_t = cont
        c = self._clock
        return Session(
            date_ist=d.isoformat(),
            pre_open_start=c.combine(d, pre[0]),
            pre_open_end=c.combine(d, pre[1]),
            open=c.combine(d, open_t),
            close=c.combine(d, close_t),
            post_close_start=c.combine(d, cal.post_close[0]) if cal.post_close else None,
            post_close_end=c.combine(d, cal.post_close[1]) if cal.post_close else None,
            is_muhurat=is_muhurat,
            is_shortened=is_shortened,
        )

    def next_trading_day(self, d: date) -> date:
        probe = d + timedelta(days=1)
        for _ in range(370):  # bounded: never loop forever past the verified horizon
            if self.is_trading_day(probe):
                return probe
            probe += timedelta(days=1)
        raise ValueError(f"no trading day found within ~1y after {d} (calendar horizon, R6)")

    def verified_horizon(self) -> date | None:
        """The furthest date the loaded calendars are verified through (None if none verified)."""
        horizons = [c.verified_through for c in self._years.values() if c.verified and c.verified_through]
        return max(horizons) if horizons else None

    # ----------------------------------------------------------------- ex-dates (Phase 1 feed)
    def ex_dates(self, symbol: str, within_days: int) -> list[CorpAction]:
        """Corporate actions for ``symbol`` within ``within_days`` (A12). Empty until the feed lands."""
        return []

    # ----------------------------------------------------------------- trade window (§7.1/§3.2.7)
    def _sticky_window(self) -> TradeWindow:
        """Read the sticky owner-set window from SQLite ``trade_window_state``; else the seed.

        The table ships with migrations v1 (Phase 0); the runtime setter is Phase 2 (§3.2.7). Until a
        row exists, the settings.yaml seed governs.
        """
        if self._conn is not None:
            try:
                row = self._conn.execute(
                    "SELECT start_ist, end_ist, squareoff_buffer_min FROM trade_window_state WHERE id=1"
                ).fetchone()
            except sqlite3.OperationalError:
                row = None
            if row is not None and row["start_ist"] and row["end_ist"]:
                return TradeWindow(
                    start=_parse_time(row["start_ist"]),
                    end=_parse_time(row["end_ist"]),
                    squareoff_buffer_min=int(row["squareoff_buffer_min"] or 0),
                )
        return self._window_seed

    def trade_window(self, d: date) -> tuple[datetime, datetime]:
        """The current sticky trade window for ``d``, clamped to the day's session (§7.1).

        Single source the gate + scheduler read. Raises if ``d`` is not a trading day (callers gate on
        ``is_trading_day`` first).
        """
        session = self.session(d)
        if session is None:
            raise ValueError(f"{d} is not a trading day; no trade window")
        w = self._sticky_window()
        start = self._clock.combine(d, w.start)
        end = self._clock.combine(d, w.end)
        # Clamp to the continuous session (shortened/muhurat-aware).
        start = max(start, session.open)
        end = min(end, session.close)
        return start, end
