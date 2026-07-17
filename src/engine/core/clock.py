"""IST clock + NTP skew check (§3.2.1, R6).

``Clock`` is the SINGLE source of "now". Every date/time value and arithmetic in the platform uses
stdlib ``datetime``/``zoneinfo`` via ``Clock``/``NSECalendar`` — never a bare ``datetime.now()``
scattered in modules, and NEVER the LLM (§3.2 convention). This is for IST correctness AND
deterministic replay: a replay run injects a controlled ``time_source`` so the golden decision log is
byte-identical (§9.1). The only sanctioned non-Clock time source is ``core.log`` (logging must survive
Clock failure).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class ClockSkewUnavailable(RuntimeError):
    """Raised when NTP skew cannot be measured — the caller must treat this conservatively (freeze)."""


class Clock:
    """The single source of "now" (IST, tz-aware).

    Parameters
    ----------
    time_source:
        Optional callable returning the current tz-aware IST datetime. Defaults to
        ``datetime.now(IST)``. Replay/tests inject a controlled source for determinism.
    ntp_servers:
        Servers for :meth:`check_skew` (R6).
    """

    def __init__(
        self,
        time_source: Callable[[], datetime] | None = None,
        ntp_servers: list[str] | None = None,
    ) -> None:
        self._time_source = time_source or (lambda: datetime.now(IST))
        self._ntp_servers = ntp_servers or ["time.windows.com", "pool.ntp.org"]

    def now(self) -> datetime:
        """Current time, tz-aware IST. Always."""
        ts = self._time_source()
        if ts.tzinfo is None:
            raise ValueError("Clock time_source returned a naive datetime; IST tz-aware required")
        return ts.astimezone(IST)

    def today(self) -> date:
        return self.now().date()

    def combine(self, d: date, t: time) -> datetime:
        """Build a tz-aware IST datetime from a date + time (the only sanctioned way to do so)."""
        return datetime.combine(d, t, tzinfo=IST)

    async def check_skew(self) -> timedelta:
        """Return clock skew vs NTP (R6). >2 s ⇒ caller publishes a risk event / freezes entries.

        Raises :class:`ClockSkewUnavailable` if no server can be reached — the gate then treats the
        inability to verify time conservatively (refuse new entries), never as "skew is fine".
        """
        try:
            import ntplib  # imported lazily; optional at the pure-Python test tier
        except ImportError as exc:  # pragma: no cover - exercised only without ntplib installed
            raise ClockSkewUnavailable("ntplib not installed") from exc

        loop = asyncio.get_running_loop()
        last_exc: Exception | None = None
        for server in self._ntp_servers:
            try:
                client = ntplib.NTPClient()
                response = await loop.run_in_executor(
                    None, lambda s=server, c=client: c.request(s, version=3, timeout=3)
                )
                return timedelta(seconds=abs(response.offset))
            except Exception as exc:  # noqa: BLE001 - try the next server
                last_exc = exc
                continue
        raise ClockSkewUnavailable(
            f"no NTP server reachable: {self._ntp_servers}"
        ) from last_exc
