"""Deterministic synthetic tick day written in the PRODUCTION Parquet layout.

Generated, not committed: the golden replay day must be byte-reproducible from source and must not
carry real recorded market data. Everything is a closed-form integer recurrence (no RNG, no clock
read), so two calls produce identical bytes-in, identical bars-out.

Layout mirrors :meth:`engine.marketdata.store.MarketStore._tick_partition_dir` —
``<root>/ticks/date=YYYY-MM-DD/symbol=<SYM>/<name>.parquet`` — and the columns/types mirror
``store._TICK_STAGE_DDL`` / ``store._TICK_COLUMNS`` (the golden test asserts the lockstep). Written
through ``duckdb COPY`` because pyarrow is not installed.

Shape of the day:

* **pre-open 09:00:00–09:14:xx** — builds no bar; the LAST pre-open print is the auction open
  stamped on the 09:15 row.
* **session 09:15:00–15:29:xx**, 6 prints per minute, with **one whole minute missing** (the gap
  minute) — a replay must not invent a bar for it.
* **one post-close print strictly after 15:30** — no bar, but counted.

Each symbol's ticks are split across TWO Parquet files per partition so the harness's single
``read_parquet`` glob is exercised against a multi-fragment partition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb

from engine.core.clock import IST

#: The three synthetic symbols, deliberately not real tradingsymbols.
SYMBOLS: tuple[str, ...] = ("SYNA", "SYNB", "SYNC")

#: Session boundaries the fixture is built against (the BarBuilder defaults).
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)

#: Pre-open prints per symbol, 45 s apart from 09:00:00 (the last one is the auction open).
PRE_OPEN_TICKS = 20
PRE_OPEN_STEP_S = 45

#: Seconds-within-minute of each session minute's prints. Six per minute puts the day over the 6,000
#: ticks the warm perf smoke asserts against.
SECONDS_IN_MINUTE: tuple[int, ...] = (1, 11, 21, 31, 41, 51)

#: The one session minute with NO prints (a real feed drops minutes; no bar may be fabricated).
GAP_MINUTE = time(11, 30)

#: The single post-close print (strictly after ``SESSION_CLOSE`` ⇒ no bar, counted instead).
POST_CLOSE_AT = time(15, 30, 30)

#: Column order pinned to ``store._TICK_COLUMNS``; the golden test asserts the two are equal.
TICK_COLUMNS: tuple[str, ...] = (
    "instrument_token", "tradingsymbol", "ltp", "volume_traded", "exchange_ts",
    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "avg_price", "bid", "ask",
)

#: Mirrors ``store._TICK_STAGE_DDL``; the widths (DECIMAL(12,2) prices, DECIMAL(14,4) day-VWAP) are
#: load-bearing so the fixture matches data the engine really produces.
_STAGE_DDL = """
    CREATE OR REPLACE TEMP TABLE _fixture_ticks (
        instrument_token BIGINT,
        tradingsymbol    TEXT,
        ltp              DECIMAL(12,2),
        volume_traded    BIGINT,
        exchange_ts      TIMESTAMPTZ,
        ohlc_open        DECIMAL(12,2),
        ohlc_high        DECIMAL(12,2),
        ohlc_low         DECIMAL(12,2),
        ohlc_close       DECIMAL(12,2),
        avg_price        DECIMAL(14,4),
        bid              DECIMAL(12,2),
        ask              DECIMAL(12,2)
    )
"""


@dataclass(frozen=True)
class SyntheticDay:
    """What the golden test asserts against, computed by the SAME code that wrote the files."""

    day: date
    root: Path                                   # the parquet root (files under <root>/ticks/…)
    symbols: tuple[str, ...]
    pre_open_ticks: int                          # total across all symbols
    session_ticks: int
    post_close_ticks: int
    session_minutes: int                         # distinct minutes that carry at least one print
    auction_open: dict[str, Decimal]             # last pre-open ltp per symbol
    first_session_ltp: dict[str, Decimal]        # first in-session print per symbol (the open)
    first_session_cum: dict[str, int]            # its cumulative volume
    open_bar_volume: dict[str, int]              # cumulative at the END of 09:15 == that bar's volume
    gap_minute: datetime                         # the minute with no prints
    post_close_at: datetime

    @property
    def total_ticks(self) -> int:
        return self.pre_open_ticks + self.session_ticks + self.post_close_ticks


def _paise(sym_idx: int, k: int, base: int) -> int:
    """Closed-form deterministic price jitter in paise: a pseudo-walk within ±₹2.00 of ``base``,
    reproducible from ``(sym_idx, k)`` alone."""
    return base + ((sym_idx * 7919 + k * 104729) % 401) - 200


def _dec2(paise: int) -> Decimal:
    """Paise → an exact ``DECIMAL(12,2)``-shaped Decimal. No float ever touches a price."""
    return Decimal(paise).scaleb(-2)


def _rows_for_symbol(
    day: date, sym_idx: int, symbol: str
) -> tuple[list[tuple], Decimal, Decimal, int, int]:
    """Every row for one symbol, in exchange_ts order, plus the pre-open/cumulative-volume landmarks the test asserts."""
    base = 10_000 + sym_idx * 5_000                     # ₹100 / ₹150 / ₹200
    token = 1_000 + sym_idx
    ohlc = (_dec2(base - 50), _dec2(base + 300), _dec2(base - 300), _dec2(base - 10))
    rows: list[tuple] = []
    k = 0
    cum = 0

    # --- pre-open (excluded from bars; the LAST print is the auction open) -------------------
    auction_open = _dec2(base)
    start = datetime.combine(day, time(9, 0), tzinfo=IST)
    for i in range(PRE_OPEN_TICKS):
        ltp = _dec2(_paise(sym_idx, k, base))
        auction_open = ltp
        rows.append(_row(token, symbol, ltp, cum, start + timedelta(seconds=i * PRE_OPEN_STEP_S), ohlc))
        k += 1
        # Pre-open prints carry cumulative volume 0: the auction volume is reported into the
        # session's first cumulative value.

    # --- session (6 prints/minute, one whole minute missing) --------------------------------------
    first_ltp: Decimal | None = None
    first_cum = 0
    open_bar_volume = 0
    minute = datetime.combine(day, SESSION_OPEN, tzinfo=IST)
    close_at = datetime.combine(day, SESSION_CLOSE, tzinfo=IST)
    gap_at = datetime.combine(day, GAP_MINUTE, tzinfo=IST)
    while minute < close_at:
        if minute == gap_at:
            minute += timedelta(minutes=1)
            continue
        for sec in SECONDS_IN_MINUTE:
            ltp = _dec2(_paise(sym_idx, k, base))
            cum += 100 + (k % 7)                       # strictly increasing (cumulative volume)
            if first_ltp is None:
                first_ltp, first_cum = ltp, cum
            rows.append(_row(token, symbol, ltp, cum, minute + timedelta(seconds=sec), ohlc))
            k += 1
        if minute.time() == SESSION_OPEN:
            # Engine up from the open: the cumulative at the end of this minute IS the 09:15 bar's volume.
            open_bar_volume = cum
        minute += timedelta(minutes=1)

    # --- one post-close print (no bar, counted) ---------------------------------------------
    cum += 100
    rows.append(
        _row(token, symbol, _dec2(_paise(sym_idx, k, base)), cum,
             datetime.combine(day, POST_CLOSE_AT, tzinfo=IST), ohlc)
    )
    assert first_ltp is not None
    return rows, auction_open, first_ltp, first_cum, open_bar_volume


def _row(token: int, symbol: str, ltp: Decimal, cum: int, ts: datetime,
         ohlc: tuple[Decimal, Decimal, Decimal, Decimal]) -> tuple:
    """One row in ``TICK_COLUMNS`` order. bid/ask straddle the ltp by one tick (₹0.05)."""
    return (
        token, symbol, ltp, cum, ts,
        ohlc[0], ohlc[1], ohlc[2], ohlc[3],
        (ltp + Decimal("0.0025")).quantize(Decimal("0.0001")),   # avg_price at sub-paise (DECIMAL(14,4))
        ltp - Decimal("0.05"), ltp + Decimal("0.05"),
    )


def write_synthetic_day(root: Path, day: date) -> SyntheticDay:
    """Write the synthetic day under ``root`` in the production layout; return what it contains.

    ``root`` is the *parquet root* (the harness appends ``ticks/date=…/symbol=…`` itself), i.e. the
    same value ``MarketStore(parquet_root=…)`` takes."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='Asia/Kolkata'")
        con.execute("SET enable_progress_bar=false")
        con.execute(_STAGE_DDL)
        placeholders = ", ".join("?" for _ in TICK_COLUMNS)

        auction: dict[str, Decimal] = {}
        first_ltp: dict[str, Decimal] = {}
        first_cum: dict[str, int] = {}
        open_vol: dict[str, int] = {}
        pre_open = session = post_close = 0

        for sym_idx, symbol in enumerate(SYMBOLS):
            (
                rows, auction[symbol], first_ltp[symbol], first_cum[symbol], open_vol[symbol],
            ) = _rows_for_symbol(day, sym_idx, symbol)
            pre_open += PRE_OPEN_TICKS
            post_close += 1
            session += len(rows) - PRE_OPEN_TICKS - 1

            part_dir = root / "ticks" / f"date={day.isoformat()}" / f"symbol={symbol}"
            part_dir.mkdir(parents=True, exist_ok=True)
            con.execute("DELETE FROM _fixture_ticks")
            con.executemany(f"INSERT INTO _fixture_ticks VALUES ({placeholders})", rows)
            # TWO fragments per partition: the harness must glob the whole partition. Split by row
            # number so the fragments do not overlap.
            half = len(rows) // 2
            for name, predicate in (("part-0", f"rn <= {half}"), ("part-1", f"rn > {half}")):
                out = part_dir / f"{name}.parquet"
                con.execute(
                    "COPY (SELECT " + ", ".join(TICK_COLUMNS) + " FROM ("
                    "SELECT *, row_number() OVER (ORDER BY exchange_ts) AS rn FROM _fixture_ticks"
                    f") WHERE {predicate} ORDER BY exchange_ts) TO '{out.as_posix()}' (FORMAT PARQUET)"
                )
    finally:
        con.close()

    open_at = datetime.combine(day, SESSION_OPEN, tzinfo=IST)
    close_at = datetime.combine(day, SESSION_CLOSE, tzinfo=IST)
    minutes = int((close_at - open_at).total_seconds() // 60) - 1      # minus the gap minute
    return SyntheticDay(
        day=day,
        root=root,
        symbols=SYMBOLS,
        pre_open_ticks=pre_open,
        session_ticks=session,
        post_close_ticks=post_close,
        session_minutes=minutes,
        auction_open=auction,
        first_session_ltp=first_ltp,
        first_session_cum=first_cum,
        open_bar_volume=open_vol,
        gap_minute=datetime.combine(day, GAP_MINUTE, tzinfo=IST),
        post_close_at=datetime.combine(day, POST_CLOSE_AT, tzinfo=IST),
    )


# ---------------------------------------------------------------------- the total-order fixture
#: Symbol for :func:`write_tie_day`. One symbol: the tie is WITHIN a symbol-second, which
#: ``(exchange_ts, tradingsymbol)`` alone cannot order.
TIE_SYMBOL = "SYNT"

#: The tied prints: same second, cumulative volume and ltp, differing ONLY in the L1 quote (a
#: depth-only update carries the previous trade's ltp and volume forward). The broker's half-spread,
#: hence its fill price, reads bid/ask, so the order of these rows is behaviour, not cosmetics.
TIE_BIDS: tuple[Decimal, ...] = (Decimal("99.90"), Decimal("99.95"))


def write_tie_day(root: Path, day: date) -> tuple[Path, date, tuple[Decimal, ...]]:
    """Write a one-symbol day whose 09:15 minute has rows tying on ``(exchange_ts, tradingsymbol,
    volume_traded, ltp)``.

    The rows are written in DESCENDING bid order, so a scan preserving file order delivers them
    descending while the total key of :data:`~engine.paper.replay.ORDER_BY_COLUMNS` delivers them
    ascending, which makes the golden assertion falsifiable.

    Returns ``(root, day, expected bids in total-key order)``.
    """
    open_at = datetime.combine(day, SESSION_OPEN, tzinfo=IST)
    ltp = Decimal("100.00")
    cum = 500
    ohlc = (Decimal("99.50"), Decimal("101.00"), Decimal("99.00"), Decimal("100.10"))
    token = 4242

    # Two tied rows at 09:15:01, highest-bid-first, then later prints so the minute closes with a bar.
    rows = [
        _row(token, TIE_SYMBOL, ltp, cum, open_at + timedelta(seconds=1), ohlc)[:10]
        + (bid, bid + Decimal("0.10"))
        for bid in reversed(TIE_BIDS)
    ]
    rows.append(_row(token, TIE_SYMBOL, ltp + Decimal("0.05"), cum + 100,
                     open_at + timedelta(seconds=30), ohlc))
    rows.append(_row(token, TIE_SYMBOL, ltp + Decimal("0.10"), cum + 200,
                     open_at + timedelta(minutes=3), ohlc))

    part_dir = root / "ticks" / f"date={day.isoformat()}" / f"symbol={TIE_SYMBOL}"
    part_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='Asia/Kolkata'")
        con.execute("SET enable_progress_bar=false")
        con.execute(_STAGE_DDL)
        placeholders = ", ".join("?" for _ in TICK_COLUMNS)
        con.executemany(f"INSERT INTO _fixture_ticks VALUES ({placeholders})", rows)
        out = part_dir / "part-0.parquet"
        # No ORDER BY: the insertion order IS the file order.
        con.execute(
            "COPY (SELECT " + ", ".join(TICK_COLUMNS) + " FROM _fixture_ticks) "
            f"TO '{out.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    return root, day, TIE_BIDS
