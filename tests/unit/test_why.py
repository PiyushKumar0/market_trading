"""``/why SYMBOL`` (plan Q1.10): every section present and absent, failure isolation, the length cap,
and that the report writes nothing."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops import why as why_mod
from engine.ops.why import SECTION_CAP, make_why_fn

TODAY = date(2026, 6, 17)
FILED = datetime(2026, 6, 10, 18, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _slow_box_tolerant_timeout(monkeypatch):
    monkeypatch.setattr(why_mod, "READ_TIMEOUT_S", 60.0)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    s.open()
    yield s
    s.close()


def _why(store, conn, clock):
    return make_why_fn(
        store=store, conn=conn, clock=clock,
        calendar=NSECalendar(config_dir() / "calendar", clock, strict=False),
    )


def _bars(sym: str, n: int = 200, *, close: str = "90") -> list[DailyBar]:
    """n sessions ending yesterday: every high 100, every close 90 (so 90% of the 52w high)."""
    return [
        DailyBar(symbol=sym, d=TODAY - timedelta(days=i), open=Decimal(close), high=Decimal("100"),
                 low=Decimal("80"), close=Decimal(close), volume=1000)
        for i in range(1, n + 1)
    ]


def _seed_duckdb(store: MarketStore, sym: str = "TCS") -> None:
    store.upsert_universe_daily([{
        "d": TODAY, "symbol": sym, "included": False, "mis_candidate": False,
        "exclusion_reasons": ["surveillance_asm", "watchlist_cap"], "median_traded_value": None,
    }])
    store.upsert_instruments_daily(
        [{"d": TODAY, "instrument_token": 1, "tradingsymbol": sym, "surveillance": "ASM-1"}]
    )
    store.upsert_earnings_calendar([{"symbol": sym, "event_date": TODAY + timedelta(days=3)}])
    store.upsert_bars_1d(_bars(sym))
    store.upsert_insider_trades([
        {"id": "a", "symbol": sym, "txn_type": "Buy", "person_category": "Promoters", "qty": 500,
         "value": Decimal("1000000"), "txn_from": date(2026, 6, 9), "txn_to": date(2026, 6, 9),
         "broadcast_dt": FILED},
        {"id": "future", "symbol": sym, "txn_type": "Sell", "qty": 1, "value": Decimal("1"),
         "txn_from": date(2026, 6, 20), "txn_to": date(2026, 6, 20),
         "broadcast_dt": datetime(2026, 6, 20, 9, 0, tzinfo=IST)},
    ])
    store.upsert_shp_quarterly([
        {"symbol": sym, "qtr_end": date(2026, 3, 31), "category": "Promoter", "pledged_pct": 12.5,
         "broadcast_dt": FILED},
        {"symbol": sym, "qtr_end": date(2026, 6, 30), "category": "Promoter", "pledged_pct": 99.0,
         "broadcast_dt": datetime(2026, 7, 15, tzinfo=IST)},
    ])


def _seed_sqlite(conn, sym: str = "TCS", reasons: list[str] | None = None) -> None:
    conn.execute(
        "INSERT INTO recommendations (rec_id, payload, delivered_at, human_action, skip_reason) "
        "VALUES ('R1', ?, '2026-06-16T09:30:00+05:30', 'expired', 'price')",
        (json.dumps({"instrument": sym, "kind": "entry"}),),
    )
    conn.execute(
        "INSERT INTO rec_outcomes (rec_id, status, net_pct, updated_at) VALUES ('R1', 'closed', 1.5, 'x')"
    )
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, qty, avg_entry, stop, target, state, origin, "
        "is_paper) VALUES ('P1', ?, 'BUY', 5, '100', '95', '110', 'OPEN', 'recommended', 0)", (sym,)
    )
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, qty, avg_entry, state, origin, is_paper) "
        "VALUES ('P2', ?, 'BUY', 7, '1', 'OPEN', 'platform', 1)", (sym,)
    )
    conn.execute(
        "INSERT INTO proposals VALUES ('PR1', 'a', 'enter', ?, 'd', '2026-06-16T09:31:00+05:30')",
        (json.dumps({"action": "enter", "tradingsymbol": sym}),),
    )
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at) "
        "VALUES ('V1', 'PR1', 'reject', ?, '2026-06-16T09:31:05+05:30')",
        (json.dumps({"reasons": reasons or ["C3 edge below cost", "day cap"]}),),
    )


def _seed_paper_verdict(conn, reasons: list[str]) -> None:
    """Later than the real verdict, so a scope leak in either direction shows."""
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at, is_paper) "
        "VALUES ('PV1', 'PR1', 'approve', ?, '2026-06-16T09:31:07+05:30', 1)",
        (json.dumps({"reasons": reasons}),),
    )


@pytest.mark.asyncio
async def test_every_section_present(store, conn, clock):
    _seed_duckdb(store)
    _seed_sqlite(conn)
    _seed_paper_verdict(conn, ["paper cap ok"])

    out = await _why(store, conn, clock)(" tcs ")

    assert out.splitlines()[0] == "why TCS - 2026-06-17 10:05 IST"
    for expected in (
        "universe: excluded on 2026-06-17: surveillance_asm, watchlist_cap",
        "surveillance: ASM-1 (2026-06-17)",
        "results: next on 2026-06-20",
        "levels (daily close 90.00 on 2026-06-16):",
        "hi52: arms on a close >= 95.00 (+5.6%); 52w high 100.00",
        "brk20: needs a close above 100.00 (+11.1%)",
        "insider (latest first): 2026-06-09 Buy Promoters 500 sh Rs1000000.00 (filed 2026-06-10)",
        "pledge: quarter 2026-03-31 (filed 2026-06-10): Promoter 12.5%",
        "last rec: 2026-06-16 entry - expired - skip reason price - outcome closed net +1.50%",
        "positions: BUY 5 @ 100 stop 95 target 110 (recommended)",
        "last verdict: reject on 2026-06-16 - reasons: C3 edge below cost; day cap",
        "last paper verdict: approve on 2026-06-16 - reasons: paper cap ok",
    ):
        assert expected in out
    assert "Sell" not in out and "99.0" not in out and "BUY 7" not in out


@pytest.mark.asyncio
async def test_every_section_absent_for_unknown_symbol(store, conn, clock):
    _seed_duckdb(store)
    _seed_sqlite(conn)

    out = await _why(store, conn, clock)("NOSUCH")

    for expected in (
        "universe: not listed on 2026-06-17", "surveillance: not in the 2026-06-17 instrument dump",
        "results: none dated in the next 10 days", "levels: no daily bars", "insider: no filings",
        "pledge: no shareholding filings", "last rec: none", "positions: none open",
        "last verdict: none", "last paper verdict: none",
    ):
        assert expected in out


@pytest.mark.asyncio
async def test_empty_stores_and_short_history(store, conn, clock):
    why = _why(store, conn, clock)
    out = await why("TCS")
    assert "universe: no snapshot for" in out and "surveillance: no instrument snapshot" in out

    store.upsert_bars_1d(_bars("TCS", 30))
    out = await why("TCS")
    assert "hi52: 30 sessions of history, needs 126" in out
    assert "brk20: needs a close above" in out


@pytest.mark.asyncio
async def test_inside_the_hi52_band(store, conn, clock):
    store.upsert_bars_1d(_bars("TCS", close="98"))
    assert "hi52: already inside the band (98% of the 52w high)" in await _why(store, conn, clock)("TCS")


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["", "A B", "x" * 21, "TCS;DROP", "TCS'--"])
async def test_bad_symbol_gets_usage(store, conn, clock, raw):
    assert await _why(store, conn, clock)(raw) == "usage: /why <symbol>"


class _Broken:
    """A store whose reads of the named getters raise or hang; everything else is the real store."""

    def __init__(self, store, *, raises=(), hangs=()):
        self._store, self._raises, self._hangs = store, set(raises), set(hangs)

    def __getattr__(self, name):
        return getattr(self._store, name)

    async def arun(self, fn, /, *args, **kwargs):
        if fn.__name__ in self._raises:
            raise RuntimeError("duckdb down")
        if fn.__name__ in self._hangs:
            await asyncio.sleep(30)
        return await self._store.arun(fn, *args, **kwargs)


@pytest.mark.asyncio
async def test_failed_or_hung_read_leaves_the_other_sections(store, conn, clock, monkeypatch):
    monkeypatch.setattr(why_mod, "READ_TIMEOUT_S", 0.05)
    _seed_duckdb(store)
    _seed_sqlite(conn)
    broken = _Broken(store, raises={"get_universe_daily"}, hangs={"get_bars_1d_frame"})

    out = await _why(broken, conn, clock)("TCS")

    assert "universe: unavailable" in out and "levels: unavailable" in out
    assert "surveillance: ASM-1" in out and "last verdict: reject" in out


@pytest.mark.asyncio
async def test_reply_stays_under_telegram_cap(store, conn, clock):
    _seed_duckdb(store)
    store.upsert_universe_daily([{
        "d": TODAY, "symbol": "TCS", "included": False, "mis_candidate": False,
        "exclusion_reasons": [f"reason_{i:03d}_" + "x" * 30 for i in range(40)],
        "median_traded_value": None,
    }])
    _seed_sqlite(conn, reasons=["long reason " * 40] * 5)
    _seed_paper_verdict(conn, ["long reason " * 40] * 5)

    out = await _why(store, conn, clock)("TCS")

    assert len(out) < 4096
    assert "...\n" in out
    assert all(len(line) <= SECTION_CAP for line in out.splitlines() if line.startswith("universe"))


@pytest.mark.asyncio
async def test_report_writes_nothing(store, conn, clock):
    _seed_duckdb(store)
    _seed_sqlite(conn)
    changes = conn.total_changes

    await _why(store, conn, clock)("TCS")

    assert conn.total_changes == changes
