"""§2.8.2 ``FilingsEventBuilder.insider_net_buy`` — PURE event derivation over synthetic
``insider_trades``-shaped rows. Covers: the trailing-window crossing (delegated verbatim to the
stage-2-validated ``event_study.insider_cluster_events``) + emitted metadata (trailing_value /
contributing count / dominant person-category / point-in-time broadcast_dt); the open-market predicate
excluding ESOP; the below-threshold empty case; equal trades of different people all counting (the
store pairs cross-exchange duplicates); and ``row_source`` id-prefix inference. No store, no network —
every input is a hand-built dict.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from engine.core.clock import IST
from engine.datafeeds.filings_events import (
    INSIDER_TRAILING_SESSIONS,
    FilingsEventBuilder,
    insider_net_buy,
    row_source,
)

SESSIONS = [date(2026, 1, 5) + timedelta(days=i) for i in range(15)]
THRESHOLD = 1_000_000                       # ₹10L abs floor for the test


def _buy(session_i: int, value: int, *, hh: int = 10, category: str = "Promoter",
         mode: str = "Market Purchase", symbol: str = "AAA") -> dict:
    d = SESSIONS[session_i]
    return {
        "id": f"bse:row{session_i}_{value}", "symbol": symbol, "txn_type": "Buy", "acq_mode": mode,
        "qty": 1000 + session_i, "value": Decimal(value), "person_category": category,
        "txn_from": d, "broadcast_dt": datetime(d.year, d.month, d.day, hh, 0, tzinfo=IST),
    }


# =========================================================================== insider_net_buy
def test_insider_net_buy_single_crossing_with_metadata():
    # Two ₹6L open-market buys on sessions 2 and 3 -> trailing sum 1.2M crosses the ₹10L floor at 3.
    filings = [_buy(2, 600_000), _buy(3, 600_000)]
    events = insider_net_buy(filings, SESSIONS, min_value_inr=THRESHOLD)
    assert len(events) == 1
    ev = events[0]
    assert ev["symbol"] == "AAA"
    assert ev["event_session"] == SESSIONS[3]
    assert ev["broadcast_dt"] == datetime(SESSIONS[3].year, SESSIONS[3].month, SESSIONS[3].day, 10, 0, tzinfo=IST)
    assert ev["trailing_value"] == Decimal("1200000")
    assert ev["contributing_filings_n"] == 2
    assert ev["person_category_dominant"] == "Promoter"


def test_insider_net_buy_excludes_esop_and_below_threshold():
    # An ESOP "Buy" is NOT open-market (§2.8.2 taxonomy) -> excluded; the two ₹4L market buys sum to
    # ₹8L, under the ₹10L floor -> no event.
    filings = [
        _buy(2, 400_000), _buy(3, 400_000),
        _buy(3, 9_000_000, mode="ESOP"),           # excluded despite being huge
    ]
    assert insider_net_buy(filings, SESSIONS, min_value_inr=THRESHOLD) == []


def test_insider_net_buy_dominant_category_is_the_mode():
    filings = [
        _buy(2, 600_000, category="Promoter"),
        _buy(3, 600_000, category="Promoter"),
        _buy(3, 600_000, category="Director"),
    ]
    ev = insider_net_buy(filings, SESSIONS, min_value_inr=THRESHOLD)[0]
    assert ev["contributing_filings_n"] == 3
    assert ev["person_category_dominant"] == "Promoter"     # 2 Promoter vs 1 Director


def test_builder_class_delegates():
    builder = FilingsEventBuilder(insider_min_value_inr=THRESHOLD)
    filings = [_buy(2, 600_000), _buy(3, 600_000)]
    assert builder.insider_net_buy(filings, SESSIONS) == insider_net_buy(
        filings, SESSIONS, min_value_inr=THRESHOLD
    )


def test_trailing_window_is_ten_sessions():
    assert INSIDER_TRAILING_SESSIONS == 10


# =========================================================================== row source
def test_row_source_from_id_prefix_and_explicit():
    assert row_source({"id": "bse:xyz"}) == "bse"
    assert row_source({"id": "a" * 64}) == "nse"
    assert row_source({"id": "bse:xyz", "source": "nse"}) == "nse"   # explicit key wins


def test_equal_trades_of_different_people_all_count():
    """Two promoters buying the same qty on the same day are two trades. The old (symbol, txn_from,
    qty) cross-source rule could drop one of them; duplicates are now paired in the store."""
    a, b = _buy(2, 600_000), _buy(2, 600_000)
    a["person_name"] = "Promoter One"                    # BSE row
    b.update(id="b" * 64, person_name="Promoter Two")    # NSE row, same symbol/date/qty
    events = insider_net_buy([a, b], SESSIONS, min_value_inr=THRESHOLD)
    assert len(events) == 1 and events[0]["trailing_value"] == Decimal("1200000")
