"""§2.8.2 ``FilingsEventBuilder.insider_net_buy`` — PURE event derivation over synthetic
``insider_trades``-shaped rows. Covers: the trailing-window crossing (delegated verbatim to the
stage-2-validated ``event_study.insider_cluster_events``) + emitted metadata (trailing_value /
contributing count / dominant person-category / point-in-time broadcast_dt); the open-market predicate
excluding ESOP; the below-threshold empty case; cross-source dedup preferring NSE; and ``row_source``
id-prefix inference. No store, no network — every input is a hand-built dict.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from engine.core.clock import IST
from engine.datafeeds.filings_events import (
    INSIDER_TRAILING_SESSIONS,
    FilingsEventBuilder,
    dedup_cross_source,
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


# =========================================================================== cross-source dedup
def _nse(symbol: str, txn_from: date, qty: int) -> dict:
    return {
        "id": "a" * 64, "symbol": symbol, "txn_type": "Buy", "acq_mode": "Market Purchase",
        "qty": qty, "value": Decimal("500000"), "txn_from": txn_from,
        "broadcast_dt": datetime(txn_from.year, txn_from.month, txn_from.day, 20, 0, tzinfo=IST),
    }


def test_row_source_from_id_prefix_and_explicit():
    assert row_source({"id": "bse:xyz"}) == "bse"
    assert row_source({"id": "a" * 64}) == "nse"
    assert row_source({"id": "bse:xyz", "source": "nse"}) == "nse"   # explicit key wins


def test_dedup_drops_bse_row_superseded_by_nse():
    d = SESSIONS[1]
    bse = _buy(1, 500_000)                 # id 'bse:...', qty 1001, txn_from SESSIONS[1]
    nse = _nse("AAA", d, bse["qty"])       # same (symbol, txn_from, qty) from NSE
    out = dedup_cross_source([bse, nse])
    assert len(out) == 1 and row_source(out[0]) == "nse"


def test_dedup_keeps_bse_row_without_nse_counterpart():
    bse = _buy(1, 500_000)
    nse_other = _nse("AAA", SESSIONS[4], 9999)      # different key -> does not supersede
    out = dedup_cross_source([bse, nse_other])
    assert bse in out and nse_other in out and len(out) == 2


def test_insider_net_buy_dedups_before_clustering():
    # The SAME disclosure from both feeds must count ONCE. bse + nse rows share (symbol, txn_from, qty);
    # a distinct second buy pushes the (deduped) trailing sum over the floor exactly once.
    d2 = SESSIONS[2]
    bse_dup = _buy(2, 600_000)
    nse_dup = _nse("AAA", d2, bse_dup["qty"])
    nse_dup["value"] = Decimal("600000")
    second = _buy(3, 600_000)
    events = insider_net_buy([bse_dup, nse_dup, second], SESSIONS, min_value_inr=THRESHOLD)
    # Without dedup the two copies would double-count to 1.8M; with dedup the trailing sum is 1.2M,
    # still one crossing but the contributing count reflects the deduped set.
    assert len(events) == 1
    assert events[0]["contributing_filings_n"] == 2
