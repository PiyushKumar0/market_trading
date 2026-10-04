"""PaperBroker v1 fill rules + postback contract (WO-P3-2, 2026-09-10; plan 3.2.9, 8.4 addendum,
9.3 conservatism cases).

Every rule asserted here is a CONSERVATISM rule -- the paper tier exists to produce a number the
live tier can only beat, never a number it has to live up to (R9). The load-bearing cases:

* market orders wait out the 700 ms simulated latency and then pay half-spread + k*sigma;
* limit orders need a TRADE-THROUGH, never a touch, and never fill better than their limit
  (the 9.3 conservatism case);
* SL-M is gap-aware: a session-open print through a stop fills at the PRINT, not at the stop
  (the 9.3 "results-day gap through a stop" case -- the loss is allowed to exceed per-trade risk);
* partials are budgeted at 10% of the MINUTE's volume -- per (order, minute), not per tick -- and
  ``filled_quantity`` is monotone to COMPLETE;
* a cumulative-volume reset (A13) yields ZERO available volume, never an uncapped fill -- and a
  counter that re-baselines and never climbs back blacks the symbol out for the REST of the session;
* resting DAY orders lapse at the session roll (Kite cancels them at the close) while GTTs persist;
* injected rejections come from a SEEDED rng so a replay reproduces them exactly;
* the order guard (3.5.3 order-surface predicate) runs BEFORE any book mutation;
* every state change emits exactly one ``OrderUpdateFrame`` carrying the Kite field names the OMS
  parser reads -- live and paper share ONE parser (plan 8.4 WO-P3-1);
* nothing raises out of ``on_tick``: it is the market-data hot path (plan 3.2). That holds for all
  THREE loops in it -- the session-lapse sweep, the match loop and the GTT loop -- and for the
  publish inside each: a postback that raises is logged and dropped, never allowed to unwind the
  book change it was reporting.

Time is driven by a mutable ``Clock`` time source (the house idiom, tests/conftest.py); ticks carry
their own ``exchange_ts`` so latency/trigger logic is asserted against exchange time, not wall time.
"""

from __future__ import annotations

import datetime as dt
import statistics
from decimal import Decimal

import pytest
from pydantic import BaseModel

from engine.core.clock import IST, Clock
from engine.core.contracts import ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.types import Bar, Tick
from engine.paper.broker import (
    SIGMA_BOOTSTRAP_PCT,
    SIGMA_FLOOR_PCT,
    PaperBroker,
    PaperOrderError,
    SigmaEstimator,
)
from engine.paper.fill_model import FillModelConfig

D = dt.date(2026, 6, 17)          # a real 2026 trading day (matches conftest FIXED_NOW)
D2 = dt.date(2026, 6, 18)         # the NEXT trading session (Thu) -- the session-roll cases
TICK_SIZE = Decimal("0.05")
SYM = "TCS"

#: The Kite postback field names the OMS parser reads (plan 3.5.1). Pinned so a rename here is a
#: test failure, not a silent live/paper parser divergence.
PINNED_KEYS = {
    "order_id",
    "status",
    "filled_quantity",
    "pending_quantity",
    "cancelled_quantity",
    "average_price",
    "status_message",
    "order_timestamp",
    "tradingsymbol",
    "transaction_type",
    "product",
    "order_type",
    "quantity",
    "price",
    "trigger_price",
    "variety",
    "tag",
}


def at(h: int, m: int, s: int = 0, ms: int = 0, day: dt.date = D) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, h, m, s, ms * 1000, tzinfo=IST)


class _Now:
    """Mutable time source injected into Clock -- tests move time explicitly."""

    def __init__(self, start: dt.datetime) -> None:
        self.value = start

    def __call__(self) -> dt.datetime:
        return self.value

    def set(self, value: dt.datetime) -> None:
        self.value = value


class _Bus:
    """Records every (topic, frame) the broker publishes."""

    def __init__(self) -> None:
        self.frames: list[tuple[str, BaseModel]] = []

    def __call__(self, topic: str, frame: BaseModel) -> None:
        self.frames.append((topic, frame))

    def data(self) -> list[dict]:
        return [f.data for _, f in self.frames]

    def for_order(self, order_id: str) -> list[dict]:
        return [d for d in self.data() if d.get("order_id") == order_id]

    def clear(self) -> None:
        self.frames.clear()


@pytest.fixture
def now() -> _Now:
    return _Now(at(10, 0))


@pytest.fixture
def bus() -> _Bus:
    return _Bus()


@pytest.fixture
def broker(now: _Now, bus: _Bus) -> PaperBroker:
    return PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1234,
        rejection_rate=0.0,          # rejections are exercised in their own test
    )


def tick(ts: dt.datetime, ltp: str, *, cum: int = 0, spread: str = "0.05", sym: str = SYM) -> Tick:
    px = Decimal(ltp)
    half = Decimal(spread)
    return Tick(
        instrument_token=1,
        tradingsymbol=sym,
        ltp=px,
        volume_traded=cum,
        exchange_ts=ts,
        bid=px - half,
        ask=px + half,
    )


def order(**kw) -> dict:
    req = {
        "variety": "regular",
        "exchange": "NSE",
        "tradingsymbol": SYM,
        "transaction_type": "BUY",
        "order_type": "MARKET",
        "product": "MIS",
        "quantity": 10,
        "price": None,
        "trigger_price": None,
        "tag": "wo-p3-2",
    }
    req.update(kw)
    return req


# =========================================================================== (a) latency + market
async def test_market_fills_only_after_the_700ms_latency_at_ltp_plus_slippage(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order())
    assert bus.for_order(oid)[0]["status"] == "OPEN"
    bus.clear()

    # 500 ms after placement -- inside the simulated latency, nothing may fill.
    broker.on_tick(tick(at(10, 0, 0, 500), "100.00"))
    assert bus.frames == []

    # 800 ms -- past eligible_at. mid bucket k=0.5; half-spread 0.05; sigma bootstrap 0.2% * 100 = 0.20
    #   => slippage = 0.05 + 0.5*0.20 = 0.15, paid by the BUYer.
    broker.on_tick(tick(at(10, 0, 0, 800), "100.00"))
    frames = bus.for_order(oid)
    assert len(frames) == 1
    done = frames[0]
    assert done["status"] == "COMPLETE"
    assert done["filled_quantity"] == 10
    assert done["pending_quantity"] == 0
    assert done["average_price"] == Decimal("100.15")


async def test_market_sell_receives_less(broker: PaperBroker, bus: _Bus, now: _Now) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(transaction_type="SELL"))
    bus.clear()
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    assert bus.for_order(oid)[-1]["average_price"] == Decimal("99.85")


# =========================================================================== (b) limit trade-through
@pytest.mark.parametrize(
    ("side", "limit", "touch", "through"),
    [
        ("BUY", "100.00", "100.00", "99.95"),
        ("SELL", "100.00", "100.00", "100.05"),
    ],
)
async def test_limit_needs_trade_through_and_never_fills_better_than_its_price(
    broker: PaperBroker, bus: _Bus, now: _Now, side: str, limit: str, touch: str, through: str
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(
        order(transaction_type=side, order_type="LIMIT", price=Decimal(limit))
    )
    bus.clear()

    # A tick exactly AT the limit is a touch, not a trade-through: no fill (9.3 conservatism).
    broker.on_tick(tick(at(10, 0, 1), touch))
    assert bus.frames == []

    broker.on_tick(tick(at(10, 0, 2), through))
    filled = bus.for_order(oid)[-1]
    assert filled["status"] == "COMPLETE"
    # The fill price is the LIMIT, never the (better) traded price -- a limit never fills worse than
    # its price, and paper never credits it with better.
    assert filled["average_price"] == Decimal(limit)


async def test_limit_respects_the_latency_too(broker: PaperBroker, bus: _Bus, now: _Now) -> None:
    now.set(at(10, 0, 0))
    await broker.place_order(order(order_type="LIMIT", price=Decimal("100.00")))
    bus.clear()
    broker.on_tick(tick(at(10, 0, 0, 500), "99.00"))     # deep through, but inside the latency
    assert bus.frames == []
    broker.on_tick(tick(at(10, 0, 0, 800), "99.00"))
    assert bus.data()[-1]["status"] == "COMPLETE"


# =========================================================================== (c) SL-M gap awareness
async def test_slm_sell_gap_through_the_stop_fills_at_the_open_print_not_the_stop(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """The 9.3 results-day case: a swing stop at 95 and the session opens at 88."""
    now.set(at(9, 14, 0))
    oid = await broker.place_order(
        order(order_type="SL-M", transaction_type="SELL", trigger_price=Decimal("95.00"))
    )
    bus.clear()

    broker.on_tick(tick(at(9, 15, 0), "88.00"))          # first tick of the session for this symbol
    filled = bus.for_order(oid)[-1]
    assert filled["status"] == "COMPLETE"
    # open bucket k=1.0; half-spread 0.05; sigma = 0.2% * 88 = 0.176 -> 88.00 - 0.226 = 87.774,
    # rounded DOWN to the 0.05 tick (conservative for a seller) = 87.75.
    assert filled["average_price"] == Decimal("87.75")
    assert filled["average_price"] < Decimal("95.00")    # the loss EXCEEDS the per-trade stop risk
    assert "gap" in filled["status_message"]


async def test_slm_sell_triggers_at_or_below_the_stop_without_a_gap(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(9, 14, 0))
    oid = await broker.place_order(
        order(order_type="SL-M", transaction_type="SELL", trigger_price=Decimal("95.00"))
    )
    bus.clear()
    broker.on_tick(tick(at(9, 20, 0), "100.00"))         # first tick, well above the stop: no trigger
    assert bus.frames == []
    broker.on_tick(tick(at(9, 20, 30), "95.50"))         # still above
    assert bus.frames == []
    broker.on_tick(tick(at(9, 21, 0), "94.90"))          # crosses
    filled = bus.for_order(oid)[-1]
    assert filled["status"] == "COMPLETE"
    # k=1.0, hs 0.05, sigma = 0.2% * 94.90 = 0.18980 -> 94.90 - 0.23980 = 94.66020 -> floor tick 94.65
    assert filled["average_price"] == Decimal("94.65")
    assert "gap" not in filled["status_message"]


async def test_slm_buy_triggers_at_or_above_the_stop(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(
        order(order_type="SL-M", transaction_type="BUY", trigger_price=Decimal("105.00"))
    )
    bus.clear()
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    broker.on_tick(tick(at(10, 0, 2), "104.95"))
    assert bus.frames == []
    broker.on_tick(tick(at(10, 0, 3), "105.00"))         # at the trigger: a stop triggers on touch
    assert bus.for_order(oid)[-1]["status"] == "COMPLETE"


async def test_harness_flagged_gap_marks_the_next_trigger_as_a_gap_fill(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(
        order(order_type="SL-M", transaction_type="SELL", trigger_price=Decimal("95.00"))
    )
    broker.on_tick(tick(at(10, 0, 1), "100.00"))         # consume the session-first-tick flag
    bus.clear()
    broker.flag_gap(SYM)                                 # the replay harness saw a data gap
    broker.on_tick(tick(at(10, 5, 0), "88.00"))
    filled = bus.for_order(oid)[-1]
    assert filled["status"] == "COMPLETE"
    assert "gap" in filled["status_message"]


# =========================================================================== (d) partial fills
async def test_partials_capped_by_participation_then_complete(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    # A closed 1-minute bar of 300 shares -> participation cap 10% = 30 per tick.
    broker.on_bar(
        Bar(
            symbol=SYM,
            ts_minute=at(10, 0),
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume=300,
        )
    )
    bus.clear()

    # One tick per MINUTE: each minute grants a fresh 10%-of-300 budget (the minute itself prints
    # no volume -- cum is flat -- so the last closed bar backs the cap).
    for i in range(4):
        broker.on_tick(tick(at(10, 1 + i, 0), "100.00", cum=5000))

    frames = bus.for_order(oid)
    assert [f["filled_quantity"] for f in frames] == [30, 60, 90, 100]
    assert [f["pending_quantity"] for f in frames] == [70, 40, 10, 0]
    assert [f["status"] for f in frames] == ["OPEN", "OPEN", "OPEN", "COMPLETE"]
    # filled_quantity is monotone non-decreasing across the whole life of the order.
    filled = [f["filled_quantity"] for f in frames]
    assert all(a <= b for a, b in zip(filled, filled[1:], strict=False))
    assert frames[-1]["average_price"] == Decimal("100.15")


async def test_participation_is_budgeted_per_minute_not_per_tick(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """The cap is a share of the MINUTE's tape, so it must be spent from a per-(order, minute)
    budget. Applied per TICK it is unbounded per minute: the fix-round probe measured a single
    order taking 101-130% of a minute's traded volume just by seeing more ticks (plan 3.2.9).
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=300))
    bus.clear()

    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=1000))          # minute baseline
    for i, cum in enumerate((1200, 1400, 1600, 1800, 2000), start=1):
        broker.on_tick(tick(at(10, 1, i), "100.00", cum=cum))

    # The minute's delta is 2000-1000 = 1000 shares. 10% of it is the ENTIRE budget for the minute,
    # however many ticks arrived inside it.
    assert bus.for_order(oid)[-1]["filled_quantity"] == 100

    # The next minute grants a fresh budget: delta 500 -> 50 more.
    broker.on_tick(tick(at(10, 2, 0), "100.00", cum=2000))
    broker.on_tick(tick(at(10, 2, 1), "100.00", cum=2500))
    assert bus.for_order(oid)[-1]["filled_quantity"] == 150


async def test_a_cumulative_volume_reset_fills_nothing(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """A13: ``volume_traded`` is CUMULATIVE, and the feed occasionally restates it downward (a
    resubscribe re-baselines it to 0). That must not empty the cap -- before the fix the reset tick
    read as "this feed has no volume at all" and the order filled in full, uncapped.
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    bus.clear()

    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=1000))          # minute baseline
    broker.on_tick(tick(at(10, 1, 1), "100.00", cum=1300))          # delta 300 -> 30
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30

    broker.on_tick(tick(at(10, 1, 2), "100.00", cum=0))             # the glitch: volume goes BACK
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30          # zero available volume

    # HIGH-WATER rule (the BarBuilder's, bar_builder.py _volume_delta): the water mark is 1300 and
    # the restated ticks stay below it, so nothing is credited to the participation base -- the
    # traded volume they imply already paid for a fill once.
    broker.on_tick(tick(at(10, 2, 0), "100.00", cum=200))
    broker.on_tick(tick(at(10, 2, 1), "100.00", cum=700))
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30

    # Only a print back ABOVE the water mark trades again, and it trades the TRUE delta over it
    # (1500 - 1300 = 200 -> 20), never the apparent 800 over the restated baseline.
    broker.on_tick(tick(at(10, 2, 2), "100.00", cum=1500))
    assert bus.for_order(oid)[-1]["filled_quantity"] == 50


async def test_a_restatement_on_a_minute_boundary_cannot_inflate_the_participation_base(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """A13, the half-fix this round closes: a restatement landing on the FIRST tick of a minute took
    the minute-roll branch instead of the reset branch, so it re-baselined the minute at the restated
    (tiny) cumulative and was never flagged. The correction back UP then read as a delta of the whole
    day's volume -- 100x the minute's real tape -- and the participation cap sized a fill off it.

    The baseline is therefore the per-symbol cumulative HIGH-WATER kept ACROSS minutes, and the reset
    test runs BEFORE the minute roll (bar_builder.py _volume_delta, the same rule).
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=50_000))
    bus.clear()

    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=100_000))    # minute baseline, high water
    broker.on_tick(tick(at(10, 1, 30), "100.00", cum=100_500))   # delta 500 -> 50
    assert bus.for_order(oid)[-1]["filled_quantity"] == 50

    broker.on_tick(tick(at(10, 2, 0), "100.00", cum=500))        # RESTATED, on the minute boundary
    assert bus.for_order(oid)[-1]["filled_quantity"] == 50       # nothing known to have traded

    broker.on_tick(tick(at(10, 2, 30), "100.00", cum=100_600))   # the correction back up
    filled = bus.for_order(oid)[-1]["filled_quantity"]
    # The true tape over the water mark is 100 shares, so at most 10 may fill -- not 10,010 off a
    # 100,100-share phantom delta.
    assert filled - 50 <= 10
    assert filled == 60


async def test_a_rebaselined_counter_that_never_recovers_blacks_the_symbol_out_for_the_session(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """The documented consequence of the A13 high-water rule, pinned so nobody "fixes" it later.

    ``volume_traded`` is compared against the per-symbol HIGH-WATER cumulative, so a feed that
    re-baselines low and never climbs back over the mark (a resubscribe on a thin name late in the
    day) offers zero available volume on EVERY remaining tick of the session -- not just the reset
    tick -- and not even a closed bar can back the cap while the flag is set. That is the intended
    conservative outcome, and it is the same one ``engine.marketdata.bar_builder`` produces from the
    same rule: a broker that under-fills costs the 8.5 gate nothing, while one that sizes fills off
    a phantom delta lies to it. The blackout is SESSION-scoped: the roll clears the baseline.
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    bus.clear()

    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=10_000))     # minute baseline / high water
    broker.on_tick(tick(at(10, 1, 30), "100.00", cum=10_300))    # delta 300 -> 30
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30

    # The counter restarts low and keeps climbing, but never back over 10,300.
    for minute, cum in ((2, 100), (3, 600), (4, 1_100), (5, 1_600)):
        broker.on_bar(
            Bar(symbol=SYM, ts_minute=at(10, minute - 1), open=Decimal("100"), high=Decimal("100"),
                low=Decimal("100"), close=Decimal("100"), volume=5_000)
        )
        broker.on_tick(tick(at(10, minute, 0), "100.00", cum=cum))
        broker.on_tick(tick(at(10, minute, 30), "100.00", cum=cum + 200))
        assert bus.for_order(oid)[-1]["filled_quantity"] == 30   # blacked out, bar volume and all

    # A NEW session re-baselines the high-water mark, so the next day fills normally.
    now.set(at(9, 14, 0, day=D2))
    fresh = await broker.place_order(order(quantity=100))
    bus.clear()
    broker.on_tick(tick(at(9, 15, 0, day=D2), "100.00", cum=500))
    broker.on_tick(tick(at(9, 15, 30, day=D2), "100.00", cum=2_500))
    assert bus.for_order(fresh)[-1]["filled_quantity"] == 100


async def test_running_bar_volume_from_ticks_drives_the_cap_before_any_bar_closes(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    bus.clear()
    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=1000))    # minute start; running volume 0
    assert bus.frames == []                                   # unknown volume with no prior bar
    broker.on_tick(tick(at(10, 1, 1), "100.00", cum=1500))    # running volume 500 -> cap 50
    assert bus.for_order(oid)[-1]["filled_quantity"] == 50


async def test_unknown_volume_does_not_block_the_fill(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """No bar and a flat cumulative volume => volume unknown => cap at the order quantity, never 0
    (a paper broker that silently never fills is worse than a pessimistic one)."""
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    bus.clear()
    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=0))
    assert bus.for_order(oid)[-1]["status"] == "COMPLETE"


# =========================================================================== (e) seeded rejections
async def _rejected_indices(seed: int, n: int, bus: _Bus, now: _Now) -> list[int]:
    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=seed,
        rejection_rate=0.005,
    )
    out: list[int] = []
    for i in range(n):
        bus.clear()
        await br.place_order(order())
        if bus.data()[-1]["status"] == "REJECTED":
            out.append(i)
    return out


async def test_injected_rejections_are_seeded_deterministic_and_rate_correct(now: _Now) -> None:
    n = 2000
    a = await _rejected_indices(4242, n, _Bus(), now)
    b = await _rejected_indices(4242, n, _Bus(), now)
    c = await _rejected_indices(99, n, _Bus(), now)
    assert a == b                       # same seed -> same rejected order indices (replay-safe)
    assert a != c                       # a different seed really does move them
    assert 2 <= len(a) <= 25            # ~0.5% of 2000 = 10 (Poisson sd ~3.2)
    assert 2 <= len(c) <= 25


async def test_a_rejected_order_never_enters_the_book(now: _Now) -> None:
    bus = _Bus()
    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=4242,
        rejection_rate=1.0,             # reject everything
    )
    oid = await br.place_order(order())
    frames = bus.for_order(oid)
    assert [f["status"] for f in frames] == ["REJECTED"]      # exactly one postback, no OPEN first
    assert frames[0]["status_message"]                        # the reason is captured (3.5.1)
    assert frames[0]["cancelled_quantity"] == 0
    br.on_tick(tick(at(10, 0, 1), "100.00"))
    assert bus.for_order(oid) == frames                       # dead: no later fill


# =========================================================================== (f) cancel / modify
async def test_cancel_of_a_resting_order_publishes_cancelled_with_the_remainder(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    broker.on_bar(
        Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal("100"), high=Decimal("100"),
            low=Decimal("100"), close=Decimal("100"), volume=300)
    )
    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=5000))    # 30 filled, 70 resting
    bus.clear()

    returned = await broker.cancel_order(oid)
    assert returned == oid
    frame = bus.for_order(oid)[-1]
    assert frame["status"] == "CANCELLED"
    assert frame["cancelled_quantity"] == 70
    assert frame["filled_quantity"] == 30                     # 3.5.1: filled_qty is PRESERVED
    assert frame["pending_quantity"] == 0

    broker.on_tick(tick(at(10, 1, 1), "100.00", cum=5000))    # cancelled: no further fills
    assert bus.for_order(oid)[-1]["status"] == "CANCELLED"


async def test_cancel_of_a_filled_order_raises(broker: PaperBroker, now: _Now) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order())
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    with pytest.raises(PaperOrderError, match="terminal"):
        await broker.cancel_order(oid)


async def test_cancel_of_an_unknown_order_raises(broker: PaperBroker) -> None:
    with pytest.raises(PaperOrderError, match="unknown"):
        await broker.cancel_order("000000000000000")


async def test_cancel_of_a_triggered_slm_raises_like_kite(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """Kite refuses to cancel an SL-M whose trigger has crossed: the order has left the trigger book
    and is on its way to becoming a market order. That refusal is a SIGNAL, and 3.2.8's square-off
    coordination is built on it -- "the cancel was refused, the protective order is going to fill,
    do NOT also send the market exit". A paper broker that accepted the cancel would hide the
    double-exit hazard until the first live session met it.
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(
        order(
            order_type="SL-M",
            transaction_type="SELL",
            trigger_price=Decimal("95.00"),
            quantity=100,
        )
    )
    bus.clear()

    # A print through the stop on a minute with no available volume: the trigger crosses and the
    # order stays resting, which is exactly the window the square-off coordination races.
    broker.on_tick(tick(at(10, 1, 0), "94.00", cum=1000))
    assert bus.frames == []
    assert (await broker.orders())[0]["status"] == "OPEN"

    with pytest.raises(PaperOrderError, match="already triggered"):
        await broker.cancel_order(oid)
    assert (await broker.orders())[0]["status"] == "OPEN"      # and the book is untouched


async def test_an_exception_in_the_match_path_rejects_only_that_order_and_never_escapes(
    now: _Now, bus: _Bus
) -> None:
    """"Nothing escapes ``on_tick``" has to hold for the ORDINARY matching loop too, not only for
    the GTT fire path: ``on_tick`` runs on the market-data hot path (plan 3.2) and an exception
    escaping it takes the feed down for every symbol. Anything the per-order body can raise
    (here an ``InstrumentStore`` lookup that blows up mid-session) is logged once, terminates THAT
    order with a REJECTED postback the OMS can act on, and lets the loop continue.
    """
    def tick_size(symbol: str) -> Decimal:
        if symbol == "BOOM":
            raise RuntimeError("instrument store unavailable")
        return TICK_SIZE

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=tick_size,
        rng_seed=1,
        rejection_rate=0.0,
    )
    now.set(at(10, 0, 0))
    first = await br.place_order(order(tradingsymbol="BOOM"))
    second = await br.place_order(order(tradingsymbol="BOOM", quantity=5))
    good = await br.place_order(order())
    bus.clear()

    br.on_tick(tick(at(10, 0, 1), "100.00", sym="BOOM"))       # must not raise
    for oid in (first, second):                                # the loop CONTINUED past the first
        frames = bus.for_order(oid)
        assert len(frames) == 1                                # published once, not per tick
        assert frames[0]["status"] == "REJECTED"               # nothing filled: a clean reject
        assert frames[0]["status_message"] == "paper: internal error"
        # No quantity may evaporate on a terminal transition (3.5.1): every share is accounted for
        # as filled or cancelled, or the OMS's position arithmetic silently loses the difference.
        assert frames[0]["filled_quantity"] + frames[0]["cancelled_quantity"] == frames[0][
            "quantity"
        ]
        assert frames[0]["pending_quantity"] == 0

    br.on_tick(tick(at(10, 0, 2), "100.00", sym="BOOM"))       # terminal: no postback storm
    assert len(bus.for_order(first)) == 1

    br.on_tick(tick(at(10, 0, 2), "100.00"))                   # an unaffected symbol still fills
    assert bus.for_order(good)[-1]["status"] == "COMPLETE"


async def test_an_internal_error_after_a_partial_fill_cancels_the_residual_and_keeps_the_fill(
    now: _Now, bus: _Bus
) -> None:
    """A partially filled order that then hits an internal error is CANCELLED, not REJECTED.

    REJECTED means "no part of this order ever traded", and the OMS reads it that way (3.5.1) -- on
    a row with 30 shares already filled and protected it would strand a live position the platform
    believes it does not hold. The 3.5.1 shape that DOES exist for this is the partial-cancel:
    ``filled_quantity`` preserved, the residual in ``cancelled_quantity``, nothing pending.
    """
    boom = {"on": False}

    def tick_size(symbol: str) -> Decimal:
        if boom["on"]:
            raise RuntimeError("instrument store unavailable")
        return TICK_SIZE

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=tick_size,
        rng_seed=1,
        rejection_rate=0.0,
    )
    now.set(at(10, 0, 0))
    oid = await br.place_order(order(quantity=100))
    br.on_bar(
        Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal("100"), high=Decimal("100"),
            low=Decimal("100"), close=Decimal("100"), volume=300)
    )
    br.on_tick(tick(at(10, 1, 0), "100.00", cum=5000))         # 30 filled, 70 resting
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30
    bus.clear()

    boom["on"] = True
    br.on_tick(tick(at(10, 2, 0), "100.00", cum=5000))         # must not raise
    frame = bus.for_order(oid)[-1]
    assert frame["status"] == "CANCELLED"
    assert frame["filled_quantity"] == 30                      # the fill that happened STAYS
    assert frame["cancelled_quantity"] == 70                   # the residual is accounted for
    assert frame["filled_quantity"] + frame["cancelled_quantity"] == frame["quantity"]
    assert frame["pending_quantity"] == 0
    assert frame["status_message"] == "paper: internal error"

    boom["on"] = False
    br.on_tick(tick(at(10, 3, 0), "100.00", cum=6000))         # terminal: no resurrection
    assert bus.for_order(oid)[-1]["status"] == "CANCELLED"
    assert bus.for_order(oid)[-1]["filled_quantity"] == 30


async def test_modify_replaces_price_trigger_and_quantity_on_a_resting_order(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(order_type="LIMIT", price=Decimal("99.00"), quantity=50))
    bus.clear()
    returned = await broker.modify_order(oid, {"price": Decimal("100.00"), "quantity": 40})
    assert returned == oid
    frame = bus.for_order(oid)[-1]
    assert frame["status"] == "OPEN"
    assert frame["price"] == Decimal("100.00")
    assert frame["quantity"] == 40
    # The modified limit is now reachable: a trade-through fills at the NEW price.
    broker.on_tick(tick(at(10, 0, 2), "99.95"))
    assert bus.for_order(oid)[-1]["average_price"] == Decimal("100.00")


async def test_modify_cannot_take_quantity_below_filled(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    broker.on_bar(
        Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal("100"), high=Decimal("100"),
            low=Decimal("100"), close=Decimal("100"), volume=300)
    )
    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=5000))    # 30 filled
    with pytest.raises(PaperOrderError, match="filled"):
        await broker.modify_order(oid, {"quantity": 10})


async def test_modify_down_to_the_filled_quantity_completes_the_order(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """Shrinking a partially filled order to exactly what has filled is the ordinary "take what I
    got, drop the rest" amendment. It leaves NOTHING to fill, so the order is COMPLETE and off the
    book -- left OPEN it would be a resting order with pending_quantity 0 that the OMS can never
    retire (3.5.1 terminal states absorb).
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=100))
    broker.on_bar(
        Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal("100"), high=Decimal("100"),
            low=Decimal("100"), close=Decimal("100"), volume=300)
    )
    broker.on_tick(tick(at(10, 1, 0), "100.00", cum=5000))    # 30 filled, 70 resting
    bus.clear()

    await broker.modify_order(oid, {"quantity": 30})
    frame = bus.for_order(oid)[-1]
    assert frame["status"] == "COMPLETE"
    assert frame["filled_quantity"] == 30
    assert frame["quantity"] == 30
    assert frame["pending_quantity"] == 0

    with pytest.raises(PaperOrderError, match="terminal"):
        await broker.cancel_order(oid)
    broker.on_tick(tick(at(10, 2, 0), "100.00", cum=6000))    # unrested: no further fills
    assert bus.for_order(oid)[-1]["status"] == "COMPLETE"


async def test_modify_to_a_non_positive_quantity_is_rejected(
    broker: PaperBroker, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(quantity=10))
    with pytest.raises(PaperOrderError, match="quantity"):
        await broker.modify_order(oid, {"quantity": 0})


# =========================================================================== (f2) session roll
async def test_a_resting_day_order_lapses_at_the_session_roll(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """Kite cancels every unfilled DAY order at the close. Without that, a paper limit left resting
    overnight fills on the next session's open print -- a fill that could never have happened, and
    the most flattering kind of lie the 8.5 gate could be told.
    """
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order(order_type="LIMIT", price=Decimal("100.00"), quantity=40))
    broker.on_tick(tick(at(10, 0, 1), "100.50"))              # no trade-through: still resting
    bus.clear()

    broker.on_tick(tick(at(9, 15, 0, day=D2), "99.00"))       # deep through, but a NEW session
    frames = bus.for_order(oid)
    assert [f["status"] for f in frames] == ["CANCELLED"]
    assert frames[0]["cancelled_quantity"] == 40
    assert frames[0]["filled_quantity"] == 0
    assert frames[0]["pending_quantity"] == 0
    assert frames[0]["status_message"] == "paper: lapsed at session close"

    broker.on_tick(tick(at(9, 15, 1, day=D2), "98.00"))       # and it can never fill again
    assert bus.for_order(oid) == frames


async def test_an_order_placed_today_survives_todays_first_tick(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """The roll is detected on the new session's FIRST TICK, which is long after the OMS may have
    placed today's orders (pre-open staging; a symbol whose first print is late). Lapsing by "was
    resting when the roll was noticed" would cancel today's own orders -- the scope is ``placed_at``.
    """
    now.set(at(10, 0, 0))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))                 # establishes session D
    now.set(at(9, 10, 0, day=D2))                                # pre-open on the NEXT session
    oid = await broker.place_order(order(order_type="LIMIT", price=Decimal("100.00"), quantity=40))
    bus.clear()

    broker.on_tick(tick(at(9, 15, 0, day=D2), "99.00"))          # first tick of D2: the roll
    frame = bus.for_order(oid)[-1]
    assert frame["status"] == "COMPLETE"                         # it traded, it did not lapse
    assert frame["filled_quantity"] == 40


async def test_the_session_lapse_sweeps_every_symbol_not_only_the_one_that_ticked(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """The lapse is a BROKER-WIDE event: Kite cancels every unfilled DAY order at the close, not
    just the ones on symbols that happen to print again the next morning. Scoped to the ticking
    symbol, an order on a name that never prints again (delisted, halted, simply dropped from the
    subscription) rests forever and stays "live protection" to 3.2.8's square-off coordination.
    """
    now.set(at(10, 0, 0))
    quiet = await broker.place_order(
        order(tradingsymbol="INFY", order_type="LIMIT", price=Decimal("100.00"), quantity=40)
    )
    broker.on_tick(tick(at(10, 0, 1), "100.00"))              # TCS establishes session D
    bus.clear()

    # INFY never prints again; TCS's first tick of the NEW session is what the broker sees.
    broker.on_tick(tick(at(9, 15, 0, day=D2), "99.00"))
    frames = bus.for_order(quiet)
    assert [f["status"] for f in frames] == ["CANCELLED"]
    assert frames[0]["cancelled_quantity"] == 40
    assert frames[0]["status_message"] == "paper: lapsed at session close"

    broker.on_tick(tick(at(9, 20, 0, day=D2), "50.00", sym="INFY"))    # deep through, but dead
    assert bus.for_order(quiet) == frames


async def test_a_raising_publish_in_the_lapse_sweep_never_escapes_on_tick(now: _Now) -> None:
    """The lapse sweep is the THIRD loop inside ``on_tick``, and the last one left unguarded.

    It is also the worst place for an escape: it runs on the FIRST tick of a session, broker-wide,
    so one raising publish there kills the market-data feed for every symbol before the day has
    started -- and it takes the rest of the sweep with it, leaving the untouched symbols' orders
    resting forever, which 3.2.8 keeps reading as live protection.

    The book change is applied BEFORE the postback is attempted, so a publish that raises loses the
    NOTIFICATION and nothing else: the order really is CANCELLED, the sweep carries on, and the
    reconciler's orderbook re-read (R5) is what closes the gap.
    """
    published: list[dict] = []

    def publish(topic: str, frame: BaseModel) -> None:
        published.append(frame.data)
        if frame.data["tradingsymbol"] == "INFY" and frame.data["status"] == "CANCELLED":
            raise RuntimeError("bus down")

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=publish,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        rejection_rate=0.0,
    )
    now.set(at(10, 0, 0))
    # INFY is swept FIRST (its symbol state is created first) and never prints again; the other two
    # must lapse anyway.
    quiet = await br.place_order(
        order(tradingsymbol="INFY", order_type="LIMIT", price=Decimal("100.00"), quantity=40)
    )
    other = await br.place_order(
        order(tradingsymbol="WIPRO", order_type="LIMIT", price=Decimal("100.00"), quantity=25)
    )
    ticking = await br.place_order(order(order_type="LIMIT", price=Decimal("100.00"), quantity=10))
    br.on_tick(tick(at(10, 0, 1), "100.50"))                  # TCS establishes session D, no fill
    published.clear()

    br.on_tick(tick(at(9, 15, 0, day=D2), "99.00"))           # the roll: must NOT raise

    book = {o["order_id"]: o for o in await br.orders()}
    assert book[quiet]["status"] == "CANCELLED"               # the failed publish did not unwind it
    assert book[quiet]["cancelled_quantity"] == 40
    assert book[other]["status"] == "CANCELLED"               # the sweep CONTINUED past the raiser
    assert book[other]["cancelled_quantity"] == 25
    assert book[ticking]["status"] == "CANCELLED"
    # ...and the ticking symbol's order lapsed rather than filling on the new session's open print,
    # even though 99.00 trades through its limit.
    assert book[ticking]["filled_quantity"] == 0
    assert {d["tradingsymbol"] for d in published} == {"INFY", "WIPRO", SYM}

    published.clear()
    br.on_tick(tick(at(9, 20, 0, day=D2), "50.00", sym="INFY"))    # deep through, but dead
    br.on_tick(tick(at(9, 20, 0, day=D2), "50.00"))
    assert published == []


async def test_gtts_survive_the_session_roll_that_lapses_resting_orders(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """A Kite GTT is good-till-triggered (a year), NOT good-for-the-day -- the asymmetry with
    resting orders is the whole reason 3.2.8 parks protection in a GTT overnight.
    """
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(_oco("95.00", "110.00"))
    resting = await broker.place_order(order(order_type="LIMIT", price=Decimal("1.00")))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    bus.clear()

    broker.on_tick(tick(at(9, 15, 0, day=D2), "99.00"))       # session roll, no trigger crossed
    assert bus.for_order(resting)[-1]["status"] == "CANCELLED"
    assert (await broker.gtts())[0]["id"] == gid
    assert (await broker.gtts())[0]["status"] == "active"

    bus.clear()
    broker.on_tick(tick(at(9, 16, 0, day=D2), "94.00"))       # it still fires on the new day
    fired = bus.data()
    assert len(fired) == 1
    assert fired[0]["price"] == Decimal("95.00")


# =========================================================================== (g) book shapes
async def test_orders_and_positions_are_kite_shaped(
    broker: PaperBroker, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    buy = await broker.place_order(order(quantity=10))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    sell = await broker.place_order(order(transaction_type="SELL", quantity=4))
    broker.on_tick(tick(at(10, 0, 2), "110.00"))
    resting = await broker.place_order(order(order_type="LIMIT", price=Decimal("1.00")))

    book = await broker.orders()
    assert isinstance(book, list)
    by_id = {o["order_id"]: o for o in book}
    assert set(by_id) == {buy, sell, resting}
    assert PINNED_KEYS <= set(by_id[buy])
    assert by_id[buy]["status"] == "COMPLETE"
    assert by_id[resting]["status"] == "OPEN"

    pos = await broker.positions()
    assert set(pos) == {"net", "day"}
    net = {(p["tradingsymbol"], p["product"]): p for p in pos["net"]}
    row = net[(SYM, "MIS")]
    assert row["quantity"] == 6                    # 10 bought, 4 sold -> signed net long 6
    assert row["average_price"] == Decimal("100.15")
    assert row["buy_quantity"] == 10
    assert row["sell_quantity"] == 4

    holdings = await broker.holdings()
    assert holdings == []                          # MIS is not a holding
    margins = await broker.margins()
    assert margins["equity"]["available"]["live_balance"] > 0


async def test_cnc_fills_show_up_as_holdings(broker: PaperBroker, now: _Now) -> None:
    now.set(at(10, 0, 0))
    await broker.place_order(order(product="CNC", quantity=7))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    holdings = await broker.holdings()
    assert len(holdings) == 1
    assert holdings[0]["tradingsymbol"] == SYM
    assert holdings[0]["quantity"] == 7
    assert holdings[0]["product"] == "CNC"


async def test_short_position_quantity_is_negative(broker: PaperBroker, now: _Now) -> None:
    now.set(at(10, 0, 0))
    await broker.place_order(order(transaction_type="SELL", quantity=5))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    pos = await broker.positions()
    assert pos["net"][0]["quantity"] == -5
    assert pos["net"][0]["average_price"] == Decimal("99.85")


async def test_positions_day_is_the_current_session_while_net_is_lifetime(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """Kite's ``day`` book is the CURRENT session's trades; ``net`` carries the position forward.
    Reporting the same rows for both makes a swing position look like it was opened today, which is
    exactly what the 3.2.8 square-off scheduler reads to decide what must be flattened.
    """
    now.set(at(10, 0, 0))
    await broker.place_order(order(quantity=10))
    broker.on_tick(tick(at(10, 0, 1), "100.00"))                  # session D: +10
    now.set(at(10, 0, 0, day=D2))
    second = await broker.place_order(order(quantity=4))
    broker.on_tick(tick(at(10, 0, 1, day=D2), "110.00"))          # session D2: +4
    second_px = bus.for_order(second)[-1]["average_price"]

    pos = await broker.positions()
    net = {(p["tradingsymbol"], p["product"]): p for p in pos["net"]}
    day = {(p["tradingsymbol"], p["product"]): p for p in pos["day"]}
    assert net[(SYM, "MIS")]["quantity"] == 14                    # lifetime
    assert net[(SYM, "MIS")]["buy_quantity"] == 14
    assert day[(SYM, "MIS")]["quantity"] == 4                     # today only
    assert day[(SYM, "MIS")]["buy_quantity"] == 4
    assert day[(SYM, "MIS")]["average_price"] == second_px
    assert net[(SYM, "MIS")]["average_price"] != second_px


async def test_orders_reports_each_row_last_update_not_the_read_time(
    broker: PaperBroker, now: _Now
) -> None:
    """The reconciler (R5) reads the orderbook and compares broker timestamps against platform
    rows. Restamping every row with ``now`` on each read makes every order look freshly updated and
    destroys exactly the signal the reconciler needs.
    """
    now.set(at(10, 0, 0))
    first = await broker.place_order(order(order_type="LIMIT", price=Decimal("1.00")))
    now.set(at(10, 5, 0))
    second = await broker.place_order(order(order_type="LIMIT", price=Decimal("2.00")))

    now.set(at(11, 30, 0))                                        # a much later read
    book = {o["order_id"]: o for o in await broker.orders()}
    assert book[first]["order_timestamp"] == "2026-06-17 10:00:00"
    assert book[second]["order_timestamp"] == "2026-06-17 10:05:00"

    now.set(at(11, 31, 0))
    await broker.cancel_order(first)                              # a real change DOES move it
    book = {o["order_id"]: o for o in await broker.orders()}
    assert book[first]["order_timestamp"] == "2026-06-17 11:31:00"
    assert book[second]["order_timestamp"] == "2026-06-17 10:05:00"


async def test_a_fill_is_stamped_with_the_exchange_timestamp(
    broker: PaperBroker, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    oid = await broker.place_order(order())
    broker.on_tick(tick(at(10, 0, 2), "100.00"))
    now.set(at(15, 0, 0))
    row = {o["order_id"]: o for o in await broker.orders()}[oid]
    assert row["order_timestamp"] == "2026-06-17 10:00:02"        # the trade's own time, not wall


# =========================================================================== (h) GTT OCO
def _oco(lower: str, upper: str) -> dict:
    return {
        "trigger_type": "two-leg",
        "tradingsymbol": SYM,
        "exchange": "NSE",
        "last_price": Decimal("100.00"),
        "trigger_values": [Decimal(lower), Decimal(upper)],
        "orders": [
            {"transaction_type": "SELL", "quantity": 10, "order_type": "LIMIT",
             "product": "CNC", "price": Decimal(lower)},
            {"transaction_type": "SELL", "quantity": 10, "order_type": "LIMIT",
             "product": "CNC", "price": Decimal(upper)},
        ],
    }


async def test_gtt_oco_first_leg_to_trigger_places_its_limit_and_kills_the_other(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(_oco("95.00", "110.00"))
    assert isinstance(gid, int)
    bus.clear()

    broker.on_tick(tick(at(10, 0, 1), "99.00"))          # between the triggers: nothing fires
    assert bus.frames == []

    broker.on_tick(tick(at(10, 0, 2), "94.00"))          # lower leg crosses
    fired = bus.data()
    assert len(fired) == 1
    assert fired[0]["status"] == "OPEN"
    assert fired[0]["order_type"] == "LIMIT"             # Kite semantics: a GTT fires a LIMIT order
    assert fired[0]["transaction_type"] == "SELL"
    assert fired[0]["price"] == Decimal("95.00")
    assert fired[0]["product"] == "CNC"

    gtts = await broker.gtts()
    assert len(gtts) == 1
    assert gtts[0]["id"] == gid
    assert gtts[0]["status"] == "triggered"

    bus.clear()
    broker.on_tick(tick(at(10, 0, 3), "120.00"))         # the upper leg is dead
    assert not [d for d in bus.data() if d["price"] == Decimal("110.00")]
    assert (await broker.gtts())[0]["status"] == "triggered"


async def test_gtt_single_leg_and_delete(broker: PaperBroker, bus: _Bus, now: _Now) -> None:
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(
        {
            "trigger_type": "single",
            "tradingsymbol": SYM,
            "exchange": "NSE",
            "last_price": Decimal("100.00"),
            "trigger_values": [Decimal("95.00")],
            "orders": [{"transaction_type": "SELL", "quantity": 10, "order_type": "LIMIT",
                        "product": "CNC", "price": Decimal("95.00")}],
        }
    )
    await broker.delete_gtt(gid)
    assert await broker.gtts() == []
    bus.clear()
    broker.on_tick(tick(at(10, 0, 5), "90.00"))
    assert bus.frames == []                              # a deleted GTT never fires
    with pytest.raises(PaperOrderError, match="unknown"):
        await broker.delete_gtt(gid)


async def test_modify_gtt_moves_the_trigger(broker: PaperBroker, bus: _Bus, now: _Now) -> None:
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(_oco("95.00", "110.00"))
    returned = await broker.modify_gtt(gid, _oco("90.00", "110.00"))
    assert returned == gid
    bus.clear()
    broker.on_tick(tick(at(10, 0, 2), "94.00"))          # above the NEW lower trigger
    assert bus.frames == []
    broker.on_tick(tick(at(10, 0, 3), "89.00"))
    assert bus.data()[-1]["price"] == Decimal("90.00")


@pytest.mark.parametrize(
    ("bad_leg", "match"),
    [
        ({"transaction_type": "SELL", "quantity": 0, "product": "CNC"}, "quantity"),
        ({"transaction_type": "SELL", "quantity": -3, "product": "CNC"}, "quantity"),
        ({"transaction_type": "LONG", "quantity": 10, "product": "CNC"}, "transaction_type"),
        ({"transaction_type": "SELL", "quantity": 10, "product": "NRML"}, "product"),
        ({"transaction_type": "SELL", "quantity": 10}, "product"),
    ],
)
async def test_place_gtt_validates_every_leg_at_placement(
    broker: PaperBroker, bad_leg: dict, match: str
) -> None:
    """A leg is only ever validated by the fill engine at FIRE time, which is inside ``on_tick`` --
    a malformed leg accepted at placement therefore detonates on the market-data hot path, days
    later, with the GTT already marked triggered. Validate it where the caller can still be told.
    """
    req = {
        "trigger_type": "single",
        "tradingsymbol": SYM,
        "exchange": "NSE",
        "last_price": Decimal("100.00"),
        "trigger_values": [Decimal("95.00")],
        "orders": [dict(bad_leg)],
    }
    with pytest.raises(PaperOrderError, match=match):
        await broker.place_gtt(req)
    assert await broker.gtts() == []                     # nothing was booked


async def test_modify_gtt_validates_every_leg_and_leaves_the_original_intact(
    broker: PaperBroker, now: _Now
) -> None:
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(_oco("95.00", "110.00"))
    bad = _oco("90.00", "110.00")
    bad["orders"][1]["quantity"] = 0
    with pytest.raises(PaperOrderError, match="quantity"):
        await broker.modify_gtt(gid, bad)
    condition = (await broker.gtts())[0]["condition"]
    assert condition["trigger_values"] == [Decimal("95.00"), Decimal("110.00")]


async def test_a_broken_gtt_leg_never_raises_out_of_on_tick_and_latches_to_failed(
    broker: PaperBroker, bus: _Bus, now: _Now
) -> None:
    """``on_tick`` is the market-data hot path (plan 3.2): an exception escaping it takes down the
    feed for EVERY symbol. A leg corrupted after placement (or by a future leg-shape change) must be
    logged ONCE and the GTT LATCHED to a distinct terminal status.

    Left "active" the trigger is re-evaluated on every subsequent tick through it -- a per-tick
    exception+log storm on the hot path for the rest of the session -- and it reads to WO-P3-6's
    GTTManager as protection that is still armed. "failed" is neither: it stops the retry and it is
    the status the GTTManager turns into a PROTECTION_FAILED reason.
    """
    now.set(at(10, 0, 0))
    gid = await broker.place_gtt(_oco("95.00", "110.00"))
    broker._gtt_book[gid].orders[0]["quantity"] = 0      # corrupted after validation
    bus.clear()

    broker.on_tick(tick(at(10, 0, 2), "94.00"))          # must not raise
    assert bus.frames == []                              # nothing booked
    assert (await broker.gtts())[0]["status"] == "failed"

    # LATCHED: deeper through the same trigger does not retry...
    broker.on_tick(tick(at(10, 0, 3), "93.00"))
    assert bus.frames == []
    assert (await broker.gtts())[0]["status"] == "failed"

    # ...and not even repairing the leg re-arms it. Only the owner of the GTT (WO-P3-6) may.
    broker._gtt_book[gid].orders[0]["quantity"] = 10
    broker.on_tick(tick(at(10, 0, 4), "92.00"))
    assert bus.frames == []
    assert (await broker.gtts())[0]["status"] == "failed"


async def test_a_raising_publish_never_leaves_a_resting_leg_with_a_gtt_that_did_not_fire(
    now: _Now,
) -> None:
    """BOOKED == FIRED, and a raising publish is not allowed to break that tie either.

    The fire path registers and RESTS the leg, marks the GTT ``triggered``, and only then attempts
    the postback -- so a bus that throws loses the notification and nothing else. The forbidden
    combination is a leg resting on the book with a GTT that is NOT ``triggered``: left ``active``
    the same trigger re-fires the leg on the next tick through it and puts a SECOND exit on a
    position that already has one (the 3.2.8 double-exit hazard), and latched ``failed`` it tells
    WO-P3-6's GTTManager that protection failed while that protection is in fact resting.

    (A leg that cannot be REGISTERED -- a corrupt leg, the case above -- still latches ``failed``:
    there, nothing is resting.)
    """
    published: list[dict] = []

    def publish(topic: str, frame: BaseModel) -> None:
        published.append(frame.data)
        if frame.data.get("price") == Decimal("95.00"):
            raise RuntimeError("bus down")

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=publish,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        rejection_rate=0.0,
    )
    now.set(at(10, 0, 0))
    await br.place_gtt(_oco("95.00", "110.00"))

    br.on_tick(tick(at(10, 0, 2), "94.00"))              # must not raise
    resting_legs = [o for o in await br.orders() if o["status"] == "OPEN"]
    gtt = (await br.gtts())[0]
    assert len(resting_legs) == 1                        # the leg IS on the book...
    assert resting_legs[0]["price"] == Decimal("95.00")
    assert gtt["status"] == "triggered"                  # ...so the GTT must say it fired
    assert not (resting_legs and gtt["status"] != "triggered")
    booked = [d for d in published if d.get("price") == Decimal("95.00")]
    assert len(booked) == 1                              # the postback was attempted exactly once

    br.on_tick(tick(at(10, 0, 3), "93.00"))              # the leg is NOT booked a second time
    assert [d for d in published if d.get("price") == Decimal("95.00")] == booked
    assert len([o for o in await br.orders() if o["status"] == "OPEN"]) == 1


# =========================================================================== (i) order guard
async def test_order_guard_runs_before_any_book_change(now: _Now, bus: _Bus) -> None:
    seen: list[str] = []

    def guard(intent: str) -> None:
        seen.append(intent)
        raise RuntimeError("order-surface violation")

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        order_guard=guard,
    )
    with pytest.raises(RuntimeError, match="order-surface violation"):
        await br.place_order(order())
    assert seen == ["entry"]
    assert bus.frames == []                     # nothing published
    assert await br.orders() == []              # nothing booked
    br.on_tick(tick(at(10, 0, 1), "100.00"))
    assert bus.frames == []


async def test_order_guard_receives_the_declared_intent(now: _Now, bus: _Bus) -> None:
    seen: list[str] = []
    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        order_guard=seen.append,
    )
    oid = await br.place_order(order(), intent="risk_reducing")
    await br.modify_order(oid, {"quantity": 5})
    await br.cancel_order(oid)
    gid = await br.place_gtt(_oco("95.00", "110.00"))
    await br.modify_gtt(gid, _oco("94.00", "110.00"))
    await br.delete_gtt(gid)
    assert seen == ["risk_reducing"] * 6


async def test_guard_blocks_cancel_without_touching_the_book(now: _Now, bus: _Bus) -> None:
    blocked = {"on": False}

    def guard(intent: str) -> None:
        if blocked["on"]:
            raise RuntimeError("blocked")

    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        order_guard=guard,
    )
    oid = await br.place_order(order(order_type="LIMIT", price=Decimal("1.00")))
    blocked["on"] = True
    bus.clear()
    with pytest.raises(RuntimeError, match="blocked"):
        await br.cancel_order(oid)
    assert bus.frames == []
    assert (await br.orders())[0]["status"] == "OPEN"      # still resting, untouched


# =========================================================================== (j) postback contract
async def test_every_postback_is_an_order_update_frame_with_the_pinned_keys(
    now: _Now, bus: _Bus
) -> None:
    br = PaperBroker(
        clock=Clock(time_source=now),
        publish=bus,
        fill_model=FillModelConfig(),
        tick_size=lambda symbol: TICK_SIZE,
        rng_seed=1,
        rejection_rate=0.0,
    )
    now.set(at(10, 0, 0))
    partial = await br.place_order(order(quantity=100))
    br.on_bar(
        Bar(symbol=SYM, ts_minute=at(10, 0), open=Decimal("100"), high=Decimal("100"),
            low=Decimal("100"), close=Decimal("100"), volume=300)
    )
    br.on_tick(tick(at(10, 1, 0), "100.00", cum=5000))
    br.on_tick(tick(at(10, 1, 1), "100.00", cum=5000))
    await br.cancel_order(partial)
    gtt_order = await br.place_order(order(order_type="LIMIT", price=Decimal("1.00")))
    await br.modify_order(gtt_order, {"price": Decimal("2.00")})
    await br.cancel_order(gtt_order)

    assert bus.frames, "the scenario must actually produce postbacks"
    for topic, frame in bus.frames:
        assert topic == ORDER_UPDATE_TOPIC
        assert isinstance(frame, OrderUpdateFrame)
        assert PINNED_KEYS <= set(frame.data), f"missing: {PINNED_KEYS - set(frame.data)}"
        assert isinstance(frame.data["order_id"], str)
        assert frame.data["status"] in {"OPEN", "COMPLETE", "CANCELLED", "REJECTED"}
        assert isinstance(frame.data["filled_quantity"], int)
        assert isinstance(frame.data["order_timestamp"], str)
        assert frame.data["tag"] == "wo-p3-2"


async def test_order_timestamp_is_kite_wire_shaped(broker: PaperBroker, bus: _Bus, now: _Now) -> None:
    now.set(at(10, 5, 30))
    await broker.place_order(order())
    ts = bus.data()[-1]["order_timestamp"]
    assert ts == "2026-06-17 10:05:30"
    assert dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")      # the Kite postback wire format


# =========================================================================== validation
@pytest.mark.parametrize(
    ("bad", "match"),
    [
        ({"tradingsymbol": None}, "tradingsymbol"),
        ({"transaction_type": "LONG"}, "transaction_type"),
        ({"order_type": "SL"}, "order_type"),
        ({"product": "NRML"}, "product"),
        ({"quantity": 0}, "quantity"),
        ({"quantity": -5}, "quantity"),
        ({"order_type": "LIMIT", "price": None}, "price"),
        ({"order_type": "SL-M", "trigger_price": None}, "trigger_price"),
        ({"variety": "iceberg"}, "variety"),
    ],
)
async def test_place_order_validates_the_request(broker: PaperBroker, bad: dict, match: str) -> None:
    with pytest.raises(PaperOrderError, match=match):
        await broker.place_order(order(**bad))


async def test_ltp_returns_decimals_for_seen_tokens(broker: PaperBroker) -> None:
    broker.on_tick(tick(at(10, 0, 1), "100.00"))
    out = await broker.ltp([1])
    assert out == {1: Decimal("100.00")}
    assert await broker.ltp([999]) == {}


# =========================================================================== sigma estimator
def test_sigma_uses_the_sample_stdev_once_enough_bars_have_closed() -> None:
    """The real estimation path (>= SIGMA_MIN_BARS returns), checked against ``statistics.stdev``
    -- an independent implementation of the same sample (n-1) stdev.
    """
    est = SigmaEstimator()
    closes = [Decimal(c) for c in ("100", "101", "99", "102", "100", "103")]   # 6 closes -> 5 rets
    for close in closes:
        est.on_close(SYM, close)

    rets = [float((b - a) / a) for a, b in zip(closes, closes[1:], strict=False)]
    expected = statistics.stdev(rets) * 100.0            # sample stdev x the reference price
    got = float(est.sigma(SYM, Decimal("100")))
    assert got == pytest.approx(expected, rel=1e-9)
    # ...and it really is the estimated path, not the bootstrap that shorter history returns.
    assert got != pytest.approx(float(Decimal("100") * SIGMA_BOOTSTRAP_PCT))


def test_sigma_falls_back_to_the_bootstrap_below_the_minimum_bars() -> None:
    est = SigmaEstimator()
    for close in ("100", "101", "99", "102", "100"):     # 5 closes -> 4 returns, below the minimum
        est.on_close(SYM, Decimal(close))
    assert est.sigma(SYM, Decimal("100")) == Decimal("100") * SIGMA_BOOTSTRAP_PCT
    # An unseen symbol is "unknown", never "calm" -- the opening minutes are the least calm of all.
    assert est.sigma("INFY", Decimal("100")) == Decimal("100") * SIGMA_BOOTSTRAP_PCT


def test_sigma_of_flat_closes_is_floored_never_zero() -> None:
    est = SigmaEstimator()
    for _ in range(8):
        est.on_close(SYM, Decimal("100"))                # dead flat: the sample stdev is exactly 0
    sigma = est.sigma(SYM, Decimal("100"))
    assert sigma == Decimal("100") * SIGMA_FLOOR_PCT
    assert sigma > 0
