"""PaperBroker v1 — a conservative, deterministic simulation of the Kite order surface.

``PaperBroker`` implements :class:`~engine.paper.surface.BrokerSurface`, so the OMS routes to it or
to :class:`~engine.broker.kite_client.KiteClient` without knowing which (``routing`` flag). No method
can place a real broker order and the module imports nothing from ``engine.broker``.

Design rules, all load-bearing:

* **Postbacks are the ONLY output.** Every state change publishes exactly one
  :class:`~engine.core.contracts.OrderUpdateFrame` on the broker's ``topic`` (production:
  ``PAPER_ORDER_UPDATE_TOPIC``, never the real ``order.update``) with a Kite-shaped ``data`` dict —
  the same field names the mt-ticker child forwards for live orders, so live and paper share ONE
  OMS parser. Price fields are :class:`~decimal.Decimal` (the OMS coerces with ``Decimal(str(v))``
  either way).
* **Conservative fills, never optimistic ones.** A market order waits out the simulated latency and
  pays half-spread + k·σ₁ₘ; a limit order needs a strict TRADE-THROUGH and is classed once, on its
  first eligible tick: through on that tick it is MARKETABLE and fills like a market order capped at
  its limit, otherwise it RESTS and fills at its limit, never at the better traded price; an SL-M
  fills at the print it triggered on (a gap through a stop costs more than the stop distance); every
  order gets a ``participation_cap`` share of each MINUTE's volume — a budget per (order, minute),
  not per tick (per tick it is unbounded per minute); prices are rounded to the tick AGAINST the
  taker.
* **Session-bounded like the real thing.** Resting DAY orders LAPSE at the session roll (Kite
  cancels unfilled DAY orders at the close), so a paper limit never fills on the next session's open
  print. GTTs do NOT lapse (good-till-triggered), which is why protection is parked in one overnight.
* **Deterministic under replay.** The only randomness is injected rejections from a seeded
  :class:`random.Random`; one draw happens on EVERY ``place_order`` regardless of branch, so the
  rejection sequence depends on the order count alone. Order and GTT ids are monotonic counters.
* **No async work inside ``on_tick``, and nothing escapes it.** ``on_tick``/``on_bar`` are cheap,
  synchronous, and publish through the injected ``publish`` (``EventBus.publish`` is
  fire-and-forget). An exception there would take down the feed for every symbol, so ALL THREE
  loops carry a log-once-and-continue guard: the **session-lapse sweep** (broker-wide on a
  session's first tick; a raise would strand every symbol after it), the **match loop** (an order
  whose match raises is terminated with one postback, :meth:`PaperBroker._reject_internal`), and the
  **GTT loop** (a leg that cannot be booked latches the GTT to ``"failed"``). Everything the
  per-order body touches (``tick_size``, ``publish``, the fill model) is injected, so "cannot raise"
  is not assertable here. Every path emits through :meth:`PaperBroker._publish_guarded`, always
  AFTER the book change is applied: a lost postback is a gap the reconciler's orderbook re-read
  closes, a state change unwound by a failed notification is a lie the OMS cannot detect. The
  caller-facing paths (``place_order``/``modify_order``/``cancel_order``) publish UNguarded — the
  caller is awaiting an answer.
* **``engine.core`` and nothing else** (``engine.core.contracts`` / ``engine.core.log`` plus
  ``engine.paper`` siblings), asserted per module by ``tests/unit/test_import_graph.py``;
  ``engine.paper.replay`` alone may also import ``engine.marketdata``.

**Known v1 simplifications** (all in the conservative direction): only ``variety="regular"`` and
order types MARKET / LIMIT / SL-M are accepted — SL, CO, AMO and iceberg are REJECTED rather than
approximated; a GTT fire is not subject to injected rejection; margins are a static stub (margin
pre-checks live in the OMS).

Two more, each with a consumer that has to know:

* **GTT status ``"failed"`` is PAPER-ONLY.** Kite's vocabulary is ``active`` / ``triggered`` /
  ``disabled`` / ``expired`` / ``deleted``; ``"failed"`` is this broker's terminal status for a
  trigger that fired and could not book its leg (see :class:`_Gtt`, :meth:`PaperBroker.gtts`). The
  paper ProtectionManager (engine.oms.protection) maps it to a PROTECTION_FAILED reason.
* **A cumulative-volume re-baseline blacks the symbol out for the rest of the session.** The
  participation base is measured from a HIGH-WATER cumulative, so a counter that restates downward
  and never climbs back over the mark offers zero available volume on every remaining tick until
  the session roll clears the baseline. Deliberate and conservative, and the same outcome as
  ``engine.marketdata.bar_builder``; see :meth:`PaperBroker._effective_volume` branch 0.

**Kite semantics deliberately NOT simplified**, because a downstream decision reads them: a cancel
of an SL-M whose trigger has already crossed RAISES — the order has left the trigger book — and the
square-off coordination reads that refusal as "the stop is going to fill, do not send a second
exit". Only the error text differs from live Kite's.
"""

from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING, Any

from engine.core.contracts import ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.log import get_logger
from engine.paper.fill_model import FillModelConfig
from engine.paper.surface import ReqLike

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from pydantic import BaseModel

    from engine.core.clock import Clock
    from engine.core.types import Bar, Tick

_log = get_logger("engine.paper.broker")

#: Base for paper order ids. 15 digits, numeric-string — Kite-shaped, so nothing downstream can
#: start depending on a paper-only id format (and a paper id never collides with a live one, which
#: is 25xxxxxxxxxxxxx-style for the current era).
ORDER_ID_BASE = 900000000000000
#: Base for paper GTT trigger ids (Kite returns an int).
GTT_ID_BASE = 900000

#: Order statuses this broker emits. Kite's vocabulary, restricted to what v1 can reach.
TERMINAL_STATUSES = frozenset({"COMPLETE", "CANCELLED", "REJECTED"})

_VALID_SIDES = frozenset({"BUY", "SELL"})
_VALID_ORDER_TYPES = frozenset({"MARKET", "LIMIT", "SL-M"})
_VALID_PRODUCTS = frozenset({"MIS", "CNC"})
_VALID_VARIETIES = frozenset({"regular"})

#: Kite postback wire format for ``order_timestamp``: naive IST, second resolution.
_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# --- sigma estimator constants (see SigmaEstimator) ------------------------------------------
#: Close-to-close returns retained for the rolling stdev.
SIGMA_WINDOW_BARS = 20
#: Below this many returns the sample stdev is noise; the bootstrap is used instead.
SIGMA_MIN_BARS = 5
#: Bootstrap 1-minute sigma as a fraction of price (20 bps) when history is too short.
SIGMA_BOOTSTRAP_PCT = Decimal("0.002")
#: Floor on any sigma estimate as a fraction of price (5 bps) — a dead-flat symbol still moves.
SIGMA_FLOOR_PCT = Decimal("0.0005")


class PaperOrderError(ValueError):
    """A rejected paper broker call.

    Live Kite raises ``kiteconnect.exceptions.InputException`` (a subclass of its ``KiteException``)
    for a malformed request, an unknown ``order_id``, or a cancel/modify of an order that is already
    terminal. ``engine.paper`` must not import pykiteconnect (it depends on ``core`` only),
    so this is the paper equivalent: a ``ValueError`` subclass, raised in the same situations, so an
    OMS ``except (InputException, PaperOrderError)`` — or a bare ``except ValueError`` — handles
    both routings identically.
    """


class SigmaEstimator:
    """Rolling 1-minute volatility in PRICE units, fed by closed-bar closes.

    ``sigma(symbol, price)`` = sample stdev of the last :data:`SIGMA_WINDOW_BARS` close-to-close
    RETURNS, multiplied by the reference price. Two guards keep an unknown or degenerate estimate
    from making the fill model optimistic:

    * fewer than :data:`SIGMA_MIN_BARS` returns (the session's first minutes, a fresh symbol, a
      restart) fall back to :data:`SIGMA_BOOTSTRAP_PCT` of price — the opening minutes are the
      *least* calm part of the day, so "unknown" must not read as "calm";
    * every estimate is floored at :data:`SIGMA_FLOOR_PCT` of price.

    Returns are Decimal throughout (no float round-trip); the stdev uses ``Decimal.sqrt``.
    """

    def __init__(self) -> None:
        self._closes: dict[str, deque[Decimal]] = {}

    def on_close(self, symbol: str, close: Decimal) -> None:
        hist = self._closes.get(symbol)
        if hist is None:
            hist = deque(maxlen=SIGMA_WINDOW_BARS + 1)   # N+1 closes -> N returns
            self._closes[symbol] = hist
        hist.append(close)

    def sigma(self, symbol: str, price: Decimal) -> Decimal:
        floor = abs(price) * SIGMA_FLOOR_PCT
        closes = list(self._closes.get(symbol, ()))
        rets = [(b - a) / a for a, b in zip(closes, closes[1:], strict=False) if a > 0]
        if len(rets) < SIGMA_MIN_BARS:
            return max(abs(price) * SIGMA_BOOTSTRAP_PCT, floor)
        n = len(rets)
        mean = sum(rets) / n
        variance = sum((r - mean) ** 2 for r in rets) / (n - 1)
        return max(variance.sqrt() * abs(price), floor)


@dataclass
class _Order:
    """One order in the paper book. Mirrors the Kite orderbook row the OMS reads."""

    order_id: str
    variety: str
    exchange: str
    tradingsymbol: str
    transaction_type: str
    order_type: str
    product: str
    quantity: int
    price: Decimal | None
    trigger_price: Decimal | None
    tag: str | None
    eligible_at: datetime
    placed_at: datetime
    status: str = "OPEN"
    filled_qty: int = 0
    cancelled_qty: int = 0
    fill_value: Decimal = Decimal(0)
    status_message: str = ""
    triggered: bool = False           # SL-M only: the trigger has been crossed
    marketable: bool | None = None    # LIMIT only: set once, on the first eligible tick
    # Timestamp of this row's LAST state change (set by _postback). Reported by orders() so a read
    # never restamps the book: the reconciler compares broker timestamps against platform rows.
    last_update_at: datetime | None = None
    # Participation budget for ONE minute: how much of `minute_key`'s volume this order has already
    # taken. Reset when the symbol's minute rolls. Per (order, minute), not per tick.
    minute_key: datetime | None = None
    minute_filled: int = 0

    @property
    def average_price(self) -> Decimal:
        if self.filled_qty <= 0:
            return Decimal(0)
        return self.fill_value / Decimal(self.filled_qty)

    @property
    def remaining(self) -> int:
        return self.quantity - self.filled_qty


@dataclass
class _Gtt:
    """A GTT trigger (single or two-leg OCO). Kite semantics: a fired leg places a LIMIT order."""

    gtt_id: int
    tradingsymbol: str
    exchange: str
    trigger_type: str
    trigger_values: list[Decimal]
    last_price: Decimal
    orders: list[dict[str, Any]]
    created_at: datetime
    #: ``"active"`` -> ``"triggered"`` on a fire, as Kite reports them — PLUS one paper-only terminal
    #: status, ``"failed"``: the trigger crossed and the leg could NOT be booked (see
    #: :meth:`PaperBroker._match_gtts`). Latched rather than left ``active`` so the trigger is not
    #: retried every tick; the paper ProtectionManager maps it to a PROTECTION_FAILED reason.
    status: str = "active"


@dataclass
class _Fill:
    symbol: str
    product: str
    side: str
    quantity: int
    price: Decimal
    #: The TRADE's own time (the tick's ``exchange_ts``), not the engine's wall clock: it scopes a
    #: fill to a session in :meth:`PaperBroker.positions`.
    ts: datetime


@dataclass
class _SymbolState:
    """Per-symbol market-data state the fill engine needs. All updated in ``on_tick``/``on_bar``."""

    last_ltp: Decimal | None = None
    session_date: date | None = None          # date of the last tick seen (drives the session roll)
    minute: datetime | None = None            # start of the minute the running volume belongs to
    #: HIGH-WATER cumulative volume, carried ACROSS minutes and cleared at the session roll
    #: (cumulative volume is a per-DAY counter). Same baseline as
    #: ``engine.marketdata.bar_builder.BarBuilder._volume_delta``: the only value a restatement can
    #: be recognised against once the restated tick has itself become "the last tick".
    last_cum: int | None = None
    #: The high-water mark as of the START of ``minute`` — the intra-minute delta is measured from
    #: HERE, never from the raw cumulative of whatever tick opened the minute.
    minute_start_cum: int = 0
    running_volume: int = 0
    last_bar_volume: int | None = None
    #: True once ANY tick for this symbol has reported a non-zero cumulative volume. Sticky: tells
    #: "this feed carries volume, this minute has none" from "no volume at all" once a counter
    #: resets to 0.
    saw_volume: bool = False
    #: Set on a tick whose cumulative volume printed BELOW the high-water mark (a restatement /
    #: resubscribe re-baseline). That tick offers zero available volume — never an uncapped fill.
    volume_reset: bool = False
    resting: list[str] = field(default_factory=list)
    gtts: list[int] = field(default_factory=list)


def _as_params(req: ReqLike) -> dict[str, Any]:
    """Coerce an OMS-built request (dict OR object) into a params dict — same permissive rule as
    :meth:`engine.broker.kite_client.KiteClient._as_params`, so both routings accept the same
    payloads."""
    if isinstance(req, dict):
        return dict(req)
    for attr in ("model_dump", "dict"):
        dump = getattr(req, attr, None)
        if callable(dump):
            return dict(dump())
    return dict(vars(req))


def _gtt_leg_request(
    symbol: str, exchange: str, trigger: Decimal, leg: dict[str, Any]
) -> dict[str, Any]:
    """Build the order request a GTT leg fires as — the ONE place the leg defaults are applied.

    Shared by :meth:`PaperBroker._validate_gtt` (placement) and :meth:`PaperBroker._fire_gtt_leg`
    (fire) so "validated at placement" means "will book at fire time". Kite semantics: a fired GTT
    places a LIMIT order, at the leg's own price when it has one and otherwise at the trigger.
    """
    out = dict(leg)
    out.setdefault("tradingsymbol", symbol)
    out.setdefault("exchange", exchange)
    out["variety"] = "regular"
    out["order_type"] = "LIMIT"
    if out.get("price") is None:
        out["price"] = trigger
    return out


def _round_to_tick(price: Decimal, tick_size: Decimal, side: str) -> Decimal:
    """Round a fill price to the instrument's tick, ALWAYS against the taker.

    A buyer rounds up, a seller rounds down — at most one tick of extra cost, never a free
    fraction. A non-positive tick size (unknown instrument) passes the price through unrounded.
    """
    if tick_size <= 0:
        return price
    steps = price / tick_size
    rounding = ROUND_CEILING if side == "BUY" else ROUND_FLOOR
    return (steps.to_integral_value(rounding=rounding) * tick_size).quantize(tick_size)


class PaperBroker:
    """A deterministic paper implementation of :class:`~engine.paper.surface.BrokerSurface`.

    Parameters
    ----------
    clock:
        The single IST :class:`~engine.core.clock.Clock`. Used for placement/cancel timestamps
        and for ``eligible_at = now + latency``. Replay injects a tick-driven time source, which is
        what makes the golden replay day byte-identical.
    publish:
        The event-bus publish callable (``EventBus.publish``). Every state change goes out as
        ``publish(topic, OrderUpdateFrame(data=...))``.
    fill_model:
        :class:`~engine.paper.fill_model.FillModelConfig` — latency, buckets/k, half-spread rules.
    tick_size:
        ``symbol -> Decimal`` tick size (``InstrumentStore.tick_size``). Injected
        rather than imported so ``engine.paper`` keeps its ``core``-only dependency.
    rng_seed:
        Seed for the injected-rejection RNG. The same seed replays the same rejections.
    rejection_rate:
        Probability that a ``place_order`` is rejected outright (base 0.5%). Chaos tests
        raise it; ``0.0`` disables it for fill-rule tests.
    participation_cap:
        Max share of the current 1-minute bar volume one fill may take (10%).
    order_guard:
        Same contract as ``KiteClient``: called synchronously with the caller's ``intent`` BEFORE
        anything else on every order/GTT mutating path, and whatever it raises propagates untouched
        with the book left completely unmodified (order-surface predicate).
    available_margin:
        The static balance :meth:`margins` reports (stub; margin math lives in the OMS).
    order_seq / gtt_seq:
        The last sequence number already issued; the next id is ``ORDER_ID_BASE + order_seq + 1``
        (``GTT_ID_BASE`` for GTTs). A restarted broker must be seeded past every stored paper id,
        or a reused id is absorbed by the OMS as a terminal duplicate.
    topic:
        The postback topic. Production passes ``PAPER_ORDER_UPDATE_TOPIC``.
    """

    def __init__(
        self,
        clock: Clock,
        publish: Callable[[str, BaseModel], None],
        fill_model: FillModelConfig,
        tick_size: Callable[[str], Decimal],
        rng_seed: int,
        *,
        rejection_rate: float = 0.005,
        participation_cap: float = 0.10,
        order_guard: Callable[[str], None] | None = None,
        available_margin: Decimal = Decimal("1000000"),
        order_seq: int = 0,
        gtt_seq: int = 0,
        topic: str = ORDER_UPDATE_TOPIC,
    ) -> None:
        self._clock = clock
        self._publish = publish
        self._fm = fill_model
        self._tick_size = tick_size
        self._rng = random.Random(rng_seed)
        self._rejection_rate = rejection_rate
        self._participation = Decimal(str(participation_cap))
        self._order_guard = order_guard
        self._available_margin = available_margin

        self._orders: dict[str, _Order] = {}
        self._gtt_book: dict[int, _Gtt] = {}
        self._fills: list[_Fill] = []
        self._symbols: dict[str, _SymbolState] = {}
        self._ltp_by_token: dict[int, Decimal] = {}
        self._gap_pending: set[str] = set()
        # The latest tick date seen on ANY symbol — what scopes positions()["day"] to the current
        # session. Broker-wide rather than per-symbol because Kite's day book is account-wide.
        self._session_date: date | None = None
        self._sigma = SigmaEstimator()
        self._next_order_seq = order_seq
        self._next_gtt_seq = gtt_seq
        self._topic = topic

    @property
    def topic(self) -> str:
        return self._topic

    # ---------------------------------------------------------------- internals: ids / state
    def _state(self, symbol: str) -> _SymbolState:
        st = self._symbols.get(symbol)
        if st is None:
            st = _SymbolState()
            self._symbols[symbol] = st
        return st

    def _mint_order_id(self) -> str:
        self._next_order_seq += 1
        return f"{ORDER_ID_BASE + self._next_order_seq:015d}"

    def _mint_gtt_id(self) -> int:
        self._next_gtt_seq += 1
        return GTT_ID_BASE + self._next_gtt_seq

    def _guard(self, intent: str) -> None:
        """Run the order-surface guard FIRST, before validation, id minting, the RNG draw, or any
        book mutation — a blocked call must leave the broker bit-identical."""
        if self._order_guard is not None:
            self._order_guard(intent)

    # ---------------------------------------------------------------- internals: postbacks
    def _data(self, order: _Order, at: datetime) -> dict[str, Any]:
        """The Kite-shaped postback payload. Field names are PINNED to what the OMS parser reads —
        renaming one here silently breaks the shared live/paper parser."""
        pending = 0 if order.status in TERMINAL_STATUSES else (
            order.quantity - order.filled_qty - order.cancelled_qty
        )
        return {
            "order_id": order.order_id,
            "status": order.status,
            "filled_quantity": order.filled_qty,
            "pending_quantity": max(pending, 0),
            "cancelled_quantity": order.cancelled_qty,
            "average_price": order.average_price,
            "status_message": order.status_message,
            "order_timestamp": at.strftime(_TS_FORMAT),
            "tradingsymbol": order.tradingsymbol,
            "transaction_type": order.transaction_type,
            "product": order.product,
            "order_type": order.order_type,
            "quantity": order.quantity,
            "price": order.price,
            "trigger_price": order.trigger_price,
            "variety": order.variety,
            "tag": order.tag,
            # Beyond the pinned set, but present on every real Kite postback and free to carry:
            "exchange": order.exchange,
            "placed_by": "paper",
        }

    def _postback(self, order: _Order, at: datetime) -> None:
        """Publish exactly one postback for one state change, and stamp the row with its time.

        This is the single choke point for "the order changed", so it is also the only place
        ``last_update_at`` is written — a state change that skipped the postback would be invisible
        to the OMS anyway.

        Raises whatever ``publish`` raises. That is correct on the CALLER-facing paths
        (``place_order`` / ``modify_order`` / ``cancel_order``): the caller is awaiting an answer and
        can be told. Inside ``on_tick`` there is no caller, so every postback there goes through
        :meth:`_publish_guarded` instead.
        """
        order.last_update_at = at
        self._publish(self._topic, OrderUpdateFrame(data=self._data(order, at)))

    def _publish_guarded(self, order: _Order, at: datetime) -> None:
        """The ONE postback path for everything emitted from inside ``on_tick``.

        ``publish`` is injected, so a bus that throws must not escape the hot path: it would take the
        feed down for every symbol (in the lapse sweep, leaving every symbol after the raiser
        resting forever). Guarantees, in all three loops and :meth:`_reject_internal`:

        * the book change is applied BEFORE the publish is attempted, so a failed publish loses the
          NOTIFICATION and never unwinds the state change it reported;
        * one log line per affected order, then carry on with the rest of the book.
        """
        try:
            self._postback(order, at)
        except Exception:
            _log.exception(
                "paper.order.postback_failed",
                order_id=order.order_id,
                symbol=order.tradingsymbol,
                status=order.status,
            )

    # ---------------------------------------------------------------- validation
    @staticmethod
    def _validate(params: dict[str, Any]) -> dict[str, Any]:
        """Validate the Kite fields the fill engine needs. Raises :class:`PaperOrderError`.

        v1 accepts only what it can simulate honestly: ``variety="regular"`` and MARKET / LIMIT /
        SL-M. Refusing SL / CO / AMO / iceberg is deliberate — approximating them would produce a
        paper number the live tier cannot reproduce.
        """
        variety = params.get("variety") or "regular"
        if variety not in _VALID_VARIETIES:
            raise PaperOrderError(
                f"unsupported variety {variety!r}: paper v1 simulates {sorted(_VALID_VARIETIES)} only"
            )
        symbol = params.get("tradingsymbol")
        if not symbol or not isinstance(symbol, str):
            raise PaperOrderError(f"tradingsymbol is required, got {symbol!r}")
        side = params.get("transaction_type")
        if side not in _VALID_SIDES:
            raise PaperOrderError(f"transaction_type must be BUY or SELL, got {side!r}")
        order_type = params.get("order_type")
        if order_type not in _VALID_ORDER_TYPES:
            raise PaperOrderError(
                f"order_type must be one of {sorted(_VALID_ORDER_TYPES)}, got {order_type!r}"
            )
        product = params.get("product")
        if product not in _VALID_PRODUCTS:
            raise PaperOrderError(f"product must be MIS or CNC, got {product!r}")
        quantity = params.get("quantity")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            raise PaperOrderError(f"quantity must be a positive integer, got {quantity!r}")
        price = params.get("price")
        if order_type == "LIMIT" and (price is None or Decimal(str(price)) <= 0):
            raise PaperOrderError(f"a LIMIT order requires a positive price, got {price!r}")
        trigger = params.get("trigger_price")
        if order_type == "SL-M" and (trigger is None or Decimal(str(trigger)) <= 0):
            raise PaperOrderError(f"an SL-M order requires a positive trigger_price, got {trigger!r}")
        return {
            "variety": variety,
            "exchange": params.get("exchange") or "NSE",
            "tradingsymbol": symbol,
            "transaction_type": side,
            "order_type": order_type,
            "product": product,
            "quantity": quantity,
            "price": None if price is None else Decimal(str(price)),
            "trigger_price": None if trigger is None else Decimal(str(trigger)),
            "tag": params.get("tag"),
        }

    def _register(self, fields: dict[str, Any]) -> _Order:
        """Mint an id, book the order and rest it — the whole state change, and NO postback.

        Separate from the publish so the callers can order it differently: :meth:`place_order`
        publishes unguarded (its caller awaits the answer), while :meth:`_fire_gtt_leg` marks the GTT
        ``triggered`` first and then publishes through :meth:`_publish_guarded`, so a raising bus
        cannot split "leg resting" from "GTT fired".
        """
        now = self._clock.now()
        order = _Order(
            order_id=self._mint_order_id(),
            eligible_at=now + self._fm.latency(),
            placed_at=now,
            **fields,
        )
        self._orders[order.order_id] = order
        self._state(order.tradingsymbol).resting.append(order.order_id)
        return order

    # ---------------------------------------------------------------- BrokerSurface: orders
    async def place_order(self, req: ReqLike, intent: str = "entry") -> str:
        """Validate, book and acknowledge an order; returns the paper ``order_id``.

        Publishes ONE postback: ``OPEN`` for an accepted order (which then rests until a tick fills
        it) or ``REJECTED`` for an injected rejection — never OPEN-then-REJECTED, so the OMS sees
        one transition per call. The order becomes fillable only at ``now + latency_ms``.
        """
        self._guard(intent)
        fields = self._validate(_as_params(req))
        # Draw on EVERY placement, before the branch, so the rejection sequence is a pure function
        # of (seed, order count) — timing, config and order contents cannot shift it.
        roll = self._rng.random()
        if roll < self._rejection_rate:
            now = self._clock.now()
            rejected = _Order(
                order_id=self._mint_order_id(),
                eligible_at=now,
                placed_at=now,
                status="REJECTED",
                status_message="paper: injected rejection (seeded rng)",
                **fields,
            )
            self._orders[rejected.order_id] = rejected      # rejected orders stay in the orderbook
            self._postback(rejected, now)
            _log.info("paper.order.rejected", order_id=rejected.order_id, intent=intent)
            return rejected.order_id
        order = self._register(fields)
        self._postback(order, order.placed_at)      # unguarded: the caller is awaiting this answer
        _log.info(
            "paper.order.placed",
            order_id=order.order_id,
            intent=intent,
            symbol=order.tradingsymbol,
            order_type=order.order_type,
            quantity=order.quantity,
        )
        return order.order_id

    async def modify_order(self, order_id: str, req: ReqLike, intent: str = "risk_reducing") -> str:
        """Replace ``price`` / ``trigger_price`` / ``quantity`` on a resting order.

        Quantity can never go below ``filled_qty`` (the filled portion is preserved — shrinking
        past it would leak an already-open, protected position out of the book), and shrinking to
        EXACTLY ``filled_qty`` COMPLETES the order (the "take what I got, drop the rest" amendment):
        left OPEN it would rest forever with ``pending_quantity`` 0, a row no transition can retire
        and one the square-off coordination would treat as live protection.
        Modifying a terminal order raises, as Kite does. The 20-modification self-cap is the
        OMS's counter, not the broker's.
        """
        self._guard(intent)
        order = self._live_order(order_id)
        params = _as_params(req)
        if "quantity" in params and params["quantity"] is not None:
            new_qty = int(params["quantity"])
            if new_qty <= 0:
                raise PaperOrderError(
                    f"cannot modify {order_id} to quantity {new_qty}: quantity must be positive"
                )
            if new_qty < order.filled_qty:
                raise PaperOrderError(
                    f"cannot modify {order_id} to quantity {new_qty}: {order.filled_qty} already "
                    "filled (preserves filled_qty)"
                )
            order.quantity = new_qty
        if "price" in params and params["price"] is not None:
            order.price = Decimal(str(params["price"]))
        if "trigger_price" in params and params["trigger_price"] is not None:
            order.trigger_price = Decimal(str(params["trigger_price"]))
        if order.filled_qty >= order.quantity:
            order.status = "COMPLETE"
            order.status_message = "paper: completed by modify down to the filled quantity"
            self._unrest(order)
        else:
            order.status_message = "paper: modified"
        self._postback(order, self._clock.now())
        return order.order_id

    async def cancel_order(
        self, order_id: str, variety: str = "regular", intent: str = "risk_reducing"
    ) -> str:
        """Cancel a resting order: ``cancelled_quantity`` = the residual, ``filled_quantity`` kept.

        The PARTIALLY_FILLED -> CANCEL_PENDING -> CANCELLED path: the filled portion stays an open,
        protectable position and the residual is recorded as a cancel, never an orphan or a
        quantity leak.

        TWO refusals, signals the square-off coordination reads (live Kite raises in exactly these
        cases and paper must too):

        * an already-terminal order — "the protective order already filled, do NOT send the market
          exit";
        * an SL-M whose trigger has CROSSED: it has left the trigger book and Kite will not take it
          back. The refusal is how the square-off learns "the stop is going to fill, do not also send
          a market exit"; accepting the cancel would hide that double-exit hazard.

        ``variety`` is accepted for call-shape parity with ``KiteClient`` and ignored: paper v1
        books only ``regular`` orders (see :meth:`_validate`).
        """
        self._guard(intent)
        order = self._live_order(order_id)
        if order.order_type == "SL-M" and order.triggered:
            raise PaperOrderError(
                f"order {order_id} is already triggered and cannot be cancelled "
                "(SL-M past its trigger is on its way to market)"
            )
        self._terminate(order, "CANCELLED", "paper: cancelled by client")
        self._postback(order, self._clock.now())
        return order.order_id

    def _live_order(self, order_id: str) -> _Order:
        order = self._orders.get(order_id)
        if order is None:
            raise PaperOrderError(f"unknown order_id {order_id!r}")
        if order.status in TERMINAL_STATUSES:
            raise PaperOrderError(
                f"order {order_id} is terminal ({order.status}) and cannot be modified or cancelled"
            )
        return order

    def _unrest(self, order: _Order) -> None:
        resting = self._state(order.tradingsymbol).resting
        if order.order_id in resting:
            resting.remove(order.order_id)

    def _terminate(self, order: _Order, status: str, message: str) -> None:
        """Move ``order`` to a terminal state with its residual cancelled: ``cancelled_qty`` absorbs
        ``order.remaining`` while ``filled_qty`` is PRESERVED (client cancel, internal-error reject
        with fills, end-of-day lapse). Callers run their own postback and logging afterwards."""
        order.cancelled_qty = order.remaining
        order.status = status
        order.status_message = message
        self._unrest(order)

    async def orders(self) -> list:
        """The whole paper orderbook (resting AND terminal rows), in the postback shape — so the
        reconciler reads one schema whichever routing produced it.

        Each row carries ITS OWN last-update time, never the read time: a fill is stamped with the
        trade's ``exchange_ts`` and a client action with the ``Clock`` time it happened at.
        """
        return [self._data(o, o.last_update_at or o.placed_at) for o in self._orders.values()]

    async def positions(self) -> Any:
        """Positions per (symbol, product), Kite-shaped: ``net`` = lifetime, ``day`` = this session.

        ``quantity`` is signed (long positive, short negative) and ``average_price`` is the average
        of the fills on the side the position sits on — a simplification of Kite's net-average that
        is exact for the open-then-close flow the OMS produces.

        ``day`` is scoped to the CURRENT session date: reporting the net book as the day book would
        make a carried position look opened today, and the square-off scheduler reads that to decide
        what to flatten.
        """
        day_fills = (
            [f for f in self._fills if f.ts.date() == self._session_date]
            if self._session_date is not None
            else []
        )
        return {"net": self._position_rows(self._fills), "day": self._position_rows(day_fills)}

    def _position_rows(self, fills: list[_Fill]) -> list[dict[str, Any]]:
        agg: dict[tuple[str, str], dict[str, Any]] = {}
        for fill in fills:
            key = (fill.symbol, fill.product)
            row = agg.setdefault(
                key,
                {
                    "tradingsymbol": fill.symbol,
                    "exchange": "NSE",
                    "product": fill.product,
                    "buy_quantity": 0,
                    "sell_quantity": 0,
                    "_buy_value": Decimal(0),
                    "_sell_value": Decimal(0),
                },
            )
            if fill.side == "BUY":
                row["buy_quantity"] += fill.quantity
                row["_buy_value"] += fill.price * fill.quantity
            else:
                row["sell_quantity"] += fill.quantity
                row["_sell_value"] += fill.price * fill.quantity

        rows: list[dict[str, Any]] = []
        for (symbol, _product), row in agg.items():
            buy_q, sell_q = row["buy_quantity"], row["sell_quantity"]
            net = buy_q - sell_q
            if net > 0:
                avg = row["_buy_value"] / Decimal(buy_q)
            elif net < 0:
                avg = row["_sell_value"] / Decimal(sell_q)
            else:
                avg = Decimal(0)
            rows.append(
                {
                    "tradingsymbol": row["tradingsymbol"],
                    "exchange": row["exchange"],
                    "product": row["product"],
                    "quantity": net,
                    "average_price": avg,
                    "buy_quantity": buy_q,
                    "sell_quantity": sell_q,
                    "buy_price": (row["_buy_value"] / Decimal(buy_q)) if buy_q else Decimal(0),
                    "sell_price": (row["_sell_value"] / Decimal(sell_q)) if sell_q else Decimal(0),
                    "last_price": self._state(symbol).last_ltp or Decimal(0),
                }
            )
        return rows

    async def holdings(self) -> list:
        """The CNC book (stub): net-long CNC positions, presented as Kite holdings rows."""
        pos = await self.positions()
        return [
            {
                "tradingsymbol": row["tradingsymbol"],
                "exchange": row["exchange"],
                "product": "CNC",
                "quantity": row["quantity"],
                "average_price": row["average_price"],
                "last_price": row["last_price"],
            }
            for row in pos["net"]
            if row["product"] == "CNC" and row["quantity"] > 0
        ]

    async def margins(self) -> Any:
        """Static margin stub. Real span/exposure math is not simulated in v1 (margin pre-checks are
        the OMS's job); the balance is constructor-configurable so chaos tests can starve it."""
        return {
            "equity": {
                "enabled": True,
                "available": {
                    "live_balance": self._available_margin,
                    "cash": self._available_margin,
                    "opening_balance": self._available_margin,
                },
                "utilised": {"debits": Decimal(0), "m2m_realised": Decimal(0)},
            },
            "commodity": {"enabled": False},
        }

    def reseed_margin(self, available: Decimal) -> None:
        """A new paper epoch starts from the capital base."""
        self._available_margin = available

    # ---------------------------------------------------------------- BrokerSurface: GTT
    async def place_gtt(self, req: ReqLike) -> int:
        """Create a single or two-leg (OCO) GTT; returns the trigger id.

        Legs are matched 1:1 with ``trigger_values``, and each leg's direction is fixed at placement
        by its trigger relative to ``last_price`` (below the mark = a downside/stop trigger, at-or-
        above = an upside/target trigger). GTT calls always carry ``intent="risk_reducing"``: in
        this platform a GTT is a protective instrument, never an entry.
        """
        self._guard("risk_reducing")
        gtt = self._validate_gtt(_as_params(req))
        self._gtt_book[gtt.gtt_id] = gtt
        self._state(gtt.tradingsymbol).gtts.append(gtt.gtt_id)
        _log.info("paper.gtt.placed", gtt_id=gtt.gtt_id, symbol=gtt.tradingsymbol)
        return gtt.gtt_id

    async def modify_gtt(self, gtt_id: int, req: ReqLike) -> int:
        """Replace an ACTIVE GTT's triggers/legs in place (the ex-date adjust path). A
        triggered GTT cannot be modified — it has already fired and there is nothing to re-arm."""
        self._guard("risk_reducing")
        existing = self._gtt_book.get(gtt_id)
        if existing is None:
            raise PaperOrderError(f"unknown gtt trigger_id {gtt_id!r}")
        if existing.status != "active":
            raise PaperOrderError(f"gtt {gtt_id} is {existing.status} and cannot be modified")
        replacement = self._validate_gtt(_as_params(req), gtt_id=gtt_id)
        replacement.created_at = existing.created_at
        self._gtt_book[gtt_id] = replacement
        return gtt_id

    async def delete_gtt(self, gtt_id: int) -> None:
        """Delete an ACTIVE GTT. Kite drops it from the account entirely, so a deleted id is
        thereafter ``unknown`` (a second delete raises) and can never fire again; a GTT that is no
        longer active cannot be deleted, as on Kite."""
        self._guard("risk_reducing")
        existing = self._gtt_book.get(gtt_id)
        if existing is None:
            raise PaperOrderError(f"unknown gtt trigger_id {gtt_id!r}")
        if existing.status != "active":
            raise PaperOrderError(f"gtt {gtt_id} is {existing.status} and cannot be deleted")
        del self._gtt_book[gtt_id]
        gtts = self._state(existing.tradingsymbol).gtts
        if gtt_id in gtts:
            gtts.remove(gtt_id)

    def restore(self, gtt_rows: Iterable[Mapping[str, Any]]) -> list[int]:
        """Re-book the ACTIVE GTTs among ``gtt_rows`` (plain dicts of the 0016 ``gtts`` columns),
        each with its stored ``last_price`` so every leg keeps its trigger direction. Other states
        are skipped. All rows are validated before any is booked; returns the restored ids."""
        restored: dict[int, _Gtt] = {}
        for row in gtt_rows:
            if str(row.get("state") or "").lower() != "active":
                continue
            gtt_id = int(row["gtt_id"])
            if gtt_id in self._gtt_book or gtt_id in restored:
                raise PaperOrderError(f"gtt {gtt_id} is already in the book")
            legs = [(row["trigger_low"], row.get("stop_limit"))]
            if row.get("trigger_high") is not None:
                legs.append((row["trigger_high"], row.get("target_limit")))
            leg = {
                "transaction_type": row.get("side"),
                "quantity": row.get("qty"),
                "product": row.get("product"),
                "order_type": "LIMIT",
            }
            gtt = self._validate_gtt(
                {
                    "tradingsymbol": row.get("symbol"),
                    "last_price": row.get("last_price"),
                    "trigger_values": [trigger for trigger, _ in legs],
                    "orders": [
                        {**leg, "price": None if price is None else Decimal(str(price))}
                        for _, price in legs
                    ],
                },
                gtt_id=gtt_id,
            )
            if row.get("created_at"):
                gtt.created_at = datetime.fromisoformat(str(row["created_at"]))
            restored[gtt_id] = gtt
        for gtt_id, gtt in restored.items():
            self._gtt_book[gtt_id] = gtt
            self._state(gtt.tradingsymbol).gtts.append(gtt_id)
            self._next_gtt_seq = max(self._next_gtt_seq, gtt_id - GTT_ID_BASE)
        return list(restored)

    async def gtts(self) -> list:
        """All GTT triggers, Kite-shaped (``id`` / ``status`` / ``condition`` / ``orders``).

        ``status`` is ``"active"`` or ``"triggered"`` as Kite reports them, and — paper only — the
        terminal ``"failed"``: the trigger crossed but its leg could not be booked, so the
        protection this GTT represented does NOT exist and was not retried. The paper
        ProtectionManager (engine.oms.protection) turns that into a PROTECTION_FAILED reason; a
        reader that only knows Kite's vocabulary must not bucket it with ``active``.
        """
        return [
            {
                "id": g.gtt_id,
                "status": g.status,
                "type": g.trigger_type,
                "created_at": g.created_at,
                "condition": {
                    "exchange": g.exchange,
                    "tradingsymbol": g.tradingsymbol,
                    "trigger_values": list(g.trigger_values),
                    "last_price": g.last_price,
                },
                "orders": [dict(o) for o in g.orders],
            }
            for g in self._gtt_book.values()
        ]

    def _validate_gtt(self, params: dict[str, Any], gtt_id: int | None = None) -> _Gtt:
        symbol = params.get("tradingsymbol")
        if not symbol or not isinstance(symbol, str):
            raise PaperOrderError(f"gtt tradingsymbol is required, got {symbol!r}")
        triggers = params.get("trigger_values") or []
        legs = params.get("orders") or []
        if not triggers or not legs:
            raise PaperOrderError("a gtt needs at least one trigger_value and one leg order")
        if len(triggers) != len(legs):
            raise PaperOrderError(
                f"gtt trigger_values ({len(triggers)}) and orders ({len(legs)}) must be 1:1 — "
                "a two-leg OCO has exactly two of each"
            )
        last_price = params.get("last_price")
        if last_price is None:
            raise PaperOrderError("a gtt needs last_price (it decides each leg's trigger direction)")
        exchange = params.get("exchange") or "NSE"
        trigger_values = [Decimal(str(t)) for t in triggers]
        leg_params = [_as_params(leg) for leg in legs]

        # Validate every leg HERE, at placement, exactly as the fire path will build it: a malformed
        # leg first seen inside ``on_tick`` would raise out of the hot path with the GTT already
        # flipped to `triggered` and lost. Validation belongs where the caller can still be told.
        for index, leg in enumerate(leg_params):
            try:
                self._validate(_gtt_leg_request(symbol, exchange, trigger_values[index], leg))
            except PaperOrderError as exc:
                raise PaperOrderError(f"gtt leg {index} is invalid: {exc}") from exc

        return _Gtt(
            gtt_id=gtt_id if gtt_id is not None else self._mint_gtt_id(),
            tradingsymbol=symbol,
            exchange=exchange,
            trigger_type=params.get("trigger_type") or ("two-leg" if len(triggers) > 1 else "single"),
            trigger_values=trigger_values,
            last_price=Decimal(str(last_price)),
            orders=leg_params,
            created_at=self._clock.now(),
        )

    # ---------------------------------------------------------------- BrokerSurface: quote
    async def ltp(self, tokens: list[int]) -> dict[int, Decimal]:
        """Last traded price per token, from the ticks this broker has seen. Unknown tokens are
        omitted (Kite omits instruments it cannot quote rather than inventing a price)."""
        return {t: self._ltp_by_token[t] for t in tokens if t in self._ltp_by_token}

    # ---------------------------------------------------------------- market data in
    def flag_gap(self, symbol: str) -> None:
        """Tell the broker the NEXT tick for ``symbol`` follows a data gap (replay harness hook).

        The session's first tick is detected automatically; this covers the other gap sources — a
        halted/resumed instrument, a backfilled span, a synthetically edited replay day. On a
        flagged tick an SL-M that triggers beyond its stop fills at the print, and the postback says
        so, which is how the "gap through a stop" case stays visible in the ledger.
        """
        self._gap_pending.add(symbol)

    def on_bar(self, bar: Bar) -> None:
        """Consume a finalized 1-minute bar: it feeds sigma and backs the participation cap when a
        minute has not printed volume yet. Cheap and synchronous (hot path)."""
        st = self._state(bar.symbol)
        st.last_bar_volume = bar.volume
        self._sigma.on_close(bar.symbol, bar.close)

    def on_tick(self, tick: Tick) -> None:
        """Drive the book from one tick. Synchronous, allocation-light, no I/O (hot path).

        Ordering is deliberate:

        1. the SESSION ROLL is handled first — every order still resting from the previous session
           lapses BEFORE this tick can match it (Kite cancels unfilled DAY orders at the close);
        2. resting orders are matched against the tick;
        3. GTTs are evaluated last. An order a GTT fires on this tick therefore cannot also fill on
           it — it rests with the full simulated latency ahead of it, which is the conservative
           reading of "a trigger is not a fill".
        """
        symbol = tick.tradingsymbol
        st = self._state(symbol)
        st.last_ltp = tick.ltp
        self._ltp_by_token[tick.instrument_token] = tick.ltp

        tick_date = tick.exchange_ts.date()
        if self._session_date is None or tick_date > self._session_date:
            rolled = self._session_date is not None
            self._session_date = tick_date
            if rolled:
                # BROKER-WIDE, not per-symbol: Kite cancels every unfilled DAY order at the close on
                # every symbol at once; an order on a name that never prints again would rest forever.
                self._lapse_all(tick_date, tick.exchange_ts)

        gap = symbol in self._gap_pending
        self._gap_pending.discard(symbol)
        if st.session_date != tick_date:
            st.session_date = tick_date
            gap = True                      # first tick of a session: the open print may have gapped
            # Cumulative volume restarts each day: carried over, tomorrow's small opening cumulative
            # would read as a restatement and zero this symbol's participation base all session.
            st.last_cum = None
            st.minute = None
            st.minute_start_cum = 0
            st.running_volume = 0

        cum = tick.volume_traded
        if cum > 0:
            st.saw_volume = True
        minute = tick.exchange_ts.replace(second=0, microsecond=0)
        # The BarBuilder rule (``bar_builder.py`` ``_volume_delta``): ``volume_traded`` is the
        # CUMULATIVE day count, so a print BELOW the high-water mark is a restatement. Tested BEFORE
        # the minute-roll branch: a restatement landing on a minute boundary would otherwise
        # re-baseline the new minute at the restated (tiny) cumulative, and the correction back up
        # would read as a delta of the whole day's volume.
        st.volume_reset = st.last_cum is not None and cum < st.last_cum
        if st.minute != minute:
            st.minute = minute
            # The new minute starts at the WATER MARK, not at this tick's cumulative: that is what
            # makes the corrective jump back up yield the true delta over the last real print.
            st.minute_start_cum = st.last_cum if st.last_cum is not None else cum
        st.last_cum = cum if st.last_cum is None else max(cum, st.last_cum)
        st.running_volume = max(st.last_cum - st.minute_start_cum, 0)

        for order_id in list(st.resting):
            order = self._orders.get(order_id)
            if order is None or order.status in TERMINAL_STATUSES:
                continue
            # NOTHING escapes on_tick: the per-order body reaches injected callables (``tick_size``,
            # ``publish``) and Decimal math. Log once, terminate THIS order with a postback the OMS
            # can act on, carry on with the rest of the book.
            try:
                self._match(order, tick, st, gap)
            except Exception:
                _log.exception(
                    "paper.order.match_failed",
                    order_id=order.order_id,
                    symbol=order.tradingsymbol,
                    order_type=order.order_type,
                )
                self._reject_internal(order, tick.exchange_ts)

        self._match_gtts(tick, st)

    def _reject_internal(self, order: _Order, at: datetime) -> None:
        """Terminate an order whose match raised, and TELL the OMS.

        Silently dropping it would leave the OMS holding a row it believes is resting (an exit that
        will never fill, read by the square-off coordination as live protection). The status is
        terminal, so this fires once however many ticks follow; the postback goes through
        :meth:`_publish_guarded`.

        The residual always lands in ``cancelled_quantity`` first, so ``filled + cancelled ==
        quantity`` holds on the frame (a leak would leave the OMS's position arithmetic short):

        * ``filled_qty > 0`` → **CANCELLED** with the residual cancelled. REJECTED means "no part of
          this order ever traded" to the OMS parser; on a partly filled row it would strand a live
          position the platform believes it does not hold.
        * nothing filled → **REJECTED**.
        """
        status = "CANCELLED" if order.filled_qty > 0 else "REJECTED"
        self._terminate(order, status, "paper: internal error")
        self._publish_guarded(order, at)

    def _lapse_all(self, session_date: date, at: datetime) -> None:
        """Terminate EVERY symbol's orders left resting from a PREVIOUS session, on the first tick
        of a new session.

        Kite cancels unfilled DAY orders at the close, so a paper order surviving the roll would fill
        on the next open print, a fill that could never have happened. Emitted as CANCELLED with the
        residual in ``cancelled_quantity`` (the filled portion is preserved as in a client cancel).

        The sweep covers the whole book, keyed on the BROKER's session date (the real event is
        account-wide; a name that never prints again must not rest forever). Every resting order's
        symbol has a state (``_register`` creates it), so iterating the states covers the book.

        Scoped by ``placed_at``, not by "resting when the roll was noticed": the roll is detected on
        the new session's FIRST TICK, after the OMS may already have placed today's orders
        (pre-open staging, a late first print); only orders placed on an earlier date lapse.

        Stamped with the ROLL tick's time, not the previous close: stamping backwards would put a
        postback before frames the OMS has already recorded. GTTs are untouched (good-till-triggered;
        the square-off coordination relies on that to hold protection).
        """
        for st in list(self._symbols.values()):
            for order_id in list(st.resting):
                order = self._orders.get(order_id)
                if order is None or order.status in TERMINAL_STATUSES:
                    continue
                if order.placed_at.date() >= session_date:
                    continue                  # placed today: not a leftover from the last session
                self._terminate(order, "CANCELLED", "paper: lapsed at session close")
                self._publish_guarded(order, at)
                _log.info(
                    "paper.order.lapsed",
                    order_id=order.order_id,
                    symbol=order.tradingsymbol,
                    cancelled=order.cancelled_qty,
                )

    # ---------------------------------------------------------------- fill engine
    def _effective_volume(self, st: _SymbolState) -> int | None:
        """Volume backing the participation cap, in precedence order:

        0. ``0`` on a tick whose cumulative volume prints BELOW the high-water mark. Nothing is
           known to have traded on it, and falling through to the "no volume at all" branch would
           remove the cap entirely.

           **This branch can black a symbol out for the rest of the session, and that is intended.**
           ``volume_reset`` is recomputed per tick against the high-water cumulative, so a feed that
           re-baselines low and never climbs back over the mark (a resubscribe on a thin name late
           in the day) takes this branch on EVERY remaining tick: no order on that symbol fills
           again until the session roll clears ``last_cum``; it also short-circuits branch 2. The
           alternative is sizing fills off a baseline the exchange has disowned (a minute's tape
           inflated ~100x). Under-filling costs the paper-expectancy gate nothing (it measures what
           DID fill); over-filling lies to it. ``engine.marketdata.bar_builder`` resolves the same
           ambiguity the same way.
        1. the running intra-minute delta of cumulative volume when anything has printed;
        2. the last CLOSED bar's volume — the minute is young and nothing has printed yet;
        3. ``0`` when this feed is KNOWN to carry volume (``saw_volume``) but this minute has none
           and no bar has closed — nothing has traded, so nothing can be taken from it;
        4. ``None`` (= unknown, no cap) only when the feed has never once reported volume for this
           symbol: reachable on an ordinary LIVE name (every tick before its first print of the day
           carries ``volume_traded == 0``) and on backfilled or synthetic ticks. A broker that
           silently never fills is less useful, and no more conservative in any way the gate
           measures, than one that fills the whole order.
        """
        if st.volume_reset:
            return 0
        if st.running_volume > 0:
            return st.running_volume
        if st.last_bar_volume:
            return st.last_bar_volume
        if st.saw_volume:
            return 0
        return None

    def _fillable_qty(self, order: _Order, st: _SymbolState) -> int:
        """How much of ``order`` may fill on this tick, spending its budget for the CURRENT minute.

        The cap is a share of the MINUTE's tape, so it is a budget per (order, minute): capping each
        TICK at 10 % bounds nothing on a minute with dozens of ticks. Re-armed when the symbol's
        minute rolls.
        """
        volume = self._effective_volume(st)
        if volume is None:
            return order.remaining
        if order.minute_key != st.minute:
            order.minute_key = st.minute
            order.minute_filled = 0
        budget = int(self._participation * Decimal(volume)) - order.minute_filled
        return max(min(order.remaining, budget), 0)

    def _match(self, order: _Order, tick: Tick, st: _SymbolState, gap: bool) -> None:
        """Decide whether ``order`` fills on ``tick``, and by how much."""
        if tick.exchange_ts < order.eligible_at:
            return                                   # still inside the simulated latency

        limit = order.price if order.order_type == "LIMIT" else None
        if limit is not None:
            # TRADE-THROUGH only: a tick exactly AT the limit is a touch, and a touch is not
            # evidence that our resting order traded — the conservatism case.
            through = tick.ltp < limit if order.transaction_type == "BUY" else tick.ltp > limit
            if order.marketable is None:
                order.marketable = through
            if not through:
                return
        elif order.order_type == "SL-M":
            if not order.triggered:
                assert order.trigger_price is not None
                crossed = (
                    tick.ltp >= order.trigger_price
                    if order.transaction_type == "BUY"
                    else tick.ltp <= order.trigger_price
                )
                if not crossed:
                    return
                order.triggered = True
                beyond = (
                    tick.ltp > order.trigger_price
                    if order.transaction_type == "BUY"
                    else tick.ltp < order.trigger_price
                )
                if gap and beyond:
                    # GAP-AWARE (results-day case): the print that triggered the stop IS the
                    # fill reference; the stop price is unobtainable and must never be credited.
                    order.status_message = (
                        f"paper: gap fill through trigger {order.trigger_price} at {tick.ltp}"
                    )

        qty = self._fillable_qty(order, st)
        if qty <= 0:
            return

        if limit is None:
            fill_price = self._market_price(order, tick)
        elif not order.marketable:
            fill_price = limit
        elif order.transaction_type == "BUY":
            fill_price = min(self._market_price(order, tick), limit)
        else:
            fill_price = max(self._market_price(order, tick), limit)

        order.filled_qty += qty
        order.minute_filled += qty            # spend this minute's participation budget
        order.fill_value += fill_price * Decimal(qty)
        self._fills.append(
            _Fill(
                symbol=order.tradingsymbol,
                product=order.product,
                side=order.transaction_type,
                quantity=qty,
                price=fill_price,
                ts=tick.exchange_ts,
            )
        )
        if order.filled_qty >= order.quantity:
            order.status = "COMPLETE"
            self._unrest(order)
        else:
            order.status = "OPEN"
            if not order.status_message:
                order.status_message = "paper: partial fill"
        # Stamped with the trade's own time (tick timestamp), which keeps replay postbacks
        # independent of Clock granularity. Guarded: the fill is already recorded, so a raising bus
        # must not reach ``on_tick``'s ``except`` and terminate an order that just traded.
        self._publish_guarded(order, tick.exchange_ts)

    def _market_price(self, order: _Order, tick: Tick) -> Decimal:
        """MARKET / triggered-SL-M fill price: ltp + signed slippage, rounded against the taker."""
        tick_size = self._tick_size(order.tradingsymbol)
        half_spread, estimated = self._fm.half_spread(
            order.tradingsymbol, tick.bid, tick.ask, tick.ltp, tick_size
        )
        sigma = self._sigma.sigma(order.tradingsymbol, tick.ltp)
        bucket = self._fm.bucket_for(tick.exchange_ts.time())
        slip = self._fm.slippage(
            order.tradingsymbol, order.transaction_type, bucket, sigma, half_spread
        )
        if estimated and not order.status_message:
            order.status_message = "paper: fill with estimated half-spread"
        return _round_to_tick(tick.ltp + slip, tick_size, order.transaction_type)

    # ---------------------------------------------------------------- GTT firing
    def _match_gtts(self, tick: Tick, st: _SymbolState) -> None:
        for gtt_id in list(st.gtts):
            gtt = self._gtt_book.get(gtt_id)
            if gtt is None or gtt.status != "active":
                continue
            for index, trigger in enumerate(gtt.trigger_values):
                # Direction is fixed at placement time by the leg's price relative to last_price:
                # a leg below the mark is a downside (stop) trigger, at-or-above is an upside
                # (target) trigger. This is how Kite's OCO legs are distinguished.
                downside = trigger < gtt.last_price
                fired = tick.ltp <= trigger if downside else tick.ltp >= trigger
                if not fired:
                    continue
                # NOTHING may escape on_tick. Legs are validated at placement, so this is a backstop;
                # the fire marks the GTT the moment the leg is REGISTERED, with the postback guarded
                # after it (:meth:`_fire_gtt_leg`), so a resting leg is never left with its trigger
                # still armed OR reported as failed protection.
                try:
                    self._fire_gtt_leg(gtt, index)
                except Exception:
                    # LATCH, do not retry: left "active" the trigger is re-evaluated on every later
                    # tick (a log storm) and reads to the paper ProtectionManager as protection still
                    # armed. "failed" is a DISTINCT terminal status (PROTECTION_FAILED); only the
                    # GTT's owner may re-arm. CONDITIONAL: a leg that already REGISTERED marked the
                    # GTT "triggered", and a later raise in the same try must never un-fire it.
                    if gtt.status == "active":
                        gtt.status = "failed"
                    _log.exception(
                        "paper.gtt.fire_failed",
                        gtt_id=gtt.gtt_id,
                        leg=index,
                        symbol=gtt.tradingsymbol,
                        trigger=str(trigger),
                    )
                break                           # OCO: the other leg dies with the trigger

    def _fire_gtt_leg(self, gtt: _Gtt, index: int) -> None:
        """Place the fired leg. Kite semantics: a GTT fires a LIMIT order at the leg's price — a
        trigger is NOT a fill, so the order joins the resting book like any other.

        BOOKED == FIRED, in that order with nothing between them: ``_register`` books and rests the
        leg, the GTT is marked ``triggered``, and only THEN is the postback attempted, guarded.
        Publishing in the middle would let a throwing ``publish`` leave a resting leg with the GTT
        latched ``"failed"`` (read as failed protection) or still ``"active"`` (the same leg
        re-fires next tick: a second exit, the square-off double-exit hazard).

        The converse stays honest: a leg that cannot be VALIDATED or REGISTERED raises before the
        mark, nothing is resting, and the caller latches the GTT ``"failed"``.
        """
        leg = _gtt_leg_request(
            gtt.tradingsymbol, gtt.exchange, gtt.trigger_values[index], gtt.orders[index]
        )
        leg["tag"] = f"gtt:{gtt.gtt_id}:{index}"     # the OMS adopts the leg by this tag
        order = self._register(self._validate(leg))
        gtt.status = "triggered"
        self._publish_guarded(order, order.placed_at)
        _log.info(
            "paper.gtt.fired",
            gtt_id=gtt.gtt_id,
            leg=index,
            order_id=order.order_id,
            trigger=str(gtt.trigger_values[index]),
        )


__all__ = [
    "GTT_ID_BASE",
    "ORDER_ID_BASE",
    "PaperBroker",
    "PaperOrderError",
    "SigmaEstimator",
    "TERMINAL_STATUSES",
]
