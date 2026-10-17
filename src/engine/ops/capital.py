"""Owner capital change from the dashboard (O19; design 2026-10-16 v2 §3.2, §4, §5).

One figure moves, ``limits.yaml capital_base_inr``, and the two rupee caps scale with it
(``floor(cap × new / old)``). :class:`CapitalChanger` refuses a change in the session window, in AUTO,
on a calendar that does not cover today, outside half..double, on an unverified file, or when it would
NEWLY trip a brake in either book. Otherwise it rewrites exactly three lines of the verified bytes,
re-signs them with a compare-and-swap ``owner_update`` and reloads the limits, so every reader of the
base (both trackers, ``PaperRisk``, the gate) sees it on its next call (§2).

Both books are checked with every held position priced the way the next session's first evaluation
sees it: the live mark, else the close of the last stored 1m bar (paper prep's own seed). After an
out-of-session restart the live caches are empty, and pricing at entry would hide open MTM from the
brakes. A held position with neither price refuses 409, as ``PaperRisk._evaluate`` does.

Ordering constraint (ruling R7): :meth:`CapitalChanger.apply` runs synchronously from the verified
read through ``owner_update`` to ``limits.reload()``. The only ``await`` before ``reload()`` is the
price prefetch, before the table is read; after it, the bounded Telegram notify. A tick interleaving
a reload between a floor check's table read and its tracker read could compare new-base equity with
old-base lines and falsely KILL.
"""

from __future__ import annotations

import asyncio
import copy
import json
import re
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import yaml
from pydantic import ValidationError

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.enums import Actor, Mode
from engine.core.log import get_logger
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.risk.exposure import ExposureTracker, MarkPrice
from engine.risk.limits import LIMITS_FILE, LimitsEngine, LimitTable, floor_limits_from
from engine.risk.mode import ModeManager
from engine.strategy.hi52_edge import StrategyEdges
from engine.strategy.scanners import hi52

_log = get_logger("engine.ops.capital")

Path = tuple[str, ...]  # a key path into the parsed yaml, not a filesystem path

BASE_PATH: Path = ("capital_base_inr",)
SCALED_PATHS: tuple[Path, ...] = (
    ("limits", "capital_cap", "max_deployed_capital_inr"),
    ("limits", "per_stock_exposure", "cnc_notional_inr"),
)
COMMIT_HINT = "commit config/limits.yaml"

#: The figure table the dashboard dialog (Task 6) prints, in its order. Every value is a string or None.
FIGURE_KEYS: tuple[str, ...] = (
    "capital_inr",
    "max_deployed_capital_inr",
    "cnc_notional_inr",
    "equity_inr",
    "paper_equity_inr",
    "risk_intraday_inr",
    "risk_swing_inr",
    "day_loss_soft_inr",
    "day_loss_hard_inr",
    "equity_floor_inr",
    "cumulative_floor_inr",
    "hi52_edge_pct",
)

#: ``last_mark(symbol, at)``: the close of the last stored 1m bar at or before ``at`` and its minute
#: (``paper_runtime.store_last_mark_fn``; the same shape as its ``LastMarkFn``).
LastMark = Callable[[str, datetime], Awaitable[tuple[Decimal, datetime] | None]]

#: Bounds the stored-price prefetch as a whole (paper's restart seeding allows 30 s). Kept short: a GET
#: stays inside the dashboard's 8 s read abort, and a POST's prefetch plus the notify wait inside its
#: 15 s write abort, so the owner sees the engine's own answer (dashboard/src/api.ts).
PREFETCH_S = 5.0
#: How long ``apply`` waits on the Telegram send before answering; the send itself keeps running.
NOTIFY_WAIT_S = 5.0

_CENT = Decimal("0.01")
_HUNDRED = Decimal(100)
_MERGE_TAG = "tag:yaml.org,2002:merge"


class CapitalRefused(Exception):
    """A refused capital change: ``status`` is the HTTP status the route answers (409, 422 or 503),
    ``note`` the owner-facing reason. The dashboard prints it, after "Capital not changed:" only when nothing
    was written (dashboard/src/lib/capital.ts ``failureText``)."""

    def __init__(self, status: int, note: str) -> None:
        super().__init__(note)
        self.status = status
        self.note = note


# --------------------------------------------------------------------------- money
def _money(x: Decimal) -> str:
    """Wire money: 2 dp, ROUND_HALF_UP (``paper_summary._money``)."""
    return str(x.quantize(_CENT, ROUND_HALF_UP))


def _rupees(x: Decimal) -> str:
    """Owner money: ``₹18,000.00`` / ``-₹1,000.00``."""
    q = x.quantize(_CENT, ROUND_HALF_UP)
    return f"{'-' if q < 0 else ''}₹{abs(q):,.2f}"


def _whole_rupees(x: Decimal) -> str:
    return f"₹{int(x):,}" if x == x.to_integral_value() else _rupees(x)


def _pct(value: float) -> Decimal:
    """A limits.yaml percent (``-5.0`` == −5%) as a fraction; str() first, never a float artifact."""
    return Decimal(str(value)) / _HUNDRED


# --------------------------------------------------------------------------- the three values
def _table_value(table: LimitTable, path: Path) -> Decimal:
    node: Any = table
    for key in path:
        node = getattr(node, key)
    return Decimal(str(node))


def current_values(table: LimitTable) -> dict[Path, int]:
    """The base and the two caps as whole rupees; a fractional value is a layout refusal (422)."""
    out: dict[Path, int] = {}
    for path in (BASE_PATH, *SCALED_PATHS):
        value = _table_value(table, path)
        if value != value.to_integral_value():
            raise CapitalRefused(422, f"limits.yaml layout: {path[-1]} is not a whole number ({value})")
        out[path] = int(value)
    return out


def scaled_values(table: LimitTable, new_base: int) -> dict[Path, int]:
    """All three paths at ``new_base``: the base itself, and each cap as ``floor(cap × new / old)``.
    Integer arithmetic, so the floor is exact; flooring never loosens an upper bound."""
    old = current_values(table)
    old_base = old[BASE_PATH]
    if old_base < 1:
        raise CapitalRefused(422, f"limits.yaml layout: capital_base_inr is {old_base}")
    out: dict[Path, int] = {BASE_PATH: new_base}
    for path in SCALED_PATHS:
        out[path] = old[path] * new_base // old_base
    return out


# --------------------------------------------------------------------------- the rewrite (§4)
def rewrite_limits_text(text: str, old: Mapping[Path, int], new: Mapping[Path, int], stamp: str) -> str:
    """Replace each path's value on its one line, keep its comment and append the provenance.

    Lines are matched by leaf key and must be single-line ``key: <digits>  # comment``. Each leaf must
    occur as a key exactly once and read exactly ``old``; every other byte is kept, LF-only.
    """
    if "\r" in text:
        raise CapitalRefused(422, "limits.yaml layout: CR line endings (the file must be LF-only)")
    edits: list[tuple[int, int, str]] = []
    for path, value in new.items():
        leaf = re.escape(path[-1])
        found = len(re.findall(rf"^[ \t]*{leaf}[ \t]*:", text, re.MULTILINE))
        if found != 1:
            raise CapitalRefused(422, f"limits.yaml layout: {path[-1]} found {found} times")
        m = re.search(rf"^[ \t]*{leaf}:[ \t]*(\d+)([ \t]*)(#[^\r\n]*)?$", text, re.MULTILINE)
        if m is None:
            raise CapitalRefused(422, f"limits.yaml layout: {path[-1]} is not a plain whole number")
        if int(m.group(1)) != old[path]:
            raise CapitalRefused(
                422, f"limits.yaml layout: {path[-1]} reads {m.group(1)}, expected {old[path]}")
        note = f"owner via dashboard {stamp}: {old[path]} → {value}"
        gap, comment = m.group(2), m.group(3)
        tail = f"{gap}{comment} · {note}" if comment else f"{gap or '  '}# {note}"
        edits.append((m.start(1), m.end(), f"{value}{tail}"))
    for start, end, replacement in sorted(edits, reverse=True):
        text = text[:start] + replacement + text[end:]
    return text


class _DuplicateKey(yaml.YAMLError):
    pass


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that refuses a duplicate mapping key (PyYAML silently keeps the last one)."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        seen: set[Any] = set()
        for key_node, _value in node.value:
            if key_node.tag == _MERGE_TAG:
                continue
            key = self.construct_object(key_node, deep=True)
            try:
                duplicate = key in seen
            except TypeError:
                continue  # unhashable: the base constructor raises its own error
            if duplicate:
                raise _DuplicateKey(f"duplicate key {key!r} (line {key_node.start_mark.line + 1})")
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _parse_unique(text: str, which: str) -> dict:
    try:
        raw = yaml.load(text, Loader=_UniqueKeyLoader)  # noqa: S506 - a SafeLoader subclass
    except _DuplicateKey as exc:
        raise CapitalRefused(422, f"limits.yaml structure: {which} has a {exc}") from exc
    except yaml.YAMLError as exc:
        first = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        raise CapitalRefused(422, f"limits.yaml structure: {which} does not parse: {first}") from exc
    if not isinstance(raw, dict):
        raise CapitalRefused(422, f"limits.yaml structure: {which} is not a mapping")
    return raw


def _walk(node: Any, path: Path) -> Any:
    for key in path:
        if not isinstance(node, dict) or key not in node:
            raise KeyError(".".join(path))
        node = node[key]
    return node


def check_structure(old_text: str, new_text: str, new: Mapping[Path, int]) -> LimitTable:
    """The parsed new file must equal the parsed old one with exactly ``new``'s paths changed, each to
    an ``int`` equal to the intended value, and must validate as a :class:`LimitTable`. Any violation
    is a 422; this is what catches duplicate keys and ``6E+4`` (a string to PyYAML)."""
    old_raw = _parse_unique(old_text, "the current file")
    new_raw = _parse_unique(new_text, "the rewritten file")
    expected = copy.deepcopy(old_raw)
    for path, value in new.items():
        dotted = ".".join(path)
        try:
            parent = _walk(expected, path[:-1])
            _walk(parent, path[-1:])
            actual = _walk(new_raw, path)
        except KeyError as exc:
            raise CapitalRefused(422, f"limits.yaml structure: {dotted} is missing") from exc
        if type(actual) is not int or actual != value:
            raise CapitalRefused(
                422, f"limits.yaml structure: {dotted} reads {actual!r}, expected the whole number {value}")
        parent[path[-1]] = value
    if new_raw != expected:
        raise CapitalRefused(422, "limits.yaml structure: the rewrite changed more than the capital lines")
    try:
        return LimitTable.model_validate(new_raw)
    except ValidationError as exc:
        raise CapitalRefused(
            422, f"limits.yaml structure: the new file does not validate ({exc.error_count()} errors)") from exc


# --------------------------------------------------------------------------- brakes (§3.2)
def _tripped(tracker: ExposureTracker, table: LimitTable) -> list[tuple[str, Decimal]]:
    """Every brake ``tracker`` trips against ``table``'s OWN lines (ruling R8), with each ₹ line:
    the floor ladder in the engine's order, then the day-loss rungs (hard first)."""
    breaches = tracker.evaluate_floors(floor_limits_from(table))
    out: list[tuple[str, Decimal]] = [(b.rung, b.threshold) for b in breaches]
    lim = table.limits
    soft = Decimal(str(lim.daily_loss_soft.day_mtm_pct))
    hard = Decimal(str(lim.daily_loss_hard.day_mtm_pct))
    lines = {"daily_loss_soft": soft, "daily_loss_hard": hard}
    for rung in tracker.evaluate_day_loss(soft, hard):
        out.append((rung, table.capital_base_inr * lines[rung] / _HUNDRED))
    return out


def _trip_note(trips: list[tuple[str, Decimal]]) -> str:
    return "would trip: " + "; ".join(f"{token} ({_rupees(line)})" for token, line in trips)


def _priced(live: MarkPrice | None, stored: Mapping[str, Decimal]) -> MarkPrice:
    """The live mark when there is one, else the stored close; None when neither."""

    def mark(symbol: str) -> Decimal | None:
        px = None if live is None else live(symbol)
        return stored.get(symbol) if px is None else px

    return mark


def _unpriced(books: list[tuple[str, ExposureTracker]]) -> dict[str, list[str]]:
    """Per book, the held symbols its tracker has no price for (``open_mtm`` would count them at entry)."""
    out: dict[str, list[str]] = {}
    for scope, book in books:
        missing = [s for s in book.held_symbols() if book.mark_of(s) is None]
        if missing:
            out[scope] = missing
    return out


def _unpriced_refusal(unpriced: Mapping[str, list[str]]) -> CapitalRefused | None:
    if not unpriced:
        return None
    held = "; ".join(f"{scope}: {', '.join(symbols)}" for scope, symbols in unpriced.items())
    return CapitalRefused(
        409, f"no price yet for held positions ({held}), so the brakes can't be checked; "
             "retry in a minute, and if it persists, change capital after the next session's close")


# --------------------------------------------------------------------------- the changer
class CapitalChanger:
    """Preview and apply a capital change. Holds no state of its own beyond in-flight notify sends:
    every call re-reads the limits, the mode, the calendar, the books and their prices."""

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        store: ProtectedStore,
        limits: LimitsEngine,
        mode: ModeManager,
        real: ExposureTracker,
        paper_mark: MarkPrice | None,
        last_mark: LastMark | None,
        edges: StrategyEdges | None,
        notify: Callable[[CatalogMessage], Awaitable[None]] | None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._store = store
        self._limits = limits
        self._mode = mode
        self._real = real
        self._paper_mark = paper_mark
        self._last_mark = last_mark
        self._edges = edges
        self._notify = notify
        #: Strong references: a send that outlives ``apply`` is not garbage-collected mid-flight.
        self._sends: set[asyncio.Task[None]] = set()

    # ----------------------------------------------------------------- refusals
    def _loaded(self) -> LimitTable:
        try:
            return self._limits.load()
        except (IntegrityError, OSError, ValueError, yaml.YAMLError) as exc:
            raise CapitalRefused(503, f"protected config unverified: {exc}") from exc

    def _gate(self) -> CapitalRefused | None:
        """Mode, calendar coverage, then the session window (all 409)."""
        if self._mode.mode() == Mode.AUTO:
            return CapitalRefused(409, "the engine is in AUTO; switch it to RECOMMEND or OFF first")
        now = self._clock.now()
        today = now.date()
        horizon = self._calendar.verified_horizon()
        if horizon is None or horizon < today:
            return CapitalRefused(
                409, "the trading calendar is not verified for today, so market hours can't be checked")
        session = self._calendar.session(today)
        if session is not None and session.open <= now <= session.close:
            return CapitalRefused(
                409, f"the market is open (today {session.open:%H:%M} to {session.close:%H:%M} IST); "
                     "change capital after the close")
        return None

    @staticmethod
    def _bounds(base: Decimal, new: object) -> CapitalRefused | None:
        if type(new) is not int or new < 1:
            return CapitalRefused(422, "capital must be a whole number of rupees, at least ₹1")
        if new == base:
            return CapitalRefused(422, f"capital is already {_whole_rupees(base)}")
        if not base / 2 <= new <= base * 2:
            return CapitalRefused(
                422, f"one change can at most halve or double the capital: "
                     f"{_whole_rupees(base / 2)} to {_whole_rupees(base * 2)}")
        return None

    def _verified(self, loaded: LimitTable) -> tuple[str, str]:
        """The verified bytes as text, and their hash for the compare-and-swap."""
        try:
            raw, sha = self._store.read_verified_bytes(LIMITS_FILE)
        except (IntegrityError, OSError) as exc:
            raise CapitalRefused(503, f"protected config unverified: {exc}") from exc
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CapitalRefused(422, f"limits.yaml layout: not UTF-8 ({exc.reason})") from exc
        try:
            on_disk = LimitTable.model_validate(_parse_unique(text, "the current file"))
        except ValidationError as exc:
            raise CapitalRefused(422, "limits.yaml structure: the current file does not validate") from exc
        if on_disk != loaded:
            # Signed, but not what the engine trades on (an out-of-band owner_update): a change built
            # on it would check brakes against one table and write another.
            raise CapitalRefused(
                409, "the signed limits.yaml differs from the limits the engine is running on (changed "
                     "outside the dashboard); restart the engine before changing capital")
        return text, sha

    def _build(self, text: str, loaded: LimitTable, new_base: int) -> tuple[str, LimitTable]:
        new = scaled_values(loaded, new_base)
        new_text = rewrite_limits_text(text, current_values(loaded), new, self._clock.today().isoformat())
        return new_text, check_structure(text, new_text, new)

    # ----------------------------------------------------------------- books
    def _live_base(self) -> Decimal:
        return self._limits.load().capital_base_inr

    def _paper_live(self) -> ExposureTracker:
        """The paper book on its runtime's marks. Built without them too (paper not composed at
        boot): its epoch's rows still meet the new base at the next boot that composes it."""
        return ExposureTracker(self._conn, self._clock, self._live_base, mark_price=self._paper_mark,
                               scope="paper")

    async def _stored_marks(self) -> dict[str, Decimal]:
        """The last stored 1m close at or before now for each held symbol, in either book, that has
        no live mark. Bounded as a whole by :data:`PREFETCH_S`; a symbol whose read fails or is cut
        off is left out (the brake step then refuses 409). Never raises."""
        stored: dict[str, Decimal] = {}
        if self._last_mark is None:
            return stored
        try:
            paper = self._paper_live()
            wanted = sorted({s for s in self._real.held_symbols() if self._real.mark_of(s) is None}
                            | {s for s in paper.held_symbols() if paper.mark_of(s) is None})
        except Exception:
            _log.exception("capital_change_held_unread")
            return stored
        at = self._clock.now()
        try:
            async with asyncio.timeout(PREFETCH_S):
                for symbol in wanted:
                    try:
                        found = await self._last_mark(symbol, at)
                        px = None if found is None else Decimal(str(found[0]))
                    except Exception as exc:
                        _log.warning("capital_change_stored_mark_failed", symbol=symbol, error=repr(exc))
                        continue
                    if px is not None and px.is_finite() and px > 0:
                        stored[symbol] = px
        except TimeoutError:
            _log.warning("capital_change_stored_marks_slow", timeout_s=PREFETCH_S,
                         missing=[s for s in wanted if s not in stored])
        return stored

    def _now(
        self, stored: Mapping[str, Decimal]
    ) -> tuple[ExposureTracker, ExposureTracker, dict[str, list[str]]]:
        """Both books as the next session's first evaluation prices them (read-only twins), and the
        held symbols each still has no price for."""
        real_now = self._real.with_mark_price(_priced(self._real.mark_of, stored))
        paper_now = self._paper_live().with_mark_price(_priced(self._paper_mark, stored))
        return real_now, paper_now, _unpriced([("real", real_now), ("paper", paper_now)])

    @staticmethod
    def _brakes(
        loaded: LimitTable, new_table: LimitTable, real_now: ExposureTracker, paper_now: ExposureTracker,
        unpriced: Mapping[str, list[str]],
    ) -> tuple[list[tuple[str, Decimal]], ExposureTracker, ExposureTracker]:
        """Rungs tripped at the new base that are not tripped now, per book; 409 while a held
        position has no price. The baseline is warmed BEFORE each what-if copy, so the copy inherits
        it rather than rebuilding it at the new base."""
        refused = _unpriced_refusal(unpriced)
        if refused is not None:
            raise refused
        new_base = new_table.capital_base_inr
        real_now.day_baseline()
        paper_now.day_baseline()
        real_new = real_now.with_capital_base(new_base)
        paper_new = paper_now.with_capital_base(new_base)
        trips: list[tuple[str, Decimal]] = []
        for scope, now_book, new_book in (("real", real_now, real_new), ("paper", paper_now, paper_new)):
            before = {rung for rung, _line in _tripped(now_book, loaded)}
            trips += [(f"{scope}:{rung}", line) for rung, line in _tripped(new_book, new_table)
                      if rung not in before]
        return trips, real_new, paper_new

    def _hi52(self, table: LimitTable) -> str | None:
        if self._edges is None:
            return None
        try:
            return str(self._edges.edge_for_table(hi52.STRATEGY_ID, table))
        except KeyError:
            return None  # hi52 not registered: no edge to show

    def _figures(
        self, table: LimitTable, real: ExposureTracker, paper: ExposureTracker,
        unpriced: Mapping[str, list[str]],
    ) -> dict[str, str | None]:
        """The dialog's figures. A book with an unpriced holding shows no equity (None), never one
        that counts the holding at entry."""
        lim = table.limits
        base = table.capital_base_inr
        equity = None if "real" in unpriced else real.equity()
        return {
            "capital_inr": _money(base),
            "max_deployed_capital_inr": _money(lim.capital_cap.max_deployed_capital_inr),
            "cnc_notional_inr": _money(lim.per_stock_exposure.cnc_notional_inr),
            "equity_inr": None if equity is None else _money(equity),
            "paper_equity_inr": None if "paper" in unpriced else _money(paper.equity()),
            # What the gate charges: pct × equity (gate._rule_per_trade_risk), not × base.
            "risk_intraday_inr": None if equity is None else _money(
                equity * _pct(lim.per_trade_risk.intraday_pct)),
            "risk_swing_inr": None if equity is None else _money(
                equity * _pct(lim.per_trade_risk.swing_position_pct)),
            "day_loss_soft_inr": _money(base * _pct(lim.daily_loss_soft.day_mtm_pct)),
            "day_loss_hard_inr": _money(base * _pct(lim.daily_loss_hard.day_mtm_pct)),
            "equity_floor_inr": _money(base * (1 + _pct(lim.equity_floor_rung.equity_pct_of_base))),
            "cumulative_floor_inr": _money(base * (1 + _pct(lim.cumulative_floor.equity_pct_of_base))),
            "hi52_edge_pct": self._hi52(table),
        }

    # ----------------------------------------------------------------- public
    async def preview(self, proposed: int | None = None) -> dict[str, Any]:
        """The current figures and, for a proposal within bounds on a verified file, the proposed
        ones. ``refusal`` is the first refusal :meth:`apply` would raise (in its order); without a
        proposal, ``allowed`` says whether a change is possible right now. Never writes."""
        stored = await self._stored_marks()
        try:
            loaded = self._loaded()
        except CapitalRefused as refused:
            return {"current": dict.fromkeys(FIGURE_KEYS), "proposed": None, "allowed": False,
                    "refusal": refused.note, "would_trip": []}
        base = loaded.capital_base_inr
        real_now, paper_now, unpriced = self._now(stored)
        current = self._figures(loaded, real_now, paper_now, unpriced)
        bounds = None if proposed is None else self._bounds(base, proposed)
        refusals: list[CapitalRefused | None] = [self._gate(), bounds]
        text: str | None = None
        try:
            text, _sha = self._verified(loaded)
        except CapitalRefused as refused:
            refusals.append(refused)
        figures: dict[str, str | None] | None = None
        would_trip: list[str] = []
        if proposed is None:
            refusals.append(_unpriced_refusal(unpriced))     # no change is possible while unpriced
        elif bounds is None and text is not None:
            try:
                _new_text, new_table = self._build(text, loaded, proposed)
                trips, real_new, paper_new = self._brakes(loaded, new_table, real_now, paper_now, unpriced)
            except CapitalRefused as refused:
                refusals.append(refused)
            else:
                figures = self._figures(new_table, real_new, paper_new, unpriced)
                would_trip = [token for token, _line in trips]
                if trips:
                    refusals.append(CapitalRefused(422, _trip_note(trips)))
        first = next((r for r in refusals if r is not None), None)
        return {
            "current": current,
            "proposed": figures,
            "allowed": first is None,
            "refusal": None if first is None else first.note,
            "would_trip": would_trip,
        }

    async def apply(self, capital_inr: int, expected_current_inr: int) -> dict[str, Any]:
        """Validate, write, re-sign and reload; then announce. Raises :class:`CapitalRefused`.

        Ruling R7: the only ``await`` before ``limits.reload()`` is the price prefetch, before the
        table is read; from ``_loaded()`` through the reload nothing yields to the loop."""
        stored = await self._stored_marks()
        loaded = self._loaded()
        base = loaded.capital_base_inr
        if type(expected_current_inr) is not int:
            raise CapitalRefused(422, "expected_current_inr must be a whole number of rupees")
        if expected_current_inr != base:
            raise CapitalRefused(
                409, f"the capital changed since this dialog opened (it is now {_whole_rupees(base)}); "
                     "reopen the dialog")
        refused = self._gate() or self._bounds(base, capital_inr)
        if refused is not None:
            raise refused
        text, sha = self._verified(loaded)
        new_text, new_table = self._build(text, loaded, capital_inr)
        real_now, paper_now, unpriced = self._now(stored)
        trips, real_new, paper_new = self._brakes(loaded, new_table, real_now, paper_now, unpriced)
        if trips:
            raise CapitalRefused(422, _trip_note(trips))
        old_figures = self._figures(loaded, real_now, paper_now, unpriced)
        new_figures = self._figures(new_table, real_new, paper_new, unpriced)
        note = json.dumps({"change": "capital_base_inr", "via": "dashboard",
                           "old": old_figures, "new": new_figures})
        try:
            self._store.owner_update(
                LIMITS_FILE, new_text, OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note=note),
                expected_sha256=sha,
            )
        except IntegrityError as exc:
            _log.warning("capital_change_cas_refused", error=str(exc))
            raise CapitalRefused(409, "limits.yaml changed since it was read; reopen the dialog") from exc
        except Exception as exc:
            _log.critical("capital_change_write_failed", error=str(exc), old=str(base), new=capital_inr)
            raise CapitalRefused(
                503, f"limits.yaml write failed: {exc}; check its integrity before the next boot") from exc
        try:
            self._limits.reload()
        except Exception as exc:
            # Signed on disk, not live: every in-process reader still agrees on the old base (one
            # cached table), and the next boot loads the new one.
            _log.critical("capital_change_reload_failed", error=str(exc), old=str(base), new=capital_inr)
            raise CapitalRefused(
                503, f"limits.yaml was changed and signed, but the engine could not reload it: {exc}; "
                     "restart the engine before the next session") from exc
        _log.warning("capital_changed", old=str(base), new=capital_inr, actor=Actor.OWNER.value)
        self._check_live_base(new_table)
        if self._notify is not None:
            await self._announce(self._notify, old_figures, new_figures)
        return {"ok": True, "old": old_figures, "new": new_figures, "commit_hint": COMMIT_HINT}

    def _check_live_base(self, new_table: LimitTable) -> None:
        """Wiring self-check after the reload: the real tracker's base must read the new table (O19
        §2: one live source, no setter). Logs; never refuses, since the change is already live."""
        try:
            live: object = self._real.capital_base
        except Exception as exc:
            live = repr(exc)
        if live != new_table.capital_base_inr:
            _log.critical("capital_change_base_mismatch", tracker=str(live),
                          table=str(new_table.capital_base_inr))

    async def _announce(
        self, notify: Callable[[CatalogMessage], Awaitable[None]],
        old: dict[str, str | None], new: dict[str, str | None],
    ) -> None:
        """The CAPITAL_CHANGED send, awaited at most :data:`NOTIFY_WAIT_S`: the change is already
        live, and the dashboard abandons the request long before Telegram's own retries end."""
        task = asyncio.get_running_loop().create_task(self._send(notify, old, new), name="capital-change-notify")
        self._sends.add(task)
        task.add_done_callback(self._sends.discard)
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=NOTIFY_WAIT_S)
        except TimeoutError:
            _log.warning("capital_change_notify_slow", timeout_s=NOTIFY_WAIT_S)

    @staticmethod
    async def _send(
        notify: Callable[[CatalogMessage], Awaitable[None]],
        old: dict[str, str | None], new: dict[str, str | None],
    ) -> None:
        try:
            await notify(catalog.capital_changed(old=old, new=new, actor="the owner via the dashboard"))
        except Exception:
            _log.exception("capital_change_notify_failed")
