"""Platform equity, day-scoped risk counters, and the §7.1 floor ladder.

``ExposureTracker`` is the single place that answers "what is the platform worth right now, and what
has it already spent today" — the basis for every percentage limit in §7.1 and for the continuous
halt ladder. Two properties make it load-bearing:

- **Everything is rebuilt from the tables** (positions / learning_ledger / equity_snapshots), never
  from in-process counters, so a same-day restart cannot reset exhausted entry capacity and an
  offline-realized loss still trips its floor rung on startup (§2.6 step 2).
- **``origin='external'`` is excluded everywhere** (O5): the owner's own trading in the shared account
  is not platform P&L and does not consume platform limits. ``origin='recommended'`` counts (§3.6).

Equity is persisted each minute (:meth:`persist_snapshot`); the floor ladder is evaluated on every
persist, after every reconcile, and on startup. Applying a breach (:meth:`apply_floor_breaches`) is
kept separate from detecting one so the startup path can re-apply a rung's FULL action against
freshly-reconciled equity before entries reopen (§2.6 step 2).

Tier-2 only: no ``engine.intelligence`` import, and no ``LimitsEngine`` import either — the limits
seam is duck-typed (:class:`FloorLimits` is the reference shape) so the two can land independently.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date
from decimal import Decimal
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor, Mode, RiskState
from engine.core.log import get_logger
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

_log = get_logger("engine.risk.exposure")

#: Positions the platform is accountable for (O5). Inlined into SQL rather than parameterised: a
#: fixed CHECK-constrained enum, no injection surface, and it keeps the predicate readable.
_PLATFORM_ORIGINS = "origin IN ('platform','recommended')"

#: Rolling window for `weekly_drawdown` — TRADING sessions, not calendar days (§7.1).
WEEKLY_DRAWDOWN_SESSIONS = 5

FlattenCallback = Callable[[], Awaitable[None]]
AlertCallback = Callable[[str], Awaitable[None]]
MarkPrice = Callable[[str], Decimal | None]

FloorRung = Literal["weekly_drawdown", "equity_floor_rung", "cumulative_floor"]
FloorAction = Literal["close_only_downgrade", "forced_exit_close_only", "kill_forced_off"]

#: Most-restrictive-wins ordering for the ladder's actions.
_ACTION_RANK: dict[str, int] = {
    "close_only_downgrade": 0,
    "forced_exit_close_only": 1,
    "kill_forced_off": 2,
}

_RISK_RANK = {RiskState.NORMAL: 0, RiskState.FROZEN: 1, RiskState.CLOSE_ONLY: 2, RiskState.KILLED: 3}


def _dec(value: Any) -> Decimal:
    """Money TEXT column → exact Decimal; NULL/empty → 0. ``str()`` first so a float that reached the
    column via a lax writer never contributes a binary-float artifact."""
    if value is None or value == "":
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _sign(side: str | None) -> Decimal:
    """Signed direction of a position's P&L. ``qty`` is stored unsigned; the side carries direction."""
    return Decimal(-1) if (side or "").upper() in ("SELL", "SHORT") else Decimal(1)


def _pct(limits: Any, name: str) -> Decimal:
    """Read one fraction off the duck-typed limits object (see :class:`FloorLimits`)."""
    return _dec(getattr(limits, name))


class OpenCounts(BaseModel):
    """Open platform-position counts for `max_open_positions` (3 total; ≤2 MIS; ≤2 CNC, §7.1)."""

    model_config = ConfigDict(frozen=True)

    total: int
    mis: int
    cnc: int


class EquitySnapshot(BaseModel):
    """One ``equity_snapshots`` row (§7.1), returned by :meth:`ExposureTracker.persist_snapshot`."""

    model_config = ConfigDict(frozen=True)

    at: AwareDatetime
    equity: Decimal
    realized_pnl: Decimal
    open_mtm: Decimal
    day_mtm: Decimal
    positions_open: int


class FloorLimits(BaseModel):
    """Reference shape for the :meth:`ExposureTracker.evaluate_floors` seam (§7.1 starting values).

    Percentages are FRACTIONS of 1 (``0.08`` = 8%), unlike ``config/costs.yaml``'s per-100 rates.
    ``LimitsEngine`` may pass its own object instead — only these four attribute names are read.
    """

    model_config = ConfigDict(frozen=True)

    weekly_drawdown_pct: Decimal = Decimal("0.08")
    equity_floor_rung_pct: Decimal = Decimal("0.10")
    cumulative_floor_pct: Decimal = Decimal("0.15")
    capital_base: Decimal | None = None      # None ⇒ the tracker's own base (O1, ₹20,000)


class FloorBreach(BaseModel):
    """A breached rung of the continuous equity halt ladder (§7.1).

    ``threshold`` is always the equity LEVEL the rung trips at, so the comparison that produced the
    breach is reconstructable from the record (for `weekly_drawdown` that is ``peak × (1 − pct)``).
    """

    model_config = ConfigDict(frozen=True)

    rung: FloorRung
    equity: Decimal
    threshold: Decimal
    action: FloorAction


class ExposureTracker:
    """Platform equity + day-scoped risk counters, rebuilt from SQLite on every call (§7.1/§2.6)."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        capital_base: Decimal,
        mark_price: MarkPrice | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._capital_base = capital_base
        self._mark_price = mark_price
        self._day_baseline: tuple[date, Decimal] | None = None

    # ----------------------------------------------------------------- platform equity (§7.1)
    def realized_net(self) -> Decimal:
        """Σ platform-attributed realized P&L net of costs. ``positions.realized_pnl`` is gross (the
        table carries a separate ``costs`` column, and §7.1 subtracts it explicitly)."""
        rows = self._conn.execute(
            f"SELECT realized_pnl, costs FROM positions WHERE {_PLATFORM_ORIGINS}"
        ).fetchall()
        total = Decimal(0)
        for row in rows:
            total += _dec(row["realized_pnl"]) - _dec(row["costs"])
        return total

    def _mark(self, symbol: str, avg_entry: Decimal) -> Decimal:
        """Mark for an open position. Falls back to ``avg_entry`` (⇒ zero MTM) when no mark is
        available — on a cold/WARMING start the caller injects the broker-reported last_price via the
        callback, since broker = truth until the live feed warms (§7.1)."""
        if self._mark_price is None:
            return avg_entry
        px = self._mark_price(symbol)
        return px if px is not None else avg_entry

    def open_mtm(self) -> Decimal:
        """Mark-to-market of open platform positions: qty × (mark − avg_entry), signed by side."""
        rows = self._conn.execute(
            f"SELECT symbol, side, qty, avg_entry FROM positions "
            f"WHERE state='OPEN' AND {_PLATFORM_ORIGINS}"
        ).fetchall()
        total = Decimal(0)
        for row in rows:
            entry = _dec(row["avg_entry"])
            mark = self._mark(row["symbol"], entry)
            total += Decimal(int(row["qty"] or 0)) * (mark - entry) * _sign(row["side"])
        return total

    def equity(self) -> Decimal:
        """Platform equity = capital base + realized net + open MTM (§7.1). The basis for every
        percentage limit and halt threshold; ``external`` positions and owner cash movements in the
        shared account are excluded (O5)."""
        return self._capital_base + self.realized_net() + self.open_mtm()

    # ----------------------------------------------------------------- day MTM + baseline (§2.6)
    def set_day_baseline(self, equity: Decimal, d: date | None = None) -> None:
        """Pin the day's opening equity explicitly (session-start hook / tests)."""
        self._day_baseline = (d or self._clock.today(), equity)

    def day_baseline(self, d: date | None = None) -> Decimal:
        """The day's opening equity, rebuilt on first use and cached in-memory for the day.

        Rebuild rule (§2.6): the last ``equity_snapshots`` row strictly BEFORE ``d`` (i.e. the prior
        session's final persist). With no prior snapshot (first-ever run), back the baseline out of
        current equity MINUS today's already-realized net — a loss realized while the engine was off
        must still count toward ``daily_loss_soft``/``daily_loss_hard`` on the first run of the day
        (§2.6 step 2 folds offline closes into the day counters, and day-MTM is one of them).
        """
        d = d or self._clock.today()
        if self._day_baseline is not None and self._day_baseline[0] == d:
            return self._day_baseline[1]
        row = self._conn.execute(
            "SELECT equity FROM equity_snapshots WHERE substr(at, 1, 10) < ? ORDER BY at DESC LIMIT 1",
            (d.isoformat(),),
        ).fetchone()
        baseline = _dec(row["equity"]) if row is not None else self.equity() - self._realized_net_closed_on(d)
        self._day_baseline = (d, baseline)
        return baseline

    def _realized_net_closed_on(self, d: date) -> Decimal:
        """Net realized P&L of platform/recommended positions CLOSED on ``d`` (gross − costs)."""
        rows = self._conn.execute(
            "SELECT realized_pnl, costs FROM positions "
            "WHERE origin IN ('platform','recommended') AND state = 'CLOSED' "
            "AND substr(closed_at, 1, 10) = ?",
            (d.isoformat(),),
        ).fetchall()
        total = Decimal("0")
        for row in rows:
            total += _dec(row["realized_pnl"]) - _dec(row["costs"])
        return total

    def day_mtm(self, d: date | None = None) -> Decimal:
        """Day P&L on an MTM basis: realized net of everything CLOSED today + the open-MTM move since
        the day baseline. Both terms fall straight out of ``equity − baseline``, because the baseline
        already contains every realization booked before today (§7.1 "MTM basis")."""
        return self.equity() - self.day_baseline(d)

    # ----------------------------------------------------------------- day-scoped counters (§2.6)
    def consecutive_losses(self, d: date | None = None) -> int:
        """Losing closes at the tail of today's ledger — count back from the latest close until a
        non-loss (§7.1 `consecutive_losses`). ``no_action`` rows are expired recommendations, not
        closes (§3.6), so they are excluded rather than allowed to break the run.

        Offline closes are folded in for free: reconcile writes their ledger rows before this runs.
        """
        d = d or self._clock.today()
        rows = self._conn.execute(
            "SELECT outcome_label FROM learning_ledger "
            "WHERE substr(closed_at, 1, 10) = ? AND outcome_label IS NOT NULL "
            "AND outcome_label != 'no_action' ORDER BY closed_at DESC",
            (d.isoformat(),),
        ).fetchall()
        count = 0
        for row in rows:
            if row["outcome_label"] != "loss":
                break
            count += 1
        return count

    def trades_opened_today(self, d: date | None = None) -> int:
        """Platform entries opened today, against `max_new_trades_day` (§7.1). Counts positions, not
        orders — a rejected/discarded entry never became one."""
        d = d or self._clock.today()
        row = self._conn.execute(
            f"SELECT COUNT(*) AS n FROM positions "
            f"WHERE substr(opened_at, 1, 10) = ? AND {_PLATFORM_ORIGINS}",
            (d.isoformat(),),
        ).fetchone()
        return int(row["n"]) if row else 0

    # ----------------------------------------------------------------- open-exposure counters (§7.1)
    def _open_rows(self) -> list[sqlite3.Row]:
        return self._conn.execute(
            f"SELECT symbol, side, product, qty, avg_entry FROM positions "
            f"WHERE state='OPEN' AND {_PLATFORM_ORIGINS}"
        ).fetchall()

    def open_position_counts(self) -> OpenCounts:
        rows = self._open_rows()
        mis = sum(1 for r in rows if (r["product"] or "").upper() == "MIS")
        cnc = sum(1 for r in rows if (r["product"] or "").upper() == "CNC")
        return OpenCounts(total=len(rows), mis=mis, cnc=cnc)

    def per_symbol_open(self, symbol: str) -> int:
        """Open platform positions in ``symbol`` (`per_stock_exposure`: 1 per symbol, C4)."""
        return sum(1 for r in self._open_rows() if r["symbol"] == symbol)

    def per_sector_open(self, sector_of: Mapping[str, str]) -> dict[str, int]:
        """Open platform positions per sector (`per_sector_exposure`, §7.1). Symbols missing from the
        weekly ``sector_map`` land in ``UNCLASSIFIED``, whose cap is 1."""
        counts: dict[str, int] = {}
        for row in self._open_rows():
            sector = sector_of.get(row["symbol"]) or "UNCLASSIFIED"
            counts[sector] = counts.get(sector, 0) + 1
        return counts

    def cnc_notional(self, symbol: str) -> Decimal:
        """Open CNC notional in ``symbol`` (`per_stock_exposure`: ≤ ₹8,000/symbol, C4)."""
        total = Decimal(0)
        for row in self._open_rows():
            if row["symbol"] == symbol and (row["product"] or "").upper() == "CNC":
                total += Decimal(int(row["qty"] or 0)) * _dec(row["avg_entry"])
        return total

    def deployed_capital(self) -> Decimal:
        """Deployed capital against `capital_cap` (≤ ₹20,000, O1).

        CNC is cash/notional. MIS is *margin* — but leverage lands in Phase 3, so Phase 2 charges MIS
        at full notional: conservative (never under-reports deployment), and the cap only tightens
        when the real margin divisor arrives.
        """
        total = Decimal(0)
        for row in self._open_rows():
            total += Decimal(int(row["qty"] or 0)) * _dec(row["avg_entry"])
        return total

    # ----------------------------------------------------------------- minute persistence (§7.1)
    def persist_snapshot(self) -> EquitySnapshot:
        """Write the current equity point, keyed at the minute (INSERT OR REPLACE ⇒ the last write in
        a minute wins, so a reconcile-triggered re-evaluation never duplicates the row)."""
        now = self._clock.now().replace(second=0, microsecond=0)
        snap = EquitySnapshot(
            at=now,
            equity=self.equity(),
            realized_pnl=self.realized_net(),
            open_mtm=self.open_mtm(),
            day_mtm=self.day_mtm(),
            positions_open=self.open_position_counts().total,
        )
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT OR REPLACE INTO equity_snapshots
                    (at, equity, realized_pnl, open_mtm, day_mtm, positions_open)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    now.isoformat(),
                    str(snap.equity),
                    str(snap.realized_pnl),
                    str(snap.open_mtm),
                    str(snap.day_mtm),
                    snap.positions_open,
                ),
            )
        return snap

    # ----------------------------------------------------------------- floor ladder (§7.1/§3.5.3)
    def weekly_drawdown_peak(self, d: date | None = None) -> Decimal | None:
        """Peak equity over the last :data:`WEEKLY_DRAWDOWN_SESSIONS` snapshotted sessions.

        Sessions are read off the distinct snapshot dates rather than the calendar: snapshots exist
        only for sessions the engine was up for, which is exactly the "last 5 sessions" the rolling
        drawdown is defined over — and it keeps the ladder independent of calendar availability (R6).
        MAX() is done in Python: the column is Decimal-as-TEXT, so SQL MAX would compare lexically.
        """
        d = d or self._clock.today()
        sessions = self._conn.execute(
            "SELECT DISTINCT substr(at, 1, 10) AS sd FROM equity_snapshots "
            "WHERE substr(at, 1, 10) <= ? ORDER BY sd DESC LIMIT ?",
            (d.isoformat(), WEEKLY_DRAWDOWN_SESSIONS),
        ).fetchall()
        if not sessions:
            return None
        oldest = sessions[-1]["sd"]
        rows = self._conn.execute(
            "SELECT equity FROM equity_snapshots WHERE substr(at, 1, 10) BETWEEN ? AND ?",
            (oldest, d.isoformat()),
        ).fetchall()
        return max((_dec(r["equity"]) for r in rows), default=None)

    def evaluate_floors(self, limits: Any) -> list[FloorBreach]:
        """Evaluate the continuous equity halt ladder, most-restrictive rung first (§7.1).

        Boundary semantics are exact and differ by rung: `equity_floor_rung` is "equity ≤ −10%" and
        `cumulative_floor` is "equity < ₹17,000" (−15%) — so equity landing precisely on the −15%
        line does NOT kill, while precisely on the −10% line DOES trip the go-flat rung.
        """
        equity = self.equity()
        raw_base = getattr(limits, "capital_base", None)
        base = _dec(raw_base) if raw_base is not None else self._capital_base
        breaches: list[FloorBreach] = []

        cumulative = base * (Decimal(1) - _pct(limits, "cumulative_floor_pct"))
        if equity < cumulative:
            breaches.append(
                FloorBreach(rung="cumulative_floor", equity=equity, threshold=cumulative,
                            action="kill_forced_off")
            )

        rung = base * (Decimal(1) - _pct(limits, "equity_floor_rung_pct"))
        if equity <= rung:
            breaches.append(
                FloorBreach(rung="equity_floor_rung", equity=equity, threshold=rung,
                            action="forced_exit_close_only")
            )

        peak = self.weekly_drawdown_peak()
        if peak is not None and peak > 0:
            weekly = peak * (Decimal(1) - _pct(limits, "weekly_drawdown_pct"))
            if equity <= weekly:
                breaches.append(
                    FloorBreach(rung="weekly_drawdown", equity=equity, threshold=weekly,
                                action="close_only_downgrade")
                )
        return breaches

    async def apply_floor_breaches(
        self,
        breaches: list[FloorBreach],
        mode_manager: ModeManager,
        kill_switch: KillSwitch,
        flatten: FlattenCallback | None = None,
        alert: AlertCallback | None = None,
    ) -> None:
        """Apply each breached rung's FULL §7.1 action, most-restrictive first.

        Idempotent by construction: ``force_downgrade`` only ever lowers the mode, the risk state is
        only ever escalated (:meth:`_escalate`), and re-triggering the kill switch is a no-op on state.
        Re-running on startup against the same equity therefore reaches the same place (§2.6 step 2).
        """
        for breach in sorted(breaches, key=lambda b: _ACTION_RANK[b.action], reverse=True):
            reason = f"{breach.rung}: equity {breach.equity} vs {breach.threshold}"
            if breach.action == "kill_forced_off":
                await kill_switch.trigger(reason, actor=Actor.RISK_GATE, flatten=True)
                await mode_manager.force_downgrade(Mode.OFF, breach.rung)
            else:
                await self._escalate(mode_manager, RiskState.CLOSE_ONLY, reason)
                await mode_manager.force_downgrade(Mode.RECOMMEND, breach.rung)
                # Phase 3 wires the real exit-all here; Phase 2 runs alert-only when unwired.
                if breach.action == "forced_exit_close_only" and flatten is not None:
                    await flatten()
            _log.critical("floor_breach_applied", rung=breach.rung, equity=str(breach.equity),
                          threshold=str(breach.threshold), action=breach.action)
            if alert is not None:
                await alert(f"FLOOR BREACH {breach.rung}: {reason}")

    async def _escalate(self, mode_manager: ModeManager, to: RiskState, reason: str) -> None:
        """Set the risk state only if ``to`` is strictly MORE restrictive than the current one — a
        milder rung firing after a harsher one must never relax the state (§3.5.3)."""
        if _RISK_RANK[mode_manager.risk_state()] >= _RISK_RANK[to]:
            return
        await mode_manager.set_risk_state(to, reason, Actor.RISK_GATE)
