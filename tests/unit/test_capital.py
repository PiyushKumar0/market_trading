"""O19 capital change (design §3.2, §4, §5): the verified three-line rewrite, the refusals, the brake
check in both scopes, and the real path through ProtectedStore + LimitsEngine on a temp copy of the
pinned limits.yaml. The real config files are never touched.

Every figure here is the conftest pin's (``PINNED_CAPITAL``: base 40,000, caps 40,000 / 12,000), never
the live repo file's: a dashboard change rewrites those three values and appends provenance to their
comments, so no assertion may lean on the live values or on the comment text."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.enums import Actor
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops import capital as cap
from engine.ops.capital import CapitalChanger, CapitalRefused
from engine.risk.exposure import ExposureTracker
from engine.risk.limits import LimitsEngine, LimitTable
from engine.risk.mode import ModeManager
from engine.strategy.hi52_edge import StrategyEdges, derived_edge_pct
from engine.strategy.scanners import hi52

SHIPPED = config_dir()                       # the session's pinned copy (tests/conftest.py), not the repo's
CALENDAR = SHIPPED / "calendar"

TRADING_DAY_AFTER_CLOSE = datetime(2026, 6, 17, 16, 0, tzinfo=IST)   # Wed, a regular session
OPEN = datetime(2026, 6, 17, 9, 15, tzinfo=IST)
CLOSE = datetime(2026, 6, 17, 15, 30, tzinfo=IST)
HOLIDAY_MIDDAY = datetime(2026, 10, 2, 11, 0, tzinfo=IST)            # Gandhi Jayanti (a Friday)
PAST_HORIZON = datetime(2027, 1, 4, 11, 0, tzinfo=IST)                # 2026.yaml is verified to 12-31
STAMP = "2026-06-17"
OWNER = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test seed")


class _Now:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


@dataclass
class Rig:
    cfg: Path
    conn: object
    now: _Now
    clock: Clock
    store: ProtectedStore
    limits: LimitsEngine
    mode: ModeManager
    real: ExposureTracker
    changer: CapitalChanger
    sent: list[CatalogMessage]

    @property
    def limits_path(self) -> Path:
        return self.cfg / "limits.yaml"

    def audit_rows(self) -> list:
        return self.conn.execute(
            "SELECT diff FROM config_audit WHERE name = 'limits.yaml' ORDER BY rowid"
        ).fetchall()

    def signature(self) -> str:
        return self.conn.execute(
            "SELECT sha256 FROM protected_config WHERE name = 'limits.yaml'"
        ).fetchone()["sha256"]


def _rig(tmp_path, conn, *, at=TRADING_DAY_AFTER_CLOSE, paper_mark=None, real_mark=None, last_mark=None,
         edges=None, notify=True) -> Rig:
    cfg = tmp_path / "config"
    cfg.mkdir()
    for name in ("limits.yaml", "envelope.yaml"):
        shutil.copyfile(SHIPPED / name, cfg / name)
    now = _Now(at)
    clock = Clock(time_source=now)
    store = ProtectedStore(cfg, conn, clock)
    store.register_initial("limits.yaml", OWNER)
    store.register_initial("envelope.yaml", OWNER)
    limits = LimitsEngine(store)
    mode = ModeManager(conn, clock)
    real = ExposureTracker(conn, clock, lambda: limits.load().capital_base_inr, mark_price=real_mark)
    sent: list[CatalogMessage] = []

    async def _notify(msg: CatalogMessage) -> None:
        sent.append(msg)

    changer = CapitalChanger(
        conn=conn, clock=clock, calendar=NSECalendar(CALENDAR, clock), store=store, limits=limits,
        mode=mode, real=real, paper_mark=paper_mark, last_mark=last_mark, edges=edges,
        notify=_notify if notify else None,
    )
    return Rig(cfg, conn, now, clock, store, limits, mode, real, changer, sent)


def _closed_position(conn, pid: str, realized: str, *, paper: bool = False,
                     closed_at: str = "2026-06-10T15:00:00+05:30") -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin, "
        "opened_at, closed_at, realized_pnl, costs, is_paper) "
        "VALUES (?, 'AAA', 'BUY', 'CNC', 10, '100', 'CLOSED', 'platform', ?, ?, ?, '0', ?)",
        (pid, "2026-06-09T10:00:00+05:30", closed_at, realized, int(paper)),
    )
    conn.commit()


def _open_position(conn, pid: str, symbol: str = "AAA", *, qty: int = 50, entry: str = "300",
                   paper: bool = False) -> None:
    """An OPEN CNC long: 50 @ 300 is 15,000 notional."""
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin, "
        "opened_at, is_paper) VALUES (?, ?, 'BUY', 'CNC', ?, ?, 'OPEN', ?, '2026-06-10T10:00:00+05:30', ?)",
        (pid, symbol, qty, entry, "platform" if paper else "recommended", int(paper)),
    )
    conn.commit()


def _snapshot(conn, open_mtm: str, *, realized: str = "0", paper: bool = False,
              at: str = "2026-06-16T15:29:00+05:30") -> None:
    """The prior session's last equity persist (the day baseline and the drawdown window read it)."""
    table = "paper_equity_snapshots" if paper else "equity_snapshots"
    equity = Decimal(40000) + Decimal(realized) + Decimal(open_mtm)
    conn.execute(
        f"INSERT INTO {table} (at, equity, realized_pnl, open_mtm, day_mtm, positions_open) "
        "VALUES (?, ?, ?, ?, '0', 1)", (at, str(equity), realized, open_mtm),
    )
    conn.commit()


def _no_mark(symbol: str) -> Decimal | None:
    """A live cache after an out-of-session restart: empty."""
    return None


def _stored(prices: dict, calls: list | None = None):
    """A ``last_mark`` over fixed closes; an Exception value raises for that symbol."""

    async def last_mark(symbol: str, at: datetime):
        if calls is not None:
            calls.append((symbol, at))
        px = prices.get(symbol)
        if isinstance(px, Exception):
            raise px
        return None if px is None else (Decimal(px), datetime(2026, 6, 17, 15, 29, tzinfo=IST))

    return last_mark


def _table(text: str) -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load(text))


def _shipped_text() -> str:
    return (SHIPPED / "limits.yaml").read_bytes().decode("utf-8")


def _sub1(pattern: str, repl: str, text: str) -> str:
    """Exactly one regex substitution; a silent no-op would make the calling test vacuous."""
    out, n = re.subn(pattern, repl, text, flags=re.MULTILINE)
    assert n == 1, f"{pattern!r} matched {n} times"
    return out


def test_the_module_reads_the_pinned_config_and_its_calendar():
    """E3 (R12's class): the live repo config moves with every dashboard change; the pin does not."""
    assert SHIPPED == config_dir() and (CALENDAR / "2026.yaml").is_file()
    assert _table(_shipped_text()).capital_base_inr == 40000                  # PINNED_CAPITAL


# --------------------------------------------------------------------------- the rewrite (§4)
def _rewrite(text: str, new_base: int) -> str:
    table = _table(text)
    new = cap.scaled_values(table, new_base)
    old = {path: int(_get(table, path)) for path in new}
    return cap.rewrite_limits_text(text, old, new, STAMP)


def _get(table: LimitTable, path: tuple[str, ...]):
    node = table.model_dump()
    for key in path:
        node = node[key]
    return node


def test_the_rewrite_changes_exactly_three_lines_and_keeps_every_other_byte():
    old_text = _shipped_text()
    new_text = _rewrite(old_text, 60000)
    old_lines, new_lines = old_text.splitlines(), new_text.splitlines()
    assert len(old_lines) == len(new_lines)
    changed = [i for i, (a, b) in enumerate(zip(old_lines, new_lines, strict=True)) if a != b]
    assert len(changed) == 3
    by_key = {new_lines[i].split(":")[0].strip(): (old_lines[i], new_lines[i]) for i in changed}
    assert set(by_key) == {"capital_base_inr", "max_deployed_capital_inr", "cnc_notional_inr"}
    for before, after in by_key.values():
        comment = before[before.index("#"):]
        assert after.split(":")[0] == before.split(":")[0]               # indent and key unchanged
        assert comment + f" · owner via dashboard {STAMP}: " in after    # provenance kept, appended
    assert by_key["capital_base_inr"][1].endswith(f" · owner via dashboard {STAMP}: 40000 → 60000")
    assert by_key["max_deployed_capital_inr"][1].endswith(f"{STAMP}: 40000 → 60000")
    assert by_key["cnc_notional_inr"][1].endswith(f" · owner via dashboard {STAMP}: 12000 → 18000")
    assert "\r" not in new_text and new_text.endswith("\n")
    parsed = yaml.safe_load(new_text)
    assert type(parsed["capital_base_inr"]) is int and parsed["capital_base_inr"] == 60000
    assert type(parsed["limits"]["capital_cap"]["max_deployed_capital_inr"]) is int
    assert parsed["limits"]["per_stock_exposure"]["cnc_notional_inr"] == 18000


def test_a_line_without_a_comment_gets_one():
    # End-anchored: the comment carries provenance once the dashboard has changed the capital.
    text = _sub1(r"^([ \t]*cnc_notional_inr: 12000)[ \t]*#.*$", r"\1", _shipped_text())
    assert "cnc_notional_inr: 12000\n" in text
    new_text = _rewrite(text, 60000)
    assert f"cnc_notional_inr: 18000  # owner via dashboard {STAMP}: 12000 → 18000\n" in new_text


def test_a_duplicate_key_line_is_refused_422():
    text = _sub1(r"^schema_version: 1\n", "schema_version: 1\ncapital_base_inr: 40000\n", _shipped_text())
    with pytest.raises(CapitalRefused) as exc:
        cap.rewrite_limits_text(
            text, {cap.BASE_PATH: 40000}, {cap.BASE_PATH: 60000}, STAMP)
    assert exc.value.status == 422
    assert exc.value.note == "limits.yaml layout: capital_base_inr found 2 times"


def test_a_missing_key_line_is_refused_422():
    text = _sub1(r"^capital_base_inr: 40000\b", "capital_base_inr: 4e4", _shipped_text())
    with pytest.raises(CapitalRefused) as exc:
        cap.rewrite_limits_text(text, {cap.BASE_PATH: 40000}, {cap.BASE_PATH: 60000}, STAMP)
    assert exc.value.status == 422 and "capital_base_inr" in exc.value.note


def test_caps_are_floored_never_rounded_up():
    table = _table(_shipped_text())
    scaled = cap.scaled_values(table, 50001)
    assert scaled == {
        cap.BASE_PATH: 50001,
        ("limits", "capital_cap", "max_deployed_capital_inr"): 50001,
        ("limits", "per_stock_exposure", "cnc_notional_inr"): 15000,   # 12000 × 50001/40000 = 15000.3
    }
    assert all(type(v) is int for v in scaled.values())
    assert cap.scaled_values(table, 20001)[("limits", "per_stock_exposure", "cnc_notional_inr")] == 6000


def test_check_structure_rejects_a_float_or_exponent_value():
    old_text = _shipped_text()
    bad = _sub1(r"^capital_base_inr: 40000\b", "capital_base_inr: 6E+4", old_text)
    with pytest.raises(CapitalRefused) as exc:
        cap.check_structure(old_text, bad, {cap.BASE_PATH: 60000})
    assert exc.value.status == 422


def test_check_structure_rejects_a_duplicate_key():
    old_text = _shipped_text()
    dup = _sub1(r"^schema_version: 1\n", "schema_version: 1\ncapital_base_inr: 60000\n", old_text)
    with pytest.raises(CapitalRefused) as exc:
        cap.check_structure(old_text, dup, {cap.BASE_PATH: 60000})
    assert exc.value.status == 422 and "duplicate" in exc.value.note


def test_check_structure_rejects_any_other_change():
    old_text = _shipped_text()
    new_text = _sub1(r"^(  max_new_trades_day:\n    count:) 5\b", r"\1 9", _rewrite(old_text, 60000))
    new = cap.scaled_values(_table(old_text), 60000)
    with pytest.raises(CapitalRefused) as exc:
        cap.check_structure(old_text, new_text, new)
    assert exc.value.status == 422


def test_check_structure_returns_the_validated_new_table():
    old_text = _shipped_text()
    new = cap.scaled_values(_table(old_text), 60000)
    table = cap.check_structure(old_text, _rewrite(old_text, 60000), new)
    assert isinstance(table, LimitTable) and table.capital_base_inr == 60000
    assert table.limits.per_stock_exposure.cnc_notional_inr == 18000


# --------------------------------------------------------------------------- bounds (§5)
@pytest.mark.parametrize("new", [80000, 20000, 60000, 20001])
async def test_bounds_accept_half_to_double(tmp_path, conn, new):
    rig = _rig(tmp_path, conn)
    out = await rig.changer.apply(new, 40000)
    assert out["ok"] is True and rig.limits.load().capital_base_inr == new


@pytest.mark.parametrize("new", [80001, 19999, 40000, 0, -5])
async def test_bounds_refuse_422_and_write_nothing(tmp_path, conn, new):
    rig = _rig(tmp_path, conn)
    before = rig.limits_path.read_bytes()
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(new, 40000)
    assert exc.value.status == 422
    assert rig.limits_path.read_bytes() == before and len(rig.audit_rows()) == 1   # the seed row only


async def test_a_non_int_capital_is_refused_422(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    for bad in (60000.0, True, "60000"):
        with pytest.raises(CapitalRefused) as exc:
            await rig.changer.apply(bad, 40000)  # type: ignore[arg-type]
        assert exc.value.status == 422


# --------------------------------------------------------------------------- window + calendar (§5)
@pytest.mark.parametrize("at", [OPEN, CLOSE, datetime(2026, 6, 17, 12, 0, tzinfo=IST), PAST_HORIZON])
async def test_the_session_window_and_an_uncovered_day_are_refused_409(tmp_path, conn, at):
    rig = _rig(tmp_path, conn, at=at)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409
    assert rig.limits.load().capital_base_inr == 40000


@pytest.mark.parametrize("at", [
    datetime(2026, 6, 17, 9, 14, 59, tzinfo=IST),
    datetime(2026, 6, 17, 15, 30, 1, tzinfo=IST),
    HOLIDAY_MIDDAY,
])
async def test_one_second_outside_the_session_and_a_holiday_are_allowed(tmp_path, conn, at):
    rig = _rig(tmp_path, conn, at=at)
    assert (await rig.changer.preview(60000))["allowed"] is True
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True


async def test_auto_mode_is_refused_409(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    conn.execute("UPDATE mode_state SET mode = 'AUTO' WHERE id = 1")
    conn.commit()
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409 and "AUTO" in exc.value.note
    assert (await rig.changer.preview(None))["allowed"] is False


# --------------------------------------------------------------------------- stale dialog (review focus 2)
async def test_a_repeated_post_is_refused_409_and_changes_nothing(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    await rig.changer.apply(60000, 40000)
    after_first = (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows()))
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409
    assert (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())) == after_first
    assert len(rig.sent) == 1


# --------------------------------------------------------------------------- unverified (review focus 3)
async def test_an_unverified_file_is_503_and_nothing_is_written(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    rig.limits.load()                                       # the engine booted on the signed file
    with open(rig.limits_path, "ab") as fh:
        fh.write(b"#")
    tampered, sig, audits = rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 503 and exc.value.note.startswith("protected config unverified: ")
    assert rig.limits_path.read_bytes() == tampered
    assert rig.signature() == sig and len(rig.audit_rows()) == audits
    preview = await rig.changer.preview(None)
    assert preview["allowed"] is False and preview["refusal"].startswith("protected config unverified")
    assert rig.sent == []


async def test_a_missing_file_is_503(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    rig.limits.load()
    rig.limits_path.unlink()
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 503


# --------------------------------------------------------------------------- brakes (review focus 1)
async def test_a_cut_that_trips_the_real_equity_floor_is_refused_422(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    _closed_position(conn, "P1", "-3000")                   # equity 37,000 at 40k; 17,000 at 20k
    before = rig.limits_path.read_bytes()
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(20000, 40000)
    assert exc.value.status == 422
    assert exc.value.note == "would trip: real:equity_floor_rung (₹18,000.00)"
    assert rig.limits_path.read_bytes() == before
    preview = await rig.changer.preview(20000)
    assert preview["allowed"] is False and preview["would_trip"] == ["real:equity_floor_rung"]
    assert preview["refusal"] == exc.value.note
    assert preview["proposed"]["equity_inr"] == "17000.00"
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True


async def test_a_cut_that_trips_the_paper_equity_floor_is_refused_422(tmp_path, conn):
    rig = _rig(tmp_path, conn, paper_mark=lambda symbol: None)
    _closed_position(conn, "PP1", "-3000", paper=True)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(20000, 40000)
    assert exc.value.status == 422
    assert exc.value.note == "would trip: paper:equity_floor_rung (₹18,000.00)"
    current = (await rig.changer.preview(None))["current"]
    assert current["paper_equity_inr"] == "37000.00"
    assert current["equity_inr"] == "40000.00"                                # the real book is clean
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True


async def test_without_a_paper_mark_the_paper_book_is_still_checked(tmp_path, conn):
    """E2 (A3): paper not composed at this boot, so no runtime mark; its epoch's rows still meet the
    new base at the next boot that composes it, so they are checked all the same."""
    rig = _rig(tmp_path, conn, paper_mark=None)
    _closed_position(conn, "PP1", "-3000", paper=True)
    assert (await rig.changer.preview(None))["current"]["paper_equity_inr"] == "37000.00"
    before = (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows()))
    preview = await rig.changer.preview(20000)
    assert preview["allowed"] is False and preview["would_trip"] == ["paper:equity_floor_rung"]
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(20000, 40000)
    assert exc.value.status == 422
    assert exc.value.note == "would trip: paper:equity_floor_rung (₹18,000.00)"
    assert (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())) == before


async def test_without_a_paper_mark_a_held_paper_position_is_priced_from_the_store(tmp_path, conn):
    rig = _rig(tmp_path, conn, paper_mark=None, last_mark=_stored({"BBB": "230"}))
    _open_position(conn, "PO", "BBB", paper=True)
    assert (await rig.changer.preview(None))["current"]["paper_equity_inr"] == "36500.00"


async def test_a_book_with_no_rows_trips_nothing_and_shows_its_base(tmp_path, conn):
    rig = _rig(tmp_path, conn, paper_mark=None)
    preview = await rig.changer.preview(20000)
    assert preview["allowed"] is True and preview["would_trip"] == []
    assert preview["current"]["paper_equity_inr"] == "40000.00"
    assert preview["proposed"]["paper_equity_inr"] == "20000.00"


# --------------------------------------------------------------------------- held positions after a restart (E1)
# The state after any out-of-session restart: both live caches are empty until the next session, so a
# held position is priced at the last stored 1m close, the price the next session's first evaluation
# starts from. 50 @ 300 last marked 230 is open MTM −3,500: equity 36,500 at 40k, 30,500 at 34k
# (≤ the 30,600 GO FLAT line).
@pytest.mark.parametrize("paper", [False, True])
async def test_a_cold_restart_still_refuses_a_cut_the_next_session_would_trip(tmp_path, conn, paper):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, paper_mark=_no_mark, last_mark=_stored({"AAA": "230"}))
    _open_position(conn, "P1", paper=paper)
    _snapshot(conn, "-3500", paper=paper)
    scope, key, other = ("paper", "paper_equity_inr", "equity_inr") if paper else \
        ("real", "equity_inr", "paper_equity_inr")
    before = (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows()))
    preview = await rig.changer.preview(34000)
    assert preview["allowed"] is False and preview["would_trip"] == [f"{scope}:equity_floor_rung"]
    assert preview["refusal"] == f"would trip: {scope}:equity_floor_rung (₹30,600.00)"
    assert preview["current"][key] == "36500.00" and preview["current"][other] == "40000.00"
    assert preview["proposed"][key] == "30500.00"
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(34000, 40000)
    assert exc.value.status == 422 and exc.value.note == preview["refusal"]
    assert (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())) == before
    assert rig.limits.load().capital_base_inr == 40000 and rig.sent == []


@pytest.mark.parametrize("last_mark", [None, _stored({})], ids=["not_wired", "no_bar"])
async def test_a_held_position_with_no_price_at_all_refuses_409(tmp_path, conn, last_mark):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, paper_mark=_no_mark, last_mark=last_mark)
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "P2", "CCC")
    _open_position(conn, "PO", "BBB", paper=True)
    note = ("no price yet for held positions (real: AAA, CCC; paper: BBB), so the brakes can't be "
            "checked; retry in a minute, and if it persists, change capital after the next session's close")
    before = (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows()))
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert (exc.value.status, exc.value.note) == (409, note)
    assert (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())) == before
    for proposed in (60000, None):
        preview = await rig.changer.preview(proposed)
        assert preview["allowed"] is False and preview["refusal"] == note
        assert preview["proposed"] is None and preview["would_trip"] == []
        # No equity figure that counts a holding at its entry price.
        assert [preview["current"][k] for k in ("equity_inr", "paper_equity_inr", "risk_intraday_inr",
                                                "risk_swing_inr")] == [None] * 4
        assert preview["current"]["capital_inr"] == "40000.00"


async def test_the_missing_price_note_names_only_the_unpriced_book(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, paper_mark=_no_mark, last_mark=_stored({"AAA": "300"}))
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "PO", "BBB", paper=True)
    preview = await rig.changer.preview(None)
    assert preview["refusal"] == ("no price yet for held positions (paper: BBB), so the brakes can't be "
                                  "checked; retry in a minute, and if it persists, change capital after the "
                                  "next session's close")
    assert preview["current"]["equity_inr"] == "40000.00" and preview["current"]["paper_equity_inr"] is None


async def test_a_gate_or_bounds_refusal_comes_before_the_missing_price(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=None)
    _open_position(conn, "P1", "AAA")
    assert (await rig.changer.preview(100000))["refusal"].startswith("one change can at most halve or double")
    conn.execute("UPDATE mode_state SET mode = 'AUTO' WHERE id = 1")
    conn.commit()
    assert "AUTO" in (await rig.changer.preview(60000))["refusal"]
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409 and "AUTO" in exc.value.note


async def test_a_cold_restart_does_not_falsely_refuse_for_day_loss(tmp_path, conn):
    """A1 (2): a position in profit at the last snapshot. Priced at entry, today's P&L would read
    −1,500 (−5% of 30,000: the soft line); priced at its stored close, today's P&L is 0."""
    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=_stored({"AAA": "330"}))
    _open_position(conn, "P1", "AAA")
    _snapshot(conn, "1500")
    preview = await rig.changer.preview(30000)
    assert preview["allowed"] is True and preview["would_trip"] == [], preview["refusal"]
    assert preview["current"]["equity_inr"] == "41500.00"
    out = await rig.changer.apply(30000, 40000)
    assert out["ok"] is True and out["old"]["equity_inr"] == "41500.00"
    assert out["new"]["equity_inr"] == "31500.00"


async def test_the_live_mark_wins_over_the_stored_close(tmp_path, conn):
    calls: list = []
    rig = _rig(tmp_path, conn, real_mark={"AAA": Decimal("250")}.get,
               last_mark=_stored({"AAA": "230", "CCC": "110"}, calls))
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "P2", "CCC", qty=10, entry="100")
    current = (await rig.changer.preview(None))["current"]
    assert current["equity_inr"] == "37600.00"            # 40,000 + 50 × (250 − 300) + 10 × (110 − 100)
    assert [symbol for symbol, _at in calls] == ["CCC"]    # a live-marked symbol is never looked up
    assert calls[0][1] == TRADING_DAY_AFTER_CLOSE          # the last bar at or before now


async def test_a_failing_stored_read_leaves_that_symbol_unpriced(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark,
               last_mark=_stored({"AAA": RuntimeError("duckdb busy"), "CCC": "100"}))
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "P2", "CCC")
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409
    assert exc.value.note.startswith("no price yet for held positions (real: AAA), ")
    assert rig.limits.load().capital_base_inr == 40000


async def test_a_bad_stored_close_is_no_price(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=_stored({"AAA": "0", "CCC": "NaN"}))
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "P2", "CCC")
    assert (await rig.changer.preview(None))["refusal"].startswith(
        "no price yet for held positions (real: AAA, CCC), ")


async def test_the_prefetch_is_bounded_and_a_cut_off_symbol_is_unpriced(tmp_path, conn, monkeypatch):
    monkeypatch.setattr(cap, "PREFETCH_S", 0.05)
    never = asyncio.Event()

    async def wedged(symbol, at):
        if symbol == "CCC":
            await never.wait()                                        # a store that never answers
        return Decimal("300"), at

    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=wedged)
    _open_position(conn, "P1", "AAA")
    _open_position(conn, "P2", "CCC")
    with pytest.raises(CapitalRefused) as exc:
        await asyncio.wait_for(rig.changer.apply(60000, 40000), timeout=10)   # unbounded: TimeoutError
    assert exc.value.note.startswith("no price yet for held positions (real: CCC), ")


async def test_the_price_prefetch_happens_before_the_table_is_read(tmp_path, conn, monkeypatch):
    """Ruling R7: the prefetch is the only await before ``limits.reload()``, and it runs before the
    table (or the signed file) is read. Moving it after ``_loaded()`` fails here."""
    loads: list[int] = []
    reads: list[str] = []
    seen: list[tuple[int, int]] = []

    async def probe(symbol, at):
        seen.append((len(loads), len(reads)))
        return Decimal("300"), at

    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=probe)
    _open_position(conn, "P1", "AAA")
    load, read = rig.limits.load, rig.store.read_verified_bytes
    monkeypatch.setattr(rig.limits, "load", lambda: (loads.append(1), load())[1])
    monkeypatch.setattr(rig.store, "read_verified_bytes", lambda name: (reads.append(name), read(name))[1])
    assert (await rig.changer.preview(60000))["allowed"] is True
    assert seen == [(0, 0)] and loads and reads
    loads.clear()
    reads.clear()
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True
    assert seen == [(0, 0), (0, 0)] and loads and reads


async def test_the_now_trackers_are_read_only_twins(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=_stored({"AAA": "230"}))
    _open_position(conn, "P1", "AAA")
    real_now, paper_now, unpriced = rig.changer._now({"AAA": Decimal("230")})
    assert unpriced == {} and real_now.equity() == Decimal("36500")
    for book in (real_now, paper_now):
        with pytest.raises(RuntimeError):
            book.persist_snapshot()
    assert rig.real.equity() == Decimal("40000")           # the live tracker keeps its own (empty) marks


async def test_a_cut_that_trips_the_day_loss_lines_names_them(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    _closed_position(conn, "P1", "-1500", closed_at="2026-06-17T11:00:00+05:30")   # −3.75% today
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(20000, 40000)                                       # −7.5% at 20k
    assert exc.value.status == 422
    assert exc.value.note == (
        "would trip: real:daily_loss_hard (-₹1,400.00); real:daily_loss_soft (-₹1,000.00)")


async def test_an_already_tripped_rung_does_not_block_a_raise(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    _closed_position(conn, "P1", "-4500")                   # equity 35,500 ≤ 36,000: tripped now
    assert (await rig.changer.preview(41000))["allowed"] is True   # still tripped at 41k (≤ 36,900): not new
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True


async def test_an_already_tripped_rung_does_not_block_a_cut_either(tmp_path, conn):
    """R8's current-table side: the current book is judged against the CURRENT table's lines.
    Against the new table's (31,500 at 35k), 35,500 would read untripped now, and the rung the
    what-if trips (30,500 ≤ 31,500) would wrongly count as new."""
    rig = _rig(tmp_path, conn)
    _closed_position(conn, "P1", "-4500")                   # equity 35,500 ≤ 36,000: tripped now
    preview = await rig.changer.preview(35000)
    assert preview["allowed"] is True and preview["would_trip"] == [], preview["refusal"]
    out = await rig.changer.apply(35000, 40000)
    assert out["ok"] is True and rig.limits.load().capital_base_inr == 35000


# --------------------------------------------------------------------------- the real path (§8)
async def test_the_real_path_signs_reloads_and_reaches_every_reader(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    audits = len(rig.audit_rows())
    out = await rig.changer.apply(60000, 40000)

    assert rig.store.load_verified("limits.yaml")["capital_base_inr"] == 60000
    table = rig.limits.load()
    assert table.capital_base_inr == 60000
    assert rig.real.capital_base == 60000                           # no setter: the one live source
    assert table.limits.capital_cap.max_deployed_capital_inr == 60000
    assert table.limits.per_stock_exposure.cnc_notional_inr == 18000

    rows = rig.audit_rows()
    assert len(rows) == audits + 1
    note = json.loads(rows[-1]["diff"])["note"]
    assert "40000" in note and "60000" in note

    assert [m.kind for m in rig.sent] == [MessageKind.CAPITAL_CHANGED]
    assert out["commit_hint"] == "commit config/limits.yaml"
    assert out["old"]["capital_inr"] == "40000.00" and out["new"]["capital_inr"] == "60000.00"
    assert out["new"]["cnc_notional_inr"] == "18000.00"
    assert out["new"]["risk_swing_inr"] == "1200.00"                 # 2% × equity 60,000
    assert out["new"]["day_loss_soft_inr"] == "-3000.00"
    assert out["new"]["equity_floor_inr"] == "54000.00"
    assert out["new"]["cumulative_floor_inr"] == "51000.00"
    assert out["new"]["paper_equity_inr"] == "60000.00"              # an empty paper book: its base
    assert (await rig.changer.preview(None))["current"] == out["new"]


async def test_a_failing_notify_is_logged_and_never_raised(tmp_path, conn, caplog):
    rig = _rig(tmp_path, conn)

    async def boom(msg):
        raise RuntimeError("telegram down")

    rig.changer._notify = boom
    with caplog.at_level(logging.ERROR, logger="engine.ops.capital"):
        out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True and rig.limits.load().capital_base_inr == 60000
    assert "capital_change_notify_failed" in [r.getMessage() for r in caplog.records]


async def test_a_slow_notify_does_not_hold_the_answer_and_still_delivers(tmp_path, conn, monkeypatch, caplog):
    """E4 (D2): the change is live before the send; the dashboard abandons the request at 15 s while
    a Telegram send can retry for over a minute."""
    monkeypatch.setattr(cap, "NOTIFY_WAIT_S", 0.05)
    rig = _rig(tmp_path, conn)
    delivered: list[CatalogMessage] = []
    release = asyncio.Event()                                         # the send outlasts the bound

    async def slow(msg):
        await release.wait()
        delivered.append(msg)

    rig.changer._notify = slow
    with caplog.at_level(logging.WARNING, logger="engine.ops.capital"):
        out = await asyncio.wait_for(rig.changer.apply(60000, 40000), timeout=10)
    assert out["ok"] is True and rig.limits.load().capital_base_inr == 60000
    assert "capital_change_notify_slow" in [r.getMessage() for r in caplog.records]
    assert delivered == [] and len(rig.changer._sends) == 1          # still in flight, held
    release.set()
    await asyncio.wait_for(asyncio.gather(*rig.changer._sends), timeout=10)
    assert [m.kind for m in delivered] == [MessageKind.CAPITAL_CHANGED]
    await asyncio.sleep(0)                                            # the done callback runs
    assert rig.changer._sends == set()


async def test_a_notify_within_the_bound_is_awaited(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    await rig.changer.apply(60000, 40000)
    assert [m.kind for m in rig.sent] == [MessageKind.CAPITAL_CHANGED]   # delivered before apply returned
    await asyncio.sleep(0)
    assert rig.changer._sends == set()


# --------------------------------------------------------------------------- after the write (T4)
async def test_a_reload_failure_is_503_and_a_retry_is_409_until_a_restart(tmp_path, conn, monkeypatch):
    """Signed on disk, not live: the cached table stays the old one, so every in-process reader
    agrees on the old base; a retry is built on a file that differs from it, so it is refused."""
    rig = _rig(tmp_path, conn)
    rig.limits.load()

    def broken():
        raise OSError("disk read error")

    monkeypatch.setattr(rig.limits, "reload", broken)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 503
    assert exc.value.note == ("limits.yaml was changed and signed, but the engine could not reload it: "
                              "disk read error; restart the engine before the next session")
    assert rig.store.load_verified("limits.yaml")["capital_base_inr"] == 60000     # the signed file: new
    assert rig.limits.load().capital_base_inr == 40000 and rig.real.capital_base == 40000   # cached: old
    assert rig.sent == []
    with pytest.raises(CapitalRefused) as retry:
        await rig.changer.apply(60000, 40000)
    assert retry.value.status == 409 and "restart the engine" in retry.value.note
    assert (await rig.changer.preview(None))["allowed"] is False


async def test_a_real_tracker_not_reading_the_reloaded_table_is_logged_critical(tmp_path, conn, caplog):
    """A wiring self-check: it logs and never refuses (the change is already live)."""
    rig = _rig(tmp_path, conn)
    rig.changer._real = ExposureTracker(conn, rig.clock, Decimal(40000))     # a boot-time copy of the base
    with caplog.at_level(logging.CRITICAL, logger="engine.ops.capital"):
        out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True and rig.limits.load().capital_base_inr == 60000
    assert "capital_change_base_mismatch" in [r.getMessage() for r in caplog.records]


async def test_a_live_reading_real_tracker_logs_no_mismatch(tmp_path, conn, caplog):
    rig = _rig(tmp_path, conn)
    with caplog.at_level(logging.CRITICAL, logger="engine.ops.capital"):
        await rig.changer.apply(60000, 40000)
    assert "capital_change_base_mismatch" not in [r.getMessage() for r in caplog.records]


async def test_without_notify_the_change_still_applies(tmp_path, conn):
    rig = _rig(tmp_path, conn, notify=False)
    out = await rig.changer.apply(60000, 40000)
    assert out["ok"] is True


async def test_a_cas_conflict_in_owner_update_is_409(tmp_path, conn, monkeypatch):
    rig = _rig(tmp_path, conn)

    def conflict(*args, **kwargs):
        raise IntegrityError("limits.yaml signature changed since it was read")

    monkeypatch.setattr(rig.store, "owner_update", conflict)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409
    assert exc.value.note == "limits.yaml changed since it was read; reopen the dialog"
    assert rig.limits.load().capital_base_inr == 40000 and rig.sent == []


async def test_any_other_write_failure_is_503_and_says_to_check_integrity(tmp_path, conn, monkeypatch):
    rig = _rig(tmp_path, conn)

    def disk_full(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(rig.store, "owner_update", disk_full)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 503
    assert exc.value.note == "limits.yaml write failed: disk full; check its integrity before the next boot"
    assert rig.sent == []


async def test_a_signed_file_that_differs_from_the_loaded_limits_is_refused_409(tmp_path, conn):
    """An out-of-band owner_update (the seed script) while the engine runs: the engine still trades
    the cached table, so a change built on the file would mix two tables."""
    rig = _rig(tmp_path, conn)
    rig.limits.load()
    other = _sub1(r"^(  max_new_trades_day:\n    count:) 5\b", r"\1 4", _shipped_text())
    rig.store.owner_update("limits.yaml", other, OWNER)
    with pytest.raises(CapitalRefused) as exc:
        await rig.changer.apply(60000, 40000)
    assert exc.value.status == 409
    assert rig.limits_path.read_text(encoding="utf-8") == other


# --------------------------------------------------------------------------- preview figures (§6)
async def test_the_preview_figures_at_the_pinned_limits(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    preview = await rig.changer.preview(None)
    assert preview["proposed"] is None and preview["allowed"] is True and preview["refusal"] is None
    assert preview["would_trip"] == []
    assert preview["current"] == {
        "capital_inr": "40000.00",
        "max_deployed_capital_inr": "40000.00",
        "cnc_notional_inr": "12000.00",
        "equity_inr": "40000.00",
        "paper_equity_inr": "40000.00",
        "risk_intraday_inr": "400.00",
        "risk_swing_inr": "800.00",
        "day_loss_soft_inr": "-2000.00",
        "day_loss_hard_inr": "-2800.00",
        "equity_floor_inr": "36000.00",
        "cumulative_floor_inr": "34000.00",
        "hi52_edge_pct": None,
    }


async def test_the_preview_in_the_window_shows_the_figures_but_refuses(tmp_path, conn):
    rig = _rig(tmp_path, conn, at=datetime(2026, 6, 17, 12, 0, tzinfo=IST))
    preview = await rig.changer.preview(60000)
    assert preview["allowed"] is False and "market" in preview["refusal"]
    assert preview["proposed"]["capital_inr"] == "60000.00"
    assert (await rig.changer.preview(None))["allowed"] is False


async def test_the_preview_of_an_out_of_bounds_value_has_no_proposed_figures(tmp_path, conn):
    rig = _rig(tmp_path, conn)
    preview = await rig.changer.preview(100000)
    assert preview["allowed"] is False and preview["proposed"] is None and preview["refusal"]


async def test_the_preview_never_writes(tmp_path, conn):
    rig = _rig(tmp_path, conn, real_mark=_no_mark, last_mark=_stored({"AAA": "230"}))
    _open_position(conn, "P1", "AAA")
    _snapshot(conn, "-3500")
    before = (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows()))
    snapshots = conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0]
    await rig.changer.preview(60000)
    await rig.changer.preview(20000)
    assert (rig.limits_path.read_bytes(), rig.signature(), len(rig.audit_rows())) == before
    assert conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0] == snapshots


async def test_the_hi52_figure_is_min_of_registered_and_derived(tmp_path, conn):
    registered = Decimal("1.53")
    holder: dict = {}
    edges = StrategyEdges({hi52.STRATEGY_ID: registered}, table_fn=lambda: holder["limits"].load())
    rig = _rig(tmp_path, conn, edges=edges)
    holder["limits"] = rig.limits
    cut = await rig.changer.preview(20000)
    raise_ = await rig.changer.preview(80000)
    current_table = rig.limits.load()
    cut_table = current_table.model_copy(update={"capital_base_inr": Decimal(20000)})
    raise_table = current_table.model_copy(update={"capital_base_inr": Decimal(80000)})
    assert cut["current"]["hi52_edge_pct"] == str(min(registered, derived_edge_pct(current_table)))
    assert cut["proposed"]["hi52_edge_pct"] == str(min(registered, derived_edge_pct(cut_table)))
    assert Decimal(cut["proposed"]["hi52_edge_pct"]) < registered
    assert raise_["proposed"]["hi52_edge_pct"] == str(min(registered, derived_edge_pct(raise_table)))
    assert raise_["proposed"]["hi52_edge_pct"] == "1.53"
