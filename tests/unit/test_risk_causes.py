"""Per-cause risk-state latch (§3.5.3): most-restrictive-wins composition and re-arm ONLY when every
latching cause has cleared. The property that matters operationally is the negative one — clearing one
cause while another is live must NOT reopen entries."""

from __future__ import annotations

import asyncio

import pytest

from engine.broker.ticker_supervisor import FeedHealth
from engine.core.db import connect
from engine.core.enums import Actor, RiskState
from engine.core.eventbus import EventBus
from engine.risk.causes import (
    CAUSE_FEED_STALE,
    CAUSE_OWNER_PAUSE,
    CAUSE_REJECTION_STORM,
    RiskStateLatch,
    feed_health_to_latch,
)
from engine.risk.events import TOPIC_RISK_STATE
from engine.risk.mode import ModeManager


@pytest.fixture
def latch(conn, clock) -> RiskStateLatch:
    return RiskStateLatch(conn, clock, ModeManager(conn, clock))


def _mode(conn, clock) -> ModeManager:
    return ModeManager(conn, clock)


# --------------------------------------------------------------------------- most-restrictive-wins
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        (RiskState.FROZEN, RiskState.CLOSE_ONLY, RiskState.CLOSE_ONLY),
        (RiskState.CLOSE_ONLY, RiskState.FROZEN, RiskState.CLOSE_ONLY),   # milder cause never relaxes
        (RiskState.FROZEN, RiskState.KILLED, RiskState.KILLED),
        (RiskState.KILLED, RiskState.CLOSE_ONLY, RiskState.KILLED),
        (RiskState.FROZEN, RiskState.FROZEN, RiskState.FROZEN),
    ],
)
async def test_two_causes_compose_most_restrictive(conn, clock, latch, first, second, expected):
    await latch.set_cause("cause_a", first, "a", Actor.RISK_GATE)
    await latch.set_cause("cause_b", second, "b", Actor.RISK_GATE)
    assert latch.resolved_state() == expected
    assert _mode(conn, clock).risk_state() == expected
    # active_causes is ordered most-restrictive first, so [0] is always the state in force.
    assert latch.active_causes()[0][1] == expected


@pytest.mark.asyncio
async def test_set_cause_applies_state_and_records_detail(conn, clock, latch):
    state = await latch.set_cause("stale_feed", RiskState.FROZEN, "tick age 7.2s", Actor.RISK_GATE)
    assert state == RiskState.FROZEN
    assert _mode(conn, clock).risk_state() == RiskState.FROZEN
    assert latch.active_causes() == [("stale_feed", RiskState.FROZEN, "tick age 7.2s")]
    row = conn.execute("SELECT * FROM risk_state_causes WHERE cause='stale_feed'").fetchone()
    assert row["state"] == "FROZEN" and row["cleared_at"] is None and row["set_at"]


@pytest.mark.asyncio
async def test_normal_is_not_a_latching_cause(latch):
    """NORMAL is the ABSENCE of causes — latching it would be a contradiction (and the table's CHECK
    constraint rejects it too)."""
    with pytest.raises(ValueError, match="not a latching state"):
        await latch.set_cause("bogus", RiskState.NORMAL, "", Actor.RISK_GATE)


# --------------------------------------------------------------------------- re-arm only when empty
@pytest.mark.asyncio
async def test_clearing_one_cause_does_not_rearm_while_another_latches(conn, clock, latch):
    await latch.set_cause("stale_feed", RiskState.FROZEN, "tick age", Actor.RISK_GATE)
    await latch.set_cause(CAUSE_REJECTION_STORM, RiskState.FROZEN, "3 rejects/60s", Actor.RISK_GATE)

    assert await latch.clear_cause("stale_feed", Actor.SYSTEM) == RiskState.FROZEN
    assert _mode(conn, clock).risk_state() == RiskState.FROZEN      # still frozen by the storm
    assert [c for c, _s, _d in latch.active_causes()] == [CAUSE_REJECTION_STORM]

    assert await latch.clear_cause(CAUSE_REJECTION_STORM, Actor.OWNER) == RiskState.NORMAL
    assert _mode(conn, clock).risk_state() == RiskState.NORMAL      # re-armed: ledger is empty
    assert latch.active_causes() == []


@pytest.mark.asyncio
async def test_clearing_the_harsher_cause_falls_back_to_the_milder_one(conn, clock, latch):
    await latch.set_cause("consecutive_losses", RiskState.FROZEN, "3 in a row", Actor.RISK_GATE)
    await latch.set_cause("equity_floor_rung", RiskState.CLOSE_ONLY, "-10%", Actor.RISK_GATE)
    assert latch.resolved_state() == RiskState.CLOSE_ONLY

    assert await latch.clear_cause("equity_floor_rung", Actor.OWNER) == RiskState.FROZEN
    assert _mode(conn, clock).risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_clear_stamps_cleared_at_and_is_idempotent(conn, clock, latch):
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner", Actor.OWNER)
    await latch.clear_cause(CAUSE_OWNER_PAUSE, Actor.OWNER)
    row = conn.execute("SELECT cleared_at FROM risk_state_causes WHERE cause=?", (CAUSE_OWNER_PAUSE,)).fetchone()
    stamped = row["cleared_at"]
    assert stamped is not None

    # Re-clearing an already-cleared cause changes nothing and still resolves to NORMAL.
    assert await latch.clear_cause(CAUSE_OWNER_PAUSE, Actor.OWNER) == RiskState.NORMAL
    again = conn.execute("SELECT cleared_at FROM risk_state_causes WHERE cause=?", (CAUSE_OWNER_PAUSE,)).fetchone()
    assert again["cleared_at"] == stamped
    # Clearing a cause that was never set is safe too.
    assert await latch.clear_cause("never_set", Actor.OWNER) == RiskState.NORMAL


@pytest.mark.asyncio
async def test_repeated_set_is_idempotent_and_keeps_the_episode_start(conn, clock, latch):
    await latch.set_cause("stale_feed", RiskState.FROZEN, "tick age 6s", Actor.RISK_GATE)
    first = conn.execute("SELECT set_at FROM risk_state_causes WHERE cause='stale_feed'").fetchone()["set_at"]

    await latch.set_cause("stale_feed", RiskState.FROZEN, "tick age 9s", Actor.RISK_GATE)
    row = conn.execute("SELECT set_at, detail FROM risk_state_causes WHERE cause='stale_feed'").fetchone()
    assert row["set_at"] == first          # same episode, not a fresh one
    assert row["detail"] == "tick age 9s"  # latest detail wins
    assert len(latch.active_causes()) == 1
    assert _mode(conn, clock).risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_escalating_then_clearing_a_single_cause_round_trips(conn, clock, latch):
    await latch.set_cause("token_invalid", RiskState.FROZEN, "session dead", Actor.RISK_GATE)
    # The same cause escalates in place (no duplicate row) — one row per cause is the PK contract.
    await latch.set_cause("token_invalid", RiskState.CLOSE_ONLY, "still dead", Actor.RISK_GATE)
    assert conn.execute("SELECT COUNT(*) AS n FROM risk_state_causes").fetchone()["n"] == 1
    assert _mode(conn, clock).risk_state() == RiskState.CLOSE_ONLY

    await latch.clear_cause("token_invalid", Actor.SYSTEM)
    assert _mode(conn, clock).risk_state() == RiskState.NORMAL


@pytest.mark.asyncio
async def test_a_cleared_cause_re_raised_starts_a_fresh_episode(conn, clock, latch):
    await latch.set_cause("stale_feed", RiskState.FROZEN, "first", Actor.RISK_GATE)
    conn.execute("UPDATE risk_state_causes SET set_at='2020-01-01T00:00:00+05:30' WHERE cause='stale_feed'")
    await latch.clear_cause("stale_feed", Actor.SYSTEM)

    await latch.set_cause("stale_feed", RiskState.FROZEN, "second", Actor.RISK_GATE)
    row = conn.execute("SELECT set_at, cleared_at FROM risk_state_causes WHERE cause='stale_feed'").fetchone()
    assert row["cleared_at"] is None
    assert row["set_at"] == clock.now().isoformat()      # fresh episode, Clock-stamped (never naive)


# --------------------------------------------------------------------------- stickiness (§2.6)
@pytest.mark.asyncio
async def test_latch_survives_a_restart(conn, clock, db_path, latch):
    await latch.set_cause(CAUSE_REJECTION_STORM, RiskState.FROZEN, "3 rejects/60s", Actor.RISK_GATE)
    conn2 = connect(db_path)
    try:
        reborn = RiskStateLatch(conn2, clock, ModeManager(conn2, clock))
        assert reborn.resolved_state() == RiskState.FROZEN
        assert [c for c, _s, _d in reborn.active_causes()] == [CAUSE_REJECTION_STORM]
        assert ModeManager(conn2, clock).risk_state() == RiskState.FROZEN
    finally:
        conn2.close()


@pytest.mark.asyncio
async def test_state_changes_are_published_and_audited(conn, clock, bus):
    """The latch delegates persistence/publish to ModeManager, so the §3.2.1 ``risk.state`` event and
    the config_audit row come for free — assert they actually do."""
    seen = []

    async def handler(event):
        seen.append(event)

    from engine.risk.events import TOPIC_RISK_STATE

    bus.subscribe(TOPIC_RISK_STATE, handler)
    mode = ModeManager(conn, clock, bus=bus)
    latch = RiskStateLatch(conn, clock, mode)

    await latch.set_cause("daily_loss_soft", RiskState.FROZEN, "day MTM -2.1%", Actor.RISK_GATE)
    await latch.clear_cause("daily_loss_soft", Actor.SYSTEM)

    assert [(e.old_state, e.new_state) for e in seen] == [
        (RiskState.NORMAL, RiskState.FROZEN),
        (RiskState.FROZEN, RiskState.NORMAL),
    ]
    assert "daily_loss_soft" in seen[0].reason and "re-armed" in seen[1].reason
    audits = conn.execute("SELECT diff FROM config_audit WHERE name='risk_state'").fetchall()
    assert len(audits) == 2


async def test_clear_inactive_cause_never_relaxes_an_out_of_ledger_state(conn, clock):
    """2026-07-28 review F1: /resume_entries (clear of causes that are NOT active) must not re-arm
    NORMAL over a freeze some latch-less path wrote directly (e.g. token-rejected)."""
    from engine.core.calendar import NSECalendar
    from engine.core.config import config_dir
    from engine.core.enums import Actor, RiskState
    from engine.risk.causes import CAUSE_OWNER_PAUSE, RiskStateLatch
    from engine.risk.mode import ModeManager

    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    latch = RiskStateLatch(conn, clock, mode)
    await mode.set_risk_state(RiskState.FROZEN, "kite_token_rejected", Actor.RISK_GATE)  # out-of-ledger

    resolved = await latch.clear_cause(CAUSE_OWNER_PAUSE, Actor.OWNER)

    assert resolved == RiskState.FROZEN
    assert mode.risk_state() == RiskState.FROZEN


# --------------------------------------------------------------------------- feed.health → feed_stale
async def _deliver(on_health, health: FeedHealth) -> None:
    """The subscriber returns at once and does its latch work in a task: let that task run."""
    await on_health(health)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_feed_stale_latches_through_warming_and_clears_on_healthy(conn, clock, latch):
    on_health = feed_health_to_latch(latch, lambda: True)
    await _deliver(on_health, FeedHealth(state="STALE", last_frame_age_s=11.0))
    for state in ("WARMING", "DEGRADED", "STOPPED"):                     # none of these is recovery
        await _deliver(on_health, FeedHealth(state=state))
    assert latch.active_causes() == [(CAUSE_FEED_STALE, RiskState.FROZEN, "no feed frame for 11s")]
    await _deliver(on_health, FeedHealth(state="HEALTHY"))
    assert latch.active_causes() == [] and _mode(conn, clock).risk_state() == RiskState.NORMAL


@pytest.mark.asyncio
async def test_feed_health_never_waits_on_the_owner_page(conn, clock):
    """The ticker publishes feed.health from its frame reader; a risk-state page stuck on Telegram
    (up to 95 s in an outage) must not hold it, while the ledger still moves at once."""
    bus = EventBus()
    telegram_down = asyncio.Event()

    async def stuck_page(_evt) -> None:
        await telegram_down.wait()

    bus.subscribe(TOPIC_RISK_STATE, stuck_page)
    latch = RiskStateLatch(conn, clock, ModeManager(conn, clock, bus))
    on_health = feed_health_to_latch(latch, lambda: True)
    await asyncio.wait_for(_deliver(on_health, FeedHealth(state="STALE", last_frame_age_s=11.0)), 1)
    assert _mode(conn, clock).risk_state() == RiskState.FROZEN
    await asyncio.wait_for(_deliver(on_health, FeedHealth(state="HEALTHY")), 1)
    assert _mode(conn, clock).risk_state() == RiskState.NORMAL
    telegram_down.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_feed_healthy_never_touches_another_cause(conn, clock, latch, monkeypatch):
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner", Actor.OWNER)
    cleared: list[str] = []
    real_clear = latch.clear_cause

    async def spy(cause, who):
        cleared.append(cause)
        return await real_clear(cause, who)

    monkeypatch.setattr(latch, "clear_cause", spy)
    await _deliver(feed_health_to_latch(latch, lambda: True), FeedHealth(state="HEALTHY"))  # every reconnect
    assert cleared == [] and _mode(conn, clock).risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_feed_stale_out_of_session_latches_nothing(conn, clock, latch):
    """Every risk-state change pages the owner; a laptop waking overnight must not page twice."""
    await _deliver(feed_health_to_latch(latch, lambda: False), FeedHealth(state="STALE", last_frame_age_s=9e3))
    assert latch.active_causes() == [] and _mode(conn, clock).risk_state() == RiskState.NORMAL
