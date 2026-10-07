"""PaperControl over paper_state and PaperSettings (plan Q4.0). The Telegram flow is in
test_telegram_commands.py, GET /paper in test_api_routes.py."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from engine.core.config import PaperSettings, Settings, load_settings
from engine.ops.paper_control import PaperControl


def test_first_use_creates_the_disabled_row(conn, clock):
    control = PaperControl(conn, clock)
    assert not control.enabled()
    assert control.state() == {
        "enabled": False, "changed_at": None, "changed_by": None,
        "epoch_started_at": None, "reset_requested_at": None,
    }


def test_set_enabled_and_reset_request_are_recorded(conn, clock):
    control = PaperControl(conn, clock)
    control.set_enabled(True, "owner")
    assert control.enabled() and control.state()["changed_by"] == "owner"
    control.request_reset("owner")
    assert control.state()["reset_requested_at"] == clock.now().isoformat()
    assert control.enabled()


def test_state_persists_across_a_new_control_on_the_same_conn(conn, clock):
    PaperControl(conn, clock).set_enabled(True, "owner")
    again = PaperControl(conn, clock)
    assert again.enabled()
    assert conn.execute("SELECT COUNT(*) FROM paper_state").fetchone()[0] == 1


def test_active_halts_excludes_cleared(conn, clock):
    control = PaperControl(conn, clock)
    conn.execute("INSERT INTO paper_halts (cause, rung, set_at) VALUES ('a', 'FROZEN', '2026-06-17T10:00:00+05:30')")
    conn.execute("INSERT INTO paper_halts (cause, rung, set_at, cleared_at) "
                 "VALUES ('b', 'FROZEN', '2026-06-17T09:00:00+05:30', '2026-06-17T09:30:00+05:30')")
    assert [h["cause"] for h in control.active_halts()] == ["a"]


def test_shipped_settings_load_with_the_subsystem_off():
    assert load_settings().paper == PaperSettings(
        subsystem_enabled=False, seed=20261006, exit_minutes_before_close=10, exit_working_timeout_min=5
    )


def test_a_typo_in_the_paper_block_fails_validation():
    with pytest.raises(ValidationError):
        Settings(paper={"subsystem_enable": True})
