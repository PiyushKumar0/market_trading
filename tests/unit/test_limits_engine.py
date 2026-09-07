"""LimitsEngine (§3.2.7): typed, hash-verified access to config/limits.yaml (§7.1)."""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from engine.core.enums import Actor
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.risk.limits import CatalystGuard, LimitsEngine, LimitTable

OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="two-step")

REAL_LIMITS_PATH = Path(__file__).resolve().parents[2] / "config" / "limits.yaml"


@pytest.fixture
def store(tmp_path, conn, clock):
    """A ProtectedStore over a config/ dir seeded with the REAL limits.yaml (copies the
    registration fixture pattern from tests/unit/test_protected_store.py)."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_bytes(REAL_LIMITS_PATH.read_bytes())
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    return ProtectedStore(cfg, conn, clock)


@pytest.fixture
def registered_store(store):
    store.register_initial("limits.yaml", OWNER_OK)
    return store


# --------------------------------------------------------------------------- full parse
def test_full_parse_of_real_limits_yaml(registered_store):
    engine = LimitsEngine(registered_store)
    table = engine.load()

    assert isinstance(table, LimitTable)
    assert table.schema_version == 1
    # O16 2026-09-07: base 40000 / caps 6-2-4
    assert table.capital_base_inr == Decimal("40000")

    # A representative sample across the block types (money, pct, int, bool, time, on_breach str).
    assert table.limits.capital_cap.max_deployed_capital_inr == Decimal("40000")
    assert table.limits.capital_cap.on_breach == "reject_entry"
    assert table.limits.per_trade_risk.intraday_pct == 1.0
    assert table.limits.per_trade_risk.swing_position_pct == 2.0
    assert table.limits.per_trade_risk.overnight_gap_mult == 2.5
    assert table.limits.daily_loss_soft.day_mtm_pct == -5.0
    assert table.limits.daily_loss_hard.day_mtm_pct == -7.0
    assert table.limits.weekly_drawdown.rolling_sessions == 5
    assert table.limits.weekly_drawdown.drawdown_pct == 8.0
    assert table.limits.equity_floor_rung.equity_pct_of_base == -10.0
    assert table.limits.cumulative_floor.equity_pct_of_base == -15.0
    assert table.limits.consecutive_losses.max_per_session == 3
    assert table.limits.max_new_trades_day.count == 5
    # O16 2026-09-07: base 40000 / caps 6-2-4
    assert table.limits.max_open_positions.total == 6
    assert table.limits.max_open_positions.max_mis == 2
    assert table.limits.max_open_positions.max_cnc == 4
    assert table.limits.per_stock_exposure.max_positions_per_symbol == 1
    assert table.limits.per_stock_exposure.cnc_notional_inr == Decimal("8000")
    assert table.limits.per_sector_exposure.max_positions_per_sector == 2
    assert table.limits.per_sector_exposure.unclassified_cap == 1
    assert table.limits.co_movement_cap.corr_max == 0.7
    assert table.limits.max_leverage.platform_cap_x == 3.0
    assert table.limits.max_leverage.phase4_start_x == 1.0

    ntw = table.limits.no_trade_windows
    assert ntw.mis_entry_start == time(9, 30)
    assert ntw.mis_entry_end == time(14, 30)
    assert ntw.cnc_entry_start == time(9, 20)
    assert ntw.cnc_entry_end == time(15, 0)
    assert ntw.no_entry_on_results_day is True
    assert ntw.nifty50_no_new_mis_after_on_expiry == time(14, 0)

    tw = table.limits.trade_window
    assert tw.must_be_within_session is True
    assert tw.mis_sub_window_must_be_nonempty_after_buffer is True

    assert table.limits.stale_data_guard.max_tick_age_s == 5
    assert table.limits.stale_data_guard.feed_heartbeat_silence_s == 10
    assert table.limits.warmup_ready.on_breach == "frozen_entries_until_sufficient"
    assert table.limits.regime_data_ready.on_breach == "frozen_regime_strategies"
    assert table.limits.clock_skew.max_skew_s == 2
    assert table.limits.entry_sanity_band.mis_pct == 1.0
    assert table.limits.entry_sanity_band.cnc_pct == 2.0
    assert table.limits.circuit_proximity.band_proximity_pct == 2.0
    assert table.limits.circuit_proximity.mis_requires_fno is True
    assert table.limits.max_holding.swing_trading_days == 20
    assert table.limits.max_holding.position_trading_days == 120
    assert table.limits.min_residual_window.min_hold_min == 10
    assert table.limits.order_rate.sustained_per_s == 1
    assert table.limits.order_rate.burst == 2
    assert table.limits.order_rate.entry_calls_per_day == 70
    assert table.limits.order_rate.broker_hard_ceiling_per_day == 5000
    assert table.limits.order_modifications.self_cap == 20
    assert table.limits.margin_buffer.min_ratio == 1.1
    assert table.limits.min_viable_size.edge_multiple_min_default == 2.0

    assert table.analyst_confidence_min == 0.55


def test_load_caches(registered_store):
    engine = LimitsEngine(registered_store)
    first = engine.load()
    second = engine.load()
    assert first is second  # cached, not re-parsed


def test_table_accessor_matches_load(registered_store):
    engine = LimitsEngine(registered_store)
    assert engine.table() is engine.load()


# --------------------------------------------------------------------------- analyst_confidence_min (§6.3)
def test_analyst_confidence_min_is_top_level_and_owner_only(registered_store):
    engine = LimitsEngine(registered_store)
    table = engine.load()
    assert table.analyst_confidence_min == 0.55
    # Outside the `limits:` map (§6.3) — LimitsBlock has no such field.
    assert not hasattr(table.limits, "analyst_confidence_min")


# --------------------------------------------------------------------------- catalyst_guard() accessor
def test_catalyst_guard_accessor_matches_yaml(registered_store):
    engine = LimitsEngine(registered_store)
    guard = engine.catalyst_guard()

    assert isinstance(guard, CatalystGuard)
    assert guard.min_source_domains == 2
    assert guard.sentiment_min_long == 0.30
    assert guard.max_catalyst_entries_day == 2
    assert guard.digest_stale_max_h == 20
    assert guard.originating_event_types == [
        "earnings_result",
        "earnings_guidance",
        "order_win",
        "m_and_a",
        "regulatory_policy",
        "govt_program",
        "sector_policy",
        "rating_change",
    ]
    assert guard.on_breach == "reject_catalyst_entry_or_disable_cat"


# --------------------------------------------------------------------------- tamper / integrity (R4)
def test_tampered_file_raises_integrity_error_on_load(registered_store, tmp_path):
    engine = LimitsEngine(registered_store)
    # Modify one byte after registration — the on-disk hash no longer matches the recorded signature.
    path = tmp_path / "config" / "limits.yaml"
    raw = bytearray(path.read_bytes())
    raw[0] = raw[0] ^ 0xFF  # flip one byte
    path.write_bytes(bytes(raw))

    with pytest.raises(IntegrityError):
        engine.load()


def test_reload_reverifies_hash(registered_store, tmp_path):
    engine = LimitsEngine(registered_store)
    engine.load()  # populate the cache while the file is untampered

    path = tmp_path / "config" / "limits.yaml"
    raw = bytearray(path.read_bytes())
    raw[0] = raw[0] ^ 0xFF
    path.write_bytes(bytes(raw))

    with pytest.raises(IntegrityError):
        engine.reload()


def test_unregistered_store_raises_integrity_error(store):
    # `store` fixture never calls register_initial — load_verified must reject it (R4).
    engine = LimitsEngine(store)
    with pytest.raises(IntegrityError):
        engine.load()


# --------------------------------------------------------------------------- unknown key -> loud failure
def test_unknown_key_in_block_fails_validation(store, tmp_path):
    tampered = REAL_LIMITS_PATH.read_text(encoding="utf-8").replace(
        "  capital_cap:\n", "  capital_cap:\n    unknown_field_xyz: 1\n"
    )
    assert "unknown_field_xyz" in tampered  # sanity: the replace actually matched
    path = tmp_path / "config" / "limits.yaml"
    path.write_text(tampered, encoding="utf-8")
    store.register_initial("limits.yaml", OWNER_OK)  # registers the TAMPERED content's hash

    engine = LimitsEngine(store)
    with pytest.raises(ValidationError):
        engine.load()
