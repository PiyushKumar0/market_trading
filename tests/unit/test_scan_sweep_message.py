"""SCAN_SWEEP owner-message rendering (§3.2.5 sweep addendum; 2026-07-29 owner feedback).

Pins the four readability requirements: trade types grouped intraday/swing, every setup carries its
full plan (entry + stop + target or exit rule), friendly strategy names instead of scanner ids, and
held positions flagged — plus the arms_when direction coming from the SCANNER, never inferred
(the price-comparison inference mislabeled a BUY breakout "below" when trigger == last price).
"""

from __future__ import annotations

from engine.notify.catalog import MessageKind, scan_sweep

LIVE = [
    {"symbol": "ADANIGREEN", "side": "SELL", "strategy_id": "orb", "style": "intraday",
     "entry": "1371.90", "stop": "1396.60", "target": "1334.85", "held": False},
]
PENDING = [
    # The DLF case that was mislabeled: BUY breakout with trigger == last MUST say "breaks above".
    {"symbol": "DLF", "side": "BUY", "strategy_id": "orb", "style": "intraday",
     "trigger": "669.90", "arms_when": "above", "last": "669.90",
     "stop": "660.00", "target": "684.75", "exit_rule": "", "held": False},
    {"symbol": "ADANIGREEN", "side": "BUY", "strategy_id": "rsi2", "style": "swing",
     "trigger": "1369.45", "arms_when": "below", "last": "1371.90",
     "stop": "1314.65", "target": None,
     "exit_rule": "exit on RSI(2) > 65 or after 10 sessions", "held": True},
]


def test_sweep_message_grouped_planful_and_direction_correct():
    msg = scan_sweep(trigger="window_open", live=LIVE, pending=PENDING, suppressed_today=7)
    assert msg.kind == MessageKind.SCAN_SWEEP
    body = msg.body

    # (1) trade types separated, intraday before swing
    assert "INTRADAY (square off the same day):" in body
    assert "SWING (hold for days):" in body
    assert body.index("INTRADAY") < body.index("SWING (hold for days)")
    # (3) friendly names, not scanner ids, in the trade lines
    assert "(intraday breakout)" in body
    assert "orb" not in body.replace("intraday breakout", "")
    # (2) full plan on every setup
    assert "entry ₹1371.90 · stop ₹1396.60 · target ₹1334.85" in body
    assert "entry ₹669.90 · stop ₹660.00 · target ₹684.75" in body
    assert "stop ₹1314.65 · exit on RSI(2) > 65 or after 10 sessions" in body
    # direction from the scanner: BUY breakout at trigger == last still says "breaks above"
    assert "DLF BUY if price breaks above ₹669.90 (now ₹669.90)" in body
    assert "ADANIGREEN BUY if price dips below ₹1369.45" in body
    # (2b) held stock clearly marked
    assert "you HOLD this" in body
    # suppressed-count trailer
    assert "7 setup(s) already evaluated today" in body


def test_sweep_message_nothing_to_trade_is_explicit():
    msg = scan_sweep(trigger="scan_now", live=[], pending=[], suppressed_today=0)
    assert "Nothing to trade right now" in msg.body
    assert msg.title == "Scan sweep: nothing to trade right now"
