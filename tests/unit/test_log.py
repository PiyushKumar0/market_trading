"""Structured logger must never crash on a field key that collides with a LogRecord attribute."""

from __future__ import annotations

import logging

from engine.core.log import get_logger

# NOTE: these tests deliberately do NOT call configure_logging() — it replaces the root handlers, which
# would evict pytest's caplog handler. The reserved-key remapping under test runs in BoundLogger
# regardless of configuration.


def test_reserved_field_keys_do_not_crash(caplog):
    log = get_logger("test.log")
    with caplog.at_level(logging.INFO):
        # 'name', 'module', 'process', 'msg' all collide with reserved LogRecord attributes — historically
        # this raised "Attempt to overwrite 'name' in LogRecord". The logger must remap, not crash.
        log.info("an_event", name="something", module="x", process=123, ok=True)
    assert any(r.getMessage() =="an_event" for r in caplog.records)


def test_bind_carries_context(caplog):
    log = get_logger("test.log").bind(request_id="abc")
    with caplog.at_level(logging.INFO):
        log.warning("bound_event", extra_field="v")
    rec = next(r for r in caplog.records if r.getMessage() =="bound_event")
    assert getattr(rec, "request_id", None) == "abc"
