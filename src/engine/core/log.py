"""Structured logging (R8).

Self-contained on the stdlib so ``core`` has no hard third-party logging dependency and is importable
in any environment (incl. the test tier without the heavy deps). Emits one JSON object per line with
a tz-aware IST timestamp, a logger name, a level, an ``event`` string, and arbitrary structured
fields. ``get_logger(...).bind(**ctx)`` returns a child logger that carries context on every record —
the structlog-style API, so swapping in structlog later is mechanical.

Timestamps here come from ``datetime.now(IST)`` directly (NOT ``core.Clock``) on purpose: logging must
never depend on a higher-level singleton and must keep working during early startup / Clock failure.
This is the one sanctioned non-Clock time source, and it is never used for trading decisions.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

_CONFIGURED = False

# Keys that already exist on a LogRecord. A structured field colliding with one of these makes the
# stdlib ``logging`` raise ("Attempt to overwrite %r in LogRecord"), so BoundLogger renames a colliding
# field key to ``key + "_"`` before passing it as ``extra`` — no caller can crash the logger.
_RESERVED = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "asctime", "taskName"}


def _safe_extra(fields: dict[str, Any]) -> dict[str, Any]:
    return {(f"{k}_" if k in _RESERVED else k): v for k, v in fields.items()}


class _JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single JSON line with structured ``extra`` fields."""

    _RESERVED = _RESERVED

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, IST).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = _safe(value)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def _safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


class BoundLogger:
    """A logger that carries bound structured context (structlog-style)."""

    __slots__ = ("_logger", "_ctx")

    def __init__(self, logger: logging.Logger, ctx: dict[str, Any] | None = None) -> None:
        self._logger = logger
        self._ctx = ctx or {}

    def bind(self, **ctx: Any) -> BoundLogger:
        merged = {**self._ctx, **ctx}
        return BoundLogger(self._logger, merged)

    def _log(self, level: int, event: str, **fields: Any) -> None:
        merged = {**self._ctx, **fields}
        self._logger.log(level, event, extra=_safe_extra(merged))

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        merged = {**self._ctx, **fields}
        self._logger.exception(event, extra=_safe_extra(merged))

    def critical(self, event: str, **fields: Any) -> None:
        self._log(logging.CRITICAL, event, **fields)


def configure_logging(
    *,
    level: str = "INFO",
    logs_dir: str | Path | None = None,
    json_lines: bool = True,
) -> None:
    """Configure the root logger once. Idempotent.

    Writes JSON lines to stderr and (if ``logs_dir`` given) to a rotating ``engine.log`` file.
    Logs are retained 90 days by the §4.5 retention policy (file rotation is a runbook/ops concern).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(level.upper())
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter: logging.Formatter = _JsonFormatter() if json_lines else logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(formatter)
    root.addHandler(stream)

    if logs_dir is not None:
        from logging.handlers import TimedRotatingFileHandler

        log_path = Path(logs_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            log_path / "engine.log", when="midnight", backupCount=90, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Third-party libraries log at INFO by default; httpx in particular logs every request URL —
    # which for the Telegram Bot API embeds the bot token — on every ~10s getUpdates poll (R8).
    for noisy in ("httpx", "httpcore", "telegram", "apscheduler"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str, **ctx: Any) -> BoundLogger:
    """Return a :class:`BoundLogger` for ``name`` with optional bound context."""
    return BoundLogger(logging.getLogger(name), ctx)
