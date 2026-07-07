"""Config loading (§3.2.1).

Loads the NON-SECRET ``config/settings.yaml`` into a typed Pydantic model and resolves runtime paths.
Secrets are NEVER here (Credential Manager / DPAPI, R10). The PROTECTED stores (``limits.yaml``,
``envelope.yaml``) are NOT loaded here either — they go through ``ProtectedStore.load_verified`` so the
gate only ever reads a hash-verified copy (R4). ``agents.yaml`` / ``costs.yaml`` are operational config
and loaded as plain dicts by their owners.

Path resolution order: explicit arg > ``MT_CONFIG_DIR`` / ``MT_DATA_DIR`` env > repo defaults.
"""

from __future__ import annotations

import os
from datetime import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


# --------------------------------------------------------------------------- paths
def repo_root() -> Path:
    """Locate the repository root by walking up to the dir containing ``pyproject.toml``."""
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: src/engine/core/config.py -> repo root is three levels up from src/
    return here.parents[3]


def config_dir() -> Path:
    env = os.environ.get("MT_CONFIG_DIR")
    return Path(env) if env else repo_root() / "config"


def _data_dir_override() -> Path | None:
    env = os.environ.get("MT_DATA_DIR")
    return Path(env) if env else None


# --------------------------------------------------------------------------- models
class Paths(BaseModel):
    data_dir: str = "data"
    duckdb: str = "data/market.duckdb"
    parquet_dir: str = "data/parquet"
    sqlite: str = "data/state.db"
    logs_dir: str = "data/logs"
    backups_dir: str = "data/backups"


class ApiCfg(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8400
    kite_login_redirect_path: str = "/kite/callback"


class TickerCfg(BaseModel):
    tcp_host: str = "127.0.0.1"
    tcp_port: int = 8401
    heartbeat_silence_kill_s: int = 10
    max_instruments_per_conn: int = 3000


class BrokerCfg(BaseModel):
    kite_login_redirect_path: str = "/kite/callback"
    token_expiry_ist: str = "06:00"
    instruments_refresh_ist: str = "08:15"


class TradeWindowCfg(BaseModel):
    start_ist: time = time(10, 0)
    end_ist: time = time(10, 30)
    squareoff_buffer_min: int = 5


class TelegramCfg(BaseModel):
    owner_chat_id: int = 0          # NOT a secret; 0 = unconfigured (engine starts without Telegram)


class LifecycleCfg(BaseModel):
    """Process-lifecycle notifications + liveness watchdog knobs (§2.2/§3.2.12).

    The heartbeat writer + standalone watchdog + ENGINE_DOWN land in Phase 1; ``notify_started`` /
    ``notify_planned_stop`` gate the Phase-0 ENGINE_STARTED / ENGINE_STOPPED sends (owner chose "notify
    both", 2026-07-07). Modelled here so the settings.yaml ``lifecycle`` block is honored, not silently
    dropped by pydantic's default extra-ignore.
    """

    heartbeat_write_s: int = 20
    down_stale_s: int = 90
    watchdog_poll_s: int = 60
    notify_planned_stop: bool = True
    notify_started: bool = True


class ClockCfg(BaseModel):
    ntp_servers: list[str] = Field(default_factory=lambda: ["time.windows.com", "pool.ntp.org"])
    max_skew_s: int = 2


class DataCfg(BaseModel):
    minute_candles_adjusted: bool | None = None   # A11 result; None until scripts/a11_check.py runs
    universe_max_watchlist: int = 50
    min_median_traded_value_inr: int = 50_000_000
    backfill_daily_years: int = 2
    backfill_minute_years: int = 1


class TotpAutomationCfg(BaseModel):
    enabled: bool = False                          # OWNER-ACCEPTED RISK (A5/§7.3); default OFF


class AuthCfg(BaseModel):
    totp_automation: TotpAutomationCfg = Field(default_factory=TotpAutomationCfg)


class FeatureFlags(BaseModel):
    paper_only: bool = True
    onedrive_backup_mirror: bool = False


class Settings(BaseModel):
    schema_version: int = 1
    env: str = "dev"
    timezone: str = "Asia/Kolkata"
    paths: Paths = Field(default_factory=Paths)
    api: ApiCfg = Field(default_factory=ApiCfg)
    ticker: TickerCfg = Field(default_factory=TickerCfg)
    broker: BrokerCfg = Field(default_factory=BrokerCfg)
    trade_window: TradeWindowCfg = Field(default_factory=TradeWindowCfg)
    telegram: TelegramCfg = Field(default_factory=TelegramCfg)
    lifecycle: LifecycleCfg = Field(default_factory=LifecycleCfg)
    clock: ClockCfg = Field(default_factory=ClockCfg)
    data: DataCfg = Field(default_factory=DataCfg)
    auth: AuthCfg = Field(default_factory=AuthCfg)
    feature_flags: FeatureFlags = Field(default_factory=FeatureFlags)

    # ---- resolved absolute paths (not from YAML; computed on load) ----
    _resolved_data_dir: Path = Path("data")

    def resolved_data_dir(self) -> Path:
        return self._resolved_data_dir

    def sqlite_path(self) -> Path:
        return self._abs(self.paths.sqlite)

    def duckdb_path(self) -> Path:
        return self._abs(self.paths.duckdb)

    def parquet_dir(self) -> Path:
        return self._abs(self.paths.parquet_dir)

    def logs_dir(self) -> Path:
        return self._abs(self.paths.logs_dir)

    def backups_dir(self) -> Path:
        return self._abs(self.paths.backups_dir)

    def _abs(self, rel: str) -> Path:
        p = Path(rel)
        if p.is_absolute():
            return p
        override = _data_dir_override()
        if override is not None and rel.startswith("data/"):
            return override / rel[len("data/"):]
        return repo_root() / rel


# --------------------------------------------------------------------------- loaders
def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file into a dict (empty dict if the file is empty)."""
    text = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    return data or {}


def load_settings(cfg_dir: str | Path | None = None) -> Settings:
    """Load ``settings.yaml`` into a :class:`Settings`, resolving the data dir."""
    base = Path(cfg_dir) if cfg_dir else config_dir()
    raw = load_yaml(base / "settings.yaml")
    if "env" not in raw and os.environ.get("MT_ENV"):
        raw["env"] = os.environ["MT_ENV"]
    settings = Settings(**raw)
    override = _data_dir_override()
    object.__setattr__(
        settings,
        "_resolved_data_dir",
        override if override is not None else repo_root() / settings.paths.data_dir,
    )
    return settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide cached settings. Tests that mutate env should call ``load_settings`` directly."""
    return load_settings()
