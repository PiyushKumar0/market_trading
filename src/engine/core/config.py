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
    # --- Phase-1 active-period / catch-up knobs (§2.6/§10.1/§10.4) ---
    active_period_starts: list[time] = Field(default_factory=lambda: [time(8, 0)])
    """Expected active-period start times (IST). The out-of-band watchdog raises
    SCHEDULED_START_MISSED when a start did not occur within ``start_grace_s`` (§2.6/§10.4)."""
    start_grace_s: int = 900
    """Grace after an expected start before SCHEDULED_START_MISSED fires (§10.4)."""
    catchup_grace_s: int = 900
    """Budget for the §2.6 startup catch-up before FROZEN-for-entries alerts escalate."""
    crashloop_window_s: int = 600
    """Repeated fast respawns inside this window coalesce into ENGINE_CRASHLOOP (§2.2/§10.7)."""


class ClockCfg(BaseModel):
    ntp_servers: list[str] = Field(default_factory=lambda: ["time.windows.com", "pool.ntp.org"])
    max_skew_s: int = 2


class DataCfg(BaseModel):
    minute_candles_adjusted: bool | None = None   # A11 result; None until scripts/a11_check.py runs
    universe_max_watchlist: int = 50
    min_median_traded_value_inr: int = 50_000_000
    backfill_daily_years: int = 2
    backfill_minute_years: int = 1


class NewsFeedsCfg(BaseModel):
    """§3.2.4 ``NewsIngest`` feed set (§2.7 step 1). Seed URLs are [VERIFY Phase-1] — feeds move;
    the G1 gate exercises them live. Headline-level only; bodies are never fetched (A3r)."""

    et_markets_rss: str = "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"
    moneycontrol_rss: str = "https://www.moneycontrol.com/rss/business.xml"
    gdelt_doc_query: str = "sourcecountry:IN (markets OR stocks OR earnings OR NSE)"


class NewsCfg(BaseModel):
    """News-intelligence ingest knobs (§2.7/§4.4 job 10). Owner-only operational config (config_audit)."""

    feeds: NewsFeedsCfg = Field(default_factory=NewsFeedsCfg)
    cluster_sim_threshold: float = 0.75   # §3.2.4 pinned clustering similarity [tunable, owner]
    et_poll_s: int = 300                  # ET Markets RSS poll cadence (§3.2.4: 5 min)
    mc_poll_s: int = 900                  # Moneycontrol RSS poll cadence (15 min, polite)
    gdelt_poll_s: int = 900               # GDELT DOC 2.0 poll cadence (15-min update granularity)
    backfill_lookback_h: int = 72         # off-period startup backfill window over RSS lookbacks (§4.4 job 10)
    gdelt_backfill_max_days: int = 90     # GDELT DOC ~3-month window — no deeper backfill exists (E6)


class CatCfg(BaseModel):
    """§2.7 `cat` strategy knobs surfaced in settings.yaml (owner-only, deliberately NOT learnable).

    Only the settings.yaml-resident keys live here; the ``catalyst_guard`` block is a PROTECTED store
    (limits.yaml via ProtectedStore.load_verified, §2.4 item 1) and is never loaded through Settings.
    """

    fanout_weight: float = 0.5            # sector/theme fan-out multiplier (§2.7 step 5)
    max_event_age_days: int = 2           # §2.7 HeadlineClusterer assignment window / `cat` catalyst
                                          # age horizon (envelope default 2; owner-only, not learnable)


class FilingsCfg(BaseModel):
    """§2.8.4 corporate-filings event thresholds — OWNER-ONLY, deliberately NOT learnable (no new
    envelope-learnable parameters in stages 1–2). STORED CONFIG ONLY in stage 1: these are read by no
    decision path yet — event typing (§2.8.2) and the catalyst wiring are later stages, gated on the
    §2.8.4 event study. Modelled here so the settings.yaml ``filings`` block is honored, not silently
    dropped by pydantic's default extra-ignore (mirrors how ``news``/``cat`` are modelled)."""

    insider_min_value_inr: int = 10_000_000       # abs ₹ floor for an insider_net_buy event (§2.8.2)
    insider_min_value_over_adv: float = 0.05      # AND value/20d-ADV floor (§2.8.2)
    pledge_delta_min_pct: float = 5.0             # QoQ promoter-pledge %-point change to flag (§2.8.2)


class PrescreenCfg(BaseModel):
    """§6.3/§3.2.5 ``SignalPreScreen`` caps (owner-tunable). Deduped candidate origination limits;
    the gate/envelope layer owns per-parameter bounds — these are coarse per-day throttles."""

    max_candidates_per_day: int = 20               # signal.candidate publications per trading day
    max_per_strategy_day: int | None = None        # optional per-strategy sub-cap (None = only the total)


class StrategyCfg(BaseModel):
    """§3.2.5 strategy-layer operational config (pre-screen throttles). Scanner PARAMETERS live in
    the learner-writable ``envelope_state`` (R4), never here."""

    prescreen: PrescreenCfg = Field(default_factory=PrescreenCfg)


class ReconcileCfg(BaseModel):
    """§3.2.3 ReconcileJob drift thresholds (A13): alert if |Δvol|>vol_drift_pct or |Δclose|>close_drift_ticks
    on more than max_bad_bar_fraction of compared bars [tunable]. Offline spans are excluded from the
    denominator (§2.6 — gap-backfilled, not drift)."""

    vol_drift_pct: float = 2.0
    close_drift_ticks: int = 1
    max_bad_bar_fraction: float = 0.01


class BackfillCfg(BaseModel):
    """§3.2.3 BackfillJob pacing + chunking (A2: ≤3 req/s; per-request range caps per Kite interval)."""

    req_per_s: int = 3                    # A2 hard budget shared via broker.rate_limiter
    minute_chunk_days: int = 60           # Kite historical max range per minute-interval request
    day_chunk_days: int = 2000            # Kite historical max range per day-interval request


class UniverseCfg(BaseModel):
    """§3.2.4 UniverseBuilder inputs (A8). The NIFTY200 list is best-effort-fetched (E5) with a
    seed-file fallback so universe build never depends on an NSE page being reachable."""

    nifty200_source_url: str = (
        "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"  # [VERIFY Phase-1] E5 anti-bot caveat
    )
    nifty200_seed_path: str = "config/universe/nifty200_seed.csv"


class JobTimesCfg(BaseModel):
    """§10.1 scheduler fire-times (IST, trading days per calendar, R6). Every job is also §2.6
    catch-up-eligible on startup — these are fire-times, not a liveness assumption."""

    instruments_ist: time = time(8, 15)         # §4.4 job 4 (A10/A8)
    surveillance_ist: time = time(8, 20)        # §4.4 job 5 (A8)
    universe_build_ist: time = time(8, 30)      # §3.2.4 UniverseBuilder
    catalyst_digest_ist: time = time(8, 35)     # §4.4 job 14 (§2.7 step 5)
    preopen_planner_ist: time = time(8, 50)     # §5.3
    reconcile_ist: time = time(15, 50)          # §4.4 job 2 (A13)
    bhavcopy_ist: time = time(18, 0)            # §4.4 job 6
    corp_actions_ist: time = time(18, 15)       # §4.4 job 7 (A12)
    earnings_ist: time = time(18, 30)           # §4.4 job 8 (R2)
    deals_ist: time = time(18, 45)              # §4.4 job 9 (flagged_instrument_days)
    filings_pit_ist: time = time(18, 35)        # §2.8 filings_pit (insider trades, date-keyed)
    filings_results_ist: time = time(18, 45)    # §2.8 filings_results (results + board-meeting dates, date-keyed)
    filings_shp_ist: time = time(18, 50)        # §2.8 filings_shp (SHP + pledge, run-latest)
    nightly_review_ist: time = time(21, 0)      # §5.5/§6.4
    backup_ist: time = time(21, 0)              # §10.5 (watermark-driven)
    sector_map_weekly_day: str = "SUN"          # §4.4 job 13 (sector_map + theme_map refresh)


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
    news: NewsCfg = Field(default_factory=NewsCfg)
    cat: CatCfg = Field(default_factory=CatCfg)
    filings: FilingsCfg = Field(default_factory=FilingsCfg)
    strategy: StrategyCfg = Field(default_factory=StrategyCfg)
    reconcile: ReconcileCfg = Field(default_factory=ReconcileCfg)
    backfill: BackfillCfg = Field(default_factory=BackfillCfg)
    universe: UniverseCfg = Field(default_factory=UniverseCfg)
    jobs: JobTimesCfg = Field(default_factory=JobTimesCfg)
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
