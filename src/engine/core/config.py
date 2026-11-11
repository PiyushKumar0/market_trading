"""Config loading.

Loads the NON-SECRET ``config/settings.yaml`` into a typed Pydantic model and resolves runtime paths.
Secrets are NEVER here (Credential Manager / DPAPI). The PROTECTED stores (``limits.yaml``,
``envelope.yaml``) are NOT loaded here either — they go through ``ProtectedStore.load_verified`` so the
gate only ever reads a hash-verified copy. ``agents.yaml`` / ``costs.yaml`` are operational config
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
from pydantic import BaseModel, ConfigDict, Field


# --------------------------------------------------------------------------- paths
@lru_cache(maxsize=1)
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
    # The child heartbeats every 1 s regardless of ticks, so in market hours tick silence beyond this
    # (heartbeats fine) ⇒ DEGRADED + owner alert. Off-hours keeps heartbeat-only semantics.
    tick_silence_degrade_s: int = 120
    # Kite can hold a WebSocket open and deliver no ticks; only a fresh connection ends it. In session,
    # DEGRADED silence past this (from the later of the last tick and the last HEALTHY entry) ⇒ kill + respawn.
    tick_silence_respawn_s: int = 300
    # Respawns per silence episode (a tick or a new session date starts a new one); once spent, the feed
    # stays DEGRADED and the owner is escalated to once.
    max_tick_silence_respawns: int = 3
    # Cadence of the periodic in-session ``feed_stats`` INFO line.
    feed_stats_interval_s: int = 300
    # The heartbeat-silence kill skips WARMING (a fresh spawn has no heartbeat yet), so WARMING needs its
    # own timeout: no heartbeat this long ⇒ kill + respawn (also recovers a system resume that froze it).
    warming_timeout_s: int = 60
    # Consecutive WARMING-timeout respawns back off exponentially (warming_timeout_s × 2ⁿ) capped here.
    warming_backoff_cap_s: int = 300
    # After this many consecutive WARMING-timeout respawns without HEALTHY, keep retrying at the capped
    # backoff but escalate once to the owner.
    max_wedge_respawns: int = 5
    # Windows: assert SetThreadExecutionState while the NSE session is open so the OS does not sleep and
    # freeze the feed. Non-Windows: no-op.
    keep_awake_in_session: bool = True


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
    """Process-lifecycle notifications + liveness watchdog knobs.

    The heartbeat writer (``ops/heartbeat.py``) and the standalone watchdog (``scripts/watchdog.py``,
    which raises ENGINE_DOWN) read these knobs; ``notify_started`` / ``notify_planned_stop`` gate the
    ENGINE_STARTED / ENGINE_STOPPED sends.
    """

    heartbeat_write_s: int = 20
    down_stale_s: int = 90
    watchdog_poll_s: int = 60
    notify_planned_stop: bool = True
    notify_started: bool = True
    # --- active-period / catch-up knobs ---
    active_period_starts: list[time] = Field(default_factory=lambda: [time(8, 0)])
    """Expected active-period start times (IST). The out-of-band watchdog raises
    SCHEDULED_START_MISSED when a start did not occur within ``start_grace_s``."""
    start_grace_s: int = 900
    """Grace after an expected start before SCHEDULED_START_MISSED fires."""
    catchup_grace_s: int = 900
    """Budget for the startup catch-up before FROZEN-for-entries alerts escalate."""
    crashloop_window_s: int = 600
    """Repeated fast respawns inside this window coalesce into ENGINE_CRASHLOOP."""


class ClockCfg(BaseModel):
    ntp_servers: list[str] = Field(default_factory=lambda: ["time.windows.com", "pool.ntp.org"])
    max_skew_s: int = 2
    calendar_horizon_alert_sessions: int = 40


class RecommendSettings(BaseModel, extra="forbid"):
    gtt_limit_offset_pct: float = 1.0
    veto_window_sessions: int = 3
    protection_first_reminder_min: int = 30
    reminder_window_start: time = time(8, 0)
    reminder_window_end: time = time(22, 0)


class PaperSettings(BaseModel, extra="forbid"):
    subsystem_enabled: bool = False
    seed: int = 20261006
    exit_minutes_before_close: int = 10
    exit_working_timeout_min: int = 5
    #: paper positions of these strategies open with no stop and no target and exit on time.
    no_stop_strategies: tuple[str, ...] = ()


class DataCfg(BaseModel):
    minute_candles_adjusted: bool | None = None   # minute-candles-adjusted check result; None until scripts/kite_candle_adjustment_check.py runs
    universe_max_watchlist: int = 50
    min_median_traded_value_inr: int = 50_000_000
    backfill_daily_years: int = 2
    backfill_minute_years: int = 1
    # Batch-universe extended leg: criteria-passing NON-index symbols (MIS ∩ EQ-master ∩ not-surveillance
    # ∩ ₹5cr median) persisted included=False / exclusion_reasons=['not_in_index'] for BATCH rules, news
    # shadow and pre-open advisory only — the tick watchlist and the risk gate stay scoped to
    # ``universe.index_name``. False = no extended leg.
    batch_universe_enabled: bool = False
    batch_universe_max: int = 600                 # top-N extended by median traded value (sanity cap)


class RssFeedCfg(BaseModel):
    """One RSS source: where to fetch it and how often. Cadence is per-feed (publishers
    differ in update rate and in how much politeness they want), so the scheduler arms one job
    per entry of :attr:`NewsFeedsCfg.rss` rather than a fixed set of named jobs."""

    url: str
    poll_s: int = 900


class NseAnnouncementsCfg(BaseModel):
    """Exchange-announcements feed: NSE ``corporate-announcements``.

    ``drop_subjects`` is owner config like ``NewsCfg.drop_title_patterns``: a case-insensitive SUBSTRING
    match ("Newspaper Publication" catches "Copy of Newspaper Publication") on routine-compliance
    subjects. Deliberately conservative — a merely LOW-value subject stays in, because a wrong drop is
    silent and permanent. ``enabled: false`` = no fetch, no scheduler job.
    """

    enabled: bool = True
    poll_s: int = 300
    drop_subjects: list[str] = Field(default_factory=lambda: [
        "Trading Window",
        "Loss of Share Certificate",
        "Compliances-Certificate",
        "Newspaper Publication",
        "Analysts/Institutional Investor Meet",
        "Change in Registrar",
        "Book Closure",
    ])


class NewsFeedsCfg(BaseModel):
    """``NewsIngest`` feed set. Headline-level only; bodies are never fetched.

    ``rss`` is an open name → feed map: adding/removing a source is a settings.yaml edit (config_audit),
    not a code change. The catalyst layer's ``min_source_domains`` gate counts registrable domains, so
    keep several distinct domains."""

    rss: dict[str, RssFeedCfg] = Field(default_factory=lambda: {
        "et": RssFeedCfg(
            url="https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", poll_s=300),
        "livemint_markets": RssFeedCfg(url="https://www.livemint.com/rss/markets", poll_s=900),
        "livemint_companies": RssFeedCfg(url="https://www.livemint.com/rss/companies", poll_s=900),
        # Both livemint feeds are ONE registrable domain; the extra domains keep "any 2 of N" corroboration
        # clearable for genuine stories.
        "hbl_markets": RssFeedCfg(
            url="https://www.thehindubusinessline.com/markets/feeder/default.rss", poll_s=900),
        "hbl_companies": RssFeedCfg(
            url="https://www.thehindubusinessline.com/companies/feeder/default.rss", poll_s=900),
        "cnbctv18_market": RssFeedCfg(
            url="https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml", poll_s=900),
        "ndtvprofit": RssFeedCfg(url="https://feeds.feedburner.com/ndtvprofit-latest", poll_s=900),
        "bs_markets": RssFeedCfg(
            url="https://www.business-standard.com/rss/markets-106.rss", poll_s=900),
        "bs_companies": RssFeedCfg(
            url="https://www.business-standard.com/rss/companies-101.rss", poll_s=900),
    })
    gdelt_doc_query: str = "sourcecountry:IN (markets OR stocks OR earnings OR NSE)"
    #: The exchange as a feed: filings carry ``symbol`` natively (no alias seed needed); NSE counts as
    #: one corroborating domain.
    nse_announcements: NseAnnouncementsCfg = Field(default_factory=NseAnnouncementsCfg)


class NewsCfg(BaseModel):
    """News-intelligence ingest knobs. Owner-only operational config (config_audit)."""

    feeds: NewsFeedsCfg = Field(default_factory=NewsFeedsCfg)
    cluster_sim_threshold: float = 0.75   # pinned clustering similarity [tunable, owner]
    #: Case-insensitive substrings marking a feed item as an auto-generated live-blog/ticker/roundup
    #: PAGE title, not a news headline — dropped at ingest. Such titles glue different companies into one
    #: cluster and burn scorer budget; no per-company catalyst signal is lost. Owner-editable (config_audit).
    drop_title_patterns: list[str] = Field(default_factory=lambda: [
        "share price live updates",
        "stock market live updates",
        "results live updates",
        "share price highlights",
        # Quote-page titles ("X Share Price , X Stock Price , ..."): the comma-spaced template never
        # appears in written prose.
        "stock price ,",
        # Multi-company roundups: breadth commentary that BRIDGES different companies' clusters.
        "stocks in news",
        "market wrap",
        "buzzing stocks",
        "top gainers",
        "gainers and losers",
        "trade spotlight",
        "stocks to buy in 2026",
        " live :",
        # F&O-desk multi-name series. Matches post-unescape titles (ingest html.unescapes).
        "f&o talk",
        # "X among 4 stocks closing above/below VWAP" screener series.
        "vwap",
        # HBL liveblog ("Stock Market Live:") and CNBC-TV18's "11:11" multi-topic digest.
        "stock market live",
        "11:11",
        # Multi-company "Q1 Results Highlights: ONGC, BSE, ..." roundups; also drops single-company
        # liveblog wrappers (the plain result article on the same domain carries the event).
        "results highlights",
    ])
    # RSS cadences are per-feed (``feeds.rss[name].poll_s``); only GDELT keeps a top-level knob.
    gdelt_poll_s: int = 3600              # GDELT DOC 2.0 poll cadence; it 429s at faster polls and only the
                                          # pre-open digest needs it
    request_timeout_s: float = 30.0       # per-fetch HTTP timeout (GDELT times out at 10 s)
    backfill_lookback_h: int = 72         # off-period startup backfill window over RSS lookbacks
    gdelt_backfill_max_days: int = 90     # GDELT DOC ~3-month window — no deeper backfill exists


class CatCfg(BaseModel):
    """`cat` strategy knobs surfaced in settings.yaml (owner-only, deliberately NOT learnable).

    Only the settings.yaml-resident keys live here; the ``catalyst_guard`` block is a PROTECTED store
    (limits.yaml via ProtectedStore.load_verified) and is never loaded through Settings.
    """

    fanout_weight: float = 0.5            # sector/theme fan-out multiplier
    # No max_event_age_days here: the clusterer window and the digest horizon are both the envelope
    # row cat.max_event_age_days (load_cat_params); a leftover settings key is ignored, not an error.
    # v2 SHADOW scanner knobs, mirroring `ins`. Frozen for the whole shadow window — moving one
    # re-opens multiplicity and restarts the clock.
    stop_pct: float = 5.0                 # disaster stop: sizing needs a risk distance and the rule's real
                                          # exit is TIME; 5 not 6 because the risk-limit overnight_gap_mult
                                          # arithmetic applies
    hold_sessions: int = 20               # = the risk-limit swing max_holding cap, the exit path
    # Deliberately NO expected_edge_pct: `cat` has no validated edge (measuring it IS the shadow), so
    # the cost-gate check fail-closed-rejects every cat candidate. Adding the key is the owner promotion gate.


class CatReversalCfg(BaseModel):
    """``cat_reversal`` SHADOW knobs — OWNER-ONLY, deliberately NOT learnable.

    A SEPARATE block from :class:`CatCfg` on purpose: the two experiments have independently
    pre-registered thresholds and clocks, and a shared ``stop_pct`` would let tuning one silently move
    the other and invalidate an open shadow window. Frozen for the whole shadow window.
    """

    stop_pct: float = 5.0                 # disaster stop: sizing needs a risk distance and the rule's real
                                          # exit is TIME; the overnight_gap_mult arithmetic applies
    hold_sessions: int = 10               # intended exit if the promotion gate ever opens (T+10).
                                          # INERT during the shadow: the cost gate rejects every candidate.
    # Deliberately NO expected_edge_pct — and, unlike `cat`, the absence is not what enforces it: the
    # strategy is listed in the gate's `no_edge_shadow_strategies`, so the cost gate rejects it whatever target
    # the analyst supplies. Adding an edge here is the owner promotion gate, post-verdict.


class FilingsCfg(BaseModel):
    """Corporate-filings event thresholds — OWNER-ONLY, deliberately NOT learnable. Modelled here so the
    settings.yaml ``filings`` block is honored, not silently dropped by pydantic's default extra-ignore."""

    insider_min_value_inr: int = 10_000_000       # abs ₹ floor for an insider_net_buy event
    insider_min_value_over_adv: float = 0.05      # NOT applied anywhere (planned AND floor)
    pledge_delta_min_pct: float = 5.0             # QoQ promoter-pledge %-point change to flag


class InsCfg(BaseModel):
    """``ins`` (insider net-BUY swing) knobs — OWNER-ONLY, deliberately NOT learnable.

    Provenance: the filings event study's insider_net_buy leg survived corrected mechanics
    (T+10 +0.7297% / T+20 +1.5797% net, CPCV +0.0359%/day) — the reason this strategy exists.

    ``stop_pct`` [4-8] and ``hold_sessions`` [10-20] are FUTURE parameter-envelope rows; they stay static
    owner settings here for now (no protected-store churn until the learning phase needs them).
    ``threshold_inr`` is owner-FIXED and never learnable at all — it defines the validated event
    population, so moving it re-opens multiplicity.
    """

    stop_pct: float = 5.0                 # disaster stop: sizing needs a risk distance and the validated
                                          # design is stopless — wide enough to rarely interrupt the drift
    hold_sessions: int = 20               # = the risk-limit swing max_holding cap = the validated T+20
                                          # horizon; the max-holding machinery is the exit
    threshold_inr: int = 10_000_000       # ₹1cr trailing-10-session open-market net-BUY floor
    expected_edge_pct: float = 1.58       # the validated T+20 NET drift, fed to the cost gate
                                          # (target is None for `ins`, so the edge cannot be derived
                                          # from levels — see risk/gate.py `_rule_min_viable_size`)


class Hi52Cfg(BaseModel):
    """``hi52`` (52wk-high-proximity swing, v2) knobs — OWNER-ONLY, never learnable.

    Only the edge lives here. The rule's own parameters — including v2's three signal-time filter
    thresholds — stay in ``hi52.DEFAULT_PARAMS`` where the pre-registration froze them; splitting
    them across two files would give the same threshold two homes and one of them would drift.

    Demoted to shadow by its written kill rule (the v2 backtest's edge sat in a survivorship-tainted
    index population and did not survive forward): the key is absent from ``settings.yaml`` and the id is
    in ``ops.main.NO_EDGE_SHADOW_STRATEGIES``. Promoting it again is a new registration.

    The default is ``None`` on purpose: any numeric default would make deleting the key a NO-OP, the
    gate consuming an edge no reader of ``settings.yaml`` can see. The composition root
    (``ops.main._strategy_expected_edge_pct``) registers ``hi52`` only when the key is present, and a
    targetless proposal with no registered edge is the gate's ordinary ``_NO_TARGET`` reject (fail-closed).
    """

    #: ``None`` = NOT registered, which is the shipped state. A value, if one is ever registered, lives in
    #: ``settings.yaml``; the learner never moves it.
    #:
    #: DERIVATION (one formula, one home: ``engine/strategy/hi52_edge.py``, which
    #: ``scripts/hi52_forward_verdict.py`` delegates to)::
    #:
    #:     edge = measured T+20 median GROSS − CostModel.breakeven_pct(N, "CNC")
    #:     N    = equity × per_trade_risk.swing_position_pct/100
    #:            ÷ (per_trade_risk.overnight_gap_mult × hi52 stop_pct/100)
    #:
    #: ``N`` is the risk-limit cap on a swing position's notional, never a hand-picked number. The cost
    #: floor RISES as the book shrinks, so the edge is derived at the worst permitted book
    #: (``equity_floor_rung``, ₹6,000): 2.0336 − 0.4982 = 1.5354 ⇒ **1.53**, rounded DOWN.
    expected_edge_pct: float | None = None


class Brk20Cfg(BaseModel):
    """``brk20`` operational knobs — OWNER-ONLY, never learnable.

    The RULE's parameters (``lookback_days``, ``vol_mult``, ``rr_target``, ``ex_skip_days``) live in
    ``brk20.DEFAULT_PARAMS`` (frozen code defaults: ``envelope.yaml`` has no ``brk20`` rows today;
    the strategy rules list them as future parameter-envelope rows) and the stop-geometry floor lives in
    ``brk20.FLOOR_PARAMS`` (owner-only by construction). This block holds the ENTRY MECHANISM (how the
    level is OFFERED, not how it is derived) and the regime gate on origination. Modelled here so the
    ``brk20:`` block in settings.yaml is not silently dropped.
    """

    #: Retest window in TRADING sessions counting ``signal_d``, the session after the crossing: the
    #: level is offered on y+1..y+N (``retest.RestingLevelBook``).
    #: 5 = the pre-registered backtest's ``limit_5_sessions``. NEVER learnable: N=1 and N=3 were measured
    #: and lost and no other N was run, so a learner moving it would select an unmeasured mechanism.
    #: 0 or less is clamped to 1 by the book (disabling the mechanism is a code change).
    retest_sessions: int = 5
    #: Regime gate on brk20 ORIGINATION (``ops.main._brk20_regime_gate``). ``False`` (plus a restart):
    #: crossings originate in any regime and none is journalled as blocked.
    regime_filter: bool = False


class PrescreenCfg(BaseModel):
    """``SignalPreScreen`` caps (owner-tunable). Deduped candidate origination limits;
    the gate/envelope layer owns per-parameter bounds — these are coarse per-day throttles."""

    max_candidates_per_day: int = 48               # signal.candidate publications per trading day; kept
                                                   # = the analyst forward cap `prescreen_cap_per_day`
                                                   # (settings.yaml ships the live value)
    #: Per-strategy publication sub-cap. int = the same cap for every strategy; MAPPING
    #: {strategy_id: cap} = per-strategy values with the reserved key `default` binding any strategy
    #: without a line of its own. Owner knob, never learner-movable.
    max_per_strategy_day: int | dict[str, int] | None = None
    #: Publication/forward admission ORDER: "ranked" = by score (prescreen) and per-strategy score
    #: quantile (analyst forwarding); "arrival" = first come first served.
    admission_mode: str = "ranked"
    #: Cumulative per-strategy sub-cap tranches by IST time-of-day: {strategy: {"HH:MM": cumulative_cap}}.
    #: Effective cap = min(max_per_strategy_day, released tranche); absent = flat caps.
    #: Owner knob, never learner-movable — it decides which candidates get evaluated at all.
    cap_release_schedule: dict[str, dict[str, int]] | None = None
    #: WHEN the analyst forward queue is drained: "paced" = one slot per ``FORWARD_PACING_MIN`` minutes
    #: on a scheduler pulse, so candidates accumulate for the ranking to rank; "immediate" = inline drain.
    forward_drain_mode: str = "paced"
    #: How much better a later candidate must score to take a full cap's admission slot from an
    #: unevaluated incumbent of the same strategy. Owner knob, never learner-movable. ``null`` DISABLES
    #: displacement ("whoever fires first keeps the slot all day"). settings.yaml ships the live value.
    displacement_margin: float | None = 0.10


class StrategyCfg(BaseModel):
    """Strategy-layer operational config (pre-screen throttles). Scanner PARAMETERS live in
    the learner-writable ``envelope_state``, never here."""

    prescreen: PrescreenCfg = Field(default_factory=PrescreenCfg)


class ReconcileCfg(BaseModel):
    """ReconcileJob drift thresholds: alert if |Δvol|>vol_drift_pct or |Δclose|>close_drift_ticks
    on more than max_bad_bar_fraction of compared bars [tunable]. Offline spans are excluded from the
    denominator (gap-backfilled, not drift)."""

    vol_drift_pct: float = 2.0
    close_drift_ticks: int = 1
    max_bad_bar_fraction: float = 0.01


class BackfillCfg(BaseModel):
    """BackfillJob pacing + chunking (≤3 req/s; per-request range caps per Kite interval)."""

    req_per_s: int = 3                    # dry-run ESTIMATE only; real pacing is rate_limiter's constant
    minute_chunk_days: int = 60           # Kite historical max range per minute-interval request
    day_chunk_days: int = 2000            # Kite historical max range per day-interval request


class UniverseCfg(BaseModel):
    """UniverseBuilder inputs — the index that seeds the eligible universe. The list is
    best-effort-fetched with a seed-file fallback so universe build never depends on an NSE
    page being reachable.

    The index is CONFIG, not a constant — swapping it is these keys plus a seed file. ``extra="forbid"``
    (narrower than the rest of this module) makes a settings.yaml left on retired keys fail loudly at
    boot rather than be silently ignored while the defaults present a different universe as live.

    The index is NIFTY Total Market (≈750 = NIFTY 500 + Microcap 250); a second list — the CORE index,
    NIFTY 500 — marks the evidence population. Legs whose registered edge or frozen shadow verdict was
    measured on NIFTY 500 (``hi52``, ``ins``, ``cat``, ``cat_reversal``) originate from — and the
    rec-outcome benchmark averages over — the core subset of the eligible set only
    (``MarketStore.get_core_eligible_symbols``); ``brk20``, the risk gate and the news/batch layer read
    the full eligible set. Pointing ``core_index_source_url`` at ``index_source_url`` makes every
    member core.
    """

    model_config = ConfigDict(extra="forbid")

    index_name: str = "NIFTY Total Market"
    """Display name of the eligible-universe index — carried on the ``universe_built`` log line and
    in the fallback alert, so ops can see WHICH index a degraded build fell back to."""
    index_source_url: str = (
        "https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"  # [VERIFIED 2026-10-08]
    )
    index_seed_path: str = "config/universe/niftytotalmarket_seed.csv"
    core_index_name: str = "NIFTY 500"
    """The evidence-population index (see the class docstring). Must be a SUBSET of the index
    above; a core name that is not an index member is simply not eligible."""
    core_index_source_url: str = (
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"  # [VERIFIED 2026-09-04]
    )
    core_index_seed_path: str = "config/universe/nifty500_seed.csv"


class JobTimesCfg(BaseModel):
    """Scheduler fire-times (IST, trading days per calendar). Every job is also
    catch-up-eligible on startup — these are fire-times, not a liveness assumption."""

    instruments_ist: time = time(8, 15)
    surveillance_ist: time = time(8, 20)
    universe_build_ist: time = time(8, 30)      # UniverseBuilder
    catalyst_digest_ist: time = time(8, 35)
    preopen_planner_ist: time = time(8, 50)
    time_exit_check_ist: time = time(9, 30)     # time exit
    reconcile_ist: time = time(15, 50)
    bhavcopy_ist: time = time(18, 0)
    corp_actions_ist: time = time(20, 15)       # after NSE's evening maintenance
    earnings_ist: time = time(18, 30)
    rec_outcomes_ist: time = time(18, 30)       # after daily_bars (18:05), whose run it waits for
    signal_outcomes_ist: time = time(18, 40)    # after rec_outcomes, whose EW returns it reads
    weekly_summary_ist: time = time(19, 0)      # after rec_outcomes (18:30)
    deals_ist: time = time(20, 30)              # flagged_instrument_days
    filings_pit_ist: time = time(18, 35)        # filings_pit (insider trades, date-keyed)
    filings_pit_fresh_ist: time = time(19, 0)   # filings_pit_fresh (BSE fresh insider, date-keyed)
    ins_crossings_ist: time = time(19, 15)      # ins_crossings — MUST stay after filings_pit_fresh
                                                # (19:00): it consumes that job's same-day BSE rows
    filings_results_ist: time = time(18, 45)    # filings_results (results + board-meeting dates, date-keyed)
    isin_map_ist: time = time(18, 40)           # isin_map — before filings_shp, which fetches mapped symbols only
    filings_shp_ist: time = time(18, 50)        # filings_shp (SHP + pledge, run-latest)
    results_line_items_ist: time = time(19, 30)  # results_line_items — after filings_results lists new filings
    feed_freshness_ist: time = time(21, 30)     # feed census — after the EOD feeds (deals 20:30, features 20:45)
    nightly_review_ist: time = time(21, 0)
    backup_ist: time = time(21, 0)              # (watermark-driven)
    sector_map_weekly_day: str = "SUN"          # weekly sector-map job; NOT read — the Sunday cadence is hard-coded in ops.main


class TotpAutomationCfg(BaseModel):
    enabled: bool = False                          # OWNER-ACCEPTED RISK; default OFF


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
    recommend: RecommendSettings = Field(default_factory=RecommendSettings)
    paper: PaperSettings = Field(default_factory=PaperSettings)
    data: DataCfg = Field(default_factory=DataCfg)
    news: NewsCfg = Field(default_factory=NewsCfg)
    cat: CatCfg = Field(default_factory=CatCfg)
    cat_reversal: CatReversalCfg = Field(default_factory=CatReversalCfg)
    filings: FilingsCfg = Field(default_factory=FilingsCfg)
    ins: InsCfg = Field(default_factory=InsCfg)
    hi52: Hi52Cfg = Field(default_factory=Hi52Cfg)
    brk20: Brk20Cfg = Field(default_factory=Brk20Cfg)
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
