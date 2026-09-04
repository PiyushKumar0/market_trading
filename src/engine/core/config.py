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
from pydantic import BaseModel, ConfigDict, Field


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
    # In-session tick-silence guard (2026-07-22 tickless-HEALTHY session): the child heartbeats every
    # 1 s REGARDLESS of ticks, so a feed that delivers NO ticks still reads HEALTHY all day. During
    # market hours, tick silence beyond this budget (heartbeats still fine) ⇒ a visible DEGRADED state
    # + owner alert. Off-hours keeps heartbeat-only semantics (no false night alarms).
    tick_silence_degrade_s: int = 120
    # Cadence of the periodic in-session ``feed_stats`` INFO line (ticks/drops/bars since last stat).
    feed_stats_interval_s: int = 300
    # WARMING-wedge guard (2026-07-23 13:41 sleep/resume incident): the HEALTHY-path heartbeat-silence
    # kill deliberately skips WARMING (a fresh spawn legitimately has no ticks/heartbeat yet), and
    # WARMING had NO timeout — so a child that dies/hangs before its first heartbeat (or a system-resume
    # that froze the machine mid-WARMING) wedged the state machine forever (zero respawns, feed dead).
    # If WARMING persists this long with no heartbeat ⇒ kill + respawn (also the generic system-resume
    # recovery: whatever state froze, the timeout fires and respawns).
    warming_timeout_s: int = 60
    # Consecutive WARMING-timeout respawns back off exponentially (warming_timeout_s × 2ⁿ) capped here.
    warming_backoff_cap_s: int = 300
    # After this many consecutive WARMING-timeout respawns without reaching HEALTHY, keep retrying at
    # the capped backoff but escalate ONCE to the owner (the feed is structurally wedged).
    max_wedge_respawns: int = 5
    # In-session OS keep-awake (Windows): while the NSE session is open, assert SetThreadExecutionState
    # so the OS does not auto-sleep mid-session and freeze the feed. Opt-out here. Non-Windows: no-op.
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
    # §3.2.4 batch-universe extended leg (2026-09-01, owner-directed after the JINDALSAW/movers
    # review): criteria-passing NON-index symbols (MIS ∩ EQ-master ∩ not-surveillance ∩ ₹5cr median)
    # persisted included=False / exclusion_reasons=['not_in_index'] for BATCH rules, news shadow and
    # pre-open advisory only — the tick watchlist and the risk gate stay scoped to the configured
    # index (NIFTY 500 since 2026-09-04, O15). The flag is the rollback: False restores the
    # pre-addendum universe shape exactly.
    batch_universe_enabled: bool = False
    batch_universe_max: int = 600                 # top-N extended by median traded value (sanity cap)


class RssFeedCfg(BaseModel):
    """One §3.2.4 RSS source: where to fetch it and how often. Cadence is per-feed (publishers
    differ in update rate and in how much politeness they want), so the scheduler arms one job
    per entry of :attr:`NewsFeedsCfg.rss` rather than a fixed set of named jobs."""

    url: str
    poll_s: int = 900


class NseAnnouncementsCfg(BaseModel):
    """§2.7 (2026-09-04 amendment) exchange-announcements feed: NSE ``corporate-announcements``.

    Its own cadence (the exchange disseminates all day, 300 s is the plan's pin) and its own
    administrative-subject drop list — filings whose SUBJECT is routine compliance carry no event
    content and would only burn scorer budget. ``drop_subjects`` is owner config exactly like
    ``NewsCfg.drop_title_patterns``: a case-insensitive SUBSTRING match ("Newspaper Publication"
    catches NSE's "Copy of Newspaper Publication"), deliberately conservative — a subject that is
    merely LOW-value stays in, because a wrong drop is silent and permanent (the item never reaches
    the corpus at all). ``enabled: false`` is the owner's off switch: no fetch, no scheduler job.
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
    """§3.2.4 ``NewsIngest`` feed set (§2.7 step 1). Headline-level only; bodies are never fetched (A3r).

    ``rss`` is an open name → feed map: adding/removing a source is a settings.yaml edit (config_audit),
    not a code change — the 2026-08-04 lesson, when the corpus silently collapsed to ET-only.
    Moneycontrol RSS was RETIRED 2026-08-04: its whole feed ecosystem has been frozen since ~2024-04
    (newest pubDate ~832 days old; 391 engine polls inserted zero headlines), which starved the §2.7
    ``min_source_domains`` corroboration gate. The Livemint markets/companies feeds that replace it
    were live-verified 2026-08-04 (HTTP 200, 35 items each, newest items minutes old)."""

    rss: dict[str, RssFeedCfg] = Field(default_factory=lambda: {
        "et": RssFeedCfg(
            url="https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", poll_s=300),
        "livemint_markets": RssFeedCfg(url="https://www.livemint.com/rss/markets", poll_s=900),
        "livemint_companies": RssFeedCfg(url="https://www.livemint.com/rss/companies", poll_s=900),
        # 2026-08-05 pool widening: both livemint feeds are ONE registrable domain, so the
        # corroboration pool was effectively two — three new DOMAINS make "any 2 of 5" clearable
        # for genuine stories (§2.7 story-level counting). All three live-verified 2026-08-05.
        "hbl_markets": RssFeedCfg(
            url="https://www.thehindubusinessline.com/markets/feeder/default.rss", poll_s=900),
        "hbl_companies": RssFeedCfg(
            url="https://www.thehindubusinessline.com/companies/feeder/default.rss", poll_s=900),
        "cnbctv18_market": RssFeedCfg(
            url="https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml", poll_s=900),
        "ndtvprofit": RssFeedCfg(url="https://feeds.feedburner.com/ndtvprofit-latest", poll_s=900),
        # Business Standard re-enters 2026-09-04 (re-probed live: HTTP 200, 35 items each, newest
        # 12 / 35 min old) — the 2026-08-05 rejection was a WAF 403 that no longer reproduces.
        "bs_markets": RssFeedCfg(
            url="https://www.business-standard.com/rss/markets-106.rss", poll_s=900),
        "bs_companies": RssFeedCfg(
            url="https://www.business-standard.com/rss/companies-101.rss", poll_s=900),
    })
    gdelt_doc_query: str = "sourcecountry:IN (markets OR stocks OR earnings OR NSE)"
    #: The exchange itself as a feed (§2.7 2026-09-04): filings carry ``symbol`` natively, so an
    #: announcement is resolvable without the alias seed, and NSE counts as one corroborating domain.
    nse_announcements: NseAnnouncementsCfg = Field(default_factory=NseAnnouncementsCfg)


class NewsCfg(BaseModel):
    """News-intelligence ingest knobs (§2.7/§4.4 job 10). Owner-only operational config (config_audit)."""

    feeds: NewsFeedsCfg = Field(default_factory=NewsFeedsCfg)
    cluster_sim_threshold: float = 0.75   # §3.2.4 pinned clustering similarity [tunable, owner]
    #: Case-insensitive substrings that mark a feed item as an auto-generated live-blog/ticker PAGE
    #: title, not a news headline — dropped at ingest (2026-08-03 G1 finding: ET's "<Company> Share
    #: Price Live Updates: ..." template glued up to 27 companies into one cluster and burned scorer
    #: budget on zero-content titles). Owner-editable like the feed set (config_audit).
    drop_title_patterns: list[str] = Field(default_factory=lambda: [
        "share price live updates",
        "stock market live updates",
        "results live updates",
        "share price highlights",
        # Moneycontrol QUOTE-page titles ("X Share Price , X Stock Price , ..." — G1 verdict row 17):
        # the comma-spaced template never appears in written prose.
        "stock price ,",
        # Multi-company ROUNDUP titles (2026-08-03 second G1 iteration): breadth commentary, not
        # company events — and worse, they BRIDGE different companies' clusters (a roundup naming
        # Adani Ports + Adani Enterprises merges both families' clusters, cross-attributing every
        # member). No catalyst signal is lost: §2.7 wants per-company events, not market wraps.
        "stocks in news",
        "market wrap",
        "buzzing stocks",
        "top gainers",
        "gainers and losers",
        # Seed-5 verdict residuals (2026-08-03): more roundup/series templates that bridge clusters.
        "trade spotlight",
        "stocks to buy in 2026",
        " live :",
        # Seed-6 row 23: ET/MC F&O-desk multi-name series ("F&O Talk: ... says <analyst>").
        # Matches post-unescape titles (ingest html.unescapes; "F&amp;O Talk" never reaches here).
        "f&o talk",
        # Seed-7 row 46 (owner-marked): "X among 4 stocks closing above/below VWAP" screener
        # series — automated screener output, not news; different companies' editions cluster
        # together on the shared template.
        "vwap",
        # 2026-08-05 new-source templates (probed at feed adoption): HBL's liveblog series titles
        # itself "Stock Market Live:" (no "updates" — the existing pattern misses it), and
        # CNBC-TV18's "11:11" branded multi-topic digest bridges unrelated companies per item.
        "stock market live",
        "11:11",
        # HBL's "Q1 Results Highlights: ONGC, BSE, Bharti Airtel, ..." multi-company roundup —
        # the G1 bridging shape, seen live 2026-08-05 before first ingest. Also drops
        # single-company "<X> Results Highlights" liveblog wrappers; the plain result article
        # on the same domain carries the event, so no catalyst signal is lost.
        "results highlights",
    ])
    # RSS cadences are per-feed (``feeds.rss[name].poll_s``); only GDELT keeps a top-level knob.
    gdelt_poll_s: int = 3600              # GDELT DOC 2.0 poll cadence — 900→3600 (2026-08-04: 429s
                                          # persisted at 1800 s and a single fresh probe still 429'd;
                                          # O12 only needs the pre-open digest, so hourly costs nothing)
    request_timeout_s: float = 30.0       # per-fetch HTTP timeout — 10→30 (2026-08-04: 121 GDELT
                                          # ConnectTimeouts at the old 10 s, ~99% of polls dead)
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
    # --- v2 SHADOW scanner knobs (§2.7 2026-08-18 amendment, WO-18): the `ins` mechanics mirror.
    # Frozen for the whole shadow window — moving one re-opens multiplicity and restarts the clock.
    stop_pct: float = 5.0                 # disaster stop: §7.1 sizing needs a risk distance and the
                                          # rule's real exit is TIME; 5 not 6 because the §7.1
                                          # overnight_gap_mult (2.5x) arithmetic applies (ins, 08-17)
    hold_sessions: int = 20               # = the §7.1 swing max_holding cap — the EXISTING
                                          # max-holding machinery is the exit path
    # Deliberately NO expected_edge_pct: `cat` has no validated edge (measuring it IS the shadow), so
    # the §7.1 C3 check fail-closed-rejects every cat candidate. Adding the key is the §8.6 owner gate.


class CatReversalCfg(BaseModel):
    """§2.7 ``cat_reversal`` SHADOW knobs (2026-08-27) — OWNER-ONLY, deliberately NOT learnable.

    A SEPARATE block from :class:`CatCfg` on purpose. ``cat`` v2 and ``cat_reversal`` are two
    experiments with independently pre-registered thresholds and independently running clocks; sharing
    a ``stop_pct`` would let a future tuning of one silently move the other and invalidate whichever
    shadow window happened to be open. Frozen for the whole ``cat_reversal`` shadow window.
    """

    stop_pct: float = 5.0                 # disaster stop: §7.1 sizing needs a risk distance and the
                                          # rule's real exit is TIME; the §7.1 overnight_gap_mult
                                          # (2.5x) arithmetic applies to a swing entry (ins, 08-17)
    hold_sessions: int = 10               # intended exit if the §8.6 gate ever opens = the longer
                                          # pre-registered horizon (T+10). INERT during the shadow:
                                          # nothing is held, because C3 rejects every candidate.
    # Deliberately NO expected_edge_pct — and, unlike `cat`, the absence is not what enforces it: the
    # strategy is listed in the gate's `no_edge_shadow_strategies`, so C3 rejects it whatever target
    # the analyst supplies. Adding an edge here is the §8.6 owner gate, post-verdict.


class FilingsCfg(BaseModel):
    """§2.8.4 corporate-filings event thresholds — OWNER-ONLY, deliberately NOT learnable (no new
    envelope-learnable parameters in stages 1–2). STORED CONFIG ONLY in stage 1: these are read by no
    decision path yet — event typing (§2.8.2) and the catalyst wiring are later stages, gated on the
    §2.8.4 event study. Modelled here so the settings.yaml ``filings`` block is honored, not silently
    dropped by pydantic's default extra-ignore (mirrors how ``news``/``cat`` are modelled)."""

    insider_min_value_inr: int = 10_000_000       # abs ₹ floor for an insider_net_buy event (§2.8.2)
    insider_min_value_over_adv: float = 0.05      # AND value/20d-ADV floor (§2.8.2)
    pledge_delta_min_pct: float = 5.0             # QoQ promoter-pledge %-point change to flag (§2.8.2)


class InsCfg(BaseModel):
    """§6.1 ``ins`` (insider net-BUY swing) knobs — OWNER-ONLY, deliberately NOT learnable in Phase 2.

    Provenance: WO-16 (2026-08-14) re-ran the §2.8.4 insider_net_buy leg under corrected mechanics
    (disclosure-date anchoring, next-session-open fills, spread-inclusive CNC costs, WO-3 margin floor
    on the CPCV stage) and it SURVIVED — T+10 +0.7297% / T+20 +1.5797% net, CPCV +0.0359%/day. It is
    the only recorded edge that did, and the reason this strategy exists.

    ``stop_pct`` [4-8] and ``hold_sessions`` [10-20] are FUTURE §6.3 envelope rows; they stay static
    owner settings here for Phase 2 (no protected-store churn until the learning phase needs them).
    ``threshold_inr`` is owner-FIXED and never learnable at all — it defines the validated event
    population, so moving it re-opens multiplicity.
    """

    stop_pct: float = 6.0                 # disaster stop: §7.1 sizing needs a risk distance, and the
                                          # validated design is stopless — this is the deliberate
                                          # deviation, wide enough to rarely interrupt the T+20 drift
    hold_sessions: int = 20               # = the §7.1 swing max_holding cap = the validated T+20
                                          # horizon; the EXISTING max-holding machinery is the exit
    threshold_inr: int = 10_000_000       # ₹1cr trailing-10-session open-market net-BUY floor
    expected_edge_pct: float = 1.58       # the validated T+20 NET drift, fed to the C3 cost gate
                                          # (target is None for `ins`, so the edge cannot be derived
                                          # from levels — see risk/gate.py `_rule_min_viable_size`)


class PrescreenCfg(BaseModel):
    """§6.3/§3.2.5 ``SignalPreScreen`` caps (owner-tunable). Deduped candidate origination limits;
    the gate/envelope layer owns per-parameter bounds — these are coarse per-day throttles."""

    max_candidates_per_day: int = 48               # signal.candidate publications per trading day
                                                   # (20→48 2026-08-18, kept = the analyst forward
                                                   # cap `prescreen_cap_per_day`; shipped value in
                                                   # settings.yaml — this default is the fallback)
    #: Per-strategy publication sub-cap. int = the same cap for every strategy; MAPPING
    #: {strategy_id: cap} = per-strategy values with the reserved key `default` binding any strategy
    #: without a line of its own (WO-1 (iii), 2026-08-13 — owner knob, never learner-movable, §6.3).
    max_per_strategy_day: int | dict[str, int] | None = None
    #: Publication/forward admission ORDER: "ranked" = by score (prescreen) and per-strategy score
    #: quantile (analyst forwarding); "arrival" = the pre-WO-1 order — the rollback flag, nothing else.
    admission_mode: str = "ranked"
    #: Cumulative per-strategy sub-cap tranches by IST time-of-day (owner-directed 2026-09-02, after
    #: three straight sessions of window-open cap lockout): {strategy: {"HH:MM": cumulative_cap}}.
    #: Effective cap = min(max_per_strategy_day, released tranche); absent = flat caps (rollback).
    #: Owner knob, never learner-movable (§6.3) — it decides which candidates get evaluated at all.
    cap_release_schedule: dict[str, dict[str, int]] | None = None
    #: WHEN the analyst forward queue is drained (2026-08-14): "paced" = one slot per
    #: ``FORWARD_PACING_MIN`` minutes on a scheduler pulse, so candidates accumulate and the ranking
    #: above has a population to rank; "immediate" = the pre-2026-08-14 inline drain — rollback only.
    forward_drain_mode: str = "paced"
    #: How much better a later candidate must score to take a full cap's admission slot from an
    #: unevaluated incumbent of the same strategy (2026-08-27 — owner knob, never learner-movable,
    #: §6.3, exactly like ``max_per_strategy_day`` above: it decides which candidates get evaluated
    #: at all). ``null`` DISABLES displacement, restoring the pre-2026-08-27 "whoever fires first
    #: keeps the slot all day" behaviour — the rollback flag, nothing else. Shipped value lives in
    #: settings.yaml with its reasoning; this default is the fallback.
    displacement_margin: float | None = 0.10


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
    """§3.2.4 UniverseBuilder inputs (A8) — the index that seeds the eligible universe. The list is
    best-effort-fetched (E5) with a seed-file fallback so universe build never depends on an NSE
    page being reachable.

    O15 (owner-directed 2026-09-04): the index is NIFTY 500, widened from NIFTY200. The fields are
    named generically because the index is now CONFIG, not a constant — swapping it is these three
    keys plus a seed file. ``extra="forbid"`` (deliberately narrower than the rest of this module,
    which takes pydantic's default ignore) exists for exactly that reason: the retired
    ``nifty200_source_url`` / ``nifty200_seed_path`` keys got no aliases, and a settings.yaml left
    on them must fail loudly at boot rather than be silently ignored while the defaults below
    present a different universe as live.
    """

    model_config = ConfigDict(extra="forbid")

    index_name: str = "NIFTY 500"
    """Display name of the eligible-universe index — carried on the ``universe_built`` log line and
    in the fallback alert, so ops can see WHICH index a degraded build fell back to."""
    index_source_url: str = (
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"  # [VERIFIED 2026-09-04]
    )
    index_seed_path: str = "config/universe/nifty500_seed.csv"


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
    filings_pit_fresh_ist: time = time(19, 0)   # §2.8 filings_pit_fresh (BSE fresh insider, date-keyed)
    ins_crossings_ist: time = time(19, 15)      # §6.1 ins_crossings — MUST stay after filings_pit_fresh
                                                # (19:00): it consumes that job's same-day BSE rows
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
    cat_reversal: CatReversalCfg = Field(default_factory=CatReversalCfg)
    filings: FilingsCfg = Field(default_factory=FilingsCfg)
    ins: InsCfg = Field(default_factory=InsCfg)
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
