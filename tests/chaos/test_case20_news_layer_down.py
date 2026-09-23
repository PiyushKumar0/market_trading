"""§9.4 chaos case 20 — news feeds dead / News Analyst down / DG3 during pre-open (§2.7).

Plan row (IMPLEMENTATION_PLAN.md §9.4, case 20), "Must hold", verbatim:

    **Two distinct rungs, one convention (§3.2.4):** feeds/scorer dead but digest RUNS ⇒
    empty-but-fresh digest, `CATALYST_WATCHLIST(0, 0)` (+ the D7 per-call Failed alert from the
    scorer itself), `cat` simply originates nothing; digest STALE (> `digest_stale_max_h`) or MISSING
    ⇒ `cat` disabled + `CATALYST_DISABLED(reason)`. Either way: **all other scanners, agents, and
    recommendations unaffected; no FROZEN state** — news is never load-bearing (E5); **non-`cat`
    candidate ranking is identical with the news layer up-but-quiet vs down (neutral feature
    defaults, §6.2)**; next healthy pre-open restores `cat` with no owner action

Composition — :class:`NewsWorld`, the §2.7 pipeline exactly as ``engine.ops.main`` runs it at
pre-open: REAL ``NewsIngest`` (RSS + GDELT) → ``HeadlineClusterer.run`` → ``EntityResolver.aload/run``
→ ``NewsScoringJob.run_batch(force=True)`` (the ``job_news_chain`` closure, main.py:1133-1158) →
``CatalystDigestJob.run`` + the CATALYST_* owner messages (the ``job_catalyst_digest`` closure,
main.py:1160-1171) → the window-open ``cat`` sweep leg (``_read_cat_watchlist`` /
``_watchlist_rows_for_symbols`` / ``cat.sweep_watchlist`` / ``SignalPreScreen.admit``,
main.py:1788-1865, with the shipped settings and the hash-verified ``catalyst_guard``). Underneath:
a tmp ``MarketStore`` (DuckDB), the migrated tmp SQLite, the REAL ``AgentHarness`` +
``BudgetGovernor`` + ``ContextAssembler``, ``ModeManager``/``RiskStateLatch``/``KillSwitch``, and a
REAL journalling ``TelegramBot`` (notifications table + main's ``alert``/``notify`` closures).
Faked, only at the boundary: the HTTP feeds (``httpx.MockTransport``), the Claude SDK (the
News Analyst's answers), and the Telegram transport. Storage faults are injected at the DuckDB read.

Clauses asserted:

* rung (i) — feeds dead / scorer dead / DG3: digest RUNS empty-but-FRESH, ``CATALYST_WATCHLIST(0, 0)``,
  the scorer's own D7 Failed alert (scorer-dead variant), ``cat`` originates nothing, no FROZEN,
  other agents still admitted — ``test_feeds_or_scorer_dead_digest_runs_empty_but_fresh``
* rung (ii) — digest MISSING / STALE because today's run failed: ``CATALYST_DISABLED(reason)``,
  ``cat`` originates nothing, no FROZEN — ``test_digest_failure_disables_cat_with_catalyst_disabled``
* rung (ii) — a STALE digest that nobody re-ran (late boot: the window-open sweep beats the catch-up
  digest) still originates nothing — ``test_stale_digest_at_sweep_time_originates_nothing``; its
  ``CATALYST_DISABLED`` alert is a DEFECT (xfail strict) —
  ``test_stale_digest_at_sweep_time_alerts_catalyst_disabled``
* non-``cat`` ranking identical, news up-but-quiet vs down (§6.2 neutral defaults), the other
  scanners unaffected — ``test_non_cat_candidates_identical_news_quiet_vs_down`` (real
  ``LiveScanContextProvider`` + ``FeatureEngine`` + the enabled per-bar scanners over identical bars)
* next healthy pre-open restores ``cat`` with no owner action —
  ``test_next_healthy_preopen_restores_cat_without_owner_action``
* control (non-vacuity): a healthy pre-open DOES originate — ``test_healthy_preopen_control_originates_cat``

Not asserted here: "recommendations unaffected" end-to-end through the RECOMMEND pipeline (case 5's
world covers the pipeline; news-down changes none of its inputs — asserted above as scanner output +
governor admission + risk state).
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

import httpx
import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import (
    NewsCfg,
    NewsFeedsCfg,
    NseAnnouncementsCfg,
    RssFeedCfg,
    config_dir,
    load_settings,
)
from engine.core.enums import Actor, DegradeTier, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.protected_store import ProtectedStore
from engine.core.types import Bar, OwnerConfirmation
from engine.datafeeds.news import Headline, NewsIngest
from engine.datafeeds.news_pipeline import CatalystDigestJob, EntityResolver, HeadlineClusterer
from engine.features.engine import ABSENT_NEWS_DEFAULTS, FeatureEngine
from engine.features.snapshots import load_snapshot
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor, TokenUsage
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.marketdata.store import DailyBar, MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind, catalyst_disabled, catalyst_watchlist
from engine.notify.telegram import TelegramBot
from engine.ops.main import INDEX_SYMBOL, VIX_SYMBOL, _read_cat_watchlist, _watchlist_rows_for_symbols
from engine.ops.news_scoring import NewsScoringJob
from engine.ops.scan_context import LiveScanContextProvider
from engine.risk.causes import RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine
from engine.risk.mode import ModeManager
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import build_enabled_scanners, cat
from engine.strategy.types import ScanContext, SignalCandidate
from tests.unit.test_budget_governor import PINNED_CFG

CAL_DIR = config_dir() / "calendar"
SETTINGS = load_settings()
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="chaos news seed")

# Real 2026 trading days (config/calendar/2026.yaml): Mon 15 / Tue 16 / Wed 17 / Thu 18 / Fri 19 June.
TUE, WED, THU, FRI = date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18), date(2026, 6, 19)

ET_FEED = "https://economictimes.indiatimes.com/markets/rssfeeds/chaos.cms"
MINT_FEED = "https://www.livemint.com/rss/chaos"
UNIVERSE = ("RELIANCE", "TCS", "INFY", "HDFCBANK")
ALIASES = {
    "reliance industries": "RELIANCE", "tata consultancy services": "TCS", "tcs": "TCS",
    "infosys": "INFY", "hdfc bank": "HDFCBANK",
}
NEWS_DEF = AgentDef(agent_id="news_analyst", model="haiku-4.5", shape="single_shot",
                    tools_enabled=False, allowed_tools=[], max_output_tokens=2000, timeout_s=30.0)


def at(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


# =========================================================================== the SDK boundary
class _Block:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


class _StructuredMessage:
    """The CLI's json_schema path: a forced ``StructuredOutput`` tool call carries the payload."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.content = [_Block(name="StructuredOutput", input=payload)]
        self.usage = None


class _ResultMessage:
    def __init__(self, usage: dict[str, int]) -> None:
        self.usage = usage
        self.total_cost_usd = 0.0
        self.is_error = False


class _Options:
    """Stand-in for ``ClaudeAgentOptions`` (every knob the harness probes for, §5.1/D10)."""

    def __init__(self, model=None, system_prompt=None, setting_sources=None, max_output_tokens=None,
                 max_turns=None, allowed_tools=None, disallowed_tools=None, output_schema=None) -> None:
        self.model, self.system_prompt, self.setting_sources = model, system_prompt, setting_sources
        self.max_output_tokens, self.max_turns = max_output_tokens, max_turns
        self.allowed_tools, self.disallowed_tools, self.output_schema = (
            allowed_tools, disallowed_tools, output_schema)


#: One ``for_news_batch`` prompt entry: the id line, then the headline line (context.py:357-362).
_CLUSTER_LINE = re.compile(r"cluster_id=(\S+) [^\n]*\n\s+headline: ([^\n]*)")

#: The analyst's canned behaviour: (cluster_id, headline) -> the raw JSON score items it emits.
Analyst = Callable[[str, str], list[dict[str, Any]]]


def score(cid: str, **fields: Any) -> dict[str, Any]:
    """One raw §5.4 score item (pre-validation), defaults = a routine market-noise cluster."""
    item = {"cluster_id": cid, "scope": "market", "entities": [], "sectors": [], "themes": [],
            "sentiment": 0.0, "materiality": 0.1, "event_type": "other", "novelty": 0.3}
    item.update(fields)
    return item


def healthy_analyst(cid: str, headline: str) -> list[dict[str, Any]]:
    """A well-behaved News Analyst: company order wins are material and positive, the rest noise."""
    h = headline.lower()
    for name in ("reliance industries", "tata consultancy services", "infosys", "hdfc bank"):
        if name in h:
            return [score(cid, scope="stock", entities=[name.title()], sentiment=0.6,
                          materiality=0.8, event_type="order_win", novelty=0.8)]
    return [score(cid)]


class NewsSDK:
    """Injected in place of ``claude_agent_sdk.query`` for the News Analyst. ``dead`` makes every call
    raise (CLI death); otherwise the analyst callback answers each cluster in the prompt."""

    def __init__(self, analyst: Analyst | None = healthy_analyst, *, dead: BaseException | None = None) -> None:
        self.analyst = analyst
        self.dead = dead
        self.calls = 0

    def __call__(self, *, prompt: str, options: Any) -> Any:
        self.calls += 1
        return self._stream(prompt)

    async def _stream(self, prompt: str) -> Any:
        if self.dead is not None:
            raise self.dead
        items: list[dict[str, Any]] = []
        for cid, headline in _CLUSTER_LINE.findall(prompt):
            items.extend(self.analyst(cid, headline))
        yield _StructuredMessage({"scores": items})
        yield _ResultMessage({"input_tokens": 2500, "output_tokens": 600})


# =========================================================================== the Telegram boundary
class _Phone:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.sent.append(text)


class _App:
    def __init__(self, bot: _Phone) -> None:
        self.bot = bot
        self.updater = None


# =========================================================================== world
class NewsWorld:
    """One engine's §2.7 news layer + the ``cat`` sweep leg over temp stores (see module docstring)."""

    def __init__(self, conn, root: Path, sdk: NewsSDK, *, now: datetime,
                 universe: tuple[str, ...] = UNIVERSE, aliases: dict[str, str] | None = None) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.conn = conn
        self.universe = universe
        self.aliases = ALIASES if aliases is None else aliases
        self.now = [now]
        self.clock = Clock(time_source=lambda: self.now[0])
        self.calendar = NSECalendar(CAL_DIR, self.clock, sqlite_conn=conn)
        self.bus = EventBus()
        self.mode = ModeManager(conn, self.clock, self.bus, self.calendar)
        self.latch = RiskStateLatch(conn, self.clock, self.mode)
        self.kill = KillSwitch(conn, self.clock, self.bus)
        self.store = MarketStore(root / "market.duckdb", root / "parquet", self.clock).open()
        cfg = root / "config"
        cfg.mkdir(exist_ok=True)
        shutil.copyfile(config_dir() / "limits.yaml", cfg / "limits.yaml")
        self.protected = ProtectedStore(cfg, conn, self.clock)
        self.protected.register_initial("limits.yaml", OWNER_OK)
        self.limits = LimitsEngine(self.protected)
        self.governor = BudgetGovernor(conn, self.clock, self.calendar, PINNED_CFG, bus=self.bus)

        self.phone = _Phone()
        self.bot = TelegramBot("t", owner_chat_id=1, clock=self.clock, mode_manager=self.mode,
                               kill_switch=self.kill, bus=self.bus, governor=self.governor, conn=conn)
        self.bot._app = _App(self.phone)
        self.bot.attach_bus(self.bus)                           # main.py:3894

        async def alert(severity: str, message: str) -> None:  # main.py:480-492
            await self.bot.send(CatalogMessage(
                kind=MessageKind.LIMIT_BREACH, title="alert", body=message,
                severity=severity if severity in ("info", "warning", "critical") else "warning",
            ))

        self.notify = self.bot.send                             # main.py:494-505 (log mirror aside)
        self.sdk = sdk
        self.harness = AgentHarness({"news_analyst": NEWS_DEF}, self.governor, self.clock, conn,
                                    query_fn=sdk, alert=alert, options_cls=_Options)

        # HTTP feeds: two RSS outlets + GDELT (empty); ``feeds_up=False`` makes every fetch fail.
        self.feeds_up = True
        self.items: dict[str, list[tuple[str, str, datetime]]] = {ET_FEED: [], MINT_FEED: []}
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self._serve))
        news_cfg = NewsCfg(feeds=NewsFeedsCfg(
            rss={"et": RssFeedCfg(url=ET_FEED, poll_s=300), "mint": RssFeedCfg(url=MINT_FEED, poll_s=900)},
            nse_announcements=NseAnnouncementsCfg(enabled=False),
        ))
        self.ingest = NewsIngest(news_cfg, self.store, self.clock, self.http)
        self.clusterer = HeadlineClusterer(
            self.store, sim_threshold=SETTINGS.news.cluster_sim_threshold,
            max_event_age_days=SETTINGS.cat.max_event_age_days,
        )
        self.resolver = EntityResolver(self.store, self.clock)
        self.scoring = NewsScoringJob(
            self.store, self.resolver, ContextAssembler(self.store, conn, self.clock, self.calendar),
            self.harness, {"news_analyst": NEWS_DEF}, self.governor, self.clock, self.calendar,
        )
        self.digest = CatalystDigestJob(self.store, self.clock, self.calendar, self.protected)
        pre = SETTINGS.strategy.prescreen
        self.prescreen = SignalPreScreen(
            [], lambda bar: ScanContext(), None,
            max_candidates_per_day=pre.max_candidates_per_day,
            max_per_strategy_day=pre.max_per_strategy_day,
            admission_mode=pre.admission_mode,
            catalyst_cap_fn=lambda: self.limits.catalyst_guard().max_catalyst_entries_day,  # main.py:836
            displacement_margin=pre.displacement_margin,
            cap_release_schedule=pre.cap_release_schedule,
        )
        self.chain_lock = asyncio.Lock()
        self._seed_market()

    # ------------------------------------------------------------------ boundary: HTTP feeds
    def _serve(self, request: httpx.Request) -> httpx.Response:
        if not self.feeds_up:
            raise httpx.ConnectError("chaos: feed host unreachable", request=request)
        url = str(request.url)
        if url.startswith("https://api.gdeltproject.org"):
            return httpx.Response(200, json={"articles": []})
        for feed, items in self.items.items():
            if url == feed:
                body = "".join(
                    f"<item><title>{escape(title)}</title><link>{escape(link)}</link>"
                    f"<pubDate>{format_datetime(published)}</pubDate></item>"
                    for title, link, published in items
                )
                xml = f'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>{body}</channel></rss>'
                return httpx.Response(200, content=xml.encode("utf-8"))
        return httpx.Response(404)

    def publish(self, feed: str, title: str, link: str, published: datetime) -> None:
        self.items[feed].append((title, link, published))

    # ------------------------------------------------------------------ seeding (the market side)
    def _seed_market(self) -> None:
        """Universe rows for every test day, curated aliases, and 30 weekdays of bars_1d per symbol
        (the digest's levels and the sweep's prior-session reference close both read them)."""
        days = (TUE, WED, THU, FRI)
        self.store.upsert_universe_daily(
            [{"d": d, "symbol": s, "included": True} for d in days for s in self.universe]
        )
        self.store.upsert_entity_aliases([
            {"alias": a, "tradingsymbol": s, "source": "curated", "added_at": self.clock.now()}
            for a, s in self.aliases.items()
        ])
        bars: list[DailyBar] = []
        probe, n = FRI - timedelta(days=1), 0
        while n < 30:
            if probe.weekday() < 5:
                for i, s in enumerate(self.universe):
                    px = Decimal(1000 + 100 * i)
                    bars.append(DailyBar(symbol=s, d=probe, open=px, high=px + 10, low=px - 10,
                                         close=px, volume=500_000))
                n += 1
            probe -= timedelta(days=1)
        self.store.upsert_bars_1d(bars)

    # ------------------------------------------------------------------ the composition-root jobs
    def set_time(self, when: datetime) -> None:
        self.now[0] = when

    async def news_chain(self) -> None:
        """``job_news_chain`` (main.py:1133-1158): backfill → cluster → resolve → forced scoring batch."""
        inserted = await self.ingest.backfill()
        seen = {h.headline_id for h in inserted}
        orphans = [
            Headline(**{k: r[k] for k in ("headline_id", "title", "source_domain", "url", "published_at")})
            for r in await self.store.arun(
                self.store.get_news,
                published_after=self.clock.now() - timedelta(days=4), unclustered_only=True,
            )
            if r["headline_id"] not in seen
        ][:500]
        headlines = inserted + orphans
        if headlines:                                          # main.resolve_news (lock + chain)
            async with self.chain_lock:
                touched = await self.clusterer.run(headlines)
                await self.resolver.aload(self.clock.today())
                await self.resolver.run(touched)
        try:
            await self.scoring.run_batch(force=True, lock=self.chain_lock)
        except Exception:  # noqa: BLE001 - main.py:1157: unscored clusters stay off the watchlist
            pass

    async def digest_job(self) -> Any:
        """``job_catalyst_digest`` (main.py:1160-1171), verbatim semantics: CATALYST_WATCHLIST on
        every successful run (incl. an empty-but-fresh (0, 0)); CATALYST_DISABLED + re-raise on failure."""
        try:
            result = await self.digest.run(self.clock.today())
        except Exception as exc:
            await self.notify(catalyst_disabled(f"digest failed: {exc}"))
            raise
        await self.notify(catalyst_watchlist(result.n_originating, result.n_context))
        return result

    async def preopen(self, d: date) -> Any:
        """The day's pre-open: news chain ~08:25, digest ~08:35 (§10.1). Returns the digest result,
        or the exception the digest job raised (the job watermark records it as failed)."""
        self.set_time(at(d, 8, 25))
        await self.news_chain()
        self.set_time(at(d, 8, 35))
        try:
            return await self.digest_job()
        except Exception as exc:  # noqa: BLE001 - the scheduler records the failure; the day goes on
            return exc

    def cat_sweep(self, d: date) -> list[SignalCandidate]:
        """The window-open sweep's ``cat`` leg (main.py:1788-1807) + the ranked admission (1859-1865)."""
        self.set_time(at(d, 10, 5))
        start, end = self.calendar.trade_window(d)
        in_window = start <= self.clock.now() <= end
        cat_rows, _rev = _read_cat_watchlist(self.store, d)
        cat_rows = _watchlist_rows_for_symbols(cat_rows, set(self.store.get_universe_eligible_symbols(d)))
        raw = cat.sweep_watchlist(
            cat_rows, params={"stop_pct": SETTINGS.cat.stop_pct, "hold_sessions": SETTINGS.cat.hold_sessions},
        ) if cat_rows else []
        return [c for c in self.prescreen.admit(raw, d, in_window=in_window) if c.strategy_id == cat.STRATEGY_ID]

    # ------------------------------------------------------------------ observations
    def messages(self, kind: MessageKind | None = None) -> list[tuple[str, str, str]]:
        rows = self.conn.execute(
            "SELECT kind, title, body FROM notifications ORDER BY created_at, rowid").fetchall()
        return [(r["kind"], r["title"], r["body"]) for r in rows
                if kind is None or r["kind"] == kind.value]

    def watchlist(self, d: date) -> dict[str, dict[str, Any]]:
        return {r["symbol"]: r for r in self.store.get_catalyst_watchlist(d)}

    def assert_not_frozen(self) -> None:
        """E5: news is never load-bearing — no news failure may FREEZE (or kill) the platform."""
        assert self.mode.risk_state() == RiskState.NORMAL
        assert self.latch.active_causes() == []
        assert not self.kill.is_killed()

    def assert_other_agents_admitted(self) -> None:
        """"agents unaffected": the intraday analyst and the planner are still admitted by the governor."""
        assert self.governor.can_invoke("intraday_analyst", "signal").allowed
        assert self.governor.can_invoke("preopen_planner", "schedule").allowed

    async def close(self) -> None:
        await self.http.aclose()
        self.store.close()


def publish_reliance_story(w: NewsWorld) -> None:
    """A genuine, two-outlet order-win story the evening before WED's pre-open (story-level corroboration)."""
    w.publish(ET_FEED, "Reliance Industries bags Rs 12,000 crore green hydrogen order from NTPC",
              "https://economictimes.indiatimes.com/markets/ril-hydrogen-order/1.cms", at(TUE, 18, 10))
    w.publish(MINT_FEED, "NTPC picks Reliance Industries for large hydrogen supply contract",
              "https://www.livemint.com/companies/ril-ntpc-contract-11.html", at(TUE, 18, 40))
    w.publish(ET_FEED, "Sensex ends flat as metal stocks drag; broader market mixed",
              "https://economictimes.indiatimes.com/markets/wrap-flat/2.cms", at(TUE, 16, 0))


def publish_tcs_story(w: NewsWorld) -> None:
    """A second genuine story the evening before THU's pre-open."""
    w.publish(ET_FEED, "Tata Consultancy Services wins USD 2 billion deal from UK insurer Aviva",
              "https://economictimes.indiatimes.com/tech/tcs-aviva-deal/3.cms", at(WED, 19, 5))
    w.publish(MINT_FEED, "Aviva signs Tata Consultancy Services for multi-year IT transformation",
              "https://www.livemint.com/companies/tcs-aviva-12.html", at(WED, 19, 30))


@pytest.fixture
async def make_world(conn, tmp_path):
    made: list[NewsWorld] = []

    async def _make(sdk: NewsSDK | None = None, *, now: datetime | None = None) -> NewsWorld:
        w = NewsWorld(conn, tmp_path / f"world{len(made)}", sdk or NewsSDK(), now=now or at(WED, 8, 20))
        await w.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
        made.append(w)
        return w

    yield _make
    for w in made:
        await w.close()


# =========================================================================== control
async def test_healthy_preopen_control_originates_cat(make_world) -> None:
    """Non-vacuity: with feeds, scorer and digest healthy the same world grades the corroborated
    story ``originating`` and the sweep admits exactly one ``cat`` candidate for it."""
    w = await make_world()
    publish_reliance_story(w)

    result = await w.preopen(WED)

    assert (result.n_originating, result.n_context) == (1, 0), result
    assert w.watchlist(WED)["RELIANCE"]["grade"] == "originating"
    assert [c.symbol for c in w.cat_sweep(WED)] == ["RELIANCE"]
    assert w.digest.digest_status(WED) == "fresh"
    assert [b for _, _, b in w.messages(MessageKind.CATALYST_WATCHLIST)] == [
        catalyst_watchlist(1, 0).body]
    w.assert_not_frozen()


# =========================================================================== rung (i)
@pytest.mark.parametrize("failure", ["feeds_dead", "scorer_dead", "dg3"])
async def test_feeds_or_scorer_dead_digest_runs_empty_but_fresh(make_world, failure: str) -> None:
    """Rung (i): the digest still RUNS and yields an EMPTY-but-FRESH watchlist — owner told
    ``CATALYST_WATCHLIST(0, 0)``, never ``CATALYST_DISABLED`` — so ``cat`` simply originates nothing.
    The scorer-dead variant also carries the scorer's own D7 per-call Failed alert; under DG3 the
    news analyst is refused by the governor before any SDK call (§5.6)."""
    sdk = NewsSDK(dead=RuntimeError("claude CLI process died (exit 1)")) if failure == "scorer_dead" else NewsSDK()
    w = await make_world(sdk)
    publish_reliance_story(w)
    if failure == "feeds_dead":
        w.feeds_up = False
    if failure == "dg3":                  # 96% of the $100 pinned weekly credit ⇒ DG3 (≥95%, <100%)
        await w.governor.record("weekly_researcher", "sonnet-4.6", TokenUsage(in_tokens=32_000_000, out_tokens=0))
        assert w.governor.degrade_tier() == DegradeTier.DG3

    result = await w.preopen(WED)

    assert not isinstance(result, Exception), result            # the digest RAN
    assert (result.n_originating, result.n_context) == (0, 0)
    assert w.watchlist(WED) == {}
    assert w.digest.digest_status(WED) == "fresh"               # empty-but-FRESH, not stale/missing
    assert [b for _, _, b in w.messages(MessageKind.CATALYST_WATCHLIST)] == [catalyst_watchlist(0, 0).body]
    assert w.messages(MessageKind.CATALYST_DISABLED) == []      # rung (i) never disables
    assert w.cat_sweep(WED) == []                               # `cat` originates nothing

    clusters = w.store.get_news_clusters()
    if failure == "feeds_dead":
        assert clusters == [] and w.sdk.calls == 0
    else:
        assert clusters and all(c["scored_at"] is None for c in clusters)   # unscored ⇒ excluded
    if failure == "scorer_dead":
        assert w.sdk.calls == 1
        failed = [b for _, t, b in w.messages(MessageKind.LIMIT_BREACH)
                  if t == "alert" and "agent news_analyst failed: sdk_error" in b]
        assert failed, w.messages()                             # the D7 per-call Failed alert
    if failure == "dg3":
        assert w.sdk.calls == 0                                 # governor refused before the SDK
        assert any("DG3" in t for _, t, _ in w.messages(MessageKind.BUDGET_WARNING))
    else:
        w.assert_other_agents_admitted()
    w.assert_not_frozen()


# =========================================================================== rung (ii)
@pytest.mark.parametrize("prior_digest", [False, True], ids=["missing", "stale"])
async def test_digest_failure_disables_cat_with_catalyst_disabled(make_world, prior_digest: bool) -> None:
    """Rung (ii): today's digest run FAILS (a DuckDB read fault injected at the store) — with no
    digest ever (MISSING) or only yesterday's (STALE, > digest_stale_max_h). The owner gets
    ``CATALYST_DISABLED(reason)``; ``cat`` originates nothing; nothing freezes."""
    w = await make_world()
    if prior_digest:
        await w.preopen(TUE)                                    # yesterday's digest ran fine
        assert w.digest.digest_status(TUE) == "fresh"
    publish_reliance_story(w)
    w.set_time(at(WED, 8, 25))
    await w.news_chain()                                        # a scored, originating-quality story
    assert any(c["scored_at"] is not None and c["symbols"] == ["RELIANCE"]
               for c in w.store.get_news_clusters())

    real_read = w.store.get_news_clusters

    def _duckdb_fault(**kw: Any) -> Any:
        raise OSError("chaos: DuckDB read failed (IO error)")

    w.store.get_news_clusters = _duckdb_fault                   # only the digest's read is hit
    try:
        w.set_time(at(WED, 8, 35))
        with pytest.raises(OSError):
            await w.digest_job()
    finally:
        w.store.get_news_clusters = real_read

    assert w.digest.digest_status(WED) == ("stale" if prior_digest else "missing")
    disabled = w.messages(MessageKind.CATALYST_DISABLED)
    assert len(disabled) == 1 and "digest failed" in disabled[0][2], w.messages()
    assert w.cat_sweep(WED) == []                               # `cat` disabled for the day
    w.assert_other_agents_admitted()
    w.assert_not_frozen()


async def _late_boot_stale_world(make_world) -> NewsWorld:
    """TUE's digest ran; the evening chain ingested + scored an originating-quality story; WED the
    engine boots late and the window-open sweep runs BEFORE the catch-up digest has (not failed —
    simply not yet) run: the digest is STALE at use time."""
    w = await make_world()
    await w.preopen(TUE)
    publish_reliance_story(w)
    w.set_time(at(TUE, 19, 30))
    await w.news_chain()
    assert any(c["scored_at"] is not None and c["symbols"] == ["RELIANCE"]
               for c in w.store.get_news_clusters())
    w.set_time(at(WED, 10, 5))
    assert w.digest.digest_status(WED) == "stale"               # TUE 08:35 → WED 10:05 = 25.5 h > 20
    return w


async def test_stale_digest_at_sweep_time_originates_nothing(make_world) -> None:
    """Rung (ii), stale-without-a-failure: ``cat`` is disabled (originates nothing) and nothing freezes."""
    w = await _late_boot_stale_world(make_world)
    assert w.cat_sweep(WED) == []
    w.assert_other_agents_admitted()
    w.assert_not_frozen()


@pytest.mark.xfail(strict=True, reason=(
    "DEFECT: a digest that is STALE/MISSING at use time without the digest job raising (late boot: "
    "the window-open cat sweep runs before the catch-up digest) raises no CATALYST_DISABLED — the only "
    "emitter is main.py:1169 (digest exception); CatalystDigestJob.digest_status (news_pipeline.py:1324) "
    "has no production caller except the API freshness read"
))
async def test_stale_digest_at_sweep_time_alerts_catalyst_disabled(make_world) -> None:
    """Rung (ii): STALE ⇒ ``cat`` disabled **+ ``CATALYST_DISABLED(reason)``** — the owner is told."""
    w = await _late_boot_stale_world(make_world)
    w.cat_sweep(WED)
    assert w.messages(MessageKind.CATALYST_DISABLED), w.messages()


# =========================================================================== recovery
async def test_next_healthy_preopen_restores_cat_without_owner_action(make_world) -> None:
    """WED: the scorer is dead AND the digest run faults ⇒ nothing originates. THU: everything is
    healthy again ⇒ ``cat`` originates the new story with NO owner command in between (no mode
    change, no re-arm, no /resume — the test issues none)."""
    w = await make_world(NewsSDK(dead=RuntimeError("claude CLI process died (exit 1)")))
    publish_reliance_story(w)
    real_run = w.digest.run

    async def _fault(d: date) -> Any:
        raise OSError("chaos: DuckDB read failed (IO error)")

    w.digest.run = _fault
    assert isinstance(await w.preopen(WED), OSError)
    assert w.cat_sweep(WED) == []
    assert len(w.messages(MessageKind.CATALYST_DISABLED)) == 1

    w.digest.run = real_run                                     # the storage fault clears
    w.sdk.dead = None                                           # the SDK recovers
    publish_tcs_story(w)
    result = await w.preopen(THU)

    assert not isinstance(result, Exception)
    assert w.watchlist(THU)["TCS"]["grade"] == "originating"
    assert [c.symbol for c in w.cat_sweep(THU)] == ["TCS"]      # restored, no owner action
    assert w.digest.digest_status(THU) == "fresh"
    assert w.mode.mode() == Mode.RECOMMEND
    w.assert_not_frozen()


# =========================================================================== non-cat ranking identity
def _weekdays_before(d: date, n: int) -> list[date]:
    out: list[date] = []
    probe = d - timedelta(days=1)
    while len(out) < n:
        if probe.weekday() < 5:
            out.append(probe)
        probe -= timedelta(days=1)
    return list(reversed(out))


#: rsi2 set-ups (test_scanners.test_rsi2_hand_computed_signal shape): 198 rising closes, one −2.00
#: session, then today's 10:05 bar lower again — RSI(2) deep in oversold, far above the 200-DMA.
RSI2_SETUPS = {"TCS": (194.50,), "INFY": (193.90,), "HDFCBANK": (195.10,)}


def _seed_scanner_market(store: MarketStore) -> None:
    days = _weekdays_before(WED, 199)
    bars = [DailyBar(symbol=INDEX_SYMBOL, d=dd, open=Decimal(str(100 + 0.5 * i)), high=Decimal(str(101 + 0.5 * i)),
                     low=Decimal(str(99 + 0.5 * i)), close=Decimal(str(100 + 0.5 * i)), volume=1)
            for i, dd in enumerate(days)]
    for sym in RSI2_SETUPS:
        closes = [100 + 0.5 * i for i in range(198)] + [196.5]
        bars += [DailyBar(symbol=sym, d=dd, open=Decimal(str(c)), high=Decimal(str(c)), low=Decimal(str(c)),
                          close=Decimal(str(c)), volume=100_000) for dd, c in zip(days, closes, strict=True)]
    store.upsert_bars_1d(bars)
    store.upsert_universe_daily([{"d": WED, "symbol": s, "included": True} for s in RSI2_SETUPS])


async def _scan_world(conn, root: Path, *, news_quiet: bool) -> tuple[list[SignalCandidate], dict[str, dict]]:
    """One world's per-bar scan at WED 10:05 through the live ``bar.1m`` path (main.py:812-852): the
    enabled per-bar scanners (the shipped settings park ``orb``), the real provider + FeatureEngine,
    the real pre-screen. ``news_quiet``: the digest ran on an empty corpus; else no digest ever ran."""
    root.mkdir(parents=True, exist_ok=True)
    now = [at(WED, 8, 35)]
    clock = Clock(time_source=lambda: now[0])
    calendar = NSECalendar(CAL_DIR, clock, sqlite_conn=conn)
    store = MarketStore(root / "market.duckdb", root / "parquet", clock).open()
    try:
        _seed_scanner_market(store)
        if news_quiet:
            cfg = root / "config"
            cfg.mkdir(exist_ok=True)
            shutil.copyfile(config_dir() / "limits.yaml", cfg / "limits.yaml")
            protected = ProtectedStore(cfg, conn, clock)
            protected.register_initial("limits.yaml", OWNER_OK)
            result = await CatalystDigestJob(store, clock, calendar, protected).run(WED)
            assert (result.n_originating, result.n_context) == (0, 0)
        now[0] = at(WED, 10, 5)
        features = FeatureEngine(store, clock, calendar, index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)
        provider = LiveScanContextProvider(store, clock, calendar, features, index_symbol=INDEX_SYMBOL)
        bus = EventBus()
        published: list[SignalCandidate] = []

        async def _capture(c: SignalCandidate) -> None:
            published.append(c)

        bus.subscribe("signal.candidate", _capture)
        pre = SETTINGS.strategy.prescreen
        prescreen = SignalPreScreen(
            build_enabled_scanners(("orb", "rsi2", "trend", "mom")), provider, bus,
            max_candidates_per_day=pre.max_candidates_per_day, max_per_strategy_day=pre.max_per_strategy_day,
            admission_mode=pre.admission_mode, displacement_margin=pre.displacement_margin,
            cap_release_schedule=pre.cap_release_schedule,
        )
        for sym, (close,) in RSI2_SETUPS.items():
            c = Decimal(str(close))
            await prescreen.handle_bar(Bar(symbol=sym, ts_minute=at(WED, 10, 5), open=c, high=c, low=c,
                                           close=c, volume=5_000))
        snapshots = {
            c.symbol: load_snapshot(store, c.features_snapshot_id).features
            for c in published if c.features_snapshot_id
        }
        return published, snapshots
    finally:
        store.close()


async def test_non_cat_candidates_identical_news_quiet_vs_down(conn, tmp_path) -> None:
    """§6.2: the non-``cat`` scanners publish the SAME candidates in the SAME order with the SAME
    scores and levels whether the news layer is up-but-quiet or down, and every candidate's feature
    snapshot carries the pinned neutral news block (only ``sentiment_available`` records which)."""
    quiet, quiet_feats = await _scan_world(conn, tmp_path / "quiet", news_quiet=True)
    down, down_feats = await _scan_world(conn, tmp_path / "down", news_quiet=False)

    def _shape(cands: list[SignalCandidate]) -> list[tuple]:
        return [(c.strategy_id, c.symbol, c.side, c.style, round(c.score, 12), c.raw_levels.entry,
                 c.raw_levels.stop, c.raw_levels.target, c.catalyst_ref) for c in cands]

    assert quiet, "vacuous: the scenario must publish non-cat candidates"
    assert all(c.strategy_id != cat.STRATEGY_ID for c in quiet)
    assert _shape(quiet) == _shape(down)                         # identical ranking + levels
    assert set(quiet_feats) == set(down_feats) == {c.symbol for c in quiet}
    news_keys = [k for k in ABSENT_NEWS_DEFAULTS if k != "sentiment_available"]
    for sym in quiet_feats:
        assert {k: quiet_feats[sym][k] for k in news_keys} == {k: ABSENT_NEWS_DEFAULTS[k] for k in news_keys}
        assert {k: down_feats[sym][k] for k in news_keys} == {k: ABSENT_NEWS_DEFAULTS[k] for k in news_keys}
        assert quiet_feats[sym]["sentiment_available"] is True      # the digest ran (fresh, empty)
        assert down_feats[sym]["sentiment_available"] is False      # no digest at all
        non_news = {k for k in quiet_feats[sym] if k not in ABSENT_NEWS_DEFAULTS}
        assert {k: quiet_feats[sym][k] for k in non_news} == {k: down_feats[sym][k] for k in non_news}
    # sanity: the snapshots really are JSON-clean scalars (never NaN), as §6.2 pins
    json.dumps(quiet_feats, allow_nan=False)
