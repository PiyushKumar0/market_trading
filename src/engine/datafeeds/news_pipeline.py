"""Deterministic news pipeline: ``HeadlineClusterer`` + ``EntityResolver``
and the ``CatalystDigestJob``.

Every algorithm here is PINNED — cluster membership drives the
``catalyst_guard.min_source_domains`` corroboration count and the resolver's symbols feed the
catalyst watchlist, so there is zero implementation latitude and NO LLM anywhere in this module:

- **HeadlineClusterer**: normalize a title to its sorted set of unique lowercase
  alphanumeric tokens; ``similarity(a, b) = difflib.SequenceMatcher(None, " ".join(tokens_a),
  " ".join(tokens_b)).ratio()``; process headlines in ``published_at`` order, assigning each greedily
  to the EARLIEST-``first_seen`` existing cluster whose REPRESENTATIVE headline scores ≥
  ``news.cluster_sim_threshold`` (default 0.75), considering only clusters with ``last_seen`` inside
  the ``cat.max_event_age_days`` window; no match ⇒ a new cluster with this headline as
  representative. A cluster carries its DISTINCT ``source_domains`` set. Deterministic and
  golden-file unit-tested: same headlines in ⇒ same clusters out. One preference on top: a joining
  member carrying an :data:`EXCHANGE_TOKEN_RE` token becomes the representative, so a filing merged
  with its press coverage stays exactly resolvable.
- **EntityResolver**: an explicit ``[NSE:<SYMBOL>]`` exchange token resolves first and exactly, under
  the same universe check and stripped before alias matching; then case-insensitive WHOLE-WORD PHRASE
  containment of the normalized
  alias in the normalized title. Alias seed = instruments-dump company names with legal suffixes
  stripped MINUS a curated common-English-word stoplist. AMBIGUOUS (an alias mapping to >1 distinct
  tradingsymbol, OR different companies' aliases matching overlapping title spans) ⇒ NO match, never
  a guess — logged to ``unresolved_entities`` (weekly suggestion loop). Out-of-universe
  entities are recorded, never traded. Sector/theme tags via ``sector_map`` + ``theme_map`` keyword
  match (same whole-word rule).
- **CatalystDigestJob**: the two pinned digest outputs — ``sentiment_agg`` (a clipped
  decay-weighted SUM, never a mean) and the day's ``catalyst_watchlist`` (grades + deterministic
  levels). It consumes the analyst scores through the persisted ``news_clusters`` columns only,
  so replay never re-invokes the LLM, and loads the ``catalyst_guard`` block
  hash-verified at run time — the enforcement site, never a constructor argument.
  Its own resolved ambiguities are listed on the class, not here.

Spec ambiguities resolved here (documented for the integrator):

- The clusterer's eligibility window is the ONE envelope row ``cat.max_event_age_days`` (read via
  :func:`load_cat_params`, so ``envelope_state`` overrides apply — the same value the digest
  uses), but the clusterer counts it in CALENDAR days relative to the headline being processed
  (``last_seen ≥ published_at − window``) while the digest counts the same number in
  TRADING SESSIONS. A calendar-window clusterer stays deterministic without an ``NSECalendar``
  dependency, and switching it to sessions would change cluster membership across weekends —
  an upstream selection change for the frozen ``cat`` v2 shadow (owner decision, post-verdict).
- ``cluster_id`` is derived from the joining headline's platform-assigned ULID
  (``"c-" + headline_id``) so the algorithm itself is a pure function of its input (golden-file
  byte-identity) — no ULID minted here.
- The resolver matches against the cluster's REPRESENTATIVE headline (the title the matching rule
  normalizes); resolver normalization preserves token ORDER (phrase containment), unlike the
  clusterer's sorted-set normalization.

Analyst pointer (deliberately NOT stubbed here): News Analyst scoring
(Tier-1 LLM, scores persisted per CLUSTER) — belongs to ``engine.intelligence``. The News
Analyst's verbatim entity strings for unmatched clusters re-enter :meth:`EntityResolver.resolve`
via ``extra_texts`` — the LLM never assigns a symbol directly.

Failure model: this module is deterministic CPU-bound work over already-ingested rows; the async
``run`` wrappers offload DuckDB access through ``MarketStore`` (convention 12) and are scheduled by
the news-pipeline job, which is never load-bearing.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import time as _time_mod
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from difflib import SequenceMatcher
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, field_validator
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import CatCfg, config_dir, load_yaml
from engine.core.log import get_logger
from engine.core.protected_store import ProtectedStore
from engine.datafeeds.news import Headline
from engine.datafeeds.sector_map import SECTOR_SOURCES
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.indicators import wilder_atr

_log = get_logger("engine.datafeeds.news_pipeline")

_TOKEN_RE = re.compile(r"[a-z0-9]+")

#: Pinned legal-suffix list for the alias seed, as NORMALIZED TOKENS. "&" is non-alphanumeric
#: and vanishes in tokenization, so the pinned "& CO" suffix is the trailing token "co" here.
LEGAL_SUFFIX_TOKENS: frozenset[str] = frozenset({
    "ltd", "limited", "pvt", "private", "co", "corp", "corporation",
    "india", "industries", "enterprises",
})

#: Curated stoplist of seed aliases that are common English words — they would false-positive on
#: ordinary headline text, so they are EXCLUDED from the alias seed. Owner-reviewed; additions arrive
#: only via the owner-approval loop.
ALIAS_STOPLIST: frozenset[str] = frozenset({
    "trent",      # Trent Ltd
    "idea",       # (curated) Vodafone Idea aliases
    "coal",       # Coal India Ltd -> "coal" after suffix strip
    "oil",        # Oil India Ltd -> "oil" after suffix strip
    "dollar",     # Dollar Industries — matched currency contexts (owner-marked)
    "page",       # Page Industries Ltd -> "page"
    "escorts",    # Escorts Kubota / Escorts Ltd
    "lupin",      # Lupin Ltd (common noun)
    "orient",     # Orient Electric / Orient Cement
    "century",    # Century Textiles / Century Plyboards
    "campus",     # Campus Activewear Ltd
    "united",     # United Spirits / United Breweries partial forms
    "bse",        # BSE Ltd — fires on VENUE mentions ("IPO set for BSE SME debut"); owner verdict:
                  # losing genuine BSE-the-company coverage is the accepted recall cost.
})

#: Sector label that must never become a keyword tag (the weekly sector-map job's fallback bucket).
_UNCLASSIFIED = "UNCLASSIFIED"

#: The ten NSE sectoral-index sector names — the ONLY sector labels allowed into the keyword
#: vocabulary. sector_map's industry fallback also mints buckets like CAPITAL_GOODS, SERVICES and
#: DIVERSIFIED for the exposure caps; as headline keywords those generic words would false-tag
#: wholesale, so they are filtered out.
_INDEX_SECTORS: frozenset[str] = frozenset(sector for sector, _ in SECTOR_SOURCES)

#: The pinned ``news_clusters`` columns (mirrors MarketStore._TABLE_SPEC — unknown keys are a
#: hard error there, so this tuple is validated on every upsert).
_CLUSTER_ROW_FIELDS: tuple[str, ...] = (
    "cluster_id", "representative", "source_domains", "first_seen", "last_seen", "scope",
    "entities", "symbols", "sectors", "themes", "sentiment", "materiality", "event_type",
    "novelty", "scored_at", "scorer_model", "untrusted",
)


def title_tokens(text: str) -> list[str]:
    """Lowercase alphanumeric tokens IN ORDER — the resolver's whole-word normalization."""
    return _TOKEN_RE.findall(text.lower())


#: The EXPLICIT EXCHANGE TOKEN. ``NewsIngest``'s ``nse_ann`` feed prefixes every announcement title
#: with ``[NSE:<SYMBOL>]``, taken verbatim from the exchange payload. It is an EXACT identification:
#: the resolver honours it ahead of alias matching (still subject to the universe check), and the
#: clusterer prefers a token-bearing headline as representative so a filing merged with its press
#: coverage stays resolvable. The character class covers tradingsymbols like ``M&MFIN``, ``BAJAJ-AUTO``.
EXCHANGE_TOKEN_RE = re.compile(r"\[NSE:([A-Z0-9&\-]+)\]")


def exchange_tokens(text: str) -> list[str]:
    """The exchange-token symbols in ``text``, in order of appearance, deduped (usually one)."""
    out: list[str] = []
    for symbol in EXCHANGE_TOKEN_RE.findall(text or ""):
        if symbol not in out:
            out.append(symbol)
    return out


def strip_exchange_tokens(text: str) -> str:
    """``text`` with every exchange token removed — alias matching must NEVER see it.

    The token is exchange-assigned identity, not prose: left in, its ``NSE`` prefix and the symbol
    itself are ordinary words to :meth:`EntityResolver._match_aliases`, and either could match an
    alias (or poison a span component into ``ambiguous``).
    """
    return EXCHANGE_TOKEN_RE.sub(" ", text or "")


#: Boilerplate strip: recurring TEMPLATE phrases in Indian financial-press headlines ("<Company> Share
#: Price Live Updates: ..."). With the template tokens included they dominate the token-set
#: similarity and glue DIFFERENT companies into one cluster. Phrases are removed as whole-word token
#: SUBSEQUENCES before the sorted-set normalization, longest first. Curated in code like
#: :data:`ALIAS_STOPLIST` (owner-reviewed; not learnable).
CLUSTERER_BOILERPLATE_PHRASES: tuple[str, ...] = (
    "share price live updates",
    "stock price live updates",
    "results live updates",
    "share price highlights",
    "live updates",
    "share price today",
    "stock market live updates",
    # Earnings-template vocabulary: same-day "<Company> Q1 Results: Profit rises N% YoY to Rs M crore"
    # headlines from DIFFERENT companies share enough template tokens to clear the 0.75 bar.
    # Similarity must run on the DISTINCTIVE tokens (company name + figures) — clusterer-only; the
    # resolver never sees this list.
    "q1 results", "q2 results", "q3 results", "q4 results",
    "net profit", "profit rises", "profit falls", "profit jumps", "profit drops",
    "profit surges", "profit dips", "revenue rises", "revenue climbs", "revenue up",
    "q1", "q2", "q3", "q4", "yoy", "qoq", "rs", "crore", "cr", "results",
)
_BOILERPLATE_TOKENSEQS: tuple[tuple[str, ...], ...] = tuple(
    sorted((tuple(p.split()) for p in CLUSTERER_BOILERPLATE_PHRASES), key=len, reverse=True)
)


def _strip_boilerplate(tokens: list[str]) -> list[str]:
    """Remove every whole-word occurrence of each boilerplate phrase from the token sequence."""
    for seq in _BOILERPLATE_TOKENSEQS:
        n = len(seq)
        out: list[str] = []
        i = 0
        while i < len(tokens):
            if tuple(tokens[i:i + n]) == seq:
                i += n
            else:
                out.append(tokens[i])
                i += 1
        tokens = out
    return tokens


def clusterer_normalize(title: str) -> str:
    """Pinned clusterer normalization: boilerplate-phrase strip (see
    :data:`CLUSTERER_BOILERPLATE_PHRASES`), then the sorted set of UNIQUE lowercase alphanumeric
    tokens. The strip runs on the ORDERED token sequence (phrases are positional); the set/sort
    happens after, so the golden-file determinism is unchanged in kind.

    A title made ENTIRELY of boilerplate falls back to its unstripped token set — two empty
    norms would be similarity 1.0 and everything template-only would collapse into one cluster.
    """
    tokens = title_tokens(title)
    stripped = _strip_boilerplate(tokens)
    return " ".join(sorted(set(stripped or tokens)))


def similarity(norm_a: str, norm_b: str) -> float:
    """Pinned similarity over two :func:`clusterer_normalize` outputs."""
    return SequenceMatcher(None, norm_a, norm_b).ratio()


def strip_legal_suffixes(company_name: str) -> str:
    """Normalized company name with trailing legal-suffix tokens stripped (never below one token).

    Stripping is iterative (``"COAL INDIA LIMITED" → "coal india" → "coal"``) — the resulting
    common-word aliases are handled by :data:`ALIAS_STOPLIST`, not by refusing the strip.
    """
    return alias_variants(company_name)[-1] if title_tokens(company_name) else ""


def alias_variants(company_name: str) -> list[str]:
    """EVERY stage of the iterative suffix strip, longest first (never below one token).

    The seed takes all stages, not just the final strip: "COAL INDIA" fully strips to "coal", which
    the stoplist rightly kills, but "coal india" is what headlines print — dropping ONLY the dangerous
    stage keeps the company resolvable. All stages map to the same symbol, so overlapping-span
    matches stay unambiguous (union of 1).
    """
    tokens = title_tokens(company_name)
    if not tokens:
        return []
    out = [" ".join(tokens)]
    while len(tokens) > 1 and tokens[-1] in LEGAL_SUFFIX_TOKENS:
        tokens.pop()
        out.append(" ".join(tokens))
    return out


class NewsCluster(BaseModel):
    """One ``news_clusters`` row: clustering fields + resolver/scorer columns carried
    through so a re-upsert (new member joins a scored cluster) never clobbers persisted LLM scores
    (replay consumes persisted scores; the LLM is never re-invoked for a past cluster).

    ``headline_ids`` is NOT a table column: membership lives on ``news.cluster_id``. On a cluster
    loaded from the store it holds only the ids assigned during THIS run — exactly the rows
    :meth:`MarketStore.set_news_cluster` must update.
    """

    cluster_id: str
    representative: str
    source_domains: list[str]                 # DISTINCT, sorted — the corroboration input
    first_seen: datetime
    last_seen: datetime
    headline_ids: list[str] = []
    # resolver output (symbols[] is EntityResolver output ONLY — the LLM never assigns one)
    scope: str | None = None
    entities: list[str] | None = None
    symbols: list[str] | None = None
    sectors: list[str] | None = None
    themes: list[str] | None = None
    # scorer columns — carried through, never written by this module
    sentiment: float | None = None
    materiality: float | None = None
    event_type: str | None = None
    novelty: float | None = None
    scored_at: datetime | None = None
    scorer_model: str | None = None
    untrusted: bool = True                    # always TRUE; forced again in to_row()

    @field_validator("first_seen", "last_seen", "scored_at")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None:
            raise ValueError("cluster timestamps must be tz-aware (naive datetimes are a bug)")
        return v.astimezone(IST)

    def to_row(self) -> dict[str, Any]:
        """The ``news_clusters`` upsert dict (pinned columns; ``untrusted`` forced TRUE)."""
        row = {f: getattr(self, f) for f in _CLUSTER_ROW_FIELDS}
        row["untrusted"] = True
        return row

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> NewsCluster:
        """Build from a ``MarketStore.get_news_clusters`` dict (NULL columns fall back to defaults)."""
        return cls(**{f: row[f] for f in _CLUSTER_ROW_FIELDS if row.get(f) is not None})


class UnresolvedEntity(BaseModel):
    """One ``unresolved_entities`` record (ambiguous/out-of-universe/no-match; suggestion loop)."""

    model_config = ConfigDict(frozen=True)

    entity_text: str
    reason: Literal["ambiguous", "out_of_universe", "no_match"]
    candidate_symbols: tuple[str, ...] = ()


class ResolvedCluster(BaseModel):
    """``EntityResolver.resolve`` output: deterministic tags for one cluster.

    ``symbols`` contains only unambiguous, in-universe resolutions; everything else is in
    ``unresolved`` (recorded, never traded / never a guess).
    """

    cluster_id: str
    entities: list[str] = []                  # matched alias strings (normalized, verbatim seeds)
    symbols: list[str] = []
    sectors: list[str] = []
    themes: list[str] = []
    unresolved: list[UnresolvedEntity] = []


class HeadlineClusterer:
    """Deterministic near-duplicate grouping into ``news_clusters``. NO LLM.

    Parameters
    ----------
    store:
        Optional :class:`MarketStore` for the store-wired :meth:`run`; :meth:`cluster` itself is a
        pure function (golden-file tested).
    sim_threshold:
        ``news.cluster_sim_threshold`` (pinned default 0.75; owner-tunable in settings.yaml).
    max_event_age_days:
        The envelope row ``cat.max_event_age_days`` (default 2; the boot passes
        ``load_cat_params(envelope_state)``, so promotions apply) counted in CALENDAR days back from
        each headline's ``published_at`` — only clusters with ``last_seen`` inside this window are
        assignment candidates. The digest counts the same number in trading sessions (module note).
    """

    def __init__(
        self,
        store: MarketStore | None = None,
        *,
        sim_threshold: float = 0.75,
        max_event_age_days: int = 2,
    ) -> None:
        self._store = store
        self._sim_threshold = float(sim_threshold)
        self._window = timedelta(days=int(max_event_age_days))

    def cluster(
        self, hs: list[Headline], existing: Sequence[NewsCluster] | None = None
    ) -> list[NewsCluster]:
        """The pinned algorithm. Returns every cluster that gained ≥1 headline from ``hs``
        (new clusters plus updated ``existing`` ones), ordered by ``(first_seen, cluster_id)``.

        Pure and deterministic: input ``existing`` clusters are copied, never mutated; ties in
        ``published_at`` order break on ``url`` (matching ``NewsIngest`` batch order).
        """
        clusters: list[NewsCluster] = [c.model_copy(deep=True) for c in (existing or [])]
        norms: dict[str, str] = {c.cluster_id: clusterer_normalize(c.representative) for c in clusters}
        touched: dict[str, NewsCluster] = {}

        # Progress visibility: a weekend-backlog pass is ~90 s of pure difflib CPU and otherwise
        # indistinguishable from a wedge in the log.
        started = _time_mod.perf_counter()
        if len(hs) >= 100:   # backlog-sized passes only — per-poll trickles would log ~1,000×/day
            _log.info("news_clustering_started", headlines=len(hs), window_clusters=len(clusters))

        for i, h in enumerate(sorted(hs, key=lambda h: (h.published_at, h.url))):
            if i and i % 100 == 0:
                _log.info("news_clustering_progress", done=i, total=len(hs),
                          elapsed_s=round(_time_mod.perf_counter() - started, 1))
            norm = clusterer_normalize(h.title)
            target: NewsCluster | None = None
            # Greedy: the EARLIEST-first_seen cluster (not the best-scoring one) at/above threshold.
            for c in sorted(clusters, key=lambda c: (c.first_seen, c.cluster_id)):
                if c.last_seen < h.published_at - self._window:
                    continue  # aged out of the cat.max_event_age_days window
                if similarity(norm, norms[c.cluster_id]) >= self._sim_threshold:
                    target = c
                    break
            if target is None:
                cid = f"c-{h.headline_id or hashlib.sha256(h.url.encode()).hexdigest()[:26]}"
                target = NewsCluster(
                    cluster_id=cid,
                    representative=h.title,
                    source_domains=[h.source_domain],
                    first_seen=h.published_at,
                    last_seen=h.published_at,
                )
                clusters.append(target)
                norms[cid] = norm
            else:
                # A token-bearing member PROMOTES itself to representative: the resolver only sees the
                # representative, so a filing merged into press coverage would otherwise lose the
                # string that names its symbol. The earliest such member keeps the seat (an existing
                # token representative is never displaced); the comparison norm follows it.
                if exchange_tokens(h.title) and not exchange_tokens(target.representative):
                    target.representative = h.title
                    norms[target.cluster_id] = norm
                if h.source_domain not in target.source_domains:
                    target.source_domains = sorted({*target.source_domains, h.source_domain})
                target.first_seen = min(target.first_seen, h.published_at)
                target.last_seen = max(target.last_seen, h.published_at)
            if h.headline_id:
                target.headline_ids = [*target.headline_ids, h.headline_id]
            touched[target.cluster_id] = target

        return sorted(touched.values(), key=lambda c: (c.first_seen, c.cluster_id))

    async def run(self, hs: list[Headline]) -> list[NewsCluster]:
        """Store-wired clustering: load window clusters, :meth:`cluster`, persist, link ``news`` rows.

        The upsert carries ALL persisted columns through :meth:`NewsCluster.to_row`, so a scored
        cluster keeps its analyst scores when a new member arrives.
        """
        if self._store is None:
            raise RuntimeError("HeadlineClusterer.run requires a MarketStore (pure calls use .cluster)")
        if not hs:
            return []
        window_start = min(h.published_at for h in hs) - self._window
        rows = await self._store.arun(self._store.get_news_clusters, last_seen_after=window_start)
        existing = [NewsCluster.from_row(r) for r in rows]
        # OFF the event loop (heartbeat invariant): the CPU-bound difflib pass would stall every
        # loop consumer (bus handlers, API, scheduler) for its duration.
        touched = await asyncio.to_thread(self.cluster, hs, existing)
        await self._store.aupsert_news_clusters([c.to_row() for c in touched])
        for c in touched:
            if c.headline_ids:
                await self._store.arun(self._store.set_news_cluster, c.headline_ids, c.cluster_id)
        existing_ids = {c.cluster_id for c in existing}
        _log.info(
            "news_clustered",
            headlines=len(hs),
            clusters_touched=len(touched),
            clusters_new=sum(1 for c in touched if c.cluster_id not in existing_ids),
        )
        return touched


class EntityResolver:
    """Deterministic-first symbol/sector/theme tagging. Ambiguous ⇒ NO match, ever.

    State (alias map, sector map, theme keywords, universe) is loaded from the store via
    :meth:`load` / :meth:`aload`, or injected directly for tests/pure use. ``universe`` is one of
    three states after :meth:`load`: a non-empty frozenset (the built batch universe, filtered
    normally); an EMPTY frozenset (the build ran but nothing passed — FAIL CLOSED, every symbol
    resolves ``out_of_universe``); or ``None`` ("universe unknown", e.g. before the 08:30 build —
    no ``universe_daily`` rows for the day at all), where no out-of-universe filtering happens here
    and the digest re-checks ``symbol ∈ universe`` before anything can be traded.

    Parameters
    ----------
    store / clock:
        Store for persistence + ``entity_aliases.added_at`` stamps. Both optional for pure use.
    aliases:
        ``alias -> tradingsymbol(s)`` seed state (normalized internally).
    sector_map:
        ``symbol -> sector`` (``sector_map``); the DISTINCT sector names are the sector
        keywords (whole-word rule) — minus the ``UNCLASSIFIED`` fallback bucket.
    theme_map:
        ``theme -> keywords[]`` (``theme_map`` seeded from config/themes.yaml).
    universe:
        Today's BATCH universe (``MarketStore.get_batch_universe_symbols``: eligible incl.
        beyond-cap rows PLUS criteria-passing non-index rows), NOT the focus watchlist
        (``data.universe_max_watchlist``), or None if not yet built. News visibility must not be
        watchlist-cap-contaminated.
    """

    def __init__(
        self,
        store: MarketStore | None = None,
        clock: Clock | None = None,
        *,
        aliases: Mapping[str, str | Iterable[str]] | None = None,
        sector_map: Mapping[str, str] | None = None,
        theme_map: Mapping[str, Iterable[str]] | None = None,
        universe: Iterable[str] | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._aliases: dict[str, frozenset[str]] = {}
        self._sector_keywords: dict[str, frozenset[str]] = {}
        self._theme_keywords: dict[str, frozenset[str]] = {}
        self._universe: frozenset[str] | None = None
        if aliases:
            self._set_aliases(
                (a, (s,) if isinstance(s, str) else tuple(s)) for a, s in aliases.items()
            )
        if sector_map:
            self.set_sector_map(sector_map)
        if theme_map:
            self.set_theme_map(theme_map)
        if universe is not None:
            self._universe = frozenset(universe)

    # ------------------------------------------------------------------ state loading
    def _set_aliases(self, pairs: Iterable[tuple[str, Iterable[str]]]) -> None:
        merged: dict[str, set[str]] = {}
        for alias, syms in pairs:
            norm = " ".join(title_tokens(alias))
            if not norm or norm in ALIAS_STOPLIST:
                # Stoplist enforced at LOAD too: a persisted row for a later-stoplisted alias must stop
                # matching on the next load, with no store surgery while the engine holds the lock.
                continue
            merged.setdefault(norm, set()).update(syms)
        self._aliases = {a: frozenset(s) for a, s in merged.items()}

    def set_sector_map(self, sector_map: Mapping[str, str]) -> None:
        """Sector keywords = the distinct INDEX sector NAMES (whole-word phrase rule).

        UNCLASSIFIED and every industry-derived bucket are excluded: those buckets exist for the
        risk-limit exposure caps and the sector features, never as headline keywords — "services" or
        "diversified" as a keyword would false-tag a large share of the corpus.
        """
        kw: dict[str, set[str]] = {}
        for sector in sector_map.values():
            if sector == _UNCLASSIFIED or sector.strip().upper() not in _INDEX_SECTORS:
                continue
            norm = " ".join(title_tokens(sector))
            if norm:
                kw.setdefault(norm, set()).add(sector)
        self._sector_keywords = {k: frozenset(v) for k, v in kw.items()}

    def set_theme_map(self, theme_map: Mapping[str, Iterable[str]]) -> None:
        kw: dict[str, set[str]] = {}
        for theme, keywords in theme_map.items():
            for keyword in keywords:
                norm = " ".join(title_tokens(str(keyword)))
                if norm:
                    kw.setdefault(norm, set()).add(theme)
        self._theme_keywords = {k: frozenset(v) for k, v in kw.items()}

    def load(self, d: Any = None) -> None:
        """(Re)load alias/sector/theme/universe state from the store (sync; see :meth:`aload`).

        CURATED rows take priority: for an alias with any ``source='curated'`` row, only the
        curated symbol(s) load — the owner's explicit mapping overrides the machine seed (e.g.
        "Reliance" pins RELIANCE over the conglomerate-prefix ambiguity union the seed produces).

        Universe loads via :meth:`MarketStore.get_batch_universe_symbols` — the BATCH universe
        (rule-passing incl. beyond-cap AND criteria-passing non-index rows), NOT the top-N focus
        watchlist; news visibility must be neither watchlist-cap- nor index-contaminated. `cat` /
        `cat_reversal` remain fail-closed at the cost gate, so the widening only grows the SHADOW
        evidence base.
        """
        if self._store is None:
            raise RuntimeError("EntityResolver.load requires a MarketStore")
        curated_syms: dict[str, set[str]] = {}
        all_syms: dict[str, set[str]] = {}
        for row in self._store.get_entity_aliases():
            all_syms.setdefault(row["alias"], set()).add(row["tradingsymbol"])
            if row.get("source") == "curated":
                curated_syms.setdefault(row["alias"], set()).add(row["tradingsymbol"])
        self._set_aliases(
            (a, tuple(sorted(curated_syms.get(a) or syms))) for a, syms in all_syms.items()
        )
        self.set_sector_map({r["symbol"]: r["sector"] for r in self._store.get_sector_map()})
        self.set_theme_map({r["theme"]: list(r["keywords"] or []) for r in self._store.get_theme_map()})
        if d is None and self._clock is not None:
            d = self._clock.today()
        if d is None:
            self._universe = None
        else:
            universe_symbols = self._store.get_batch_universe_symbols(d)
            if universe_symbols:
                self._universe = frozenset(universe_symbols)
            else:
                rows = self._store.get_universe_daily(d)
                if rows:
                    # Rows exist but none passed (every row excluded) — the build ran, so this is
                    # "empty", not "unknown".
                    _log.warning("universe_empty_fail_closed", d=str(d), rows=len(rows))
                    self._universe = frozenset()
                else:
                    self._universe = None

    async def aload(self, d: Any = None) -> None:
        if self._store is None:
            raise RuntimeError("EntityResolver.aload requires a MarketStore")
        await self._store.arun(self.load, d)

    def seed_aliases(self, instruments: Iterable[Mapping[str, Any] | tuple[str, str]]) -> int:
        """Build the alias SEED from instruments-dump rows and merge + persist it.

        ``instruments`` yields dicts with ``name``/``tradingsymbol`` (the ``instruments_daily`` row
        shape) or plain ``(company_name, tradingsymbol)`` tuples. Each company name seeds
        EVERY suffix-strip stage (:func:`alias_variants`); aliases in :data:`ALIAS_STOPLIST` are
        dropped per stage. Returns the number of (alias, symbol) pairs seeded.

        DICT rows are filtered to NSE EQUITIES: every derivative row's ``name`` is its underlying, so
        the full dump maps one company name to hundreds of contract symbols and the ambiguity rule
        un-matches good aliases. Tuple rows are trusted as (company_name, symbol).
        """
        pairs: list[tuple[str, str]] = []
        stripped_stage: set[str] = set()
        stoplisted = 0
        for item in instruments:
            if isinstance(item, tuple):
                name, symbol = item
            else:
                if str(item.get("exchange") or "") != "NSE" or str(item.get("instrument_type") or "") != "EQ":
                    continue
                name, symbol = item.get("name"), item.get("tradingsymbol")
            if not name or not symbol:
                continue
            for stage, alias in enumerate(alias_variants(str(name))):
                if alias in ALIAS_STOPLIST:
                    stoplisted += 1
                    continue
                pairs.append((alias, str(symbol)))
                if stage > 0:
                    stripped_stage.add(alias)

        # Conglomerate-surname guard: a STRIPPED-stage alias ("ADANI ENTERPRISES" → "adani") that
        # token-PREFIXES another company's alias is ambiguous by construction — a bare "Adani"
        # headline must refuse with candidates. Union the prefixed symbols in; the multi-symbol rule
        # then refuses, while span subsumption still resolves "Adani Power" to ADANIPOWER. Curated
        # rows override at load().
        alias_syms: dict[str, set[str]] = {}
        for a, s in pairs:
            alias_syms.setdefault(a, set()).add(s)
        by_first: dict[str, list[str]] = {}
        for a in alias_syms:
            by_first.setdefault(a.split(" ", 1)[0], []).append(a)
        for a in stripped_stage:
            if a not in alias_syms:
                continue
            toks = a.split(" ")
            for b in by_first.get(toks[0], ()):
                if b != a and b.split(" ")[: len(toks)] == toks:
                    pairs.extend((a, s) for s in alias_syms[b] - alias_syms[a])

        self._set_aliases(
            list((a, syms) for a, syms in self._aliases.items())
            + [(a, (s,)) for a, s in pairs]
        )
        if self._store is not None and pairs:
            added_at = self._clock.now() if self._clock is not None else None
            self._store.upsert_entity_aliases(
                [{"alias": a, "tradingsymbol": s, "source": "seed", "added_at": added_at}
                 for a, s in sorted(set(pairs))]
            )
        _log.info("entity_aliases_seeded", seeded=len(pairs), stoplisted=stoplisted)
        return len(pairs)

    def seed_curated_aliases(self, cfg: Mapping[str, Any]) -> int:
        """Merge + persist the owner's ``config/aliases.yaml`` CURATED additions. Same normalization and
        stoplist as the dump seed; persisted with ``source='curated'`` so the daily re-seed never
        confuses provenance. Returns the number of pairs applied."""
        pairs: list[tuple[str, str]] = []
        for item in cfg.get("aliases") or []:
            alias, symbol = item.get("alias"), item.get("tradingsymbol")
            if not alias or not symbol:
                continue
            norm = " ".join(title_tokens(str(alias)))
            if not norm or norm in ALIAS_STOPLIST:
                continue
            pairs.append((norm, str(symbol)))
        self._set_aliases(
            list((a, syms) for a, syms in self._aliases.items())
            + [(a, (s,)) for a, s in pairs]
        )
        if self._store is not None and pairs:
            added_at = self._clock.now() if self._clock is not None else None
            self._store.upsert_entity_aliases(
                [{"alias": a, "tradingsymbol": s, "source": "curated", "added_at": added_at}
                 for a, s in sorted(set(pairs))]
            )
        _log.info("entity_aliases_curated", applied=len(pairs))
        return len(pairs)

    # ------------------------------------------------------------------ resolution (pinned rule)
    def resolve(self, c: NewsCluster, *, extra_texts: Sequence[str] = ()) -> ResolvedCluster:
        """Resolve one cluster deterministically. Never guesses.

        ``extra_texts`` is the analyst seam: verbatim entity STRINGS emitted by the News Analyst
        for unmatched clusters re-enter here under the same whole-word rule (the LLM never assigns
        a symbol). An extra text matching nothing is logged ``no_match``.

        EXPLICIT EXCHANGE TOKENS are honoured FIRST: a ``[NSE:<SYMBOL>]`` token in the representative
        (or an extra text) resolves to that symbol directly and is then held to the SAME universe
        check as any alias match. The token is stripped before alias matching so it is never read as
        free text, while alias matching still runs on the rest of the title. A token contributes no
        ``entities`` row: it is not an alias, and the suggestion loop must not learn one from it.
        """
        rep_text = strip_exchange_tokens(c.representative)
        token_symbols = exchange_tokens(c.representative)
        resolved, unresolved = self._match_aliases(rep_text)
        for text in extra_texts:
            tokens = exchange_tokens(text)
            r2, u2 = self._match_aliases(strip_exchange_tokens(text))
            if not r2 and not u2 and not tokens:
                unresolved.append(UnresolvedEntity(entity_text=text, reason="no_match"))
            token_symbols += [s for s in tokens if s not in token_symbols]
            resolved += r2
            unresolved += u2

        entities: set[str] = set()
        symbols: set[str] = set()
        for symbol in token_symbols:
            if self._universe is not None and symbol not in self._universe:
                # An exact exchange symbol is still not a licence to trade it.
                unresolved.append(
                    UnresolvedEntity(
                        entity_text=symbol, reason="out_of_universe", candidate_symbols=(symbol,)
                    )
                )
            else:
                symbols.add(symbol)
        for aliases_in, symbol in resolved:
            entities.update(aliases_in)
            if self._universe is not None and symbol not in self._universe:
                # Out-of-universe: recorded, never traded.
                unresolved.extend(
                    UnresolvedEntity(
                        entity_text=a, reason="out_of_universe", candidate_symbols=(symbol,)
                    )
                    for a in aliases_in
                )
            else:
                symbols.add(symbol)

        return ResolvedCluster(
            cluster_id=c.cluster_id,
            entities=sorted(entities),
            symbols=sorted(symbols),
            sectors=self._match_keywords(rep_text, self._sector_keywords),
            themes=self._match_keywords(rep_text, self._theme_keywords),
            unresolved=sorted(
                set(unresolved), key=lambda u: (u.entity_text, u.reason, u.candidate_symbols)
            ),
        )

    async def run(self, clusters: Sequence[NewsCluster]) -> list[ResolvedCluster]:
        """Store-wired resolution: resolve each cluster, persist tags onto ``news_clusters`` (carrying
        every other column through — scores are never clobbered), log unresolved entities."""
        if self._store is None:
            raise RuntimeError("EntityResolver.run requires a MarketStore (pure calls use .resolve)")
        out: list[ResolvedCluster] = []
        rows: list[dict[str, Any]] = []
        for c in clusters:
            rc = self.resolve(c)
            out.append(rc)
            updated = c.model_copy(update={
                "entities": rc.entities, "symbols": rc.symbols,
                "sectors": rc.sectors, "themes": rc.themes,
            })
            rows.append(updated.to_row())
            for u in rc.unresolved:
                await self._store.arun(
                    self._store.log_unresolved_entity,
                    u.entity_text,
                    u.reason,
                    cluster_id=c.cluster_id,
                    candidate_symbols=list(u.candidate_symbols),
                )
        if rows:
            await self._store.aupsert_news_clusters(rows)
        _log.info(
            "news_resolved",
            clusters=len(out),
            with_symbols=sum(1 for r in out if r.symbols),
            unresolved=sum(len(r.unresolved) for r in out),
        )
        return out

    # ------------------------------------------------------------------ matching internals
    def _match_aliases(
        self, text: str
    ) -> tuple[list[tuple[tuple[str, ...], str]], list[UnresolvedEntity]]:
        """Whole-word phrase alias matches over ``text`` with the ambiguity rules applied.

        Returns ``(resolved, unresolved)`` where ``resolved`` pairs the matched alias strings with
        their SINGLE unambiguous tradingsymbol. Overlapping title spans are grouped into connected
        components; a component whose symbol union is >1 (a multi-symbol alias, or different
        companies' aliases overlapping) resolves to NOTHING and every alias in it is logged
        ``ambiguous`` with the union as candidates.

        SUBSUMPTION: a match whose span is STRICTLY contained in a longer match's span is subsumed —
        the most specific phrase wins (else curating "SBI" would kill every "SBI Card" headline, and
        "PVR Inox" would refuse because the contained "Inox" poisons the component). Staggered
        partial overlaps still refuse — never a guess.
        """
        tokens = title_tokens(text)
        matches: list[tuple[int, int, str, frozenset[str]]] = []
        for alias, syms in self._aliases.items():
            alias_toks = alias.split(" ")
            n = len(alias_toks)
            for i in range(len(tokens) - n + 1):
                if tokens[i:i + n] == alias_toks:
                    matches.append((i, i + n, alias, syms))
        matches = [
            m for m in matches
            if not any(
                o[0] <= m[0] and m[1] <= o[1] and (o[1] - o[0]) > (m[1] - m[0])
                for o in matches
            )
        ]
        matches.sort(key=lambda m: (m[0], m[1], m[2]))

        components: list[list[tuple[int, int, str, frozenset[str]]]] = []
        comp_end = -1
        for m in matches:
            if components and m[0] < comp_end:
                components[-1].append(m)
                comp_end = max(comp_end, m[1])
            else:
                components.append([m])
                comp_end = m[1]

        resolved: list[tuple[tuple[str, ...], str]] = []
        unresolved: list[UnresolvedEntity] = []
        for comp in components:
            union = tuple(sorted({s for *_x, syms in comp for s in syms}))
            aliases_in = tuple(sorted({alias for _s, _e, alias, _y in comp}))
            if len(union) == 1:
                resolved.append((aliases_in, union[0]))
            else:
                unresolved.extend(
                    UnresolvedEntity(entity_text=a, reason="ambiguous", candidate_symbols=union)
                    for a in aliases_in
                )
        return resolved, unresolved

    @staticmethod
    def _contains_phrase(tokens: list[str], phrase_toks: list[str]) -> bool:
        n = len(phrase_toks)
        return any(tokens[i:i + n] == phrase_toks for i in range(len(tokens) - n + 1))

    def _match_keywords(self, text: str, keyword_map: Mapping[str, frozenset[str]]) -> list[str]:
        """Whole-word phrase keyword tagging (same rule as aliases) — sector/theme tags."""
        tokens = title_tokens(text)
        tags: set[str] = set()
        for phrase, tagset in keyword_map.items():
            if self._contains_phrase(tokens, phrase.split(" ")):
                tags.update(tagset)
        return sorted(tags)


# =========================================================================== catalyst digest
#: The analyst event types that carry the T+1 PEAD sign-agreement requirement.
EARNINGS_EVENT_TYPES: frozenset[str] = frozenset({"earnings_result", "earnings_guidance"})

#: ``cat.*`` behaviour knobs the digest needs. All but ``fanout_weight`` are parameter-envelope rows;
#: ``fanout_weight`` is settings.yaml-resident (owner-only, deliberately NOT learnable).
CAT_PARAM_KEYS: tuple[str, ...] = (
    "materiality_min", "novelty_min", "max_event_age_days", "decay_halflife_h",
    "confirm_move_pct", "stop_atr_mult", "rr_target", "fanout_weight",
)

#: `catalyst_guard` fallbacks, used ONLY for a key absent from the hash-verified block. Each
#: fails to LESS activity (empty event-type list ⇒ nothing can originate), so a truncated guard
#: block can never widen the origination surface.
_GUARD_FALLBACKS: dict[str, Any] = {
    "min_source_domains": 2,
    "sentiment_min_long": 0.30,
    "digest_stale_max_h": 20,
    "originating_event_types": (),
}

#: Inclusion floor: below this WEIGHTED materiality a cluster feeds `sentiment_agg`
#: only — no watchlist row at all (the scoring-rubric noise line).
INCLUSION_FLOOR = 0.2


class _BestCluster(NamedTuple):
    """The cluster chosen to REPRESENT a symbol in the day's ``catalyst_watchlist``.

    Two materiality values, deliberately distinct: ``rank_materiality`` is the RECENCY-DECAYED weight
    and exists ONLY to order candidates — it never reaches a threshold, a grade or a stored column.
    ``weighted_materiality`` is the undecayed ``materiality × fanout`` value that the inclusion floor,
    the :func:`originating_conditions` AND-list and the persisted ``materiality`` column all use: the
    row must describe its cluster faithfully, and origination must not narrow just because the whole
    corpus aged an hour.
    """

    cluster: NewsCluster
    rank_materiality: float
    weighted_materiality: float
    weighted_sentiment: float
    age_sessions: int


class _StoryMember(NamedTuple):
    """One age-eligible cluster's contribution to a ``(symbol, event_type)`` STORY.

    The story-level view (``story_domains`` / ``story_refs``) plus the facts :func:`reversal_source`
    needs to reason about DIRECTION over time: when the cluster was first seen, its UNDECAYED weighted
    materiality / sentiment, and whether the cluster NAMED this symbol. Materiality and sentiment
    arrive already multiplied by ``cat.fanout_weight`` for a fanned-out sector/theme cluster.

    ``named`` is False for a fan-out membership: the resolver placed the symbol in the story because
    the cluster tagged its sector/theme, not because anything resolved to the symbol itself.
    """

    cluster_id: str
    first_seen: datetime
    weighted_materiality: float
    weighted_sentiment: float
    named: bool


def reversal_source(
    members: Sequence[_StoryMember],
    *,
    winner_id: str,
    winner_first_seen: datetime,
    sentiment_min: float,
) -> str | None:
    """Did this story ESTABLISH the opposite direction before the winning cluster reversed it?

    Returns the ``cluster_id`` of the earlier, floor-clearing, OPPOSITE-direction (short) cluster the
    winner reverses, or ``None``. The caller invokes this only when the winning direction is ``long``
    (``cat_reversal``), so "opposite" always means the short side — a positive story getting denied is
    deliberately NOT detected here (an exit-side signal owned by the risk-reducing exit path).

    The four conditions, each load-bearing:

    * **``named`` — the predecessor RESOLVED to this symbol**, not reached by sector/theme fan-out.
      A sector-wide bearish cluster clears the two bars below for every constituent at once (0.6
      materiality × 0.5 fan-out = 0.30 > floor; −0.7 × 0.5 = −0.35 ≤ −0.30), so without this one
      sector headline would manufacture a "reversal" per constituent. Fan-out still corroborates and
      can WIN the row; it cannot be what was REVERSED. Applies to the PREDECESSOR only.
    * **strictly earlier ``first_seen``** — equal timestamps do NOT qualify: two clusters landing in
      the same instant are a disagreement between outlets, not a story that changed its mind. Strict
      ``<`` also excludes the winner from reversing itself.
    * **cleared :data:`INCLUSION_FLOOR` on its own UNDECAYED weighted materiality** — the bar that
      decides whether a cluster may REPRESENT a symbol at all; a sub-floor murmur must not become
      "an established bearish claim" merely by being reversed.
    * **weighted sentiment <= -``sentiment_min``** — the exact mirror of the long classification the
      row uses (``sentiment_min_long``), so "opposite direction" cannot drift from it.

    Ties break on the strongest claim (highest weighted materiality), then on ``cluster_id`` ascending
    — a total order over corpus-only facts, so replay yields the identical answer. This function
    reads NOTHING outside its arguments: no clock, no store, no LLM call.
    """
    best_id: str | None = None
    best_key: tuple[float, str] | None = None
    for m in members:
        if not m.named:
            continue
        if m.cluster_id == winner_id or m.first_seen >= winner_first_seen:
            continue
        if m.weighted_materiality < INCLUSION_FLOOR:
            continue
        if m.weighted_sentiment > -sentiment_min:
            continue
        key = (-m.weighted_materiality, m.cluster_id)
        if best_key is None or key < best_key:
            best_key, best_id = key, m.cluster_id
    return best_id


#: ``earnings_calendar.kind`` marking a results day T (no-entry day + the reaction bar).
#: ``EarningsCalendarJob.classify_event`` already folds results-considering board meetings into it.
_RESULTS_KIND = "results"

_ATR_PERIOD = 14
#: ``bars_1d`` rows needed strictly before ``d`` for levels: the Wilder ATR seed lands at index
#: ``period − 1``, so 15 rows give a seeded value plus one recursion step. Fewer ⇒ levels NULL.
_MIN_LEVEL_BARS = 15
#: Calendar span pulled for the per-symbol daily history (≈60 sessions ≫ _MIN_LEVEL_BARS, and wide
#: enough to contain the results-day-T bar, which lies inside the cluster lookback window).
_HISTORY_LOOKBACK_DAYS = 90
#: Bound on the day-at-a-time calendar walks (a missing calendar year must not spin).
_MAX_CALENDAR_SCAN_DAYS = 60

_PAISE = Decimal("0.01")
_HUNDRED = Decimal("100")


def load_cat_params(envelope: Mapping[str, Any] | None = None) -> dict[str, float]:
    """``cat.*`` digest params: ``config/envelope.yaml`` defaults + settings.yaml ``cat``, overridden
    by ``envelope`` (namespaced ``cat.rr_target`` and bare ``rr_target`` both resolve; every other
    strategy's keys are ignored, so the live ``envelope_state`` mapping can be passed whole).

    The bounds file is read UNVERIFIED here because only its DEFAULTS are used — every value that
    can license a trade comes from the caller's ``envelope_state`` row or from the hash-verified
    ``catalyst_guard`` block, never from this read.
    """
    cfg = config_dir()
    params: dict[str, float] = {
        name[len("cat."):]: float(spec["default"])
        for name, spec in (load_yaml(cfg / "envelope.yaml").get("parameters") or {}).items()
        if name.startswith("cat.") and isinstance(spec, Mapping)
    }
    settings_cat = load_yaml(cfg / "settings.yaml").get("cat") or {}
    params["fanout_weight"] = float(settings_cat.get("fanout_weight", CatCfg().fanout_weight))
    for key, value in (envelope or {}).items():
        bare = key[len("cat."):] if key.startswith("cat.") else key
        if bare in CAT_PARAM_KEYS:
            params[bare] = float(value)
    missing = [k for k in CAT_PARAM_KEYS if k not in params]
    if missing:
        raise KeyError(f"cat params missing from envelope.yaml/settings.yaml and overrides: {missing}")
    return params


def guard_value(guard: Mapping[str, Any], key: str) -> Any:
    """One ``catalyst_guard`` value, falling back to the pinned default (:data:`_GUARD_FALLBACKS`)."""
    value = guard.get(key)
    return _GUARD_FALLBACKS[key] if value is None else value


def originating_conditions(
    *,
    weighted_materiality: float,
    weighted_sentiment: float,
    event_type: str | None,
    source_domain_count: int,
    novelty: float | None,
    in_universe: bool,
    flagged: bool,
    results_day_t: bool,
    earnings_reaction_agrees: bool,
    materiality_min: float,
    novelty_min: float,
    guard: Mapping[str, Any],
) -> dict[str, bool]:
    """The watchlist AND-list, one boolean per condition — ``originating`` iff ALL are True.

    Pure: every argument is already-gathered evidence, so each condition is unit-testable in
    isolation and the store reads stay out of the rule. ``guard`` is the hash-verified
    ``catalyst_guard`` block. Materiality/sentiment arrive ALREADY weighted by
    ``cat.fanout_weight`` for a fanned-out sector/theme cluster (the weight multiplies both
    BEFORE the comparison). Short-direction candidates fail ``sentiment_long`` by construction —
    the shorts gate, not a separate rule. ``source_domain_count`` is the STORY-level
    union: distinct domains across all age-eligible clusters for the same
    ``(symbol, event_type)``, gathered by the digest — not one cluster's own set.
    """
    allowed = tuple(guard_value(guard, "originating_event_types") or ())
    return {
        "materiality": weighted_materiality >= materiality_min,
        "sentiment_long": weighted_sentiment >= float(guard_value(guard, "sentiment_min_long")),
        "event_type": event_type is not None and event_type in allowed,
        "source_domains": source_domain_count >= int(guard_value(guard, "min_source_domains")),
        "novelty": novelty is not None and novelty >= novelty_min,
        "in_universe": in_universe,
        "not_flagged": not flagged,
        "not_results_day": not results_day_t,
        "earnings_reaction": earnings_reaction_agrees,
    }


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, value))


def _age_hours(ran_at: datetime, first_seen: datetime) -> float:
    """Hours from ``first_seen`` to the run's single fixed ``ran_at``, CLAMPED AT ZERO.

    The clamp is the point: a future ``first_seen`` (clock skew, a bad feed timestamp) must be able to
    fade a contribution, never AMPLIFY it beyond its unaged weight — a negative age would make
    :func:`_decay` return a factor above 1.
    """
    return max(0.0, (ran_at - first_seen).total_seconds() / 3600.0)


def _decay(age_h: float, halflife: float) -> float:
    """Recency decay: ``0.5 ^ (age_h / cat.decay_halflife_h)``.

    ONE definition for both consumers, so ``sentiment_agg`` and the watchlist's best-cluster ranking
    age a story on the same clock; a second copy of this exponent would let the two outputs disagree
    about which news is current.

    Only one caller bounds it: the ``sentiment_agg`` leg is an additive SUM and drops clusters past
    ``6 * halflife`` (<1.6% contribution), while the ranking is a pure comparison key where a cutoff
    would delete a symbol's row rather than down-weight it. The asymmetry is deliberate.
    """
    return 0.5 ** (age_h / halflife)


def _paise(value: Decimal) -> Decimal:
    """Quantize to the DECIMAL(12,2) column scale. NO tick snapping here: the watchlist stores raw
    deterministic levels; the live scanner is what publishes tick-legal order prices."""
    return value.quantize(_PAISE, rounding=ROUND_HALF_UP)


def _reaction_agrees(bars: Sequence[DailyBar], t_day: date, sentiment: float) -> bool:
    """PEAD agreement: ``sign(cluster sentiment) == sign(close_T − open_T)`` from ``bars_1d``.

    ``close_T == open_T`` ⇒ reaction sign 0 ⇒ never agrees (conservative). A MISSING T bar is
    treated identically: an unverifiable reaction can never license origination.
    """
    bar = next((b for b in bars if b.d == t_day), None)
    if bar is None or sentiment == 0:
        return False
    reaction = bar.close - bar.open
    if reaction == 0:
        return False
    return (reaction > 0) == (sentiment > 0)


class CatalystDigestResult(BaseModel):
    """One digest run's outcome. ``(0, 0)`` is a SUCCESS — an empty-but-FRESH digest
    means `cat` simply originates nothing; only STALE/MISSING disables it."""

    model_config = ConfigDict(frozen=True)

    d: date
    n_originating: int
    n_context: int
    sentiment_rows: int
    ran_at: datetime


class CatalystDigestJob:
    """The catalyst digest (~08:35, before the 08:50 planner; idempotent run-latest catch-up).

    Two outputs, both deterministic and both re-derivable from persisted scores (no LLM):

    (i) ``sentiment_agg`` — ``clip(Σᵢ sentimentᵢ·materialityᵢ·wᵢ·0.5^(age_hᵢ/half-life), −1, +1)`` per
    ``(scope, scope_key)`` over scored clusters with ``age_h ≤ 6×`` half-life, age measured from
    cluster ``first_seen`` to the run time. A decayed SUM, never a mean. Each row also carries the
    UNCLIPPED ``raw_sum`` and the contributing ``n_clusters``, since the clip is lossy.

    (ii) ``catalyst_watchlist`` — every scored cluster whose TRADING-SESSION event age ≤
    ``cat.max_event_age_days``, best cluster per symbol, graded by :func:`originating_conditions`
    with levels on ``originating`` rows only. "Best" is the highest materiality after the
    SAME ``0.5^(age_h/cat.decay_halflife_h)`` decay (ii) shares with (i) — a ranking key only
    (see :class:`_BestCluster`), so the two outputs always agree on which news is current.

    Parameters
    ----------
    store / clock / calendar:
        DuckDB access (all reads/writes offloaded via ``arun``, convention 12), the single "now",
        and the weekend/holiday-aware session arithmetic.
    protected_store:
        Loads ``limits.yaml`` HASH-VERIFIED at run time — the ``catalyst_guard`` block is never
        accepted as a constructor argument (the enforcement site verifies it). A
        verification failure propagates: an unverifiable anti-manipulation surface must yield NO
        digest (which disables `cat` for the day), never a permissive one.
    envelope:
        Live ``envelope_state`` values; see :func:`load_cat_params` for defaults + key forms.

    Spec ambiguities resolved here (documented for the integrator):

    - ``market``-scope clusters contribute to the ``market`` row ONLY — never a symbol row and
      never a watchlist row (market news feeds regime context, "**never** origination").
    - Sector/theme fan-out consumes the RESOLVER's tags (``sectors``/``themes``) and is intersected
      with today's BATCH universe when one exists; with no universe row yet the raw constituents are
      used and the ``in_universe`` condition still blocks origination.
    - The ``market``/``market`` row is written on EVERY run (0.0 when no market cluster scored), so
      an empty-but-fresh digest is distinguishable from a missing one (the fail-safe ladder needs it).
    - The row's ``materiality`` is the WEIGHTED, UNDECAYED value (what the grade decision used);
      the recency decay orders candidates and is never stored. ``event_age_h`` is informational,
      ``event_age_sessions`` is the eligibility clock.
    - A cluster below :data:`INCLUSION_FLOOR` can corroborate a story but never REPRESENT a symbol:
      the floor gates candidacy, not the finished row, so decayed ranking cannot let a fresh
      sub-floor mention take the slot and delete a still-material story's row. A symbol carries a
      row iff some cluster targeting it clears the floor — unchanged by the decay.
    - ``source_domain_count`` is the STORY-level union: distinct domains across ALL age-eligible
      clusters targeting the same ``(symbol, event_type)`` — cross-outlet paraphrase never merges
      under the pinned similarity, so cluster-level counting is structurally unpassable.
      ``cluster_refs`` = best cluster first + every corroborating cluster id.
    - ``expires_at`` is the first trading day the event EXCEEDS the age horizon (session age
      ``max_event_age_days + 1``) — i.e. the first digest day it no longer qualifies.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        calendar: NSECalendar,
        protected_store: ProtectedStore,
        envelope: Mapping[str, Any] | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._protected = protected_store
        self._params = load_cat_params(envelope)

    # ------------------------------------------------------------------ session arithmetic
    def event_age_sessions(self, first_seen: datetime, d: date) -> int:
        """Watchlist event age: TRADING DAYS in ``(first_seen date, d]``.

        The digest runs pre-open, so ``d`` is the first session the event can be traded into and
        counts as age 1 — a Friday-evening or weekend event is therefore age 1 at Monday's digest
        (which keeps Friday reporters T+1-PEAD-eligible), and a same-day event is age 0.
        """
        start = first_seen.astimezone(IST).date()
        probe, age = start + timedelta(days=1), 0
        while probe <= d:
            if self._calendar.is_trading_day(probe):
                age += 1
            probe += timedelta(days=1)
        return age

    def _oldest_eligible_date(self, d: date, max_days: int) -> date:
        """Earliest ``first_seen`` DATE whose session age at ``d`` can still be ≤ ``max_days``."""
        probe, remaining = d, max_days
        for _ in range(_MAX_CALENDAR_SCAN_DAYS):
            if remaining <= 0:
                break
            probe -= timedelta(days=1)
            if self._calendar.is_trading_day(probe):
                remaining -= 1
        return probe

    def _expires_at(self, first_seen: datetime, max_days: int) -> date:
        """The trading day the event EXCEEDS ``cat.max_event_age_days`` (session age max+1)."""
        probe = first_seen.astimezone(IST).date()
        for _ in range(max_days + 1):
            probe = self._calendar.next_trading_day(probe)
        return probe

    # ------------------------------------------------------------------ guard
    def _guard(self) -> dict[str, Any]:
        """The ``catalyst_guard`` block from the HASH-VERIFIED ``limits.yaml`` — loaded per run, at
        the enforcement site, exactly like the gate loads its limits."""
        limits = self._protected.load_verified("limits.yaml")
        guard = (limits.get("limits") or {}).get("catalyst_guard")
        if not isinstance(guard, Mapping):
            raise ValueError("limits.yaml has no `limits.catalyst_guard` block (owner-only surface)")
        return dict(guard)

    # ------------------------------------------------------------------ run
    async def run(self, d: date) -> CatalystDigestResult:
        ran_at = self._clock.now()
        guard = self._guard()
        max_days = int(self._params["max_event_age_days"])
        halflife = float(self._params["decay_halflife_h"])

        # One lookback covering BOTH computations: the sentiment decay horizon (6× half-life) and
        # the session-age horizon expanded to calendar days (weekends/holidays included).
        window_start = min(
            ran_at - timedelta(hours=6.0 * halflife),
            self._clock.combine(self._oldest_eligible_date(d, max_days), time(0, 0)),
        )
        rows = await self._store.arun(
            self._store.get_news_clusters, scored=True, last_seen_after=window_start
        )
        clusters = [
            NewsCluster.from_row(r)
            for r in rows
            if r.get("sentiment") is not None and r.get("materiality") is not None
        ]
        sector_symbols = _reverse_sector_map(await self._store.arun(self._store.get_sector_map, as_of=d))
        theme_symbols = {
            r["theme"]: set(r["symbols"] or [])
            for r in await self._store.arun(self._store.get_theme_map)
        }
        # The digest grades over the BATCH universe, so in_universe covers criteria-passing non-index
        # names — shadow-only by construction (both cat legs fail closed at the cost gate).
        universe = set(await self._store.arun(self._store.get_batch_universe_symbols, d))

        sentiment_rows = self._sentiment_rows(clusters, ran_at, sector_symbols, theme_symbols, universe)
        await self._store.arun(self._store.upsert_sentiment_agg, sentiment_rows)

        watch_rows = await self._watchlist_rows(
            d, clusters, ran_at, guard, sector_symbols, theme_symbols, universe,
            earnings_from=window_start.date(),
        )
        await self._store.arun(self._store.replace_catalyst_watchlist, d, watch_rows)

        n_originating = sum(1 for r in watch_rows if r["grade"] == "originating")
        result = CatalystDigestResult(
            d=d,
            n_originating=n_originating,
            n_context=len(watch_rows) - n_originating,
            sentiment_rows=len(sentiment_rows),
            ran_at=ran_at,
        )
        _log.info(
            "catalyst_digest",
            d=str(d),
            clusters=len(clusters),
            sentiment_rows=result.sentiment_rows,
            n_originating=result.n_originating,
            n_context=result.n_context,
        )
        return result

    def digest_status(self, d: date) -> Literal["fresh", "stale", "missing"]:
        """Fail-safe input for features-v2 + the ``CATALYST_DISABLED`` alert.

        ``missing`` = no digest artifact at all (no ``sentiment_agg`` stamp, no ``d`` watchlist);
        ``stale`` = the latest stamp is older than ``catalyst_guard.digest_stale_max_h`` (or day
        ``d`` has rows with no stamp at all — an age that cannot be established is never "fresh").
        An EMPTY digest is ``fresh``: only stale/missing disables `cat`.
        """
        as_of = self._store.latest_sentiment_as_of()
        if as_of is None:
            return "missing" if not self._store.get_catalyst_watchlist(d) else "stale"
        age_h = (self._clock.now() - as_of).total_seconds() / 3600.0
        return "stale" if age_h > float(guard_value(self._guard(), "digest_stale_max_h")) else "fresh"

    # ------------------------------------------------------------------ sentiment_agg
    def _sentiment_rows(
        self,
        clusters: Sequence[NewsCluster],
        ran_at: datetime,
        sector_symbols: Mapping[str, set[str]],
        theme_symbols: Mapping[str, set[str]],
        universe: set[str],
    ) -> list[dict[str, Any]]:
        halflife = float(self._params["decay_halflife_h"])
        fanout = float(self._params["fanout_weight"])
        totals: dict[tuple[str, str], float] = defaultdict(float)
        # Contributing clusters per (scope, key), in lockstep with ``totals``: with the unclipped sum
        # this measures SATURATION ("−1.000" alone cannot tell two mild headlines from nine severe).
        counts: dict[tuple[str, str], int] = defaultdict(int)
        for c in clusters:
            age_h = _age_hours(ran_at, c.first_seen)
            # Past six half-lives a cluster contributes under 1.6% of its weight; the cutoff is
            # meaningful because this is an additive total (the watchlist ranking has none; see _decay).
            if age_h > 6.0 * halflife:
                continue
            base = float(c.sentiment) * float(c.materiality) * _decay(age_h, halflife)
            w = fanout if c.scope in ("sector", "theme") else 1.0
            for symbol in self._symbol_targets(c, sector_symbols, theme_symbols, universe):
                totals[("symbol", symbol)] += base * w
                counts[("symbol", symbol)] += 1
            for sector in c.sectors or []:
                totals[("sector", sector)] += base
                counts[("sector", sector)] += 1
            for theme in c.themes or []:
                totals[("theme", theme)] += base
                counts[("theme", theme)] += 1
            if c.scope == "market":
                totals[("market", "market")] += base
                counts[("market", "market")] += 1
        totals.setdefault(("market", "market"), 0.0)   # digest-ran marker (see class docstring)
        counts.setdefault(("market", "market"), 0)     # …and the marker contributes no cluster
        return [
            {
                "scope": scope, "scope_key": key, "as_of": ran_at,
                "value": _clip(total),                 # the formula's clipped value, unchanged
                "raw_sum": total,                      # …and what it was clipped FROM
                "n_clusters": counts[(scope, key)],
            }
            for (scope, key), total in sorted(totals.items())
        ]

    def _symbol_targets(
        self,
        c: NewsCluster,
        sector_symbols: Mapping[str, set[str]],
        theme_symbols: Mapping[str, set[str]],
        universe: set[str],
    ) -> set[str]:
        """Symbols a cluster contributes to: its resolved ``symbols`` plus, for a sector/theme
        cluster, the fan-out constituents of the RESOLVER's tags (the LLM never assigns a symbol)."""
        if c.scope == "market":
            return set()
        symbols = set(c.symbols or [])
        if c.scope in ("sector", "theme"):
            tags = (c.sectors or []) if c.scope == "sector" else (c.themes or [])
            source = sector_symbols if c.scope == "sector" else theme_symbols
            fan: set[str] = set()
            for tag in tags:
                fan |= source.get(tag, set())
            symbols |= (fan & universe) if universe else fan
        return symbols

    # ------------------------------------------------------------------ watchlist
    async def _watchlist_rows(
        self,
        d: date,
        clusters: Sequence[NewsCluster],
        ran_at: datetime,
        guard: Mapping[str, Any],
        sector_symbols: Mapping[str, set[str]],
        theme_symbols: Mapping[str, set[str]],
        universe: set[str],
        *,
        earnings_from: date,
    ) -> list[dict[str, Any]]:
        p = self._params
        max_days = int(p["max_event_age_days"])
        fanout = float(p["fanout_weight"])
        # Same envelope knob `sentiment_agg` decays with: the ranking must age a story on the SAME
        # clock as the aggregate (see the ranking comment below).
        halflife = float(p["decay_halflife_h"])
        sentiment_min = float(guard_value(guard, "sentiment_min_long"))

        # PRIOR session's bulk/block-deal flags: the ~08:35 digest runs before the 20:30 deals job
        # writes day-``d`` rows, so a ``d`` read is structurally empty. Same semantics as the scan
        # context: a deal on the last completed session flags the symbol today.
        try:
            flagged_day: date | None = self._calendar.previous_trading_day(d)
        except ValueError:       # no prior session resolvable (calendar hole) — no flags, no error
            flagged_day = None
        flagged = (
            {r["symbol"]
             for r in await self._store.arun(self._store.get_flagged_instrument_days, flagged_day)}
            if flagged_day is not None else set()
        )
        results_days: dict[str, set[date]] = defaultdict(set)
        for r in await self._store.arun(self._store.get_earnings_calendar, earnings_from, d):
            if r.get("kind") == _RESULTS_KIND:
                results_days[r["symbol"]].add(r["event_date"])

        # Best cluster per symbol = highest RECENCY-DECAYED weighted materiality; ties break on
        # cluster_id (determinism: decay is a pure function of first_seen against the run's single
        # fixed ``ran_at``). Ranking on RAW materiality would let a stale cluster hold the slot
        # indefinitely while `sentiment_agg` (which decays) disagreed; decaying the key lets a fresher
        # cluster overtake it with no "is this a denial of that" detection (at the 24h default
        # half-life a same-day follow-up outranks a day-old cluster of up to ~2× its materiality).
        #
        # This path deliberately does NOT take the `6 * halflife` cutoff from the `sentiment_agg`
        # leg: here the decayed value is a pure ORDERING key, and a cutoff would drop the cluster
        # from the corroboration and reversal maps below, or delete a symbol's row outright when its
        # one floor-clearing cluster is old. Eligibility is the SESSION bound (`max_event_age_days`,
        # applied above); ranking needs no second, hour-based bound.
        #
        # The same pass builds the STORY-level corroboration maps, deliberately upstream of every
        # filter below: cross-outlet paraphrase never merges under the pinned similarity (0/1,400 live
        # cross-feed pairs ≥ 0.75), so the min_source_domains count is the union of domains across ALL
        # age-eligible clusters targeting the same (symbol, event_type) — an event_type disagreement
        # between outlets loses the corroboration (fails to LESS activity, never false-corroboration).
        best: dict[str, _BestCluster] = {}
        story_domains: dict[tuple[str, str | None], set[str]] = defaultdict(set)
        story_refs: dict[tuple[str, str | None], set[str]] = defaultdict(set)
        # Same story grouping, keeping the DIRECTION-over-time facts `reversal_source` needs. Built in
        # this pass from the same corpus, so the verdict stays a pure function of (clusters, ran_at).
        story_members: dict[tuple[str, str | None], list[_StoryMember]] = defaultdict(list)
        for c in clusters:
            age_sessions = self.event_age_sessions(c.first_seen, d)
            if age_sessions > max_days:
                continue
            w = fanout if c.scope in ("sector", "theme") else 1.0
            weighted_materiality = float(c.materiality) * w
            weighted_sentiment = float(c.sentiment) * w
            rank_materiality = weighted_materiality * _decay(_age_hours(ran_at, c.first_seen), halflife)
            named = set(c.symbols or ())        # resolved to the symbol vs reached by fan-out
            for symbol in self._symbol_targets(c, sector_symbols, theme_symbols, universe):
                story_domains[(symbol, c.event_type)].update(c.source_domains or [])
                story_refs[(symbol, c.event_type)].add(c.cluster_id)
                story_members[(symbol, c.event_type)].append(
                    _StoryMember(
                        cluster_id=c.cluster_id,
                        first_seen=c.first_seen,
                        weighted_materiality=weighted_materiality,
                        weighted_sentiment=weighted_sentiment,
                        named=symbol in named,
                    )
                )
                if weighted_materiality < INCLUSION_FLOOR:
                    # Noise-line clusters corroborate (above) but can never REPRESENT a symbol. The
                    # floor is tested on the UNDECAYED value and gates CANDIDACY, not the finished
                    # row: a symbol has a row iff some cluster of its own clears the floor. Testing
                    # the decayed value, or placing the floor after the ranking, would let a fresh
                    # sub-floor mention win the slot and delete a still-material story's row.
                    continue
                current = best.get(symbol)
                if current is None or (-rank_materiality, c.cluster_id) < (
                    -current.rank_materiality, current.cluster.cluster_id
                ):
                    best[symbol] = _BestCluster(
                        cluster=c,
                        rank_materiality=rank_materiality,
                        weighted_materiality=weighted_materiality,
                        weighted_sentiment=weighted_sentiment,
                        age_sessions=age_sessions,
                    )

        rows: list[dict[str, Any]] = []
        for symbol in sorted(best):
            # From here down uses the UNDECAYED weighted materiality: decay only chose WHICH cluster
            # speaks for the symbol (the inclusion floor already bound at candidacy).
            c, _rank, weighted_materiality, weighted_sentiment, age_sessions = best[symbol]
            symbol_results = results_days.get(symbol, set())
            t_day = max((t for t in symbol_results if t < d), default=None)
            history: list[DailyBar] | None = None
            reaction_agrees = True             # vacuous unless this IS an earnings event with a T
            if c.event_type in EARNINGS_EVENT_TYPES and t_day is not None:
                history = await self._history(symbol, d)
                reaction_agrees = _reaction_agrees(history, t_day, weighted_sentiment)
            # Story-level corroboration: the guard input is the (symbol, event_type) domain union,
            # not this cluster's own (normally single-outlet) set.
            corroboration = story_domains.get((symbol, c.event_type), set()) | set(c.source_domains or [])
            conditions = originating_conditions(
                weighted_materiality=weighted_materiality,
                weighted_sentiment=weighted_sentiment,
                event_type=c.event_type,
                source_domain_count=len(corroboration),
                novelty=c.novelty,
                in_universe=symbol in universe,
                flagged=symbol in flagged,
                results_day_t=d in symbol_results,
                earnings_reaction_agrees=reaction_agrees,
                materiality_min=float(p["materiality_min"]),
                novelty_min=float(p["novelty_min"]),
                guard=guard,
            )
            originating = all(conditions.values())
            levels: dict[str, Decimal] = {}
            if originating:
                if history is None:
                    history = await self._history(symbol, d)
                levels = self._levels(history)
                if not levels:
                    # Never blocks the grade: these v1 levels are audit-only (no reader, see
                    # _levels), and a thin-history symbol is caught by warmup_ready anyway.
                    _log.warning(
                        "catalyst_levels_unavailable", symbol=symbol, d=str(d), bars=len(history)
                    )
            direction = (
                "long" if weighted_sentiment >= sentiment_min
                else "short" if weighted_sentiment <= -sentiment_min
                else None
            )
            # `cat_reversal`: this story previously ESTABLISHED the opposite (short) direction and the
            # winning cluster reverses it — a resolved-uncertainty relief setup. LONG-ONLY by
            # construction (a positive story being denied is the exit side). Purely additive: no
            # existing field, grade, level or condition reads it, so `cat` v2's event definition is
            # untouched.
            reversal_of = (
                reversal_source(
                    story_members.get((symbol, c.event_type), ()),
                    winner_id=c.cluster_id,
                    winner_first_seen=c.first_seen,
                    sentiment_min=sentiment_min,
                )
                if direction == "long" else None
            )
            rows.append({
                "entry_id": str(ULID()),
                "symbol": symbol,
                "grade": "originating" if originating else "context",
                "direction": direction,
                "event_type": c.event_type,
                # Best cluster FIRST, then every corroborating cluster id (audit trail).
                "cluster_refs": [c.cluster_id] + sorted(
                    story_refs.get((symbol, c.event_type), set()) - {c.cluster_id}
                ),
                "materiality": weighted_materiality,
                "source_domain_count": len(corroboration),
                "event_age_h": (ran_at - c.first_seen).total_seconds() / 3600.0,
                "event_age_sessions": age_sessions,
                "expires_at": self._expires_at(c.first_seen, max_days),
                # NULL on every non-reversal row — the `cat_reversal` scanner's ONLY extra input.
                "reversal_of": reversal_of,
                **levels,
            })
        return rows

    async def _history(self, symbol: str, d: date) -> list[DailyBar]:
        """``bars_1d`` strictly BEFORE ``d`` — the digest is pre-open, day ``d`` has no bar yet."""
        return await self._store.arun(
            self._store.get_bars_1d,
            symbol,
            d - timedelta(days=_HISTORY_LOOKBACK_DAYS),
            d - timedelta(days=1),
        )

    def _levels(self, bars: Sequence[DailyBar]) -> dict[str, Decimal]:
        """v1 reference levels from bars strictly before ``d``; ``{}`` on thin history.

        These come from the retired v1 cat confirmation design (``confirm_trigger = prior_close ×
        (1 + cat.confirm_move_pct/100)``, an ATR stop, an ``rr_target`` target; ``invalidation =
        prior_close``; bands collapsed to a point). They are persisted for the stored
        ``catalyst_watchlist`` schema and the audit trail ONLY: nothing reads them (cat v2 enters at
        the reference close with a ``stop_pct`` disaster stop and no target; the API still passes
        the row through). ``cat.confirm_move_pct``, ``cat.stop_atr_mult`` and ``cat.rr_target`` still
        feed this computation (via ``self._params``).
        """
        if len(bars) < _MIN_LEVEL_BARS:
            return {}
        atr = wilder_atr(
            [b.high for b in bars], [b.low for b in bars], [b.close for b in bars], _ATR_PERIOD
        )
        latest = float(atr.iloc[-1])
        if not math.isfinite(latest) or latest <= 0:
            return {}
        prior_close = bars[-1].close
        # Quantize the trigger FIRST: stop/target are anchored to the PUBLISHED trigger, so the
        # persisted levels satisfy the strategy-rule arithmetic exactly as stored (no residual drift).
        trigger = _paise(prior_close * (Decimal(1) + Decimal(str(self._params["confirm_move_pct"])) / _HUNDRED))
        stop = _paise(trigger - Decimal(str(self._params["stop_atr_mult"])) * Decimal(str(latest)))
        target = _paise(trigger + Decimal(str(self._params["rr_target"])) * (trigger - stop))
        return {
            "confirm_trigger": trigger,
            "invalidation": _paise(prior_close),
            "stop_band_low": stop,
            "stop_band_high": stop,
            "target_band_low": target,
            "target_band_high": target,
        }


def _reverse_sector_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, set[str]]:
    """``sector_map`` rows → ``sector -> {symbols}`` (the sector fan-out constituents)."""
    out: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        out[row["sector"]].add(row["symbol"])
    return dict(out)
