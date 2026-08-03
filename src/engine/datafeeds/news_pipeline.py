"""Deterministic news pipeline, §2.7 steps 2–3 + 5: ``HeadlineClusterer`` + ``EntityResolver``
(§3.2.4) and the ``CatalystDigestJob`` (§3.2.4 / §4.4 job 14).

Every algorithm here is PINNED by the plan — cluster membership drives the §7.1
``catalyst_guard.min_source_domains`` corroboration count and the resolver's symbols feed the
catalyst watchlist, so there is zero implementation latitude and NO LLM anywhere in this module:

- **HeadlineClusterer** (§2.7 step 2): normalize a title to its sorted set of unique lowercase
  alphanumeric tokens; ``similarity(a, b) = difflib.SequenceMatcher(None, " ".join(tokens_a),
  " ".join(tokens_b)).ratio()``; process headlines in ``published_at`` order, assigning each greedily
  to the EARLIEST-``first_seen`` existing cluster whose REPRESENTATIVE headline scores ≥
  ``news.cluster_sim_threshold`` (default 0.75), considering only clusters with ``last_seen`` inside
  the ``cat.max_event_age_days`` window; no match ⇒ a new cluster with this headline as
  representative. A cluster carries its DISTINCT ``source_domains`` set. Deterministic and
  golden-file unit-tested (§9.1): same headlines in ⇒ same clusters out.
- **EntityResolver** (§2.7 step 3): case-insensitive WHOLE-WORD PHRASE containment of the normalized
  alias in the normalized title. Alias seed = instruments-dump company names with legal suffixes
  stripped MINUS a curated common-English-word stoplist. AMBIGUOUS (an alias mapping to >1 distinct
  tradingsymbol, OR different companies' aliases matching overlapping title spans) ⇒ NO match, never
  a guess — logged to ``unresolved_entities`` (weekly suggestion loop, §5.5). Out-of-universe
  entities are recorded, never traded. Sector/theme tags via ``sector_map`` + ``theme_map`` keyword
  match (same whole-word rule).
- **CatalystDigestJob** (§2.7 step 5): the two pinned digest outputs — ``sentiment_agg`` (a clipped
  decay-weighted SUM, never a mean) and the day's ``catalyst_watchlist`` (grades + deterministic
  §6.1 levels). It consumes the step-4 scores through the persisted ``news_clusters`` columns only,
  so replay never re-invokes the LLM (R8/§9.6), and loads the ``catalyst_guard`` block
  hash-verified at run time (§2.4 item 1) — the enforcement site, never a constructor argument.
  Its own resolved ambiguities are listed on the class, not here.

Spec ambiguities resolved here (documented for the integrator):

- The clusterer's ``cat.max_event_age_days`` eligibility window is measured in CALENDAR days
  relative to the headline being processed (``last_seen ≥ published_at − window``) — the
  trading-SESSION age count applies to the §2.7 step-5 digest, not to clustering, and a
  calendar-window clusterer stays deterministic without an ``NSECalendar`` dependency.
- ``cluster_id`` is derived from the joining headline's platform-assigned ULID
  (``"c-" + headline_id``) so the algorithm itself is a pure function of its input (§9.1
  golden-file byte-identity) — no ULID minted here.
- The resolver matches against the cluster's REPRESENTATIVE headline (the title the §3.2.4 rule
  normalizes); resolver normalization preserves token ORDER (phrase containment), unlike the
  clusterer's sorted-set normalization.

Phase-2 pointer (deliberately NOT stubbed here, per plan): §2.7 step 4 — News Analyst scoring
(§5.4, Tier-1 LLM, scores persisted per CLUSTER) — belongs to ``engine.intelligence``. The News
Analyst's verbatim entity strings for unmatched clusters re-enter :meth:`EntityResolver.resolve`
via ``extra_texts`` — the LLM never assigns a symbol directly (§3.2.4).

Failure model: this module is deterministic CPU-bound work over already-ingested rows; the async
``run`` wrappers offload DuckDB access through ``MarketStore`` (convention 12) and are scheduled by
the §4.4 job-10 pipeline, which is never load-bearing (E5).
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from difflib import SequenceMatcher
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator
from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import CatCfg, config_dir, load_yaml
from engine.core.log import get_logger
from engine.core.protected_store import ProtectedStore
from engine.datafeeds.news import Headline
from engine.marketdata.store import DailyBar, MarketStore
from engine.strategy.indicators import wilder_atr

_log = get_logger("engine.datafeeds.news_pipeline")

_TOKEN_RE = re.compile(r"[a-z0-9]+")

#: §3.2.4 pinned legal-suffix list for the alias seed, as NORMALIZED TOKENS. "&" is non-alphanumeric
#: and vanishes in tokenization, so the pinned "& CO" suffix is the trailing token "co" here.
LEGAL_SUFFIX_TOKENS: frozenset[str] = frozenset({
    "ltd", "limited", "pvt", "private", "co", "corp", "corporation",
    "india", "industries", "enterprises",
})

#: Curated stoplist of seed aliases that are common English words — they would false-positive on
#: ordinary headline text, so they are EXCLUDED from the alias seed (§3.2.4: TRENT/IDEA/…).
#: TODO(owner review, Phase 1): the owner reviews and extends this list once against the real
#: NIFTY200 instruments dump; additions beyond it arrive only via the §5.5 owner-approval loop.
ALIAS_STOPLIST: frozenset[str] = frozenset({
    "trent",      # Trent Ltd
    "idea",       # (curated) Vodafone Idea aliases
    "coal",       # Coal India Ltd -> "coal" after suffix strip
    "oil",        # Oil India Ltd -> "oil" after suffix strip
    "page",       # Page Industries Ltd -> "page"
    "escorts",    # Escorts Kubota / Escorts Ltd
    "lupin",      # Lupin Ltd (common noun)
    "orient",     # Orient Electric / Orient Cement
    "century",    # Century Textiles / Century Plyboards
    "campus",     # Campus Activewear Ltd
    "united",     # United Spirits / United Breweries partial forms
    "bse",        # BSE Ltd — fires on VENUE mentions ("IPO set for BSE SME debut", quote-page
                  # boilerplate); G1 owner verdict 2026-08-03 rows 17/21. Genuine BSE-the-company
                  # coverage is the accepted recall cost (venue noise dominates).
})

#: Sector label that must never become a keyword tag (§4.4 job 13 fallback bucket).
_UNCLASSIFIED = "UNCLASSIFIED"

#: The pinned §4.3 ``news_clusters`` columns (mirrors MarketStore._TABLE_SPEC — unknown keys are a
#: hard error there, so this tuple is validated on every upsert).
_CLUSTER_ROW_FIELDS: tuple[str, ...] = (
    "cluster_id", "representative", "source_domains", "first_seen", "last_seen", "scope",
    "entities", "symbols", "sectors", "themes", "sentiment", "materiality", "event_type",
    "novelty", "scored_at", "scorer_model", "untrusted",
)


def title_tokens(text: str) -> list[str]:
    """Lowercase alphanumeric tokens IN ORDER — the resolver's whole-word normalization (§3.2.4)."""
    return _TOKEN_RE.findall(text.lower())


#: §3.2.4 boilerplate strip (2026-08-03, plan-amended): recurring TEMPLATE phrases in Indian
#: financial-press headlines. ET emits dozens of near-identical "<Company> Share Price Live
#: Updates: ..." titles a day; with the template tokens included, the boilerplate dominated the
#: token-set similarity and glued DIFFERENT companies into one cluster (worst observed live: 27
#: companies in a single cluster — G1 sample finding). Phrases are removed as whole-word token
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
    # Earnings-template vocabulary (2026-08-03, G1 seed-6): same-day "<Company> Q1 Results:
    # Profit rises N% YoY to Rs M crore" headlines from DIFFERENT companies shared enough
    # template tokens to clear the 0.75 similarity bar (3 live cross-company merges: Maruti+CDSL,
    # TataSteel+SunPharma, Infosys+TataConsumer). Similarity must run on the DISTINCTIVE tokens
    # (company name + figures), so the template predicates are stripped — clusterer-only; the
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
    """§3.2.4 pinned clusterer normalization: boilerplate-phrase strip (see
    :data:`CLUSTERER_BOILERPLATE_PHRASES`), then the sorted set of UNIQUE lowercase alphanumeric
    tokens. The strip runs on the ORDERED token sequence (phrases are positional); the set/sort
    happens after, so the §9.1 golden-file determinism is unchanged in kind.

    A title made ENTIRELY of boilerplate falls back to its unstripped token set — two empty
    norms would be similarity 1.0 and everything template-only would collapse into one cluster.
    """
    tokens = title_tokens(title)
    stripped = _strip_boilerplate(tokens)
    return " ".join(sorted(set(stripped or tokens)))


def similarity(norm_a: str, norm_b: str) -> float:
    """§3.2.4 pinned similarity over two :func:`clusterer_normalize` outputs."""
    return SequenceMatcher(None, norm_a, norm_b).ratio()


def strip_legal_suffixes(company_name: str) -> str:
    """Normalized company name with trailing legal-suffix tokens stripped (never below one token).

    Stripping is iterative (``"COAL INDIA LIMITED" → "coal india" → "coal"``) — the resulting
    common-word aliases are handled by :data:`ALIAS_STOPLIST`, not by refusing the strip.
    """
    return alias_variants(company_name)[-1] if title_tokens(company_name) else ""


def alias_variants(company_name: str) -> list[str]:
    """EVERY stage of the iterative suffix strip, longest first (never below one token).

    The seed takes all stages, not just the final strip (G1 seed-5 row 20, 2026-08-03): "COAL
    INDIA" fully strips to "coal", which the stoplist rightly kills — but the unstripped
    "coal india" stage is exactly what headlines print, and dropping ONLY the dangerous stage
    keeps the company resolvable instead of erasing it. All stages map to the same symbol, so
    overlapping-span matches stay unambiguous (union of 1).
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
    """One ``news_clusters`` row (§4.3): step-2 grouping fields + step-3/step-4 columns carried
    through so a re-upsert (new member joins a scored cluster) never clobbers persisted LLM scores
    (R8 — replay consumes persisted scores; the LLM is never re-invoked for a past cluster).

    ``headline_ids`` is NOT a table column: membership lives on ``news.cluster_id``. On a cluster
    loaded from the store it holds only the ids assigned during THIS run — exactly the rows
    :meth:`MarketStore.set_news_cluster` must update.
    """

    cluster_id: str
    representative: str
    source_domains: list[str]                 # DISTINCT, sorted — the §7.1 corroboration input
    first_seen: datetime
    last_seen: datetime
    headline_ids: list[str] = []
    # step-3 resolver output (symbols[] is EntityResolver output ONLY — the LLM never assigns one)
    scope: str | None = None
    entities: list[str] | None = None
    symbols: list[str] | None = None
    sectors: list[str] | None = None
    themes: list[str] | None = None
    # step-4 scorer columns (Phase 2) — carried through, never written by this module
    sentiment: float | None = None
    materiality: float | None = None
    event_type: str | None = None
    novelty: float | None = None
    scored_at: datetime | None = None
    scorer_model: str | None = None
    untrusted: bool = True                    # §2.4: always TRUE; forced again in to_row()

    @field_validator("first_seen", "last_seen", "scored_at")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None:
            raise ValueError("cluster timestamps must be tz-aware (naive datetimes are a bug, §3.2)")
        return v.astimezone(IST)

    def to_row(self) -> dict[str, Any]:
        """The ``news_clusters`` upsert dict (pinned §4.3 columns; ``untrusted`` forced TRUE)."""
        row = {f: getattr(self, f) for f in _CLUSTER_ROW_FIELDS}
        row["untrusted"] = True
        return row

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> NewsCluster:
        """Build from a ``MarketStore.get_news_clusters`` dict (NULL columns fall back to defaults)."""
        return cls(**{f: row[f] for f in _CLUSTER_ROW_FIELDS if row.get(f) is not None})


class UnresolvedEntity(BaseModel):
    """One ``unresolved_entities`` record (§3.2.4 — ambiguous/out-of-universe/no-match; §5.5 loop)."""

    model_config = ConfigDict(frozen=True)

    entity_text: str
    reason: Literal["ambiguous", "out_of_universe", "no_match"]
    candidate_symbols: tuple[str, ...] = ()


class ResolvedCluster(BaseModel):
    """§3.2.4 ``EntityResolver.resolve`` output: deterministic tags for one cluster.

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
    """§2.7 step 2 — deterministic near-duplicate grouping into ``news_clusters``. NO LLM.

    Parameters
    ----------
    store:
        Optional :class:`MarketStore` for the store-wired :meth:`run`; :meth:`cluster` itself is a
        pure function (golden-file tested, §9.1).
    sim_threshold:
        ``news.cluster_sim_threshold`` (pinned default 0.75; owner-tunable in settings.yaml).
    max_event_age_days:
        ``cat.max_event_age_days`` (envelope default 2) — only clusters with ``last_seen`` inside
        this window are assignment candidates.
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
        """The pinned §3.2.4 algorithm. Returns every cluster that gained ≥1 headline from ``hs``
        (new clusters plus updated ``existing`` ones), ordered by ``(first_seen, cluster_id)``.

        Pure and deterministic: input ``existing`` clusters are copied, never mutated; ties in
        ``published_at`` order break on ``url`` (matching ``NewsIngest`` batch order).
        """
        clusters: list[NewsCluster] = [c.model_copy(deep=True) for c in (existing or [])]
        norms: dict[str, str] = {c.cluster_id: clusterer_normalize(c.representative) for c in clusters}
        touched: dict[str, NewsCluster] = {}

        for h in sorted(hs, key=lambda h: (h.published_at, h.url)):
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
                if h.source_domain not in target.source_domains:
                    target.source_domains = sorted({*target.source_domains, h.source_domain})
                target.first_seen = min(target.first_seen, h.published_at)
                target.last_seen = max(target.last_seen, h.published_at)
            if h.headline_id:
                target.headline_ids = [*target.headline_ids, h.headline_id]
            touched[target.cluster_id] = target

        return sorted(touched.values(), key=lambda c: (c.first_seen, c.cluster_id))

    async def run(self, hs: list[Headline]) -> list[NewsCluster]:
        """Store-wired step 2: load window clusters, :meth:`cluster`, persist, link ``news`` rows.

        The upsert carries ALL persisted columns through :meth:`NewsCluster.to_row`, so a scored
        cluster keeps its step-4 scores when a new member arrives (R8).
        """
        if self._store is None:
            raise RuntimeError("HeadlineClusterer.run requires a MarketStore (pure calls use .cluster)")
        if not hs:
            return []
        window_start = min(h.published_at for h in hs) - self._window
        rows = await self._store.arun(self._store.get_news_clusters, last_seen_after=window_start)
        existing = [NewsCluster.from_row(r) for r in rows]
        touched = self.cluster(hs, existing=existing)
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
    """§2.7 step 3 — deterministic-first symbol/sector/theme tagging. Ambiguous ⇒ NO match, ever.

    State (alias map, sector map, theme keywords, universe) is loaded from the store via
    :meth:`load` / :meth:`aload`, or injected directly for tests/pure use. ``universe=None`` means
    "universe unknown" (e.g. before the 08:30 build): no out-of-universe filtering happens here and
    the §2.7 step-5 digest re-checks ``symbol ∈ universe`` before anything can be traded.

    Parameters
    ----------
    store / clock:
        Store for persistence + ``entity_aliases.added_at`` stamps. Both optional for pure use.
    aliases:
        ``alias -> tradingsymbol(s)`` seed state (normalized internally).
    sector_map:
        ``symbol -> sector`` (§4.3 ``sector_map``); the DISTINCT sector names are the sector
        keywords (whole-word rule) — minus the ``UNCLASSIFIED`` fallback bucket.
    theme_map:
        ``theme -> keywords[]`` (§4.3 ``theme_map`` seeded from config/themes.yaml).
    universe:
        Today's included universe symbols, or None if not yet built.
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
                # Stoplist enforced at LOAD too, not only at seed time — a previously-persisted row
                # for a later-stoplisted alias (BSE, 2026-08-03) must stop matching immediately on
                # the next load, without requiring store surgery while the engine holds the lock.
                continue
            merged.setdefault(norm, set()).update(syms)
        self._aliases = {a: frozenset(s) for a, s in merged.items()}

    def set_sector_map(self, sector_map: Mapping[str, str]) -> None:
        """Sector keywords = the distinct sector NAMES (whole-word phrase rule), minus UNCLASSIFIED."""
        kw: dict[str, set[str]] = {}
        for sector in sector_map.values():
            if sector == _UNCLASSIFIED:
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
        curated symbol(s) load — the owner's explicit mapping overrides the machine seed (§6.3
        platform-suggests-owner-sets; e.g. "Reliance" pins RELIANCE over the conglomerate-prefix
        ambiguity union the seed produces).
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
        universe_rows = self._store.get_universe_daily(d, included_only=True) if d is not None else []
        self._universe = frozenset(r["symbol"] for r in universe_rows) or None

    async def aload(self, d: Any = None) -> None:
        if self._store is None:
            raise RuntimeError("EntityResolver.aload requires a MarketStore")
        await self._store.arun(self.load, d)

    def seed_aliases(self, instruments: Iterable[Mapping[str, Any] | tuple[str, str]]) -> int:
        """Build the §3.2.4 alias SEED from instruments-dump rows and merge + persist it.

        ``instruments`` yields dicts with ``name``/``tradingsymbol`` (the ``instruments_daily`` row
        shape, §4.3) or plain ``(company_name, tradingsymbol)`` tuples. Each company name seeds
        EVERY suffix-strip stage (:func:`alias_variants`); aliases in :data:`ALIAS_STOPLIST` are
        dropped per stage (common English words — owner-reviewed in Phase 1). ``entity_aliases``
        starts as exactly this seed; returns the number of (alias, symbol) pairs seeded.

        DICT rows are filtered to NSE EQUITIES (2026-08-03: seeding the FULL dump poisoned
        resolution — every derivative row's ``name`` is its underlying, so one company name mapped
        to hundreds of contract symbols and the ambiguity rule un-matched previously-good aliases;
        live resolution fell 108→39 clusters). Tuple rows are trusted as (company_name, symbol).
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

        # Conglomerate-surname guard (G1 seed-6 rows 6/19/25): a STRIPPED-stage alias ("ADANI
        # ENTERPRISES" → "adani") that token-PREFIXES another company's alias is ambiguous by
        # construction — a bare "Adani"/"Godrej" headline must refuse with candidates, not default
        # to whichever family member's name happened to strip shortest. Union the prefixed symbols
        # in; the §3.2.4 multi-symbol rule then refuses, while span subsumption still resolves
        # "Adani Power" to ADANIPOWER. Curated rows override at load() ("Reliance" → RELIANCE).
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
        """Merge + persist the ``config/aliases.yaml`` CURATED additions (§3.2.4 "curated additions";
        §6.3 platform-suggests-owner-sets — this file is the owner's surface). Same normalization and
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
        """Resolve one cluster deterministically (§3.2.4). Never guesses.

        ``extra_texts`` is the Phase-2 seam: verbatim entity STRINGS emitted by the News Analyst
        for unmatched clusters re-enter here under the same whole-word rule (the LLM never assigns
        a symbol). An extra text matching nothing is logged ``no_match``.
        """
        resolved, unresolved = self._match_aliases(c.representative)
        for text in extra_texts:
            r2, u2 = self._match_aliases(text)
            if not r2 and not u2:
                unresolved.append(UnresolvedEntity(entity_text=text, reason="no_match"))
            resolved += r2
            unresolved += u2

        entities: set[str] = set()
        symbols: set[str] = set()
        for aliases_in, symbol in resolved:
            entities.update(aliases_in)
            if self._universe is not None and symbol not in self._universe:
                # Out-of-universe: recorded, never traded (§2.7 step 3 / §9.1).
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
            sectors=self._match_keywords(c.representative, self._sector_keywords),
            themes=self._match_keywords(c.representative, self._theme_keywords),
            unresolved=sorted(
                set(unresolved), key=lambda u: (u.entity_text, u.reason, u.candidate_symbols)
            ),
        )

    async def run(self, clusters: Sequence[NewsCluster]) -> list[ResolvedCluster]:
        """Store-wired step 3: resolve each cluster, persist tags onto ``news_clusters`` (carrying
        every other column through — scores are never clobbered), log unresolved entities (§5.5)."""
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
        """Whole-word phrase alias matches over ``text`` with the §3.2.4 ambiguity rules applied.

        Returns ``(resolved, unresolved)`` where ``resolved`` pairs the matched alias strings with
        their SINGLE unambiguous tradingsymbol. Overlapping title spans are grouped into connected
        components; a component whose symbol union is >1 (a multi-symbol alias, or different
        companies' aliases overlapping) resolves to NOTHING and every alias in it is logged
        ``ambiguous`` with the union as candidates.

        SUBSUMPTION refinement (2026-08-03, G1 seed-6 row 47): a match whose span is STRICTLY
        contained in a longer match's span is subsumed — the most specific phrase wins. Without
        it, curating "SBI" (SBIN) silently killed every "SBI Card" headline, and "PVR Inox"
        refused because the contained "Inox" (a different company) poisoned the component.
        Staggered partial overlaps (neither contains the other) still refuse — never a guess.
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
        """Whole-word phrase keyword tagging (same rule as aliases) — sector/theme tags (§3.2.4)."""
        tokens = title_tokens(text)
        tags: set[str] = set()
        for phrase, tagset in keyword_map.items():
            if self._contains_phrase(tokens, phrase.split(" ")):
                tags.update(tagset)
        return sorted(tags)


# =========================================================================== §2.7 step 5 (digest)
#: The §5.4 event types that carry the O13 T+1 PEAD sign-agreement requirement (§6.1 `cat`).
EARNINGS_EVENT_TYPES: frozenset[str] = frozenset({"earnings_result", "earnings_guidance"})

#: ``cat.*`` behaviour knobs the digest needs. All but ``fanout_weight`` are §6.3 envelope rows;
#: ``fanout_weight`` is settings.yaml-resident (owner-only, deliberately NOT learnable).
CAT_PARAM_KEYS: tuple[str, ...] = (
    "materiality_min", "novelty_min", "max_event_age_days", "decay_halflife_h",
    "confirm_move_pct", "stop_atr_mult", "rr_target", "fanout_weight",
)

#: §7.1 `catalyst_guard` fallbacks, used ONLY for a key absent from the hash-verified block. Each
#: fails to LESS activity (empty event-type list ⇒ nothing can originate), so a truncated guard
#: block can never widen the origination surface (§2.7 fail-safe ladder).
_GUARD_FALLBACKS: dict[str, Any] = {
    "min_source_domains": 2,
    "sentiment_min_long": 0.30,
    "digest_stale_max_h": 20,
    "originating_event_types": (),
}

#: §2.7 step 5(ii) inclusion floor: below this WEIGHTED materiality a cluster feeds `sentiment_agg`
#: only — no watchlist row at all (the §5.4 rubric noise line).
INCLUSION_FLOOR = 0.2

#: ``earnings_calendar.kind`` marking a results day T (R2 no-entry day + the O13 reaction bar).
#: ``EarningsCalendarJob.classify_event`` already folds results-considering board meetings into it.
_RESULTS_KIND = "results"

_ATR_PERIOD = 14
#: ``bars_1d`` rows needed strictly before ``d`` for levels: the Wilder ATR seed lands at index
#: ``period − 1``, so 15 rows give a seeded value plus one recursion step. Fewer ⇒ levels NULL.
_MIN_LEVEL_BARS = 15
#: Calendar span pulled for the per-symbol daily history (≈60 sessions ≫ _MIN_LEVEL_BARS, and wide
#: enough to contain the O13 results-day-T bar, which lies inside the cluster lookback window).
_HISTORY_LOOKBACK_DAYS = 90
#: Bound on the day-at-a-time calendar walks (a missing calendar year must not spin, R6).
_MAX_CALENDAR_SCAN_DAYS = 60

_PAISE = Decimal("0.01")
_HUNDRED = Decimal("100")


def load_cat_params(envelope: Mapping[str, Any] | None = None) -> dict[str, float]:
    """``cat.*`` digest params: ``config/envelope.yaml`` defaults + settings.yaml ``cat``, overridden
    by ``envelope`` (namespaced ``cat.rr_target`` and bare ``rr_target`` both resolve; every other
    strategy's keys are ignored, so the live §6.5 ``envelope_state`` mapping can be passed whole).

    The bounds file is read UNVERIFIED here because only its DEFAULTS are used — every value that
    can license a trade comes from the caller's ``envelope_state`` row or from the hash-verified
    ``catalyst_guard`` block (§2.4 item 1), never from this read.
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
    """One ``catalyst_guard`` value, falling back to the pinned §7.1 default (:data:`_GUARD_FALLBACKS`)."""
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
    """The §2.7 step-5(ii) AND-list, one boolean per condition — ``originating`` iff ALL are True.

    Pure: every argument is already-gathered evidence, so each condition is unit-testable in
    isolation and the store reads stay out of the rule. ``guard`` is the hash-verified §7.1
    ``catalyst_guard`` block. Materiality/sentiment arrive ALREADY weighted by
    ``cat.fanout_weight`` for a fanned-out sector/theme cluster (§2.7: the weight multiplies both
    BEFORE the comparison). Short-direction candidates fail ``sentiment_long`` by construction —
    the §1.4.9 shorts gate, not a separate rule.
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


def _paise(value: Decimal) -> Decimal:
    """Quantize to the DECIMAL(12,2) column scale. NO tick snapping here: the watchlist stores raw
    deterministic levels; the live scanner is what publishes tick-legal order prices (§3.2.5)."""
    return value.quantize(_PAISE, rounding=ROUND_HALF_UP)


def _reaction_agrees(bars: Sequence[DailyBar], t_day: date, sentiment: float) -> bool:
    """O13 PEAD agreement: ``sign(cluster sentiment) == sign(close_T − open_T)`` from ``bars_1d``.

    ``close_T == open_T`` ⇒ reaction sign 0 ⇒ never agrees (conservative, §6.1). A MISSING T bar is
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
    """One digest run's outcome (§2.7 step 5). ``(0, 0)`` is a SUCCESS — an empty-but-FRESH digest
    means `cat` simply originates nothing; only STALE/MISSING disables it (§2.7 fail-safe ladder)."""

    model_config = ConfigDict(frozen=True)

    d: date
    n_originating: int
    n_context: int
    sentiment_rows: int
    ran_at: datetime


class CatalystDigestJob:
    """§2.7 step 5 / §4.4 job 14 (~08:35, before the 08:50 planner; idempotent run-latest catch-up).

    Two outputs, both deterministic and both re-derivable from persisted scores (no LLM, R8):

    (i) ``sentiment_agg`` — ``clip(Σᵢ sentimentᵢ·materialityᵢ·wᵢ·0.5^(age_hᵢ/half-life), −1, +1)`` per
    ``(scope, scope_key)`` over scored clusters with ``age_h ≤ 6×`` half-life, age measured from
    cluster ``first_seen`` to the run time. A decayed SUM, never a mean.

    (ii) ``catalyst_watchlist`` — every scored cluster whose TRADING-SESSION event age ≤
    ``cat.max_event_age_days``, best cluster per symbol, graded by :func:`originating_conditions`
    with §6.1 levels on ``originating`` rows only.

    Parameters
    ----------
    store / clock / calendar:
        DuckDB access (all reads/writes offloaded via ``arun``, convention 12), the single "now",
        and the weekend/holiday-aware session arithmetic (R6).
    protected_store:
        Loads ``limits.yaml`` HASH-VERIFIED at run time — the ``catalyst_guard`` block is never
        accepted as a constructor argument (§2.4 item 1: the enforcement site verifies it). A
        verification failure propagates: an unverifiable anti-manipulation surface must yield NO
        digest (which disables `cat` for the day), never a permissive one.
    envelope:
        Live §6.5 ``envelope_state`` values; see :func:`load_cat_params` for defaults + key forms.

    Spec ambiguities resolved here (documented for the integrator):

    - ``market``-scope clusters contribute to the ``market`` row ONLY — never a symbol row and
      never a watchlist row (§2.7: market news feeds regime context, "**never** origination").
    - Sector/theme fan-out consumes the RESOLVER's tags (``sectors``/``themes``) and is intersected
      with today's included universe when one exists; with no universe row yet the raw constituents
      are used and the ``in_universe`` condition still blocks origination.
    - The ``market``/``market`` row is written on EVERY run (0.0 when no market cluster scored), so
      "the digest ran" is observable even for an empty corpus — otherwise an empty-but-fresh digest
      would be indistinguishable from a missing one (§2.7 fail-safe ladder needs that distinction).
    - The row's ``materiality`` is the WEIGHTED value (what the grade decision used); ``event_age_h``
      is informational, ``event_age_sessions`` is the eligibility clock.
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

    # ------------------------------------------------------------------ session arithmetic (R6)
    def event_age_sessions(self, first_seen: datetime, d: date) -> int:
        """§2.7 step-5(ii) event age: TRADING DAYS in ``(first_seen date, d]``.

        The digest runs pre-open, so ``d`` is the first session the event can be traded into and
        counts as age 1 — a Friday-evening or weekend event is therefore age 1 at Monday's digest
        (which keeps Friday reporters T+1-PEAD-eligible, O13), and a same-day event is age 0.
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

    # ------------------------------------------------------------------ guard (§2.4 item 1)
    def _guard(self) -> dict[str, Any]:
        """The ``catalyst_guard`` block from the HASH-VERIFIED ``limits.yaml`` — loaded per run, at
        the enforcement site, exactly like the gate loads its limits (§2.4 item 1 / §7.1)."""
        limits = self._protected.load_verified("limits.yaml")
        guard = (limits.get("limits") or {}).get("catalyst_guard")
        if not isinstance(guard, Mapping):
            raise ValueError("limits.yaml has no `limits.catalyst_guard` block (§7.1 owner-only surface)")
        return dict(guard)

    # ------------------------------------------------------------------ run (§2.7 step 5)
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
        universe = {
            r["symbol"]
            for r in await self._store.arun(self._store.get_universe_daily, d, included_only=True)
        }

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
        """§2.7 fail-safe input for features-v2 + the ``CATALYST_DISABLED`` alert (scanner: Phase 3).

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

    # ------------------------------------------------------------------ step 5(i): sentiment_agg
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
        for c in clusters:
            # Clamp a future first_seen (clock skew / bad feed timestamp) to age 0: decay may fade a
            # contribution, never AMPLIFY it beyond its unaged weight.
            age_h = max(0.0, (ran_at - c.first_seen).total_seconds() / 3600.0)
            if age_h > 6.0 * halflife:
                continue
            base = float(c.sentiment) * float(c.materiality) * 0.5 ** (age_h / halflife)
            w = fanout if c.scope in ("sector", "theme") else 1.0
            for symbol in self._symbol_targets(c, sector_symbols, theme_symbols, universe):
                totals[("symbol", symbol)] += base * w
            for sector in c.sectors or []:
                totals[("sector", sector)] += base
            for theme in c.themes or []:
                totals[("theme", theme)] += base
            if c.scope == "market":
                totals[("market", "market")] += base
        totals.setdefault(("market", "market"), 0.0)   # digest-ran marker (see class docstring)
        return [
            {"scope": scope, "scope_key": key, "as_of": ran_at, "value": _clip(value)}
            for (scope, key), value in sorted(totals.items())
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

    # ------------------------------------------------------------------ step 5(ii): watchlist
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
        sentiment_min = float(guard_value(guard, "sentiment_min_long"))

        flagged = {
            r["symbol"] for r in await self._store.arun(self._store.get_flagged_instrument_days, d)
        }
        results_days: dict[str, set[date]] = defaultdict(set)
        for r in await self._store.arun(self._store.get_earnings_calendar, earnings_from, d):
            if r.get("kind") == _RESULTS_KIND:
                results_days[r["symbol"]].add(r["event_date"])

        # Best cluster per symbol = highest WEIGHTED materiality; ties break on cluster_id (§9.1
        # determinism: the same corpus must yield the same watchlist).
        best: dict[str, tuple[NewsCluster, float, float, int]] = {}
        for c in clusters:
            age_sessions = self.event_age_sessions(c.first_seen, d)
            if age_sessions > max_days:
                continue
            w = fanout if c.scope in ("sector", "theme") else 1.0
            weighted_materiality = float(c.materiality) * w
            for symbol in self._symbol_targets(c, sector_symbols, theme_symbols, universe):
                current = best.get(symbol)
                if current is None or (-weighted_materiality, c.cluster_id) < (
                    -current[1], current[0].cluster_id
                ):
                    best[symbol] = (c, weighted_materiality, float(c.sentiment) * w, age_sessions)

        rows: list[dict[str, Any]] = []
        for symbol in sorted(best):
            c, weighted_materiality, weighted_sentiment, age_sessions = best[symbol]
            if weighted_materiality < INCLUSION_FLOOR:
                continue                       # below the noise line: sentiment_agg only, no row
            symbol_results = results_days.get(symbol, set())
            t_day = max((t for t in symbol_results if t < d), default=None)
            history: list[DailyBar] | None = None
            reaction_agrees = True             # vacuous unless this IS an earnings event with a T
            if c.event_type in EARNINGS_EVENT_TYPES and t_day is not None:
                history = await self._history(symbol, d)
                reaction_agrees = _reaction_agrees(history, t_day, weighted_sentiment)
            conditions = originating_conditions(
                weighted_materiality=weighted_materiality,
                weighted_sentiment=weighted_sentiment,
                event_type=c.event_type,
                source_domain_count=len(c.source_domains),
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
                    # Never blocks the grade: the scanner's live confirmation still needs its own
                    # levels, and a thin-history symbol is caught by warmup_ready anyway (§7.1).
                    _log.warning(
                        "catalyst_levels_unavailable", symbol=symbol, d=str(d), bars=len(history)
                    )
            direction = (
                "long" if weighted_sentiment >= sentiment_min
                else "short" if weighted_sentiment <= -sentiment_min
                else None
            )
            rows.append({
                "entry_id": str(ULID()),
                "symbol": symbol,
                "grade": "originating" if originating else "context",
                "direction": direction,
                "event_type": c.event_type,
                "cluster_refs": [c.cluster_id],
                "materiality": weighted_materiality,
                "source_domain_count": len(c.source_domains),
                "event_age_h": (ran_at - c.first_seen).total_seconds() / 3600.0,
                "event_age_sessions": age_sessions,
                "expires_at": self._expires_at(c.first_seen, max_days),
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
        """Deterministic §6.1 `cat` levels from bars strictly before ``d``; ``{}`` on thin history.

        Only the PRICE leg of the §6.1 confirmation is computable at 08:35: ``confirm_trigger =
        prior_close × (1 + cat.confirm_move_pct/100)``. The ``max()`` against the day's first-30-min
        high and the relative-volume leg are LIVE scanner conditions — the trigger here is the floor
        the scanner raises, never the whole rule. ``invalidation = prior_close``: a confirmation move
        fully retraced voids the setup (documented decision — §2.7 pins the level set, not this
        choice). Stop/target BANDS collapse to a point because ``entry == confirm_trigger`` is the
        only entry price knowable pre-open; the live scanner recomputes both at the true entry.
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
        # persisted levels satisfy the §6.1 arithmetic exactly as stored (no residual drift).
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
    """``sector_map`` rows → ``sector -> {symbols}`` (the §2.7 sector fan-out constituents)."""
    out: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        out[row["sector"]].add(row["symbol"])
    return dict(out)
