"""Deterministic news pipeline, §2.7 steps 2–3: ``HeadlineClusterer`` + ``EntityResolver`` (§3.2.4).

Both algorithms are PINNED by the plan — cluster membership drives the §7.1
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
(§5.4, Tier-1 LLM, scores persisted per CLUSTER) — and step 5 — ``CatalystDigestJob`` (§4.4 job 14)
— belong to ``engine.intelligence``. The News Analyst's verbatim entity strings for unmatched
clusters re-enter :meth:`EntityResolver.resolve` via ``extra_texts`` — the LLM never assigns a
symbol directly (§3.2.4).

Failure model: this module is deterministic CPU-bound work over already-ingested rows; the async
``run`` wrappers offload DuckDB access through ``MarketStore`` (convention 12) and are scheduled by
the §4.4 job-10 pipeline, which is never load-bearing (E5).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from engine.core.clock import IST, Clock
from engine.core.log import get_logger
from engine.datafeeds.news import Headline
from engine.marketdata.store import MarketStore

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


def clusterer_normalize(title: str) -> str:
    """§3.2.4 pinned clusterer normalization: sorted set of UNIQUE lowercase alphanumeric tokens."""
    return " ".join(sorted(set(title_tokens(title))))


def similarity(norm_a: str, norm_b: str) -> float:
    """§3.2.4 pinned similarity over two :func:`clusterer_normalize` outputs."""
    return SequenceMatcher(None, norm_a, norm_b).ratio()


def strip_legal_suffixes(company_name: str) -> str:
    """Normalized company name with trailing legal-suffix tokens stripped (never below one token).

    Stripping is iterative (``"COAL INDIA LIMITED" → "coal india" → "coal"``) — the resulting
    common-word aliases are handled by :data:`ALIAS_STOPLIST`, not by refusing the strip.
    """
    tokens = title_tokens(company_name)
    while len(tokens) > 1 and tokens[-1] in LEGAL_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens)


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
            if not norm:
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
        """(Re)load alias/sector/theme/universe state from the store (sync; see :meth:`aload`)."""
        if self._store is None:
            raise RuntimeError("EntityResolver.load requires a MarketStore")
        self._set_aliases(
            (row["alias"], (row["tradingsymbol"],)) for row in self._store.get_entity_aliases()
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
        shape, §4.3) or plain ``(company_name, tradingsymbol)`` tuples. Each company name gets its
        legal suffixes stripped (:func:`strip_legal_suffixes`); aliases in :data:`ALIAS_STOPLIST`
        are dropped (common English words — owner-reviewed in Phase 1). ``entity_aliases`` starts as
        exactly this seed; returns the number of (alias, symbol) pairs seeded.
        """
        pairs: list[tuple[str, str]] = []
        stoplisted = 0
        for item in instruments:
            if isinstance(item, tuple):
                name, symbol = item
            else:
                name, symbol = item.get("name"), item.get("tradingsymbol")
            if not name or not symbol:
                continue
            alias = strip_legal_suffixes(str(name))
            if not alias:
                continue
            if alias in ALIAS_STOPLIST:
                stoplisted += 1
                continue
            pairs.append((alias, str(symbol)))

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
        """
        tokens = title_tokens(text)
        matches: list[tuple[int, int, str, frozenset[str]]] = []
        for alias, syms in self._aliases.items():
            alias_toks = alias.split(" ")
            n = len(alias_toks)
            for i in range(len(tokens) - n + 1):
                if tokens[i:i + n] == alias_toks:
                    matches.append((i, i + n, alias, syms))
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
