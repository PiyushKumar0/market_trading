"""News-Analyst scoring job — the ONLY writer of the score columns.

The job is a thin, deterministic shell around one Tier-1 agent: it decides WHETHER to call (time
window, batch trigger, governor), chunks the queue to the batch cap, and writes the returned scores
back onto ``news_clusters`` under the fan-out purity rule. Everything that could widen the
origination surface is decided here, in Python, never by the model:

- **Fan-out purity.** LLM-emitted ``sectors``/``themes`` are advisory and
  are INTERSECTED with (never a superset of) the RESOLVER's deterministic tags already persisted on
  the cluster. A sector the resolver never tagged therefore fans out to nothing.
- **The LLM never assigns a symbol.** Its ``entities`` are verbatim STRINGS that
  re-enter :meth:`EntityResolver.resolve` as ``extra_texts``; only that resolver's unambiguous,
  in-universe matches are merged into ``symbols``.
- **Fail to zero (the fail-safe ladder).** A blocked governor, a harness failure or an out-of-enum
  ``event_type`` all leave the affected clusters UNSCORED, which excludes them from the digest's
  watchlist — `cat` originates nothing. No alert is raised here: the digest owns the ``CATALYST_*``
  alerts, and a scoring gap that never reaches the digest is not an owner-visible condition.

Decisions:

- The 30-minute age trigger is read from :meth:`BudgetGovernor.news_batch_cadence_min` (30 by
  default; 60 at DG2+ or when the news analyst is over its own share), not hard-coded.
- The market-hours leg of the time gate requires a trading day; the 19:00–22:00 sweep does not.
  Weekend/holiday news is eligible for the next session's digest (`cat.max_event_age_days` counts
  SESSIONS), so refusing to score it would shove the weekend queue into the pre-open batch.
- ``sectors``/``themes`` are intersected against the cluster's PERSISTED resolver columns, not a
  fresh ``resolve()``: the persisted tags are what the digest's fan-out consumes, and alias/theme-map
  state may have drifted.
- ``entities`` is persisted as the RESOLVER's matched-alias output (``extra_texts`` can only add
  matches), not model prose. The model's UNMATCHED strings are logged to ``unresolved_entities``
  (reason ``no_match``): the weekly alias-suggestion inputs.
- A missing ``news_analyst`` roster entry is a construction-time error, not a runtime skip.

**Chain-lock discipline.** The caller may hand ``run_batch`` the ``news_chain_lock`` that serialises
the ``news_clusters`` read-modify-write against the per-feed polls. The lock is held for the STORE
HOPS ONLY — the initial queue read and each chunk's write-back — NEVER across the model call: the
polls' resolve deadline covers lock ACQUISITION, so a hold spanning an LLM await makes them time out.
Because the hold is short, the write-back is always CONDITIONAL: under the same hold the FULL
unscored rows are re-read and each score is applied to the CURRENT row, never the pre-LLM snapshot.
The upsert overwrites every non-key column, so this is what lets a poll merge landing during the
model call (``source_domains``, ``first_seen``/``last_seen``, ``representative``,
``symbols``/``sectors``/``themes``) SURVIVE; an id that left the queue meanwhile (merged away, or
scored by another batch) is skipped rather than re-created as a zombie row or clobbered (drops are
logged). ``lock=None`` is not a distinct code path: the job owns a private ``asyncio.Lock()`` for
that run, so the conditional write-back always applies.

**Accepted residual.** Chunks 2..N are prompted from the BATCH's initial snapshot (the queue is read
once), so a poll merge landing after a chunk's context was built yields a score computed without the
merged headline; only a merge landing before write-back could be reverted, and the CONDITIONAL
write-back stops that. The stale-skip count (``stale`` on ``news_scoring_batch``, folded into
``remaining`` on ``news_scoring_chunk_failed``) makes the interleaving visible.

The batch is SINGLE-FLIGHTED on a lock the job owns (``_run_lock``): the chain's forced pre-open
batch and the 300 s scoring tick are distinct scheduler jobs that would otherwise read the same
queue and bill the same clusters twice. A queued batch waits rather than skipping — see
:meth:`NewsScoringJob.run_batch`.
"""

from __future__ import annotations

import asyncio
import html
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.datafeeds.news_pipeline import EntityResolver, NewsCluster, title_tokens
from engine.intelligence.agents import news_analyst
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.intelligence.schemas import ClusterScore
from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.news_scoring")

#: Batch trigger: this many queued clusters fire a call regardless of age.
MIN_BATCH_CLUSTERS = 8

#: Scoring windows (IST wall-clock, inclusive bounds).
MARKET_WINDOW: tuple[time, time] = (time(9, 0), time(15, 30))
SWEEP_WINDOW: tuple[time, time] = (time(19, 0), time(22, 0))

#: The agents.yaml call class for this job — the governor's admission key and the assembler's
#: ``call_class`` on a news context (they must agree, or the audit trail lies about what was asked).
CALL_CLASS = "batch"

SKIP_OUTSIDE_WINDOWS = "outside scoring windows"
SKIP_BATCH_THRESHOLD = "batch threshold not met"
SKIP_GOVERNOR = "governor"


class ScoringResult(BaseModel):
    """One :meth:`NewsScoringJob.run_batch` outcome. Frozen: a job result is evidence.

    ``skipped_reason`` is set ONLY when no SDK call was attempted. A run that called and failed
    mid-queue returns the partial ``scored`` count with no reason — the remaining clusters simply
    stay queued for the next cadence, which is the fail-safe behaviour, not an error state.
    """

    model_config = ConfigDict(frozen=True)

    scored: int = 0
    dropped: tuple[str, ...] = ()       # Per-item drops (out-of-enum event_type &c) — left UNSCORED
    skipped_reason: str | None = None


class NewsScoringJob:
    """Batched cluster scoring: queue → the Tier-1 news analyst (model per agents.yaml) → ``news_clusters`` score columns.

    Parameters
    ----------
    store:
        DuckDB access; every read/write is offloaded through ``arun``/``a*`` (convention 12).
    resolver:
        The :class:`EntityResolver`, already loaded with alias/sector/theme/universe state.
        It is the ONLY thing allowed to turn the model's entity strings into symbols.
    assembler / harness / agent_defs:
        The context assembler, the sole SDK caller, and the loaded ``agents.yaml`` roster.
    governor:
        Admission + the DG2 batching cadence.
    clock / calendar:
        The single "now" and the trading-day fact for the time gate (no naive datetimes).
    """

    def __init__(
        self,
        store: MarketStore,
        resolver: EntityResolver,
        assembler: ContextAssembler,
        harness: AgentHarness,
        agent_defs: Mapping[str, AgentDef],
        governor: BudgetGovernor,
        clock: Clock,
        calendar: NSECalendar,
    ) -> None:
        agent_def = agent_defs.get(news_analyst.AGENT_ID)
        if agent_def is None:
            raise KeyError(
                f"agents.yaml has no enabled {news_analyst.AGENT_ID!r} definition — the news scoring "
                "job cannot be wired to a parked agent"
            )
        self._store = store
        self._resolver = resolver
        self._assembler = assembler
        self._harness = harness
        self._def = agent_def
        self._governor = governor
        self._clock = clock
        self._calendar = calendar
        # SINGLE-FLIGHT: see run_batch. Owned by the job, not the caller (two distinct scheduler jobs).
        self._run_lock = asyncio.Lock()

    # ------------------------------------------------------------------ run
    async def run_batch(
        self, *, force: bool = False, lock: asyncio.Lock | None = None
    ) -> ScoringResult:
        """Score the queued clusters. ``force`` is the PRE-OPEN batch: it ignores both the
        time gate and the ≥8/30-min trigger and scores everything unscored.

        ``lock`` is the caller's news-chain lock: taken for the queue read and for each chunk's
        write-back, NEVER across the model call — see the module docstring. ``None`` makes the job
        own a private lock for this run; the conditional re-read/write-back applies either way.

        The whole body runs under the job's own ``_run_lock``: the chain's forced pre-open batch and
        the 300 s ``scoring_tick`` are DISTINCT scheduler jobs and nothing else keeps them apart.
        It WAITS rather than skips: a batch queued behind another re-reads a queue the first one
        emptied and returns in one store read, whereas a skip would drop the tick's legitimate work
        (indistinguishable from a scoring gap downstream). Held OUTSIDE ``lock``, never inside: the
        chain lock's holders never take this one, so the nesting order cannot cycle."""
        async with self._run_lock:
            return await self._run_batch(force=force, lock=lock)

    async def _run_batch(self, *, force: bool, lock: asyncio.Lock | None) -> ScoringResult:
        """One batch, already single-flighted by :meth:`run_batch`. ``lock`` is the caller's chain
        lock; a caller with none gets a private one for this run."""
        now = self._clock.now()
        if not force and not self._in_scoring_window(now):
            return ScoringResult(skipped_reason=SKIP_OUTSIDE_WINDOWS)
        lock = lock if lock is not None else asyncio.Lock()

        # One hold for the whole read side (queue + theme vocabulary + the gates that read them):
        # everything in here is a store read or pure CPU, so the hold stays short.
        async with lock:
            rows = await self._store.arun(self._store.get_news_clusters, scored=False)
            clusters = [NewsCluster.from_row(row) for row in rows]
            if not force and not self._batch_ready(clusters, now):
                return ScoringResult(skipped_reason=SKIP_BATCH_THRESHOLD)
            if not clusters:
                return ScoringResult()  # empty queue: no governor decision, no SDK call, nothing to say

            decision = self._governor.can_invoke(news_analyst.AGENT_ID)
            if not decision.allowed:
                # Fail to ZERO: the clusters stay unscored, so the digest excludes them and
                # `cat` originates nothing. No alert here — CATALYST_* belongs to the digest.
                _log.warning(
                    "news_scoring_blocked",
                    queued=len(clusters),
                    tier=decision.tier.value,
                    reason=decision.reason,
                )
                return ScoringResult(skipped_reason=SKIP_GOVERNOR)

            theme_vocabulary = [
                row["theme"] for row in await self._store.arun(self._store.get_theme_map)
            ]

        scored = 0
        dropped: list[str] = []
        stale_total = 0
        for chunk in _chunks(clusters, news_analyst.MAX_CLUSTERS_PER_CALL):
            context = self._assembler.for_news_batch(
                [_prompt_row(c) for c in chunk], theme_vocabulary
            )
            # OUTSIDE the lock: the polls' resolve deadline covers lock acquisition (module docstring).
            result = await self._harness.run_single_shot(
                self._def,
                context,
                news_analyst.parse_output,
                json_schema=news_analyst.output_json_schema(),
                call_class=CALL_CLASS,
            )
            if not result.ok:
                # Stop at the first failed chunk: the remaining clusters stay queued for the next
                # cadence rather than being re-sent inside a run that is already failing. Stale ids
                # already left the queue, so they are subtracted from ``remaining``.
                _log.warning(
                    "news_scoring_chunk_failed",
                    reason=result.reason,
                    call_id=result.call_id,
                    scored_so_far=scored,
                    remaining=len(clusters) - scored - len(dropped) - stale_total,
                )
                break
            scores, chunk_dropped = result.payload
            dropped.extend(chunk_dropped)
            async with lock:
                written, stale = await self._write_back(chunk, scores)
                scored += written
                stale_total += stale

        _log.info(
            "news_scoring_batch",
            queued=len(clusters),
            scored=scored,
            dropped=len(dropped),
            stale=stale_total,
            force=force,
            model=self._def.model,
        )
        return ScoringResult(scored=scored, dropped=tuple(dropped))

    # ------------------------------------------------------------------ triggers
    def _in_scoring_window(self, now: datetime) -> bool:
        """Market hours on a trading day, or the evening sweep (see the module's decisions)."""
        t = now.time()
        if SWEEP_WINDOW[0] <= t <= SWEEP_WINDOW[1]:
            return True
        return self._calendar.is_trading_day(now.date()) and MARKET_WINDOW[0] <= t <= MARKET_WINDOW[1]

    def _batch_ready(self, clusters: Sequence[NewsCluster], now: datetime) -> bool:
        """≥8 queued clusters, or the oldest has waited out the (governor-set) cadence."""
        if len(clusters) >= MIN_BATCH_CLUSTERS:
            return True
        if not clusters:
            return False
        cadence = timedelta(minutes=self._governor.news_batch_cadence_min())
        return min(c.first_seen for c in clusters) <= now - cadence

    # ------------------------------------------------------------------ write-back (fan-out purity)
    async def _write_back(
        self, chunk: Sequence[NewsCluster], scores: Sequence[ClusterScore]
    ) -> tuple[int, int]:
        """Persist one chunk's scores, matched by ``cluster_id``. Returns ``(written, stale)``: the
        rows written and how many sent ids had already left the queue by write-back time (merged away
        or scored by a rival batch) — the caller accumulates it so its logs don't count such an id as
        still-remaining (``_run_batch``'s ``stale_total``).

        Called under the chain lock: re-reads the FULL unscored rows under that SAME hold and, for
        each id we sent, applies the score columns onto the CURRENT cluster — never the pre-LLM
        snapshot. An id no longer in the queue is skipped rather than resurrected as a zombie row or
        clobbered with an older score, and — the upsert overwrites every non-key column — a merge that
        landed during the model call SURVIVES instead of being reverted. The chunk snapshot is only
        the "this id was sent" membership test.
        """
        sent = {c.cluster_id: c for c in chunk}
        live = {
            row["cluster_id"]: NewsCluster.from_row(row)
            for row in await self._store.arun(self._store.get_news_clusters, scored=False)
        }
        now = self._clock.now()
        rows: list[dict[str, Any]] = []
        stale: list[str] = []
        for score in scores:
            if score.cluster_id not in sent:
                # A cluster_id we did not send: nothing to write it onto, and inventing a row would
                # let the model create a cluster. Ignored, but never silently.
                _log.warning("news_score_unknown_cluster", cluster_id=score.cluster_id)
                continue
            # The score applies to the FRESH re-read, so the resolve input, symbol union and
            # sector/theme intersection below read the CURRENT cluster columns.
            cluster = live.get(score.cluster_id)
            if cluster is None:
                stale.append(score.cluster_id)
                continue
            # ``entities`` must be verbatim strings from the headline the model was SHOWN (the sent
            # snapshot — a merge may have changed the live representative since). Anything else is
            # injected or misaligned and could attach another company's symbol (chaos case 21).
            shown = sent[score.cluster_id].representative
            entities = [e for e in score.entities if _in_headline(e, shown)]
            if len(entities) < len(score.entities):
                _log.warning(
                    "news_entities_not_in_headline", cluster_id=score.cluster_id,
                    dropped=[e for e in score.entities if e not in entities],
                )
            resolved = self._resolver.resolve(cluster, extra_texts=entities)
            await self._log_unmatched(cluster, score, resolved)
            rows.append(
                cluster.model_copy(
                    update={
                        "scope": score.scope,
                        "entities": resolved.entities,
                        # The LLM never assigns a symbol: only the resolver's
                        # unambiguous, in-universe matches join the existing set.
                        "symbols": sorted({*(cluster.symbols or []), *resolved.symbols}),
                        # FAN-OUT PURITY: advisory ∩ deterministic, never a superset.
                        "sectors": sorted(set(score.sectors) & set(cluster.sectors or [])),
                        "themes": sorted(set(score.themes) & set(cluster.themes or [])),
                        "sentiment": score.sentiment,
                        "materiality": score.materiality,
                        "event_type": score.event_type,
                        "novelty": score.novelty,
                        "scored_at": now,
                        "scorer_model": self._def.model,
                    }
                ).to_row()
            )
        if stale:
            # Once per chunk, with the ids: a silent drop here would look like a scoring gap.
            _log.warning("news_score_writeback_dropped", count=len(stale), cluster_ids=stale)
        if rows:
            await self._store.aupsert_news_clusters(rows)
        return len(rows), len(stale)

    async def _log_unmatched(
        self, cluster: NewsCluster, score: ClusterScore, resolved: Any
    ) -> None:
        """Log the model's UNMATCHED entity strings for the weekly alias-suggestion loop.

        Only ``no_match`` entries whose text is one of this score's strings are logged: an ambiguous
        alias in the headline was already recorded by the resolver; re-logging would bury new ones.
        """
        emitted = set(score.entities)
        for entry in resolved.unresolved:
            if entry.reason != "no_match" or entry.entity_text not in emitted:
                continue
            await self._store.arun(
                self._store.log_unresolved_entity,
                entry.entity_text,
                entry.reason,
                cluster_id=cluster.cluster_id,
                candidate_symbols=list(entry.candidate_symbols),
            )


def _in_headline(entity: str, headline: str) -> bool:
    """``entity`` occurs in ``headline`` as a whole-word token run, under the resolver's own
    normalization. HTML entities are decoded first: feeds deliver ``L&amp;T`` while the model
    writes ``L&T``."""
    needle = title_tokens(html.unescape(entity))
    hay = title_tokens(html.unescape(headline))
    n = len(needle)
    return n > 0 and any(hay[i:i + n] == needle for i in range(len(hay) - n + 1))


def _chunks(clusters: Sequence[NewsCluster], size: int) -> list[Sequence[NewsCluster]]:
    """Queue split into ≤``size`` batches, oldest first (``get_news_clusters`` orders by first_seen)."""
    return [clusters[i:i + size] for i in range(0, len(clusters), size)]


def _prompt_row(cluster: NewsCluster) -> dict[str, Any]:
    """The ONLY cluster fields the prompt may see: identity, recency, corroboration, headline.

    Built explicitly rather than dumping the row, so a future column cannot leak into the prompt —
    in particular the score columns themselves, which would let a re-score anchor on a prior score.
    """
    return {
        "cluster_id": cluster.cluster_id,
        "representative": cluster.representative,
        "source_domains": list(cluster.source_domains),
        "first_seen": cluster.first_seen,
    }
