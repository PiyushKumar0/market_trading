"""News-Analyst scoring job (§2.7 step 4 / §5.4) — the ONLY writer of the step-4 score columns.

The job is a thin, deterministic shell around one Tier-1 agent: it decides WHETHER to call (time
window, batch trigger, governor), chunks the queue to the §5.4 cap, and writes the returned scores
back onto ``news_clusters`` under the §3.2.4 fan-out purity rule. Everything that could widen the
origination surface is decided here, in Python, never by the model:

- **Fan-out purity (§3.2.4, asserted in §9.1).** LLM-emitted ``sectors``/``themes`` are advisory and
  are INTERSECTED with (never a superset of) the RESOLVER's deterministic tags already persisted on
  the cluster. A sector the resolver never tagged therefore fans out to nothing.
- **The LLM never assigns a symbol (§2.7 step 3).** Its ``entities`` are verbatim STRINGS that
  re-enter :meth:`EntityResolver.resolve` as ``extra_texts``; only that resolver's unambiguous,
  in-universe matches are merged into ``symbols``.
- **Fail to zero (§2.7 fail-safe ladder).** A blocked governor, a harness failure or an out-of-enum
  ``event_type`` (D7) all leave the affected clusters UNSCORED, which excludes them from the digest's
  watchlist — `cat` originates nothing. No alert is raised here: the digest owns the ``CATALYST_*``
  alerts, and a scoring gap that never reaches the digest is not an owner-visible condition.

Spec ambiguities resolved here (documented for the integrator):

- The 30-minute age trigger is read from :meth:`BudgetGovernor.news_batch_cadence_min` rather than
  hard-coded, so the §5.6 DG2 row ("news-analyst batching coarsens 30 → 60 min") is actually load-
  bearing instead of decorative. It returns 30 at DG0/DG1, which is the §5.4 default.
- The market-hours leg of the time gate requires a trading day (R6); the 19:00–22:00 sweep does not.
  Weekend/holiday news is eligible for the next session's digest (`cat.max_event_age_days` counts
  SESSIONS), so refusing to score it on a Sunday evening would only shove the whole weekend queue
  into the pre-open batch it is supposed to keep small.
- ``sectors``/``themes`` are intersected against the cluster's PERSISTED step-3 columns, not against
  a fresh ``resolve()`` of the headline: the persisted tags are what the digest's fan-out will
  actually consume, and alias/theme-map state may have drifted since step 3.
- ``entities`` is persisted as the RESOLVER's matched-alias output (a superset of the step-3 value,
  since ``extra_texts`` can only add matches) — the column keeps its §3.2.4 meaning rather than
  becoming a dump of model prose. The model's UNMATCHED strings are logged to
  ``unresolved_entities`` (reason ``no_match``): those are precisely the §5.5 weekly alias-suggestion
  inputs the plan wants from this seam.
- A missing ``news_analyst`` entry in the roster is a construction-time error, not a runtime skip: a
  parked agent means the job should not have been wired at all.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.datafeeds.news_pipeline import EntityResolver, NewsCluster
from engine.intelligence.agents import news_analyst
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentDef, AgentHarness
from engine.intelligence.schemas import ClusterScore
from engine.marketdata.store import MarketStore

_log = get_logger("engine.ops.news_scoring")

#: §5.4 batch trigger: this many queued clusters fire a call regardless of age.
MIN_BATCH_CLUSTERS = 8

#: §5.4 scoring windows (IST wall-clock, inclusive bounds).
MARKET_WINDOW: tuple[time, time] = (time(9, 0), time(15, 30))
SWEEP_WINDOW: tuple[time, time] = (time(19, 0), time(22, 0))

#: The agents.yaml call class for this job — the governor's admission key and the assembler's
#: ``call_class`` on a news context (they must agree, or the audit trail lies about what was asked).
CALL_CLASS = "batch"

SKIP_OUTSIDE_WINDOWS = "outside scoring windows"
SKIP_BATCH_THRESHOLD = "batch threshold not met"
SKIP_GOVERNOR = "governor"


class ScoringResult(BaseModel):
    """One :meth:`NewsScoringJob.run_batch` outcome. Frozen: a job result is evidence (R8).

    ``skipped_reason`` is set ONLY when no SDK call was attempted. A run that called and failed
    mid-queue returns the partial ``scored`` count with no reason — the remaining clusters simply
    stay queued for the next cadence, which is the §2.7 fail-safe behaviour, not an error state.
    """

    model_config = ConfigDict(frozen=True)

    scored: int = 0
    dropped: tuple[str, ...] = ()       # D7 per-item drops (out-of-enum event_type &c) — left UNSCORED
    skipped_reason: str | None = None


class NewsScoringJob:
    """§5.4 batched cluster scoring: queue → Haiku → ``news_clusters`` step-4 columns.

    Parameters
    ----------
    store:
        DuckDB access; every read/write is offloaded through ``arun``/``a*`` (convention 12).
    resolver:
        The step-3 :class:`EntityResolver`, already loaded with alias/sector/theme/universe state.
        It is the ONLY thing allowed to turn the model's entity strings into symbols (§2.7 step 3).
    assembler / harness / agent_defs:
        The §3.2.6 context assembler, the sole SDK caller, and the loaded ``agents.yaml`` roster.
    governor:
        §5.6 admission + the DG2 batching cadence.
    clock / calendar:
        The single "now" and the trading-day fact for the time gate (§3.2 — no naive datetimes).
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
                f"agents.yaml has no enabled {news_analyst.AGENT_ID!r} definition — the §5.4 scoring "
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

    # ------------------------------------------------------------------ run
    async def run_batch(self, *, force: bool = False) -> ScoringResult:
        """Score the queued clusters (§5.4). ``force`` is the PRE-OPEN batch: it ignores both the
        time gate and the ≥8/30-min trigger and scores everything unscored (§5.4 pre-open rule)."""
        now = self._clock.now()
        if not force and not self._in_scoring_window(now):
            return ScoringResult(skipped_reason=SKIP_OUTSIDE_WINDOWS)

        rows = await self._store.arun(self._store.get_news_clusters, scored=False)
        clusters = [NewsCluster.from_row(row) for row in rows]
        if not force and not self._batch_ready(clusters, now):
            return ScoringResult(skipped_reason=SKIP_BATCH_THRESHOLD)
        if not clusters:
            return ScoringResult()      # empty queue: no governor decision, no SDK call, nothing to say

        decision = self._governor.can_invoke(news_analyst.AGENT_ID, CALL_CLASS)
        if not decision.allowed:
            # Fail to ZERO (§2.7): the clusters stay unscored, so the digest excludes them and `cat`
            # originates nothing. No alert here — CATALYST_* belongs to the digest.
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
        for chunk in _chunks(clusters, news_analyst.MAX_CLUSTERS_PER_CALL):
            context = self._assembler.for_news_batch(
                [_prompt_row(c) for c in chunk], theme_vocabulary
            )
            result = await self._harness.run_single_shot(
                self._def,
                context,
                news_analyst.parse_output,
                json_schema=news_analyst.output_json_schema(),
                call_class=CALL_CLASS,
            )
            if not result.ok:
                # Stop at the first failed chunk: the remaining clusters stay queued for the next
                # cadence rather than being re-sent inside a run that is already failing (D7).
                _log.warning(
                    "news_scoring_chunk_failed",
                    reason=result.reason,
                    call_id=result.call_id,
                    scored_so_far=scored,
                    remaining=len(clusters) - scored - len(dropped),
                )
                break
            scores, chunk_dropped = result.payload
            dropped.extend(chunk_dropped)
            scored += await self._write_back(chunk, scores)

        _log.info(
            "news_scoring_batch",
            queued=len(clusters),
            scored=scored,
            dropped=len(dropped),
            force=force,
            model=self._def.model,
        )
        return ScoringResult(scored=scored, dropped=tuple(dropped))

    # ------------------------------------------------------------------ triggers (§5.4)
    def _in_scoring_window(self, now: datetime) -> bool:
        """§5.4: market hours on a trading day, or the evening sweep (see the module's decisions)."""
        t = now.time()
        if SWEEP_WINDOW[0] <= t <= SWEEP_WINDOW[1]:
            return True
        return self._calendar.is_trading_day(now.date()) and MARKET_WINDOW[0] <= t <= MARKET_WINDOW[1]

    def _batch_ready(self, clusters: Sequence[NewsCluster], now: datetime) -> bool:
        """§5.4: ≥8 queued clusters, or the oldest has waited out the (governor-set) cadence."""
        if len(clusters) >= MIN_BATCH_CLUSTERS:
            return True
        if not clusters:
            return False
        cadence = timedelta(minutes=self._governor.news_batch_cadence_min())
        return min(c.first_seen for c in clusters) <= now - cadence

    # ------------------------------------------------------------------ write-back (§3.2.4 purity)
    async def _write_back(
        self, chunk: Sequence[NewsCluster], scores: Sequence[ClusterScore]
    ) -> int:
        """Persist one chunk's scores, matched by ``cluster_id``. Returns the rows written."""
        by_id = {c.cluster_id: c for c in chunk}
        now = self._clock.now()
        rows: list[dict[str, Any]] = []
        for score in scores:
            cluster = by_id.get(score.cluster_id)
            if cluster is None:
                # A cluster_id we did not send: nothing to write it onto, and inventing a row would
                # let the model create a cluster. Ignored, but never silently.
                _log.warning("news_score_unknown_cluster", cluster_id=score.cluster_id)
                continue
            resolved = self._resolver.resolve(cluster, extra_texts=score.entities)
            await self._log_unmatched(cluster, score, resolved)
            rows.append(
                cluster.model_copy(
                    update={
                        "scope": score.scope,
                        "entities": resolved.entities,
                        # The LLM never assigns a symbol (§2.7 step 3): only the resolver's
                        # unambiguous, in-universe matches join the existing set.
                        "symbols": sorted({*(cluster.symbols or []), *resolved.symbols}),
                        # FAN-OUT PURITY (§3.2.4/§9.1): advisory ∩ deterministic, never a superset.
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
        if rows:
            await self._store.aupsert_news_clusters(rows)
        return len(rows)

    async def _log_unmatched(
        self, cluster: NewsCluster, score: ClusterScore, resolved: Any
    ) -> None:
        """Log the model's UNMATCHED entity strings for the §5.5 weekly alias-suggestion loop.

        Only ``no_match`` entries whose text is one of this score's strings are logged: an ambiguous
        alias found in the representative headline was already recorded by step 3, and re-logging it
        on every scoring run would bury the genuinely new suggestions in duplicates.
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


def _chunks(clusters: Sequence[NewsCluster], size: int) -> list[Sequence[NewsCluster]]:
    """Queue split into ≤``size`` batches, oldest first (``get_news_clusters`` orders by first_seen)."""
    return [clusters[i:i + size] for i in range(0, len(clusters), size)]


def _prompt_row(cluster: NewsCluster) -> dict[str, Any]:
    """The ONLY cluster fields the prompt may see (§5.4): identity, recency, corroboration, headline.

    Built explicitly rather than dumping the row, so a future column cannot leak into the prompt —
    in particular the step-4 columns themselves, which would let a re-score anchor on a prior score.
    """
    return {
        "cluster_id": cluster.cluster_id,
        "representative": cluster.representative,
        "source_domains": list(cluster.source_domains),
        "first_seen": cluster.first_seen,
    }
