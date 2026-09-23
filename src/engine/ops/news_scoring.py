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

**Chain-lock discipline (2026-09-09, §2.6 hardening ii).** The caller may hand ``run_batch`` the
``news_chain_lock`` that serialises the ``news_clusters`` read-modify-write against the per-feed
polls. The lock is held for the STORE HOPS ONLY — the initial queue read and each chunk's write-back
— never across the model call: held around the whole batch it also spanned one LLM await per chunk,
and the polls (whose resolve deadline covers lock ACQUISITION) queued behind it (14
``news_resolve_timeout`` on 2026-09-07, nine within one second). Shortening the hold reintroduces the
interleaving the lock existed to prevent, so the write-back is always CONDITIONAL: under the same
hold the FULL unscored rows are re-read and each score is applied to the CURRENT row, never to the
pre-LLM snapshot. Since the upsert overwrites every non-key column, that is what makes a poll merge
landing during the model call (``source_domains``, ``first_seen``/``last_seen``, ``representative``,
``symbols``/``sectors``/``themes``) SURVIVE the write-back rather than be silently reverted; an id
that left the queue meanwhile — merged away, or scored by another batch — is skipped rather than
re-created as a zombie row / clobbered with an older score (drops are logged). ``lock=None`` is not a
distinct code path (2026-09-23): the only production caller always passes ``news_chain_lock``, so the
job now owns a private ``asyncio.Lock()`` for that run when the caller passes none, and the
conditional re-read/write-back above always applies — a caller with nothing to serialise against
still gets the safety, just against no rival.

**Accepted residual.** Chunks 2..N are still prompted from the BATCH's initial snapshot (the queue is
read once, before the first chunk), so a poll merge that lands mid-batch — after its chunk's context
was already built, before that chunk's write-back — yields a score computed without the merged
headline. This is not a regression from the 2026-09-09 change: the whole-batch-hold predecessor had
the identical property (a merge landing after the model saw the prompt is never re-scored; only a
merge landing before write-back can be reverted, and CONDITIONAL write-back is precisely what stops
that). The write-back's stale-skip count (surfaced as ``stale`` on ``news_scoring_batch`` and folded
into ``remaining`` on ``news_scoring_chunk_failed``) makes this interleaving visible rather than
silently absorbed into an over-counted queue.

The batch is also SINGLE-FLIGHTED on a lock the job owns (``_run_lock``): the chain's forced pre-open
batch and the 300 s scoring tick are distinct scheduler jobs, and with the chain lock no longer
spanning the model call they would otherwise read the same queue and bill the same clusters twice. A
queued batch waits rather than skipping — see :meth:`NewsScoringJob.run_batch`.
"""

from __future__ import annotations

import asyncio
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
        # SINGLE-FLIGHT (2026-09-09): see run_batch. Owned by the job, not the caller — the two
        # callers are different scheduler jobs and share nothing else.
        self._run_lock = asyncio.Lock()

    # ------------------------------------------------------------------ run
    async def run_batch(
        self, *, force: bool = False, lock: asyncio.Lock | None = None
    ) -> ScoringResult:
        """Score the queued clusters (§5.4). ``force`` is the PRE-OPEN batch: it ignores both the
        time gate and the ≥8/30-min trigger and scores everything unscored (§5.4 pre-open rule).

        ``lock`` is the caller's news-chain lock (2026-09-09): taken for the queue read and for each
        chunk's write-back, NEVER across the model call — see the module docstring. ``None`` (no
        production caller passes this) makes the job own a private lock for this run instead — the
        conditional re-read/write-back always applies either way (2026-09-23).

        The whole body runs under the job's own ``_run_lock`` (2026-09-09): the chain's forced
        pre-open batch and the 300 s ``scoring_tick`` are DISTINCT scheduler jobs, and now that the
        chain lock is released across the model call nothing else keeps them apart — both would read
        the same unscored queue and send the same clusters to the model twice. It WAITS rather than
        skips: a batch queued behind another re-reads a queue the first one emptied and returns in a
        single store read, whereas skipping would silently drop the tick's legitimate work whenever a
        pre-open batch happened to be in flight (and a skip is indistinguishable from a scoring gap
        downstream). Held OUTSIDE ``lock``, never inside: the chain lock's holders never take this
        one, so the nesting order cannot cycle."""
        async with self._run_lock:
            return await self._run_batch(force=force, lock=lock)

    async def _run_batch(self, *, force: bool, lock: asyncio.Lock | None) -> ScoringResult:
        """One batch, already single-flighted by :meth:`run_batch`. ``lock`` is the caller's chain
        lock; a caller with none (2026-09-23: none in production) still gets one, owned privately for
        this run, so the conditional write-back's safety is unconditional from the job's own view."""
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

            decision = self._governor.can_invoke(news_analyst.AGENT_ID, CALL_CLASS)
            if not decision.allowed:
                # Fail to ZERO (§2.7): the clusters stay unscored, so the digest excludes them and
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
            # OUTSIDE the lock (2026-09-09): one await per chunk, and the per-feed polls' resolve
            # deadline covers lock acquisition — see the module docstring's 09-07 timeouts.
            result = await self._harness.run_single_shot(
                self._def,
                context,
                news_analyst.parse_output,
                json_schema=news_analyst.output_json_schema(),
                call_class=CALL_CLASS,
            )
            if not result.ok:
                # Stop at the first failed chunk: the remaining clusters stay queued for the next
                # cadence rather than being re-sent inside a run that is already failing (D7). Stale
                # ids (merged/rescored away at an earlier chunk's write-back) are subtracted too: they
                # already left the queue, so counting them as still-queued overstates this log's number.
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
    ) -> tuple[int, int]:
        """Persist one chunk's scores, matched by ``cluster_id``. Returns ``(written, stale)``: the
        rows written and how many sent ids had already left the queue by write-back time (merged away
        or scored by a rival batch) — the caller accumulates ``stale`` so its logs don't count an id
        that is no longer queued as still-remaining (see ``_run_batch``'s ``stale_total``).

        Called under the (always-present, 2026-09-23) chain lock: re-reads the FULL unscored rows
        under that SAME hold (2026-09-09) and, for each id we actually sent, applies the step-4 score
        columns onto the CURRENT cluster — never onto the pre-LLM snapshot. Two things ride on that:
        an id no longer in the queue (a poll merged it away, or another batch scored it) is skipped
        rather than resurrected as a zombie row / clobbered with an older score, and — because the
        upsert overwrites every non-key column — a merge that landed during the model call
        (``source_domains``, ``first_seen``/``last_seen``, ``representative``,
        ``symbols``/``sectors``/``themes``) SURVIVES instead of being reverted. The chunk snapshot is
        kept only as the "this id was sent" membership test. Volumes are tiny; the extra read is free.
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
            # The row the score is applied to is the FRESH re-read (the snapshot only ever answers
            # "did we send this id"); everything below — the resolve input, the symbol union, the
            # sector/theme intersection — therefore reads the CURRENT step-2/3 columns.
            cluster = live.get(score.cluster_id)
            if cluster is None:
                stale.append(score.cluster_id)
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
        if stale:
            # Once per chunk, with the ids: a silent drop here would look like a scoring gap.
            _log.warning("news_score_writeback_dropped", count=len(stale), cluster_ids=stale)
        if rows:
            await self._store.aupsert_news_clusters(rows)
        return len(rows), len(stale)

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
