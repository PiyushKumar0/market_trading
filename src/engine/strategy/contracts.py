"""Per-strategy EVALUATION CONTRACTS, rendered into every signal-candidate context (WO-20a, 2026-08-20).

The analyst judges a candidate within its OWN strategy's frame — timeframe class, exit mechanism,
reward basis, evidence status, entry-anchor semantics and the strategy's own disqualifier list —
instead of inside a single intraday day-trade rubric that fits exactly one of the seven legs.

WHY THIS FILE EXISTS (the funnel autopsy, 08-13 -> 08-19). 63/63 analyst evaluations ended
``no_action``, zero proposals were ever created and the §7.1 risk gate has never been invoked. The
declines decomposed into two inputs that lie and one rubric that structurally cannot pass the shapes
the platform validated:

* **E1** — ``features.rel_volume`` is cumulative session volume over the 20d median FULL-DAY volume
  with no time-of-day adjustment, and was read as time-adjusted participation ("roughly 3% of normal
  participation for this time of day"); phantom thinness was cited in 44/63 declines.
* **E2** — ``sentiment_agg`` is a CLIPPED decay-weighted SUM that saturates at ±1.0 once a handful of
  same-direction headlines land, and was read as an extreme regime reading (46/63 declines).
* **E3** — ``rsi2`` (indicator exit) and ``ins`` (time exit) carry ``target=None`` BY DESIGN and were
  declined for "no target ⇒ reward cannot be framed"; 52/63 declines were exactly those two legs,
  one layer above the gate seam (``strategy_expected_edge_pct``) that already handles the case.
* **E4** — swing candidates were judged on intraday microstructure (five 1m bars, VWAP position,
  opening-range completeness at 09:20) — a category error for a 5-20 session hold, and on the one
  validated leg (``ins``) it re-imposed the exact discretionary filter the WO-16 validation removed.

E1/E2 are fixed at render time in :mod:`engine.intelligence.context`; E3/E4 are fixed here plus in
``intraday.SYSTEM_PROMPT`` rule 12/13. Evidence status is stated HONESTLY per leg — six of the seven
are exploratory and say so, which is why the claim cannot live in the shared prompt any more.

PLAIN DATA. No imports beyond the stdlib-free module body itself: this is read by the context
assembler on the hot path and must never drag a heavy module (store, pandas, numba) into an import
graph that does not already have one. Nothing here is a limit, a filter or a gate — the §7.1 gate is
untouched by WO-20 and remains the hard disposal layer.
"""

from __future__ import annotations

#: Returned for a ``strategy_id`` with no registered contract. Never silently empty: an unregistered
#: id is a deployment defect (a new scanner shipped without its frame), and the analyst must be told
#: that the frame it is judging in is missing rather than inferring one.
UNKNOWN_CONTRACT = "UNKNOWN (unregistered strategy_id — evaluate conservatively and flag it)"

_INS = """class: swing (CNC), event-driven. Exit is TIME: hold 20 trading sessions (the validated T+20 horizon). The 5% stop is a DISASTER stop, not the exit.
evidence status: the platform's ONLY validated edge (WO-16: +1.58% net at T+20, CPCV promotable). Validation used UNCONDITIONAL next-open entries with NO price/volume/trend filter. Insider-buy crossings cluster in drawdowns, so a declining tape, below-VWAP price, red daily series, or a day-plan 'avoid' grade on this symbol are the EXPECTED entry population — context, never disqualifiers here. Declining on chart structure re-imposes the exact filter the validation removed.
reward basis: target=None BY DESIGN. Reward is the validated expected edge, which the deterministic gate consumes when target is null. Never decline for a missing target; never invent one.
entry anchor: entry is the crossing session's close — a stale-by-design pre-open REFERENCE for a next-open-style fill (replay determinism; the gate sizes off max(entry, LTP)). entry below the live price does NOT mean 'bidding into weakness'.
valid decline grounds, exhaustively: a fresh materially-negative symbol catalyst of the fraud/default/pledge-invocation class; surveillance flags; the results-day entry ban; evident liquidity collapse; concrete evidence the disclosure itself is suspect. Absent those grounds the validated design expects this candidate to become a proposal — if you decline, name which ground applies."""

#: The C3-cost-check reminder for every leg that ships with NO configured validated edge (rsi2,
#: trend, mom): the gate can only verify C3 economics from an explicit target on these legs, so a
#: targetless enter is rejected as unverifiable regardless of merit. One constant, not three pastes.
_NO_EDGE_GATE_NOTE = (
    "no configured validated edge — the gate verifies C3 economics only from an explicit target; "
    "include a target_price derived from shown levels when proposing, or the enter is gate-rejected "
    "as unverifiable."
)

_RSI2 = f"""class: swing (CNC), 2-5 sessions typical. Exit = RSI(2) recovery or max_hold_days. target=None BY DESIGN — reward is the mean-reversion exit, not a price level; never decline solely for a missing target.
design: fires on SHORT-TERM WEAKNESS in names above their 200-DMA in an uptrending index. Red recent tape, below-VWAP price and a down last-30m drift are the expected entry population. Judge whether the pullback is ORDERLY (routine profit-taking within an intact uptrend) vs DISORDERLY (news-driven break, structural distribution) — not whether the tape is red.
intraday microstructure (opening-range completeness, session participation, VWAP position) is context, NOT disqualifying for a multi-session hold. A day-plan 'avoid' grade that merely restates 'this symbol fell recently' does not bind a counter-trend entry; a plan warning citing a concrete adverse catalyst does.
evidence status: exploratory — no validated net edge at retail costs. Be selective on reversion quality (uptrend intact, orderly pullback, sane stop distance vs the cost floor), within THIS frame.
gate note (2026-08-26): rsi2 carries {_NO_EDGE_GATE_NOTE} Declining still never requires a target."""

_ORB = """class: intraday (MIS), same-day squareoff; the rule supplies a price target. The intraday frame FULLY applies: participation, VWAP, acceptance above the range, the day plan's read — judge exactly as an intraday breakout trade.
evidence status: exploratory — repeatedly tested gross-negative at retail costs in this book; demand exceptional quality."""

_TREND = f"""class: positional (CNC), up to 120 sessions; exit = trailing ATR stop. target=None BY DESIGN — never decline solely for a missing target. Judge on the DAILY series (cross validity, ADX regime, structure); a single session's intraday tape is context only.
evidence status: exploratory — no validated net edge at retail costs.
gate note (2026-08-26): {_NO_EDGE_GATE_NOTE}"""

_MOM = f"""class: swing (CNC) on a rebalance cadence; exit = the next rebalance. target=None BY DESIGN — never decline solely for a missing target. Judge relative-strength quality on the daily series; intraday microstructure is context only.
evidence status: exploratory — momentum entries repeatedly tested net-negative at retail costs; demand exceptional quality.
gate note (2026-08-26): {_NO_EDGE_GATE_NOTE}"""

_BRK20 = """class: swing (CNC). Entry is LIMIT-AT-LEVEL: the broken 20d-high level itself, BELOW the current price by design — a retest fill, not a chase and not 'bidding into weakness'. Stop geometry is pre-vetted upstream (overnight-gap floor). Judge breakout validity on the DAILY series (fresh cross, volume confirmation, margin), with the intraday tape as context.
evidence status: exploratory — no expectancy presumption; originates for judgement."""

_CAT = """class: swing (CNC), long-only, news-catalyst drift. Exit is TIME (the §7.1 max-holding path, 20 trading sessions); the 5% stop is a DISASTER stop, not the exit. target=None BY DESIGN — never decline solely for a missing target, and never invent one (see the gate note).
design: a pure EOD-batch translation of today's 'originating' catalyst_watchlist rows, mirroring ins — NOT a per-bar scanner. The intraday price/volume confirmation the v1 rule required was RETIRED (WO-18, 2026-08-18): it refuted 3x on proxies and the drift this rule targets is a 2-4-week phenomenon an intraday trigger cannot express. Judge the STORY (materiality, source class, novelty) on the digest's own read; the intraday tape at admission time is not evidence for this rule.
entry anchor: entry is the prior session's close — a stale-by-design pre-open REFERENCE for a next-open-style fill (replay determinism; the gate sizes off max(entry, LTP)). entry below the live price does NOT mean 'bidding into weakness'.
evidence status: exploratory, SHADOW — no validated edge; measuring one is the entire point (WO-18 verdict criteria: >=30 sessions / >=20 signals, T+10/T+20 net-drift study).
gate note (2026-08-28): cat is registered as a no-edge SHADOW strategy, so the deterministic gate rejects EVERY proposal at the C3 cost check whatever target it carries. Your evaluation is recorded as validation evidence and cannot become a recommendation. Judge it honestly on its merits anyway — a thesis written to flatter a rule that cannot trade only corrupts the study."""

_CAT_REVERSAL = """class: swing (CNC), long-only, news-catalyst REVERSAL. Exit is TIME (the §7.1 max-holding path); the 5% stop is a DISASTER stop, not the exit. target=None BY DESIGN — never decline solely for a missing target, and never invent one (see the gate note).
design: the event is NOT 'good news arrived'. This story ESTABLISHED a bearish claim first (a materially-scored, opposite-direction cluster of the same symbol+event_type), and the winning cluster REVERSES it — a denial, refutation or withdrawal that resolves a known uncertainty. The motivating case: a government stake-sale story on HINDZINC, officially denied by DIPAM the next day, +5.1%. Judge whether the reversal is CREDIBLE and AUTHORITATIVE (an official/primary-source denial of a specific prior claim) rather than a second opinion, a partial walk-back, or an unsourced rebuttal — that distinction is the whole thesis and it is the one thing you can judge that the deterministic layer cannot.
entry anchor: entry is the prior session's close — a stale-by-design pre-open REFERENCE for a next-open-style fill (replay determinism; the gate sizes off max(entry, LTP)). Note the reversal day's own repricing is already in that close: this rule targets the RESIDUAL drift, not the jump.
evidence status: exploratory, SHADOW — no validated edge; measuring one is the entire point. Pre-registered horizons T+5/T+10.
gate note (2026-08-27): cat_reversal is registered as a no-edge SHADOW strategy, so the deterministic gate rejects EVERY proposal at the C3 cost check whatever target it carries. Your evaluation is recorded as validation evidence and cannot become a recommendation. Judge it honestly on its merits anyway — a thesis written to flatter a rule that cannot trade only corrupts the study."""

#: ``strategy_id`` -> the contract rendered into that candidate's context block. The keys are exactly
#: the §6.1 deterministic legs; a new leg MUST land here in the same commit that ships it, or its
#: candidates reach the analyst with :data:`UNKNOWN_CONTRACT` and a warning in the log.
STRATEGY_CONTRACTS: dict[str, str] = {
    "ins": _INS,
    "rsi2": _RSI2,
    "orb": _ORB,
    "trend": _TREND,
    "mom": _MOM,
    "brk20": _BRK20,
    "cat": _CAT,
    "cat_reversal": _CAT_REVERSAL,
}


def contract_text(strategy_id: str) -> str:
    """The evaluation contract for ``strategy_id``, or :data:`UNKNOWN_CONTRACT` for an unknown id.

    Total by construction — a context assembly must never fail on an unregistered strategy (D7 fail
    to zero): a thinner frame is a worse call, a raised exception is a lost one.
    """
    return STRATEGY_CONTRACTS.get(strategy_id, UNKNOWN_CONTRACT)


__all__ = ["STRATEGY_CONTRACTS", "UNKNOWN_CONTRACT", "contract_text"]
