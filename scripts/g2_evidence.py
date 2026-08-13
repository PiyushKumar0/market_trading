"""Gate G2 evidence collector (IMPLEMENTATION_PLAN.md §8.3).

Computes the machine-checkable half of the Phase-2 acceptance gate and prints one line per
criterion with a MET / NOT-MET / N-A verdict against the §8.3 bars. The owner-only items
(manual executions, watchlist-precision reviews, broker order-book audit) are surfaced as
N-A rows so the checklist in ``runbooks/RUNBOOK.md`` stays 1:1 with the gate text.

SAFETY — this script runs against the LIVE engine's data:

* ``data/state.db`` is opened through a ``file:...?mode=ro`` URI. Read-only at the SQLite
  layer, so a bug here cannot write, migrate, or lock out the running engine.
* ``data/market.duckdb`` is NEVER touched (single-writer; the engine holds it). Every number
  below comes from ``state.db`` + committed config.
* No engine package import — stdlib only (sqlite3/json/argparse/pathlib/datetime/re). The
  engine's composition root has import side effects; this tool must stay inert.
* Engine logs are NOT parsed: every metric has a first-class DB record (see SOURCES below),
  and the daily logs run to hundreds of MB. Log events are named in the source notes only as
  the manual fallback if a DB row is ever missing.

SOURCES (stated again in the printed output, per criterion):

* digest timing       -> ``job_runs`` where ``job_id='catalyst_digest'`` (last_success_at)
* trade-window open   -> ``config_audit`` where ``name='trade_window_state'`` (diff JSON history),
                         falling back to the current ``trade_window_state`` row
* analyst validity    -> ``agent_calls`` (one row per SDK call, retries included)
* recommendations     -> ``recommendations`` (delivered_at, human_action, payload JSON)
* platform orders     -> ``orders`` (broker-side confirmation stays owner-manual)
* budget              -> ``budget_ledger`` vs ``config/agents.yaml: budget_allocations_usd``
* trading sessions    -> ``config/calendar/<year>.yaml`` holidays + weekday rule

Usage::

    .venv\\Scripts\\python.exe scripts\\g2_evidence.py
    .venv\\Scripts\\python.exe scripts\\g2_evidence.py --from 2026-07-29 --to 2026-08-13
    .venv\\Scripts\\python.exe scripts\\g2_evidence.py --json data/reports/g2_evidence.json
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from bisect import bisect_right
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30), "IST")
REPO = Path(__file__).resolve().parent.parent

# §8.3 bars.
BAR_DIGEST_PCT = 90.0        # catalyst digest before window-open on >=90% of sessions
BAR_NEWS_SCHEMA_PCT = 95.0   # News Analyst batches schema-valid on >=95% of calls
BAR_DELIVERY_PCT = 90.0      # >=90% of recommendations delivered before validity opens
BAR_TAKEN_MIN = 5            # owner has executed >=5 recommendations manually
BAR_SESSIONS_MIN = 20        # "4 weeks of daily recommendations" ~ 20 trading sessions

MET, NOT_MET, NA = "MET", "NOT-MET", "N-A"


# --------------------------------------------------------------------------- helpers
def parse_ts(raw: str | None) -> datetime | None:
    """ISO-8601 (with offset) -> aware datetime in IST. Returns None on anything unparseable."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if dt.tzinfo is None:                     # defensive: the platform never writes naive (§9.1)
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def parse_hhmm(raw: str | None) -> time | None:
    if not raw:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", str(raw).strip())
    return time(int(m.group(1)), int(m.group(2))) if m else None


def pct(num: int, den: int) -> float | None:
    return None if den == 0 else 100.0 * num / den


def fmt_pct(v: float | None) -> str:
    return "n/a" if v is None else f"{v:.1f}%"


def verdict_pct(v: float | None, bar: float) -> str:
    return NA if v is None else (MET if v >= bar else NOT_MET)


# --------------------------------------------------------------------------- calendar
def load_holidays(years: set[int]) -> tuple[set[date], list[str]]:
    """Holiday dates from ``config/calendar/<year>.yaml``.

    Deliberately a targeted line scan rather than a YAML dependency: the block is a flat list of
    ``- { date: "YYYY-MM-DD", name: ... }`` rows, and this tool stays stdlib-only. Muhurat days
    live in ``special_sessions`` and are NOT counted as sessions here (weekend dates).
    """
    holidays: set[date] = set()
    notes: list[str] = []
    for year in sorted(years):
        path = REPO / "config" / "calendar" / f"{year}.yaml"
        if not path.exists():
            notes.append(f"calendar {year}.yaml MISSING - sessions for {year} are weekday-only")
            continue
        in_block = False
        found = 0
        verified = None
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if re.match(r"^verified:\s*", line):
                verified = line.split(":", 1)[1].strip().split("#")[0].strip()
            if re.match(r"^holidays:\s*$", line):
                in_block = True
                continue
            if in_block:
                if line and not line[0].isspace() and not line.lstrip().startswith("-"):
                    in_block = False
                    continue
                m = re.search(r'date:\s*"?(\d{4}-\d{2}-\d{2})"?', line)
                if m:
                    holidays.add(date.fromisoformat(m.group(1)))
                    found += 1
        notes.append(f"calendar {year}.yaml: {found} holidays, verified={verified}")
    return holidays, notes


def trading_sessions(d_from: date, d_to: date) -> tuple[list[date], list[str]]:
    holidays, notes = load_holidays({d_from.year, d_to.year})
    out: list[date] = []
    cur = d_from
    while cur <= d_to:
        if cur.weekday() < 5 and cur not in holidays:
            out.append(cur)
        cur += timedelta(days=1)
    return out, notes


# --------------------------------------------------------------------------- window history
class WindowHistory:
    """``config_audit`` history of ``trade_window_state`` -> "window in force at instant t"."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.rows: list[tuple[datetime, time, int]] = []   # (effective_from, start, audit id)
        self.unparsed = 0
        for rid, diff, at in conn.execute(
            "SELECT id, diff, at FROM config_audit WHERE name='trade_window_state' ORDER BY at"
        ):
            ts, start = parse_ts(at), None
            try:
                start = parse_hhmm((json.loads(diff or "{}") or {}).get("start"))
            except (ValueError, AttributeError, TypeError):
                start = None
            if ts is None or start is None:
                self.unparsed += 1
                continue
            self.rows.append((ts, start, int(rid)))
        self._keys = [r[0] for r in self.rows]

        cur = conn.execute(
            "SELECT start_ist, changed_at FROM trade_window_state WHERE id=1"
        ).fetchone()
        self.current_start = parse_hhmm(cur[0]) if cur else None
        self.current_changed_at = parse_ts(cur[1]) if cur else None

    def at(self, t: datetime) -> tuple[time | None, str]:
        """The window ``start`` in force at instant ``t``, plus a provenance tag.

        Tags: ``audit#<id>`` = a real history row at-or-before ``t``; ``assumed:first-known`` =
        ``t`` predates the whole ``config_audit`` history (the seeded value is not recorded
        anywhere, so the earliest known start is used and the day is flagged); ``current`` =
        no history at all, so the live ``trade_window_state`` row is used.
        """
        if not self.rows:
            return self.current_start, "current"
        i = bisect_right(self._keys, t)
        if i == 0:
            return self.rows[0][1], "assumed:first-known"
        ts, start, rid = self.rows[i - 1]
        return start, f"audit#{rid}"

    def changed_again_on(self, d: date, after: datetime) -> bool:
        return any(ts.date() == d and ts > after for ts, _, _ in self.rows)


# --------------------------------------------------------------------------- criteria
def criterion_digest(conn, sessions, wh: WindowHistory) -> dict[str, Any]:
    """C1: catalyst digest produced BEFORE that day's trade-window open, per session."""
    digests: dict[date, tuple[datetime | None, str | None]] = {}
    for run_for, last_ok, status in conn.execute(
        "SELECT run_for_date, last_success_at, status FROM job_runs WHERE job_id='catalyst_digest'"
    ):
        try:
            digests[date.fromisoformat(str(run_for))] = (parse_ts(last_ok), status)
        except ValueError:
            continue

    rows, ok = [], 0
    for d in sessions:
        got = digests.get(d)
        if got is None or got[0] is None:
            rows.append({"date": d.isoformat(), "digest_at": None, "window_open": None,
                         "source": "no job_runs row", "before_open": False, "late_change": False})
            continue
        ts, _status = got
        start, src = wh.at(ts)
        if start is None:
            rows.append({"date": d.isoformat(), "digest_at": ts.isoformat(), "window_open": None,
                         "source": src, "before_open": False, "late_change": False})
            continue
        open_at = datetime.combine(d, start, tzinfo=IST)
        before = ts < open_at
        ok += 1 if before else 0
        rows.append({
            "date": d.isoformat(), "digest_at": ts.isoformat(),
            "window_open": open_at.isoformat(), "source": src, "before_open": before,
            "late_change": wh.changed_again_on(d, ts),
        })
    return {"sessions": len(sessions), "before_open": ok, "pct": pct(ok, len(sessions)),
            "detail": rows, "unparsed_audit_rows": wh.unparsed}


def criterion_schema(conn, d_from: date, d_to: date) -> dict[str, Any]:
    """C2: analyst schema-validity, split intraday-analyst vs news-analyst."""
    lo, hi = d_from.isoformat(), d_to.isoformat() + "T99"   # ASCII upper bound on ISO stamps
    out: dict[str, Any] = {"agents": {}}
    for agent in ("intraday_analyst", "news_analyst"):
        total = si = timeouts = sdk_err = 0
        for fail_reason, n in conn.execute(
            "SELECT fail_reason, COUNT(*) FROM agent_calls "
            "WHERE agent_id=? AND at>=? AND at<=? GROUP BY fail_reason", (agent, lo, hi)
        ):
            total += n
            if fail_reason == "schema_invalid":
                si = n
            elif fail_reason == "timeout":
                timeouts = n
            elif fail_reason == "sdk_error":
                sdk_err = n
        returned = total - timeouts - sdk_err            # calls that actually produced text
        out["agents"][agent] = {
            "calls": total, "schema_invalid": si, "timeouts": timeouts, "sdk_errors": sdk_err,
            "valid_pct_all_calls": pct(total - si, total),
            "valid_pct_of_returned": pct(returned - si, returned),
        }
    row = conn.execute("SELECT MIN(at), MAX(at), COUNT(*) FROM agent_calls").fetchone()
    out["coverage"] = {"first_row": row[0], "last_row": row[1], "rows_total": row[2]}
    # Like-for-like undercount check: ledger rows for the SAME two agents over the SAME range.
    out["ledger_rows_in_range"] = conn.execute(
        "SELECT COUNT(*) FROM budget_ledger WHERE at>=? AND at<=? "
        "AND agent_id IN ('intraday_analyst','news_analyst')", (lo, hi)
    ).fetchone()[0]
    return out


def criterion_recs(conn, d_from: date, d_to: date) -> dict[str, Any]:
    """C3/C4: delivery timeliness + owner-executed outcomes."""
    lo, hi = d_from.isoformat(), d_to.isoformat() + "T99"   # ASCII upper bound on ISO stamps
    delivered = in_time = late = unknown = 0
    session_days: set[str] = set()
    for delivered_at, payload in conn.execute(
        "SELECT delivered_at, payload FROM recommendations WHERE delivered_at>=? AND delivered_at<=?",
        (lo, hi),
    ):
        delivered += 1
        dt = parse_ts(delivered_at)
        if dt:
            session_days.add(dt.date().isoformat())
        try:
            body = json.loads(payload or "{}")
        except ValueError:
            body = {}
        valid_until = parse_ts(body.get("valid_until"))
        if dt is None or valid_until is None:
            unknown += 1
        elif dt < valid_until:
            in_time += 1
        else:
            late += 1

    actions = {k: 0 for k in ("taken", "closed", "dismissed", "expired", "open")}
    for human_action, n in conn.execute(
        "SELECT human_action, COUNT(*) FROM recommendations GROUP BY human_action"
    ):
        actions[human_action or "open"] = actions.get(human_action or "open", 0) + n

    return {
        "delivered_in_range": delivered, "delivered_before_validity_closes": in_time,
        "delivered_after_validity_closed": late, "timestamps_unusable": unknown,
        "pct_in_time": pct(in_time, delivered),
        "sessions_with_a_recommendation": len(session_days),
        "human_actions_all_time": actions,
        "total_rows_all_time": conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0],
    }


def criterion_orders(conn) -> dict[str, Any]:
    """§8.3 'zero API orders placed' - the platform-side half (broker side is owner-manual)."""
    total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    live = conn.execute("SELECT COUNT(*) FROM orders WHERE is_paper=0").fetchone()[0]
    return {"order_rows": total, "live_order_rows": live,
            "order_events": conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0]}


def load_budget_config() -> dict[str, Any]:
    """``budget_allocations_usd`` + ``llm.monthly_credit_usd`` from config/agents.yaml (line scan)."""
    path = REPO / "config" / "agents.yaml"
    allocations: dict[str, float] = {}
    credit = None
    if not path.exists():
        return {"allocations": allocations, "monthly_credit_usd": credit, "path": str(path),
                "error": "config/agents.yaml not found"}
    in_block = False
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = re.match(r"^\s{2}monthly_credit_usd:\s*([0-9.]+)", line)
        if m:
            credit = float(m.group(1))
        if re.match(r"^budget_allocations_usd:\s*$", line):
            in_block = True
            continue
        if in_block:
            m = re.match(r"^\s+([A-Za-z_][A-Za-z0-9_]*):\s*([0-9.]+)", line)
            if m:
                allocations[m.group(1)] = float(m.group(2))
            elif line.strip() and not line.startswith((" ", "\t", "#")):
                in_block = False
    return {"allocations": allocations, "monthly_credit_usd": credit, "path": str(path)}


def criterion_budget(conn, month: str) -> dict[str, Any]:
    """C5: month-to-date ledger spend per agent vs allocation + the single console-diff total."""
    cfg = load_budget_config()
    spend: dict[str, dict[str, Any]] = {}
    total = 0.0
    for agent_id, n, cost in conn.execute(
        "SELECT agent_id, COUNT(*), SUM(CAST(cost_usd AS REAL)) FROM budget_ledger "
        "WHERE month=? GROUP BY agent_id ORDER BY agent_id", (month,)
    ):
        c = float(cost or 0.0)
        total += c
        spend[agent_id or "(null)"] = {"calls": n, "spend_usd": c}
    for agent_id, alloc in cfg["allocations"].items():
        if agent_id == "reserve":
            continue
        spend.setdefault(agent_id, {"calls": 0, "spend_usd": 0.0})
    for agent_id, rec in spend.items():
        alloc = cfg["allocations"].get(agent_id)
        rec["alloc_usd"] = alloc
        rec["pct_of_alloc"] = pct(int(round(rec["spend_usd"] * 1e6)), int(round(alloc * 1e6))) \
            if alloc else None
    months = [r[0] for r in conn.execute(
        "SELECT DISTINCT month FROM budget_ledger ORDER BY month")]
    return {"month": month, "per_agent": spend, "total_usd": total,
            "allocations": cfg["allocations"], "monthly_credit_usd": cfg["monthly_credit_usd"],
            "alloc_sum_usd": sum(cfg["allocations"].values()) or None,
            "ledger_months": months}


# --------------------------------------------------------------------------- rendering
def table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip(),
           "  ".join("-" * widths[i] for i in range(len(headers)))]
    for r in rows:
        out.append("  ".join(c.ljust(widths[i]) for i, c in enumerate(r)).rstrip())
    return "\n".join(out)


def render(res: dict[str, Any]) -> str:
    L: list[str] = []
    meta, dg, sc = res["meta"], res["digest"], res["schema"]
    rec, orders, bud = res["recommendations"], res["orders"], res["budget"]

    L.append("=" * 100)
    L.append("Gate G2 evidence - IMPLEMENTATION_PLAN.md 8.3 (Phase 2, RECOMMEND live)")
    L.append("=" * 100)
    L.append(f"Range        : {meta['from']} .. {meta['to']}  ({meta['sessions']} trading sessions)")
    L.append(f"State DB     : {meta['db']}")
    L.append(f"DB open mode : {meta['db_uri']}   (READ-ONLY; data/market.duckdb never opened)")
    L.append(f"Generated at : {meta['generated_at']}")
    L.append("")

    news = sc["agents"]["news_analyst"]
    intra = sc["agents"]["intraday_analyst"]
    rows = [
        ["1", "Catalyst digest before window-open",
         f"{dg['before_open']}/{dg['sessions']} sessions = {fmt_pct(dg['pct'])}",
         ">= 90% of sessions", verdict_pct(dg["pct"], BAR_DIGEST_PCT)],
        ["2a", "News Analyst schema-valid",
         f"{news['calls'] - news['schema_invalid']}/{news['calls']} calls = "
         f"{fmt_pct(news['valid_pct_all_calls'])}",
         ">= 95% of calls", verdict_pct(news["valid_pct_all_calls"], BAR_NEWS_SCHEMA_PCT)],
        ["2b", "Intraday Analyst schema-valid",
         f"{intra['calls'] - intra['schema_invalid']}/{intra['calls']} calls = "
         f"{fmt_pct(intra['valid_pct_all_calls'])}",
         "(no 8.3 bar - context)", NA],
        ["3", "Recs delivered before validity closes",
         f"{rec['delivered_before_validity_closes']}/{rec['delivered_in_range']} = "
         f"{fmt_pct(rec['pct_in_time'])}",
         ">= 90% delivered in time", verdict_pct(rec["pct_in_time"], BAR_DELIVERY_PCT)],
        ["4", "Owner-executed recommendations",
         f"taken={rec['human_actions_all_time'].get('taken', 0)} "
         f"closed={rec['human_actions_all_time'].get('closed', 0)}",
         ">= 5 taken manually",
         MET if rec["human_actions_all_time"].get("taken", 0) >= BAR_TAKEN_MIN else NOT_MET],
        ["5", "Duration: sessions with >=1 recommendation",
         f"{rec['sessions_with_a_recommendation']}/{dg['sessions']} sessions",
         ">= 20 sessions (4 weeks)",
         MET if rec["sessions_with_a_recommendation"] >= BAR_SESSIONS_MIN else NOT_MET],
        ["6", "Zero API orders (platform side)",
         f"orders={orders['order_rows']} live={orders['live_order_rows']} "
         f"events={orders['order_events']}",
         "0 rows; broker book owner-audited",
         MET if orders["live_order_rows"] == 0 else NOT_MET],
        ["7", f"Budget MTD {bud['month']} (D6, re-scoped 2026-08-13)",
         f"total ${bud['total_usd']:.2f} vs alloc ${bud['alloc_sum_usd'] or 0:.0f}",
         "ledger arithmetic + within self-imposed alloc",
         MET if bud["alloc_sum_usd"] and bud["total_usd"] <= bud["alloc_sum_usd"] else NA],
        ["8", "Watchlist-precision reviews (2 weekly)", "[owner-manual]",
         "2 weekly samples reviewed", NA],
        ["9", "Payload completeness (gate/cost/checklist)",
         "[owner-manual] - no rec payloads to sample" if rec["total_rows_all_time"] == 0
         else f"[owner-manual] - sample from {rec['total_rows_all_time']} rows",
         "every payload", NA],
    ]
    L.append(table(["#", "G2 CRITERION", "MEASURED", "BAR", "VERDICT"], rows))
    L.append("")

    L.append("-" * 100)
    L.append("C1 detail - catalyst digest vs trade-window open, per session")
    L.append("-" * 100)
    drows = [[
        r["date"],
        (r["digest_at"] or "-- NOT PRODUCED --")[:19],
        (r["window_open"] or "unknown")[11:16] if r["window_open"] else "unknown",
        r["source"],
        "before" if r["before_open"] else "AFTER/none",
        "yes" if r["late_change"] else "",
    ] for r in dg["detail"]]
    L.append(table(["SESSION", "DIGEST last_success_at", "OPEN", "WINDOW SRC",
                    "RESULT", "WINDOW RESET LATER SAME DAY"], drows))
    L.append("")

    L.append("-" * 100)
    L.append(f"C5/C7 detail - budget ledger month-to-date ({bud['month']})")
    L.append("-" * 100)
    brows = []
    for agent in sorted(bud["per_agent"]):
        r = bud["per_agent"][agent]
        brows.append([agent, str(r["calls"]), f"{r['spend_usd']:.4f}",
                      "-" if r["alloc_usd"] is None else f"{r['alloc_usd']:.0f}",
                      fmt_pct(r["pct_of_alloc"])])
    brows.append(["TOTAL (self-imposed budget)", "", f"{bud['total_usd']:.4f}",
                  f"{bud['alloc_sum_usd'] or 0:.0f}",
                  fmt_pct(pct(int(round(bud["total_usd"] * 1e6)),
                              int(round((bud["alloc_sum_usd"] or 0) * 1e6))))])
    L.append(table(["AGENT", "BILLED CALLS", "SPEND USD", "ALLOC USD", "% OF ALLOC"], brows))
    L.append("")
    L.append("  >> D6 RE-SCOPED (owner clarification 2026-08-13): SDK usage bills against the Claude")
    L.append("     subscription's WEEKLY usage limits, not a monthly credit (Anthropic's June-15 notice")
    L.append("     paused the credit change) - there is NO console dollar figure to reconcile against.")
    L.append("     The dollar ledger above is the platform's SELF-IMPOSED budget (DG ladder input);")
    L.append("     the check is ledger arithmetic + staying within the self-imposed allocations.")
    L.append(f"  Self-imposed monthly budget configured: ${bud['monthly_credit_usd']}. "
             f"Ledger months present: {', '.join(bud['ledger_months'])}.")
    L.append("")

    L.append("-" * 100)
    L.append("SOURCES + COVERAGE CAVEATS (read before quoting any number above)")
    L.append("-" * 100)
    L.append("C1 digest time  : job_runs(job_id='catalyst_digest').last_success_at - a DB record, so")
    L.append("                  engine logs were NOT parsed. Caveat: job_runs is upserted per")
    L.append("                  (job_id, run_for_date), so this is the LAST success for that date, not")
    L.append("                  necessarily the first; a same-day re-run overwrites an earlier time.")
    L.append("                  Log fallback if a row is ever missing: event 'catalyst_digest' in")
    L.append("                  data/logs/engine.log[.YYYY-MM-DD] (fields d/clusters/n_originating).")
    L.append("C1 window open  : config_audit(name='trade_window_state').diff JSON 'start', taking the")
    L.append("                  value IN FORCE AT THE DIGEST INSTANT (the WINDOW SRC column names the")
    L.append("                  exact audit row id used). The owner re-set the window mid-session on")
    L.append("                  many days - rows flagged in the last column had a LATER same-day reset,")
    L.append("                  so judging them against a different value is defensible; re-read the")
    L.append("                  detail table before signing off. 'assumed:first-known' = the digest")
    L.append("                  predates the whole config_audit history (the seeded window is recorded")
    L.append("                  nowhere), so the earliest known start was used.")
    L.append(f"                  Current sticky window: start={_hhmm(res['window']['current_start'])} "
             f"(set {res['window']['current_changed_at']}).")
    L.append("C2 schema rate  : agent_calls - ONE ROW PER SDK CALL, RETRIES INCLUDED (harness.py")
    L.append("                  _persist docstring), so a schema_invalid attempt that succeeded on")
    L.append("                  retry still counts against the rate. This is the conservative reading.")
    L.append(f"                  Coverage: {sc['coverage']['rows_total']} rows, "
             f"{(sc['coverage']['first_row'] or '?')[:19]} .. {(sc['coverage']['last_row'] or '?')[:19]}.")
    L.append("                  ALL DAYS IN RANGE ARE DB-BASED - no log-derived days, nothing lost to")
    L.append("                  log rotation. budget_ledger was NOT used for call counts: the ledger")
    L.append("                  only ever sees calls that SUCCEEDED (harness.py comment at DG4), so it")
    L.append(f"                  undercounts the two analysts ({sc['ledger_rows_in_range']} ledger rows "
             f"vs {intra['calls'] + news['calls']} agent_calls rows, same agents + range).")
    L.append(f"                  Excluding timeouts/sdk_errors from the denominator instead: "
             f"news={fmt_pct(news['valid_pct_of_returned'])}, "
             f"intraday={fmt_pct(intra['valid_pct_of_returned'])}.")
    L.append("C3/C4 recs      : recommendations(delivered_at, payload, human_action). The payload has")
    L.append("                  created_at + valid_until and NO valid_from (core/contracts.py:178), so")
    L.append("                  'delivered before the validity window opens' is measured as delivered")
    L.append("                  strictly before valid_until - the window opens at created_at, which is")
    L.append("                  <= delivered_at by construction in RecommendationBook.deliver().")
    L.append(f"                  Rows: {rec['total_rows_all_time']} all-time, "
             f"{rec['delivered_in_range']} delivered in range, "
             f"{rec['timestamps_unusable']} with unusable timestamps.")
    L.append(f"                  human_action tally (all time): "
             f"{json.dumps(rec['human_actions_all_time'], sort_keys=True)}.")
    L.append("C6 orders       : orders / order_events tables. This proves the PLATFORM placed nothing;")
    L.append("                  the 'broker order book empty' half of 8.3 is an owner-manual Kite")
    L.append("                  Console audit - the engine cannot evidence its own absence there.")
    L.append("C5/C7 budget    : budget_ledger(month) vs config/agents.yaml budget_allocations_usd.")
    L.append("                  Allocations include a 'reserve' line that no agent spends against.")
    L.append("                  D6 re-scoped 2026-08-13: SDK usage bills against subscription weekly")
    L.append("                  usage limits, not a monthly credit - no console dollar reconciliation")
    L.append("                  exists; the bar is ledger arithmetic + self-imposed allocation adherence.")
    L.append("Sessions        : " + " | ".join(meta["calendar_notes"]))
    L.append("")
    return "\n".join(L)


def _hhmm(v: str | None) -> str:
    return v or "unset"


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    today = datetime.now(IST).date()
    ap = argparse.ArgumentParser(
        description="Gate G2 evidence (IMPLEMENTATION_PLAN.md 8.3). Read-only against state.db.")
    ap.add_argument("--from", dest="date_from", default="2026-07-29",
                    help="range start, YYYY-MM-DD (default 2026-07-29, first RECOMMEND session)")
    ap.add_argument("--to", dest="date_to", default=today.isoformat(),
                    help="range end, YYYY-MM-DD (default today)")
    ap.add_argument("--db", default=str(REPO / "data" / "state.db"), help="path to state.db")
    ap.add_argument("--month", default=None,
                    help="budget month key YYYY-MM (default: the month of --to)")
    ap.add_argument("--json", dest="json_path", default=None,
                    help="also dump the raw numbers to this JSON path (default: off)")
    args = ap.parse_args(argv)

    d_from, d_to = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    if d_to < d_from:
        ap.error("--to is before --from")
    month = args.month or d_to.strftime("%Y-%m")

    db = Path(args.db).resolve()
    if not db.exists():
        print(f"ERROR: state.db not found at {db}", file=sys.stderr)
        return 2
    uri = f"file:{db.as_posix()}?mode=ro"

    sessions, cal_notes = trading_sessions(d_from, d_to)
    conn = sqlite3.connect(uri, uri=True)
    try:
        wh = WindowHistory(conn)
        res = {
            "meta": {
                "from": d_from.isoformat(), "to": d_to.isoformat(), "sessions": len(sessions),
                "session_dates": [d.isoformat() for d in sessions],
                "db": str(db), "db_uri": uri, "calendar_notes": cal_notes,
                "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
                "bars": {"digest_pct": BAR_DIGEST_PCT, "news_schema_pct": BAR_NEWS_SCHEMA_PCT,
                         "delivery_pct": BAR_DELIVERY_PCT, "taken_min": BAR_TAKEN_MIN,
                         "sessions_min": BAR_SESSIONS_MIN},
            },
            "window": {
                "current_start": wh.current_start.strftime("%H:%M") if wh.current_start else None,
                "current_changed_at": wh.current_changed_at.isoformat()
                if wh.current_changed_at else None,
                "audit_rows": len(wh.rows), "unparsed_audit_rows": wh.unparsed,
            },
            "digest": criterion_digest(conn, sessions, wh),
            "schema": criterion_schema(conn, d_from, d_to),
            "recommendations": criterion_recs(conn, d_from, d_to),
            "orders": criterion_orders(conn),
            "budget": criterion_budget(conn, month),
        }
    finally:
        conn.close()

    print(render(res))

    if args.json_path:
        out = Path(args.json_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(res, indent=2, sort_keys=True), encoding="utf-8")
        print(f"raw numbers written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
