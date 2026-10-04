"""Replay live analyst prompts against a candidate model / effort / CLI and compare with production.

Answers "what changes if the analysts run on X?" with the engine's own prompts and answers instead of
synthetic ones. Two steps:

  1. ``sample`` pins a sample set: the newest live engine calls per agent, read from the Agent SDK
     transcripts (``~/.claude/projects/<repo>/*.jsonl``, entrypoint ``sdk-py``). The first user message
     is the assembled prompt; the ``StructuredOutput`` tool input is the live answer. Replays write
     sdk-py transcripts into the same folder, so only the earliest transcript of each prompt counts as
     live, and the chosen set is saved by file name for every run to reuse.
  2. ``run`` replays the set. Options come from the engine's own harness and ``config/agents.yaml``
     (model, effort, pinned CLI, schema, allowlist), so ``baseline`` is exactly production; the
     candidate is production with ``--model`` / ``--effort`` / ``--cli-path`` overridden. Each answer is
     validated with the agent's real parser and compared with the live answer and the baseline.

Every call draws on the subscription quota the live engine uses, so ``run`` refuses during market hours
on trading days (CLAUDE.md, Operations) unless ``--force``. Results land in
``data/reports/replays/<run>/`` (``results.jsonl``, ``report.md``).

    python scripts/replay_agents.py sample --news 4 --intraday 3 --preopen 2 --nightly 1
    python scripts/replay_agents.py run --samples data/reports/replays/samples-<ts>.json \\
        --baseline --model sonnet-5.5 --effort medium
    python scripts/replay_agents.py run --samples ... --repeat 3          # production run-to-run noise
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import re
import statistics
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from ulid import ULID

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_yaml, repo_root
from engine.intelligence.agents import intraday, news_analyst, nightly, preopen
from engine.intelligence.harness import MODEL_API_IDS, AgentHarness, load_agent_roster, resolve_cli_path

AGENTS: dict[str, tuple[str, Any]] = {             # short name -> (agents.yaml id, agent module)
    "news": ("news_analyst", news_analyst),
    "intraday": ("intraday_analyst", intraday),
    "preopen": ("preopen_planner", preopen),
    "nightly": ("nightly_reviewer", nightly),
}
EFFORTS = ("low", "medium", "high", "xhigh", "max", "none")
OUT_ROOT = repo_root() / "data" / "reports" / "replays"


# --------------------------------------------------------------------------- transcripts
def transcripts_dir() -> Path:
    # Claude Code names a project's transcript folder after its path with every other character as '-'.
    return Path.home() / ".claude" / "projects" / re.sub(r"[^A-Za-z0-9]", "-", str(repo_root()))


def read_call(path: Path) -> tuple[str | None, str | None, Any, str | None]:
    """``(entrypoint, prompt, live structured output, live model)`` of one transcript."""
    entrypoint = prompt = output = model = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        entrypoint = entrypoint or rec.get("entrypoint")
        msg = rec.get("message") or {}
        if rec.get("type") == "user" and prompt is None and isinstance(msg.get("content"), str):
            prompt = msg["content"]
        if rec.get("type") == "assistant":
            if msg.get("model") and msg["model"] != "<synthetic>":
                model = msg["model"]
            for block in msg.get("content") or []:
                if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("name") == "StructuredOutput":
                    output = block.get("input")
    return entrypoint, prompt, output, model


def classify(output: Any) -> str | None:
    if not isinstance(output, dict):
        return None
    for key, agent in (("scores", "news"), ("action", "intraday"), ("regime", "preopen"), ("focus", "preopen"),
                       ("summary", "nightly")):
        if key in output:
            return agent
    return None


def cmd_sample(args: argparse.Namespace) -> None:
    want = {a: getattr(args, a) for a in AGENTS if getattr(args, a)}
    # Intraday reads a deeper pool: most live calls are no_action, which never exercises a thesis.
    depth = {a: n * 20 if a == "intraday" else n for a, n in want.items()}
    # A replay re-sends an earlier live prompt, so only the EARLIEST transcript of each prompt is live.
    originals: dict[str, tuple[float, str, dict[str, Any]]] = {}
    for path in transcripts_dir().glob("*.jsonl"):
        entrypoint, prompt, output, model = read_call(path)
        agent = classify(output) if entrypoint == "sdk-py" and prompt else None
        mtime = path.stat().st_mtime
        if agent in want and (prompt not in originals or mtime < originals[prompt][0]):
            originals[prompt] = (mtime, agent, {"file": path.name, "live_model": model, "action": output.get("action")})
    pool: dict[str, list[dict[str, Any]]] = {a: [] for a in want}
    for _, agent, entry in sorted(originals.values(), key=lambda v: v[0], reverse=True):
        if len(pool[agent]) < depth[agent]:
            pool[agent].append(entry)
    chosen = {a: entries[: want[a]] for a, entries in pool.items()}
    if "intraday" in chosen and all(e["action"] == "no_action" for e in chosen["intraday"]):
        acted = next((e for e in pool["intraday"] if e["action"] != "no_action"), None)
        if acted and chosen["intraday"]:
            chosen["intraday"][-1] = acted
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    out = Path(args.out) if args.out else OUT_ROOT / f"samples-{time.strftime('%Y%m%dT%H%M%S')}.json"
    out.write_text(json.dumps({"agents": chosen}, indent=1), encoding="utf-8")
    print(f"wrote {out}")
    for agent, entries in chosen.items():
        print(f"  {agent}: {len(entries)}/{want[agent]}  " + ", ".join(f"{e['file'][:8]}:{e['action']}" for e in entries))


# --------------------------------------------------------------------------- validation + metrics
def validate(agent: str, output: Any) -> str | None:
    """None when the agent's real parser accepts ``output``, else the rejection."""
    raw = json.dumps(output)
    try:
        if agent == "intraday":
            clock = Clock()
            intraday.parse_output(raw, proposal_id=str(ULID()), valid_until=clock.now() + timedelta(minutes=15),
                                  inputs_digest="replay")
        elif agent == "news":
            _, dropped = news_analyst.parse_output(raw)
            if dropped:
                return f"{len(dropped)} clusters dropped"
        else:
            AGENTS[agent][1].parse_output(raw)
    except Exception as exc:  # noqa: BLE001 - any parser rejection is the finding
        return f"{type(exc).__name__}: {exc}"[:300]
    return None


def news_scores(output: Any) -> dict[str, dict[str, Any]]:
    scores = (output or {}).get("scores", [])
    while isinstance(scores, dict):           # a CLI schema retry can wrap the batch once more
        scores = scores.get("scores", [])
    return {s["cluster_id"]: s for s in scores if isinstance(s, dict) and "cluster_id" in s}


def news_diff(a: Any, b: Any) -> dict[str, float] | None:
    sa, sb = news_scores(a), news_scores(b)
    common = sorted(set(sa) & set(sb))
    if not common:
        return None
    return {
        "d_materiality": statistics.mean(abs(sa[c]["materiality"] - sb[c]["materiality"]) for c in common),
        "d_sentiment": statistics.mean(abs(sa[c]["sentiment"] - sb[c]["sentiment"]) for c in common),
        "event_agree": sum(sa[c]["event_type"] == sb[c]["event_type"] for c in common) / len(common),
    }


def shape(agent: str, output: Any) -> dict[str, Any]:
    o = output if isinstance(output, dict) else {}
    if agent == "news":
        scores = news_scores(o)
        return {"clusters": len(scores), "high_materiality": sum(s["materiality"] >= 0.5 for s in scores.values())}
    if agent == "intraday":
        return {"action": o.get("action"), "symbol": o.get("tradingsymbol"),
                "entry_missing": o.get("action") == "enter" and not o.get("entry_price")}
    if agent == "preopen":
        return {"focus": len(o["focus"]) if "focus" in o else None, "warnings": len(o.get("warnings") or [])}
    return {"summary_chars": len(o.get("summary") or ""), "lessons": len(o.get("lessons") or [])}


# --------------------------------------------------------------------------- replay
def market_hours_now() -> bool:
    clock = Clock()
    now = clock.now()
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    return calendar.is_trading_day(now.date()) and (9, 15) <= (now.hour, now.minute) < (15, 30)


def build_options(defs: dict, agent: str, cli_path: str | None, overrides: dict[str, Any]) -> Any:
    agent_id, module = AGENTS[agent]
    harness = AgentHarness(defs, None, Clock(), None, cli_path=cli_path)   # option building needs neither
    options, prefix = harness._build_options(defs[agent_id], system_prompt=module.SYSTEM_PROMPT,
                                             json_schema=module.output_json_schema(), max_turns=1)
    assert not prefix, "options surface lost its system_prompt knob"
    return dataclasses.replace(options, **overrides) if overrides else options


async def replay_one(prompt: str, options: Any) -> dict[str, Any]:
    import claude_agent_sdk as sdk  # noqa: PLC0415 - heavy; only the run path needs it

    started = time.monotonic()
    result, thinking, error = None, 0, None
    try:
        async for message in sdk.query(prompt=prompt, options=options):
            if isinstance(message, sdk.AssistantMessage):
                thinking += sum(isinstance(b, sdk.ThinkingBlock) for b in message.content)
            elif isinstance(message, sdk.ResultMessage):
                result = message
    except Exception as exc:  # noqa: BLE001 - one failed call is a data point, not the end of the run
        error = f"{type(exc).__name__}: {exc}"[:500]
    usage = (getattr(result, "usage", None) or {})
    return {
        "secs": round(time.monotonic() - started, 1), "error": error,
        "subtype": getattr(result, "subtype", None), "stop_reason": getattr(result, "stop_reason", None),
        "num_turns": getattr(result, "num_turns", None), "out_tokens": usage.get("output_tokens"),
        "cache_read": usage.get("cache_read_input_tokens"), "cache_write": usage.get("cache_creation_input_tokens"),
        "thinking_blocks": thinking, "output": getattr(result, "structured_output", None),
    }


def cmd_run(args: argparse.Namespace) -> None:
    samples = json.loads(Path(args.samples).read_text(encoding="utf-8"))["agents"]
    agents = [a for a in (args.agents.split(",") if args.agents else samples) if samples.get(a)]
    cfg = load_yaml(config_dir() / "agents.yaml")
    defs = load_agent_roster(cfg).defs
    cli_path = resolve_cli_path(cfg, repo_root())

    overrides: dict[str, Any] = {}
    if args.model:
        overrides["model"] = MODEL_API_IDS.get(args.model, args.model)
    if args.effort:
        overrides["effort"] = None if args.effort == "none" else args.effort
    if args.cli_path:
        overrides["cli_path"] = str(Path(args.cli_path).resolve())
    conditions = ([("baseline", {})] if args.baseline else []) + [("candidate", overrides)]

    calls = sum(len(samples[a]) for a in agents) * len(conditions) * args.repeat
    print(f"{calls} calls: agents={agents} conditions={[c for c, _ in conditions]} repeat={args.repeat} "
          f"candidate overrides={overrides or 'none (= production)'}")
    if args.dry_run:
        return
    if market_hours_now() and not args.force:
        sys.exit("refusing: market hours on a trading day — replays share the live engine's quota (--force)")

    run_dir = OUT_ROOT / time.strftime("%Y%m%dT%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(
        {"samples": str(Path(args.samples).resolve()), "agents": agents, "overrides": overrides,
         "baseline": args.baseline, "repeat": args.repeat}, indent=1), encoding="utf-8")
    results_path = run_dir / "results.jsonl"
    rows: list[dict[str, Any]] = []
    for agent in agents:
        options_by_condition = {c: build_options(defs, agent, cli_path, o) for c, o in conditions}
        for entry in samples[agent]:
            _, prompt, live, live_model = read_call(transcripts_dir() / entry["file"])
            if prompt is None:
                print(f"  skip {entry['file']}: transcript gone (Claude Code prunes after cleanupPeriodDays)")
                continue
            for rep in range(args.repeat):
                for condition, options in options_by_condition.items():
                    row = {"agent": agent, "file": entry["file"], "condition": condition, "repeat": rep,
                           "model": options.model, "effort": options.effort, "live_model": live_model,
                           **asyncio.run(replay_one(prompt, options)), "live_output": live}
                    row["invalid"] = (row["error"] or "no structured output") if row["output"] is None \
                        else validate(agent, row["output"])
                    rows.append(row)
                    with results_path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(row, default=str) + "\n")
                    print(f"  {agent:8} {entry['file'][:8]} {condition:9} r{rep} "
                          f"{'OK ' if not row['invalid'] else 'BAD'} {row['secs']:>6}s out={row['out_tokens']} "
                          f"{row['invalid'] or ''}", flush=True)
    report = render_report(rows)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(f"\n{report}\nresults: {run_dir}")


def cmd_report(args: argparse.Namespace) -> None:
    text = (Path(args.run_dir) / "results.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in text.splitlines()]
    print(render_report(rows))


# --------------------------------------------------------------------------- report
def _median(values: list[Any]) -> Any:
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def render_report(rows: list[dict[str, Any]]) -> str:
    lines = ["| agent | condition | n | valid | refusals | errors | median out tokens | median secs | "
             "thinking calls | vs live |", "|---|---|---|---|---|---|---|---|---|---|"]
    by_group: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        by_group.setdefault((r["agent"], r["condition"]), []).append(r)
    for (agent, condition), group in sorted(by_group.items()):
        lines.append(
            f"| {agent} | {condition} | {len(group)} | {sum(not r['invalid'] for r in group)} | "
            f"{sum(r.get('stop_reason') == 'refusal' for r in group)} | {sum(bool(r['error']) for r in group)} | "
            f"{_median([r['out_tokens'] for r in group])} | {_median([r['secs'] for r in group])} | "
            f"{sum(r['thinking_blocks'] > 0 for r in group)} | {vs_live(agent, group)} |")
    lines += ["", "Per sample (first repeat): live -> each condition", ""]
    for r in rows:
        if r["repeat"] == 0:
            lines.append(f"- {r['agent']} {r['file'][:8]} {r['condition']}: live {shape(r['agent'], r['live_output'])} "
                         f"-> {shape(r['agent'], r['output'])}" + (f" -- INVALID {r['invalid']}" if r["invalid"] else ""))
    return "\n".join(lines) + "\n"


def vs_live(agent: str, group: list[dict[str, Any]]) -> str:
    ok = [r for r in group if r["output"] is not None]
    if not ok:
        return "-"
    if agent == "news":
        diffs = [d for r in ok if (d := news_diff(r["live_output"], r["output"]))]
        hi_live = statistics.mean(shape(agent, r["live_output"])["high_materiality"] for r in ok)
        hi_new = statistics.mean(shape(agent, r["output"])["high_materiality"] for r in ok)
        if not diffs:
            return "no common clusters"
        return (f"mean abs d-materiality {statistics.mean(d['d_materiality'] for d in diffs):.3f}, event agree "
                f"{statistics.mean(d['event_agree'] for d in diffs):.0%}, mat>=0.5 {hi_new:.1f} vs live {hi_live:.1f}")
    if agent == "intraday":
        same = sum(shape(agent, r["output"])["action"] == shape(agent, r["live_output"])["action"] for r in ok)
        missing = sum(shape(agent, r["output"])["entry_missing"] for r in ok)
        return f"action matches live {same}/{len(ok)}, entries without price {missing}"
    if agent == "preopen":
        present = sum(shape(agent, r["output"])["focus"] is not None for r in ok)
        return f"focus list present {present}/{len(ok)}"
    chars = _median([shape(agent, r["output"])["summary_chars"] for r in ok])
    return f"median summary {chars} chars (live {_median([shape(agent, r['live_output'])['summary_chars'] for r in ok])})"


# --------------------------------------------------------------------------- cli
def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sample", help="pin a sample set of live calls")
    for agent, default in (("news", 4), ("intraday", 3), ("preopen", 2), ("nightly", 1)):
        s.add_argument(f"--{agent}", type=int, default=default, help=f"live {agent} calls (default {default})")
    s.add_argument("--out", help="sample file (default data/reports/replays/samples-<ts>.json)")

    r = sub.add_parser("run", help="replay a pinned sample set")
    r.add_argument("--samples", required=True)
    r.add_argument("--agents", help="comma list of news,intraday,preopen,nightly (default: all in the set)")
    r.add_argument("--baseline", action="store_true", help="also replay production options, paired")
    r.add_argument("--model", help=f"candidate model: agents.yaml name ({', '.join(MODEL_API_IDS)}) or API id")
    r.add_argument("--effort", choices=EFFORTS, help="candidate effort ('none' = send no effort)")
    r.add_argument("--cli-path", help="candidate Claude Code CLI executable")
    r.add_argument("--repeat", type=int, default=1, help="runs per sample and condition (noise)")
    r.add_argument("--dry-run", action="store_true", help="print the call plan only")
    r.add_argument("--force", action="store_true", help="run even during market hours")

    p = sub.add_parser("report", help="re-render a run's report")
    p.add_argument("run_dir")

    args = parser.parse_args(argv)
    {"sample": cmd_sample, "run": cmd_run, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
