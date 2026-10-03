"""``scripts/replay_agents.py`` sampling: replays must never pass for live calls."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "replay_agents.py"
spec = importlib.util.spec_from_file_location("replay_agents", SCRIPT)
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)


def transcript(folder: Path, name: str, prompt: str, output: dict, model: str, mtime: float) -> None:
    p = folder / f"{name}.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in (
        {"type": "user", "entrypoint": "sdk-py", "message": {"content": prompt}},
        {"type": "assistant", "entrypoint": "sdk-py", "message": {"model": model, "content": [
            {"type": "tool_use", "name": "StructuredOutput", "input": output}]}},
    )), encoding="utf-8")
    os.utime(p, (mtime, mtime))


NEWS = {"scores": [{"cluster_id": "c1", "materiality": 0.6, "sentiment": 0.1, "event_type": "other"}]}


@pytest.fixture
def folder(tmp_path, monkeypatch):
    t = tmp_path / "transcripts"
    t.mkdir()
    monkeypatch.setattr(replay, "transcripts_dir", lambda: t)
    monkeypatch.setattr(replay, "OUT_ROOT", tmp_path / "out")
    return t


def sample(tmp_path, *args) -> dict:
    out = tmp_path / "samples.json"
    replay.main(["sample", "--preopen", "0", "--nightly", "0", *args, "--out", str(out)])
    return json.loads(out.read_text(encoding="utf-8"))["agents"]


def test_a_replay_of_a_live_prompt_is_not_sampled_as_live(folder, tmp_path):
    transcript(folder, "live", "PROMPT-A", NEWS, "claude-sonnet-5", 1_000)
    transcript(folder, "replay", "PROMPT-A", NEWS, "claude-sonnet-5-5", 2_000)   # newer, same prompt
    transcript(folder, "other", "PROMPT-B", NEWS, "claude-sonnet-5", 500)

    got = sample(tmp_path, "--news", "2", "--intraday", "0")

    assert [e["file"] for e in got["news"]] == ["live.jsonl", "other.jsonl"]
    assert got["news"][0]["live_model"] == "claude-sonnet-5"


def test_intraday_keeps_one_acted_call_when_the_newest_are_no_action(folder, tmp_path):
    for i in range(3):
        transcript(folder, f"quiet{i}", f"Q{i}", {"action": "no_action"}, "m", 3_000 + i)
    transcript(folder, "acted", "E", {"action": "enter"}, "m", 1_000)

    got = sample(tmp_path, "--news", "0", "--intraday", "2")

    assert [e["action"] for e in got["intraday"]] == ["no_action", "enter"]


def test_news_scores_unwraps_a_schema_retry_wrapper():
    wrapped = {"scores": {"scores": NEWS["scores"]}}
    assert replay.news_scores(wrapped) == replay.news_scores(NEWS) == {"c1": NEWS["scores"][0]}
