"""config/research_trials.yaml, the research trial ledger (review §9 #8). Pins the production file."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from engine.core.config import config_dir
from engine.learning.multiplicity import load_trials


def test_every_registered_edge_has_a_registered_or_grandfathered_entry():
    settings = yaml.safe_load((config_dir() / "settings.yaml").read_text(encoding="utf-8"))
    edged = {k for k, v in settings.items() if isinstance(v, dict) and "expected_edge_pct" in v}
    assert edged >= {"hi52", "ins"}
    covered = {t.family.removeprefix("edge/") for t in load_trials()
               if t.family.startswith("edge/") and t.result in ("registered", "grandfathered")}
    assert edged <= covered


def test_ledger_ids_are_unique():
    ids = [t.id for t in load_trials()]
    assert len(ids) == len(set(ids))


_ROW = {"id": "x", "date": "2026-10-29", "family": "edge/x", "hypothesis": "h", "population": "p",
        "construct": "c", "variants": 1, "result": "null", "report": "r"}


@pytest.mark.parametrize("bad", [{"date": "20261029"}, {"date": "2026-13-01"}, {"result": "promising"},
                                 {"variants": 0}, {"extra": 1}])
def test_a_bad_row_raises(tmp_path: Path, bad):
    p = tmp_path / "t.yaml"
    p.write_text(yaml.safe_dump({"trials": [_ROW | bad]}), encoding="utf-8")
    with pytest.raises((ValueError, TypeError)):
        load_trials(p)


def test_a_duplicate_id_raises(tmp_path: Path):
    p = tmp_path / "t.yaml"
    p.write_text(yaml.safe_dump({"trials": [_ROW, _ROW]}), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_trials(p)
