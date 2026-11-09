"""scripts/fetch_macro_external.py: participant-OI parsing, the FII long share and the manifest guard."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_fetch_macro_external", _ROOT / "scripts" / "fetch_macro_external.py")
fx = importlib.util.module_from_spec(_spec)
sys.modules["mt_fetch_macro_external"] = fx
_spec.loader.exec_module(fx)

_FIX = Path(__file__).resolve().parent / "fixtures" / "macro_oi"


def test_parse_oi_header_variants():
    got = {n: fx.parse_participant_oi((_FIX / n).read_bytes()) for n in
           ("oi_02012012.csv", "oi_01022022.csv", "oi_06112026.csv")}
    assert all(set(v) == {"fut_index_long", "fut_index_short"} for v in got.values())
    assert got["oi_02012012.csv"] == {"fut_index_long": 55396, "fut_index_short": 1387}
    assert got["oi_01022022.csv"] == {"fut_index_long": 46449, "fut_index_short": 99799}
    assert got["oi_06112026.csv"] == {"fut_index_long": 48656, "fut_index_short": 149072}


def test_parse_oi_missing_fii_row_returns_none():
    text = ('"title",,\nClient Type,Future Index Long,Future Index Short\n'
            "Client,1,2\nDII,3,4\nPro,5,6\n")
    assert fx.parse_participant_oi(text.encode()) is None
    assert fx.parse_participant_oi(b"") is None


def test_long_share_from_parsed_rows(tmp_path):
    for n, d in (("oi_06112026.csv", "06112026"), ("oi_02012012.csv", "02012012")):
        (tmp_path / f"fao_participant_oi_{d}.csv").write_bytes((_FIX / n).read_bytes())
    (tmp_path / "fao_participant_oi_03012012.csv").write_bytes(b"garbage")
    s = fx.fii_long_share(tmp_path)
    assert list(s.index) == [date(2012, 1, 2), date(2026, 11, 6)]
    assert s.iloc[0] == pytest.approx(55396 / (55396 + 1387))
    assert s.iloc[1] == pytest.approx(48656 / (48656 + 149072))


def test_manifest_refuses_overwrite_without_resume(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps({"files": {}}), encoding="utf-8")
    assert fx.main(["--out", str(tmp_path)]) == 2
    assert json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8")) == {"files": {}}
