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


def test_parse_oi_bare_cr_line_endings():
    got = fx.parse_participant_oi((_FIX / "fao_participant_oi_04102017.csv").read_bytes())
    assert got == {"fut_index_long": 147736, "fut_index_short": 105587}


def test_parse_oi_decimal_indian_grouped_values():
    got = fx.parse_participant_oi((_FIX / "fao_participant_oi_20012012.csv").read_bytes())
    assert got == {"fut_index_long": 425358, "fut_index_short": 251879}


def test_parse_oi_headerless_file_uses_standard_positions():
    got = fx.parse_participant_oi((_FIX / "fao_participant_oi_24022014.csv").read_bytes())
    assert got == {"fut_index_long": 266471, "fut_index_short": 174343}


def test_parse_oi_scrambled_labels_file_is_none():
    # 2013-08-22: column names sit in the title row, row labels are shifted and TOTAL is blank, so the FII row
    # cannot be identified with confidence
    assert fx.parse_participant_oi((_FIX / "fao_participant_oi_22082013.csv").read_bytes()) is None


def test_parse_oi_headerless_unbalanced_total_is_none():
    text = '"title"\nClient,1,2\nFII,3,4\nTOTAL,10,11\n'
    assert fx.parse_participant_oi(text.encode()) is None


@pytest.mark.parametrize("raw", [b"\xff\xfe\x00", b'"unterminated,1,2\n', b"Client Type,Future Index Long,Future Index Short\nFII,nan,3\n",
                                 b"Client Type,Future Index Long,Future Index Short\nFII,1\n"])
def test_parse_oi_malformed_never_raises(raw):
    assert fx.parse_participant_oi(raw) is None


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
