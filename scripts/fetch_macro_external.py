"""Macro mood study: download the external series the backtests read, with a manifest.

Stores, under ``data/research/external/`` (or ``--out``): the raw Yahoo v8 chart JSON for four symbols,
one NSE participant-wise open-interest CSV per NIFTY session since 2012-01-02, and ``manifest.json``.
A 404 is final (``missing``); any other failure lands in ``errors`` and is retried on ``--resume``.
Network only; never touches the engine's stores.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path

_SCRIPTS = str(Path(__file__).resolve().parent)
if _SCRIPTS not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _SCRIPTS)

import pandas as pd  # noqa: E402

from macro_mood_common import yahoo_daily  # noqa: E402

_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = _ROOT / "data" / "research" / "external"
YAHOO_SYMBOLS = ("^NSEI", "^GSPC", "^IXIC", "BZ=F")
YAHOO_PERIOD1 = int(datetime(2007, 1, 1, tzinfo=UTC).timestamp())
OI_FIRST_DAY = date(2012, 1, 2)
OI_URL = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_{:%d%m%Y}.csv"
OI_NAME = "fao_participant_oi_{:%d%m%Y}.csv"
_OI_NAME_RE = re.compile(r"^fao_participant_oi_(\d{2})(\d{2})(\d{4})\.csv$")
NSE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.nseindia.com/"}
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}
NSE_MIN_GAP_S = 1.0
_SAVE_EVERY = 50


def _norm(cell: str) -> str:
    return " ".join(cell.replace("_", " ").split()).lower()


def _to_int(cell: str) -> int:
    return int(re.sub(r"[\s\",]", "", cell))


def parse_participant_oi(csv_bytes: bytes) -> dict | None:
    """The FII/FPI row's index-futures long and short contracts, or None when the row or a column is absent.

    The first line is a title; the header is found by its first cell ("Client Type" / "CLIENT_TYPE"), and
    column names are compared lowercased with whitespace and tabs collapsed."""
    rows = csv.reader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace")))
    header = None
    for row in rows:
        if header is None:
            if row and _norm(row[0]) == "client type":
                header = [_norm(c) for c in row]
            continue
        if not row or _norm(row[0]).upper() not in ("FII", "FPI"):
            continue
        try:
            long_i, short_i = header.index("future index long"), header.index("future index short")
            return {"fut_index_long": _to_int(row[long_i]), "fut_index_short": _to_int(row[short_i])}
        except (ValueError, IndexError):
            return None
    return None


def fii_long_share(oi_dir: Path) -> pd.Series:
    """FII long / (long + short) index-futures contracts per file date, sorted; unparseable files are skipped."""
    out: dict[date, float] = {}
    for p in Path(oi_dir).iterdir():
        m = _OI_NAME_RE.match(p.name)
        if not m:
            continue
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            continue
        got = parse_participant_oi(p.read_bytes())
        if got is None:
            continue
        total = got["fut_index_long"] + got["fut_index_short"]
        if total > 0:
            out[d] = got["fut_index_long"] / total
    return pd.Series(out, dtype="float64").sort_index().rename_axis("date")


def _http_get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return 404, b""
        raise


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _save_manifest(path: Path, manifest: dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _store(out: Path, manifest: dict, rel: str, url: str, body: bytes) -> None:
    dest = out / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(body)
    manifest["files"][rel] = {"url": url, "fetched_at": _now(), "bytes": len(body),
                              "sha256": hashlib.sha256(body).hexdigest()}
    manifest["errors"].pop(rel, None)


def _fetch_yahoo(out: Path, manifest: dict) -> None:
    for sym in YAHOO_SYMBOLS:
        rel = f"yahoo/{sym}.json"
        if rel in manifest["files"]:
            continue
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym, safe='')}"
               f"?period1={manifest['period1']}&period2={manifest['period2']}&interval=1d")
        try:
            status, body = _http_get(url, YAHOO_HEADERS)
            if status != 200 or not body:
                raise ValueError(f"HTTP {status}, {len(body)} bytes")
            _store(out, manifest, rel, url, body)
        except Exception as exc:  # noqa: BLE001 - recorded, retried on --resume
            manifest["errors"][rel] = f"{type(exc).__name__}: {exc}"
        _save_manifest(out / "manifest.json", manifest)


def _fetch_oi(out: Path, manifest: dict, limit: int | None) -> None:
    nifty = out / "yahoo" / "^NSEI.json"
    if "yahoo/^NSEI.json" not in manifest["files"]:
        manifest["errors"]["nse_oi"] = "no ^NSEI file, session list unavailable"
        return
    manifest["errors"].pop("nse_oi", None)
    days =[d for d in yahoo_daily(nifty).index if d >= OI_FIRST_DAY]
    if limit is not None:
        days = days[:limit]
    missing = set(manifest["missing"])
    last = 0.0
    done = 0
    for d in days:
        rel = f"nse_oi/{OI_NAME.format(d)}"
        if rel in manifest["files"] or d.isoformat() in missing:
            continue
        wait = NSE_MIN_GAP_S - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)
        url = OI_URL.format(d)
        try:
            last = time.monotonic()
            status, body = _http_get(url, NSE_HEADERS)
            if status == 404:
                manifest["missing"].append(d.isoformat())
                manifest["errors"].pop(d.isoformat(), None)
            elif status == 200 and body:
                _store(out, manifest, rel, url, body)
                manifest["errors"].pop(d.isoformat(), None)
            else:
                raise ValueError(f"HTTP {status}, {len(body)} bytes")
        except Exception as exc:  # noqa: BLE001 - recorded, retried on --resume
            last = time.monotonic()
            manifest["errors"][d.isoformat()] = f"{type(exc).__name__}: {exc}"
        done += 1
        if done % _SAVE_EVERY == 0:
            _save_manifest(out / "manifest.json", manifest)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--resume", action="store_true", help="fetch only what the existing manifest lacks")
    ap.add_argument("--oi-limit", type=int, default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    out: Path = args.out
    mpath = out / "manifest.json"
    if mpath.exists() and not args.resume:
        print(f"{mpath} exists; pass --resume to continue it", file=sys.stderr)
        return 2
    if mpath.exists():
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
        for k, v in (("files", {}), ("missing", []), ("errors", {})):
            manifest.setdefault(k, v)
    else:
        manifest = {"created_at": _now(), "period1": YAHOO_PERIOD1, "period2": int(time.time()),
                    "files": {}, "missing": [], "errors": {}}
    out.mkdir(parents=True, exist_ok=True)
    _fetch_yahoo(out, manifest)
    _fetch_oi(out, manifest, args.oi_limit)
    _save_manifest(mpath, manifest)
    print(f"files={len(manifest['files'])} missing={len(manifest['missing'])} errors={len(manifest['errors'])}")
    return 1 if manifest["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
