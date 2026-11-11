#!/usr/bin/env python
"""Pool widening study: quoted bid-ask spreads per liquidity band, from live Kite quote snapshots.

The cost model's ``spread_pct`` (0.02%) was measured on NIFTY 200 names. The bands this study tests trade thinner
books, so each band is priced at its own measured spread.

Each round reads the quote (best bid and ask, five-level depth) of every listed equity with a band
(``pool_widening_common``) and appends one row per name to a CSV. The quote API takes 500 names a request, and
requests are spaced 1.5 s apart. Rounds run only inside the continuous session, 09:20 to 15:20, away from both
auctions. ``--summary`` prints, per band, the median over names of each name's median spread, and how often five
levels could absorb a Rs 20,000 order.

Standalone owner tool: pykiteconnect with the API key and access token from DPAPI secrets. Reads the research
snapshot read-only and never touches the engine's stores.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time as time_mod
from collections import defaultdict
from datetime import date, datetime, time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
for p in (_REPO_ROOT / "src", Path(__file__).resolve().parent):
    if str(p) not in sys.path:  # pragma: no cover - environment shim
        sys.path.insert(0, str(p))

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST
import duckdb  # noqa: E402
import pool_widening_common as pw  # noqa: E402
from engine.core.clock import IST  # noqa: E402

ORDER_INR = 20_000.0
WINDOW = (time(9, 20), time(15, 20))
CHUNK = 500
GAP_S = 1.5
FIELDS = ("ts", "symbol", "band", "rank", "median_tv_inr", "last_price", "bid", "ask", "bid_qty", "ask_qty",
          "spread_pct", "rt20k_pct", "depth_ok")


def banded_names(db: Path, as_of: date, master: set[str]) -> dict[str, tuple[str, int, float]]:
    """``symbol -> (band, rank, median)`` for the listed equities in ``master`` with a band on ``as_of``."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        rows = con.execute(
            """
            with r as (
              select symbol, close * volume as tv,
                     row_number() over (partition by symbol order by d desc) as rn
              from bars_1d where d <= ? and d > ? - interval 45 day
            )
            select symbol, median(tv) from r where rn <= ? group by symbol having count(*) >= ?
            """,
            [as_of, as_of, pw.LIQ_SESSIONS, pw.LIQ_MIN_SESSIONS],
        ).fetchall()
    finally:
        con.close()
    ranked = sorted(((s, float(m)) for s, m in rows if s in master and m is not None), key=lambda x: (-x[1], x[0]))
    out = {}
    for rank, (sym, med) in enumerate(ranked, start=1):
        band = pw.band_of(rank, med)
        if band is not None:
            out[sym] = (band, rank, med)
    return out


def _walk(levels: list[dict], notional: float) -> float | None:
    """Average price of ``notional`` rupees taken from ``levels``; None when they cannot absorb it."""
    left, qty, spent = notional, 0, 0.0
    for lv in levels:
        price, avail = float(lv.get("price") or 0), int(lv.get("quantity") or 0)
        if price <= 0 or avail <= 0:
            continue
        take = min(avail, int(left // price) + 1)
        qty += take
        spent += take * price
        left -= take * price
        if left <= 0:
            return spent / qty
    return None


def measure(q: dict) -> dict:
    """Spread and Rs 20,000 round-trip cost from one quote; empty when either side has no price (a circuit)."""
    depth = q.get("depth") or {}
    bids, asks = depth.get("buy") or [], depth.get("sell") or []
    bid = float(bids[0]["price"]) if bids else 0.0
    ask = float(asks[0]["price"]) if asks else 0.0
    if bid <= 0 or ask <= 0 or ask < bid:
        return {}
    mid = (bid + ask) / 2
    buy, sell = _walk(asks, ORDER_INR), _walk(bids, ORDER_INR)
    return {
        "bid": bid, "ask": ask, "bid_qty": int(bids[0].get("quantity") or 0),
        "ask_qty": int(asks[0].get("quantity") or 0),
        "spread_pct": round((ask - bid) / mid * 100, 5),
        "rt20k_pct": None if buy is None or sell is None else round((buy - sell) / mid * 100, 5),
        "depth_ok": buy is not None and sell is not None,
    }


def run_round(kc, names: dict[str, tuple[str, int, float]], out: Path) -> int:
    syms = sorted(names)
    rows = []
    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i + CHUNK]
        quotes = kc.quote([f"NSE:{s}" for s in chunk])
        ts = datetime.now(IST).isoformat(timespec="seconds")
        for s in chunk:
            q = quotes.get(f"NSE:{s}")
            m = measure(q) if q else {}
            if not m:
                continue
            band, rank, med = names[s]
            rows.append({"ts": ts, "symbol": s, "band": band, "rank": rank, "median_tv_inr": round(med),
                         "last_price": q.get("last_price"), **m})
        time_mod.sleep(GAP_S)
    new = not out.exists()
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)
    return len(rows)


def summary(path: Path) -> str:
    per: dict[tuple[str, str], list[float]] = defaultdict(list)
    rt: dict[tuple[str, str], list[float]] = defaultdict(list)
    ok: dict[str, list[bool]] = defaultdict(list)
    rounds: set[str] = set()
    with path.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            rounds.add(r["ts"][:16])
            per[(r["band"], r["symbol"])].append(float(r["spread_pct"]))
            if r["rt20k_pct"]:
                rt[(r["band"], r["symbol"])].append(float(r["rt20k_pct"]))
            ok[r["band"]].append(r["depth_ok"] == "True")
    lines = [f"{path}  rounds {len(rounds)}",
             "band                  names  median spread%  p75 spread%  median rt20k%  depth absorbs 20k"]
    for band in pw.BANDS:
        names = [statistics.median(v) for (b, _), v in per.items() if b == band]
        rts = [statistics.median(v) for (b, _), v in rt.items() if b == band]
        if not names:
            continue
        p75 = statistics.quantiles(names, n=4)[2] if len(names) >= 4 else max(names)
        share = sum(ok[band]) / len(ok[band])
        lines.append(f"{band:<21} {len(names):>5}  {statistics.median(names):>14.4f}  {p75:>11.4f}  "
                     f"{(statistics.median(rts) if rts else float('nan')):>13.4f}  {share:>16.1%}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pool_widening_spreads", description=__doc__.splitlines()[0])
    ap.add_argument("--db", type=Path, required=True, help="research snapshot with whole-market bars_1d")
    ap.add_argument("--as-of", type=date.fromisoformat, required=True, help="last bar day for the bands")
    ap.add_argument("--out", type=Path, required=True, help="CSV the rounds append to")
    ap.add_argument("--rounds", type=int, default=1)
    ap.add_argument("--interval-min", type=float, default=20.0)
    ap.add_argument("--summary", action="store_true", help="print the per-band summary of --out and exit")
    args = ap.parse_args(argv)
    if args.summary:
        print(summary(args.out))
        return 0

    from kiteconnect import KiteConnect  # heavy import deferred; owner tool only

    from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, Secrets

    secrets = Secrets()
    api_key, token = secrets.get_optional(KITE_API_KEY), secrets.get_optional(KITE_ACCESS_TOKEN)
    if not api_key or not token:
        print("pool_widening_spreads: Kite key or access token missing from DPAPI secrets", file=sys.stderr)
        return 2
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(token)
    master = set(json.loads((_REPO_ROOT / "data/universe/surveillance.json").read_text("utf-8"))
                 ["equity_master"]["symbols"])
    names = banded_names(args.db, args.as_of, master)
    counts = {b: sum(1 for v in names.values() if v[0] == b) for b in pw.BANDS}
    print(f"banded names {len(names)} {counts}", flush=True)
    for i in range(args.rounds):
        now = datetime.now(IST)
        if not (WINDOW[0] <= now.time() <= WINDOW[1]):
            print(f"{now:%H:%M} outside {WINDOW[0]:%H:%M}-{WINDOW[1]:%H:%M}; stopping", flush=True)
            break
        n = run_round(kc, names, args.out)
        print(f"{now:%H:%M:%S} round {i + 1}: {n} quotes", flush=True)
        if i + 1 < args.rounds:
            time_mod.sleep(args.interval_min * 60)
    print(summary(args.out), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
