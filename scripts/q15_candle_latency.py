"""§14 Q15 measurement: how quickly is the just-closed minute's OFFICIAL candle available from the
Kite historical API intraday?

The answer bounds the §2.6 warm-up path on a cold start close to the trade window (official candles
are canonical, §4.4 job 2) — if the candle for minute M is only served at M+~90 s, a start at
window-open minus one minute cannot warm ORB features from official data and must stay FROZEN.

Run DURING A LIVE SESSION (needs a valid daily access token):

    .venv\\Scripts\\python.exe scripts/q15_candle_latency.py --minutes 20

Method: for each observed minute boundary, poll ``historical_data`` for the just-closed minute on a
few liquid symbols every ``--poll-s`` seconds until that minute's candle appears (or ``--timeout-s``
gives up); the sample is the delay from the minute close to first availability. Percentiles land in
``data/reports/q15_latency.json``. Pacing: with the default 3 symbols and 2 s poll interval the
request rate stays ≤1.5 req/s, inside the ≤3 req/s historical budget (A2) — this script must not be
run concurrently with an engine that is backfilling.

Standalone owner tool (like scripts/a11_check.py): talks to pykiteconnect directly and reads the
API key + access token from DPAPI secrets; it never touches the engine's stores.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time as time_mod
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:  # pragma: no cover - environment shim
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from engine.core.clock import IST  # noqa: E402
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, Secrets  # noqa: E402

DEFAULT_SYMBOLS = ["RELIANCE", "HDFCBANK", "INFY"]  # liquid, always-on NSE names

# fetch(token, frm, to) -> list of candle dicts with a "date" key (kite historical_data shape)
FetchFn = Callable[[int, datetime, datetime], list]


def measure_minute(
    tokens: dict[str, int],
    minute_start: datetime,
    fetch: FetchFn,
    *,
    poll_s: float,
    timeout_s: float,
    now_fn: Callable[[], datetime],
    sleep_fn: Callable[[float], None],
) -> dict[str, float | None]:
    """Poll until the candle for ``[minute_start, minute_start+1m)`` appears per symbol; return the
    delay (s) from the minute CLOSE to first availability, or None if the timeout gave up."""
    minute_end = minute_start + timedelta(minutes=1)
    pending = dict(tokens)
    delays: dict[str, float | None] = {s: None for s in tokens}
    while pending and (now_fn() - minute_end).total_seconds() < timeout_s:
        for symbol, token in list(pending.items()):
            try:
                candles = fetch(token, minute_start, minute_end)
            except Exception as exc:  # noqa: BLE001 - transient API errors: keep polling
                print(f"  {symbol}: fetch error ({exc}); retrying", file=sys.stderr)
                continue
            if any(_candle_matches(c, minute_start) for c in candles):
                delays[symbol] = round((now_fn() - minute_end).total_seconds(), 3)
                del pending[symbol]
        if pending:
            sleep_fn(poll_s)
    return delays


def _candle_matches(candle, minute_start: datetime) -> bool:
    ts = candle.get("date") if isinstance(candle, dict) else getattr(candle, "date", None)
    if ts is None:
        return False
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts == minute_start


def percentiles(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {}
    ordered = sorted(samples)
    qs = statistics.quantiles(ordered, n=100, method="inclusive") if len(ordered) > 1 else ordered * 99
    return {
        "n": len(ordered),
        "min_s": ordered[0],
        "p50_s": qs[49],
        "p90_s": qs[89],
        "p99_s": qs[98],
        "max_s": ordered[-1],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="measure intraday official-candle availability latency (§14 Q15)")
    parser.add_argument("--minutes", type=int, default=15, help="number of minute boundaries to sample")
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--poll-s", type=float, default=2.0, help="poll interval per symbol (A2 budget)")
    parser.add_argument("--timeout-s", type=float, default=180.0, help="give up on a minute after this")
    parser.add_argument("--out", default=str(_REPO_ROOT / "data" / "reports" / "q15_latency.json"))
    args = parser.parse_args(argv)

    from kiteconnect import KiteConnect  # heavy import deferred; owner tool only

    secrets = Secrets()
    api_key = secrets.get_optional(KITE_API_KEY)
    access_token = secrets.get_optional(KITE_ACCESS_TOKEN)
    if not api_key or not access_token:
        print("q15: kite_api_key / kite_access_token missing from DPAPI secrets — run during a "
              "live session after the daily login (§10.2)", file=sys.stderr)
        return 2
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)

    print(f"q15: resolving instrument tokens for {args.symbols} …")
    instruments = kc.instruments("NSE")
    by_symbol = {i["tradingsymbol"]: int(i["instrument_token"]) for i in instruments}
    tokens = {s: by_symbol[s] for s in args.symbols if s in by_symbol}
    missing = [s for s in args.symbols if s not in by_symbol]
    if missing:
        print(f"q15: symbols not found in the NSE dump, skipped: {missing}", file=sys.stderr)
    if not tokens:
        print("q15: no resolvable symbols", file=sys.stderr)
        return 2

    def fetch(token: int, frm: datetime, to: datetime) -> list:
        return kc.historical_data(instrument_token=token, from_date=frm, to_date=to, interval="minute")

    now_fn = lambda: datetime.now(IST)  # noqa: E731 - standalone tool; the engine's Clock is not needed here
    per_minute: list[dict] = []
    all_delays: dict[str, list[float]] = {s: [] for s in tokens}

    for i in range(args.minutes):
        # Wait for the next minute boundary (+0.2 s so "just closed" is unambiguous).
        now = now_fn()
        boundary = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        time_mod.sleep(max(0.0, (boundary - now).total_seconds()) + 0.2)
        minute_start = boundary - timedelta(minutes=1)
        print(f"[{i + 1}/{args.minutes}] minute {minute_start.strftime('%H:%M')} closed; polling …")
        delays = measure_minute(
            tokens, minute_start, fetch,
            poll_s=args.poll_s, timeout_s=args.timeout_s, now_fn=now_fn, sleep_fn=time_mod.sleep,
        )
        per_minute.append({"minute": minute_start.isoformat(), "delay_s": delays})
        for symbol, d in delays.items():
            print(f"  {symbol}: {'timeout' if d is None else f'{d:.1f}s'}")
            if d is not None:
                all_delays[symbol].append(d)

    flat = [d for ds in all_delays.values() for d in ds]
    report = {
        "measured_at": now_fn().isoformat(),
        "symbols": {s: percentiles(ds) for s, ds in all_delays.items()},
        "aggregate": percentiles(flat),
        "timeouts": sum(
            1 for m in per_minute for d in m["delay_s"].values() if d is None
        ),
        "poll_s": args.poll_s,
        "minutes_sampled": args.minutes,
        "per_minute": per_minute,
        "question": "§14 Q15 — intraday official-candle availability latency (bounds §2.6 cold-start warm-up)",
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    agg = report["aggregate"]
    if agg:
        print(f"q15: aggregate p50={agg['p50_s']:.1f}s p90={agg['p90_s']:.1f}s "
              f"p99={agg['p99_s']:.1f}s over {agg['n']} samples → {out}")
    else:
        print(f"q15: no samples collected (all timeouts?) → {out}")
    return 0


if __name__ == "__main__":  # pragma: no cover - owner measurement tool
    raise SystemExit(main())
