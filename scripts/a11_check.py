#!/usr/bin/env python
"""A11 empirical check — are Kite *minute* candles corporate-action adjusted? (§4.4 job 11, §8.1).

One-off Phase-0/1 task (plan §1.4 item, §4.4 job 11, §8.1 "A11 check", §14 Q10). The plan provisionally
assumes minute candles are **not** corp-action adjusted while DAILY candles **are** (A11; see `bars_1d`
"adjusted per A11 finding", §4.3); this script answers it empirically against a real recent split/bonus
ex-date so the bar-stitching job can be configured correctly.

Method
------
A corporate action (split / bonus) introduces a mechanical price discontinuity at the ex-date: the
"raw" pre-ex price is divided by the action ratio R on the ex-date. Two ways the candle API can present
history across that ex-date:

  * **adjusted**   — every pre-ex price is divided by R, so the series is *continuous* across the ex-date
                     (the gap between the last pre-ex bar and the first ex-date bar is ~normal overnight).
  * **unadjusted** — pre-ex prices are left raw, so there is a large mechanical *jump* (~factor R) between
                     the last pre-ex bar and the first ex-date bar.

DAILY candles are corp-action adjusted (the A11 baseline). We measure the *daily* close-to-close gap
across the ex-date (this is the adjusted/continuous reference) and the *minute* gap (last pre-ex session
close → first ex-date session open). If the minute gap matches the raw mechanical jump (≈ factor R), the
minute series is **unadjusted**; if it matches the daily/adjusted continuity, it is **adjusted**.

Session / network
-----------------
Talks to Kite REST directly via ``pykiteconnect`` (the ``engine.broker`` REST wrappers are not built yet
in Phase 0), using the daily access token stored in Windows Credential Manager / DPAPI via
``engine.core.secrets`` (R10). All network calls are guarded; with no valid session the script prints how
to log in first and exits non-zero. Prices are ``Decimal`` throughout. This script NEVER writes config —
it prints the instruction to record the answer in ``config/settings.yaml`` (``data.minute_candles_adjusted``)
and in plan §14 Q10.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

# --- repo src on sys.path so `engine.*` imports resolve when run as a loose script -------------------
_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.core.secrets import (  # noqa: E402
    KITE_ACCESS_TOKEN,
    KITE_API_KEY,
    MissingSecretError,
    Secrets,
)

IST = ZoneInfo("Asia/Kolkata")
_log = get_logger("scripts.a11_check")

# Exit codes (so a wrapping smoke harness can branch).
EXIT_OK = 0
EXIT_NO_SESSION = 2
EXIT_NETWORK = 3
EXIT_INSUFFICIENT_DATA = 4
EXIT_BAD_ARGS = 5

# A relative price discontinuity at the ex-date larger than this fraction is treated as a raw mechanical
# corp-action jump rather than normal overnight drift. Splits/bonuses are >=10% by construction; an
# ordinary overnight move is rarely this large, so the boundary is wide and robust. [tunable]
_RAW_JUMP_THRESHOLD = Decimal("0.08")


# ----------------------------------------------------------------------------- session helpers
def _login_help(secrets: Secrets) -> str:
    """Human-readable instructions for getting a valid Kite session before re-running."""
    have_key = secrets.has(KITE_API_KEY)
    have_tok = secrets.has(KITE_ACCESS_TOKEN)
    lines = [
        "No usable Kite session. The A11 check needs a valid daily access token (A5).",
        f"  kite_api_key present:      {have_key}",
        f"  kite_access_token present: {have_tok}",
        "",
        "To fix:",
        "  1. Seed kite_api_key / kite_api_secret once via:  python scripts/dpapi_set.py",
        "  2. Complete today's login (the token rotates ~06:00 IST, A5):",
        "       run the engine and tap the Telegram login URL, OR use the manual paste fallback",
        "       (SessionManager.complete_login) — both store kite_access_token via DPAPI.",
        "  3. Re-run:  python scripts/a11_check.py --symbol <SYM> --ex-date YYYY-MM-DD",
        "",
        "You can also pass an explicit token for a one-off run:  --token <instrument_token>",
        "and override the access token via the KITE_ACCESS_TOKEN env var if needed.",
    ]
    return "\n".join(lines)


def _build_kite(api_key: str, access_token: str) -> Any:
    """Construct an authenticated ``KiteConnect``. Import is local so the script imports cleanly even
    when the heavy broker dep is not installed (the owner installs it; pyproject pins pykiteconnect==5.2.0)."""
    try:
        from kiteconnect import KiteConnect
    except ImportError as exc:  # pragma: no cover - depends on owner-installed deps
        print(
            "pykiteconnect is not installed in this environment. Install project deps "
            "(uv sync / pip install pykiteconnect==5.2.0) and re-run.",
            file=sys.stderr,
        )
        raise SystemExit(EXIT_NO_SESSION) from exc
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


# ----------------------------------------------------------------------------- candle fetch
def _to_decimal(value: Any) -> Decimal:
    """Coerce a Kite candle price to ``Decimal`` without float corruption (route through ``str``)."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise ValueError(f"non-numeric price from candle API: {value!r}") from exc


@dataclass(frozen=True)
class Candle:
    """One OHLC candle; ``ts`` is tz-aware IST, prices are ``Decimal``."""

    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


def _parse_candle(row: dict[str, Any]) -> Candle:
    ts = row["date"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    else:
        ts = ts.astimezone(IST)
    return Candle(
        ts=ts,
        open=_to_decimal(row["open"]),
        high=_to_decimal(row["high"]),
        low=_to_decimal(row["low"]),
        close=_to_decimal(row["close"]),
        volume=int(row.get("volume", 0) or 0),
    )


def _fetch_candles(
    kite: Any, token: int, frm: datetime, to: datetime, interval: str
) -> list[Candle]:
    """Fetch historical candles, guarding the network call. Raises ``SystemExit`` on auth/network errors."""
    try:
        raw = kite.historical_data(
            instrument_token=token,
            from_date=frm,
            to_date=to,
            interval=interval,
            continuous=False,
            oi=False,
        )
    except Exception as exc:  # noqa: BLE001 - classify and exit cleanly
        from kiteconnect import exceptions as kx  # local import; dep is owner-installed

        if isinstance(exc, (kx.TokenException, kx.PermissionException)):
            print(
                "Kite rejected the session (token expired/invalid). Re-login first.\n",
                file=sys.stderr,
            )
            print(_login_help(Secrets()), file=sys.stderr)
            raise SystemExit(EXIT_NO_SESSION) from exc
        _log.error("historical_fetch_failed", interval=interval, error=str(exc))
        print(f"Network/API error fetching {interval} candles: {exc}", file=sys.stderr)
        raise SystemExit(EXIT_NETWORK) from exc
    candles = [_parse_candle(r) for r in raw]
    candles.sort(key=lambda c: c.ts)
    return candles


def _resolve_token(kite: Any, symbol: str, exchange: str) -> int:
    """Resolve an NSE tradingsymbol to its instrument token via the instruments dump (A10)."""
    try:
        instruments = kite.instruments(exchange)
    except Exception as exc:  # noqa: BLE001
        _log.error("instruments_fetch_failed", exchange=exchange, error=str(exc))
        print(
            f"Could not download the {exchange} instruments list to resolve {symbol!r}: {exc}\n"
            "Pass the instrument token directly with --token to skip this lookup.",
            file=sys.stderr,
        )
        raise SystemExit(EXIT_NETWORK) from exc
    want = symbol.strip().upper()
    for inst in instruments:
        if str(inst.get("tradingsymbol", "")).upper() == want:
            return int(inst["instrument_token"])
    print(
        f"Symbol {symbol!r} not found in the {exchange} instruments dump. "
        "Check the tradingsymbol, or pass --token explicitly.",
        file=sys.stderr,
    )
    raise SystemExit(EXIT_BAD_ARGS)


# ----------------------------------------------------------------------------- analysis
def _last_before(candles: list[Candle], boundary: date) -> Candle | None:
    """Last candle strictly before the ex-date (the final pre-ex print)."""
    prior = [c for c in candles if c.ts.date() < boundary]
    return prior[-1] if prior else None


def _first_on_or_after(candles: list[Candle], boundary: date) -> Candle | None:
    """First candle on or after the ex-date (the first ex-date print)."""
    later = [c for c in candles if c.ts.date() >= boundary]
    return later[0] if later else None


def _rel_gap(pre: Decimal, post: Decimal) -> Decimal:
    """Signed relative gap (post-pre)/pre as a ``Decimal`` fraction."""
    if pre == 0:
        raise ValueError("pre-ex price is zero; cannot compute relative gap")
    return (post - pre) / pre


@dataclass(frozen=True)
class Verdict:
    minute_candles_adjusted: bool | None  # None = inconclusive
    daily_gap: Decimal                    # adjusted/continuous reference (daily close->close across ex)
    minute_gap: Decimal                   # last pre-ex minute close -> first ex-date minute open
    implied_minute_ratio: Decimal         # pre/post on the minute series (~R if unadjusted, ~1 if adjusted)
    pre_minute_close: Decimal
    post_minute_open: Decimal
    pre_daily_close: Decimal
    post_daily_close: Decimal
    notes: list[str]


def _decide(
    daily: list[Candle], minute: list[Candle], ex_date: date
) -> Verdict:
    notes: list[str] = []

    d_pre = _last_before(daily, ex_date)
    d_post = _first_on_or_after(daily, ex_date)
    m_pre = _last_before(minute, ex_date)
    m_post = _first_on_or_after(minute, ex_date)

    if not (d_pre and d_post):
        print(
            "Insufficient DAILY candles spanning the ex-date — widen --window-days or check the ex-date.",
            file=sys.stderr,
        )
        raise SystemExit(EXIT_INSUFFICIENT_DATA)
    if not (m_pre and m_post):
        print(
            "Insufficient MINUTE candles spanning the ex-date. Kite minute history is range-limited "
            "(typically the last ~60 days per request and ~recent years total) — pick a MORE RECENT "
            "split/bonus ex-date, or widen --window-days.",
            file=sys.stderr,
        )
        raise SystemExit(EXIT_INSUFFICIENT_DATA)

    # Daily series is the adjusted reference: close-to-close across the ex-date should be ~continuous.
    daily_gap = _rel_gap(d_pre.close, d_post.close)
    # Minute series: last pre-ex print close -> first ex-date print open is where a raw jump would show.
    minute_gap = _rel_gap(m_pre.close, m_post.open)
    implied_ratio = (m_pre.close / m_post.open) if m_post.open != 0 else Decimal(0)

    notes.append(
        f"daily ref: last pre-ex {d_pre.ts.date()} close={d_pre.close} -> "
        f"first ex {d_post.ts.date()} close={d_post.close} (gap {daily_gap:+.4%})"
    )
    notes.append(
        f"minute:    last pre-ex {m_pre.ts:%Y-%m-%d %H:%M} close={m_pre.close} -> "
        f"first ex {m_post.ts:%Y-%m-%d %H:%M} open={m_post.open} (gap {minute_gap:+.4%})"
    )

    daily_continuous = abs(daily_gap) < _RAW_JUMP_THRESHOLD
    minute_jumped = abs(minute_gap) >= _RAW_JUMP_THRESHOLD

    if not daily_continuous:
        # The daily-adjusted baseline (A11) itself shows a jump: either this is not a clean split/bonus
        # ex-date, or daily candles are NOT adjusted as assumed. Don't claim a minute verdict.
        notes.append(
            "WARNING: the DAILY (assumed-adjusted) series ALSO shows a large gap across the ex-date — "
            "the A11 daily-adjusted baseline may not hold for this symbol/date (re-verify the ex-date, "
            "or that the action is a clean split/bonus). Minute verdict withheld."
        )
        return Verdict(
            minute_candles_adjusted=None,
            daily_gap=daily_gap,
            minute_gap=minute_gap,
            implied_minute_ratio=implied_ratio,
            pre_minute_close=m_pre.close,
            post_minute_open=m_post.open,
            pre_daily_close=d_pre.close,
            post_daily_close=d_post.close,
            notes=notes,
        )

    if minute_jumped:
        notes.append(
            f"minute series shows the RAW mechanical jump (~factor {implied_ratio:.3f}) while the daily "
            "series is continuous => minute candles are UNADJUSTED."
        )
        adjusted = False
    else:
        notes.append(
            "minute series is continuous across the ex-date (no raw jump), matching the daily-adjusted "
            "series => minute candles are ADJUSTED."
        )
        adjusted = True

    return Verdict(
        minute_candles_adjusted=adjusted,
        daily_gap=daily_gap,
        minute_gap=minute_gap,
        implied_minute_ratio=implied_ratio,
        pre_minute_close=m_pre.close,
        post_minute_open=m_post.open,
        pre_daily_close=d_pre.close,
        post_daily_close=d_post.close,
        notes=notes,
    )


# ----------------------------------------------------------------------------- reporting
def _print_report(symbol: str, ex_date: date, token: int, v: Verdict) -> None:
    sep = "=" * 78
    print(sep)
    print(f"A11 CHECK — are Kite MINUTE candles corp-action adjusted?  (§4.4 job 11, §14 Q10)")
    print(sep)
    print(f"symbol={symbol}  instrument_token={token}  ex_date={ex_date.isoformat()}")
    print("")
    print("Evidence:")
    for note in v.notes:
        print(f"  - {note}")
    print("")
    print(f"  daily close->close gap across ex-date : {v.daily_gap:+.4%}  (adjusted/continuous reference)")
    print(f"  minute last-pre-close -> first-ex-open : {v.minute_gap:+.4%}")
    print(f"  implied minute pre/post ratio          : {v.implied_minute_ratio:.4f}  (~1 adjusted, ~R unadjusted)")
    print("")

    if v.minute_candles_adjusted is None:
        print("VERDICT: minute_candles_adjusted = INCONCLUSIVE")
        print("         (see WARNING above — pick a clean recent split/bonus ex-date and re-run).")
    else:
        print(f"VERDICT: minute_candles_adjusted = {str(v.minute_candles_adjusted).lower()}")
    print(sep)
    print("")
    print("ACTION REQUIRED (this script does NOT write config — record the answer yourself):")
    if v.minute_candles_adjusted is None:
        print("  Verdict inconclusive; do NOT record a value yet. Re-run with a cleaner ex-date.")
    else:
        val = str(v.minute_candles_adjusted).lower()
        print(f"  1. Set in config/settings.yaml:")
        print(f"         data:")
        print(f"           minute_candles_adjusted: {val}")
        print(f"  2. Record the answer in IMPLEMENTATION_PLAN.md §14 Q10 (A11).")
        print(f"  3. Configure the bar-stitching / backfill job accordingly (A11): if minute candles are")
        if v.minute_candles_adjusted:
            print(f"     ADJUSTED, no re-adjustment is needed when stitching minute history across ex-dates.")
        else:
            print(f"     UNADJUSTED, the backfill job must corp-action-adjust minute bars across ex-dates")
            print(f"     (using CorpActionsJob ex-dates, A12) before they are comparable to daily/adjusted bars.")
    print(sep)


# ----------------------------------------------------------------------------- CLI
def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {s!r}") from exc


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="a11_check",
        description="A11: empirically determine whether Kite MINUTE candles are corp-action adjusted "
        "(§4.4 job 11, §8.1, §14 Q10). One-off Phase-0/1 task.",
    )
    p.add_argument("--symbol", required=True, help="NSE tradingsymbol, e.g. TATAMOTORS")
    p.add_argument(
        "--ex-date",
        required=True,
        type=_parse_date,
        help="Known recent split/bonus EX-DATE (YYYY-MM-DD). Pick a RECENT one — minute history is "
        "range-limited by Kite.",
    )
    p.add_argument(
        "--token",
        type=int,
        default=None,
        help="Instrument token (optional). If omitted, resolved from the NSE instruments dump.",
    )
    p.add_argument("--exchange", default="NSE", help="Exchange for the instruments dump (default NSE).")
    p.add_argument(
        "--window-days",
        type=int,
        default=10,
        help="Trading-day-ish padding on each side of the ex-date to fetch (default 10 calendar days).",
    )
    p.add_argument(
        "--api-key",
        default=None,
        help="Override Kite API key (default: from DPAPI secret kite_api_key).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    configure_logging(level="INFO")
    args = _build_arg_parser().parse_args(argv)

    secrets = Secrets()

    # --- credentials (R10): API key + daily access token ---
    api_key = args.api_key
    if not api_key:
        try:
            api_key = secrets.get(KITE_API_KEY)
        except MissingSecretError:
            print(_login_help(secrets), file=sys.stderr)
            return EXIT_NO_SESSION
    access_token = os.environ.get("KITE_ACCESS_TOKEN") or secrets.get_optional(KITE_ACCESS_TOKEN)
    if not access_token:
        print(_login_help(secrets), file=sys.stderr)
        return EXIT_NO_SESSION

    kite = _build_kite(api_key, access_token)

    # --- resolve instrument token ---
    token = args.token if args.token is not None else _resolve_token(kite, args.symbol, args.exchange)

    # --- fetch window spanning the ex-date (tz-aware IST; minute history is range-limited) ---
    pad = timedelta(days=max(1, args.window_days))
    frm = datetime.combine(args.ex_date - pad, time(9, 0), tzinfo=IST)
    to = datetime.combine(args.ex_date + pad, time(15, 30), tzinfo=IST)

    _log.info(
        "a11_fetch_window",
        symbol=args.symbol,
        token=token,
        ex_date=args.ex_date.isoformat(),
        frm=frm.isoformat(),
        to=to.isoformat(),
    )

    daily = _fetch_candles(kite, token, frm, to, "day")
    minute = _fetch_candles(kite, token, frm, to, "minute")

    _log.info("a11_fetched", daily_candles=len(daily), minute_candles=len(minute))

    verdict = _decide(daily, minute, args.ex_date)
    _print_report(args.symbol, args.ex_date, token, verdict)

    _log.info(
        "a11_verdict",
        symbol=args.symbol,
        ex_date=args.ex_date.isoformat(),
        minute_candles_adjusted=verdict.minute_candles_adjusted,
        daily_gap=str(verdict.daily_gap),
        minute_gap=str(verdict.minute_gap),
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
