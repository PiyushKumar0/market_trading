#!/usr/bin/env python
"""Best-effort re-scrape of the Zerodha charges page -> prints a config/costs.yaml DIFF proposal (C2).

Run before EACH release (C2: broker fees now change mid-year). This script NEVER writes
``config/costs.yaml`` — it prints a human-reviewed proposal (path, current value, scraped value,
and the matched page snippet as evidence). The owner applies accepted changes by hand and bumps
``verified_on``; the §9.1 CostModel worked-example tests then re-anchor against the new rates.

Method: fetch https://zerodha.com/charges (or ``--html-file`` for an offline/saved copy), strip the
HTML to text, and run a table of labelled regexes for the equity charge classes the CostModel
consumes (brokerage, STT, exchange txn, SEBI, stamp, GST, DP). Extraction is deliberately
BEST-EFFORT (the page is marketing HTML, not an API): anything not confidently matched is reported
as "not found — verify by hand", never guessed. The DP charge published as "Rs X + GST per scrip"
is normalized to the GST-inclusive per-scrip-per-sell-day figure costs.yaml stores.

Exit codes: 0 = scrape ran and the diff (possibly empty) was printed; 1 = fetch/read failure;
2 = page fetched but nothing recognizable was extracted (page structure changed — update patterns).
"""

from __future__ import annotations

import argparse
import html as _html
import re
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import httpx
import yaml

DEFAULT_URL = "https://zerodha.com/charges"
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "costs.yaml"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) market-trading-rescrape/1.0"

_PAISA = Decimal("0.01")
_NUM = r"([0-9]+(?:\.[0-9]+)?)"
_RS = r"(?:Rs\.?|₹|INR)\s*"


def _strip_html(raw: str) -> str:
    """HTML -> whitespace-collapsed text (no bs4 dependency — best-effort by design)."""
    text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", raw)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = _html.unescape(text)
    return re.sub(r"\s+", " ", text)


def _snippet(text: str, m: re.Match, pad: int = 40) -> str:
    return text[max(m.start() - pad, 0) : m.end() + pad].strip()


def scrape_rates(text: str) -> tuple[dict[str, Decimal], dict[str, str]]:
    """Extract {dotted costs.yaml path: Decimal} + {path: evidence snippet} from page text.

    Only confidently matched charge classes are returned. Regexes are anchored on the page's
    published phrasings (e.g. "0.03% or Rs. 20/executed order whichever is lower"); a rephrase
    simply drops that class from the result — flagged, never guessed.
    """
    found: dict[str, Decimal] = {}
    evidence: dict[str, str] = {}

    def put(path: str, value: Decimal, m: re.Match) -> None:
        if path not in found:                       # first match wins (equity section leads the page)
            found[path] = value
            evidence[path] = _snippet(text, m)

    # Brokerage — delivery free; intraday "0.03% or Rs 20 per executed order, whichever is lower".
    m = re.search(r"(?i)(?:free|zero(?:\s+brokerage)?)[^.]{0,60}?equity\s+delivery", text) or re.search(
        r"(?i)equity\s+delivery[^.]{0,80}?(?:free|zero\s+brokerage)", text
    )
    if m:
        put("brokerage.delivery_cnc.pct", Decimal("0"), m)
        put("brokerage.delivery_cnc.flat_inr", Decimal("0"), m)
    m = re.search(
        rf"(?i){_NUM}\s*%\s*or\s*{_RS}{_NUM}\s*(?:/|per\s+)executed\s+order[^.]{{0,60}}?whichever\s+is\s+lower",
        text,
    )
    if m:
        put("brokerage.intraday_mis.pct", Decimal(m.group(1)), m)
        put("brokerage.intraday_mis.cap_inr", Decimal(m.group(2)), m)

    # STT — delivery "0.1% on buy & sell"; intraday "0.025% on the sell side" (equity leads F&O).
    m = re.search(rf"(?i){_NUM}\s*%\s*on\s*buy\s*&\s*sell", text)
    if m:
        put("stt.delivery.buy_pct", Decimal(m.group(1)), m)
        put("stt.delivery.sell_pct", Decimal(m.group(1)), m)
    m = re.search(rf"(?i){_NUM}\s*%\s*on\s*the\s*sell\s*side", text)
    if m:
        put("stt.intraday.buy_pct", Decimal("0"), m)
        put("stt.intraday.sell_pct", Decimal(m.group(1)), m)

    # Exchange transaction charge — "NSE: 0.00297%" (first occurrence = equity columns).
    m = re.search(rf"(?i)NSE:?\s*{_NUM}\s*%", text)
    if m:
        put("exchange_txn_charge.nse_pct_per_side", Decimal(m.group(1)), m)

    # SEBI — "Rs 10 / crore" within the SEBI charges section (stamp also quotes /crore, so anchor).
    m = re.search(rf"(?i)SEBI[^%]{{0,120}}?{_RS}{_NUM}\s*/\s*crore", text)
    if m:
        put("sebi_charge.per_crore_inr", Decimal(m.group(1)), m)

    # Stamp duty — "0.015% or Rs 1500 / crore on buy side" (delivery) then intraday 0.003%/300.
    stamp = list(re.finditer(rf"(?i){_NUM}\s*%\s*or\s*{_RS}[0-9,]+\s*/\s*crore\s*on\s*buy\s*side", text))
    if stamp:
        put("stamp_duty.delivery_buy_pct", Decimal(stamp[0].group(1)), stamp[0])
    if len(stamp) >= 2:
        put("stamp_duty.intraday_buy_pct", Decimal(stamp[1].group(1)), stamp[1])

    # GST — "18% on (brokerage + SEBI charges + transaction charges)".
    m = re.search(rf"(?i){_NUM}\s*%\s*on\s*\(?\s*brokerage", text)
    if m:
        put("gst.pct", Decimal(m.group(1)), m)

    # DP charge — "Rs 13.5 + GST per scrip" (normalize to GST-inclusive) or a flat "Rs X per scrip".
    m = re.search(rf"(?i){_RS}{_NUM}(\s*\+\s*GST)?\s*(?:\(.{{0,40}}?\)\s*)?per\s+scrip", text)
    if m:
        dp = Decimal(m.group(1))
        if m.group(2):
            gst_pct = found.get("gst.pct", Decimal("18"))
            dp = (dp * (1 + gst_pct / 100)).quantize(_PAISA, rounding=ROUND_HALF_UP)
        put("dp_charge.per_scrip_per_sell_day_inr", dp, m)

    return found, evidence


def _yaml_get(raw: dict, dotted: str):
    node = raw
    for key in dotted.split("."):
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=DEFAULT_URL, help="charges page URL")
    parser.add_argument("--html-file", type=Path, default=None, help="parse a saved HTML file instead of fetching")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="path to config/costs.yaml")
    args = parser.parse_args(argv)

    try:
        current = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    except OSError as exc:
        print(f"ERROR: cannot read {args.config}: {exc}")
        return 1

    if args.html_file is not None:
        try:
            raw = args.html_file.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            print(f"ERROR: cannot read {args.html_file}: {exc}")
            return 1
        source = str(args.html_file)
    else:
        try:
            resp = httpx.get(args.url, headers={"User-Agent": _UA}, timeout=30.0, follow_redirects=True)
            resp.raise_for_status()
            raw = resp.text
        except httpx.HTTPError as exc:
            print(f"ERROR: fetch failed for {args.url}: {type(exc).__name__}: {exc}")
            print("Best-effort scrape aborted -- verify https://zerodha.com/charges by hand (C2).")
            return 1
        source = args.url

    found, evidence = scrape_rates(_strip_html(raw))
    if not found:
        print(f"ERROR: no charge classes recognized on {source} -- page structure changed.")
        print("Update scripts/rescrape_costs.py patterns and verify the page by hand (C2).")
        return 2

    print(f"# costs.yaml diff proposal -- scraped from {source}")
    print(f"# current file: {args.config} (verified_on: {current.get('verified_on')!r})")
    print("# NOT applied automatically -- review, edit config/costs.yaml by hand, bump verified_on.")
    print()
    changes = 0
    for path in sorted(found):
        cur = _yaml_get(current, path)
        new = found[path]
        cur_dec = None if cur is None else Decimal(str(cur))
        if cur_dec is not None and cur_dec == new:
            print(f"  OK        {path} = {new}")
        else:
            changes += 1
            print(f"  CHANGED   {path}: {cur_dec} -> {new}")
            print(f"            evidence: ...{evidence[path]}...")
    missing = [
        p for p in (
            "brokerage.delivery_cnc.pct", "brokerage.intraday_mis.pct", "brokerage.intraday_mis.cap_inr",
            "stt.delivery.buy_pct", "stt.delivery.sell_pct", "stt.intraday.sell_pct",
            "exchange_txn_charge.nse_pct_per_side", "sebi_charge.per_crore_inr",
            "stamp_duty.delivery_buy_pct", "stamp_duty.intraday_buy_pct", "gst.pct",
            "dp_charge.per_scrip_per_sell_day_inr",
        ) if p not in found
    ]
    for path in missing:
        print(f"  NOT FOUND {path} -- verify by hand")
    print()
    if changes:
        print(f"{changes} proposed change(s) above. costs.yaml was NOT modified.")
    else:
        print("No rate changes detected against the current costs.yaml.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
