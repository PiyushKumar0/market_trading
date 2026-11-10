"""RSS summary and article-body capture for the macro mood-signal study.

Runs every 15 minutes from Windows Task Scheduler. Writes only its own SQLite file; the engine's
stores are never opened. The first capture of a url wins: an edited title or a later page change
never overwrites, and each url's page is fetched once.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "research" / "news_content.sqlite"

# Frozen copy of config/settings.yaml news.feeds.rss as of 2026-11-10; deliberately not read from settings.
FEEDS: dict[str, str] = {
    "et": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "livemint_markets": "https://www.livemint.com/rss/markets",
    "livemint_companies": "https://www.livemint.com/rss/companies",
    "hbl_markets": "https://www.thehindubusinessline.com/markets/feeder/default.rss",
    "hbl_companies": "https://www.thehindubusinessline.com/companies/feeder/default.rss",
    "cnbctv18_market": "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml",
    "ndtvprofit": "https://feeds.feedburner.com/ndtvprofit-latest",
    "bs_markets": "https://www.business-standard.com/rss/markets-106.rss",
    "bs_companies": "https://www.business-standard.com/rss/companies-101.rss",
}

BODY_WORDS = 400
MIN_PARAGRAPH_WORDS = 8
PAGE_GAP_S = 2.0
LOCK_STALE_S = 30 * 60
FETCH_TIMEOUT_S = 30

_SCHEMA = """
CREATE TABLE IF NOT EXISTS items(
    url TEXT PRIMARY KEY, feed TEXT, title TEXT, summary TEXT, published_at TEXT,
    first_captured_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bodies(
    url TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, http_status INTEGER,
    source TEXT CHECK(source IN ('jsonld','p','none')), text TEXT);
CREATE TABLE IF NOT EXISTS capture_runs(
    run_at TEXT, feed TEXT, items_seen INTEGER, items_new INTEGER, error TEXT);
"""

_TAG = re.compile(r"<[^>]+>")
_SCRIPT_STYLE = re.compile(r"<(script|style)\b.*?</\1\s*>", re.I | re.S)
_LDJSON = re.compile(r"<script\b[^>]*application/ld\+json[^>]*>(.*?)</script\s*>", re.I | re.S)
_PARA = re.compile(r"<p\b[^>]*>(.*?)</p\s*>", re.I | re.S)


def _text(raw: str) -> str:
    """Tags stripped, entities unescaped, whitespace collapsed."""
    return " ".join(html.unescape(_TAG.sub(" ", raw)).split())


def _pubdate(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw.strip())
        if parsed.tzinfo is None:  # "-0000" style unknown-offset dates parse naive: treat as UTC
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(IST).isoformat()
    except (ValueError, TypeError):
        return None


def parse_feed(xml_bytes: bytes) -> list[dict]:
    """RSS items as url/title/summary/published_at. Raises ET.ParseError on malformed XML."""
    root = ET.fromstring(xml_bytes)
    out: list[dict] = []
    for item in root.iter("item"):
        url = (item.findtext("link") or "").strip()
        if not url:
            continue
        desc = item.findtext("description")
        out.append({
            "url": url,
            "title": _text(item.findtext("title") or ""),
            "summary": (_text(desc) or None) if desc else None,
            "published_at": _pubdate(item.findtext("pubDate")),
        })
    return out


def _cut(text: str) -> str:
    return " ".join(text.split()[:BODY_WORDS])


def _article_bodies(node):
    if isinstance(node, list):
        for n in node:
            yield from _article_bodies(n)
    elif isinstance(node, dict):
        kind = node.get("@type")
        kinds = kind if isinstance(kind, list) else [kind]
        if any(isinstance(k, str) and "Article" in k for k in kinds) and isinstance(node.get("articleBody"), str):
            yield node["articleBody"]
        yield from _article_bodies(node.get("@graph"))


def extract_body(page: str) -> tuple[str, str]:
    """(source, first 400 words): JSON-LD articleBody, else the long <p> blocks, else ('none', '')."""
    for block in _LDJSON.findall(page):
        try:
            data = json.loads(block.strip())
        except ValueError:
            continue
        for body in _article_bodies(data):
            text = _cut(_text(body))
            if text:
                return "jsonld", text
    stripped = _SCRIPT_STYLE.sub(" ", page)
    paras = [t for t in (_text(p) for p in _PARA.findall(stripped)) if len(t.split()) >= MIN_PARAGRAPH_WORDS]
    if paras:
        return "p", _cut(" ".join(paras))
    return "none", ""


def default_fetch(url: str) -> tuple[int, bytes]:
    """(status, body); an HTTP error status is returned, a network failure raises."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_S) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, b""


def _acquire_lock(path: Path) -> bool:
    for _ in range(2):
        try:
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
            return True
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age < LOCK_STALE_S:
                return False
            try:
                path.unlink()
            except FileNotFoundError:
                pass
    return False


def capture(
    db: Path,
    fetch: Callable[[str], tuple[int, bytes]],
    now: Callable[[], datetime],
    *,
    max_new: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """One pass over FEEDS. Returns {'skipped', 'seen', 'new', 'feeds': {name: (seen, new)}}."""
    db.parent.mkdir(parents=True, exist_ok=True)
    lock = db.with_name("news_content.lock")
    if not _acquire_lock(lock):
        return {"skipped": True, "seen": 0, "new": 0, "feeds": {}}
    try:
        return _capture_locked(db, fetch, now, max_new, sleep)
    finally:
        lock.unlink(missing_ok=True)


def _capture_locked(db, fetch, now, max_new, sleep) -> dict:
    stamp = lambda: now().astimezone(IST).isoformat()  # noqa: E731
    con = sqlite3.connect(db, timeout=30)
    feeds: dict[str, tuple[int, int]] = {}
    total_new = 0
    fetched_page = False
    try:
        con.executescript(_SCHEMA)
        run_at = stamp()
        for name, feed_url in FEEDS.items():
            seen = new = 0
            error = None
            try:
                status, content = fetch(feed_url)
                if status != 200:
                    raise RuntimeError(f"http {status}")
                items = parse_feed(content)
                seen = len(items)
                for it in items:
                    if max_new is not None and total_new >= max_new:
                        break
                    cur = con.execute(
                        "INSERT OR IGNORE INTO items VALUES (?,?,?,?,?,?)",
                        (it["url"], name, it["title"], it["summary"], it["published_at"], stamp()))
                    con.commit()
                    if cur.rowcount == 0:
                        continue
                    new += 1
                    total_new += 1
                    if fetched_page:
                        sleep(PAGE_GAP_S)
                    fetched_page = True
                    _store_body(con, it["url"], fetch, stamp)
            except Exception as exc:  # a feed failure is data, never a crash
                error = f"{type(exc).__name__}: {exc}"
            con.execute("INSERT INTO capture_runs VALUES (?,?,?,?,?)", (run_at, name, seen, new, error))
            con.commit()
            feeds[name] = (seen, new)
    finally:
        con.close()
    return {"skipped": False, "seen": sum(s for s, _ in feeds.values()), "new": total_new, "feeds": feeds}


def _store_body(con: sqlite3.Connection, url: str, fetch, stamp: Callable[[], str]) -> None:
    status: int | None = None
    source, text = "none", ""
    try:
        status, content = fetch(url)
        if status == 200:
            source, text = extract_body(content.decode("utf-8", errors="replace"))
    except Exception:  # network failure: recorded as no body; the url is not retried
        pass
    # Stamped once the fetch has returned: a request suspended by host standby across 09:00 can
    # return a page edited after the open, and the stamp is what the 09:00 body cut-off reads.
    fetched_at = stamp()
    con.execute("INSERT OR IGNORE INTO bodies VALUES (?,?,?,?,?)", (url, fetched_at, status, source, text))
    con.commit()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--max-new", type=int, default=None, help="stop after N new items in this run")
    args = ap.parse_args(argv)
    try:
        res = capture(args.db, default_fetch, lambda: datetime.now(IST), max_new=args.max_new)
    except Exception as exc:  # the scheduler must always see exit 0
        print(f"capture failed: {type(exc).__name__}: {exc}")
        return 0
    if res["skipped"]:
        print("skipped: another run holds the lock")
    for name, (seen, new) in res["feeds"].items():
        print(f"{name}: seen={seen} new={new}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
