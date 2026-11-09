"""``scripts/news_content_capture.py``: feed parsing, body extraction, first-capture-wins storage,
failure logging and the overlap lock."""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "news_content_capture.py"
_spec = importlib.util.spec_from_file_location("_news_content_capture_under_test", _PATH)
ncc = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = ncc
_spec.loader.exec_module(ncc)

FIX = Path(__file__).parent / "fixtures" / "news_content"
IST = ZoneInfo("Asia/Kolkata")
EMPTY_RSS = b"<rss><channel></channel></rss>"


def _now():
    return datetime(2026, 11, 10, 10, 0, tzinfo=IST)


def _fake(pages=None, a=None, b=None):
    """fetch(url): feed `et` serves feed_a, `livemint_markets` feed_b, other feeds are empty;
    pages come from `pages` (url -> (status, bytes)), default 404."""
    feeds = {
        ncc.FEEDS["et"]: (200, a if a is not None else (FIX / "feed_a.xml").read_bytes()),
        ncc.FEEDS["livemint_markets"]: (200, b if b is not None else (FIX / "feed_b.xml").read_bytes()),
    }
    calls: list[str] = []

    def fetch(url):
        calls.append(url)
        if url in feeds:
            return feeds[url]
        if url in ncc.FEEDS.values():
            return 200, EMPTY_RSS
        return (pages or {}).get(url, (404, b""))

    fetch.calls = calls
    return fetch


def _cap(db, fetch, **kw):
    return ncc.capture(db, fetch, _now, sleep=lambda s: None, **kw)


def test_parse_feed_strips_tags_and_keeps_summary():
    items = ncc.parse_feed((FIX / "feed_a.xml").read_bytes())
    assert [i["url"] for i in items] == ["https://a.example/1", "https://a.example/2", "https://a.example/3"]
    first = items[0]
    assert first["title"] == "Sensex & Nifty rally today"
    assert first["summary"] == "Markets rose on Rs 5 & more."
    assert first["published_at"] == "2026-11-10T09:30:00+05:30"
    assert items[1]["summary"] is None and items[1]["published_at"] is None
    assert items[2]["summary"] == "plain" and items[2]["published_at"] is None


def test_extract_body_prefers_jsonld_then_paragraphs_then_none():
    assert ncc.extract_body((FIX / "page_jsonld.html").read_text(encoding="utf-8")) == (
        "jsonld", "Nifty closed higher on Tuesday as banks & metals led gains.")
    source, text = ncc.extract_body((FIX / "page_paragraphs.html").read_text(encoding="utf-8"))
    assert source == "p"
    assert text.startswith("The central bank held rates steady on Tuesday")
    assert "Analysts expect the rupee" in text
    assert "Share" not in text and "Read more" not in text and "ignored script" not in text
    assert ncc.extract_body("<html><body><p>too short</p></body></html>") == ("none", "")
    assert ncc.extract_body("") == ("none", "")


def test_body_cut_to_400_words():
    page = "<p>" + " ".join(f"w{i}" for i in range(900)) + "</p>"
    source, text = ncc.extract_body(page)
    assert source == "p" and len(text.split()) == 400 and text.endswith("w399")


def test_capture_stores_each_url_once_and_never_refetches(tmp_path):
    db = tmp_path / "r" / "nc.sqlite"
    pages = {"https://a.example/1": (200, (FIX / "page_jsonld.html").read_bytes()),
             "https://b.example/1": (200, (FIX / "page_paragraphs.html").read_bytes())}
    res = _cap(db, _fake(pages))
    assert res["new"] == 4 and res["feeds"]["et"] == (3, 3) and res["feeds"]["livemint_markets"] == (2, 1)
    con = sqlite3.connect(db)
    assert con.execute("SELECT feed FROM items WHERE url='https://a.example/1'").fetchone() == ("et",)
    before = con.execute("SELECT url, fetched_at, source FROM bodies ORDER BY url").fetchall()
    assert [r[2] for r in before] == ["jsonld", "none", "none", "p"]
    assert con.execute("SELECT first_captured_at FROM items LIMIT 1").fetchone()[0] == "2026-11-10T10:00:00+05:30"

    edited = (FIX / "feed_a.xml").read_bytes().replace(b"rally  today", b"EDITED")
    fetch2 = _fake(pages, a=edited)
    later = lambda: datetime(2026, 11, 10, 11, 0, tzinfo=IST)  # noqa: E731
    res2 = ncc.capture(db, fetch2, later, sleep=lambda s: None)
    assert res2["new"] == 0
    assert not [c for c in fetch2.calls if c not in ncc.FEEDS.values()]
    assert con.execute("SELECT title FROM items WHERE url='https://a.example/1'").fetchone()[0] == (
        "Sensex & Nifty rally today")
    assert con.execute("SELECT url, fetched_at, source FROM bodies ORDER BY url").fetchall() == before
    assert con.execute("SELECT COUNT(*) FROM capture_runs").fetchone() == (18,)


def test_capture_sleeps_between_page_requests_only(tmp_path):
    slept: list[float] = []
    ncc.capture(tmp_path / "nc.sqlite", _fake(), _now, sleep=slept.append)
    assert slept == [2.0] * 3  # four pages, three gaps


def test_max_new_stops_the_run(tmp_path):
    res = _cap(tmp_path / "nc.sqlite", _fake(), max_new=2)
    assert res["new"] == 2
    con = sqlite3.connect(tmp_path / "nc.sqlite")
    assert con.execute("SELECT COUNT(*) FROM bodies").fetchone() == (2,)
    assert con.execute("SELECT COUNT(*) FROM capture_runs").fetchone() == (9,)


def test_capture_403_page_keeps_summary_and_records_status(tmp_path):
    db = tmp_path / "nc.sqlite"
    _cap(db, _fake({"https://a.example/1": (403, b"denied")}))
    con = sqlite3.connect(db)
    assert con.execute("SELECT http_status, source, text FROM bodies WHERE url='https://a.example/1'"
                       ).fetchone() == (403, "none", "")
    assert con.execute("SELECT summary FROM items WHERE url='https://a.example/1'").fetchone() == (
        "Markets rose on Rs 5 & more.",)


def test_feed_500_and_malformed_xml_are_logged_not_raised(tmp_path):
    db = tmp_path / "nc.sqlite"
    base = _fake(b=b"<rss><channel><item>")

    def fetch(url):
        if url == ncc.FEEDS["et"]:
            return 500, b""
        return base(url)

    res = _cap(db, fetch)
    assert res["new"] == 0
    errs = dict(sqlite3.connect(db).execute("SELECT feed, error FROM capture_runs").fetchall())
    assert "500" in errs["et"] and errs["livemint_markets"] and errs["hbl_markets"] is None


def test_feed_network_exception_is_logged(tmp_path):
    def fetch(url):
        raise OSError("boom")

    _cap(tmp_path / "nc.sqlite", fetch)
    rows = sqlite3.connect(tmp_path / "nc.sqlite").execute("SELECT error FROM capture_runs").fetchall()
    assert len(rows) == 9 and all("boom" in r[0] for r in rows)


def test_fresh_lock_skips_run_and_stale_lock_is_replaced(tmp_path):
    db = tmp_path / "nc.sqlite"
    lock = tmp_path / "news_content.lock"
    lock.write_text("")
    fetch = _fake()
    assert _cap(db, fetch)["skipped"] is True and fetch.calls == [] and lock.exists()
    old = time.time() - 31 * 60
    os.utime(lock, (old, old))
    res = _cap(db, fetch)
    assert res["skipped"] is False and res["new"] == 4
    assert not lock.exists()
