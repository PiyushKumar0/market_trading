"""News ingest — §2.7 step 1 / §3.2.4 ``NewsIngest`` / §4.4 job 10 (E5/E6 — never load-bearing).

HEADLINE-LEVEL ONLY: title, source domain, url, published_at. Article bodies are NEVER fetched —
cheap, and keeps the prompt-injection surface headline-sized (A3r). Output is UNTRUSTED Tier-1 input
only (§2.4): every ``news`` row is written ``untrusted=true`` (forced by ``MarketStore.insert_news``).

Feed set (CONFIG — ``settings.yaml news.feeds``; changing it is an owner config change, config_audit):

- **RSS**, a config-driven name → ``{url, poll_s}`` map (``news.feeds.rss``; seeds: ET Markets at
  5 min + Livemint markets/companies at 15 min). Parsed with stdlib ``xml.etree.ElementTree`` over an
  injected ``httpx.AsyncClient`` (convention 11 — E5 best-effort). Moneycontrol RSS was removed
  2026-08-04: the feed has been frozen since ~2024-04 (391 polls, zero inserts) and its silence left
  the corpus single-source, which no §2.7 corroboration gate can pass.
  Malformed items are tolerated: an item without a title or an absolute link is skipped; an item with
  a missing/unparsable ``pubDate`` keeps the headline with ``published_at`` = ingest time (Clock) —
  conservative-recent, never a naive datetime.
- **GDELT DOC 2.0** ``artlist`` JSON (15-min update granularity): the pinned query from settings plus
  a client-side domain filter to major Indian financial press (:data:`GDELT_DOMAIN_ALLOWLIST`).
  The ``timespan`` request parameter is the lookback window — the SAME poll with a widened
  ``timespan`` is the §4.4 job 10 off-period startup backfill (RSS feeds have a fixed publisher-side
  lookback; GDELT is the only feed with a controllable window, capped at
  ``news.gdelt_backfill_max_days`` ≈ the ~3-month DOC window, E6).
- **NSE corporate announcements** (``nse_ann``, §2.7 amendment 2026-09-04): the exchange as a source.
  The 30-session coverage audit found only 47% of each day's top-10 movers had ANY cluster naming
  them, while the filings that moved them (UNITDSPR's Reg-30 disclosure, HINDZINC's LoI ~2 h ahead of
  the press) sat unread on ``corporate-announcements``. Items become ordinary ``news`` rows with
  ``source_domain="nseindia.com"`` (so the exchange counts as ONE corroborating domain under
  ``catalyst_guard.min_source_domains``, never two) and a title prefixed by the explicit
  ``[NSE:<SYMBOL>]`` token :class:`~engine.datafeeds.news_pipeline.EntityResolver` honours — the
  payload carries ``symbol`` natively, so no alias guess is involved anywhere.

Dedupe is by URL: within the polled batch here, and against the ``news`` table by
``MarketStore.insert_news`` (idempotent re-polls / overlapping windows / startup backfill).

Failure model (E5): every feed fetch is individually guarded — a dead feed logs a structured warning
and contributes zero headlines; :meth:`NewsIngest.poll` never raises into the caller's loop. Feeds
down ⇒ clusters age out ⇒ empty watchlist (§2.7 fail-safe ladder); no news failure can FREEZE
anything.
"""

from __future__ import annotations

import html
import json
import math
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, field_validator
from ulid import ULID

from engine.core.clock import IST, Clock
from engine.core.config import NewsCfg
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.datafeeds.isin_map import NSE_ANNOUNCEMENTS_URL
from engine.marketdata.store import MarketStore

_log = get_logger("engine.datafeeds.news")

#: GDELT DOC 2.0 API endpoint (the query itself is config — ``news.feeds.gdelt_doc_query``).
GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

#: §3.2.4 / §4.4 job 10: GDELT results are domain-filtered to major Indian financial press.
#: Module-pinned (not settings) in Phase 1 — widening it is a code change the owner reviews, same
#: posture as the feed set being config_audit'd. Compared against the normalized registrable host
#: (lowercase, ``www.`` stripped).
GDELT_DOMAIN_ALLOWLIST: frozenset[str] = frozenset({
    "economictimes.indiatimes.com",
    "moneycontrol.com",
    "livemint.com",
    "business-standard.com",
    "financialexpress.com",
    "thehindubusinessline.com",
    "businesstoday.in",
    "ndtvprofit.com",
    "cnbctv18.com",
    "zeebiz.com",
})

#: The NON-RSS feed keys accepted by :meth:`NewsIngest.poll`. The RSS keys are config-driven
#: (``news.feeds.rss``), so the accepted set is built per-instance in ``poll``; each source has its
#: own cadence (per-feed ``poll_s`` / ``news.gdelt_poll_s`` / ``news.feeds.nse_announcements.poll_s``)
#: and the scheduler may poll each alone.
GDELT_KEY = "gdelt"
NSE_ANN_KEY = "nse_ann"

#: The ONLY ``corporate-announcements`` fields this feed reads, pinned against the LIVE payload
#: (probed once through :func:`engine.core.nse_http.nse_get`, 2026-09-04 15:14 IST). The response is
#: a bare JSON list of rows; one row verbatim::
#:
#:     {"an_dt": "04-Sep-2026 15:14:44", "attFileSize": "2.60 MB",
#:      "attchmntFile": "https://nsearchives.nseindia.com/corporate/MOHITIND_04092026151409_NOTICEMIL04092026.pdf",
#:      "attchmntText": "Mohit Industries Limited has informed the Exchange regarding Notice of
#:                       Annual General Meeting to be held on September 30, 2026",
#:      "bflag": null, "csvName": null, "desc": "Shareholders meeting", "difference": "00:00:01",
#:      "dt": "04092026151444", "exchdisstime": "04-Sep-2026 15:14:45", "fileSize": "2.60 MB",
#:      "hasXbrl": true, "old_new": null, "orgid": null, "seq_id": "106769737",
#:      "smIndustry": "Textile Products", "sm_isin": "INE954E01012",
#:      "sm_name": "Mohit Industries Limited", "sort_date": "2026-09-04 15:14:44",
#:      "symbol": "MOHITIND"}
#:
#: ``desc`` is the SUBJECT (the announcement category ``news.feeds.nse_announcements.drop_subjects``
#: filters on); ``attchmntText`` is the one-line disclosure body. There is no ``subject`` key in this
#: payload — the plan's "subject-or-attachment-text" is exactly this pair.
NSE_ANN_FIELDS: tuple[str, ...] = (
    "symbol", "desc", "attchmntText", "attchmntFile", "sort_date", "exchdisstime", "an_dt", "seq_id",
)

#: Deterministic per-announcement URL for an item with no attachment. Dedupe is URL-based, so every
#: item needs a stable unique key; ``seq_id`` is the exchange's own monotonic announcement id.
NSE_ANN_ITEM_URL = "https://www.nseindia.com/companies-listing/corporate-filings-announcements#{seq_id}"

#: The exchange is ONE corroborating domain no matter which host serves the PDF (attachments live on
#: ``nsearchives.nseindia.com``) — pinned, never derived from the item URL.
NSE_ANN_DOMAIN = "nseindia.com"

#: Exchange timestamps, tried in this order. ``sort_date`` first because it is the only
#: locale-INDEPENDENT form (``%b`` parsing follows LC_TIME); live they differ by ~1 s
#: (an_dt 15:14:44 → exchdisstime 15:14:45), which is immaterial at a 300 s cadence.
_ANN_DT_FIELDS: tuple[tuple[str, str], ...] = (
    ("sort_date", "%Y-%m-%d %H:%M:%S"),
    ("exchdisstime", "%d-%b-%Y %H:%M:%S"),
    ("an_dt", "%d-%b-%Y %H:%M:%S"),
)

#: §2.7: stored titles are headline-sized (A3r) — the disclosure body can run to a paragraph.
NSE_ANN_TITLE_MAX = 300


def _clean_title(title: str) -> str:
    """Whitespace-collapse + HTML-unescape. ET double-escapes entities in RSS ("F&amp;amp;O"
    survives XML parsing as "F&amp;O") — stored titles must hold the human form ("F&O", "M&M")
    or drop-pattern and alias matching silently miss (G1 seed-6 row 23)."""
    return html.unescape(" ".join(title.split()))


class Headline(BaseModel):
    """One ingested headline (§3.2.4 ``NewsIngest.poll`` output; §4.3 ``news`` row shape).

    HEADLINE-LEVEL ONLY (A3r): no body, no summary. ``published_at`` is tz-aware IST always
    (§3.2 convention). ``headline_id`` is the platform-assigned ULID minted at ingest — the key
    ``HeadlineClusterer`` uses to link ``news.cluster_id`` (§2.7 step 2).
    """

    model_config = ConfigDict(frozen=True)

    headline_id: str | None = None
    title: str
    source_domain: str
    url: str
    published_at: datetime

    @field_validator("published_at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("published_at must be tz-aware (naive datetimes are a bug, §3.2)")
        return v.astimezone(IST)


def _domain(url: str) -> str:
    """Registrable host of ``url``, normalized: lowercase, port and leading ``www.`` stripped."""
    host = urlsplit(url).netloc.lower().rsplit("@", 1)[-1].split(":", 1)[0]
    return host[4:] if host.startswith("www.") else host


class NewsIngest:
    """§3.2.4 ``NewsIngest`` — best-effort headline poller writing ``news`` rows (untrusted, §2.4).

    Parameters
    ----------
    cfg:
        The typed ``settings.yaml news`` block (feed URLs, cadences, backfill windows).
    store:
        The single-writer :class:`MarketStore` (convention 12) — all persistence goes through it.
    clock:
        Single source of "now" (§3.2) — the fallback ``published_at`` stamp and lookback anchor.
    http:
        Injected ``httpx.AsyncClient`` (convention 11 / E5). Owned by the caller; never closed here.
    """

    def __init__(
        self,
        cfg: NewsCfg,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        request_timeout_s: float | None = None,
    ) -> None:
        self._cfg = cfg
        self._store = store
        self._clock = clock
        self._http = http
        # None ⇒ the settings value (news.request_timeout_s); an explicit argument still wins so
        # tests/probes can tighten it.
        self._timeout = float(request_timeout_s if request_timeout_s is not None else cfg.request_timeout_s)

    # ------------------------------------------------------------------ public surface
    async def poll(
        self,
        *,
        feeds: tuple[str, ...] | list[str] | None = None,
        lookback_h: float | None = None,
    ) -> list[Headline]:
        """Poll the configured feeds, dedupe by URL, persist new ``news`` rows; return the NEW headlines.

        ``feeds`` selects a subset of the valid keys — every ``news.feeds.rss`` name plus
        :data:`GDELT_KEY` and :data:`NSE_ANN_KEY` (None = all of them) — so the scheduler can honor
        the distinct §3.2.4 per-feed cadences. ``lookback_h`` widens the GDELT ``timespan`` window —
        the off-period backfill knob (§4.4 job 10); None = the routine window (2× ``gdelt_poll_s``, so
        consecutive polls overlap and boundary items are never missed — URL dedupe absorbs the overlap).

        :data:`NSE_ANN_KEY` stays a VALID key even when the feed is disabled (it just fetches
        nothing): an owner toggling config off must not turn an armed poll into a ValueError.

        Never raises for a feed failure (E5): each source is fetched under its own guard and a dead
        feed just contributes nothing. Returns only the headlines actually INSERTED (post-dedupe),
        each carrying its minted ``headline_id`` — the §2.7 step-2 clusterer input.
        """
        valid = tuple(self._cfg.feeds.rss) + (GDELT_KEY, NSE_ANN_KEY)
        selected = tuple(feeds) if feeds is not None else valid
        unknown = set(selected) - set(valid)
        if unknown:
            raise ValueError(f"unknown feed key(s) {sorted(unknown)}; allowed: {valid}")

        batch: list[Headline] = []
        for name, feed in self._cfg.feeds.rss.items():
            if name in selected:
                batch += await self._fetch_guarded(name, self._fetch_rss(feed.url))
        if GDELT_KEY in selected:
            batch += await self._fetch_guarded("gdelt_doc", self._fetch_gdelt(lookback_h))
        if NSE_ANN_KEY in selected and self._cfg.feeds.nse_announcements.enabled:
            batch += await self._fetch_guarded(NSE_ANN_KEY, self._fetch_nse_ann())

        # §3.2.4/§4.4 job 10 drop list (2026-08-03): auto-generated live-blog/ticker PAGE titles are
        # not news headlines — they carry zero event content, burn scorer budget, and their template
        # tokens glued different companies into one cluster (the G1 contamination finding). Owner-
        # editable in settings (news.drop_title_patterns), case-insensitive substring match.
        patterns = [p.lower() for p in (self._cfg.drop_title_patterns or [])]
        if patterns:
            kept: list[Headline] = []
            dropped = 0
            for h in batch:
                title = h.title.lower()
                if any(p in title for p in patterns):
                    dropped += 1
                else:
                    kept.append(h)
            if dropped:
                _log.info("news_boilerplate_dropped", dropped=dropped, kept=len(kept))
            batch = kept

        deduped = self._dedupe(batch)
        inserted = await self._store.arun(self._insert_batch, deduped)
        _log.info(
            "news_polled",
            feeds=list(selected),
            fetched=len(batch),
            unique=len(deduped),
            inserted=len(inserted),
        )
        return inserted

    async def backfill(self, *, lookback_h: float | None = None) -> list[Headline]:
        """§4.4 job 10 off-period startup backfill: the SAME poll with widened lookback windows.

        RSS feeds surface their whole publisher-side lookback on every fetch; GDELT gets
        ``news.backfill_lookback_h`` (or the caller's wider ``lookback_h``, e.g. sized to the actual
        off period), capped at ``news.gdelt_backfill_max_days`` (~3-month DOC window — headlines older
        than that are lost permanently, acceptable per E5/E6: catalysts past
        ``cat.max_event_age_days`` are ineligible anyway).
        """
        return await self.poll(lookback_h=lookback_h or float(self._cfg.backfill_lookback_h))

    # ------------------------------------------------------------------ fetchers (each E5-guarded)
    async def _fetch_guarded(self, feed: str, coro: Any) -> list[Headline]:
        """Run one feed fetch; on ANY failure log + return [] — a feed error never reaches the caller."""
        try:
            return await coro
        except Exception as exc:  # E5: best-effort feed — degrade to zero headlines, never raise
            _log.warning("news_feed_error", feed=feed, error=f"{type(exc).__name__}: {exc}")
            return []

    async def _fetch_rss(self, url: str) -> list[Headline]:
        """Fetch + parse one RSS 2.0 feed, tolerating malformed items (skip-and-count)."""
        resp = await self._http.get(url, timeout=self._timeout)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)  # noqa: S314 - trusted-shape parse of untrusted CONTENT; no entities resolved by ET
        headlines: list[Headline] = []
        malformed = 0
        for item in root.iter("item"):
            try:
                title = _clean_title(item.findtext("title") or "")
                link = (item.findtext("link") or "").strip()
                domain = _domain(link)
                if not title or not link or not domain:
                    malformed += 1
                    continue
                headlines.append(
                    Headline(
                        title=title,
                        source_domain=domain,
                        url=link,
                        published_at=self._parse_pubdate(item.findtext("pubDate")),
                    )
                )
            except Exception:  # one bad item never kills the feed
                malformed += 1
        if malformed:
            _log.warning("news_rss_malformed_items", url=url, skipped=malformed)
        return headlines

    def _parse_pubdate(self, raw: str | None) -> datetime:
        """RFC-2822 ``pubDate`` → tz-aware IST; missing/unparsable ⇒ ingest time (Clock), never naive."""
        if raw:
            try:
                parsed = parsedate_to_datetime(raw.strip())
                if parsed.tzinfo is None:  # "-0000" style unknown-offset dates parse naive ⇒ treat as UTC
                    parsed = parsed.replace(tzinfo=UTC)
                return parsed.astimezone(IST)
            except (ValueError, TypeError):
                pass
        return self._clock.now()

    async def _fetch_gdelt(self, lookback_h: float | None) -> list[Headline]:
        """GDELT DOC 2.0 ``artlist`` JSON — pinned query + Indian-financial-press domain filter."""
        params = {
            "query": self._cfg.feeds.gdelt_doc_query,
            "mode": "artlist",
            "format": "json",
            "maxrecords": "250",
            "sort": "datedesc",
            "timespan": self._gdelt_timespan(lookback_h),
        }
        resp = await self._http.get(GDELT_DOC_URL, params=params, timeout=self._timeout)
        resp.raise_for_status()
        payload = json.loads(resp.content)
        headlines: list[Headline] = []
        skipped = 0
        for art in payload.get("articles", []):
            try:
                title = _clean_title(str(art.get("title") or ""))
                url = str(art.get("url") or "").strip()
                domain = _domain(url) or str(art.get("domain") or "").lower()
                if not title or not url or not domain:
                    skipped += 1
                    continue
                if domain not in GDELT_DOMAIN_ALLOWLIST:
                    skipped += 1  # outside the Indian-financial-press allowlist (§3.2.4)
                    continue
                headlines.append(
                    Headline(
                        title=title,
                        source_domain=domain,
                        url=url,
                        published_at=self._parse_seendate(art.get("seendate")),
                    )
                )
            except Exception:
                skipped += 1
        if skipped:
            _log.info("news_gdelt_filtered", skipped=skipped, kept=len(headlines))
        return headlines

    def _parse_seendate(self, raw: Any) -> datetime:
        """GDELT ``seendate`` (``YYYYMMDDTHHMMSSZ``, UTC) → tz-aware IST; unparsable ⇒ ingest time."""
        if raw:
            try:
                return datetime.strptime(str(raw), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC).astimezone(IST)
            except ValueError:
                pass
        return self._clock.now()

    def _gdelt_timespan(self, lookback_h: float | None) -> str:
        """GDELT ``timespan`` for the lookback window, capped at ``gdelt_backfill_max_days`` (E6)."""
        if lookback_h is None:
            lookback_h = 2.0 * self._cfg.gdelt_poll_s / 3600.0  # 2× cadence ⇒ overlapping windows
        capped_h = min(float(lookback_h), self._cfg.gdelt_backfill_max_days * 24.0)
        minutes = max(15, math.ceil(capped_h * 60))
        if minutes < 60:
            return f"{minutes}min"
        hours = math.ceil(minutes / 60)
        if hours <= 72:
            return f"{hours}h"
        return f"{math.ceil(hours / 24)}d"

    async def _fetch_nse_ann(self) -> list[Headline]:
        """NSE ``corporate-announcements`` → token-prefixed headlines (§2.7 amendment 2026-09-04).

        Fetched through :func:`engine.core.nse_http.nse_get`, never a bare GET: the ``/api`` edge is
        cookie-gated and answers an un-primed request with a MISLEADING 404 (§A3). The endpoint URL
        is the one the §2.8 ISIN job already uses — one constant, one probe-verified contract.

        A row is unusable without ``symbol`` (the token IS the resolution) and is skipped; every
        other field degrades: no attachment ⇒ the deterministic :data:`NSE_ANN_ITEM_URL`, no
        parsable exchange timestamp ⇒ ingest time (Clock), never naive.
        """
        cfg = self._cfg.feeds.nse_announcements
        resp = await nse_get(self._http, NSE_ANNOUNCEMENTS_URL, timeout=self._timeout)
        payload = json.loads(resp.content)
        rows = payload if isinstance(payload, list) else []
        if isinstance(payload, dict):  # tolerated envelope shapes (mirrors isin_map's parser)
            for key in ("data", "rows", "records"):
                if isinstance(payload.get(key), list):
                    rows = payload[key]
                    break

        drops = [s.lower() for s in (cfg.drop_subjects or [])]
        headlines: list[Headline] = []
        dropped = skipped = 0
        for raw in rows:
            try:
                if not isinstance(raw, dict):
                    skipped += 1
                    continue
                symbol = str(raw.get("symbol") or "").strip().upper()
                subject = str(raw.get("desc") or "").strip()
                if not symbol:
                    skipped += 1  # no symbol ⇒ no token ⇒ nothing this feed can add
                    continue
                if any(d in subject.lower() for d in drops):
                    dropped += 1  # administrative subject (owner-editable list)
                    continue
                body = str(raw.get("attchmntText") or "").strip() or str(raw.get("sm_name") or "")
                attachment = str(raw.get("attchmntFile") or "").strip()
                seq_id = str(raw.get("seq_id") or "").strip()
                if not attachment.startswith("http") and not seq_id:
                    skipped += 1  # no attachment AND no seq id ⇒ no stable dedupe key exists
                    continue
                url = (
                    attachment if attachment.startswith("http")
                    else NSE_ANN_ITEM_URL.format(seq_id=seq_id)
                )
                subject_and_body = ": ".join(p for p in (subject, body) if p)
                headlines.append(
                    Headline(
                        title=_clean_title(f"[NSE:{symbol}] {subject_and_body}")[:NSE_ANN_TITLE_MAX],
                        source_domain=NSE_ANN_DOMAIN,
                        url=url,
                        published_at=self._parse_ann_dt(raw),
                    )
                )
            except Exception:  # one bad row never kills the feed
                skipped += 1
        if dropped or skipped:
            _log.info(
                "news_nse_ann_filtered", dropped=dropped, skipped=skipped, kept=len(headlines)
            )
        return headlines

    def _parse_ann_dt(self, raw: dict[str, Any]) -> datetime:
        """Exchange timestamp (IST, naive on the wire) → tz-aware IST; unparsable ⇒ ingest time.

        The fields are tried in :data:`_ANN_DT_FIELDS` order. These stamps are NAIVE IST strings —
        attaching IST is the correct reading, and the tz-aware Headline validator is the backstop.
        """
        for field, fmt in _ANN_DT_FIELDS:
            value = str(raw.get(field) or "").strip()
            if not value:
                continue
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=IST)
            except ValueError:
                continue
        return self._clock.now()

    # ------------------------------------------------------------------ dedupe + persist
    @staticmethod
    def _dedupe(batch: list[Headline]) -> list[Headline]:
        """Within-batch URL dedupe, in deterministic (published_at, url) order — first wins."""
        seen: set[str] = set()
        out: list[Headline] = []
        for h in sorted(batch, key=lambda h: (h.published_at, h.url)):
            if h.url in seen:
                continue
            seen.add(h.url)
            out.append(h)
        return out

    def _insert_batch(self, batch: list[Headline]) -> list[Headline]:
        """Insert row-by-row (sync; called via ``store.arun``) so cross-poll URL dupes are dropped
        and the return value is exactly the NEW headlines, each with its minted ``headline_id``."""
        inserted: list[Headline] = []
        for h in batch:
            hid = h.headline_id or str(ULID())
            row = {
                "headline_id": hid,
                "title": h.title,
                "source_domain": h.source_domain,
                "url": h.url,
                "published_at": h.published_at,
            }
            if self._store.insert_news([row]) == 1:
                inserted.append(h.model_copy(update={"headline_id": hid}))
        return inserted
