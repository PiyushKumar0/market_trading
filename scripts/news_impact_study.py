#!/usr/bin/env python
"""News impact study (``edge/news_impact``): one sample's verdicts and diagnostics from the engine's live news scores.

The six hypotheses (``info``, ``stock_next_open``, ``stock_pullback``, ``domain_companies``, ``domain_groups``,
``global_sectors``) are built from each cluster's first successful News Analyst score, timed by that score's ``at``
and resolved to symbols by the engine's resolver over the research copy's own tables plus the frozen alias
supplement; the domain and global ones read the mapper's store. Every threshold, hold, window and verdict rule is the
registration's, held in ``news_impact_common``.

* An orphan is a material stock story that this run's resolver resolves to no symbol with a bar on or before its D-1
  (an unlisted, foreign or group company, a firm before its listing, or a listed name the aliases miss). The mapper
  maps it, and the two domain hypotheses read it with the domain stories; in the stock hypotheses it still counts as
  unresolved, or a not-yet-listed symbol drops as not liquid. Orphan status is this run's, so a story an older copy
  resolved, and the mapper therefore skipped, holds the run until it is mapped; a mapping this run reads under no
  kind is counted as ``mapped_not_orphan``.
* Signals are per (key, entry session D): several stories on one key for one D make one signal. A key that another
  story the same hypothesis reads maps the other way on that D is skipped and counted.
* One position per key at a time within a hypothesis: a signal on or before the exit session of the last kept trade
  for its key is skipped. A signal that makes no trade (a reason string) keeps nothing and blocks nothing; a pullback
  trade's exit counts from its fill.
* A trade hypothesis's population is the same construct applied to every eligible key on the sample's own entry
  sessions, the pullback's with each kept trade's own P0 session; its gross excess values feed the permutation test.
* While any domain, global or orphan story the sample or the excluded window needs is unmapped, the run is waiting
  and computes no hypothesis, so the stock hypotheses never land ahead of the rest. A story without a readable
  headline can never be mapped, so it is counted as unmappable and never holds the run.
* The excluded window (the rally that prompted the study) runs through the same code and is reported apart, as not
  evidence; it never feeds a verdict.
* Diagnostics decide nothing.

Read-only: the research DuckDB copy through ``research_common.open_snapshot``, ``state.db`` and the mapper store
through ``mode=ro`` URIs. Writes only its report. Exit 0, or 2 on a refusal (an unreadable store, an alias supplement
that is missing, malformed or not the registered file, the data gate).
``--counts-only`` prints counts alone, never an outcome, and writes nothing.
"""

from __future__ import annotations

import argparse
import importlib.util
import math
import statistics
import sys
import time as _time
import warnings
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_REPO_SRC = str(_ROOT / "src")
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from engine.learning.multiplicity import permutation_pvalue  # noqa: E402
from engine.strategy.regime import INDEX_SMA, RISING_LOOKBACK, index_regime  # noqa: E402

try:
    from scipy.stats import spearmanr as _spearmanr
except ImportError:  # pragma: no cover - scipy ships with the research extras
    _spearmanr = None

_SCRIPTS = str(Path(__file__).resolve().parent)
if _SCRIPTS not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _SCRIPTS)


def _load_sibling(name: str) -> Any:
    mod_name = f"mt_{name}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, Path(__file__).with_name(f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


nic = _load_sibling("news_impact_common")
rc = nic.rc
Story = nic.Story
Trade = nic.Trade
IST = nic.IST

DEFAULT_DB = _ROOT / "data" / "research" / "full_market_2026-11-09.duckdb"
DEFAULT_STATE_DB = _ROOT / "data" / "state.db"
DEFAULT_MAPPINGS = _ROOT / "data" / "research" / "news_impact.sqlite"
#: Sample A's cut: the copy above was taken after the 2026-11-09 session.
DEFAULT_AS_OF_A = date(2026, 11, 9)
REPORTS_DIR = _ROOT / "data" / "reports"
EXCLUDED = "excluded"

TRADE_HYPOTHESES = ("stock_next_open", "stock_pullback", "domain_companies", "domain_groups", "global_sectors")
HYPOTHESES = ("info", *TRADE_HYPOTHESES)
#: How each hypothesis turns a signal into a trade.
CONSTRUCT = {"info": "sector", "stock_next_open": "sector", "stock_pullback": "pullback",
             "domain_companies": "beta", "domain_groups": "basket", "global_sectors": "basket"}
SPLIT_HYPOTHESES = ("stock_next_open", "stock_pullback", "domain_companies")
BOOLEAN_SPLITS = ("volume_confirm", "above_sma50", "regime_up", "gap_up_2pct", "strong_story", "two_domains")
CATEGORY_SPLITS = ("model", "event_type")
#: The story kinds (``nic.map_kind``) each mapped hypothesis reads: orphans join the domain stories.
DOMAIN_KINDS = frozenset({"domain", "orphan"})
GLOBAL_KINDS = frozenset({"global"})
#: The mapped hypotheses that read domain and orphan stories together, split by which of them made each signal.
DOMAIN_HYPOTHESES = ("domain_companies", "domain_groups")
ORIGIN = "origin"
#: The hypotheses split by whether a signal's stories had reached an outlet outside ``nic.INDIAN_NEWS_DOMAINS``.
OUTLET_HYPOTHESES = ("domain_companies", "domain_groups", "global_sectors")
GLOBAL_OUTLET = "global_outlet"
#: Every category split, in report order.
ALL_CATEGORY_SPLITS = (*CATEGORY_SPLITS, ORIGIN)
#: A category is reported with its own test only from this many trades.
MIN_CATEGORY = 10
STRONG_STORY = 0.8
VOLUME_MULT = 2.0
VOLUME_WINDOW = 20
SMA_WINDOW = 50
GAP_UP = 0.02
#: NIFTY open(D) -> close(D+4): the market-direction diagnostic's five sessions.
MARKET_SESSIONS = 5
NEGATIVE_CI = 0.95
#: Gives beta more than ``BETA_SESSIONS`` sessions of history before the first D-1.
PANEL_LOOKBACK = timedelta(days=400)
MAX_HOLD = max(nic.HOLD.values())

CONFLICT = "conflict"
OVERLAP = "overlap"
NO_SECTOR = "no_sector"

NOTES = (
    "pullback arm: the peers are measured from the fill session's open, not from P0, an approximation",
    "pullback arm: fills happen only on pullbacks, so the arm mixes entry timing with selection",
    "sample A holds 16 cat v2 signals whose 10-session outcomes were seen on 2026-11-10 (entered 2026-09-02 to "
    "2026-10-27); no other outcome in A had been looked at",
    "three scorer models served the window and are pooled; the per-model split is a diagnostic",
    "a stock that the D-1 sector snapshot leaves unclassified, or does not list, is counted as no_sector",
    "orphan stock stories (material, resolving to no symbol) are mapped and read by both domain hypotheses; they "
    "also count as unresolved in the stock hypotheses",
)


# ----------------------------------------------------------------------------- stories and signals
@dataclass(frozen=True)
class Placed:
    """A first score with its kind and entry session (None when it cannot be placed)."""

    story: Story
    kind: str | None
    d: date | None


@dataclass(frozen=True)
class Signal:
    key: str
    d: date
    stories: tuple[Story, ...]
    #: The pullback limit's session: the earliest pre-news session among the signal's stories.
    p0_session: date | None = None


Window = tuple[date, date]


def place(stories: Sequence[Story], sessions: Sequence[date]) -> list[Placed]:
    """Each story's kind and entry session. ``sessions`` are regular sessions that start before every story."""
    return [Placed(s, nic.story_kind(s), nic.placed_session(s.at, sessions)) for s in stories]


def _in(d: date | None, window: Window) -> bool:
    return d is not None and window[0] <= d <= window[1]


def _material_positive(s: Story) -> bool:
    return s.materiality >= nic.MATERIAL and s.sentiment >= nic.POSITIVE


def _material_negative(s: Story) -> bool:
    return s.materiality >= nic.MATERIAL and s.sentiment <= nic.NEGATIVE


def _by_time(stories: Sequence[Story]) -> tuple[Story, ...]:
    return tuple(sorted(stories, key=lambda s: (s.at, s.cluster_id)))


def stock_pairs(placed: Sequence[Placed], syms: dict[str, frozenset[str]], window: Window,
                keep: Callable[[Story], bool]) -> tuple[dict[tuple[str, date], list[Story]], Counter]:
    """Stock stories in ``window`` that ``keep`` accepts, grouped per (symbol, D); counts the stories and those that
    resolved to no symbol."""
    pairs: dict[tuple[str, date], list[Story]] = defaultdict(list)
    counts: Counter = Counter()
    for p in placed:
        if p.kind != "stock" or not _in(p.d, window) or not keep(p.story):
            continue
        counts["stories"] += 1
        found = syms.get(p.story.cluster_id, frozenset())
        if not found:
            counts["stories_unresolved"] += 1
        for sym in sorted(found):
            pairs[(sym, p.d)].append(p.story)
    return pairs, counts


def _sorted_pairs(pairs: dict[tuple[str, date], Any]) -> list[tuple[tuple[str, date], Any]]:
    return sorted(pairs.items(), key=lambda kv: (kv[0][1], kv[0][0]))


def info_signals(placed: Sequence[Placed], syms: dict[str, frozenset[str]],
                 window: Window) -> tuple[list[Signal], Counter]:
    """One observation per (symbol, D) over every stock story, whatever its materiality or sentiment."""
    pairs, counts = stock_pairs(placed, syms, window, lambda s: True)
    return [Signal(k, d, _by_time(v)) for (k, d), v in _sorted_pairs(pairs)], counts


def info_x(sig: Signal) -> float:
    """The mean sentiment x materiality of the signal's stories."""
    return statistics.fmean(s.sentiment * s.materiality for s in sig.stories)


def stock_signals(placed: Sequence[Placed], syms: dict[str, frozenset[str]], window: Window,
                  sessions: Sequence[date]) -> tuple[list[Signal], Counter]:
    """Material positive stock stories per (symbol, D), each a signal for both stock arms, unless a material
    negative stock story names the same symbol on the same D (``conflict``). The pullback limit sits at the close
    before the first of the signal's stories."""
    pos, counts = stock_pairs(placed, syms, window, _material_positive)
    neg, _ = stock_pairs(placed, syms, window, _material_negative)
    counts[CONFLICT] += 0
    out = []
    for (sym, d), stories in _sorted_pairs(pos):
        if (sym, d) in neg:
            counts[CONFLICT] += 1
            continue
        p0s = [x for x in (nic.pre_news_session(s.at, sessions) for s in stories) if x is not None]
        out.append(Signal(sym, d, _by_time(stories), min(p0s) if p0s else None))
    return out, counts


Resolve = Callable[[Sequence[str]], list[str | None]]


def mapped_pairs(placed: Sequence[Placed], mappings: dict[str, Any], window: Window, kinds: str | Collection[str],
                 field: str, resolve: Resolve | None,
                 orphans: Collection[str] = frozenset()) -> tuple[dict, dict, Counter]:
    """The (key, D) pairs that stories of ``kinds`` (``nic.map_kind`` with ``orphans``: ``domain``, ``global``,
    ``orphan``) in ``window`` map +1 and -1, each with its stories by cluster. ``field`` is ``groups`` (keys are the
    names) or ``companies`` (names go through ``resolve``; an unresolved name is counted by its direction)."""
    kinds = frozenset((kinds,) if isinstance(kinds, str) else kinds)
    plus: dict[tuple[str, date], dict[str, Story]] = defaultdict(dict)
    minus: dict[tuple[str, date], dict[str, Story]] = defaultdict(dict)
    counts: Counter = Counter()
    for p in placed:
        if nic.map_kind(p.story, orphans) not in kinds or not _in(p.d, window):
            continue
        m = mappings.get(p.story.cluster_id)
        if m is None:
            counts["unmapped"] += 1
            continue
        counts["stories"] += 1
        items = m[1] if field == "groups" else m[2]
        names, dirs = [], []
        for it in items or ():
            if isinstance(it, list | tuple) and len(it) == 2 and isinstance(it[0], str) and it[1] in (-1, 1):
                names.append(it[0])
                dirs.append(int(it[1]))
            else:
                counts["malformed_items"] += 1
        keys = resolve(names) if resolve is not None else list(names)
        for key, direction in zip(keys, dirs, strict=True):
            if key is None:
                counts["unresolved_plus" if direction == 1 else "unresolved_minus"] += 1
                continue
            (plus if direction == 1 else minus)[(key, p.d)][p.story.cluster_id] = p.story
    return plus, minus, counts


def mapped_signals(plus: dict, minus: dict) -> tuple[list[Signal], int]:
    """A signal per (key, D) mapped +1 by at least one story and -1 by none; the int counts the conflicts."""
    out, conflicts = [], 0
    for (key, d), stories in _sorted_pairs(plus):
        if (key, d) in minus:
            conflicts += 1
            continue
        out.append(Signal(key, d, _by_time(list(stories.values()))))
    return out, conflicts


def origin(sig: Signal, orphans: Collection[str]) -> str:
    """Which stories made a mapped signal: ``domain`` (all domain stories), ``orphan`` (all orphan stories) or
    ``mixed``."""
    found = {"orphan" if s.cluster_id in orphans else "domain" for s in sig.stories}
    return found.pop() if len(found) == 1 else "mixed"


def company_resolver(resolver: Any) -> Resolve:
    """``company_symbols`` one name at a time, cached: the same names recur across stories."""
    cache: dict[str, str | None] = {}

    def resolve(names: Sequence[str]) -> list[str | None]:
        for n in names:
            if n not in cache:
                cache[n] = nic.company_symbols(resolver, [n])[0][0]
        return [cache[n] for n in names]

    return resolve


# ----------------------------------------------------------------------------- trades
Make = Callable[[Signal, int], Any]


def _unclassified(p: nic.Panel, sym: str, d: date) -> bool:
    k = p.pos.get(d)
    if not k:
        return False
    sector = p.sector_of[nic.sector_snapshot(p, p.sessions[k - 1])].get(sym)
    return sector is None or sector == nic.UNCLASSIFIED


def make_trade(p: nic.Panel, construct: str, sig: Signal, hold: int) -> Trade | str:
    """The signal's trade under ``construct``, or why it has none. ``few_peers`` from a stock the D-1 snapshot does
    not classify is ``no_sector``."""
    if construct in ("sector", "beta"):
        r = nic.stock_trade(p, sig.key, sig.d, hold, construct)
    elif construct == "pullback":
        r = nic.NO_PRIOR_SESSION if sig.p0_session is None else nic.pullback_trade(p, sig.key, sig.d,
                                                                                    sig.p0_session, hold)
    elif construct == "basket":
        r = nic.basket_trade(p, sig.key, sig.d, hold)
    else:
        raise ValueError(f"unknown construct {construct!r}")
    if r == nic.FEW_PEERS and _unclassified(p, sig.key, sig.d):
        return NO_SECTOR
    return r


def maker(p: nic.Panel, name: str) -> Make:
    construct = CONSTRUCT[name]

    def make(sig: Signal, hold: int) -> Trade | str:
        return make_trade(p, construct, sig, hold)

    return make


def keep_trades(signals: Sequence[Signal], make: Make, hold: int) -> tuple[list[tuple[Signal, Trade]], Counter]:
    """One position per key: signals in date order; one on or before the exit session of the key's last kept trade
    is skipped (``overlap``). A signal whose construct gives a reason keeps nothing and blocks nothing."""
    kept: list[tuple[Signal, Trade]] = []
    counts: Counter = Counter()
    busy: dict[str, date] = {}
    for sig in sorted(signals, key=lambda s: (s.d, s.key)):
        last = busy.get(sig.key)
        if last is not None and sig.d <= last:
            counts[OVERLAP] += 1
            continue
        r = make(sig, hold)
        if isinstance(r, str):
            counts[r] += 1
            continue
        kept.append((sig, r))
        busy[sig.key] = r.exit_date
    return kept, counts


def population(p: nic.Panel, construct: str, kept: Sequence[Signal], hold: int) -> tuple[list[float], Counter]:
    """The construct's gross excess for every eligible key on the kept signals' entry sessions: every stock liquid
    as of D-1, or every vocabulary group. For the pullback, each distinct (D, P0 session) pair of the kept signals
    is one run with every stock's limit at its close(P0 session), so the population's limits sit where the
    sample's do; the other constructs run once per distinct D. Reasons are counted."""
    vals: list[float] = []
    counts: Counter = Counter()
    if construct == "pullback":
        runs = sorted({(s.d, s.p0_session) for s in kept if s.p0_session is not None})
    else:
        runs = [(d, None) for d in sorted({s.d for s in kept})]
    for d, p0_session in runs:
        k = p.pos[d]
        d_prev = p.sessions[k - 1]
        keys = list(nic.ncs.VOCAB) if construct == "basket" else sorted(nic.liquid(p, d_prev))
        for key in keys:
            r = make_trade(p, construct, Signal(key, d, (), p0_session), hold)
            if isinstance(r, str):
                counts[r] += 1
            else:
                vals.append(r.excess)
    return vals, counts


def trade_row(sig: Signal, t: Trade, *, with_p0: bool = False) -> dict[str, Any]:
    row = asdict(t)
    row["stories"] = [s.cluster_id for s in sig.stories]
    if with_p0:
        row["p0_session"] = sig.p0_session
    return row


def net_summary(trades: Sequence[Trade]) -> dict[str, Any]:
    nets = [t.net for t in trades]
    return {"n": len(nets), "mean_net": statistics.fmean(nets) if nets else None,
            "median_net": statistics.median(nets) if nets else None,
            "hit_rate": sum(v > 0 for v in nets) / len(nets) if nets else None}


def _counts(c: Counter) -> dict[str, int]:
    return {k: int(v) for k, v in sorted(c.items())}


def _drops(*parts: Counter) -> tuple[dict[str, int], int]:
    """The merged drop counts and, apart, the number of stories that fed the signals (a total, not a drop)."""
    total: Counter = Counter()
    for c in parts:
        total.update(c)
    stories = int(total.pop("stories", 0))
    return _counts(total), stories


# ----------------------------------------------------------------------------- statistics
def spearman(x: Sequence[float], y: Sequence[float]) -> float | None:
    """Spearman's rho, average ranks for ties; None below two pairs or with a constant side."""
    xa, ya = np.asarray(x, dtype="float64"), np.asarray(y, dtype="float64")
    if xa.size < 2:
        return None
    if _spearmanr is not None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = float(_spearmanr(xa, ya)[0])
    else:  # pragma: no cover - the fallback when scipy is absent
        r = float(pd.Series(xa).rank(method="average").corr(pd.Series(ya).rank(method="average")))
    return r if math.isfinite(r) else None


def _session_draws(days: Sequence[date], draws: int, seed: int) -> Iterator[np.ndarray]:
    """Index arrays of a circular moving-block bootstrap over the sorted distinct entry sessions.

    Consecutive sessions' holds overlap, so their outcomes are dependent; resampling single sessions would make the
    interval too narrow. Each draw takes ceil(S / block) block starts uniformly from the S sessions, each block the
    ``BOOT_BLOCK`` sessions from its start (wrapping past the last session to the first), concatenates them,
    truncates to S sessions and yields every observation of each chosen session. Fewer than ``BOOT_BLOCK`` sessions
    use one block of S. No draw for an empty sample."""
    groups: dict[date, list[int]] = defaultdict(list)
    for i, d in enumerate(days):
        groups[d].append(i)
    sessions = [np.asarray(groups[d], dtype=np.int64) for d in sorted(groups)]
    n = len(sessions)
    if not n:
        return
    block = min(nic.BOOT_BLOCK, n)
    offsets = np.arange(block, dtype=np.int64)
    rng = np.random.default_rng(seed)
    for _ in range(draws):
        starts = rng.integers(0, n, math.ceil(n / block))
        pick = ((starts[:, None] + offsets[None, :]) % n).ravel()[:n]
        yield np.concatenate([sessions[i] for i in pick])


def info_verdict(obs: Sequence[tuple[date, float, float]]) -> dict[str, Any]:
    """Spearman rho of (x, y) over ``(D, x, y)`` observations, with a percentile interval at 1 - ALPHA from
    ``BOOT_DRAWS`` moving-block resamples of entry sessions. ``insufficient`` below ``MIN_INFO_OBS``; ``refuted`` when
    rho <= 0; ``supported`` when the interval's lower bound is above 0; else ``null``. A draw whose rho is undefined
    (a constant side) is left out of the interval and counted."""
    n = len(obs)
    days = [o[0] for o in obs]
    out: dict[str, Any] = {"label": "insufficient", "n": n, "sessions": len(set(days)), "rho": None, "ci": None,
                           "ci_level": 1.0 - nic.ALPHA, "boot_draws": nic.BOOT_DRAWS, "undefined_draws": None}
    if n >= 2:
        x = np.asarray([o[1] for o in obs], dtype="float64")
        y = np.asarray([o[2] for o in obs], dtype="float64")
        out["rho"] = spearman(x, y)
        boots = [spearman(x[i], y[i]) for i in _session_draws(days, nic.BOOT_DRAWS, nic.SEED)]
        finite = [b for b in boots if b is not None]
        out["undefined_draws"] = len(boots) - len(finite)
        if finite:
            lo, hi = np.percentile(finite, [100.0 * nic.ALPHA / 2.0, 100.0 * (1.0 - nic.ALPHA / 2.0)])
            out["ci"] = [float(lo), float(hi)]
    rho, ci = out["rho"], out["ci"]
    if n < nic.MIN_INFO_OBS:
        out["label"] = "insufficient"
    elif rho is not None and rho <= 0:
        out["label"] = "refuted"
    elif ci is not None and ci[0] > 0:
        out["label"] = "supported"
    else:
        out["label"] = "null"
    return out


def mean_ci(days: Sequence[date], vals: Sequence[float], level: float = NEGATIVE_CI) -> list[float] | None:
    """A percentile interval of the mean from ``BOOT_DRAWS`` moving-block resamples of entry sessions."""
    if not vals:
        return None
    v = np.asarray(vals, dtype="float64")
    means = [float(v[i].mean()) for i in _session_draws(days, nic.BOOT_DRAWS, nic.SEED)]
    lo, hi = np.percentile(means, [100.0 * (1.0 - level) / 2.0, 100.0 * (1.0 + level) / 2.0])
    return [float(lo), float(hi)]


# ----------------------------------------------------------------------------- supporting-data splits
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _us(t: datetime) -> int:
    return (t - _EPOCH) // timedelta(microseconds=1)


def load_news_domains(conn: duckdb.DuckDBPyConnection,
                      cluster_ids: Sequence[str]) -> dict[str, list[tuple[str, int]]] | None:
    """Each cluster's ``news`` rows as (source_domain, ingested_at in epoch microseconds); None for a copy without
    a ``news`` table, which leaves the source-domain split unavailable."""
    has = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'news'").fetchone()[0]
    if not has:
        return None
    out: dict[str, list[tuple[str, int]]] = defaultdict(list)
    ids = sorted(set(cluster_ids))
    if ids:
        rows = conn.execute("SELECT cluster_id, source_domain, epoch_us(ingested_at) FROM news "
                            "WHERE cluster_id IN (SELECT unnest(?::VARCHAR[]))", [ids]).fetchall()
        for cid, dom, us in rows:
            out[str(cid)].append((str(dom), int(us)))
    return dict(out)


def _finite(*xs: float) -> bool:
    return all(math.isfinite(x) for x in xs)


def split_features(p: nic.Panel, sig: Signal,
                   news: dict[str, list[tuple[str, int]]] | None) -> dict[str, Any]:
    """The supporting data of one signal, read on its D-1 or D; None where the bars cannot say."""
    k = p.pos[sig.d]
    col = p.sym_col.get(sig.key)
    f: dict[str, Any] = dict.fromkeys(BOOLEAN_SPLITS)
    if col is not None and k >= 1:
        v_prev, c_prev, o_d = p.volumes[k - 1, col], p.closes[k - 1, col], p.opens[k, col]
        if k - 1 - VOLUME_WINDOW >= 0:
            vw = p.volumes[k - 1 - VOLUME_WINDOW:k - 1, col]
            if _finite(v_prev, *vw):
                f["volume_confirm"] = bool(v_prev >= VOLUME_MULT * float(np.median(vw)))
        if k - SMA_WINDOW >= 0:
            cw = p.closes[k - SMA_WINDOW:k, col]
            if _finite(*cw):
                f["above_sma50"] = bool(c_prev > float(cw.mean()))
        if _finite(o_d, c_prev) and c_prev > 0:
            f["gap_up_2pct"] = bool(o_d / c_prev - 1.0 >= GAP_UP)
    if k >= 1:
        closes = p.nifty["close"].to_numpy(dtype="float64")[max(0, k - INDEX_SMA - RISING_LOOKBACK):k]
        r = index_regime(list(closes), p.sessions[k - 1])
        f["regime_up"] = None if r is None else bool(r.up)
    f["strong_story"] = max(s.materiality for s in sig.stories) >= STRONG_STORY if sig.stories else None
    if news is not None and sig.stories:
        best = max(len({dom for dom, us in news.get(s.cluster_id, ()) if us < _us(s.at)}) for s in sig.stories)
        f["two_domains"] = best >= 2
    first = sig.stories[0] if sig.stories else None
    f["model"] = (first.model or "unknown") if first else "unknown"
    f["event_type"] = first.event_type if first else "unknown"
    return f


def _members_before(s: Story, news: dict[str, list[tuple[str, int]]]) -> Iterator[str]:
    """The domains of the story's cluster members ingested strictly before its first score, as ``two_domains``
    reads them."""
    cut = _us(s.at)
    return (dom for dom, us in news.get(s.cluster_id, ()) if us < cut)


def global_outlet(sig: Signal, news: dict[str, list[tuple[str, int]]] | None) -> bool | None:
    """Whether any of the signal's stories had, before its first score, a cluster member from one of
    ``nic.GLOBAL_FEED_DOMAINS``. None without a ``news`` table."""
    if news is None:
        return None
    return any(dom in nic.GLOBAL_FEED_DOMAINS for s in sig.stories for dom in _members_before(s, news))


def outlet_strays(signals: Sequence[Signal], news: dict[str, list[tuple[str, int]]] | None) -> dict[str, int] | None:
    """The member domains in neither ``nic.INDIAN_NEWS_DOMAINS`` nor ``nic.GLOBAL_FEED_DOMAINS``, counted over the
    members (ingested before each story's first score) of the signals' distinct stories, so a host the global-outlet
    split does not know is visible. None without a ``news`` table."""
    if news is None:
        return None
    known = nic.INDIAN_NEWS_DOMAINS | nic.GLOBAL_FEED_DOMAINS
    stories = {s.cluster_id: s for sig in signals for s in sig.stories}
    out: Counter = Counter(dom for s in stories.values() for dom in _members_before(s, news) if dom not in known)
    return _counts(out)


ONE_SIDED = "one side of the split is empty, so there is no permutation p"


def _subset(sub: Sequence[float], all_nets: Sequence[float]) -> dict[str, Any]:
    """A subset's summary and its permutation p against same-size random subsets of every trade. An empty subset
    has nothing to test, and one holding every trade equals every draw, so neither has a p; a note says why."""
    out = {"n": len(sub), "mean_net": statistics.fmean(sub) if sub else None,
           "median_net": statistics.median(sub) if sub else None, "perm_p": None}
    if not sub or len(sub) == len(all_nets):
        out["note"] = ONE_SIDED
    else:
        out["perm_p"] = permutation_pvalue(sub, all_nets, draws=nic.SPLIT_DRAWS, seed=nic.SEED)
    return out


def split_report(rows: Sequence[tuple[Signal, Trade]], feats: Sequence[dict[str, Any]], booleans: Sequence[str],
                 categories: Sequence[str], unavailable: Sequence[str] = ()) -> dict[str, Any]:
    """Each boolean split's true subset, and each category with at least ``MIN_CATEGORY`` trades, against
    same-size random subsets of all the hypothesis's trades (``SPLIT_DRAWS``, fixed seed)."""
    nets = [t.net for _, t in rows]
    out: dict[str, Any] = {"n": len(nets), "boolean": {}}
    for name in booleans:
        if name in unavailable:
            out["boolean"][name] = {"available": False}
            continue
        vals = [f[name] for f in feats]
        sub = [n for n, v in zip(nets, vals, strict=True) if v is True]
        out["boolean"][name] = {"available": True, **_subset(sub, nets), "false": sum(v is False for v in vals),
                                "unknown": sum(v is None for v in vals)}
    for cat in categories:
        groups: dict[str, list[float]] = defaultdict(list)
        for n, f in zip(nets, feats, strict=True):
            groups[str(f[cat])].append(n)
        out[cat] = {"tested": {g: _subset(v, nets) for g, v in sorted(groups.items()) if len(v) >= MIN_CATEGORY},
                    "small": {g: len(v) for g, v in sorted(groups.items()) if len(v) < MIN_CATEGORY}}
    return out


# ----------------------------------------------------------------------------- context
@dataclass
class Ctx:
    panel: nic.Panel
    sessions: list[date]
    placed: list[Placed]
    syms: dict[str, frozenset[str]]
    mappings: dict[str, Any]
    companies: Resolve
    #: The orphan stories' cluster ids by this run's resolver (``nic.orphan_ids``).
    orphans: frozenset[str] = frozenset()


def _calendar_sessions(cal: Any, stories: Sequence[Story], sample: str, as_of: date) -> list[date]:
    """Regular sessions from a fortnight before the first story or window to a month past the sample's and the
    excluded window's ends or ``as_of``, so every story can be placed and the data gate's exit session found."""
    windows = (nic.SAMPLES[sample], nic.SAMPLES[EXCLUDED])
    starts = [w[0] for w in windows] + [s.at.astimezone(IST).date() for s in stories[:1]]
    ends = [w[1] for w in windows] + [as_of]
    return nic.regular_sessions(cal, min(starts) - timedelta(days=14), max(ends) + timedelta(days=30))


def _read_mappings(path: Path) -> dict[str, Any]:
    from_mapper = _load_sibling("news_impact_map")
    try:
        return from_mapper.read_mappings(Path(path))
    except Exception as exc:  # noqa: BLE001 - any unreadable mapper store is one refusal
        raise nic.Unavailable(f"{path}: mappings unreadable ({type(exc).__name__}: {exc})") from exc


def _mappable(s: Story) -> bool:
    """The mapper's own rule for a story it can send: a headline that cleans to text."""
    return nic.ncs._clean(s.headline) is not None


def _needed_unmapped(placed: Sequence[Placed], mappings: dict[str, Any], windows: Sequence[Window],
                     orphans: Collection[str]) -> Iterator[tuple[str, Placed]]:
    for p in placed:
        kind = nic.map_kind(p.story, orphans)
        if kind is not None and any(_in(p.d, w) for w in windows) and p.story.cluster_id not in mappings:
            yield kind, p


def unmapped(placed: Sequence[Placed], mappings: dict[str, Any], windows: Sequence[Window],
             orphans: Collection[str]) -> Counter:
    """The domain, global and orphan stories entering in any of ``windows`` that have no mapping yet but can get
    one, by kind. A story without a readable headline is never sent to the mapper, so it is left out here: waiting
    on it would hold the run for ever."""
    return Counter(kind for kind, p in _needed_unmapped(placed, mappings, windows, orphans) if _mappable(p.story))


def unmappable(placed: Sequence[Placed], mappings: dict[str, Any], windows: Sequence[Window],
               orphans: Collection[str]) -> int:
    """The domain, global and orphan stories entering in any of ``windows`` that have no mapping and no readable
    headline."""
    return sum(not _mappable(p.story) for _, p in _needed_unmapped(placed, mappings, windows, orphans))


def mapped_not_orphan(placed: Sequence[Placed], mappings: dict[str, Any], orphans: Collection[str]) -> int:
    """The stories with a mapping in the store that this run reads as neither domain, global nor orphan (a story the
    mapper took as an orphan by another copy, which this run's resolver reads as listed): their mappings feed
    nothing, so they are counted rather than lost silently."""
    return sum(1 for p in placed if p.story.cluster_id in mappings and nic.map_kind(p.story, orphans) is None)


#: Sessions from the last entry to the longest exit: a pullback filled on the last resting session, then held.
LAST_EXIT_OFFSET = nic.PULLBACK_SESSIONS - 1 + MAX_HOLD - 1


def data_gate(p: nic.Panel, sessions: Sequence[date], sample: str) -> None:
    """Refuses unless the panel reaches the sample's last possible exit: its last entry session plus
    ``LAST_EXIT_OFFSET`` sessions, a pullback order filled on its last resting session and held the longest hold."""
    lo, hi = nic.SAMPLES[sample]
    days = [d for d in sessions if lo <= d <= hi]
    if not days:
        raise rc.Refusal(f"data gate: no regular session in sample {sample}'s window {lo}..{hi}")
    i = list(sessions).index(days[-1]) + LAST_EXIT_OFFSET
    if i >= len(sessions):
        raise rc.Refusal(f"data gate: the calendar does not reach the last possible exit of sample {sample}")
    exit_ = sessions[i]
    if p.sessions[-1] < exit_:
        raise rc.Refusal(f"data gate: the panel ends {p.sessions[-1]}, before {exit_}, the last possible exit of "
                         f"sample {sample} ({LAST_EXIT_OFFSET} sessions after its last entry {days[-1]})")


def _stock_syms(resolver: Any, placed: Sequence[Placed]) -> dict[str, frozenset[str]]:
    return {p.story.cluster_id: nic.stock_symbols(resolver, p.story) for p in placed if p.kind == "stock"}


def _open(sample: str, state_db: Path, db: Path, as_of: date, etfs: frozenset[str] | None,
          themes: dict[str, frozenset[str]] | None, *, mappings: Path | None, check_waiting: bool,
          supplement: Mapping[str, str] | None = None):
    """The alias supplement, stories, sessions and placements; the snapshot and its resolver, which decide the
    orphans; then (unless waiting) the panel and the data gate. ``supplement`` None reads the frozen file.
    Returns ``(conn or None, stories, load_counts, sessions, placed, mappings, waiting, orphans, ctx or None)``."""
    if sample not in ("A", "B"):
        raise ValueError(f"sample must be A or B, got {sample!r}")
    extra = nic.frozen_alias_supplement() if supplement is None else supplement
    cal = nic.load_calendar()
    limit = datetime.combine(as_of, nic.ENTRY_CUTOFF, IST)
    stories, load_counts = nic.load_first_scores(Path(state_db), limit)
    sessions = _calendar_sessions(cal, stories, sample, as_of)
    placed = place(stories, sessions)
    maps = _read_mappings(mappings) if mappings is not None else {}
    conn = rc.open_snapshot(Path(db))
    try:
        resolver = nic.indexed_resolver(nic.build_resolver(conn, extra))
        syms = _stock_syms(resolver, placed)
        orphans = nic.orphan_ids(stories, resolver, sessions=sessions, first_bars=nic.load_first_bars(conn),
                                 symbols=syms)
        waiting = (unmapped(placed, maps, (nic.SAMPLES[sample], nic.SAMPLES[EXCLUDED]), orphans) if check_waiting
                   else Counter())
        if sum(waiting.values()):
            conn.close()
            return None, stories, load_counts, sessions, placed, maps, waiting, orphans, None
        panel = nic.load_panel(conn, nic.SAMPLES[sample][0] - PANEL_LOOKBACK, as_of, cal, etfs=etfs, themes=themes)
        data_gate(panel, sessions, sample)
    except BaseException:
        conn.close()
        raise
    ctx = Ctx(panel, sessions, placed, syms, maps, company_resolver(resolver), orphans)
    return conn, stories, load_counts, sessions, placed, maps, waiting, orphans, ctx


# ----------------------------------------------------------------------------- one window
def window_signals(ctx: Ctx, window: Window, *, mapped: bool = True) -> tuple[dict[str, list[Signal]],
                                                                              dict[str, Counter], dict[str, Any]]:
    """Every hypothesis's signals in ``window``, their signal-level counts, and the -1 pairs for the negative
    side."""
    sig: dict[str, list[Signal]] = {}
    counts: dict[str, Counter] = {}
    sig["info"], counts["info"] = info_signals(ctx.placed, ctx.syms, window)
    stock, c = stock_signals(ctx.placed, ctx.syms, window, ctx.sessions)
    sig["stock_next_open"], counts["stock_next_open"] = stock, Counter(c)
    sig["stock_pullback"], counts["stock_pullback"] = stock, Counter(c)
    neg: dict[str, Any] = {}
    if not mapped:
        return sig, counts, neg
    negative, _ = stock_pairs(ctx.placed, ctx.syms, window, _material_negative)
    neg["stock_material_negative"] = ("sector", nic.HOLD["info"],
                                      [Signal(k, d, _by_time(v)) for (k, d), v in _sorted_pairs(negative)])
    plus, minus, c = mapped_pairs(ctx.placed, ctx.mappings, window, DOMAIN_KINDS, "companies", ctx.companies,
                                  ctx.orphans)
    sig["domain_companies"], conflicts = mapped_signals(plus, minus)
    counts["domain_companies"] = Counter({"stories": c["stories"], CONFLICT: conflicts,
                                          "unresolved_names": c["unresolved_plus"],
                                          "malformed_items": c["malformed_items"]})
    neg["domain_companies_minus"] = ("beta", nic.HOLD["domain_companies"],
                                     [Signal(k, d, _by_time(list(v.values()))) for (k, d), v in _sorted_pairs(minus)])
    neg_unresolved = c["unresolved_minus"]
    for name, kinds in (("domain_groups", DOMAIN_KINDS), ("global_sectors", GLOBAL_KINDS)):
        plus, minus, c = mapped_pairs(ctx.placed, ctx.mappings, window, kinds, "groups", None, ctx.orphans)
        sig[name], conflicts = mapped_signals(plus, minus)
        counts[name] = Counter({"stories": c["stories"], CONFLICT: conflicts, "malformed_items": c["malformed_items"]})
        neg[f"{name}_minus"] = ("basket", nic.HOLD[name],
                                [Signal(k, d, _by_time(list(v.values()))) for (k, d), v in _sorted_pairs(minus)])
    neg["_unresolved_minus_names"] = neg_unresolved
    return sig, counts, neg


def _info_rows(p: nic.Panel, signals: Sequence[Signal]) -> tuple[list[tuple[Signal, Trade, float]], Counter]:
    rows, drops = [], Counter()
    for sig in signals:
        r = make_trade(p, CONSTRUCT["info"], sig, nic.HOLD["info"])
        if isinstance(r, str):
            drops[r] += 1
        else:
            rows.append((sig, r, info_x(sig)))
    return rows, drops


def _legs(kept: Sequence[tuple[Signal, Trade]]) -> dict[str, int]:
    return {"voided_legs": sum(t.voided_legs for _, t in kept), "no_bar_legs": sum(t.no_bar_legs for _, t in kept)}


def evaluate_window(ctx: Ctx, window: Window, *, evidence: bool) -> dict[str, Any]:
    """The hypotheses over ``window``. As evidence: verdicts with their populations and every diagnostic. Not as
    evidence: the trades, drops and plain summaries only."""
    p = ctx.panel
    sig, sig_counts, neg = window_signals(ctx, window)
    hyps: dict[str, Any] = {}
    info_rows, info_drops = _info_rows(p, sig["info"])
    info_trades = [{**trade_row(s, t), "x": x} for s, t, x in info_rows]
    drops_info, stories_info = _drops(info_drops, sig_counts["info"])
    obs = [(s.d, x, t.excess) for s, t, x in info_rows]
    hyps["info"] = {"hold": nic.HOLD["info"], "stories": stories_info, "signals": len(sig["info"])}
    if evidence:
        hyps["info"]["verdict"] = info_verdict(obs)
    else:
        hyps["info"]["n"] = len(obs)
        hyps["info"]["rho"] = spearman([o[1] for o in obs], [o[2] for o in obs])
    hyps["info"]["trades"] = info_trades
    hyps["info"]["drops"] = drops_info
    kept_all: dict[str, list[tuple[Signal, Trade]]] = {}
    drops_all: dict[str, Counter] = {}
    for name in TRADE_HYPOTHESES:
        hold = nic.HOLD[name]
        kept, drops = keep_trades(sig[name], maker(p, name), hold)
        kept_all[name], drops_all[name] = kept, drops
        merged, n_stories = _drops(drops, sig_counts[name])
        h: dict[str, Any] = {"hold": hold, "stories": n_stories, "signals": len(sig[name])}
        if evidence:
            vals, pop_drops = population(p, CONSTRUCT[name], [s for s, _ in kept], hold)
            h["verdict"] = nic.verdict([t for _, t in kept], vals, hold)
            h["population"] = {"sessions": sorted({s.d for s, _ in kept}), "n": len(vals),
                               "drops": _counts(pop_drops)}
        else:
            h["summary"] = net_summary([t for _, t in kept])
        h["trades"] = [trade_row(s, t, with_p0=CONSTRUCT[name] == "pullback") for s, t in kept]
        h["drops"] = merged
        h["legs"] = _legs(kept)
        hyps[name] = h
    if not evidence:
        return {"hypotheses": hyps}
    return {"hypotheses": hyps, "diagnostics": _diagnostics(ctx, window, sig, kept_all, drops_all, neg)}


def _diagnostics(ctx: Ctx, window: Window, sig: dict[str, list[Signal]],
                 kept_all: dict[str, list[tuple[Signal, Trade]]], drops_all: dict[str, Counter],
                 neg: dict[str, Any]) -> dict[str, Any]:
    p = ctx.panel
    horizons: dict[str, Any] = {}
    for name in TRADE_HYPOTHESES:
        horizons[name] = {}
        for h in nic.HORIZONS:
            kept, drops = keep_trades(sig[name], maker(p, name), h)
            horizons[name][str(h)] = {**net_summary([t for _, t in kept]), "drops": _counts(drops)}
    stories = [s.cluster_id for name in TRADE_HYPOTHESES for sg, _ in kept_all[name] for s in sg.stories]
    news = load_news_domains(p.conn, stories)
    no_news = ("two_domains", GLOBAL_OUTLET) if news is None else ()
    splits: dict[str, Any] = {}
    for name in TRADE_HYPOTHESES:
        rows = kept_all[name]
        if name in SPLIT_HYPOTHESES:
            feats, booleans, cats = [split_features(p, s, news) for s, _ in rows], BOOLEAN_SPLITS, CATEGORY_SPLITS
        elif name == "global_sectors":
            feats = [{"event_type": s.stories[0].event_type if s.stories else "unknown"} for s, _ in rows]
            booleans, cats = (), ("event_type",)
        else:
            feats, booleans, cats = [{} for _ in rows], (), ()
        if name in OUTLET_HYPOTHESES:
            for f, (s, _) in zip(feats, rows, strict=True):
                f[GLOBAL_OUTLET] = global_outlet(s, news)
            booleans = (*booleans, GLOBAL_OUTLET)
        if name in DOMAIN_HYPOTHESES:
            for f, (s, _) in zip(feats, rows, strict=True):
                f[ORIGIN] = origin(s, ctx.orphans)
            cats = (*cats, ORIGIN)
        splits[name] = split_report(rows, feats, booleans, cats, unavailable=no_news)
    negative: dict[str, Any] = {}
    for name, value in neg.items():
        if name.startswith("_"):
            continue
        construct, hold, signals = value
        vals, days, drops = [], [], Counter()
        for s in signals:
            r = make_trade(p, construct, s, hold)
            if isinstance(r, str):
                drops[r] += 1
            else:
                vals.append(r.excess)
                days.append(s.d)
        negative[name] = {"hold": hold, "n": len(vals), "mean_excess": statistics.fmean(vals) if vals else None,
                          "ci95": mean_ci(days, vals), "drops": _counts(drops)}
    negative["domain_companies_minus"]["drops"]["unresolved_names"] = int(neg["_unresolved_minus_names"])
    filled, unfilled = len(kept_all["stock_pullback"]), drops_all["stock_pullback"][nic.UNFILLED]
    strays = None if news is None else {
        name: outlet_strays([s for s, _ in kept_all[name]], news) for name in OUTLET_HYPOTHESES}
    return {"horizons": horizons, "splits": splits, "negative_side": negative,
            "market_direction": market_direction(ctx, window),
            "pullback_fill_rate": {"filled": filled, "unfilled": int(unfilled),
                                   "rate": filled / (filled + unfilled) if filled + unfilled else None},
            "source_domains_available": news is not None, "outlet_stray_domains": strays}


def market_direction(ctx: Ctx, window: Window) -> dict[str, Any]:
    """Per entry session, the sum of the mapped market direction over global stories, against the NIFTY's
    open(D) -> close(D+4); Spearman rho."""
    p = ctx.panel
    per_d: dict[date, int] = defaultdict(int)
    for pl in ctx.placed:
        if pl.kind == "global" and _in(pl.d, window) and pl.story.cluster_id in ctx.mappings:
            m = ctx.mappings[pl.story.cluster_id][0]
            per_d[pl.d] += int(m) if m in (-1, 0, 1) else 0
    opens = p.nifty["open"].to_numpy(dtype="float64")
    closes = p.nifty["close"].to_numpy(dtype="float64")
    xs, ys, skipped = [], [], 0
    for d in sorted(per_d):
        k = p.pos.get(d)
        j = None if k is None else k + MARKET_SESSIONS - 1
        if j is None or j >= len(p.sessions) or not _finite(opens[k], closes[j]) or opens[k] <= 0:
            skipped += 1
            continue
        xs.append(per_d[d])
        ys.append((closes[j] / opens[k] - 1.0) * 100.0)
    return {"n": len(xs), "rho": spearman(xs, ys), "sessions_skipped": skipped}


# ----------------------------------------------------------------------------- the run
def _git_head() -> str | None:
    """The checkout's HEAD commit, read from the files (no git process); None when it cannot be read."""
    try:
        git = _ROOT / ".git"
        if git.is_file():
            git = (_ROOT / git.read_text(encoding="utf-8").split(":", 1)[1].strip()).resolve()
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head
        ref = head[5:]
        common = git
        if (git / "commondir").is_file():
            common = (git / (git / "commondir").read_text(encoding="utf-8").strip()).resolve()
        for base in (git, common):
            if (base / ref).is_file():
                return (base / ref).read_text(encoding="utf-8").strip()
        packed = common / "packed-refs"
        if packed.is_file():
            for line in packed.read_text(encoding="utf-8").splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == ref:
                    return parts[0]
    except (OSError, IndexError):
        return None
    return None


def _kind_label(p: Placed, orphans: Collection[str]) -> str:
    """The kind a story is counted under in the report: ``orphan`` apart from the other stock stories."""
    return "orphan" if nic.map_kind(p.story, orphans) == "orphan" else (p.kind or "none")


def _kinds(placed: Sequence[Placed], orphans: Collection[str], window: Window | None = None) -> dict[str, int]:
    return _counts(Counter(_kind_label(p, orphans) for p in placed if window is None or _in(p.d, window)))


def _meta(sample: str, as_of: date, state_db: Path, db: Path, mappings: Path, load_counts: dict[str, int],
          placed: Sequence[Placed], orphans: Collection[str]) -> dict[str, Any]:
    return {"family": nic.FAMILY, "sample": sample, "as_of": as_of,
            "windows": {"sample": list(nic.SAMPLES[sample]), "excluded": list(nic.SAMPLES[EXCLUDED])},
            "stores": {"db": str(db), "state_db": str(state_db), "mappings": str(mappings)},
            "cost_round_trip_pct_cnc": nic.cost(), "notional_inr": str(nic.NOTIONAL), "n_trials": nic.N_TRIALS,
            "alpha": nic.ALPHA, "fold_pass_min": nic.FOLD_PASS_MIN, "min_trades": nic.MIN_TRADES,
            "min_info_obs": nic.MIN_INFO_OBS, "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "git_head": _git_head(), "first_scores": dict(load_counts),
            "stories_by_kind": {"all": _kinds(placed, orphans),
                                "sample_window": _kinds(placed, orphans, nic.SAMPLES[sample])},
            "notes": list(NOTES)}


def run(sample: str, state_db: Path, db: Path, mappings_db: Path, as_of: date, *,
        etfs: frozenset[str] | None = None, themes: dict[str, frozenset[str]] | None = None,
        supplement: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The sample's report document. Waiting (no hypothesis computed) while any needed story is unmapped. Raises
    ``research_common.Refusal`` or ``Unavailable`` on a refusal. ``etfs`` and ``themes`` default to the macro mood
    study's files, ``supplement`` to the frozen alias supplement."""
    conn, _, load_counts, _, placed, maps, waiting, orphans, ctx = _open(
        sample, state_db, db, as_of, etfs, themes, mappings=mappings_db, check_waiting=True, supplement=supplement)
    meta = _meta(sample, as_of, Path(state_db), Path(db), Path(mappings_db), load_counts, placed, orphans)
    meta["unmappable"] = unmappable(placed, maps, (nic.SAMPLES[sample], nic.SAMPLES[EXCLUDED]), orphans)
    meta["mapped_not_orphan"] = mapped_not_orphan(placed, maps, orphans)
    if ctx is None:
        n = sum(waiting.values())
        return {"meta": meta, "status": f"waiting: {n} stories unmapped (domain {waiting['domain']}, "
                                        f"global {waiting['global']}, orphan {waiting['orphan']})",
                "hypotheses": {}, "diagnostics": {}, "excluded_window": {}}
    try:
        main_part = evaluate_window(ctx, nic.SAMPLES[sample], evidence=True)
        excluded = evaluate_window(ctx, nic.SAMPLES[EXCLUDED], evidence=False)
        meta["panel"] = {"first_session": ctx.panel.sessions[0], "last_session": ctx.panel.sessions[-1],
                         "notes": list(ctx.panel.notes)}
    finally:
        conn.close()
    return {"meta": meta, "status": "complete", "hypotheses": main_part["hypotheses"],
            "diagnostics": main_part["diagnostics"],
            "excluded_window": {"label": "not evidence", "window": list(nic.SAMPLES[EXCLUDED]),
                                "hypotheses": {k: v for k, v in excluded["hypotheses"].items() if k != "info"},
                                "info": excluded["hypotheses"]["info"]}}


def counts_only(sample: str, state_db: Path, db: Path, as_of: date, *, etfs: frozenset[str] | None = None,
                themes: dict[str, frozenset[str]] | None = None, supplement: Mapping[str, str] | None = None,
                mappings: Path | None = None) -> dict[str, Any]:
    """Counts for validating the stock-story pipeline on real data before sign-off: stories by kind (orphans apart),
    signals, trades, drops by reason and population sizes for the stock hypotheses, and, with ``mappings``, the
    mappings this run reads under no kind. No return, excess, net, rho or label is ever put in the result. Skips the
    waiting check and the mapped hypotheses."""
    t0 = _time.perf_counter()
    conn, _, load_counts, _, placed, maps, _, orphans, ctx = _open(
        sample, state_db, db, as_of, etfs, themes, mappings=mappings, check_waiting=False, supplement=supplement)
    try:
        windows: dict[str, Any] = {}
        for wname, window in (("sample", nic.SAMPLES[sample]), (EXCLUDED, nic.SAMPLES[EXCLUDED])):
            sig, sig_counts, _ = window_signals(ctx, window, mapped=False)
            w: dict[str, Any] = {}
            rows, drops = _info_rows(ctx.panel, sig["info"])
            merged, n_stories = _drops(drops, sig_counts["info"])
            w["info"] = {"stories": n_stories, "signals": len(sig["info"]), "trades": len(rows), "drops": merged}
            for name in ("stock_next_open", "stock_pullback"):
                kept, drops = keep_trades(sig[name], maker(ctx.panel, name), nic.HOLD[name])
                merged, n_stories = _drops(drops, sig_counts[name])
                w[name] = {"stories": n_stories, "signals": len(sig[name]), "trades": len(kept), "drops": merged}
                if wname == "sample":
                    vals, pop_drops = population(ctx.panel, CONSTRUCT[name], [s for s, _ in kept], nic.HOLD[name])
                    w[name]["population"] = {"sessions": len({s.d for s, _ in kept}), "size": len(vals),
                                             "drops": _counts(pop_drops)}
            windows[wname] = w
        panel = (ctx.panel.sessions[0], ctx.panel.sessions[-1])
    finally:
        conn.close()
    by_window = {wn: _kinds(placed, orphans, nic.SAMPLES[ws]) for wn, ws in (("sample", sample), (EXCLUDED, EXCLUDED))}
    return {"sample": sample, "as_of": as_of, "panel": panel, "first_scores": dict(load_counts),
            "stories_by_kind": {"all": _kinds(placed, orphans), **by_window}, "windows": windows,
            "mapped_not_orphan": mapped_not_orphan(placed, maps, orphans),
            "seconds": round(_time.perf_counter() - t0, 1)}


# ----------------------------------------------------------------------------- rendering
def _f(v: Any, fmt: str = "+.4f") -> str:
    return "-" if v is None else format(v, fmt)


def _kv(d: dict[str, Any]) -> str:
    return ", ".join(f"{k} {v}" for k, v in d.items()) if d else "none"


def _ci(ci: Any) -> str:
    return "-" if not ci else f"[{ci[0]:+.4f}, {ci[1]:+.4f}]"


def _note(s: dict[str, Any]) -> str:
    return f"; {s['note']}" if s.get("note") else ""


def _trade_line(t: dict[str, Any]) -> str:
    extra = f" fill {t['fill_price']:.2f}" if t.get("fill_price") is not None else ""
    extra += f" x {t['x']:+.3f}" if "x" in t else ""
    extra += f" legs {t['legs']}" if t.get("legs", 1) != 1 else ""
    return (f"      {t['key']:<14} signal {t['signal_date']} entry {t['entry_date']} exit {t['exit_date']} gross "
            f"{t['gross']:+.3f} bench {t['bench']:+.3f} excess {t['excess']:+.3f} net {t['net']:+.3f}{extra}")


def render(doc: dict[str, Any]) -> str:
    """The plain-text report; reads only JSON-safe values, so it renders the written JSON identically."""
    m = doc["meta"]
    out = [f"NEWS IMPACT STUDY ({m['family']}) sample {m['sample']} as of {m['as_of']}",
           f"status: {doc['status']}",
           f"windows: sample {m['windows']['sample'][0]}..{m['windows']['sample'][1]}; excluded (not evidence) "
           f"{m['windows']['excluded'][0]}..{m['windows']['excluded'][1]}",
           f"stores: db {m['stores']['db']}; state {m['stores']['state_db']}; mappings {m['stores']['mappings']}",
           f"cost: CNC round trip {m['cost_round_trip_pct_cnc']:.4f}% at Rs {m['notional_inr']}; N {m['n_trials']}; "
           f"alpha {m['alpha']:.6f}; fold pass min {m['fold_pass_min']:.2f}; min trades {m['min_trades']}; min info "
           f"obs {m['min_info_obs']}",
           f"generated {m['generated_at']}; git {m['git_head'] or 'unknown'}",
           f"first scores: {_kv(m['first_scores'])}",
           f"stories by kind: all ({_kv(m['stories_by_kind']['all'])}); sample window "
           f"({_kv(m['stories_by_kind']['sample_window'])})",
           f"unmappable: {m['unmappable']} domain, global and orphan stories in the sample and excluded windows "
           f"without a readable headline (never mapped, never waited on)",
           f"mapped_not_orphan: {m['mapped_not_orphan']} stories with a mapping that this run reads as neither "
           f"domain, global nor orphan (their mappings feed nothing)"]
    if "panel" in m:
        out.append(f"panel: {m['panel']['first_session']}..{m['panel']['last_session']}")
        out += [f"panel note: {n}" for n in m["panel"]["notes"]]
    out += [f"note: {n}" for n in m["notes"]]
    if not doc["hypotheses"]:
        return rc.ascii_text("\n".join(out))
    out += ["", "HYPOTHESES"]
    for name, h in doc["hypotheses"].items():
        v = h["verdict"]
        if name == "info":
            out.append(f"  {name:<18} {v['label']:<12} n={v['n']} sessions {v['sessions']} rho {_f(v['rho'])} "
                       f"CI at {v['ci_level']:.4f} {_ci(v['ci'])} (undefined draws {v['undefined_draws']})")
        else:
            out.append(f"  {name:<18} {v['label']:<12} n={v['n']} mean net {_f(v['mean_net'])} median net "
                       f"{_f(v['median_net'])} perm p {_f(v['perm_p'], '.4f')} fold pass {_f(v['fold_pass'], '.2f')} "
                       f"halves {_f(v['half1_mean'])}/{_f(v['half2_mean'])} drop-top-3 {_f(v['drop_top3_mean'])}")
            pop = h["population"]
            out.append(f"    population: {pop['n']} values on {len(pop['sessions'])} entry sessions; drops "
                       f"{_kv(pop['drops'])}")
            out.append(f"    legs: {_kv(h['legs'])}")
        out.append(f"    hold {h['hold']}; stories {h['stories']}; signals {h['signals']}; drops {_kv(h['drops'])}")
        out += [_trade_line(t) for t in h["trades"]]
    dg = doc["diagnostics"]
    out += ["", "DIAGNOSTICS (decide nothing)", "  horizons:"]
    for name, by_h in dg["horizons"].items():
        for h, s in by_h.items():
            out.append(f"    {name:<18} hold {h:>2}: n={s['n']} mean net {_f(s['mean_net'])} median net "
                       f"{_f(s['median_net'])} hit {_f(s['hit_rate'], '.3f')}; drops {_kv(s['drops'])}")
    out.append(f"  supporting-data splits (true subset vs same-size random subsets of the hypothesis's trades; "
               f"source domains available: {dg['source_domains_available']}):")
    for name, sp in dg["splits"].items():
        out.append(f"    {name} (n={sp['n']})")
        for split, s in sp["boolean"].items():
            if not s["available"]:
                out.append(f"      {split:<15} unavailable")
                continue
            out.append(f"      {split:<15} n={s['n']} mean net {_f(s['mean_net'])} median net {_f(s['median_net'])} "
                       f"perm p {_f(s['perm_p'], '.4f')} (false {s['false']}, unknown {s['unknown']})"
                       f"{_note(s)}")
        for cat in ALL_CATEGORY_SPLITS:
            if cat not in sp:
                continue
            for g, s in sp[cat]["tested"].items():
                out.append(f"      {cat} {g}: n={s['n']} mean net {_f(s['mean_net'])} median net "
                           f"{_f(s['median_net'])} perm p {_f(s['perm_p'], '.4f')}{_note(s)}")
            if sp[cat]["small"]:
                out.append(f"      {cat} below {MIN_CATEGORY} trades: {_kv(sp[cat]['small'])}")
    out.append("  negative side (gross excess; 95% moving-block session bootstrap CI of the mean):")
    for name, s in dg["negative_side"].items():
        out.append(f"    {name:<26} hold {s['hold']:>2}: n={s['n']} mean {_f(s['mean_excess'])} CI {_ci(s['ci95'])}; "
                   f"drops {_kv(s['drops'])}")
    md = dg["market_direction"]
    out.append(f"  global market direction vs NIFTY open(D)->close(D+4): n={md['n']} rho {_f(md['rho'])} "
               f"(sessions skipped {md['sessions_skipped']})")
    fr = dg["pullback_fill_rate"]
    out.append(f"  pullback fill rate: {fr['filled']} filled, {fr['unfilled']} unfilled, rate {_f(fr['rate'], '.3f')}")
    strays = dg["outlet_stray_domains"]
    out.append("  member domains in neither the Indian nor the global-feed list (rows before the first score):"
               + (" unavailable" if strays is None else ""))
    for name, doms in (strays or {}).items():
        out.append(f"    {name:<18} {_kv(doms)}")
    ex = doc["excluded_window"]
    out += ["", f"EXCLUDED WINDOW {ex['window'][0]}..{ex['window'][1]} ({ex['label']}; never feeds a verdict)"]
    i = ex["info"]
    out.append(f"  info               n={i['n']} rho {_f(i['rho'])}; stories {i['stories']}; signals {i['signals']}; "
               f"drops {_kv(i['drops'])}")
    for name, h in ex["hypotheses"].items():
        s = h["summary"]
        out.append(f"  {name:<18} n={s['n']} mean net {_f(s['mean_net'])} median net {_f(s['median_net'])} hit "
                   f"{_f(s['hit_rate'], '.3f')}; stories {h['stories']}; signals {h['signals']}; drops "
                   f"{_kv(h['drops'])}")
        out += [_trade_line(t) for t in h["trades"]]
    return rc.ascii_text("\n".join(out))


def render_counts(c: dict[str, Any]) -> str:
    """The counts-only printout: counts and the run time, nothing else."""
    out = [f"NEWS IMPACT STUDY counts only: sample {c['sample']} as of {c['as_of']}; panel {c['panel'][0]}.."
           f"{c['panel'][1]}",
           f"first scores: {_kv(c['first_scores'])}"]
    for wn, kinds in c["stories_by_kind"].items():
        out.append(f"stories by kind ({wn}): {_kv(kinds)}")
    for wn, w in c["windows"].items():
        out.append(f"window {wn}:")
        for name, h in w.items():
            out.append(f"  {name:<16} stories {h['stories']}, signals {h['signals']}, trades {h['trades']}; drops "
                       f"{_kv(h['drops'])}")
            if "population" in h:
                pop = h["population"]
                out.append(f"  {'':<16} population: {pop['size']} on {pop['sessions']} entry sessions; drops "
                           f"{_kv(pop['drops'])}")
    out.append(f"mapped_not_orphan {c['mapped_not_orphan']}: stories with a mapping this run reads under no kind")
    out.append(f"run time {c['seconds']} s")
    return rc.ascii_text("\n".join(out))


# ----------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None, *, etfs: frozenset[str] | None = None,
         themes: dict[str, frozenset[str]] | None = None, supplement: Mapping[str, str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="news_impact_study", description=__doc__.splitlines()[0])
    ap.add_argument("--sample", required=True, choices=("A", "B"))
    ap.add_argument("--db", type=Path, default=None,
                    help=f"research DuckDB copy, read-only (sample A default {DEFAULT_DB}); B needs its own")
    ap.add_argument("--state-db", type=Path, default=DEFAULT_STATE_DB, help="engine state.db, read-only")
    ap.add_argument("--mappings", type=Path, default=DEFAULT_MAPPINGS, help="the mapper's store, read-only")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None,
                    help=f"YYYY-MM-DD (sample A default {DEFAULT_AS_OF_A}); B needs its own")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    ap.add_argument("--counts-only", action="store_true",
                    help="print counts only (no outcome of any kind); skips the mapped hypotheses; writes nothing")
    args = ap.parse_args(argv)
    if args.sample == "B" and (args.db is None or args.as_of is None):
        print("news_impact_study: REFUSED: sample B needs --db (a fresh copy taken after its last exit) and --as-of")
        return 2
    db = args.db if args.db is not None else DEFAULT_DB
    as_of = args.as_of if args.as_of is not None else DEFAULT_AS_OF_A
    try:
        if args.counts_only:
            print(render_counts(counts_only(args.sample, args.state_db, db, as_of, etfs=etfs, themes=themes,
                                            supplement=supplement, mappings=args.mappings)))
            return 0
        doc = run(args.sample, args.state_db, db, args.mappings, as_of, etfs=etfs, themes=themes,
                  supplement=supplement)
        text = render(doc)
    except (rc.Refusal, nic.Unavailable) as exc:
        print(f"news_impact_study: REFUSED: {rc.ascii_text(str(exc))}")
        return 2
    stamp = datetime.now(IST).strftime("%Y%m%dT%H%M%S")
    out = args.out if args.out is not None else REPORTS_DIR / f"news_impact_{args.sample}_{stamp}.json"
    md = rc.write_reports(doc, f"# NEWS IMPACT STUDY\n\n```\n{text}\n```\n", out)
    print(text)
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
