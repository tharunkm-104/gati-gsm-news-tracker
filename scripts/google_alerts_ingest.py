"""
google_alerts_ingest.py

Second, independent ingestion path for the GATI mobility news pipeline —
runs alongside (not replacing, for now) the existing Cowork/Slack-fed
pipeline that writes to data/items.csv.

Flow:
  1. Pull all configured Google Alerts RSS feeds.
  2. Filter to the date window in plain Python. Default: since the last
     successful run (watermark in the state file, capped), falling back to
     the last 24h (or back to Saturday on a Monday) when there is no state.
  3. Drop denylisted domains / live blogs, de-duplicate the candidates
     themselves (same article via several feeds), and drop URLs already
     posted in the last 7 days (canonicalised, so /amp/, m., utm_ variants
     of a seen URL are caught too).
  4. ONE Gemini call for source gating, same-batch dedup, tagging, ranking.
     Candidates carry `possible_repeat_of` hints (computed in Python) that
     point at similar already-posted items.
  5. Deterministic validation, then TOPIC dedup of the result against the
     last 7 days of the Alerts CSV: one small focused Gemini call that compares
     only headline+summary, then a Python safety net for near-certain repeats.
  6. Append rows to data/google_alerts_items.csv; optionally post to Slack.

Reliability (this is what makes it safe to schedule several times a day):
  - Gemini calls retry with backoff on 429/5xx/timeouts.
  - If the main Gemini call fails, the run FAILS CLOSED: nothing is written,
    nothing is posted, exit code 1. A failed run is never reported to Slack
    as a "quiet news day". The next scheduled run covers the gap because the
    window starts at the last SUCCESSFUL run.
  - A state file (data/google_alerts_state.json) records the last successful
    run and whether today's bulletin is done, so extra scheduled runs exit
    immediately (no tokens spent). The workflow must commit that file.
  - If the Slack post fails, the formatted bulletin is kept in the state file
    and posted by the next run.

Env vars:
  GEMINI_API_KEY (required), GEMINI_MODEL, ALERTS_CSV_PATH, ALERTS_STATE_PATH,
  POST_TO_SLACK (+ SLACK_BOT_TOKEN, SLACK_CHANNEL_ID), WINDOW_HOURS (one-off
  backfill), FORCE_RUN=true (ignore "already done today"), MAX_CANDIDATES,
  ARTICLE_MAX_CHARS, MAX_ARTICLE_FETCHES, MAX_CATCHUP_HOURS,
  GEMINI_MAX_ATTEMPTS, TOPIC_DEDUP_PASS (default true),
  TOPIC_DEDUP_THRESHOLD, GEMINI_PROMPT_PATH.

ISOLATION: reads and writes only the Alerts CSV and the Alerts state file;
never opens data/items.csv.
"""

import csv
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from urllib.parse import urlparse, urlunparse, parse_qs, unquote, urlencode

import feedparser
import requests
try:
    import trafilatura
except ImportError:  # keep the run alive; items fall back to title-only
    trafilatura = None
    print("WARNING: trafilatura not installed - skipping full-article fetch. "
          "Add it to scripts/requirements-alerts.txt.")
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ALERTS_CSV_PATH = os.environ.get("ALERTS_CSV_PATH", "data/google_alerts_items.csv")
STATE_PATH = os.environ.get("ALERTS_STATE_PATH", "data/google_alerts_state.json")
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
# Verify current free-tier-eligible model name before running — these get
# renamed/re-limited through 2026. Check ai.google.dev/gemini-api/docs/models.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

# Token / runtime controls
ARTICLE_MAX_CHARS = int(os.environ.get("ARTICLE_MAX_CHARS", "1500"))  # the lede carries the event
MAX_CANDIDATES = int(os.environ.get("MAX_CANDIDATES", "80"))
MAX_CATCHUP_HOURS = int(os.environ.get("MAX_CATCHUP_HOURS", "72"))
GEMINI_MAX_ATTEMPTS = int(os.environ.get("GEMINI_MAX_ATTEMPTS", "5"))
RETRY_DELAYS = [15, 45, 90, 180]  # seconds between attempts (last value reused)
MAX_OUTPUT_TOKENS = 12000

# Topic dedup
TOPIC_DEDUP_PASS = os.environ.get("TOPIC_DEDUP_PASS", "true").lower() == "true"
# Calibrated on real data: same-event pairs from different outlets score ~0.3-0.6 (left to Gemini),
# near-certain repeats (same story reposted / re-reported) score >= 0.7.
TOPIC_DEDUP_THRESHOLD = float(os.environ.get("TOPIC_DEDUP_THRESHOLD", "0.7"))
TOPIC_HINT_THRESHOLD = 0.25

FIELDNAMES = [
    "date", "headline", "url", "categories", "summary",
    "vibe", "in_top7", "source", "theme", "countries", "stakeholders",
]

# Your 26 live feeds, grouped by bucket (bucket label is informational only —
# Gemini re-derives categories per item from content, this is just for your
# own reference / easy editing).
FEEDS = {
    "India Specific": [
        "https://www.google.com/alerts/feeds/05363041802822053495/6507183828257652649",
        "https://www.google.com/alerts/feeds/05363041802822053495/6507183828257649724",
        "https://www.google.com/alerts/feeds/05363041802822053495/17855979211063778577",
        "https://www.google.com/alerts/feeds/05363041802822053495/17855979211063778087",
        "https://www.google.com/alerts/feeds/05363041802822053495/6189939704157505002",
    ],
    "Destination Countries": [
        "https://www.google.com/alerts/feeds/05363041802822053495/13122070091431283778",
        "https://www.google.com/alerts/feeds/05363041802822053495/4081299559572155202",
        "https://www.google.com/alerts/feeds/05363041802822053495/6692561262367125889",
        "https://www.google.com/alerts/feeds/05363041802822053495/1840906225829004900",
        "https://www.google.com/alerts/feeds/05363041802822053495/16436978337792180809",
        "https://www.google.com/alerts/feeds/05363041802822053495/707663747370913286",
        "https://www.google.com/alerts/feeds/05363041802822053495/781103908505826968",
        "https://www.google.com/alerts/feeds/05363041802822053495/781103908505826945",
        "https://www.google.com/alerts/feeds/05363041802822053495/781103908505826250",
        "https://www.google.com/alerts/feeds/05363041802822053495/1575230517897388894",
        "https://www.google.com/alerts/feeds/05363041802822053495/17543801947914402670",
        "https://www.google.com/alerts/feeds/05363041802822053495/781103908505825726",
    ],
    "Competitor Countries": [
        "https://www.google.com/alerts/feeds/05363041802822053495/5467385533706295912",
        "https://www.google.com/alerts/feeds/05363041802822053495/17543801947914401445",
        "https://www.google.com/alerts/feeds/05363041802822053495/175361601541894231",
        "https://www.google.com/alerts/feeds/05363041802822053495/5467385533706296273",
    ],
    "Demographics & Fertility": [
        "https://www.google.com/alerts/feeds/05363041802822053495/1203409653122086951",
        "https://www.google.com/alerts/feeds/05363041802822053495/1203409653122086099",
        "https://www.google.com/alerts/feeds/05363041802822053495/5467385533706296630",
    ],
    "Global & Multilateral": [
        "https://www.google.com/alerts/feeds/05363041802822053495/175361601541896188",
        "https://www.google.com/alerts/feeds/05363041802822053495/5467385533706297040",
        "https://www.google.com/alerts/feeds/05363041802822053495/572631228672254336",
        "https://www.google.com/alerts/feeds/05363041802822053495/13233358519035931584",
        "https://www.google.com/alerts/feeds/05363041802822053495/808210082760888087"
    ],
}
ALL_FEED_URLS = [url for urls in FEEDS.values() for url in urls]

# Deterministic Python-side quality gate. This runs BEFORE anything reaches
# Gemini, so "when in doubt, drop" soft prompt instructions are no longer
# the only line of defence for obvious junk domains. Extend this list as
# you spot more offenders — it's cheap to maintain and catches repeat
# offenders with zero token cost.
DOMAIN_DENYLIST = {
    "indianeagle.com",
    "visahq.com",
    "visaverge.com",
    "travelandtourworld.com",
    "worldvisaacademy.com",
    "y-axis.com",
    "terratern.com",
    "radvisionworld.com",
    "immigrationnewscanada.com",
    "thevisaexperts.com",
    "migrantimes.com",
    "visaguide.world",
    "yen.com.gh",
}

# Rolling/continuously-updated pages can't be reliably cited or re-read
# later — reject by URL pattern regardless of what the LLM decides.
LIVE_BLOG_URL_PATTERNS = ["live-blog", "/live/", "liveblog", "live-updates"]


class GeminiFailure(Exception):
    """The Gemini call failed after all retries (or returned unusable output)."""


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def clean_and_unwrap_url(raw_url):
    """
    Google Alerts links are often wrapped in a tracking redirect
    (google.com/url?...&url=<real link>), and the real link inside can end
    up double-percent-encoded (%252F instead of /) once it round-trips
    through that wrapper. Unwrap the redirect and fully decode before this
    URL goes anywhere near the CSV or Gemini.
    """
    if not raw_url:
        return raw_url
    parsed = urlparse(raw_url)
    if "google.com" in parsed.netloc and parsed.path.startswith("/url"):
        query = parse_qs(parsed.query)
        if "url" in query:
            raw_url = query["url"][0]
        elif "q" in query:
            raw_url = query["q"][0]

    unquoted = unquote(raw_url)
    # Repeatedly decode in case of double/triple encoding, with a hard cap
    # so a malicious or malformed string can't loop forever.
    for _ in range(5):
        next_pass = unquote(unquoted)
        if next_pass == unquoted:
            break
        unquoted = next_pass
    return unquoted.strip()


_TRACKING_PARAMS = ("utm_", "fbclid", "gclid", "mc_", "ref", "cmpid", "ocid", "ito", "taid")


def canonical_url(url):
    """
    Comparison key only (never stored): the same article often shows up as
    http/https, with/without www., on m./amp. hosts, with /amp/ suffixes,
    utm_ tracking, fragments or trailing slashes. Collapse those so URL
    dedup catches them.
    """
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        host = p.netloc.lower()
        for prefix in ("www.", "m.", "amp.", "mobile."):
            if host.startswith(prefix):
                host = host[len(prefix):]
        path = re.sub(r"/amp/?$", "", p.path.rstrip("/"), flags=re.IGNORECASE)
        path = re.sub(r"^/amp/", "/", path, flags=re.IGNORECASE)
        query = [(k, v) for k, v in parse_qs(p.query, keep_blank_values=True).items()
                 if not k.lower().startswith(_TRACKING_PARAMS)]
        q = urlencode(sorted((k, v[0]) for k, v in query))
        return urlunparse(("https", host, path, "", q, ""))
    except Exception:
        return url.strip().lower()


def is_denylisted_domain(url):
    domain = urlparse(url).netloc.lower()
    return any(bad in domain for bad in DOMAIN_DENYLIST)


def is_live_blog_url(url):
    lowered = url.lower()
    return any(pattern in lowered for pattern in LIVE_BLOG_URL_PATTERNS)


# ---------------------------------------------------------------------------
# STEP 1 — FETCH
# ---------------------------------------------------------------------------

def _entry_published_iso(entry):
    """
    Use the date feedparser has ALREADY parsed (published_parsed /
    updated_parsed, UTC struct_time) and store it as ISO-8601. Returns ""
    if the entry has no usable date.
    """
    struct = entry.get("published_parsed") or entry.get("updated_parsed")
    if not struct:
        return ""
    return datetime(*struct[:6], tzinfo=timezone.utc).isoformat()


def _clean_title(title):
    """Alerts titles can carry <b>..</b> highlight tags and HTML entities."""
    return html.unescape(re.sub(r"<[^>]+>", "", title or "")).strip()


def fetch_all_feeds():
    """Pull every configured feed, return a flat list of raw, cleaned entries."""
    raw_items = []
    for url in ALL_FEED_URLS:
        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            print(f"  [feed] failed to parse {url}: {e}")
            continue
        for entry in parsed.entries:
            link = clean_and_unwrap_url(entry.get("link", "").strip())
            raw_items.append({
                "title": _clean_title(entry.get("title", "")),
                "url": link,
                "source_domain": urlparse(link).netloc,
                "published": _entry_published_iso(entry),
            })
    return raw_items


def fetch_article_text(url, timeout=8, max_chars=None):
    """
    Best-effort full-article-text fetch, so Gemini reasons from the actual
    article rather than just a headline. Failures are expected and fine —
    paywalls, bot-blocking, timeouts, non-HTML responses — fall back to
    title-only for that item rather than failing the run.
    """
    if trafilatura is None:
        return None
    max_chars = max_chars or ARTICLE_MAX_CHARS
    try:
        resp = requests.get(
            url, timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; GATIMobilityBot/1.0)"},
        )
        if resp.status_code != 200:
            return None
        text = trafilatura.extract(resp.text, include_comments=False)
        if not text:
            return None
        return text.strip()[:max_chars]
    except Exception as e:
        print(f"  [article-fetch] failed for {url}: {e}")
        return None


def enrich_with_article_text(items, max_items=None):
    """
    Adds an 'article_text' field to each item (None if fetch failed).
    max_items caps how many fetches run per invocation, to bound runtime
    and avoid hammering publishers if a feed has an unusually large batch.
    """
    enriched = []
    for i, item in enumerate(items):
        if max_items is not None and i >= max_items:
            item["article_text"] = None
        else:
            item["article_text"] = fetch_article_text(item["url"])
        enriched.append(item)
    fetched_ok = sum(1 for it in enriched if it.get("article_text"))
    print(f"  [article-fetch] got full text for {fetched_ok}/{len(enriched)} items.")
    return enriched


def python_prefilter(items):
    """
    Deterministic quality gate, pre-Gemini: drop denylisted domains and
    live-blog/rolling-coverage URLs before spending any tokens on them.
    """
    kept = []
    for item in items:
        if not item["url"]:
            continue
        if is_denylisted_domain(item["url"]):
            print(f"  [python gate] dropped denylisted domain: {item['url']}")
            continue
        if is_live_blog_url(item["url"]):
            print(f"  [python gate] dropped live-blog URL: {item['url']}")
            continue
        kept.append(item)
    return kept


def _title_key(title):
    t = re.sub(r"\s+[-|–—]\s+[^-|–—]{2,40}$", "", (title or "").lower())  # drop " - Outlet"
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def dedupe_candidates(items):
    """
    The same article often arrives through several of the 26 feeds. Collapse
    by canonical URL and by normalised title BEFORE fetching article text or
    calling Gemini — this is the biggest token saver.
    """
    seen_u, seen_t, out = set(), set(), []
    for item in items:
        cu, tk = canonical_url(item["url"]), _title_key(item["title"])
        if cu in seen_u or (tk and tk in seen_t):
            continue
        seen_u.add(cu)
        if tk:
            seen_t.add(tk)
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# STEP 2 — DATE WINDOW (deterministic, no LLM)
# ---------------------------------------------------------------------------

def in_date_window(published_iso, now=None, window_start=None):
    """
    If window_start is given (watermark from the last successful run, or a
    WINDOW_HOURS backfill), use [window_start, now]. Otherwise the original
    default: last 24 hours, or back to Saturday 00:00 UTC on a Monday run.
    Google Alerts feeds only hold what they've collected since each alert
    was created, so a wider window can't return older items than exist.
    """
    now = now or datetime.now(timezone.utc)
    if not published_iso:
        return False
    try:
        pub_dt = datetime.fromisoformat(published_iso)
    except ValueError:
        return False

    if window_start is not None:
        return window_start <= pub_dt <= now

    if now.weekday() == 0:  # Monday: back to Saturday 00:00 UTC
        start = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start <= pub_dt <= now
    return now - timedelta(hours=24) <= pub_dt <= now


# ---------------------------------------------------------------------------
# STATE (makes extra scheduled runs harmless and missed runs self-healing)
# ---------------------------------------------------------------------------

def load_state():
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            state = json.load(f)
        return state if isinstance(state, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


def compute_window_start(state, now):
    """None => use the in_date_window default."""
    override = os.environ.get("WINDOW_HOURS")
    if override:
        return now - timedelta(hours=int(override))
    last = state.get("last_success_utc")
    if last:
        try:
            start = datetime.fromisoformat(last) - timedelta(hours=3)  # overlap; dedup absorbs it
            return max(start, now - timedelta(hours=MAX_CATCHUP_HOURS))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# TOPIC SIMILARITY (deterministic) — same event, different wording
# ---------------------------------------------------------------------------

_STOP = set("""
a an the and or but of to in on for with by from at as is are was were be been being it its this that
these those their there than then also not no nor so such into onto over under after before amid about
against between during while more most less new says say said will would could may might can has have
had do does did up down out off per via vs
visa visas work works worker workers student students india indian indians us uk immigration migrant
migrants migration government govt policy rule rules country countries labour labor job jobs foreign
international overseas abroad report reports update plan plans move moves key latest major big
programme program
""".split())


def _stem(tok):
    for suf in ("ing", "ed", "es"):
        if tok.endswith(suf) and len(tok) - len(suf) >= 4:
            tok = tok[:-len(suf)]
            break
    else:
        if tok.endswith("s") and not tok.endswith("ss") and len(tok) - 1 >= 3:
            tok = tok[:-1]
    if tok.endswith("e") and len(tok) > 4:
        tok = tok[:-1]
    return tok


def topic_tokens(text):
    text = (text or "").lower()
    text = re.sub(r"(?<=\d),(?=\d{3})", "", text)            # 70,000 -> 70000
    text = text.replace("h-1b", "h1b").replace("f-1", "f1").replace("j-1", "j1")
    out = set()
    for tok in re.findall(r"[a-z0-9]+", text):
        if tok in _STOP:
            continue
        if len(tok) < 3 and not any(ch.isdigit() for ch in tok):
            continue
        out.add(_stem(tok))
    return out


def topic_score(tokens_a, tokens_b):
    """Overlap coefficient on distinctive tokens. Returns (score, shared_count)."""
    if min(len(tokens_a), len(tokens_b)) < 3:
        return 0.0, 0
    shared = len(tokens_a & tokens_b)
    return shared / min(len(tokens_a), len(tokens_b)), shared


def _country_set(s):
    return {c.strip().lower() for c in (s or "").split("|") if c.strip()}


def _countries_compatible(a, b):
    ca, cb = _country_set(a), _country_set(b)
    return (not ca or not cb or bool(ca & cb) or "global" in ca or "global" in cb)


def attach_repeat_hints(candidates, seen_items):
    """Add `possible_repeat_of` (history ids) to candidates that look like a past topic."""
    if not seen_items:
        return
    hist = [(h["id"], topic_tokens(f"{h['headline']} {h['summary']}")) for h in seen_items]
    flagged = 0
    for c in candidates:
        toks = topic_tokens(f"{c['title']} {(c.get('article_text') or '')[:300]}")
        scored = []
        for hid, htoks in hist:
            s, shared = topic_score(toks, htoks)
            if s >= TOPIC_HINT_THRESHOLD and shared >= 3:
                scored.append((s, hid))
        if scored:
            scored.sort(reverse=True)
            c["possible_repeat_of"] = [hid for _, hid in scored[:2]]
            flagged += 1
    print(f"  [topic hints] {flagged}/{len(candidates)} candidates flagged as possible repeats of past items.")


# ---------------------------------------------------------------------------
# STEP 3 — HISTORY: last 7 days of the Alerts CSV
# ---------------------------------------------------------------------------

def parse_any_date(s):
    """ISO first; tolerate DD-MM-YYYY / DD/MM/YYYY (a spreadsheet round-trip rewrites dates)."""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def load_seen_set(days=7):
    """
    Return (seen_canonical_urls, seen_headline_stems, seen_items) for the last
    `days` days of ALERTS_CSV_PATH. seen_items carries id + headline + summary
    + countries so events can be matched by TOPIC, not headline wording.
    """
    seen_urls, seen_stems, seen_items = set(), set(), []
    if not os.path.exists(ALERTS_CSV_PATH):
        return seen_urls, seen_stems, seen_items

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
    with open(ALERTS_CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row_date = parse_any_date(row.get("date"))
            if row_date is None or row_date < cutoff:
                continue
            seen_urls.add(canonical_url(row.get("url", "")))
            seen_stems.add(row.get("headline", "").strip().lower())
            seen_items.append({
                "id": f"H{len(seen_items) + 1}",
                "date": row_date.isoformat(),
                "headline": row.get("headline", ""),
                "summary": row.get("summary", ""),
                "countries": row.get("countries", ""),
                "url": row.get("url", ""),
            })
    return seen_urls, seen_stems, seen_items


def compact_history(seen_items):
    """What Gemini actually sees: no URLs (Python already checks those), short summaries."""
    return [{"id": h["id"], "date": h["date"], "headline": h["headline"],
             "summary": h["summary"][:200], "countries": h["countries"]} for h in seen_items]


# ---------------------------------------------------------------------------
# STEP 4 — GEMINI (retrying) : tagging call
# ---------------------------------------------------------------------------

GEMINI_PROMPT_TEMPLATE = """You are a research analyst producing a daily migration/labour-mobility news
bulletin for GATI Foundation, an Indian organisation working on India's
overseas employment ecosystem.

You will receive a JSON array of candidate items pulled from Google Alerts
RSS feeds (title, url, source_domain, published, and article_text where a
full-text fetch succeeded -- article_text is null/missing for some items
when the fetch failed, which is expected and fine). These have already
been filtered to the correct date window and are NOT yet checked for
source quality, duplication, or already-posted status. When article_text
is present, base your reading of the item on it, not just the title --
it is the actual article, the title/snippet alone can be misleading or
mismatched. When article_text is absent, do your best from title/source/
url alone, and be more conservative in PART 1.6(d) (content-matches-
summary) since you can't verify the destination content directly. Your
job has five parts.

PART 1 -- SOURCE QUALITY GATE
You are being given items that ALREADY passed a Python domain denylist
filter, so obvious junk domains are gone. Your job is the harder judgment
call: drop any item whose source is not a genuine newsroom, wire service,
government/multilateral body, or established trade/specialist publication.
Exclude content-aggregator sites, SEO/affiliate "visa news" blogs, and
immigration-consultancy lead-gen sites (their tell: generic listicle framing,
no clear byline/masthead, heavy keyword stuffing, thin original reporting,
or a domain with no track record of original migration-policy journalism).
Outside the Indian/Tier-1 wire/government press you already know well,
OTHER sources are acceptable IF they are a real newsroom with a clear
editorial masthead, named bylines, and a consistent track record of
original migration/labour-policy reporting. When genuinely unsure whether
an unfamiliar outlet qualifies, drop the item rather than include it.

PART 1.6 -- URL & FORMAT INTEGRITY (mandatory, per item, before it may pass)
Confirm ALL of the following:
  (a) SPECIFIC ARTICLE: the URL points to one dated, individually-published
      article -- NOT a homepage, section/tag page, topic page, or a
      continuously-updated "live blog" / "live updates" / rolling-coverage
      page (these change content after publication and cannot be reliably
      cited or re-read later). Reject any URL containing patterns like
      "live-blog", "/live/", "liveblog", or similar rolling-coverage
      indicators.
  (b) SOURCE MATCHES URL: the "source" field you write must be the outlet
      that actually owns the article's domain -- never cite one outlet's
      name against another domain's URL.
  (c) EDITORIAL, NOT SPONSORED: reject advertiser/sponsored/branded-content
      pages even on an otherwise acceptable domain.
  (d) CONTENT MATCHES SUMMARY: the summary you write must describe what is
      actually on that specific URL -- never write a summary based only on
      the title/snippet string if it looks like it may not match the real
      destination article (Google Alerts snippets occasionally get mismatched
      to redirect URLs). If title and URL seem to describe different
      stories, drop the item rather than guess.
Drop the item if it fails any of (a)-(d).

PART 2 -- SAME-BATCH DEDUPLICATION
Before finalizing output, group ALL surviving items by underlying event
(not by headline wording) -- e.g. multiple outlets covering one policy
announcement, one court ruling, one election result are ONE event, even if
reported in different batches or with differently-worded headlines. For
each group of 2+ items covering the same event, output ONLY the single
best-sourced item (highest tier, most original reporting, clearest
attribution) and silently discard the rest of that group entirely -- do not
include them even with in_top7=false. Do this grouping pass deliberately
across the WHOLE item set you were given, not just within any sub-section
of it.

PART 3 -- ALREADY-POSTED CHECK
You are given the items already posted in the last 7 days (date, headline,
summary, countries, url). Compare each surviving candidate against them by
UNDERLYING EVENT, using the summaries -- NOT by headline wording. A new
article from a different outlet, with a differently-worded headline, about
the same announcement, report, ruling or statistic as a past item is the
SAME story and must be dropped. Only keep it if it contains a genuine
material update (a new number, a signed deal, a confirmed effective date,
a court ruling, a government response) -- in that case keep it, prefix the
headline with "UPDATE:", and make the summary about what changed. If the
only difference is a different outlet or extra background, drop it.

PART 4 -- CATEGORISE, INFER RELEVANCE, AND TAG
For every item that survives Parts 1-3, read the full title/content and:

(a) categories -- pipe-separated, containing ONLY values from this exact
set (copy them verbatim, nothing else -- never put a theme name, a country
name, or any other string in this field): India Specific | Destination
Countries | Competitor Countries | Demographics & Fertility | Global &
Multilateral.
  - "Competitor Countries" means ONLY these labour-sending rivals:
    Philippines, Vietnam, Indonesia, Turkey, Brazil, Poland, Romania,
    Ukraine, Bulgaria, Bangladesh, Nepal, Sri Lanka. A story about a
    country where Indians go to study/work (Japan, Germany, Australia,
    Canada, the Gulf, etc.) is "Destination Countries", never Competitor.
  - Use "India Specific" only when India, Indian workers, Indian students,
    NRIs, or an India bilateral deal is directly named.
  - For Destination/Competitor Country items where India is NOT named,
    judge whether the development would still plausibly affect Indian
    workers or students. If yes, state that inferred relevance explicitly
    in the summary sentence -- do not silently assume it.

(b) theme -- exactly one, copied exactly: Enforcement and Crisis | Students
and Education | Bilateral Deals and Trade | Remittances and Diaspora |
Demographics and Workforce | Skills and Talent | Labour and Workers | Visas
and Work Permits | Other.

(c) countries -- pipe-separated, max 4, plain English names (United States,
United Kingdom, UAE, South Korea...); use "India" only when India is the
sole subject; use "European Union" / "Gulf Region" only for bloc-wide
stories; use "Global" when no specific country applies.

(d) vibe -- negative | neutral | positive, from the standpoint of Indian
workers' and India's mobility interests.

(e) summary -- one sentence: what happened and why it matters for India
(state inferred relevance here if not explicit in the source).

(f) stakeholders -- pipe-separated, max 5, the specific groups directly
named or unambiguously identified in the article as affected (e.g. "H-1B
holders", "Gulf blue-collar workers", "Indian nursing graduates",
"overseas recruitment agencies", "MEA", "Indian IT firms", "international
students (undergraduate)"). This is a factual extraction, not a
prediction -- only name groups the article itself identifies or makes
unambiguous, never groups you're inferring might plausibly be affected
downstream. List the groups AFFECTED, not the authorities acting (e.g.
"international students", not "DHS" or "the government"). This field is for internal analysis only and is never shown
in the Slack bulletin.

PART 5 -- RANK
1. Highest: India Specific
2. High: Destination Countries (extra weight: Europe, Japan, South Korea)
3. Medium: Competitor Countries
4. Contextual: Demographics & Fertility, Global & Multilateral
Tie-break: policy over opinion, bilateral over domestic, data over
commentary, more recent over older.
HARD LIMIT: in_top7=true on AT MOST 7 items total, and no fewer than 5 if
at least 5 items survived Parts 1-3. Count your true values before
returning output. This limit applies across the ENTIRE item set you were
given, not per-bucket and not per-batch -- e.g. if 3 items all cover the
same Australia migration-cap announcement, Part 2 should already have
reduced that to 1 item, which then competes for ONE of the 7 slots, not
three. Every surviving item goes in the output regardless of in_top7
value -- the limit controls how many are true, not how many rows you
return.

OUTPUT
Return ONLY a JSON array of objects, one per surviving item, matching this
schema -- no prose, no markdown fences:
[{{
  "date": "YYYY-MM-DD",
  "headline": "max 10 words",
  "url": "...",
  "categories": "pipe-separated bucket names",
  "summary": "one sentence",
  "vibe": "negative|neutral|positive",
  "in_top7": true,
  "source": "publication name",
  "theme": "single theme from Part 4b list",
  "countries": "pipe-separated, max 4",
  "stakeholders": "pipe-separated, max 5, factual only"
}}]

INPUT ITEMS:
{items_json}

ALREADY POSTED (last 7 days -- URLs and headline stems):
{seen_set_json}

TODAY'S DATE: {today_date}
"""

# Appended in code AFTER formatting, so it also applies when the external
# prompt file overrides the template above (no need to keep two files in sync).
PROMPT_ADDENDUM = """

ADDITIONAL NOTES (added by the pipeline)
- Entries under ALREADY POSTED now carry an `id` (and no url). A candidate may
  carry `possible_repeat_of`: ids of already-posted items that look like the
  same underlying event. Check those first. Drop the candidate if it is the
  same event, unless it has a genuine material update (then keep it and prefix
  the headline with "UPDATE:").
- Judge by EVENT, not wording: another outlet covering the same announcement,
  report, ruling or statistic is a repeat, even with a different headline.
"""

RESPONSE_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "date": {"type": "STRING"},
            "headline": {"type": "STRING"},
            "url": {"type": "STRING"},
            "categories": {"type": "STRING"},
            "summary": {"type": "STRING"},
            "vibe": {"type": "STRING", "enum": ["negative", "neutral", "positive"]},
            "in_top7": {"type": "BOOLEAN"},
            "source": {"type": "STRING"},
            "theme": {"type": "STRING"},
            "countries": {"type": "STRING"},
            "stakeholders": {"type": "STRING"},
        },
        "required": FIELDNAMES,
    },
}

_RETRYABLE_HINTS = ("unavailable", "overloaded", "timeout", "timed out", "deadline", "temporar",
                    "connection", "resource_exhausted", "rate limit", "try again",
                    "429", "500", "502", "503", "504")


def _is_retryable(exc):
    code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
    if isinstance(code, int):
        return code in (408, 429, 500, 502, 503, 504)
    msg = str(exc).lower()
    return any(h in msg for h in _RETRYABLE_HINTS)


def _generate_json(prompt, schema, attempts, label):
    """
    One Gemini call that must return parseable JSON. Retries (with backoff) on
    overload / rate-limit / network errors AND on empty or unparseable output.
    Raises GeminiFailure when it gives up — the caller decides what that means.
    """
    client = genai.Client(api_key=GEMINI_API_KEY)
    last = None
    for i in range(attempts):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=schema,
                    max_output_tokens=MAX_OUTPUT_TOKENS,
                ),
            )
            return json.loads(response.text)
        except (json.JSONDecodeError, TypeError, AttributeError) as e:
            last = f"unusable output ({type(e).__name__}: {e})"
        except Exception as e:
            if not _is_retryable(e):
                raise GeminiFailure(f"{label}: non-retryable error: {e}") from e
            last = f"{type(e).__name__}: {e}"
        if i < attempts - 1:
            delay = RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)]
            print(f"  [gemini:{label}] attempt {i + 1}/{attempts} failed ({last}); retrying in {delay}s")
            time.sleep(delay)
    raise GeminiFailure(f"{label}: gave up after {attempts} attempts ({last})")


def load_prompt_template():
    """
    Prefer prompt/google_alerts_gemini_prompt.txt (matches the repo's
    existing convention for prompt/daily_briefing_prompt_v2.txt) so the
    prompt can be versioned/edited independently of this script. Falls
    back to the inline GEMINI_PROMPT_TEMPLATE constant if that file isn't
    there yet, so nothing breaks before you've moved it.
    """
    external_path = os.environ.get(
        "GEMINI_PROMPT_PATH", "prompt/google_alerts_gemini_prompt.txt"
    )
    if os.path.exists(external_path):
        with open(external_path, encoding="utf-8") as f:
            return f.read()
    return GEMINI_PROMPT_TEMPLATE


def tag_items_gemini(candidate_items, seen_items):
    """Returns tagged rows ([] is a legitimate 'nothing qualified'); raises GeminiFailure on failure."""
    if not candidate_items:
        return []

    template = load_prompt_template()
    if "{seen_set_json}" not in template:
        print("WARNING: the prompt template has no {seen_set_json} placeholder — "
              "Gemini will NOT see the already-posted history. Fix the prompt file.")

    payload = []
    for c in candidate_items:
        p = {k: c[k] for k in ("title", "url", "source_domain", "published")}
        if c.get("article_text"):
            p["article_text"] = c["article_text"]
        if c.get("possible_repeat_of"):
            p["possible_repeat_of"] = c["possible_repeat_of"]
        payload.append(p)

    prompt = template.format(
        items_json=json.dumps(payload, ensure_ascii=False),
        seen_set_json=json.dumps(compact_history(seen_items), ensure_ascii=False),
        today_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ) + PROMPT_ADDENDUM
    print(f"  [gemini:tag] prompt ~{len(prompt) // 4:,} tokens ({len(prompt):,} chars).")

    rows = _generate_json(prompt, RESPONSE_SCHEMA, GEMINI_MAX_ATTEMPTS, "tag")
    if not isinstance(rows, list):
        raise GeminiFailure("tag: response was not a JSON array")

    # Basic field-presence validation; drop malformed rows rather than crash.
    valid_rows = []
    for row in rows:
        if isinstance(row, dict) and all(k in row for k in FIELDNAMES):
            valid_rows.append(row)
        else:
            print(f"Dropping malformed row (missing fields): {row}")
    return valid_rows


# ---------------------------------------------------------------------------
# STEP 4.5 — DETERMINISTIC POST-VALIDATION (does not trust Gemini's word)
# ---------------------------------------------------------------------------

VALID_CATEGORIES = {
    "India Specific", "Destination Countries", "Competitor Countries",
    "Demographics & Fertility", "Global & Multilateral",
}
VALID_THEMES = {
    "Enforcement and Crisis", "Students and Education",
    "Bilateral Deals and Trade", "Remittances and Diaspora",
    "Demographics and Workforce", "Skills and Talent",
    "Labour and Workers", "Visas and Work Permits", "Other",
}
# Rough rank used only to break ties when enforcing the top-7 cap.
CATEGORY_PRIORITY = {
    "India Specific": 4,
    "Destination Countries": 3,
    "Competitor Countries": 2,
    "Demographics & Fertility": 1,
    "Global & Multilateral": 1,
}


def _pipe(s):
    """Normalise a multi-value cell to 'a|b|c' (same delimiter as data/items.csv)."""
    return "|".join(p.strip() for p in (s or "").split("|") if p.strip())


def sanitize_categories(cat_string):
    """Strip anything that isn't one of the five valid category names —
    catches theme/country strings Gemini occasionally leaks into this
    field despite the schema being typed as STRING, not an enum."""
    parts = [p.strip() for p in (cat_string or "").split("|")]
    valid = [p for p in parts if p in VALID_CATEGORIES]
    return "|".join(dict.fromkeys(valid)) if valid else "Global & Multilateral"


def sanitize_theme(theme_string):
    theme_string = (theme_string or "").strip()
    return theme_string if theme_string in VALID_THEMES else "Other"


def row_priority(row):
    """Higher = more important, for top-7 tie-breaking."""
    cats = [c.strip() for c in row.get("categories", "").split("|")]
    return max((CATEGORY_PRIORITY.get(c, 0) for c in cats), default=0)


def _is_top(row):
    return row.get("in_top7") in (True, "True", "true", "TRUE")


def post_validate_rows(rows):
    """
    Deterministic cleanup pass, independent of anything Gemini claimed:
      - re-clean/re-check every URL (denylist, live-blog pattern)
      - sanitize categories/theme against the fixed value sets
      - drop duplicate (canonical) URLs within this batch
      - enforce the top-7 cap in Python, by priority, regardless of how
        many rows Gemini itself marked in_top7=true
    """
    cleaned = []
    seen_urls_this_batch = set()

    for row in rows:
        url = clean_and_unwrap_url(row.get("url", ""))
        if not url:
            continue
        if is_denylisted_domain(url):
            print(f"  [post-validate] dropped denylisted domain: {url}")
            continue
        if is_live_blog_url(url):
            print(f"  [post-validate] dropped live-blog URL: {url}")
            continue
        cu = canonical_url(url)
        if cu in seen_urls_this_batch:
            print(f"  [post-validate] dropped duplicate URL within batch: {url}")
            continue
        seen_urls_this_batch.add(cu)

        row["url"] = url
        row["categories"] = sanitize_categories(row.get("categories", ""))
        row["theme"] = sanitize_theme(row.get("theme", ""))
        row["countries"] = _pipe(row.get("countries", ""))
        row["stakeholders"] = _pipe(row.get("stakeholders", ""))
        cleaned.append(row)

    # Heuristic same-event backstop on near-identical headlines sharing a country.
    deduped = []
    for row in cleaned:
        row_countries = set(c.strip() for c in row.get("countries", "").split("|"))
        row_headline_norm = row.get("headline", "").strip().lower()
        is_dup = False
        for kept in deduped:
            kept_countries = set(c.strip() for c in kept.get("countries", "").split("|"))
            if not (row_countries & kept_countries):
                continue
            similarity = SequenceMatcher(
                None, row_headline_norm, kept.get("headline", "").strip().lower()
            ).ratio()
            if similarity >= 0.72:
                print(f"  [post-validate] heuristic same-event dedup: "
                      f"dropped '{row.get('headline')}' (similar to "
                      f"'{kept.get('headline')}')")
                if row_priority(row) > row_priority(kept):
                    deduped.remove(kept)
                    deduped.append(row)
                is_dup = True
                break
        if not is_dup:
            deduped.append(row)
    cleaned = deduped

    # Hard-enforce the top-7 cap here — do not trust Gemini's in_top7 count.
    marked_true = [r for r in cleaned if _is_top(r)]
    if len(marked_true) > 7:
        print(f"  [post-validate] Gemini marked {len(marked_true)} as in_top7=true; "
              f"capping to 7 by priority.")
        marked_true.sort(key=row_priority, reverse=True)
        keep_urls = {r["url"] for r in marked_true[:7]}
        for r in cleaned:
            r["in_top7"] = r["url"] in keep_urls

    return cleaned


# ---------------------------------------------------------------------------
# STEP 4.6 — TOPIC DEDUP of the final rows (the "no repeated topics" layer)
# ---------------------------------------------------------------------------

def _row_tokens(row):
    return topic_tokens(f"{row.get('headline', '')} {row.get('summary', '')}")


def topic_backstop(rows, seen_items):
    """
    Deterministic, high-confidence-only: drop a row whose headline+summary
    overlap an already-posted item (or a higher-priority row in this batch)
    above TOPIC_DEDUP_THRESHOLD. Never drops 'UPDATE:' rows. Near-misses are
    logged so the threshold can be tuned from real runs.
    """
    hist = [(h, topic_tokens(f"{h['headline']} {h['summary']}")) for h in seen_items]
    ordered = sorted(rows, key=lambda r: (not _is_top(r), -row_priority(r)))
    kept, kept_tokens, dropped = [], [], []
    for row in ordered:
        toks = _row_tokens(row)
        verdict = None
        if not str(row.get("headline", "")).upper().startswith("UPDATE:"):
            pools = [(h["headline"], t, h.get("countries", "")) for h, t in hist]
            pools += [(k["headline"], t, k.get("countries", "")) for k, t in zip(kept, kept_tokens)]
            for other_headline, otoks, ocountries in pools:
                s, shared = topic_score(toks, otoks)
                if s >= TOPIC_DEDUP_THRESHOLD and shared >= 4 and _countries_compatible(row.get("countries"), ocountries):
                    verdict = (other_headline, s)
                    break
                if 0.4 <= s < TOPIC_DEDUP_THRESHOLD and shared >= 3:
                    print(f"  [topic backstop] near-miss {s:.2f}: '{row.get('headline')}' ~ '{other_headline}'")
        if verdict:
            print(f"  [topic backstop] dropped '{row.get('headline')}' (same topic as "
                  f"'{verdict[0]}', score {verdict[1]:.2f})")
            dropped.append(row)
        else:
            kept.append(row)
            kept_tokens.append(toks)
    return kept


TOPIC_DEDUP_PROMPT = """You are de-duplicating a news bulletin by TOPIC (underlying event), not by wording.

NEW items (ids N1, N2, ...) are about to be posted. HISTORY items (ids H1, H2, ...) were already
posted in the last 7 days. For EVERY new item decide whether it covers the same underlying event
(same announcement, report, ruling, statistic, deal or incident) as a HISTORY item, or as an
EARLIER-numbered NEW item. A different outlet, different headline wording, or extra background
does NOT make it a different event.

A genuinely new development in an ongoing story is NOT a duplicate: a court ruling, lawsuit,
official response, signed deal, confirmed effective date or new figure that goes beyond the earlier
item is a material update. A reaction or lawsuit about a previously reported rule is a material
update only if it adds new facts rather than restating the original news.

Return ONLY a JSON array with exactly one object per NEW item:
  {"id": "N1", "duplicate_of": "H3" or "N2" or "", "material_update": true or false}
duplicate_of is "" when the item is a distinct event. material_update is true only when
duplicate_of is set AND the item adds a genuinely new development.

NEW:
<<NEW>>

HISTORY:
<<HISTORY>>
"""

TOPIC_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "id": {"type": "STRING"},
            "duplicate_of": {"type": "STRING"},
            "material_update": {"type": "BOOLEAN"},
        },
        "required": ["id", "duplicate_of", "material_update"],
    },
}


def llm_topic_dedup(rows, seen_items):
    """
    One small, focused Gemini call (headline + summary only, roughly 5k tokens)
    asking ONLY "is this the same event as anything we already posted?" —
    a far easier task than finding repeats inside the big tagging prompt.
    Fails OPEN: any error leaves the rows untouched, so a flaky second call
    can never lose the day's bulletin.
    """
    if not rows or (len(rows) < 2 and not seen_items):
        return rows

    ordered = sorted(rows, key=lambda r: (not _is_top(r), -row_priority(r)))
    ids = {f"N{i + 1}": r for i, r in enumerate(ordered)}
    new_json = [{"id": i, "headline": r["headline"], "summary": r["summary"],
                 "countries": r["countries"]} for i, r in ids.items()]
    prompt = (TOPIC_DEDUP_PROMPT
              .replace("<<NEW>>", json.dumps(new_json, ensure_ascii=False))
              .replace("<<HISTORY>>", json.dumps(compact_history(seen_items), ensure_ascii=False)))

    try:
        decisions = _generate_json(prompt, TOPIC_SCHEMA, 3, "topic-dedup")
    except GeminiFailure as e:
        print(f"  [topic-dedup] skipped (kept all rows): {e}")
        return rows
    if not isinstance(decisions, list):
        print("  [topic-dedup] skipped: response was not a JSON array.")
        return rows

    valid_refs = set(ids) | {h["id"] for h in seen_items}
    drop, updated = {}, []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        rid, ref = d.get("id"), (d.get("duplicate_of") or "").strip()
        if rid not in ids or not ref or ref == rid or ref not in valid_refs:
            continue
        if d.get("material_update") is True:
            updated.append(rid)
        else:
            drop[rid] = ref

    # Safety valve: if the model wants to drop an implausible share of the day, distrust it.
    if len(ids) >= 4 and len(drop) > max(3, int(0.7 * len(ids))):
        print(f"  [topic-dedup] ignored: it wanted to drop {len(drop)}/{len(ids)} rows (implausible).")
        return rows

    for rid in updated:
        r = ids[rid]
        if not str(r["headline"]).upper().startswith("UPDATE:"):
            r["headline"] = "UPDATE: " + r["headline"]
    for rid, ref in drop.items():
        print(f"  [topic-dedup] dropped '{ids[rid]['headline']}' (same event as {ref})")
    survivors = [r for i, r in ids.items() if i not in drop]
    print(f"  [topic-dedup] {len(drop)} dropped, {len(updated)} kept as UPDATE, {len(survivors)} remain.")
    return survivors


def refill_top7(rows):
    """After dedup drops, make sure the bulletin still has up to 5 top items (cap of 7 unchanged)."""
    target = min(5, len(rows))
    top = [r for r in rows if _is_top(r)]
    if len(top) >= target:
        return rows
    rest = sorted((r for r in rows if not _is_top(r)), key=row_priority, reverse=True)
    for r in rest[: target - len(top)]:
        r["in_top7"] = True
    return rows


# ---------------------------------------------------------------------------
# STEP 5 — APPEND TO THE ALERTS CSV, SORTED BY DATE
# ---------------------------------------------------------------------------

def append_rows(rows):
    if not rows:
        print("No rows to append.")
        return

    existing_rows = []
    if os.path.exists(ALERTS_CSV_PATH):
        with open(ALERTS_CSV_PATH, newline="", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
    for r in existing_rows:  # heal spreadsheet-mangled dates so sorting/history keep working
        d = parse_any_date(r.get("date"))
        if d:
            r["date"] = d.isoformat()

    all_rows = existing_rows + rows
    all_rows.sort(key=lambda r: r.get("date", ""))

    os.makedirs(os.path.dirname(ALERTS_CSV_PATH) or ".", exist_ok=True)
    with open(ALERTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in FIELDNAMES})

    print(f"Appended {len(rows)} new row(s). {os.path.basename(ALERTS_CSV_PATH)} now has {len(all_rows)} total.")


# ---------------------------------------------------------------------------
# STEP 6 — FORMAT + POST TO SLACK (optional, off by default)
# ---------------------------------------------------------------------------

BUCKET_HEADERS = [
    ("India Specific", ":flag-in: India"),
    ("Destination Countries", ":airplane: Destination Countries"),
    ("Competitor Countries", ":earth_asia: Competitor Countries"),
    ("Demographics & Fertility", ":bar_chart: Demographics & Fertility"),
    ("Global & Multilateral", ":bar_chart: Demographics & Fertility"),  # folded in per spec
]
VIBE_EMOJI = {"negative": ":red_circle:", "neutral": ":large_yellow_circle:", "positive": ":large_green_circle:"}


def _format_item_block(row):
    emoji = VIBE_EMOJI.get(row.get("vibe", "neutral"), ":large_yellow_circle:")
    country = (row.get("countries", "") or "Global").split("|")[0].strip()
    return (
        f"{emoji} *{row.get('headline', '')}*\n"
        f":world_map: *{country}* | :newspaper: *{row.get('source', '')}* — "
        f"Published {row.get('date', '')}\n"
        f"{row.get('summary', '')}\n"
        f":link: {row.get('url', '')}"
    )


def _format_bucket_sections(rows):
    """Group rows by bucket in the fixed display order, skipping empties."""
    seen_buckets_rendered = set()
    sections = []
    for bucket_name, header in BUCKET_HEADERS:
        if header in seen_buckets_rendered:
            continue  # Global & Multilateral folds into the same header as Demographics
        bucket_rows = [
            r for r in rows
            if bucket_name in [c.strip() for c in r.get("categories", "").split("|")]
        ]
        if not bucket_rows:
            continue
        seen_buckets_rendered.add(header)
        blocks = "\n\n".join(_format_item_block(r) for r in bucket_rows)
        sections.append(f"{header}\n\n{blocks}")
    return sections


def format_slack_bulletin(rows):
    """
    Returns (main_text, thread_text) matching the existing bulletin's
    emoji/format conventions. main_text covers in_top7=true rows, grouped
    by bucket; thread_text covers everything else. Either half can come
    back empty-string if there's nothing to show there.
    """
    top_rows = [r for r in rows if _is_top(r)]
    rest_rows = [r for r in rows if r not in top_rows]

    if len(rows) < 3:
        main_text = f"_Quiet news day — only {len(rows)} item(s) found in the last 24 hours._"
    else:
        main_sections = _format_bucket_sections(top_rows)
        main_text = "\n\n".join(main_sections) if main_sections else \
            "_Quiet news day — no items cleared the quality/relevance bar._"

    if rest_rows:
        thread_sections = _format_bucket_sections(rest_rows)
        thread_text = "\n\n".join(thread_sections)
    else:
        thread_text = ":card_index_dividers: *No additional items today beyond the main briefing.*"

    return main_text, thread_text


def post_to_slack(main_text, thread_text):
    """
    Posts the main bulletin, then the remaining items as a thread reply,
    directly via the Slack Web API. Returns True only if BOTH posts landed
    (so the caller can keep the bulletin and retry on the next run).

    Requires SLACK_BOT_TOKEN (scope chat:write) and SLACK_CHANNEL_ID.
    Gated behind POST_TO_SLACK=true so nothing posts unless you opt in.
    """
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_CHANNEL_ID"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        main_resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=headers,
            json={"channel": channel, "text": main_text, "mrkdwn": True},
            timeout=10,
        ).json()
        if not main_resp.get("ok"):
            print(f"  [slack] main post failed: {main_resp}")
            return False

        thread_resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=headers,
            json={"channel": channel, "text": thread_text, "mrkdwn": True,
                  "thread_ts": main_resp["ts"]},
            timeout=10,
        ).json()
        if not thread_resp.get("ok"):
            print(f"  [slack] thread reply failed: {thread_resp}")
            return False
    except Exception as e:
        print(f"  [slack] post failed: {e}")
        return False
    return True


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    post_enabled = os.environ.get("POST_TO_SLACK", "false").lower() == "true"
    force = os.environ.get("FORCE_RUN", "false").lower() == "true"
    state = load_state()

    # 0. A bulletin that was built earlier but never reached Slack goes out first.
    if post_enabled and state.get("pending_post"):
        pending = state["pending_post"]
        print("Found an unposted bulletin from an earlier run — retrying Slack post.")
        if post_to_slack(pending["main"], pending["thread"]):
            state.pop("pending_post")
            state["last_complete_date"] = pending.get("date", today)
            save_state(state)
            print("  Posted the pending bulletin.")
        else:
            print("  Still could not post; will try again next run.")

    # 1. Already finished today? Extra scheduled runs cost nothing.
    if not force and state.get("last_complete_date") == today and not state.get("pending_post"):
        print(f"Today's run ({today}) already completed — nothing to do. (FORCE_RUN=true overrides.)")
        return

    print("Fetching Google Alerts RSS feeds...")
    raw_items = fetch_all_feeds()
    print(f"  {len(raw_items)} raw entries across {len(ALL_FEED_URLS)} feeds.")
    if not raw_items:
        # Zero entries from all 26 feeds is a fetch problem, not a quiet news day.
        print("ERROR: no entries from any feed — treating as a failed run (nothing written or posted).")
        sys.exit(1)

    undated = sum(1 for item in raw_items if not item["published"])
    dated = sorted(item["published"] for item in raw_items if item["published"])
    if dated:
        print(f"  feed date range: {dated[0]} -> {dated[-1]} ({undated} undated)")
    else:
        print(f"  WARNING: none of the {len(raw_items)} entries carry a parseable date.")

    window_start = compute_window_start(state, now)
    if window_start:
        print(f"  window: {window_start.isoformat()} -> now")
    windowed = [item for item in raw_items if in_date_window(item["published"], now, window_start)]
    print(f"  {len(windowed)} entries in the date window.")

    gated = python_prefilter(windowed)
    print(f"  {len(gated)} entries survive the Python domain/live-blog gate.")

    unique = dedupe_candidates(gated)
    print(f"  {len(unique)} unique after collapsing the same article across feeds.")

    seen_urls, seen_stems, seen_items = load_seen_set(days=7)
    print(f"  history: {len(seen_items)} items / {len(seen_urls)} URLs in the last 7 days.")

    # Exact-URL dedup against history (canonicalised: amp/m./utm variants count as seen).
    candidates = [item for item in unique if canonical_url(item["url"]) not in seen_urls]
    print(f"  {len(candidates)} candidates after the URL pre-filter.")

    if len(candidates) > MAX_CANDIDATES:
        candidates.sort(key=lambda it: it["published"], reverse=True)
        print(f"  trimming {len(candidates)} -> {MAX_CANDIDATES} candidates (newest first; "
              f"raise MAX_CANDIDATES to keep more).")
        candidates = candidates[:MAX_CANDIDATES]

    max_fetches = os.environ.get("MAX_ARTICLE_FETCHES")
    candidates = enrich_with_article_text(
        candidates, max_items=int(max_fetches) if max_fetches else None
    )
    attach_repeat_hints(candidates, seen_items)
    print(f"  sending {len(candidates)} candidates to Gemini.")

    try:
        tagged_rows = tag_items_gemini(candidates, seen_items)
    except GeminiFailure as e:
        # Fail closed: no CSV write, no Slack post, no state change. The window starts at the
        # last SUCCESSFUL run, so the next scheduled run covers this gap automatically.
        print(f"ERROR: Gemini failed — {e}\nNothing written or posted; the next run will retry this window.")
        sys.exit(1)
    print(f"  Gemini returned {len(tagged_rows)} tagged row(s).")

    final_rows = post_validate_rows(tagged_rows)
    if final_rows:
        if TOPIC_DEDUP_PASS:  # semantic judgment first (can mark UPDATEs) ...
            final_rows = llm_topic_dedup(final_rows, seen_items)
        final_rows = topic_backstop(final_rows, seen_items)  # ... then a deterministic safety net
        final_rows = refill_top7(final_rows)
    print(f"  {len(final_rows)} row(s) remain after validation + topic dedup "
          f"({sum(1 for r in final_rows if _is_top(r))} marked in_top7=true).")

    append_rows(final_rows)
    state["last_success_utc"] = now.isoformat()

    if post_enabled:
        main_text, thread_text = format_slack_bulletin(final_rows)
        if post_to_slack(main_text, thread_text):
            print("  Posted to Slack.")
            state["last_complete_date"] = today
        else:
            print("  WARNING: Slack post failed — bulletin saved in state and will be re-posted next run.")
            state["pending_post"] = {"main": main_text, "thread": thread_text, "date": today}
    else:
        print("  POST_TO_SLACK not set to 'true' — skipping Slack post "
              "(data still written to ALERTS_CSV_PATH).")
        state["last_complete_date"] = today
    save_state(state)


if __name__ == "__main__":
    main()
