"""
google_alerts_ingest.py

Second, independent ingestion path for the GATI mobility news pipeline —
runs alongside (not replacing, for now) the existing Cowork/Slack-fed
pipeline that writes to items.csv.

Flow:
  1. Pull all configured Google Alerts RSS feeds.
  2. Filter to the correct date window in plain Python (last 24h, or
     Sat+Sun on a Monday run) — no LLM needed for this.
  3. Drop anything already posted in the last 7 days (reads items.csv).
  4. Batch whatever survives and send ONE call to Gemini for: source
     quality gating, same-day dedup, category/theme/country tagging,
     India-relevance inference, and ranking.
  5. Append the returned rows to items.csv (same schema as the existing
     pipeline) and sort by date.

Wire-up notes for you:
  - Requires env vars: GEMINI_API_KEY
  - Requires: pip install feedparser google-genai   (confirm package name
    at pypi.org before running — Google has renamed this SDK before, per
    your own note)
  - ISOLATION: this script reads and writes ONLY data/google_alerts_items.csv
    by default — a completely separate file from data/items.csv, which
    stays owned by your existing daily-update.yml / fetch_and_process.py
    pipeline. This script never opens, reads, or writes items.csv. Its
    7-day "already posted" dedup check is against its OWN file only, not
    against items.csv. The two pipelines are fully independent until you
    decide to merge them — override ALERTS_CSV_PATH if you ever want to
    point it elsewhere.
  - This script does NOT post to Slack and does NOT touch the tracker's
    data/ files. It only appends tagged rows to google_alerts_items.csv,
    so you can inspect Gemini's output for as long as you like before
    deciding whether/how to feed it into the tracker.
  - The Gemini prompt lives in this file as GEMINI_PROMPT_TEMPLATE below.
    If you'd rather version it independently of the code, move that
    string into prompts/gemini_tagging_prompt.txt and load it with
    open(...).read() instead — functionally identical, just easier to
    diff/edit without touching the script.
"""

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs, unquote
from difflib import SequenceMatcher

import feedparser
import requests
import trafilatura
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# Isolated from the tracker's own data/items.csv on purpose — see module
# docstring. Override only if you deliberately want to point this
# somewhere else.
ALERTS_CSV_PATH = os.environ.get("ALERTS_CSV_PATH", "data/google_alerts_items.csv")
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
# Verify current free-tier-eligible model name before running — these get
# renamed/re-limited through 2026. Check ai.google.dev/gemini-api/docs/models.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

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


def fetch_all_feeds():
    """Pull every configured feed, return a flat list of raw, cleaned entries."""
    raw_items = []
    for url in ALL_FEED_URLS:
        parsed = feedparser.parse(url)
        for entry in parsed.entries:
            link = clean_and_unwrap_url(entry.get("link", "").strip())
            raw_items.append({
                "title": entry.get("title", "").strip(),
                "url": link,
                "source_domain": urlparse(link).netloc,
                "published": _entry_published_iso(entry),
            })
    return raw_items


def fetch_article_text(url, timeout=8, max_chars=4000):
    """
    Best-effort full-article-text fetch, so Gemini reasons from the actual
    article rather than just a headline. Failures are expected and fine —
    paywalls, bot-blocking, timeouts, non-HTML responses — fall back to
    title-only for that item rather than failing the run.
    Requires `requests` and `trafilatura` (see requirements-alerts.txt).
    """
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
    and avoid hammering publishers if a feed has an unusually large batch
    on a given day — trim the candidate list before calling this if you
    want a different cap than the default.
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


# ---------------------------------------------------------------------------
# STEP 2 — DATE WINDOW (deterministic, no LLM)
# ---------------------------------------------------------------------------

def in_date_window(published_iso, now=None):
    """
    Default: last WINDOW_HOURS (24) hours. On a Monday run, widen to cover
    Saturday 00:00 UTC onward so weekend news isn't lost. Set the
    WINDOW_HOURS env var (e.g. 168 for 7 days) for a one-off backfill —
    note Google Alerts feeds only hold what they've collected since each
    alert was created, so a wider window can't return older items than
    the feeds actually contain.
    """
    now = now or datetime.now(timezone.utc)
    if not published_iso:
        return False
    try:
        pub_dt = datetime.fromisoformat(published_iso)
    except ValueError:
        return False

    override = os.environ.get("WINDOW_HOURS")
    if override:
        return now - timedelta(hours=int(override)) <= pub_dt <= now

    if now.weekday() == 0:  # Monday: back to Saturday 00:00 UTC
        window_start = (now - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return window_start <= pub_dt <= now
    return now - timedelta(hours=24) <= pub_dt <= now


# ---------------------------------------------------------------------------
# STEP 3 — DEDUP AGAINST LAST 7 DAYS (reads existing items.csv)
# ---------------------------------------------------------------------------

def load_seen_set(days=7):
    """Return (seen_urls: set, seen_headline_stems: set) from items.csv."""
    seen_urls = set()
    seen_stems = set()
    if not os.path.exists(ALERTS_CSV_PATH):
        return seen_urls, seen_stems

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
    with open(ALERTS_CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                row_date = datetime.strptime(row["date"], "%Y-%m-%d").date()
            except (ValueError, KeyError):
                continue
            if row_date >= cutoff:
                seen_urls.add(row.get("url", ""))
                seen_stems.add(row.get("headline", "").strip().lower())
    return seen_urls, seen_stems


# ---------------------------------------------------------------------------
# STEP 4 — GEMINI TAGGING (quality gate, same-day dedup, tagging, ranking)
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
You are given a list of URLs and headline stems already posted in the last
7 days. If a surviving item matches one of these, drop it UNLESS it
contains a genuine material update (a new number, a signed deal, a
confirmed effective date, a court ruling) -- in that case keep it, prefix
the headline with "UPDATE:", and make the summary about what changed.

PART 4 -- CATEGORISE, INFER RELEVANCE, AND TAG
For every item that survives Parts 1-3, read the full title/content and:

(a) categories -- pipe-separated, containing ONLY values from this exact
set (copy them verbatim, nothing else -- never put a theme name, a country
name, or any other string in this field): India Specific | Destination
Countries | Competitor Countries | Demographics & Fertility | Global &
Multilateral.
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
downstream. This field is for internal analysis only and is never shown
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


def tag_items_gemini(candidate_items, seen_urls, seen_stems):
    if not candidate_items:
        return []

    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = load_prompt_template().format(
        items_json=json.dumps(candidate_items, ensure_ascii=False),
        seen_set_json=json.dumps(
            {"urls": sorted(seen_urls), "headline_stems": sorted(seen_stems)},
            ensure_ascii=False,
        ),
        today_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,
        ),
    )

    try:
        rows = json.loads(response.text)
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Gemini response did not parse as JSON: {e}")
        print(response.text if hasattr(response, "text") else response)
        return []

    # Basic field-presence validation; drop malformed rows rather than crash.
    valid_rows = []
    for row in rows:
        if all(k in row for k in FIELDNAMES):
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


def sanitize_categories(cat_string):
    """Strip anything that isn't one of the five valid category names —
    catches theme/country strings Gemini occasionally leaks into this
    field despite the schema being typed as STRING, not an enum."""
    parts = [p.strip() for p in (cat_string or "").split("|")]
    valid = [p for p in parts if p in VALID_CATEGORIES]
    return " | ".join(valid) if valid else "Global & Multilateral"


def sanitize_theme(theme_string):
    theme_string = (theme_string or "").strip()
    return theme_string if theme_string in VALID_THEMES else "Other"


def row_priority(row):
    """Higher = more important, for top-7 tie-breaking."""
    cats = [c.strip() for c in row.get("categories", "").split("|")]
    return max((CATEGORY_PRIORITY.get(c, 0) for c in cats), default=0)


def post_validate_rows(rows):
    """
    Deterministic cleanup pass, independent of anything Gemini claimed:
      - re-clean/re-check every URL (denylist, live-blog pattern)
      - sanitize categories/theme against the fixed value sets
      - drop exact-duplicate URLs within this batch
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
        if url in seen_urls_this_batch:
            print(f"  [post-validate] dropped duplicate URL within batch: {url}")
            continue
        seen_urls_this_batch.add(url)

        row["url"] = url
        row["categories"] = sanitize_categories(row.get("categories", ""))
        row["theme"] = sanitize_theme(row.get("theme", ""))
        cleaned.append(row)

    # Heuristic same-event backstop: Gemini's own Part 2 dedup is a semantic
    # judgment call and can miss pairs (as it did on the Australia migration
    # story last run). This is NOT a substitute for Part 2 — it only catches
    # the narrow case of near-identical headlines sharing a country, which
    # is a decent proxy for "same underlying event" without needing another
    # LLM call. Keeps the higher-priority (or more India-relevant) item.
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
    marked_true = [r for r in cleaned if r.get("in_top7") in (True, "True", "true")]
    if len(marked_true) > 7:
        print(f"  [post-validate] Gemini marked {len(marked_true)} as in_top7=true; "
              f"capping to 7 by priority.")
        marked_true.sort(key=row_priority, reverse=True)
        keep_urls = {r["url"] for r in marked_true[:7]}
        for r in cleaned:
            r["in_top7"] = r["url"] in keep_urls

    return cleaned


# ---------------------------------------------------------------------------
# STEP 5 — APPEND TO items.csv, SORTED BY DATE
# ---------------------------------------------------------------------------

def append_rows(rows):
    if not rows:
        print("No rows to append.")
        return

    file_exists = os.path.exists(ALERTS_CSV_PATH)
    existing_rows = []
    if file_exists:
        with open(ALERTS_CSV_PATH, newline="", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))

    all_rows = existing_rows + rows
    all_rows.sort(key=lambda r: r.get("date", ""))

    os.makedirs(os.path.dirname(ALERTS_CSV_PATH) or ".", exist_ok=True)
    with open(ALERTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in FIELDNAMES})

    print(f"Appended {len(rows)} new row(s). items.csv now has {len(all_rows)} total.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

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
    top_rows = [r for r in rows if r.get("in_top7") in (True, "True", "true")]
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
    Posts the main bulletin, then the remaining items as a thread reply —
    same two-message shape as the existing Cowork-fed pipeline, but posted
    directly via the Slack Web API (no relay channel needed — see the
    chat discussion on why Gemini doesn't need the Cowork-style workaround).

    Requires:
      SLACK_BOT_TOKEN   — a Slack app's Bot User OAuth Token, scope chat:write
      SLACK_CHANNEL_ID  — the target channel's ID (not its name)
    Gated behind POST_TO_SLACK=true so nothing posts unless you opt in.
    """
    token = os.environ["SLACK_BOT_TOKEN"]
    channel = os.environ["SLACK_CHANNEL_ID"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    main_resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": channel, "text": main_text, "mrkdwn": True},
        timeout=10,
    ).json()
    if not main_resp.get("ok"):
        print(f"  [slack] main post failed: {main_resp}")
        return

    thread_ts = main_resp["ts"]
    thread_resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={
            "channel": channel, "text": thread_text, "mrkdwn": True,
            "thread_ts": thread_ts,
        },
        timeout=10,
    ).json()
    if not thread_resp.get("ok"):
        print(f"  [slack] thread reply failed: {thread_resp}")


def main():
    print("Fetching Google Alerts RSS feeds...")
    raw_items = fetch_all_feeds()
    print(f"  {len(raw_items)} raw entries across {len(ALL_FEED_URLS)} feeds.")

    undated = sum(1 for item in raw_items if not item["published"])
    dated = sorted(item["published"] for item in raw_items if item["published"])
    if dated:
        print(f"  feed date range: {dated[0]} -> {dated[-1]} ({undated} undated)")
    elif raw_items:
        print(f"  WARNING: none of the {len(raw_items)} entries carry a parseable date.")

    windowed = [item for item in raw_items if in_date_window(item["published"])]
    print(f"  {len(windowed)} entries in today's date window.")

    gated = python_prefilter(windowed)
    print(f"  {len(gated)} entries survive the Python domain/live-blog gate.")

    seen_urls, seen_stems = load_seen_set(days=7)
    print(f"  {len(seen_urls)} URLs / {len(seen_stems)} headline stems seen in last 7 days.")

    # Cheap pre-filter: drop exact URL matches before even sending to Gemini
    # (saves tokens; Gemini still gets the seen-set for near-duplicate/UPDATE
    # judgment on headline-level matches it can't catch by URL alone).
    candidates = [item for item in gated if item["url"] not in seen_urls]
    print(f"  {len(candidates)} candidates after exact-URL pre-filter.")

    # Best-effort full-text fetch so Gemini reasons from the real article,
    # not just the RSS title/snippet. Cap via MAX_ARTICLE_FETCHES if a
    # given day's batch is unusually large and you want to bound runtime.
    max_fetches = os.environ.get("MAX_ARTICLE_FETCHES")
    candidates = enrich_with_article_text(
        candidates, max_items=int(max_fetches) if max_fetches else None
    )
    print(f"  sending {len(candidates)} candidates to Gemini.")

    tagged_rows = tag_items_gemini(candidates, seen_urls, seen_stems)
    print(f"  Gemini returned {len(tagged_rows)} tagged row(s).")

    final_rows = post_validate_rows(tagged_rows)
    print(f"  {len(final_rows)} row(s) remain after deterministic post-validation "
          f"({sum(1 for r in final_rows if r.get('in_top7'))} marked in_top7=true).")

    append_rows(final_rows)

    if os.environ.get("POST_TO_SLACK", "false").lower() == "true":
        main_text, thread_text = format_slack_bulletin(final_rows)
        post_to_slack(main_text, thread_text)
        print("  Posted to Slack.")
    else:
        print("  POST_TO_SLACK not set to 'true' — skipping Slack post "
              "(data still written to ALERTS_CSV_PATH).")


if __name__ == "__main__":
    main()
