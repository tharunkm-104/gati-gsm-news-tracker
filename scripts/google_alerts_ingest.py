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
  4. Batch whatever survives in chunks (with rate-limiting delays) and send
     calls to Gemini for: source quality gating, same-day dedup,
     category/theme/country tagging, India-relevance inference, and ranking.
  5. Append the returned rows to items.csv (same schema as the existing
     pipeline) and sort by date.
"""

import csv
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import feedparser
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ALERTS_CSV_PATH = os.environ.get("ALERTS_CSV_PATH", "data/google_alerts_items.csv")
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-lite")

# Rate-limiting parameters to stay within free-tier limits (15 RPM)
BATCH_SIZE = 12         # Process max 12 items per Gemini call
RATE_LIMIT_DELAY = 5.0  # Pause 5 seconds between batch calls

FIELDNAMES = [
    "date", "headline", "url", "categories", "summary",
    "vibe", "in_top7", "source", "theme", "countries",
]

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


# ---------------------------------------------------------------------------
# STEP 1 — FETCH
# ---------------------------------------------------------------------------

def fetch_all_feeds():
    """Pull every configured feed, return a flat list of raw entries."""
    raw_items = []
    for url in ALL_FEED_URLS:
        parsed = feedparser.parse(url)
        for entry in parsed.entries:
            raw_items.append({
                "title": entry.get("title", "").strip(),
                "url": entry.get("link", "").strip(),
                "source_domain": urlparse(entry.get("link", "")).netloc,
                "published": entry.get("published", "") or entry.get("updated", ""),
            })
    return raw_items


# ---------------------------------------------------------------------------
# STEP 2 — DATE WINDOW (deterministic, no LLM)
# ---------------------------------------------------------------------------

def in_date_window(published_str, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        parsed_struct = feedparser._parse_date(published_str)
        if parsed_struct is None:
            return False
        pub_dt = datetime(*parsed_struct[:6], tzinfo=timezone.utc)
    except Exception:
        return False

    if now.weekday() == 0:  # Monday
        window_start = (now - timedelta(days=now.weekday() + 1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)  # back up to Saturday 00:00
        return window_start <= pub_dt <= now
    else:
        window_start = now - timedelta(hours=24)
        return window_start <= pub_dt <= now


# ---------------------------------------------------------------------------
# STEP 3 — DEDUP AGAINST LAST 7 DAYS
# ---------------------------------------------------------------------------

def load_seen_set(days=7):
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
# STEP 4 — GEMINI TAGGING WITH RETRIES & CHUNKING
# ---------------------------------------------------------------------------

GEMINI_PROMPT_TEMPLATE = """You are a research analyst producing a daily migration/labour-mobility news
bulletin for GATI Foundation, an Indian organisation working on India's
overseas employment ecosystem.

You will receive a JSON array of candidate items pulled from Google Alerts
RSS feeds (title, url, source_domain, published). These have already been
filtered to the correct date window and are NOT yet checked for source
quality, duplication, or already-posted status. Your job has five parts.

PART 1 -- SOURCE QUALITY GATE
Drop any item whose source is not a genuine newsroom, wire service,
government/multilateral body, or established trade/specialist publication.
Exclude content-aggregator sites, SEO/affiliate "visa news" blogs, and
immigration-consultancy lead-gen sites.

PART 2 -- SAME-BATCH DEDUPLICATION
If multiple surviving items describe the same underlying event, keep only
the single best-sourced item and discard the rest.

PART 3 -- ALREADY-POSTED CHECK
Given a list of URLs and headline stems already posted in the last 7 days.
If an item matches one, drop it UNLESS it contains a genuine material update.

PART 4 -- CATEGORISE, INFER RELEVANCE, AND TAG
For every item that survives:
(a) categories -- pipe-separated: India Specific | Destination Countries |
Competitor Countries | Demographics & Fertility | Global & Multilateral.
(b) theme -- exactly one: Enforcement and Crisis | Students and Education |
Bilateral Deals and Trade | Remittances and Diaspora | Demographics and Workforce |
Skills and Talent | Labour and Workers | Visas and Work Permits | Other.
(c) countries -- pipe-separated, max 4, plain English names.
(d) vibe -- negative | neutral | positive.
(e) summary -- one sentence: what happened and why it matters for India.

PART 5 -- RANK
Mark top items in_top7=true, all others in_top7=false.

OUTPUT
Return ONLY a JSON array of objects matching this schema:
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
  "countries": "pipe-separated, max 4"
}}]

INPUT ITEMS:
{items_json}

ALREADY POSTED (last 7 days):
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
        },
        "required": FIELDNAMES,
    },
}


def clean_json_text(raw_text: str) -> str:
    """Strip markdown code block fences if present in Gemini output."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.MULTILINE)
    return cleaned.strip()


def tag_items_gemini(candidate_items, seen_urls, seen_stems):
    if not candidate_items:
        return []

    # Configure Gemini Client with automatic Exponential Backoff for 429/503 errors
    client = genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=types.HttpOptions(
            retry_options=types.HttpRetryOptions(
                attempts=5,
                initial_delay=2.0,
                max_delay=30.0,
                exp_base=2.0,
                http_status_codes=[429, 500, 503, 504],
            )
        )
    )

    all_valid_rows = []
    
    # Process candidates in smaller chunks to avoid exceeding TPM and output token limits
    chunked_candidates = [
        candidate_items[i:i + BATCH_SIZE]
        for i in range(0, len(candidate_items), BATCH_SIZE)
    ]

    print(f"  Processing {len(candidate_items)} candidates across {len(chunked_candidates)} batch(es)...")

    for idx, batch in enumerate(chunked_candidates, start=1):
        prompt = GEMINI_PROMPT_TEMPLATE.format(
            items_json=json.dumps(batch, ensure_ascii=False),
            seen_set_json=json.dumps(
                {"urls": sorted(seen_urls), "headline_stems": sorted(seen_stems)},
                ensure_ascii=False,
            ),
            today_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        )

        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=RESPONSE_SCHEMA,
                ),
            )

            raw_text = clean_json_text(response.text if hasattr(response, "text") else "")
            rows = json.loads(raw_text)

            for row in rows:
                if all(k in row for k in FIELDNAMES):
                    all_valid_rows.append(row)
                else:
                    print(f"Dropping malformed row: {row}")

        except Exception as e:
            print(f"Error processing batch {idx}/{len(chunked_candidates)}: {e}")

        # Rate Limiting Delay between batches to keep Requests Per Minute (RPM) low
        if idx < len(chunked_candidates):
            time.sleep(RATE_LIMIT_DELAY)

    return all_valid_rows


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

    print(f"Appended {len(rows)} new row(s). {ALERTS_CSV_PATH} now has {len(all_rows)} total.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("Fetching Google Alerts RSS feeds...")
    raw_items = fetch_all_feeds()
    print(f"  {len(raw_items)} raw entries across {len(ALL_FEED_URLS)} feeds.")

    windowed = [item for item in raw_items if in_date_window(item["published"])]
    print(f"  {len(windowed)} entries in today's date window.")

    seen_urls, seen_stems = load_seen_set(days=7)
    print(f"  {len(seen_urls)} URLs / {len(seen_stems)} headline stems seen in last 7 days.")

    candidates = [item for item in windowed if item["url"] not in seen_urls]
    print(f"  {len(candidates)} candidates after exact-URL pre-filter.")

    tagged_rows = tag_items_gemini(candidates, seen_urls, seen_stems)
    print(f"  Gemini returned {len(tagged_rows)} tagged row(s).")

    append_rows(tagged_rows)


if __name__ == "__main__":
    main()