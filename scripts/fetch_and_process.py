import os, csv, json, io, re, unicodedata
import requests

TOKEN = os.environ["SLACK_BOT_TOKEN"]
CHANNEL = os.environ["SLACK_CHANNEL_ID"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

FIELDNAMES = ["date", "headline", "url", "categories", "summary", "vibe", "in_top7", "source", "theme", "countries"]


def normalize_vibe(raw):
    """Canonicalise a vibe cell to exactly 'positive' / 'neutral' / 'negative'.

    A plain `.strip().lower()` equality check is fragile: Slack and LLM-
    generated text can carry invisible Unicode "format" characters (zero-
    width space/joiner, BOM, soft hyphen...) that survive an ordinary
    strip() and silently break an exact match, so a genuinely-positive
    item quietly falls through to the neutral bucket with no error
    anywhere. This strips those out first, then matches on a 3-letter
    prefix so 'Positive', 'positive ', 'pos' etc. all resolve the same
    way. Returns '' — not a default — when nothing recognisable is
    found, so the caller can log it instead of miscounting it.
    """
    if not raw:
        return ""
    cleaned = "".join(ch for ch in raw if unicodedata.category(ch) != "Cf")
    cleaned = cleaned.strip().lower()
    for canon in ("positive", "negative", "neutral"):
        if cleaned.startswith(canon[:3]):
            return canon
    return ""


URL_RE = re.compile(r"^https?://[^\s<>|]+$")
KNOWN_BUCKETS = {"India Specific", "Destination Countries", "Competitor Countries",
                  "Demographics & Fertility", "Global & Multilateral"}


def clean_url(raw):
    """Best-effort recovery of a url cell, with a hard reject if it still
    looks wrong afterwards.

    Slack auto-links bare URLs in posted text — even inside code fences —
    and Cowork itself sometimes echoes that same <url|label> markup back
    into the CSV cell it's writing. In the simple case that's just
    `<https://example.com>`, harmless to unwrap. But it can go further and
    swallow a second copy of the URL plus the start of the next field into
    a garbled `<url|junk>` span, which then shifts every column after it
    on that row (categories/summary/vibe/etc all land one field over).

    This strips simple `<...>` wrapping, then validates what's left is a
    single clean URL with no leftover angle brackets, pipes or whitespace.
    Returns '' — not a best guess — when the cell doesn't check out, so
    the caller can reject the whole row instead of silently ingesting one
    with misaligned columns.
    """
    s = (raw or "").strip()
    m = re.match(r"^<(https?://[^\s<>|]+)", s)
    if m:
        s = m.group(1)
    return s if URL_RE.match(s) else ""


def categories_look_valid(raw):
    """Loose sanity check, not a hard gate: every '|'-separated token
    should be one of the five known bucket names. A blank value is fine
    (older rows predate this column). Mismatches are logged, not
    rejected, since this alone is weaker evidence of corruption than a
    broken url."""
    raw = (raw or "").strip()
    if not raw:
        return True
    return all(tok.strip() in KNOWN_BUCKETS for tok in raw.split("|"))


NON_URL_FIELDS = ("headline", "categories", "summary", "vibe", "in_top7", "source", "theme", "countries")


def row_is_structurally_sane(row):
    """Catches a worse variant of the same Slack/Cowork corruption than
    clean_url() can: sometimes the url cell itself parses perfectly
    clean, but an entire extra garbage field gets inserted right after
    it — so every field from categories onward silently shifts one
    column left. A shifted vibe/theme/source can still look like a
    plausible word (that's exactly how a real 'positive' item quietly
    became 'neutral' before), so per-field validity checks alone won't
    catch it. Two structural signals do, reliably:
      - in_top7 must be exactly 'true' or 'false' (case-insensitive) —
        a shifted row ends up with something like 'negative' or a
        source name there instead, which can never happen legitimately.
      - none of the non-url fields should ever contain a bare URL —
        a shifted row ends up with the duplicated/garbled link text
        sitting in categories or summary instead of in url.
    Returns (True, "") if the row looks structurally sound, else
    (False, reason) for logging.
    """
    top7 = (row.get("in_top7") or "").strip().lower()
    if top7 not in ("true", "false"):
        return False, f"in_top7={row.get('in_top7')!r} is not true/false"
    for field in NON_URL_FIELDS:
        if "http" in (row.get(field) or "").lower():
            return False, f"{field} unexpectedly contains a URL fragment"
    return True, ""


# Slack's mrkdwn has no concept of a fenced-code "language" tag (unlike GitHub
# Markdown), so a message posted as ```csv ... ``` and one posted as ``` ... ```
# are indistinguishable once they hit the API — Cowork may or may not include
# the "csv" tag literally. Anchor on the actual header row instead of the tag.
CSV_FENCE_RE = re.compile(
    r"```(?:csv)?\s*\n?"
    r"(date,headline,url,categories,summary,vibe,in_top7,source.*?)"
    r"\s*```",
    re.DOTALL | re.IGNORECASE,
)


def get_all_csv_blocks(limit=50):
    """Return every CSV block found in the recent channel history (newest first).

    Returns all matches, not just the latest one — if the workflow has been
    silently failing for several days, there can be a backlog of unprocessed
    blocks sitting in the channel, and only picking the newest would silently
    drop the rest.
    """
    resp = requests.get(
        "https://slack.com/api/conversations.history",
        headers=HEADERS,
        params={"channel": CHANNEL, "limit": limit},
    ).json()

    if not resp.get("ok"):
        raise RuntimeError(
            f"Slack API error: {resp.get('error')} "
            f"(check bot token scope, channel ID, and that the bot is invited to the channel)"
        )

    blocks = []
    for msg in resp.get("messages", []):
        text = msg.get("text", "")
        m = CSV_FENCE_RE.search(text)
        if m:
            blocks.append(m.group(1))
    return blocks


def load_items_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def normalize_date(d):
    """Coerce any DD-MM-YYYY straggler back to the canonical YYYY-MM-DD."""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
        return d
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", d)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    return d  # unrecognized — leave as-is but consider logging/flagging this case

def save_items_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in FIELDNAMES})


def main():
    items = load_items_csv("data/items.csv")

    # Drop any row already on file that fails the structural sanity check
    # (see row_is_structurally_sane docstring) — these are rows where an
    # earlier run ingested a column-shifted message, so several of a row's
    # fields are actually silently holding the NEXT field's value. There's
    # no reliable way to un-shift a row after the fact (the true summary
    # text is usually gone entirely, not just misplaced), so the safest
    # move is to drop it and log it rather than let it keep skewing every
    # chart on the site. Do this BEFORE building seen_urls, so a corrected
    # repost of the same URL on a later run isn't blocked by the dedup set.
    sane_items, dropped = [], []
    for row in items:
        ok, reason = row_is_structurally_sane(row)
        if ok:
            sane_items.append(row)
        else:
            dropped.append((row.get("date"), row.get("headline", "")[:70], reason))
    if dropped:
        print(f"Dropped {len(dropped)} already-stored row(s) that look column-shifted:")
        for d, h, reason in dropped:
            print(f"  - {d} {h!r} ({reason})")
    items = sane_items

    # Clean up any row already on file whose vibe didn't survive the old
    # exact-match check cleanly (see normalize_vibe docstring) — items.csv
    # is read directly by feed.html/analysis.html, so this keeps the file
    # itself correct, not just the daily_summary.json aggregate.
    for row in items:
        vibe = normalize_vibe(row.get("vibe") or "")
        if vibe and vibe != row.get("vibe"):
            print(f"Fixed stored vibe on {row.get('date')} {row.get('headline','')[:70]!r}: "
                  f"{row.get('vibe')!r} -> {vibe!r}")
            row["vibe"] = vibe
        elif not vibe:
            row["vibe"] = "neutral"

    seen_urls = {row["url"] for row in items if row.get("url")}

    blocks = get_all_csv_blocks()
    added = 0
    if blocks:
        for csv_text in blocks:
            for row in csv.DictReader(io.StringIO(csv_text)):
                url = clean_url(row.get("url") or "")
                if not url:
                    print(f"WARNING: could not extract a clean URL for "
                          f"{row.get('date', '?')} {(row.get('headline') or '')[:70]!r} — "
                          f"row skipped. This usually means Slack (or Cowork echoing Slack's "
                          f"own link markup) mangled the url cell; the raw value was "
                          f"{(row.get('url') or '')[:120]!r}. Check #mobility-news-dump and "
                          f"add this item manually if it's genuine.")
                    continue
                if url in seen_urls:
                    continue  # dedup — safe to re-run this script any time
                clean = {k: (row.get(k) or "").strip() for k in FIELDNAMES}
                clean["url"] = url
                sane, reason = row_is_structurally_sane(clean)
                if not sane:
                    print(f"WARNING: row for {clean['date']} {clean['headline'][:70]!r} "
                          f"looks column-shifted ({reason}) — skipped rather than ingested "
                          f"wrong. Check #mobility-news-dump and add this item manually if genuine.")
                    continue
                if not categories_look_valid(clean["categories"]):
                    print(f"WARNING: unrecognised categories {clean['categories']!r} on "
                          f"{clean['date']} {clean['headline'][:70]!r} — kept as-is, but this "
                          f"can also be a symptom of the same column-shift bug. Worth checking.")
                vibe = normalize_vibe(clean["vibe"])
                if not vibe:
                    print(f"WARNING: unrecognised vibe {clean['vibe']!r} on "
                          f"{clean['date']} {clean['headline'][:70]!r} — "
                          f"defaulting to neutral. Check for a stray character in the Slack post.")
                    vibe = "neutral"
                clean["vibe"] = vibe
                items.append(clean)
                seen_urls.add(url)
                added += 1
        print(f"Added {added} new item(s) from {len(blocks)} block(s). Total logged: {len(items)}.")
    else:
        print("No CSV blocks found in recent messages — skipping (gap will show in chart).")

    save_items_csv("data/items.csv", items)

    by_date = {}
    for row in items:
        d = row.get("date", "").strip()
        if not d:
            continue
        # Re-normalise even for rows already on file: older rows written
        # before this fix may still carry an invisible character that was
        # never cleaned up, and this keeps the daily_summary.json counts
        # correct without needing a one-off backfill.
        vibe = normalize_vibe(row.get("vibe") or "") or "neutral"
        bucket = by_date.setdefault(d, {"green": 0, "yellow": 0, "red": 0})
        if vibe == "positive":
            bucket["green"] += 1
        elif vibe == "negative":
            bucket["red"] += 1
        else:
            bucket["yellow"] += 1

    summary = [
        {"date": d, **by_date[d], "total": sum(by_date[d].values())}
        for d in sorted(by_date.keys())
    ]

    os.makedirs("data", exist_ok=True)
    with open("data/daily_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"daily_summary.json rebuilt — {len(summary)} day(s) on file.")


if __name__ == "__main__":
    main()
