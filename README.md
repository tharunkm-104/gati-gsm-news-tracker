# gsm-news-tracker

GATI Foundation's internal tracker for daily labour-mobility and migration news. Cowork runs a daily search, tags each item, and posts it to Slack; this site reads that log and displays it.

## Pages
| Page | What it is |
|---|---|
| `index.html` | Landing page — daily volume by sentiment. |
| `feed.html` | Every logged item as a card (headline, summary, source, date, tags), grouped by date, country or theme. |
| `analysis.html` | Filterable breakdowns — by country, theme, focus bucket and source — plus a country × theme heatmap. |

`feed.html` and `analysis.html` share one filter sidebar: period, sentiment, theme, focus bucket, country, source, main-bulletin-only, hide repeats, hide excluded sources. Filter state lives in the URL.

## Data
`data/items.csv` (rebuilt daily by the GitHub Action) columns:
`date, headline, url, categories, summary, vibe, in_top7, source, theme, countries`

- `tracks` — `positive developments` / `neutral developments` / `negative developments`, judged against GATI's thesis (India's workforce as a solution to developed-market labour shortages), not the article's own tone
- `categories` — briefing bucket(s), pipe-separated: India Specific / Destination Countries / Competitor Countries / Demographics & Fertility / Global & Multilateral
- `theme` — one of: Visas and Work Permits, Skills and Talent, Labour and Workers, Students and Education, Bilateral Deals and Trade, Enforcement and Crisis, Remittances and Diaspora, Demographics and Workforce, Other
- `countries` — pipe-separated; `India` only for India-only developments; `Global` when none applies

`scripts/fetch_and_process.py` reads the CSV Cowork posts to `#mobility-news-dump`, validates each row, and rebuilds `data/items.csv` and `data/daily_summary.json`. The prompt Cowork runs from is in `prompt/daily_briefing_prompt_v2.txt`.
