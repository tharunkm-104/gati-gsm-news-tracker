# GATI Tracker — Slack Work Objects embed: setup & test plan

Purely additive. Nothing in `index.html`, `data/`, `scripts/` or `.github/` changes.

## What gets added to the repo root

```
api/slack/events.js     # the Slack endpoint (link_shared, entity_details_requested, signature check)
lib/verify.js           # Slack v0 signature verification
lib/entity.js           # the Work Object entity (Content item) + which URLs count as "the dashboard"
package.json            # one dependency: @slack/web-api
vercel.json             # frame-ancestors header + CORS header for /data/*
test/events.test.js     # local tests (npm install && npm test)
slack-app-manifest.yaml # Slack app definition
```

## 1. Create the Slack app (about 5 min)

1. https://api.slack.com/apps → **Create New App → From a manifest** → pick your workspace → paste `slack-app-manifest.yaml`.
2. **Basic Information → App Credentials → Signing Secret**: copy it (you need it in step 2).
3. Sidebar → **Work Object Previews**: confirm the toggle is on and **Content item** is selected. (If the manifest import didn't set it, turn it on and select it yourself.)
4. Same page → **embed domain allow list**: add `gati-mobility-sentiment-tracker.vercel.app`. Slack does not load embeds until this list exists, and silently strips the preview for any domain not on it.
5. Leave **allow-same-origin OFF**. The dashboard is public and has no cookies, so the default opaque origin (`Origin: null`) is the safest option and works with the CORS header in `vercel.json`.

> If Slack refuses the manifest because of `rich_previews`, delete that block, import again, and do step 3 in the UI.
> If it complains about event subscriptions with no Request URL, delete the `event_subscriptions` block, import, and add both bot events in the UI in step 3 below.

## 2. Deploy to Vercel (project already exists)

1. Confirm the Vercel project is connected to this GitHub repo (Project → Settings → Git) and that **Output Directory** is empty/default. The daily GitHub Action commit then triggers a Vercel redeploy, which is how the dashboard data stays fresh.
2. Copy the files above into the repo root, commit, push to the production branch.
3. Vercel → Project → Settings → **Environment Variables** (Production):
   - `SLACK_SIGNING_SECRET` — from step 1.2
   - `SLACK_BOT_TOKEN` — added in step 4 below (starts with `xoxb-`)
   - optional `DASHBOARD_URL` — defaults to `https://gati-mobility-sentiment-tracker.vercel.app/`
4. Redeploy after changing env vars (env changes only apply to new deployments).

## 3. Point Slack at the endpoint

Slack app → **Event Subscriptions** → Enable → Request URL:
`https://gati-mobility-sentiment-tracker.vercel.app/api/slack/events` → wait for **Verified ✓**.
Confirm **Subscribe to bot events** lists `link_shared` and `entity_details_requested`. Save.

## 4. Install and add the token

**Install App → Install to Workspace** → copy **Bot User OAuth Token** → put in Vercel as `SLACK_BOT_TOKEN` → redeploy.

## Scopes and events (verified against Slack docs)

| Need | Value | Source |
|---|---|---|
| Receive `link_shared` | bot scope `links:read` | link_shared event page |
| Call `chat.unfurl` | bot scope `links:write` | chat.unfurl method page |
| `entity_details_requested` event | no scope | event page |
| `entity.presentDetails` | no scope | method page |
| Unfurl domains | `unfurl_domains` (max 5) | manifest / App Unfurl Domains |

## Manual test plan

**A. Before touching Slack**
1. `npm install && npm test` → 7 tests pass (signature checks, unfurl payload, presentDetails payload).
2. After deploy:
   ```
   curl -sI https://gati-mobility-sentiment-tracker.vercel.app/ | grep -i content-security-policy
   # expect: frame-ancestors https://*.slack.com https://*.slack-gov.com https://*.slack-mcps.com
   curl -sI https://gati-mobility-sentiment-tracker.vercel.app/data/daily_summary.json | grep -i access-control-allow-origin
   # expect: access-control-allow-origin: *
   curl -s -o /dev/null -w "%{http_code}\n" -X POST https://gati-mobility-sentiment-tracker.vercel.app/api/slack/events -d '{}'
   # expect: 401  (unsigned requests are rejected)
   ```

**B. In Slack**
1. Use a **public test channel** (private channel: run `/invite @GATI Tracker` first).
2. Post a **new message** containing `https://gati-mobility-sentiment-tracker.vercel.app/`. (Messages posted before the app was installed will not unfurl.)
3. Expect a Work Object card titled "GATI Labour Mobility Sentiment Tracker" within a few seconds.
4. Click the card. The flexpane opens on the right and shows the **live dashboard** (not an image). Click the range toggle buttons: the charts should redraw. If you see "Could not load live data", see troubleshooting.
5. Repeat with the GitHub Pages URL; it should unfurl to the same card and open the Vercel-hosted embed.
6. On the Slack mobile app, open the same message and tap the card (this is what the `*.slack-mcps.com` origin is for).

**Troubleshooting**

| Symptom | Likely cause |
|---|---|
| No card at all | Function not receiving events (Vercel → Logs), `links:write` missing, domain not in `unfurl_domains`, or `SLACK_BOT_TOKEN` unset/old |
| Card shows but flexpane is plain metadata, no dashboard | Embed domain not on the allow list (step 1.4), or entity type not enabled |
| Flexpane blank / refused to connect | `frame-ancestors` header missing (check curl in A.2) |
| Dashboard loads but says "Could not load live data" | `/data/*` missing `Access-Control-Allow-Origin` (sandboxed iframe sends `Origin: null`) |
| Vercel logs show `invalid signature` | Wrong `SLACK_SIGNING_SECRET`, or env var changed without redeploying |
