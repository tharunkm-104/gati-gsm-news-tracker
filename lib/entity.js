// The dashboard as a Slack Work Object ("Content item" entity).

export const CANONICAL_URL =
  process.env.DASHBOARD_URL || 'https://gsm-news-tracker.vercel.app/';

// Hosts whose pasted links should unfurl. Keep in sync with the domains entered in:
//   Slack app settings -> Event Subscriptions -> App Unfurl Domains, and
//   Slack app settings -> Work Object Previews -> Embedded Previews -> Domain Allowlist
// Both of those fields want a bare hostname (no "https://", no trailing slash).
const VERCEL_HOST = new URL(CANONICAL_URL).hostname;

// Optional second host, e.g. if the dashboard is also mirrored on GitHub Pages.
// Set to null and remove the check below if that's no longer the case.
const PAGES_HOST = 'tharunkm-104.github.io';
const PAGES_PATH = '/gati-gsm-news-tracker'; // update this if the repo/Pages path changed too

export const EXTERNAL_REF_ID = 'gati-gsm-news-tracker'; // must never change (related-conversations tracking)

export function isDashboardLink(rawUrl) {
  let u;
  try {
    u = new URL(rawUrl);
  } catch {
    return false;
  }
  if (u.hostname === VERCEL_HOST) return !u.pathname.startsWith('/api/');
  if (PAGES_HOST && u.hostname === PAGES_HOST) return u.pathname.startsWith(PAGES_PATH);
  return false;
}

/**
 * Builds the entity metadata.
 *  - unfurl (chat.unfurl):            pass appUnfurlUrl, no previewUrl  -> declares embed support
 *  - flexpane (entity.presentDetails): pass previewUrl, no appUnfurlUrl   -> Slack loads it in the iframe
 */
export function buildEntity({ appUnfurlUrl, previewUrl } = {}) {
  const fullSizePreview = {
    is_supported: true,
    mime_type: 'application/vnd.slack-embed',
    ...(previewUrl ? { preview_url: previewUrl } : {}),
  };

  const entity = {
    url: CANONICAL_URL,
    external_ref: { id: EXTERNAL_REF_ID },
    entity_type: 'slack#/entities/item',
    entity_payload: {
      attributes: {
        title: { text: 'GATI Labour Mobility News Tracker' },
        display_type: 'Live dashboard',
        product_name: 'GATI Foundation',
        full_size_preview: fullSizePreview,
      },
    },
  };
  if (appUnfurlUrl) entity.app_unfurl_url = appUnfurlUrl;
  return entity;
}
