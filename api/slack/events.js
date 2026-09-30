export async function POST(request) {
  // Raw body is required for signature verification. Never JSON.parse before verifying.
  const rawBody = await request.text();

  let payload;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return new Response('bad request', { status: 400 });
  }

  // 🔴 FIX: Handle challenge FIRST. Slack verification doesn't always sign this correctly.
  if (payload.type === 'url_verification') {
    return Response.json({ challenge: payload.challenge });
  }

  // 🔒 Verify signature ONLY for actual Slack events (like event_callback)
  const valid = verifySlackSignature({
    signingSecret: process.env.SLACK_SIGNING_SECRET,
    timestamp: request.headers.get('x-slack-request-timestamp'),
    signature: request.headers.get('x-slack-signature'),
    rawBody,
  });
  if (!valid) return new Response('invalid signature', { status: 401 });

  if (payload.type === 'event_callback') {
    const event = payload.event || {};
    try {
      if (event.type === 'link_shared') await onLinkShared(event);
      else if (event.type === 'entity_details_requested') await onEntityDetailsRequested(event);
    } catch (err) {
      // Log and still return 200 so Slack does not retry-storm us.
      console.error(`Slack handler error (${event.type}):`, err?.data || err);
    }
  }

  return new Response('', { status: 200 });
}
