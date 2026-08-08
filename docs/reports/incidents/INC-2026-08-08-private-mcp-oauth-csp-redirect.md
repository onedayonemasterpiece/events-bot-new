# INC-2026-08-08 Private MCP OAuth redirect blocked by CSP

Status: open
Severity: sev2
Service: Private Events MCP OAuth browser authorization
Opened: 2026-08-08
Closed: —
Owners: events-bot production / Private Events MCP
Related incidents: —
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The production authorization form accepted the operator bootstrap token and
created an authorization code, but Chrome did not follow the cross-origin 302
callback to ChatGPT. The authorization page sent
`Content-Security-Policy: form-action 'self'`, so the browser blocked the
redirect chain after the form POST.

## User / Business Impact

- ChatGPT could discover the MCP and display the Events Bot authorization page.
- Clicking **Предоставить доступ** appeared to do nothing, so a new ChatGPT MCP
  connection could not complete.
- The existing bot, `/healthz`, Codex MCP access and event SQLite remained
  healthy; the impact was limited to browser OAuth completion.

## Detection

- The operator reported the inert button and supplied a Chrome DevTools CSP
  console screenshot.
- Production OAuth audit independently showed five consecutive
  `authorize/granted` rows, proving that the token and POST handler succeeded
  and that failure occurred at the browser redirect boundary.
- Existing aiohttp tests followed the 302 manually with redirects disabled and
  therefore did not exercise browser CSP enforcement.

## Timeline

- 2026-08-08T18:58:47Z–18:59:36Z — five operator attempts reached production
  and were recorded as `authorize/granted`.
- 2026-08-08T19:00Z — operator reported the blocked connection with DevTools
  evidence.
- 2026-08-08T19:02Z — new regression assertions reproduced the missing allowed
  callback origins for both ChatGPT and Codex authorization pages.
- 2026-08-08T19:03Z — minimal dynamic callback-origin CSP fix passed the two
  OAuth round-trip tests locally.

## Root Cause

1. The authorization page permitted form submission only to `'self'`.
2. Successful authorization returns a 302 to the already validated client
   callback on another origin (`https://chatgpt.com` or Codex loopback).
3. Chrome applies `form-action` to the form navigation redirect chain and
   blocked that callback even though the server had already granted access.

## Contributing Factors

- The existing test asserted the server-side 302 but did not assert that CSP
  allowed the validated callback origin.
- Non-browser smoke clients do not enforce Content Security Policy.
- The initial hotfix draft derived the CSP source from raw `netloc`; independent
  review found that the legacy ChatGPT redirect validator accepted userinfo and
  alternate ports. The final fix therefore rejects non-canonical authority and
  query components and emits a canonical client-specific CSP origin.

## Automation Contract

### Treat as regression guard when

- changing Private Events MCP OAuth authorize HTML, CSP, redirect validation,
  callback allowlists, or ChatGPT/Codex client registration;
- changing browser security headers on the authorization surface.

### Affected surfaces

- `private_events_mcp/oauth.py::handle_authorize_get`;
- ChatGPT HTTPS callback and Codex exact loopback callback;
- production OAuth browser smoke and credential rotation.

### Mandatory checks before closure or deploy

- authorization-page CSP contains `'self'` plus only the origin derived from
  the already validated client redirect URI;
- ChatGPT confidential and Codex public OAuth+PKCE round trips pass;
- invalid callback hosts/paths remain rejected;
- full `tests/test_private_events_mcp_*.py`, compileall and `git diff --check`
  pass;
- real Chrome authorization follows the 302 back to ChatGPT after deployment;
- `/healthz`, exact in-container SHA, SQLite `quick_check`, DB-unchanged and
  secret-redaction checks remain green;
- rotate the private path and credentials because the incident screenshot
  exposed the former path.

### Required evidence

- PR/head and independent review receipt;
- merged `origin/main` SHA and Fly release;
- production OAuth audit/browser receipt without codes or tokens;
- sanitized health, DB and log evidence.

## Immediate Mitigation

The affected page remains available for diagnosis, but the operator should not
retry until the CSP hotfix and credential rotation are deployed.

## Corrective Actions

- Add the exact validated redirect origin to the authorization page's
  `form-action` directive while retaining `'self'`, `default-src 'none'`,
  `base-uri 'none'` and `frame-ancestors 'none'`.
- Canonicalize ChatGPT to `https://chatgpt.com`, reject userinfo, alternate
  ports and callback queries, and never reflect raw authority text into CSP.
- Add ChatGPT and Codex regression assertions for their distinct callback
  origins.

## Follow-up Actions

- [ ] Complete one real ChatGPT browser connection after exact-main deploy.
- [ ] Rotate the bootstrap token again after the operator's first successful
  ChatGPT connection.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: `scripts/deploy_fly_main.sh`
- regression checks: pending full suite and browser acceptance
- post-deploy verification: pending

## Prevention

OAuth browser acceptance now treats CSP callback navigation as part of the
client contract rather than relying only on non-browser 302 assertions.
