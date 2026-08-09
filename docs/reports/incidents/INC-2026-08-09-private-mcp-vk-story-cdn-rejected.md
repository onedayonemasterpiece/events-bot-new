# INC-2026-08-09 Private MCP rejected VK story CDN

Status: open
Severity: sev2
Service: Private Events MCP VK story reads
Opened: 2026-08-09
Closed: —
Owners: events-bot production / Private Events MCP
Related incidents: `INC-2026-08-09-private-mcp-missing-bearer-http-200`, `INC-2026-08-08-private-mcp-oauth-csp-redirect`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The first production read-only acceptance run after enabling the image/story
workspace found that `social_content_stories` rejected a valid active VK video
story. VK's fixed `stories.get` method succeeded, but the adapter denied the
provider-returned media URL before minting an opaque media ref because the
strict CDN suffix list did not include the VK video host `*.okcdn.ru`.

## User / Business Impact

- ChatGPT could resolve the configured VK community and see that story reading
  was available, but the actual bounded story-list call failed.
- No write was attempted and no provider credential, native ID, media URL or
  story body was exposed.
- Telegram story reads, event/incident evidence tools, `/healthz`, webhook and
  scheduler remained healthy.

## Detection

- The post-deploy MCP acceptance probe returned a sanitized workspace denial
  for VK `list_stories`.
- A direct call through the same dedicated story-reader transport proved that
  `stories.get(owner_id=...)` succeeded.
- A shape-only production probe printed no URL or content and showed the active
  story's video host was `vkvd740.okcdn.ru`; the adapter then reproduced
  `provider_media_invalid` deterministically.

## Timeline

- 2026-08-09T10:20Z — exact merged main `65926e63...` deployed as Fly release
  v1949 with the reviewed media/story gate enabled.
- 2026-08-09T10:31Z — ChatGPT/Codex evidence smokes, health and Telegram
  story-list checks passed; VK story-list returned a sanitized denial.
- 2026-08-09T10:45Z — fixed-method `stories.get` succeeded independently,
  isolating the failure to adapter media projection rather than credentials or
  VK API permissions.
- 2026-08-09T10:51Z — a metadata-only probe identified the provider host
  `vkvd740.okcdn.ru`; no signed URL, token, native content or query was logged.
- 2026-08-09T10:54Z — a failing regression test reproduced rejection by both
  the adapter URL validator and secure media transport boundary.

## Root Cause

1. VK returned an active video story whose official playback files were hosted
   on `vkvd740.okcdn.ru`.
2. The new strict story-media validators allowed VK/UserAPI domains but omitted
   the VK-operated `okcdn.ru` video CDN used by the real response.
3. The adapter validates media hosts before minting an opaque ref, so the
   otherwise valid story page failed closed as `provider_media_invalid`.

## Contributing Factors

- Unit fixtures covered official nested VK 5.199 story shapes but used only a
  `userapi.com` image URL and a synthetic `userapi.com` video URL.
- The nonmutating pre-deploy tests could not observe the production community's
  current provider-selected CDN hostname.

## Automation Contract

### Treat as regression guard when

- changing VK story read projection, VK media hostname validation, secure
  provider media retrieval, story fixtures or media/story production smoke.

### Affected surfaces

- `private_events_mcp_vk_adapter.py` story media projection;
- `private_events_mcp_vk_transport.py` secure provider media boundary;
- `social_content_stories` for VK targets;
- production VK story-reader role and acceptance smoke.

### Mandatory checks before closure or deploy

- failing-before/passing-after test for an observed `vkvd*.okcdn.ru` story URL;
- sibling/look-alike host, HTTP and userinfo controls remain rejected;
- complete Private Events MCP test glob, compileall, Ruff and diff-check pass;
- exact existing ChatGPT endpoint/client/resource remains unchanged and Codex
  still lists exactly seven evidence tools with no social surface;
- production `stories.get` and MCP `social_content_stories` both succeed for the
  same community without logging or returning provider URLs;
- `/healthz`, SQLite `quick_check`, missing/invalid-bearer challenges, webhook
  and scheduler regression checks remain green after exact-main deploy.

### Required evidence

- regression test receipt and exact reviewed PR head;
- merged and deployed main SHA, Fly release and in-container SHA;
- sanitized provider-host probe and post-deploy MCP story-list receipt;
- confirmation that no social mutation/provider upload occurred.

## Immediate Mitigation

No retry or mutation was performed. The serving process remains healthy; only
the failing VK story-read acceptance path is being changed.

## Corrective Actions

- Add only the observed provider-owned `*.okcdn.ru` suffix to both strict VK
  media validators.
- Preserve HTTPS, canonical authority, public-DNS, no-userinfo, no-fragment,
  no-redirect, byte-limit and provider-URL redaction controls.
- Add an exact positive regression and malicious look-alike controls.

## Follow-up Actions

- [ ] Merge the narrow hotfix through reviewed PR and deploy exact main.
- [ ] Repeat the production MCP VK story-list and aggregate-statistics smoke.
- [ ] Refresh the existing ChatGPT connector metadata and run the first
      user-supplied image prepare/approval/commit canary separately.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: `scripts/deploy_fly_main.sh` from clean exact `origin/main`
- regression checks: pending
- post-deploy verification: pending

## Prevention

Production story acceptance includes a metadata-only hostname inventory for
provider-selected CDN drift and an exact allowlist regression without ever
recording signed media URLs.
