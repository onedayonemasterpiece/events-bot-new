# INC-2026-09-13-dataset-loop-chatgpt-oauth-discovery

Status: monitoring
Severity: sev1
Service: Dataset Loop MCP / KenigEvents Identity
Opened: 2026-09-13
Closed: —
Owners: Dataset Loop / My Data Hub
Related incidents: —
Related docs: Dataset Loop `docs/authentication-and-connection-tokens.md`; My Data Hub `docs/operations/chatgpt-cimd-oauth.md`

## Summary

ChatGPT could not create the Dataset Loop custom MCP connector with OAuth. The public MCP endpoint accepted owner personal connection tokens, but did not advertise OAuth protected-resource metadata and the identity server did not issue Dataset Loop resource-bound scopes.

## User / Business Impact

- The product owner could not connect Dataset Loop to ChatGPT through the supported OAuth flow.
- Existing owner-PCT API access remained available, but it did not satisfy ChatGPT's custom connector OAuth discovery contract.

## Detection

- User-visible ChatGPT connector error: the MCP server did not implement OAuth.
- Direct probes confirmed a `404` protected-resource metadata response and a missing `WWW-Authenticate` resource-metadata challenge before mitigation.

## Timeline

- 2026-09-13 UTC — user reported the ChatGPT connector failure with screenshot evidence.
- 2026-09-13 UTC — traced the mismatch to Dataset Loop running in `owner_pct` mode and missing Dataset resource/scopes in KenigEvents Identity.
- 2026-09-13 UTC — merged My Data Hub PR 41 and restarted the identity/control-plane release from `origin/main`.
- 2026-09-13 UTC — deployed Dataset Loop SHA `5eed4961d8952920e3328b2f19498bfbaf597717` with `oidc_and_pct` and canonical issuer/audience/JWKS settings.
- 2026-09-13 UTC — cancelled stale queued run `2db244f7-2076-5c26-8f28-bdfc86808aa6` after it prevented an attempted legacy rollback from loading the newer run schema.
- 2026-09-13 UTC — public discovery, challenge, health, authenticated initialize, and authenticated tool-list probes passed; awaiting real ChatGPT UI reconnection confirmation.

## Root Cause

1. Dataset Loop production used the PCT-only authentication mode even though OAuth discovery code existed in the newer release.
2. KenigEvents Identity did not configure the Dataset Loop MCP resource or its `artifacts:read`, `runs:read`, and `runs:write` scopes.
3. Therefore ChatGPT had no standards-based route from the MCP URL to authorization-server metadata.

## Contributing Factors

- Deployment configuration lagged behind the OAuth-capable application code.
- The legacy systemd runtime override obscured the exact release/configuration relationship.
- Connector discovery was not included in the prior live acceptance gate.

## Automation Contract

### Treat as regression guard when

- Dataset Loop auth mode, public ingress, MCP discovery, issuer/audience/JWKS, OAuth scopes, identity bootstrap, or ChatGPT connector documentation changes.

### Affected surfaces

- Dataset Loop `/mcp` and `/.well-known/oauth-protected-resource/mcp`.
- KenigEvents Identity authorization-server metadata and resource-bound token issuance.
- Dataset Loop systemd release configuration and public nginx ingress.
- ChatGPT custom MCP connector creation and tool scan.

### Mandatory checks before closure or deploy

- Protected-resource metadata returns the canonical MCP resource, identity authorization server, and all three Dataset scopes.
- Unauthenticated MCP initialize returns `401` with `WWW-Authenticate` pointing to protected-resource metadata.
- Authenticated initialize and `tools/list` succeed without logging any bearer token.
- Identity metadata advertises dynamic client registration/metadata capability and Dataset scopes.
- Dataset health reports live Kaggle runtime and expected feature flags.
- Deployed Dataset fix is reachable from `origin/main`; otherwise the incident remains open/monitoring.
- Real ChatGPT connector completes OAuth and scans tools.

### Required evidence

- Identity deployed SHA `a59b911a9213a3d57263959bdde646927f765de4` reachable from My Data Hub `origin/main`.
- Dataset deployed SHA `5eed4961d8952920e3328b2f19498bfbaf597717` and operator pointer SHA `234da61` on the pushed hotfix branch.
- Unit/regression suites: 34 My Data Hub tests, 16 Dataset OAuth/operator tests, and 28 Dataset continuation/release tests passed.
- Live public discovery/challenge/health plus authenticated initialize/tool-list evidence.
- Final real ChatGPT UI connection confirmation.

## Immediate Mitigation

- Enabled dual `oidc_and_pct` auth so existing owner-PCT clients continue to work while ChatGPT can use OAuth.
- Added the Dataset MCP resource and scopes to the identity server and restarted both services from exact releases.

## Corrective Actions

- Dataset operator now writes canonical OAuth issuer, audience, JWKS, tenant, and auth-mode settings atomically.
- Identity bootstrap now includes the Dataset resource and required ChatGPT scopes.
- Removed the legacy Dataset runtime override and preserved media feature flags in narrow dedicated drop-ins.
- Added code-level deployment and discovery regression tests and canonical connection documentation.

## Follow-up Actions

- [ ] Product owner: retry connector creation in ChatGPT and confirm OAuth login plus tool scan.
- [ ] Dataset maintainers: land the deployed Dataset fix in `origin/main`; it currently remains on the pushed hotfix/media-discovery line.
- [ ] Dataset maintainers: add the public discovery/challenge and authenticated tool-list probes to the recurring live acceptance gate.
- [ ] Product owner: rotate the previously exposed owner PCT after OAuth connection is confirmed.

## Release And Closure Evidence

- deployed SHA: Identity `a59b911a9213a3d57263959bdde646927f765de4`; Dataset `5eed4961d8952920e3328b2f19498bfbaf597717`.
- deploy path: My Data Hub unified control plane; Dataset Loop operator-prepared exact release under user systemd.
- regression checks: all targeted test suites passed; no bearer token was printed by authenticated probes.
- post-deploy verification: health `ok`; native Dataset loop, media discovery, and media retrieval enabled; OAuth discovery/challenge valid; authenticated MCP initialize and 10-tool listing passed.

## Prevention

- Treat protected-resource discovery and identity resource/scope configuration as one release contract.
- Keep the exact Dataset runtime release and auth settings operator-owned rather than hidden in broad legacy overrides.
- Do not close this incident until both `origin/main` reachability and the real ChatGPT UI OAuth flow are proven.
