# INC-2026-08-23 Private MCP ChatGPT refresh reconnect loop

Status: open
Severity: sev2
Service: Private Events MCP OAuth
Opened: 2026-08-23
Owners: events-bot production / Private Events MCP
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The installed ChatGPT MCP connection repeatedly returned to browser
authorization around the access-token lifetime. The operator had to enter the
same temporary/bootstrap operator token again even though the connector URL,
OAuth client identity and server were unchanged.

## User impact

- MCP access was interrupted roughly every 10–15 minutes.
- Recovery required a manual reconnect and repeated bootstrap-token entry.
- The temporary operator token was correct; an invalid value would have failed
  the authorization POST immediately instead of producing a working short
  session.

## Root cause

The server used a 15-minute access token and issued refresh tokens only when the
authorization grant explicitly contained `offline_access`. ChatGPT can complete
a valid confidential-client authorization without that optional OIDC-style
scope, leaving the installed connector with only an access token. On expiry it
must restart browser authorization.

The existing smoke explicitly requested `offline_access`, so it verified the
strict rotating lane while missing the valid ChatGPT grant that omitted the
scope.

## Corrective change

- Preserve the existing OAuth endpoint, path secret, ChatGPT client ID/secret,
  signing key and operator token.
- For the predefined confidential ChatGPT client only, add a bounded refresh
  token when a successful authorization-code response otherwise lacks one.
- Bind that token to the exact authenticated client, subject, resource and
  granted scopes. Reuse it until its configured expiry so simultaneous refresh
  attempts cannot spend one another's replacement.
- Preserve the existing rotating, replay-resistant behavior for explicit
  `offline_access` grants.
- Preserve the original rotation policy for public Codex and OpenCode clients.
- Later additive Telegram item-link/media/audio read fields must preserve the
  same endpoint, client/resource/audience and refresh state. A normal service
  restart may reconnect automatically; rollout must not delete/re-add the app,
  rotate connector identity or require another bootstrap token.

## Regression contract

Before merge:

1. The complete `tests/test_private_events_mcp_*.py` suite passes.
2. A ChatGPT grant without `offline_access` returns a refresh token.
3. Two concurrent refresh requests using that confidential token both succeed
   and keep the same bounded refresh token.
4. An explicit `offline_access` ChatGPT grant still rotates; replaying the spent
   token returns `invalid_grant`.
5. Codex and OpenCode behavior and exact scope boundaries remain unchanged.
6. `compileall` and `git diff --check` pass.

Before closure:

1. Merge the reviewed PR to `main`.
2. Deploy exact `origin/main` only through `scripts/deploy_fly_main.sh`.
3. Verify `/healthz`, exact in-container SHA and SQLite health.
4. Reconnect the existing ChatGPT app at most once; do not delete/recreate it
   and do not rotate the stable connector identity.
5. Confirm a successful OAuth audit sequence containing `token/refreshed` and
   `refresh_mode=confidential_stable` for a grant without `offline_access`, or
   the existing rotating mode when ChatGPT explicitly requests it.
6. Confirm that MCP remains usable beyond one 15-minute access-token lifetime.
7. Verify that logs and receipts contain no operator token, OAuth client secret,
   private path, access token or refresh token.

## Rollback

Revert the hotfix commit and redeploy exact reverted `main`. Existing connector
identity and credentials remain valid; rollback only restores the previous
short-session behavior.
