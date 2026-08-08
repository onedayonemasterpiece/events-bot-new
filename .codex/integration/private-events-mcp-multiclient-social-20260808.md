# Private Events MCP multi-client/social integration — 2026-08-08

## Scope and status

Integration branch: `feature/private-events-mcp-multiclient-social`
Rebased base: `origin/main` at `0bfbc3f94a6a8bebd9d7c849c3699e3358efde30`
Pre-report implementation head: `5d8f876531be5279a2ae37f07a35ddf5a8469266`

No code or transport from superseded PR #365 is present. No GitHub issue was
created. Activation, merge, credentials and deployment remain gated on exact-head
independent approval and GitHub Actions.

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R1 | ChatGPT confidential OAuth and Codex static public OAuth are isolated | Done | Distinct exact resources/endpoints, client registry, mandatory S256 PKCE, loopback-only Codex callback, exact code/token/refresh bindings and negative cross-client tests. |
| R2 | Codex uses MCP only for event/incident/operations evidence | Done | Codex maximum/default scopes are the three read domains; `tools/list` is exactly seven read tools; social scope authorization and direct social calls fail closed. |
| R3 | ChatGPT can generically read/publish allowlisted Telegram/VK text | Done | Four scope-filtered tools: `telegram_read`, `vk_read`, `prepare_text_publication`, `publish_prepared_text`; no event ID/outbox coupling. |
| R4 | Social actions cannot become raw provider passthrough | Done | Strict alias-only policy, runtime rejection of undeclared arguments, fixed Telegram operations and literal VK `wall.get`/`wall.post`; no raw ID/URL/method/media/edit/delete/forward/MAX surface. |
| R5 | Destructive calls are bounded and auditable | Done | One-use exact-hash prepare/commit ticket, 90-day bounded replay ledger, atomic persistent daily reservation, timeout `outcome_unknown`/unsafe-to-retry, and redacted append-only action audit including denials. |
| R6 | Provider credentials remain isolated | Done | Dedicated `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP` only, no E2E/S22/generic-session/bot-token fallback; VK uses the existing injected throttled runtime helper; adapters are constructed only after the MCP enabled gate. |
| R7 | Secret/untrusted-data boundaries | Done | Recursive credential/operator redaction includes bare Telegram bot-token shapes; provider text is explicitly untrusted; private URLs and authorization material are filtered from access logs and script receipts. |
| R8 | CI/docs/operations are synchronized | Done | Canonical runbook, E2E index, `.env.example`, CHANGELOG, overlay installer and explicit GitHub MCP compile/test gate are updated. |
| R9 | Merge/deploy/live OAuth MCP acceptance | Pending | Must use the exact merged `origin/main` SHA after independent approval and green GitHub Actions. |

## Integrated lanes

- OAuth/client policy and secure credential generation: commits `2a7146ae0` through `061ab5e4e`.
- CI release gate: `470b47388`, evidence `c8c206add`.
- Provider-neutral social core and integration hardening: `60a6caac3` through `a6dfdfbbf`.
- Provider adapters: `117e65770`, evidence `223575c1a`.
- Overlay/CI reconciliation: `5d8f87653`.

Lane evidence is under `.codex/lanes/{oauth_implementation,mcp_ci_gate,social-core,provider_adapters}/RESULTS.md`.

## Validation receipt

Commands on the rebased implementation head:

```bash
PYTHONPATH=. python -m compileall -q private_events_mcp private_events_mcp_provider_adapters.py tests scripts main_part2.py
PYTHONPATH=. pytest -q tests/test_private_events_mcp_*.py
git diff --check origin/main...HEAD
```

Result after VK runtime-log hardening: compile PASS; **65 passed**; diff check PASS. The only warnings are three
existing aiohttp `NotAppKeyWarning` notices exercised by the disabled-create-app
regression test.

A local read-only provider probe used the dedicated MCP Telegram role and the
fixed VK adapter. Both returned two recent records; no text or credential was
logged and no publication was attempted. The sanitized untracked receipt is at
`artifacts/codex/private-mcp-live-read-probe/receipt.json`.

An independent review found and reproduced a VK provider-error log leak in the pre-final head; the runtime boundary was then hardened to suppress publication text, owner, GUID, provider message/captcha data and every token fragment, with a real `main.vk_api` regression test. A follow-up review also required bounded social-audit retention and audit coverage for pre-handler denials; both are now closed with a 90-day immutable window and a centralized denial hook covering schema, malformed input, insufficient scope, policy, ticket and budget failures. Final exact-head re-review is required.

Tracked activation/credential files: 0. Legacy `prod_ops_mcp`, `PROD_OPS_MCP_*`
and static-bearer paths in this diff: 0.

## Remaining release gates

1. Obtain independent security/code approval on the exact final head.
2. Push/open draft PR and require all GitHub Actions to pass; resolve review on a
   new exact SHA if anything changes.
3. Merge, fetch and verify the exact merged commit is reachable from
   `origin/main`.
4. Generate fresh credentials from that checkout with explicit
   `--enable-chatgpt-social`; store artifacts outside Git with 0700/0600 modes.
5. Stage exact aliases and provider secrets, deploy only through
   `scripts/deploy_fly_main.sh`, then prove exact in-container SHA, `/healthz`,
   quick_check, DB unchanged and webhook/job non-regression.
6. Run ChatGPT and Codex OAuth/MCP smokes. Codex must expose exactly seven read
   tools. ChatGPT social reads may run against allowlisted aliases; a live write
   requires exact operator-supplied text and target and must never be invented as
   a smoke payload.
