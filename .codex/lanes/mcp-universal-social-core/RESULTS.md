# Lane mcp-universal-social-core Results

## Status
committed

## Requirement IDs
- R07
- R08
- R11

## Branch
`agent/mcp-universal-social/core-contract`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/mcp-universal-social-core`

## Base SHA
`384f58166cb3f9cd6095564da6a8fc02003e8167`

## Head SHA
Review-remediated implementation commit: `b3e7fceff6610730fdf197325b8f77b338289147`.
The final lane head additionally contains only this updated results receipt.

## Files changed
- `private_events_mcp/social_workspace.py` (new provider-neutral contract)
- `tests/test_private_events_mcp_social_workspace_contract.py` (new focused adversarial suite)
- `.codex/lanes/mcp-universal-social-core/RESULTS.md` (lane receipt)

## Commands run
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m compileall -q private_events_mcp tests/test_private_events_mcp_social_workspace_contract.py`
- `PYTHONPATH=. /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_social_workspace_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Tests / verification
- Focused contract suite: `29 passed in 1.52s`.
- Compileall: passed.
- Diff checks: passed.
- Every exported JSON Schema passes Draft 2020-12 meta-validation and is closed at the tool boundary.
- Exact granular Telegram/VK scopes are tested, including separate discovery, public/private/dialog reads, DM send, post publish, each mutation, story read/write, analytics, and audience scopes. The complete set is disjoint from `CODEX_MAX_SCOPES`.
- Saved Messages/self resolution accepts no locator value. Username, canonical profile link, and provider-ID exact resolution require `expected_target_kinds=[user]`; response validation rejects a non-user canonical preview.
- Editorial sampling is single-target, channel/group/community-only, page-size <=25, total <=100, and requires public/private read access plus consent, purpose, ephemeral/no-index, and server-state hooks. Server-minted sample/cursor continuity, immutable target/kind/date/purpose/total binding, and cumulative count are adversarially tested.
- Prepare output is always `awaiting_human_approval` and carries a deterministic action digest. Commit rejects `confirm=true`; it requires opaque approval ref/receipt and an external atomic one-use hook bound to client, subject, resource, digest, expiry, and zero prior uses.
- Statuses cover approval and provider uncertainty. `outcome_unknown` is schema-bound to `retry_safe=false`; successful exact-person `send_message` requires a matching read-after-write item receipt.
- Closed bounded external-data outputs exist for target search/list/get, item list/get, thread, reactions, stories, statistics and audience; root and nested content carry `trust=untrusted_external_data`.
- Asset stage/status uses bounded upload handles, digests and opaque asset refs; no filesystem path or arbitrary URL input exists.
- Mandatory safety hooks cover recursive redaction, encoded response cap, durable append audit, durable idempotency, and rate/egress/media budgets. Missing, invalid, non-recursive, non-durable and denied hooks fail closed in tests.

## Risks
- This remains a contract-only lane by design. It does not modify or wire existing MCP server, protocol, OAuth, access-policy, tool catalog, adapters, storage, deployment, or secrets.
- Integration must advertise the new scopes only to the ChatGPT client/resource. None may be added to Codex max/default scopes or its visible tool catalog.
- Provider adapters must mint and resource-bind opaque target/item/asset/sample/cursor/approval references. Native provider IDs are accepted only by the exact-person resolver and may never cross the action boundary.
- Approval and idempotency hooks describe atomic durable contracts; the integration/storage lane must implement those guarantees transactionally.
- Output schemas provide strict item/string bounds and the safety layer hard-caps encoded output at 128 KiB. The transport’s existing response/egress limits remain required defense in depth.
- Rich-text links and canonical profile links are descriptive/provider-resolution values only; integration must not add generic outbound fetching.

## Merge notes
Cherry-pick all lane commits through the final receipt commit. The implementation is isolated in a new module/test and intentionally has no existing-file wiring conflicts.
