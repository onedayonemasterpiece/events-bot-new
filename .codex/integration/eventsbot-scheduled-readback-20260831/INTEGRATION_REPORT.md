# EVENTSBOT-MCP-20260831-SCHEDULED-READBACK integration report

Base: `origin/main` `64f75d10f7aff33fa616cee212878bd9d03673b1`.
Integration branch: `fix/eventsbot-scheduled-readback-reschedule-20260831`.

## Integrated lanes

| Lane | Worker implementation | Integration commits | Result |
|---|---|---|---|
| telegram-ledger | `8744e4fd0`, `ce16378d0` | `b8c1e97b3`, `71b33c88a` | durable intent/claim state, cancellation finalization, raw scheduled/live reconciliation and scheduled album delete |
| vk-diagnostics | `bd6945be1` | `9ffda2d26` | bounded safe multipart observations, flat/nested receipt parsing, logical postponed read/delete and safe retry seam |
| tools-runtime | `9ff10be9d` | `e07d60168` | adapter-owned mutation deadline, scheduled-list tool, expanded receipts and bounded single-flight retry |
| docs-ops | `43142a4e8` | `4988e8efa` | incident/regression contract, canonical private MCP docs, routes and changelog |
| integration fixes | — | `72bfa6f7b`, `175116d87`, `6ff6e30ee`, `455a4c33a`, `c95ce237d` | durable wrappers/expired-ref recovery, complete mutation/readback receipts and a total Telegram session deadline |

The result-only commits are retained on the branch for lane evidence. No lane
published, scheduled, retried, deleted, or altered provider content.

## Requirement disposition before release

| ID | Status | Evidence |
|---|---|---|
| R1 | Done | Runtime no longer wraps mutations in an equal outer `wait_for`; Telegram finalizes or releases cancellation according to the persisted mutation boundary. |
| R2 | Done | Additive encrypted Telegram intent, claim lease/deadline, mutation timestamp and bounded reconciliation-attempt columns. |
| R3 | Done | Raw scheduled-history plus post-slot live-history exact matching, logical album collapse, exact/ambiguous/bounded/no-match terminal receipts. |
| R4 | Done | `social_scheduled_items_list`, existing schedule scopes, hard cap 25, opaque/redacted logical publications. |
| R5 | Done | Telegram scheduled namespace deletes all exact album members and proves absence; VK exact postponed delete proves absence. |
| R6 | Done | Same logical action/preparation/operation bounded retry, max three attempts, `BEGIN IMMEDIATE` single-flight guard, provider safe-failure revalidation. |
| R7 | Done | Old failures proven at image 1 before save/post; new safe observations distinguish receipt shape/type/length/decoding without payloads or secrets. Exact old empty field remains unrecoverable. |
| R8 | Done locally | Focused integration: 308 passed. Final complete `tests/test_private_events_mcp_*.py`: 546 passed, 3 existing aiohttp warnings. `compileall` and `git diff --check`: pass. Independent checklist review found no remaining code blocker. |
| O1 | Partial | Incident/docs complete; merge, exact-main deploy, historical Telegram provider readback and refreshed ChatGPT action-catalogue acceptance remain release/operational gates. |

## Integration corrections

- Added `DurableVKWorkspaceAdapter.scheduled_items()` and durable
  `retry()` proxies so production wrapping cannot hide the implemented provider
  capabilities.
- Added a principal/resource/policy-bound expired-reference recovery path used
  only for reconciliation of an already-bound historical operation. It recovers
  encrypted provider bindings but cannot authorize a new mutation or reopen
  staged bytes.
- Updated the Telegram story test ledger fake to the migration-safe claim
  protocol; this fixed the only four full-suite failures found after lane merge.
- Exposed the VK mutation boundary, final readback and exact postponed-delete
  absence evidence in real receipts, not only encrypted attempt diagnostics.
- Extended the Telegram adapter deadline across the serialized local queue,
  governor, client setup/auth and provider body; separately bounded teardown
  cannot strand a claim or leave the local lock held after cancellation.

## Local verification

```text
targeted integrated MCP files: 308 passed in 10.48s
complete Private Events MCP suite: 546 passed, 3 warnings in 23.08s
python -m compileall: pass
git diff --check: pass
```

The three warnings are pre-existing aiohttp `NotAppKeyWarning` instances from
the disabled-provider `create_app` test. They are unrelated to this change.

## Remaining integration-owned gates

1. Merge to `origin/main` after PR CI and deploy only a clean exact-main checkout.
2. Verify runtime SHA, health, databases, runtime-log mirror and tool catalogue.
3. Reconcile the three historical operation refs and exact provider queues by
   read-only provider calls; Codex must not perform a provider content mutation.
4. Keep the incident open unless the refreshed ChatGPT action definition is
   accepted in a genuinely new chat and the exact operational handoff is safe.
