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
| O1 | Partial | PR `#600`, exact-main Fly `v2054`, health/DB/log checks and historical Telegram/VK readbacks are complete with zero exact copies. Refreshed ChatGPT action-catalogue acceptance remains the incident closure gate. |

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

## Production acceptance

- PR `#600` merged as `5cb1fbc9e870890770ca89dfd44917feef0c40f1`;
  all required CI jobs passed.
- Clean exact-main deploy produced Fly release `v2054`; `/healthz`, both SQLite
  quick checks, immutable image SHA and runtime-log mirror passed.
- Read-only provider reconciliation found Telegram scheduled/live `0 / 0` and
  VK postponed/live `0 / 0`. Historical Telegram status converged terminally to
  `reconciliation_no_match`; both VK attempts remain definite pre-wall failures.
- Server catalogue exposes scheduled-list and bounded retry under existing
  scopes. No provider content was published, retried, deleted or altered.
- Machine handoff is `READY_FOR_14_00_PUBLICATION`; refreshed ChatGPT
  action-control publication and a genuinely new-chat canary remain the only
  incident closure gate.
