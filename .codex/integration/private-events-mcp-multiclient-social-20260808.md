# Private Events MCP universal social integration — 2026-08-08

## Status

Integration branch: `integration/private-events-mcp-universal-social`
Rebased base: `origin/main` at `93a94b6aba181c40491f03fc3ebcdcc2dc320ced`
Release-gate remediation candidate: `829abf86bb9c333c0400e528ad265cacfbe6ff40`

Implementation, fake-provider validation and canonical documentation are
complete. The first exact-head reviews requested changes; commits `e433fa95f0`,
`06a6dc55a`, and `829abf86b` close those findings. Repeat exact-head
independent review, PR CI, merge,
credentials, deploy and live provider canaries remain pending. Production was
not changed.
No GitHub issue was created. Superseded PR #365 code is absent.

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Telegram Saved/dialog/channel reads and search | Done in code | Dedicated role adapter, opaque refs, bounded cursors, Saved readback tests |
| R02 | Telegram exact-person/Saved communication and typed rich mutations | Done in code; live canary pending | Exact target preview, typed action, immutable preflight, operation CAS, rights tests |
| R03 | Telegram stories/media/stats/recommendations | Partial | Read/stat contracts exist; story/media activation is rejected until an authenticated byte-bound upload path is implemented and reviewed |
| R04 | VK public/dialog/content reads and discovery | Done in code | Fixed 5.199 adapter, public/private scope isolation and cursor-binding tests |
| R05 | VK messages/posts/comments/reactions/edit/delete/schedule | Done in code; live canary pending | Explicit actor matrix, full-intent idempotency and reconcile tests |
| R06 | VK stories/media/statistics | Partial | Statistics contracts exist; story/media activation is rejected until authenticated upload and exact actor grants pass a later gate |
| R07 | ChatGPT catalog, granular scopes, approval, audit and budgets | Done in code | Capability-filtered catalog, exact human target/item browser preview, durable hashes/CAS/UTC-day budgets |
| R08 | Codex remains evidence-only | Done | Exact separate resource, max scopes and seven-tool regression |
| R09 | Fake-provider tests and CI gate | Done locally; repeat GitHub CI pending | Full private MCP suite 205 passed after remediation; explicit workflow gate already present |
| R10 | Docs/env/generator/rollout | Done | Canonical runbook, E2E index, env template, fresh generator and CHANGELOG |
| R11 | One-target editorial sample up to 100 posts | Done in code | Runtime + real VK adapter 4×25 integration test, cumulative 100, exact target/purpose/access/authorization-basis cursor binding |
| R12 | Deeper incident/stability investigation surface | Deferred as requested | Begins only after social workspace release |

## Integrated lanes and reviews

| Lane | Status | Reviewed head/evidence |
|---|---|---|
| Provider-neutral contract | Integrated | focused contract suite and schema validation |
| OAuth/client policy | Approved and integrated | independent approval at `fbbc04447f90710cba8aff88a1396e23ca85e966` |
| Durable runtime/tool layer | Approved and integrated | independent approval at `3e3555ece127d229b7e3390c14134321b222ae08` |
| VK workspace | Approved and integrated | final isolation/idempotency remediation integrated from `26b4b878dcaded7278cd9403a63b36fc3b60226a` |
| Telegram workspace | Approved and integrated | final rights/operation/fencing remediation integrated from `2ddb16984e27ad4a7ad9e45383c3307a325d986a` |
| Provider bindings/server approval/docs | Remediated; exact-head re-review pending | encrypted restart-safe refs/receipts, DB 0600 from creation, human-readable target/item approval, closed catalog |

Worker evidence remains under `.codex/lanes/`. The current integration report
supersedes the earlier alias-only 65-test report.

## Validation receipt

On remediation candidate `829abf86bb9c333c0400e528ad265cacfbe6ff40` rebased directly onto the recorded
`origin/main`:

```bash
PYTHONPATH=. python -m compileall -q \
  private_events_mcp \
  private_events_mcp_provider_adapters.py \
  private_events_mcp_telegram_adapter.py \
  private_events_mcp_vk_adapter.py \
  private_events_mcp_workspace_providers.py \
  tests scripts main_part2.py
PYTHONPATH=. python -m pytest -q tests/test_private_events_mcp_*.py
git diff --check origin/main...HEAD
```

Result: compile PASS; **205 passed**, three unchanged aiohttp
`NotAppKeyWarning` warnings; diff check PASS. Targeted integration run was
**92 passed** before the full gate. Changed/new MCP modules also pass focused
Ruff checks; the monolithic `main_part2.py` retains repository-pre-existing full
file lint debt outside the integration hunk.

Security/hygiene checks:

- tracked `activation/`: 0;
- credential-shaped literals in the feature diff: 0;
- `prod_ops_mcp`, `PROD_OPS_MCP_*`, static Bearer/PR365 transport: 0;
- role fallback from MCP Telegram to E2E/S22/generic session/bot token: 0;
- provider calls during the local gate: 0 (fakes only);
- social mutations available to Codex: 0;
- invalid approval requests receive no-store/self-only CSP error pages and do
  not disclose the prepared content;
- auth/social SQLite state is separate from the event DB and mode `0600`;
- provider bindings, cursors, exact previews and receipts are encrypted and
  restart-safe; a provider-attempted crash state reconciles without re-send;
- publication attempts use durable UTC-day budgets (not hourly reset);
- disabled action/media/story classes are absent from the ChatGPT catalog, and
  a stale preparation cannot bypass a newly disabled action kill switch;
- forward and item-only attempt budgets use the actual destination/source target;
- media/story activation itself fails configuration until real upload storage.

## Remaining release gates

1. Push the remediation, rerun independent security/code review on the exact
   remote head; any change invalidates the verdict.
2. Refresh draft PR #410, run every repository-required GitHub Action and keep
   it draft until exact-head approval.
3. Merge only after green CI/review. Fetch and record the exact merged
   `origin/main` SHA.
4. Generate fresh credentials from that exact checkout into a new 0700 path;
   install secrets without logging them. Keep all universal social flags off.
5. Deploy only through `scripts/deploy_fly_main.sh`, prove exact in-container
   SHA, Fly release, `/healthz`, `quick_check`, DB unchanged, webhook/scheduler
   and disk/log health.
6. Activate in stages: evidence/Codex, Telegram public read/editorial, Saved
   exact `Привет мир` prepare → browser approval → commit → readback, then one
   explicitly authorized exact-person reminder. VK and media/story actions stay
   off until their actor/upload capability canaries pass.
7. Return only sanitized receipts; never publish the private MCP path, client
   secret, bootstrap/approval tokens, session, access or refresh token.
