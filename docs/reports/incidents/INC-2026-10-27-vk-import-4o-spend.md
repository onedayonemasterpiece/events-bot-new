# INC-2026-10-27 VK import GPT-4o spend

Status: open
Severity: sev2
Service: scheduled VK source import / OpenAI budget
Opened: 2026-10-27
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-28-lite-fallback-overload`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/operations/release-governance.md`
Registry: `inc_c0f42301403e5cc613f3da63`

## Summary and impact

The September emergency setting `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL=4o` stayed in
production after Google models regained partial availability. It selects OpenAI
directly for every scheduled VK source parse, including conditional verification
and terminal adjudication. The resulting spend is independent of a fresh Lite
failure. OpenAI `token_usage` records for 25 and 26 October (Europe/Kaliningrad)
show 79 and 93 untagged GPT-4o calls, using 722,704 and 846,928 tokens. The
same periods had only 8 and 5 tagged Telegram public-writer fallback calls.
Poster OCR also used 3,170,821 and 3,757,274 `gpt-4o-mini` tokens; that is a
separate lane and is not changed by this incident fix.

## Detection and timeline

- 2026-09-28: commit `d587707d8` pinned the five bounded daily VK batches to
  `4o` as an availability measure during the Lite overload incident.
- 2026-10-25–26: OpenAI calls clustered at the five scheduled import slots. A
  source can require more than one physical parse call, so the 12-post batch cap
  did not cap the run at 12 OpenAI requests.
- 2026-10-27: live Fly machine `148eddde9b5778`, image
  `deployment-01M3TZEJY3GPN766ZZBE31NGAM`, still reported the `4o` setting.
  `EVENT_PARSE_LLM` was unset and `EVENT_PARSE_ENABLE_4O_FALLBACK=0`. The
  deployed parser selects `EVENT_PARSE_LLM` or the caller's `gemma_model`, proving
  these scheduled requests are direct routing, not provider failover.
- 2026-10-27: a scoped Fly machine environment update to a Google Lite route
  was rejected before mutation: `We require your billing information` (Fly
  request `01M5YT0HVT05MJTAEA822826PF-yyz`). Readback confirmed the machine
  stayed started on the same image and `4o` setting.
- 2026-10-28: the production host was identified as RunCoveer; the Fly machine
  is not the active production runtime. Its running Events container used
  release `e64e89ee703ee19d011c07be0a1dc0d7009cca38` and still had
  `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL=4o` in its production env file. At 16:58 UTC
  that one setting was changed to `gemini-3.1-flash-lite`, and only the Events
  container was recreated with its existing image and release. The new container
  reports the Lite route and is healthy; `/healthz` reports `ready=true`,
  `db=ok`, and `scheduler.vk_auto_import=ok`.

Google availability was degraded, but not wholly absent. The shared Google
ledger recorded 3.1 Lite physical attempts with 1,135 successes / 370 provider
failures on 25 October and 549 / 547 on 26 October. The 3.5 Lite sibling had
38 and 167 successes on those days. Counts span all ledger consumers; they do
not establish every VK carrier's outcome. During this incident the configured
monitoring reserve chain also prefers compatible Flash models before Lite.

## Root cause and contributing factors

1. The temporary `4o` setting remained in RunCoveer's production env file after
   the emergency ended. It had also been committed to `fly.toml`.
2. The model override name says `GEMMA_MODEL`, while deployed `main.py` also
   interprets the value `4o` as an OpenAI backend selector.
3. The RunCoveer env file was not covered by the `fly.toml` regression guard.
   Per-source verification/adjudication multiplies physical calls.
4. Untagged parse `token_usage` rows lack a consumer label, making the source
   of spend harder to identify. Add explicit route metadata in a later change.

## Automation contract

### Treat as regression guard when

Changing VK auto-import parser model config, caller-scoped backend selection,
Google monitoring route, or OpenAI budget enforcement.

### Affected surfaces

RunCoveer production env, `fly.toml`, `main.py` source parser, `vk_auto_queue.py`,
Google model availability, OpenAI `token_usage`, and scheduled VK import receipts.

### Mandatory checks before closure or deploy

- Production `fly.toml` model routes scheduled VK import through Google, with a
  test proving the parse call cannot select OpenAI.
- Existing caller-scoped 4o test and bounded Google model-chain tests pass.
- Candidate release is clean exact `origin/main`; current running SHA is its
  ancestor. Verify no unrelated production code is silently dropped.
- Live RunCoveer env and `/healthz` show the Google route and healthy runtime.
- One bounded scheduled or explicitly authorized VK import uses Google model
  receipts and creates no untagged GPT-4o parse usage; reconcile technical
  terminals without turning them into product exclusions.

### Required evidence

PR/merge SHA, targeted test result, RunCoveer image and runtime SHA,
live model setting, post-change import `ops_run` and provider-model receipts,
and OpenAI usage delta.

## Immediate mitigation and corrective action

Changed the RunCoveer production route from `4o` to `gemini-3.1-flash-lite`
for scheduled VK batches, without changing the running source release. The
merged PR #752 had corrected `fly.toml`, but that file did not control the
active RunCoveer container. The Fly billing denial did not block this fix.

## Follow-up actions

- [ ] Verify the next RunCoveer VK batch (scheduled 2026-10-29 04:15 UTC)
  against Google provider receipts and OpenAI usage.
- [ ] Keep the RunCoveer env route aligned with the repository's release
  configuration in the next normal deployment process.
- [ ] Add `consumer=event_parse` and import correlation to OpenAI usage records
  in a separately reviewed observability change.
- [ ] Review the distinct high-volume `gpt-4o-mini` poster OCR lane.

## Release and closure evidence

- Running SHA: `e64e89ee703ee19d011c07be0a1dc0d7009cca38` (unchanged).
- Mitigation: RunCoveer env update and single Events container recreation at
  2026-10-28 16:58 UTC; image unchanged.
- Regression checks: PR #752 passed 30 event-parse tests, eight focused tests,
  and all four CI jobs before merge. Its Fly config change did not reach
  RunCoveer production.
- Post-change verification: new container `healthy`; `/healthz` HTTP 200 with
  `ready=true`, `db=ok`, `scheduler.vk_auto_import=ok`; env reads Lite route and
  `EVENT_PARSE_ENABLE_4O_FALLBACK=0`. Provider receipt remains pending until
  the next scheduled batch.
