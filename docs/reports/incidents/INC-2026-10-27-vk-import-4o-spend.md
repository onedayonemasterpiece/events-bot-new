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

Google availability was degraded, but not wholly absent. The shared Google
ledger recorded 3.1 Lite physical attempts with 1,135 successes / 370 provider
failures on 25 October and 549 / 547 on 26 October. The 3.5 Lite sibling had
38 and 167 successes on those days. Counts span all ledger consumers; they do
not establish every VK carrier's outcome. During this incident the configured
monitoring reserve chain also prefers compatible Flash models before Lite.

## Root cause and contributing factors

1. The temporary `4o` setting was committed to `fly.toml` and not removed when
   the emergency ended.
2. The model override name says `GEMMA_MODEL`, while deployed `main.py` also
   interprets the value `4o` as an OpenAI backend selector.
3. The production config had no regression guard against keeping that costly
   route. Per-source verification/adjudication multiplies physical calls.
4. Untagged parse `token_usage` rows lack a consumer label, making the source
   of spend harder to identify. Add explicit route metadata in a later change.

## Automation contract

### Treat as regression guard when

Changing VK auto-import parser model config, caller-scoped backend selection,
Google monitoring route, or OpenAI budget enforcement.

### Affected surfaces

`fly.toml`, `main.py` source parser, `vk_auto_queue.py`, Google model availability,
Fly release config, OpenAI `token_usage`, and scheduled VK import receipts.

### Mandatory checks before closure or deploy

- Production `fly.toml` model routes scheduled VK import through Google, with a
  test proving the parse call cannot select OpenAI.
- Existing caller-scoped 4o test and bounded Google model-chain tests pass.
- Candidate release is clean exact `origin/main`; current running SHA is its
  ancestor. Verify no unrelated production code is silently dropped.
- Live Fly config and `/healthz` show the new release and Google route.
- One bounded scheduled or explicitly authorized VK import uses Google model
  receipts and creates no untagged GPT-4o parse usage; reconcile technical
  terminals without turning them into product exclusions.

### Required evidence

PR/merge SHA, targeted test result, Fly deployment/image and runtime SHA,
live model setting, post-release import `ops_run` and provider-model receipts,
OpenAI usage delta, and any billing blocker resolution.

## Immediate mitigation and corrective action

Prepared a narrow production-config change from `4o` to
`gemini-3.1-flash-lite` for the 12-post scheduled VK batches. The existing
monitoring text chain still admits at most two compatible Google sends. The
attempted direct Fly config mitigation was denied by Fly billing and did not
alter production.

## Follow-up actions

- [ ] Restore Fly billing eligibility, deploy the merged exact-main change,
  and verify the next VK batch against Google and OpenAI ledgers.
- [ ] Add `consumer=event_parse` and import correlation to OpenAI usage records
  in a separately reviewed observability change.
- [ ] Review the distinct high-volume `gpt-4o-mini` poster OCR lane.

## Release and closure evidence

- Deployed SHA: pending.
- Deploy path: `scripts/deploy_fly_main.sh` after Fly billing is restored.
- Regression checks: pending.
- Post-deploy verification: pending.
