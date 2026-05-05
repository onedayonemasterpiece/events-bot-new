# INC-2026-05-05 Smart Update Gemma 3 Fallback Hallucination

Status: open
Severity: sev2
Service: Smart Update / Google AI gateway
Opened: 2026-05-05
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-05-80-stories-source-coverage`
Related docs: `docs/llm/request-guide.md`, `docs/features/telegram-monitoring/README.md`

## Summary

During the `80 историй о главном` production backfill, Smart Update attempted to call `gemma-3-27b-it` for writer/topic stages. Google returned 404 because that provider model is no longer available for the configured API. The fallback path then tried `gpt-4o`; one create/writer attempt produced unrelated `ЕвроДэнс'90` content before Smart Update guardrails rejected the title/facts and the process was killed.

## User / Business Impact

- LLM-first imports can fail or become unsafe when the configured first-hop model is stale.
- Fallback to a different model family can introduce unrelated generated prose if the writer prompt is asked to complete a partially failed import.
- The `80 историй` backfill had to be rerun in deterministic-safe mode with `SMART_UPDATE_LLM_DISABLED=True`, so the missing rows were restored but not yet enriched by the intended LLM writer path.

## Detection

- Found during manual production Smart Update backfill on 2026-05-05.
- Runtime logs showed `404 models/gemma-3-27b-it is not found for API version v1beta`.
- A fallback writer response tried to inject unrelated `ЕвроДэнс'90` event text into an `80 историй` source.

## Timeline

- 2026-05-05 21:00 UTC: deployed source-ingestion/media fix from SHA `acc89995`.
- 2026-05-05 21:05 UTC: first production backfill with normal Smart Update LLM path started.
- 2026-05-05 21:08 UTC: Gemma 3 404 and unsafe fallback content observed; process killed before durable backfill `event_source` rows were created.
- 2026-05-05 21:15 UTC: safe production backfill rerun with `SMART_UPDATE_LLM_DISABLED=True`; missing source rows/events were restored.

## Root Cause

1. Smart Update still defaults some text stages to legacy `gemma-3-27b-it`.
2. The Google AI gateway did not fail closed for a model-not-found first hop in this surface.
3. Fallback generation was allowed to continue with a broad writer path instead of requiring source-grounded repair or explicit operator approval.

## Contributing Factors

- The `80 историй` source coverage incident required manual backfill before the Smart Update model chain was corrected.
- Existing guardrails caught the bad title/fact drift, but only after a fallback generation attempt.

## Automation Contract

### Treat as regression guard when

- Changing Smart Update writer/extract prompts or model chains.
- Changing Google AI gateway fallback policy.
- Running production backfills that call Smart Update with LLM enabled.

### Affected surfaces

- `smart_event_update.py`
- `google_ai/client.py`
- Smart Update writer/topic stages
- production backfill scripts and operator runbooks

### Mandatory checks before closure or deploy

- Verify active Smart Update model names against provider `ListModels` or a live minimal call.
- Add a focused test or smoke that a model-not-found first hop fails closed for Smart Update writer stages.
- Replay the `80 историй` offending sources with LLM enabled on a prod snapshot after the model chain fix.
- Confirm no unrelated fallback text appears in title, description, fact log, or Telegraph page.

### Required evidence

- deployed SHA reachable from `origin/main`;
- focused test output;
- prod-snapshot replay output for `kraftmarket39/140`, `kraftmarket39/193`, `kraftmarket39/202`, and `vk.com/wall-30777579_15138`;
- post-deploy health check.

## Immediate Mitigation

- Killed the unsafe production backfill before durable source rows were created.
- Reran the required source backfill in deterministic-safe mode to restore database coverage without fallback prose generation.

## Corrective Actions

- Open: replace/repair Smart Update model chain and make model-not-found fallback fail closed for writer stages.

## Follow-up Actions

- [ ] Fix Smart Update model configuration away from `gemma-3-27b-it`.
- [ ] Add writer-stage fallback guardrails so provider 404 does not continue into broad creative fallback.
- [ ] Rerun LLM-enabled Smart Update replay for the restored `80 историй` events after the model-chain fix.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep Smart Update model-chain changes tied to live provider compatibility smoke.
- Treat provider `NotFound` as a configuration incident, not as a normal retryable generation failure for writer stages.
