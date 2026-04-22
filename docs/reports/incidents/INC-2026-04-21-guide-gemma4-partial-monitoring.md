# INC-2026-04-21 Guide Gemma 4 Partial Monitoring

Status: monitoring
Severity: sev2
Service: guide excursions monitoring / scheduled guide digest
Opened: 2026-04-21
Closed: -
Owners: bot operations / guide excursions
Related incidents: -
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/llm/request-guide.md`, `docs/operations/cron.md`

## Summary

Scheduled guide-excursions monitoring started finishing as `partial` after the Gemma 4 rollout. The operator-facing Telegram UI reported `kaggle result marked as partial`, and scheduled digest publication risked being skipped even when the run had fresh material.

## User / Business Impact

- Operators saw guide monitoring as failed or degraded for scheduled runs on April 20-21, 2026.
- Fresh guide material could be imported but not reliably published because the scheduled path treats any Kaggle `partial` as an error gate.
- The report initially hid the exact provider errors behind a generic `kaggle result marked as partial` line.

## Detection

- Detected from Telegram operator report for `ops_run_id=774`, `run_id=f5cc85e89511`.
- Production `ops_run.details_json` and persisted Kaggle results under `/data/guide_monitoring_results/` were used as evidence.

## Timeline

- 2026-04-18 07:05 UTC: `ops_run_id=723`, `run_id=563f009fa425`, `success`: `llm_ok=20`, `llm_deferred=0`, `llm_error=0`.
- 2026-04-18 11:20 UTC: `ops_run_id=727`, `run_id=9132ef466ff7`, `success`: `llm_ok=19`, `llm_deferred=0`, `llm_error=0`.
- 2026-04-18 18:10 UTC: `ops_run_id=732`, `run_id=8b0fa1cc56ff`, `success`: `llm_ok=33`, `llm_deferred=0`, `llm_error=0`.
- 2026-04-19 07:05 UTC: `ops_run_id=737`, `run_id=bf8569cee486`, `success`: `llm_ok=18`, `llm_deferred=0`, `llm_error=0`.
- 2026-04-19 18:10 UTC: `ops_run_id=746`, `run_id=97921f0bd604`, `success`: `llm_ok=30`, `llm_deferred=0`, `llm_error=0`.
- 2026-04-19 18:30 UTC: guide digest issue `#36` was created and published successfully to `@wheretogo39` / `@youwillsee39`.
- 2026-04-20 07:03 UTC: Fly release `970` deployed shortly before the first observed `partial` run.
- 2026-04-20 07:05 UTC: `ops_run_id=751`, `run_id=dc4bbf72877d`, `partial`: `llm_deferred=7`, `llm_error=3`.
- 2026-04-20 11:20 UTC: `ops_run_id=755`, `run_id=fc89c94e492d`, later marked `crashed` by startup cleanup with no source reports or results path.
- 2026-04-20 18:10 UTC: `ops_run_id=765`, `run_id=d299a50d73c0`, `partial`: one deferred timeout on `@vkaliningrade/4661`, while 11 occurrences were extracted.
- 2026-04-21 07:05 UTC: `ops_run_id=770`, `run_id=bfb07004c5e4`, `success`.
- 2026-04-21 11:20 UTC: `ops_run_id=774`, `run_id=f5cc85e89511`, `partial`: one provider error on `@vkaliningrade/4674`.
- 2026-04-21 18:10 UTC: `ops_run_id=779`, `run_id=a13e5f3e1d35`, `partial`: one provider error on `@twometerguide/2908`, while 10 occurrences were extracted.
- 2026-04-22 07:05 UTC: `ops_run_id=784`, `run_id=47313bc11072`, `success`: `llm_ok=16`, `llm_deferred=0`, `llm_error=0`, `occurrences_updated=4`.
- 2026-04-22 07:53 UTC: deployed `c1f6f966` / Fly release `977`, separating non-fatal Kaggle partials into warnings so scheduled digest is not blocked by isolated post-level LLM errors.
- 2026-04-22 08:10 UTC: deployed `059317cc` / Fly release `978`, adding bounded local Gemma 4 timeouts for digest preview stages.
- 2026-04-22 08:13 UTC: attempted sidecar catch-up on the 1024 MB Fly machine OOM-killed the app process before `guide_digest_issue` creation; sidecar catch-up must not be run beside the main runtime at this size.
- 2026-04-22 08:18 UTC: after temporary scale-up to 2048 MB, compensating catch-up published guide digest issue `#37` for occurrence `#134` (`Тайны Северной горы. Часть 2`) to `@wheretogo39` and `@youwillsee39`.
- 2026-04-22 08:20 UTC: memory was scaled back to 1024 MB; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.

Production `guide_digest_issue` evidence: after `#36` on 2026-04-19 18:30 UTC, no guide digest issues were created through the inspected 2026-04-22 window. This means the scheduled digest path did not reach `build_guide_digest_preview()` for the partial `full` runs; it returned earlier on the scheduler/import error gate.

Compensating publish evidence: issue `#37` was created at `2026-04-22 08:18:57 UTC`, marked `published` at `2026-04-22 08:18:59 UTC`, and `guide_occurrence #134` now has `published_new_digest_issue_id=37`; remaining eligible unpublished `new_occurrences` in the digest window: `0`.

## Error Inventory Since Gemma 4 Rollout

Production runs from 2026-04-18 through 2026-04-22 show three post-level error families:

- `schema_anyof`: 3 occurrences, all in `ops_run_id=751`, all `@vkaliningrade` posts (`4674`, `4673`, `4669`), exact message `Provider error: ValueError: Unknown field for Schema: anyOf`.
- `llm_deferred_timeout`: 8 occurrences:
  - `ops_run_id=751`: `@tanja_from_koenigsberg/3979`, `@katimartihobby/1934`, `@twometerguide/2914`, `2913`, `2910`, `2908`, `2904`;
  - `ops_run_id=765`: `@vkaliningrade/4661`.
- `provider_500`: 2 occurrences:
  - `ops_run_id=774`: `@vkaliningrade/4674`, exact message `Provider error: InternalServerError: 500 Internal error encountered.`;
  - `ops_run_id=779`: `@twometerguide/2908`, exact message `Provider error: InternalServerError: 500 Internal error encountered.`.

Timeouts were not limited to huge posts: observed timed-out post text lengths ranged from about 315 chars (`@tanja_from_koenigsberg/3979`) to about 2408 chars (`@twometerguide/2913`), so length alone does not explain the failure.

## Root Cause

1. The first Gemma 4 production run used native `response_schema`, but the guide runner still passed a provider-incompatible schema with `anyOf` in `_single_occurrence_wrapper_schema`. Gemma 4 / `google.generativeai` rejected that as `ValueError: Unknown field for Schema: anyOf`.
2. Later partial runs were triggered by transient provider failures on individual posts: `Provider error: InternalServerError: 500 Internal error encountered.`
3. The scheduled guide path marks the whole Kaggle result as `partial` when any single LLM post fails or defers, and the downstream digest path treats this too broadly instead of publishing eligible fresh material from successful posts.

## Contributing Factors

- Native structured output was enabled for Gemma 4 before the guide schema subset was fully provider-compatible.
- Provider 500/timeouts are recorded per source/post in the Kaggle bundle, but the Telegram summary collapses them to `kaggle result marked as partial`.
- The digest gate couples publish eligibility to run-level success instead of occurrence-level freshness and extraction quality.
- `ask_gemma()` retries explicit provider `retry after ... ms` hints, and `GoogleAIClient` retries retryable provider errors internally, but the Kaggle wrapper does not currently retry `asyncio.TimeoutError` and does not add a second bounded retry for provider 5xx after the client has exhausted its short internal retry loop.
- Scheduled digest auto-publish in `scheduling._run_scheduled_guide_excursions()` checks `not result.errors`; `run_guide_monitor()` adds `kaggle result marked as partial` to `errors` for any Kaggle `partial=true`, so a single post-level LLM failure suppresses auto-publish for the whole scheduled `full` run.
- Guide digest writer/enrich/dedup are Gemma 4 by default in current code and production env has no override: `GUIDE_DIGEST_WRITER_MODEL`, `GUIDE_OCCURRENCE_ENRICH_MODEL`, and `GUIDE_EXCURSIONS_DEDUP_MODEL` resolve to `gemma-4-31b` on the guide key (`GOOGLE_API_KEY2`). `4o` is not used in the guide pipeline.
- One-off prod sidecar scripts that import `guide_excursions.service` can double the app memory footprint because `digest_writer` resolves the shared gateway through `main`; on the 1024 MB Fly machine this caused OOM during manual catch-up. Prefer the running bot path or temporarily scale memory before sidecar catch-up.

## Automation Contract

### Treat as regression guard when

- Changing guide Gemma model routing, `response_schema`, or `GoogleAIClient` structured-output handling.
- Changing guide scheduled monitor import, `partial` status semantics, or scheduled auto-publish logic.
- Changing `/guide_report`, `/guide_runs`, or guide run observability.

### Affected surfaces

- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`
- `google_ai/client.py`
- `guide_excursions/service.py`
- `guide_excursions/kaggle_service.py`
- scheduled guide monitoring / auto-publish path in `scheduling.py`
- production `/data/guide_monitoring_results/*/guide_excursions_results.json`

### Mandatory checks before closure or deploy

- Verify no guide response schema passed to Gemma 4 contains provider-unsupported keys such as `anyOf`.
- Run a guide monitor smoke or live scheduled-equivalent run with `llm_error=0` for the schema path.
- Simulate or verify a single-post provider failure/deferred result does not suppress digest publication when eligible fresh occurrences were imported from other posts.
- Verify bounded retry behavior for `llm_deferred_timeout` and provider `5xx` in the Kaggle guide wrapper without hiding terminal failures from `/guide_report`.
- Verify `/guide_report <ops_run_id>` exposes source/post-level LLM error details, not only the generic run-level partial marker.
- For scheduled daily/full runs, confirm same-day fresh material is either published or explicitly reported as no eligible digest candidates.

### Required evidence

- `ops_run_id` / `run_id` evidence for the failing and fixed runs.
- Persisted Kaggle results path or equivalent log snippets showing exact source/post errors.
- Regression test or live smoke evidence for fail-open digest behavior.
- Deployed SHA reachable from `origin/main` if corrective code is deployed.

## Immediate Mitigation

- Current production evidence on 2026-04-22 07:05 UTC shows a clean scheduled light run: `ops_run_id=784`, `run_id=47313bc11072`, `llm_ok=16`, `llm_deferred=0`, `llm_error=0`.
- The schema-level `anyOf` issue is no longer present in current `origin/main` / `af33b146`; `_single_occurrence_wrapper_schema` now returns a plain object wrapper.

## Corrective Actions

- Implemented: guide scheduled digest can publish eligible fresh material even if the scan result is `partial` due to unrelated post-level LLM errors; those partials are recorded as warnings instead of blocking `result.errors`.
- Implemented: run completion, `/guide_report`, and `/guide_runs` keep warning/`llm_error` visibility.
- Implemented: bounded retry around guide Gemma timeouts/provider 5xx at the Kaggle wrapper layer, with per-attempt diagnostics.
- Implemented: local Gemma 4 digest-preview stages (`enrich`, `dedup`, `digest_writer`) have bounded per-call timeouts and fall back to existing deterministic content/heuristics, so catch-up cannot hang before `guide_digest_issue` creation.

## Follow-up Actions

- [x] Add regression coverage for Gemma 4-compatible guide response schemas.
- [x] Add regression coverage for scheduled guide auto-publish with mixed successful occurrences and one provider failure.
- [x] Add regression coverage for local Gemma 4 digest-preview calls being timeout-bounded.
- [ ] Add regression coverage for timeout/provider-5xx retry classification in the Kaggle guide runner.
- [ ] Decide whether provider 500/timeout on a post with a past date should be downgraded to non-blocking warning after import, while preserving visibility in reports.

## Release And Closure Evidence

- deployed SHAs: `c1f6f966` (`fix(guide): keep digest publishing after nonfatal partials`), `059317cc` (`fix(guide): bound digest Gemma calls`), both reachable from `origin/main`.
- deploy path: manual `fly deploy -a events-bot-new-wngqia`; latest Fly release `978`, image `deployment-01KPT3T9CQ6MR0CAXM7EP8P5NW`, machine `48e42d5b714228` started.
- regression checks: `python -m pytest tests/test_scheduling_guide_digest.py tests/test_guide_kaggle_schema_contract.py tests/test_guide_local_llm_timeout_contract.py -q` -> `5 passed`; `python -m py_compile guide_excursions/service.py scheduling.py kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py guide_excursions/llm_support.py guide_excursions/enrich.py guide_excursions/dedup.py guide_excursions/digest_writer.py`; `git diff --check`.
- post-deploy verification: `fly status` shows release `978` started; `/healthz` returned `{"ok": true, "ready": true, "db": "ok", "issues": []}` after catch-up and scale-back.
- compensating catch-up: guide digest issue `#37`, published targets `@wheretogo39` message `47` and `@youwillsee39` message `65`; remaining eligible unpublished `new_occurrences`: `0`.

## Prevention

- Keep the guide Gemma 4 schema subset limited to provider-supported JSON schema fields.
- Keep guide digest publication gated by occurrence-level eligibility, not by a blanket run-level `partial` marker.
- Preserve source/post-level LLM diagnostics in both persisted results and Telegram operator surfaces.
