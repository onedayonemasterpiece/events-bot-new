# INC-2026-04-21 Guide Gemma 4 Partial Monitoring

Status: open
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

- 2026-04-20 07:05 UTC: `ops_run_id=751`, `run_id=dc4bbf72877d`, `partial`: `llm_deferred=7`, `llm_error=3`.
- 2026-04-20 11:20 UTC: `ops_run_id=755`, `run_id=fc89c94e492d`, later marked `crashed` by startup cleanup with no source reports or results path.
- 2026-04-20 18:10 UTC: `ops_run_id=765`, `run_id=d299a50d73c0`, `partial`: one deferred timeout on `@vkaliningrade/4661`, while 11 occurrences were extracted.
- 2026-04-21 07:05 UTC: `ops_run_id=770`, `run_id=bfb07004c5e4`, `success`.
- 2026-04-21 11:20 UTC: `ops_run_id=774`, `run_id=f5cc85e89511`, `partial`: one provider error on `@vkaliningrade/4674`.
- 2026-04-21 18:10 UTC: `ops_run_id=779`, `run_id=a13e5f3e1d35`, `partial`: one provider error on `@twometerguide/2908`, while 10 occurrences were extracted.
- 2026-04-22 07:05 UTC: `ops_run_id=784`, `run_id=47313bc11072`, `success`: `llm_ok=16`, `llm_deferred=0`, `llm_error=0`, `occurrences_updated=4`.

## Root Cause

1. The first Gemma 4 production run used native `response_schema`, but the guide runner still passed a provider-incompatible schema with `anyOf` in `_single_occurrence_wrapper_schema`. Gemma 4 / `google.generativeai` rejected that as `ValueError: Unknown field for Schema: anyOf`.
2. Later partial runs were triggered by transient provider failures on individual posts: `Provider error: InternalServerError: 500 Internal error encountered.`
3. The scheduled guide path marks the whole Kaggle result as `partial` when any single LLM post fails or defers, and the downstream digest path treats this too broadly instead of publishing eligible fresh material from successful posts.

## Contributing Factors

- Native structured output was enabled for Gemma 4 before the guide schema subset was fully provider-compatible.
- Provider 500/timeouts are recorded per source/post in the Kaggle bundle, but the Telegram summary collapses them to `kaggle result marked as partial`.
- The digest gate couples publish eligibility to run-level success instead of occurrence-level freshness and extraction quality.

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

- Pending: make guide scheduled digest publish eligible fresh material even if the scan result is `partial` due to unrelated post-level LLM errors.
- Pending: improve report/import surfacing so source/post-level LLM errors are visible in operator summaries.

## Follow-up Actions

- [ ] Add regression coverage for Gemma 4-compatible guide response schemas.
- [ ] Add regression coverage for scheduled guide auto-publish with mixed successful occurrences and one provider failure.
- [ ] Decide whether provider 500 on a post with a past date should be downgraded to non-blocking warning after import, while preserving visibility in reports.

## Release And Closure Evidence

- deployed SHA: pending for remaining corrective actions
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep the guide Gemma 4 schema subset limited to provider-supported JSON schema fields.
- Keep guide digest publication gated by occurrence-level eligibility, not by a blanket run-level `partial` marker.
- Preserve source/post-level LLM diagnostics in both persisted results and Telegram operator surfaces.
