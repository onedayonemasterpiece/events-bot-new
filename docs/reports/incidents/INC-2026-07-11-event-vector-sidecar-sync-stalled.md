# INC-2026-07-11 Event vector sidecar sync stalled

Status: monitoring
Severity: sev2
Service: event search / related-event vector sidecar
Opened: 2026-07-11
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-07-07-new-event-quality-degradation.md`, `INC-2026-07-02-static-search-92-percent-no-cards.md`
Related docs: `docs/features/static-site/event-search.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Production Supabase sidecar stopped receiving new event search documents and embeddings after 2026-07-02 while Fly SQLite continued receiving events. The existing vector audit could still report full semantic coverage because it generated ephemeral local embeddings; it did not persist that coverage to the production sidecar.

## User / Business Impact

- Smart search and related-event recall omit a substantial part of the current catalogue.
- Vector-first identity recall for new imports cannot retrieve events absent from the sidecar.
- Internal audit success could be mistaken for production index health.

## Detection

- Reported after comparing the latest stored embedding timestamp with the current event inventory.
- Initial evidence: latest indexed document `2026-07-02T15:51:19Z`; latest embedding `2026-07-02T15:57:08Z`.
- At detection, only 217 of 334 actionable current events had both `search_v3` and `related_v1` embeddings.
- Existing monitoring did not alert on index freshness, source-to-sidecar lag or coverage.

## Timeline

- 2026-07-02 15:57 UTC — last observed production embedding write.
- 2026-07-02 15:33 UTC — merge `7a21b4be` dropped the intended (but not production-enabled) Smart Update → StaticSiteBuilder handoff.
- 2026-07-02 15:51–15:57 UTC — one-off manual preview/backfill wrote 399 documents through event 6613 and masked the missing production owner.
- 2026-07-11 — gap detected; incident opened; root-cause investigation and catch-up started.
- 2026-07-11 08:21 UTC — first catch-up exposed an additional full-export latency defect: static hero-image dimension probes blocked vector projection; the run was explicitly superseded before sidecar mutation and the vector fast path was changed to skip remote image probes.

## Root Cause

1. Persistent vector ingestion had no enabled production owner or independent schedule; the July 2 rows came from a manual preview refresh.
2. The optional future Smart Update → StaticSiteBuilder handoff was then deleted by merge conflict resolution in `7a21b4be`, while its enum, documentation and a test survived.
3. PR CI installed dependencies but did not run tests, so the orphan handoff test did not gate the merge.

## Contributing Factors

- Persistent sidecar coverage and ephemeral audit-vector coverage were not reported separately.
- There was no freshness/coverage regression gate tied to the regular import/build path.
- The old preview-coupled design defaulted to only 50 events and could return exit 0 after a provider-call cap left embeddings missing.
- Runtime file logging was disabled by policy and there was no durable vector `ops_run` evidence.

## Automation Contract

### Treat as regression guard when

- changing event vector document construction, embedding generation, static build/export, Smart Update completion, scheduler routing, or Supabase sidecar writes.

### Affected surfaces

- `scripts/sync_event_search_vectors_to_supabase.py`
- Smart Update and VK auto-import completion paths
- static-site/Kaggle build scheduling and artifacts
- Supabase `event_search_documents` and `event_embeddings`
- Fly scheduler/runtime logs and `ops_run` evidence

### Mandatory checks before closure or deploy

- targeted unit/integration tests for incremental idempotent synchronization;
- freshness and coverage check against the current Fly event inventory;
- verify both `search_v3` and `related_v1` rows for newly imported events;
- live Telegram-UI VK auto-import of 1-3 candidates and stepwise Smart Update evidence;
- verify runtime evidence is sufficient to attribute each sync run and failure;
- deploy SHA must be reachable from `origin/main`.

### Required evidence

- deployed SHA and manual Fly deploy result;
- pre/post sidecar counts, max timestamps and missing-current-event counts;
- live E2E UI and runtime/DB evidence;
- compensating catch-up result for 2026-07-11.

## Immediate Mitigation

Implemented and deployed a dedicated full-catalog projection lane. Compensating
run `ops_run=3559` reconciled 335 current documents and both 335-vector kinds;
the immediate idempotency rerun `ops_run=3561` made zero provider calls.

## Corrective Actions

- Add coalesced post-Smart-Update `event_vector_sync` plus an independent three-hour reconciliation.
- Persist structured full-run counts/errors in `ops_run`; fail incomplete provider-cap runs.
- Prune projections absent from the authoritative current catalog.
- Restore the optional static-build handoff without making it the owner of vector freshness.
- Add live bounded Telegram-UI VK import evidence and per-row/run correlation logs.
- Align incoming identity vectors with clean canonical `related_v1` semantics.

## Follow-up Actions

- [ ] Add durable freshness/coverage alerting if not safely delivered in the incident fix.
- [x] Separate persistent-index coverage from ephemeral vector-audit coverage in operator docs/reports.
- [ ] Restore missing Behave VK/Smart Update feature files or remove the obsolete release-smoke references completely.
- [ ] Repository governance: audit/remove six long-lived `origin/hotfix/*` branches still reported ahead of `origin/main`; four contain patch-unique commits and need an owner decision rather than silent deletion.
- [ ] Test-suite baseline: add the missing NumPy test dependency and repair the pre-existing `main_part2.py` collection-time `VkActor` annotation NameError so full `pytest` can collect; incident-targeted suites are the release gate meanwhile.

## Release And Closure Evidence

- deployed SHA: `412b0311212e05b941b4a40be99ad8fdf160365d` (incident base fix `643444db376fdd072ed406cdb6ad3c0a804e59a4`)
- deploy path: manual `flyctl deploy --remote-only` from a clean worktree; SHA reachable from `origin/main`
- regression checks: 101 incident/relevant tests passed; full-suite collection remains blocked by pre-existing NumPy and `main_part2.VkActor` defects recorded below
- post-deploy verification: `/healthz` ready; Fly machine version 1620; production env enables the three-hour sync; `ops_run=3559` success with 335 docs, 344 changed/missing embeddings, 326 unchanged, 291 stale rows removed, zero cap remainder; post-check has 334/334 actionable and 275/275 strict-future coverage for both kinds, zero hash-contract issues; `ops_run=3561` completed in 8.043 s with 670 unchanged skips and zero provider calls
- pending closure gate: bounded Telegram-UI VK import and new-event vector follow-up; E2E account requires an explicitly approved temporary superadmin grant

## Prevention

- PR CI now compiles the incident-critical modules and runs vector/static/VK regression tests.
- Regular projection no longer depends on a manual preview or remote image probes.
- Durable `ops_run` evidence separates persistent coverage from ephemeral audits.
- VK runs with failed rows are no longer marked successful and carry end-to-end correlation identifiers.


## 2026-09-06 recurrence evidence

Protected voice recall audit found persisted projection frozen sinceSept4 while
canonical Fly imports continued. Root localization is a retained terminal static
claim, matching the August12 regression family. Evidence and constrained recovery
requirements: `INC-2026-09-06-voice-search-relevance.md`, section Recall audit.
No production catch-up or guard release was performed by the preview task; do not
interpret a successful voice classifier as restored index freshness.
