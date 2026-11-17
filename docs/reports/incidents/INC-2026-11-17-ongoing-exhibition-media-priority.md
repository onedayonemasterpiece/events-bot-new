# INC-2026-11-17 Ongoing exhibition media review loses publication priority

Status: open
Severity: sev2
Service: Events JobOutbox / initial VK publishing
Opened: 2026-11-17
Closed: —
Owners: KenigEvents (codex)
Related incidents: INC-2026-11-10-vk-event-multiphoto, INC-2026-06-28-vk-stale-event-publication
Related docs: [VK publishing](../../features/vk-publishing/README.md), [release governance](../../operations/release-governance.md)

## Summary and impact

An active exhibition beginning 29 October and ending 25 November did not receive
its initial announcement. Its pending media review was ranked as a past event,
while the publication repeatedly deferred on that existing dependency.

## Detection and timeline

- 2026-11-16 17:14 UTC: media review 160189 for event 11100 last updated, attempts zero.
- 2026-11-17 07:12 UTC: read-only investigation found VK job 160191 repeatedly waiting on that review.
- 2026-11-17 07:16 UTC: actual container source matched main 193578780; targeted logs showed future reviews running while the ongoing exhibition was never picked.
- Baseline regression: ongoing, ends-today and date-range cases failed; future and expired controls passed.
- Technical review found that an inferred future end date could promote an already-started one-day event. The added inferred-end control failed before correction (1 FAIL / 5 PASS).

## Root cause and classification

Mechanical scheduling defect: `_run_due_jobs_once` used only the start date for
media eligibility. It omitted canonical `end_date` and did not use the existing
publication-span helper. This changes no event meaning, model verdict or source
date. A worker handling one job per pass and continuing future work exposed the
wrong ordering; there is no global media-worker outage.

## Automation contract

### Treat as regression guard when

Changing media-review ordering, initial-publication dependencies or publication-span eligibility.

### Affected surfaces

The existing JobOutbox due-job sort; no queue schema, configuration or provider contract changes.

### Mandatory checks before closure or deploy

- Actual due runner: future, ongoing, ends-today, canonical date range, expired and inferred-end event controls.
- Existing CAPTCHA pause, fresh worker selection and due-filter tests.
- Required CI and normal exact-main Events-only release.
- Observe ordinary review completion and initial publication, without manual queue reset or replay.

### Required evidence

Retained investigation and baseline/fixed logs under the existing
`20261112T111614Z-similar-events-check` task directory; release SHA/image and live receipts after deployment.

## Corrective actions and prevention

Select the canonical end date and reuse `_event_vk_publish_end_date` for media
eligibility. Select time and inferred-end provenance so the existing
`_event_start_has_passed_for_publication` rule excludes already-started timed
one-day events, matching the stale-publication incident's existing contract.
Preserve the existing start-date ordering, dependency and rate
limits. Extend the existing due-runner test with the distinguishing cases.
No manual mitigation or production data changes were performed.

## Release and closure evidence

Code changed locally; release not started. Incident remains open until the normal
media and publication receipts are observed after release.

## Follow-up actions

- KenigEvents: complete CI, release from main, then inspect ordinary event 11100 receipts.
- Investigate repeated materialization failures for event 10557 separately; they are not the eligibility root cause.
