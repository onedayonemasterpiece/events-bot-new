# INC-2026-07-18 Social metrics postponed VK row rejected batch import

Status: open
Severity: sev2
Service: scheduled SocialMetricsCollector / static Popular feed signals
Opened: 2026-07-18
Closed: —
Owners: events-bot production
Related incidents: —
Related docs: `docs/features/post-metrics/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The Kaggle collector treated a matching postponed VK row returned by
`wall.getById` as already public even when its API `date` was still in the
future. Fly correctly rejected that `published` resolution, but strict atomic
validation then discarded all otherwise valid Telegram/VK observations in the
same result. Runs `social-metrics:991310` and `social-metrics:991311` failed
consecutively on 2026-07-18.

## User / Business Impact

- compact popularity snapshots stopped advancing after the successful
  `social-metrics:991309` import at 06:43 UTC;
- the public Popular feed remained available from existing data, but new
  reactions/views/shares and age buckets could become stale;
- each failed result contained 34 valid observations, so keeping the defect
  would repeatedly lose useful current-day signal while the postponed VK rows
  remained scheduled.

## Detection

- detected during a requested production data-collection and product-impact
  audit;
- `kaggle_run_ledger` showed two consecutive `import_failed` rows with
  `ValueError: invalid published VK resolution`;
- the latest Kaggle output was downloaded to the ignored investigation path
  `artifacts/codex/popular-metrics-20260718/latest/` and showed five direct VK
  matches whose `post_ts` exceeded `observed_ts + 900` seconds;
- `/healthz` remained ready, so the feature-level scheduled failure was not
  visible in the generic application readiness surface.

## Timeline

- 2026-07-18 06:43 UTC — `social-metrics:991309` imported successfully.
- 2026-07-18 07:30 UTC — `social-metrics:991310` failed during strict import.
- 2026-07-18 08:00 UTC — `social-metrics:991311` reproduced the same failure.
- 2026-07-18 08:02 UTC — latest Kaggle result isolated the future-dated direct
  postponed rows as the rejecting payload.

## Root Cause

1. `_resolve_vk` validated semantic title/date/time/place anchors but did not
   distinguish a scheduled postponed item from an already public wall item.
2. VK `wall.getById` returned exact stored postponed IDs with future `date`
   values, and the collector emitted them as `status=published`.
3. Fly's import boundary intentionally rejects future-dated published evidence
   and atomically rejects incomplete or invalid results, so valid observations
   in the same result were not partially written.

## Contributing Factors

- the original resolver fixture covered wrong stored IDs and bounded wall
  recovery, but not a semantically matching exact postponed row with a future
  publish timestamp;
- the feature scheduler had ledger evidence but no dedicated readiness issue
  for consecutive import failures.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/SocialMetricsCollector/social_metrics_collector.py::_resolve_vk`;
- changing VK postponed-to-live resolution, `wall.getById`/`wall.get` calls, or
  `_validated_vk_resolutions`;
- changing scheduled social-metrics result validation or import atomicity.

### Affected surfaces

- `kaggle/SocialMetricsCollector/social_metrics_collector.py`;
- `social_metrics_batch.py::_validated_vk_resolutions`;
- `social_metrics_kaggle.py::run_social_metrics_kaggle_batch`;
- Kaggle kernel `zigomaro/kenigevents-social-metrics-collector`;
- `kaggle_run_ledger`, `social_metric_snapshot`, static Popular export.

### Mandatory checks before closure or deploy

- focused SocialMetricsCollector/Kaggle tests pass;
- a future-dated exact direct row is not emitted as published;
- a valid already-public wall-scan fallback remains publishable;
- a real post-deploy scheduled or compensating run reaches
  `status=done, phase=imported` and imports current observations;
- both Kaggle resource leases release, temporary datasets are deleted, SQLite
  `quick_check=ok`, `/healthz ready=true`, and fresh logs contain no repeated
  `invalid published VK resolution`.

### Required evidence

- test output and clean commit;
- deployed SHA and Fly release;
- post-deploy `kaggle_run_ledger` row plus callback/resource-release logs;
- post-deploy snapshot freshness and application/database health;
- confirmation that deployed SHA is reachable from `origin/main`.

## Immediate Mitigation

- keep Fly's strict atomic validator unchanged;
- classify exact future-dated VK rows as not yet public and continue the single
  bounded wall scan instead of importing unverifiable publication evidence.

## Corrective Actions

- add a shared timestamp eligibility guard inside the Kaggle resolver for both
  direct and wall-scan items;
- add positive and negative regression controls for scheduled direct rows.

## Follow-up Actions

- [ ] Add a bounded consecutive-failure signal for `social_metrics_batch` to a
  feature-level scheduler/health diagnostic.
- [ ] Reconcile the historical non-terminal `social-metrics:991296` created row
  through the generic stale Kaggle-ledger recovery policy.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The collector now applies the same future-timestamp boundary as the strict Fly
validator before it can label provider evidence `published`, while retaining a
wall-scan positive control so the guard cannot become a blanket skip.
