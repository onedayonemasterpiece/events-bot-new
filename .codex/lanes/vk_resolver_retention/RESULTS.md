# RESULTS — vk_resolver_retention

- Requirements: R02, R03, R04
- Mode: read-only discovery
- Status: completed

## Findings accepted by the integrator

- Postponed-to-live resolution belongs in the existing Kaggle batch: direct
  `wall.getById` chunks, followed by at most one shared bounded wall scan.
- Fly must export compact candidates and revalidate strict matching evidence
  before writing `event_publication`; raw wall history is never persisted.
- Result coverage must equal manifest coverage; a missing result row is not a
  successful partial run.
- The fixed kernel needs a deterministic atomic per-slot claim because the
  generic run-config upsert otherwise rotates the callback token on a race.
- Dataset refs must be visible to failure cleanup; previously an exception in
  `_launch_sync` lost the local refs and leaked private temporary datasets.
- Storage remains bounded to four social buckets per post, legacy age-day keys
  and one upserted publication row, with the existing rolling cleanup.
- Both managed TG channels and both managed VK groups are one owned publisher
  family; component-wise max prevents internal distribution from inflating
  totals or creating false multi-source popularity.
