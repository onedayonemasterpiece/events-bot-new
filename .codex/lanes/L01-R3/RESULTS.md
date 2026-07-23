# Lane L01 Results

## Status

committed

## Requirement IDs

- R01

## Branch

`agent/r3-duration-forecast/L01`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/r3-duration-forecast`

## Base SHA

`68576d5b70f57164c00386b05cff126586c3f700`

## Head SHA

Implementation commit: `54365911c2462b86c8b1cf336864f2e442f632b4`

The final lane tip also contains this results record and is reported to the
integrator in the handoff message.

## Files changed

- `db.py`
- `models.py`
- `smart_event_update.py`
- `site/scripts/export-production-preview-data.py`
- `site/src/lib/desktopEventTransport.ts`
- `site/src/lib/types.ts`
- `tests/test_smart_update_duration_forecast.py`
- `tests/test_static_site_preview_duration.py`
- `.codex/lanes/L01/RESULTS.md`

## Delivered behavior

- Added nullable `event.duration_forecast_minutes` to the ORM model, new-table
  DDL, and idempotent SQLite startup migration.
- Added narrow, fail-closed transport eligibility derived from the same checked
  rail/bus schedule directories used by the static site. Eligibility requires a
  real date/start time, a single-day event, and a supported rail city, exact bus
  route venue/start, or the reviewed Kaup venue.
- Smart Update uses its existing `_ask_gemma_json` production provider path,
  model routing, shared provider/key chain, limiter and retry/timeout behavior.
  No provider/API call was added to StaticSiteBuilder or the static exporter.
- A source-labelled duration, explicit time range, or explicit multi-day end
  prevents prediction. A newly extracted duration also clears a stale forecast.
- Forecasts are validated to 15–720 minutes and persisted only at confidence
  `>= 0.5`; provider failure/insufficient evidence leaves the field `NULL`.
- The exporter only validates and exports the stored value. The transport
  projection resolves an existing explicit end first, then a source-labelled
  duration, then `duration_forecast_minutes`, and otherwise leaves the event
  unchanged for the existing safe schedule-cutoff/null fallback.

## Commands run

```text
git status --short --branch
git rev-parse HEAD
python3 -m py_compile smart_event_update.py models.py db.py
/home/dev/.venvs/events-bot-image-geometry/bin/pytest -q \
  tests/test_smart_update_duration_forecast.py \
  tests/test_static_site_preview_duration.py
/home/dev/.venvs/events-bot-image-geometry/bin/pytest -q \
  tests/test_static_site_transport_experiment.py \
  tests/test_static_site_bus_boarding.py \
  tests/test_static_site_preview_duration.py \
  tests/test_event_age_rating_db.py::test_event_age_columns_and_roundtrip \
  tests/test_smart_event_update_date_media_helpers.py::test_conservative_date_update_allows_only_safe_merge_cases \
  tests/test_smart_update_native_schema.py::test_g4_split_create_disables_4o_fallback_for_experimental_stages
/home/dev/.venvs/events-bot-image-geometry/bin/python -m py_compile \
  smart_event_update.py models.py db.py \
  site/scripts/export-production-preview-data.py
git diff --check
```

A copied pre-change `db.sqlite` was also initialized through `Database.init()`.
Before init it had no forecast column; afterward:

```text
(67, 'duration_forecast_minutes', 'INTEGER', 0, None, 0)
```

This proves the existing-snapshot `ALTER TABLE` path is nullable and
backward-compatible.

## Tests / verification

- Targeted duration suite: **9 passed**.
  - extracted duration skips provider prediction;
  - non-transport event skips provider prediction;
  - eligible missing-duration event calls the production wrapper contract and
    round-trips the forecast through SQLite;
  - actual Smart Update create flow commits the field;
  - newly extracted duration clears a stale forecast without a provider call;
  - export validation rejects invalid forecasts;
  - static transport ordering is explicit end/source duration → forecast →
    unchanged safe fallback.
- Related schema/static transport slice: **18 passed**.
- `py_compile`: passed.
- `git diff --check`: passed.
- A broader opportunistic Smart Update slice produced **98 passed, 6 failed**.
  All six failures occur before the new duration hook and are baseline/date or
  pre-existing mocked-label expectations:
  two fixture dates are now past on 2026-07-23, and four tests reject existing
  `anchor_role_review` / `create_bundle_grounding` labels while expecting only
  `eventness_review`. No failure involved `duration_forecast`.

## Integration cleanup for obsolete R2 preview estimates

The L01 base does **not** contain
`site/src/data/event-duration-estimates.json` or its import/use, so no deletion
could be committed on this branch. The integration branch includes older commit
`5cfdc4df`, and must apply this replacement before accepting R01:

1. Delete `site/src/data/event-duration-estimates.json`.
2. In `site/src/lib/eventTransport.ts`, remove:
   - `durationEstimatesData` import;
   - `EventDurationEstimateRecord`, `EventDurationEstimateData`, and
     `EventDurationEstimate`;
   - `durationEstimates`, `previewDurationEstimatesEnabled`, and
     `previewDurationEstimate(...)`;
   - the `llm_estimated` `EventEndBasis` variant and `durationEstimate` result
     field;
   - every preview-estimate conditional that changes calendar coverage or
     exposes model/provenance.
3. Keep `event.time_range_end` as the only non-cutoff input inside
   `getEventTransportSuggestion`. L01 projects the persisted Smart Update
   forecast into this value only after explicit timing/source duration loses,
   so no second static estimate source is needed.
4. In `site/src/components/EventTransportSchedule.astro`, remove
   estimate/provider/provenance/range diagnostics, `data-duration-estimate-*`,
   `data-predicted-event-end`, and “прогноз ИИ” service copy. Keep only a neutral
   practical schedule rationale and the existing organizer-time caution where
   applicable.
5. Delete or rewrite `site/tests/event-transport-estimates.test.mjs` so it tests
   the persisted exported forecast path rather than the removed static JSON.

The expected final invariant is one predicted-duration source:
`Smart Update -> event.duration_forecast_minutes -> preview export -> transport
fallback`. There must be no build-time/provider/static JSON enrichment.

## Risks

- Existing eligible rows are not bulk-backfilled; they receive a forecast on a
  later Smart Update. This avoids an unrequested provider sweep.
- If the checked static transport directory is absent from a runtime image,
  eligibility fails closed and makes no provider call.
- The integration cleanup above is mandatory because the obsolete R2 commit is
  outside this lane base; keeping it would violate the sole-source requirement.

## Merge notes

- Cherry-pick the final L01 tip, not only the implementation commit, so this
  evidence record is retained.
- Resolve the known `5cfdc4df` static estimate conflict according to the exact
  cleanup above.
- Integrator owns canonical docs and `CHANGELOG.md`; this lane intentionally did
  not edit them.
