# R15-DAILY-SHARE-FINAL results

- **Status:** Done
- **Requirements:** daily service-share enqueue guarantee; in-Kaggle legacy preview contract gate
- **Branch:** `agent/r15-daily-share-final`
- **Base SHA:** `11d8c9846432414020cc5201eb650f5cfbf38eba`
- **Implementation head SHA:** `18779efc15a305655197453f9ca9d8f02b9e1d70`

## Scope contract

Writable implementation scope: `main.py`, `static_site_release.py`,
`kaggle/StaticSiteBuilder/static_site_builder.py`,
`tests/test_static_site_release.py`, and new focused tests. No scheduler,
semantic/exporter, site UI, canonical documentation, or `CHANGELOG.md` files
were edited.

## Outcome

### Daily service-share freshness

- Reused the existing, sole scheduler mechanism:
  `scheduling._enqueue_static_site_calendar_refresh`, startup catch-up, and
  coalesced `static_site_calendar_rollover` at Europe/Kaliningrad midnight.
  No second scheduler or root cutover was added.
- Every enabled StaticSiteBuilder request records the local calendar date and a
  stable `service-share-daily:<timezone>:<date>` force-fingerprint marker, so a
  legitimate Smart Update build covers that day's daily share.
- Startup and midnight daily triggers use the existing SQLite `BEGIN IMMEDIATE`
  outbox/coalesce transaction to detect the durable marker. Same-day requests
  return `daily-already-requested` without inserting or rearming work.
- A next-day request replaces the marker, changes request watermark/fingerprint
  evidence, and reuses the single coalesced row. It does not use the
  operator-only `force_rebuild` escape.
- Disabled builder returns `disabled` before writing an outbox row.

### In-Kaggle preview contract

- Production-candidate runs now execute, before the root build:
  `npm run build:preview` and `npm run check:preview` under an isolated
  `PREVIEW_BUILD_ID=preview-gate-<build>` environment.
- The subsequent production build must clear the ephemeral preview output.
  The gate is never archived or published and adds no artifact kind.
- Build result checks include structured
  `preview_contract: {status: ok, archived: false, published: false}`.
  Host release validation fails closed on missing/failed or leaked evidence.

## Changed files

- `main.py`
- `static_site_release.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `tests/test_static_site_release.py`
- `tests/test_static_site_daily_share_enqueue.py`
- `tests/test_static_site_builder_preview_contract.py`
- `.codex/lanes/R15-DAILY-SHARE-FINAL/RESULTS.md` (evidence only)

## Validation

```text
python3 -m py_compile main.py static_site_release.py \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  tests/test_static_site_daily_share_enqueue.py \
  tests/test_static_site_builder_preview_contract.py \
  tests/test_static_site_release.py

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_static_site_build_debounce.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_daily_share_enqueue.py \
  tests/test_static_site_builder_preview_contract.py \
  tests/test_static_site_release.py

git diff --check
```

Result: **62 passed**, compile and whitespace checks passed.

Focused coverage includes first daily enqueue, terminal same-day no-op, next-day
requeue and changed force marker/watermark, two same-day Smart Updates without
suppression, Smart Update coverage of the daily share, disabled builder, preview
gate environment isolation/order, and host rejection of incomplete or archived
preview gate evidence.

## Risks / integration gates

- The exact npm commands are contract-tested with a mocked runner; the full
  Kaggle CPU production-candidate execution remains the integration acceptance
  gate because this lane does not have the staged Kaggle source/runtime.
- The implementation assumes the existing `build:production` clean-output
  contract. It fails closed if the prior `preview-gate-*` directory survives.
- Canonical docs and `CHANGELOG.md` remain owned by the integration/docs lane.

## Merge notes

Merge implementation commit `18779efc15a305655197453f9ca9d8f02b9e1d70`
plus the following results-only metadata commit.
