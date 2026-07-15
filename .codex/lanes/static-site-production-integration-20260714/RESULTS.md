# Lane L05 Results

## Status

committed

## Requirement IDs

- R01
- R02
- R03
- R04
- R05
- R06
- R07
- R08
- R09
- R10

## Branch

`integration/static-site-production-20260714`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-site-production-integration-20260714`

## Base SHA

`91a8e92741688b1298d3b234aecbe61994b18762`

## Head SHA

Implementation commit: `6c1dc3c1c9e73531bd2923a43af34ae8341f31db`

## Files changed

- shared desktop/mobile announcement lockup and final wide-`о` favicon;
- production event-detail desktop composition, media rail and efficient viewer;
- role-aware related cards and no-horizontal-crop document framing;
- LLM-first media-role schema/classifier and immutable WebP derivatives;
- production export/build checks, enqueue/backfill helper, docs and changelog;
- focused media/public-gate tests.

## Commands run

- `PREVIEW_BUILD_ID=preview-20260715t-static-prod-integration-v1 npm run build:preview`
- `PREVIEW_BUILD_ID=preview-20260715t-static-prod-integration-v1 npm run check:preview`
- `python3 -m py_compile db.py models.py media_dedup.py event_media.py scripts/enqueue_static_event_media_enrichment.py site/scripts/export-production-preview-data.py`
- `node --check site/scripts/build-preview.mjs`
- `node --check site/scripts/check-preview.mjs`
- `git diff --check`
- local Playwright desktop/mobile, grouped carousel, safe-boundary and related-card geometry checks.

## Tests / verification

- focused pytest suite: `35 passed` in a temporary requirements-complete venv;
- Astro build: `420` pages, complete;
- preview contract: passed;
- desktop `1536×864`: rail/CTA release leaves a `100px` shell-safe gap and the graphite feed is true full bleed;
- related documents: image width equals card media-shell width with zero left/right gap;
- efficient portrait viewer: previous and next both advance by viewport groups;
- mobile `390×844`: no horizontal overflow and no desktop side rail;
- Gemini 3.1 Pro (High) final visual/product audit: `PASS`, no blockers.

The base system Python does not include `pytest`; the successful focused run was
performed before cleanup in an isolated temporary venv. This is tooling state,
not a test failure.

## Risks

- Public/prod promotion and semantic-role backfill are release-lane work and
  remain open in R10 at this checkpoint.
- Legacy/missing semantic roles deliberately fail closed to width-fit/no crop;
  photo cover is unlocked only after exact `event_photo` classification.

## Merge notes

- Do not merge the stale header branch. The implementation copied only the
  final shared lockup/wordmark/favicon outcome from `8a1bbc59^..d9ccc527`.
- Promote by cherry-picking the implementation and orchestration commits onto a
  clean branch from the latest `origin/main`, then merge to `main` before any
  production deploy.
