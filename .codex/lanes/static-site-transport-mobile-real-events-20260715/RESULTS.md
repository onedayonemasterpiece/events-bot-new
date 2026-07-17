# Lane L01 Results

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

## Branch

`integration/static-site-transport-mobile-real-events-20260715`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-site-transport-mobile-real-events-20260715`

## Base SHA

`8ecff8ab` plus accepted mobile commits through `e2b700db`

## Head SHA

Implementation commit: `9d669856`

## Files changed

Accepted production event UI, mobile v4, transport components/data/assets/ICS,
fresh event/related manifests, Smart Update build debounce, tests and canonical
documentation.

## Commands run

- production SQLite snapshot export with pgvector related refresh;
- `PREVIEW_BUILD_ID=preview-20260715t-production-transport-mobile-real-events-v1 npm --prefix site run build:preview`;
- `npm --prefix site run check:preview`;
- bus/rail directory checks;
- focused pytest, Python/Node syntax and `git diff --check`;
- public Playwright at `1920×1080` and `390×844`;
- exact prefix-only Object Storage cleanup.

## Tests / verification

- `4 passed`;
- `282` public events and `282 × 40` non-dangling related chains;
- `0` provider calls, `564` unchanged embeddings reused;
- public event/transport ICS: `200 text/calendar`;
- desktop/mobile overflow: `0`;
- current preview remains HTTP 200 after cleanup;
- 19 unreferenced pre-July prefixes removed, stable `/p` and `/ics` untouched.
- Telegram handoff verified in `KenigEvents · UI review`, topic `37`, messages
  `59–62`.

## Risks

Automatic Kaggle artifact generation is staged in `fly.toml` with
`ENABLE_STATIC_SITE_KAGGLE_BUILDER=1` and pgvector mode, but is not active until
this branch is merged and deployed.
Atomic production-root promotion remains a separate release gate.

## Merge notes

Review and merge this combined branch; do not merge the historical transport or
stale header branches independently.
