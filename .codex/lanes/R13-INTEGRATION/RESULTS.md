# Lane R13-INTEGRATION Results

## Status
implementation-complete; release-pipeline pending

## Requirement IDs
- R13-01
- R13-02
- R13-03
- R13-04
- R13-05

## Branch
integration/festivals-production-r13-20260726

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/r13-production`

## Base SHA
`7ba887a9`

## Head SHA
`0abe04ab` plus the final release-contract follow-up commit.

## Files changed
See final integration commit.

## Commands run
- focused Node behavior suites
- 81 focused Python tests
- full preview build/check (432 pages)
- Playwright desktop/mobile festival and Today-order probes
- production export from a local copy of the current Fly SQLite database
- production-root build/check (420 Astro routes, 275 event pages, 1212 files)
- immutable secret-candidate build/check (1217 files)
- Chromium release gate, including festival desktop/mobile and crop/keyboard checks
- agy Gemini Pro acceptance attempt plus official Antigravity eligibility research

## Tests / verification
- Local copied production DB: `quick_check=ok`; 21 unique calendar rows; repeat
  dry-run reports 21 unchanged.
- Production desktop contract: 275/275 event pages passed.
- Production artifact: `ADD-BUILD-07/09` passed.
- Secret candidate: `ADD-BUILD-07/10` passed, including candidate-wide
  `noindex,nofollow,noarchive,nosnippet` and browser-safe Search config.
- Browser release gate: all nine checks passed; festival page has 21 cards,
  zero broken images and zero horizontal overflow at 1440x900 and 390x844.
- Mobile Today order probe starts `10:00, 10:00, 11:00, 12:00...`.
- Gemini Pro acceptance is **blocked, not complete**. `a-gemini` exited before
  model execution with Antigravity account eligibility/location text. Official
  Antigravity documentation lists the Netherlands as supported, so the provider
  response is recorded without inferring the account's actual region. No
  Flash/Lite/Gemma substitute was used. Redacted evidence:
  `artifacts/codex/r13-gemini/`.

## Risks
Public root promotion remains blocked by the absent reader-atomic publisher and is outside this implementation.
Production DB mutation, Kaggle generation and immutable upload still require the
clean `origin/main` release phase; local validation never wrote to Fly `/data/db.sqlite`.

## Merge notes
Merge the final follow-up after tests, then run DB backup/migration/backfill and
the canonical Kaggle builder from the exact `origin/main` SHA.
