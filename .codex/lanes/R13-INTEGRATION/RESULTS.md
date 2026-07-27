# Lane R13-INTEGRATION Results

## Status
released to immutable noindex review; owner visual acceptance pending

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
Production page source: `f93fdd2c99339c7a935a4c6aa2627c827f73b5c9`.
Host recovery/control fix: `709eda27032122aca2f8d2b1e5464b2cc3289b58`.

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
- 68 focused release/status/handoff tests after the production receipt-lock
  regression
- exact immutable publication recovery and final public desktop/mobile QA

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
- Production SQLite: `quick_check=ok`; 21 public 2026 festival rows with 21
  unique slugs/orders (`9 announced`, `4 date-pending`, `8 program-pending`);
  58 legacy festival rows were not rewritten.
- Final candidate:
  `production-secret-20260727T004208-0af7c1de`,
  `1131` objects, `246` event pages, current receipt verified at
  `2026-07-26T23:54:49Z`.
- Public QA passed 19 linked route types at both `1440×900` and `390×844`,
  including 21 festival cards, three grounded club details, no broken festival
  images, no horizontal overflow, editable Search, chronological Today and the
  free medallion on event `6667`.
- Telegram review handoff: chat `-1004337049383`, reply to `548`, message
  `692`; read-back confirmed the exact text and 22 link entities.
- Gemini Pro acceptance is **blocked, not complete**. `a-gemini` exited before
  model execution with Antigravity account eligibility/location text. Official
  Antigravity documentation lists the Netherlands as supported, so the provider
  response is recorded without inferring the account's actual region. No
  Flash/Lite/Gemma substitute was used. Redacted evidence:
  `artifacts/codex/r13-gemini/`.

## Risks
Public root promotion remains outside this review release. The immutable
candidate is deliberately `noindex` and did not mutate root or stable ICS keys.
One ordinary Smart Update follow-up remains debounced in the outbox; it does
not alter or invalidate this immutable review URL.

## Merge notes
Both durable SHAs are in `origin/main`. Fly image
`deployment-01KYGC7D94F2272DFZG7C8DV9F` runs the receipt recovery fix, and
`STATIC_SITE_REPO_SHA` is pinned to `709eda27…` for subsequent builds.
