# Focus auth product fixes results

## Status

Integration and local verification complete; immutable candidate release and
live hosted E2E are the remaining gate.

## Requirements

- R01–R13 from `EXECUTION_MATRIX.md`

## Branch and worktree

- Branch: `integration/focus-email-otp-e2e-20260729`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/focus-r15-e2e-final`
- Base SHA: `4db8374f`

## Delivered

- One-stage onboarding: install or continue, authenticate or skip, done, home.
- PWA launch resume from the real scoped root instead of the legacy preview hub.
- Per-page `Lab` score, issue report, optional private screenshot, and mass
  invitation share.
- Private Supabase feedback table/RPC and owner-scoped private Storage bucket.
- Canonical docs, E2E index, changelog, and deferred hero P0.

## Verification

- `npm --prefix site run test:focus-group-product`: 53/53 passed.
- `npm --prefix site run build`: 464 pages passed.
- PostgreSQL transactional contract:
  `supabase/tests/focus_group_feedback_contract.sql` passed.
- Anonymous live RPC probe: rejected.
- Local Chromium mobile flow:
  QR → install stage → manifest start → identity stage → skip → ordinary home.
- Local mobile page-family sweep: Lab panel visible on all mapped principal
  page families, with no horizontal overflow.

## Read-only lanes

- `focus_onboarding_map`: completed; identified the old PWA resume gap.
- `focus_feedback_map`: completed; mapped the shared layout and safe feedback
  insertion/data boundary.
- `focus_copy_ux_review`: completed; exact plain-language state copy applied.

## Remaining release gate

- Commit/push exact source.
- Build and publish one immutable noindex candidate.
- Repeat QR/PWA, email code/link, Yandex and authenticated feedback browser E2E.
- Send the exact candidate link and regenerated QR to Telegram.
