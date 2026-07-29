# Focus auth product fixes results

## Status

Integration, immutable candidate release and live hosted E2E are complete.

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
- Immutable noindex candidate:
  `preview-20260729-focus-simple-r15-a5cc0256`.
- Hosted mobile flow:
  QR → install choice → PWA resume at identity → six-digit email OTP →
  ordinary home with hero and visible Lab panel.
- Hosted authenticated feedback: score, issue report and private screenshot
  upload passed; synthetic rows, object and test identity were removed after
  verification.
- Hosted mobile sweep: 15/15 mapped page families expose one Lab panel and
  have no horizontal overflow.
- Hosted desktop flow: ordinary home has hero, Lab panel and no horizontal
  overflow at 1440 px.
- The regenerated QR was decoded before sending and decoded again after
  Telegram delivery; messages `828` and `829` reply to thread anchor `548`.

## Read-only lanes

- `focus_onboarding_map`: completed; identified the old PWA resume gap.
- `focus_feedback_map`: completed; mapped the shared layout and safe feedback
  insertion/data boundary.
- `focus_copy_ux_review`: completed; exact plain-language state copy applied.

## Release result

- Exact source commit: `a5cc0256cad62c8de290beaf3d78daf646b06cd1`.
- Candidate:
  `https://kenigevents.ru/preview-20260729-focus-simple-r15-a5cc0256/`.
- Mass invitation:
  `https://kenigevents.ru/preview-20260729-focus-simple-r15-a5cc0256/fokus-gruppa/priglashenie/#invite=focus-group-2026-announcements`.
- Preview output gate has one pre-existing unrelated Pianissimo rail fixture
  mismatch (`5297`, expected gallery-count marker); focus-group product tests,
  build, hosted route checks and live focus E2E passed.
