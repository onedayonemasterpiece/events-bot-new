# Focus-group product prototype — integration report

## Scope

- Base: `origin/main` at `9ee8f56f6e822542d9af62d7bf7c532d5e10e032`.
- Integration branch:
  `integration/static-site-focus-group-product-20260727`.
- Scope: page/product mechanics only.
- Explicitly absent: live Supabase changes, production Auth/membership, email
  delivery, feedback persistence/analysis, prize execution, static candidate
  publish or production deploy.

## Lane integration

| Lane | Requirement IDs | Worker branch | Status | Worker head | Integration evidence |
|---|---|---|---|---|---|
| product-docs | R01, R02, R09 | `agent/focus-group/product-docs` | merged | `24bce1c1` | cherry-picked as `6b5aa5ba`; manual templates and lifecycle/product spec |
| for-me | R06, R07 | `agent/focus-group/for-me` | merged | `dee371f7` (`98483fd5` implementation) | cherry-picked as `a16178c8` + `ab2a23a5`; build/Playwright evidence in lane RESULTS |
| focus-shell | R03, R04, R05, R08, R10 | `agent/focus-group/focus-shell` | merged | `e0c840dd` (`c662d8ce` implementation) | cherry-picked as `59e1d2a1` + `d3894590`; unit/build/Playwright/icon evidence in lane RESULTS |
| integration | R11, R12 | this branch | integrated | pending final SHA | lifecycle specimen, member-only `/dlya-menya/` feedback, contract tests, canonical docs and changelog |

No worker changes were rejected, abandoned or left uncommitted. Read-only
research lanes changed no files.

## Integration changes

- Added `/fokus-gruppa/zavershenie/` for automatic/operator end-state
  comparison and explicit personalization continuity.
- Exposed reusable focus feedback on `/dlya-menya/` only when the bounded
  preview marker exists; normal static/personalization fallback remains usable.
- Added focus product unit/source-contract tests and a package script.
- Reconciled actual route names in the product spec and updated canonical
  routing, static-site docs, backlog index and changelog.

## Verification

- `npm run test:focus-group-product` — 12/12 passed.
- Worker full builds — 431 and 434 pages passed.
- Worker Playwright — mobile/desktop/no-JS/localStorage/feedback/icon checks
  passed; see lane RESULTS.
- Final `npm run build` — 435 pages built successfully in 1m 44s. The
  pre-existing `listingPresentation.ts` JSON import-attributes warning remains
  non-fatal.
- Final integration browser QA — passed on the built static output:
  - 390px root and 1440px participant surfaces have no horizontal overflow;
  - an invite fragment is stripped from the URL, its bearer value is not
    retained, and only the bounded preview hint is stored;
  - participant tools on `/dlya-menya/` are hidden in a fresh context and shown
    after the preview hint;
  - all three lifecycle variants switch correctly;
  - ending browser preview removes only the preview hint and preserves the
    local personalization key;
  - no browser console errors were observed.
- Screenshots and the machine-readable browser result are in the ignored local
  directory `artifacts/codex/focus-group-product-20260727/`.

## Requirement closure

| ID | Status | Evidence | Remaining production boundary |
|---|---|---|---|
| R01 | Done | Act Opus logo and pending thank-you panel; one pair/two invitations; no feedback/share/invite multiplier | partner spelling/rights, rules and fulfilment need separate approval |
| R02 | Done | `manual-email-templates.md` and one-recipient SOP | no mail is sent or automated |
| R03 | Done | fragment intake strips token, stores bounded 72-hour hint, share specimen, marker-gated hub | server invite redemption/cap/revoke absent by scope |
| R04 | Done | ordinary Astro root is a focus testing stub; secret hub noindex/no-referrer | production/secret builders intentionally unchanged |
| R05 | Done | overall NPS, page usefulness, improvement and typed event-fact issue specimens | no server persistence/sampling enforcement |
| R06 | Done | separate auto-picks opt-in/off/eligibility UI after explicit/interpretable signals | no sender/scheduler/delivery |
| R07 | Done | 16 category cards; native tri-state; inferred meter separate from evidence sufficiency; explainable local feed | not a cross-device/online ML profile |
| R08 | Done | invite → lab badge/congratulation → email/Yandex choice → preview hub | choices do not send OTP or launch OAuth |
| R09 | Done | automatic/operator/cancelled end UI; preview access clears independently from personalization | no production command/cron |
| R10 | Done | reusable lab badge with visually inspected CC0 SVG Repo 287837 icon | final merge window may restyle it without changing attribution |
| R11 | Done | no live DB, deploy, production build or outbound message action | production rollout remains separate work |
| R12 | Done | pages repeatedly distinguish marker/opaque path from auth and prototype from production | backend security gates remain mandatory for later implementation |
