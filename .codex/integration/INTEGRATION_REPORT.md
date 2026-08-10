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
| integration | R11, R12 | this branch | integrated | `f7aea6ad` | lifecycle specimen, member-only `/dlya-menya/` feedback, contract tests, canonical docs and changelog |
| pwa-membership | R13–R16 | `agent/focus-group/pwa-membership` | merged | `1d71dd1c` (`51af4b49` implementation) | cherry-picked as `32de33db` + `c198e9e1`; exact logo, focus manifest/start controller, optional identity and independent participation state |
| egg-program | R17–R19 | `agent/focus-group/egg-program` | merged | `a718a2b1` (`a609528b` implementation) | cherry-picked as `f4955358` + `b7d5910d`; collection-first rank and 12-placement product contract |
| egg-prototype | R20 | `agent/focus-group/egg-prototype` | merged | `8b345c09` (`8b94f2bc` implementation) | cherry-picked as `8f96dc86` + `a3b7af87`; collection UI and fail-closed `FG-E12` placement |
| extension integration | R21 | this branch | integrated | `38074d03` | prize-copy reconciliation, canonical docs/changelog, PWA local incident regression subset and final browser/build gates |

No worker changes were rejected, abandoned or left uncommitted. Read-only
research lanes changed no files.

## Integration changes

- Added `/fokus-gruppa/zavershenie/` for automatic/operator end-state
  comparison and explicit personalization continuity.
- Exposed reusable focus feedback on `/dlya-menya/` only when the independent
  30-day participation marker is active; normal static/personalization fallback
  remains usable.
- Added focus product unit/source-contract tests and a package script.
- Reconciled actual route names in the product spec and updated canonical
  routing, static-site docs, backlog index and changelog.
- Added a centred exact copy of `docs/reference/PWA-icon.png` to the mobile join
  screen, a focus-specific manifest and state-aware PWA launch route.
- Split participation, personalization and collection into independent local
  state contracts. Email/Yandex identity intent is optional in the prototype;
  an explicit no-confirmation path remains available.
- Added `/fokus-gruppa/kollektsiya/`, a collection-first reward model and a
  versioned 12-placement matrix. `FG-E12` exists only after the third current
  saved/calendar item and never moves to a shorter list.
- Added the explicit non-prize research mission `Два экрана`: real phone and
  desktop coverage use separate future receipts, while an honestly labelled
  single-device equivalent protects accessibility and prize fairness.
- Restored the root manifest/install/telemetry mounts required by
  `INC-2026-07-27-pwa-presentation-install-missing`.

## Verification

- `npm run test:focus-group-product` — 28/28 passed after removal of the
  obsolete short-lived marker compatibility tests.
- `node --test tests/pwa-install.test.mjs` — 8/8 passed.
- `npm run test:static-release` — 10/10 passed.
- Worker full builds — 431 and 434 pages passed.
- Worker Playwright — mobile/desktop/no-JS/localStorage/feedback/icon checks
  passed; see lane RESULTS.
- Initial `npm run build` — 435 pages built successfully in 1m 44s. Final
  continuity-corrected build — 436 pages built successfully in 1m 45s.
  The
  pre-existing `listingPresentation.ts` JSON import-attributes warning remains
  non-fatal.
- Final integration browser QA — passed on the built static output:
  - 390px root and 1440px participant surfaces have no horizontal overflow;
  - an invite fragment is stripped from the URL, its bearer value is not
    retained, and only the separate participation marker is stored;
  - participant tools on `/dlya-menya/` are hidden in a fresh context and shown
    after participation activation;
  - all three lifecycle variants switch correctly;
  - ending participation on the device removes only the participation marker
    and preserves the local personalization key;
  - no browser console errors were observed.
- Extension mobile Chromium QA on the built static output:
  - supplied focus logo is centred at 390px with no horizontal overflow;
  - invite fragment is stripped and never stored;
  - focus manifest/install guidance and the optional identity skip are visible;
  - skip activates participation, PWA relaunch returns to the secret hub;
  - activation stores one exact 30-day window and there is no second
    short-lived access marker;
  - `Удалить локальный профиль` removes personalization but preserves focus
    participation;
  - `FG-E12` is absent at two items, inserted immediately after item three,
    persists once found and stays absent when the current list becomes short;
  - the final collection page presents the three-step phone/desktop mission,
    its non-prize boundary and the single-device alternative without overflow;
  - no console errors were observed.
- Screenshots and the machine-readable extension browser result are in the
  ignored local directory
  `artifacts/codex/focus-group-pwa-eggs-20260727/`; first-stage browser evidence
  remains in `artifacts/codex/focus-group-product-20260727/`.
- Independent checklist review: R01–R03 and R05–R12 accepted within the
  explicit prototype boundary. R04 is Partial because the marker-gated secret
  hub is implemented, but the existing site routes are not physically
  relocated below the secret prefix; doing that belongs to the deliberately
  deferred production-build integration.
- Extension checklist review initially found R19 Partial because the responsive
  placement matrix alone did not operationalize real phone **and** desktop
  coverage. Commit `38074d03` closes that finding with the separate `Два
  экрана` mission and fair single-device equivalent. R13–R21 are now Done
  within the explicit prototype/design boundary.

## Requirement closure

| ID | Status | Evidence | Remaining production boundary |
|---|---|---|---|
| R01 | Superseded | Act Opus logo remains, but the original “any performance/equal draw” copy was replaced by the later owner decision R17–R19 | current prize contract is R17–R19 |
| R02 | Done | `manual-email-templates.md` and one-recipient SOP | no mail is sent or automated |
| R03 | Done | fragment intake strips the token and creates one independent 30-day participation state; there is no short-lived preview access layer | server invite redemption/cap/revoke absent by scope |
| R04 | Partial | ordinary Astro root is a focus testing stub; secret hub is noindex/no-referrer and marker-gated, but links onward to the existing route tree | relocating the whole static site beneath the secret prefix requires the deferred production/secret builder integration |
| R05 | Done | overall NPS, page usefulness, improvement and typed event-fact issue specimens | no server persistence/sampling enforcement |
| R06 | Done | separate auto-picks opt-in/off/eligibility UI after explicit/interpretable signals | no sender/scheduler/delivery |
| R07 | Done | 16 category cards; native tri-state; inferred meter separate from evidence sufficiency; explainable local feed | not a cross-device/online ML profile |
| R08 | Done | invite → lab badge/congratulation → PWA offer → optional email/Yandex intent or explicit skip → 30-day participant hub | choices do not send OTP or launch OAuth |
| R09 | Done | automatic/operator/cancelled end UI; participation clears independently from personalization | no production command/cron |
| R10 | Done | reusable lab badge with visually inspected CC0 SVG Repo 287837 icon | final merge window may restyle it without changing attribution |
| R11 | Done | no live DB, deploy, production build or outbound message action | production rollout remains separate work |
| R12 | Done | pages repeatedly distinguish marker/opaque path from auth and prototype from production | backend security gates remain mandatory for later implementation |
| R13 | Done | exact supplied 1254×1254 artwork is centred above the mobile-first join flow; byte-equality test passes | final compression/derivatives may be added without changing the source artwork |
| R14 | Done | focus manifest launches through state-aware onboarding and redirects active local participation to `/zakrytaya-afisha/`; Android/iOS fallbacks are honest | browsers control install and app launch; no code can force either |
| R15 | Done | email/Yandex intent and `Продолжить … без подтверждения` are separate explicit paths | no real identity session by scope |
| R16 | Done | independent participation key, exact 30-day activation window and reset-isolation tests; PWA return flow uses the same state | cleared browser data is not recoverable until server membership exists |
| R17 | Done | pending UI and docs consistently define one prize as exactly two theatre tickets | theatre, performances, dates and fulfilment remain rules/legal gates |
| R18 | Done | collection coverage ranks first; bounded participation is capped at 40/7 and counts NPS response, likes, dislikes, text feedback, Search, saves and page breadth without sentiment advantage | reward-grade receipts/anti-abuse/leaderboard backend absent by scope |
| R19 | Done | versioned 12-row placement matrix covers mobile, desktop, keyboard/screen-reader equivalents, prerequisites and fail-closed states; a separate non-prize `Два экрана` mission requests one meaningful phone and desktop probe with an honestly labelled single-device equivalent | actual receipts, pairing and site-wide production insertion remain later implementation |
| R20 | Done | collection prototype demonstrates locked/eligible/found/unavailable and idempotent `FG-E12` immediately after saved item three only | demo list is not wired to production saved events |
| R21 | Done | docs/routes/changelog, targeted tests, incident regression and full build/browser QA on the integration branch | no production deploy was requested or performed |

## Incident regression control

- Incident: `INC-2026-07-27-pwa-presentation-install-missing` (closed; used as
  a regression contract because root/PWA surfaces changed).
- Affected local surfaces: root HTML, global/focus manifests, install
  controllers and focus onboarding.
- Local/source regression subset passed:
  - `node --test tests/pwa-install.test.mjs` — 8/8;
  - `npm run test:static-release` — 10/10;
  - cleanable full Astro build — 436 pages;
  - worker synthetic Android one-shot `beforeinstallprompt` and mobile browser
    smoke;
  - root source-contract confirms manifest, install action and telemetry mounts;
  - focus mobile browser QA confirms manifest/start/return behavior.
- Production HTTP headers, icon read-back, Object Storage upload and live
  installability were intentionally not run: this task creates no candidate and
  performs no deploy. Therefore the full incident contract is **not** claimed as
  passed. These deploy-only gates remain mandatory if this branch is promoted.

---

# First-party action-map integration report

## Scope

- Base: `origin/main` at `d7731ab4235b325e9ca52d13c45fba83eaf5de0b`
- Integration branch: `integration/action-map-contract`
- Requirements: R01, R02, R03, R04, R06

| Lane | Requirements | Worker head | Status | Integration evidence |
|---|---|---|---|---|
| `action-map-events` | R01–R04, R06 | `6297267fb9086e198c8e5a369e2af5d36f05bc33` | merged | cherry-picked as six ordered commits; no conflicts |
| `action-map-design` | R05 | `e46eb71daf1cf8726cf540b92fef86a577e65c2f` | external-repo merged | integrated separately in `lovekgd-design-system` |

## Verification

- Exact source/canonical attachment comparison: `cmp` PASS, 61,558 bytes.
- SHA-256: `4ade21e6ad03d6e5d9bc934af17ad8bccb1463ebe595f16d8bafe75c0e88048a`.
- `docs/routes.yml` parses and all six new routes resolve.
- Added relative Markdown links resolve.
- Action-map producer enums match the canonical source and design consumer: `insufficient-data`, `instrument-better`.
- `git diff --check origin/main..HEAD`: PASS.
- No runtime, schema or deployment changes are included.
