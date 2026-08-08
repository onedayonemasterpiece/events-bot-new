# L5 prelaunch form implementation results

- Status: **Done (implementation/release-control lane)**
- Base: `60eda72a497042c68ab86a1d56dcca2703833398` (recorded in `/dev/shm/prelaunch-base-sha`)
- Implementation head: `c81af38b8`
- Commits:
  - `4c12b5c8f` — main reconciliation, client/server contract, tests, docs and incident.
  - `c81af38b8` — proven incident root cause correction, non-redundant v3 conflict contract and exact-main release control.

## Files

```
.github/scripts/publish-prelaunch-root-main.sh
.github/workflows/prelaunch-root-release.yml
.gitignore
CHANGELOG.md
docs/features/static-site-pages/prelaunch.md
docs/reports/incidents/INC-2026-08-08-prelaunch-registration-confirmation-and-dedup.md
docs/reports/incidents/README.md
docs/routes.yml
site/package.json
site/scripts/check-prelaunch-form-security.mjs
site/scripts/prepare-prelaunch-artwork.mjs
site/src/assets/prelaunch-approved/desktop/part-00.b64part
site/src/assets/prelaunch-approved/desktop/part-01.b64part
site/src/assets/prelaunch-approved/desktop/part-02.b64part
site/src/assets/prelaunch-approved/desktop/part-03.b64part
site/src/assets/prelaunch-approved/desktop/part-04.b64part
site/src/assets/prelaunch-approved/desktop/part-05.b64part
site/src/assets/prelaunch-approved/desktop/part-06.b64part
site/src/assets/prelaunch-approved/desktop/part-07.b64part
site/src/assets/prelaunch-approved/desktop/part-08.b64part
site/src/assets/prelaunch-approved/desktop/part-09.b64part
site/src/assets/prelaunch-approved/mobile/part-00.b64part
site/src/assets/prelaunch-approved/mobile/part-01.b64part
site/src/assets/prelaunch-approved/mobile/part-02.b64part
site/src/assets/prelaunch-approved/mobile/part-03.b64part
site/src/assets/prelaunch-approved/mobile/part-04.b64part
site/src/assets/prelaunch-approved/mobile/part-05.b64part
site/src/assets/prelaunch-approved/mobile/part-06.b64part
site/src/assets/prelaunch-approved/mobile/part-07.b64part
site/src/components/PrelaunchPage.astro
site/src/layouts/PrelaunchLayout.astro
site/src/lib/backendOperationCatalog.ts
site/src/lib/prelaunchEmail.test.ts
site/src/lib/prelaunchEmail.ts
site/src/pages/index.astro
site/src/scripts/prelaunchForm.ts
site/src/styles/prelaunch-calibration.css
site/src/styles/prelaunch-consent.css
site/src/styles/prelaunch-page.css
site/src/styles/prelaunch-polish.css
site/src/styles/prelaunch-static.css
site/tests/prelaunch-form-contract.test.mjs
supabase/migrations/20260803113000_prelaunch_launch_notifications_v1.sql
supabase/migrations/20260806163000_prelaunch_updates_consent_v2.sql
supabase/migrations/20260808143744_prelaunch_registration_result_and_race_safe_dedup.sql
```

## Commands / tests

- `npm --prefix site run test:prelaunch-form` — PASS, 7/7.
- Disk-backed `PUBLIC_PRELAUNCH_MODE=on npm --prefix site run build` — prelaunch root emitted; approved background reconstruction hashes matched.
- `check-prelaunch-form-security.mjs` against rebuilt artifact — PASS; covered first success, exact repeat state, reload, explicit reset, retained input on error, runtime duplicate-submit lock, capacity/rejection/down routes and idempotent relay replay.
- `git diff --exit-code 9d8fc9203a69f385407a57e23310bb47f2db4e2d -- <visual/page/layout/index/artwork paths>` — PASS; visual/background/assets/SEO-GEO sources unchanged.
- `bash -n .github/scripts/publish-prelaunch-root-main.sh` — PASS.
- `git diff --check` — PASS.

## Production evidence incorporated

Production reproduction returned HTTP 400 / PostgreSQL `22023` / `invalid_prelaunch_consent`. Protected function inspection showed only `launch-2026-09-01-v1`; migration history lacked v2 while the deployed browser sent `prelaunch-updates-2026-v1`. The existing v2 migration was then applied and independently verified by the integration owner. Production already has `UNIQUE(email)`; v3 therefore relies on that verified constraint with `ON CONFLICT(email) DO NOTHING` and does not create a redundant index.

## Risks / remaining release work

- This lane did not apply v3, push, dispatch GitHub Actions or mutate production DB/site by ownership instruction.
- The manual workflow must run from `refs/heads/main` with `expected_sha == origin/main == HEAD`; it stages, byte-verifies, backs up and rolls back root objects.
- Final closure still requires integration-owner v3 apply, two real form submissions plus protected one-row proof and cleanup, exact GitHub Actions run, and post-deploy success/reload verification.
- Browser evidence redacts the input before screenshots and strips request bodies/identity values from JSON/stdout.

## Merge notes

Merge in order `4c12b5c8f`, then `c81af38b8`. Shared files were reconciled additively from current main. The deployed prelaunch visual/page/layout/index/artwork bytes match production SHA; only form runtime behavior and release/test/docs contracts changed.
