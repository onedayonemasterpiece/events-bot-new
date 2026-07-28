# R02 logout / focus-membership separation — results

## Scope and outcome

- Lane: `logout_membership` (`R02`)
- Requirements: `R02.1` visible login/account/logout state; `R02.2` successful logout ends the Supabase session and failed logout remains visible; `R02.3` logout preserves the 30-day focus marker; `R02.4` only the explicit leave-focus action clears that marker; `R02.5` personalization reset/continuity stays independent; `R02.6` regression coverage.
- Result: complete within the assigned lane.

## Evidence

- Base SHA: `fa64169904daf96bff993bb52798103f3becfd32`
- Implementation head SHA: `3ec4b6ae4f90ddcaaf926e9be23ec3172f9ae44d`
- Branch: `agent/focus-group/logout-membership-20260728`
- Supabase contract checked against the official Auth sign-out and `onAuthStateChange` documentation on 2026-07-28. The current JavaScript contract returns `{ error }` from `auth.signOut()` and emits `SIGNED_OUT`; no relevant client sign-out breaking change was found in the current Auth changelog.

## Changed files

- `site/src/components/Reference4MobileMenu.astro`
- `site/src/components/auth/StaticSiteAuthRuntime.astro`
- `site/src/lib/staticSiteAuth.ts`
- `site/src/pages/dlya-menya/index.astro`
- `site/src/pages/zakrytaya-afisha/index.astro`
- `site/src/lib/focus-group-prototype.test.ts`
- `site/tests/static-site-auth.test.mjs`
- `site/tests/focus-group-product-surface.test.mjs`

## Commands and tests

- `node --test tests/static-site-auth.test.mjs tests/focus-group-product-surface.test.mjs tests/focus-pwa-membership.test.mjs tests/focus-easter-eggs.test.mjs` — PASS, 26/26.
- `npm run test:focus-group-product` — PASS, 30/30.
- `npm ci` — PASS; installed the pinned dependencies needed for the build (working-tree ignored).
- `npm run build` — PASS, 436 pages built.
- `node --test tests/static-site-auth.test.mjs tests/pwa-install.test.mjs tests/focus-pwa-membership.test.mjs` — PASS, 19/19.
- `git diff --check` / `git diff --cached --check` — PASS.

## Risks / limitations

- A live Yandex OAuth round trip was not executed in this lane. The controller behavior is covered by source-contract tests and the full Astro build, but final environment acceptance should exercise a real configured Supabase/Yandex session.
- The build emitted the pre-existing Vite warning about inconsistent JSON import attributes in `relatedCardLayout.mjs`; this lane did not touch that area.
- `npm ci` reported five dependency audit findings (one low, four high). Dependency remediation was outside this lane and no lockfile changed.
