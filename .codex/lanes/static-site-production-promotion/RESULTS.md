# Static-site production promotion lane results

## Lane

- Lane ID: `static-site-production-promotion`
- Branch: `agent/static-site-production-promotion/static-site-production-promotion-20260715`
- Base SHA: `b26453c1fbe63b191a72396bccae430ee837d6cb`
- Implementation SHA before this report-only commit: `55ff1f0c15280626e09ac1e01bead2c2a36882a4`
- Status: complete and committed; not pushed; no deployment performed
- Execution: one serial lane because build, artifact validation, publisher, and canonical-mode contracts share the same files and invariants

## Scope contract

Requirement IDs: `R1` through `R9` from the production-root static-site build/check/publish request.

Writable files were limited to:

- `site/package.json`
- `site/scripts/build-production.mjs`
- `site/scripts/check-production.mjs`
- `site/scripts/deploy-production-yc.mjs`
- `site/src/**` only for explicit preview/production mode support
- focused static-site tests if needed
- `docs/features/static-site-pages/astro-preview.md`
- this results file

Forbidden files were not changed:

- `CHANGELOG.md`
- `docs/features/event-media/README.md`

Done-when criteria: preserve preview behavior, produce and validate an indexable root production artifact, implement immutable-stage/public-verify/safe-root-promotion and rollback tooling without broad bucket deletion, preserve stable ICS and preview keys, document exact gates, run targeted checks, and commit a clean lane without deploying or pushing.

## Requirement results

- **R1 — Done:** Preview mode remains explicit, noindex, and preview-prefixed. A fixed-build comparison found 876 deterministic preview files byte-identical; timestamp-bearing build metadata, sitemap timestamps, and ICS `DTSTAMP` files were excluded from byte comparison and passed the existing preview checker.
- **R2 — Done:** Production mode emits a root listing page, root event/listing/search URLs and canonicals, indexable event/listing pages, production robots, and a production sitemap that excludes QA/lab and `__preview` URLs.
- **R3 — Done:** Added checked production artifact generation and Yandex Object Storage publisher commands.
- **R4 — Done:** Publisher stages to `_static/releases/<build-id>/root/`, seals a versioned `static_release_manifest_v1`, verifies staged public HTTP bytes, promotes immutable assets before supporting files/HTML/root HTML, retains old hashed assets, rotates previous/current release pointers, and supports explicit rollback.
- **R5 — Done:** Stable `ics/<event_id>.ics` files are mapped and updated individually and are protected from managed-root cleanup.
- **R6 — Done:** Production checker rejects preview/noindex leakage, incorrect robots/sitemap/canonicals, QA URLs, hash/inventory drift, wrong build mode, and malformed release metadata.
- **R7 — Done:** Canonical documentation includes exact build/check/plan/publish/rollback commands, confirmation gates, release protocol, and deletion protections.
- **R8 — Done:** Node syntax checks, npm install, Astro preview and production builds, artifact checks, publisher plan, preview byte comparison, and focused source assertions passed.
- **R9 — Done:** Implementation is committed on the lane branch. The results file is committed separately so the final lane head can be reported externally.

## Changed files

- `.codex/lanes/static-site-production-promotion/RESULTS.md`
- `docs/features/static-site-pages/astro-preview.md`
- `site/package.json`
- `site/scripts/build-production.mjs`
- `site/scripts/check-production.mjs`
- `site/scripts/deploy-production-yc.mjs`
- `site/src/components/EventHero.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/events.ts`
- `site/src/lib/seo.ts`
- `site/src/pages/partners/index.astro`
- `site/src/pages/partnerstvo/index.astro`
- `site/src/pages/poisk/index.astro`
- `site/src/pages/populyarnoe/index.astro`
- `site/src/pages/robots.txt.ts`
- `site/src/pages/segodnya/index.astro`
- `site/src/pages/sitemap.xml.ts`
- `site/src/pages/sobytiya/[slug].astro`
- `site/src/pages/vyhodnye/index.astro`
- `site/src/pages/vystavki/index.astro`
- `site/src/pages/zavtra/index.astro`

## Commands and evidence

- `npm ci` — passed; installed 265 packages. NPM reported two low-severity dependency vulnerabilities.
- `node --check scripts/build-production.mjs` — passed.
- `node --check scripts/check-production.mjs` — passed.
- `node --check scripts/deploy-production-yc.mjs` — passed.
- `PUBLIC_PERSONALIZATION_SUPABASE_URL=https://example.supabase.co PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY=publishable-test-key PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex PREVIEW_BUILD_ID=preview-lane-acceptance npm run build:preview` — passed, 420 Astro pages built.
- `PREVIEW_BUILD_ID=preview-lane-acceptance npm run check:preview` — passed.
- Fixed-ID baseline/final preview SHA-256 comparison — passed for 876 deterministic files; the existing preview checker validated the complete artifact.
- `PRODUCTION_BUILD_ID=production-lane-55ff1f0c npm run build:production` from committed, clean SHA — passed, 420 Astro pages built.
- `npm run check:production` — passed: 1,265 managed root files and 399 stable ICS mappings.
- `npm run plan:production` — passed without credentials or remote mutation; promotion counts were 3 immutable assets, 854 supporting files, 407 HTML files, and root HTML last.
- Clean-manifest assertion — passed: `git_dirty=false`, full SHA `55ff1f0c15280626e09ac1e01bead2c2a36882a4`, 1,265 files, 399 stable ICS mappings.
- `git diff --check` — passed before the implementation commit.
- Focused search confirmed no `aws s3 sync`, no `--delete`, and no production artifact `/__preview/` or preview noindex directive.

## Safety and residual risks

- No bucket, credential, or live production endpoint was touched. Staged/public HTTP verification and object-copy behavior therefore still require an operator-controlled first real release.
- `publish` and `rollback` fail closed unless the artifact metadata is production-mode and the exact `KENIGEVENTS_SITE_PRODUCTION_CONFIRM=<command>:<release-id>` value is supplied.
- The branch intentionally remains based on requested SHA `b26453c1`; current `origin/main` advanced by two commits during the lane and should be reconciled by the integrator without reverting unrelated changes.
- Old `_astro` assets are deliberately retained. Storage lifecycle/retention can be added separately only after accounting for old HTML references.
- Full dependency audit was not remediated because the two reported vulnerabilities are low severity and dependency upgrades are outside this lane.
