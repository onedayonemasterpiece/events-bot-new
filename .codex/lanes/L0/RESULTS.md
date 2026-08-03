# L0 — StaticSiteBuilder incident / release pipeline

## Outcome

- **Implementation SHA:** `efeb7aa5d` (`fix static preview occurrence gate`).
- Created regression contract
  `INC-2026-08-03-static-site-builder-failure-storm` with status **open**.
- Latest reproducible blocker fixed: `check:preview` no longer requires the
  fixture-specific literal `ещё 1 показ` when current Popular rankings contain
  no repeated-date family.
- The replacement gate remains fail-closed: a selected linked family must show
  its exact distinct repeat count and a linked occurrence cannot render as a
  second desktop card.

## Live incident evidence (read-only)

Rechecked production before implementation and again at 2026-08-03 18:12 UTC.

- Runtime mirror enabled: `ENABLE_RUNTIME_FILE_LOGGING=1`,
  `RUNTIME_LOG_DIR=/data/runtime_logs`; active file plus seven rotations exist.
- Builder enabled: `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`.
- Audited window: 26 failed runs and one alive run. Failed stages: 16
  `build:preview`, four `check:browser-release`, four `check:preview`, one
  export, one resource-busy.
- Exact current terminal failure in Kaggle log:
  `Error: Popular desktop V28 must collapse repeated dates into one family card`
  at `site/scripts/check-preview.mjs:367`.
- Durable successful pointer remained
  `static-site:production-secret-20260801T222854-379b6264:f20575db9f54`
  (`2026-08-02T00:34:02Z`). No root or stable ICS mutation occurred.
- At final recheck pre-fix run
  `static-site:production-secret-20260803T200633-67785640:cb56106a01bf`
  was still alive in `export` under job `47635`; no overlapping canary was
  launched.

Sanitized/uncommitted evidence is under
`artifacts/codex/INC-2026-08-03-static-site-builder-failure-storm/`, especially
`kaggle-terminal-excerpt-20260803T1802Z.txt`,
`live-summary-20260803T1814Z.txt`, and `precommit-live-recheck.txt`.

## Validation

Commands run:

```bash
npm --prefix site run test:popular-occurrence-contract
npm --prefix site run test:occurrences
node --check site/scripts/check-preview.mjs
node --check site/scripts/popular-occurrence-contract.mjs
python3 -m py_compile \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  scripts/run_static_site_builder_kaggle.py main.py models.py
PREVIEW_BUILD_ID=preview-20260803t180433-524f0a14 \
  npm --prefix site run check:preview
```

Results:

- targeted Popular contract: **2/2 pass** (zero-family and multi-family;
  negative cases cover duplicate linked card, missing/wrong count);
- occurrence regressions: **16/16 pass**;
- Node syntax and Python compile checks: **pass**;
- full generated clean-main preview artifact check: **pass**, 288 events.
  The shared generated tree was reused read-only because the integrator imposed
  a disk constraint; no `npm install` or duplicate build output was created.

The integrator separately reported that clean-main `build:preview` and
`check:preview` pass, while `check:browser-release` has independent fixture/live
failures (`no static multi-image recommendation journey` locally; event 6407
image shell escape in production CI). Those browser-gate failures are not
misattributed to the Popular assertion and still block incident closure until
integrated work resolves them.

## Files owned / changed

- `site/scripts/check-preview.mjs`
- `site/scripts/popular-occurrence-contract.mjs`
- `site/scripts/popular-occurrence-contract.behavior.test.mjs`
- `site/package.json`
- `docs/reports/incidents/INC-2026-08-03-static-site-builder-failure-storm.md`
- `docs/reports/incidents/README.md`
- `docs/features/static-site-pages/astro-preview.md`

## Integration / closure notes

- No files under `site/src`, `CHANGELOG.md`, or `docs/routes.yml` were touched.
- `CHANGELOG.md` remains for the integrator because this lane was explicitly
  forbidden to edit it.
- No canary was started: the production resource was occupied by a pre-fix
  automatic run, and the separate browser gate is not yet green.
- Do not mark the incident closed until the fix is reachable from
  `origin/main`, an exact-main-SHA no-root-promotion canary reaches terminal
  success, and its immutable candidate receipt is verified.
