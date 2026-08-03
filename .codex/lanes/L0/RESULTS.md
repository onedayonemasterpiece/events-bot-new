# L0 — StaticSiteBuilder incident / release pipeline

## Outcome

- **Implementation SHAs:** `efeb7aa5d` (Popular preview gate) and `08dffcfe6`
  (browser fallback/specimen gate).
- Created and closed regression contract
  `INC-2026-08-03-static-site-builder-failure-storm` after an exact-main
  no-root-promotion canary and immutable candidate receipt were verified.
- Latest reproducible blocker fixed: `check:preview` no longer requires the
  fixture-specific literal `ещё 1 показ` when current Popular rankings contain
  no repeated-date family.
- The replacement gate remains fail-closed: a selected linked family must show
  its exact distinct repeat count and a linked occurrence cannot render as a
  second desktop card.
- Clean preview validation now requires the real deterministic multi-image
  `6408 → 6407` journey.
- The CI event `6407` shell-escape failure was reproduced with Chromium and a
  production-equivalent missing CDN image. Root cause was a gate-state bug: an
  intentionally hidden failed `<img>` has a zero rectangle. Loaded images still
  have strict shell containment; missing images must have no paint layer and a
  visible bounded fallback.

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
  was still alive in `export` under job `47635`.
- At 18:35 UTC that run had failed at the same pre-fix `check:preview` gate:
  the audited total became 27 failures, and the durable pointer was unchanged.

Sanitized/uncommitted evidence is under
`artifacts/codex/INC-2026-08-03-static-site-builder-failure-storm/`, especially
`kaggle-terminal-excerpt-20260803T1802Z.txt`,
`live-summary-20260803T1814Z.txt`, and `precommit-live-recheck.txt`.

## Validation

Commands run:

```bash
npm --prefix site run test:popular-occurrence-contract
npm --prefix site run test:browser-release-gate
npm --prefix site run test:occurrences
node --check site/scripts/check-preview.mjs
node --check site/scripts/popular-occurrence-contract.mjs
python3 -m py_compile \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  scripts/run_static_site_builder_kaggle.py main.py models.py
PREVIEW_BUILD_ID=preview-20260803t180433-524f0a14 \
  npm --prefix site run check:preview
npm --prefix site run check:browser-release -- \
  --root /home/dev/.codex/worktrees/events-bot-new/static-site-unified-20260803/integration/site/dist/preview-20260803t180433-524f0a14 \
  --report /tmp/l0-browser-fixed-report.json
```

Results:

- targeted Popular contract: **2/2 pass** (zero-family and multi-family;
  negative cases cover duplicate linked card, missing/wrong count);
- occurrence regressions: **16/16 pass**;
- browser behavior contracts: **12/12 pass**, including real Chromium 404 for
  the event `6407` card and deterministic `6408 → 6407` discovery;
- Node syntax and Python compile checks: **pass**;
- full generated clean-main preview artifact check: **pass**, 288 events.
  The shared generated tree was reused read-only because the integrator imposed
  a disk constraint; no `npm install` or duplicate build output was created.

The full Chromium gate is now **green** on that tree: 33 static candidates,
selected journey `6408 → 6407`, and all nine release checks passed. The report
also confirms loaded event `6407` remains inside its shell. The separate forced
missing-image browser test proves the corrected fallback branch.

## Files owned / changed

- `site/scripts/check-preview.mjs`
- `site/scripts/check-browser-release-gate.mjs`
- `site/scripts/browser-release-gate.behavior.test.mjs`
- `site/scripts/popular-occurrence-contract.mjs`
- `site/scripts/popular-occurrence-contract.behavior.test.mjs`
- `site/package.json`
- `docs/reports/incidents/INC-2026-08-03-static-site-builder-failure-storm.md`
- `docs/reports/incidents/README.md`
- `docs/features/static-site-pages/astro-preview.md`

## Live closure evidence

- PR #317 merged the follow-up manifest-bound festival inventory gate to
  `origin/main` as `6c7970207c9c5c597d8175b69dcafca115cc0502`; Fly machine
  version 1904 reported that exact in-container SHA and passed `/healthz`.
- The single fixed-SHA canary was
  `static-site:production-secret-20260803T224034-4493aaed:1814d7f84627`.
  Both production-root and secret-candidate Chromium gates passed; the Kaggle
  ledger reached `done/report`, 100%, `events=395`, error `null`.
- Host recovery adopted the same terminal result after the first 5,400-second
  wrapper timeout; it did not launch another Kaggle run. Full immutable
  readback verified 3,305 objects / 674,467,502 bytes and atomically advanced
  the candidate pointer at `2026-08-03T22:51:24Z`.
- The receipt matches the deployed SHA, run, snapshot, input fingerprint,
  result SHA and manifest SHA. The public review candidate returns HTTP 200
  with noindex/noarchive protections.
- Production root and stable ICS hashes/ETags remained byte-for-byte unchanged,
  root promotion remained disabled, and final `/healthz` was ready with no
  issues. Canonical closure details are in the incident record merged by PR
  #319 (`origin/main@7efa00560725334d16cc06c1e47b858b258e4370`).
- No OTP flow was invoked.

## Integration / closure notes

- No files under `site/src`, `CHANGELOG.md`, or `docs/routes.yml` were touched.
- `CHANGELOG.md` remains for the integrator because this lane was explicitly
  forbidden to edit it.
- The incident is closed: the fix is reachable from `origin/main`, the exact
  main SHA reached terminal success, and its immutable candidate receipt plus
  public non-mutation evidence were independently verified.
