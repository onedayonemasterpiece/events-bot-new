# INC-2026-08-03-static-site-builder-failure-storm StaticSiteBuilder failure storm

Status: open
Severity: sev2
Service: KenigEvents static-site secret-candidate builder
Opened: 2026-08-03
Closed: —
Owners: static-site release pipeline
Related incidents: —
Related docs: `docs/features/static-site-pages/astro-preview.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The production StaticSiteBuilder launched 27 times in the audited window and
recorded 27 failures without advancing the durable secret-candidate pointer.
The public production root stayed on its prior release. The newest four
terminal runs, followed by the in-flight retry, converged on
`npm run check:preview`; the exact Kaggle log showed
that a mutable-catalog Popular check required the fixture-specific text
`ещё 1 показ` even when the ranked desktop selection contained no repeated-date
family.

## User / Business Impact

- reviewers did not receive a fresh immutable candidate for current event data;
- repeated long Kaggle jobs consumed builder capacity and delayed release
  feedback;
- the public root was not replaced by a failed candidate and remained
  available, so this was a release-pipeline degradation rather than a public
  site outage.

## Detection

- Incident audit correlated `kaggle_run_ledger`, `joboutbox`,
  `static_site_build_state`, the production runtime-file mirror and the current
  Kaggle kernel log.
- At 2026-08-03 18:08 UTC the audited window contained 26 failed runs and one
  alive retry. Failure stages were: 16 `build:preview`, four
  `check:browser-release`, four `check:preview`, one export failure and one
  resource-busy failure.
- The ledger retained only the failed command. The exact assertion was visible
  in Kaggle logs, which is why opaque terminal status alone is not acceptable
  closure evidence.
- GitHub Actions run `30810580372`, job `91676107974`, supplied a second exact
  browser failure: event `6407` was reported as escaping its media shell while
  exercising the deterministic `6408 → 6407` gallery journey.

## Timeline

- 2026-08-02 00:34 UTC — last successful durable secret candidate recorded:
  `static-site:production-secret-20260801T222854-379b6264:f20575db9f54`.
- 2026-08-03 13:14–17:21 UTC — later runs failed at browser and preview gates as
  the pipeline progressed through successive revisions.
- 2026-08-03 17:28 UTC — run
  `static-site:production-secret-20260803T192742-afa652c6:4b81a21ee570`
  started.
- 2026-08-03 18:01 UTC — that run failed in `check:preview`; Kaggle log line
  3534 identified the Popular repeated-date assertion.
- 2026-08-03 18:06 UTC — automatic job `47635` acquired another production
  claim. It was still alive during the read-only audit and runs pre-fix code.
- 2026-08-03 18:08 UTC — runtime mirror and durable-state recheck confirmed the
  old successful pointer remained intact.
- 2026-08-03 18:31 UTC — the last pre-fix automatic run also terminated at
  `check:preview`; the durable successful pointer still did not move.
- 2026-08-03 18:35 UTC — local generated-tree preview and Chromium browser
  gates passed after both gate corrections; no live canary was launched.
- 2026-08-03 19:04 UTC — after the exact merged SHA reached Fly, the durable
  single-flight owner launched exact-main canary
  `static-site:production-secret-20260803T210348-d0fa8b9b:de85cdfea8e8`.
- 2026-08-03 19:47 UTC — the canary passed the corrected Popular gate and the
  first eight Chromium journeys, then failed at the festival-calendar selector.
  Exact HTML and Chromium reproduction showed 18 valid active cards from the
  21-row SQLite source ledger because three exact editions had ended.
- 2026-08-03 19:49 UTC — both retryable outbox rows were placed on an explicit
  incident hold before their next due time. The worker had already dequeued the
  operator row before that transaction and launched one stale-SHA retry at
  19:50 UTC; it remained the sole active run and no additional run was started.

## Root Cause

1. `site/scripts/check-preview.mjs` unconditionally searched rendered Popular
   HTML for the exact Russian literal `ещё 1 показ`.
2. Popular is selected from the current mutable production catalog. It can
   validly contain zero repeated-date families, or a family with a repeat count
   other than one.
3. The gate therefore tested the incidental catalog composition rather than
   the intended contract: linked occurrences must collapse to one card and a
   selected family must expose its actual repeat count.
4. The browser geometry gate separately applied the loaded-image rectangle
   invariant to both successful images and intentionally hidden failed images.
   A CDN/network failure puts the card in `is-image-missing`, hides the `<img>`
   (a zero rectangle) and paints the bounded fallback. The old check called
   that valid fallback state an image-shell escape before evaluating the
   already-present missing-image contract.
5. The next exact-main canary exposed a second mutable-calendar assumption:
   `assertFestivalCalendar` selected only `data-festival-count="21"` even though
   the exporter intentionally removes exact festival editions after their end
   date. On 2026-08-03 the source ledger still contained 21 accepted rows while
   the release manifest and rendered page correctly contained 18 active cards.

The earlier 22 failures were not assigned this assertion as a shared root
cause: the durable ledger places them in earlier build/export/browser stages.
The latest five failures form the current reproducible blocker addressed here.

## Contributing Factors

- The assertion had no isolated zero-family/multi-family behavior test.
- Automatic retries can launch another expensive run while a deterministic
  catalog-dependent gate remains broken.
- The callback ledger truncates the useful failure to `CalledProcessError`;
  exact diagnosis requires Kaggle logs.

## Automation Contract

### Treat as regression guard when

- changing Popular ranking, occurrence-family linkage or temporal labels;
- changing `check:preview`, the Kaggle builder, static-site retry/reconciliation
  or secret-candidate adoption.

### Affected surfaces

- `site/scripts/check-preview.mjs` and
  `site/scripts/popular-occurrence-contract.mjs`;
- `kaggle/StaticSiteBuilder/static_site_builder.py` preview pre-gate;
- production `kaggle_run_ledger`, `joboutbox` and
  `static_site_build_state`;
- Kaggle kernel `zigomaro/kenigevents-static-site-builder`;
- immutable `/_review/<token>/` candidate publication.

### Mandatory checks before closure or deploy

- `npm --prefix site run test:popular-occurrence-contract`;
- `npm --prefix site run test:browser-release-gate`, including an actual
  Chromium 404/missing-image card for event `6407`;
- `PREVIEW_BUILD_ID=<unique> npm --prefix site run build:preview` followed by
  `check:preview` on the same tree;
- an exact-SHA, no-root-promotion production canary reaches terminal success;
- ledger `report_written`, artifact checks and a create-only immutable review
  receipt agree on build id, event count and SHA;
- the durable candidate changes only after all gates pass; production root and
  stable ICS remain untouched;
- release SHA is reachable from `origin/main` before incident closure.

### Required evidence

- Exact failure excerpt:
  `artifacts/codex/INC-2026-08-03-static-site-builder-failure-storm/kaggle-terminal-excerpt-20260803T1802Z.txt`.
- Read-only live summary and runtime-mirror inventory in the same artifact
  directory.
- Test output, exact canary run id, candidate receipt and deployed SHA.

## Immediate Mitigation

- Failed candidates were not adopted. The durable pointer continued to resolve
  to the 2026-08-02 successful candidate and the public root was not promoted.
- No OTP, production row or publication state was mutated during diagnosis.

## Corrective Actions

- Replace the unconditional literal assertion with a data-aware contract.
- Accept a valid ranked selection with no repeated-date family.
- For every selected event that does have links, require the actual distinct
  repeat count in its temporal label and reject rendering a linked event as a
  second desktop card.
- Add isolated behavior coverage for zero-family, valid multi-family, duplicate
  linked-card and missing-summary cases.
- Make the clean preview pre-gate require the real deterministic multi-image
  `6408 → 6407` journey discovered from generated HTML. The browser gate itself
  remains fail-closed and data-driven for other valid journeys.
- Apply shell containment only to loaded image pixels. For the existing
  missing-image state, require the failed image layer to be `display:none` and
  the bounded fallback to be visibly present.
- Treat the canonical destination URL plus `DOMContentLoaded` event shell as
  completed keyboard navigation; remote media is not allowed to hold that
  navigation assertion open until the later `load` event.
- Derive the festival browser inventory from the checked production/candidate
  manifest. Require the canonical SQLite projection source, positive source and
  rendered counts, `rendered <= source`, and exact DOM card/image parity at both
  desktop and mobile sizes. The 21-card fallback remains only for the clean
  preview fixture that has no release manifest.

## Follow-up Actions

- [ ] Static-site owner: propagate a bounded command-output tail into terminal
  builder status so the ledger is actionable without a separate log download.
- [ ] Static-site owner: evaluate a deterministic-failure circuit breaker for
  same-SHA/same-fingerprint retries after this incident is restored.
- [ ] Release owner: merge to `origin/main`, run the exact-SHA no-publish canary
  and record the resulting immutable candidate receipt.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending merge to `origin/main`; no root promotion authorized
- regression checks: targeted occurrence tests, browser behavior tests,
  generated preview check and full local Chromium release gate pass; exact-SHA
  canary pending
- post-deploy verification: pending terminal ledger success and immutable
  review receipt

## Prevention

The release gate now derives its expectation from the selected events instead
of a fixture literal. This keeps catalog evolution from failing a valid build
while preserving fail-closed checks for actual linked-family duplication and
missing repeat summaries.
