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
recorded 26 failures without advancing the durable secret-candidate pointer.
The public production root stayed on its prior release. The newest four
terminal runs converged on `npm run check:preview`; the exact Kaggle log showed
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

## Root Cause

1. `site/scripts/check-preview.mjs` unconditionally searched rendered Popular
   HTML for the exact Russian literal `ещё 1 показ`.
2. Popular is selected from the current mutable production catalog. It can
   validly contain zero repeated-date families, or a family with a repeat count
   other than one.
3. The gate therefore tested the incidental catalog composition rather than
   the intended contract: linked occurrences must collapse to one card and a
   selected family must expose its actual repeat count.

The earlier 22 failures were not assigned this assertion as a shared root
cause: the durable ledger places them in earlier build/export/browser stages.
The latest four failures form the current reproducible blocker addressed here.

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
- regression checks: targeted behavior test passes; full build/check and canary
  pending
- post-deploy verification: pending terminal ledger success and immutable
  review receipt

## Prevention

The release gate now derives its expectation from the selected events instead
of a fixture literal. This keeps catalog evolution from failing a valid build
while preserving fail-closed checks for actual linked-family duplication and
missing repeat summaries.
