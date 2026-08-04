# INC-2026-08-03-static-site-builder-failure-storm StaticSiteBuilder failure storm

Status: closed
Severity: sev2
Service: KenigEvents static-site secret-candidate builder
Opened: 2026-08-03
Closed: 2026-08-03
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
- 2026-08-03 20:23 UTC — PR #317 merged the manifest-bound festival gate to
  `origin/main` as `6c7970207c9c5c597d8175b69dcafca115cc0502`.
- 2026-08-03 20:27 UTC — the canonical main-only Fly deploy completed on
  machine version `1904`, image
  `deployment-01KZ4MS14C0C89AX9Q5NEVF7J6`; the in-container repository marker
  matched the full main SHA and `/healthz` was ready with no issues.
- 2026-08-03 20:38 UTC — the already-launched stale-SHA retry terminated at its
  browser gate. At 20:39 UTC its exact-owner row `47663` was held only after the
  terminal ledger state was verified, then the operator issued the one fixed-SHA
  closure request. The existing row was requeued rather than creating a second
  owner.
- 2026-08-03 20:41 UTC — exact-main run
  `static-site:production-secret-20260803T224034-4493aaed:1814d7f84627`
  started from snapshot `snapshot-20260803T204042-34de319bb2` and input
  fingerprint `724be4edcc6aa0f8f9aff526e742bdb5592c654b86d464acfa9405392ff722a0`.
- 2026-08-03 21:16–21:28 UTC — the production-root and secret-candidate
  Chromium gates passed, the archive was verified, resources were released,
  and the durable Kaggle ledger reached `done/report`, 100%, error `null`.
- 2026-08-03 22:10 UTC — the host wrapper reached its 5,400-second timeout only
  after the successful Kaggle output and immutable candidate upload. The retry
  adopted that exact result at 22:13 UTC; it did not launch another kernel or
  issue another operator request. Create conflicts were accepted only for the
  already-written immutable objects, followed by unconditional full readback.
- 2026-08-03 22:51 UTC — full readback completed and the atomic SQLite compare-
  and-swap advanced the durable candidate to the exact fixed run. Job `47663`
  finished `done`, its active claim cleared and its prior timeout error cleared.
  The unrelated 2026-08-04 calendar-rollover job remained pending and was not
  repurposed for incident closure.
- 2026-08-04 00:02–01:52 UTC — the next automatic calendar-rollover build also
  reached terminal Kaggle success and uploaded its immutable candidate. Its
  first host wrapper timed out during full readback; continuing Smart Update
  effects then repeatedly moved the exact-output recovery row to the end of a
  new 15-minute quiet window. A guarded due-now recovery preserved the exact
  run/handoff, launched no new kernel, completed readback/CAS and exposed the
  pending-row debounce starvation addressed by the follow-up regression.

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

The earlier 22 failures were not assigned these assertions as a shared root
cause: the durable ledger places them in earlier build/export/browser stages.
The initial five-run preview cluster and the follow-up exact-main festival
browser failure are the reproducible blockers addressed here.

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
- Diagnosis was read-only. Closure mutations were limited to the documented
  incident holds, the single exact-main request/requeue, its immutable candidate
  publication and the final candidate-pointer compare-and-swap. No OTP flow or
  public-root publication was authorized.

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
- Keep an exact active remote-handoff recovery due immediately while merging
  later Smart Update evidence into its payload. The ordinary trailing debounce
  continues to apply only to a genuinely new pending build, not to adoption of
  an already completed immutable output.

## Follow-up Actions

- [ ] Static-site owner: propagate a bounded command-output tail into terminal
  builder status so the ledger is actionable without a separate log download.
- [ ] Static-site owner: evaluate a deterministic-failure circuit breaker for
  same-SHA/same-fingerprint retries after this incident is restored.
- [x] Release owner: merge to `origin/main`, deploy the exact SHA, run the
  no-root-promotion canary and record the resulting immutable candidate receipt.

## Release And Closure Evidence

- **Main/deploy:** PR #317 is merged and the deployed SHA is
  `6c7970207c9c5c597d8175b69dcafca115cc0502`, reachable from `origin/main`.
  Deployment used `scripts/deploy_fly_main.sh --remote-only`; Fly machine
  version `1904` reported the same full SHA. Root promotion remained disabled.
- **Local L0/L1 truth:** the focused Chromium release-gate suite passed `12/12`.
  The reconstructed production tree passed the relevant 1,124-event desktop
  contract and the complete local Chromium release gate, including exactly 18
  manifest-declared active festival cards with zero broken images or overflow
  at desktop and mobile sizes. The broader sparse local build subsequently hit
  an unrelated empty interest-club projection guard; that result was not
  represented as a green full production build. The exact production canary
  below is the authoritative end-to-end acceptance.
- **Kaggle terminal:** build `production-secret-20260803T224034-4493aaed`, run
  `static-site:production-secret-20260803T224034-4493aaed:1814d7f84627`, repo
  SHA `6c7970207c9c5c597d8175b69dcafca115cc0502`, snapshot
  `snapshot-20260803T204042-34de319bb2` (SHA-256
  `bc95d27b354574b53eb540e82c52973a5099a17bffe9e3083768251da0717649`),
  input fingerprint
  `724be4edcc6aa0f8f9aff526e742bdb5592c654b86d464acfa9405392ff722a0`.
  The ledger finished `done/report` at `2026-08-03T21:28:50.137141Z`, 100%,
  error `null`; both browser gates and all candidate manifest checks were `ok`.
- **Artifact/receipt:** result SHA-256
  `e7e3a1457199382ca255a8c1861f087926ae957bcb51cacd0bf9e0e83f5a844c`;
  counts were 395 events, 1,124 event pages, 3,299 files, 3,010 pages and
  652,859,326 result bytes. The atomic candidate receipt was verified at
  `2026-08-03T22:51:24.678337Z`; `--show-current-review` returned
  `current_review_ready` for the same run, SHA, snapshot and result.
- **Independent immutable readback:** the candidate prefix contains exactly
  3,305 objects / 674,467,502 bytes. Its single manifest object has SHA-256
  `ae4430e6e24f3560903d9ae5a98de0c18bbf5d4ffb563bfe5705395d3b2f95c8`,
  exactly matching the durable receipt; all eight manifest checks are `ok`.
  The bearer token is intentionally omitted; only its SHA-256
  `b2b83c7b3364ef350115f4c30f6c9e1a375b7c28f1cd55e50e1292380969392c`
  is retained. The public candidate returned HTTP 200 with private/no-store,
  `noindex`, `noarchive` and `no-referrer` protections.
- **Non-mutation and health:** production root SHA-256 remained
  `28b345b51bb72ca2d0633ebeefff6725e06759650f401832f0cb3809a4be3e69`
  (5,589 bytes, unchanged ETag), and stable `ics/6408.ics` remained
  `e1ccd3ce252977f1ad48e7705bf2e18a76c59cb3702d29415c2d9ad98f2a3902`
  (927 bytes, unchanged ETag). The receipt records `root_mutation=false` and
  `stable_ics_mutation=false`; `ENABLE_STATIC_SITE_ROOT_PROMOTION` was false.
  Final `/healthz` was ready with `issues=[]`, and `/data` retained about 52%
  free space.
- **Evidence bundle:** redacted receipts, ledger state, independent storage and
  public probes, pre/post public hashes, deploy verification and local browser
  output are under
  `artifacts/codex/INC-2026-08-03-static-site-builder-failure-storm/closure/`.

## Prevention

The release gates now derive mutable Popular and festival expectations from
their checked build inputs instead of fixture literals: selected occurrence
families for Popular and the hash-bound release manifest for the active
festival inventory. Catalog evolution no longer fails a valid build, while
linked-family duplication, missing repeat summaries, wrong projection source,
invalid count ordering, DOM/image parity and visual geometry remain
fail-closed.

The outbox coalescer also distinguishes a new build from an exact active
remote-output recovery. New Smart Update effects cannot indefinitely postpone
the latter, while their event/reason evidence remains merged for the next
normal build decision.
