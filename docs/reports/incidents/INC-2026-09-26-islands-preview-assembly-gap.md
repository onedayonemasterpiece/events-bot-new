# INC-2026-09-26-islands-preview-assembly-gap Review preview lost the floating shell after Home → Today

Status: mitigated
Severity: sev3
Service: KenigEvents static-site review preview
Opened: 2026-09-26
Closed: —
Owners: static-site UI/release
Related docs: `docs/features/static-site-pages/review-builds.yaml`, `docs/operations/e2e-scenarios.md`

## Summary

The review link described as the latest interface used the Home assembly from commit `1ea0faeb`, while the accepted listing floating-island changes lived on the divergent `fix/islands-review-20260906` branch. Clicking «Сегодня» therefore reached an older shell in that full-site preview.

## User / Business Impact

- The owner could not review one coherent version of Home and Today and had to question whether the migration was finished.
- The old preview link was misleading as a full-interface review target. Production root was not changed.

## Detection

- Owner reported the solid old header after clicking «Сегодня» from Home.
- Branch, build-manifest and Telegram review history established that focused island previews and the Home full-site preview were separate artifacts.

## Timeline

- 2026-09-06: Home and island branches and their separate previews were created.
- 2026-09-26: Owner observed the shell mismatch in the linked full-site preview.
- 2026-09-26: Both branches were integrated into one complete local build; the Home → Today browser journey and Today/Weekend archetype checks passed.

## Root Cause

1. The shared island work was reviewed in selective previews and never integrated into the full Home preview branch.
2. The handoff identified the Home preview as the latest interface without qualifying its route coverage.
3. No browser gate traversed Home → Today in a single build.

## Contributing Factors

- Build names and review messages did not distinguish a focused specimen from an integrated site.
- The complete-preview build and the focused archetype build used different branch tips.

## Automation Contract

### Treat as regression guard when

- Changing Home navigation, `EventLayout`, shared islands, listing pages or a review-preview assembly.

### Affected surfaces

- `site/src/layouts/EventLayout.astro`, `site/src/components/ShellIslandRuntime.astro`, Home and one-day listing pages, `site/tests/home-to-today-islands.playwright.mjs`, preview publication and the review-build registry.

### Mandatory checks before closure or deploy

- Full preview build and `check:preview` from one committed SHA.
- Browser click Home → Today at mobile and desktop widths; verify floating shell and absence of the old solid header.
- Today/Weekend archetype checks and one-day density checks; inspect public Home and Today URLs after upload.
- Identify historical event data explicitly in the owner handoff.

### Required evidence

- Committed source SHA and its `preview-build.json`, passing browser checks, public URL and registry row, plus Telegram review message.
- Mainline reachability or an explicit open PR before closing the incident.

## Immediate Mitigation

- Reclassified the old Home preview as a Home-only review artifact.
- Built an integrated full-site candidate in an isolated branch and added the missing browser journey.

## Corrective Actions

- Keep a single immutable build ID for the integrated review, register it and check its public Home → Today flow.
- Do not describe focused specimen previews as complete interface builds.

## Follow-up Actions

- [ ] Merge the integrated branch after owner voice review.
- [ ] Rebuild with a current event snapshot when the UI is approved.

## Release And Closure Evidence

- Deployed SHA: pending preview publication.
- Deploy path: isolated immutable preview prefix, no production-root promotion.
- Regression checks: local Home → Today and Today/Weekend checks passed; public readback pending.
- Post-deploy verification: pending.

## Prevention

- `site/tests/home-to-today-islands.playwright.mjs` now verifies the exact cross-route behavior the owner reported.
