# INC-2026-08-01-focus-pwa-shortcut-instead-of-install Android offered a shortcut instead of PWA install

Status: monitoring
Severity: sev2
Service: KenigEvents focus-group PWA onboarding
Opened: 2026-08-01
Closed: —
Owners: static-site / focus-group release
Related incidents: `INC-2026-07-27-pwa-presentation-install-missing`
Related docs: `docs/features/static-site-focus-group/README.md`

## Summary

The focus invitation published a valid manifest but registered no service
worker and its fallback copy treated Chrome's plain home-screen shortcut as an
equivalent installation. On Android the operator therefore saw `Добавить
иконку на главный экран` instead of the expected `Установить приложение`, so
the onboarding could not proceed to acceptance of later steps.

## User / Business Impact

- The operator could not retest the focus-group journey from its first step.
- A shortcut could be mistaken for the installed `Анонсы` application.
- The resilient remote-data contract was not exercised and must not be marked
  accepted from this interrupted run.

## Detection

- Detected by direct Android phone acceptance.
- Live manifest and icon probes were healthy, while the invitation exposed no
  active service-worker contract.

## Root Cause

1. The focus manifest was present, but the invitation did not register a
   worker for its candidate/root scope.
2. The install controller attached only after hydration and did not consume an
   already-fired one-shot `beforeinstallprompt` event.
3. Android fallback copy incorrectly recommended `Добавить на главный экран`,
   hiding the distinction between a shortcut and PWA installation.

## Automation Contract

### Treat as regression guard when

- changing focus onboarding, manifest, service worker, base path, install copy
  or candidate publication.

### Mandatory checks before closure or deploy

- `node --test site/tests/pwa-install.test.mjs`;
- `node --experimental-strip-types --test site/tests/focus-pwa-membership.test.mjs`;
- clean Astro build with a non-root `SITE_BASE_PATH`;
- mobile Chromium: manifest errors empty, installability errors empty, worker
  active under the exact candidate scope, early prompt reaches the install CTA;
- live candidate read-back for invitation, manifest, worker and icons.

## Corrective Actions

- Added a cache-free, network-only worker at the release root.
- Registered it from the focus invitation using base-aware URL and scope.
- Captured `beforeinstallprompt` in the document head and handed it to the
  hydrated one-shot controller.
- Removed shortcut-as-install copy from the Android path.

## Release And Closure Evidence

- deployed SHA: pending
- live candidate: pending
- phone acceptance: pending

## Prevention

Keep this record open through real Android acceptance. Passing transport or
desktop tests does not close the install regression.
