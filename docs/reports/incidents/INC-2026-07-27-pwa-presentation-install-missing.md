# INC-2026-07-27-pwa-presentation-install-missing Presentation QR opened a non-installable root

Status: open
Severity: sev2
Service: KenigEvents static-site PWA installation
Opened: 2026-07-27
Closed: —
Owners: static-site / release
Related incidents: —
Related docs: `docs/features/static-site-pages/mobile-shell.md`, `docs/features/static-site-pages/astro-preview.md`, `docs/operations/release-governance.md`

## Summary

The presentation QR encoded
`https://kenigevents.ru/?install=presentation`, but the production root still
served the 2026-07-17 landing object without a web app manifest, PWA icons or
install controller. Scanning the QR therefore opened the site but could not
produce an Android PWA installation action.

## User / Business Impact

- Presentation participants could open the site but could not follow the
  promised install-to-home-screen flow.
- The workaround was the browser menu, but the live root did not explain it.
- The QR itself was correct and stable; the missing production PWA surface made
  its stated purpose misleading.

## Detection

- The operator reported: “на страницу переходит, PWA не срабатывает”.
- Live HTTP inspection returned `404` for `/manifest.webmanifest`,
  `/manifest.json`, `/sw.js` and `/service-worker.js`.
- The root response had `Last-Modified: Fri, 17 Jul 2026 14:04:35 GMT` and no
  `<link rel="manifest">`.
- No release gate compared the PWA contract already merged in source/preview
  with the mutable production root objects.

## Timeline

- 2026-07-27 11:40 UTC — pre-send live probe showed the root landing page and
  missing manifest; QR artifact was still sent with an explicit warning.
- 2026-07-27 11:49 UTC — operator confirmed that the scanned page opened but
  PWA installation did not start.
- 2026-07-27 11:50 UTC — incident workflow opened; production source and
  release path were inspected.

## Root Cause

1. The PWA manifest/controller work existed in the R6 preview source and was
   reachable from `origin/main`, but production root objects were still the
   older 2026-07-17 landing release.
2. The root landing template linked no presentation install component; the
   shared install action existed only in the full site footer.
3. The presentation QR query parameter had no deployed UI contract.

## Contributing Factors

- Preview publication verified its own prefixed manifest but did not promote
  root `manifest.webmanifest` and icons.
- Source acceptance and mutable-root acceptance were separate, with no live PWA
  probe in the production gate.
- `beforeinstallprompt` is browser-controlled and cannot be replaced by an
  automatic install; without explicit waiting/fallback copy, absence of the
  button looked like a broken page.

## Automation Contract

### Treat as regression guard when

- changing the static-site root, mobile footer, PWA manifest/icons,
  `beforeinstallprompt` controller, Object Storage root publisher or the
  presentation QR target.

### Affected surfaces

- `site/src/pages/index.astro`
- `site/src/pages/manifest.webmanifest.ts`
- `site/src/components/PwaInstallAction.astro`
- `site/src/lib/pwa-install-controller.js`
- Yandex Object Storage root objects and their MIME/cache metadata
- `https://kenigevents.ru/?install=presentation`

### Mandatory checks before closure or deploy

- `node --test site/tests/pwa-install.test.mjs`
- `npm run test:static-release`
- Astro build from a clean, pushed SHA
- Android/mobile Chromium smoke for waiting, real/synthetic
  `beforeinstallprompt`, one-shot prompt and accepted state
- root HTML contains `rel=manifest` and the presentation controller
- live `/manifest.webmanifest` returns `200` with
  `application/manifest+json`
- live `192×192` and `512×512` icons return `200` with `image/png`
- deployed fix is reachable from `origin/main`

### Required evidence

- deployed SHA and branch/PR/merge
- pre/post HTTP headers and manifest body
- local and live browser smoke output/screenshots
- Object Storage upload/read-back evidence without credential leakage

## Immediate Mitigation

- Kept the stable QR target unchanged.
- Added honest in-page Chrome/manual-install guidance for the presentation
  query while preserving the browser-controlled confirmation step.

## Corrective Actions

- Mount the shared PWA action on the root landing page.
- Treat `install=presentation` as a presentation mode that is visible before
  browser eligibility, becomes actionable only after a real
  `beforeinstallprompt`, and reports fallback/success states.
- Publish root manifest/icons with correct MIME and bounded revalidation.
- Add unit and mobile browser regression coverage.

## Follow-up Actions

- [ ] Add the live root PWA probe to the canonical production static-site
  release gate.
- [ ] Keep automatic production root promotion aligned with the immutable
  static release instead of relying on a manually retained landing object.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The incident record is now a regression contract for root/PWA/release changes;
closure requires both source tests and live Object Storage read-back rather
than preview-only evidence.
