# INC-2026-07-27-pwa-presentation-install-missing Presentation QR opened a non-installable root

Status: closed
Severity: sev2
Service: KenigEvents static-site PWA installation
Opened: 2026-07-27
Closed: 2026-07-27
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
- 2026-07-27 12:08 UTC — PR `#138` merged the fix into `origin/main` after
  `python-ci` and `static-browser-release-gate` passed.
- 2026-07-27 12:12 UTC — the bounded root PWA object set was published to
  Yandex Object Storage after backing up the prior root.
- 2026-07-27 12:13 UTC — public HTTP/MIME/icon checks and live Android Chromium
  presentation-flow smoke passed without console or request errors.
- 2026-07-27 12:14 UTC — the cache-busted v2 QR was decoded locally and
  delivered as an uncompressed Telegram document.

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
- Sent a cache-busted replacement QR using
  `https://kenigevents.ru/?install=presentation&v=2` so presentation devices do
  not reuse the pre-fix five-minute root response.

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

- deployed source SHA:
  `edf7ca459be665baac2d20c947a27f04c0aa72e8`; reachable from `origin/main`
  through merge `325d5b14dee8715772848ba66fe36f1df1c4fd4f` (PR `#138`).
- deploy path: clean pushed hotfix worktree; bounded S3-compatible Object
  Storage upload of `index.html`, `manifest.webmanifest`, both PWA icons,
  `favicon.svg` and the root content-hashed CSS. Prior root backup:
  `_static/hotfix-backups/20260727T120937Z-pwa-presentation/index.html`.
- regression checks:
  - `site/tests/pwa-install.test.mjs`: `5/5` passed;
  - `site/scripts/static-release.behavior.test.mjs`: `10/10` passed;
  - GitHub `python-ci`: passed;
  - GitHub `static-browser-release-gate`: passed;
  - clean-SHA Astro build: `431` pages;
  - local Android Chromium smoke: waiting → native event → one-shot accepted;
  - live Android Chromium smoke: manifest + fixed install card + accepted state,
    no console/request errors.
  - Chrome DevTools `Page.getAppManifest` resolved the live root manifest with
    zero manifest errors; `Page.getInstallabilityErrors` returned an empty
    installability-error set.
- post-deploy verification:
  - `/`: `200`, `text/html; charset=utf-8`;
  - `/manifest.webmanifest`: `200`,
    `application/manifest+json; charset=utf-8`, root `id/scope/start_url`;
  - both PWA icon URLs: `200`, `image/png`, decoded as exact `192×192` and
    `512×512`;
  - replacement QR decoded to the exact v2 URL and was delivered to Telegram
    message `719` as a document.

## Prevention

The incident record is now a regression contract for root/PWA/release changes;
closure requires both source tests and live Object Storage read-back rather
than preview-only evidence.
