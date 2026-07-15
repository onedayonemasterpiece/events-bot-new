# F18 service-share preview runbook

> Scope: footer-only HTTPS preview. Never updates production/current pointer,
> stable `/ics/`, stable service-share assets or production telemetry.

## Evidence placeholders

The integrator must replace these in run evidence, not silently in canonical claims:

- implementation SHA: `<IMPLEMENTATION_SHA>`;
- build id: `<PREVIEW_BUILD_ID>`;
- preview URL: `<PUBLIC_PREVIEW_URL>`;
- manifest SHA-256: `<SERVICE_SHARE_MANIFEST_SHA256>`;
- Kaggle GPU/CPU run IDs: `<GPU_RUN_ID>` / `<CPU_RUN_ID>`.

## Preflight

1. `git fetch origin --prune`.
2. Work only from a clean, pushed branch with recorded SHA.
3. Confirm `PUBLIC_SERVICE_SHARE_DESKTOP_MODE` is `d0` unless the build is an
   explicitly labelled D1/D2 research preview.
4. Generate/select the accepted catalog snapshot and keep DB/log artifacts only
   under `artifacts/codex/f18-service-share/`.
5. Verify GPU gate and CPU final use the same bundle/date/catalog/selection/
   composition identity and terminal status evidence.
6. Verify WebP, true PNG and manifest bytes/SHA locally.

## Local build and contract checks

```bash
npm --prefix site run build
PREVIEW_BUILD_ID=<PREVIEW_BUILD_ID> npm --prefix site run build:preview
PREVIEW_BUILD_ID=<PREVIEW_BUILD_ID> npm --prefix site run check:preview
npx playwright test tests/playwright/service_share_contract.spec.ts --reporter=line
git diff --check
```

Required families: preview index, listing, search, event detail and
`/lab/service-share/`. Mobile baseline `390×844`; desktop baselines `1366×768`
and `1440×900`.

## Preview-only publish safety gate

`site/scripts/deploy-preview-yc.mjs` теперь является prefix-only командой для
этого preview: она отклоняет build id без `preview-`, пишет только под
`s3://<bucket>/<PREVIEW_BUILD_ID>/`, не обновляет stable `/ics/`, production
pointer или production service-share manifest. Перед реальной загрузкой обязателен
просмотр object plan через dry-run:

```bash
PREVIEW_BUILD_ID=<PREVIEW_BUILD_ID> \
KENIGEVENTS_SITE_DEPLOY_DRY_RUN=1 \
npm --prefix site run deploy:preview
```

Реальная публикация использует ту же команду без
`KENIGEVENTS_SITE_DEPLOY_DRY_RUN`. Реализация обязана:

1. requires a unique `preview-*` build id;
2. allowlists every destination key under `<PREVIEW_BUILD_ID>/`;
3. rejects `/ics/`, current/pointer/manifest promotion and any key outside prefix;
4. supports a dry-run/object plan that is reviewed before upload;
5. applies immutable cache metadata to content-addressed WebP/PNG and correct
   `Content-Type` to WebP, PNG and JSON;
6. records the exact command as `<PREVIEW_ONLY_DEPLOY_COMMAND>` in evidence.

Если dry-run показывает хотя бы один key вне preview prefix, публикация
**Blocked**. Строка о запрете stable ICS в stdout является обязательной частью
evidence.

## Public verification

After safe upload, verify against the public HTTPS preview:

```bash
curl -fsSIL <PUBLIC_PREVIEW_URL>
curl -fsSIL -H 'Origin: https://kenigevents.ru' <WEBP_URL>
curl -fsSIL -H 'Origin: https://kenigevents.ru' <PNG_URL>
curl -fsSL <MANIFEST_URL> -o artifacts/codex/f18-service-share/public-manifest.json
```

Acceptance:

- HTML, WebP, PNG and manifest return 200;
- MIME exactly `image/webp`, `image/png`, JSON;
- assets are CORS-readable and immutable/versioned;
- HTTP bytes and SHA-256 equal manifest;
- PNG signature is valid;
- public Playwright repeats the mobile/desktop suite through the HTTPS URL;
- no production pointer/current object or stable ICS was touched.

## Reporting boundary

Automated checks may close only the footer test implementation. Report native
Android/iOS/Windows/macOS as **Pending** unless executed on real devices. Header/
menu placement remains **Deferred until V12**. Do not call the result full F18 or
production-ready.
