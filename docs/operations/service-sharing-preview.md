# F18 service-share preview runbook

> Scope: footer-only HTTPS preview. Never updates production/current pointer,
> stable `/ics/`, stable service-share assets or production telemetry.

## Desktop refinement after paste testing

The first footer preview used a full-width tinted card and one desktop action
whose D1/D2 research modes mixed image, HTML and plain-text clipboard
representations.  Real messenger paste testing showed that this was both too
visually dominant for a footer utility and too ambiguous: receiving apps could
prefer the image even when the user intended to send text and a link.

The accepted follow-up contract is explicit instead of format-priority based:

- desktop **«Скопировать карточку»** writes one `ClipboardItem` containing only
  `image/png`;
- desktop **«Скопировать текст и ссылку»** uses `clipboard.writeText()` with the
  frozen share copy and canonical `https://kenigevents.ru/` URL;
- neither desktop action silently changes into the other intent;
- both controls have equal outline/secondary weight; the footer component has
  no banner background, shadow or marketing question;
- success remains inside the invoked control: its icon becomes a persistent
  green check until the next invocation, while the `aria-live` copy is visually
  hidden and occupies `1×1` px, so acknowledgement does not add a row or change
  footer height;
- the text-and-link control uses the unmodified geometry of SVG Repo library id
  `svgrepo-528355-link-minimalistic` through a `currentColor` mask. The durable
  source and CC0 metadata live under `site/public/assets/icons/`;
- mobile keeps one **«Поделиться»** action and the verified WebP file → native
  text+URL → clipboard/link fallback chain.

There is no prior artifact proving that a Gemini Pro consultant approved the
original dominant footer block. A new Antigravity visual probe under the display
alias `Gemini 3.1 Pro (High)` independently rejected that treatment and supported
the two-intent redesign, but the CLI did not expose a canonical provider model
ID. Under the repository consultant policy this evidence is retained as
`supplementary probe material`, not presented as a completed Gemini Pro gate.

Cube-face preparation now distinguishes document protection from photographic
framing. Explicit `ocr_text` posters remain contain-framed unless `safe_crop` is
set. `visual_only` assets and conservative photo fallbacks are center-cover
cropped before the renderer adds the event title/date; the face manifest records
the crop mode, reason and source rectangle. Always regenerate a fresh catalog
snapshot and selection before a render so new OCR/crop metadata is propagated.

### Verified refined preview

- implementation/evidence SHA: `dc2e82cf20c4`;
- build id: `preview-20260715t0940z-f18-share-refine`;
- [footer preview](https://kenigevents.ru/preview-20260715t0940z-f18-share-refine/__preview/);
- [service-share lab](https://kenigevents.ru/preview-20260715t0940z-f18-share-refine/lab/service-share/);
- dynamic snapshot: `2026-07-15T07:10:00Z`, `284` eligible events, `15`
  normalized places, `84` additions in the exact trailing 168 hours;
- asset version: `20260715-896b8af26ac6679f`; manifest SHA-256
  `5383a1d81c033be971bc494f1bf0790d465a8bf05cd7ce98e986ac81f56c9fe3`;
- PNG: `826,924` bytes,
  `e6335fa840aed21246dfd2b34078025fe19c99c08d5414271c01d788e330420d`;
  WebP: `72,414` bytes,
  `ee8f8d78e1ce0d037a20b9e7331a42998cf4b39328539933ea6e7e243c091c10`;
- exact-bundle Kaggle GPU debug
  `service-share:2026-07-15:debug:20260715T092428Z` -> CPU final
  `service-share:2026-07-15:final:20260715T092658Z`, bundle SHA-256
  `82d19d5f18cec0e593eb1e9ba39986ef85d140163f51a51fa295e5497db1a035`;
- dry-run: `1,288` planned objects and every destination below the preview
  prefix; deploy explicitly preserved stable `/ics/` objects;
- local and public HTTPS Playwright: `14/14` each; controller `5/5`, renderer
  `23/23`, `build:preview`, `check:preview`, MIME/byte/hash checks and
  `git diff --check` passed; a real public Chromium clipboard readback confirmed
  that the text action contains the canonical URL without a preview path and the
  card action contains exactly one `image/png` representation;
- final CPU card and public desktop footer screenshot were sent to Telegram
  Saved Messages as messages `32270` and `32271`.

## Проверенный preview 2026-07-15

- implementation SHA до фиксации этого evidence:
  `8fafd9923f416fbd7cabb7cd31c0628376d4472d`;
- build id: `preview-20260715t0752z-f18-service-share-footer`;
- [footer preview](https://kenigevents.ru/preview-20260715t0752z-f18-service-share-footer/__preview/);
- [service-share lab](https://kenigevents.ru/preview-20260715t0752z-f18-service-share-footer/lab/service-share/);
- manifest SHA-256:
  `dc40d9aca2975e8c5e193bfc749f346d278c5c0eb071190a57d8ca27336b3d0b`;
- asset version: `20260715-399690a07ba97209`;
- WebP: `71,692` bytes,
  `7d124bbe39c9c9a744e36f01f8f5cc6ffff5f16265e500047ebc2c5646a50a00`;
- PNG: `818,803` bytes,
  `aaab887ca248de15aef30f6226eb40c048abcb5eed52296baa23bfef8bd7337a`;
- GPU run: `service-share:2026-07-15:debug:20260715T073944Z`, verified
  `GPU/OPTIX`, composition gates passed;
- CPU run: `service-share:2026-07-15:final:20260715T074157Z`, verified
  `CPU`, exact bundle SHA
  `33240c76e2b83b44a17220783e6220ee42cb4bf1741e48c310771ef82dff1767`;
- dry-run: `1,286` planned objects, `0` destinations outside preview prefix;
- public Playwright: `12 passed`; local controller: `5 passed`; renderer:
  `17 passed`; `build:preview` and `check:preview` passed.

CPU kernel завершился штатно и записал terminal `report_written`; локальный
launcher потерял процесс ожидания уже после remote start, поэтому output был
повторно скачан по kernel ref, провалидирован тем же bundle/composition contract,
а временный input dataset удалён. Это отмечено в локальном redacted receipt, а не
скрыто как обычный непрерывный launcher run.

## Preflight

1. `git fetch origin --prune`.
2. Work only from a clean, pushed branch with recorded SHA.
3. Confirm the desktop build exposes both isolated intents (`image` and `text`)
   and does not re-enable the retired mixed-format D1/D2 clipboard experiment.
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

Direct publication of local `site/dist` is retired. Service-share previews use
the canonical StaticSiteBuilder transaction: a checked Kaggle artifact is
downloaded and the trusted host publishes it create-only under the exact
`preview-*` build prefix with `--publish-preview`. This publisher cannot
express stable `/ics/`, production root or another build prefix. The canonical
command, page-class slicing and receipt requirements live in
[`kaggle-static-site-builder.md`](kaggle-static-site-builder.md#single-build-and-publish-rail-decision-2026-09-03).

For a focused service-sharing review, include every required surface class in
the runner request (normally `event`, `date`, `personal` and `lab`) rather than
building locally and uploading the resulting directory.

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
