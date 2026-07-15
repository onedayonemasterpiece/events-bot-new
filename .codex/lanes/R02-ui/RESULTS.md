# R02 UI results

## Delivered

- Replaced the footer banner treatment with a quiet, in-flow utility row: no container background, border, radius, gradient, or shadow; removed the marketing question and kept the neutral label `Поделиться афишей`.
- Desktop now exposes two equal-weight outline actions:
  - `Скопировать карточку` writes one `ClipboardItem` with **only** `image/png`.
  - `Скопировать текст и ссылку` uses **only** `navigator.clipboard.writeText` with share text plus the canonical URL.
- Image-copy failure stays an explicit image error (`Не удалось скопировать картинку`) and never silently becomes text copy.
- Mobile below 768px keeps exactly one native `Поделиться` action and the existing verified WebP → text+URL → clipboard/canonical-link fallback chain.
- Added per-button busy/disabled state, shared polite live status, visible focus rings, 44px targets, safe flex wrapping, and reduced-motion handling. No header changes.
- Updated the lab from D0/D1/D2 product candidates to the two explicit desktop intents.
- Updated unit and Playwright coverage for pure PNG, isolated text copy, no cross-intent fallback, visual de-escalation, per-button busy state, mobile transport, and 200% zoom wrapping.

## Validation

- `node --test tests/playwright/service_share_controller.test.mjs` — **5 passed**.
- `npm run test:service-share:playwright` — **14 passed**.
- `npm run build` — **421 pages built**, success.
- `git diff --check` — clean.

## Integration note

`site/scripts/check-preview.mjs` still asserts the superseded banner/D0-D1-D2 source markers and is outside this lane's writable scope. The integration lane must update those static marker checks before running `check:preview`.
