# R9-EXHIBITIONS results

## Status

Committed. R7 is acceptance-locked without changing the already-correct
exhibitions card geometry.

## Requirement

- **R7:** mobile exhibition medallion scale only.

## Git

- Base branch: `integration/mobile-acceptance-r9-20260723`
- Base SHA: `74bb254c4d20c1e488568fde445515131e64cbd5`
- Lane branch: `agent/mobile-acceptance-r9/exhibitions`
- Implementation head SHA: `596fe6afc31a05902f48b3a93c7433f826be6b01`

## Exact donor provenance

- Telegram topic: `KenigEvents · UI review`, topic anchor message `548`.
- Telegram requirement message `549` names the exhibitions donor as
  `integration/exhibitions-personal-discovery-prototype-20260719` with accepted
  tip `54cfa903`.
- Accepted branch/tip:
  `integration/exhibitions-personal-discovery-prototype-20260719@54cfa903`.
- Immutable accepted preview:
  <https://kenigevents.ru/preview-20260720-exhibitions-personal-v12-465c2bc5/lab/exhibitions-personal/>.
- The seal geometry itself entered that accepted history in
  `9a4e36cdc52e34f3f3458ba65df2082f82f68d1c` and was then publicly accepted and
  documented by `54cfa903`.
- Exact accepted geometry: one institutional seal, `44px` desktop and `36px`
  mobile, top-left of the deck, below the `+N` counter, outside photo-count and
  gallery semantics, hidden after image failure.

Telegram read evidence is stored outside git at:
`artifacts/codex/r9-exhibitions/telegram/messages-548-549-621-630.json`.
The approved local E2E human session was used; the reserved S22 bundle was not
used.

## Donor vs R8 measurements at 390px

Playwright measured the public donor and public R8 rather than inferring size
from source alone:

| Measurement | Accepted v12 donor | Current public R8 |
|---|---:|---:|
| Viewport | `390×844` | `390×844` |
| Visible seal | `36×36` | `36×36` |
| Deck | `331.21875×187.1875` | `331.21875×187.1875` |
| Row width | `352` | `352` |
| Document `scrollWidth/clientWidth` | `390/390` | `390/390` |
| Seal overlaps `+N` | no | no |
| Visible seal image | loaded, `320×320` intrinsic | loaded, `512×512` intrinsic |
| Console/page errors | none | none |

The comparison established that R8 had not actually drifted below the accepted
`36px` mobile target. Increasing it would have diverged from the exact donor.
Therefore this lane deliberately made no presentation/CSS change and instead
added a focused regression gate that prevents the reported scale from being
shrunk in a later integration.

Public comparison artifacts:

- `artifacts/codex/r9-exhibitions/playwright/public-comparison-390.json`
- `artifacts/codex/r9-exhibitions/playwright/donor-regression/`
- `artifacts/codex/r9-exhibitions/playwright/r8-regression/`

The locally built lane artifact reproduced the current geometry at 390px:
`36×36` seal, `331.21875×187.1875` deck, `352px` row, no horizontal overflow,
no counter overlap, loaded `512×512` intrinsic image, and no console errors.
Screenshot and JSON:
`artifacts/codex/r9-exhibitions/playwright/local-r9/`.

## Files changed

- `site/tests/exhibitions-medallion-mobile.test.mjs`
  - locks `44px` desktop / `36px` mobile in both public and lab surfaces;
  - locks the donor offsets and z-index hierarchy;
  - locks full-width/overflow-safe mobile deck geometry;
  - locks one fail-closed institutional seal outside deck media semantics.
- `site/tests/exhibitions-medallion-mobile.playwright.mjs`
  - reusable 390px browser measurement and screenshot gate;
  - asserts loaded image, no horizontal overflow, in-deck placement, no `+N`
    overlap, and no console/page errors.
- `.codex/lanes/R9-EXHIBITIONS/RESULTS.md`

No generic event-detail medallion, rail, search, production code, asset,
`CHANGELOG.md`, or canonical documentation file was edited.

## Commands and validation

- `node --test site/tests/exhibitions-medallion-mobile.test.mjs`
  - **PASS**, 2/2.
- Public donor Playwright:
  `EXHIBITIONS_URL=<accepted-v12-url> node site/tests/exhibitions-medallion-mobile.playwright.mjs`
  - **PASS**, screenshot + measurements recorded.
- Public R8 Playwright:
  `EXHIBITIONS_URL=https://kenigevents.ru/preview-20260723-unified-corrections-r8/vystavki/ node site/tests/exhibitions-medallion-mobile.playwright.mjs`
  - **PASS**, screenshot + measurements recorded.
- `PREVIEW_BUILD_ID=preview-r9-exhibitions-local npm run build:preview`
  - produced the checked local preview artifact.
- `PREVIEW_BUILD_ID=preview-r9-exhibitions-local npm run check:preview`
  - **PASS**, 288 events.
- Local Playwright against
  `http://127.0.0.1:41739/preview-r9-exhibitions-local/vystavki/`
  - **PASS**, screenshot + measurements recorded.
- `git diff --check`
  - **PASS**.

An additional legacy
`node site/scripts/check-exhibitions-personal-prototype.mjs` probe passed its
medallion assertions but reported one pre-existing unrelated navigation
assertion (`shared mobile navigation exposes current section for badge
extension`). It is outside R7 and no navigation file was changed.

Generated `site/dist` and `site/node_modules` were removed after QA to release
disk space. Evidence under `artifacts/` is intentionally uncommitted.

## Documentation delta

Per lane ownership, canonical docs and `CHANGELOG.md` were not edited. The
integrator may add one concise R9 acceptance note stating that the canonical
exhibitions seal remains `44px` desktop / `36px` mobile and is now covered by
source plus Playwright regression gates. No broader documentation rewrite is
needed.

## Risks

- The reported visual concern may have referred to a different medallion
  surface. This lane intentionally did not touch generic event-detail
  medallions; that is separately owned.
- Intrinsic logo artwork can have internal whitespace even when its CSS seal is
  exactly `36px`. Changing individual logo crops/assets is not part of R7.
