# R9-EXHIBITIONS results

## Status

Committed. R7 implements the integrated R9 correction: exhibition seals are
`44×44 CSS px` on mobile as well as desktop. The owner requested that the
mobile seals be enlarged, but did not prescribe this exact numeric size.

## Requirement

- **R7:** enlarge mobile exhibition medallions without changing card/deck
  geometry, hierarchy, overflow behavior, or image reliability.

## Git

- Base branch: `integration/mobile-acceptance-r9-20260723`
- Base SHA: `74bb254c4d20c1e488568fde445515131e64cbd5`
- Lane branch: `agent/mobile-acceptance-r9/exhibitions`
- Initial regression commit: `596fe6afc31a05902f48b3a93c7433f826be6b01`
- R9 scale implementation SHA:
  `9bfc1fbf97d7132860707b25738bd7d6cd485267`

## Donor provenance and updated acceptance

- Telegram topic: `KenigEvents · UI review`, topic anchor message `548`.
- Telegram requirement message `549` names the exhibitions donor as
  `integration/exhibitions-personal-discovery-prototype-20260719` at accepted
  tip `54cfa903`.
- Immutable historical donor:
  <https://kenigevents.ru/preview-20260720-exhibitions-personal-v12-465c2bc5/lab/exhibitions-personal/>.
- The historical seal implementation entered that branch in
  `9a4e36cdc52e34f3f3458ba65df2082f82f68d1c` and used `44px` desktop /
  `36px` mobile.
- The latest owner feedback after R8 explicitly said the mobile seals were too
  small. R9 therefore keeps the donor structure, offsets, hierarchy,
  fail-closed behavior, and desktop size while choosing and validating
  `44×44 CSS px` as the integrated correction.

Telegram read evidence remains outside git at:
`artifacts/codex/r9-exhibitions/telegram/messages-548-549-621-630.json`.
The approved local E2E human session was used; S22 was not used.

## Implementation

- Public `/vystavki/` surface:
  `.ex-deck__medallion` at `<=820px` changed from `36×36` to `44×44`.
- Lab exhibitions surface received the same rule so the review donor and public
  component cannot drift.
- Desktop remains `44×44`.
- Placement remains `top:7px; left:7px` on mobile.
- Seal remains below the `+N` counter (`z-index:260` vs `300`), outside
  `deckMedia`, `aria-hidden`, noninteractive, and hidden by the existing broken
  image handler.
- Deck width, deck height, row grid, media count, and card content were not
  changed.

## Playwright evidence

Local checked preview:
`preview-r9-exhibitions-44`, route `/vystavki/`.

| Width | Seal | Deck | Row width | Document width | `+N` overlap | Image | Console/page errors |
|---:|---:|---:|---:|---:|---|---|---|
| 320 | `44×44` | `261.21875×160` | `282` | `320/320` | no | loaded, intrinsic `512×512` | none |
| 390 | `44×44` | `331.21875×187.1875` | `352` | `390/390` | no | loaded, intrinsic `512×512` | none |
| 430 | `44×44` | `371.21875×206.390625` | `392` | `430/430` | no | loaded, intrinsic `512×512` | none |

For every width the seal remained fully inside the deck, document
`scrollWidth` equaled `clientWidth`, and the loaded image had nonzero intrinsic
dimensions.

Screenshots, JSON measurements, and command logs:

- `artifacts/codex/r9-exhibitions/owner-44/playwright/exhibitions-medallion-320.png`
- `artifacts/codex/r9-exhibitions/owner-44/playwright/exhibitions-medallion-390.png`
- `artifacts/codex/r9-exhibitions/owner-44/playwright/exhibitions-medallion-430.png`
- matching `.json` reports in the same directory
- `artifacts/codex/r9-exhibitions/owner-44/build.log`
- `artifacts/codex/r9-exhibitions/owner-44/check-preview.log`

`owner-44` is a historical artifact-directory name created by the worker. It
is retained so the evidence links remain valid and must not be interpreted as
proof that the owner specified or accepted the exact `44px` value.

## Files changed

- `site/src/components/ExhibitionsPersonalSurface.astro`
- `site/src/pages/lab/exhibitions-personal/index.astro`
- `site/scripts/check-exhibitions-personal-prototype.mjs`
- `site/tests/exhibitions-medallion-mobile.test.mjs`
- `site/tests/exhibitions-medallion-mobile.playwright.mjs`
- `.codex/lanes/R9-EXHIBITIONS/RESULTS.md`

No generic event-detail medallion, rail, search, asset, `CHANGELOG.md`, or
canonical documentation file was edited.

## Commands and validation

- `node --test site/tests/exhibitions-medallion-mobile.test.mjs`
  - **PASS**, 2/2.
- `PREVIEW_BUILD_ID=preview-r9-exhibitions-44 npm run build:preview`
  - **PASS**, 389 pages.
- `PREVIEW_BUILD_ID=preview-r9-exhibitions-44 npm run check:preview`
  - **PASS**, 288 events.
- Reusable Playwright gate at `320`, `390`, and `430`:
  - **PASS** at all widths;
  - screenshots and JSON measurements recorded;
  - no overflow, `+N` overlap, broken visible seal, or browser error.
- `git diff --check`
  - **PASS**.

## Documentation delta

Per lane ownership, canonical docs and `CHANGELOG.md` were not edited. The
integrator should record that the R9 mobile correction supersedes the historical
v12 mobile value: exhibition seals are now `44px` on mobile and desktop, while
the one-seal/fail-closed/deck-hierarchy contract remains unchanged. This is an
implemented and tested design choice in response to “too small”, not a claim
that the owner specified or separately accepted the exact `44px` value.

## Risks

- Historical v12 documentation still says `36px` mobile until the integrator
  reconciles the shared canonical docs/CHANGELOG.
- Individual logo artwork may contain internal whitespace, but the seal box is
  now consistently `44×44` and asset/crop changes remain outside R7.
