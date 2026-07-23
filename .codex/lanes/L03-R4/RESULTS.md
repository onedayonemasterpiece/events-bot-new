# L03-R4 — clubs future-meeting badge

## Result

Implemented R03 for `InterestClubCard` without changing the accepted abstract
fallback artwork:

- desktop (`>760px`): `Ближайших встреч: N` is a dedicated overlay in the
  upper-right corner of the media/card;
- the badge uses a restrained amber surface and a directional lower glow
  (downward shadow plus a blurred radial pseudo-element below the badge);
- mobile keeps the existing badge placement in `.club-card__head`;
- the desktop and mobile instances are mutually exclusive at their responsive
  breakpoint, and the label does not wrap;
- neither viewport has horizontal overflow.

Stable regression hooks:

- `data-club-future-badge="desktop"`
- `data-club-future-badge="mobile"`

## Validation

```text
PREVIEW_BUILD_ID=preview-r4-clubs-badge-l03 \
  PUBLIC_INTEREST_CLUBS_ENABLED=1 npm --prefix site run build:preview
PASS — 389 pages

PREVIEW_BUILD_ID=preview-r4-clubs-badge-l03 \
  node --test site/tests/interest-club-catalog.browser.test.mjs
PASS — 1/1

node --test site/tests/interest-club-catalog.test.mjs
PASS — 4/4

git diff --check
PASS
```

Browser assertions cover:

- 1440px: absolute upper-right placement inside both media and card bounds,
  non-wrapping label, directional warm glow, no mobile duplicate;
- 390px: desktop badge hidden, original head badge visible, badge and cards
  within viewport, `scrollWidth === clientWidth === 390`.

## Screenshot evidence

Ignored local acceptance artifacts:

- `artifacts/codex/L03-R4/clubs-badge-desktop-1440.png`
- `artifacts/codex/L03-R4/clubs-badge-mobile-390.png`

