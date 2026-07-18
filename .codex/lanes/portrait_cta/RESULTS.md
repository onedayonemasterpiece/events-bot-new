# Lane portrait_cta — RESULTS

## Scope

- Lane: `portrait_cta`
- Requirement: `R04`
- Base SHA: `4542a7dfaedf3d86ea4b5e4618e06e717f0dc8cf`
- Head SHA (validated implementation): `edaba3660275da579baaa4d6fa5ffc516b17f40d`
- Branch: `agent/static-event-preprod/portrait-cta`
- Push/merge: not performed, per lane instruction

## Result

The desktop action panel now receives the resolved desktop media family from
`DesktopEventPage`:

- `split` portrait/document pages attempt the accepted compact one-row grid;
- `editorial` wide-photo pages keep the accepted tall three-row card with the
  calendar/share/like controls on the bottom row;
- Split admission is fail-closed: rendered child containment, overlap and
  intrinsic overflow are measured, and only a component that actually fits may
  remain inline. This does not infer the layout from viewport width or from a
  slug.

The executable browser matrix covers two portrait specimens and two wide-photo
specimens at `1536x864`, equivalent to FHD at 125% desktop scaling.

## Browser geometry evidence

Command:

```bash
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4173/preview-20260718t161104-4542a7df \
  npm --prefix site run check:desktop-cta-geometry
```

All four specimens passed family routing, layout, containment, no-overflow and
control-row alignment:

| Event | Resolved family/layout | Panel geometry | Contract |
| --- | --- | --- | --- |
| `opera-i-dzhaz-znamensk-6876` | `split` / `inline` | `675.84 x 117.34` | one row |
| `myuzikl-alye-parusa-kaliningrad-4783` | `split` / `inline` | `675.84 x 100.78` | one row |
| `kontsert-more-muzyki-svetlogorsk-6551` | `editorial` / `stacked` | `475.52 x 227.13` | three rows, utilities at bottom |
| `tribyut-linkin-park-ot-yalta-band-pos-romanovo-5374` | `editorial` / `stacked` | `475.52 x 227.13` | three rows, utilities at bottom |

## Commands and tests

- `npm --prefix site run build:preview` — passed; 380 pages built, immutable
  preview `preview-20260718t161104-4542a7df`.
- `npm --prefix site run check:preview` — passed; 303 events,
  `strict_related=false`.
- `PREVIEW_BUILD_ID=preview-20260718t161104-4542a7df node --test site/tests/event-detail-runtime-regressions.test.mjs`
  — passed, 8/8.
- `node --experimental-strip-types --test site/tests/event-media-quality.test.mjs`
  — passed, 6/6, including the frozen Alye Parusa portrait-family contract.
- `STATIC_SITE_REVIEW_BASE_URL=... npm --prefix site run check:desktop-cta-geometry`
  — passed, 4/4 real-event browser geometry specimens.
- `bash -n site/scripts/check-desktop-cta-geometry-playwright.sh` — passed.
- `git diff --check` — passed before commit.

## Risks / follow-up

- Split panels intentionally fall back to the three-row safe geometry if future
  localized CTA wording, counters or a narrower component allocation do not fit.
  The accepted FHD/125% portrait specimens remain inline and are protected by
  executable geometry checks.
- The Playwright gate requires an immutable built preview/candidate and
  `playwright-cli`; it does not publish or promote the preview.

## Changed files

- `site/src/components/DesktopEventActionPanel.astro`
- `site/src/components/DesktopEventPage.astro`
- `site/scripts/check-desktop-cta-geometry-playwright.sh`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `docs/features/static-site-pages/event-page-product-design.md`
- `docs/operations/e2e-scenarios.md`
- `CHANGELOG.md`
