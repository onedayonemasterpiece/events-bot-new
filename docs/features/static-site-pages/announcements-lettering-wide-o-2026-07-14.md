# «Анонсы» wide-o lettering — 2026-07-14

> **Status:** approved static-site UI optical master R2; display/trademark refinements remain a separate future round.
>
> **Lab:** `/lab/header-lettering/`; the desktop-header examples consume the same runtime vector.

## Direction

The wordmark uses one expanded lowercase `о` as the only deliberate width accent. Both `н` remain normal width. This keeps `Анонсы` readable at UI sizes, lets the `о` evolve into a standalone service mark and avoids turning the middle of the word into a two-letter decorative block.

The umbrella endorsement remains ordinary HTML text: `Полюбить Калининград`. Only the service word `Анонсы` becomes outlined SVG lettering.

## Source archive and boundary

The untouched user-supplied package was moved from the accidental `docs/reference/` location to:

`docs/features/static-site-pages/assets/announcements-lettering-wide-o/announcements-lettering-wide-o-concept-assets.zip`

The archive is preserved as source evidence. Its concept SVGs and favicon PNGs are not copied into runtime automatically and are not represented as accepted finals.

## Measured source defects

The original `announcements-wordmark-wide-o-concept.svg` was inspected as geometry rather than judged only from the concept-board PNG.

| Measurement | Source |
|---|---:|
| Lowercase x-height | `1056` |
| `н` vertical / crossbar | about `295 / 247` |
| Wide `о` outer / inner | about `1640×1092 / 1260×744` |
| Wide `о` side / horizontal | about `190 / 174` |
| `А–н` | about `33` |
| `н–о` | about `39` |
| `о–н` | about `204` |
| `н–с` / `с–ы` | about `87 / 85` |
| ViewBox optical padding | about `99` left / `195` right |

The core problem was therefore measurable: the special `о` was 30–36% lighter than the neighbouring `н`, while its two side spaces differed by more than five times. Its mathematical superellipse was exported as thousands of `line-to` points, and the generic round `с` / font-derived `ы` did not share its construction.

## Gemini Pro constructive review

Two independent `Gemini 3.1 Pro (High)` reviews were run through Antigravity `agy`.

### Source review

The source review confirmed:

- the `о` was too light and read as an inserted icon;
- the `о–н` space was a word-space rather than kerning;
- `н` crossbars were too dark for the UI size;
- `с` and the bowl of `ы` needed related squircle shoulders;
- the final UI SVG should have one compound path, no transforms and a tight zero-origin viewBox.

The consultant's first numeric proposal was `300 / 200` for the wordmark `о` and `250 / 220–235` for the standalone small mark.

### R1 acceptance review

R1 implemented `300 / 200`. The rendered size board showed that this fixed the weakness but over-corrected it: the `о` became too button-like and dominated the word, especially in reverse. Gemini marked the weight as `P0 before preview` and accepted the vector engineering, lighter `н` crossbar, rebuilt `с` / `ы` family and standalone mark.

R2 therefore does not copy either consultant number blindly. It uses the visual compromise below.

Raw ignored evidence:

- `artifacts/codex/announcements-lettering-wide-o-20260714/gemini-constructive-review.md`;
- `artifacts/codex/announcements-lettering-wide-o-20260714/gemini-r1-acceptance-review.md`;
- `wordmark-ui-r1-board.png` and `wordmark-ui-r2-board.png` in the same artifact folder.

## Working R2 geometry

| Element | R2 contract | Rationale |
|---|---:|---|
| Lowercase x-height | `1056` | Preserves the source scale for direct comparison. |
| `н` vertical / crossbar | `294–295 / 205` | Keeps the upright mass but removes the dark fence-like bridge. |
| Wide `о` outer | `1600×1104` | Maintains one obvious width accent and `24`-unit overshoots. |
| Wide `о` side / horizontal | `260 / 190` | Still materially heavier than the source `190 / 174`, but more open than button-like R1 `300 / 200`. |
| `А–н` | `22` | The open diagonal provides its own negative space. |
| `н–о` / `о–н` | `63 / 63` | Symmetric protective space around the special glyph. |
| `н–с` / `с–ы` | `49 / 50` | Tighter than ordinary mechanical spacing, without collisions. |
| Standalone mark side / horizontal | `250 / 230` | Lower contrast and a more open counter for 16–32px use. |

The wordmark `о` uses four cubic segments for the outer contour and four for the rounder inner counter. It is redrawn at width; no `scaleX` is used. The `с` carries the same flat shoulder / curved corner logic without inheriting the exceptional width. The `ы` bowl uses the same curve-weight family but remains a subordinate compact form.

## Runtime SVG contract

### Wordmark

`site/public/brand/announcements-wordmark-ui.svg`

- one compound `<path>`;
- `1413` bytes before any transport compression;
- `fill="currentColor"`;
- no `<text>`, stroke, filter, mask or transform;
- zero-origin `viewBox` with bounded optical side padding;
- paths, not a runtime font dependency.

### Standalone mark

`site/public/brand/announcements-mark-ui.svg`

- a separate optical drawing, not the wordmark `о` cropped mechanically;
- lighter sides and a more open counter for small sizes;
- its open-counter logic is adapted into the production tag favicon rather than cropped mechanically.

### HTML integration

`site/src/components/brand/AnnouncementsWordmark.astro` references the external path through same-origin SVG `<use>`. Colour remains controlled by CSS `color`, while the functional home link keeps the accessible label. Visible page titles and metadata still contain normal text.

## Honest remaining display-master P1 work

R2 is a working optical master, not a claim of a finished trademark drawing. A later display/master round should still consider:

- narrowing or softening the flat apex of `А`;
- refining the lower `с` terminal rather than leaving a hard cut;
- adding a small optical relief at the bowl/stem join in `ы`;
- producing a separate display master and testing print/reproduction requirements;
- deciding whether the standalone `о` replaces, complements or remains separate from the existing umbrella monogram.

## Acceptance matrix

| Check | Contract |
|---|---|
| `20px` | Word remains readable; the `о` counter is visibly open; no pair becomes a space. |
| `24px` | Reverse white-on-terracotta keeps the `А`, `о`, `с` and `ы` counters open. |
| `32px` | Squircle family is visible without making every rounded glyph equally wide. |
| `64px` | Curves remain smooth; the eight-segment `о` does not expose polygonal noise. |
| Header `1024px` | Secondary endorsement/wordmark yields before navigation; all five links remain. |
| Header `1280–1920px` | Wordmark is calm enough not to compete with the event title or right-aligned navigation. |
| SVG | One wordmark path, zero transforms, `<2KiB`, currentColor, no text/stroke/filter/mask. |

## Production use

The user approved the R3 full-name text tag on 2026-07-14. The R2 wordmark is now consumed by the shared desktop/mobile lockup component, and its optically opened standalone-`о` logic informs the installed transparent tag favicon. Normative placement, sizes and clear space are maintained in [`design-system/brand-lockups.md`](design-system/brand-lockups.md) and [`design-system/favicon.md`](design-system/favicon.md); this document remains the construction/audit record.
