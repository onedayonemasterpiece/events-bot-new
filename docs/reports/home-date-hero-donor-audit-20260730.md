# Home Hero Talk and date mosaic donor audit — 2026-07-30

## Scope and sources

- Home donor: `b5f4797d`, `site/src/data/briefingLab.ts` and
  `site/src/pages/lab/briefing/index.astro`.
- Public Home reference:
  `/preview-20260717t1458-briefing-lab-b5f4797d/lab/briefing/`
  with `variant=c`, `scenario=media_review_hay_day`.
- Date donor: accepted mobile calendar v23 public HTML/CSS/JS under
  `/preview-20260721-mobile-calendar-v23/date-2026-07-24/`; v21 parallax was
  used only to trace the visual lineage.
- Rejected integration baseline: focus candidate `21f02dfc`.

The audit compared source contracts, DOM, computed geometry, initial paint,
terminal opacity topology and scroll/resize lifecycle. A feature name or the
presence of tiles was not accepted as evidence of donor parity.

## Home findings

| Contract | Donor | Rejected implementation | Root cause |
|---|---|---|---|
| Copy model | Authored semantic fragments; only selected words link | `event.title` plus generic eyebrow/date; whole scene was an anchor | Donor data schema was discarded and a new card-like API was invented |
| Plane | `100vw`, no card frame | constrained `1240px` rounded card with border/shadow | Hero remained inside the former page card composition |
| Media geometry | `75vw` to viewport right; `16/18/20×5`; `row-gap:0` | small rounded 16×7 media card with gaps both ways | Responsive donor geometry was not transplanted |
| Final image state | multi-band partial field; copy/face shields; only edge cells full | all 112 tiles animated to `opacity:1` | “Irregular” was reduced to staggered delays instead of opacity topology |
| Loading | raster is decoder/preload only; atomic next-scene decode | raster visible until JS marked mosaic ready | fallback image was treated as presentation |
| Safety | crop/face projection and upscale ceiling `1.10` | no face projection or runtime upscale abstention | media eligibility was assumed to be enough at all viewport sizes |

Why it passed earlier: unit tests asserted event IDs, modes and family
deduplication only. They did not assert fragment links, viewport geometry,
tile count, terminal opacity bands, preload dimensions or face/copy shields.
The documentation was then changed to describe the simplified replacement,
so it stopped acting as a donor regression contract.

## Date findings

| Contract | Donor v23 | Rejected implementation | Root cause |
|---|---|---|---|
| First paint | invisible `1×1` preload; tiles only | full `372×202` photo before decode | visible fallback was hidden only after JS |
| Vertical origin | page head `y=64`, hero `y=0` | page head `y=84`, hero `y=20` | Reference4 menu overrode rail `64px` main padding with `84px` |
| Base field | sparse exact `6×11` matrix, strong top-right and weak bottom-left | every column from 2 onward `.55–.94` regardless of row | accepted matrix was replaced with a column shortcut |
| Runtime | one crypto seed per load, stable through height resize | deterministic build seed for every reload | exploratory deterministic behavior was documented as accepted behavior |
| Protection | text max `.04`, first columns max `.06`, face min `.56`, global max `.92` | none | v23 geometry runtime was omitted |
| Reduced motion | static partial mosaic | full image | reduced-motion fallback bypassed the visual contract |

## Transplanted invariants

Home:

- 28-entry grounded editorial launch bank with semantic fragments;
- no whole-scene anchor;
- `100vw` plane, `75vw` right-edge mosaic;
- adaptive `16/18/20×5`, `row-gap:0`;
- donor directional/noise opacity levels and irregular entry accents;
- copy/face protection, invisible decoder and `1.10` upscale abstention.

Date:

- exact donor sparse `6×11` field;
- invisible `1×1` preload and no full-raster state;
- one per-load crypto schedule, stable on height-only resize;
- text/face/edge clamps and `.92` maximum;
- scoped `64px` rail shell override restoring hero `y=0`;
- reduced motion keeps the partial mosaic.

## Measured acceptance evidence

Local Playwright at `1440×900`:

- Home `x=0`, `width=1440`;
- media `x=360`, `right=1440`, `width=1080`;
- `16×5 = 80` active tiles, row gap `0`;
- 70/80 cells below `.98`, 10 full edge cells;
- decoder image `1×1`, no horizontal overflow;
- four fragment links and zero whole-scene anchors.

Local Playwright at `390×844`:

- date hero `x=18`, `y=0`, `width=372`, `right=390`;
- decoder image `1×1`, no full-size image, 66 tiles;
- 27 cells at or below `.061`, maximum `.92`;
- top-right mean `.770`, bottom-left mean `.033`;
- height-only resize retained seed and all alpha values;
- reload generated a different seed while preserving the directional field.

Raw non-committed QA output and screenshots live under
`artifacts/codex/hero-audit-20260730/`.
