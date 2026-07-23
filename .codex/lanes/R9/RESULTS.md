# R9 integrated mobile acceptance

Branch: `integration/mobile-acceptance-r9-20260723`

Preview build: `preview-20260723-unified-corrections-r9`

The build is an immutable `noindex` review surface. Production generation and
the stable production prefix are not changed.

## Requirement coverage

| ID | Status | Integrated result |
| --- | --- | --- |
| R1 | Done | Full mobile event detail uses the accepted shared stitched-leather Reference4 tag with an immediate `#98401f` fallback and no pale logo scrim. |
| R2 | Done | Rail media reserves the final `112px` slot and keeps a decode-aware skeleton until success or a stable error state. |
| R3 | Done | Exactly one explicitly classified `visual_only` portrait may use the `140×112` landscape `5:4` cover; OCR, unknown/document media and multi-image rows fail closed. |
| R4 | Done | Weekend/date listings use the full `56px` date rail and calendar sheet above the `64px` dock. Only generated routes are links; unavailable days are disabled non-links. |
| R5 | Done | The accepted A-tail amber artifact is reachable on mobile `/vyhodnye/` after the large like, only when the explicit noindex research flag is present. Production is hard-blocked. |
| R6 | Done | The compact page title and shelf headings physically stick below the `64px` header; Popular retains the accepted `80px` shelf heading. |
| R7 | Done | Public and lab exhibition seals use the owner-corrected `44×44px` mobile size without counter overlap or horizontal overflow. |
| R8 | Done | Search keeps the accepted in-button progress visual and owns duplicate-submit, success, error, abort, logout and page-exit reset states. This is a visual/lifecycle acceptance, not owner acceptance of the live authenticated backend journey. |
| R9 | Done | Mobile event organizer/Main and free/Secondary circular medallions share one responsive diameter and baseline; semantic role ordering is unchanged. |
| R10 | Done | The rail continuation cue is a literal straight `48×23` inline SVG with a horizontal shaft and symmetric head. |

## Donor and evidence boundary

- Accepted rail/calendar/Search lineage:
  `integration/mobile-search-unified-v14-20260722@3f5b88f9` and the preserved
  v28 public specimens.
- Accepted leather asset lineage:
  `mobile-head-skinny-leather-3x.webp` from `94833f10`, integrated through the
  shared Reference4 mobile shell.
- Accepted exhibition lineage:
  `integration/exhibitions-personal-discovery-prototype-20260719@54cfa903`;
  the later owner correction raises the integrated mobile seal from the old
  `36px` donor value to `44px`.
- Accepted artifact contract:
  `docs/features/static-site-pages/amber-artifact-easter-egg.md`, A-tail only.
- The skeleton and the narrow single-visual crop guard are explicit R9
  corrections, not claimed donor copies.

## Integrated validation

- Focused Node suite: `52/52` passed.
- Occurrence resolver/formatter suite: `10/10` passed.
- Astro preview build: `389` pages, pass.
- `check:preview`: pass, `288` events.
- `check:unified-prototype`: pass, `18` primary routes, `39` hub links,
  `288` event pages and `373` checked related cards.
- Rail Playwright: pass at `320×700` and `390×844`, including skeleton,
  cached/error media states, sticky hierarchy, date/calendar geometry,
  emitted-link HTTP checks, A-tail collection and reduced motion.
- Exhibition Playwright: pass at `320`, `390` and `430px`; seal is exactly
  `44×44px`, complete, non-overlapping and without horizontal overflow.
- Search Playwright: pass at `390×844`, DPR 2; progress state and duplicate
  submit lifecycle verified against the configured preview project reference.
- Integrated event screenshot at `390px`: no horizontal overflow; organizer
  and free medallions both measure `89.6875px` and share the same bottom edge.

Uncommitted browser evidence is stored under
`artifacts/codex/mobile-acceptance-r9/` and is intentionally excluded from Git.
