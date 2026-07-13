# Desktop event media families — 2026-07-12

Status: **clean noindex desktop-only review pages; no layout is promoted to production**.

Current clean review target: <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/>. Preserved v4 review: <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/>. Preserved v3 review: <https://kenigevents.ru/preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/>. Preserved v2 review: <https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/>. Preserved media matrix: <https://kenigevents.ru/preview-20260712t-desktop-media-families/lab/event-desktop/>.

The previous `preview-20260712t-desktop-multimedia-full-flow` pages are rejected as a product review surface: they exposed research/service explanations, changed the accepted media-family geometry, moved the gallery into the story flow and did not make the parallax behavior clear. They remain only as failure/rollback evidence. The production mobile hero-overlap composition is explicitly outside this change.

## Core split

The desktop renderer must not infer one crop rule from orientation alone.

| Media contract | Layout behavior | Crop contract |
|---|---|---|
| `visual_only` / no meaningful OCR | Treat as a photograph: build a strong hero, use the available canvas, use focal `object-position` when available. | `cover` is expected; assertive crop is allowed. |
| `ocr_text` | Treat as a document/poster: keep embedded date, venue, partners and conditions readable; allocate width from source ratio and viewport height. | `contain` by default. `cover` requires an explicit source-grounded/manual `safe_crop` decision **and** measured source-area loss `<=20%`; otherwise automatic fallback to `contain`. |
| `unknown` | Safety fallback until classification is reliable. | Follow OCR policy, not photo policy. |

The 20% limit is measured from actual frame and source geometry:

```text
scale = max(frameWidth / imageWidth, frameHeight / imageHeight)
visibleFraction = frameArea / (sourceArea * scale²)
cropFraction = 1 - visibleFraction
```

Geometry is only a limit, not evidence that edge text is safe. The lab therefore enables cover only when `data-ocr-safe-cover=true`; all other OCR specimens remain contained even when the geometric crop would be under 20%. The lab exposes `data-measured-crop` and `data-potential-cover-crop` for Playwright acceptance. It does not hide empty areas with blur, duplicated images, gradients or fake backdrops.

## Real corpus evidence

Audit source: `312` real event pages from `preview-20260711t-desktop-event-layouts`.

- `194` are `visual_only`, `113` are `ocr_text`, `5` are `unknown`.
- OCR orientation: `81` portrait, `18` landscape, `14` square.
- OCR title length: median `29` characters, P25 `19`, P75 `38`.
- For portrait OCR, `52/81` (`64.2%`) satisfy the provisional Typographic Lead gate: `<=35` characters and `<=5` words.

Conclusion: Typographic Lead is not a universal layout, but its eligible population is large enough for a deterministic branch. A title outside the gate routes to Gallery Exhibition or Split Canvas.

Representative real specimens:

- no OCR: `5658` stage photo, `5264` pianist photo, `6794` vertical ship photo, `6033` vertical submarine photo;
- OCR: `4671` dense square poster, `6345` portrait poster, `6510` square poster + long H1, `6661` portrait poster + one-word H1.

The corpus audit is a design sample, not a claim that `image_text_mode` is perfect. Known source-grounded classifier misses still require exporter/Smart Update repair; layout safety must fail toward OCR-safe treatment.

## Candidate matrix

Each surviving candidate is rendered twice, once under each media contract.

1. **Editorial Slab**
   - no OCR: wide cover hero with a lower overlapping fact slab;
   - OCR: poster and slab become adjacent; safe-cover is used only with an explicit safe flag and within budget.
2. **Split Canvas**
   - no OCR: stable 55/45 photo/information split;
   - OCR: media-column width follows `viewport height × source ratio`.
3. **Gallery Exhibition**
   - no OCR: vertical photo may fill the frame through cover;
   - OCR: the frame follows source ratio, minimizing side fields without cropping text.
4. **Typographic Lead**
   - no OCR: experiment for strong vertical imagery and a short/medium HTML title;
   - OCR: deterministic short-title branch only; otherwise fallback.

`Immersive Bottom Horizon` and `Billboard + Action Rail` are not carried forward. The former makes OCR/title contrast fragile; the latter spends useful canvas on a service rail and reintroduces the empty-field problem.

## Width and height acceptance

The stage is height-aware (`svh`) and title type scales against both `vw` and `vh`. Event titles wrap naturally; `line-clamp`, ellipsis and title overflow clipping are forbidden.

The primary QA matrix is based on the June 2026 Windows resolution distribution in the official [Steam Hardware & Software Survey](https://store.steampowered.com/hwsurvey/?platform=pc): `1920×1080` is `52.77%`, `2560×1440` is `22.04%`, `2560×1600` is `5.93%`, `3840×2160` is `5.02%`, `3440×1440` is `3.08%`, and `1366×768` is `2.33%`. Steam is a practical desktop proxy, not KenigEvents audience analytics.

Automated viewports:

- `1366×768` baseline low-height desktop;
- `1920×1080` dominant Full HD;
- `2560×1440` QHD;
- `3440×1440` ultrawide;
- `1440×650` and `1920×600` deliberate browser-window stress cases.

For every viewport:

1. all eight title boxes stay fully inside their stage;
2. page horizontal overflow is zero;
3. OCR `data-measured-crop <= 0.20`;
4. all mandatory actions remain present and accessible;
5. production mobile components and styles are absent from the diff.

## Promotion recommendation

Do not promote one desktop composition globally.

- no OCR: Editorial Slab for landscape/square; Split Canvas as the stable fallback; Gallery is allowed for strong vertical photos;
- OCR: Gallery for portrait, Split for other ratios; safe-cover is a source-grounded, measured optimization, never a default;
- Typographic Lead: only after the title gate and media-quality gate pass.

This work changes only `/lab/event-desktop/`, desktop-only example routes, their lab components/data and checks/docs. Production `EventHero.astro`, production event-detail composition and the accepted mobile overlap geometry remain untouched; `EventLayout.astro` is only reused as the existing noindex shell.

## Published acceptance evidence

- Static build: `434` pages; `check:preview` passed.
- Public HTTP `200`: lab HTML, generated CSS, preview index, search and real event `6345`.
- Public Chromium repeated the six-viewport matrix with zero horizontal overflow, zero title/action clipping, zero action/content overlap and no OCR crop over budget.
- The only OCR cover specimen is the explicitly safe portrait Split case; measured crop is `0.05–0.08%`. All other OCR cases remain `contain`, including cases where geometric cover loss would be under 20% but edge text was not proven safe.

## Fresh active-catalog multimedia audit

The full-flow round uses a dated read-only production SQLite snapshot taken on `2026-07-12`, the exporter's active-event predicate and the same public projection gate as static generation. The grain is one public-eligible active event; long-running events are included when `end_date >= 2026-07-12`.

| Measure | Result | Interpretation |
|---|---:|---|
| Eligible active events | `326` | Current analysis denominator, not the older 312-page design sample above. |
| Events with at least one image | `312` | `95.7%` of eligible events. |
| Multi-image events | `195` | `59.8%` of eligible events; multiple-media behavior is a normal case. |
| Collected asset references | `1080` | Includes same-event source/managed URL aliases and at most 35 items for audit. Static rendering still caps the public gallery at 12. |
| Assets with confirmed dimensions | `1032/1080` (`95.6%`) | Unique URL resolution is `923/954` (`96.8%`); unresolved assets remain an explicit data-quality limitation. |
| Confirmed `visual_only` / OCR assets | `845 / 187` | Counts only assets with confirmed geometry. |

Orientation uses conservative bands: portrait `<0.9`, square `0.9–1.1`, landscape `>1.1`, and strong landscape `>=1.25`.

### Alternative-landscape hypothesis

- Strict population: `26` events have more than one image, no OCR image in their audited set, a current portrait/square visual primary, and at least one strong landscape visual alternative.
- Broader population: `30` events satisfy the same geometry when only primary and alternative must be `visual_only`.
- Within the static first-12 media contract, only `8/26` strict events have a landscape alternative at least `1600×720`; `19/26` reach `1280×720`.
- Therefore “a landscape exists” is not sufficient evidence for Editorial Slab. The alternate must also pass resolution, text-safety, semantic relevance and duplicate/derivative gates.

The approved routing rule for the lab is:

1. Primary already `visual_only` landscape and `>=1600×720` → Editorial Slab candidate.
2. Portrait/square primary → keep it unless a vision/LLM relevance gate proves that an alternative is at least as representative of this concrete event.
3. Promotion is vetoed for OCR/unknown media, source resolution below the Editorial target, unrelated crowd/venue/rehearsal shots, transport cards, or uncertain duplicate classification.
4. A crop/composite derivative may replace the first image only after a same-visual verifier and a semantic hero scorer both pass; geometry alone never promotes it.

Event `6032` is the explicit counterexample: the primary shows the Б-413 submarine; the large landscape alternatives show other objects in the broader museum complex. Event `6604` is the opposite data-shape warning: the square poster is embedded into a horizontal composite with an extra service panel, while the current OCR classifier marks both as visual-only. This is a classifier/dedup case, not permission to crop the text-heavy image.

### Resolution-constrained, not automatically “bad”, images

For this audit only, the operational slots are:

```text
Split-ready:     source width >= 800 and height >= 720
Editorial-ready: source width >= 1600 and height >= 720
```

- `403/845` confirmed visual-only assets are Split-ready but not Editorial-ready.
- `76/199` current visual-only primaries are in that same tier.

These are **source-resolution proxies**, not a trustworthy count of visually bad images. They do not measure blur, compression artifacts, focal correctness or semantic quality. The UI may safely render them in a smaller Split/Gallery slot, but Editorial promotion needs visual QA or a calibrated image-quality model in addition to pixel geometry.

## Duplicate and derivative policy

Existing 256-bit `dHash16` with the production Hamming threshold `<=20` found six near-duplicate pairs across six active events, but no orientation-changing pair. That result is useful for re-encodes and very close copies, not for ruling out crops or composites.

The contact-sheet review found at least one clear cross-ratio composite (`6604`) and same-ratio duplicate evidence (`6821`). The Gemini consultation also produced false positive derivative calls for `6032/6033`, where the images are visibly different museum photos. This is acceptance evidence that a VLM opinion must not delete media by itself.

Target two-stage pipeline:

1. **Candidate generation:** exact URL/SHA and dHash first, then multi-crop DINOv2 or SigLIP image embeddings within one event. DINOv2 is designed as an all-purpose visual-feature model; SigLIP supplies scalable image-language embeddings ([DINOv2 paper](https://arxiv.org/abs/2304.07193), [SigLIP paper](https://arxiv.org/abs/2303.15343)).
2. **Same-visual verifier:** local correspondence with LoFTR or SuperPoint + LightGlue, followed by RANSAC/geometric inlier checks. These match local visual evidence rather than only semantic similarity ([LoFTR, CVPR 2021](https://openaccess.thecvf.com/content/CVPR2021/html/Sun_LoFTR_Detector-Free_Local_Feature_Matching_With_Transformers_CVPR_2021_paper.html), [LightGlue paper](https://arxiv.org/abs/2306.13643)).
3. **Decision classes:** `exact/reencode`, `crop`, `composite`, `same_scene_distinct_frame`, `different`; only the first three may collapse, and composites retain the higher-information source rather than blindly keeping the first URL.
4. **Calibration:** tune on a labeled KenigEvents pair set for at least `99.5%` precision; an uncertain band stays visible and enters human review. A vision-language model may score event relevance for hero selection but cannot be the only destructive dedup judge.

## Research constraints applied

- A desktop-carousel usability experiment reported slower performance and a significant preference for the non-carousel interface, so the prototypes keep one editorial hero and expose the rest in a manual, non-autoplay gallery ([First Monday study](https://firstmonday.org/ojs/index.php/fm/article/view/11801)).
- W3C explicitly identifies parallax scrolling as non-essential motion that can trigger vestibular reactions. Parallax is therefore bounded to no-OCR Editorial/Split media, ends before the gallery/related feed, and is disabled by `prefers-reduced-motion` ([WCAG 2.3.3 explanation](https://www.w3.org/WAI/WCAG22/Understanding/animation-from-interactions)).
- If a carousel is introduced, W3C requires keyboard operation, announced state and user control over motion. The v3 Editorial photo probe autorotates only while the page is at the top, pauses on hover/manual choice and is disabled under reduced motion; the reading-column probe is scroll-driven and reversible ([WAI carousel tutorial](https://www.w3.org/WAI/tutorials/carousels/)).
- June 2026 global screen-resolution data reports `1920×1080` at `20.2%` and `1366×768` at `5.71%`, but screen resolution is not browser viewport. The lab retains `1440×650` and `1920×600` stress windows and must collect first-party viewport analytics before rollout ([StatCounter](https://gs.statcounter.com/screen-resolution-stats/desktop/worldwide/2020)).

## Gemini 3.1 Pro critical review

The approved `gemini` wrapper ran the Antigravity-hosted **Gemini 3.1 Pro (High)** consultation successfully (`status=0`); no Flash/Lite substitution was used. Raw local evidence is under `artifacts/codex/desktop-multimedia-analysis-20260712/gemini-pro-review.md`.

Accepted from the review:

- Editorial is not a universal default because `403` visual assets cannot support its target resolution without upscale.
- Do not promote a secondary landscape merely to fill the canvas.
- Default families remain Editorial for proven high-quality no-OCR landscape, Split for constrained/portrait no-OCR, and Gallery/contain for OCR.
- Sticky media must release before `Смотрите дальше`; no hero autoplay; reduced-motion is a hard gate.
- Neural candidate generation and geometric verification are separate stages.

Corrected after human/data review:

- `6032/6033` are not same-visual derivatives; the consultation overgeneralized from shared event context.
- The `>=800×720` / `>=1600×720` thresholds are operational slot contracts, not proof that an image will look good.
- Feedback bullets and transport details in prototypes are visibly marked as planned structure; no comment or schedule fact is invented.

## Clean full-flow real-event pages (v2)

The overview keeps the analytics and comparison matrix. The six directly reviewable event URLs contain only event UI: no candidate labels, audit metrics, technical rationale, planned-feature labels or scenario navigation.

| Family | No OCR / photo | OCR / poster |
|---|---|---|
| Editorial Slab | [Спектакль «Гараж»](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/editorial-photo/) | [«Эпидемия. Огненная рукопись»](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/editorial-ocr/) |
| Split Canvas | [Концерт «Закрытие сезона»](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/split-photo/) | [«Великие учителя»](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/split-ocr/) |
| Gallery Exhibition | [Экскурсия по Светлогорску](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/gallery-photo/) | [Магомаев и Анна Герман](https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/examples/gallery-ocr/) |

### Product and interaction contract

- The accepted media-family geometry is copied from the media-families lab: Editorial photo uses the `25%` lower image inset plus `min(64%, 820px)` info slab and `min(28%, 330px)` action slab; Split photo remains `55/45`; OCR Split and Gallery widths are ratio/viewport-height aware; Gallery media never exceeds `48%` of the stage.
- The desktop header is sticky. H1, exact date/weekday/time, venue and the primary CTA must remain fully visible in the first viewport, including `1440×650` and `1920×600` stress windows.
- No-OCR images use a bounded `96px` in-hero parallax range inside an overflow-clipped media frame. OCR images never receive parallax. `prefers-reduced-motion: reduce` removes the transform.
- Clicking the image or photo count opens the existing production fullscreen viewer with keyboard arrows, close/Escape and a readable counter. There is no second/lower-page gallery. Editorial photo may show an in-hero thumbnail rail without moving the event-story hierarchy.
- The event story continues through full description, a clean consolidated-feedback empty state, practical facts, an honest transport empty state for out-of-city examples, and the dark `Смотрите дальше` release boundary.
- `Смотрите дальше` reuses the production `EventCard` `split-actions` renderer, including normalized media/body alignment, `Не интересно`, calendar where eligible, share and like controls. The hero action row keeps stable calendar/share/like positions and shows truthful share/like values including zero; it invents no calendar count.
- Medallions reuse `EventTokenMedallions` and remain prominent; short desktop windows reduce them only to `72px`, not to placeholder circles.
- The whole clean event surface is `display:none` below `1024px`. No production event/mobile component or stylesheet is changed.

### Gemini 3.1 Pro gate

A fresh Antigravity `Gemini 3.1 Pro (High)` review was completed before v2 implementation. The approved `/home/dev/.local/bin/gemini` wrapper resolved its unoverridden default to `Gemini 3.1 Pro (High)` and invoked `a-agy-model`; the UTC run window was `2026-07-12 19:47:27–19:47:54`, exit status was `0`, and stderr was empty. Local evidence: `artifacts/codex/desktop-clean-event-pages-v2-20260712/gemini-pro-provenance.md`, `gemini-pro-brief.md` and `gemini-pro-review.md`.

Applied findings: preserve the exact approved geometry, remove the lower gallery and all service copy, keep photo parallax clipped to the hero, disable it for OCR, use honest empty states, reuse production cards/controls and keep the sticky header below the fullscreen viewer z-layer. The review did not override the measured ratio-aware OCR Split geometry from the existing media-family contract.

### Acceptance

Local Chromium acceptance covers all six pages at `1024×768`, `1366×768`, `1440×650`, `1920×600`, `1920×1080`, `2560×1440` and `3440×1440` (`42` layout runs), plus six `390×844` desktop-only isolation checks and a reduced-motion run. The gate checks first-viewport H1/date/venue/CTA visibility, zero horizontal overflow, family geometry, OCR crop/parallax policy, sticky header, fullscreen gallery open/advance/Escape, production related-card controls and absence of technical copy/lower gallery.

## Desktop scroll compositions (v3)

V3 does not replace the preserved v2 pages and still changes no production mobile component. It applies the latest scroll interpretation to the clean real-event examples:

- **Editorial photo:** the wide visual and ticket cluster form one sticky, slowly moving media layer. The white title/date/venue slab is normal document content and rises over the visual while scrolling. The thumbnail rail sits between the slab and ticket cluster; optional autorotation is controllable and stops after manual interaction, page scroll, hover or reduced-motion preference.
- **Editorial OCR:** the poster is flush left, uncropped and occupies the full available stage height. The right column consumes the remaining width and adds the short canonical event digest before venue and medallions.
- **Split OCR:** the poster has a `380px` desktop floor and a `620px` cap. Its natural ratio determines the full stage height; the right information and CTA rows remain compact enough for a `1920×600` first viewport while the poster leaves through ordinary document scrolling.
- **Sticky media + reading column:** a separate candidate keeps roughly `48%` sticky media on the left while the complete event information scrolls on the right. The active image is a deterministic function of scroll progress, so reverse scroll restores the previous image. The media is constrained by the reading shell and releases before `Смотрите дальше`.
- **Related rows:** cards are grouped by their measured row. A row with one OCR/document card follows its natural ratio; multiple OCR cards use the geometric minimax ratio `sqrt(minRatio × maxRatio)`. If the worst calculated crop exceeds `15%`, OCR cards fall back to `contain` on a solid graphite field. Photo cards use `cover`; media, body and action rows stay aligned.

Direct review URLs:

| Composition | URL |
|---|---|
| Editorial photo scroll | <https://kenigevents.ru/preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/examples/editorial-photo/> |
| Editorial OCR full-height | <https://kenigevents.ru/preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/examples/editorial-ocr/> |
| Split OCR natural scroll | <https://kenigevents.ru/preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/examples/split-ocr/> |
| Sticky media + reading column | <https://kenigevents.ru/preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/examples/reading-photo/> |

Gemini 3.1 Pro (High) completed a fresh v3 implementation review and its accepted recommendations are reflected above. The `a-opus` review was not completed because the provider returned `Individual quota reached`; it was not replaced by a lower-class model. Local evidence is stored under `artifacts/codex/desktop-scroll-compositions-v3-20260712/` and is not committed.

## Desktop continuous scroll compositions (v4)

V4 is still a noindex desktop-only experiment and does not modify the production mobile event page. It corrects the scroll model rather than adding another decorative shell:

- **Editorial Slab:** title/date/place/medallions, `О событии`, the complete description, consolidated-feedback state and practical facts now form one continuous cream slab. There is no second description block below the hero, and a duplicated source-level `О событии` heading is removed before rendering. The wide image stays behind the slab with bounded slow parallax, exits fully by the end of the description, and does not remain behind feedback/practical content; the thumbnail rail and CTA rise on the right, stick at `97px`, and release with the stage before `Смотрите дальше`.
- **OCR-primary Editorial rescue:** event `4671` explicitly promotes its verified `2560×1709` visual performance photo (`image_assets[3]`) instead of forcing the square OCR poster into the wide hero. The original poster remains the second labelled `Афиша` thumbnail and stays available in the fullscreen gallery.
- **Split OCR:** the full event information lives in the right column. The left poster is a physical tall track, not a fixed background or abrupt source swap; it moves upward at `0.35×` document progress and remains at least `380px` wide.
- **Physical image strip:** all four review images coexist in one vertical track. Scrolling down translates the track only upward, so the next image enters continuously from below; reverse scroll is naturally reversible. No time-based or wheel-hijacking carousel is used.
- **Portrait + adaptive Bento:** a selected portrait is followed by up to eight real event images. Runtime natural dimensions classify visual media as square, wide `2×1`, or tall `1×2`; tall spans are disabled below `1280px`, and the column changes from two to three to four tracks at `1280px` and `1600px`. The whole media column moves at `0.48×` and releases before related cards.
- **Motion and boundaries:** slow-media travel is clamped to measured track overflow. The stage gets the larger of natural content height and the media travel requirement; the content itself is never artificially stretched. `prefers-reduced-motion` removes transformations and returns media to normal flow.

Direct review URLs:

| Composition | URL |
|---|---|
| Editorial photo · continuous slab | <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/examples/editorial-photo/> |
| OCR-primary event · promoted landscape Editorial | <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/examples/editorial-ocr/> |
| Split OCR · slow physical poster | <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/examples/split-ocr/> |
| Physical vertical image strip | <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/examples/reading-photo/> |
| Portrait + adaptive Bento | <https://kenigevents.ru/preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/examples/bento-portrait/> |

Gemini `gemini-3.1-pro-preview` / Pro High completed the v4 design gate. Applied constraints include a `97px` side-stack top, negative-only bounded media translation, no scroll-jacking, two Bento columns below `1280px`, and no tall `1×2` spans in that narrow band. `a-opus` returned `Individual quota reached` and was not replaced by a lower-class model. Local prompts, raw reviews and Playwright evidence are stored under `artifacts/codex/desktop-scroll-compositions-v4-20260712/` and are not committed.

## Desktop media polish (v5)

V5 is a correction of the same desktop-only noindex lab, not a new production layout. The accepted mobile event surface and production `EventHero.astro` / `EventLayout.astro` remain unchanged.

- **Editorial motion:** downward document scroll now decreases the image's internal Y offset from `+32px` toward `-32px`. The visual therefore travels upward more slowly than the foreground slab instead of drifting down against the scroll direction. The existing scale/bleed keeps the bounded transform inside its clipped viewport, and reduced motion still removes it.
- **Editorial photo navigation:** the six previews are restored to one compact row (`44–68px` wide, `48px` high). A preview opens the existing fullscreen gallery at that exact image; the redundant `N фото` pill is absent from this composition.
- **Split OCR:** media and information use an exact `50/50` stage at desktop widths. The physical poster track retains its overflow-bounded release but moves at the slower `0.28` coefficient.
- **Portrait + Bento:** the media and reading columns also use `50/50`. The adaptive grid has square base cells, uses four columns from `1440px` and three below it, keeps OCR/unknown images top-aligned, centers ordinary photos, and promotes a real visual image with natural ratio `>=1.3` to a `2×1` cell. Every tile opens its own gallery position.
- **Related cards:** `Смотрите дальше` keeps the graphite section, but the card body and utility row are cream/light. Share is light and like remains red directly on the common graphite surface; neither action receives a second dark card wrapper.

The proportion decision combines the 12-column desktop convention from [Material responsive layout](https://m1.material.io/layout/responsive-ui.html), readable text-measure guidance from [MDN responsive design](https://developer.mozilla.org/en-US/docs/Learn_web_development/Core/CSS_layout/Responsive_Design), and inline inverse-surface guidance from [Carbon color](https://carbondesignsystem.com/elements/color/overview/). In this real-event lab, exact `50/50` gives the poster/photo enough physical width while the right column keeps prose constrained to a readable measure; narrower viewports change Bento density rather than collapsing the media column.

A fresh Antigravity **Gemini 3.1 Pro (High)** implementation review ran successfully on `2026-07-13` (`exit=0`, empty stderr; no Flash/Lite substitution). It agreed with `50/50`, a roughly `65ch` prose measure, `+32px → -32px` bounded Editorial travel, compact one-row previews, square Bento cells, a `>=1.3` wide-image threshold and the inverse related-card surface. Local prompt, provenance, raw review and Playwright evidence are stored under `artifacts/codex/desktop-media-polish-v5-20260713/` and are not committed.

Direct review URLs:

| Composition | URL |
|---|---|
| Editorial photo · corrected parallax and compact fullscreen rail | <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/examples/editorial-photo/> |
| OCR-primary event · landscape Editorial | <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/examples/editorial-ocr/> |
| Split OCR · 50/50 and slower physical poster | <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/examples/split-ocr/> |
| Physical vertical image strip | <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/examples/reading-photo/> |
| Portrait + square/wide Bento | <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v5/lab/event-desktop/examples/bento-portrait/> |

Local Chromium acceptance at `1024×768`, `1440×650`, `1920×600` and `1920×1080` records zero horizontal overflow. It additionally proves decreasing Editorial internal Y, a single-row six-item rail, selected-index fullscreen opening for Editorial and Bento, exact `0.5` Split/Bento media ratios, square base cells plus a real `2×1` image, and inverse related-card computed styles. Public HTTP returned `200` for the overview, all five active examples, preview index and generated CSS. Public Chromium repeated the key interaction/geometry/theme checks at `1440×900`, found no runtime errors or horizontal overflow, and confirmed that the desktop lab remains hidden at `390×844`.

## Desktop media polish (v6)

V6 remains confined to the noindex desktop lab at `min-width:1024px`; the production mobile event page, `EventHero.astro` and `EventLayout.astro` are unchanged. V5 remains published as a rollback/reference surface.

- **Stronger Editorial motion:** the internal photo offset now travels from `+64px` toward `-64px` during downward reading. A `160px` vertical bleed keeps the enlarged cover image inside the viewport, while reduced-motion still disables the transform.
- **Compact count-aware rail:** a six-cell Editorial rail shows five concrete previews and uses the last cell for `+N фото` when more media exists. That cell opens the first hidden gallery frame. Event `5658` therefore shows five previews plus `+2`, without restoring the duplicate floating photo-count control.
- **Source-grounded OCR safety:** lab scenarios explicitly correct known text-heavy source frames (`5658/image_assets[4]` and `5783/image_assets[2]`) instead of inferring poster semantics from filenames or aspect ratio. Those frames use the existing fullscreen `contain` contract. The Split OCR hero is also fully contained in its 50% media viewport rather than clipped by a physical poster track.
- **Persistent Split CTA:** the Split OCR action cluster becomes sticky below the `73px` header after reaching it and remains available while the long information column scrolls. An opaque paper mask fills the deliberate `12px` header gap so passing article text never appears as a clipped line above the CTA. Its stage/content containing block releases it before `Смотрите дальше`; no fixed body-level CTA is introduced.
- **Shorter related cards:** at desktop widths, visual-only rows use a square-to-`4:3` ratio and `cover`. Mixed rows use a shared square frame; OCR and unknown/document-safe media always use `contain` on graphite. A neutral cream/grey visual placeholder replaces a black hole while slow third-party lazy images load; the pending visual `<img>` background is explicitly transparent so it cannot cover the shell placeholder. Row grouping still aligns media, body and action boundaries, and all generated lab state is removed below `1024px`.

Review surface: <https://kenigevents.ru/preview-20260713t-desktop-media-polish-v6/lab/event-desktop/>.

Local and public Chromium acceptance cover all eight scenarios, selected-index fullscreen opening, the `+2` rail cell, OCR `contain`, Editorial `+64px → -58.8px` travel, Split CTA stick/release geometry, related-row media ratios, four desktop viewport sizes and the hidden `390×844` lab root. The public overview and all direct scenario URLs return HTTP `200`.

Gemini 3.1 Pro (High) then reviewed the real public browser captures for every top and `Смотрите дальше` state, plus the Editorial/Split gallery and sticky states. Its corrected verdict has no blocker: **Editorial Photo** is the preferred photo composition, **Split OCR** the preferred poster composition, and **Gallery Photo** the conservative fallback. It explicitly retained the stronger Editorial parallax, count-aware rail, sticky CTA contracts and OCR-safe containment. Its only material finding was a black frame while a third-party VK lazy image was still loading (`complete=true`, `1920×2560` after network settling); v6 now supplies a neutral visual placeholder for that latency state.

## Desktop focus v7: two final families

V7 removes Gallery/Reading/Bento from the active review surface and keeps only two desktop families. This remains a noindex laboratory at `min-width:1024px`; production `EventHero.astro`, `EventLayout.astro`, the mobile event composition and every rule below `1024px` are unchanged.

### Editorial

Editorial is allowed only when a real, semantically suitable horizontal visual is at least `1200px` wide with ratio `>=1.33`. The size gate is necessary but not sufficient: the asset still needs a meaning/quality decision.

- **Pinned comparison:** the media viewport is native `position:sticky`. The image moves upward inside its clipped frame through at most `140px` of measured real bleed. There is no JS exit transform: the containing stage releases the sticky element at normal `1.0x` document speed.
- **Continuous comparison:** the media viewport is never sticky. A uniform positive compensation of `0.35 * stageScroll` gives a measured net screen speed near `0.65x`; the media naturally crosses the top edge without an unpin/release transition.
- The `1280×853` «Гараж» source uses `object-position:50% 80%`. Necessary cover loss is allocated to the upper scene so the actors' legs and floor remain in the initial meaningful frame.
- If an event also has an OCR poster, CTA stays first in the sticky side stack, followed by a compact non-parallax OCR document and then the count-aware photo rail. The companion uses `contain` and opens the existing fullscreen gallery at its exact source index.

The current fixture metadata for `4671/image_assets[3]` says `1080×1350`, but the browser-decoded storage object is actually `2560×1709`; this asset therefore must not be classified from stale dimensions alone. The focused v7 OCR+horizontal example uses event `5783/image_assets[3]`, likewise verified from the source file as `2560×1707`, and retains `image_assets[0]` as the OCR companion. The OCR-without-landscape Split example uses event `5077/image_assets[0]`, a real `955×1280` poster whose physical height exceeds the media viewport at the 50/50 desktop split and whose valid registration CTA can be assessed in context.

### Split/Fallback

Split owns OCR without a strong horizontal image, portrait-only sets and low-resolution landscapes such as `800×602`.

- Media and information are exact `50/50` columns at desktop widths.
- The left viewport is sticky below the `73px` header. Its physical media track renders the selected image at `width:100%; height:auto`; the measured overflow, not `object-fit:contain` in a fixed frame, determines upward travel. A coefficient of `0.36` extends the stage enough to reveal the complete lower edge.
- The compact rail is part of that physical track directly below the selected image. It keeps stable fullscreen gallery indexes, shows five concrete cells when necessary and reserves the sixth for `+N`.
- `О событии` is the real section H2; the canonical summary is a subordinate lead paragraph, and source HTML headings are demoted one level.
- The action cluster remains sticky inside the information column. A `64px` trailing paper safe zone forces it to release before, and never touch, the graphite `Смотрите дальше` section.

### Routing examples

| Reality | Review route |
|---|---|
| Strong horizontal, pinned native release | `/lab/event-desktop/examples/editorial-photo/` |
| Same event, continuous `0.65x` media | `/lab/event-desktop/examples/editorial-photo-continuous/` |
| Strong horizontal + OCR companion | `/lab/event-desktop/examples/editorial-ocr-companion/` |
| OCR without a valid horizontal hero | `/lab/event-desktop/examples/split-ocr/` |
| Portrait-only visual set | `/lab/event-desktop/examples/split-portrait/` |
| Low-resolution horizontal fallback | `/lab/event-desktop/examples/split-low-resolution/` |

### Consultant and acceptance gate

Antigravity resolved the unoverridden consultant request to **Gemini 3.1 Pro (High)**. The design-contract run exited `0` with empty stderr and rejected the v6 accelerated exit, portrait-dimension fabrication, fixed-frame poster shrink and blind center cropping. It selected the CTA → OCR companion → rail order and the same Editorial/Split routing matrix. Local evidence is stored under `artifacts/codex/desktop-event-focus-v7-20260713/gemini/` and is not committed.

Acceptance requires desktop geometry at `1024×768`, `1440×650`, `1440×900`, `1920×600` and `1920×1080`; selected-index fullscreen opening; pinned normal-speed release; continuous monotonically upward `~0.65x` movement; exact Split half width; full poster-track travel; a CTA-to-related gap of at least `64px`; zero horizontal overflow; reduced-motion reset; and a `390×844` isolation smoke proving the laboratory root stays hidden.

The final local and public Playwright runs passed `57/57` checks. A targeted public boundary sample measured `64.28125px` between the sticky CTA and `Смотрите дальше`. In the related grid the light image/body/utility surface remains one card, while share and like intentionally sit without a second footer container on the common graphite section background (white share, red like), matching the approved inversion contract. Gemini 3.1 Pro (High) re-read these two pieces of evidence after its first-pass ambiguity, revised both items to `PASS` and returned the final recommendation **SHIP**.
