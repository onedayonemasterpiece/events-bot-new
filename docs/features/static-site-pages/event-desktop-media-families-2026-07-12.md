# Desktop event media families — 2026-07-12

Status: **clean noindex desktop-only review pages; no layout is promoted to production**.

Current clean review target: <https://kenigevents.ru/preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/>. Preserved media matrix: <https://kenigevents.ru/preview-20260712t-desktop-media-families/lab/event-desktop/>.

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
- If a carousel is later introduced, W3C requires keyboard operation, announced state and user control over motion; the current prototype avoids autoplay entirely ([WAI carousel tutorial](https://www.w3.org/WAI/tutorials/carousels/)).
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
