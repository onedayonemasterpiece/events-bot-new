---
name: smart-image-crop
description: Use in events-bot-new when selecting, implementing, reviewing, or testing image crop and aspect-ratio behavior for event heroes, galleries, listing/discovery cards, thumbnails, previews, or share images—especially for OCR posters, unknown media, mixed photo/poster sets, focal/face-aware cover, no-letterbox layouts, and bounded vertical poster crop.
---

# Smart Image Crop

Choose the asset before choosing its geometry. Preserve text and identity, let
verified photos sell the event, and never disguise a ratio mismatch with fields.

## Non-negotiable contract

1. **No fields in bounded media tiles.** No letterbox/pillarbox, decorative
   four-sided padding, blurred duplicate, or ambient fill. Either make the
   container follow the source ratio or use an allowed crop. A fullscreen
   viewer may have surrounding page canvas, but do not bake bars into the image.
2. **Fail closed on text.** Treat `ocr_text`, `unknown`, missing classification,
   posters, schedules, maps, menus, and attendee instructions as text-protected.
3. **Crop only text-free media by default.** `visual_only` is necessary but not
   sufficient for aggressive cover: also require event relevance, adequate
   resolution, `safe_crop=true`, and focal/face evidence when available.
4. **OCR normally stays uncropped.** Adapt the container to the natural ratio.
   For an excessively vertical OCR asset only, a vertical cover crop is allowed
   when the **combined source area removed from top and bottom is at most 20%**,
   every OCR/face box plus safety margin remains visible, and the crop is
   recorded. Never use the 20% exception for left/right OCR crop.
5. **If those constraints cannot coexist, change the layout—not the evidence.**
   Use a natural-ratio card, a separate poster companion, another approved
   event-relevant asset, or an intentional text fallback.

## Workflow

### 1. Inspect the contract and the real asset set

- Read `site/src/lib/types.ts`, the target component, and its build/check tests.
- For listing product exploration, also use `static-listing-visual-lab`.
- Build a real corpus containing OCR, unknown, visual-only, portrait, landscape,
  square, mixed-gallery, low-resolution, and no-image cases.
- Load [references/project-findings.md](references/project-findings.md) when
  changing static-site behavior or choosing a surface-specific ratio.

Do not infer crop safety from the URL, orientation, first gallery position, or
`safe_crop` alone. Asset selection order is approval/deduplication → event
relevance → semantic role/text mode → surface purpose → resolution → geometry.

### 2. Compute the loss before writing CSS

For source ratio `s = width / height` and target ratio `t`, cover removes:

```text
loss = 1 - min(s / t, t / s)
```

This is the fraction of source area removed. If `s < t`, loss is vertical
(top/bottom); if `s > t`, it is horizontal (left/right).

Use the deterministic planner for a single candidate:

```bash
python3 .codex/skills/smart-image-crop/scripts/plan_crop.py \
  --width 800 --height 1200 --target 4:5 \
  --image-text-mode ocr_text \
  --ocr-box 0.08,0.12,0.84,0.68 --pretty
```

The planner never recommends fixed-frame `contain`. It returns either an exact
or natural-ratio no-field layout, or an evidence-safe cover crop.

### 3. Select the presentation mode

| State | Presentation | Crop gate |
|---|---|---|
| `visual_only` event photo | nearest approved surface ratio + `cover` | relevant, adequate resolution, `safe_crop`, focal/face-safe |
| OCR/document/unknown | natural-ratio, edge-to-edge | none |
| excessively vertical OCR | bounded vertical `cover` | total loss `<= 0.20`, OCR/face boxes retained |
| mixed gallery | safe event photo for browse; identity poster preserved in detail/gallery | selection precedes crop |
| low resolution | another approved asset or intentional fallback | never upscale/pad |
| no image | standard designed fallback | keep event rank/order |

Use a finite token set for normalized text-free photos rather than arbitrary
ratios: mobile portrait `4:5`, square `1:1`, browse-wide `4:3` or `3:2`, and
wide hero/share `16:10` or `1.91:1` only when the surface requires it. Choose the
token with the least safe loss; do not inherit desktop geometry blindly on mobile.

### 4. Implement no-field geometry

For protected media, the frame follows the source:

```css
.media--natural { aspect-ratio: var(--source-w) / var(--source-h); }
.media--natural > img { display:block; width:100%; height:auto; }
```

For fixed-height desktop flows, derive card width from source ratio instead:

```css
.card { width: calc(var(--media-h) * var(--source-ratio)); }
.card__media { height: var(--media-h); }
.card__media > img { width:100%; height:100%; }
```

Because frame and source ratios match, `contain` is unnecessary and cannot
create bars. For approved crop:

```css
.media--cover { overflow:hidden; aspect-ratio:var(--target-ratio); }
.media--cover > img {
  width:100%; height:100%; object-fit:cover;
  object-position:var(--object-position, 50% 50%);
}
```

Emit real `width`/`height`, reserve geometry to prevent CLS, provide responsive
derivatives and `srcset`, and never select a derivative smaller than its rendered
size at device pixel ratio.

### 5. Validate pixels and invariants

- Assert the rendered mode, source/target ratio, crop loss, crop axis, retained
  OCR/face boxes, and object position in markup or tests.
- Capture mobile, desktop, and the breakpoint where the geometry changes.
- Inspect pixels: no bars, no missing poster text, no cut faces, no accidental
  upscale, no overflow, and no geometry-driven event reorder/drop.
- Test OCR near the top and bottom, extreme portrait, extreme landscape,
  missing dimensions, unknown classification, and a safe visual photo.
- With parallax or pan, preserve the same crop envelope throughout, do not zoom
  OCR, and disable motion for `prefers-reduced-motion`.

Run the bundled planner tests after editing its rules:

```bash
python3 .codex/skills/smart-image-crop/scripts/test_plan_crop.py
```

## Forbidden shortcuts

- `object-fit:cover` for every card or thumbnail.
- `object-fit:contain` inside a mismatched fixed-ratio colored frame.
- Choosing a wide but unrelated image merely because it fills the slot.
- Treating `safe_crop` as overlay-safe; text overlay needs a separate safe region
  plus OCR/face/saliency clearance and contrast evidence.
- Using crop or padding to rescue weak pixels.
- Hiding a bad crop behind blur, gradients, or duplicated background media.
