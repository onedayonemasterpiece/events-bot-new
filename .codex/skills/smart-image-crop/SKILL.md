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
   container use an approved named ratio and apply an allowed crop, or route the
   asset out of the normalized card family. A fullscreen/document viewer may
   follow the source ratio, but do not bake bars into the image.
2. **Fail closed on text.** Treat `ocr_text`, `unknown`, missing classification,
   posters, schedules, maps, menus, and attendee instructions as text-protected.
3. **Crop only text-free media by default.** `visual_only` is necessary but not
   sufficient for aggressive cover: also require event relevance, adequate
   resolution, `safe_crop=true`, and focal/face evidence when available.
4. **OCR normally stays uncropped.** Use an exact matching token or a dedicated
   natural-ratio document/detail surface outside normalized card grids.
   For an excessively vertical OCR asset only, a vertical cover crop is allowed
   when the **combined source area removed from top and bottom is at most 20%**,
   every OCR/face box plus safety margin remains visible, and the crop is
   recorded. Never use the 20% exception for left/right OCR crop.
5. **Normalized cards never invent arbitrary ratios.** If no named token is
   safe, use a separate poster companion/detail viewer, another approved
   event-relevant asset, or an intentional fixed-token text fallback.

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
  --width 800 --height 1200 --target P \
  --image-text-mode ocr_text \
  --ocr-box 0.08,0.12,0.84,0.68 --pretty
```

The planner never recommends fixed-frame `contain`. For cards/heroes/share it
returns an exact named-token frame, an evidence-safe named-token crop, or a
fallback/route decision. Natural source ratios are reserved for explicit
document/viewer surfaces.

### 3. Select the presentation mode

| State | Presentation | Crop gate |
|---|---|---|
| `visual_only` event photo | nearest approved surface ratio + `cover` | relevant, adequate resolution, `safe_crop`, focal/face-safe |
| OCR/document/unknown in a card | exact named token, or fixed-token fallback | no crop |
| OCR/document/unknown in detail/viewer | natural-ratio, edge-to-edge | outside normalized card family |
| excessively vertical OCR | bounded vertical `cover` | total loss `<= 0.20`, OCR/face boxes retained |
| mixed gallery | safe event photo for browse; identity poster preserved in detail/gallery | selection precedes crop |
| low resolution | another approved asset or intentional fallback | never upscale/pad |
| no image | standard designed fallback | keep event rank/order |

### 4. Choose a named ratio token

Never emit arbitrary `1.20`, `1.25`, `1.35`, or a raw source ratio for a
normalized card. Those values may be packing measurements, not media contracts.

| Token | Ratio | Allowed surfaces | Default use |
|---|---:|---|---|
| `P` | `4:5` (`0.8`) | card, hero, share | portrait poster/photo; mobile card |
| `S` | `1:1` (`1.0`) | card, hero, share | square/neutral card |
| `W` | `4:3` (`1.333…`) | card, hero | regular browse-wide card |
| `L` | `3:2` (`1.5`) | card, hero | landscape/editorial card |
| `H` | `16:10` (`1.6`) | hero only | cinematic detail hero |
| `OG` | `40:21` (`1200:630`) | share only | Open Graph preview |

Token selection is **surface- and composition-aware**, not a per-image
minimization contest. A listing family, editorial row, hero composition, or
campaign may deliberately request a wider allowed token (`W`, `L`, or `H`) even
when another token would remove fewer pixels. Record that intent as a stable
surface/role rule and apply it consistently to the cohort.

Use lowest crop loss as the safe baseline only when the surface has no stronger
composition requirement. In all cases compute loss for the requested token and
discard it if safety gates fail. OCR additionally rejects every horizontal crop
and every vertical crop above `20%` or intersecting protected boxes. If the
intentional token is unsafe, select another relevant asset, use another allowed
token, or render the fixed-token fallback; never waive the evidence gates and
never invent a fifth card ratio.

### 5. Implement no-field geometry

Natural ratio is allowed only in a dedicated document/detail/viewer surface:

```css
.media--natural { aspect-ratio: var(--source-w) / var(--source-h); }
.media--natural > img { display:block; width:100%; height:auto; }
```

For a normalized fixed-height card, derive width from the selected token:

```css
.card { width: calc(var(--media-h) * var(--ratio-token)); }
.card__media { height: var(--media-h); }
.card__media > img { width:100%; height:100%; }
```

For an exact token match, `contain` is unnecessary. For an approved crop:

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

### 6. Validate pixels and invariants

- Assert the token name, token-selection reason (`surface-default`,
  `minimum-safe-loss`, or a named editorial/composition role), rendered mode,
  source/target ratio, crop loss, crop axis, retained OCR/face boxes, and object
  position in markup or tests.
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
- Raw source ratios or experimental packing ratios as new card media tokens.
- Choosing a different token for every image solely to minimize crop when the
  product surface requires a coherent shared composition.
- Choosing a wide but unrelated image merely because it fills the slot.
- Treating `safe_crop` as overlay-safe; text overlay needs a separate safe region
  plus OCR/face/saliency clearance and contrast evidence.
- Using crop or padding to rescue weak pixels.
- Hiding a bad crop behind blur, gradients, or duplicated background media.
