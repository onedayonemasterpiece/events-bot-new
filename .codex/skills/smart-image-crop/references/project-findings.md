# Project findings: OCR-safe smart crop

## Audit scope

Snapshot: 2026-07-19 after `git fetch origin --prune`.

The repository audit used `git log --all -G` and `git grep` across every local
and `origin/*` ref, then inspected the relevant component histories. The ref
inventory contained 786 refs; 225 branch refs matched the static/listing/event
page/image-geometry families (133 unique tips). The history scan found 130
unique crop/aspect/media-contract commits under `site/src`, `site/scripts`, and
`site/tests`.

These counts document the research snapshot, not a permanent repository
invariant. Re-run the scan when a later task depends on newer branches.

## Practices worth keeping

### 1. Text mode is the first fork

Commits `db47db08`, `14dd20bf`, and `7b3f1afe` established the durable rule:
OCR/unknown is protected; verified `visual_only` may cover. The strongest part
of this work is the fail-closed classification, not the early fixed `3:4`/`4:5`
frame itself.

Adopt:

- `ocr_text | unknown` never silently becomes photo-cover;
- explicit mode markers and build assertions;
- a real regression fixture for both text poster and text-free photo.

Improve:

- early `contain` inside a fixed dark frame preserved text but created fields;
  current no-field policy makes the container/card follow the source ratio.

### 2. Late listing branches solved mixed ratios best

The `listing-surfaces-v19` through `v26` branches introduced
`site/src/lib/listingPresentation.ts`, `ListingEventCard.astro`, and the listing
design-system CSS. Their best ideas are:

- choose by semantic role before geometry;
- preserve poster/unknown evidence instead of forcing photo behavior;
- the branches computed card width from fixed media height × source ratio. This
  was a strong no-field experiment, but the final normalized-card contract now
  snaps safe media to named `P/S/W/L` tokens rather than exposing every raw
  source ratio as a separate card width;
- crop only a classified `event_photo` with `visual_only`, high role confidence,
  `safe_crop`, focal evidence, and enough retained area;
- prefer a relevant wider candidate but do not pretend unknown media is a photo;
- do not upscale small assets; include the original as the last `srcset` DPR
  candidate when derivatives stop too early;
- preserve rank order while flex-wrap absorbs different card widths.

The branch used approximate browse targets `1.35` (regular), `1.20` (weekend),
and `1.25` (popular), plus adaptive safe-photo envelopes. These are packing
experiments, not approved image-ratio tokens; do not ship them as new ratios.

### 3. Related cards contributed the 20% crop math

`site/src/lib/relatedCardLayout.mjs` (commits `be7c8a85` / `7c856ebf`) defines:

```text
potentialCoverCrop = 1 - min(sourceRatio / targetRatio,
                             targetRatio / sourceRatio)
MAX_DOCUMENT_CROP = 0.2
```

It also contributed useful row logic:

- use a geometric mean/minimax ratio for a row;
- clamp the shared target to the intersection of every protected asset's safe
  ratio interval;
- record per-card and worst-row crop loss for acceptance tests.

Keep the math. Replace its incompatible-row `document-contain` fallback with
natural-ratio cards, row splitting, or a different layout, because fixed-frame
contain violates the no-fields requirement.

For a source ratio `s`, all cover targets with at most loss `L` lie in:

```text
[s × (1 - L), s / (1 - L)]
```

With OCR and `L=0.20`, use only the upper part of this interval (`target >= s`),
because the exception permits top/bottom crop, not left/right crop.

### 4. Event detail pages separate identity from emotion

`site/src/lib/desktopEventPresentation.ts` and `EventHero.astro` show that one
geometry cannot serve every event:

- a crop-safe high-resolution landscape event photo may become an editorial
  hero;
- a classified identity poster remains a separate companion/gallery item;
- non-identity documents (schedule, map, attendee note) must not monopolize the
  hero when a strong event photo exists;
- portrait/square and low-resolution assets route to split/natural presentation;
- a weak single portrait is not visibly upscaled;
- `recommended_object_position` → event override → focal point → center is a
  sensible object-position precedence;
- auto-rotation is allowed only among compatible visual photos, not across OCR
  and photo families.

This is a strong general rule: **asset selection precedes crop selection**.

### 5. Fullscreen and motion need a stable crop envelope

The accepted gallery work preserves OCR/unknown as a complete document and lets
visual-only photos use controlled cover/pan. The mobile OCR parallax work
(`94596e7e`) proved three useful gates:

- no OCR zoom;
- movement cannot uncover a layout gap or exceed the accepted crop envelope;
- perceived velocity can match the photo hero without changing OCR scale;
- `prefers-reduced-motion` is a hard stop.

A viewer canvas around a natural-ratio image is acceptable; a fabricated bar or
blurred duplicate inside a bounded preview tile is not.

### 6. Geometry metadata belongs offline, not in page runtime

The `integration/image-geometry-20260717` line (`2d019629` plus follow-ups)
added source dimensions, face boxes, and a valuable-region result through a
durable asynchronous enrichment job with pixel-identity caching and strict box
validation. The static-site contract also carries OCR boxes, saliency boxes,
focal point, recommended fit/position, and safe-crop state.

Adopt:

- normalized 0..1 boxes tied to a pixel fingerprint;
- strict coordinate validation;
- batch/offline enrichment and cached reuse;
- item-level failure, with unknown/fail-closed rendering until metadata exists;
- visual contact-sheet inspection before rollout.

Do not run OCR/face/saliency analysis in the static page.

## Surface decision guide

### Event hero

- OCR/unknown: full-width natural ratio on mobile; split/natural document column
  on desktop. No fixed cinematic frame.
- Verified event photo: `4:3`, `3:2`, or `16:10` cover depending on viewport and
  composition; use focal/face-safe position.
- Mixed poster + photo: photo may lead browse/editorial emotion; poster stays
  available as event identity.

### Listing/discovery card

- Normalize to one of `P 4:5`, `S 1:1`, `W 4:3`, `L 3:2`. Never create a raw
  source-ratio or experimental `1.20/1.25/1.35` card.
- OCR/unknown: use an exact token match without crop. For a too-vertical source,
  a token may use the bounded crop exception below. If no token is safe, show a
  fixed-token designed fallback and keep the poster for detail/gallery.
- Excessively vertical OCR: first try the natural card. If density truly cannot
  work, allow top/bottom crop only when total loss is `<=20%` and protected
  boxes remain visible.
- Verified photo: choose the nearest approved token and cover at focal point.
- Packing must never reorder or omit ranked events.

### Thumbnail/navigation rail

- A thumbnail is still evidence. Do not crop OCR merely because the text is
  small; use its natural-ratio thumb, a compact identity tile, or a separate
  photo thumbnail.
- Visual-only thumbnails may cover after the normal gates.

### Gallery/viewer

- Natural-ratio OCR/unknown; surrounding viewer canvas is not a media-tile field.
- Cover/pan only visual photos; stop auto behavior on manual reverse navigation
  and under reduced motion.

### Share/social preview

- Required output targets may include `1200×630` (`1.91:1`), `1080×1350`
  (`4:5`), and `1080×1080` (`1:1`).
- Generate separate compositions. Do not derive all variants by center-cropping
  one OCR poster.
- Keep event title/date/place/CTA as HTML/rendered layout text outside the source
  image unless a separately persisted overlay-safe region exists.

## Rejected patterns found in history

- one `cover` rule for every listing image;
- one fixed ratio plus `contain`/dark bars for every OCR poster;
- choosing landscape only because it fills better;
- using `safe_crop` as permission for text overlay;
- low-resolution upscale or padded rescue;
- blur-fill/duplicated backdrop to hide ratio mismatch;
- arbitrary per-card photo ratios that destroy row rhythm;
- raw OCR/source ratios promoted into an unbounded family of card ratios;
- geometry-driven reorder, drop, or rank-aware Bento packing;
- parallax/zoom that changes the accepted crop after first paint.
