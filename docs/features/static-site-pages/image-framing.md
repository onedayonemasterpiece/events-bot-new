# Event image framing and focal metadata

> Status: **pixel-current producer/export implemented; joint owner framing correction active, not visually accepted**.
> Current authority: owner follow-up in #621/5551113067 and subsequent full-pool clarification, mirrored in DS contract 1.14.2. The rules below are acceptance requirements; they do not certify the current deployed candidate.

## Contract

- Hero/gallery photography classified as `visual_only` always fills its frame
  with `cover`; semantic-role uncertainty must not turn a non-OCR photograph
  into a letterboxed document. OCR/text/unknown hero media keeps the stricter
  full-poster `contain` policy.
- Missing `image_text_mode`, missing OCR evidence, or
  `media_semantic_status=error|unknown` is **not** permission to infer
  `visual_only`. Large event media fails closed to document-safe `contain`
  until classification succeeds. This protects text-heavy posters such as the
  `6686` regression without an event-id exception.
- Compact cards never have unused bands around an image: every packed card uses
  `cover`, in both static related rows and hydrated continuation rows.
- A normal OCR/document card defines an exact feasible row ratio and is not
  cropped. Only after grouping the full eligible pool proves insufficient may a safest
  vertical crop widen that interval: at most `20%` of source area, with positive
  evidence that important text is retained. Tallness or the area budget alone
  is never permission to crop an OCR/unknown source.
- Cards in a row have one media height and one total card height. Cards have equal widths within a row; variable-width packing is prohibited.
  Before the first paint, cards may be grouped across the full eligible pool.
  After paint, append/feedback must preserve the visible prefix and active card;
  raw retrieval order is not the same thing as this visible-order guarantee.
- A non-final row is always full. The optimizer may emit one incomplete row
  only as the final row of the section; it may choose which cards belong to
  that remainder so OCR-safe grouping does not create holes above it.
- Without a constraining OCR/document neighbour the compact target is
  width/height `5/4` (a horizontal frame). A wider target is allowed when it
  reduces the whole-page height or aligns with a horizontal neighbour.
- Framing metadata is versioned with the media/content hash and may be manually
  overridden.
- A human-reviewed override is keyed by the exact source media URL/hash, not by
  event id, title or visual similarity. Every consumer of that same source
  (`EventCard`, related rows and hydrated continuations) must apply the same
  reviewed role, dimensions and replacement asset. This is metadata reuse, not
  semantic inference.

## Global compact-row optimizer

`site/src/lib/relatedCardLayout.mjs` is the shared row owner. Compose from the
**complete eligible collection/discovery pool before applying the initial page
limit**. Group compatible document ratios and use genuinely `visual_only`
fillers, then paginate the resulting stable sequence. A conflict among the first
12 Free cards or the first 10 related cards does not establish corpus scarcity.
Do not limit the search to three extra candidates or discard admitted events to
manufacture a feasible prefix. Server and hydrated clients must agree on the
planned sequence, and appending must never move already visible cards.

Within this constraint, prefer full compatible natural-ratio rows, less safe
crop and less source-order displacement. Only the final row may be incomplete;
its cards keep regular column widths, not stretched widths. All admitted IDs
must survive exactly once. Protected-text and painted-bounds checks are joint:
`contain` in an arbitrary fixed shell is protection, not accepted no-fields
geometry. A natural image with a matching natural frame may use `contain`
without letterboxing; CSS `object-fit` alone is not the acceptance evidence.

A document without usable positive crop evidence contributes only its natural
ratio. A proposed fallback must fit the approved 20% vertical-crop budget **and**
retain important text using source-bound evidence. No unconditional
`[naturalRatio, naturalRatio / 0.8]` interval is allowed. If no safe complete
partition exists, retain all events and report the smallest actual full-pool
conflict explicitly; do not call remaining fields a PASS. Existing diagnostics
(`rowWorstCrop`, `coverCrop`, row status) support, but do not replace, decoded
browser measurements of image bounds, source crop, row geometry and text safety.

## Pixel-current bbox contract

Static export joins `event_image_geometry` only for `status=classified`, exact
poster/geometry `pixel_sha256` equality and the current geometry model/prompt.
The public asset carries normalized `face_boxes`, one `valuable_region`, the
coordinate space and pixel/model/prompt provenance. Missing tables, incomplete
boxes and stale provenance emit no usable geometry rather than old coordinates.

Stored coordinates remain reusable metadata; the exporter does not invent a
final crop. A surface with a **known** target aspect may run the deterministic
protected-region solver: it adds a bounded margin around faces plus the valuable
region, chooses a `cover` window only when the complete union fits, and returns
the matching CSS object position without whole-percent rounding that could move
the browser crop past a tight protected boundary. If the union does not fit,
geometry is stale, the image is OCR/text, or the responsive target ratio is
unknown, the strict OCR/large-surface renderer uses `contain`. A `visual_only`
hero/gallery is the deliberate product exception and fills with `cover`.
Compact cards use the separate bounded-row contract above: documents constrain
the row instead of creating bands, and the scarcity fallback may lose at most `20%` of source area only with
positive important-text safety evidence.

Production desktop post-build contract checks the actual surface contract. A
non-OCR hero/gallery media must compute to `cover`. In recommendation rows,
every loaded image must have zero unused frame budget. Natural-frame
documents need no crop; any `document-safe-cover` case must have proven text
safety and an applied crop no greater than `20%`. The gate decodes the real image, checks loaded/fallback
layers, equal media and card heights per row, records the independent
unused-frame/crop ratios and retains the rendered row screenshot.

## Required producer

Offline Smart Update/media preparation or Kaggle enrichment emits:

- `image_text_mode`;
- normalized focal point or CSS object position;
- face/person boxes where reliable;
- confidence, model/algorithm version and media hash;
- manual override and recompute status.

The browser/static renderer consumes metadata but does not run vision models.

## Acceptance

- golden corpus across posters, portraits, groups, architecture, text-heavy images and sparse photos;
- no cut faces/heads or important text; OCR crop only under the proven scarcity fallback above;
- zero image-frame bands for classified visual media and for classified
  documents alike; an unclassified/error document remains whole in a natural
  frame until positive crop evidence exists, not a silent fixed-frame fields
  exception. Card and media shells keep equal outer heights within each row;
- every non-final compact row is full and only the final row may be incomplete;
- no global fixed body/action reservation; row chrome follows real row content;
- grouping uses the full eligible pool before pagination, retains all IDs and freezes the rendered prefix;
- repeatable output for the same versioned media;
- visual regression for hero/card/list/gallery/share formats;
- safe fallback when metadata is missing/low confidence;
- content change invalidates stale focal metadata.

The `6686` event-detail regression additionally verifies that source media used
by event `6764` reuses its existing reviewed `visual_only` replacement
(`1080×720`) in the related grid. The resulting three-card row is full, uses a
shared horizontal `5/4` frame and `cover`, and has equal measured media and
outer-card heights. Other `unknown/error` media still fail closed; this
regression does not introduce an event-id exception. The portrait source
currently selected for event `6821` has an independent exact URL/SHA-256
no-OCR review after the semantic classifier returned `error`; the reviewed
source is treated as a photograph and fills the card, while every unreviewed
`unknown/error` source remains fail-closed.

## R15 mobile-rail crop contract

On mobile listing rails, **every** event/asset-consistent, classified crop-safe
`visual_only` photo uses one horizontal `140×112` (`5:4`) identity, regardless
of source orientation, portrait/landscape shape, gallery size or position. The
media box is `aspect-ratio: 5 / 4`, the image computes to `object-fit: cover`,
and no letterbox/backdrop/repeated-edge band may remain. A portrait photo is not
permission to reintroduce a vertical `4:5` rail variant.

This rule is deliberately fail-closed. Event-level `ocr_text`/`unknown` state,
an OCR/text/document asset, unknown/error semantics, contradictory event versus
asset classification, or missing positive crop-safety evidence uses authored
geometry with `contain` even if an upstream selector requested adaptive cover.
Only the positively proven `visual_only` case gets the horizontal crop.

Event `5297` (`Фестиваль Pianissimo: Игорь Сидоров`) is the frozen regression:
its rail image must remain a single horizontal crop without bands at the mobile
breakpoint. Browser acceptance measures the rendered media box and computed
fit; a source-code declaration alone does not close the regression. The
multi-image portrait regression is event `6823`: all three selected gallery
cells must independently measure `140×112`, ratio `1.25`, and `cover`.

The production catalog is not required to retain an expired `5297` page. While
that route is generated, the output gate inspects its exact rail DOM. After the
route expires, the same gate requires a current generated visual-only rail cell
with `140×112`/`cover` and evaluates the immutable `5297` two-photo geometry and
`65% 35%` focal point through the real resolver. This is a release-gate fixture,
not a public-event resurrection or an event-id exception in product code.

## Joint protected-crop delivery

The planner, static EventCard, hydrated card factory and flow-grid binding share
`relatedCardLayout` proof and renderer policy. Related/Free display payloads carry
existing safe_crop, current/geometry pixel identity, geometry status/coordinate
space and normalized OCR boxes; serializers must not strip this evidence.
A proof-enabled vertical fallback uses `document-protected-cover` and the existing
`reviewed-bounded` permission, not generic allowed crop. Horizontal target shrink,
stale hashes and absent boxes fail closed. Single-column mobile document media
returns to its natural uncropped frame. Positive/negative fixtures cover both
layout selection and renderer binding; the real comparison corpus's unproven
tail remains explicitly unaccepted, not silently promoted by those fixtures.
