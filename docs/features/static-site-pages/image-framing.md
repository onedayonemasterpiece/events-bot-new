# Event image framing and focal metadata

> Status: **pixel-current producer/export plus surface-specific consumers implemented**.

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
  cropped. Only a very tall document (source width/height below `4/5`) may widen
  that interval, and no more than `20%` of its source area may be cropped.
- `ListingEventCard` uses that same universal exception instead of keeping a
  very tall OCR poster in an unnecessarily narrow natural-width strip. With
  known dimensions and explicit `ocr_text`, its target ratio is
  `sourceRatio / 0.8`, treatment is `document-safe-cover`, and vertical
  retention is at least `0.8`. Unknown/error media remains natural `contain`;
  there is no event-id or asset-URL exception.
- Cards in a row have one media height and one total card height. Cards may be
  reordered between rows; input order is not an acceptance requirement.
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

`site/src/lib/relatedCardLayout.mjs` enumerates feasible card groupings (up to
the surface row-size cap) and uses dynamic programming over the complete set of
cards. It minimizes the sum of normalized full-row heights, including the
row-local intrinsic title/place/action height, rather than greedily optimizing
the current row. The body is bounded by the card's two-line copy contract and
is estimated from those same text tracks in the search cost; CSS then stretches
only to the tallest real content inside that row. It must not reserve the
rejected global `184px + 58px + 56px` chrome. Ties prefer fewer rows, less
visual crop and less displacement from source order.

The solver first enumerates the optional final remainder of size
`cardCount % rowSize`, then partitions everything else into exact full rows.
Fullness is therefore a hard constraint, not a score that a shorter row can
outvote. The browser gate checks row cardinality, row-local equal media/card
heights and that at least one content-owning card per row has no large synthetic
body gap. If the ranked prefix is mathematically OCR-incompatible, the packer
may inspect at most three following candidates and keeps the largest feasible
card count; it never repairs incompatibility by opening an earlier partial row.

For each candidate row the solver intersects document-safe target intervals.
An ordinary document contributes only its natural ratio; a very tall document
contributes `[naturalRatio, naturalRatio / 0.8]`. Infeasible combinations are
rejected. The selected target is then shared by every card in the row, so media
and outer-card heights are equal and no image uses `contain` inside a fixed
frame. `rowWorstCrop`, `coverCrop` and `rowCost` are emitted as diagnostics and
checked against the rendered result.

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
the row instead of creating bands, and very tall documents alone may lose at
most `20%` of their area.

Production desktop post-build contract checks the actual surface contract. A
non-OCR hero/gallery media must compute to `cover`. In recommendation rows,
every image must compute to `cover` with zero unused frame budget; document
cards must additionally compute to `document-safe-cover` with applied crop no
greater than `20%`. The gate decodes the real image, checks loaded/fallback
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
- no cut faces/heads; no OCR crop except the documented very-tall `<=20%` case;
- zero image-frame bands for classified visual media and for classified
  documents admitted to bounded cover; an unclassified/error document is the
  deliberate fail-closed exception and remains whole until media enrichment
  supplies positive crop evidence. Card and media shells still keep equal
  outer heights within each compact row;
- every non-final compact row is full and only the final row may be incomplete;
- no global fixed body/action reservation; row chrome follows real row content;
- the globally selected grouping has the minimum normalized total page height;
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
