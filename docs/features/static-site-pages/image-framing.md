# Event image framing and focal metadata

> Status: **pixel-current producer/export plus surface-specific consumers implemented**.

## Contract

- OCR/text/unknown images use full-poster/natural/contain presentation and are not meaningfully cropped.
- Compact event-detail recommendation cards (`Смотрите дальше` and desktop
  `Ещё события`) treat the OCR result `visual_only` as permission to fill the
  shared card frame with `cover`. They prefer exact crop geometry, then exported
  object-position/focal metadata, then a deterministic center fallback.
- Hero, fullscreen and other large/responsive surfaces keep their stricter
  role/geometry policy. When their safe crop cannot be proven, they use
  contain/natural presentation rather than cut heads, text or the main subject.
- Framing metadata is versioned with the media/content hash and may be manually overridden.

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
geometry is stale, the image is OCR/text, semantic role is not an explicitly
classified `event_photo`, or the responsive target ratio is unknown, the strict
hero/large-surface renderer uses `contain`. Compact event-detail recommendation
cards are the deliberate exception: their known row ratio and `visual_only`
classification restore the accepted photo preview crop, while `ocr_text` and
`unknown` remain document `contain`.

Production desktop post-build contract checks the actual surface contract. A
hero `cover` still requires protected-region proof. In recommendation rows,
every `visual_only` card must compute to `visual-cover`/`cover` with zero unused
frame budget; document cards must compute to `document-contain`/`contain`.
The gate decodes the real image, checks the loaded/fallback layers, records the
unused-frame ratio and retains the rendered row screenshot.

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
- no cut faces/heads or significant OCR text;
- repeatable output for the same versioned media;
- visual regression for hero/card/list/gallery/share formats;
- safe fallback when metadata is missing/low confidence;
- content change invalidates stale focal metadata.
