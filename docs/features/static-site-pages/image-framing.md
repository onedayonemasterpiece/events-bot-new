# Event image framing and focal metadata

> Status: **pixel-current producer/export and fail-closed consumer implemented**.

## Contract

- OCR/text/unknown images use full-poster/natural/contain presentation and are not meaningfully cropped.
- Visual-only images may use cover only when focal/face metadata or a safe deterministic fallback supports it.
- When safe crop cannot be proven, use contain/natural presentation rather than cut heads, text or the main subject.
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
the browser crop past a tight protected boundary. If the union does not fit, geometry is stale,
the image is OCR/text, semantic role is not an explicitly classified
`event_photo`, or the responsive target ratio is unknown, the renderer uses
`contain`. Responsive hero/listing layouts therefore do not claim a safe bbox
crop using an approximate ratio.

Production desktop post-build contract проверяет ту же fail-closed семантику:
`cover` допустим только с `protected_regions_fit` и совпадающим
`data-protected-crop-fit`; `contain` является корректным результатом для
responsive target с неизвестным aspect ratio и обязан иметь явный reason.
Contract не должен требовать legacy `cover` только из-за `visual_only`, иначе
полностью успешная Astro-сборка ошибочно отклонит безопасное bbox-поведение.

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
