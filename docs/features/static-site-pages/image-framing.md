# Event image framing and focal metadata

> Status: **renderer preview implemented; metadata producer missing**.

## Contract

- OCR/text/unknown images use full-poster/natural/contain presentation and are not meaningfully cropped.
- Visual-only images may use cover only when focal/face metadata or a safe deterministic fallback supports it.
- When safe crop cannot be proven, use contain/natural presentation rather than cut heads, text or the main subject.
- Framing metadata is versioned with the media/content hash and may be manually overridden.

## Required producer

Offline Smart Update/media preparation or Kaggle enrichment must emit:

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
