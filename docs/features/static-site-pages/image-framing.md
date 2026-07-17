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

## Bounded crop-interval probe

A reproducible research CLI now exists at
`scripts/inspect/probe_briefing_crop_interval.py`. It asks Gemma 4 for only the
smallest crop-critical vertical interval and keeps all crop geometry in a
validated deterministic solver. The 2026-07-17 two-image comparison found Gemma
4 31B usable as the candidate interval author; Gemma 4 26B A4B lost a principal
stage element and is not accepted as the sole author. Direct model-authored
`focusY` is explicitly rejected.

This does **not** close the missing-producer status. Production still needs
content-hash versioning, face/head-box goldens, target-aspect evaluation,
manual override and persisted metadata. Full evidence is in
[`crop-interval-gemma4-probe-2026-07-17.md`](../../reports/static-typed-briefing-consultation-2026-07-15/crop-interval-gemma4-probe-2026-07-17.md).
