# Event image framing and focal metadata

> Status: **geometry producer implemented; renderer adoption is
> surface-specific and the no-field contract remains a release gate**.

## Contract

- Bounded hero/card/thumbnail previews have no letterbox/pillarbox fields,
  decorative four-sided padding, duplicated blur or ambient ratio fill.
- OCR/text/unknown images use an edge-to-edge natural-ratio container. A
  mismatched fixed frame with `object-fit: contain` is not an acceptable
  no-crop solution because it creates fields.
- Visual-only images may use cover only when event relevance, resolution,
  `safe_crop`, focal/face metadata or a reviewed deterministic fallback supports
  it.
- OCR normally stays uncropped. For an excessively vertical OCR asset only,
  top/bottom cover crop is allowed when the combined removed source area is at
  most `20%`, all OCR and face boxes plus safety margin remain visible, and the
  measured crop is exposed to acceptance tests. Left/right OCR crop is forbidden.
- When safe crop cannot be proven, adapt the card/container to the source ratio,
  choose another approved event-relevant asset, split poster and photo roles, or
  render an intentional text fallback rather than cutting evidence.
- Framing metadata is versioned with the media/pixel hash and may be manually
  overridden.

For source ratio `s = width / height` and target ratio `t`, cover loss is:

```text
loss = 1 - min(s / t, t / s)
```

If `s < t`, the loss is top/bottom; if `s > t`, it is left/right. For a crop
budget `L`, the safe target interval is `[s × (1 - L), s / (1 - L)]`. The OCR
exception uses only `t >= s` from that interval.

## Producer and renderer boundary

Smart Update's durable `event-image-geometry-v1` enrichment already produces
pixel-fingerprint-bound source dimensions, normalized face boxes and a valuable
region. Static export/rendering must progressively expose and consume the full
contract:

- `image_text_mode`;
- normalized focal point or CSS object position;
- normalized OCR boxes and valuable/saliency region;
- face/person boxes where reliable;
- confidence, model/algorithm version and media hash;
- manual override and recompute status.

The browser/static renderer consumes metadata but does not run vision models.

## Acceptance

- golden corpus across posters, portraits, groups, architecture, text-heavy images and sparse photos;
- no internal bars/fields in bounded previews;
- exact source/target ratios, crop axis and removed-area fraction are test-visible;
- OCR vertical crop is at most `20%` total and keeps every protected box;
- no cut faces/heads or significant OCR text;
- repeatable output for the same versioned media;
- visual regression for hero/card/list/gallery/share formats;
- safe fallback when metadata is missing/low confidence;
- content change invalidates stale focal metadata.

Agent workflow, branch-derived implementation evidence and deterministic crop
calculator: [`.codex/skills/smart-image-crop/SKILL.md`](../../../.codex/skills/smart-image-crop/SKILL.md).
