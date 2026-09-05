# Event image framing and focal metadata

> Authority: owner CODE continuation, 2026-09-05; DS contract 1.14.3. **No image-frame fields; crop is the final fallback.** Supersedes proof-required contain fallback. This is a requirement, not deployed acceptance.

## Shared contract

- Compose equal-width rows from the **full eligible pool before pagination**. Prefer natural document ratios, compatible neighbours and visual-only fillers. Preserve every admitted identity exactly once. Only the last row may be incomplete, retaining ordinary column widths.
- Reassign flexible fillers before freezing the initial sequence (three passes, up to 1000 candidates). Append/feedback must preserve the visible prefix and active card.
- Minimize cropping through grouping and current protected-region evidence. If no complete natural/proven partition exists, use a balanced shared target minimizing worst crop and **cover**, never fields, blur padding, repeated edges, stretching or dropped events.
- Twenty percent is the **preferred crop budget, not a hard veto**. The final fallback may exceed it rather than produce fields. Use available current OCR/face/valuable-region/focal evidence to protect important content; never invent proof or reclassify unknown media as visual-only.
- Natural single-column/mobile document frames may use contain when source/frame ratios match without fields. This avoids needless crop, not an exception permitting fixed-frame bands. Visual-only hero/gallery photography fills with cover. Missing/broken resources retain separate truthful fallback states.
- Large exhibition row cards use the shared card-radius role; deck and media use the shared hero-radius role. Local consumers must not invent smaller large-object corners.

## Common implementation and acceptance

`site/src/lib/relatedCardLayout.mjs` owns row planning. Static EventCard,
AdaptiveEventCardGrid, EventLayout and hydrated factories honor the planned
geometry without independently forcing unknown media back to contain.
Natural/proven grouping comes first. Proven crops use `document-protected-cover`
and `reviewed-bounded`. Irreducible residue uses `document-fallback-cover` and
`fallback-minimal`; unproven safety is `unverified-text`. Preferred-budget
within/exceeds diagnostics must never silently select fields.

Measure decoded painted bounds, actual crop, truthful safety state, equal row/card
geometry, all identities and visible order jointly on real static/hydrated desktop
and mobile pages. CSS declarations and synthetic tests alone do not prove the
visible outcome. Report unavoidable crop and unavailable text proof honestly.
The День Гурьевска poster and Free collection are common-algorithm test examples,
not individually adjusted assets. Check wide/tall OCR, unknown and visual-only
sources, resource states and rounded clipping.

## Pixel-current metadata

Export joins `event_image_geometry` only for classified state, matching pixel hash
and current model/prompt. Payloads retain normalized face/OCR boxes, valuable
region, coordinate space and provenance. Missing/stale evidence cannot authorize
a proven-safe claim. A known target uses the protected-region solver and unrounded
object position where possible; inability to retain the union invokes the honest
final crop above, not fields. Serializers must preserve existing evidence.
Human-reviewed overrides are bound to exact source URL/hash, not event IDs or
titles. All consumers reuse the same reviewed metadata; content changes invalidate
stale geometry. Existing 6686/6764 and 6821 reviewed-source fixtures remain valid
metadata reuse, not product ID exceptions.

## Required producer

Offline Smart Update/media preparation or Kaggle enrichment emits:

- `image_text_mode`;
- normalized focal point or CSS object position;
- face/person boxes where reliable;
- confidence, model/algorithm version and media hash;
- manual override and recompute status.

The browser/static renderer consumes metadata but does not run vision models.

## Mobile rail

Classified crop-safe visual-only rail cells use horizontal 140×112 (5:4), cover,
regardless of orientation or gallery position. Document rails prefer natural
geometry without bands; constrained geometry uses the explicit final fallback,
not semantic reclassification. Existing 5297/6823 regressions retain this photo
identity; expired routes are not resurrected for tests. Inspect actual rendered
bounds and focal positioning, not source declarations alone.

The consumer-closure checker permits `--ex-row-radius` only as a binding to `--ke-shape-radius-card`; a literal private radius is rejected. The generated token-impact graph includes these shared shape consumers. These checker/graph updates do not alter the frozen real-candidate runtime.
