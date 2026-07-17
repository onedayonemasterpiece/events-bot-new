# Media decision matrix

Use fresh counts; the states below are decision rules, not fixed percentages.

| Event media state | Browse-card asset | Image treatment | HTML text overlay | Required fallback / note |
|---|---|---|---|---|
| No approved image | None | Intentional standard text tile | No | Keep rank and card rhythm; never fabricate, blur-fill, or add a padded pseudo-image |
| One OCR poster | Identity poster | Natural/nearest safe full-bleed ratio with consistent radius; bounded crop only within an accepted document-loss budget | No | Metadata outside; do not place the poster on a decorative four-sided stage |
| One unknown or unclassified document | Identity asset only when clearly relevant | Natural/nearest safe full-bleed ratio until classified | No | Fail closed: no center crop and no promotion into a textless-photo token |
| One verified `event_photo` with adequate resolution, focal point, and `safe_crop=1` | That photo | Cover at focal point | Only with separate `overlay_safe_region` evidence | Otherwise put title/date outside |
| Multiple, mixed OCR + photo | Semantically relevant safe event photo for discovery; poster remains visible in detail/gallery | Surface-specific photo cover | Same overlay gate | Do not choose landscape only because it fills; optional quiet `+N фото` cue |
| Multiple, all OCR/documents | Best identity poster by role, order, and quality | Natural/nearest safe full-bleed poster ratio; no forced photo behavior | No | Low-resolution primary may be rescued by a better approved identity asset |
| Portrait-only verified photos | Best relevant portrait photo | Portrait-friendly outer zone; bounded cover at focal point | Overlay gate still applies | Do not force a shallow landscape crop |
| Landscape-only verified photos | Best relevant landscape photo | Wide cover at focal point | Overlay gate still applies | External copy remains default |
| Square-only | Best relevant square asset | Square/neutral stage | Mode-dependent | Do not stretch into arbitrary wide media |
| Mixed orientation | Select by semantic role + surface + quality, then choose geometry | Per-surface target aspect | Mode-dependent | Asset selection precedes crop; ranking never changes |
| Low resolution | Better approved identity asset if one exists | Never upscale or pad; otherwise replace the image with an intentional standard text tile | No | Keep the event in its ranked position; do not make bad pixels look intentional with empty fields |

## Selection order

1. Approval and duplicate gate.
2. Semantic relevance to this event; never promote an attractive but unrelated gallery image.
3. Media role and text/document mode.
4. Surface intent: inspiration cards may prefer an event photo; detail identity may prefer a poster.
5. Resolution/derivative readiness.
6. Focal and crop safety.
7. Source/display order as a tiebreaker, not as unquestioned truth.

## Finite desktop photo tokens

Verified textless photos may enter a finite token family only when `safe_crop=1`, focal evidence exists, and the derivative meets the token's minimum resolution. The initial candidates are `P 4:5`, `S 1:1`, `W 4:3`, and `L 3:2`; a task may reduce this set but must not silently add arbitrary widths. OCR, unknown, and unsafe assets remain outside this family.

## Harmony rule

Keep the row height, named aspect tokens, metadata tracks, radius, and actions stable. Let verified photos cover edge-to-edge; let documents use a natural/nearest safe full-bleed ratio. If an accepted surface allows bounded OCR crop, enforce the measured loss budget. Prefer a short/incompatible row or intentional text tile over destructive crop—but on ranked feeds continue without reordering or dropping items.

## Long-title contract

For every shortlisted desktop family, render real p95, p99, and maximum active titles. Record how many current titles exceed the proposed line budget. Do not solve overflow with hidden SEO text, hover-only disclosure, or silent raw ellipsis. A visible, fact-preserving `browse_title` may be generated for the bounded overflow set when it is fully validated; the canonical title remains the detail H1, structured-data value, accessible full label, and source of truth.
