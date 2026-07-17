# Media decision matrix

Use fresh counts; the states below are decision rules, not fixed percentages.

| Event media state | Browse-card asset | Image treatment | HTML text overlay | Required fallback / note |
|---|---|---|---|---|
| No approved image | None | Intentional branded solid stage | No | Keep full card geometry; never fabricate or blur-fill |
| One OCR or unknown document | Identity poster | Contain/natural or bounded crop only within an accepted document-loss budget | No | Metadata outside; unknown fails closed as document |
| One verified `event_photo` with adequate resolution, focal point, and `safe_crop=1` | That photo | Cover at focal point | Only with separate `overlay_safe_region` evidence | Otherwise put title/date outside |
| Multiple, mixed OCR + photo | Semantically relevant safe event photo for discovery; poster remains visible in detail/gallery | Surface-specific photo cover | Same overlay gate | Do not choose landscape only because it fills; optional quiet `+N фото` cue |
| Multiple, all OCR/documents | Best identity poster by role, order, and quality | Poster stage; no forced photo behavior | No | Low-resolution primary may be rescued by a better approved identity asset |
| Portrait-only verified photos | Best relevant portrait photo | Portrait-friendly outer zone; bounded cover at focal point | Overlay gate still applies | Do not force a shallow landscape crop |
| Landscape-only verified photos | Best relevant landscape photo | Wide cover at focal point | Overlay gate still applies | External copy remains default |
| Square-only | Best relevant square asset | Square/neutral stage | Mode-dependent | Do not stretch into arbitrary wide media |
| Mixed orientation | Select by semantic role + surface + quality, then choose geometry | Per-surface target aspect | Mode-dependent | Asset selection precedes crop; ranking never changes |
| Low resolution | Best approved semantically correct asset | Smaller rendered footprint or contained stage | No by default | Show honestly; “high-quality smart crop” is not universal input reality |

## Selection order

1. Approval and duplicate gate.
2. Semantic relevance to this event; never promote an attractive but unrelated gallery image.
3. Media role and text/document mode.
4. Surface intent: inspiration cards may prefer an event photo; detail identity may prefer a poster.
5. Resolution/derivative readiness.
6. Focal and crop safety.
7. Source/display order as a tiebreaker, not as unquestioned truth.

## Harmony rule

Keep card width, outer media zone, metadata tracks, and actions stable. Let verified photos cover; let documents sit on a deliberate plain/tinted stage. If an accepted surface allows bounded OCR crop, enforce the measured loss budget. Prefer a short/incompatible row over destructive crop—but on ranked feeds continue with the next row without reordering or dropping items.
