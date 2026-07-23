# L05 results — personal large feed and fail-closed media

## Scope

- **R02 — Done:** `/dlya-menya/` now renders the canonical `EventCard` in the same one-column large-feed container behavior as Search. The former bespoke 3/2/1 compact grid rules were removed; `EventHero` was not substituted.
- **R07 — Done:** the exporter no longer treats missing `image_text_mode` plus absent OCR/visual evidence as `visual_only`. Such pending/error rows export `unknown` and `unknown_document`. A classified `event_photo` remains affirmative visual evidence. EventCard resolution, `EventHero`, and `DesktopEventPage` all contain unknown/semantic-error media, and unclassified assets are no longer assigned the `event_photo` role by the hero/gallery fallback.
- Preserved negative controls: explicit classified `visual_only` media still covers.
- Preserved mixed-media control: synthetic event `6529` keeps its classified photo primary and classified OCR schedule secondary/contained.
- Event `6686` is covered only as a fail-closed regression specimen; no event-specific role or visual classification was added.

## Files

Only the L05 writable files were changed, plus this result record. No preview JSON, `EventLayout`, shared gate, canonical docs, or changelog files were edited.

## Verification

- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_static_site_content_projection.py tests/test_static_site_public_gate.py` — **22 passed**.
- `node --test tests/visual-keyboard-regressions.test.mjs tests/image-crop.test.mjs tests/event-gallery-interactions.test.mjs tests/event-media-quality.test.mjs` (from `site/`) — **37 passed**.
- `node --test tests/visual-keyboard-regressions.test.mjs` after final hero role hardening — **20 passed**.
- `npm run build` (from `site/`) — **passed**, 311 pages built; includes `/dlya-menya/`, event `6686`, and event `6529`.
- Generated `/dlya-menya/` contains the shared `authorized-search__results personal-page__feed-list` marker.
- `git diff --check` — **passed**.
