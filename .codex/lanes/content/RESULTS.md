# Content lane results — static event v11 regression repair

## Scope

- R02: canonical/content/media consistency for event `5756` (`Женитьба`).
- R03: semantically complete desktop lead for event `5658` (`Гараж`).
- No production database row was mutated.

## Evidence

Read-only snapshot:

- `/home/dev/.codex/worktrees/events-bot-new/static-site-autogen-template-parity-20260717/artifacts/codex/static-site-template-parity-20260717/snapshot/immutable/snapshot-20260717t-template-parity-v11.sqlite`
- snapshot id: `snapshot-20260717t-template-parity-v11`
- snapshot SHA-256 recorded by the release lane: `3105191211ed79a3e34284c8d4c01a4e5631b6adbb62512035b0f00ef120d443`

### Event 5756

The canonical row is internally inconsistent:

- row occurrence: `2026-08-09 18:00`, title `Женитьба`, ticket URL `/spektakli/jenitba`;
- row `event_type`: `экскурсия`;
- row short description and generated description combine the 18:00 play with a theatre tour;
- canonical `source_text` is an exact structured first-party occurrence for `Женитьба`, `2026-08-09 18:00`, the same ticket URL, and explicitly says `О спектакле`;
- `event_source.id=3514684` is a different occurrence, `Экскурсия "Закулисье театра"`, on the same date at `14:30`, and was attached to event `5756` alongside the correct play source `event_source.id=3514685`.

The media ledger is also mixed:

- `eventposter.id=10499` is a generic August ticket-sale advert. Its stored `image_text_mode=ocr_text`, `media_role=unknown_document`, `safe_crop=0`, but the exporter previously ignored the stored mode when OCR text was blank and projected it as `visual_only`, permitting crop on mobile;
- shared cross-event assets `10500`, `10501`, `10642`, `10643`, `10644` appear on multiple unrelated theatre events; `10642..10644` have source paths under the theatre excursion gallery;
- event-exclusive assets `10645`, `10646`, `10647` have first-party `zhenitba` source paths and are the source-coherent fallback family; `10645` is now selected as primary by the generic cross-event-boilerplate guard;
- the generic advert stays in the gallery as `ocr_text`/`unknown_document`/`contain`; it is not relabelled as an event poster.

### Event 5658

- `short_description` ends after `обсуждение планов застройки.`;
- the stored full LLM description contains the same prefix followed by `превращается в остросюжетный конфликт.`;
- the exporter used the short field verbatim (or a raw character slice when absent), producing a false sentence boundary in the lead.

## Implementation

- Exact structured-source consistency guard activates only when source date, time and ticket URL all equal the canonical occurrence and an explicit first-party heading (`О спектакле`, etc.) contradicts the stored type. It projects source-backed title/type/description and emits `structured_source_occurrence_conflict_guard`.
- Leads now preserve authored summaries unless they are a proven punctuated prefix of the full description. The replacement is a complete source sentence; a genuinely unsentenced long source is visibly truncated at a word boundary with `…`.
- Stored `eventposter.image_text_mode` now wins over empty OCR text.
- A classified non-identity document cannot own the hero when a strong classified event photo exists.
- A cross-event reused advert/generic gallery asset yields to a strong event-exclusive `visual_only` asset; if no strong exclusive alternative exists, the shared fallback is preserved.
- Primary alt text and `safe_crop` are derived from the selected primary asset, not the old first ledger row.

## Generated projection proof

Slice export from the immutable snapshot (`--include-ids 5658,5756 --skip-related --skip-image-probes`) produced:

- `5658`: complete lead through `превращается в остросюжетный конфликт.`;
- `5756`: slug `zhenitba-kaliningrad-5756`, type `спектакль`, source-backed play description, primary `eventposter.id=10645` asset, generic advert retained as contained OCR gallery material.

## Production repair recommendation (not executed)

The exporter guard prevents the current user-visible mix-up, but the root data must still be repaired LLM-first in Smart Update/import:

1. prevent occurrence merge on date alone when structured time/title/ticket URL conflict;
2. detach `event_source.id=3514684` (14:30 tour) from `event_id=5756` and create/reconcile the actual 14:30 tour occurrence rather than silently deleting the source;
3. run the existing LLM/VLM event-local media reconciliation for `eventposter` rows `10499..10647` and `12069`; do not approve a filename-only SQL repair;
4. audit adjacent theatre rows `5754`, `5755`, `5757`: their stored types/descriptions and shared media show the same occurrence-coalescing family;
5. after canonical repair, regenerate the snapshot and confirm the exporter guard is no longer needed for those rows (the note should disappear).

## Validation

- `python3 -m py_compile site/scripts/export-production-preview-data.py`
- `pytest -q tests/test_static_site_*.py` → `46 passed`
- real immutable-snapshot two-event export completed successfully.
