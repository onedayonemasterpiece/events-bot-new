# INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped Near-duplicate VK poster passes OCR-grouping dedup while semantically relevant tram photo is dropped by token scoring

Status: open
Severity: sev3
Service: Telegraph render path / `_select_eventposter_render_urls` / `_score_eventposter_against_event`
Opened: 2026-05-11
Closed: —
Owners: Telegraph render owner / poster ingest owner
Related incidents: none yet on this exact surface
Related docs: `docs/features/telegraph-cache-sanitizer/README.md`, `media_dedup.py`

## Summary

Production event `4727` («Аудиоспектакль "Путешествие налегке"», 2026-06-28 20:00, Драматический театр) was published to Telegraph (`Audiospektakl-Puteshestvie-nalegke-05-08`) with two **near-duplicate** poster images visible side by side, while the **tram photo** that is the natural visual anchor for an «аудиоспектакль в театральном трамвае» was silently dropped despite being present in `eventposter`.

The two near-duplicate posters in the rendered page are stored under almost identical Supabase URLs (`d067…0022a.webp` and `d067…0002a.webp`, differ in 2 characters of a hash-shaped filename). They are byte-different (different `poster_hash`) because VK re-encoded the same source poster twice with slightly different compression. Their OCR readings are also slightly different — `Путешествие налегке` vs `Путешествие на плечах` — the second one is an OCR misread of the same poster.

The tram photo (`C75B5B35-…-big.jpg`, ocr_title `в ДЕПО`) is in `eventposter` (id 7996) but did not survive the per-poster Telegraph scoring step.

## User / Business Impact

- The most evocative image for a "тeatral'nyy tramvay" play (the tram itself) is missing from the public Telegraph card.
- Two near-duplicate poster images appear next to each other, looking like a glitch.
- The 3D preview surface uses a different selection pipeline and does include the tram photo, so the operator sees a divergence between 3D preview and the public Telegraph render.
- Pattern risk: any event whose VK upload contains two re-encoded copies of the same poster with slight OCR variance is exposed to the same regression.

## Detection

- 2026-05-11 operator review of the «Путешествие налегке» Telegraph card.
- No alert fired — both selection stages succeeded structurally.

## Timeline

- 2026-05-08: event 4727 imported, 6 `eventposter` rows created (ids 7936, 7996, 7997, 8000, 8001, 8002), Telegraph page `Audiospektakl-Puteshestvie-nalegke-05-08` rendered with cover + 2 near-duplicate poster URLs.
- 2026-05-11: operator reported the issue alongside other May 11 quality reviews; this incident record opened.

## Root Cause

1. **`_select_eventposter_render_urls` near-duplicate gap** ([main.py:1907-1958](main.py#L1907-L1958)): the function groups poster rows by a normalised OCR signature (title + compact text). When the same poster has been ingested twice with slightly different OCR (`Путешествие налегке` vs `Путешествие на плечах`), the rows fall into different groups and both survive. The `phash:<phash>` fallback at [main.py:1953](main.py#L1953) only activates when OCR is **absent**, so a phash match cannot rescue OCR-divergent near-duplicates.

2. **`phash` is not consistently populated** at ingest time: in the 4727 evidence sample only 1 of 6 `eventposter` rows carries a `phash` (id 7936). Without phash on every row, no perceptual-hash-based merge can run as a secondary dedup layer even if `_group_key` were rewritten to consult it.

3. **`_score_eventposter_against_event` token-only scoring** ([main.py:2028-2111](main.py#L2028-L2111)): the per-poster relevance score is built from event-title token overlap, title substring match, and date/time match against the OCR text. For event 4727, the «Путешествие налегке» poster scores ~8 (title-token overlap + title-substring), but the tram photo (`в ДЕПО`) scores 0, the route schema (`Октябрьская Улица`) scores 0, and an unrelated abonement poster also scores 0. The filter at [main.py:16502](main.py#L16502) then keeps only `score ≥ max(2.0, best_score - 2.0)` = 6.0, so the tram photo is dropped along with the truly unrelated posters. The signal that a tram photo is the **semantic** anchor for an «аудиоспектакль в театральном трамвае» is not captured by token overlap.

## Contributing Factors

- VK occasionally delivers two near-identical copies of the same upload, with slight pixel/compression differences, producing two `eventposter` rows.
- OCR can read the same poster text with small variations between two copies, defeating exact-text grouping.
- The Telegraph render path treats poster selection as deterministic ranking, with no LLM-judged "is this poster topically relevant to the event" fallback for low-score images.

## Automation Contract

### Treat as regression guard when

- changing `_select_eventposter_render_urls` (`main.py:1875+`);
- changing `_score_eventposter_against_event` (`main.py:2028+`) or the scoring threshold in the Telegraph render path (`main.py:16486+`);
- changing how `phash` is computed at poster ingest (`smart_event_update.py:14737`, `main.py:5060`);
- adding any LLM-based poster relevance step on the Telegraph path.

### Affected surfaces

- code: `main.py::_select_eventposter_render_urls`, `main.py::_score_eventposter_against_event`, the Telegraph render block around `main.py:16456-16527`, poster ingest paths that fill `EventPoster.phash`.
- data: production event 4727 and its 6 `eventposter` rows; future VK uploads with re-encoded duplicate posters.

### Mandatory checks before closure or deploy

- Replay against 4727 fixture: after the fix, exactly one of the two near-duplicate `d067…` posters must reach Telegraph, and the tram photo `C75B5B35-…-big.jpg` must be included in the rendered set.
- 3D-preview pipeline must continue to surface the tram photo (no regression on the 3D side that already does the right thing).
- No legitimate multi-poster album where two different posters describe one event must be collapsed into one image as a side effect of the new dedup rule.

### Required evidence

- Updated Telegraph page for event 4727 (or a fresh-import equivalent) showing cover + tram photo + one poster (without the near-duplicate).
- Test fixtures: `_select_eventposter_render_urls` with the 6-row 4727 sample must produce the expected selection.

## Immediate Mitigation

- None yet on the code path. Operator-side workaround: manual re-import of event 4727 via the bot might not help, because the underlying near-duplicate ingest and the token-scoring filter are deterministic for this source.

## Corrective Actions

- None landed yet. See Follow-up Actions for the two-layer fix plan.

## Follow-up Actions

- [ ] Owner: poster ingest / no due date / make `phash` computation mandatory and idempotent for every new `EventPoster` row (both ingest paths in `main.py:5060` re-encode flow and `smart_event_update.py:14737` mirror flow); include backfill of `phash` for existing rows so the dedup rule can apply retroactively.
- [ ] Owner: Telegraph render / no due date / extend `_group_key` in `_select_eventposter_render_urls` with a secondary perceptual-hash merge: when two groups have phashes within a small Hamming distance, merge them into one group regardless of OCR divergence.
- [ ] Owner: Telegraph render / no due date / before dropping a low-score `eventposter` row, ask an LLM relevance judge (reuse the existing `poster_relevance` stage in `smart_event_update.py`) whether the poster is topically related to the event (e.g. for an «аудиоспектакль в трамвае», a tram photo is relevant even with zero title-token overlap). Keep token-scoring as the cheap default; LLM only on near-threshold cases.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- This incident record sits in the index as the canonical regression contract for VK-near-duplicate posters and semantic poster relevance.
- Any future change to the Telegraph render path must replay the 4727 fixture to prove no regression.

## 2026-07-13 automated-gate hardening

Perceptual hashes are now candidate evidence only: neither Smart Update nor
Telegraph/TG/VK renderer physically prunes by Hamming distance. Exact raw/pixel
SHA is deterministic; crop/re-encode and semantic relevance use automatic
pairwise vision review. This preserves distinct tram/event photos while keeping
unresolved candidates non-public. Telegraph is a read-only media consumer, so
the 4727-style renderer mutation path no longer exists. Canonical contract:
`docs/features/event-media/README.md`.
