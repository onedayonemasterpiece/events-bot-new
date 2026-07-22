# W03 — Authorized search occurrence-family contract

## Scope

- Requirement IDs: W03 / production search per-family occurrence gap.
- Base SHA: `1cb17f74a93c97c3c74d3337d272166a5dff18f6`.
- Implementation SHA: `36e6ba66`.
- Final lane head: the report commit immediately following the implementation SHA; resolve with `git rev-parse HEAD` on `agent/calendar-occurrences/search-backend`.
- No deploy attempted.

## Delivered

- Search-vector snapshot projection now derives connected components exclusively from reciprocal explicit `other_date_ids`; inactive/ranged/asymmetric/dangling links fail closed and there is no title/type/venue inference.
- DTOs carry exact `occurrence_member_ids`, compact occurrence text and the full accessibility label at the card/display layers. Exact regression strings include `2, 9 ноября 19:00` and `4 ноября 17:00, 19:00`.
- Edge search preserves the highest-ranked family representative, collapses before logical pagination, repeats collapse after LLM reranking and shares family identity with fallback filtering.
- Pagination now reads the bounded complete RPC window (60, the existing RPC cap) from raw offset zero and applies logical family offset afterward, preventing a lower-ranked sibling from resurfacing on a later page.
- Canonical linked-events/static-site docs and `[Unreleased]` changelog are synchronized.

## Verification

- `node --experimental-strip-types --test supabase/functions/event-search/occurrence-families.test.mjs` — PASS, 5/5.
- `/tmp/events-bot-pytest-20260721/bin/pytest -q tests/test_event_vector_sync.py` — PASS, 12/12.
- `python3 -m py_compile scripts/sync_event_search_vectors_to_supabase.py` — PASS.
- `git diff --check` — PASS.
- Supabase changelog checked on 2026-07-21 as required by the repository skill; no relevant Edge Function/client breaking change applies to this local contract.

The occurrence incident record was intentionally not edited: this lane adds search DTO/Edge coverage but does not add production repair or incident-closure evidence; the incident remains OPEN.

## Changed files

- `CHANGELOG.md`
- `docs/features/linked-events/README.md`
- `docs/features/static-site-pages/README.md`
- `scripts/sync_event_search_vectors_to_supabase.py`
- `supabase/functions/event-search/index.ts`
- `supabase/functions/event-search/occurrence-families.ts`
- `supabase/functions/event-search/occurrence-families.test.mjs`
- `tests/test_event_vector_sync.py`
- `.codex/lanes/W03/RESULTS.md`

## Risks / integration notes

- The Edge handler deliberately requests the existing RPC maximum of 60 candidates for stable post-collapse pagination. This increases per-request result normalization versus the old candidate-window-sized fetch, but does not add provider/LLM candidates because only the logical page enters verification.
- Cherry-pick implementation commit `36e6ba66` and the following report commit; no conflicts are expected on the two production files. If W01 touched the same docs/changelog, reconcile the two small additive paragraphs/bullet without dropping either contract.
