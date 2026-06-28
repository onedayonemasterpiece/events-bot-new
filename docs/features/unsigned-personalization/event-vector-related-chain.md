# Event vector related-chain v2

Status: implemented for static-site preview/export; production promotion still gated by CDN/release checks.

## Product role

`Смотрите дальше` on an event page must be a real discovery chain, not a hand-written rail. The chain is built offline during Smart Update/static-site generation and rendered as static HTML/JSON. Page views must never call LLM or vector APIs.

## Retrieval contract

Current P0 implementation in `site/scripts/export-production-preview-data.py`:

1. Build a canonical text document per event from title, type, topics, summary, visible description, venue/city and admission state.
2. Build a local sparse TF-IDF vector index (`local_tfidf_sparse_v1`). This is the central retrieval layer for the current preview; it avoids extra embedding-provider calls and is deterministic on Kaggle CPU.
3. Score candidates with vector cosine + deterministic business features: category/topic overlap, city/venue/date proximity, free/paid compatibility and source popularity.
4. Hard-filter self links, duplicate date links, inactive/cancelled/sold-out candidates.
5. Add mutual links for strong candidates so a new/updated event is discoverable from older related pages.
6. Optionally audit changed chains with Gemma 4 26B via the shared GoogleAIClient + Supabase limiter. This is a batch verification step, not a retrieval source and not a page-view path.

The consultant recommendation to move vector search into the center of related retrieval is accepted. The production-upgrade option is BGE-M3/pgvector or another ANN backend, but it is P1: the P0 shipped layer is local sparse vector retrieval because the current catalogue is small enough and the user explicitly did not want routine embedding API spend.

## Cache and LLM-call policy

The related cache file (`event_related_chain_cache.json`) stores:

- schema/version (`event_related_chain_v2_cache_*`);
- ordered event ids and event fingerprints;
- vector chains;
- per-anchor Gemma audit cache;
- `gemma_verified_model` once the selected model has audited the current fingerprint set.

Rules:

- If event ids and fingerprints are unchanged and `gemma_verified_model` matches, rebuilds reuse cache and make **0 provider calls**.
- If a new/changed event appears, only affected/new anchors require Gemma audit; vector retrieval is still recomputed offline for the batch.
- Gemma verification must use `models/gemma-4-26b-a4b-it` only through `GoogleAIClient` with Supabase reserve/finalize. Local limiter fallback and direct provider bypass are disabled for this path.
- If the limiter env/RPC is unavailable, the audit fails/skips loudly; the build may still emit vector-only related chains but must not be reported as Gemma-verified.

## Kaggle/Smart Update integration

`main.py` enqueues `JobTask.static_site_build` 15 minutes after Smart Update changes when `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`. The handler runs `scripts/run_static_site_builder_kaggle.py` with:

- `/data/db.sqlite` snapshot/export source;
- `--export-in-kaggle` so event export and related-chain generation happen inside the same Kaggle CPU job as Astro build;
- `--related-cache /data/static_site_event_related_chain_cache.json` so repeated rebuilds reuse Gemma audits;
- `--gemma-related-verify --gemma-related-model models/gemma-4-26b-a4b-it` when the production gate enables Gemma verification.

Kaggle API-started kernels cannot rely on Kaggle UI secrets. The runner therefore creates encrypted split private datasets (`secrets.enc` + `fernet.key/fernet.keys`) for the minimal runtime env required by the limiter (`GOOGLE_API_KEY4`, `SUPABASE_URL`, `SUPABASE_KEY`/service key), attaches them to the kernel, loads them into memory, and cleans up the secret datasets after a waited run.

## Logging / investigation

The exporter emits JSONL-style stages to stderr with scope `static_site.event_related_chain_v2`, including:

- `build_related_start`;
- `cache_check`;
- `vector_rebuild`;
- `gemma_initial_audit_required` / `gemma_audit_call` / `gemma_audit_error` / `gemma_audit_complete`;
- counts of events, changed ids, provider calls and cache hits.

This is the first place to inspect when related chains look wrong. Candidate entries also carry `related_score`, `vector_similarity`, `deterministic_score`, `reason_codes` and `retrieval_sources` in `preview-related.json`.

## Acceptance evidence to keep current

- Local smoke through Supabase limiter: one Gemma 4 26B call succeeds; rerun with unchanged cache reports `cache_hit_no_provider` and `provider_calls=0`.
- Kaggle static-site run must show encrypted secret envs loaded, Gemma audit status `ok`/`partial` for a verified run, `npm run check:preview` passed, and 50 real future/tomorrow events exported.
- Browser QA must verify UI regressions: sold-out CTA disabled, reset timestamp visible, photo CTA not overlapping, drawer auto-closes, update time is Kaliningrad time, OCR hero still parallax-scrolls.
