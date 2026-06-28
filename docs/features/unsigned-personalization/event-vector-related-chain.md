# Event sparse related-chain v1

Status: implemented as a **temporary lexical/sparse baseline** for static-site
preview/export; it is not production-grade semantic vector retrieval.

Target replacement: `docs/features/unsigned-personalization/semantic-vector-retrieval.md`.

## Product role

`Смотрите дальше` on an event page must be a real discovery chain, not a
hand-written rail. The chain is built offline during Smart Update/static-site
generation and rendered as static HTML/JSON. Page views must never call LLM,
embedding models or vector APIs.

Important terminology rule: the current layer uses TF-IDF sparse vectors and
cosine similarity over lexical/event-feature documents. It may be called
`lexical_related`, `sparse_related` or `tfidf_related`, but it must not be called
semantic embeddings or production semantic vector search.

## Retrieval contract

Current P0 lexical implementation in `site/scripts/export-production-preview-data.py`:

1. Build a canonical text document per event from title, type, topics, summary,
   visible description, venue/city and admission state.
2. Build a local sparse TF-IDF vector index (`local_tfidf_sparse_v1`). This is
   the central retrieval layer for the current preview; it avoids extra
   embedding-provider calls and is deterministic on Kaggle CPU, but it is
   lexical matching, not semantic retrieval.
3. Score candidates with sparse cosine + deterministic features: controlled
   category/topic/facet overlap, city/venue/date proximity and entry-state
   compatibility. Source popularity/global likes are **not** allowed to lift an
   unrelated candidate into `pure_related`.
4. Hard-filter self links, duplicate date links, inactive/cancelled/sold-out
   candidates.
5. Add mutual links for strong candidates so a new/updated event is discoverable
   from older related pages.
6. Optionally audit changed chains with Gemma 4 26B via the shared
   `GoogleAIClient` + Supabase limiter. This is a batch verification step, not a
   retrieval source and not a page-view path.

The generated manifest must be honest:

- `schema_version=event_sparse_related_chain_v1`;
- `algorithm=event_sparse_related_chain_v1`;
- `retrieval_method=local_tfidf_sparse_v1`;
- `semantic_embeddings=false`;
- candidates carry `lexical_similarity`, `deterministic_score`,
  `related_score`, mandatory `slot_type` and `reason_codes`.

The consultant recommendation to move real vector search into the center of
related retrieval is accepted. The production target is BGE-M3 embeddings,
builder-owned SQLite embedding/release artifacts, optional Supabase pgvector
canary and golden quality gates. See `semantic-vector-retrieval.md`.

Until that cutover passes quality gates, this TF-IDF layer is a fallback and
preview baseline only. It must not be used to claim that semantic vector search
is implemented.

Known lexical failure mode: generic token overlap such as `город` can rank an
unrelated concert above an urban-planning event. The semantic migration must use
this as a hard-negative gate.

## Cache and LLM-call policy

The related cache file stores:

- schema/version (`event_sparse_related_chain_v1_cache_*`);
- ordered event ids and event fingerprints;
- sparse related chains;
- per-anchor Gemma audit cache;
- `gemma_verified_model` once the selected model has audited the current
  fingerprint set.

Rules:

- If event ids and fingerprints are unchanged and `gemma_verified_model`
  matches, rebuilds reuse cache and make **0 provider calls**.
- If a new/changed event appears, only affected/new anchors require Gemma audit;
  sparse retrieval is still recomputed offline for the batch.
- Gemma verification must use `models/gemma-4-26b-a4b-it` only through
  `GoogleAIClient` with Supabase reserve/finalize. Local limiter fallback and
  direct provider bypass are disabled for this path.
- If the limiter env/RPC is unavailable, the audit fails/skips loudly; the build
  may still emit sparse related chains but must not be reported as
  Gemma-verified.

## Kaggle/Smart Update integration

`main.py` enqueues `JobTask.static_site_build` 15 minutes after Smart Update
changes when `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`. The handler runs
`scripts/run_static_site_builder_kaggle.py` with:

- `/data/db.sqlite` snapshot/export source;
- `--export-in-kaggle` so event export and related-chain generation happen
  inside the same Kaggle CPU job as Astro build;
- `--related-cache /data/static_site_event_related_chain_cache.json` so repeated
  rebuilds reuse Gemma audits;
- `--gemma-related-verify --gemma-related-model models/gemma-4-26b-a4b-it` when
  the production gate enables Gemma verification.

Kaggle API-started kernels cannot rely on Kaggle UI secrets. The runner
therefore creates encrypted split private datasets (`secrets.enc` +
`fernet.key/fernet.keys`) for the minimal runtime env required by the limiter
(`GOOGLE_API_KEY4`, `SUPABASE_URL`, `SUPABASE_KEY`/service key), attaches them to
the kernel, loads them into memory, and cleans up the secret datasets after a
waited run.

## Logging / investigation

The exporter emits JSONL-style stages to stderr with scope
`static_site.event_sparse_related_chain_v1`, including:

- `build_related_start`;
- `cache_check`;
- `sparse_rebuild` / `vector_rebuild` compatibility stage names while the code is
  being renamed;
- `gemma_initial_audit_required` / `gemma_audit_call` / `gemma_audit_error` /
  `gemma_audit_complete`;
- counts of events, changed ids, provider calls and cache hits.

This is the first place to inspect when related chains look wrong. Candidate
entries carry `related_score`, `lexical_similarity`, `deterministic_score`,
`slot_type`, `reason_codes` and `retrieval_sources` in `preview-related.json`.

## Acceptance evidence to keep current

- `npm run check:preview` must verify the sparse manifest contract.
- Public Playwright regression must cover forbidden admission copy, visible card
  hashtags, registration link overrides, `Показать ещё`, visible images and the
  6447 golden-anchor ordering (`6310` before `5261`).
- Browser QA must verify UI regressions: sold-out CTA disabled, reset timestamp
  visible, photo CTA not overlapping, drawer auto-closes, update time is
  Kaliningrad time, OCR hero still parallax-scrolls.
