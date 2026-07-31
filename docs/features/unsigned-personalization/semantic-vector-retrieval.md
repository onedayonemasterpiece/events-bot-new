# Semantic vector retrieval for events

> Status: **two-document pgvector retrieval implemented and used by the current full-catalog static preview**. The accepted production candidate is the separate personalization Supabase project with `pgvector` + `gemini-embedding-2` (`vector(768)`), now split by `embedding_doc_kind`: `search_v3` for user search and `related_v1` for event-to-event related pages. The earlier TF-IDF/sparse chain remains only an honest lexical rollback/baseline and must not be called semantic search.

## Why this document exists

The static site needs two different retrieval modes:

1. **Public related/discovery on event pages** — computed offline after Smart Update/static export and published as static JSON/HTML. Ordinary page views must not call LLMs, embedding providers or Supabase vector search.
2. **Authorized one-line search** — explicit user action after login, with quota, query embedding and optional LLM verification over already retrieved IDs.

The old `local_tfidf_sparse_v1` layer could rank “Музыка нашего города” near an urban-planning event because of the lexical token “город”. Semantic retrieval must use real embeddings and must prove on golden anchors that architectural/urban candidates outrank lexical false positives.

## R15 shared static BGE boundary

The unusual-events candidate introduces a separate, pinned BGE-M3 space for
offline static consumers. It reuses the factual `related_v1` **document
contract**, but does not mix BGE's 1024-dimension vectors with the
768-dimension Gemini pgvector rows described below. One BGE encode boundary
produces each event vector once for public static related retrieval, unusual
prototype scoring, family evidence and presentation concept support; no
consumer creates a second embedding pass. Ordinary views remain static and the
required build counter is `provider_calls=0`.

The complete taxonomy, hashes, activation metrics, cache/last-good behavior and
rollout boundary are canonical in
[`docs/features/unusual-events/README.md`](../unusual-events/README.md).
Gemini `search_v3` authorized Search below remains valid and is not silently
migrated by R15. Gemini `related_v1` may remain only as an explicitly selected
rollback/comparison canary; it is not a concurrent production public-related
source once the shared-BGE mode is enabled.

## Relevance contract for authorized search

Authorized search is a two-stage semantic pipeline, not a deterministic keyword gate:

1. **pgvector retrieval** returns nearest event candidates from `gemini-embedding-2` vectors over compact event search documents. pgvector is the index/search engine; it does not create vectors by itself, so the current implementation still creates one `gemini-embedding-2` query vector per explicit authorized search.
2. **LLM verifier** receives only the retrieved candidate IDs plus compact public facts and decides which candidates are exact matches for the user query. The online verifier is Gemini Lite first, with Gemma 4 26B only as slower overflow. It may return fewer than the requested limit, including zero.

High-match invariant: raw pgvector candidates are not exact search results until the LLM explicitly approves them. On LLM timeout/error/quota miss/insufficient facts/over-approval, exact results are empty and candidates are downgraded to the separately headed possible/discovery section. For audience-sensitive queries such as “интересно детям”, relevance must be decided by the LLM from event facts rather than by regex filters. Adult/professional urban-planning events are rejected by the verifier when the facts do not support child/family suitability; they can still appear later only under the separately headed fallback/discovery section, not as exact search results.

## Layer boundaries

- **Fly SQLite / Smart Update** remains the canonical event/source/publication DB.
- **Astro/static-site artifacts** remain the public page source: event pages, listings, discovery JSON, sitemap and ICS.
- **Personalization Supabase/Postgres** is a sidecar search/retrieval layer only. It stores compact public search documents, trusted card snapshots, embeddings, quota ledgers and compact search audit. It is not the canonical event DB.
- **Browser direct table select/insert/update is forbidden** for search documents, embeddings, quota and audit tables.
- **Allowed browser path** for search: Supabase Auth session → Edge Function `event-search` → authenticated RPC. Public event-page related does not use this online path.

## Implemented P0 components

### Supabase schema

Migrations:

- `supabase/migrations/20260628_event_search_pgvector.sql` — base pgvector/search/quota/audit schema and authenticated search RPCs.
- `supabase/migrations/20260628_event_search_weekday_and_related_rpc.sql` — weekday facets and service-role-only related RPC for builders.
- `supabase/migrations/20260628_event_search_public_fields_and_model_filter.sql` — public/searchable/card fields, model/dimension-scoped search RPC and safer fallback filtering.
- `supabase/migrations/20260629_event_search_query_facets.sql` — explicit-query facet boost for weekday/time/admission words in authorized search.
- `supabase/migrations/20260630_event_search_embedding_doc_kind.sql` — implemented the two-document split: `event_search_documents.related_digest/related_text_hash`, `event_embeddings.embedding_doc_kind`, `(event_id, embedding_model, embedding_dim, embedding_doc_kind)` primary key, partial HNSW indexes for `search_v3` and `related_v1`, and doc-kind filters in both search and related RPCs.

Main tables:

- `event_search_documents` — compact factual `search_digest` for query search, cleaner `related_digest` for event-to-event similarity, facets, dates, status, canonical path/slug and trusted `card_snapshot`; no raw OCR or source HTML.
- Admission facets fail closed: an event with `unknown`/status-only admission is not tagged as `ticketed`; `ticketed` is emitted only for an explicit ticket, registration or phone-booking contract, while `free` still requires an explicit free flag.
- `event_embeddings` — `gemini-embedding-2`, `embedding_dim=768`, `vector(768)`, `embedding_doc_kind` (`search_v3` or `related_v1`), partial HNSW cosine indexes, `(event_id, embedding_model, embedding_dim, embedding_doc_kind)` primary key.
- `search_quota_plans`, `user_search_quota_ledger`, `event_search_requests` — authorized search quotas and privacy-preserving audit.

Main RPCs:

- `event_related_candidates_by_event_id_v1(...)` — backend/static-builder only, granted to `service_role`, revoked from `public`, `anon`, `authenticated`.
- `search_events_by_embedding_v1(...)` — authenticated search RPC, filters `is_public`, `is_searchable`, active lifecycle and model/dimension.
  For explicit user search it also accepts optional `p_weekday_iso`, `p_time_of_day_filter` and `p_admission_filter` facets; these facets are extracted from the query by the Edge Function and used only as a small post-retrieval boost over the nearest pgvector candidates, not as a separate broad keyword search.
- `event_search_fallback_cards_v1(...)`, `get_event_search_quota_v1(...)`, `reserve_event_search_quota_v1(...)`, `record_event_search_request_v1(...)`.

RLS is enabled; browser roles have no direct raw-table grants.

### Embedding model

Accepted P0 model: **`gemini-embedding-2` with output dimension `768`**.

Reasons:

- project quota is enough for current active/future catalogue backfills and low-volume authorized search;
- 768 dimensions keep pgvector storage/index small enough for the Supabase free-tier budget;
- the same embedding space is used for event-to-event related and query-to-event search;
- model and dimension are stored and filtered so `gemini-embedding-001`, `gemini-embedding-2` or any future model cannot be mixed in one retrieval call.

Embedding inputs are deterministic factual manifests, not raw OCR:

```text
Document: title: {title} | text: {search_digest}
Query:    task: search result | query: {user_query}
```

### Poster/OCR boundary

Raw poster OCR does **not** go directly into either vector document kind:

- `/poisk/` embeds and searches `search_v3`, built from canonical public event facts and the curated `search_digest`;
- static event-to-event related chains embed and search `related_v1`, built from cleaner title/type/category/summary/venue/audience context;
- the LLM verifier for `/poisk/` receives retrieved event ids plus compact public facts (`search_digest`/card facts), not raw poster OCR text.

Poster OCR can still influence search **indirectly** when Smart Update has already promoted a source-grounded poster fact into the canonical event row, for example a corrected title, date/time, venue/address, ticket status, topic or `search_digest`. That is intentional: the vector layer indexes the accepted event meaning, not the raw image transcript.

Do not add raw OCR dumps, poster service headings or unreviewed poster venue strings to `search_v3`/`related_v1`. Posters often contain commercial venue names, sponsor names, ticket-office brands and layout labels; embedding them raw would let those high-salience names swamp semantic similarity and make unrelated events look close. If OCR supplies a venue, it must first pass the extraction/update grounding path and become a canonical venue/address before it appears in vector search.

### Two-document vector decision (implemented)

The current implementation deliberately uses **one embedding model and one table**, but **two document representations** per event:

- `search_v3` — broad query-search document. It includes weekday/month/season, daypart (`утро/день/вечер/ночь`), weekend/holiday language, free/registration/paid/sold-out status, charity/family/tourist/Pushkin-card hints and query-friendly synonyms. This is the document kind used by `/poisk/`.
- `related_v1` — cleaner event-to-event document. It emphasizes title, event type, category, format, themes, concise meaning, venue/context and audience. Calendar/admission noise is intentionally reduced so “бесплатно вечером” does not beat true thematic similarity in related cards. This is the document kind used by static related generation.
- When canonical `location_name` is only the same settlement fallback as `city`, the preview/vector projection exports no synthetic `venue_name`; both `search_v3` and `related_v1` keep the city once. Exact presentation deduplication does not infer a venue and does not replace the LLM grounding verdict.

Why this is not “two vector databases”: both kinds use `gemini-embedding-2/vector(768)` and live in `event_embeddings`; the key difference is `embedding_doc_kind`. RPCs filter by `embedding_model`, `embedding_dim` and `embedding_doc_kind`, so search and related retrieval cannot accidentally mix representations.

Implemented storage impact after the 2026-06-30 backfill: `event_search_documents≈3.8 MiB`, `event_embeddings≈9.4 MiB` for about 404 `search_v3` vectors and 343 `related_v1` vectors; total personalization DB size was about 25 MiB, comfortably below the 500 MiB free-tier budget. Expired/past events should keep their canonical facts in Fly SQLite, while Supabase vector rows can be pruned or archived by lifecycle once pages are no longer public/actionable.

### Vector sync / document generation

Script: `scripts/sync_event_search_vectors_to_supabase.py`.

It consumes the exported `site/src/data/preview-events.json`, builds compact search docs, hashes embedding input and upserts only changed vectors. The current document version is `event-search-doc-v3-search-facets` and includes:

- event category/type/title/summary and a truncated factual description;
- venue/address/city;
- date/time plus weekday ISO/RU, weekend/weekday, month, season and RU daypart words in `search_v3`;
- admission/availability (`бесплатно`, `регистрация`, `по билетам`, sold-out when present) in `search_v3`;
- controlled tags, Pushkin-card/family/charity/tourist hints when source text or existing topics explicitly support them;
- a cleaner `related_v1` digest that removes most calendar/price noise and keeps theme/format/audience/venue context;
- trusted card URL/snapshot fields.

The list above is deliberately canonical-field-only. It excludes raw OCR, raw source HTML, raw poster text and provider/debug payloads; see the poster/OCR boundary above for the only supported indirect path.

The sync is incremental by `(event_id, embedding_model, embedding_dim, embedding_doc_kind, text_hash)`: unchanged `search_v3` or `related_v1` rows are skipped independently, so adding the second document kind does not force a full re-embedding after the initial backfill.

### Static related pipeline

Exporter: `site/scripts/export-production-preview-data.py`.

For `--related-mode pgvector`:

1. write preview event JSON;
2. optionally run `--sync-pgvector-vectors` to upsert documents/vectors;
3. call service-role RPC `event_related_candidates_by_event_id_v1` for each anchor;
4. score primarily by `vector_similarity`, with small deterministic/facet boosts only;
5. optionally run Gemma 4 26B verifier over retrieved IDs only;
6. export static discovery JSON with `algorithm_id=event_pgvector_related_chain_v2_two_doc`, `retrieval_method=supabase_pgvector_hnsw_cosine_v1`, `semantic_embeddings=true`, `embedding_document_version=related_v1`.

Astro pages still read only the static JSON/HTML chain on page view.

Static related-generation model policy is deliberately different from the
interactive `/poisk/` policy:

- primary/audit model is Gemma 4 26B (`models/gemma-4-26b-a4b-it`);
- there is **no** Flash-Lite/Gemini-Lite fallback for static related generation;
- provider `5xx/429/timeout` must be handled by repeated Gemma attempts with
  pauses/backoff (`STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS`,
  `STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC`) because static rebuild speed is
  less important than preserving the scarce `gemini-3.1-flash-lite` quota for
  online rescue and other critical flows;
- strict production/canary builds must publish “similar” cards only when Gemma
  explicitly accepted the candidate (`llm_semantic_score >= 0.72`); weak
  0.55–0.71 candidates are adjacent/explore material only;
- if all Gemma attempts fail for an anchor, the builder records the error. A
  release/canary gate may either fail the build or publish the anchor without a
  strict similar block, but it must not silently label raw pgvector order as
  Gemma-verified related.

#### Gemma static-related verifier contract v4

The full-catalog v61 stress run showed that the previous verifier contract was
too verbose for Gemma 4 26B: retry classes were dominated by malformed JSON
(`json_unterminated`, `json_expecting_value`, `json_comma`) and timeouts, while
successful calls had p50/p95 around `22s/31s`. The root issue is output size and
truncation, not a need to replace Gemma with Flash-Lite.

The accepted v4 contract is a compromise between JSON stability and semantic
quality:

- input prompt is compact XML-like factual blocks, not a large JSON-stringified
  instruction payload;
- Gemma evaluates candidates in compact batches of `10` by default
  (`STATIC_SITE_GEMMA_RELATED_CANDIDATE_LIMIT`, clamped to `6..12`);
- static related uses `2` passes by default
  (`STATIC_SITE_GEMMA_RELATED_PASSES`, clamped to `1..3`), so the normal strict
  audit inspects up to `20` pgvector candidates without forcing one large
  fragile JSON response;
- fact text defaults to `360` chars per event;
- native structured output schema returns
  `ranked[].event_id`, `ranked[].llm_semantic_score`,
  `ranked[].similarity_class`, `ranked[].confidence`, `ranked[].reject`;
- model-provided `similarity_class` and `confidence` are preserved because they
  are product/QA signals, while verbose `reason_codes` are not requested;
- output cap defaults to `768` tokens; timeout defaults to `60s`; default static
  attempts are `2` with `10s` backoff;
- JSON rescue is syntax-only: it may salvage fully complete verdict objects from
  a truncated `ranked` array, but it never invents ids, scores or semantic
  decisions.

For the **strict “Похожие” block**, pgvector remains the recall stage and Gemma
is the precision stage. The normal offline build now verifies two compact
batches (`1..10` and `11..20`) and merges/reorders by Gemma score/class. The
remaining lower-ranked pgvector candidates should be shown later only under a
separate adjacent/discovery heading such as “Возможно, вам будет интересно”,
not silently mixed into strict similar cards.

Prompt/schema audit evidence:

- Gemini Pro review: `artifacts/codex/related-gemma-prompt-audit-20260630/gemini-3.1-pro-review.md`.
- Opus consultation was requested but blocked: `a-opus`/`agy` returned empty
  output and Claude Opus returned `401`; evidence is in
  `artifacts/codex/related-gemma-prompt-audit-20260630/OPUS_BLOCKER.md`.
- Local live smoke after v3 compaction: synthetic 4-candidate call `4.65s`,
  real anchors `6447/5878/5370` returned valid JSON in `8.20s/6.43s/6.22s`.
- Local live smoke after v4 restored model `similarity_class`/`confidence`:
  synthetic 4-candidate call returned valid JSON in `7.00s`.
  This smoke was followed by the full v62 run below; the remaining next step is
  to move recovered-cache reuse into the normal Kaggle success path so future
  failed notebooks do not require manual artifact recovery.

### v62 full-catalog two-document related evidence

Run: `preview-20260630-event-pages-v62-two-vector-gemma-full`. Scope: `343` future/actionable events on the 2026-06-30 snapshot. Retrieval: `search_v3` and `related_v1` vectors in the same `event_embeddings` table, static related using only `related_v1`.

Important evidence:

- Supabase vector sidecar after backfill: about `25 MB` database size; `event_embeddings≈9.4 MiB`, `event_search_documents≈4.1 MiB`; counts `search_v3≈404`, `related_v1≈343`.
- Kaggle generated the reusable v2 related cache for `343` anchors; because the notebook later ended `ERROR`, the accepted preview was rebuilt locally from the recovered `event_related_chain_cache.json` and `events.sqlite` with `0` new provider calls.
- Cache-hit export: `343` anchors, `343` Gemma cache hits, `provider_calls=0`, `algorithm=event_pgvector_related_chain_v2_two_doc`, `embedding_doc_kind=related_v1`.
- Static preview checks passed and the v62 tree is uploaded under `s3://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/` (`343` event pages and `343` discovery JSON files). Public `https://kenigevents.ru/...` GET currently returns `404` because bucket public-read policy is not enabled for uploaded objects; `https://static.kenigevents.ru/...` also fails TLS validation until the CDN certificate/domain binding is completed.
- Golden public discovery checks:
  - `6447` («Как договориться о будущем города») strict related is only `4759` and `6310`; both have model-provided `llm_semantic_score`, `similarity_class` and `llm_confidence`, and the prior music false-positive is absent from strict related.
  - `5878` («Песни СССР») strict related starts with music/retro/concert events (`3398`, `5777`, `6488`, `6481`, `5733`).
  - `5370` («Точка и линия») strict related starts with art/exhibition events (`6214`, `5969`, `6080`, `5391`).
- Browser evidence: mocked authorized search UI smoke and real Edge smoke both rendered scrollable cards with shared actions; the real Edge smoke for `концерт классической музыки` returned `12` rendered cards.

The v62 failure mode was operational, not semantic: leaving `node22` in `/kaggle/working` made the failed notebook artifact too large/noisy. The Kaggle kernel now deletes transient `node22` and extracted site paths on both success and failure while preserving recoverable cache/SQLite outputs. The exporter also refuses to overwrite a larger expensive related cache with a smaller canary run unless `STATIC_SITE_ALLOW_RELATED_CACHE_SHRINK=1` is explicitly set.

### Authorized search

Design doc: `docs/features/unsigned-personalization/authorized-event-search.md`.

Implemented source pieces:

- `site/src/components/AuthorizedEventSearch.astro` — one-line search UI, Yandex OAuth entry, quota/status text, same EventCard contract.
- `supabase/functions/event-search/index.ts` — authenticated Edge Function source: quota reservation before provider call, direct multi-key Google provider rotation/failover for Gemini query embedding, pgvector RPC, Gemini Lite verifier first, optional Gemma 4 26B overflow and fallback cards.

2026-07-01 smart-search capacity gate: query embedding rotates across all five
Google keys because online query embedding is a new workload and can safely use
the full `gemini-embedding-2` pool with a `1000 RPD` buffer for
static/vector/backfill work. Gemini Lite verification rotates across the shared
non-guide pool (`GOOGLE_API_KEY5`, `GOOGLE_API_KEY4`, `GOOGLE_API_KEY3`,
`GOOGLE_API_KEY`) and keeps `GOOGLE_API_KEY2` as the fixed guide-monitoring
reserve/failover lane. Product registration is counted from
`auth.identities.provider='custom:yandex'`, not from all historical
`auth.users` rows; on 2026-07-01 this means `1` effective Yandex-registered site
user, not `47` total Auth rows. The canary limit is therefore `1000/day` search
and `1000/day` verifier calls per registered Yandex user (`10000/month` each),
leaving `800` Lite RPD as cross-service buffer and `1000` embedding RPD as
backfill/diagnostic buffer. The plan is applied by
`supabase/migrations/20260701180316_event_search_key5_quota_capacity.sql`.

External gate completed on 2026-07-01 from branch
`feature/smart-search-quota-key5-site`: Google key secrets are present,
`EVENT_SEARCH_EMBEDDING_KEY_ENVS` uses all five keys,
`EVENT_SEARCH_LLM_KEY_ENVS` uses the non-guide shared pool, `GOOGLE_API_KEY2` is
configured as `EVENT_SEARCH_LLM_RESERVE_KEY_ENVS`, the quota migration is
applied, and `event-search` is deployed with the Lite-first/Gemma-overflow code
path. Final live smoke used the all-key embedding pool and an active shared LLM
lane (`GOOGLE_API_KEY3`) with `11` exact items and quota `999/999` remaining
after the smoke.

## v59 strict static-related process and evidence

The current accepted process for event-page bottom recommendations is **offline strict related generation**:

```text
Smart Update changed event/source facts
  -> coalesced static-site job after update quiet period
  -> export active/future event slice from Fly SQLite
  -> upsert changed public search documents + changed/missing embeddings in Supabase
  -> pgvector RPC returns nearest event-to-event candidates
  -> Gemma 4 26B verifies/reorders only retrieved IDs
  -> static JSON/HTML publishes only Gemma-approved similar candidates
```

Recompute policy:

- do not recompute if event ids, event search fingerprints and the Gemma policy signature are unchanged;
- if a new event appears or an existing event’s factual search document changes, recompute the active/future graph, not only the changed anchor, because a new event can become the best related item for older pages;
- current implementation persists the related graph in the static-builder related cache (`raw_chains`, Gemma audit cache and `gemma_verified_event_ids`). The cache key includes event fingerprints, candidate fingerprints and Gemma policy signature, so unchanged anchor/candidate pairs are reused. Target persistence is still `event_similarity_edges` (P1): store `(anchor_event_id, candidate_event_id, related_score, vector_similarity, llm_semantic_score, confidence, similarity_class, doc_kind, model, policy_signature, source_fingerprints, computed_at)`. A changed/new event recomputes its own outgoing edges and can update reverse incoming edges for older anchors without re-verifying unchanged pairs. Astro generation then sorts from cached edges and applies lifecycle/date exclusions at render time.
- static related can run less often than Astro page rendering: Astro can reuse a valid related cache, while lifecycle/date changes are still applied at export/generation time;
- when generating pages, exclude events that already started today, ended, were cancelled, deleted or duplicated, so related cards remain actionable.

v59 strict canary (`preview-20260629-event-pages-v59-related-gemma50`) used 50 real production events focused on 2026-06-30/2026-07-01 and supplements later active future events when the two-day window has fewer than 50 events. Metrics:

- pgvector sync: 44 new/changed `gemini-embedding-2/vector(768)` embeddings, 6 unchanged, 32.59s wall;
- pgvector retrieval: 50 anchors, 40 raw candidates per anchor;
- Gemma verification: 50 successful anchors, 60 total attempts after retries, first-pass wall 22m53s plus 3m53s fill-missing;
- successful Gemma calls first-pass timing: min 6.0s, p50 18.3s, avg 17.0s, max 27.1s;
- final cache-hit export: 0 provider calls, 0.47s;
- strict similar lengths: min 1, max 8, avg 3.02;
- golden `6447` now publishes `6310` as the only strict first related candidate (`llm_semantic_score=0.88`) and does not let the earlier music false-positive into the similar block.

Artifacts for this canary are under `artifacts/codex/static-site-related-20260629/` and are intentionally not committed. The committed preview fixture keeps the generated `preview-events.json` and `preview-related.json` so reviewers can inspect the public output without re-running provider calls.

## v48 canary evidence

Public Kaggle-built preview:

- <https://kenigevents.ru/preview-20260628-event-pages-v48-pgvector-gemma-kaggle/__preview/>
- control JSON: <https://kenigevents.ru/preview-20260628-event-pages-v48-pgvector-gemma-kaggle/data/discovery/6447.json>

Evidence from 2026-06-29 UTC:

- local sync/backfill: `70` documents processed, `12` new/changed `gemini-embedding-2` vectors after weekday/category hardening;
- live personalization Supabase: `event_search_documents=76`, `event_embeddings=76` for `gemini-embedding-2/vector(768)`;
- golden anchor `6447` (“Как договориться о будущем города”): first non-self related candidate is `6310` (“Архитектурно-урбанистическая студия...”) with `vector_similarity≈0.8592`, ahead of the known lexical false-positive `5261` (“Музыка нашего города”);
- Gemma 4 26B verifier local canary: `status=ok`, `audited_anchors=15`, `provider_calls=7`, `cache_hits=8`, `errors=[]`;
- Kaggle CPU canary: `preview-20260628-event-pages-v48-pgvector-gemma-kaggle`, `ok=true`, `event_count=70`, vector sync `provider_calls=0` because vectors were already current, `npm run check:preview` passed inside the notebook;
- live public smoke: `/data/discovery/6447.json` returns `algorithm_id=event_pgvector_related_chain_v1`, `strategy=event_pgvector_related_chain_v1_manifest`, first candidate `6310`, `llm_semantic_score=0.92`.
- authorized RPC smoke: temporary Supabase Auth user + real JWT + quota reservation + Gemini Embedding 2 query vector + `search_events_by_embedding_v1`; query `урбанистика будущее города` returns `6310` in top-3 and cleanup removes smoke rows/user.
- authorized facet RPC smoke after `20260629_event_search_query_facets.sql`: query `урбанистика в четверг вечером по регистрации` with facets `weekday_iso=4`, `time_of_day=evening`, `admission=registration_required` returns event `6310` top-1 (`similarity≈0.9255`) while preserving the pgvector nearest-candidate path.
- mocked browser UI smoke: `scripts/smoke_authorized_search_ui.py` renders the static preview with public Supabase env, simulates a Supabase Auth callback, calls a mocked `event-search`, and verifies authorized search cards keep the shared feed-card actions plus investigation metadata (`request_id`, `served_list_id`, `served_list_hash`, `surface=authorized_event_search`). This is frontend regression evidence only, not the final Yandex OAuth/deployed Edge E2E.
- production handoff command test passes `--related-mode pgvector`, the
  completed vector receipt revision, `--pgvector-*`, `--gemma-related-*`, CDN
  asset/ICS bases, status callback args and browser-safe AuthorizedEventSearch
  public env into the Kaggle runner; production explicitly omits
  `--sync-pgvector-vectors` because the dedicated Fly owner already completed
  the projection.
- deploy/browser readiness check: `scripts/check_authorized_search_readiness.py --env-file .env --probe-auth-config --probe-yandex-provider --probe-edge --strict` is green for the current personalization Supabase/Yandex canary; v51 additionally verifies the browser PKCE callback path with a mocked public preview smoke and the live `custom:yandex` provider now points at the Yandex userinfo adapter instead of direct non-standard Yandex JSON.

## Job sequence after Smart Update

1. Smart Update updates canonical events in Fly SQLite.
2. The coalesced Fly `event_vector_sync` owner upserts only changed documents
   and embeddings, then writes a complete revision receipt.
3. The coalesced static build waits at the vector barrier until that receipt
   covers its source revision.
4. Kaggle StaticSiteBuilder runs on CPU from an immutable production SQLite
   snapshot and emits compact preview events.
5. The exporter reads only the bounded compact related-candidate RPC; it does
   not repeat the vector write projection.

### Durable production projection

Production ownership is independent of static preview builds. With
`ENABLE_EVENT_VECTOR_SYNC=1`, every completed Smart Update enqueues one
coalesced `event_vector_sync:prod` outbox job (default debounce: 90 seconds),
and APScheduler performs a full actionable-catalog reconciliation every
`EVENT_VECTOR_SYNC_INTERVAL_MINUTES` (default: 180). The job exports the whole
current catalogue, projects both document kinds by hash, removes stale sidecar
rows, fails/retries on an incomplete provider-call cap, and persists structured
counts/errors in `ops_run(kind='event_vector_sync')`. The vector-only export
skips remote image-dimension probes; it reuses canonical media URLs/card facts
without turning a semantic projection into a slow media-quality crawl.

StaticSiteBuilder remains a build consumer. A manual canary/backfill may
explicitly refresh vectors, but production keeps
`STATIC_SITE_SYNC_PGVECTOR_VECTORS=0`; ephemeral audit embeddings never count
as persistent sidecar coverage.
6. Related graph is recomputed for the whole active/future set, not only changed anchors: a new event can become the best related candidate for older events.
7. Gemma 4 26B verifier reranks/rejects already retrieved candidates; strict publication must not label raw pgvector candidates as similar when Gemma verification fails.
8. Astro builds HTML/JSON/ICS from the static manifest.
   For focus-group/auth previews, the Kaggle kernel maps only browser-safe public values into Astro:
   `PUBLIC_PERSONALIZATION_SUPABASE_URL`, `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` and `PUBLIC_YANDEX_AUTH_PROVIDER`.
   Backend-only Supabase secret/service keys are used only for vector sync/RPC and are never copied to `PUBLIC_*`.
9. Preview/public checks run before publishing. Last-good static build remains fallback.
10. CDN/Object Storage promotion is a separate release gate from successful Kaggle artifact creation.

## Product surfaces

- **Похожие / Смотрите дальше** on event detail pages: static related chain from `event_pgvector_related_chain_v2_two_doc` / `related_v1` in current v62 builds; mobile is feed-like, desktop can be grid/list. Historical v48/v59 previews used `event_pgvector_related_chain_v1`.
- **Для меня**: local/profile-aware personalization on top of static manifests; no online pgvector call on ordinary page view.
- **Умный поиск**: authorized explicit search with quota; results are rendered by the shared `KenigEventsRenderEventCard` feed-card renderer, so like, share, “не интересно”, calendar/detail actions and local personalization feedback stay identical to `Смотрите дальше`.
  The authorized-results container carries `request_id`, `served_list_id`, `served_list_hash` and `algorithm_id`; strong actions inherit this context for investigation.
  Query words like “пятница”, “вечером”, “бесплатно” are parsed into compact facets and logged as facet metadata only; the raw query is still not persisted.
- When exact search results are exhausted, UI starts a clearly separate **«Возможно, вам будет интересно»** section.

## Quality gates

Golden anchors are mandatory:

```yaml
- anchor_event_id: 6447
  must_include:
    - 6310 # Архитектурно-урбанистическая студия
  must_exclude_top:
    - 5261 # Музыка нашего города
  notes: "Urban-planning intent must beat lexical 'город' overlap."
- anchor_event_id: 5878
  expectation: top candidates are music/concert/retro/classical/vocal-adjacent.
- anchor_event_id: 5237
  expectation: opera/classical/music lecture/classical concert candidates dominate.
- anchor_event_id: 5370
  expectation: art/exhibition/museum candidates dominate.
- anchor_event_id: 6322
  expectation: family/outdoor/kids/daytime candidates dominate.
```

Required checks before production cutover:

- active/future embedding coverage >= configured threshold, target 95%+;
- all active vectors in a build use one `embedding_model` and `embedding_dim`;
- no hard-negative in top slots for golden anchors;
- candidate payload includes `slot_type`, score breakdown and `vector_similarity` for pgvector chains;
- cancelled/postponed events are excluded; sold-out can be shown only with explicit status;
- no popularity/source-like boost can push an unrelated event into `pure_related`;
- static preview includes a related-quality/debug route or equivalent artifact for reviewers;
- authorized search stores no raw query text and returns cards from trusted snapshots only.

## Forbidden patterns

- Calling TF-IDF/BM25/cosine over lexical features “semantic vector search”.
- Browser direct table access to `event_search_documents`, `event_embeddings`, quota/audit tables or profile tables.
- LLM/embedding/vector calls on ordinary public page views.
- Mixing embeddings from different models/dimensions in one query or index.
- Embedding raw HTML, raw OCR dumps, raw source JSON or UI boilerplate.
- Updating only changed anchors for related graph publication.
- Letting LLM create events or overwrite dates/prices/status/card fields.
- Treating Supabase pgvector as replacement for Fly SQLite or as the default read path for static pages.

## Rollback

If pgvector quality or availability fails, publish the last-good static sparse manifest with explicit metadata:

- `algorithm_id=event_sparse_related_chain_v1`;
- `retrieval_method=local_tfidf_sparse_v1`;
- `semantic_embeddings=false`.

Rollback must be visible in manifest metadata and must not be labeled semantic.
