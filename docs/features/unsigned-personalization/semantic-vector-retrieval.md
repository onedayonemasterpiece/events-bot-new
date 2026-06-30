# Semantic vector retrieval for events

> Status: **P0 pgvector semantic retrieval implemented; strict Gemma-verified static related canary published in v59**. The accepted production candidate is the separate personalization Supabase project with `pgvector` + `gemini-embedding-2` (`vector(768)`). The earlier TF-IDF/sparse chain remains only an honest lexical rollback/baseline and must not be called semantic search.

## Why this document exists

The static site needs two different retrieval modes:

1. **Public related/discovery on event pages** — computed offline after Smart Update/static export and published as static JSON/HTML. Ordinary page views must not call LLMs, embedding providers or Supabase vector search.
2. **Authorized one-line search** — explicit user action after login, with quota, query embedding and optional LLM verification over already retrieved IDs.

The old `local_tfidf_sparse_v1` layer could rank “Музыка нашего города” near an urban-planning event because of the lexical token “город”. Semantic retrieval must use real embeddings and must prove on golden anchors that architectural/urban candidates outrank lexical false positives.

## Relevance contract for authorized search

Authorized search is a two-stage semantic pipeline, not a deterministic keyword gate:

1. **pgvector retrieval** returns nearest event candidates from `gemini-embedding-2` vectors over compact event search documents.
2. **LLM verifier** receives only the retrieved candidate IDs plus compact public facts and decides which candidates are exact matches for the user query. It may return fewer than the requested limit, including zero.

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

Main tables:

- `event_search_documents` — compact factual `search_digest`, facets, dates, status, canonical path/slug and trusted `card_snapshot`; no raw OCR or source HTML.
- `event_embeddings` — `gemini-embedding-2`, `embedding_dim=768`, `vector(768)`, HNSW cosine index, `(event_id, embedding_model, embedding_dim)` primary key.
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

### One vector vs two vectors decision

The current production-canary implementation still uses **one embedding row per event** (`event_embeddings(event_id, embedding_model, embedding_dim)`) fed by `search_digest`. This is acceptable for the first full-catalog stress test because the catalogue is only a few hundred active/future events and Gemma verification/reranking is already the final semantic authority.

However, one vector is an explicit compromise, not the target architecture:

- **Related pages** need a “pure similarity” representation: title, event type, genre/theme, format, people/organizer/venue and concise meaning. Calendar words, price words and broad “вечером/летом/бесплатно” facets should not overpower real thematic similarity.
- **User search** needs a broader representation: weekday/month/season, daypart (`утро/день/вечер/ночь`), weekend/holiday, free/registration/paid/sold-out status, charity, audience, mood/occasion and query-friendly synonyms.
- LLM verification can clean false positives, but using a polluted shared vector wastes candidate slots before the LLM sees them. With only 12–40 candidates, losing slots to calendar/price noise can be worse than the LLM cost.

Decision:

1. **P0/full stress test:** keep one embedding table/model and strengthen `search_digest` only with compact factual search facets. This avoids a migration while we measure quality/cost on the full future corpus.
2. **P1 production hardening:** add document-kind separation:
   - `event_embeddings.embedding_doc_kind = 'related_v1' | 'search_v2'`;
   - `related_digest_v1` stays clean and is used by static related graph generation;
   - `search_digest_v2`/`v3` carries query facets and is used by `/poisk/`;
   - RPCs must filter by both model/dimension and `embedding_doc_kind`.
3. The two-vector target still uses the same embedding model (`gemini-embedding-2/vector(768)`) unless a future eval proves a better model; “two vectors” means two **document representations**, not two unrelated vector databases.

### Vector sync / document generation

Script: `scripts/sync_event_search_vectors_to_supabase.py`.

It consumes the exported `site/src/data/preview-events.json`, builds compact search docs, hashes embedding input and upserts only changed vectors. The current document version is `event-search-doc-v3-search-facets` and includes:

- event category/type/title/summary and a truncated factual description;
- venue/address/city;
- date/time plus weekday ISO/RU, weekend/weekday, month, season and RU daypart words;
- admission/availability (`бесплатно`, `регистрация`, `по билетам`, sold-out when present);
- controlled tags, Pushkin-card/family/charity/tourist hints when source text or existing topics explicitly support them;
- trusted card URL/snapshot fields.

This v3 digest is intentionally still a shared P0 compromise. The P1 related/search split above should remove calendar/admission-heavy wording from the `related_v1` document.

### Static related pipeline

Exporter: `site/scripts/export-production-preview-data.py`.

For `--related-mode pgvector`:

1. write preview event JSON;
2. optionally run `--sync-pgvector-vectors` to upsert documents/vectors;
3. call service-role RPC `event_related_candidates_by_event_id_v1` for each anchor;
4. score primarily by `vector_similarity`, with small deterministic/facet boosts only;
5. optionally run Gemma 4 26B verifier over retrieved IDs only;
6. export static discovery JSON with `algorithm_id=event_pgvector_related_chain_v1`, `retrieval_method=supabase_pgvector_hnsw_cosine_v1`, `semantic_embeddings=true`.

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

### Authorized search

Design doc: `docs/features/unsigned-personalization/authorized-event-search.md`.

Implemented source pieces:

- `site/src/components/AuthorizedEventSearch.astro` — one-line search UI, Yandex OAuth entry, quota/status text, same EventCard contract.
- `supabase/functions/event-search/index.ts` — authenticated Edge Function source: quota reservation before provider call, Gemini query embedding, pgvector RPC, optional Gemma verifier/rerank and fallback cards.

Remaining external gates: Supabase custom OAuth provider `custom:yandex`, Yandex app credentials and Edge Function deployment/env configuration.


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
- target persistence is `event_similarity_edges` (P1): store `(anchor_event_id, candidate_event_id, related_score, vector_similarity, llm_semantic_score, similarity_class, doc_version, model, policy_signature, source_fingerprints, computed_at)`. A changed/new event recomputes its own outgoing edges and can update reverse incoming edges for older anchors without re-verifying unchanged pairs. Astro generation then sorts from cached edges and applies lifecycle/date exclusions at render time.
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
- production handoff command test: `main._static_site_build_kaggle_command` now passes `--related-mode pgvector`, `--sync-pgvector-vectors`, `--pgvector-*`, `--gemma-related-*`, CDN asset/ICS bases, status callback args and browser-safe AuthorizedEventSearch public env (`PUBLIC_PERSONALIZATION_SUPABASE_URL`, publishable key, `custom:yandex`) into the Kaggle runner.
- deploy/browser readiness check: `scripts/check_authorized_search_readiness.py --env-file .env --probe-auth-config --probe-yandex-provider --probe-edge --strict` is green for the current personalization Supabase/Yandex canary; v51 additionally verifies the browser PKCE callback path with a mocked public preview smoke and the live `custom:yandex` provider now points at the Yandex userinfo adapter instead of direct non-standard Yandex JSON.

## Job sequence after Smart Update

1. Smart Update updates canonical events in Fly SQLite.
2. Static-site build is coalesced after the update window, as for Telegraph/page generation.
3. Kaggle StaticSiteBuilder runs on CPU from a production SQLite snapshot.
4. Static exporter emits compact preview events and search documents.
5. Vector sync upserts changed `event_search_documents` and only changed/missing `event_embeddings` into personalization Supabase.
6. Related graph is recomputed for the whole active/future set, not only changed anchors: a new event can become the best related candidate for older events.
7. Gemma 4 26B verifier reranks/rejects already retrieved candidates; strict publication must not label raw pgvector candidates as similar when Gemma verification fails.
8. Astro builds HTML/JSON/ICS from the static manifest.
   For focus-group/auth previews, the Kaggle kernel maps only browser-safe public values into Astro:
   `PUBLIC_PERSONALIZATION_SUPABASE_URL`, `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` and `PUBLIC_YANDEX_AUTH_PROVIDER`.
   Backend-only Supabase secret/service keys are used only for vector sync/RPC and are never copied to `PUBLIC_*`.
9. Preview/public checks run before publishing. Last-good static build remains fallback.
10. CDN/Object Storage promotion is a separate release gate from successful Kaggle artifact creation.

## Product surfaces

- **Похожие / Смотрите дальше** on event detail pages: static related chain from `event_pgvector_related_chain_v1`; mobile is feed-like, desktop can be grid/list.
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
