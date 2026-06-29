# Semantic vector retrieval for events

> Status: **P0 pgvector semantic retrieval implemented and canary-published in v48**. The accepted production candidate is the separate personalization Supabase project with `pgvector` + `gemini-embedding-2` (`vector(768)`). The earlier TF-IDF/sparse chain remains only an honest lexical rollback/baseline and must not be called semantic search.

## Why this document exists

The static site needs two different retrieval modes:

1. **Public related/discovery on event pages** — computed offline after Smart Update/static export and published as static JSON/HTML. Ordinary page views must not call LLMs, embedding providers or Supabase vector search.
2. **Authorized one-line search** — explicit user action after login, with quota, query embedding and optional LLM verification over already retrieved IDs.

The old `local_tfidf_sparse_v1` layer could rank “Музыка нашего города” near an urban-planning event because of the lexical token “город”. Semantic retrieval must use real embeddings and must prove on golden anchors that architectural/urban candidates outrank lexical false positives.

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

### Vector sync / document generation

Script: `scripts/sync_event_search_vectors_to_supabase.py`.

It consumes the exported `site/src/data/preview-events.json`, builds compact search docs, hashes embedding input and upserts only changed vectors. The current document version is `event-search-doc-v2-weekday` and includes weekday, time-of-day, admission/availability and card URL fields.

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

### Authorized search

Design doc: `docs/features/unsigned-personalization/authorized-event-search.md`.

Implemented source pieces:

- `site/src/components/AuthorizedEventSearch.astro` — one-line search UI, Yandex OAuth entry, quota/status text, same EventCard contract.
- `supabase/functions/event-search/index.ts` — authenticated Edge Function source: quota reservation before provider call, Gemini query embedding, pgvector RPC, optional Gemma verifier/rerank and fallback cards.

Remaining external gates: Supabase custom OAuth provider `custom:yandex`, Yandex app credentials and Edge Function deployment/env configuration.

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
- deploy/browser readiness check: `scripts/check_authorized_search_readiness.py --env-file .env --probe-auth-config --probe-yandex-provider --probe-edge --strict` is green for the current personalization Supabase/Yandex canary; v51 additionally verifies the browser PKCE callback path with a mocked public preview smoke.

## Job sequence after Smart Update

1. Smart Update updates canonical events in Fly SQLite.
2. Static-site build is coalesced after the update window, as for Telegraph/page generation.
3. Kaggle StaticSiteBuilder runs on CPU from a production SQLite snapshot.
4. Static exporter emits compact preview events and search documents.
5. Vector sync upserts changed `event_search_documents` and only changed/missing `event_embeddings` into personalization Supabase.
6. Related graph is recomputed for the whole active/future set, not only changed anchors: a new event can become the best related candidate for older events.
7. Optional Gemma 4 26B verifier reranks/rejects already retrieved candidates; malformed/timeout responses fall back to vector order.
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
