# Semantic vector retrieval for events

> Status: **updated target architecture with P0 pgvector infrastructure applied**. The current public static related preview still uses the honest lexical/sparse baseline `event_sparse_related_chain_v1`, but the accepted semantic sidecar is now Supabase pgvector + `gemini-embedding-2` (`vector(768)`) for authorized search and future semantic related canaries.

## Why this document exists

The current preview related-chain uses local sparse TF-IDF/cosine matching. That
is useful as a deterministic lexical baseline, but it is **not** semantic vector
search and must not be described as semantic embeddings.

The failure mode is visible on real data: an event about the future/urban
planning of Kaliningrad can rank a concert named “Музыка нашего города” too high
because both texts contain “город”. A semantic layer must understand that
“Архитектурно-урбанистическая студия” is closer to the urban-planning intent.

## Do not conflate these layers

- `event_sparse_related_chain_v1` / `local_tfidf_sparse_v1` = lexical sparse
  baseline, not semantic vector search.
- `pure_related` = events genuinely similar to the opened event.
- `adjacent_discovery` = nearby discovery/anti-bubble candidates, not “similar”.
- `promo` = explicit campaign slot, never masquerades as organic similarity.
- `related_chain` = offline event-to-event graph.
- `personalized_feed` = user/profile-aware feed after consent/JS activation.
- builder-owned SQLite artifact/release snapshot != mutable Fly web-runtime
  vector store.
- Supabase pgvector RPC != default page-view path.

## Rollout levels

### L0 — lexical sparse baseline (current)

- Exporter builds `event_sparse_related_chain_v1` from TF-IDF sparse vectors,
  controlled facets and deterministic constraints.
- Manifest explicitly says `semantic_embeddings=false` and
  `retrieval_method=local_tfidf_sparse_v1`.
- Static HTML renders 6–10 initial cards; JSON top-up uses the same static chain.

### L1 — semantic shadow mode

- BGE-M3 embeddings are generated offline on Kaggle for active/future events.
- Semantic related output is emitted side-by-side with the lexical baseline.
- UI is not cut over; `/__preview/related-quality/` compares old vs new results.

### L2 — static semantic cutover

- Static HTML/JSON uses the semantic related graph only after golden gates pass.
- Page views still do not call LLMs, embedding models or pgvector.
- Missing semantic artifact falls back to sparse baseline with
  `fallback_used=true`; it must not be labeled semantic.

### L3 — static golden-facet personalization

- Browser local profile chooses precomputed golden-interest facet manifests from
  CDN/same-origin JSON.
- Merge/rerank/filter happens client-side over compact card projections.
- No Supabase read is required for an ordinary page view.

### L4 — pgvector authorized search / semantic canary

- Supabase/Postgres+pgvector is the accepted retrieval layer for explicit authorized search and future semantic related canaries.
- Access is through Supabase Edge Function/RPC, never browser direct table `select`.
- RPC returns compact card snapshots only: no profile vectors, no debug internals, no raw event table.
- Strict quotas, payload caps, RLS/grants tests, cache and fallback are mandatory before production traffic.

### L5 — learned ranker

- CatBoost/LightGBM/two-tower ranker is considered only after enough compact
  exposure/action summaries exist for offline evaluation.

## Embedding model decision

P0 accepted model: **`gemini-embedding-2` with `outputDimensionality=768`**, stored in Supabase pgvector.

Reasons:

- current project quota includes `Gemini Embedding 2` (`100 RPM / 30K TPM / 1K RPD` from the Google AI Studio quota screen);
- 768 dimensions keep pgvector storage/index size small enough for the 500 MB Supabase free-tier budget;
- explicit search creates embeddings only for authenticated user queries, not for ordinary page views;
- event-document backfills of the active/future catalogue are feasible as batch jobs after Smart Update.

Important API contract: Gemini Embedding 2 does not use `taskType`; include the task in the text prompt (`title: ... | text: ...` for documents, `task: search result | query: ...` for queries).

BGE-M3 remains a possible offline comparison lane, but it is no longer the accepted P0 implementation target for this project stage.

## Storage ownership

Use two stores with explicit roles:

- **Builder-owned SQLite artifact / release snapshot**: canonical embedding/job
  manifest and precomputed related rows tied to a source event snapshot. This is
  produced by the static-site/embedding job and versioned by build/release id. It
  is **not** a mutable vector store owned by the Fly web runtime.
- **Supabase/Postgres + pgvector**: future L4 canary online ANN/RPC layer for
  dynamic personalization and candidate refresh. It is not the source of truth
  for events and is not the default page-view read path.

Production release artifacts should include:

- `embedding_manifest.sqlite` or equivalent SQLite tables;
- `event_related.sqlite/json` with 30–40 ranked IDs per anchor when possible;
- related-quality report;
- release manifest with `build_id`, `source_snapshot_id`, `model_version`,
  `pipeline_version`, `text_manifest_version` and git SHA.

## Data model outline

### Builder SQLite tables

```sql
CREATE TABLE event_embedding (
    event_id        INTEGER NOT NULL,
    model_version   TEXT NOT NULL,
    embedding_type  TEXT NOT NULL, -- dense | sparse | colbert | audit
    vector_blob      BLOB NOT NULL, -- float32-packed dense vector for BGE-M3
    text_hash        TEXT NOT NULL,
    input_text       TEXT,
    dimensions       INTEGER NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (event_id, model_version, embedding_type)
);

CREATE TABLE event_related (
    event_id          INTEGER NOT NULL,
    related_event_id  INTEGER NOT NULL,
    slot_type         TEXT NOT NULL, -- pure_related | adjacent_discovery | promo
    score             REAL,
    rank              INTEGER NOT NULL,
    retrieval_method  TEXT NOT NULL, -- semantic_dense | hybrid_semantic | promo
    model_version     TEXT NOT NULL,
    pipeline_version  TEXT NOT NULL,
    computed_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (event_id, related_event_id, pipeline_version)
);

CREATE TABLE embedding_manifest (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    model_version     TEXT NOT NULL,
    pipeline_version  TEXT NOT NULL,
    text_doc_version  TEXT NOT NULL,
    events_total      INTEGER NOT NULL,
    events_embedded   INTEGER NOT NULL,
    events_skipped    INTEGER NOT NULL,
    started_at        TEXT NOT NULL,
    completed_at      TEXT,
    status            TEXT NOT NULL,
    config_json       TEXT,
    quality_report    TEXT
);
```

### Supabase pgvector canary table

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE event_embeddings (
    event_id        BIGINT NOT NULL REFERENCES event_search_documents(event_id),
    embedding_model TEXT NOT NULL,
    embedding_dim   SMALLINT NOT NULL DEFAULT 768,
    embedding       vector(768) NOT NULL,
    text_hash       TEXT NOT NULL,
    embedded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, embedding_model, embedding_dim)
);

CREATE INDEX event_embeddings_embedding_hnsw_idx
ON event_embeddings
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 128);
```

The table is closed to browser roles. Authenticated access is through controlled RPC/Edge Function only; see `authorized-event-search.md` for applied schema and verification evidence.

## Embedding text manifest

Never embed raw HTML, raw JSON or UI boilerplate. Build a deterministic text
manifest and hash it:

```text
Категория: <controlled category>
Тип: <event type>
Название: <title>
Кратко: <clean short summary>
Описание: <clean description, trimmed>
Место: <venue / city>
Темы: <controlled tags/facets>
Условия: <free / registration / ticketed / price range>
```

`text_hash` changes only when this manifest changes. Smart Update/static build
can skip unchanged events.

## Retrieval pipeline

1. **Candidate generation**
   - Dense ANN by BGE-M3 vectors: top 50–100 future/active candidates.
   - Metadata prefilters: not self, not cancelled, not same-occurrence duplicate,
     date eligibility, city/surface constraints.
2. **Pure related**
   - Semantic similarity is the primary score.
   - Business boosts are limited to category/topic/venue/date coherence.
   - Source popularity and global likes must not push an unrelated candidate into
     pure related.
3. **Adjacent discovery / anti-bubble**
   - Separate slots for nearby but not identical interests.
   - Use MMR/diversity to avoid ten events of the same type/venue/source.
4. **Promo**
   - Insert via explicit campaign rules and labels.
   - Promo does not masquerade as organic similarity.
5. **Personalized feed**
   - Build compact session/short/mid/long interest vectors from liked/viewed/saved
     events.
   - Query can blend context event and profile vector.
   - Negative actions exclude/downrank future candidates; they do not move/delete
     the card the user is currently interacting with.

## Product surfaces

- `Похожие события`: only `pure_related`.
- `Смотрите дальше`: may mix `pure_related`, `adjacent_discovery` and labeled
  promo/serendipity.
- `Для меня`: local/profile-aware personalized feed using golden facets first;
  pgvector only after L4 gates.

Recommended chain sizes:

- related-chain artifact: 30–40 ranked IDs per event when enough events exist;
- static HTML: first 6–10 cards;
- JSON top-up: more candidates from the same chain for filtering, “Не
  интересно” replacements and `Показать ещё`.

## Job sequence after Smart Update

1. Smart Update records changed event ids.
2. Static-site embedding job starts after the existing coalescing window.
3. Job loads active/future events from a release/source SQLite snapshot.
4. Job builds text manifests and hashes.
5. Unchanged manifests reuse existing embeddings.
6. Changed/new events are embedded in batch on Kaggle with BGE-M3.
7. Full related graph is recomputed, not only changed anchors: a new event can
   become the best neighbor of older events.
8. Embeddings and related rows are written into the builder-owned SQLite artifact;
   optional pgvector sync is a separate L4 canary step.
9. Static HTML/JSON is generated from the related artifact.
10. Quality gates run before publish; last-good static build remains fallback.

## Quality gates

Golden anchors are mandatory, not examples only:

```yaml
- anchor_event_id: 6447
  must_include:
    - 6310 # Архитектурно-урбанистическая студия
  must_exclude:
    - 5261 # Музыка нашего города
  notes: "Urban-planning intent must beat lexical 'город' overlap."
```

Required gates:

- embedding coverage for active/future events >= 95%;
- all active vectors use the same `model_version`;
- anchor recall@6 >= configured threshold;
- zero hard-negative violations in top 10;
- no deploy if semantic pipeline silently falls back to TF-IDF while still
  claiming semantic output;
- no popularity/like/source-count boost is allowed to create a top-6
  `pure_related` hard-negative;
- every candidate has mandatory `slot_type` and score breakdown.

Static preview must include `/__preview/related-quality/` with:

- anchor-by-anchor top 10;
- score breakdown;
- slot type: pure/discovery/promo;
- model/pipeline versions;
- old lexical baseline vs new semantic result during migration;
- obvious failure flags.

## Migration plan

1. **Terminology cleanup (P0)**
   - Current TF-IDF layer is `event_sparse_related_chain_v1` /
     `local_tfidf_sparse_v1`.
   - Old `event_vector_related_chain_v2` may exist only as compatibility alias
     for reading old artifacts, not in new logs/manifests/debug labels.
2. **Executable sparse gates (P0)**
   - Mandatory `slot_type`.
   - No popularity in `pure_related`.
   - Golden anchor checks for obvious failures.
3. **Semantic infrastructure (P1)**
   - Add builder SQLite embedding tables, BGE-M3 Kaggle notebook/script and
     golden anchor file.
4. **Backfill / shadow mode (P1)**
   - Generate embeddings for existing events.
   - Keep lexical and semantic outputs side by side.
   - Build related-quality preview and review control anchors + random events.
5. **Static cutover (P1/P2)**
   - Switch static `related.json` to semantic pipeline after gates pass.
   - Keep lexical output as rollback artifact.
6. **Golden facets and pgvector canary (P2+)**
   - Enable static facet manifests first.
   - Enable pgvector RPC only after RLS/grants/rate/payload/fallback gates.

## Forbidden patterns

- Calling TF-IDF/BM25/cosine over lexical features “semantic vector search”.
- Embedding raw HTML/JSON or UI boilerplate.
- Mixing vectors from different models in one ANN index.
- Updating related only for changed anchors.
- Letting popularity/likes/source counts dominate pure related.
- Browser direct table select from Supabase for recommendations.
- Supabase pgvector RPC as default page-view path before L4 gates.
- LLM/embedding calls on page view or browser hot path.
- Deploying related changes without golden-anchor and preview-quality evidence.
