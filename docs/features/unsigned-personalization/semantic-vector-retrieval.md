# Semantic vector retrieval for events

> Status: accepted target architecture after Gemini Pro + Opus consultation on
> 2026-06-28. Implementation is not complete until embeddings are generated,
> indexed, quality-gated and used by static build/RPC paths.

## Why this document exists

The current preview related-chain uses local sparse TF-IDF/cosine matching. That
is useful as a deterministic lexical baseline, but it is **not** semantic vector
search and must not be described as semantic embeddings.

The failure mode is already visible: an event about the future/urban planning of
Kaliningrad can rank a concert named “Музыка нашего города” too high because both
texts contain “город”. A real semantic layer must understand that
“Архитектурно-урбанистическая студия” is much closer.

## Decisions

### Embedding model

Primary: **local BGE-M3 on Kaggle GPU/CPU batch jobs**.

Reasons:

- strong multilingual/Russian quality for short and noisy event texts;
- no request/day ceiling for full backfills and re-embeds;
- reproducible model/versioned outputs;
- offline job, never page hot path.

Fallback candidate: `intfloat/multilingual-e5-large`.

Google/Gemini embeddings are allowed only as an audit/comparison lane unless a
separate budget/limit decision is made. With the current free-tier quota they are
not the production primary for full catalogue backfills.

### Storage

Use two stores with explicit roles:

- **Fly SQLite**: canonical embedding/job manifest and precomputed related rows
  tied to the canonical event catalogue.
- **Supabase/Postgres + pgvector**: online ANN/RPC layer for dynamic
  personalization and candidate refresh, not source of truth for events.

This keeps static generation reproducible and lets the browser use Supabase only
for lightweight personalized reads/writes when enabled.

### Static and dynamic paths

- Static event pages get a precomputed 10-slot discovery block in HTML/JSON.
- Browser personalization never calls an embedding model or LLM.
- Dynamic feed can call Supabase RPC against already-stored vectors/profile
  vectors after JS activation.

## Data model outline

### SQLite canonical tables

```sql
CREATE TABLE event_embedding (
    event_id        INTEGER NOT NULL,
    model_version   TEXT NOT NULL,
    embedding_type  TEXT NOT NULL, -- dense | sparse | colbert | audit
    vector_blob      BLOB NOT NULL, -- float32-packed dense vector for BGE-M3
    text_hash       TEXT NOT NULL,
    input_text      TEXT,
    dimensions      INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
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

### Supabase pgvector live table

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE event_embedding_live (
    event_id       INTEGER PRIMARY KEY,
    model_version  TEXT NOT NULL,
    embedding      vector(1024) NOT NULL,
    text_hash      TEXT NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX event_embedding_live_hnsw_idx
ON event_embedding_live
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 128);
```

For a catalogue of tens of thousands of events, 1024-dimensional float vectors
plus HNSW index are still expected to fit in the personalization DB budget if raw
telemetry is kept compact and old vectors are cleaned by model/version policy.

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

`text_hash` changes only when this manifest changes. Smart Update can then skip
unchanged events.

## Retrieval pipeline

1. **Candidate generation**
   - Dense ANN by BGE-M3 vectors: top 50–100 future/active candidates.
   - Metadata prefilters: not self, not cancelled, not same occurrence duplicate,
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
   - Build a compact user/profile vector from liked/viewed/saved events with
     session/short/mid/long horizons.
   - Query vector can blend context event and profile vector.
   - Negative actions exclude or downrank; they do not delete the current card
     while the user is interacting with it.

Recommended initial 10-slot composition:

- positions 1–6: `pure_related`;
- positions 7–9: `adjacent_discovery`;
- position 10: promo or serendipity.

Exact numbers are configurable and must be validated by quality reports.

## Job sequence after Smart Update

1. Smart Update records changed event ids.
2. Static-site embedding job starts after the existing coalescing window.
3. Job loads active/future events from SQLite.
4. Job builds text manifests and hashes.
5. Unchanged manifests reuse existing embeddings.
6. Changed/new events are embedded in batch on Kaggle with BGE-M3.
7. Full related graph is recomputed, not only changed anchors: a new event can
   become the best neighbor of older events.
8. Embeddings are upserted into SQLite canonical store and synced to Supabase
   pgvector.
9. Static HTML/JSON is generated from `event_related`.
10. Quality gates run before publish; last-good static build remains fallback.

## Quality gates

Create a curated golden dataset in the repository:

```yaml
- anchor_event_id: 6447
  must_include:
    - <Архитектурно-урбанистическая студия event_id>
  must_exclude:
    - <Музыка нашего города event_id>
  notes: "Urban-planning intent must beat lexical 'город' overlap."
```

Required gates:

- embedding coverage for active/future events >= 95%;
- all active vectors use the same `model_version`;
- anchor recall@6 >= configured threshold;
- zero hard-negative violations in top 10;
- no deploy if semantic pipeline silently falls back to TF-IDF while still
  claiming semantic output.

Static preview must include `/__preview/related-quality/` with:

- anchor-by-anchor top 10;
- score breakdown;
- slot type: pure/discovery/promo;
- model/pipeline versions;
- old lexical baseline vs new semantic result during migration;
- obvious failure flags.

## Migration plan

1. **Terminology cleanup**
   - Rename the current TF-IDF layer to lexical/sparse related.
   - Stop using “semantic”, “embedding” or “vector search” for TF-IDF outputs.
2. **Infrastructure**
   - Add SQLite embedding tables, Supabase pgvector table/RPC, BGE-M3 Kaggle
     notebook/script and golden anchor file.
3. **Backfill / shadow mode**
   - Generate embeddings for existing events.
   - Keep both JSONs side by side: legacy lexical and semantic candidate output.
   - Build related-quality preview and review at least control anchors + random
     events.
4. **Static cutover**
   - Switch `related.json` to semantic pipeline after gates pass.
   - Keep legacy lexical output as rollback artifact for a short burn-in window.
5. **Personalization cutover**
   - Enable profile-vector RPC and listing continuation on top of pgvector.
   - Keep static related as fallback if Supabase/RPC is unavailable.

## Forbidden patterns

- Calling TF-IDF/BM25/cosine over lexical features “semantic vector search”.
- Embedding raw HTML/JSON or UI boilerplate.
- Mixing vectors from different models in one ANN index.
- Updating related only for changed anchors.
- Letting popularity/likes/source counts dominate pure related.
- LLM/embedding calls on page view or browser hot path.
- Deploying related changes without golden-anchor and preview-quality evidence.

