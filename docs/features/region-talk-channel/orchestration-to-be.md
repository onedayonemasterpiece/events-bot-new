# Region Talk To-Be orchestration and vector queues

Status: target architecture for the 20-link product goal after the July 2026 Kaggle memory findings. This document is the canonical plan for the queue-driven runner/orchestrator shape; it does not replace the existing source, post, image and publication criteria.

## Why the main notebook should not be split into hard modes

The normal `RegionTalkCandidateReport` run should remain queue-driven and opportunistic, not a set of mutually exclusive manual modes. Except for the expensive Telegram discovery step, each launch can consume whatever YDB queues are already ready:

1. consume BGE-M3 enrichment already written by the external worker;
2. fuse E5+BGE text/vector evidence for rows that now have both sides;
3. apply the full source/text/product gates and enqueue only verified text candidates to `image_queue_item`;
4. consume actual-image scores already written by `RegionTalkImageDiagnostic`;
5. build/update `publication_candidate_item`, call Gemini Lite only for image-ready finalists within the shared limiter, and export/send the operator shortlist;
6. only then spend remaining runtime on source/post discovery and new E5 scoring/enqueueing for BGE.

This keeps every launch useful in several directions while still bounding one Kaggle run to about 20-30 minutes. Explicit modes remain useful only for probes, recovery and tests (`vector_probe_only`, BGE-only batch validation, no-discovery maintenance), not as the default product workflow.

## Notebook/process topology

### 1. `RegionTalkCandidateReport` — main queue consumer/producer

Uses Telegram only for source/post discovery and, when needed, public post refetch. Default auth role: `TELEGRAM_AUTH_BUNDLE_DISCOVERY1` (implemented default).

Responsibilities:

- source discovery and source-status decisions;
- bounded post discovery with human-like pacing;
- E5 text vectorization in the main run, because earlier E5-only runs were stable enough;
- writing `text_vector_enrichment_item` rows for E5 and `bge_m3_pending`/queue markers for BGE;
- consuming BGE-M3 rows written by the external worker;
- fusion/scoring after both E5 and BGE are present;
- full source/text gates before image scoring:
  - main subject is Kaliningrad Oblast;
  - not multi-region/other-region;
  - not ad/promo/tour/announcement/news/trash;
  - source is nonlocal blogger/travel/media/personal, while pure Kaliningrad-local sources are kept in a separate future-monitoring list;
  - has enough story/emotion/useful route/visit-impression value;
- writing `candidate_memory_item` and text-confirmed `image_queue_item`;
- consuming actual-image scores and building ranked `publication_candidate_item` rows;
- Gemini Lite final verifier under Supabase `google_ai` reserve/limiter, max 100 calls for the 20-link goal;
- lightweight XLSX/CSV shortlist export.

### 2. `RegionTalkBgeM3Enrichment` — clean BGE-M3 vectorization worker

Uses no Telegram session and no image/LLM code. It may run in parallel with the main notebook if Kaggle account capacity allows.

Responsibilities:

- read compact text rows from YDB (`publication_candidate_item`, `candidate_memory_item`, `image_queue_item`, `processed_post_item`, `post_live_item`) or future explicit BGE queue rows;
- compute only BGE-M3 dense vectors and BGE-derived scores;
- write `text_vector_enrichment_item` rows with:
  - `model_id=BAAI/bge-m3`;
  - `encoder_contract=bge_m3_flagembedding_dense_v1`;
  - semantic-bank scores;
  - KO vs external geo-bank scores;
  - optional dense vector for downstream diversity/history search;
- write run evidence as `bge_m3_enrichment_result:<run_id>` and `bge_m3_enrichment_result:latest`.

The first implementation is intentionally a probe/worker, not a full fusion engine. Fusion remains in the main CandidateReport because it owns the product gates and publication context.

### 3. `RegionTalkQwen3Embedding06BEnrichment` — research-only embedding probes

Uses no Telegram session and no image/LLM code. This worker is **not** part of
production fusion until the quality comparison says it should be.

Responsibilities:

- read the same compact live-YDB text rows as the BGE worker;
- load a research embedding model through `sentence-transformers`
  (`Qwen/Qwen3-Embedding-0.6B` by default, `google/embeddinggemma-300m` in
  `--model-size embeddinggemma` CPU probes);
- compute the same semantic-bank and KO-vs-external geo-bank scores so the
  comparison is apples-to-apples against BGE;
- write research rows such as `qwen3_embedding_0_6b_enrichment_item` or
  `embeddinggemma_300m_enrichment_item`, not as production
  `text_vector_enrichment_item`;
- write matching run evidence as `<model_short>_enrichment_result:<run_id>` and
  `:latest`.

Quality comparison is a separate research gate. It joins BGE and research-model
rows by `post_url`/`post_id+text_hash`, overlays confident YDB labels
(`publication_candidate_item`, `candidate_memory_item`, `image_queue_item`) and
reports agreement, margin deltas and disagreements. Only if a research model
shows equal or better retrieval quality on enough confirmed/rejected rows should
it become a third vector worker or replace BGE. The 2026-07-08 CPU probe kept
Qwen 4B/8B out of the CPU plan and kept EmbeddingGemma-300M as research-only.

### 4. `RegionTalkImageDiagnostic` — actual image scorer

Uses a separate Telegram auth role when it has to download Telegram media. Target role: `TELEGRAM_AUTH_BUNDLE_DISCOVERY2`. It may run in parallel with CandidateReport only when it does not share the same Telegram auth key.

Responsibilities stay unchanged:

- lease `image_queue_item` rows that already passed text/source/vector gates;
- fetch actual images;
- compute postcardness/aesthetic/technical/safety scores;
- write `image_queue_status=actual_scored` with `image_model_input_type=actual_image`.

### 5. Local/server orchestrator

A plain Python process, later inside `eventsbot`, controls short Kaggle launches by reading YDB queue counts and Kaggle kernel status. It is not a Kaggle notebook.

Pseudo-loop:

```text
while confirmed_sent < 20 and llm_calls_used < 100 and progress_budget_ok:
  read YDB queue metrics + Kaggle active kernels

  if bge_pending >= threshold and bge_kernel_free:
      notify operator chat: BGE-M3 batch started
      launch RegionTalkBgeM3Enrichment (no Telegram session)

  if research_qwen_probe_enabled and qwen_pending >= threshold and qwen_kernel_free:
      notify operator chat: Qwen3 research batch started
      launch RegionTalkQwen3Embedding06BEnrichment (no Telegram session)

  if image_queue_pending >= threshold and image_kernel_free and DISCOVERY2 available:
      notify operator chat: image scoring started
      launch RegionTalkImageDiagnostic

  if main_kernel_free and DISCOVERY1 available:
      notify operator chat: CandidateReport queue/discovery run started
      launch CandidateReport with <=30 min runtime

  after completions:
      run stats notifier
      send newly Gemini-confirmed links
      stop if no-progress cycles or operator limits are reached
```

Suggested stop/trigger counters:

- `new_publics_discovered`;
- `sources_processed`;
- `posts_fetched`;
- `posts_e5_scored`;
- `bge_pending` / `bge_scored` / `bge_failed_retryable`;
- `fusion_passed_text_gate`;
- `image_pending` / `image_actual_scored` / `strong_images`;
- `publication_ready`;
- `llm_confirmed`;
- `sent_to_chat`.

## Vector banks and caches

What exists now:

- `semantic_bank_embedding` rows cache finite prototype vectors for the current semantic bank per model/hash.
- These are control/prototype vectors, not post vectors.
- Current compact rows persist only some fused score fields; per-post raw E5/BGE vectors are not durable enough for anti-vector diversity.

To-Be vector rows:

- `text_vector_enrichment_item:<post_id>:<model>:<text_hash>` — one row per post/text/model/encoder contract.
- `vector_bank_embedding_item:<bank_hash>:<model>:<encoder_contract>` — future cache for semantic, KO geo and external geo prototype banks using the exact same encoder contract as the worker.
- `publication_semantic_history_item:<publication_candidate_id|post_id>` — vector reference for already confirmed/sent/published posts.

BGE-M3 worker writes the first row kind now. Qwen3 writes a separate research
row kind until accepted. CandidateReport should later consume both E5 and BGE
rows and write fused decisions without loading BGE.

## Non-region geo discriminator

The non-region problem must not be solved only by literal keyword lists. The To-Be discriminator is vector-based:

- KO geo bank: Kaliningrad Oblast cities, resorts, settlements, landmarks and nature locations from `kaliningrad-place-lexicon-v1.csv` plus curated aliases.
- External Russia geo bank: Russian regions, major cities and common travel destinations outside Kaliningrad Oblast.
- External country geo bank: nearby countries and common foreign travel destinations.

For every text/model store:

- `*_ko_geo_top`, `*_ko_geo_score`;
- `*_external_geo_top`, `*_external_geo_score`;
- `*_ko_vs_external_geo_margin`.

Search/scoring against already stored vectors is math-only and does not require loading E5/BGE. The model is required only when a new text/bank item needs a new embedding.

## True diversity / semantic anti-vector

The current MVP penalty (`same source`, `same place`, `same content type`) is only a safety fallback. The target ranking must use semantic anti-overlap:

1. When a candidate is Gemini-confirmed or sent to the operator chat, write/update `publication_semantic_history_item` with its E5+BGE vector refs. The earlier shorthand “E5 durable vector rows” was incomplete: the anti-vector should be dual-model, and if Qwen3 wins the research gate its vector ref may be added/replaced under the same contract.
2. For each new publication-ready candidate, compute cosine similarity against the confirmed/sent/published history.
3. Prefer candidates with lower max similarity to the already selected history, while keeping minimum quality gates on image/text.
4. Use MMR-style ranking:

```text
rank_score = quality_score - diversity_weight * max_similarity_to_history
```

This creates a real semantic “anti-vector”: from strong candidates, choose the one farthest in meaning from what was already selected/published.

## Guardrails that must not be dropped

No row enters image scoring/final Gemini just because it has BGE vectors. The full gate remains:

```text
source/post discovery
  -> E5 text score + BGE-M3 enrichment
  -> fused KO-only / non-region / non-ad / non-news / substance / source-class gate
  -> image_queue_item
  -> actual-image scoring
  -> publication base gate
  -> semantic anti-vector ranking
  -> Gemini Lite final verifier
  -> operator chat / lightweight XLSX
```

Rows that are text-only, local-regional, ads/tours, news, multi-region roundups, other-region travel, low substance, or metadata-only-image rows must stay out of final publication candidates.

## Current validation hypothesis

The architecture depends on BGE-M3 being stable when isolated in its own notebook. The immediate validation is therefore:

1. run `RegionTalkBgeM3Enrichment` with small real-YDB batches (`8`, `12`, `24` rows);
2. check model load time, batch time, memory/runtime failures, YDB writes and row payload sizes;
3. only after stable evidence, wire CandidateReport consumption/fusion around the new `text_vector_enrichment_item` rows.
