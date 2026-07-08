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
- writing E5 `text_vector_enrichment_item` rows with compact text/hash so the
  BGE worker can enrich the same post without Telegram or loading E5;
- consuming BGE-M3 rows written by the external worker from
  `text_vector_enrichment_item`;
- fusion/scoring after both E5 and BGE are present; if BGE is required but
  missing, CandidateReport keeps the row as
  `dual_model_vector_enrichment_pending`/`text_candidate_pending_bge_m3` and
  does not enqueue it for images;
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

- read compact text rows from YDB (`text_vector_enrichment_item` E5 rows first,
  then `publication_candidate_item`, `candidate_memory_item`,
  `image_queue_item`, `processed_post_item`, `post_live_item`) or future
  explicit BGE queue rows;
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

A plain Python process, later inside `eventsbot`, controls short Kaggle launches
by reading YDB queue counts and Kaggle kernel status. It is not a Kaggle
notebook. The implementation is `scripts/region_talk_orchestrator.py`: default
mode is read-only/dry-run JSON (`metrics` + `actions`); `--execute` runs the
first safe action, while the production/debug loop uses `--execute-ready` to run
all non-conflicting ready work in one cycle.

The orchestrator must reuse the same durable control contour as CherryFlash,
Telegram Monitoring and Guide monitoring:

- `video_announce.kaggle_client.KaggleClient` (or read-only official Kaggle API
  fallback only for status polling) for kernel status/push control;
- `kaggle_registry.register_job` / `update_job_meta` for local recovery metadata;
- active-kernel checks before launches, with action resources:
  `telegram:DISCOVERY1`, `telegram:DISCOVERY2`, `kaggle:bge_m3`,
  `local:gemini`;
- row-level YDB queue metrics read through primary-key prefix ranges, not
  table-wide `kind` scans, so `ResourceExhausted` in one queue does not stall the
  whole loop;
- one Python runtime/venv for the orchestrator and child launchers, so local
  preflight dependencies (`ydb`, `openpyxl`, `kaggle`, `telethon`) are stable.

Server/production runs should pass explicit `REGION_TALK_YDB_ENDPOINT`,
`REGION_TALK_YDB_DATABASE` and a least-privilege token/service-account secret.
Local debugging may add `--allow-yc-fallback` to let
`/home/dev/yandex-cloud/bin/yc` discover the YDB endpoint and mint a short-lived
IAM token; this is deliberately opt-in so unattended runs do not open browser
auth.

Current supervised command shape:

```bash
artifacts/codex/region-talk-ydb-venv/bin/python scripts/region_talk_orchestrator.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --allow-yc-fallback \
  --execute-ready \
  --max-actions-per-cycle 4 \
  --limit 10000
```

Important invariants:

- BGE-M3 is launched immediately when either `bge_pending_sample_total >= 1`
  or `text_vector_e5_without_bge_exact_text_total >= 1`; the latter is the
  production invariant for dual coverage and is not hidden by raw legacy BGE
  totals.
- CandidateReport is still included in the same ready cycle to keep
  discovery/E5 growing in parallel while BGE/Image consume older queues.
- Main CandidateReport uses a non-aggressive discovery profile by default:
  about 12 source scans per run, 5 similar-channel seeds, up to 5
  recommendations per seed, and 2 keyword-discovery queries. This keeps
  `publics_total` / source frontier growth visible on every healthy run without
  turning the Telegram session into an aggressive crawler. The orchestrator no-progress signature uses every numeric live metric that it emits, so source, text/vector, image, publication and scan-depth counters are monitored together without manual omissions. History depth metrics (`history_*_post_age_days`) are used to decide whether to lower/raise scan depth for speed versus coverage.
- CandidateReport history fetches are freshness-bounded by
  `REGION_TALK_HISTORY_MAX_POST_AGE_DAYS=365` by default. It should not crawl
  deeper than one year for normal product monitoring. Sources with at least
  `REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD=30` text posts
  on one UTC day are terminally rejected as high-volume/news-like feeds before
  spending more history budget.
- CandidateReport child env is forced to live YDB, E5-only main embedding and
  external BGE-M3 fusion (`REGION_TALK_STATE_BACKEND=ydb`,
  `REGION_TALK_TEXT_EMBEDDING_MODEL_IDS=intfloat/multilingual-e5-base`,
  `REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE=1`). Source/text-vector
  YDB read windows are 6000 rows so queue counters are not rebuilt from a
  truncated source state once the frontier exceeds 1500 rows.
- `RegionTalkBgeM3Enrichment` uses `--batch-limit 12 --batch-size 4` so the row
  count and in-memory batch size are separate. Production launches set
  `REGION_TALK_BGE_E5_ONLY=1`,
  `REGION_TALK_BGE_INPUT_KINDS=text_vector_enrichment_item` and
  `REGION_TALK_BGE_YDB_SCAN_LIMIT=6000`: BGE consumes E5 rows only, preserves
  the paired E5 text hash, and never re-embeds BGE rows. The scan limit is
  intentionally larger than the batch limit because legacy BGE rows can sort
  before E5 rows in YDB.
- If a Kaggle kernel is already active, that action is skipped but other
  non-conflicting resources continue (for example active BGE does not block
  ImageDiagnostic or CandidateReport).
- Notifier/finalizer are local maintenance actions and may run while Kaggle
  notebooks are active; newly Gemini-confirmed unsent rows are notified first.

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

The local orchestrator/notifier depends on the Python `ydb[yc]` package. On Debian/Ubuntu hosts with PEP-668 externally managed Python, run it from a small virtualenv (for example under `artifacts/codex/region-talk-ydb-venv/`) instead of relying on system-wide auto-install.

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

BGE-M3 worker writes BGE rows, while CandidateReport writes E5 rows and consumes
both E5+BGE rows for fused decisions without loading BGE in the main notebook.
Qwen3/Gemma write separate research row kinds until accepted.

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
3. after stable evidence, CandidateReport consumption/fusion was wired around
   `text_vector_enrichment_item`: main defaults to E5-only, BGE rows are fused
   from YDB, and image queue rows require fused E5+BGE when production flag
   `REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE=1` is enabled.

The orchestrator also computes a deterministic regex diagnostic over merged post
rows. It reports `regex_ko_raw_posts_total`, `regex_ko_filtered_posts_total`
(after external-region/multiregion/ad/news/substance filters),
`vector_ko_candidate_posts_total`, `regex_filtered_without_vector_posts_total`
and `vector_without_regex_filtered_posts_total`. These regex numbers are a
monitoring comparator only: if filtered regex KO volume is materially higher
than vector KO volume, the vector gates/prototypes need review; regexes do not
accept/reject production candidates.
 Keyword-source queue health is monitored separately via
`publics_keyword_discovered_total`, `publics_keyword_scanned_with_posts_total`,
`publics_keyword_with_ko_candidates_total`,
`publics_keyword_pending_after_cursor_total` and
`publics_keyword_ko_yield_percent`; keyword-discovered rows are sorted ahead of
generic product-priority rows after insertion so the next scan actually tests
the channels where keyword search saw a Kaliningrad hit. If keyword-discovered
channels do not show a high KO yield after scanning, the keyword query/frontier
insertion logic needs review.
