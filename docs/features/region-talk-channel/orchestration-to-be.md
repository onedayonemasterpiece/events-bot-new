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

If Telegram/VK media resolves to a video/non-image file or an image decoder
cannot open the downloaded payload, ImageDiagnostic must write a terminal
`image_queue_status=not_reviewable_unsupported_media` with
`media_acquisition_status=unsupported_media_or_decode_failed`. Such rows are
excluded from future leases; they must not loop forever as
`needs_actual_image_fetch` and consume the whole image batch.
An identified video is terminal only for the image worker, not for the product
funnel. If its source is confirmed external and strict KO-only fused E5+BGE
text gates pass, the local finalizer includes it as
`media_review_mode=operator_video_review`; Gemini checks text only and the
operator watches the video after the confirmed link reaches the review chat.
Other unsupported media remain terminal product rejects.
The orchestrator reports terminal image counters
`image_not_reviewable_no_media_total`,
`image_not_reviewable_unsupported_media_total` and
`image_rejected_text_gate_total` next to pending/in-progress/actual-scored
counts so non-growth of `image_actual_scored_total` is explainable.

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
- exact latest Candidate/BGE/Image heartbeat rows included in every metric
  snapshot with `run_id`, event, phase, status, timestamp and sequence, so an
  active Kaggle status cannot be attributed to an older YDB run;
  CandidateReport recreates its lightweight YDB heartbeat pool after the heavy
  snapshot/retention write and retries once, so a completed kernel is not left
  displaying `report_write_started` after a transient `ResourceExhausted`;
- LLM budget metrics expose the latest active budget id, its reserved and
  remaining calls, plus a separate historical reserved total. Daily budget
  rows are never summed into a fictitious `remaining` capacity above the
  configured 100-call ceiling;
- processed-post throughput exposes raw YDB rows, canonical unique posts and
  duplicate-identity rows separately, both cumulatively and for the latest
  CandidateReport run. `processed_posts_unique_total` is deduplicated by post
  identity and must not count the online write plus final snapshot twice;
- every lexical/regex KO hit receives one mutually exclusive latest product
  outcome (`source_local`, `source_spam`, stale, vector not-KO/multiregion/ad/
  news/low-substance, dual-vector pending, media outcome, Gemini rejection,
  confirmed or sent). Both cumulative and latest-run reason maps are reported,
  so high scan volume cannot hide real KO movement or its drop-off reason;
- latest-run conversion rates are first-class metrics: heuristic KO hits per
  unique processed post, text accepts/publications per heuristic KO hit,
  KO-bearing sources per scanned source, and fast-check hits per checked
  source. These rates control breadth tuning; raw post volume alone does not;
- `image_queue_total` retained as the transparent raw/audit row count, plus
  `image_product_eligible_total` for rows accepted by the current strict gate;
  `--target-image-queue` follows the eligible delta rather than rejected rows
  retained for audit;
- `publication_candidate_total` likewise remains the raw historical/audit URL
  count, while `publication_active_candidate_total` excludes sent, rejected and
  eligibility tombstone rows; `--target-publication-candidates` follows only
  the active delta;
- a publication tombstone is sent back to the finalizer when its persisted
  authoritative-source fingerprint differs from the current source ledger, so
  newly accumulated scan evidence can promote an earlier `review` decision;
  fingerprint v2 includes substantive scan/KO/candidate counters but excludes
  volatile row-update timestamps, preventing unchanged tombstones from being
  re-finalized on every CandidateReport snapshot;
  existing v1 tombstones are rewritten once with the v2 fingerprint even when
  their eligibility verdict remains unchanged, closing the migration loop;
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
  --target-ko-sources 5 \
  --target-processed-posts 50 \
  --limit 10000
```

Important invariants:

- BGE-M3 is launched immediately when `bge_pending_sample_total >= 1`, using
  the worker's own text-length/PK contract. Raw E5/BGE coverage remains visible,
  but the scorecard also reports actionable coverage and E5 rows excluded below
  `REGION_TALK_BGE_MIN_TEXT_CHARS` (default 24); ultra-short captions must not
  masquerade as a runnable BGE backlog or be hidden from the raw metric.
- This is the current dual-vector normalizer: at most one BGE kernel may run at
  once, but while actionable E5-without-BGE rows remain the loop polls in the
  shorter downstream interval (60 seconds by default) and relaunches BGE after
  the previous kernel completes. CandidateReport continues bounded discovery
  in parallel; the normalizer never drops either model or disables discovery.
  The BGE batch reserves 80% of bounded capacity for exact keyword/fast-check
  posts (fresh-first) and 20% for generic FIFO backlog, filling unused capacity
  from either side. This reduces known-KO latency without starving breadth.
- CandidateReport is still included in the same ready cycle to keep
  discovery/E5 growing in parallel while BGE/Image consume older queues.
- Candidate breadth is runtime-adaptive while the 20-minute notebook guardrail
  remains unchanged. A completed run below 15 minutes and BGE backlog at or
  below one 48-row batch permits eight history and eight fast-check sources;
  15-17.5 minutes or excess BGE backlog uses six; above 17.5 minutes uses five.
  This spends measured headroom on scanning more publics, not on deeper history.
- Main CandidateReport uses a non-aggressive discovery profile by default:
  about 12 source scans per run, 5 similar-channel seeds, up to 5
  recommendations per seed, and a 7-query lexicon-driven Telegram global-search
  slice: 3 travel/toponym keyword phrases plus 4 rotating hashtags from
  `kaliningrad-place-lexicon-v1.csv`. The query bank is the full region
  city/settlement/POI lexicon, but each run consumes only a small human-like
  slice because live YDB/local probes showed broad raw `Калининград` searches
  mostly rediscover local/regional publics and hashtag spam. This keeps
  `publics_total` / source frontier growth visible on every healthy run without
  turning the Telegram session into an aggressive crawler. The orchestrator no-progress signature uses every numeric live metric that it emits, so source, text/vector, image, publication and scan-depth counters are monitored together without manual omissions. History depth metrics (`history_*_post_age_days`) are used to decide whether to lower/raise scan depth for speed versus coverage.
- The default orchestrator runtime window is 20 minutes
  (`REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS=1200`), still below the 30-minute
  product bound. This is intentional: a 12-minute window made the run
  anti-product by skipping keyword discovery and capping E5 to the remaining
  runtime instead of the intended model timeout. The 20-minute window keeps
  fast-check, keyword/hashtag search, similar discovery, E5, source queue and
  image handoff all eligible to run in one cycle.
- Debug/product loops can be goal-driven without hiding metrics. The
  orchestrator still emits and monitors the full numeric funnel, but optional
  delta targets (`--target-new-publics`, `--target-processed-posts`,
  `--target-ko-sources`, `--target-image-queue`,
  `--target-publication-candidates`) add an explicit stop condition against the
  loop baseline. This prevents a bounded test run from being mistaken for the
  long-running product goal while still allowing short, measurable debugging
  sprints.
- Orchestrator metric JSON includes both detailed names and snapshot-compatible
  aliases (`pending_scan`, `source_posts_scanned_sum`, `processed_post_rows`,
  `candidate_memory_rows`, `publication_queue_total`, ...). Audits should use
  the canonical detailed names internally, but operator-facing comparisons may
  use the aliases to avoid “same metric, different name” confusion.
- Goal metrics must not be hidden by debug read windows. Source/frontier rows
  can be bounded with `--limit`, but `processed_post_item` and `post_live_item`
  use independent larger caps so `processed_posts_unique_total` keeps moving
  after the first 6000 rows.
- CandidateReport history fetches are freshness-bounded by
  `REGION_TALK_HISTORY_MAX_POST_AGE_DAYS=365` by default. It should not crawl
  deeper than one year for normal product monitoring. Sources with at least
  `REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD=30` text posts
  on one UTC day are terminally rejected as high-volume/news-like feeds before
  spending more history budget.
- CandidateReport keyword/hashtag discovery is breadth-first. Search hits that
  survive the cheap source-surface filter receive the durable
  `ko_keyword_or_fast_check` priority lane and are selected before ordinary
  backlog without physically rewriting `queue_seq` or `queue_order`. The handoff must persist
  the current run's actual scan/fast-check/keyword evidence; bounded
  handoff is acceptable, losing current-run status rows is not. Obvious local
  Kaliningrad publics are written as
  `rejected_local_region_source` (separate future-monitoring list), and obvious
  hashtag-spam/commercial bait or repeated spoiler-hidden-text sources are
  written as `rejected_spam_source` before Telegram resolve/history calls.
  Normal uncertain sources are not rejected by regex; they stay in the vector
  pipeline.
- Exact KO post URLs are a post-level priority lane, not an exclusive global
  stop-the-world mode. Every CandidateReport run consumes them before history
  scans; when more than three ready links already have cached Telegram private
  entities, the orchestrator raises the paced batch up to eight while keeping
  the uncached username-resolve allowance at one. Lower-probability discovery
  then receives its bounded share in the same run, preserving source growth.
- Keyword/hashtag discovery is higher product priority than similar-channel
  exploration because it can directly produce a source with a concrete
  Kaliningrad Oblast post. In the default orchestrated run, keyword discovery
  therefore runs before similar discovery. Similar discovery may still run, but
  it must not consume the whole discovery tail and cause keyword search to be
  skipped for runtime-budget reasons.
- The full `image_queue_item` product handoff runs before source-queue tail
  handoff. Source-queue persistence is important, but it must not return the
  notebook early before `candidate_memory_item` rows have been checked against
  the strict image/product gate and blocker metrics have been emitted.
- Orchestrated CandidateReport runs set `REGION_TALK_LIGHTWEIGHT_REPORT=1`.
  Complete operational state remains in YDB; the per-run workbook/JSON contains
  only summary, funnel, blocker, candidate/image/publication shortlist and
  observability sheets. This avoids spending several tail minutes repeatedly
  serializing the full multi-thousand-row source ledger. A deliberate offline
  audit can unset the flag to build the full workbook.
  The launcher must serialize this flag into `region_talk_run_config.json`;
  setting it only in the local orchestrator process does not affect Kaggle.
- CandidateReport, BGE-M3 and ImageDiagnostic share one Kaggle input-dataset
  readiness contract. It accepts both Kaggle SDK file objects and the mapping
  rows returned by the project wrapper; a ready dataset must not consume its
  whole launch timeout merely because the wrapper normalized files to dicts.
- Pending VK image rows are prefetched by
  `scripts/region_talk_vk_media_prefetch.py` before Kaggle scoring. The future
  server-side orchestrator uses its production VK user token to resolve public
  photo CDN URLs and persists those URLs in the existing `image_queue_item`.
  Local debugging may explicitly use the existing Fly app as a read-only token
  proxy; no VK token is copied out, and only public attachment URLs are written
  to YDB. Later CandidateReport queue refreshes must merge and preserve these
  prefetch fields rather than reconstructing the row without them.
  ImageDiagnostic still downloads and scores the actual bytes itself. Its VK
  branch consumes the prefetched public URL before attempting `wall.getById`;
  the VK API is only fallback, so an IP-bound Kaggle token cannot mask a valid
  server-resolved image.
- Source selection is queue-first. Once the durable YDB source queue exists,
  pending rows after `unified_source_queue` cursor are selected before legacy
  CSV/static seeds, even if the static seed has a lower numeric priority. The
  cursor is monotonic and must not move backward because of historical pending
  gaps or keyword reinserts; gaps are diagnostics, not a reason to rescan old
  seed rows for hours. When YDB contains both canonical cursor rows and retained
  per-run cursor history, loaders and metrics prefer the canonical cursor row
  (`queue_cursor:source` / `queue_cursor:image`) and do not let retained
  per-run history override the active cursor.
- `queue_seq` is the immutable admission identity of a source row. Missing or
  duplicate sequences are repaired only after a complete source-queue read;
  keyword/fast-check priority is stored separately and does not renumber the
  whole tail. `queue_order` remains a legacy cursor/display field during the
  migration. Manual, keyword, hashtag and similar arrivals all use the same
  canonical ledger and are reported as separate inflow cohorts. The one-time
  full-ledger repair is written with YDB `BulkUpsert` in bounded 500-row chunks
  (`REGION_TALK_SOURCE_QUEUE_REPAIR_BULK_CHUNK_SIZE`), not one YQL execution per
  row; otherwise a 7k-row migration can consume the entire notebook budget.
- Exact post-link fetch is the first **bounded** intake phase of every normal
  CandidateReport run (`REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST=1`, three rows
  by default). It scans up to `REGION_TALK_POST_LINK_QUEUE_SCAN_LIMIT=5000`
  durable rows before selecting the bounded batch, so a blocked PK prefix
  cannot starve later ready work. Terminal, cooldown and entity-wait rows are excluded from the
  actionable head; ready rows are ordered cached-entity first, newest first and
  then by attempt count. This phase does not replace discovery: the same run
  continues source history, fast-check, keyword/hashtag and similar discovery.
- The orchestrated source-selection profile is YDB-queue-only when durable queue
  rows are available. Static CSV seeds are fallback/bootstrap data, not a source
  of repeated scans once the live queue exists.
- `candidate_memory_item` is an audit/history layer, not the image-publication
  queue itself. CandidateReport may keep rows there for BGE wait, weak media,
  local-source diagnostics or later refetch, but `image_queue_item` is now a
  strict product handoff:
  - `vector_gate_status=vector_accept_candidate`;
  - `text_vector_fusion_status=fused_e5_bge_m3` when external BGE is required;
  - `kaliningrad_oblast_only_scope=true` and Kaliningrad/KO is the main subject;
  - no external/multi-region geo evidence;
  - post is not ad/promo;
  - source is not Kaliningrad-local, official/government/news/afisha,
    travel-deal/promo or hashtag-spam/commercial bait;
  - actual media evidence is present. Candidate-memory rows can recover media
    evidence from durable `processed_post_item`/post-live rows before deciding
    whether to enqueue, so compact candidate-memory records no longer lose
    `has_media`, `media_count`, `primary_media_path` or `image_status`.
  The image handoff emits explicit blocker counters
  (`image_queue_blocked_local_source_before_image_total`,
  `image_queue_blocked_official_or_promo_source_before_image_total`,
  `image_queue_blocked_post_ad_or_promo_before_image_total`,
  `image_queue_blocked_not_vector_accept_before_image_total`,
  `image_queue_blocked_missing_fusion_before_image_total`,
  `image_queue_blocked_no_media_before_image_total`) next to
  `image_queue_product_eligible_total`. These counters are required for
  reflection: if image/publication metrics do not move, the run must show
  whether the blocker is source quality, vector/BGE lag, missing media, or true
  lack of eligible posts.
- Every admitted image row carries a versioned pre-image attestation from the
  shared `publication_eligibility()` contract. Only `decision=accept` with
  `gate_version=region_talk_publication_eligibility_v1` may be leased by
  ImageDiagnostic. Unknown sources become `needs_source_review`; local/spam
  sources are rejected. The finalizer joins the authoritative source ledger,
  reapplies the same contract to actual-image rows, refreshes legacy unsigned
  rows. Publication rows also carry a fingerprint of the authoritative source
  classification; the notifier rereads the live source ledger and sends only
  when that fingerprint still matches, so a channel reclassified as local or
  spam cannot leak through a stale Gemini acceptance.
- ImageDiagnostic treats a missing/failed Telethon media fetch as retry/terminal
  evidence. Public `t.me/s` HTML recovery is disabled by default
  (`REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK=0`) and is only an explicit
  diagnostic opt-in, so it cannot mask Telegram cooldowns in the orchestrator.
- CandidateReport source-local preflight search is the prioritization bridge
  between broad source discovery and expensive history scans. After a source is
  added to YDB and passes cheap local/spam title filters, a bounded in-channel
  search should query the lexicon bank (`Калининград`, `Куршская коса`,
  `Балтийск`, `Черняховск`, `Рыбная деревня`, `Виштынецкое озеро`, ...), stop on
  a fresh hit within 365 days, then promote the source into the durable priority
  lane and insert the exact post URL into a known-post fetch queue. A stale hit stays
  as evidence but lower priority. This prevents the current failure mode where a
  keyword/global-search post is only source context and the exact post can be
  lost until a later deep scan.
- Exact post-link queue hygiene is source-level: if the queued link belongs to
  an obvious Kaliningrad-local public or hashtag-spam/commercial source, the
  `post_link_queue_item` becomes terminal `terminal_source_rejected` before
  Telethon fetch. This keeps local-source evidence for future monitoring but
  prevents local channels such as regional afisha/news publics from consuming
  the scarce external-publication exact-post budget.
- During FloodWait recovery the default orchestrated CandidateReport remains
  cached-entity-first. It may spend at most one explicit exact-post entity
  warmup resolve per run (`REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN=1`,
  bounded by `REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN=1`) and only after
  the shared cooldown gate says the method is available. Fast-check/history
  select cached `channel_id/access_hash` rows first and do not silently bypass
  cooldowns. Rows that cannot be resolved in cached-entity-only mode are treated
  as an access-level queue attempt (`skipped_cached_entity_only_no_private_entity`)
  so they stop starving the primary backlog until a separate resolve/cache lane
  can enrich them.
- CandidateReport child env is forced to live YDB, E5-only main embedding and
  external BGE-M3 fusion (`REGION_TALK_STATE_BACKEND=ydb`,
  `REGION_TALK_TEXT_EMBEDDING_MODEL_IDS=intfloat/multilingual-e5-base`,
  `REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE=1`). Source/text-vector
  The canonical source queue is read with a 20k full-read safety window (and
  refuses queue-sequence repair when that read is truncated). The orchestrator
  must keep `REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF=0` for
  production runs: live evidence showed that exiting right after source-queue
  handoff also skips the post-fusion `build_image_candidate_queue(...)`, so
  BGE-promoted candidate-memory rows never reach ImageDiagnostic.
- `RegionTalkBgeM3Enrichment` keeps row count and in-memory batch size separate.
  The orchestrator default is `--batch-limit 48 --batch-size 4` after the live
  YDB backlog showed E5 production outpacing 24-row BGE batches; operators can
  tune this with `REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT` and
  `REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE` without code changes. Production launches set
  `REGION_TALK_BGE_E5_ONLY=1`,
  `REGION_TALK_BGE_INPUT_KINDS=text_vector_enrichment_item` and
  `REGION_TALK_BGE_YDB_SCAN_LIMIT=6000`: BGE consumes E5 rows only, preserves
  the paired E5 text hash, and never re-embeds BGE rows. The scan limit is
  intentionally larger than the batch limit because legacy BGE rows can sort
  before E5 rows in YDB.
  The orchestrator launches this worker from `bge_pending_sample_total`, which
  is computed by the worker's own `collect_text_rows()` contract. A legacy
  E5/BGE pair-gap metric alone is not actionable when the BGE PK already exists,
  so it must not trigger repeated zero-row CPU notebook runs.
- If a Kaggle kernel is already active, that action is skipped but other
  non-conflicting resources continue (for example active BGE does not block
  ImageDiagnostic or CandidateReport).
- The child launchers remain the authoritative session-safety gate. If the
  orchestrator's read-only Kaggle status snapshot says a kernel is free, but the
  launcher immediately sees a queued/running conflicting Region Talk kernel and
  refuses the launch, the orchestrator records that execution as
  `skipped_active_kernel_race` rather than a hard failure. This keeps the
  long-running loop stable without weakening Telegram auth-bundle isolation.
- Notifier/finalizer are local maintenance actions and may run while Kaggle
  notebooks are active; newly Gemini-confirmed unsent rows are notified first.
  CandidateReport's Gemini modes are disabled in the orchestrated profile, so
  the local finalizer is the single verifier owner. It uses the shared Supabase
  limiter plus a durable daily/debug budget clamped to 100 requests.
- A strong actual-image finalist blocked only by sparse source evidence causes
  a marker on the existing source row. The next CandidateReport run selects
  this one bounded source-attestation scan before ordinary backlog; discovery,
  keyword, fast-check and similar-channel intake remain enabled. Product
  priority is evaluated before cache preference, but uncached Telegram work is
  still limited to one controlled resolve lane with the existing human-like
  delays; ordinary cached sources retain preference inside the same lane.
  If Gemini later rejects/defers that post terminally or eligibility is
  revoked, finalizer clears the marker and the backlog metric excludes it;
  terminal non-candidates must not trigger repeated attestation scans.

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

  if strong_finalist_source_evidence_backlog > 0:
      mark its existing source row for bounded attestation completion

  if strict_finalizer_pending > 0 and durable_llm_budget_remaining > 0:
      run local finalizer (max three new Gemini requests per cycle)

  after completions:
      run stats notifier
      send newly Gemini-confirmed links with canonical URL/chat delivery key
      and deterministic Telegram random_id
      stop if no-progress cycles or operator limits are reached
```

The local orchestrator/notifier depends on the Python `ydb[yc]` package. On Debian/Ubuntu hosts with PEP-668 externally managed Python, run it from a small virtualenv (for example under `artifacts/codex/region-talk-ydb-venv/`) instead of relying on system-wide auto-install.

Suggested stop/trigger counters:

- `new_publics_discovered`;
- `sources_processed`;
- `publics_total`;
- `publics_backlog_after_cursor_total`;
- `publics_unscanned_after_cursor_total`;
- `publics_scanned_or_rejected_before_cursor_total`;
- `posts_fetched`;
- `source_scan_posts_per_scanned_public_avg`;
- `source_latest_scan_run_sources_total`;
- `source_latest_scan_run_posts_total`;
- `source_latest_scan_run_posts_per_source_avg`;
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
`publics_keyword_ko_yield_percent`; the same block also exposes
`publics_keyword_queue_rows_total`, `publics_keyword_edge_targets_total` and
`publics_keyword_queue_missing_total` so keyword hits that were already present
in the frontier cannot disappear from monitoring. Keyword-discovered rows,
including existing pending frontier rows, historical `source_edge_item` /
`source_candidate_item` keyword evidence, context-only rows with just
`canonical_source_key`+handle, and fake legacy `processed_*` rows without scan
evidence, receive the same priority lane so the next scan actually tests the
channels where keyword search saw a Kaliningrad hit. Source queue cursor calculation treats the cursor as the point
before the next primary `pending_scan` gap, not simply the largest processed
order; otherwise a later processed row could hide unscanned keyword rows behind
the cursor. The source selector also prioritizes keyword-evidence rows even when
legacy queue drift placed them before the stored cursor. The orchestrator also
reports keyword-sourced post diagnostics:
`publics_keyword_post_rows_with_text_total`, `publics_keyword_regex_ko_raw_posts_total`,
`publics_keyword_regex_ko_filtered_posts_total`,
`publics_keyword_vector_ko_candidate_posts_total`, source-level regex hit counts,
and regex/vector deltas. If keyword-discovered channels do not show a high KO
yield after real scans, or regex KO hits materially exceed vector KO candidates,
the keyword query/frontier insertion or vector prototype tuning needs review.
CandidateReport also emits explicit business heartbeat summary events
`keyword_discovery_done` and `similar_discovery_done`, so a run is not judged
only by "stage started" messages. The orchestrator reports latest-run discovery
metrics (`publics_keyword_latest_run_*`, `publics_similar_latest_run_*`) in
addition to cumulative evidence metrics. Similar-channel discovery is not
product-throttled just because it grows the frontier: breadth is useful, but it
must be measured separately from KO/publication funnel conversion.

The compatibility metric `publics_keyword_with_ko_candidates_total` is broad:
it may include a source with only preliminary `candidate_posts_found`. Product
reporting must instead show
`keyword_sources_with_preliminary_candidates_total`,
`keyword_sources_with_confirmed_ko_posts_total` and
`keyword_external_sources_with_confirmed_ko_posts_total`. Fast-check is labelled
as a keyword-match stage and reports exact-post conversion through processed,
dual-vectorized, strict-text-accepted, image/video, publication, Gemini and sent
stages. All post-stage counters use unique normalized post URLs and the latest
durable row, so repeated upserts cannot inflate conversion.

Fast-check KO is a preflight prioritizer, not a terminal no-KO classifier.
`fast_check_status=no_hit` means "the small source-local query budget did not
find a fresh KO hit"; the source remains pending/lower-priority for normal scan.
Only explicit local-region or spam surface filters become terminal rejections at
this stage.

Local/nonlocal source classification must use both surface signals and scanned
post profile. A source such as `Дом китобоя` / `domkitoboya` is a local
Kaliningrad institution even though its title does not contain the city name.
After a source scan, repeated museum/exhibition/address/local-institution
vocabulary in the post sample is terminal local-source evidence for the
external-publication funnel. Such sources are routed to the future local-region
monitoring list and blocked before image diagnostics/final Gemini publication
verification.

Kaliningrad-only POI institutions are also local source surfaces even when the
city name is absent. For example `Музей Мирового Океана` /
`world_ocean_museum` must be terminally routed to the local monitoring list;
its own posts are not external visitor evidence. A live profile found 15 KO
posts in 16 scanned posts and otherwise wasted dual-vector fusion on 14 memory
rows (6 accepts, 8 rejects), while the downstream image gate correctly kept
them out. The source-level classification must prevent that work earlier.

For live YDB runs CandidateReport does not rewrite the entire source queue on
every handoff. `REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS` defaults to 500 and
the orchestrator currently sets it to 80 with
`REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL=0`. The bounded payload
keeps changed/current-run rows, keyword-evidence rows, the forward cursor
neighbourhood and pending/retry backlog. Current-run scan rows
(`last_scan_run_id`), fast-check rows and keyword-evidence rows are mandatory in
the handoff. Keyword priority never creates a tail shift, preventing one search
hit from turning a short run into thousands of transactional YDB upserts. If the mandatory set itself exceeds the configured handoff cap, it
is still capped with current-run scan evidence first, then fast-check, then
keyword evidence, then generic status changes. This keeps the 30-minute run
contract realistic without
hiding queue size: `source_queue_total` stays the full queue count, while
`source_queue_handoff_rows` reports how many rows were actually written to
`source_queue_item`; the duplicate `source_status_item` mirror is off by default
in orchestrated runs.

For live orchestrated runs those independent stable-key queue rows use YDB
`BulkUpsert` (`REGION_TALK_YDB_ONLINE_QUEUE_BULK_UPSERT=1`). They are not one
atomic business transition, so compiling and executing one YQL statement per
row only wastes CandidateReport runtime. A profiled run spent 64.3 seconds on
80 source rows and then failed its final state snapshot with `DeadlineExceed`.
Bulk queue handoff removes that serial transaction amplification. Retention and
legacy-payload cleanup are disabled in this latency-sensitive path and belong
to a separate maintenance operation.

The CandidateReport scoring budget is workload-aware. Source count alone is
insufficient because exact-post, keyword and history lanes can produce more
than forty posts from five selected sources. Before E5 starts, the notebook
reserves `REGION_TALK_RUNTIME_FIXED_TAIL_SECONDS` (300 seconds in the
orchestrator) for queue assembly and durable YDB writes, estimates scoring cost
with `REGION_TALK_RUNTIME_SECONDS_PER_SCORED_POST`, and caps this run's scoring
pool. The existing KO-evidence-first sorting is applied before the cap, so
deferred low-priority posts do not displace keyword/fast-check evidence. The
`runtime_scoring_budget_applied` heartbeat exposes all inputs and the resulting
limit; `source_queue_build_done.source_queue_handoff_write_seconds` exposes the
actual queue-write cost.

If the latest Candidate heartbeat stops in `state_write_started` or
`report_write_started` without a terminal runtime, the next orchestrator cycle
must treat it as a near-limit failure and select the conservative five-source
budget. A missing runtime is not interpreted as unlimited headroom.

Transient YDB failures (`ConnectionLost`, deadline/timeout) close the cached
driver and retry on the next write, but must not disable all online writes for
the rest of the notebook. Only authentication/authorization failures disable
online writes by default (`REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR=1`);
transient-error disablement is an explicit opt-in
(`REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_TRANSIENT_ERROR=1`).

Source-level monitoring totals must not rely only on the latest compact source
row, because a historical partial run can leave stale/lower source counters.
The orchestrator therefore reports repaired totals plus raw source-row and
repair-delta counters: `publics_scanned_with_posts_*`,
`publics_with_ko_candidates_*` and `source_queue_posts_scanned_*`. The repaired
`source_queue_posts_scanned_total` is at least `processed_posts_unique_total`,
and `publics_with_ko_candidates_total` is at least the number of sources
observed in text/image/publication candidate rows.
Rows marked `processed_*` without scan evidence (`posts_scanned`,
history-fetch timestamp or durable cursor evidence) are treated as fake
processed state, not real history scans. CandidateReport returns them to
`pending_scan`, and the orchestrator exposes
`publics_fake_processed_without_scan_evidence_total` plus
`publics_keyword_fake_processed_without_scan_evidence_total` so legacy queue
drift cannot make keyword yield look scanned when no posts were actually read.
Live progress rows such as `source_selected_for_run` are observability rows only:
they must write `online_source_item` / `source_status_item`, but must not
overwrite durable `source_queue_item` without `queue_order` and queue status.
When YDB source status rows are merged back, empty live fields do not clobber
non-empty durable queue fields.
