# Region Talk To-Be orchestration and vector queues

Status: target architecture for the 20-link product goal after the July 2026 Kaggle memory findings. This document is the canonical plan for the queue-driven runner/orchestrator shape; it does not replace the existing source, post, image and publication criteria.

## Why the main notebook should not be split into hard modes

The normal `RegionTalkCandidateReport` run should remain queue-driven and opportunistic, not a set of mutually exclusive manual modes. Except for the expensive Telegram discovery step, each launch can consume whatever YDB queues are already ready. The product-SLA order is:

1. consume BGE-M3 enrichment already written by the external worker;
2. fuse E5+BGE text/vector evidence for rows that now have both sides;
3. apply the full source/text/product gates and enqueue only verified text candidates to `image_queue_item`;
4. consume actual-image scores already written by `RegionTalkImageDiagnostic`;
5. build/update `publication_candidate_item`, call Gemini Lite only for image-ready finalists within the shared limiter, and export/send the operator shortlist;
6. only then spend remaining runtime on source/post discovery and new E5 scoring/enqueueing for BGE.

When exact-post, fast-check or confirmed-external acquisition has already
produced actionable post bodies, the run protects enough time for the
E5/state/image handoff. This is
`REGION_TALK_DEFER_DISCOVERY_ON_CRITICAL_WORK=1`, but the boundary is adaptive:
one exact post must not stop the high-probability acquisition tail while more
than ten minutes remain. By default acquisition is deferred only after a useful
batch of eight actionable posts has accumulated, or when runtime remaining is
at most 600 seconds. This prevents both failure modes observed live: losing a
proven KO post behind lower-probability work and spending the E5 startup cost on
one post while leaving about 17 minutes unused. Discovery stays enabled and
continues in this run when headroom is healthy, or in the next cycle after a
real critical batch/runtime boundary.

This keeps every launch useful in several directions while still bounding one Kaggle run to about 20-30 minutes. Explicit modes remain useful only for probes, recovery and tests (`vector_probe_only`, BGE-only batch validation, no-discovery maintenance), not as the default product workflow.

The server-side orchestrator must not materialize dense E5/BGE embedding arrays
merely to calculate queue and pairing metrics. It reads the complete vector
ledger through a YDB scalar `JSON_VALUE` projection containing identity,
contract, text-hash, lifecycle and the bounded working-text fields needed to
decide whether BGE work is actionable. Actual dense vectors remain in YDB for
dual-model fusion; this optimization changes only the control-plane read and
does not remove either model or alter scoring.

CandidateReport remains one self-contained Kaggle worker, but its reviewed
Python source has crossed Kaggle's script-source limit of about 1 MB. The
canonical launcher therefore uploads a small deterministic `zlib+b85` wrapper
that expands the exact reviewed worker bytes in memory, with a local 950 KB
safety gate before push. This is transport packaging only: it creates no
additional mutable code dataset and changes no pipeline stage, model or queue.

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

Parallel Telegram sessions are necessary but not sufficient: the two notebooks
also share `image_queue_item` in YDB. Writer ownership is therefore explicit:

- CandidateReport owns creation of a new image row and text/source/publication-
  eligibility status transitions;
- ImageDiagnostic owns leases, media-download fields and actual visual scores;
- CandidateReport writes only new/status-changed image rows and writes each
  such row at most once per run. It must never replay every historical image
  row from its start-of-run snapshot, because that can overwrite an
  `actual_scored` result written concurrently by ImageDiagnostic.
- A current fused E5+BGE text acceptance is allowed to replace a prior soft
  `deferred_text_gate` with `needs_actual_image_fetch` even when the old row has
  metadata-probe diagnostic fields. The merge still preserves every actual
  album/frame score, but it cannot restore the stale pre-BGE queue status or
  eligibility reason and delay media work by another main-notebook cycle.

The final `report_written` business heartbeat is also a durable orchestration
contract. It receives a bounded extra retry budget on transient YDB
`RESOURCE_EXHAUSTED`; ordinary progress heartbeats retain the small default
budget so observability recovery does not create sustained write pressure.
Media-fetch, current-image inference and CLIP/LAION/NIMA model-load boundaries
are durable heartbeats as well; this distinguishes slow CPU/model startup from
a dead image worker without changing the visual consensus.

Responsibilities stay unchanged:

- lease `image_queue_item` rows that already passed text/source/vector gates;
- acquire the bounded complete Telegram `grouped_id` album or every VK photo
  attachment and persist a compact manifest rather than a Kaggle-local path;
- compute per-frame postcardness/aesthetic/technical diagnostics and persist
  compact `image_frame_score_item` rows;
- write the versioned post-level album decision together with
  `image_model_input_type=actual_image`.

The transitional deployed image contract is
`region_talk_image_album_guard_v2`; CandidateReport/finalizer consume
`region_talk_publication_eligibility_v5`. It preserves the unchanged legacy
positive path only for a completely acquired album. Partial acquisition,
missing component output and uncalibrated low legacy scores are non-terminal
`needs_visual_review`/`scoring_retry`, not publication tombstones. The
orchestrator includes pending, retry and old-contract low-score rescore rows in
`image_actionable_work_total`; a queue with only versioned rescore work must
still launch ImageDiagnostic. Source-level exclusion by average raw image
score is disabled and repaired only for the exact former image-quality reason;
local/spam/compliance decisions remain terminal.

The scorecard reports post rows and per-frame work separately:
`image_actual_scored_total`, `image_actual_frames_scored_total`,
`image_legacy_auto_accept_total`, `image_visual_review_pending_total`,
`image_partial_album_acquisition_total`, `image_scoring_retry_total` and
`image_contract_rescore_backlog_total`. The historical `>=0.66`/`>=0.70`
counters remain compatibility diagnostics only and may not be described as a
calibrated strong-image decision. See
[`image-scoring-audit-methodology-v2.md`](../../reference/image-scoring-audit-methodology-v2.md)
for the source-disjoint calibration, shadow and automatic-reject stop/go
criteria.

`image_visual_review_pending_total` and
`image_partial_album_acquisition_total` are immutable-ledger/raw compatibility
counters. Current work is reported separately as
`image_visual_review_active_total` and `image_partial_album_active_total`; the
difference is `image_visual_review_tombstoned_total`. A rejected or delivered
publication must not remain an active image-review backlog merely because its
historical image row is retained. `publication_lifecycle_contradiction_total`
must be zero after finalizer reconciliation.

`needs_visual_review` is not allowed to remain a product dead end for a fully
acquired strict-funnel album. ImageDiagnostic therefore has a bounded
multimodal adjudication lane for rows that already have current fused E5+BGE,
current publication eligibility, a complete album/manifest and all legacy
diagnostic components, but missed only the uncalibrated legacy scalar image
threshold. Gemini receives the actual resized images from the complete album,
not post text or metadata. A consistent `accept` produces the versioned
`vlm_visual_accept` attestation and may proceed to the normal final publication
verifier. `reject`, `review`, provider errors and budget deferrals remain
non-terminal visual-review outcomes during rollout; they do not become
automatic publication tombstones.

This lane is deliberately small: the orchestrator defaults to at most two new
VLM calls per ImageDiagnostic run. It shares the same durable daily Region Talk
budget (hard ceiling 100), Supabase Google-AI limiter and request ledger as the
publication finalizer. Request identity includes normalized post URL, complete
media-manifest hash, album cardinality, prompt/decision contract and model, so
a completed unchanged verdict replays without another paid call while provider
errors remain retryable under the original reservation. The scorecard exposes
`image_vlm_backlog_total`, `image_vlm_completed_total`,
`image_vlm_accept_total`, `image_vlm_reject_nonterminal_total`,
`image_vlm_review_total` and
`image_vlm_error_or_budget_deferred_total` without hiding the legacy counters.
The per-run VLM heartbeat counts unique image work keys, not repeated polls of
the same row. ImageDiagnostic may inspect the complete historical image ledger
to calculate current eligibility, but it must write an already blocked row only
when its material eligibility/status/evidence changed. Audit timestamps and
run-lineage fields alone never trigger a YDB UPSERT; this keeps a two-item image
run bounded instead of rewriting the historical ledger.

A producer gate-version bump is not a quality rejection. Missing/stale
attestations are written as `deferred_refresh`; existing actual-image evidence
is preserved for CandidateReport to re-attest. Runtime heartbeats separate
`publication_eligibility_blocked_count` (current terminal source/text/
compliance blocks) from
`publication_eligibility_refresh_deferred_count` (non-terminal contract
refresh). They also report posts and distinct frames separately.

Publication text restoration is a two-owner handoff, not a polling dead end.
CandidateReport owns the bounded governed Telegram refetch and current E5+BGE
rebuild. As soon as exact full text is again present in current candidate
memory, the orchestrator reports
`publication_text_restore_ready_for_finalizer_total` and schedules the normal
finalizer even if the older publication row still says `awaiting_text_restore`.
Without restored text the finalizer remains suppressed, so the queue cannot hot
loop or bypass Telegram request governance.

The same current-state rule applies to visual review. A historical publication
row may still say `visual_review_pending` after ImageDiagnostic has produced a
current `legacy_auto_accept` or manifest-bound `vlm_visual_accept`. The
orchestrator reports this as
`publication_visual_review_resolved_ready_for_finalizer_total` and immediately
reopens the finalizer; it does not wait for CandidateReport to rewrite the old
publication snapshot and does not treat a stale review label as a terminal
verdict. This transition additionally requires the current candidate-memory
row to remain KO-only, non-advertising, non-multiregion and accepted by fused
E5+BGE. A visual accept alone can never resurrect a newer source/text reject.

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
- a loop cycle retries transient YDB endpoint/session failures up to three
  times with bounded exponential backoff (15/30/60 seconds by default). A
  one-off `Deadline exceeded` or endpoint-discovery outage therefore cannot
  terminate a long-running orchestrator while Kaggle notebooks continue
  unmanaged. Authentication, configuration and code errors remain fail-fast;
  successful snapshots expose `cycle_transient_retries` for audit;
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
- text-vector totals use a full 20k row metric window rather than the former
  shared 6k cap; otherwise adding BGE rows could make the displayed E5 total
  fall even though no E5 data was removed;
- every lexical/regex KO hit receives one mutually exclusive latest product
  outcome (`source_local`, `source_spam`, stale, vector not-KO/multiregion/ad/
  news/low-substance, dual-vector pending, media outcome, Gemini rejection,
  confirmed or sent). Both cumulative and latest-run reason maps are reported,
  so high scan volume cannot hide real KO movement or its drop-off reason;
- latest-run conversion rates are first-class metrics: heuristic KO hits per
  unique processed post, text accepts/publications per heuristic KO hit,
  KO-bearing sources per scanned source, and fast-check hits per checked
  source. These rates control breadth tuning; raw post volume alone does not;
- the operator stats message names every denominator and lifecycle stage in
  plain Russian. In particular, source coverage is a disjoint
  `total / ever scanned / never scanned` population, direct KO rows distinguish
  `text read` from `dual-vector complete` and `text suitable before media`, and
  media/publication totals are explicitly marked as historical ledgers versus
  active backlog. Stable machine metric keys remain available in JSON;
  ever-scanned coverage is derived from durable source/post scan evidence, not
  `total - current pending`, so selecting an old source for rescan cannot make
  historical coverage move backwards. The current primary-pending and
  pending-with-scan-evidence counters remain visible on a separate line;
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
  current fingerprint v3 includes material source classification/policy fields
  but excludes monotonic scan counters and volatile row-update timestamps,
  preventing unchanged tombstones from being re-finalized on every
  CandidateReport snapshot;
  already sent rows never re-enter Gemini or delivery. If their live source
  fingerprint or eligibility-gate version changes, the finalizer performs an
  attestation-only refresh and persists the current verdict with zero LLM calls;
  this keeps current-confirmed metrics consistent without duplicate chat posts;
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

Production uses the server wrapper rather than this local command:

```text
APScheduler (06:20, 13:20, 21:20 Europe/Kaliningrad)
  -> scripts/region_talk_scheduled_runner.py
  -> scripts/region_talk_orchestrator.py --loop --execute-ready
  -> CandidateReport / BGE-M3 / ImageDiagnostic / finalizer / notifier
```

The schedule is gated by `ENABLE_REGION_TALK_SCHEDULED=1`. The wrapper requires
explicit endpoint/database plus `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON`,
Kaggle credentials, separate `TELEGRAM_AUTH_BUNDLE_DISCOVERY1` and
`TELEGRAM_AUTH_BUNDLE_DISCOVERY2`, and the local E2E/notifier session. It never
uses interactive `yc` fallback. One run is bounded to 90 minutes by default,
uses `/data/region_talk_orchestrator.lock`, retains redacted operational output
under `/data/runtime_logs/region_talk/`, and records success/failure/skip in
SQLite `ops_run`. Confirmed candidates continue to go only to the operator chat;
the target Telegram/VK publishing surfaces stay disabled.

Important invariants:

- BGE-M3 is launched immediately when `bge_pending_sample_total >= 1`, using
  the worker's own text-length/PK contract. Raw E5/BGE coverage remains visible,
  but the scorecard also reports actionable coverage and E5 rows excluded below
  `REGION_TALK_BGE_MIN_TEXT_CHARS` (default 24). Generic ultra-short captions
  are excluded, but exact keyword/fast-check posts bypass that optimization:
  direct KO evidence must receive BGE and cannot remain permanently pending.
- CandidateReport and the isolated BGE worker use the same semantic prototype
  bank contract. A BGE PK identifies post/model/text but does not freeze the
  prototype bank; when the stored bank version/hash is stale, the worker
  recomputes and overwrites that same PK instead of counting it as already
  complete. The orchestrator passes existing BGE payloads (not only their PKs)
  through the same contract check and therefore still launches the worker for
  stale-bank rows. This prevents a permanent `wait_bge_existing_e5` loop after
  bank calibration changes.
- This is the current dual-vector normalizer: at most one BGE kernel may run at
  once, but while actionable E5-without-BGE rows remain the loop polls in the
  shorter downstream interval (60 seconds by default) and relaunches BGE after
  the previous kernel completes. CandidateReport continues bounded discovery
  in parallel; the normalizer never drops either model or disables discovery.
  The BGE batch reserves 80% of bounded capacity for exact keyword/fast-check
  posts (fresh-first) and 20% for generic FIFO backlog, filling unused capacity
  from either side. This reduces known-KO latency without starving breadth.
- When BGE arrives after CandidateReport has already marked an exact link
  fetched with `vector_defer_wait_bge_m3`, the next CandidateReport reopens
  only that URL as `bge_ready_rescore`. The default exact budget is five per
  run so fresh links and a few BGE-ready rescoring rows can both move. It stays
  under the normal cached-entity/human-like governor; terminal Gemini/operator
  decisions are never reopened. One exact slot per run is reserved for
  `bge_ready_rescore` (`REGION_TALK_BGE_READY_EXACT_RESCORE_PER_RUN`, default
  `1`) so continuous keyword inflow cannot starve the fusion completion lane.
  The scorecard reports pending-new exact links and BGE-ready-rescore links as
  separate counts; the orchestrator sizes its bounded exact batch from both.
  If complete exact text still exists in active candidate/vector state, up to
  eight BGE-ready rows per run are fused directly from YDB **in addition to**
  the Telegram fetch budget. Only older rows whose working text was already
  compacted consume the single human-like refetch slot. This clears dual-model
  lag faster while reducing Telegram requests and preserving fresh discovery.
  Finalizer backlog metrics ignore fingerprint-only counter changes for posts
  already tombstoned from a source that is still durably local/spam. A new
  eligibility-gate version or an actual source-classification change still
  reopens the row. This prevents terminal local videos from masquerading as
  recurring publication work.
- The default global keyword plan reserves two phrase slots for travel intent
  (`ездили`, `путешествие`, `отзыв`, `маршрут`) and rotates the remaining POI
  slots. This keeps broad geographic recall but raises the share of personal
  travel posts over generic news/official mentions; explicit env query plans
  remain untouched.
- A direct post that passes KO-only fused E5+BGE text checks promotes its source
  into the bounded five-post attestation lane before image handoff. This lets a
  strong exact post reach image/Gemini promptly without declaring an unknown
  source external or weakening local/spam safeguards.
  This attestation lane is ordered ahead of generic known-KO rescans even when
  `REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES=1`; generic rescans must not
  consume all four history slots while a text-passed finalist waits.
  The expensive all-source profile pass stays disabled, but after the finalist
  itself has at least five freshly sampled posts CandidateReport computes and
  persists its source scope. Thus source evidence can actually resolve to
  external/local instead of repeatedly rescanning an `unknown` source.
  A finalist with five historical counters but still-unknown scope remains due
  for one bounded profile-producing fetch; reaching the numeric counter alone
  is not treated as completed attestation.
  If Telegram reaches the one-year history cutoff with fewer than five posts,
  that exhaustive recent sample is sufficient. A sparse trip series is not
  called local merely because 3/4 posts concern KO: one non-KO post plus a
  non-local title confirms an external source; ratio-only local inference
  requires all ten sampled posts to concern KO (title/handle local evidence can
  still reject immediately).
- The dual semantic bank includes the negative meaning “a place in another
  region merely resembles Kaliningrad”. A narrow safety guard also rejects a
  sole Kaliningrad mention whose grammatical role is explicitly comparative
  (`похоже на/напоминает Калининград`), preventing another-region routes from
  reaching media merely because both encoders liked the travel-story form.
- E5 rows carry the durable source terminal decision. The BGE selector excludes
  rows already classified as local-region or spam and reports
  `bge_source_terminal_skipped_sample_total`; this removes wasted CPU work but
  does not change the E5+BGE requirement for eligible external posts.
- The normalizer reports actionable backlog, one-run CPU capacity (currently 48
  rows), capacity load percentage and whether one next BGE launch can drain the
  backlog. Candidate discovery continues in parallel.
- CandidateReport is still included in the same ready cycle to keep
  discovery/E5 growing in parallel while BGE/Image consume older queues.
- An orchestrated ImageDiagnostic launch waits at most 120 seconds for its
  initial live-YDB lease and does not wait after draining an available batch.
  The former 600-second empty poll burned a whole CPU notebook when the single
  pending metric changed before the worker acquired it; later Candidate output
  is picked up by the next short orchestrator cycle instead.
- Candidate breadth is runtime- and backlog-adaptive while the 20-minute
  notebook guardrail remains unchanged. Generic uncertain history stays at
  four sources per run. While at least 12 supported confirmed-external blogger
  sources remain unscanned, the latest CandidateReport completed within 750
  seconds and actionable BGE lag still fits one 48-row CPU batch, the next run
  uses six history slots: up to five for the finite confirmed-blogger cohort and
  one for publication-source attestation or the normal queue. This spends
  measured headroom on scanning more high-probability sources, not on deeper
  history, and automatically returns to four after the cohort drains or either
  runtime/BGE guardrail is hit.
- The pre-researched `region_talk_external_blogger_evidence` registry is not a
  side report. Every eligible confirmed non-local record with a supported
  Telegram/VK identity is normalized and deduplicated into the one canonical
  `source_queue_item` queue with `priority_lane=confirmed_external_blogger`.
  `REGION_TALK_CONFIRMED_BLOGGER_PRIORITY_ENABLED=1` makes this cohort own the
  bounded first-scan slots independently of the generic known-KO rescan flag.
  Unsupported records remain visible in registry coverage metrics instead of
  being falsely counted as queued sources. The 2026-07-17 live baseline was
  196 eligible people/projects, 100 with a supported TG/VK identity, 142
  deduplicated supported source surfaces in the queue, 103 source histories
  scanned and 35 active confirmed-source first scans still pending.
- Source-history work is first-pass idempotent. As long as any Telegram/VK row
  in the one canonical queue lacks durable primary-scan evidence, known-KO and
  confirmed-blogger delta rescans are suppressed globally. A dual-text
  finalist that explicitly needs bounded source-attestation completion is the
  only product exception. Successful live source-status evidence repairs a
  stale pending queue row with its original run lineage; queue reconstruction
  never relabels historical scans as work performed in the current run.
- Orchestrator source metrics use the same evidence boundary. A fast-check or
  exact-post fetch may increase post throughput, but it is reported separately
  as `publics_pending_post_probe_only_total` and cannot increase the count of
  publics whose history was scanned. `publics_with_any_processed_post_total`
  remains available as the broader post-presence metric.
- External-blogger reporting has two explicit grains. The registry block counts
  people/projects (`records`): total, confirmed, review, confirmed-local,
  eligible non-local, supported-TG/VK, unsupported, queued, source-history
  scanned and KO-producing. The execution block counts canonical TG/VK
  sources: total, queued, scanned, KO and active unscanned. A number such as
  `confirmed_external_blogger_pending_total` is therefore only an active source
  backlog; it is not allowed to answer "how many confirmed bloggers remain".
  Physical `pipeline_status=stored_only` is shown as a data-quality fact but is
  not used as the operational lifecycle source of truth.
- Main CandidateReport uses a non-aggressive discovery profile by default:
  about 12 source scans per run, 5 similar-channel seeds, up to 5
  recommendations per seed, and a 7-query lexicon-driven Telegram global-search
  slice: 3 travel/toponym keyword phrases plus 4 rotating hashtags from
  `kaliningrad-place-lexicon-v1.csv`. The query bank is the full region
  city/settlement/POI lexicon, but each run consumes only a small human-like
  slice because live YDB/local probes showed broad raw `Калининград` searches
  mostly rediscover local/regional publics and hashtag spam. This keeps
  `publics_total` / source frontier growth visible on every healthy run without
  turning the Telegram session into an aggressive crawler. Every numeric funnel
  metric remains visible and must be reflected on; no inconvenient metric may
  be removed from the scorecard. The no-progress **controller**, however, uses
  only monotonic durable product milestones (new actually scanned sources,
  processed/KO posts, exact dual/text acceptance, image scoring, publication
  and delivery). Kernel launches, retries and unrelated scalar churn are
  activity, not product progress, and cannot keep a zero-yield loop alive.
  History depth metrics (`history_*_post_age_days`) remain visible and are used
  to decide whether to lower/raise scan depth for speed versus coverage.
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
  the uncached username-resolve allowance at one. If that batch returns real
  post bodies, the current run proceeds directly to E5/YDB handoff and defers
  lower-probability discovery to the next eligible cycle. If it returns no
  actionable body, history/fast-check/keyword/similar continue normally.
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
- Orchestrated CandidateReport runs set
  `REGION_TALK_WRITE_REPORT_ARTIFACTS=0`. Complete operational state remains in
  YDB; after publication/source/image queue writes and compact state persistence
  the worker writes only minimal `output.json` + `stage_status.json`. It does
  not assemble 58 report sheets or serialize XLSX/CSV/full JSON/Markdown/HTML.
  A deliberate offline/manual audit may set the flag to `1`; its separate
  `REGION_TALK_LIGHTWEIGHT_REPORT=1` then selects 18 review sheets instead of
  the full workbook. Both flags must be serialized into
  `region_talk_run_config.json`; setting them only in the local orchestrator
  process does not affect Kaggle.
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
  then by attempt count. This phase does not replace discovery globally. A
  useful critical batch may defer the remaining acquisition phases for one
  cycle so the posts reach E5/BGE handoff immediately. The default defer gate
  is at least eight actionable posts or no more than 600 seconds remaining;
  below eight posts with more headroom, bounded confirmed-source/fast-check
  acquisition continues in the same run. Discovery remains enabled and resumes
  in the next cycle after a real defer boundary.
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
  `gate_version=region_talk_publication_eligibility_v5` may enter the normal
  ImageDiagnostic lane. A bounded v2/v3/v4 migration exception exists only for
  old accepted low-score actual-image rows requiring the album-safe rescore;
  stale/missing versions otherwise defer for producer refresh and never erase
  prior image evidence. Unknown sources become `needs_source_review`;
  local/spam sources are rejected. The finalizer joins the authoritative source ledger,
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
  The same child profile keeps `REGION_TALK_SOURCE_QUEUE_RECLASSIFY_FULL=0`,
  indexes image evidence once, and reuses unchanged durable queue rows. It
  emits bounded 500-row progress events and uses a one-shot stack watchdog
  (`REGION_TALK_STACK_WATCHDOG_REPEAT=0`), avoiding the second native
  all-thread dump that coincided with the supervised Python 3.12 crash while
  retaining stage-level observability and all queue self-repair contracts.
  Local control-plane fallback to `yc` is bounded by
  `REGION_TALK_YC_CLI_TIMEOUT_SECONDS` (default 20): an expired browser-backed
  profile fails with an explicit authentication action instead of hanging the
  orchestrator. Scheduled automation should use the dedicated
  `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON`, not a short-lived user IAM token.
  When `REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL=1` and the launcher
  delegates credential loading to Kaggle User Secrets, the worker verifies the
  key immediately after loading run config/secrets. A missing key is terminal
  before state read; Kaggle must never fall through to the Yandex Compute
  metadata address `169.254.169.254`.
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
- Finalizer reads each required YDB kind through the existing keyset-paginated
  selector and reuses that single snapshot for strict pre-image source
  attestation. It must not rescan the full candidate/source/status kinds after
  the snapshot has already succeeded. Source-priority, onboarding and
  publication writes remain bounded UPSERT batches (20 rows per transaction by
  default). This avoids the observed 2026-07-17 `DEADLINE_EXCEEDED` on the
  redundant second candidate-memory pass without weakening any gate.
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

The orchestrator must pass one explicit `REGION_TALK_YDB_NAMESPACE` to every
CandidateReport/BGE/Image/finalizer launch. The current production namespace is
`region_talk_compact`: an LZ4 row-family table with checkpoint-v4 singleton
state and row-level product entities. It must not mix a Candidate run on the
legacy `region_talk` namespace with downstream workers on the compact namespace.
Storage health is an orchestration guardrail: monitor physical table bytes,
checkpoint payload bytes, actual row writes per kind and E5/BGE actionable
backlog. A one-row queue-sequence repair writing thousands of source rows is a
failure, not successful progress.

Checkpoint-v4 has no embedded fallback collection. The orchestrated
CandidateReport therefore uses full row-level read floors (`posts=20000`,
`vectors=20000`, `candidates=5000`, `sources=20000`) even when per-run scoring
and discovery batches remain small. These read limits are state-integrity
limits, not work-batch limits, and must not be reduced to shorten a run.
CandidateReport loads this complete starting snapshot exactly once per run and
passes it to exact-link selection, acquisition and report/state assembly. Exact
selection must not issue an overlapping multi-kind scan, and the scoring tail
must not reload the whole state after Telegram acquisition. The required
heartbeat is one `state_load_completed`; a second full load or a post-fetch
`RESOURCE_EXHAUSTED` is a failed canary, not a partial product success.
The BGE worker independently scans at least 20,000 `text_vector_enrichment_item`
rows. A 6,000-row prefix window is invalid once the shared E5+BGE kind exceeds
that size: newer E5 PKs can otherwise remain invisible and actionable dual
backlog will never drain.
Even an empty BGE pass must finish with
`bge_enrichment_done(status=no_rows)` so the orchestrator can distinguish a
clean empty queue from a worker stuck after YDB loading.

Publication finalization is URL-idempotent. A row with `sent_to_chat=true` is
terminal even when source-attestation fields later change: it may be retained
for audit, but must not consume Gemini again or increment `accepted_new`.
The same monotonic rule applies to durable Gemini rejects. Stale
`awaiting_text_restore`/`visual_review_pending` projections are reconciled to
their terminal provider/delivery state before any text fetch or Gemini call;
only a new hard source/text/compliance reject may revoke an earlier accept.

## Periodic critical product consultation

The orchestrator operator must request an **explicitly critical** agy Gemini
Pro review after either three complete product cycles (or roughly two hours of
supervised work), two technically successful cycles with zero new publication
output, or before a material architecture/policy change. The evidence packet
must include the complete metric snapshot and deltas (not selected favorable
metrics), run IDs, heartbeat/log artifact links, branch and commit SHA, Gemini
budget use, exact/confirmed/keyword/similar funnel outcomes, image raw versus
active backlog, publication lifecycle contradictions and delivered links.

Only a Pro-class response is a consultant review under the project policy.
The prompt asks the consultant to challenge both the implementation and the
operator hypothesis against the sole product goal: more safe, external,
non-advertising KO publication candidates. Its recommendations are advisory:
Codex must verify them against code, live data, session safety and dual E5+BGE
quality, and must reject recommendations that merely inflate activity, weaken
gates or stop discovery without evidence.

`queue_seq_repaired_this_run` is a transient marker scoped by
`queue_seq_repair_run_id`. It is cleared while reconstructing the queue and only
rows marked by the current build may be bulk-upserted as a sequence repair.

Kaggle `--no-wait` inputs are temporary private datasets, not durable state. If
creation fails twice, the launcher may delete only Region Talk input datasets
older than the six-hour safety TTL (bounded per attempt), protect the current
ref, and then make one final create retry.

A semantic contradiction is fail-closed before image/Gemini capacity: any
`is_multi_region_roundup`, `is_multi_topic_digest` or `is_digest_or_roundup`
marker rejects the row even if an older `kaliningrad_oblast_only_scope=true`
field survived in candidate memory.

ImageDiagnostic keeps an in-memory set of processed `image_queue_id`/post URLs.
A stale YDB read immediately after an UPSERT must not lease or score the same
post again; report counters are unique-post counters, not processing attempts.

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

The product scoreboard separately reports the all-time unique-post conversion
into the canonical pre-content geographic scope gate:

- denominator: `processed_posts_unique_total`;
- numerator: `ko_scope_detected_posts_unique_total`, unique processed posts with
  `kaliningrad_oblast_only_scope=true` before ad/news/substance/media/Gemini
  filters;
- rate: `processed_to_ko_scope_conversion_percent` and
  `processed_to_ko_scope_detected_per_1000`;
- coverage: `ko_scope_evaluated_posts_unique_total` and
  `ko_scope_evaluation_coverage_percent`;
- conditional yield: `evaluated_to_ko_scope_conversion_percent`.

The 2026-07-15 baseline was `525 / 12,075 = 4.35%` (43.5 per 1,000),
but only `3,563 / 12,075 = 29.51%` historical rows carried the current
scope/vector evaluation contract. Therefore the end-to-end conversion must be
read together with coverage; it cannot by itself distinguish low-yield source
selection from an incomplete historical evaluation pass. Raw lexical
`heuristic_ko_raw_posts_total` remains a lower-bound diagnostic, not this KPI.
The hard KO-only safety projection recognizes grammatical case forms only for
canonical allow-listed external geographies. This prevents a text such as
`Куршская коса … каньон Сулак в Дагестане` from reaching media/Gemini merely
because the external-region list stores `Дагестан` in nominative form. It does
not replace semantic dual-vector scoring or create positive acceptance from a
regex; policy refresh v4 simply recomputes the current source/text handoff for
active older rows.
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
`fast_check_status=no_hit` is the rollback-compatible result of the old fixed
two-query check. In the opt-in `adaptive_cursor_v1` experiment,
`no_hit_partial` means only the persisted query slice was exhausted and
`no_hit_exhausted` means the full configured lexicon bank was exhausted. None
of these is a source rejection; the source remains pending/lower-priority for
normal scan. Only explicit local-region or spam surface filters become terminal
rejections at this stage. The cursor advances only after a successful Telethon
source-local search RPC, so runtime reserve, cooldown and FloodWait cannot
silently skip terms.

The adaptive strategy is rollout-gated rather than a new default. The first
comparison run uses at most five sources × ten terms, cached Telegram entities
only, zero username resolves, two result rows per term, 5–9 second pauses and
stop-on-first-hit. Acceptance is based on incremental fresh exact KO URLs from
query positions 3+, honest RPC/time cost, no FloodWait, durable cursor recovery
and unchanged downstream E5+BGE gates. `legacy_v1` and the pushed rollback
branch remain available if the marginal yield does not justify the requests.

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
The live YDB source writer must preserve the full terminal evidence
(`source_geo_class`, topic/quick class, filter version/reason/hits and
`next_action`), not only the generic queue status and scope; otherwise sparse
online overlays make the classification appear incomplete.
The overlay is monotonic over the complete `SOURCE_QUEUE_STATE_FIELDS`
contract: a status/classification update must preserve `queue_seq`,
`queue_order`, admission metadata and cursor priority rather than turn an
ordered queue row into a sparse unordered row.

The delayed candidate-memory fusion pass repeats the same terminal source
guard before looking up E5/BGE rows. Pending posts from a recognizably local or
spam source are marked `dropped_local_source` / `dropped_spam_source` and do
not consume fusion work or create misleading image-fetch retries. The
`bge_m3_memory_source_blocked` heartbeat counter makes that saved work visible.
Candidate-memory online persistence is bounded, but rows whose terminal
local/spam source cleanup changed in the current run are ordered before BGE
fusion changes and ordinary refreshed rows. This prevents the live-write cap
from being occupied by the same healthy rows while already identified local or
spam candidates remain incorrectly operational across repeated cycles. The
one-time cleanup has its own bounded allowance
(`REGION_TALK_YDB_ONLINE_CANDIDATE_CLEANUP_MAX_ROWS`, default 500), so it may
exceed the ordinary 80-row refresh cap and drain the known terminal population
without repeatedly writing only its first page. A not-refetched memory row also
retains its terminal local/spam audit lifecycle; it is not reset to generic
`source_not_refetched_this_run` and rewritten on every cycle.

Publication telemetry distinguishes current-state and ledger counters. The
human stats line labels `publication_confirmed_total` as current-confirmed,
while sent rows and completed delivery records are displayed separately as
`sent-ledger` and `deliveries-completed`; a finalizer cleanup may legitimately
reduce current-confirmed without erasing historical delivery evidence.

ImageDiagnostic may receive both a generic Region Talk config dataset and its
own image-run config dataset. Runtime config loading explicitly applies the
config colocated with `image_diag_input.json` last, independent of Kaggle mount
glob order. This is required for explicit zero values such as
`WAIT_AFTER_DRAIN_SECONDS=0`; otherwise an older generic 600-second value can
silently restore empty CPU polling after the queue has drained.

For live YDB runs CandidateReport does not rewrite the entire source queue on
every handoff. `REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS` defaults to 500 and
the orchestrator currently sets it to 80 with
`REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL=0`. The bounded payload
keeps changed/current-run rows, keyword-evidence rows, the forward cursor
neighbourhood and pending/retry backlog. Actual current-run scan rows
(`last_scan_run_id` equal to the current run), fast-check rows and
keyword-evidence rows are mandatory in
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

Current funnel-calibration allocation favors direct KO evidence without
disabling discovery: up to eight exact links are read first when cached entity
capacity exists, fast-check uses ten sources, keyword discovery uses six
queries, while generic history is limited to four sources and ten posts per
source. A similar-discovered source receives at most one additional cached-only
scan slot and only when it has a persisted direct KO URL/fast-check hit/KO post
or publication-source evidence. Mere similarity never earns that slot and no
extra Telegram resolve allowance is created.

Confirmed external bloggers keep the complete adaptive place/POI bank, but no
single run is allowed to spend most of its wall time traversing that bank. Each
source advances by at most eight terms per wave and persists its cursor; the
whole fast-check stage is capped at 180 seconds. Reaching that cap is a normal
`no_hit_partial`/deferred continuation, not a source rejection. The next run
continues with later low-frequency locations, while exact links, E5 handoff,
keyword/similar discovery and the durable tail retain their time budgets.
Operator metrics expose query count, query time, total stage time, configured
cap and whether it was exhausted.

Exact-post lifecycle transitions are online-write owned. With the normal
`REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE=1` contract, the final compact snapshot
must not replay its stale start-of-run `post_link_queue`: otherwise a link that
was already closed as `terminal_source_rejected` can reappear as
`pending_fetch`/`fetched` on every run. Full row replay remains available only
for explicit maintenance. Orchestrator metrics classify `operator_rejected`
as terminal and report authoritative source-terminal cleanup separately from
real exact-fetch and BGE-rescore work. This prevents local-source cleanup from
inflating the product backlog or triggering an unnecessary BGE run.

The compact processed-post online projection includes `first_seen_run_id`.
The acquisition-side online writer owns this lineage before vector scoring: it
primes a fetch-path-independent cache from the loaded durable
`processed_post_item` rows, assigns the current run only to genuinely unseen
post identities, and preserves that first run through repeated fetch/scoring
writes. This is required because unchanged or runtime-deferred posts may never
reach the later scoring-state rebuild, while the final compact snapshot
intentionally does not overwrite authoritative row-level post state.
Legacy rows that predate the field inherit their prior durable observation run
(or an explicit legacy marker), so their first post-fix rescan is not counted
as a newly acquired post.
Latest-run KO conversion is joined back to the authoritative
`processed_post_item` identities touched by that CandidateReport run. Routine
candidate/image/publication reconciliation may refresh a downstream row's
`run_id`, but it must not make an old post look like a newly processed KO,
media or publication success in the per-run scorecard.

Publication source attestation uses `region_talk_source_fingerprint_v3`.
Material source decisions (local/external/spam/compliance/topic and surface
filter reasons) invalidate an accepted row, while monotonic scan/KO/candidate
counters do not. This lets CandidateReport continue scanning a confirmed
external source in parallel without making a just-accepted Gemini row
undeliverable before the notifier runs. Existing v2 rows are refreshed by the
finalizer without another Gemini call before delivery.
Without it, a run that really acquired new posts was reported as zero new and
all work appeared to be a rescan, undermining the data-driven controller.

Durable `gemini_reject`/`gemini_needs_review` rows never re-enter automatic
finalizer backlog merely because source counters or the authoritative source
fingerprint changed. Those changes do not alter the existing content verdict;
only an explicit operator `--reverify-existing` run may spend Gemini on them
again. Eligibility tombstones remain refreshable when their underlying source
classification or gate contract materially changes.

The same current-text ownership applies to asynchronous text restoration. A
historical publication row may retain `awaiting_text_restore` for audit after
CandidateReport has restored and re-scored the body, but it is active work only
while current candidate memory still has a KO-only, non-ad, non-multiregion
fused E5+BGE accept. Operator metrics therefore expose text-restore
`raw/active/tombstoned` separately; a stale restore marker cannot make a current
dual-text rejection look like publication backlog.

If the latest Candidate heartbeat stops in `state_write_started` or
`report_write_started`, this is reported as a late-tail failure. It does not
justify replacing high-yield exact/fast-check work with deeper generic history.

Transient YDB failures (`ConnectionLost`, deadline/timeout) close the cached
driver and retry on the next write, but must not disable all online writes for
the rest of the notebook. Only authentication/authorization failures disable
online writes by default (`REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR=1`);
transient-error disablement is an explicit opt-in
(`REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_TRANSIENT_ERROR=1`).

Source-level monitoring totals must not rely only on the latest compact source
row, because a historical partial run can leave stale/lower source counters.
Every completed source-history read therefore carries two monotonic queue
fields: `source_history_scan_ever_completed` and
`source_history_posts_scanned_max`. Exact-post and fast-check probes never set
them. The operator label «реально просмотрено» requires a successful history
timestamp or a positive durable history-post count; a generic
`fetch_attempted`, resolve cooldown, FloodWait or access denial is an attempt,
not a viewed source. Those failures remain visible in their own reason/status
metrics and cannot inflate confirmed-blogger scan coverage.
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
