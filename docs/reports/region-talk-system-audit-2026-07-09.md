# Region Talk system audit brief — data-driven discovery/funnel balance

Date: 2026-07-09
Branch/SHA at audit creation: `agent/region-talk/bge-m3-enrichment-test` / `88156df4`
Status: audit brief for external critical review by Gemini Pro (`a-gemini`) and Opus (`a-opus`).
Current live metrics source: dry-run orchestrator snapshot saved at `artifacts/codex/region-talk-system-audit-20260709/orchestrator_metrics_current.json`.

## 1. Why this audit exists

Region Talk is intended to be a **data-driven funnel**, not a one-off notebook. The system should continuously grow all core product indicators while staying safe for Telegram/Kaggle/YDB constraints:

1. discover more relevant public channels/communities;
2. quickly reject local Kaliningrad-only, spam, high-volume news-like and irrelevant sources;
3. find external blogger/travel/nonlocal channels that have at least one fresh Kaliningrad Oblast post;
4. score the found posts with dual text embeddings (`intfloat/multilingual-e5-base` + `BAAI/bge-m3`) without loading both models in one Kaggle notebook;
5. send only text-confirmed, non-ad, non-multiregion, non-news, useful/emotional KO posts with media to image scoring;
6. score actual images in the external ImageDiagnostic notebook;
7. run Gemini Lite only on strong text+image finalists, staying under 100 LLM calls for a goal of 20 confirmed posts;
8. notify the operator chat about each confirmed candidate and stop at 20.

The current system has made progress on orchestration, BGE separation and observability, but live metrics show **weak product movement**: many posts are being processed, while image/publication/Gemini indicators are flat and the source funnel still spends too much resource in depth rather than breadth.

## 2. Product goals and constraints

### Primary product target

Find **20 high-confidence publication candidates** about Kaliningrad Oblast, each confirmed by Gemini Lite and sent to the operator Telegram chat with a short explanation.

A valid final candidate must satisfy all of these:

- main content is about Kaliningrad Oblast, not a passing mention;
- source is external/nonlocal or mixed travel/blogger/media, not a pure local Kaliningrad feed;
- post is not advertising, promo, a paid tour/service post, event/news/trash, or a multi-region roundup;
- text has useful/emotional/firsthand/route/review/memorable details;
- actual images are strong/open-card/postcard-like enough;
- final Gemini Lite verifier accepts it.

### Operational constraints

- CandidateReport run target: about 20–30 minutes per Kaggle run.
- Main notebook uses E5 only; BGE-M3 runs in isolated worker notebook.
- BGE-M3 and ImageDiagnostic may run in parallel with CandidateReport if sessions/resources do not conflict.
- Telegram sessions are role-scoped: `DISCOVERY1` for main discovery, `DISCOVERY2` for image scoring/media fetch if needed.
- Human-like Telegram pacing is mandatory; large FloodWait values are blockers/degraded-mode evidence, not something to sleep through.
- Normal monitoring should not scan deeper than one year.
- Sources with >=30 text posts/day are rejected as high-volume/news-like before further history budget is spent.
- Minimum deterministic acceptance: regex/heuristics are diagnostics and guardrails; final text semantics remain vector/LLM-first.

## 3. Current architecture summary

### Notebooks/processes

1. `RegionTalkCandidateReport`
   - source/post discovery with Telegram session `DISCOVERY1`;
   - E5 text vectorization;
   - writes E5 `text_vector_enrichment_item` rows;
   - consumes external BGE rows when available;
   - dual fusion/text gate;
   - enqueues text-confirmed media rows to `image_queue_item`;
   - consumes image scores and calls Gemini Lite for finalists.

2. `RegionTalkBgeM3Enrichment`
   - no Telegram;
   - reads compact E5 rows from YDB;
   - computes BGE-M3 vectors/scores;
   - writes BGE `text_vector_enrichment_item` rows.

3. `RegionTalkImageDiagnostic`
   - separate image scoring notebook;
   - consumes `image_queue_item` rows that already passed text/source gates;
   - writes actual-image scores or terminal non-reviewable statuses.

4. Local/server orchestrator
   - reads YDB queue metrics;
   - launches safe non-conflicting notebooks;
   - runs local finalizer/notifier;
   - should monitor and reflect on **all** emitted metrics, not hide inconvenient ones.

### Current queue types

- `source_queue_item` — intended canonical source/public queue with `queue_order` and cursor.
- `source_status_item` / `online_source_item` — live overlays/status registry.
- `processed_post_item` / `post_live_item` — compact fetched/scored post state.
- `text_vector_enrichment_item` — E5/BGE vector rows.
- `candidate_memory_item` — text/vector/source candidate memory.
- `image_queue_item` — downstream image work items.
- `publication_candidate_item` — Gemini/publication candidates.
- **To implement:** `post_link_queue_item` — exact known-post queue for keyword/preflight hits.

## 4. Metrics snapshot and deltas

### Sources of numbers

- “Previous/evening baseline” below is reconstructed from the operator-provided 5-hour delta table: baseline = reported current − reported Δ. It should be treated as an audit baseline, not a separately archived YDB snapshot.
- “Current” is from dry-run command:

```bash
artifacts/codex/region-talk-ydb-venv/bin/python scripts/region_talk_orchestrator.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --allow-yc-fallback \
  --limit 10000 \
  --skip-kaggle-status
```

Snapshot artifact: `artifacts/codex/region-talk-system-audit-20260709/orchestrator_metrics_current.json`.

### Funnel table

| Group | Metric | Current | Previous/evening baseline | Δ | Audit note |
|---|---|---:|---:|---:|---|
| Sources | `publics_total` — total channels/publics in working queue | 2332 | 2294 | +38 | Breadth grows, but not enough alone. |
| Sources | `publics_primary_unscanned_pending_total` — primary unscanned pending | 1792 | 1604 | +188 | Backlog after discovery is growing faster than primary processing. |
| Sources | `publics_backlog_after_cursor_total` — backlog after cursor | 1810 | — | — | Core queue-health number; should be paired with cursor movement. |
| Sources | `publics_touched_or_not_pending_total` — no longer plain pending / touched | 508 | 690 | -182 | Previously confusing metric; must not be used alone. |
| Sources | `publics_terminal_processed_total` — final processed source-level | 182 | 144 | +38 | Healthy direction, but insufficient if backlog grows +188. |
| Sources | `publics_needs_rescan_or_retry_total` | 325 | 114 | +211 | Major problem: retry/rescan pile grows faster than terminal progress. |
| Sources | `publics_scanned_with_posts_total` | 348 | 229 | +119 | Source scanning grew, but publication funnel did not. |
| Region | `publics_processed_no_ko_total` | 163 | 135 | +28 | Too many no-KO scans if keyword discovery is supposed to prioritize KO-positive sources. |
| Region | `publics_processed_found_ko_candidate_total` | 18 | 7 | +11 | Good, but still too slow for 20 publication candidates. |
| Region | `publics_processed_found_ko_low_image_quality_total` | 1 | 2 | -1 | Not a core growth metric. |
| Posts | `source_latest_scan_run_posts_total` | 8194 | 6072 | +2122 | High resource spend. Needs breadth/depth balance. |
| Posts | `processed_posts_unique_total` | 10000 | 6384 | +3616 | Very high post churn; product output remained flat. |
| Posts | `source_latest_scan_run_posts_per_source_avg` | 26.52 | — | — | Depth currently moderate per source, but total churn still high. |
| Candidates | `candidate_memory_total` | 524 | 377 | +147 | Text/vector candidates grew, but downstream did not. |
| Vectors | `text_vector_e5_total` | 1694 | — | — | Raw count only. Exact-paired metric matters more. |
| Vectors | `text_vector_bge_m3_total` | 1710 | — | — | Raw BGE > E5 can be legacy/mismatch; not proof of dual coverage. |
| Vectors | `text_vector_e5_without_bge_exact_text_total` | 920 | — | — | Serious dual-pipeline backlog; BGE worker must catch exact E5 text rows. |
| Images | `image_queue_total` | 61 | 58 | +3 | Only small movement despite +147 candidate memory. |
| Images | `image_actual_scored_total` | 26 | 26 | 0 | Image stage is flat. This blocks publication growth. |
| Images | `image_not_reviewable_no_media_total` | 31 | 28 | +3 | Many text candidates do not have reviewable media. |
| Images | `image_not_reviewable_unsupported_media_total` | 3 | — | — | Terminal status is good, but these do not produce candidates. |
| Publication | `publication_candidate_total` | 16 | 16 | 0 | No growth at publication-candidate stage. |
| Publication | `publication_confirmed_total` | 7 | 7 | 0 | No progress toward 20 confirmed. |
| Publication | `publication_sent_total` | 7 | — | — | Existing confirmed rows have been sent; not new discovery success. |
| Publication | `publication_ready_total` | 0 | 0 | 0 | Still no explicit ready queue growth. |
| Keyword | `publics_keyword_queue_rows_total` | 10 | — | — | Too small for the intended keyword-driven acceleration. |
| Keyword | `publics_keyword_scanned_with_posts_total` | 8 | — | — | Keyword sources are mostly scanned, but volume is low. |
| Keyword | `publics_keyword_with_ko_candidates_total` | 8 | — | — | High yield among keyword rows, but too few rows. |
| Keyword | `publics_keyword_regex_ko_filtered_posts_total` | 12 | — | — | Deterministic comparator says more KO-like posts exist. |
| Keyword | `publics_keyword_vector_ko_candidate_posts_total` | 3 | — | — | Possible vector/gate under-recall or stricter semantic filter. |

### High-level metric interpretation

The system is currently **not balanced**:

- It can add/scan sources and process many posts.
- It can produce candidate memory rows.
- It is not converting those into image-scored/publication/Gemini-confirmed rows.
- Backlog/retry grows faster than terminal source progress.
- Keyword-driven acceleration is underused in volume.
- Exact post hits from search/preflight are not yet first-class queue items, so valuable hits can become weak source hints instead of immediate post scoring work.

## 5. Main problems already observed

### P1. Source discovery and source scanning are not yet sufficiently breadth-first

Evidence:

- `processed_posts_unique_total` +3616 while `publication_candidate_total` stayed 16 and `publication_confirmed_total` stayed 7.
- `publics_primary_unscanned_pending_total` +188 while terminal processed sources +38.
- `publics_needs_rescan_or_retry_total` +211.

Interpretation: the system is spending a lot of resource on post-level work and retry/rescan accumulation without enough movement through fresh public sources.

Required correction:

- primary objective before full queue pass: inspect more never-scanned publics;
- rescan only after the queue has had a primary pass or when a known high-value source is due;
- hard-reject high-volume/news-like feeds early;
- use source-local preflight to avoid deep scans of sources that have no KO evidence.

### P2. Global hashtag/keyword search was too narrow and too noisy

Previously the orchestrator default was effectively only:

```text
#Калининград|#Зеленоградск|#Светлогорск|#КуршскаяКоса
```

That was wrong: the region model is a 194-row city/settlement/POI lexicon, not 3 cities and one POI.

Already changed in commit `88156df4`:

- CandidateReport now builds discovery query banks from `kaliningrad-place-lexicon-v1.csv`;
- current generated banks: 116 keyword terms, 83 hashtag terms, 206 source-local preflight terms;
- orchestrator uses bounded rotating slice: 3 keyword + 4 hashtag per run;
- global keyword/hashtag calls go through `TelegramRequestGovernor.humanlike_pause`.

Remaining risk:

- global hashtag search is still inherently spam/local-heavy;
- must stay small and rotating;
- must be combined with immediate surface filtering and source-local preflight.

### P3. Exact post hits are not yet protected as first-class post work

Current problem:

- Global keyword/hashtag search can find a concrete `keyword_hit_post_url`.
- The system currently records it as source/context evidence and promotes the source after cursor.
- The exact post itself is not guaranteed to be scored next.

This is inefficient because if Telegram search already found a post containing “Калининград”/POI, that post is a high-value unit of work. It should not wait for a later broad history scan.

To-be correction:

- create `post_link_queue_item` keyed by post URL hash;
- enqueue exact hits from global search and source-local preflight;
- refetch by URL/id using the existing candidate-link mechanism generalized beyond `candidate_memory_item` / `image_queue_item`;
- score through the normal E5+BGE/text/image/LLM funnel;
- do not accept it automatically.

### P4. Source queue is intended as one queue, but effective control path is more complex

Intended canonical queue:

- `source_queue_item` / `unified_source_queue_cursor_position`.

Effective layers currently influencing behavior:

- `source_queue_item`;
- `source_status_item`;
- `online_source_item`;
- per-source cursors;
- source frontier rows;
- seed rows;
- public blogger catalog rows;
- keyword rows;
- similar-channel rows;
- selector priority buckets and product priority scores.

Risk:

- metrics can look simple while scheduling behavior remains multi-layered;
- cursor movement can be hard to audit;
- rows can be “touched” without truly consuming history budget;
- fake processed/legacy rows can hide missing scans if not repaired.

Required correction:

- maintain one operator-visible source queue as the scheduling source of truth;
- keep overlays only as status/evidence, not alternative hidden queues;
- every source selection should explain: canonical key, queue order, cursor relation, due state, skip/reject reason, and whether resource was spent.

### P5. Image/publication stages are flat

Evidence:

- `candidate_memory_total` +147;
- `image_queue_total` only +3;
- `image_actual_scored_total` +0;
- `publication_candidate_total` +0;
- `publication_confirmed_total` +0.

Possible causes:

1. many text candidates have no media or unsupported media;
2. text gates may be admitting candidate-memory rows that cannot become publication candidates;
3. BGE exact-pairing backlog blocks image queue when external BGE is required;
4. ImageDiagnostic is not being launched often enough or is blocked by session/resource coordination;
5. image queue terminal states may be correct but product funnel needs more media-rich source selection.

Required correction:

- report candidate-memory rows by media presence and BGE status;
- prioritize source/post discovery toward media-rich travel/blogger posts;
- launch ImageDiagnostic whenever pending rows exist and `DISCOVERY2` is free;
- keep terminal no-media/unsupported rows visible, not counted as progress.

### P6. Dual-vector pipeline still has exact-pairing backlog

Evidence:

- `text_vector_e5_total=1694`;
- `text_vector_bge_m3_total=1710`;
- `text_vector_e5_without_bge_exact_text_total=920`.

Raw E5/BGE counts are misleading. The exact text-hash pairing matters because fusion/scoring requires the same post/text version.

Required correction:

- BGE worker should consume E5 rows first, by exact text hash;
- orchestrator must launch BGE whenever `text_vector_e5_without_bge_exact_text_total >= 1`;
- audit should ignore raw BGE > E5 as success unless exact-paired coverage grows.

### P7. Regex comparator suggests potential vector under-recall or gate mismatch

Current keyword diagnostics:

- `publics_keyword_regex_ko_filtered_posts_total=12`;
- `publics_keyword_vector_ko_candidate_posts_total=3`.

This does not prove the vector gate is wrong; regex can overcount ads/multiregion/footer hashtag mentions. But it is a strong signal for review:

- inspect the 9 regex-positive/vector-negative posts;
- classify false positives vs vector false negatives;
- tune semantic banks/prototypes if vector under-recall is confirmed;
- never use regex as final accept/reject.

### P8. Channel/source dedup must happen before any resource spend

The system already has canonicalization in places (`canonical_source_key`, queue dedupe, frontier merge), but the invariant is not yet stated strongly enough as an end-to-end rule.

Required invariant:

> Any newly observed channel/source must be normalized to a canonical source key and checked against all durable source states before resolve/history/search/media/LLM work is attempted.

The dedup check must cover at least:

- `source_queue_item`;
- `source_status_item`;
- `online_source_item`;
- `source_candidate_item`;
- source graph targets where a canonical key is known;
- terminal rejected local/spam/high-volume statuses;
- processed/no-KO/processed-found-KO statuses and next allowed rescan time;
- pending queue rows that should be merged/promoted instead of duplicated;
- exact post rows (`post_live_item`, `processed_post_item`, `candidate_memory_item`, `image_queue_item`, `publication_candidate_item`, future `post_link_queue_item`) when a post URL is known.

Expected outcomes:

- already terminal local/spam source: do not resolve or scan again; append evidence only;
- already pending source: merge evidence, maybe promote after cursor, no duplicate row;
- already processed source: rescan only if due and after primary queue policy allows;
- known exact post: do not refetch/revector unless text hash/model/version is missing;
- metrics must expose duplicate saves: `source_duplicate_pre_network_total`, `source_duplicate_terminal_skip_total`, `source_duplicate_existing_queue_promoted_total`, `post_duplicate_known_skip_total`.

Current coverage:

- source roots are normalized through `canonical_source_url()` /
  `canonical_source_key()`;
- `build_unified_source_queue()` dedupes entries by `canonical_source_key`;
- `build_source_frontier_unique()` groups frontier rows by canonical key where
  available;
- keyword rows are deduped before cursor insertion;
- terminal statuses (`rejected_*`, high-volume, local/spam) are preserved in the
  queue builder and in the normal Telegram history path.

Known dedup/pre-spend gaps that need implementation work:

1. **Public-web-first bypass risk** — if
   `REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST=1`, public `t.me/s` fetch can happen
   before the normal source-surface prefilter. This can spend fetch budget on a
   local/spam/rejected source.
2. **VK path lacks the same pre-resource gate** — VK wall fetch can be attempted
   before source-surface terminalization.
3. **Frontier promotion depends on loaded queue/status rows** — if YDB read
   limits or snapshot drift hide an old rejected/processed source row, a stale
   frontier row can be promoted again.
4. **Similar-channel rows can be written before surface terminalization** —
   queue build can later reject them, but repeated recommendation passes may
   rediscover/write the same low-value source.
5. **`source_candidate_item` is not a complete terminal ledger** — terminal
   decisions live primarily in `source_queue_item`/`source_status_item`; if only
   candidate rows are loaded, prior rejection can be invisible.

Implementation touchpoints for the invariant:

- add a pure authoritative source-ledger helper around canonical key + terminal
  status + due/cooldown decision;
- call it before selected-for-run writes, before Telegram resolve/history, before
  public-web-first, before VK fetch, and before source-local preflight;
- make terminal source status available by key even when queue handoff/read
  limits are active;
- add tests for terminal local/spam/high-volume source skipped before public web,
  VK and Telegram resource calls.

## 6. Updated hybrid search model

### Priority P0 — known exact post queue

Inputs:

- global keyword/hashtag hits;
- source-local preflight hits;
- manually discovered post URLs;
- future post links from source graph.

Action:

- write `post_link_queue_item`;
- refetch exact post next;
- run normal text/vector/image/LLM gates.

Why:

- highest precision work unit;
- avoids losing known KO-containing posts;
- directly increases candidate throughput.

### Priority P1 — source-local preflight

When a source enters the system:

1. canonical dedup across all source lists;
2. cheap local/spam/high-volume/title filter;
3. resolve only through governor/cache;
4. in-channel search across a bounded rotating subset of the 206-term preflight bank;
5. if hit <=365 days: promote source after cursor and enqueue exact post;
6. if stale hit >365 days: keep evidence, lower priority;
7. if no hit: keep in backlog, do not deep-scan immediately unless source quality is high.

### Priority P2 — global lexicon keyword/hashtag search

Current implementation after `88156df4`:

- query source: `place_lexicon`;
- per run: 3 keyword + 4 hashtag rotating slice;
- global search paced through Telegram governor;
- sources surviving surface filter are inserted after cursor;
- local/spam terminal rows are visible but not scanned.

Risk controls:

- small batch size;
- surface spam/local filtering before history;
- no automatic post acceptance;
- exact post queue for useful hits.

### Priority P3 — similar channels / catalog / graph expansion

Use for breadth, but do not let generic backlog dominate resource spend.

Rules:

- catalog/similar rows are source candidates, not monitored automatically;
- source-local preflight can promote them;
- repeated no-hit preflight should lower priority;
- dedup must merge evidence into existing source records.

## 7. Anti-spam/local/high-volume source filters

### Local-region source indicators

Terminal separate list, not product scan:

- title/handle contains `Калининград`, `Кёниг`, `kenig`, `kgd`, `39`, regional towns/resorts;
- source is clearly an events/news/afisha/local guide feed.

These sources can be valuable for future local monitoring but are not the current external travel/blogger publication target.

### Spam/bait indicators

Terminal reject before source scan:

- repeated phrases like “ты не сможешь уйти”, “ты не можешь устоять”, “ты хочешь ещё”, etc.;
- VPN/promo/crypto/trading/betting/casino/bonus/free spins;
- cheap-flight coupon feeds;
- repeated spoiler/hidden-text posts;
- hashtag sets unrelated to travel/region and dominated by bait categories.

### High-volume news-like indicators

- >=30 text posts/day => terminal high-volume/news-like rejection.
- Rationale: these sources are unlikely to produce rich travel/visit-impression posts and consume excessive history budget.

## 8. Data-driven orchestration expectations

A healthy orchestrator cycle should report growth or a clear blocker in each layer:

1. **Source breadth:** new deduped candidate sources and cursor movement.
2. **Source quality:** more fresh-KO sources, fewer retries, fewer high-depth no-KO scans.
3. **Post throughput:** exact-hit posts scored, not just random backlog posts.
4. **Dual vector coverage:** exact E5+BGE pairs increase, BGE backlog decreases.
5. **Image:** actual scored images increase when text-confirmed media rows exist.
6. **Publication:** Gemini candidates/confirmed/sent increase toward 20.
7. **Diversity:** future anti-vector diversity should use dual embeddings over publication history, not only heuristics.
8. **Dedup savings:** duplicates skipped/promoted before network work are counted.

No-progress should not be hidden. If any layer is flat for multiple cycles, the orchestrator should explicitly reflect:

- is it a discovery problem?
- a source queue/cursor problem?
- a vector backlog problem?
- an image worker/session problem?
- a text gate too strict/too loose problem?
- an LLM limiter/finalizer problem?

## 9. Future/potential problems to audit

### 9.1 Over-optimization for easy metrics

Risk: growing `publics_total`, `processed_posts_unique_total` or `candidate_memory_total` can look like progress while final candidates stay flat.

Mitigation:

- publish all metrics together;
- focus on conversion ratios and downstream movement;
- never remove “uncomfortable” metrics.

### 9.2 Source queue starvation

Risk: keyword/preflight promotions can keep inserting after cursor and starve generic backlog.

Mitigation:

- cap promotions per run;
- use age/freshness/source quality;
- track source age in backlog and starvation count.

### 9.3 False local-source rejects

Risk: a nonlocal travel channel might include `39` or `Калининград` in one campaign/title and be terminally rejected.

Mitigation:

- local reject requires title/handle/source pattern, not one post;
- keep terminal rows visible for audit;
- allow manual/reactivation status.

### 9.4 Regex vs vector mismatch

Risk: vector banks may miss small-place/POI mentions; regex may overcount hashtag/footer mentions.

Mitigation:

- sample regex-positive/vector-negative posts;
- expand semantic banks using lexicon-derived prototypes;
- keep regex diagnostic only.

### 9.5 BGE worker lag

Risk: E5 produces faster than BGE catches up, blocking image queue.

Mitigation:

- launch BGE on exact-pair backlog;
- increase BGE batch limit within CPU/Kaggle stability;
- report exact-pair coverage, not raw model totals.

### 9.6 Image scoring bottleneck

Risk: text candidates accumulate but images remain unscored.

Mitigation:

- prioritize media-rich exact-hit posts;
- launch ImageDiagnostic whenever pending rows exist;
- terminal unsupported/no-media rows must exit the queue and be reported.

### 9.7 Telegram FloodWait/session risk

Risk: global search/resolve/preflight can create long FloodWaits.

Mitigation:

- shared governor for all Telethon calls;
- network resolve caps;
- entity cache;
- no ad-hoc scripts without pacing;
- no session role borrowing.

### 9.8 YDB row overlay ambiguity

Risk: source status split across queue/status/online rows makes audits confusing.

Mitigation:

- source_queue_item is canonical scheduling state;
- overlays must not overwrite queue order/status with empty status;
- metrics should show raw vs repaired totals.

### 9.9 Diversity / anti-vector not fully implemented

Current diversity penalty is partly heuristic. Desired product behavior is semantic anti-vector diversity against previously published posts, ideally using dual E5+BGE vectors for publication history.

Future work:

- enqueue published/history posts for E5+BGE;
- compute semantic distance to already published posts;
- rank candidates by high image/text score plus semantic novelty.

## 10. Acceptance criteria for next implementation step

The next implementation should be judged by measurable movement, not just code presence:

1. `post_link_queue_item` exists and receives exact keyword/preflight post hits.
2. Source-local preflight runs through the Telegram governor and writes status/evidence.
3. Dedup-before-resource-spend counters are emitted.
4. Keyword/preflight sources with fresh hits are inserted after cursor and exact posts are scored in the next run.
5. `publics_primary_unscanned_pending_total` stops growing faster than `publics_terminal_processed_total` over several cycles.
6. `text_vector_e5_without_bge_exact_text_total` decreases after BGE cycles.
7. `image_actual_scored_total` grows when image queue has pending actual media.
8. `publication_candidate_total` and `publication_confirmed_total` grow toward 20.
9. Regex/vector KO mismatch is sampled and categorized.
10. Every dashboard/report includes all core metrics, including no-growth and retry metrics.

## 11. Questions for external audit reviewers

Please critically review the system as a data-driven acquisition/publication funnel.

1. Is the proposed hybrid search model balanced between breadth, precision, rate-limit safety and downstream candidate yield?
2. Are the metrics sufficient to prevent cherry-picking and detect no-progress honestly?
3. Is `post_link_queue_item` the right abstraction for exact hits, or should exact hits be folded into an existing post queue differently?
4. Is source-local preflight likely to reduce resource waste, or can it create a new hidden bottleneck/FloodWait risk?
5. Is the dedup-before-resource-spend invariant complete enough? What row kinds/statuses are missing?
6. Which bottleneck should be attacked first: source discovery/preflight, BGE exact-pair backlog, image scoring, or vector gate tuning?
7. What leading indicators should the orchestrator use to decide that a run is unhealthy and self-correct?
8. What is the most likely false-positive/false-negative failure mode in the updated design?
9. How should anti-vector diversity be implemented without hurting relevance?
10. What would you simplify or remove from the current queue/metrics design?

## 12. Evidence and changed files

Recent relevant commits:

- `82539d02 Fix Region Talk hashtag cursor filtering`
- `88156df4 Make Region Talk discovery queries lexicon-driven`

Relevant files:

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `scripts/region_talk_orchestrator.py`
- `docs/features/region-talk-channel/source-discovery.md`
- `docs/features/region-talk-channel/orchestration-to-be.md`
- `docs/features/region-talk-channel/ydb-schema.md`
- `docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv`
- `artifacts/codex/region-talk-system-audit-20260709/orchestrator_metrics_current.json`

## 13. External review results (2026-07-09)

Audit prompt sent to:

- Gemini Pro class via `a-gemini` / `Gemini 3.1 Pro (High)`.
- Opus class via `a-opus` / `Claude Opus 4.6 (Thinking)`.

Local artifacts, not committed:

- prompt: `artifacts/codex/region-talk-system-audit-20260709/external_review_prompt.md`
- Gemini review: `artifacts/codex/region-talk-system-audit-20260709/gemini_review.md`
- Opus review: `artifacts/codex/region-talk-system-audit-20260709/opus_review_full.md`

Both reviews converged on the same core diagnosis: the current system can generate upstream activity while downstream publication movement stays flat. A valid next step must therefore be judged by end-to-end funnel movement, not only by source/post volume growth.

### 13.1 Gemini Pro review — key critical points

Gemini called the current model **not balanced** because it still behaves like deep crawling for a surgical task. Main findings:

1. Exact keyword/preflight hits are effectively lost when converted only into source hints; exact posts need a fast lane.
2. BGE exact-pair backlog is an immediate tactical bottleneck because dual fusion cannot work on E5-only rows.
3. Image worker starvation and media eligibility are underdiagnosed.
4. Queue/status overlays make cursor behavior and resource waste hard to audit.
5. Dedup-before-spend must also cover public web/VK paths and should add pre-network skip counters.
6. Source-local preflight must be rate-budgeted; probing hundreds of terms per channel is unsafe.
7. Missing metrics include E5→BGE lag, source duplicate pre-network skips, API/FloodWait counters, and post→image conversion.

Gemini's recommended 24-hour order:

1. Drain BGE exact-pair backlog.
2. Add exact post fast lane (`priority_post_task` / equivalent).
3. Add a single canonical source decision helper before VK/public-web/Telegram network calls.
4. Limit source-local preflight to a small, governed subset of high-value queries.

### 13.2 Opus review — key critical points

Opus gave a sharper downstream-first criticism: the system is currently producing work, not outcomes. Main findings:

1. The first bottleneck to attack is **image scoring**, because `image_actual_scored_total` had zero growth while upstream rows grew.
2. The second bottleneck is `candidate_memory -> image_queue` conversion: growth in candidate memory is not meaningful unless eligible media rows grow.
3. BGE backlog should run in the background, but the system must expose whether candidates are blocked by missing BGE or by missing media.
4. A separate `post_link_queue_item` may add lifecycle complexity; an alternative is to fold exact hits into existing `processed_post_item` with `discovery_method`, `discovery_priority`, `keyword_hit_query`, and `needs_immediate_scoring` fields. If a separate queue is used, it should be an ephemeral intake buffer that drains immediately.
5. Metrics still need mandatory conversion ratios and stall indicators: candidate→image, image→publication, source scan depth p95, API budget, exact-hit queued/scored, and time-to-first-candidate.
6. Queue design should be simplified toward one authoritative source ledger/registry with terminal status, priority, next scan time, and explicit rejection reason.

Opus's recommended 24-hour order:

1. Diagnose/run ImageDiagnostic and determine whether there are pending rows with actual media.
2. Instrument candidate memory eligibility: has media, no media, waiting BGE, image-queue eligible.
3. Make exact hits first-class immediate work, preferably without creating another long-lived lifecycle.
4. Add hard resource budgets and pre-network dedup decisions.
5. Only then expand keyword/preflight breadth.

### 13.3 Consolidated implementation implications

The external reviews slightly disagree on first tactical priority:

- Gemini: first drain BGE exact-pair backlog, then exact post fast lane.
- Opus: first unblock image scoring and candidate→image eligibility, while BGE drains in background.

The safer combined plan is:

1. Run a no-discovery downstream diagnostic first: `candidate_memory -> image_queue -> image_actual_scored -> publication_candidate`.
2. In parallel or immediately after, run BGE worker until exact-pair lag is visibly shrinking.
3. Implement exact-hit fast lane, but avoid adding another permanent queue if the existing processed-post lifecycle can carry priority metadata cleanly.
4. Add pre-network source dedup gate and counters before increasing global/preflight discovery volume.
5. Add stop/reflect rules to the orchestrator: if downstream counters are flat for N cycles while upstream grows, reduce discovery/deep scans and prioritize downstream drains/diagnostics.
