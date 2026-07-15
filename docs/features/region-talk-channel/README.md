# О Калининграде говорят / Region Talk Channel

> Canonical slug: `region-talk-channel`. User-facing working name: **«О Калининграде говорят»**. Product/implementation alias: **Kaliningrad-best-post-monitoring**. Status: **MVP live-YDB runner / production-candidate pipeline under supervised orchestration**. Telegram/VK public publishing remains disabled; the current product goal is a Gemini-confirmed operator queue and Telegram notifications with source links.

## Document map

- [Source discovery](source-discovery.md) — как находить и оценивать внешние русскоязычные источники.
- [Post discovery](post-discovery.md) — мониторинг утверждённых источников, semantic bank v1 и candidate scoring.
- [Image postcardness](image-postcardness.md) — каскад оценки фотографий и VLM JSON contract.
- [Image-scoring false-negative review](image-scoring-false-negative-review.md) — live evidence, locked operator labels, compliance no-spend gate и полный prompt для внешнего консультанта.
- [Image-scoring audit methodology v2](../../reference/image-scoring-audit-methodology-v2.md) — принятый consultant decision record: album-level acquisition, selective decision, calibration/shadow и stop/go критерии.
- [YDB schema draft](ydb-schema.md) — все новые persistent-данные фичи живут в YDB, не в SQLite.
- [MVP candidate report](mvp-candidate-report.md) — cumulative/delta-aware XLSX review workbook contract.
- [Seed sources v1](seed-sources-v1.md) + [CSV](seed-sources-v1.csv) — стартовый seed-list для MVP-1 probe.
- [Seed sources v2](seed-sources-v2.md) + [CSV](seed-sources-v2.csv) — 300+ row MVP-1.x source frontier; profile-probe before monitoring.
- [Kaliningrad place lexicon](kaliningrad-place-lexicon.md) + [CSV](kaliningrad-place-lexicon-v1.csv) — regional place recall/scope guardrail.
- [LLM/VLM verifier contract](llm-verifier-contract.md) — Gemini Flash-Lite verifier/post-writer только для top candidates.
- [Publication queue](publication-queue.md) — queue, slots, idempotency, diversity caps, dry-run.
- [Source onboarding profile](source-onboarding-profile.md) — доказательный профиль автора/канала и абзац `О блогере` для финального кандидата.
- [To-Be orchestration and vector queues](orchestration-to-be.md) — короткие queue-driven прогоны, отдельный BGE-M3 worker, YDB triggers, non-region geo bank и semantic anti-vector diversity.
- [Telegram/VK publishing](telegram-vk-publishing.md) — future publishing contracts, VK carousel/card risk, Telegram Bot API modes.
- [Risk register](risk-register.md) — legal/media, VK token, Telegram read, autonomy, cost and reliability risks.
- [Implementation plan](implementation-plan.md) — MVP-0 → MVP-5 phases and readiness checklist.
- [MVP-1 test-run runbook](test-run-runbook.md) — bounded Candidate Report Only runbook.

## Product intent

**«О Калининграде говорят»** — автономный discovery/publishing pipeline, который показывает, как о Калининградской области говорят за пределами региона: тревел-блогеры, авторские каналы, travel/architecture/history/nature/city-life сообщества, маршруты выходного дня, море, гастрономия и визуально красивые места.

Формат канала **не** должен выглядеть как “мы украли чужой пост”. Целевой формат:

- короткое собственное summary;
- что именно в регионе отметил источник;
- какие смыслы были позитивными, нейтрально-полезными или конструктивно-критичными;
- ссылка на оригинальный источник;
- визуальная карточка/карусель только если `rights_policy` это разрешает.

Это **не** новостной канал, не происшествия, не политика, не региональный агрегатор и не трэш. Это curated discovery внешних позитивных, полезных или mixed-but-valuable публикаций о регионе.

## Scope summary

Pipeline должен уметь:

1. Находить внерегиональные русскоязычные Telegram/VK/web-источники, которые потенциально пишут о Калининградской области.
2. Мониторить новые посты только у `accepted`/`candidate` источников с курсорами и `next_fetch_after`.
3. Находить содержательные посты о регионе, а не keyword-only совпадения.
4. Отсекать новости, трэш, политику, происшествия, low-quality ads, repost farms и unsafe visuals.
5. Оценивать позитивные, нейтрально-полезные и конструктивно-негативные смыслы.
6. Оценивать фотографии: красивость, открыточность, региональную визуальную релевантность, техническое качество и publication safety.
7. Сохранять source/post/media/candidate/run state в **YDB sidecar**.
8. Формировать MVP candidate report / favorites table, прежде всего **XLSX** для ручного просмотра.
9. Запускать final LLM/VLM verifier только на top candidates.
10. Для финального кандидата собирать компактный публичный evidence pack, переиспользуемый профиль источника и один проверяемый абзац `О блогере`.
11. В будущем генерировать короткий пост, ставить в очередь, публиковать в Telegram и VK и вести ledger.

## Non-goals for MVP

- Не создавать сейчас Telegram-канал и VK-сообщество.
- Не писать production-код и не подключать реальную автопубликацию в docs-first pass.
- Не мониторить личные профили как основной источник.
- Не делать новости, происшествия, политику, скандалы или региональные калининградские новостные паблики.
- Не копировать полные тексты чужих постов.
- Не публиковать и не модифицировать чужие фото без `rights_policy`.
- Не включать fully autonomous publish без dry-run/shadow mode и manual gates.
- Не делать LLM/VLM на каждый пост/картинку без vector/scoring cascade.
- Не хранить новые persistent-данные фичи в SQLite; SQLite можно читать только как legacy/input snapshot, если это уже принято проектом.

## Relationship to existing repo architecture

### Subscriber Acquisition

Related: [`docs/features/subscriber-acquisition/`](../subscriber-acquisition/README.md).

Reuse product discipline: social-source discovery, Telegram/VK surface discovery, link/forward/mention graph, anti-spam caution, sparse high-signal actions and manual review before public actions. Difference: subscriber acquisition ищет места для точечных рекомендаций событий, а Region Talk ищет внешние **посты о регионе** для curated channel/report.

### Post Metrics & Popularity

Related: [`docs/features/post-metrics/README.md`](../post-metrics/README.md), `source_parsing/post_metrics.py`.

Post metrics remain the quantitative layer (`views`, `likes`, comments/reposts where available, per-source baselines). Region Talk is the qualitative discovery/curation layer. `engagement_normalized_score` may read metrics as a weak tie-breaker, but strong media + semantic fit + rights/safety gates dominate.

### Unsigned personalization semantic retrieval

Related: [`semantic-vector-retrieval.md`](../unsigned-personalization/semantic-vector-retrieval.md), [`event-detail-related.md`](../unsigned-personalization/event-detail-related.md), [`event-detail-related-probe.md`](../unsigned-personalization/event-detail-related-probe.md).

Reuse the architectural pattern:

```text
vector retrieval = recall stage
verifier = precision stage
heavy computation offline/Kaggle
public/future publish hot path has no provider/vector calls
```

MVP vector recall must use **dual-model enrichment**, not an A/B choice: run both `intfloat/multilingual-e5-base` and `BAAI/bge-m3`, merge/union their top-K semantic matches and keep per-model evidence so recall becomes broader and quality improves through score fusion. The implementation must record `embedding_model`, `embedding_dim`, document version and per-model/fused scores so every accepted candidate remains auditable.

The production CPU split is durable and independent: CandidateReport loads E5
from a pinned Kaggle Model input, writes E5 evidence to YDB and releases the
model; the separate BGE-M3 notebook enriches the same text rows later. A
transient Hugging Face download failure is not a reason to weaken or replace the
dual-vector contract.

The BGE worker has two distinct workloads: **missing live pairs**, which block
CandidateReport fusion, and **stale semantic-bank refresh**, which is background
maintenance for an already paired post. Every CPU batch must drain all
selectable missing pairs before it spends remaining capacity on stale refresh;
the exact/fast-check versus generic 80/20 reserve is applied independently
inside each population. Operator metrics report these two populations
separately so a large maintenance backlog cannot be mistaken for product
throughput or starve fresh KO evidence.

### Kaggle static/offline discipline

Related: [`docs/operations/kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md), `kaggle_status.py`, `kaggle_registry.py`, `video_announce/kaggle_client.py`.

Region Talk should be an offline Kaggle job, not an uncontrolled publisher:

```text
immutable run config / source seeds / YDB cursor snapshot
  → Kaggle discovery/scoring run with status ledger and locks
  → artifacts: xlsx/csv/json/md/html + model reports
  → manual review / favorites
  → optional dry-run previews
  → controlled publication only in later MVPs
```

Status events should include `preflight_ok`, `sources_scanned`, `posts_fetched`, `media_scored`, `candidates_created`, `report_written`, `queue_written`, `publish_dry_run_done` and terminal failure. Secrets must be injected as Kaggle secrets or encrypted split datasets and never printed.

### MVP-1 implemented runner

Implementation entrypoints added for the first Candidate Report Only run:

- local/Kaggle script: `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`;
- Kaggle metadata: `kaggle/RegionTalkCandidateReport/kernel-metadata.json`;
- Kaggle launcher: `kaggle/execute_region_talk_candidate_report.py`;
- BGE-M3 enrichment script: `kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py`;
- BGE-M3 Kaggle metadata: `kaggle/RegionTalkBgeM3Enrichment/kernel-metadata.json`;
- BGE-M3 launcher: `kaggle/execute_region_talk_bge_m3_enrichment.py`;
- Qwen3-Embedding-0.6B research script: `kaggle/RegionTalkQwen3Embedding06BEnrichment/region_talk_qwen3_embedding_06b_enrichment.py`;
- Qwen3 research launcher: `kaggle/execute_region_talk_qwen3_embedding_06b_enrichment.py`;
- BGE-vs-Qwen quality comparison helper: `scripts/region_talk_embedding_quality_compare.py`;
- focused smoke tests: `tests/test_region_talk_candidate_report.py`.

Telegram source monitoring is explicitly **Telethon-based**. Manual Kaggle handoff uses role-scoped sessions: `RegionTalkCandidateReport` and `RegionTalkImageDiagnostic` default to `TELEGRAM_AUTH_BUNDLE_DISCOVERY1` / `TELEGRAM_AUTH_BUNDLE_DISCOVERY2` respectively, while local Telegram Saved Messages delivery uses only the E2E human session (`TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION`). `TELEGRAM_AUTH_BUNDLE_S22` is reserved for production Kaggle/remote monitoring and must not be shipped with Region Talk runs unless explicitly selected for that one run. Never run two Kaggle kernels against the same Telethon auth key concurrently. Region Talk does not use Bot API for reading public channel history and it never calls Telegram/VK publication APIs.

### Reuse existing Kaggle infrastructure

Before implementation, the Region Talk runner must inspect and reuse existing repo patterns instead of writing a runner from scratch. Required local anchors:

- Telegram Monitoring: `kaggle/TelegramMonitor/telegram_monitor.py`, `source_parsing/telegram/service.py`, `source_parsing/telegram/split_secrets.py`;
- CherryFlash: `kaggle/CherryFlash/`, `scripts/run_cherryflash_live.py`;
- generic Kaggle status/registry: `kaggle_status.py`, `kaggle/kaggle_status_client.py`, `kaggle_registry.py`;
- Kaggle push/dataset client: `video_announce/kaggle_client.py`;
- static/offline artifact discipline: `kaggle/StaticSiteBuilder/static_site_builder.py`.

Reuse run id generation, status ledger, run lock/resource lease, dry-run mode, progress events, artifact paths, secrets handling, retry/backoff, failure reporting, immutable run config and existing publisher/lock patterns where relevant. MVP-1 must keep `REGION_TALK_DISABLE_PUBLISH=1`.

### YDB sidecar boundary

All new persistent state for this feature goes to YDB: sources, source state, graph edges, posts, media, embeddings, semantic matches, candidates, favorites, publication assets, queue, ledger, verifier cache and discovery runs. Core Fly SQLite remains the event bot database and may be read only as an input snapshot/legacy source. Do not add Region Talk tables to SQLite without a separate architecture decision.

## Architecture shape

```text
source catalogs / links / mentions / manual seeds
  → source candidates
  → source scoring
  → monitored sources

monitored sources
  → new posts
  → region relevance anchors + semantic vector recall
  → non-news / non-trash / non-politics filters
  → text value scoring
  → image postcardness scoring
  → top candidate verifier
  → XLSX/CSV/JSON/Markdown candidate report + favorites table
  → manual review
  → publication queue (future)
  → Telegram publish + VK carousel publish (future)
  → publication ledger
```

The feature has three contours:

1. **Source Discovery** — find/evaluate external sources that may write about Kaliningrad Oblast.
2. **Post Discovery** — scan monitored sources and select concrete posts about the region.
3. **Publishing** — candidate → verifier → render assets → queue → Telegram/VK publish → ledger. Disabled until later MVPs.

## MVP result: XLSX-first candidate report

For the current MVP, the visible result is a spreadsheet/report, not a live channel:

- latest workbook: `artifacts/region-talk/candidates-latest.xlsx`;
- per-run immutable workbook: `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.xlsx`;
- companion CSV/JSON/Markdown/HTML artifacts in the same per-run folder.

The XLSX must be cumulative and delta-aware: it shows what was found, what is new this run, what changed stage, what became candidate/favorite, what dropped out, which image-quality gates passed/failed, and the final candidates for human eye review. See [MVP candidate report](mvp-candidate-report.md).

## External references and technical constraints

- Telegram Bot API supports HTTPS requests, JSON/form/multipart payloads and file uploads via multipart; `sendMediaGroup` sends albums of 2–10 media items. Use it only after the bot is admin in the future channel and publication is enabled. Source: <https://core.telegram.org/bots/api>.
- YDB Python SDK supports endpoint/database/auth/TLS configuration through a Driver, Query Service for YQL/transactions and Table Service for schema/bulk operations; production docs must include retries/backoff and structured errors. Source: <https://ydb-platform.github.io/ydb-python-sdk/>.
- VK wall image publishing target path is `photos.getWallUploadServer` → upload binary image → `photos.saveWallPhoto` → `wall.post` attachments, but a 2026 `vk-api-schema` issue reports `photos.getWallUploadServer` error 27 with Community Token while text-only `wall.post` works. VK image upload token type must be validated in dry-run before enabling VK image publishing. Source: <https://github.com/VKCOM/vk-api-schema/issues/242>.
- RegionTalkImageDiagnostic and the VK media prefetcher use the same read-token policy as CandidateReport: public `wall.getById` reads prefer `VK_SERVICE_TOKEN`/`VK_SERVICE_KEY` when `REGION_TALK_VK_READ_SERVICE_FIRST=1`. IP-bound user tokens are only fallback inputs. This prevents a Kaggle IP change from degrading a VK album to one prefetched frame and a nonterminal visual-review row. Existing nonterminal review rows whose only blocker is an incomplete album are reopened for bounded acquisition repair on the next run, including the persisted `actual_scored + needs_visual_review + partial` state; genuinely ambiguous fully acquired albums remain in review.

## Acceptance status for this docs pass

- Production implementation: **partially implemented / supervised live-YDB pipeline**:
  CandidateReport, isolated BGE-M3 enrichment, ImageDiagnostic, publication
  finalizer/notifier and `scripts/region_talk_orchestrator.py` operate against
  live YDB under dry-run/no-publication guardrails.
- Short-run runtime contract: CandidateReport must not spend the tail of a
  10–30 minute debug cycle rewriting unchanged queues. The source queue handoff
  is bounded to changed/current/keyword/cursor-neighbourhood rows, duplicate
  `source_status_item` mirroring is disabled by default, candidate-memory YDB
  handoff writes only changed/refetched rows unless BGE fusion changed memory
  statuses, and both source/candidate handoffs have explicit row caps. Telethon
  network operations are guarded by per-call timeouts and cached-entity-only
  mode can defer username cache misses instead of burning FloodWait.
- Product-heartbeat contract: `current_run_reviewable_candidates` counts only
  current-run rows whose lifecycle stage is actually ready for operator
  review. Newly-created candidate-memory rows still waiting for BGE, actual
  media acquisition or a policy refresh remain visible through
  `candidate_memory_new_this_run`, but are not called reviewable output.
- Fast-check KO contract: a source-local keyword hit is both an exact-post task
  (`post_link_queue_item`) and a source-priority signal. CandidateReport must
  persist `fast_check_status=ko_hit` on the corresponding `source_queue_item`,
  move it into the after-cursor priority band, and treat it as primary due on
  the next source-selection pass even if that source has previous partial scan
  evidence. This prevents fast-check from becoming an inert side queue.
- Channel/community creation: **not done**.
- Real tokens/secrets: **not introduced**.
- Publishing: **not performed**.
- Next useful task: keep the orchestrator loop running, watch the live funnel
  metrics for growth, and fix whichever stage stops increasing before the
  20-link Gemini-confirmed target is reached.


## MVP-1.x strict selection update

Пост должен быть содержательно про Калининградскую область. Можно несколько городов/посёлков/мест внутри области; нельзя брать мульти-региональные подборки, рекламу/промо/анонсы, прошлогодние посты или слабые по содержанию визуальные дампы. Image scoring запускается только после freshness, Kaliningrad-only, non-ad, substance and non-news/non-trash gates.
### Confirmed-external first-scan evidence

Generic `fetch_attempted` is not proof that a source history was scanned.  It
may be set while a confirmed-external blogger is only admitted/enriched in the
shared queue.  A first history pass is considered complete only when there is
an explicit history timestamp, a positive durable `posts_scanned` counter, or
a recorded access-level deferral such as cached-entity/cooldown handling.  This
keeps zero-post `pending_scan` bloggers in the primary high-probability lane
instead of incorrectly cooling them down as rescans.
