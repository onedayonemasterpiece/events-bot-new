# О Калининграде говорят / Region Talk Channel

> Canonical slug: `region-talk-channel`. User-facing working name: **«О Калининграде говорят»**. Product/implementation alias: **Kaliningrad-best-post-monitoring**. Status: **MVP live-YDB runner / scheduled discovery enabled**. Telegram/VK public publishing remains disabled; the current product goal is a Gemini-confirmed operator queue and Telegram notifications with source links.

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
- [External publications](external-publications.md) — broad-web prompt, JSON Schema, staging importer, public-interest contract и on-demand anti-vector queue для материалов изданий.
- [To-Be orchestration and vector queues](orchestration-to-be.md) — короткие queue-driven прогоны, отдельный BGE-M3 worker, YDB triggers, non-region geo bank и semantic anti-vector diversity.
- [Telegram/VK publishing](telegram-vk-publishing.md) — future publishing contracts, VK carousel/card risk, Telegram Bot API modes.
- [Future café/restaurant review vertical](../../backlog/features/region-talk-cafe-reviews/README.md) — отдельная to-be гипотеза для серийных неместных обзоров заведений; текущий Region Talk не меняется.
- [Risk register](risk-register.md) — legal/media, VK token, Telegram read, autonomy, cost and reliability risks.
- [Implementation plan](implementation-plan.md) — MVP-0 → MVP-5 phases and readiness checklist.
- [MVP-1 test-run runbook](test-run-runbook.md) — bounded Candidate Report Only runbook.
- [Gemini effectiveness audit](../../reports/region-talk-effectiveness-gemini-audit-2026-07-15.md) — внешний product-funnel review по live evidence.
- [Independent consultant prompt](../../reports/region-talk-effectiveness-external-consultant-prompt-2026-07-15.md) — GitHub-readable handoff с веткой, файлами, метриками и полным Gemini input.

## Product intent

**«О Калининграде говорят»** — автономный discovery/publishing pipeline, который показывает, как о Калининградской области говорят за пределами региона: тревел-блогеры, авторские каналы, а также нерегиональные журналы, научные издания, профессиональные платформы и культурные медиа. Темы включают travel/architecture/history/nature/city-life, науку с понятным широкому читателю зерном, маршруты, море, гастрономию и визуально красивые места.

Формат канала **не** должен выглядеть как “мы украли чужой пост”. Целевой формат:

- короткое собственное summary;
- что именно в регионе отметил источник;
- какие смыслы были позитивными, нейтрально-полезными или конструктивно-критичными;
- ссылка на оригинальный источник;
- исходное hero/альбом/видео с явной ссылкой на источник и оригинал; визуальная
  диагностика подтверждает связь медиа с материалом и точный порядок ревизии.

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
8. Хранить операционный результат в YDB и по явному запросу формировать XLSX/favorites report для ручного аудита; автоматический orchestration не обязан экспортировать его каждый run.
9. Запускать final LLM/VLM verifier только на top candidates.
10. Для финального кандидата собирать компактный публичный evidence pack, переиспользуемый профиль источника и один проверяемый абзац `О блогере`.
11. В будущем генерировать короткий пост, ставить в очередь, публиковать в Telegram и VK и вести ledger.
12. Принимать результаты широкого внешнего web-research по строгому JSON-контракту без собственного crawler и держать их в staging до штатных Region Talk gates.

## Non-goals for MVP

- Не создавать сейчас Telegram-канал и VK-сообщество.
- Не писать production-код и не подключать реальную автопубликацию в docs-first pass.
- Не мониторить личные профили как основной источник.
- Не делать новости, происшествия, политику, скандалы или региональные калининградские новостные паблики.
- Не копировать полные тексты чужих постов.
- Не выдавать исходное медиа за собственное и не терять явную атрибуцию/ссылку
  на оригинал; association/materialization gate не должен подменяться
  спекулятивным запретом на перенос медиа.
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
throughput or starve fresh KO evidence. The orchestrator launches the CPU BGE
notebook immediately only for **missing current pairs**. A stale-only worker
sample remains visible as maintenance telemetry, but does not shorten the
control-loop interval or suppress confirmed-blogger source breadth; maintenance
can still be run explicitly without weakening the E5+BGE production gate.

Place-name queries are recall signals, not automatic region verdicts. Ambiguous
canonical names such as `Светлый`, `Пионерский` and `Сокольники` remain in the
wide source-local fast-check ladder, but they require an explicit regional or
place-form context (`Калининградская область`, another unambiguous KO anchor,
`город Светлый`, etc.) before they count as KO evidence. This narrow guard is
needed because both semantic models can correctly see generic lifestyle text
as an “impression” while the only apparent geo anchor is a homonym (the live
regression was `светлый диван`). Historical aliases such as `Циммербуде`
remain independently usable. The guard runs before image/Gemini spend and does
not replace or weaken the mandatory dual E5+BGE text decision.

Text-policy refreshes propagate monotonically through the downstream ledger.
Candidate memory is reconciled by canonical public post URL, not a fetch-path
specific internal post id; a newly rejected post remains as a compact audit
row but loses active status and working text. If a post that already has image
evidence is later rejected by the current source/text/vector policy,
CandidateReport must persist that image row as
`rejected_text_gate`; simply omitting it from an in-memory queue rebuild is not
enough because the old row-level YDB payload would remain readable. Album/frame
diagnostics stay attached for audit, but the row is no longer actionable. The
publication finalizer also treats current candidate-memory source/text/vector
fields as authoritative over the image-admission snapshot, so a stale
`actual_scored` row cannot bypass a newer rejection between asynchronous
notebook writes. Changed-only image handoff is scoped by the current transition
timestamp: a persisted historical `status_changed_this_run=true` marker must
not consume the bounded YDB write batch on a later run. The early/final handoff
also excludes only rows that were actually persisted, not every row selected
before the writer cap. Audit tombstones are created only for already-durable
candidate-memory entities; compatibility imports of old processed posts that
never had a candidate row stay out of the durable candidate ledger. This keeps
the invalidation monotonic without turning thousands of old rejects into a new
operational/storage backlog.

The inverse transition is also evidence-preserving. If a reversible false
source/text rejection is corrected and the image ledger still contains a
complete `actual_scored` result with durable actual-image/diagnostic evidence
and a positive actual-frame count, CandidateReport restores that result instead
of downloading or scoring the album again. This does not weaken the current text gate: the
restored row becomes actionable only after the current source, KO-only,
non-ad/non-multiregion and dual E5+BGE checks accept it. Already delivered URLs
remain immutable for Gemini and Telegram delivery. When their source policy or
fingerprint changes, the finalizer may refresh only the eligibility/source
attestation with zero Gemini calls, so current confirmed metrics reconcile
without duplicate notification.

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

Telegram source monitoring is explicitly **Telethon-based** and uses only the
role-scoped discovery sessions: `RegionTalkCandidateReport` defaults to
`TELEGRAM_AUTH_BUNDLE_DISCOVERY1`, while `RegionTalkImageDiagnostic` defaults
to `TELEGRAM_AUTH_BUNDLE_DISCOVERY2`. Region Talk functional notification may
use Bot API or a role-scoped Telethon discovery identity. Production defaults
to `DISCOVERY2`; the notifier and orchestrator fail closed while
ImageDiagnostic owns that bundle, so an auth key is never connected locally
and on Kaggle at the same time. `TELEGRAM_AUTH_BUNDLE_E2E` belongs exclusively to
Codex/manual live E2E and, like generic `TELEGRAM_SESSION`, is neither read nor
passed to Region Talk scripts. `TELEGRAM_AUTH_BUNDLE_S22` remains reserved for
its separate production monitoring role. Never run two Kaggle kernels against
the same Telethon discovery key concurrently. Region Talk does not use Bot API
for reading public channel history and it never calls Telegram/VK
**public-channel** publication APIs.

The same role boundary applies to legacy publication-copy backfill. Exact
Telegram bodies are refetched through an idle discovery identity (D2 by
production default because CandidateReport normally owns D1), exact VK bodies
through read-only `wall.getById`, and both go through the current grounded LLM
writer. A local per-bundle lock protects against concurrent agents. Operator
delivery and future MTProto public delivery can retain custom premium-emoji
entity support without making a generic/E2E session part of Region Talk.

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

## MVP result: live YDB queue; report on demand

Для текущего MVP продуктовый source of truth — row-level YDB state и
Gemini-confirmed operator queue. Исторически CandidateReport создавал workbook
каждый run, когда отладка шла вручную. Теперь автоматический orchestrator задаёт
`REGION_TALK_WRITE_REPORT_ARTIFACTS=0`: после source/image/publication handoff и
compact state write worker оставляет только минимальные `output.json` и
`stage_status.json`.

Для явного offline/manual review флаг можно вернуть в `1`; тогда сохраняются:

- latest workbook: `artifacts/region-talk/candidates-latest.xlsx`;
- per-run immutable workbook: `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.xlsx`;
- companion CSV/JSON/Markdown/HTML artifacts in the same per-run folder.

Ручной XLSX остаётся cumulative/delta-aware audit artifact. Он не является
pipeline stage и его отсутствие не означает потерю candidate/image/publication
state. См. [MVP candidate report](mvp-candidate-report.md).

## External references and technical constraints

- Telegram Bot API supports HTTPS requests, JSON/form/multipart payloads and file uploads via multipart; `sendMediaGroup` sends albums of 2–10 media items. Use it only after the bot is admin in the future channel and publication is enabled. Source: <https://core.telegram.org/bots/api>.
- YDB Python SDK supports endpoint/database/auth/TLS configuration through a Driver, Query Service for YQL/transactions and Table Service for schema/bulk operations; production docs must include retries/backoff and structured errors. Source: <https://ydb-platform.github.io/ydb-python-sdk/>.
- VK wall image publishing target path is `photos.getWallUploadServer` → upload binary image → `photos.saveWallPhoto` → `wall.post` attachments, but a 2026 `vk-api-schema` issue reports `photos.getWallUploadServer` error 27 with Community Token while text-only `wall.post` works. VK image upload token type must be validated in dry-run before enabling VK image publishing. Source: <https://github.com/VKCOM/vk-api-schema/issues/242>.
- RegionTalkImageDiagnostic and the VK media prefetcher use the same read-token policy as CandidateReport: public `wall.getById` reads prefer `VK_SERVICE_TOKEN`/`VK_SERVICE_KEY` when `REGION_TALK_VK_READ_SERVICE_FIRST=1`. IP-bound user tokens are only fallback inputs. This prevents a Kaggle IP change from degrading a VK album to one prefetched frame and a nonterminal visual-review row. Existing nonterminal review rows whose only blocker is an incomplete album are reopened for bounded acquisition repair on the next run, including the persisted `actual_scored + needs_visual_review + partial` state; genuinely ambiguous fully acquired albums remain in review.

## Acceptance status for this docs pass

- Production implementation: **scheduled-runner rollout candidate**:
  CandidateReport, isolated BGE-M3 enrichment, ImageDiagnostic, publication
  finalizer/notifier and `scripts/region_talk_orchestrator.py` operate against
  live YDB under no-publication guardrails. `scripts/region_talk_scheduled_runner.py`
  adds fail-closed non-interactive preflight, cross-process single-flight,
  retained JSONL logs and `ops_run(kind=region_talk)` accounting; APScheduler
  registers three configurable local-time slots when
  `ENABLE_REGION_TALK_SCHEDULED=1`. A separate five-minute watchdog resumes
  the latest due slot after a deploy/process interruption by consulting the
  same durable ledger and bounded retry cap; it never launches beside a
  `running` or successful session.
- Short-run runtime contract: CandidateReport must not spend the tail of a
  10–30 minute debug cycle rewriting unchanged queues. The source queue handoff
  is bounded to changed/current/keyword/cursor-neighbourhood rows, duplicate
  `source_status_item` mirroring is disabled by default, candidate-memory YDB
  handoff writes only changed/refetched rows unless BGE fusion changed memory
  statuses, and both source/candidate handoffs have explicit row caps. Telethon
  network operations are guarded by per-call timeouts and cached-entity-only
  mode can defer username cache misses instead of burning FloodWait. The single
  per-run uncached Telegram resolve lane must be selected only from nonterminal
  source rows: a historical `rejected_*` audit row is retained in YDB but may
  never consume that scarce human-like resolve allowance ahead of a live
  confirmed external blogger.
- Product-heartbeat contract: `current_run_reviewable_candidates` counts only
  current-run rows whose lifecycle stage is actually ready for operator
  review. Newly-created candidate-memory rows still waiting for BGE, actual
  media acquisition or a policy refresh remain visible through
  `candidate_memory_new_this_run`, but are not called reviewable output.
- Confirmed-blogger acquisition contract: the manually researched external
  blogger registry is normalized into the same canonical source queue, not
  kept as a passive list. Supported TG/VK identities carry
  `priority_lane=confirmed_external_blogger`; their first history scans consume
  the bounded high-probability slots before ordinary cold backlog regardless of
  whether generic known-KO rescans are enabled. Registry records without a
  supported TG/VK source stay visible as unsupported coverage, not fake queue
  progress.
- Finalizer YDB contract: kind reads are keyset-paginated and the successful
  live snapshot is reused for pre-image source-attestation priority. Do not
  repeat the complete candidate/source/status scan in the same finalizer run;
  all finalizer writes remain bounded batch UPSERTs.
- Orchestrator environment contract: an explicitly supplied `--env-file` must
  exist before any live action is planned or launched, and an existing relative
  path is normalized to an absolute path before it is passed to child launchers.
  This prevents a linked worktree without its own untracked `.env` from running
  a superficially successful but Telegram/VK/Gemini-unconfigured CandidateReport.
  A deployed runtime may intentionally have no `.env`: in that case credentials
  are inherited from process secrets and the orchestrator must not forward a
  nonexistent `/app/.env` to child launchers.
- Kaggle script-upload contract: all Region Talk script kernels explicitly set
  `events_bot_disable_status_instrumentation=true`. The official Kaggle client
  sends the metadata `code_file` contents as the executable request body, so
  the shared wrapper mode that renames the worker and imports it as a sibling
  file is not valid for these uploads. Region Talk workers already persist
  stage-specific YDB heartbeats and must run as one self-contained script.
- Kaggle YDB runtime contract: workers install only the pinned pure-Python YDB
  SDK (`ydb==3.31.2`) when it is absent. Using the authorized service-account
  key, they create the documented PS256 JWT and exchange it at the fixed IAM
  REST endpoint, then give the short-lived token to `ydb.AccessTokenCredentials`.
  This avoids `ydb[yc]`, whose full Yandex Cloud SDK dependency would replace
  Kaggle's shared native cryptography/protobuf stack after the worker starts.
- Visual backlog contract: the orchestrator and ImageDiagnostic share the
  current `region_talk_visual_adjudicator_v2` / `region_talk_visual_decision_v2`
  attestation versions. A completed v2 verdict is terminal for that exact media
  manifest and must not be relaunched as an apparent stale v1 backlog.
- Scheduled notification contract: Fly defaults to the role-scoped
  `TELEGRAM_AUTH_BUNDLE_DISCOVERY2` Telethon transport; Bot API remains an
  explicit alternative. The orchestrator maps notification to the same
  `telegram:DISCOVERY2` resource as ImageDiagnostic, and both the planner and
  notifier recheck Kaggle status before connecting. The scheduled wrapper removes
  `TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION`/`TG_SESSION` from every child environment.
  After the orchestrator exits it attempts the fail-closed `DISCOVERY2`
  operator-reaction sync before recalculating the publication plan; a busy
  ImageDiagnostic/D2 lane is deferred to the next slot without failing discovery.
  A read-only
  `--dry-run` renders the queue without connecting any Telegram transport.
  The selected discovery account must already belong to
  `REGION_TALK_NOTIFY_CHAT_ID`; Bot API `getChat`/`sendMessage` also fail closed
  when that alternative is selected. The E2E session remains outside the
  application and is available only to Codex/manual live-E2E tooling.
- Fast-check KO contract: a source-local keyword hit is both an exact-post task
  (`post_link_queue_item`) and a source-priority signal. CandidateReport must
  persist `fast_check_status=ko_hit` on the corresponding `source_queue_item`,
  move it into the after-cursor priority band, and treat it as primary due on
  the next source-selection pass even if that source has previous partial scan
  evidence. This prevents fast-check from becoming an inert side queue.
- Channel/community creation: **not done**.
- Real tokens/secrets: **not introduced**.
- Publishing: **not performed**.
- Rollout gate: the dedicated least-privilege YDB service-account key,
  independent `DISCOVERY1` / `DISCOVERY2` sessions, deployment from
  `origin/main`, scheduler health and a supervised scheduled-equivalent cycle
  are installed/verified. The cycle launched CandidateReport+BGE, reported
  `image_vlm_backlog_total=0` and did not relaunch ImageDiagnostic for completed
  v2 verdicts. Operator delivery uses the idle `DISCOVERY2` identity by default;
  adding the bot to the pinned chat is needed only for the optional Bot API
  transport.
- Only after this autonomy gate is green does product work move to diversity
  ordering and generation of the actual target-channel post.
- That next stage is now implemented for newly accepted rows: final verifier
  v7 persists grounded Telegram/VK drafts with explicit source/original link
  and claim/support evidence. Public sending remains disabled; the remaining
  product gate is operator approval and public publisher enablement. Durable
  selection is implemented by `region_talk_daily_pair_antivector_v1`: after
  each scheduled discovery session it recalculates 14 days with exactly one
  external article and one Telegram/VK post per day, using separate long-range
  anti-vector histories plus a same-day cross-lane similarity guard.


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

Historical VK albums that exhausted their bounded media attempts specifically
on IP-bound token error `1130` receive one durable, versioned retry reset when
the Kaggle worker has a service read token.  The normal three-attempt cap still
applies after that migration and the reset marker prevents a retry loop.
