# Компактная модель данных и Kaggle-оркестрация

## 1. Решение по БД

### MVP

Использовать ту же физическую YDB допустимо, чтобы не копировать source catalog и уже собранные post evidence. Граница доступа:

- отдельный service account нового проекта;
- `read-only` на versioned Region Talk source/post projection;
- `read/write` только на собственную таблицу `topic_day_state`;
- никаких прав на core Fly SQLite или Supabase персонализации;
- секреты Kaggle/publisher не сохраняются в YDB payloads.

Если новый runtime позже переедет в другой cloud project, read contract заменяется ежедневным `source-catalog.jsonl.zst` / `post-snapshot.jsonl.zst` с schema version, cutoff и checksum. Бизнес-алгоритм от места хранения не меняется.

### Read-only source registry contract

Каноническим grain Region Talk считается только `source_queue_item:<canonical_source_key>`. Новый проект не объединяет `source_status_item`, `online_source_item` и `source_candidate_item` в собственную копию реестра.

Минимальная projection:

```text
canonical_source_key
platform
canonical_url / source_url
handle
source_title
queue_seq
source_queue_status
source_scope / source_geo_class / source_topic_class
added_from + evidence reference
last scan/success timestamps
exclusion reason
```

Для начального local allowlist берутся подтверждённые `local_region/kaliningrad_local` rows, включая Region Talk status `rejected_local_region_source`: для Region Talk это reject, для «Темы дня» — потенциально полезный seed. На момент аудита таких rows было 103, поэтому они требуют ручной проверки полноты и ownership grouping.

Если платформа позволяет bounded fetch по дате, новая фича каждый retry перечитывает ровно окно `D-1` и не заводит вечный cursor row на каждый source. Отдельный per-source cursor допускается только если API/стоимость доказали его необходимость; тогда он должен иметь один canonical grain, а не четыре overlays.

## 2. Одна таблица, три долговечных grain

Предпочтительная физическая форма:

```text
topic_day_state
  pk           Utf8        # primary key
  kind         Utf8        # edition | run | publication
  source_day   Date?
  target       Utf8?
  status       Utf8
  updated_at   Timestamp
  expires_at   Timestamp?
  payload_json JsonDocument
```

Это не свободный catch-all: разрешены только три grain, одна строка на одну сущность, без `latest_state` overlays.

Для `run` rows физически настраивается YDB TTL по `expires_at`, а не только application-side фильтр. `edition` и `publication` остаются без TTL. YDB удаляет rows в фоне и не обещает точное удаление в момент expiration, поэтому queries всё равно фильтруют логически просроченные rows: <https://ydb.tech/docs/en/concepts/ttl>.

### `edition:{YYYY-MM-DD}` — одна строка на редакционный день

Постоянная запись:

- `source_day`, timezone, input cutoff и source-catalog version;
- состояние `planned|running|draft_ready|awaiting_approval|approved|rejected|publishing|published|partial|no_topic|failed|unknown`;
- compact coverage/counter summary;
- выбранный topic fingerprint и scores;
- top candidate summaries (обычно 3, без raw corpus);
- winning evidence post ids/URLs и finalist metric observations;
- author-role assignments с evidence ids;
- final Telegram/VK text hashes;
- asset/artifact manifest URI + SHA-256;
- E5/BGE published-topic centroid blob/ref и anti-repeat policy version;
- publication ids/URLs как удобный summary, но source of truth по target остаётся publication row.

Эта же строка является published-topic memory. Отдельная таблица/row на «антивектор» не нужна.

### `run:{run_id}` — одна строка на попытку

TTL, например 30–90 дней:

- `edition_id`, attempt number, code/config/model/prompt/render versions;
- Kaggle dataset/kernel refs без секретов;
- текущая phase/status/heartbeat;
- компактные business counters;
- last error summary;
- report/artifact URI + hash.

Полные status events пишутся в redacted `kaggle_status_events.jsonl` run-artifact. В YDB сохраняются milestones и текущий state; частые `alive` coalesce и не создают строку на каждый heartbeat.

### `publication:{edition_id}:{platform}:{target_id}` — одна строка на surface

Постоянная запись и idempotency claim:

- `payload_hash`, `idempotency_key`;
- `pending|claimed|prepared|sending|sent|scheduled|live|failed|unknown`;
- `claim_owner`, `claimed_at`, attempts;
- platform message/post id и URL;
- API/result fingerprint, но не полный чувствительный response;
- `last_error_class`, retryability и outcome certainty;
- verification timestamp.

`unknown` никогда автоматически не превращается обратно в `pending`.

## 3. Что намеренно не пишем в YDB

- raw Telegram/VK payload каждого поста;
- каждый дневной post embedding;
- все pairwise similarity edges;
- каждый rejected cluster;
- каждую картинку или frame;
- полный prompt/response каждого LLM call;
- ежедневные копии source registry;
- отдельные строки `queue/status/online/candidate` для одного source;
- агрегат `latest_state`, дублирующий row-level state;
- heartbeat event каждую минуту.

## 4. Run-artifacts и retention

Тяжёлое состояние хранится сжато в Kaggle output/Object Storage:

| Artifact | Содержание | Стартовый TTL |
|---|---|---:|
| `corpus.jsonl.zst` | все eligible posts и видимые metric observations | 30 дней |
| `corpus-manifest.json` | ids, hashes, cutoff, coverage, schema | 180 дней |
| `embeddings.npz` | E5/BGE float16/float32 arrays + document ids | 30 дней |
| `neighbors.parquet` | top-K edges, calibrated scores, fusion evidence | 30 дней |
| `clusters.json` | candidate clusters и LLM split/merge reports | 90 дней |
| `topic-pack.json` | verified facts, roles, anti-repeat, drafts | 1 год |
| `media-manifest.json` | media refs, scores, rights/safety, selected index | 1 год для winner; 30 дней для остальных |
| `render-manifest.json` | asset hashes, dimensions, source attribution | постоянно для опубликованного выпуска |
| `publish-report.json` | redacted per-target result | постоянно |
| `source-metric-baselines.parquet` | rolling medians/cohorts и sample sizes | rolling 120 дней |

YDB хранит только URI, SHA-256, schema version и ключевой summary. Media bytes non-winning candidates удаляются по короткому retention. Победившее фото из публичного source post может стать publication asset только при проверяемом original URL, обязательном link overlay и safety/identity pass.

## 5. Changed-only и бюджет записей

Нормальный день:

- 1 `edition` row;
- 1 успешный `run` row; дополнительные rows только на реальные retry attempts;
- 1 Telegram publication row;
- 1 VK publication row;
- 0 rows на raw posts/vectors/rejected candidates.

Целевой steady-state — около 4 YDB rows на день, не тысячи. Upsert выполняется только при изменении state/payload hash. Counters считаются из manifest, а не увеличиваются конкурентными sparse overlays.

## 6. DAG оркестратора

Оркестратор — лёгкий отдельный сервис по расписанию. Он не анализирует тексты и не публикует сам; он создаёт edition/run, выдаёт immutable config, запускает Kaggle, принимает callbacks, применяет leases и делает recovery.

Канонические patterns, которые надо перечитать перед реализацией:

- `docs/features/kaggle-status-framework/README.md` — signed run config, callback ledger, `event_uid`, heartbeat, leases;
- `docs/operations/cron.md` — heavy gate, materialized handoff, startup/live catch-up;
- `docs/features/tg-publishing/README.md` — durable post ids/hashes и `unknown outcome` после send timeout;
- `docs/features/vk-publishing/README.md` — hash idempotency, media upload, wall-post reconciliation;
- `docs/features/guide-excursions-monitoring/README.md` — fail-closed media и per-target evidence;
- `docs/features/cherryflash/README.md` — run-scoped Kaggle target configs и multi-platform reports.

```text
schedule/watchdog
  → claim edition(source_day)
  → Kaggle corpus worker
  → immutable corpus manifest barrier
  → Kaggle E5/BGE worker(s)
  → exact document-hash coverage barrier
  → Kaggle finalizer: clusters + LLM + anti-repeat + media + render
  → immutable approval/publication pack
  → controlled Kaggle publisher phase
  → platform verification + report_written
```

### Worker topology

Предпочтительно разделить:

1. `corpus` — network/source acquisition, role-scoped read session;
2. `embeddings` — E5 и BGE последовательно с unload либо два независимых workers и строгий join barrier;
3. `finalizer-publisher` — clustering, LLM/VLM, render и controlled publish.

Embedding worker не получает Telegram/VK publication secrets. Publisher не получает human/E2E session. Если corpus worker использует Telegram client session, у него должен быть **собственный новый role-scoped auth bundle** и lease; нельзя занимать `TELEGRAM_AUTH_BUNDLE_S22` или E2E bundle без отдельного решения.

MVP может использовать один Kaggle notebook только после memory/load smoke двух моделей и при сохранении тех же stage barriers. Один непрозрачный notebook без durable handoff не допускается.

## 7. Immutable run config

Минимальный config:

```json
{
  "run_id": "topic-day:2026-07-10:attempt-1",
  "edition_id": "topic-day:2026-07-10",
  "source_day": "2026-07-10",
  "timezone": "Europe/Kaliningrad",
  "target_publish_time_local": "10:00",
  "cutoff": "2026-07-11T00:00:00+02:00",
  "source_catalog_version": "...",
  "corpus_manifest_sha256": "...",
  "models": {"e5": "id@revision", "bge": "id@revision"},
  "policy_versions": {"cluster": "...", "roles": "...", "anti_repeat": "..."},
  "targets": ["telegram", "vk"],
  "publish_enabled": false,
  "publication_mode": "manual_canary|green_auto|full_auto",
  "manual_approval_id": null,
  "automation_policy_version": null,
  "resource_leases": ["topic_day:2026-07-10"]
}
```

Callback token передаётся отдельно по существующему signed status contract; в server state хранится только hash.

Для `publication_mode=manual_canary` реальный `publish_enabled=true` допустим только вместе с действующим `manual_approval_id`, `approved_at`, `approved_by` и неизменившимся `payload_hash`. Изменение текста, ссылок или assets после approve аннулирует approval и возвращает выпуск на просмотр.

Для `green_auto|full_auto` вместо approval обязательны `automation_policy_version`, immutable gate report, подтверждённый activation state и тот же `payload_hash`. Режим не может включиться только env-флагом без сохранённого activation decision/evidence.

Manual approval на canary-этапе приходит только из allowlisted закрытого Telegram admin-чата и только от одного configured admin user id; одного approve достаточно. Callback data содержит `edition_id`, action и короткий payload-hash fingerprint; server заново читает полный state, проверяет Telegram user id, chat id, актуальный `payload_hash` и одноразовый decision state. Callback не доверяет присланному тексту/URL и не содержит секретов.

## 8. Status contract

Milestones:

```text
kernel_started
preflight_ok
input_snapshot_loaded
embedding_e5_started / embedding_e5_done
embedding_bge_started / embedding_bge_done
clusters_built
topic_selected | no_topic
anti_repeat_checked
contributors_classified
media_scored
draft_built
platform_assets_built
publish_target_started
publish_target_done | publish_target_failed | publish_target_unknown
report_written
alive
```

`alive` содержит business progress: `sources_done/total`, `posts_done/total`, metric coverage, model stage, embeddings paired/total, clusters count, LLM calls/budget, media done/total, current target. Callback `event_uid` защищает только status event и **не заменяет** publication idempotency.

## 9. Publication state и неизвестный outcome

Перед внешним API finalizer atomically claims publication row и фиксирует `payload_hash`.

```text
pending → claimed → prepared → sending
                           ↘ sent → scheduled → live
                           ↘ failed
                           ↘ unknown
```

Если timeout/обрыв произошёл во время `sendMessage`, `sendPhoto`, `sendMediaGroup` или `wall.post`, платформа могла принять запрос. Состояние — `unknown`; blind retry запрещён до reconcile по каналу/API. Это отдельный контракт от retriable failure до external side effect.

Для VK все изображения загружаются до `wall.post`; неполный upload блокирует post. После успешного `wall.post` ошибка записи в YDB не должна приводить к повторному `wall.post`: сначала выполняется API reconciliation.

Telegram и VK независимы. `telegram=sent, vk=failed` даёт `edition=partial`; compensating run публикует только VK.

## 10. Расписание, watchdog и catch-up

Точные часы остаются product decision, но operational contract фиксирован:

- один `edition_id` на local `source_day`;
- целевой public slot — `10:00 Europe/Kaliningrad`;
- extended scheduler misfire grace;
- startup catch-up;
- live watchdog до editorial deadline;
- recovery проверяет materialized Kaggle handoff и per-target delivery, а не только факт старта cron;
- свежий heartbeat означает «ждать», а не запускать дубль;
- `no_topic` считается осознанно обработанным слотом и отправляет операторский отчёт;
- на manual canary отсутствие approve к 10:00 оставляет edition в `awaiting_approval`; approve позже 10:00 запускает публикацию сразу, без переноса на следующий день;
- после включения full auto green-gate edition публикуется в 10:00 без approval row; admin decision требуется только для явно сохранённых manual-fallback классов;
- пропущенный сегодняшний слот после фикса требует compensating run и проверки результата.

| Найденное состояние | Recovery |
|---|---|
| нет run/handoff | создать попытку |
| fresh heartbeat | ждать |
| failure до publish | safe retry/resume |
| target уже `sent/live` | не повторять target |
| один target успешен | publish-only compensation второго |
| `sending/unknown` | reconcile, без retry |
| explicit `no_topic` | закрыть слот без public post |
| deadline прошёл | alert + manual recovery |

## 11. Feedback control

Оркестратор следит за полезным выходом, а не только throughput. Если растут `posts_fetched`/embeddings, но не растут verified clusters, media-ready finalists или editions, новые discovery lanes замораживаются и сначала разбирается bottleneck. Это прямой урок Region Talk, где большой upstream stock не гарантировал publication-ready output.
