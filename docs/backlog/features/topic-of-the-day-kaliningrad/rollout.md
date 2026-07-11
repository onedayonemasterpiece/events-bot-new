# Этапы внедрения, проверка качества и открытые решения

## 1. Статус этого прохода

Сделана только документация. Не сделаны:

- новый project/repository/runtime;
- channel/community creation;
- YDB DDL или записи;
- Kaggle kernel/dataset;
- source crawling;
- LLM/VLM calls;
- renderer;
- Telegram/VK публикации.

## 2. Этапы

### MVP-0 — docs and contracts

Готово, когда согласованы:

- продуктовый формат `0..1` edition/day;
- source catalog boundary;
- topic/role/anti-repeat JSON contracts;
- compact YDB grain;
- Kaggle DAG, leases, status и recovery;
- Telegram/VK visual/text contracts;
- rights policy и human approval policy.

### MVP-1 — Candidate Report Only

Без публичных side effects:

- read-only source catalog projection;
- один bounded day corpus;
- E5+BGE embeddings на immutable manifest;
- 2–3 cluster algorithms в shadow comparison;
- top clusters, evidence, raw/normalized metrics;
- LLM cluster/fact/role reports;
- anti-repeat dry-run против тестовой history;
- media contact sheet и model reports;
- JSON/HTML/XLSX/operator preview artifacts.

Hard flags:

```text
TOPIC_DAY_DRY_RUN=1
TOPIC_DAY_DISABLE_PUBLISH=1
```

### MVP-2 — короткий preview/review canary

**Shadow mode** означает: система ежедневно делает полный анализ, выбирает тему, пишет текст и строит карточки, но ничего не публикует в открытый канал. Владелец вручную сравнивает результат с реальными постами. Цель — найти ошибки кластеризации, ролей, ссылок, антиповтора и media до первого публичного выпуска.

Фиксированный период shadow mode не обязателен. На коротком canary каждый конкретный выпуск сначала показывается на preview, и после ручного approve его можно публиковать. Решение принимает один configured administrator.

Preview доставляется в закрытый Telegram admin-чат только с кнопками `✅ Одобрить к публикации` / `❌ Отклонить выпуск`. Approve до слота разрешает publisher отправить выпуск в `10:00 Europe/Kaliningrad`; callback сам не публикует. Approve после 10:00 запускает publisher сразу.

Каждый день редактор независимо отвечает:

1. Какая тема действительно была главной?
2. Какие посты относятся/не относятся к ней?
3. Кто дал лучший фактический, визуальный, эмоциональный и конструктивный вклад?
4. Есть ли повтор прошлых дней?
5. Можно ли публиковать media?
6. Не пропущена ли сильная альтернативная тема?

На этом этапе калибруются thresholds/weights, а не подгоняются единичные примеры.

### MVP-3 — Telegram, manual approval

- final preview и approval id;
- approval привязан к `payload_hash`; любые изменения текста/media/ссылок требуют нового approve;
- один single-card Telegram post;
- durable publication claim;
- API/live verification;
- edit/delete/reconcile runbook;
- ни один выпуск не публикуется без ручного approve.

### MVP-4 — VK multi-photo post

- target-specific renderer;
- all-media-upload-first gate;
- wall post id reconciliation;
- tag cloud visual QA;
- partial Telegram/VK compensation;
- manual approval сохраняется.

### MVP-5 — ускоренный переход к full auto

Full auto — зафиксированный целевой режим, а не дальняя гипотеза. Manual canary должен быть коротким и с первого дня собирать structured labels: `approve|reject`, rejection reason, topic alternative, fact/link/media/anti-repeat defects. Эти labels калибруют confidence и hard gates.

Самый быстрый безопасный rollout:

1. **Manual canary:** 10 подряд рассмотренных editions через две кнопки, проверка exactly-once и критических ошибок.
2. **Full auto:** `allowed_general` и `allowed_local_history_exception` одновременно переходят в автомат. Прошедший все hard gates выпуск публикуется в 10:00; ambiguity автоматически становится `no_topic`, а не уходит на ручное рассмотрение.
3. **Silent success:** нет обязательного preview, предварительного admin alert/cancel window или success-report message. Успешный run просто создаёт публичную публикацию.

Зафиксированный activation gate: **10 подряд рассмотренных editions, 0 критических дефектов и зелёный live E2E**; затем включить full auto сразу для `allowed_general` и `allowed_local_history_exception`. Это не требует ждать 30 календарных дней или заполнения всего golden set.

Автопубликация допустима только для high-confidence editions:

- достаточное source coverage;
- winner margin;
- фактологический critic pass;
- role confidence/margin;
- anti-repeat `allow`;
- media rights/safety pass или safe deterministic fallback;
- healthy callback/lease/publication ledger;
- отсутствие unresolved `unknown` прошлого target.

Все остальные выпуски автоматически становятся `no_topic`; постоянной manual-fallback очереди после activation нет.

## 3. Golden set

Golden set содержит минимум 30 разных исторических/тестовых дней. Это объём набора примеров для replay и не означает, что перед первым ручным выпуском обязательно ждать 30 календарных дней.

Набор включает:

- один явный массовый сюжет;
- два конкурирующих сюжета;
- много несвязанных постов без темы;
- репостную волну одного владельца;
- продолжающийся сюжет с реальным новым развитием;
- повтор без нового развития;
- один факт, о котором пишут все одинаковым текстом;
- конфликтующие версии;
- видео-доминантный сюжет;
- сильный текст со слабыми/запрещёнными media;
- missing views/comments/reposts;
- локальные паблики сильно разного размера;
- одинаковое имя автора/паблика и неизвестная identity.

Разметка хранит:

- post → gold cluster/noise;
- ownership groups;
- gold winner и допустимые alternatives;
- facts/attributed claims/conflicts;
- eligible roles и role winners;
- same-topic/material-newness к истории;
- allowed/blocked media;
- допустимый public draft.

## 4. Quality gates

Стартовые acceptance targets уточняются после baseline, но обязательны сами метрики:

| Контур | Метрика |
|---|---|
| Corpus | source success/coverage, post completeness, rich-link preservation |
| Clustering | pairwise precision/recall/F1, B-cubed F1, noise quality |
| Winner | top-1 editor agreement, winner regret, support calibration |
| Facts | grounded-claim precision; unsupported public claims должны быть 0 |
| Links | author/post attribution accuracy; wrong links должны быть 0 |
| Roles | precision и abstention quality по каждой категории |
| Metrics | availability coverage, age/source normalization correctness |
| Anti-repeat | false allow, false block, material-update recall |
| Media | human preference, wrong-media rate, rights/safety violations |
| Render | mobile readability, identity/role consistency |
| Publishing | duplicate rate 0, correct partial/unknown reconciliation |
| Operations | handoff success, heartbeat freshness, catch-up completion |

Автопубликацию нельзя включать только по среднему score: zero-tolerance gates по unsupported claims, wrong attribution, rights violation и duplicate publication важнее.

## 5. A/B и model policy

- E5-only, BGE-only и fusion сравниваются на одном corpus.
- Graph/agglomerative baseline и HDBSCAN/BERTopic-like shadow не смешиваются без отчёта.
- Model revisions, prompts, calibration thresholds и render versions фиксируются в edition.
- Новый model/prompt проходит replay всего golden set.
- LLM не вызывается на весь raw corpus без необходимости: vector recall → top clusters → structured LLM cascade.
- Degraded run с одной embedding lane не автопубликуется, пока отдельная оценка не докажет безопасность.

## 6. Live E2E до production

После реализации нужны реальные проверки, не fixtures-only:

1. sandbox/private Telegram channel: single-card post, HTML links, caption, image, duplicate retry и timeout reconcile;
2. VK test group: ordered multi-photo wall post, attachment order, scheduled→live mapping, delete/edit/reconcile;
3. partial target: Telegram success + VK failure → VK-only compensation;
4. scheduler misfire/startup catch-up;
5. stale heartbeat/remote unknown;
6. auth lease collision;
7. no-topic day;
8. repeated-topic day;
9. platform API `unknown outcome` без blind retry;
10. operator preview и manual approval expiry.
11. sensitive topic rejected by default и разрешённый heritage/ОКН exception.

## 7. Риски

| Риск | Мера |
|---|---|
| Кластер склеивает разные события | entity/time fingerprint, bridge pruning, LLM split, golden set |
| Один медиахолдинг выглядит большинством | ownership grouping, один голос на group |
| Большой паблик побеждает raw views | per-source/per-age normalization, capped engagement |
| «Компетентность» выдумана | verified author profile или abstain/`best_explainer` |
| Комментарии недоступны | nullable availability; не присваивать `most_discussed` |
| Тема повторяется | exact + dual-vector + fingerprint + material-newness verifier |
| Важное продолжение ошибочно заблокировано | новый факт/стадия/result contract + human review |
| Неверное/чужое media | score all bounded album items, evidence binding, rights gate, прямая source-post ссылка на каждом использованном фото |
| Source link потерян в rich text | сохранять entities/embedded URLs до normalization |
| E5/BGE рассинхронизированы | immutable corpus, exact text-hash join, coverage barrier |
| Kaggle завершился после side effect без report | per-target claim, `unknown`, platform reconcile |
| Scheduler создаёт дубль | edition idempotency, fresh heartbeat wait, target ledger |
| YDB разрастается | ~4 rows/day, artifacts+TTL, changed-only, no post embeddings in DB |
| Upstream растёт, выпусков нет | feedback control: остановить discovery и устранить bottleneck |

## 8. Внутренний daily audit artifact

Даже при automation сохраняется компактный внутренний отчёт:

- source coverage и failures;
- число eligible posts/ownership groups;
- top-3 clusters и почему выбран winner;
- E5/BGE agreement/disagreement;
- metric availability;
- выбранные роли и abstentions;
- anti-repeat nearest history и решение;
- selected media/rights;
- Telegram/VK payload hashes и platform ids;
- final status `published|partial|no_topic|failed|unknown`.

Это артефакт, а не серия отдельных YDB rows и не обязательное Telegram-сообщение администратору. При успешном full-auto run отдельный preview, alert или post-success report в admin-чат не отправляется.

## 9. Открытые продуктовые решения

Нужны ответы владельца продукта, но они не блокируют docs-first архитектуру. Уже решены: `0..1`, один Telegram administrator, только две кнопки, late approve → publish immediately, slot 10:00, любые публичные source photos со ссылкой, полноценный голос официальных источников, sensitive-topic policy с heritage exception, 10-edition activation gate, одновременный heritage auto и отсутствие full-auto alerts.

1. Какие источники объединяются в одну ownership group?
2. Нужен ли Telegram album/video в первом релизе или начинаем с одной Bento-карточки?
3. Как называются и кем администрируются новый Telegram channel и VK community?
4. Нужно ли уведомлять администратора при `no_topic`/пропуске дня или сохранять полностью тихий режим?
5. При `Telegram=published`, `VK=failed` автоматически повторять только VK или ждать ручного решения?

## 10. Definition of Done для будущей реализации

Фича не считается готовой, пока одновременно не выполнены:

- source catalog и coverage contract доказаны live;
- golden set/replay versioned;
- compact YDB grain соблюдён;
- 100% eligible E5/BGE pairing либо осознанный no-publish degraded status;
- facts/links/roles проходят validators;
- anti-repeat проверен на реальной истории;
- media права и best-of-album подтверждены;
- Telegram/VK live E2E зелёные;
- idempotency, `unknown`, partial compensation и catch-up проверены;
- канонические docs и changelog синхронизированы;
- production release и первый live edition имеют platform evidence.
