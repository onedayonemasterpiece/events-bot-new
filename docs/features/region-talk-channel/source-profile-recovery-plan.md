# Region Talk: восстановление профилей источников и входных данных Writer

Дата среза: 2026-08-02  
Статус: **реализовано в integration; live import/capture/backfill закрываются
release evidence после merge/deploy**
Решение: **BOUNDED_RECOVERY_IMPLEMENTED_FAIL_CLOSED**

## 0. Карта реализации

- social capture: `scripts/region_talk_source_profile_capture.py` и существующие
  role-scoped adapters CandidateReport;
- capture/profile ordering and budgets:
  `scripts/region_talk_publication_finalizer.py`;
- publisher profile projection/import:
  `scripts/region_talk_publisher_profile.py` и
  `scripts/region_talk_publisher_profile_import.py`;
- explicit correction review:
  `scripts/region_talk_publisher_profile_correction_review.py`;
- Writer vNext/backfill/operator revision:
  `scripts/region_talk_publication_draft_backfill.py`,
  `scripts/region_talk_goal_notify.py` и
  `scripts/region_talk_preproduction_footer_repair.py`;
- guarded protected import:
  `.github/workflows/region-talk-publisher-profile-import.yml`.

Реализация не включает автопубликацию. Три publisher sidecar-файла получают
production effect только после protected import из точного `main`; социальные
captures требуют свободной штатной Telegram/VK role session. Фактические
receipt, readback, новые operator message IDs и 20-message audit не следует
подменять результатами unit-тестов: они фиксируются при live closure.

## 1. Что было сломано до recovery

Механизм `source onboarding` уже существует, но production-путь существенно уже исходного продуктового замысла.

До этой реализации `scripts/region_talk_publication_finalizer.py`:

1. собирает доказательства только из уже имеющейся authoritative source row, внешнего реестра и `source_memory_rows`;
2. ограничивает пакет примерно восемью фрагментами;
3. для social-источника считает достаточным один authored excerpt;
4. не читает описание Telegram/VK-источника, закреплённый пост и архив специально для профиля;
5. строит профиль только после `gemini_accept`, из остатка общего LLM-бюджета;
6. вследствие этого профиль может быть формально `ready`, хотя фактически описывает лишь текущий пост.

Это не сбой одного prompt. Недостаток появляется до Writer: в его evidence pack нет устойчивой информации о том, кто говорит, о чём источник пишет постоянно и почему его взгляд стоит внимания.

## 2. Целевой результат

Для каждой публикации Region Talk Writer должен получать два независимых пакета.

### A. `content evidence`

Текущий материал:

- точный текст;
- 1–3 сильных факта или наблюдения;
- grounded hook;
- ограничения и спорные утверждения;
- медиа-связь.

### B. `source profile evidence`

Переиспользуемый профиль автора/канала/издания:

- кто это;
- откуда и с какой позиции говорит, если доказано;
- постоянные темы;
- типичные форматы;
- характер наблюдений;
- кому полезен источник;
- чем его редакционный взгляд отличается;
- какие формулировки запрещены как неподтверждённые.

Writer не должен строить профиль источника из текущего материала.

## 3. Bounded MVP для Telegram/VK — без перфекционизма

### 3.1. Acquisition

До генерации публичного текста, при первом кандидате нового или изменившегося источника:

1. получить публичное описание источника;
2. получить закреплённый пост, если он существует и доступен;
3. прочитать **50 последних authored posts по умолчанию**;
4. разрешить bounded диапазон **30–80**:
   - 30 — достаточно при коротком архиве или ранней устойчивой сходимости;
   - 50 — штатная глубина;
   - до 80 — только если темы разнородны и профиль ещё неустойчив;
5. не скачивать медиа ради профиля;
6. не учитывать:
   - простые репосты;
   - сервисные уведомления;
   - конкурсы и рекламные интеграции без авторского содержания;
   - одинаковые дубли;
   - комментарии не от владельца источника.

Не нужен векторный индекс всего архива для MVP. Достаточны bounded fetch, детерминированная нормализация и одна LLM-компактизация на изменившийся fingerprint.

### 3.2. Детерминированный pre-digest

До LLM вычислить:

- число просмотренных и пригодных постов;
- временной диапазон;
- доли authored/repost/service;
- повторяющиеся темы и named entities;
- типичные форматы: маршрут, дневник, практический совет, историческая справка, фотоистория, профессиональный разбор;
- устойчивые self-facts только из прямых самоописаний;
- географическую базу и внешность источника;
- 8–16 разнообразных representative excerpts, а не первые восемь подряд.

### 3.3. Один LLM-вызов на профиль

LLM получает description + pinned + pre-digest + representative excerpts и возвращает:

- atomic claims с evidence IDs;
- `entity_type`;
- `stable_topics`;
- `recurring_formats`;
- `voice_and_observation`;
- `intended_audience`;
- `distinctive_value`;
- `location_scope`;
- `reader_brief`;
- `do_not_say`;
- confidence/status.

Второй кандидат того же источника не вызывает профильную модель, если fingerprint не изменился.

### 3.4. Готовность

Профиль social-источника `ready`, когда:

- есть минимум 20 пригодных authored posts; доступные описание и закреп
  сохраняются как дополнительные evidence surfaces;
- общий capture содержит минимум 30 просмотренных постов, если архив это позволяет;
- минимум три тематически различные representative excerpts;
- каждое утверждение reader brief связано с evidence ID;
- имя, число и род автора не противоречат источнику;
- externality подтверждена отдельно.

Один текущий пост больше не является достаточным доказательством.

## 4. Издания и журналы

Для издания профиль строится не по архиву из 50 статей, а по официальным страницам:

- About / миссия / тематика;
- редакционная политика или правила отбора;
- навигация и повторяющиеся форматы;
- peer-review policy для журналов;
- архив/периодичность/языки/доступ;
- exact byline и section конкретной статьи;
- local edition / regional desk check.

`editorial_pack.source_overview` остаётся короткой совместимой проекцией. Полная карточка хранится отдельно как publisher profile и переиспользуется всеми статьями домена.

Подготовлены schema-valid sidecars:

- `region-talk-publisher-profile-enrichment-archi-ru-2026-08-02.json`;
- `region-talk-publisher-profile-enrichment-peasantstudies-ru-2026-08-02.json`;
- `region-talk-publisher-profile-enrichment-rg-ru-2026-08-02.json`.

Они содержат профили Архи.ру, «Крестьяноведения», «Российской газеты» и candidate-level correction для статьи «Российской газеты».

### Важное исправление externality

Связанная статья «Российской газеты»:

- находится в `reg-szfo`;
- подписана `Денис Гонтарь (Калининградская область)`;
- является работой местного корреспондента о собственном регионе.

По действующей политике Region Talk федеральный бренд не превращает локальную публикацию в внешний взгляд. Live YDB row нужно повторно прочитать и переоценить; до этого профиль нельзя использовать для улучшения её текста.

## 5. Новая последовательность конвейера

```text
source discovered
  ↓
source identity / externality attestation
  ↓
source capture:
  description + pinned + 30–80 authored posts
  OR official publisher pages/policies
  ↓
deterministic profile digest
  ↓
one bounded source-profile LLM call on changed fingerprint
  ↓
durable reusable source/publisher profile in YDB
  ↓
candidate verifier
  ↓
content hook plan
  ↓
Writer:
  paragraph 1 = grounded hook + compact source value
  paragraph 2 = concrete material details
  ↓
Critic + deterministic validators
  ↓
operator review
```

Профиль должен быть готов **до public-copy Writer**. Он не обязан блокировать первичный semantic candidate report, но отсутствие профиля должно давать отдельный статус `needs_source_profile`, а не generic copy.

## 6. YDB

### 6.1. Новые/уточнённые projections

#### `source_profile_capture_item`

Сырая bounded capture-метаинформация без полного бессрочного архива:

- `canonical_source_key`;
- platform/source IDs;
- description and pinned evidence;
- scanned/authored/selected counts;
- date range;
- capture fingerprint/version;
- compact deterministic digest;
- representative excerpts with source message URLs/IDs;
- capture status/reason/freshness.

#### `source_onboarding_profile_item`

Существующую сущность сохранить, но повысить контракт:

- profile kind: social creator/channel or publisher;
- stable atomic claims;
- dimensions;
- evidence IDs;
- reader brief;
- do_not_say;
- profile fingerprint/version;
- freshness;
- last successful capture fingerprint.

#### Publisher import

Sidecar profile import:

- schema validation;
- source key normalization;
- exact evidence hash;
- idempotent replay;
- conflict fail-closed;
- profile upsert отдельно от candidate intake;
- candidate corrections только как review queue, не как автоматическая блокировка без live re-read.

Импортированный sidecar сохраняет собственную доказательную форму полей:
`outlet_identity` может быть строкой, а `intended_audience` и
`distinctive_value` — массивами evidence-linked утверждений. Перед Writer
backfill детерминированно проецирует эту форму в общий контракт
`{text, evidence_ids}`. Отсутствующие evidence refs добираются только из
официальных evidence items того же профиля по `supports`; если полный набор из
трёх измерений собрать нельзя, статья остаётся `needs_source_profile`.
Свежий sidecar не должен затенять более старый Writer-ready профиль только из-за
различия формы данных.

### 6.2. Не хранить лишнее

Для долгой жизни достаточно:

- description/pinned evidence;
- selected excerpts;
- aggregate digest;
- IDs/URLs/timestamps;
- hashes.

Полный текст всех 80 постов можно держать только в короткоживущем artifact/capture, если это необходимо для аудита.

## 7. Writer vNext

### Первый абзац

Обязательная структура:

1. **hook** из текущего материала, 45–110 знаков;
2. одно компактное предложение о ценности источника.

Пример:

> В Балтийск ехали «для галочки», а нашли город с суровым морским характером. Umka Blog — авторский тревел-канал, где маршруты складываются из личных открытий и деталей дороги.

### Второй абзац

- 1–2 выразительные детали;
- без URL;
- без «материал доступен по ссылке»;
- без повторного CTA;
- без полного пересказа.

### CTA

Детерминированный renderer:

- `Подробнее — в блоге Umka Blog`;
- `Подробнее — в статье на Архи.ру`;
- `Подробнее — в статье журнала «Крестьяноведение»`;
- fallback: `Подробнее — в оригинальной публикации`.

Вся CTA-фраза содержит одну ссылку.

## 8. Обязательные валидаторы

- sentence completion перед footer;
- URL отсутствует в двух editorial paragraphs;
- hook ссылается на `content_fact`, а не на publisher fact;
- source sentence ссылается на profile evidence;
- число/род/имя источника согласованы;
- запрет неподтверждённых `известный`, `ведущий`, `главный`, `крупнейший`, `обязательный`;
- запрещены `публикация позволяет`, `материал представляет ценность`, `оригинал доступен`;
- итоговый текст повторно валидируется после footer repair/normalization;
- writer/profile fingerprints учитывают profile version и evidence hash.

## 9. Backfill

После реализации:

1. импортировать publisher-profile sidecars через новый guarded importer;
2. перечитать live YDB;
3. переоценить externality связанного RG-кандидата;
4. построить social profiles для всех текущих неопубликованных confirmed candidates;
5. инвалидировать только неопубликованные stale copy revisions;
6. перегенерировать тексты;
7. отправить новые ревизии в operator chat;
8. не переносить старые реакции на новый fingerprint;
9. не автопубликовать.

## 10. Acceptance gates

### Data

- social capture: 30–80 scanned, default 50;
- минимум 20 authored или fail-closed reason;
- description/pinned status видимы;
- representative excerpts тематически разнообразны;
- 100% profile claims имеют evidence refs;
- повторный неизменившийся capture не вызывает LLM.

### Publisher

- минимум две source-level страницы;
- журнал: About + review policy + article/issue;
- local edition/byline проверены;
- sidecar schema valid;
- replay пишет 0 новых rows;
- profile update не меняет candidate decision автоматически.

### Copy

На тестовом корпусе минимум 20 ревизий:

- 18/20 имеют конкретный hook;
- 20/20 имеют grounded source sentence;
- 0 URL внутри абзацев;
- 0 оборванных предложений;
- 0 неподтверждённых prestige claims;
- 20/20 имеют одну динамическую CTA;
- RG local-correspondent case не проходит как external clean candidate.

### Safety

- publication permission не создаётся;
- manual review не повышается автоматически;
- published rows не переписываются;
- provider calls идут через общий limiter;
- Telegram session role не переиспользуется конкурентно;
- no media download for profile capture;
- dry-run и production execute разделены.

## 11. Не входит в bounded recovery

- полный векторный индекс каждого канала;
- постоянный crawl всей истории;
- биографическое расследование вне публичных self-facts;
- автоматическая оценка репутации;
- автопубликация;
- массовый LLM-анализ каждого поста.

Это можно добавить позже только при измеримом дефиците качества.
