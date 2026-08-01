# Подборки статического сайта: анализ извлечения и простой общий проект

Статус: **проектный анализ**, обновлён 2026-08-01.
Исходные требования и последующие уточнения владельца сохранены в [`podborki.md`](./podborki.md).

## 1. Короткий ответ на уточнения

### Бесплатно

Да: **сам признак бесплатности не новый**. `Event.is_free`, индекс по нему,
консервативное извлечение и страница «Бесплатно» существуют давно. Перестраивать
их ради этого проекта не нужно.

Новыми в предыдущем предложении были не только цитата, но ещё две возможности:

1. отличать явное «платно» от «источник ничего не сказал»;
2. безопасно исправлять старое `true`, если официальный источник явно сообщает
   цену или платный вход.

Сейчас входной `false` часто означает именно `unknown`, а merge практически
монотонный: `false -> true` возможен, `true -> false` при обычном обновлении —
нет. Кроме того, static exporter может повторно вывести `free` из текста
`ticket_status`, даже если канонический bool уже исправлен. Поэтому простое
last-write-wins тоже опасно: неизвестность не должна снимать подтверждённую
бесплатность.

**Решение для первой версии:** оставить `is_free` и текущий predicate страницы.
Полную taxonomy `free / paid / mixed / unknown` не делать обязательным условием
релиза. Если в этом треке исправляем provenance/correction, достаточно одного
необязательного JSON-поля:

```json
{
  "basis": "explicit_source|structured|manual|legacy",
  "evidence_quote": "Вход свободный, по регистрации",
  "source_url": "...",
  "input_hash": "...",
  "policy_version": "is-free-decision-v1",
  "locked": false
}
```

Правило merge минимальное:

- отсутствие сведений ничего не меняет;
- явное бесплатное основание включает `is_free`;
- явная цена/платный вход из источника достаточного trust может снять флаг;
- ручное решение с lock автоматически не перетирается;
- `ticket_status` влияет на подпись кнопки, но не имеет права сам создавать
  каноническую бесплатность.

Цитата обязательна только для решения из текста/OCR/LLM. Для нативного
структурированного поля парсера достаточно source URL/ID и hash. Более богатые
`mixed`, donation, registration/booking нужны только если позже появятся
отдельные индексируемые фильтры или UI этих режимов, но не для сохранения
нынешней подборки.

Любая реализация correction должна идти через открытый regression contract
`INC-2026-05-09-event-location-alias-free-dup-regressions`; в текущем документе
это пока проектное решение, а не изменение production-данных.

### События для детей

Пользователь помнит название правильно.

Фактическое состояние в `origin/main@2e9996f4`:

- в меню видна короткая подпись **«Детям»**;
- ссылка ведёт на `/poisk/?q=события%20для%20детей`;
- строка **«события для детей»** — это поисковый запрос;
- отдельной статической страницы пока нет;
- `/podborki/besplatno-s-detmi/` — другая, более узкая noindex-подборка «Бесплатные события с детьми».

Итоговое именование без изобретения нового продукта:

- пункт меню: **«Детям»**;
- название будущей страницы: **«События для детей»**;
- будущий URL: `/detyam/`;
- до готовности статической выборки меню продолжает вести в существующий поиск.

Здесь важно отличать:

- `6+` — возраст допуска;
- «семейный» или `FAMILY` — полезный кандидатный сигнал;
- «программа создана для детей 7–12 лет» — прямое основание включить событие.

BGE **может** определять детскую/семейную аудиторию и должен использоваться как
дешёвый offline recall и страховка. Проблема не в неспособности модели, а в
текущем входном документе: `related_v1` уже подмешивает `FAMILY/KIDS_SCHOOL` и
даже широкие regex-флаги «для детей/для семьи». Такой BGE во многом повторяет
исходную эвристику и не является независимой проверкой.

Нагрузку на LLM не надо увеличивать большим «жирным» запросом. Минимальный
гибрид:

1. существующие LLM-topics `KIDS_SCHOOL` и `FAMILY` дают первичный candidate
   pool;
2. общий Kaggle+BGE pass ищет пропуски и конфликты, но его audience head должен
   кодировать source/evidence text без подмешанных audience labels;
3. маленькое `audience_decision` в существующем Smart Update сохраняется для
   нового/изменённого события; если общий rich-facts запрос от этого теряет
   точность, это отдельный короткий strict-schema pass только для
   `topic ∪ BGE` кандидатов и разногласий;
4. решение кешируется по event evidence hash + prompt/schema/model hashes;
5. timeout, invalid JSON, неподтверждённая цитата или конфликт дают abstain, а
   не автоматическое включение.

Таким образом, не появляется LLM-вызов на каждый page view и не нужен повторный
вызов на каждое старое событие. BGE обеспечивает высокий recall, LLM — узкую
проверку роли и контекста: «для детей» против «детские рисунки», «лекция для
родителей», названия библиотеки, 0+/6+ и других ложных совпадений.

### Театр — только театры

Предыдущая формулировка «venue/organization entities» была чрезмерной. Для первой версии не нужна новая база сущностей и не нужен LLM.

Достаточно маленького версионированного списка официальных театров:

```text
canonical_name
name aliases
официальные source/parser IDs или домены
варианты location_name
```

Если смысл страницы — **афиша конкретных театров**, основное правило должно быть таким:

1. событие пришло из официального источника театра;
2. либо явно связано с этим театром через уже нормализованное место/организатора;
3. театр присутствует в проверенном allowlist.

Это надёжнее, чем фильтровать все события с темой `THEATRE`. Последние нужны для страницы «Спектакли», включая камерные, уличные и независимые постановки.

## 2. Scope

В этот проект входят:

- Бесплатно;
- События для детей;
- Клубы по интересам;
- Выставки;
- Популярное;
- Необычное;
- Для меня;
- Театр — афиша официальных театров;
- Спектакли — любые постановки;
- Научпоп;
- Наука;
- Сильные впечатления;
- приезжающие известные люди из России;
- зарубежные гости;
- замки, рыцари и средневековье.

Отдельный, но связанный surface этого проекта:

- страницы подтверждённых площадок `/mesta/<slug>/` с медальоном и их
  ближайшими событиями.

Не входят:

- **кино и источники кинотеатров** — не менять и не добавлять;
- **фестивали и семь типов фестивальных страниц** — у них отдельный трек, в этом документе они больше не проектируются.

## 3. Что уже реально существует

### 3.1. Данные событий

Production-срез 2026-08-01 содержит 408 актуальных/продолжающихся событий, подходящих под общий static public pool.

| Поле | Покрытие |
|---|---:|
| `topics`, валидный JSON | 408 / 408 |
| непустые `topics` | 403 / 408 |
| `search_digest` | 408 / 408 |
| `short_description` | 406 / 408 |
| `event_type` | 405 / 408 |
| место | 407 / 408 |
| poster/photo | 392 / 408 |
| прямой source URL | 408 / 408 |
| `organizer_names` | 16 / 408 |

Тематический запас:

| Сигнал | Событий |
|---|---:|
| театральные topics | 98 |
| события из известных официальных theatre parser families | 65 |
| точный `event_type=спектакль` | 66 |
| `FAMILY` или `KIDS_SCHOOL` | 72 |
| primary exhibitions | 41 |
| `SCIENCE_POP` | 7 |
| `PERSONALITIES` | 17 |
| `HISTORICAL_IMMERSION` | 11 |
| `is_free=true` | 48 |

### 3.2. Smart Update уже делает большую часть фактической работы

После создания или объединения события Smart Update уже:

- извлекает grounded rich facts с точными цитатами;
- отдельно извлекает подтверждённых организаторов;
- обрабатывает возраст;
- создаёт `search_digest`;
- вызывает LLM-классификацию `topics`;
- после изменений ставит в очередь vector sync и статическую сборку.

Нужен не новый Smart Update и не отдельный LLM-запрос на каждую подборку.
Нужны три узкие доработки, каждая с benchmark до объединения в большой prompt:

1. optional `is_free_decision/provenance` только для явного free/paid correction;
2. grounded audience decision для `topic ∪ BGE` кандидатов и конфликтов;
3. структурированные именованные участники и подтверждённый приезд.

Audience/people сначала можно piggyback в уже существующий create/rich-facts
stage, но это не догма: если расширенная schema ухудшает точность, один маленький
hash-cached adjudicator для изменившихся кандидатов дешевле и надёжнее, чем
«жирный» вызов для каждого события.

### 3.3. Общий Kaggle+BGE-конвейер уже почти есть

На `origin/main` реализованы:

- `site/scripts/static_event_bge.py` — единая точка кодирования BGE-M3;
- `site/scripts/unusual_event_semantics.py` — prototype bank, hard negatives, thresholds и abstention;
- `site/scripts/export-production-preview-data.py:build_shared_bge_and_unusual()`;
- интеграция в существующий `kaggle/StaticSiteBuilder`;
- NPZ cache, receipt, input hashes, changed-row reuse, last-good для «Необычного»;
- проверенный real-CPU candidate на 326 событиях: 30 concepts, precision `1.0`, hard-negative FPR `0`, recall `0.8`, provider calls `0`.

Но этот путь не является текущим production consumer:

- `STATIC_SITE_RELATED_MODE=pgvector`;
- `STATIC_SITE_UNUSUAL_ENABLED=0`;
- на Fly отсутствуют текущие BGE NPZ/receipt и unusual cache/last-good.

Важно исправить прежнюю формулировку. Владелец ничего не выключал. Git history
показывает, что `STATIC_SITE_UNUSUAL_ENABLED` был **сразу добавлен со значением
`0`** в `db526dbb` 27 июля и ни в одном tracked commit не становился `1`.
«Необычное» проходило реальный BGE canary только как immutable noindex review
candidate; public-root rollout не выполнялся. Старый review URL сейчас отвечает
`200`, но его события истекли, поэтому product test больше не находит необычных
concepts. `/neobychnoe/` в public root отвечает `404`.

Это не пользовательская команда «выключить» и не падение BGE. Это дефект
управления выпуском: вычисление, показ страницы и rollout были связаны одним
opt-in-флагом, а текущий approved manifest не был обязательным артефактом
каждой production-candidate сборки. Расследование сохранено в
`INC-2026-08-01-unusual-feed-disabled-by-config.md` и остаётся открытым до
свежего canary/приёмки.

Одновременно все 408 событий уже имеют актуальные `related_v1` документы и Gemini-векторы в Supabase:

- 408 документов;
- 408 `search_v3` vectors;
- 408 `related_v1` vectors;
- 0 пропусков и 0 hash mismatch;
- последний unchanged-sync проверил весь набор за 51.14 секунды с 0 provider calls.

Gemini-векторы 768d нельзя смешивать с BGE-M3 1024d. Можно переиспользовать
те же source event fields, builder и content-change machinery, но не готовый
`related_v1` audience text: он содержит derived topics/regex leakage. Новый
evidence-only `collection_semantics_v1` получает собственный text hash. Для
холодного BGE-запуска масштаб небольшой: около 408 событий плюс
fixtures/prototypes, API-стоимость равна нулю, матрица занимает примерно 4 MB.

### 3.4. BGE в проекте действительно используется шире, но это разные домены

- static shared BGE (`static_event_bge.py`) — готовый, но dormant consumer для
  «Необычного» и экспериментального BGE-related;
- Event Age BGE — отдельный включённый production pipeline оценки возраста;
- Region Talk BGE-M3 — отдельный включённый YDB enrichment pipeline;
- остальные найденные BGE/Qwen/EmbeddingGemma варианты — probes/tests.

Event Age и Region Talk подтверждают, что Kaggle+BGE как технология в проекте
работает, но их матрицы и contracts нельзя физически переиспользовать для
static events. Переиспользовать надо общий pattern: pinned model, один CPU run,
hash cache, frozen gold, hard negatives, abstention и receipt.

## 4. Главное архитектурное решение

Нужен **один batch запускающийся внутри существующего StaticSiteBuilder**, но не один универсальный алгоритм, который притворяется, что умеет всё.

Один запуск делает последовательно:

1. получает полный eligible event catalog;
2. применяет простые точные фильтры по уже известным полям;
3. один раз кодирует BGE только новые/изменённые event documents;
4. одним matrix pass считает все мягкие semantic labels;
5. добавляет существующие popularity и club-relation результаты;
6. пишет один `collection-batch-v1.json`;
7. Astro строит только те страницы, у которых соответствующий label прошёл gate.

Таким образом, конвейер один, model load один, manifest один, но способы принятия решения остаются правильными для природы данных.

### 4.1. Пять простых адаптеров

| Адаптер | Для чего | Как работает |
|---|---|---|
| `fact` | Бесплатно, приезжающие люди | использует подтверждённый факт Smart Update и source evidence |
| `hybrid_audience` | События для детей | LLM-topic/decision как primary, BGE как recall/disagreement insurance, abstain при неясности |
| `type/topic/source` | Выставки, спектакли, научпоп, театр | детерминированный фильтр по canonical fields и allowlists |
| `semantic_bge` | Необычное, Наука, Сильные впечатления, medieval experience | общий BGE event matrix, отдельные prototypes/thresholds для каждого label |
| `existing_special` | Популярное, клубы, Для меня | переиспользует существующие ranking/relation/personalization contracts |

Не надо создавать отдельный сервис, scheduler или таблицу для каждой страницы.

### 4.2. Один общий выходной manifest

Минимальный generated artifact:

```json
{
  "schema_version": "collection-batch-v1",
  "catalog_hash": "...",
  "generated_at": "...",
  "labels": {
    "kids": {
      "strategy": "hybrid_audience",
      "status": "pass",
      "items": [123, 456]
    },
    "science": {
      "strategy": "semantic_bge",
      "status": "blocked",
      "failure_codes": ["missing_gold"]
    }
  }
}
```

На первом этапе этого файла достаточно. Не нужна новая универсальная DB-таблица collection decisions. BGE scores остаются rebuildable artifacts и не записываются в `Event`.

Для каждого label manifest обязан отдельно хранить:

- `compute_status` — scorer действительно запускался или почему не запускался;
- `quality_status` — прошёл ли gold/gate;
- `publication_status` — разрешена ли именно эта страница;
- hashes модели, document contract, prototypes/head и catalog input;
- `item_count`, а для approved-empty — явную причину и проверенный supply count.

Один параметр больше не должен одновременно выключать computation, скрывать
route и обходить validation. Для production-candidate shared semantic compute
становится обязательным; publication каждого нового surface остаётся
fail-closed и включается только принятым release manifest. После публичного
релиза отсутствие env не должно убирать страницу: аварийная остановка —
осознанное incident-действие, которое удерживает последнюю валидную версию или
блокирует promotion, а не тихо публикует пустой fallback.

## 5. Что обрабатывает Smart Update, а что BGE

### 5.1. Только Smart Update/LLM: факты, которые нельзя угадывать по похожести

#### Бесплатно

Не добавлять полный `admission-v1` только ради существующей страницы. Оставить
`is_free`; при доработке correction добавить компактный `is_free_decision`.
Source-native parser пишет structured basis без LLM, а текст/OCR принимается
только с grounded quote.

Особенно важно исправить merge behavior: unknown ничего не снимает, но новый
достаточно доверенный источник с явной ценой/платным входом может исправить
ошибочный `true`. Static export больше не переугадывает canonical bool по prose
`ticket_status`.

#### События для детей

Сохранять компактное grounded-решение:

```json
{
  "value": "kids|family|none|unknown",
  "confidence": 0.93,
  "evidence_quote": "...",
  "reason_code": "explicit_child_age_range",
  "policy_version": "audience-decision-v1"
}
```

Возраст и `FAMILY/KIDS_SCHOOL` остаются supporting signals, не доказательством:
0+/6+ — возраст допуска, а не целевая аудитория. Exact-полка статической
страницы использует подтверждённые `kids|family`. Общий BGE head строит
high-recall очередь, находит пропуски и разногласия.

Сначала пробуем piggyback в существующем create/rich-facts pass. Если benchmark
показывает ухудшение большой schema, audience выносится не в вызов на каждое
событие, а в один маленький cached adjudicator только для BGE/topic кандидатов,
изменившихся строк и конфликтов. Это лучше, чем гарантированно ухудшить общий
«жирный» prompt.

#### Приезжающие люди

Использовать уже существующий `people_org_facts`, но не оставлять его плоскими строками. Минимальный результат:

```json
{
  "name": "...",
  "role": "performer|speaker|author|host",
  "appearance": "confirmed|mentioned|unknown",
  "origin_scope": "russia_nonlocal|foreign|local|unknown",
  "evidence_quote": "..."
}
```

BGE может найти кандидатов, но не имеет права определять, что человек действительно приедет, и тем более угадывать страну по имени.

Если всё же сохраняется слово «селебрити», известность должна быть отдельной ручной редакционной отметкой. Надёжное автоматическое название — «К нам едут» с полками «из России» и «зарубежные гости».

### 5.2. Только точные фильтры: BGE и новые LLM-вызовы не нужны

#### Театр

Небольшой checked-in реестр официальных театров и их источников/aliases. Основной membership — официальный source, запасной — exact alias места/организатора.

#### Спектакли

`event_type=спектакль`, общий public eligibility, occurrence-family collapse. Театральные topics используются только для поиска возможных ошибок типизации.

#### Выставки

Существующая primary-type projection. Topic `EXHIBITIONS` — очередь проверки пропусков.

#### Научпоп

Текущий `SCIENCE_POP`. Из-за семи событий страница публикуется только при минимуме, например, трёх разных occurrence families; пустота не заполняется обычными лекциями.

#### Популярное

Существующий ranking по TG/VK/social metrics. BGE не должен подменять популярность тематической похожестью.

#### Клубы

Существующие approved club identities и grounded relations. Общий BGE-классификатор не заменяет связь конкретного события с конкретным клубом.

### 5.3. Общий Kaggle+BGE: мягкие смысловые подборки

#### Необычное

Почти готово. В общий batch переносится существующий scorer без изменения значения label. После исправления stale JS gate и shadow-run можно принимать rollout отдельно.

#### Наука

Нужен отдельный label, потому что `SCIENCE_POP` сейчас объединяет слишком разные смыслы. BGE получает несколько положительных типов и hard negatives:

- научная конференция/семинар;
- участие в исследовании или наблюдении;
- публичное представление научных результатов;
- минусы: обычное обучение, реклама со словом «научный», научпоп-лекция без научной деятельности.

Сначала владелец утверждает 10–15 примеров, затем строится head. Новый `is_science` в БД не нужен.

#### Сильные впечатления

Это editorial semantic label, а не факт. Нужны наблюдаемые механики:

- физическое или пространственное участие;
- редкий доступ;
- иммерсивность;
- выраженное живое действие;
- исключить обычное событие с рекламными словами «незабываемое» и «уникальное».

Используется та же event matrix, но собственные prototypes и hard negatives.

#### Замки, рыцари, средневековье

Две причины включения:

- exact castle/fortification venue из небольшого allowlist;
- medieval/chivalric/living-history meaning из BGE head.

`HISTORICAL_IMMERSION` с 11 событиями — candidate pool, а не окончательный фильтр.

## 6. Что именно надо изменить в существующем BGE-коде

Не строить новый pipeline. Достаточны шесть ограниченных доработок:

1. Переименовать feature-specific orchestration `build_shared_bge_and_unusual` в общий collection batch adapter, сохранив unusual scorer как один consumer.
2. Разделить cache event vectors и prototype banks. Сейчас hash всего unusual prototype bank входит в совместимость cache, поэтому добавление одного label ошибочно заставит перекодировать все события.
3. Кодировать объединённый namespaced набор prototypes за одну загрузку модели; у каждого label остаются отдельные thresholds, gold и status.
4. Хранить NPZ в `float32`, соответствующем encoder contract, вместо текущего `float64` writer.
5. Ввести evidence-only semantic document contract без сгенерированных topics и
   regex-строки `Аудитория/свойства`; иначе audience BGE проверяет собственную
   подсказку. Перекодирование делается один раз в том же CPU session, после чего
   все новые heads используют одну матрицу. «Необычное» перед переходом проходит
   повторный frozen-gold canary на новом document contract.
6. Для production-candidate всегда выполнять semantic compute и его validation,
   не завязывая это на publication env. Disabled/blocked label всё равно пишет
   свежий status receipt; approved label — current manifest или совместимый
   непустой last-good.

Ключи инвалидирования:

- event vector меняется только при изменении evidence-only
  `collection_semantics_v1_text_hash` или model/document contract;
- prototype vector — только при изменении текста этого prototype;
- label decision — при изменении event vector, prototypes или head этого label;
- дата/lifecycle — пересчитывают eligibility, но не semantic vector;
- изменение UI не требует повторного BGE.

Один label с ошибкой блокируется отдельно и не ломает остальные страницы.

Для «Необычного» отдельно исправляются уже найденные reliability gaps:

- failed/non-approved quality evaluation сначала пробует валидный last-good;
- last-good, после revalidation оставшийся без items, не считается успешным;
- `migration=true` — только одноразовый baseline, не вечный production default;
- promotion проверяет delivery status, manifest hash и item count, а не только
  факт существования semantic artifact;
- stale assertion в `unusual-events-source-contract.test.mjs` чинится по
  текущему `hasEvents || weekendAvailable`, не ослабляя unusual checks.

## 7. Матрица по подборкам

| Подборка | Данных сейчас | Механизм | Основная работа |
|---|---:|---|---|
| Бесплатно | 48 candidates | существующий `is_free` + exact filter | predicate готов; точечно provenance/correction, без обязательной admission taxonomy |
| События для детей | 72 topic candidates | LLM primary + evidence-only BGE insurance | audience gold, cached disagreement adjudication, `/detyam/` |
| Клубы | 6 approved; свежих future relations мало | existing relation | источники и freshness, не новый classifier |
| Выставки | 41 exact | type | quality gate и rollout |
| Популярное | полный каталог и social metrics | existing ranking | editorial top sample, не extraction |
| Необычное | historical review canary, текущего manifest нет | existing BGE head | always-compute candidate, новый canary, current receipt, owner rollout |
| Для меня | 408-event candidate pool | existing vectors + personal ranker | profile/consent/issue/secret page, не extraction |
| Театр | 65 official-source candidates | official source/alias allowlist | утвердить список театров |
| Спектакли | 66 exact | event type | страница и gate |
| Научпоп | 7 exact topic | topic | conditional page и supply monitoring |
| Наука | нет точного label | shared BGE | definition, gold, head |
| Сильные впечатления | нет точного label | shared BGE | definition, gold, head |
| Гости из России | `PERSONALITIES=17`, но это не приезды | Smart Update people fact | structured appearance/origin + backfill |
| Зарубежные гости | аналогично | Smart Update people fact | тот же общий people extraction |
| Замки/рыцари/средневековье | 11 history candidates | venue allowlist + shared BGE | небольшой allowlist, gold, head |

Вывод по достаточности данных:

- **готовы без нового extraction:** Бесплатно (для самой выдачи), Выставки,
  Популярное, Спектакли; Театр после маленького allowlist; Научпоп условно при
  достаточном supply;
- **данные есть, нужен repair/rollout существующего механизма:** Необычное,
  Клубы, Для меня;
- **нужно компактное смысловое доизвлечение:** События для детей и приезжающие
  люди;
- **нужен новый head на общей BGE-матрице и gold, но не Smart Update-поля:**
  Наука, Сильные впечатления, medieval/knights experience.

## 8. Страницы конкретных площадок: продуктовое решение, SEO и GEO

### 8.1. Решение

Страницы площадок **нужны** и для качественного curated-набора должны быть
**indexable**, а не `noindex`:

```text
/mesta/<stable-slug>/
```

Они отвечают на самостоятельный запрос пользователя: «Что сейчас проходит
именно здесь?», дают адрес/карту, будущую афишу и одну стабильную ссылку. Для
партнёра это медальонный landing, ссылка для соцсетей и канал «исправить
сведения / передать анонс». Партнёрский статус не меняет органический порядок
событий и не превращает площадку в организатора без отдельного доказательства.

`noindex` оставляем только secret preview, partner preview, диагностическим и
не прошедшим gate кандидатам. Хорошая venue page должна индексироваться:
иначе теряется и обычный поиск, и GEO/AI-search ценность. Google требует
индексируемость/snippet eligibility для supporting links в AI search, а OpenAI
рекомендует разрешать `OAI-SearchBot` для ChatGPT Search. Это обычное качественное
SEO, специальный «текст для нейросетей» не нужен.

Нельзя автоматически делать 116 страниц из сырых `location_name + address +
city`: там уже есть aliases, разные залы, ошибки city и похожие названия. Это
thin/doorway риск и низкое качество для пользователя. Curated venue entity —
да, массовый `GROUP BY location_name` — нет.

### 8.2. Минимальная модель без нового сервиса

В проекте уже есть `docs/reference/locations.md`, location aliases/resolver и
23 уникальных `venue_brand` medallions. Нужен небольшой versioned registry:

```ts
type VenueProfile = {
  id: string;
  slug: string;
  canonicalName: string;
  address: string;
  city: string;
  officialUrl: string;
  medallionSlug: string;
  schemaType: "Place" | "MusicVenue" | "PerformingArtsTheater" | "Museum";
  status: "candidate" | "public";
};
```

Event membership — только exact canonical venue key или явно подтверждённый
alias. Fuzzy/substring matching, BGE и LLM для projection не нужны. Физическая
площадка и организация — разные сущности: venue page показывает события,
проходящие там; offsite-программа организатора не добавляется автоматически.

### 8.3. Launch gate и первая партия

Для первого indexable выпуска нужны одновременно:

- verified name/address/city, stable slug и официальный URL;
- принятый локальный медальон;
- не менее 5 независимых future event families на 3 датах при запуске;
- ручная проверка всей выдачи или первых 20 карточек с нулём чужих площадок;
- полезный SSR HTML, canonical, внутренняя ссылка и sitemap entry.

Пилот из production-среза 2026-08-01:

| Площадка | Ближайшие события/даты | Решение |
|---|---:|---|
| Янтарь-холл | 69 / 64 | первая партия |
| Филиал Третьяковской галереи | 29 / 22 | первая партия |
| Драматический театр | 20 / 16 | первая партия |
| Театр эстрады / Дом искусств | 18 / 22 будущих даты | первая партия |
| Музыкальный театр | 18 / 19 будущих дат | первая партия |
| Филармония им. Светланова | 14 / 14 | первая партия |

Кафедральный собор/Остров Канта, КОНБ/Дом китобоя, Музей Мирового океана,
Бар Бастион/Бастион Холл и Кауп сначала требуют relation/alias/city repair.

После индексации временно пустую проверенную страницу не выключаем: сохраняем
evergreen venue facts и явно пишем дату последней проверки афиши. Alias/rename —
`301`, закрытие без преемника — `410`; не `noindex` как средство canonicalization.

### 8.4. Медальон, schema и GEO

Минимальный верх страницы:

```text
[крупный медальон]
События в Янтарь-холле
Светлогорск · улица Ленина, 11
Афиша обновлена 1 августа
[Открыть карту] [Официальный сайт]
Ближайшие события
```

Только подтверждённые дополнительные факты: описание места, доступность,
транспорт, часы кассы/посещения, вложенные залы. Медальон не заменяет обычную
текстовую ссылку.

JSON-LD venue page: `Place` или точный subtype, stable `@id`
`https://kenigevents.ru/mesta/<slug>/#place`, visible `name/address/url/image`,
`sameAs`, а `geo/hasMap` только при проверенных координатах, плюс
`BreadcrumbList`. `Event.location` на leaf event page ссылается на тот же
`@id`. Listing не размечается десятками standalone `Event`: Google Event rich
result относится к отдельной странице одного события.

GEO/citability дают SSR HTML, точный первый абзац, адрес, даты, официальный
источник, видимый `updated_at`, stable entity ID и двусторонние venue↔event
ссылки. Перед выпуском надо восстановить живой `/robots.txt` (сейчас `404`) и
проверить sitemap/Search Console/Яндекс Вебмастер.

Источники решения: [Google AI features](https://developers.google.com/search/docs/appearance/ai-features),
[Google spam policies](https://developers.google.com/search/docs/essentials/spam-policies),
[Google canonical guidance](https://developers.google.com/search/docs/crawling-indexing/consolidate-duplicate-urls),
[Google Event structured data](https://developers.google.com/search/docs/appearance/structured-data/event),
[OpenAI crawlers](https://developers.openai.com/api/docs/bots),
[Schema.org Place](https://schema.org/Place).

## 9. Supabase egress: отдельный обязательный контракт

### 9.1. Наблюдаемое состояние

Production-аудит 2026-08-01:

- 408 search documents и 816 embeddings; vectors текущие sync/static paths не
  скачивают;
- vector sync читает примерно 90 139 B (88 KiB) hash projections за полный
  408-event run, но 32 запуска за сутки дали около 2.88 MB read egress и 10 148
  повторных document upserts при только 176 изменившихся embeddings;
- cold pgvector-related делает 408 компактных RPC, 24 480 строк только
  `{event_id, vector_similarity}`, всего 1 440 717 B (1.374 MiB); warm build
  использует локальный cache и не читает Supabase;
- обычная static personalization сейчас static-first и почти не читает
  Supabase; browser search может тянуть до 60 полных card snapshots, а затем
  отдельно до 60 полных `search_digest`, хотя пользователю нужно 12–24 результата.

### 9.2. Решение для подборок и venue pages

Новые static collections и `/mesta/*` не должны добавлять Supabase read egress:

```text
immutable Fly SQLite snapshot
  -> существующий Kaggle StaticSiteBuilder
  -> local CPU BGE + exact adapters
  -> compact collection/venue manifests
  -> static HTML/Object Storage/CDN
```

- ноль Supabase RPC на BGE label, страницу или площадку;
- не записывать BGE scores/collection memberships/venue manifests в Supabase;
- не скачивать vectors в builder/browser;
- auth/telemetry runtime на обычных venue/collection pages инициализировать
  лениво: callback, существующая session или явный user action, а не при каждом
  page load;
- будущий «следить за площадкой» отправляет только `venue_id + action`.

Для существующих Supabase consumers:

1. vector sync: один corpus receipt/hash или запрос только changed IDs;
   coalesce один barrier после Smart Update плюс редкий fallback, не десятки
   одинаковых outbox runs;
2. related: один bulk compact RPC или постепенный переход static related на
   локальную shared BGE matrix; не 408 HTTP requests;
3. search: один RPC возвращает IDs/similarity + минимальные display facts и уже
   compact <=420-char digest, SQL/Edge сразу схлопывает families и ограничивает
   12/24 rows;
4. personalization: сохранить static-first; компактный fallback — максимум 30
   minimal cards с corpus revision/ETag/TTL, либо удалить несуществующий RPC.

Каждая сборка пишет receipt `supabase_read_calls/rows/bytes`, cache status и
corpus revision. Новая подборка принимается только при **нулевом приросте**
Supabase egress относительно того же catalog build. Это следует официальной
рекомендации Supabase сокращать поля, строки и число запросов и использовать
кеширование: [Supabase egress guidance](https://supabase.com/docs/guides/platform/manage-your-usage/egress).

## 10. Минимальный quality gate

Для всех страниц общие проверки:

- событие active, canonical, не merged/silent;
- дата ещё актуальна, включая ongoing;
- event ID существует в `static_event_public_projection_v2`;
- occurrence-family duplicates схлопнуты;
- catalog/policy/model hashes совпадают;
- manifest не старше 24 часов;
- минимум три разные сущности/occurrence families, если у страницы нет более строгого правила.

Для нового semantic BGE label достаточно начать с:

- не менее 15 проверенных positives;
- не менее 20 похожих hard negatives;
- recall не ниже `0.80`;
- hard-negative FPR не выше `0.05`;
- ручной просмотр верхних 20 кандидатов;
- при провале label получает `blocked`, а не заполняется похожими обычными событиями.

Для BGE audience insurance критерий другой: это candidate generator, поэтому
на frozen human gold нужен recall не ниже `0.95`; precision публикации проверяет
grounded LLM decision. Gold ведётся отдельно для `KIDS_SCHOOL` и `FAMILY` и
включает hard negatives: 12+/16+ adult events, дети как авторы/тема, лекции
только для родителей/педагогов, детское слово в названии места, generic family
wording и взрослые вечерние форматы.

Это не большая ML-платформа. Нужны один policy JSON, один общий gold JSON и один generated batch manifest. Особые тесты «Необычного», клубов, популярного и персонализации остаются своими, потому что проверяют другое поведение.

## 11. Эффективная очередь реализации

### Шаг 1 — восстановить обязательный semantic compute и свежий «Необычное» candidate

- отделить compute/quality/publication state;
- production-candidate всегда запускает shared BGE, даже если старый enable env
  отсутствует или равен `0`;
- исправить last-good/empty/migration validation и stale JS assertion;
- выполнить компенсирующий fresh pinned-BGE canary на текущем catalog;
- сохранить NPZ/receipt/cache/manifest и дать владельцу стабильную current review
  ссылку; public root включать только после приёмки.

### Шаг 2 — общий batch без новых semantic страниц

- обобщить существующий BGE cache и отделить event rows от prototype banks;
- перейти на evidence-only collection document и повторно откалибровать unusual;
- создать `collection-batch-v1.json`;
- подключить существующие exact adapters;
- новые heads держать в shadow;
- доказать cold run на текущих 408 событиях и warm run с 0 re-encode.

### Шаг 3 — быстрые страницы без нового LLM extraction

- Спектакли;
- Театр после утверждения allowlist;
- Научпоп с minimum-supply;
- усиление gates Выставок;
- приёмка и включение Необычного;
- шесть `/mesta/*` pilot pages после exact relation review, robots/sitemap/schema
  gate.

### Шаг 4 — компактная доработка Smart Update, не один жирный prompt

1. Бесплатно не re-extract: оставить `is_free`; добавить decision/provenance только
   вместе с correction policy и May-9 replay.
2. Audience сначала piggyback в существующий rich-facts/create pass и прогнать
   benchmark. Если accuracy общей schema падает — вынести один маленький
   changed/candidate-only adjudicator, а не утяжелять каждый Smart Update.
3. Structured people также benchmark-ить как компактный stage; не объединять
   независимые сложные решения только ради меньшего числа названий функций.

Backfill только `topic ∪ BGE` audience candidates и `PERSONALITIES`/named-person
candidates, hash-bound и cached. Не прогонять LLM по всем 408 строкам без нужды.

После quality review:

- обновить Бесплатно;
- материализовать «События для детей» `/detyam/`;
- построить «К нам едут».

### Шаг 5 — три новых BGE heads

В рамках одного prototype bank и одного запуска:

- Наука;
- Сильные впечатления;
- medieval/knights experience.

Добавлять label только после короткой owner-разметки, не параллельно строить отдельные pipelines.

### Шаг 6 — Для меня

Переиспользовать текущий event pool/vectors. Это отдельная задача доставки персонального результата: profile, consent, daily issue, high-entropy secret URL и noindex page. Новый event classifier для неё не нужен.

## 12. Что сознательно не строим

- отдельный Kaggle kernel на каждую подборку;
- отдельную LLM-команду на каждый label;
- LLM/BGE-вызовы при открытии страницы;
- generic `tags` или десятки `is_science/is_medieval/is_emotional` колонок;
- knowledge graph театров и площадок для MVP;
- BGE как единственное доказательство цены, аудитории, личности, страны или физического приезда;
- смешивание Gemini 768d и BGE-M3 1024d vectors;
- автоматическое заполнение пустой подборки нерелевантными событиями;
- фестивальный проект;
- кино и источники кинотеатров.

## 13. Итог

Гипотеза владельца в основном подтверждается:

- **один Kaggle+BGE запуск действительно может классифицировать все мягкие подборки**;
- большая часть инфраструктуры уже реализована в треке «Необычное»;
- владелец её не выключал: production enable сразу появился равным `0`, а
  review canary не был public rollout и к настоящему моменту истёк;
- проблема не в работоспособности BGE, а в смешанном compute/publication control,
  отсутствии обязательного current manifest и привязке cache к unusual bank;
- её надо обобщить, а не создавать новые конвейеры;
- твёрдые факты остаются обязанностью Smart Update;
- BGE может и должен страховать аудиторию, но на evidence-only документе без
  утечки topics/regex и с grounded adjudication конфликтов;
- точные типы, официальные источники, popularity и club relations не надо прогонять через BGE;
- итогом одного existing StaticSiteBuilder run должен быть один проверяемый collection manifest;
- страницы площадок нужны как небольшой indexable `/mesta/*` registry с
  медальоном; массовые страницы по сырым location strings не нужны;
- новые collections/venues должны давать нулевой дополнительный Supabase egress.

Минимальное число новых смысловых механизмов:

1. один обязательный shared semantic compute внутри existing StaticSiteBuilder;
2. один общий evidence-only BGE event matrix с несколькими label heads;
3. один небольшой cached LLM adjudicator только для factual/ambiguous candidates;
4. один generated collection manifest;
5. маленькие curated registries театров/замков/площадок, без нового сервиса.

Это даёт меньше разрозненных решений, повторное использование уже оплаченной исследовательской работы и fail-closed качество выборки.
