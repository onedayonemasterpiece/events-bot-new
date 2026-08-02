# Подборки статического сайта: анализ извлечения и простой общий проект

Статус: **анализ завершён; data-prep MVP слит в main, развёрнут и получил
production backfill; real Kaggle cold canary запущен, но terminal cold/warm
acceptance ещё не закрыт; PR A ontology/source-review contract реализован
fail-closed**, обновлён 2026-08-02.
Исходные требования и последующие уточнения владельца сохранены в [`podborki.md`](./podborki.md).

## 0. Состояние реализации data-prep MVP

### 0.1. Quality PR A (2026-08-02)

Ошибочно названный provisional `gold` удалён из `tests/fixtures` и перенесён в
`docs/review-data/static_collections_review_seed_v1.json`. Это по-прежнему не
owner gold и не разрешение на публикацию. `static_collection_policy.v2.json`
разделяет `child_directed`, `family_suitable`, `joint_family_activity`,
`science_pop` и `research_in_action`, сохраняет строгие
`strong_impressions`/`medieval` и оставляет все semantic heads `blocked`.

Все строки seed получили `family_id`, `occurrence_date`, EventSource refs,
raw quote и hash model document. Известные дефекты 5757, 6696/6766, 6878,
7307, 7326 и повторные families зафиксированы отдельными hash-bound receipts.
Неразрешённые строки исключены из supply; 7326 оставлен только как
`family_suitable` по прямой исходной цитате. CI запускает промежуточный
`--mode review`: он проверяет миграцию/provenance/families, но не требует
будущие PR-B owner gold, scores и winning prototypes. Публичные routes,
navigation/sitemap, киноисточники и фестивальный track не менялись.

Исходная реализация подготовлена в ветке
`integration/static-collections-data-prep-20260801`, слита PR #182 и развёрнута
из main SHA `c5e3f6bc79e912992379280644515137917a414d` на Fly v1853. Это только граница данных:
Astro routes, страницы, navigation, sitemap и публичное включение в этой ветке
не делались. Киноисточники и фестивальный extraction/page track не менялись.

Сделано:

- один обязательный `collection_semantics_v1` compute для
  `production-candidate` внутри существующего StaticSiteBuilder; запуск получает
  уже переданный Fly SQLite snapshot и не читает core events из Supabase;
- общий BGE-M3 event cache отделён от prototype banks, хранится в `float32`,
  проверяется физическим/self hash и допускает reuse event rows при изменении
  prototype; один batch пишет `collection-batch-v1.json`;
- новые semantic heads остаются `blocked` до owner-approved gold; пустой starter
  fixture не выдаётся за калибровку. Поэтому ветка fail-closed и пока не
  восстанавливает public `/neobychnoe/`;
- exact projection для «Бесплатно», «События для детей», спектаклей, выставок,
  научпопа, театральных организаций и пилотных площадок; театр и место являются
  разными ролями общего checked-in registry;
- `Event.collection_decisions` и компактный candidate-only grounded adjudicator
  для admission/audience/people. `Event.is_free` сохранён как совместимый bool;
  `unknown`/provider failure его не снимают, а exporter больше не выводит free из
  prose `ticket_status`;
- клубная evaluation стала durable outbox-задачей с одним successor, accepted
  history по `(club,event,input_hash)`, retry без уничтожения last accepted
  relation, shadow-only discovery и `interest-clubs-static-v2.json` с
  включительным шестимесячным lifecycle; production-control fixture фиксирует
  шесть approved identities и 13 подтверждённых relations;
- strict trailing debounce: автоматическая сборка ждёт 15 минут после последнего
  Smart Update; operator/calendar triggers остаются immediate; во время running
  build накапливается один follow-up;
- добавлены исполняемые bounded backfill-контуры: source-bound
  `backfill_static_collection_facts.py` и durable club relation catch-up
  `backfill_interest_club_relations.py`; оба сначала строят проверяемый plan и
  требуют явный `--apply`;
- `Database.init()` действительно мигрирует production SQLite со старого
  `UNIQUE(club_id,event_id)` на hash-versioned evaluation history с проверкой
  сохранности количества строк; одного Alembic-файла для Fly недостаточно;
- fingerprint включает collection decisions, source-bound evidence, club
  relation/evaluation truth, semantic policy/manifest identity.

Generated handoff для следующего окна:

- `site/src/data/collection-batch-v1.json`;
- `site/src/data/venue-pages-v1.json`;
- `site/src/data/interest-clubs-static-v2.json`;
- существующий `site/src/data/interest-clubs.json` сохранён для совместимости;
- BGE NPZ/receipt, semantic receipt и unusual cache остаются builder artifacts,
  а не источником смысла в Astro.

Production execution 2026-08-01:

- runtime migration добавила `Event.collection_decisions` и сохранила `4608`
  evaluation rows при замене legacy unique key на hash-versioned history;
- admission apply обработал `6` событий/`9` источников: `5` source decisions
  применены, `4` не изменили truth, deferral `0`;
- audience apply: `73` источника, `58` applied, `11` unchanged, `4` deferred;
  people apply: `38` источников, `29` applied, `1` unchanged, `8` deferred;
- club catch-up поставил `80` exact known-identity кандидатов в durable outbox;
  первая волна увеличила grounded relations с `19` до `34`, а provider-limited
  хвост остаётся retryable вместо потери last-good;
- все 6 approved identities сейчас проходят шестимесячное правило; две shadow
  identities не публикуются и не approve-ятся автоматически;
- cold StaticSiteBuilder run
  `static-site:production-secret-20260801T191228-efb845fd:c1acf5c7d03b`
  стартовал на immutable snapshot `snapshot-20260801T171228-1202c99aa1`; после
  backfill поставлен один single-flight successor `JobOutbox.id=46465`.

Не завершено и не должно маскироваться deploy/backfill evidence:

1. дождаться terminal current-catalog real Kaggle CPU cold run и выполнить warm
   run с нулём
   re-encode unchanged events и `provider_calls=0`;
2. разметить owner gold для новых heads и заново откалибровать «Необычное» на
   evidence-only document;
3. дождаться durable retry хвоста club provider deferrals и проверить terminal
   relation/evaluation counts в post-backfill successor;
4. после quality acceptance передать manifests в отдельную Astro/UI ветку.

Production-аудит, на котором основаны fixtures и contracts, выполнен read-only
на Fly SQLite 2026-08-01 15:43 UTC (`PRAGMA integrity_check=ok`; DB mtime
15:33 UTC). Дополнительной полной выгрузки ради разработки не делалось. На этом
срезе все 6 approved клубов имели подтверждённую активность в окне
2026-02-01..2026-08-01; у шести пилотных площадок было соответственно 80, 29,
23, 23, 20 и 14 current/future event rows. Эти числа — audit controls, не
runtime hardcode.

## 1. Короткий ответ на уточнения

### Бесплатно

Да: **сам признак бесплатности не новый**. `Event.is_free`, индекс по нему,
консервативное извлечение и страница «Бесплатно» существуют давно. Перестраивать
саму страницу и её простой predicate не нужно; ниже речь только о найденной
ошибке коррекции source facts.

Проверка была нужна не для оправдания новой схемы, а чтобы выяснить, есть ли
реальная проблема. Простой поиск конфликтов `is_free + positive price` её не
показывает, но source chronology production-событий показывает: **проблема
реальна уже сейчас**.

В срезе 2026-08-01 среди 411 актуальных событий 49 имеют `is_free=true`:

- событие `5370 «Точка и линия»` — подтверждённый false positive: бесплатным
  был отдельный кураторский круглый стол, ошибочно перенесённый на платную
  длительную выставку; поздние официальные источники говорят о билетах, но
  sticky `true` не снимается;
- ещё 6 из 49 free rows не имеют явного source evidence бесплатного входа и
  требуют adjudication; итого review-set — 7/49, или 14,3%;
- минимум 5 актуальных false negatives содержат прямое «вход свободный» /
  «бесплатный показ», но DB оставляет `false`; в одном случае 200 ₽ —
  добровольный донат, в четырёх также ошибочно извлечена цена.

Компактный audit receipt, который должен стать regression fixture в ветке A:

| Event ID | Verdict | Source-bound evidence |
|---:|---|---|
| 5370 | confirmed false positive | free quote относится к отдельному круглому столу; более поздние official rows: «Билеты» / Пушкинская карта |
| 7145 | confirmed false negative | «Вход свободный»; 200 ₽ прямо описаны как добровольный донат |
| 7244, 7246, 7247 | confirmed false negative | official descriptions: «БЕСПЛАТНЫЙ ПОКАЗ. РЕГИСТРАЦИЯ…»; ticket prices извлечены не из admission этих показов |
| 7287 | confirmed false negative | source прямо сообщает о бесплатных festival concerts |
| 7280, 7281, 7349, 7350, 4211, 5376 | review / abstain | у текущего `true` нет явной source-фразы о бесплатном входе; open/accessibility prose не считается admission evidence |

Audit predicate: canonical active non-silent unmerged event с effective end не
раньше 2026-08-01; source chronology читается из `event_source` по `event_id` и
`imported_at`. В fixture сохраняются минимальные quotes, source URL hashes и
verdict/reason, а не полный чужой текст. Так следующий агент может воспроизвести
вывод без доступа к текущему live snapshot.
Исправление admission у уже существующего `7287` не подключает и не меняет
festival extraction/pages; это общий ticket fact события.

В коде причина воспроизводима: merge умеет `false -> true`, но не умеет
`true -> confirmed paid`; ticket fields обновляются раздельно, а exporter ещё
раз повторно угадывает free regex-ом из prose `ticket_status`. Исторические
инциденты апреля–мая подтверждают, что это повторяющийся класс ручных ремонтов.

Новое нужно **не ради самой цитаты**. Нужна source-bound correction provenance,
чтобы различать `confirmed_free`, `confirmed_paid` и `unknown`, не переносить
факт соседнего мероприятия и безопасно отменять устаревший `true`.

**Финальное решение MVP:** сохранить `Event.is_free` как совместимый
материализованный bool, но добавить компактное admission decision с состоянием,
source/evidence quote, source URL, input hash, decided_at и manual lock.
`unknown` ничего не снимает; явный high-trust paid-факт того же события снимает
free; явный free-факт устанавливает его. Exporter перестаёт выводить
бесплатность из произвольного `ticket_status`.

`mixed` сейчас не нужен как обязательное четвёртое состояние ради этой полки:
условные/смешанные режимы сохраняются в ticket details и идут в review, а не
автоматически считаются бесплатными. Если production supply таких режимов
потребует отдельного UI/filter, taxonomy расширяется отдельным решением.

Это не массовый re-extraction 6 969 исторических строк. Один раз аудируется
current public pool из 411 строк детерминированными checks, LLM/review получает
только кандидатов: исправить `5370`, пять подтверждённых false negatives и
разобрать шесть unsupported free rows. Обязательны incident replays, включая
`INC-2026-05-09-event-location-alias-free-dup-regressions`.

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

Предыдущая фраза про будущий абстрактный allowlist действительно была сырой.
Production-инвентаризация ниже уже фиксирует конкретные театры, aliases,
источники, места, активность и пограничные случаи. Для projection не нужен LLM,
но нужен **общий маленький реестр мест и организаций**, потому что это разные
отношения. Например, театр `Act.Opus` является организацией, а играет в Доме
молодёжи; считать Дом молодёжи самим театром было бы ошибкой.

Страница `/teatr/` — не вторая плоская лента спектаклей. Это справочник
подтверждённых театральных сущностей с медальоном и компактным ближайшим
расписанием по каждой. Полная жанровая лента любых постановок остаётся на
`/spektakli/`. Так официальный большой репертуар Янтарь-холла не подавляет все
остальные театры в одной выдаче.

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

Источник чисел в этом документе — **реальная production SQLite
`/data/db.sqlite` на Fly**, прочитанная только read-only SQL, плюс receipts/logs
реальных StaticSiteBuilder runs. Committed `preview-events.json` и сайты
площадок не используются для подсчёта supply, membership или выбора формата.
Официальные сайты упоминаются только как вторичное подтверждение существования
venue-intent/официального URL; они не являются источником событий для этого
проекта. Поисковый спрос пока не измерен, потому что выгрузки Search Console/
Яндекс Вебмастера в проекте нет.

### 3.1. Данные событий

Базовый production-срез 2026-08-01 содержал 408 актуальных/продолжающихся
событий, подходящих под общий static public pool. Более поздний в тот же день
free-аудит увидел 411 eligible rows после новых импортов; это живой каталог, а
не фиксированный лимит, поэтому implementation gates привязываются к
snapshot/catalog hash, а не к числу 408.

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
| `is_free=true` | 49 |

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

1. source-bound admission correction только для конфликтных/неподтверждённых
   current facts;
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

### 3.5. Клубы: реестр уже есть, но правило показа реализовано неверно

Это не задача нового classifier. В production уже есть канонический реестр:

- `interest_club` — identity, stable slug, aliases, source anchors и редакционный
  `public_status`;
- `interest_club_event` — grounded relation клуба и события;
- `interest_club_evaluation` — verdict, quote, input hash и policy version;
- Smart Update инкрементально запускает существующую relation-проверку.

Состояние production на 2026-08-01:

- 8 identities: 6 `approved`, 2 `shadow`;
- 15 `active` relations, и все 15 имеют согласованное
  `accepted/yes` evaluation с тем же hash/policy/evidence;
- ещё 1 relation `deferred` и 3 `review`; они не считаются активностью;
- текущий exporter показывает только 3 клуба из-за сочетания окна 90 дней,
  требования двух дат и festival-фильтра;
- live root ещё старее: показывает четыре клуба из сборки 17 июля и называет
  уже прошедшую встречу будущей.

| Registry identity | Статус | Последняя подтверждённая активность | Решение на 2026-08-01 |
|---|---|---:|---|
| Game Vibes | approved | 2026-07-26 | показывать |
| Клуб исследователей нейронок | approved | 2026-07-04 | показывать |
| Клуб исследователей технологий | approved | 2026-06-10 | показывать |
| «С тобой всё в порядке» | approved | 2026-04-18 | показывать |
| АвтоРетроКлуб | approved | 2026-07-18 | показывать |
| СИНЕМАНГО | approved | 2026-03-28 | показывать |
| psychology-book-signal | shadow | — | не показывать |
| quizprosvet | shadow | — | не показывать |

Значит, в MVP должны появиться **все шесть approved клубов**, а не три. Число
не хардкодится: оно является результатом реестра и окна активности.

#### Как реестр и сведения обновляются

Текущий positive relation pipeline работает, но у него два операционных пробела:
identity создаются только reviewed fixture-командой, а post-Smart-Update
evaluation запускается best-effort background task без durable replay. Поэтому
MVP не переписывает клубы через BGE, а доводит один существующий контур:

1. На каждый новый/изменённый event после Smart Update ставится durable
   idempotent club-relation job; accepted/review/deferred пишутся в существующие
   таблицы и обычным образом coalesce static build.
2. Ошибка LLM/provider не демотирует совместимое последнее accepted relation:
   она записывает deferred evaluation и retry. Новое решение заменяет relation
   только после успешной hash-bound проверки.
3. Для **новых identities** Smart Update создаёт только `shadow` candidate, если
   source text явно называет клуб/регулярную серию и даёт grounded name/source
   anchor. Публичный `approved` остаётся owner/review решением: два разных
   occurrence dates либо явное подтверждение регулярности, без fuzzy-name
   догадки.
4. Перед MVP один bounded discovery/backfill проходит только события последних
   шести месяцев и future с клубными candidate signals. Он не прогоняет весь
   архив и не публикует shadow автоматически.
5. Название, aliases, source anchors, описание и merge/rename меняются только
   через reviewed registry update. Дата активности и `data_updated_at` выводятся
   автоматически из relations/evaluations/events, поэтому новая встреча сразу
   отражается в manifest и sitemap lastmod.

Так реестр остаётся маленьким и контролируемым, но не застывает на fixture от
17 июля. Отдельный Kaggle classifier клубов или новый reviewer-сервис для MVP
не нужен; достаточно existing outbox, existing LLM relation schema и одного
reviewable shadow report.

#### Простое правило lifecycle

Редакторское решение и свежесть не смешиваются:

- `public_status=approved` означает, что identity клуба подтверждена;
- `catalog_state=visible`, если есть будущая подтверждённая встреча **или**
  последняя подтверждённая активность попадает в включительное окно шести
  календарных месяцев; для 2026-08-01 cutoff — 2026-02-01;
- после окна клуб становится `dormant` и скрывается из общего каталога,
  навигации и sitemap; identity, slug и история не удаляются;
- следующая accepted relation автоматически возвращает его в `visible` с тем
  же ID/slug;
- `archived/merged` остаются явными редакционными решениями, а не таймером.

Не нужна новая DB-колонка `hidden`: derived state считается в exporter и
пишется в manifest. Approval уже подтверждает повторяемость клуба, поэтому
нельзя повторно требовать две встречи внутри каждого скользящего окна. Одна
новая проверенная встреча достаточна, чтобы показать approved identity.

Подтверждённая активность — `interest_club_event.active` плюс совпадающее
`interest_club_evaluation.accepted/yes` с теми же `input_hash` и
`policy_version`, связанное с canonical active non-silent event. Повторы одного
occurrence из разных источников схлопываются. Явно подтверждённое участие клуба
в уже существующем festival-scoped событии может обновить активность клуба и
показываться как встреча клуба; это не создаёт фестивальную подборку и не
расширяет фестивальный трек.

Граница scope здесь явная: `СИНЕМАНГО` — уже approved identity клуба по
интересам, а не подключение кинотеатров; festival-scoped row — уже существующее
событие, используемое только как evidence активности клуба. Ни киноисточники,
ни festival extraction/manifests/pages этим не меняются.

Manifest v2 хранит `catalog_state`, `last_verified_activity_date`,
`next_meeting_date`, counts за 6/12 месяцев и `data_updated_at=max(identity,
relation,evaluation,event)`. Его fingerprint обязан учитывать relations за все
шесть месяцев, а не только future rows. На карточке без будущей даты пишется
«Новых дат пока нет. Последняя подтверждённая встреча — …», а не устаревшее
«будущая встреча». Техническая ошибка manifest не маскируется продуктовым empty
state.

### 3.6. Театры: production-инвентаризация и решение реестра

Простой поиск по слову «театр» дал множество ложных кандидатов: `кинотеатр`,
описания вместо места и разовые фестивальные названия. Поэтому ниже только
exact entity/source audit production-событий. В v1 входят **восемь театральных
организаций**, для которых официальный source binding уже подтверждается
проектом, а не догадкой по названию.

| Театральная организация | Future / 6 мес. / 12 мес. | Official bindings |
|---|---:|---|
| Калининградский областной драматический театр | 23 / 176 / 271 | `parser:dramteatr`, `dramteatr39.ru`, TG/VK `dramteatr39` |
| Калининградский областной музыкальный театр | 20 / 110 / 153 | `parser:muzteatr`, `muzteatr39.ru`, TG `muztear39`, VK `muzteatr39` |
| Калининградский областной театр кукол | 1 / 23 / 32 | official VK `koenigkukol39` / group `20898960` |
| Калининградский театр эстрады / Дом искусств | 23 / 35 / 67 | `parser:estrada`, official host/VK `teatrestrady39` |
| Театр современной драмы «Акт.Опус» | 1 / 27 / 39 | `actop.us`, official VK `actopustheatre` |
| Театр «Третий этаж» | 0 / 31 / 43 | official VK `tretazh` |
| «Мой театр» | 1 / 8 / 9 | official VK `moyteatr_kld` |
| «Город-Театр», Железнодорожный | 2 / 0 / 12 | official VK `gorodteatr39` |

Воспроизводимый контракт именно этой таблицы:

```text
eligible = canonical AND unmerged AND active AND non-silent
future   = effective_end >= 2026-08-01
6 months = event range overlaps 2026-02-01..2026-07-31
12 months= event range overlaps 2025-08-01..2026-07-31
member   = exact official source binding
        OR exact normalized organizer alias
        OR exact normalized canonical venue tuple/approved alias
```

Official source binding разбирается структурированно: parser ID, exact TG
username, VK owner/group ID или equal/subdomain official hostname; SQL
substring не используется. Поэтому final counts выше предварительного
exact-venue-only probe: они намеренно включают подтверждённые offsite rows
`Act.Opus`, «Моего театра» и «Город-Театра». Counts являются event rows, а не
суммой parser-only rows и не family count. Ветка A сохраняет этот registry и
контрольные ID sets как минимальный fixture/receipt, чтобы любые изменения
чисел объяснялись snapshot hash и reason breakdown.

Зафиксированные home venue tuples из существующего location reference:

- Драматический театр — Мира 4, Калининград;
- Музыкальный театр — Мира 87, Калининград;
- Театр кукол — Победы 1А, Калининград;
- Театр эстрады / Дом искусств — Ленинский проспект 155, Калининград;
- «Третий этаж», сцена «Чердак» — Коммунальная 6, Калининград;
- «Мой театр» — Больничная 24, Калининград;
- «Город-Театр» — Черняховского 9, Железнодорожный;
- `Act.Opus` часто играет в Доме молодёжи, Октябрьская 76, но это shared venue,
  а не alias организации; offsite source rows не переписываются на этот адрес.

Здесь counts — event rows до family collapse; разные даты одного спектакля
сохраняются, дубли одного occurrence схлопываются позже. У восьми организаций
вместе 71 future row на 48 start dates. «Город-Театр» имеет свежие будущие
offsite-события в Гусеве, поэтому его нельзя скрывать только из-за отсутствия
домашних событий за предыдущие шесть месяцев.

Осознанные исключения из v1:

- **Янтарь-холл** — подтверждённая performing-arts площадка, но не theater
  organization: он получает `/mesta/`, а его постановки — `/spektakli/`, но не
  отдельную организацию в `/teatr/`;
- «Солёная ворона» зарегистрирована как кафе и в основном содержит
  music/dining — не официальный театр;
- «Содружество актёров» Николая Захарова имеет только qTickets/location rows без
  подтверждённого official binding — candidate до evidence;
- «Театр Слово», «Театр-школа 217», «Сказки Холмогорья» и generic «Театр» в
  Советске имеют только строковые/location evidence — не включать;
- `кинотеатр` и амфитеатр — лексические false positives; киноисточники не
  затрагиваются.

Если ожидаемый театр, например «Тильзит», отсутствует среди восьми, это не
молчаливое отрицание его существования, а `candidate_missing_source`: добавить
его можно после появления/привязки официального источника. Реестр должен иметь
review-отчёт excluded/candidate причин, чтобы расширение было управляемым.

Medallion coverage сейчас 4/8: готовые entries есть у Драмтеатра, Музыкального,
Дома искусств и `Act.Opus`; у Театра кукол, «Третьего этажа», «Моего театра» и
«Город-Театра» их нет. Это не причина портить membership: registry хранит
`medallion=null` и review reason, а UI использует честный text/initial fallback
до отдельной source/provenance-приёмки изображения. Substring-matching
существующего organizer-medallion JSON не становится entity resolver.

#### Механика связи события с театром

Один общий `place/organization registry`, а не отдельные несогласованные
театральный и venue-списки, хранит:

```text
stable entity ID + slug + kind(place|organization|mixed)
canonical name + exact aliases
official parser/source/domain bindings
canonical venue binding, address/city и medallion
status(public|candidate|dormant|excluded)
flags: official_theatre, venue_page_candidate
```

Membership строится в строгом порядке:

1. canonical `venue_id` связывает событие с театральным **местом**;
2. dedicated parser/source binding связывает событие с театральной
   **организацией**, включая её подтверждённое offsite-мероприятие;
3. exact grounded organizer alias — запасной путь организации;
4. конфликт venue/organization сохраняет оба ID и идёт в review, а не
   склеивает сущности;
5. generic Telegram/VK/агрегатор, topic `THEATRE` или слово «театр» сами по себе
   никогда не дают public membership.

Для этого exporter должен получать из `event_source` не только URL, но и
`source_type`, parser/source ID, username/domain и trust. Текущий
`organizer_names` технически пригоден, но в production пока почти не даёт
theatre matches, поэтому source и exact venue остаются основой. BGE может
выявлять подозрительные пропуски типизации, но не определяет entity membership.
`/teatr/` выводит восемь entity-секций с ближайшими occurrence IDs;
`/spektakli/` по-прежнему строится по `event_type=спектакль`. Offsite event из
официального источника театра остаётся программой организации; guest event в
его canonical venue остаётся афишей места; при пересечении допустимы два
`theatre_ids` с разными reason codes. Киноисточники и фестивальные страницы
этим реестром не добавляются и не меняются.

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

### 4.3. Когда запускается BGE и кто кого ждёт

Отдельный BGE notebook не нужен. Фактическая цепочка уже почти правильная:

```text
import / merge
  -> Smart Update и grounded facts
  -> vector sync (~90 секунд)
  -> один coalesced static_site_build
  -> immutable Fly SQLite snapshot
  -> один existing StaticSiteBuilder kernel на Kaggle
       export полного catalog
       shared BGE для новых/изменённых documents
       exact/semantic manifests
       related projection
       Astro build + checks
  -> host validation / publication
```

**Решение:** shared BGE всегда выполняется внутри этого же StaticSiteBuilder,
после материализации frozen catalog и до related/Astro. Astro в том же kernel
синхронно ждёт manifests. Нет второго upload snapshot, второго запуска Kaggle,
отдельного resource lease или внешней связи «генератор ждёт классификатор».

Сегодня Smart Update ставит static build на `+15 минут`, а atomic coalescing уже
держит максимум один running и один pending job. Но текущая реализация не равна
строгому условию владельца:

- действует максимум 30 минут от первого effect;
- если pending уже стал due, новый Smart Update оставляет запуск `now`, вместо
  нового полного quiet window.

В ветке data-prep это меняется для trigger=`smart_update` на простой
**trailing debounce**: каждый новый effect переустанавливает единственный
pending job на `latest_effect_at + 15 минут`; 30-минутный cap для этой причины
не форсирует дорогой build посреди активной серии. Calendar rollover и явный
operator request остаются осознанными immediate overrides и могут запускаться
внутри этих 15 минут; quiet-window contract относится именно к автоматическим
`smart_update` effects. Это означает «одна логическая сборка после 15 минут без
свежего Smart Update», а не обещание физического exactly-once:
доказанный внешний сбой может потребовать retry, а совпадающий успешный
fingerprint даёт no-op без нового Kaggle push.

Если обновление приходит во время running build, frozen snapshot не меняется.
Создаётся ровно один pending follow-up, последующие effects переносят его на
свои `+15 минут`; текущая сборка заканчивает честный manifest своего snapshot,
а follow-up берёт последние данные. Никакого параллельного второго kernel.

#### Взаимоотношение BGE и audience LLM

Grounded `audience_decision` из Smart Update является publication truth и уже
попадает в snapshot. BGE в текущей сборке — high-recall страховка и detector
разногласий. Он не может задним числом мутировать замороженную DB.

- подтверждённые `kids|family` из snapshot попадают в `/detyam/`;
- новый BGE-only кандидат получает `needs_adjudication` и в эту сборку не
  включается;
- validated receipt возвращает только compact candidate IDs/hashes на Fly;
- один durable candidate-only LLM job проверяет их и при изменении canonical
  decision пользуется обычным coalesced follow-up build;
- второй Kaggle run для того же fingerprint не запускается.

Перед первым public rollout выполняется targeted backfill по `topics ∪ BGE`,
поэтому стартовая страница не ждёт same-build callback. В steady state временный
пропуск неясного события предпочтительнее неточного включения; last-good/public
страница остаётся precision-safe.

## 5. Что обрабатывает Smart Update, а что BGE

### 5.1. Только Smart Update/LLM: факты, которые нельзя угадывать по похожести

#### Бесплатно

Оставить `is_free` совместимым полем страницы, но добавить компактный
source-bound `admission_decision` (`confirmed_free|confirmed_paid|unknown`) с
evidence/source/hash/lock. Source-native parser пишет structured basis без LLM,
а текст/OCR принимается только с grounded quote.

Unknown ничего не снимает; новый достаточно доверенный источник с явной
ценой/платным входом **того же события** может исправить ошибочный `true`.
Static export больше не переугадывает canonical bool по prose `ticket_status`.
Targeted backfill ограничен текущими конфликтами/unsupported facts; полный
исторический каталог не прогоняется.

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

Checked-in реестр восьми уже перечисленных организаций, их official
source/parser/domain/VK IDs и exact aliases. Membership — union независимых
оснований `official_source | exact_organizer | exact_canonical_venue`, с
сохранённым reason и допустимой multi-membership; source сохраняет offsite
программу. Янтарь-холл остаётся venue, а не theatre organization. BGE, topic и
substring не включают событие.

#### Спектакли

`event_type=спектакль`, общий public eligibility, occurrence-family collapse. Театральные topics используются только для поиска возможных ошибок типизации.

#### Выставки

Существующая primary-type projection. Topic `EXHIBITIONS` — очередь проверки пропусков.

#### Научпоп

Текущий `SCIENCE_POP`. Из-за семи событий страница публикуется только при минимуме, например, трёх разных occurrence families; пустота не заполняется обычными лекциями.

#### Популярное

Существующий ranking по TG/VK/social metrics. BGE не должен подменять популярность тематической похожестью.

#### Клубы

Существующие approved identities и согласованные grounded relations. Exporter
меняет только lifecycle projection: approved + future или подтверждённая
активность за шесть календарных месяцев; после окна hidden/dormant, следующая
встреча автоматически возвращает. Все шесть текущих approved identities
показываются. Общий BGE не заменяет relation.

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
| Бесплатно | 49 `true`; 1 confirmed FP, 5 confirmed FN, 6 review | existing bool + source-bound admission correction | targeted current-pool repair, убрать exporter re-inference, incident replay |
| События для детей | 72 topic candidates | LLM primary + evidence-only BGE insurance | audience gold, cached disagreement adjudication, `/detyam/` |
| Клубы | 8 identities: 6 approved, 2 shadow; 15 accepted active relations | existing registry/relation | 6-month lifecycle manifest; сейчас должны показываться все 6 approved |
| Выставки | 41 exact | type | quality gate и rollout |
| Популярное | полный каталог и social metrics | existing ranking | editorial top sample, не extraction |
| Необычное | historical review canary, текущего manifest нет | existing BGE head | always-compute candidate, новый canary, current receipt, owner rollout |
| Для меня | 408-event candidate pool | existing vectors + personal ranker | profile/consent/issue/secret page, не extraction |
| Театр | 8 verified organizations; 71 future rows / 48 dates | official source + exact organizer/venue registry | registry уже зафиксирован; сохранить reason/multi-membership и excluded review |
| Спектакли | 66 exact | event type | страница и gate |
| Научпоп | 7 exact topic | topic | conditional page и supply monitoring |
| Наука | нет точного label | shared BGE | definition, gold, head |
| Сильные впечатления | нет точного label | shared BGE | definition, gold, head |
| Гости из России | `PERSONALITIES=17`, но это не приезды | Smart Update people fact | structured appearance/origin + backfill |
| Зарубежные гости | аналогично | Smart Update people fact | тот же общий people extraction |
| Замки/рыцари/средневековье | 11 history candidates | venue allowlist + shared BGE | небольшой allowlist, gold, head |

Вывод по достаточности данных:

- **готовы без нового extraction:** Выставки, Популярное, Спектакли, Театр по
  уже зафиксированному реестру; Научпоп условно при достаточном supply;
- **данные есть, нужен repair/rollout существующего механизма:** Бесплатно,
  Необычное, Клубы, Для меня;
- **нужно компактное смысловое доизвлечение:** События для детей и приезжающие
  люди;
- **нужен новый head на общей BGE-матрице и gold, но не Smart Update-поля:**
  Наука, Сильные впечатления, medieval/knights experience.

## 8. Страницы конкретных площадок: продуктовое решение, SEO и GEO

### 8.1. Решение и его продуктовое основание

Корректная формулировка не «всем площадкам обязательно нужны SEO-страницы», а
такая: **у страниц площадок есть самостоятельный пользовательский сценарий и
достаточный supply для дешёвого пилота; поэтому делаем шесть curated-кандидатов,
проверяем спрос и только прошедшие gate страницы индексируем**:

```text
/mesta/<stable-slug>/
```

Основной job пользователя: «Я знаю это место или увидел его в карточке; покажи,
что ещё там будет». Это отличается и от категории «Театр», и от организатора,
и от произвольного поиска. Страница позволяет:

- продолжить путь из события к другим датам того же места;
- проверить точный адрес, город, карту и официальный сайт до покупки/поездки;
- сохранить или переслать одну стабильную ссылку на расписание места;
- после окончания одного события не возвращаться в общий каталог и не вводить
  имя площадки заново.

Для партнёра это вторичная, а не главная причина: готовый медальонный landing,
ссылка для сайта/соцсетей и канал «исправить сведения / передать анонс». В P0
не нужны кабинет, отчётность или платное ранжирование. Партнёрский статус не
скрывает чужие события и не превращает площадку в организатора без отдельного
доказательства.

Для KenigEvents это стабильный evergreen-узел между быстро истекающими event
pages, нормализованный `Place @id` и практическая причина довести venue aliases
до качества. Но entity/SEO-польза сама по себе не доказывает продуктовый спрос.

### 8.2. Что доказано, а что пока гипотеза

- Доказан **supply**: место есть у 407 из 408 актуальных событий, а у шести
  сильных кандидатов — от 14 до 69 ближайших событий и много разных дат.
- В текущей карточке имя/адрес и медальон уже являются заметным decision fact,
  но не дают продолжения на другие события места. Значит, information scent
  уже есть, а destination отсутствует.
- Внешняя выдача содержит самостоятельные venue schedule pages, например
  [официальную афишу Янтарь-холла](https://www.yantarhall.com/afisha/),
  [филиал Третьяковской галереи](https://kaliningrad.tretyakovgallery.ru/) и
  [филармонию](https://filarmonia39.ru/); это подтверждает существование типа
  намерения, но **не его частотность для нашего сайта**.
- В репозитории нет достаточной выгрузки Search Console, Яндекс Вебмастера,
  Wordstat или внутренних venue-query logs. Поэтому нельзя утверждать, что спрос
  уже измерен или высок.

Именно поэтому решение — ограниченный пилот, а не массовый rollout. Альтернативы
не дают того же результата:

| Вариант | Что полезного | Почему не заменяет curated venue page |
|---|---|---|
| Предзаполненный поиск | дешёвый ad-hoc fallback | результат меняется, нет verified venue facts, stable entity URL и партнёрской ссылки |
| Страница организатора | показывает программу организации | организация может работать offsite, а зал принимать чужих организаторов |
| Постоянный `noindex` | годится для preview/партнёрской сверки | не даёт поисковой/GEO-ценности и не должен попадать в sitemap |
| Вообще без страницы | нулевая поддержка | оставляет тупик после event page; нормально только для слабых и непроверенных мест |

### 8.3. Index/noindex — не компромисс, а состояния

`candidate` сначала публикуется только в secret/partner preview: `noindex`, без
sitemap и публичной навигации. `public` получает `index,follow`, self-canonical,
sitemap и внутренние ссылки из event pages только после relation/content gate.

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

Есть общий blocker: live-корень 2026-08-01 отдаёт
`meta robots=noindex,nofollow`, а `/robots.txt` отвечает `404`. Поэтому до
исправления общей indexability даже принятые площадки остаются preview; нельзя
объявлять SEO/GEO-релиз только потому, что route построился.

### 8.4. Минимальная модель без нового сервиса

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

### 8.5. Launch gate и первая партия

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

### 8.6. Медальон, schema и GEO

Минимальный верх страницы:

```text
[крупный медальон]
События в Янтарь-холле
Светлогорск · улица Ленина, 11
Афиша обновлена 1 августа
[Открыть карту] [Официальный сайт]
События, которые мы нашли
```

Если нет измеренной полноты официальной афиши, страница не обещает «все
события» и честно говорит «События, которые мы нашли». Только подтверждённые
дополнительные факты: описание места, доступность,
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

### 8.7. Как пилот отвечает «нужно ли это людям и партнёрам»

Пилот оценивается не раньше шести недель после реальной индексации и накопления
не менее 100 совокупных человеческих сессий; при меньшей выборке решение только
продлевается. Минимально собираем:

- входы по referrer и venue-name queries из Search Console/Яндекс Вебмастера;
- `venue -> event`, официальный сайт/билет, карта — как одну meaningful action;
- возвраты/переходы из event medallion;
- correction reports и фактическое использование ссылки площадкой;
- freshness и ошибки relation.

Рабочий go-критерий для расширения: meaningful action не ниже 20% после
достаточной выборки, ноль критических чужих площадок, freshness gate выполнен
не менее чем у 95% сборок и venue-intent impressions появились хотя бы у трёх
из шести страниц. Это пилотные пороги, а не уже измеренный baseline. Если venue
page не лучше предзаполненного поиска или поддержка aliases систематически
дороже пользы, реестр не масштабируем. Партнёрские переходы усиливают решение,
но их отсутствие само по себе не отменяет пользовательскую пользу.

### 8.8. Формат MVP: расписание с календарём-навигацией

Прямой ответ: **не календарная сетка как основное содержание и не лента больших
карточек**. Для venue-intent основной surface — компактное хронологическое
расписание, а календарь служит быстрым переключателем даты.

#### Что показывают наши production-данные

Read-only срез `/data/db.sqlite` на 2026-08-01: только active canonical,
`silent=false`, текущие/будущие события, exact venue-name aliases. Это raw
canonical event rows до финального venue relation review; сайты площадок в
расчёте не использовались.

Воспроизводимый query/result сохранён локально в
`artifacts/codex/podborki-venue-format-20260801/` (не коммитится).

| Площадка | Занятых start dates | Медиана событий на дату | Максимум на дату | Событий-диапазонов | Сигнал для формата |
|---|---:|---:|---:|---:|---|
| Янтарь-холл | 64 | 1 | 5 | 7 | длинный schedule, календарь полезен как переход |
| Третьяковская галерея | 22 | 1 | 3 | 6 | нужны отдельные «Идёт сейчас» и события по датам |
| Драматический театр | 16 | 1 | 3 | 0 | повторяющиеся постановки на разных датах |
| Дом искусств | 22 | 1 | 2 | 0 | почти чистая хронология |
| Музыкальный театр | 19 | 1 | 2 | 1 | повторяющиеся постановки на разных датах |
| Филармония | 14 | 1 | 1 | 0 | один концерт — одна дата, идеален список |

У всех шести медиана — **одно событие на занятую дату**. Поэтому большая
month-grid в основном покажет точки/единицы и заставит пользователя открывать
дни, чтобы узнать названия. Она скрывает полезную информацию вместо ускорения.

В то же время полная выдача площадки содержит от 14 до десятков событий. Лента
больших discovery-карточек превращает известный venue-intent в длинный скролл,
повторяет афиши одного спектакля на разных датах и расходует визуальный вес на
то, что пользователь уже выбрал — место. Постеры в production покрыты хорошо,
но здесь они нужны как миниатюра для узнавания, а не как главный способ
навигации.

| Формат | Сильная сторона | Проблема на наших данных | Роль в MVP |
|---|---|---|---|
| Большая month-grid | быстро выбрать известную дату | клетки почти всегда содержат одно событие и скрывают название | только вторичный date picker |
| Большие event cards | вдохновение и визуальный browse | длинный скролл, повторные показы одной постановки, лишний media weight | не использовать как основную выдачу |
| Компактный список по датам | сразу видны дата, время и название | менее «журнальный» вид | основной schedule |
| Schedule + date picker + ongoing shelf | объединяет быстрый выбор и длинные форматы | требует occurrence-aware projection | рекомендуемый единый шаблон |

Отдельно важна occurrence-семантика. В Драмтеатре 11 строк относятся к трём
повторяющимся exact-title families, в Музыкальном театре 9 строк — к четырём.
Для venue schedule это не мусорные дубли: разные даты показа надо сохранить.
Схлопываются только повторы **одного occurrence** из нескольких источников;
manifest хранит `family_id` и список occurrence date/time. В общем списке одно
название может повторяться компактной строкой под каждой релевантной датой.

#### Один адаптивный шаблон, не два разных продукта

```text
[медальон площадки]
Название · адрес · город · обновлено
[карта] [официальный сайт] [исправить сведения]

Идёт сейчас                         # блок скрыт, если пуст
[компактные cards диапазонов]

Ближайшие даты: 2 · 3 · 5 · 8 ... [Календарь]

Август
Сб, 2 августа
19:00  [thumbnail] Название · тип · возраст · цена/билет

Вс, 3 августа
18:00  [thumbnail] Название · тип · возраст · цена/билет
```

- Venue hero большой: медальон и факты места. Большого hero события нет — иначе
  нужен спорный editorial/partner ranking.
- `Идёт сейчас` содержит один раз выставки и другие диапазоны с честным
  `до <дата>`, а не размножает их на каждый календарный день.
- Основной список сгруппирован по датам и отсортирован по времени; poster —
  небольшая lazy-loaded миниатюра.
- Горизонтальная лента ближайших дат видна сразу, month calendar открывается по
  кнопке. Выбранная дата фильтрует/прокручивает уже SSR-rendered schedule.
- На mobile используются компактные time rows; на desktop — существующие
  listing cards умеренной плотности. Это развитие уже реализованных
  `DateListingSurface`, `ListingEventCard` и `MobileListingRailSurface`, а не
  новый календарный UI с нуля.
- Если `Идёт сейчас` пуст, шаблон просто начинается с расписания. Отдельные
  layouts для музея и театра не нужны.

#### Почему не большие карточки хотя бы сверху

В MVP — не нужны. Медальон уже даёт сильный визуальный якорь, а «главное событие
площадки» требует нового ranking contract и может выглядеть как скрытая платная
позиция. После пилота можно проверить маленький блок из 1–3 редакционно
подтверждённых рекомендаций, но только как отдельную гипотезу; он не должен
мешать расписанию или менять органический порядок.

#### URL, SEO/GEO и измерение

- indexable canonical остаётся только `/mesta/<slug>/`;
- выбор даты — client state или `?date=YYYY-MM-DD` с canonical на venue root;
  отдельные crawlable страницы на каждую дату не создаются;
- все названия и даты присутствуют в SSR HTML: нейросети и поиск получают
  читаемое расписание, а не пустой JS-calendar;
- пилот измеряет `date_select`, `calendar_open`, переход в event, билет/официальный
  сайт/карту и долю дошедших до поздних дат; analytics отправляется компактно
  после действия, а не создаёт Supabase read на page load.

Решение о масштабировании venue registry и решение о смене формата разделены.
Шесть страниц проверяют relation quality и один общий schedule-template. После
подтверждения шаблона новые площадки добавляются по тому же gate; число `6` не
становится продуктовым лимитом.

## 9. Egress всего конвейера, а не только Supabase

### 9.1. Ответ на возражение про Fly

Да: предыдущая фраза была неполной. Fly SQLite выбран **не потому, что исходящий
трафик Fly бесплатный**. По действующему тарифу Fly для Европы public egress
стоит `$0.02/GB`; реальный тариф организации надо сверить в billing. Причины
переиспользовать этот snapshot другие:

1. Fly SQLite — канонический core-каталог событий и площадок, тогда как Supabase
   в проекте отвечает за auth/search/personalization, а не за вторую копию core.
2. Production StaticSiteBuilder уже обязан передать один immutable snapshot в
   Kaggle для обычной статической сборки.
3. Если вычислить все подборки **внутри того же build**, snapshot и model load не
   передаются второй раз. Предельная стоимость новых labels — compact manifest и
   HTML, а не ещё один catalog transfer.
4. Отдельный collection job или чтение тех же карточек из Supabase создали бы
   второй оплачиваемый путь и две расходящиеся версии каталога.

То есть обещание такое: **нулевой прирост Supabase egress и почти нулевой
предельный трафик на подборку в уже необходимой сборке**, но не «нулевая общая
стоимость static pipeline». Новый label не имеет права повышать частоту полных
сборок.

### 9.2. Измеренный end-to-end путь

```text
Fly /data/db.sqlite
  -> immutable snapshot + site source + warm caches
  -> private per-run Kaggle dataset
  -> Kaggle CPU: export + exact adapters + BGE + Astro
  -> root/review/browser archives обратно на Fly
  -> Fly publisher -> Yandex Object Storage -> CDN -> browser
```

Production success 2026-08-01 (`404` events) дал следующую базу:

- SQLite snapshot: `310,312,960 B` (`295.94 MiB`);
- warm related cache: `20,611,144 B` (`19.66 MiB`);
- production tree: `642,764,482 B`, 3 289 файлов;
- Kaggle outputs, скачанные на Fly: `232,841,551 B` — root tar, review tar и
  browser evidence;
- cold Supabase related: 404 compact RPC, 24 240 строк, `1,426,605 B`; warm
  cache — `0 B`;
- vector-sync hash projection: около `90,139 B` на полный каталог, но 32
  запусков в сутки уже превращали маленький запрос в 2.88 MB и лишние upserts.

Build evidence: `artifacts/codex/static-site-live-20260801/kaggle-success.log`
и `artifacts/codex/podborki-data-audit-20260801/pipeline-prod-probe.json`;
стоимость публикации дополнительно сверена с
`scripts/run_static_site_builder_kaggle.py` и `static_site_atomic_root.py`.

Это logical/application bytes, а не показание счётчика провайдера. Точная сумма
счёта требует receipts и billing dashboards.

| Граница | Что происходит сейчас | Кто считает egress | Вклад новой подборки в том же build |
|---|---|---|---|
| Fly -> Kaggle | каждый run создаёт уникальный dataset с полным snapshot, source и caches | Fly public egress | `0` snapshot bytes; только небольшой source/policy delta |
| Supabase -> Kaggle | cold pgvector-related/vector sync; warm cache может дать ноль | Supabase uncached egress | контракт: `0 calls / 0 rows / 0 bytes` |
| Kaggle compute/storage | private dataset + quota-based CPU notebook/output | отдельного денежного egress-тарифа в доступной официальной документации не найдено; бюджет — quota/availability | тот же run и model load; только policy/HTML delta |
| Kaggle -> Fly | Fly скачивает все output archives | для Fly это inbound | только сжатый HTML/manifest delta, но весь archive всё равно скачивается |
| Fly -> Object Storage | secret candidate и изменившиеся production objects публикуются с Fly | Fly public egress; Yandex inbound free | новые HTML/manifest objects, без копий media |
| Object Storage -> Fly verification | publisher проверяет remote tree; текущий путь может читать тела объектов повторно | Yandex outgoing, Fly inbound | не Supabase и не Fly outbound, но реальный общий cost hotspot |
| CDN -> browser | только реальный пользовательский трафик | Yandex CDN | полезный demand-driven трафик, shared media cache |

Грубая верхняя application-byte оценка текущего Fly egress на полный build —
около `0.91 GiB` (input dataset + production root) или до `1.50 GiB`, если
пересылается и полный review candidate; после перевода в decimal GB это примерно
`$0.020–$0.032/build` при `$0.02/GB`. Реальная
публикация переиспользует неизменившиеся production objects, но receipt пока не
показывает uploaded bytes, поэтому считать её «бесплатной» нельзя. Главный
рычаг — число full builds и дублирование review tree, а не микросжатие JSON.

### 9.3. Самая простая архитектура для этого проекта

P0, обязательный для подборок:

1. После coalesced Smart Update barrier создаётся один snapshot и одна сборка на
   revision; superseded job прекращается **до** upload.
2. Exact adapters, общий BGE pass, venue projection и Astro работают в одном
   существующем StaticSiteBuilder run.
3. Astro получает только ID-проекции: `item_ids`, порядок, status и hashes. Он
   join-ит их с уже экспортированным catalog; полные card JSON не копируются в
   каждый label/venue manifest.
4. Collection/venue pages не вызывают Supabase до auth callback, найденной
   сохранённой session или явного действия пользователя.
5. Main/status/secret Kaggle datasets удаляются по terminal receipt; Fly
   snapshot удаляется после проверенного handoff; review candidate получает TTL
   48 часов. Сейчас main/status dataset cleanup в runner явно не завершён — это
   найденный retention gap.
6. Release receipt считает байты на каждой границе. Без receipt есть только
   оценка, а не доказанная экономия.

Не делаем для P0 отдельную передачу данных из Supabase и не переносим туда core.
Не делаем также немедленно новый transport-service. Если receipts подтвердят,
что повторная пересылка root/review tree действительно доминирует, отдельным P1
Kaggle пишет checked tree прямо в уникальный Yandex staging prefix по
короткоживущим prefix-scoped credentials, а Fly получает manifests/hashes и
только управляет promotion. Это убирает relay через Fly, но не является
условием запуска подборок: без измерения это лишняя сложность.

### 9.4. Бюджеты и acceptance

Минимальный, измеримый контракт:

- target при текущем объёме не более 2 успешных full builds/day; все изменения
  в 15-минутном окне coalesce. Превышение создаёт cost alert/разбор причины, но
  **не** молча пропускает нужное обновление и не является ещё одним enable-флагом;
- Fly -> Kaggle input до `350 MiB`; production tree до `700 MiB`, 3 500 objects;
- warm build: Supabase `0 calls / 0 rows / 0 bytes`; cold fallback не более трёх
  bulk calls и `2 MiB`; новая подборка/площадка всегда даёт ровно нулевой delta;
- одна новая подборка: до 20 новых objects и `1 MiB` payload на retained release,
  без дублированных изображений;
- хранить current + один rollback production release и не более одного review
  candidate с TTL 48 h; orphan datasets/prefixes старше 24 h запрещены;
- page load до явного auth/search action не делает Supabase requests;
- receipt: `fly_input_bytes`, `kaggle_output_bytes`, `yandex_put_bytes`,
  `yandex_verification_get_bytes`, object counts, retained bytes,
  `supabase_read_calls/rows/bytes/cache_status`, `yandex_cdn_bytes/requests` и
  `yandex_cdn_resource_count/package`, repo/snapshot SHA.

Пороги — начальные guardrails по наблюдаемой сборке, не обещание провайдерского
счёта. Проверяются холодным и идентичным тёплым canary и сверяются с usage
dashboard.

Официальные опорные данные на 2026-08-01:

- [Fly pricing](https://fly.io/docs/about/pricing/) — inbound free, Europe public
  egress `$0.02/GB`;
- [Supabase egress](https://supabase.com/docs/guides/platform/manage-your-usage/egress)
  — egress считается по всем сервисам; сверх quota `$0.09/GB` uncached и
  `$0.03/GB` cached для Pro/Team;
- [Yandex Object Storage pricing](https://yandex.cloud/en/docs/storage/pricing)
  — входящий трафик бесплатен, оплачиваются storage, operations и outgoing;
- [Yandex CDN pricing](https://yandex.cloud/en/docs/cdn/pricing) — с 2026-07-01
  действует новая модель с package/resource fee, включённым порогом 150 GB на
  resource и отдельным outgoing/request overage; точные рублёвые ставки зависят
  от billing account;
- [Kaggle quota guidance](https://www.kaggle.com/docs/efficient-gpu-usage) описывает
  бесплатный quota-based notebook compute, но не обещает денежный egress-тариф
  или production SLA. Поэтому для Kaggle сейчас фиксируем **`$0` отдельно
  наблюдаемых network charges**, а не «навсегда бесплатно», и контролируем
  runtime/dataset/output quota и cleanup.

## 10. Минимальный quality gate

Для всех страниц общие проверки:

- событие active, canonical, не merged/silent;
- дата ещё актуальна, включая ongoing;
- event ID существует в `static_event_public_projection_v2`;
- в тематических подборках occurrence-family duplicates схлопнуты; в venue
  schedule сохраняются разные даты показа одной family, но схлопнуты повторы
  одного occurrence из нескольких источников;
- catalog/policy/model hashes совпадают;
- manifest не старше 24 часов;
- минимум три разные сущности/occurrence families, если у страницы нет более строгого правила.

Отдельные exact gates:

- Бесплатно: только canonical materialized `is_free` из current
  `confirmed_free`/совместимого reviewed legacy; никакого regex из
  `ticket_status`; current conflict/review receipt должен быть закрыт или
  содержать явные abstains. Focused baseline сейчас 3/5 tests pass: отдельно
  чинятся explicit «Вход свободный» и zero-price/eventness replay, плюс fixtures
  `5370`, `7145`, `7244`, `7246`, `7247`, `7287`;
- Клубы: identity approved, relation/evaluation hash-policy match, inclusive
  six-calendar-month cutoff; контрольный snapshot 2026-08-01 даёт 6 visible,
  а не 3/4;
- Театр: только 8 public registry organizations, каждый event содержит
  `theatre_id` и reason `official_source|organizer|venue`; excluded fixtures
  Янтарь-холла, «Солёной вороны», кинотеатров и festival-name не проходят;
- Площадки: только stable place ID и exact canonical/approved alias; organization
  и place не склеиваются.

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

Это не большая ML-платформа. Нужны один policy JSON, отдельные provisional
review seed и owner-approved gold, а также один generated batch manifest.
Особые тесты «Необычного», клубов, популярного и персонализации остаются
своими, потому что проверяют другое поведение.

## 11. Эффективная очередь реализации

Текущая ветка содержит **только этот анализ**. Код, конфиг, production jobs и
данные в этом окне не меняются. Реализацию безопаснее разделить ровно на две
ветки по стабильной границе manifest, а не смешивать extraction и Astro UI.

### 11.1. Ветка A — сбор данных и подготовка

Предлагаемое имя следующей ветки: `feature/static-collections-data-prep`, от
свежего `origin/main`. Она не меняет публичные routes, sitemap и меню и может
быть принята отдельно.

Один компактный набор source contracts:

- `site/scripts/static_collection_policy.v2.json` — единые ontology-v2
  definitions, стратегия, minimum supply и fail-closed publication state;
- `site/scripts/static_collection_prototypes.v1.json` — namespaced prototypes и
  hard negatives для `unusual/science/strong_impressions/medieval`;
- `docs/review-data/static_collections_review_seed_v1.json` — provisional
  source-bound review seed, не gold и не publication truth;
- будущий `tests/fixtures/static_collections_owner_gold_v1.json` — отдельный
  immutable owner-approved calibration input, создаваемый только после review;
- `site/scripts/static_place_org_registry.v1.json` — общий stable ID/slug,
  `kind=place|organization`, exact aliases/facts/source bindings, medallion и
  flags `official_theatre`, `venue_page_candidate`, `medieval_site`; в первой
  версии явно содержит восемь theater organizations и шесть venue-page pilots.
  Отдельные theatre/venue/castle allowlists не плодятся.

Один nullable JSON-контейнер в `Event`, а не множество bool-колонок:

```json
{
  "admission_decision": {
    "value": "confirmed_free|confirmed_paid|unknown"
  },
  "audience_decision": {},
  "people_appearances": []
}
```

Его ключи версионируются и merge-ятся независимо; индексируемый `Event.is_free`
остаётся совместимым materialized value. Smart Update benchmark-ит piggyback;
если расширение rich-facts снижает качество, используется один короткий
candidate-only adjudicator. Backfill ограничен `topic ∪ BGE`
audience-кандидатами/конфликтами, people candidates и найденными admission
corrections.

Exporter-side Python, а не Astro, выполняет exact adapters. В частности,
`collect_source_records()` сохраняет `event_source.source_type/domain/trust`,
которые текущий `collect_source_urls()` теряет и которые нужны театрам.

Один BGE batch:

- строит evidence-only `collection_semantics_v1` без topics/regex leakage;
- одной загрузкой BGE кодирует изменившиеся events и все namespaced prototypes;
- event cache не инвалидируется из-за изменения чужого prototype bank;
- пишет `float32` NPZ и per-label status;
- выполняется в production-candidate независимо от старого unusual publication
  flag;
- один проваленный label блокируется отдельно.

Два новых generated outputs и один обновлённый существующий являются
единственной границей с сайтом:

1. `collection-batch-v1.json`: snapshot/catalog/policy/model hashes,
   compute/quality/publication status, `item_ids`, family/supply counts, failure
   codes, last-good identity и egress receipt;
2. `venue-pages-v1.json`: verified venue facts, `family_id` и exact occurrence
   event/date/time IDs, date/family counts, registry/catalog hashes и per-venue
   gate/status.
3. `interest-clubs-static-v2.json`: только public club identities, derived
   six-month state, last/next activity, event IDs и exclusion receipt counts.

Идентификаторы во всех трёх manifests обязаны существовать в
`production-catalog.json`. Полные cards и изображения туда не копируются.

Основные файлы ветки A:

- `models.py`, `db.py`, одна Alembic migration, `smart_event_update.py`;
- `site/scripts/export-production-preview-data.py`, `static_event_bge.py`,
  `unusual_event_semantics.py`;
- `kaggle/StaticSiteBuilder/static_site_builder.py`,
  `scripts/run_static_site_builder_kaggle.py`, `main.py`;
- узкий backfill, Python tests, канонические docs и `CHANGELOG.md`.

Она **не трогает** `site/src/pages`, UI components, sitemap и navigation.

Done ветки A:

- schema сначала развёрнута совместимо со старыми readers;
- targeted backfill имеет before/after/abstain counts;
- current-catalog cold canary и идентичный warm canary дают `0` повторно
  закодированных event rows;
- каждый label имеет явный `pass/blocked`, нет silent disabled;
- клубный projector даёт 6 visible approved identities на контрольном срезе и
  автоматически скрывает/возвращает их по six-month rule;
- новый/изменённый club event переживает restart/provider error: durable retry
  не стирает accepted last-good, shadow candidate не выходит в public;
- theater adapter даёт 8 организаций и сохраняет source/venue/organizer reason;
- шесть venue candidates имеют review evidence;
- admission audit чинит подтверждённые current ошибки и выдаёт явный abstain/
  review для неподтверждённых;
- прирост Supabase по каждой новой сущности равен нулю;
- fresh secret candidate закрывает обязательные проверки инцидента
  `INC-2026-08-01-unusual-feed-disabled-by-config`, но public rollout всё ещё
  требует приёмки владельца;
- публичное поведение сайта не изменилось.

### 11.2. Handoff A -> B

Ветка Astro стартует только после merge A и получения реального immutable
artifact. Handoff bundle:

1. точный `origin/main` SHA и snapshot/catalog hashes;
2. три generated manifests и их SHA-256;
3. policy/prototype/venue-registry и BGE receipt hashes;
4. per-label/per-venue gate table и approved public route list;
5. cold/warm counts и egress receipt;
6. свежая secret review URL/evidence для «Необычного».

Astro не имеет права заново угадывать membership по raw fields, расширять
`blocked` label или публиковать `candidate` venue.

### 11.3. Ветка B — страницы, SEO и навигация в другом окне

Предлагаемое имя: `feature/static-collections-astro`, от `origin/main` после A.
Один `CollectionListingSurface.astro` и один loader `staticCollections.ts`
обслуживают все labels; route files остаются тонкими. Venue использует отдельный
`venues.ts` и `/mesta/[slug]/`, но тот же event-card surface.

Сохраняются существующие canonical URLs:

- `/podborki/besplatnye-sobytiya/`, `/vystavki/`, `/populyarnoe/`,
  `/neobychnoe/`, `/kluby-po-interesam/`, `/dlya-menya/`.

Новые explicit routes:

- `/detyam/`;
- `/teatr/`;
- `/spektakli/`;
- `/nauchpop/`;
- `/nauka/`;
- `/silnye-vpechatleniya/`;
- `/k-nam-edut/` с двумя полками «из России» и «зарубежные гости», чтобы не
  выпускать две тонкие страницы из одного extraction contract;
- `/zamki-rytsari-srednevekove/`;
- `/mesta/` и `/mesta/<slug>/`.

Из-за существующего top-level `[preview]` не добавляется новый generic
top-level `[slug]`; используются explicit wrappers. `Для меня` остаётся
noindex/auth/secret delivery, а не обычной подборкой.

Навигацию нельзя просто раздувать пятнадцатью primary links. Делается одна
центральная navigation registry, которую используют четыре сейчас отдельно
захардкоженных consumer: `EventLayout.astro`, `Reference4MobileMenu.astro`,
`HomeQuickNav.astro`, `SiteFooter.astro`.

- desktop primary сохраняет даты/главные surfaces и ссылку «Подборки» на
  `/podborki/` hub;
- mobile plane «Подборки» показывает только `publication_status=public`;
- «Детям» переключается с search на `/detyam/` только после public manifest;
- Home Quick Nav содержит лишь несколько самых полезных входов, а не весь
  каталог;
- footer получает «Подборки» и «Площадки»;
- venue name и медальон event page ведут на принятую `/mesta/<slug>/`, но
  обычная текстовая ссылка остаётся доступной;
- blocked/candidate labels отсутствуют в navigation и sitemap;
- фестивали не меняются, кино не добавляется.

Build/release checks связывают manifest hashes с release, сравнивают карточки с
`item_ids`, проверяют sitemap/index policy, shared venue `@id`, отсутствие
Supabase requests и round-trip venue<->event. Если уже публичный обязательный
manifest пропал, promotion блокируется или сохраняется previous root; ссылка не
исчезает молча.

### 11.4. Порядок выпуска

#### Шаг 1 — исправить orchestration и восстановить обязательный semantic compute

- заменить smart-update scheduling на один strict trailing `+15 минут` pending
  job; проверить update-during-running и no-op fingerprint;
- отделить compute/quality/publication state;
- production-candidate всегда запускает shared BGE, даже если старый enable env
  отсутствует или равен `0`;
- исправить last-good/empty/migration validation и stale JS assertion;
- выполнить компенсирующий fresh pinned-BGE canary на текущем catalog;
- сохранить NPZ/receipt/cache/manifest и дать владельцу стабильную current review
  ссылку; public root включать только после приёмки.

#### Шаг 2 — общий batch без новых semantic страниц

- обобщить существующий BGE cache и отделить event rows от prototype banks;
- перейти на evidence-only collection document и повторно откалибровать unusual;
- создать `collection-batch-v1.json`;
- подключить существующие exact adapters;
- новые heads держать в shadow;
- доказать cold run на полном current catalog и warm run с 0 re-encode.

#### Шаг 3 — точные реестры и projection без нового LLM extraction

- заменить club 90-day/2-date filter на approved + six-calendar-month lifecycle,
  проверить все 6 текущих identities, fingerprint и truthful empty states;
- перенести relation evaluation в durable idempotent outbox, сохранять
  compatible last-good при provider failure и выполнить bounded six-month
  shadow-discovery report для новых club identities;
- зафиксировать общий place/organization registry: 8 theater organizations,
  exact source/venue/organizer binding reasons и явный excluded report;
- спроецировать 6 venue pilots из той же registry, без сканирования официальных
  сайтов и без повторного чтения Supabase;
- добавить exact manifests для Спектаклей, Выставок и Научпопа с minimum-supply;
- киноисточники и фестивальные страницы не трогать.

#### Шаг 4 — компактная доработка Smart Update, не один жирный prompt

1. Admission: добавить source-bound decision/provenance, двунаправленный
   evidence-aware merge, убрать `ticket_status` re-inference; targeted current
   audit исправляет `5370`, пять confirmed false negatives и adjudicate шесть
   unsupported free rows, затем проходит incident replay.
2. Audience сначала piggyback в существующий rich-facts/create pass и прогнать
   benchmark. Если accuracy общей schema падает — вынести один маленький
   changed/candidate-only adjudicator, а не утяжелять каждый Smart Update.
3. Structured people также benchmark-ить как компактный stage; не объединять
   независимые сложные решения только ради меньшего числа названий функций.

Backfill только admission conflict/review set, `topic ∪ BGE` audience candidates
и `PERSONALITIES`/named-person candidates, hash-bound и cached. Не прогонять
LLM по всем историческим 6 969 строкам.

#### Шаг 5 — три новых BGE heads

В рамках одного prototype bank и одного запуска:

- Наука;
- Сильные впечатления;
- medieval/knights experience.

Добавлять label только после короткой owner-разметки, не параллельно строить отдельные pipelines.

#### Шаг 6 — общий canary и handoff в Astro-окно

- получить один immutable candidate с тремя manifests и exact hashes;
- проверить per-label fail-closed, 8 theaters, 6 clubs, 6 venues, admission
  repairs, audience disagreement queue, cold/warm egress receipt;
- передать ветку и bundle следующему агенту; только там строятся routes, страницы
  и общая navigation registry;
- recommended UI rollout: сначала исправно работающие существующие/точные
  страницы, затем accepted «Необычное», `/detyam/` и «К нам едут», потом новые
  BGE heads; venue public index только после общего robots/indexability gate.

#### Шаг 7 — Для меня

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
- BGE запускается внутри того же StaticSiteBuilder после strict 15-minute
  Smart Update quiet window; отдельный notebook и второй snapshot не нужны;
- твёрдые факты остаются обязанностью Smart Update;
- бесплатность давно работает, но source-аудит нашёл текущий sticky false
  positive, минимум пять false negatives и шесть unsupported facts; нужен
  targeted correction provenance, а не массовая новая taxonomy всего архива;
- BGE может и должен страховать аудиторию, но на evidence-only документе без
  утечки topics/regex и с grounded adjudication конфликтов;
- клубный реестр уже содержит 6 approved identities; после замены ошибочного
  90-day/2-date projection на six-month lifecycle все шесть должны показываться
  и автоматически скрываться/возвращаться по активности;
- театр больше не является «allowlist потом»: production audit зафиксировал 8
  official organizations, точные source/venue/organizer bindings и exclusions;
- точные типы, официальные sources, popularity и club relations не надо
  прогонять через BGE;
- итогом одного existing StaticSiteBuilder run должны быть проверяемые
  collection, venue и club manifests;
- страницы площадок имеют достаточное продуктовое основание для пилота из шести
  curated candidates; index получают только прошедшие relation/content и общий
  robots/indexability gate, массовые страницы по raw locations запрещены;
- venue MVP использует большой медальон места, отдельную полку ongoing и
  компактное расписание по датам; календарь — навигация, а не основное
  содержание, лента больших event cards не используется;
- один существующий Fly snapshot выбран как source-of-truth и переиспользуемый
  input, а не как бесплатный транспорт: новые collections/venues дают нулевой
  дополнительный Supabase egress, не создают второй Kaggle build и отчитываются
  по Fly/Kaggle/Yandex bytes целиком;
- data-prep и Astro glue разделяются generated manifests, поэтому в следующем
  окне UI не сможет незаметно переопределить quality decisions.

Минимальное число новых смысловых механизмов:

1. один обязательный shared semantic compute внутри existing StaticSiteBuilder;
2. один общий evidence-only BGE event matrix с несколькими label heads;
3. один небольшой cached LLM adjudicator только для factual/ambiguous
   candidates, включая admission/audience/people schemas;
4. три компактных ID-only manifests как граница с Astro;
5. один маленький общий place/organization registry и существующий DB-реестр
   клубов, без нового сервиса.

Это даёт меньше разрозненных решений, повторное использование уже оплаченной исследовательской работы и fail-closed качество выборки.
