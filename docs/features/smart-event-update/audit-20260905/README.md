# Smart Update: аудит целостности occurrences и подготовленный пакет исправлений

Дата: 2026-09-05. База анализа: `events-bot-new@b8f463f5c35fa62befcfed171a7a8a0886af20f7`.
Статус: **PREPARED / NOT APPLIED TO RUNTIME / NOT RELEASED**.

Это продолжение [DATA-аудита #621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621#issuecomment-5550038387), а не повторный аудит интерфейса. Цель — не исправить несколько карточек вручную, а не допускать повторного смешения программы, даты, времени, площадки и связей при импорте и обновлении событий.

В пакете есть исполняемый патч и его регрессии. Runtime-файлы ветки намеренно не изменены: пользователь запросил аудит и подготовку правок; применение, интеграционные проверки и историческая очистка разделены. `occurrence-integrity.patch` изменяет `linked_events.py`, добавляет обычный pytest-файл в `tests/` и запись в корневой CHANGELOG. `reproduce.py` применяет его **только к временной копии** и выполняет RED/GREEN. Остальные процессные изменения ниже — подготовленные решения и требования к тестам, не выданные за реализованный код.

## Главный вывод

Проверка «похоже ли это на существующее событие?» не заменяет проверку «относятся ли все поля этого кандидата к одному пункту исходной программы?». Детерминированный ключ обеспечивает повторяемость выбранного представления, но не исправляет неверный tuple. И даже правильное решение основного identity gate может быть испорчено последующим независимым пересчётом `linked_event_ids`.

Поэтому нужны три отдельные границы: **целостность кандидата → разрешённая запись в canonical event → корректные отношения и последующие эффекты**. Новая система очередей, отдельный оркестратор и массовая замена промптов не требуются.

## Что проверено и чего это не доказывает

Прочитаны инструкции репозитория, основные документы Smart Update/identity state machine, relevant `smart_update_identity.py`, точный `linked_events.py`, существующие ссылки на callers и DATA evidence. База `linked_events.py` восстановлена байт-в-байт; Git blob `af0ebafdd4e7f6a18ade11e083eb9cb031eabfb9` проверен локально. Поле `identity_status`/`merged_into_event_id` сверено с моделью Event.

Полный production replay гигантского `smart_event_update.py`, запуск бота, реальные model/provider calls, актуальная prod-конфигурация gate modes и конкурентный импорт здесь **не проверены**. Чтение исходников/старых решений не является трассировкой транзакции конкретного исторического события. В частности, причина, почему после записанного решения DISTINCT у8737 сохранился конфликтующий tuple, не приписывается конкретной ветке записи или гонке без candidate/transaction trace.

## 1. Процессная карта

| Этап | Установленное основание / риск | Подготовленное действие |
|---|---|---|
| Разбор поста и общей афиши в отдельные кандидаты | DATA8737 и8702 подтверждают смешение полей разных строк одной программы. Само наличие времени/места где-либо в полном посте не доказывает принадлежность кандидату. | В существующем producer/repair проходе сохранять scope конкретного occurrence и проверять совместную принадлежность title/date/time/venue/admission. Не определять смысл наборами ключевых слов. |
| Candidate identity и повторная обработка | `stable_candidate_identity` предпочитает explicit/source-native/vendor/ticket identity; fallback использует date/end_date/time и producer ordinal. После исправления ошибочной даты либо перестановки ordinal ключ может измениться. | Не запрещать законное изменение даты ради стабильности ключа. До create сопоставить исправленный кандидат с прежней записью и отдельными настоящими occurrences. Проверить edited post/reordered packet/replay. |
| Выбор MATCH / DISTINCT / RETRY | Нормативно это разные исходы. В прочитанном gate `SKIP_MERGE_SIDE_EFFECTS` и `AUTOMATIC_RESOLUTION_REQUIRED` блокируют эффекты в enforce. Не все modes одинаковы. | Интеграционный тест каждого исхода должен наблюдать Event, источники, posters, relations и outbox. Нельзя считать проверку только возвращённого enum достаточной. |
| Применение разрешённых полей | Нет полной исторической mutation trace8737/8702. Нельзя объявить виновным конкретный setter по одному итоговому значению. | Сначала воспроизвести на копии с фиксированными ответами LLM и input packet. После blocked/distinct исключить позднюю запись полей старого кандидата. Не подменять canonical факт общим source default. |
| Пересчёт повторов | В `linked_events.py` есть воспроизведённая асимметрия и неполная очистка обратных связей. | Готовый патч P1 ниже. Это отрицательные ограничения совместимости, а не новый semantic matcher. |
| Media review и публикация | У8589 pending posters; у8702 нет подготовленного media. Это не одно состояние и не причина объявлять события ложными. | Проверять pending/approved/failed как отдельную зависимость; позднее получение изображения не должно пересоздавать event или размножать jobs. |
| Export и UI | DATA показал потерю time confidence и превращение общего links списка в other dates. Здесь уже работает отдельный CODE/UI поток. | Не дублировать его изменения в `site/`. Передать upstream contract; source repair, export, snapshot и опубликованную сборку принимать раздельно. |

### 8737: ошибка не исправляется заменой19:09 на19:00

Frozen record: программа «Упрямая любовь» / «Королевская хитрость»,5.09,19:09, `time_is_default=true`. В исходной афише с SHA256 `e9b6ad283e1e9786257b19954e15a9565c452ca66b45bc2b24630b2f82d6da61` четыре разные программы на5/12/19/26сентября; все в16:00. Целевая программа относится к26сентября. В текущем DATA-read уже существовала правильная запись8658 на26.09/16:00.

Нужно восстановить соответствие строк программы и кандидатов, затем identity-проверку, а не делать локальный hotfix часов. Нельзя переносить8737 на26сентября без проверки8658, удалять все похожие названия или превращать обычное повторение программы в дубль.

### 8702: источник содержит несколько площадок

В source evidence [wall-104963527_12387](https://vk.com/wall-104963527_12387)5.09/15:00 относится к Железнодорожным воротам, Гвардейский проспект51а;18:00 — к Собору. Record8702 сочетает title/time первого пункта с venue второго. На этапе проверки occurrence нужно сопоставлять **весь tuple**, включая билетную ссылку и бесплатность. «Билеты» не доказывает платность, а «вход свободный» у соседнего пункта не доказывает бесплатность текущего.

### 5370: связь по теме не равна другим датам одной программы

Выставка5370 идёт12.06.2026–28.03.2027.8608 — самостоятельная экскурсия6.09/16:00 по этой выставке. Два отдельных Event должны сохраниться; их нельзя объединять в группу повторов только из-за названия и площадки. P1 убирает неверные occurrence-edges; создание полноценного typed relation «экскурсия по выставке» **не реализовано** этим патчем. Не называть отсутствие неверного other-date полноценной delivery нового relation UI.

## 2. P1 — готовый патч пересчёта `linked_event_ids`

### Воспроизведённые дефекты baseline

1. Длительная запись с `end_date` отбрасывается только как base. Candidate-query выбирает лишь id/title/date/time; соседняя экскурсия может включить выставку и записать взаимные links. Результат зависит от того, с какого Event вызван пересчёт.
2. Ранний выход для invalid/base с range не снимает уже существующие неправильные связи.
3. Тот же venue/title не исключает другой event_type, город, cancelled/merged/silent запись.
4. У всех выбранных членов группы перезаписываются outgoing links, но удаление прежних backlinks исходит только из old links базового события. Прежние связи других членов могут остаться односторонними.
5. Совпадающий точный слот и конфликтующие siblings могут попасть в полный взаимосвязанный набор через третий Event.

### Изменения

Один существующий owner, неизменный публичный API. Eligibility применяется симметрично к base и candidate; reject-rails проверяют записанные type/city/lifecycle/identity/end_date. Group проверяется попарно, а не только против base. В тот же transaction обновляются нужные outgoing и известные обратные edges каждого изменённого члена. Отклонённый base снимает свои известные связи, не удаляя прочие связи соседей.

На одной дате отдельные посещения допускаются только при разных известных часах. Unknown/default не доказывает ни дубль, ни другой сеанс. Реально подтверждённое19:09 не запрещается и не округляется. Разные даты остаются возможными даже при неизвестном времени. `00:00` сохраняет прежнюю локальную семантику unknown; новый midnight contract не вводится.

Существующий fuzzy title matcher сохранён. **PASS этих ограничений не доказывает тождество программы.** Не добавлены списки площадок, замена названий по словам или специальные ветки для8737/8702/5370. Не удаляются Event/source/poster/job records.

### Исполненные регрессии

Финальный набор: **39 тестов**. Точный baseline: **31 FAIL /8 PASS**, подготовленный patch: **39 PASS**, без skips. Первоначальный промежуточный набор34 позже расширен; это не два складываемых покрытия.

Проверяются обе очередности exhibition/tour, очистка исторических wrong edges, idempotence, известные противоречия type/city/status/range, реальные повторы и разные часы, exact-slot duplicates, default/unknown, допустимое19:09, отсутствие транзитивного объединения несовместимых siblings, backlinks всех членов, preservation чужих edges, invalid ID safety, rollback commit failure, отсутствующий Event и group cap.

SQL выполнялся на **настоящей временной SQLite через SQLAlchemy**. Из production файла AST загружаются неизменённые тела функций/SELECT; подменены только imports `db`, `models`, `sqlmodel` на тестовые зависимости. Это не полноценный запуск production SQLModel/async driver/Smart Update. Синхронный session обёрнут async facade; конкурентность этим не проверяется. Зелёные результаты не подтверждают LLM extraction, provider/runtime, outbox atomicity или исторический repair.

### До применения P1

На полном checkout выполнить существующие linked-events/Smart Update suites и настоящий async Database integration test. Проверить, что callers не выходят до вызова recompute при изменении типа/диапазона и что `changed_event_ids` всех соседей доходят до существующего refresh path. Эта важная граница не закрыта прямым unit-вызовом функции.

Неизвестные backlinks, отсутствующие в outgoing links, полный legacy graph, concurrent writers и candidate-limit saturation требуют отдельных проверок. Глобальный scan/repair не добавлен. При legacy null lifecycle/identity сохранены прежние defaults; это совместимость, не доказательство статуса. Type comparison строгий по записанному значению: новые alias equivalences нужно доказывать отдельно. Попарная проверка ограничена существующим max_candidates, но full-corpus performance не измерена.

## 3. P2 — occurrence-scoped поля и исправление identity

Следующий пакет в существующих producer/Smart Update owners, не новая служба:

- Сначала добавить failing replay для sanitized8737 и8702; заменить реальный LLM на фиксированные ответы, не вызывать провайдера.
- Existing extraction/repair должен обрабатывать title/date/time/venue/address/admission как согласованный occurrence. Whole-post «встретилось значение» допустимо как предварительное evidence, но не как подтверждение принадлежности строке.
- Сохранять explicit/default/unknown distinction. Не повышать default в explicit из-за другого времени на общей афише или generated description.
- Исправленный кандидат снова проходит существующее reconciliation до create. Existing source-native/vendor identity предпочтительнее ordinal, но при отсутствии native ID нельзя выдумывать его. При edited schedule нужно сверить прежнюю привязку и соседние occurrences, а не объявить любой новый hash новым событием.

Обязательные положительные тесты: два настоящих сеанса одного фильма; разные фильмы на одной площадке; разные мероприятия одного multi-event поста; явный перенос организатором; законное необычное время19:09. Отрицательные: дата19.09 как время; чужой адрес; бесплатность соседнего пункта; прежняя программа на новой дате; общий poster hash как единственная причина merge.

Эти replay-тесты **описаны, но не исполнены/не включены в39**. `source-cases.json` — входные данные и независимые ожидаемые факты для них, не имитация готового process verifier.

## 4. P3 — действия gate должны совпадать с записанными эффектами

В existing integration suites зафиксировать:

| Сценарий | Ожидаемый проверяемый эффект |
|---|---|
| MATCH с разрешёнными полями | Только целевой Event и допустимые поля; согласованные source ownership и зависимые jobs. |
| DISTINCT | Старый Event не меняется; отдельный настоящий occurrence сохраняется через штатный create/reconciliation, без identity glue. |
| RETRY / unresolved | Нет ложного SUCCESS или тихо потерянного кандидата; остаётся штатная durable возможность автоматического повтора. Не новая ручная очередь. |
| Исключение после решения, до/после canonical write, перед source/poster/outbox | Ни частично применённый merge, ни двойной enqueue при повторе. Фактические commit boundaries нужно проверить, не предположить. |
| Два concurrent входа на один occurrence | Одна canonical identity, согласованный source binding и bounded идемпотентные effects. |
| Два разных occurrence одного источника | Оба сохранены; replay/перестановка порядка не склеивает и не размножает их. |
| Pending → approved/failed media | Не меняет identity; не выдаёт unsaved media за опубликованное; retry не плодит posters/jobs. |

Проверять реальные таблицы и written values, а не только RETURN enum, prompt text или mocked assertion на число вызововов. Existing gates/locks/retries переиспользовать. Не выполнять большой рефакторинг1.9MB orchestration в отсутствие доказанного failing path.

## 5. Исторические записи — отдельный repair, не часть этого запуска

После фикса остановить воспроизводимый дефект на staging copy, затем получить dry-run через существующий repair-путь: exact affected IDs, before/after tuples, source evidence, survivor identity, old/new relation edges, зависимые публикации. Не перезаписывать frozen review snapshot и не удалять public URLs. Повтор dry-run/apply должен быть идемпотентен. Изменения production, выбор survivor для8737/8658 и компенсирующие перепубликации требуют отдельной контролируемой операции; здесь она не запускалась.

## Проверка и использование пакета

Из checkout репозитория с установленными pytest и SQLAlchemy:

```sh
python docs/features/smart-event-update/audit-20260905/reproduce.py --repo . --out /tmp/smart-update-audit
```

Команда проверяет blob исходного `linked_events.py`, копирует только нужные файлы во временную директорию, применяет патч там, запускает39 тестов на baseline и исправлении, сохраняет raw outputs/JUnit/receipt. Несовпадение baseline останавливает воспроизведение; script не откатывает рабочую ветку и не читает prod DB.

После review, отдельно от reproduce:

```sh
git apply --check docs/features/smart-event-update/audit-20260905/occurrence-integrity.patch
git apply docs/features/smart-event-update/audit-20260905/occurrence-integrity.patch
python -m pytest --noconftest -q tests/test_linked_occurrence_integrity.py
```

Это явное применение к выбранному checkout, не deploy. Затем обязательны native integration/caller regressions выше. Не объявлять Smart Update полностью исправленным по одному P1.

## Источники и статус сохранения

- [DATA report](https://github.com/onedayonemasterpiece/events-bot-new/issues/621#issuecomment-5550038387), [frozen evidence](https://github.com/onedayonemasterpiece/events-bot-new/issues/621#issuecomment-5549948896).
- [Exact linked owner](https://github.com/onedayonemasterpiece/events-bot-new/blob/b8f463f5c35fa62befcfed171a7a8a0886af20f7/linked_events.py).
- [Identity gate/helpers and stable_candidate_identity](https://github.com/onedayonemasterpiece/events-bot-new/blob/b8f463f5c35fa62befcfed171a7a8a0886af20f7/smart_update_identity.py).
- [Canonical Smart Update docs](https://github.com/onedayonemasterpiece/events-bot-new/blob/b8f463f5c35fa62befcfed171a7a8a0886af20f7/docs/features/smart-event-update/README.md), [state machine](https://github.com/onedayonemasterpiece/events-bot-new/blob/b8f463f5c35fa62befcfed171a7a8a0886af20f7/docs/features/smart-event-update/identity-state-machine.md).

Никаких заявлений о production PASS, full audit coverage всех ветвей, новых approved данных или опубликованной сборке. Пакет хранится отдельно от параллельных UI/foundations/STATUS изменений.
