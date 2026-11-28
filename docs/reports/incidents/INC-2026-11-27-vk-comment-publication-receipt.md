# INC-2026-11-27: комментарии не находят опубликованный анонс

Статус: open. Владелец исправления: KenigEvents root. Источник — read-only проверка production 27 ноября12:49–12:52 UTC.

## Наблюдение

Событие11515 «Люди и судьбы: калининградское время»: Event и два frozen payload содержат отложенный16159, опубликованный receipt77828 связывает его с16180. Direct service GET подтвердил публичный16180 с тремя похожими в теле и пустым списком комментариев. Calendar171576 и related171577 сделали по11 неуспешных попыток с `vk_calendar_public_post_not_found` и `vk_relations_post_missing`.

Canonical дата27ноября, time19:00, конец18декабря; публичный заголовок содержит диапазон без времени. Время изменения canonical фактов не установлено. Отсутствие поста в первых100 или точную историческую причину header mismatch не утверждаем.

## Общая причина и исправление

Оба normal job paths обходили готовую связь stored→live из `event_publication` и заново сопоставляли mutable заголовок. Календарный ICS consumer дополнительно выбирал строки только по live URL и потерял бы события, у которых остался stored URL.

Используем единственный published receipt своего target для frozen stored URL; проверяем прямым service GET физический owner/ID, публичность и исходный postponed ID либо frozen publication timestamp. Heuristic wall_scan receipt сам по себе не разрешает публикацию. Конфликтующий receipt или public identity даёт ошибку, а не иной адрес. Без receipt работает существующий путь. Calendar принимает old/live aliases одного поста, lifecycle-фильтры сохранены; related использует ту же frozen выборку и уже прочитанный item. Canonical факты/URL, старые посты и отправленные комментарии не редактируются.

## Различающая проверка

`tests/test_vk_comment_publication_receipt.py`: actual job entrypoint с fixture DB и fake VK API. На исходном main оба positive controls падают точными production errors; negative controls не создают комментарий. После исправления проверяются одна отправка на live ID и отсутствие дубля при повторе, ICS membership по old/live aliases, отсутствие canonical URL writes, frozen выборка, conflicts/date/owner/deleted и посторонние receipts. Изолированный контейнер без сети и production DB/env. Before: 2 FAIL/10 PASS; positive controls падают точными production errors. Первый after: 64 PASS/2 FAIL из-за fixture, где конфликтующий receipt был законно исключён по canonical URL. Fixture исправлен, без расширения SQL. Итоговый expanded suite: 82 PASS/26.95s; сеть отключена, production DB/env не подключались. Отдельный simplicity guard SOURCE PASS: +27 production lines, дополнительные7 к первоначальной оценке нужны для physical identity и old/live ICS membership. Final SOURCE exact5227df353 дал CHANGES: legacy payload без publish_date ошибочно перекрывался receipt, а доказанный identity conflict повторялся без конца. Root восстановил legacy resolver для неполного payload; доказанный конфликт использует существующую terminal классификацию обеих разновидностей комментария, missing/not-public остаётся retryable. Контрпримеры на прежнем коде: 13 FAIL / 100 PASS. Исправленный expanded suite: 113 PASS / 74.04s, без сети и production DB/env. Native followup simplicity guard SOURCE PASS. Net production +29. Rollout ещё не запускался.

## Живая приёмка

Не выполнена. Требуются normal main release, продолжение существующих jobs и фактический единичный комментарий. Ручной replay, DB reset и массовый backfill не разрешены. Ранее выпущенный PR1113 и его CUTOVER_USED не повторять.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/event11515-*`.

## 28 ноября: published receipt ещё отсутствует, header уже изменён

Обычные комментарии события11573 к public16354 пришли через86 и96мин после публикации04:50UTC. Существующий projection snapshot с manifest05:00:26 содержит Event.time14:00 и receipt statusmissing/liveNULL. Сохранённый фактический пост имеет тот же title, но date-only header «📅 28 ноября». Оба no-receipt fallback строили header из изменяемого canonical и отвергали этот пост. Исторические wall.get responses не сохранены: это доказанный дефект условия, а не утверждение единственной причины всех восьми ошибок.

Механическое исправление использует уже frozen payload.post_url/publish_date и существующее одно service wall.get. Published receipt fast path сохранён. При отсутствии receipt выбирается единственный публичный неудалённый пост нашего owner с исходным postponed ID, либо без такого ID — с большим live ID, точным временем исходной публикации и точным title. Не выбирать ближайшее время, другой слот, противоречивый postponed ID или неоднозначные результаты. Legacy payload без publish_date и прочие consumers общего recovery остаются прежними. Новых полей, очередей, retries, providers, контроллеров и canonical/VK edits нет.

Обязательные проверки: оба actual handler вида с date-only prepared header и canonical14:00; соседний одноимённый пост в другом слоте первым в ответе; owner/deleted/postpone/date/postponed-ID/ambiguous negative controls; старые receipt conflicts и legacy/early-publication/CAPTCHA/once checks. Before14FAIL: calendar выбрал соседний live30, related продолжил со stored10; negative controls тоже не остановили dispatch. Первый expanded after160PASS/5FAIL из-за прежних fixtures без owner/mapping и API толькоgetById. Fixtures обновлены без ослабления production predicates: physical postponed_id допускает actual-public +120; known-time related использует wall.get, legacy сохраняет recovery. Добавлены title/id negative и explicit-mapping positive. Итог171PASS/115.04s, networknone, без production DB/env; release ещё не запускался. Живая приёмка — новая обычная публикация и единичные comments, без replay или правок уже отправленных.

Exact SOURCE review первого PR1119 head8a4ce1d59 дал CHANGES: общий normal feeding-series post имеет synthetic title publication view, который отличается от individual canonical; receipt canonical URL conflict должен завершаться до service GET, даже если provider недоступен. Root восстановил существующую same-day publication view с guard одинакового source URL и прежний pre-read conflict check, не возвращая mutable date header. Новые grouped/individual URL и unavailable-provider direct/worker контрпримеры: Before8FAIL, после правки expanded suite179PASS/114.71s, networknone без production DB/env. Native simplicity delta PASS. Net production+23 в бюджете30.

## 28 ноября: живая регрессия точной секунды PR1119

PR1119 развёрнут из main53a280e84696fd55a18d01f166295e33646d4a75 в08:37:56UTC. Source4633 и finalimage179 tests PASS; runtime ready/db/scheduler/tasks healthy. Продуктовая приёмка FAIL: обычный calendar173057/event11593 в08:38:18 отверг public16411 с датой08:00:01 при frozen publish_date08:00:00. До выпуска тот же job находил пост и доходил до multipart upload; receipt ещё missing, canonical URL уже live. Другая обычная публикация11572 также имела +2 секунды. Точный первый second не является контрактом VK.

Существующий `_vk_postponed_next_slot` округляет normal reservation до минуты и разделяет слоты минимум60сек. Оба сравнения заменены общей локальной проверкой принадлежности этой минуте: T <= actual < T+60 для minute-aligned payload. Для payload с точными секундами сохраняется exact timestamp. Canonical live URL не становится самостоятельным доказательством; physical mapping, единственность title/owner/public match, receipt conflict до API, individual publication-view guard и actual comment due остаются. Нет новой очереди, состояния, повторов или правок VK.

Различающие actual handler tests: planned aligned, actual+1/+2, оба типа comment, canonical old/live — BEFORE8FAIL. Дополнительные контроли: +59, -1/+60/+600, exact/nonminute drift, две кандидатуры одной минуты. AFTER запущен в изолированном контейнере без сети и production DB/env; результат root прочитает из slot-drift-after-20261128.log. Новый выпуск ещё не запускался; обычный job173057 остаётся в штатном backoff до09:38:18 без reset/replay. Предыдущий multipart timeout отдельно открыт.

## 28 ноября: фестивальная страница удерживает общий worker

Runtime173191/event11440 с08:23:49 занимал единственный running outbox slot, пока импорт13657 держал process-global heavy gate. Calendar173057 был due08:23:59; inline импорт получил timeout90s08:36:02. Точный suspended festival stack не записан; source плюс зарегистрированный gate holder указывают на lock inversion, не доказательство вечного deadlock. Независимый actual worker тест с реальным semaphore воспроизвёл блокировку: сначала толькоfestivaldue, import удерживает gate; baseline не возвращает worker за3s, BEFORE1FAIL/10.28s.

Снять wrapper с sync_festival_page небезопасно: direct callers читают старый path до remote create/recreate и сохраняют URL после вызова; transport semaphore не покрывает DB commit. SOURCE65ba3e66 отклонил такое вычитание. Минимальная правка переносит admission в начало worker iteration, используя тот же heavy_operation(mode=try). Busy не claim-ит/не изменяет задание, остальные кандидаты идут дальше; acquired scope сохраняет guard через handler/commit. Net+9 при уточнённом бюджете12 вместо исходной оценки0; 667 строк сдвинуты на уровень для lexical cleanup, это не новый алгоритм. Новых gate/queue/retry/state/worker/priority нет.

Initial AFTER219PASS/1FAIL148.47s: SQLite вернул naive UTC с теми же компонентами, тест сравнивал с aware UTC. Assertion нормализован существующим _ensure_utc, source не менялся. Focused rerun всех3новых actual cases:3PASS4.40s; первый полностью прошёл calendar while gate held, затем release и сохранение festival URL. Дополнительные actual sync controls проверяют direct create и PAGE_ACCESS_DENIED recreate против worker, один remote create и сохранённый path. No network/production DB/env в isolated runner. Runtime173166 после cutover1120 reaper09:07:52 временно parkederror next2036;173191 вернулся обычным accepted smart update08:49:47, повторноmergedpending08:58:44. Сам reaper не доставляет страницу; уже parked строки вручную не сбрасывались. Новый release по этой правке не запускался.
