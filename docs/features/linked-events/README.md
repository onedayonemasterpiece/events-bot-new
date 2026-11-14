# Связанные события: единый контракт

> **Статус:** каноника для переноса во все ветки, где показываются связи между
> событиями. Production source of truth — `origin/main`; лабораторные ветки
> ниже дают требования и визуальные образцы, но не становятся production
> автоматически.

Цель фичи — одна модель данных и одно предсказуемое поведение на странице
события, в mobile feed, на списочных страницах, в Telegram/Telegraph и в
будущих персональных поверхностях.

## Главное решение: четыре публичных связи + duplicate review

| Канонический тип | Что означает | Публичная подпись | Нельзя смешивать с |
| --- | --- | --- | --- |
| `same_occurrence_group` | та же программа/активность в другое время или дату | **Другие даты** / **Другое время** | похожими событиями и дедупликацией |
| `semantic_related` | действительно похожее, но самостоятельное событие | **Похожие события** | другими датами и широкой афишей |
| `broad_discovery` | ограниченное продолжение выбора и anti-bubble | **Ещё события** | утверждением «похожие» |
| `personalized_feed` | выдача изменена совместимым профилем пользователя | **Для вас** / **По вашим интересам** | анонимным статическим fallback |

`duplicate_same_slot` — отдельный непубличный outcome: два rows с одной
программой и одним slot должны идти в merge/review, а не показываться как
«другая дата». Он не входит в четыре публичных relation surfaces.

`festival`, площадка, организатор, источник, рубрика и поисковая выдача — это
**membership/filter context**, а не relation edge. Они могут быть признаками
ранжирования, но не дают права называть два события «похожими» или «другими
датами».

## Единая цепочка данных

```text
Fly SQLite Event (канонические факты и lifecycle)
  ├─ linked_event_ids -> same_occurrence_group
  ├─ static export -> активный публичный каталог
  ├─ offline retrieval + quality gates -> semantic_related
  ├─ bounded diverse tail -> broad_discovery
  └─ consented compatible profile -> personalized_feed rerank/filter
       -> static HTML/JSON/card projection
       -> один card renderer и один interaction controller на всех surfaces
```

- Fly SQLite остаётся источником истины для идентичности, дат, статусов и
  `linked_event_ids`.
- Supabase/pgvector может хранить поисковые документы, embeddings, компактную
  выдачу и telemetry, но не становится источником истины о lifecycle события.
- LLM/vector/provider не вызываются в page-view hot path. Страница всегда
  полезна как статический fallback.
- Любая ветка должна потреблять единый результат связи, а не заново решать
  regex/эвристиками, что является другой датой или похожим событием.

Историческое исключение в runtime lineage `origin/main@3d0af26c`: frontend
`getLinkedSessionIds()` объединяет explicit `other_date_ids` с inference по
normalized title/type/venue/city. В текущей интеграции это удалено: совпадение
текста больше не создаёт family; family требует взаимных explicit links, а
неполная coverage остаётся честным migration gap исходной базы.

Практический canary этой границы — `6318` (2 ноября) и `6586` (3 ноября).
Компоненты и formatter уже поддерживают требуемую одну карточку
`2, 3 ноября 19:00`, включая разные площадки, но production/export snapshot на
2026-07-21 содержит для обеих строк пустые `linked_event_ids`. Поэтому текущая
раздельная выдача — data coverage gap, а не повод вернуть frontend inference.
Для focused review допустим только явно зафиксированный взаимный fixture edge;
перед общим rollout требуется source-grounded взаимная canonical repair.
Legacy `recompute_linked_event_ids()` требует одинаковую площадку и может снять
такую межплощадочную ручную связь, поэтому durable repair должен получить
manual provenance/lock либо целевой `occurrence_group_id`, а не голый
незащищённый edge.

## Каноническая frontend-проекция

Contract/donor `feature/related-events-compact-unified-20260721` выборочно
интегрирован в три слоя:

1. `site/src/lib/eventOccurrences.ts` — pure resolver взаимных explicit links,
   lifecycle/graph issues, slot grouping, compact/rail formatting и card
   collapse (`none` / `per-date` / `per-family`).
2. `EventOccurrenceLabel.astro` — одна подпись для больших и rail cards;
   `EventOccurrenceNav.astro` — один selector для event detail.
3. `EventCard`, `EventListItem`, detail/CTA и hydrated discovery получают готовую
   projection, но не определяют identity и не собирают даты локально.
4. `sync_event_search_vectors_to_supabase.py` проецирует тот же
   reciprocal-explicit family id и exact compact/a11y labels в
   `card_snapshot`; Edge Function до LLM/final response сворачивает выдачу
   по этому family key. Так live authorized search остаётся
   `per-family`, а не только статический fallback.
   Материализованный DTO содержит точные `occurrence_member_ids`, compact label
   и полный `aria-label`; Edge Function сначала сворачивает полный ranked window,
   затем применяет logical pagination, повторяет collapse после LLM rerank и
   переносит тот же family seen-set в fallback. Первый/highest-ranked member
   остаётся representative; malformed one-way/dangling metadata fail closed.

Канонические примеры: `2, 9 ноября 19:00` и
`4 ноября 17:00, 19:00`. Rail показывает ту же projection в две строки и один
полный `aria-label`. Конкретная date-list сворачивает family только внутри даты;
entity/ranked surface оставляет одного representative на family. Полная матрица
и fail-closed правила — `REL-045`–`REL-048`.

## Поверхности

| Surface | Обязательное поведение |
| --- | --- |
| Статическая страница события | `Другие даты` рядом с датой/CTA; ниже контента конечный блок `Похожие события`; отдельный `Ещё события` допустим только как честно отделённый broad tail. |
| Mobile event detail | те же relation types и порядок; cards идут вертикально, без бесконечного скролла и без дублирующего второго блока. |
| Mobile feed / «Для меня» | использует тот же card/feedback/profile contract; скрывает siblings одной occurrence group после явного negative action, но не называет всю ленту «похожей». |
| `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`, категории | основной SEO-список не меняется; персональная часть — progressive enhancement, конечная и дедуплицированная с уже показанными событиями. |
| Telegraph/бот | компактная строка `Другие даты`; тот же `linked_event_ids`, сортировка и lifecycle-фильтры. |
| Галерея события | финальный CTA может вести на первый `semantic_related`, но не маскируется под фотографию и подписан `Смотреть похожее`. |

## Что является каноникой

1. [Единые требования и acceptance checklist](requirements.md) — документ,
   который нужно передавать во все implementation branches.
2. [Инвентаризация production, старых реализаций и лабораторий](inventory.md) —
   откуда взяты решения и какие ветки нельзя принимать за production.
3. [Визуальные паттерны и скриншоты](visual-patterns.md) — принятые механики,
   экспериментальные варианты и их provenance.
4. [Передача решения в рабочие ветки](handoff.md) — donor branch, короткий
   prompt, integration order и обязательные проверки.

Подробные subordinate contracts остаются на своих местах:

- event-detail ranking/personalization:
  `docs/features/unsigned-personalization/event-detail-related.md`;
- retrieval/cache/quality:
  `docs/features/unsigned-personalization/semantic-vector-retrieval.md`;
- listing continuation:
  `docs/features/static-site-pages/listing-personal-feed.md`;
- page/card/media behavior:
  `docs/features/static-site-pages/README.md`.

При расхождении терминов или поведения эти документы должны быть приведены к
этому umbrella contract; нельзя создавать ещё один параллельный «related»
словарь.

## Уже работающая legacy-механика `linked_event_ids`

### Source-programme adjudication (2026-11-14)

`recompute_linked_event_ids` больше не утверждает programme identity по fuzzy
title/venue. Обычные source import boundaries (`add_events_from_text`,
`vk_intake.process_event`, VK auto/review, parser `process_source_events` и
реальный TG Monitor `source_parsing.telegram.handlers.process_telegram_results`)
собирают accepted IDs в transient context, общий для вложенных вызовов.
После завершения children accepted IDs обрабатываются порциями по 32: семь
children требуют одного дополнительного `_ask_gemma_json`, 33 — двух. Ни один
accepted target не отбрасывается из-за размера batch. Повторные parser/Smart
Update/linked-source observers только дополняют текущий transient context. Нынешние upstream ответы не содержат pair adjudication, поэтому
piggyback отсутствующего verdict не имитируется. Изолированная ручная правка
сохраняет singleton API и может выполнить один запрос.

Нет persistent cache/state/schema, новых queues/workers/services или provider
bypass. ContextVar содержит только ID и selected child excerpt текущего
awaited source import; после выхода контекст сбрасывается, accepted imports
не становятся ошибкой при недоступности derived programme observer.
Запрос выполняется вне DB session; изменённые пары получают штатный
`telegraph_build`. Publisher не вызывает LLM, его same-day поведение не изменено;
удаление ложной связи само по себе не запускает VK публикацию.

Recall metadata pool ограничен прежним максимумом 800 и окном 120 дней назад /
365 вперёд. До SQL LIMIT выборка сужается source aliases, городом, venue или title tokens
(только recall). Aliases имеют приоритет, далее — близость даты к batch вместо
самых ранних строк. После SQL title similarity/venue/city выбирают peers,
но никогда не становятся semantic verdict.
В запрос попадают до 32 targets текущей порции и 24 recall peers. Все остальные
source children участвуют в recall, включая соседние порции и даты вне окна
внешнего поиска; source children приоритетны до SQL LIMIT. Старые linked peers загружаются отдельно и не могут
быть вытеснены из adjudication top24. Используются canonical/active/not silent/
not merged rows. Лимит внешнего recall остаётся ограничением полноты; отсутствие
peer/ответа не доказывает independent_event.

Пакет child содержит title/type/date/time, venue/city, выбранный
`occurrence_scope_text` до 600 символов, до четырёх accepted canonical facts
по 160 символов, short description до 300 символов и canonical address до
160 символов. Адрес — child-selected evidence, не детерминированный verdict.
Raw carrier не используется
ни как child summary, ни как доказательство quotes. Поэтому отсутствие
`source_text` не запрещает связывание при наличии accepted фактов. Выход ограничен
6000 tokens. Вместо quadratic pair output ответ содержит `groups` с `members`
(`event_id`, индивидуальная `quote`) и отдельные sparse `decisions` для
`duplicate_same_slot` / `independent_event` old edges. Группа из 32 дат требует
32 короткие цитаты (в prompt запрошено до 120 символов) вместо 496 pair objects. Модель должна выбрать canonical representative
каждого slot; разные матчи/материалы/аудитории не становятся одной программой.
IDs и цитаты всех members проверяются по фактическому packet evidence; повтор
member в нескольких группах, конфликтующие verdicts, same-slot members или
address-only identity quote отклоняют новые решения, сохраняя известные edges.
Проверенная группа разворачивается в reciprocal edges существующим writer,
включая peers и соседние порции; persisted group_id не создаётся. Schema types
lowercase, существующий `_gemma_native_response_schema` преобразует их для Google.

Четыре исхода: `same_programme_other_slot`, `duplicate_same_slot`,
`independent_event`, `unknown`. Проверяются passed target/peer IDs и дословные
цитаты из child excerpt / accepted facts / canonical summary обеих сторон.
Contradictory output не применяется. Same-slot не создаёт «Другие даты»;
группа даёт только явно подтверждённую моделью clique, не inference из старых
edges; stale backlinks
снимаются. Unknown/отсутствующий ответ не создаёт edges и сохраняет существующие
взаимные semantic links, включая восстановленную группу 10483–10489.

Fixtures `test_linked_events_location_identity.py` используют реальные
`vk_intake.process_event` и TG Monitor results entrypoints с mocked child import
и моделью. Проверяются семь TG/VK children с одним запросом; полная 32-session
группа с ответом <4000 JSON characters; 33 targets с проверкой final target и
cross-chunk reciprocal links. Старый sporting edge окружён 30 более близкими
recall rows: grounded negative всё равно удаляет обе стороны. Отдельные controls
отклоняют foreign ID, conflicting memberships, address-only quotes и same-slot
группы. Fixtures также проверяют адрес/NULL address, source facts без source_text,
801 раннюю постороннюю строку, самостоятельные события и разные материалы
живописи. Page-build не выполняется (fixture URL + failing pagebuild mock);
observer только ставит штатные задачи. Это plumbing tests, не реальный provider
semantic quality. **READY FOR PARENT TESTS**: 36 parent PASS относятся только к
предыдущему HEAD `eb540a75`, не к текущему изменению.

Существующие same-slot copies требуют source adjudication через normal
match/create flow. Отдельного API merge двух существующих Event rows нет:
model duplicate outcome не выдаётся за выполненное слияние. Native correction
план ниже остаётся review-first и не запускается этим изменением.

Пересчёт вызывается после Smart Update create/merge и правок
`title/location_name/date/time`, в source parsing и в ручном UI. Изменённые
siblings получают `telegraph_build`. Same-day Telegram/VK публикация выбирает
детерминированный anchor (раннее время, затем меньший `event.id`) и показывает
все времена дня.

Это production foundation, но не целевая модель: стабильный target —
`occurrence_group_id` + relation kind/provenance/version/confidence/review/manual
lock, а `linked_event_ids` становится совместимым derived projection. До этой
миграции нельзя изобретать группу на frontend по title/type/venue.

Текущий foundation также не полный продуктовый контракт: статическая
страница должна ещё применять lifecycle-фильтры, переключать выбранную дату со
всеми зависимыми CTA/calendar/source данными и не отправлять siblings в блок
`semantic_related`.

### Cross-month compact labels

The formatter groups reciprocal occurrence dates by year and month instead of
repeating the month for every date. Thus a same-time family renders
`24, 25 июля, 27 сентября 19:00`; the two-line rail date is
`24, 25 июл, 27 сен`, with the full grouped schedule in `aria-label`. The
existing exact contracts `2, 9 ноября 19:00` and
`4 ноября 17:00, 19:00` remain unchanged. This formatting change does not relax
family identity: only reciprocal explicit `other_date_ids` may form a family.

## VK: карточки других дат и похожих событий

Штатный `vk_sync` читает `static_site_event_related.json` после общего Kaggle
`related-only` и выбирает только `pure_related`. `similar` с broad fallback
не является источником похожести. `linked_event_ids` дают отдельную карточку
других дат. Отменённые, скрытые, начавшиеся и объединённые записи исключаются.
Known canonical `linked_event_ids` исключаются из semantic retrieval и VK-подборки. Общий verifier получает как краткую подпись, так и подробное описание выбранного события: наличие подписи не должно скрывать факты программы. Полный многособытийный source carrier не подменяет выбранную программу. Вердикты `same_occurrence_group`/`duplicate_same_slot`, если они получены, не становятся semantic recommendations, но verifier не создаёт canonical family: её полнота остаётся ответственностью ingestion.

При включённом strict verifier первоначальное VK-задание с афишей ждёт anchor с `strict_verified=true` в существующем pending job. Старый непроверенный JSON не разрешает публикацию; проверенный anchor с пустым списком похожих пригоден для публикации.

Для похожих выбираются до пяти самостоятельных событий с публично доступными VK-постами. Перед выбором существующий service actor пакетно читает `wall.getById`; отложенные, удалённые и недоступные посты исключаются. Ошибка чтения оставляет обновление штатному retry, а не удаляет уже показанные связи.

Карточки 1080×1350 используют кожаную бирку сайта, шрифты Cygre без засечек,
дату, время, название, локацию и существующие curated графические медальоны.
Бесплатный вход обозначается только по `Event.is_free`; PNG получен из
канонического `free-listing-medallion.svg` для Pillow, без runtime SVG dependency.
Оригинальные изображения остаются первыми; карточки занимают только свободные
места из десяти вложений VK. С карточками всегда используется карусель. Для одной
исходной афиши подпись на загружаемой копии указывает на другие даты или похожие
события. Исходное изображение не меняется.

Список похожих без ссылок попадает в текст. Один комментарий
группы содержит VK-ссылки; его receipt хранится в существующем `Setting`,
а `guid` и чтение комментариев восстанавливают потерянный ответ без дубля. Уже отправленные комментарии не редактируются и не удаляются: после публикации существующий receipt завершает обновление без provider write.
Для отложенного поста комментарий ставится после публикации через существующий
JobOutbox comment handler с payload `relations`, который сохраняет ту же выборку, что и карточка; новый граф не пересобирает ссылки во время задержки, не включая отключённые
календарные docs/save операции. Действуют обычные CAPTCHA pause/recovery.

Новый пост с изображением ждёт своего anchor в общем результате и создаётся
сразу с полной каруселью. Уже поставленное задание использует существующий
outbox deferral и остаётся pending; обычный worker повторяет его после готовности.
Завершение расчёта не сканирует каталог и не создаёт/возобновляет VK задания.
Пустой список `pure_related` является готовым результатом и не мешает публикации.
Для постов без исходных изображений ожидание похожести не требуется.

Последующий пересчёт графа не ставит существующие публичные или отложенные
посты на редактирование. Это заменяет первоначальный автоматический backfill
после каждого расчёта. Обычные изменения канонического события остаются
существующему source sync; постоянного обратного переписывания рекомендаций нет.
HTML/Astro не запускаются, второй publisher или отдельная очередь не добавляются.

Названия на raster-карточке очищаются существующим guide emoji sanitizer:
декоративные emoji не превращаются в отсутствующий glyph Cygre. Переносы
считаются существующим pixel-wrap helper, длинное название заканчивается
многоточием и не перекрывает медальоны. В текстовом VK списке полное название
сохраняется. Изменение renderer отражено в существующей версии VK format hash.

После публикации postponed VK ID может измениться. Существующий
`_recover_managed_vk_live_url` сохраняет найденный public URL. Его результат —
boolean, поэтому delayed comment всегда
берёт обновлённый `Event.source_vk_post_url`, а не return value recovery.

VK также может не вернуть `postponed_id`: это подтверждено живой public
выборкой 13 ноября. Consumer передаёт уже полученные public items в
существующий exact-title/date matcher, включая его legacy date rules и
проверку единственности. Новый запрос на каждого peer не нужен; нет fuzzy
matching или второго алгоритма восстановления.

Карточка сохраняет явный диапазон дат DD.MM–DD.MM и не показывает служебное
полночное время, когда canonical `time_is_default=true`.


Хэш VK-публикации учитывает канонический текст, факты и исходные изображения,
но не текущую выборку рекомендаций. Изменение графа или числа дополнительных
карточек не является причиной редактировать пост с неизменным источником.
Проверка сохранности исходных изображений и обычная коррекция фактов сохраняются.
При восстановлении после CAPTCHA ещё не созданные анонсы и однократные
комментарии идут раньше обновлений managed-постов и страниц дайджеста.
Managed URL определяется по настроенной группе, включая старые строки
без хэша; URL чужого источника не считается уже созданным анонсом.
Прежний интервал отправки сохраняется.

Прежний хэш с рекомендациями принимается без VK-записи только при точном
совпадении полного исходного ввода и прежней выборки: текущей либо уже
сохранённой в payload однократного комментария. Существующий локальный хэш
обновляется через обычное сохранение результата. Если совпадение не доказано,
изменённые факты не помечаются синхронизированными без публикации.

На карточках формат события (`event_type`, например «Кинопоказ» или «Спектакль»)
показывается рядом с площадкой независимо от наличия краткого описания.
Используется канонический тип из события, без вывода жанра из названия.
Готовое краткое описание остаётся нейтральным дополнительным текстом,
только если оно целиком помещается с учётом названия и медальонов.

Общий limiter
остаётся единственным источником RPM/TPM pacing; фиксированная пауза на каждый
anchor не добавляется. `strict_verified` означает завершение всех проходов
проверки anchor, а не включённый флаг. Пустой набор кандидатов — готовый
пустой результат; частичный сбой не разрешает первоначальную публикацию.

## Передача native owner: коррекция данных после review

Это read-only план; в данном эксперименте данные и публичные поверхности
не изменялись. Восстановленные `10483–10489` (`meowafisha/9080`, включая 10488)
не переигрывать и не включать в повторный repair.

1. Снять read-only строки `Event`, все `EventSource` (включая aliases,
   candidate/occurrence keys), `EventSourceFact` и принятые identity decisions
   для `10285–10291`, `10707–10713` и `10990–10992`. Сверить каждый child
   с его локальным фрагментом `kultrunaya_chaika/8657` или спортивного carrier.
   Подготовить таблицу `source child → programme → date/time → canonical row → copies`
   с дословными цитатами, не summary всего исходного поста. ID не является
   доказательством canonical ownership. Проверить, что пары `10285+n/10707+n`
   действительно имеют один slot; указанное соответствие пока дано владельцем,
   а не подтверждено новым production чтением.
2. Для source adjudication использовать существующий `smart_event_update` и
   его обычный match/create flow (`_llm_match_or_create_bundle` / final identity
   adjudication), передавая исходный source child и сохранённые ключи.
   Принадлежность источника и фактов должна перейти к принятому существующему
   canonical row штатным match outcome. Exact replay может вернуть no-op:
   это НЕ означает, что отдельный уже существующий duplicate row был слит.
   **Общего API merge двух существующих rows нет.** Нельзя выдумывать такой
   вызов, менять request key для обхода replay или обещать, что повторный import
   удалит `10707–10713`. Неустранённые existing copies остаются отдельной
   незавершённой domain-задачей; новый repair framework не предлагается.
3. Только после принятого canonical соответствия построить source-grounded
   programme edges между разными slots `10285–10291`. Existing
   `recompute_linked_event_ids(db, event_id)` — доменный путь reciprocal update
   и stale backlink cleanup; запускать его для такой коррекции можно лишь
   после проверки model-selected canonical representative каждого slot.
   При недостаточном подтверждении неизвестные peers не связывать: выбранная
   моделью другая копия slot не доказывает выполненное слияние строк.
4. Для `10990 ↔ 10991` review должен подтвердить `independent_event`
   (матчи за 5-е и 3-е места), а для `10992` — отдельный финал. После review
   прямой вызов существующего writer
   `recompute_linked_event_ids(db, 10990, programme_decisions={10991: "independent_event", 10992: "independent_event"})`
   после source-grounded review удалит неверный edge с обеих сторон; passed verdicts
   не выполняют нового model request, publisher или enqueue сами по себе.
   Проверить обе строки, отсутствие link на 10992 и результат
   `_same_day_linked_publish_group`: отдельный anchor для каждого матча.
   При unknown существующая неверная взаимная связь останется, поэтому
   unknown не считается выполненной коррекцией.
5. Acceptance после будущей разрешённой коррекции: семь canonical дат имеют
   взаимные links, copies не участвуют в «Другие даты», sports не сворачиваются,
   самостоятельные standup/photo/children/painting rows сохраняются для
   независимого semantic-related retrieval. Никаких VK post/comment/site edits
   этот план не предусматривает.
