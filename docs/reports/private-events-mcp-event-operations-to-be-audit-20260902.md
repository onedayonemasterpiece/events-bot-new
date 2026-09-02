# Аудит TO-BE: EventsBot MCP — операции над событиями

> Дата аудита: 2 сентября 2026 года  
> Аудируемый baseline: `78017e7ab313d733afb6125b6465a3f7d7166f8b`  
> Аудируемый документ: `docs/operations/private-events-mcp-event-operations-to-be.md`, revision 2  
> Вердикт revision 2: **не готова к реализации без поправок**  
> Вердикт после принятия revision 3: **implementation-ready для поэтапной разработки; runtime ещё не реализован**

## 1. Цель аудита

Проверить, можно ли реализовать TO-BE без повторного продуктового проектирования,
не создавая второй событийный, публикационный или промо-контур. Аудит сопоставляет
требования с фактическими моделями, очередями, Smart Update, MCP security boundary,
статическим сайтом, промо и накопленными инцидентами.

## 2. Проверенные источники

### Канонические документы и код EventsBot

- `docs/operations/private-events-mcp.md`;
- `docs/features/smart-event-update/README.md`;
- `docs/features/promo-campaigns/README.md`;
- `docs/features/promo-campaigns/partner-promo.md`;
- `docs/features/event-media/README.md`;
- `docs/features/afishaengagement/README.md`;
- `docs/operations/sqlite-db-init.md`;
- `private_events_mcp/server.py`, `tool_catalog.py`, `access_policy.py`;
- `models.py`, `db.py`, `static_site_release.py`, `vk_auto_queue.py`;
- `source_parse_contract.py`;
- `INC-2026-05-07-vk-time-reschedule-wrong-match`;
- `INC-2026-08-24-vk-lifecycle-replay-stale-tg-repost`.

### IdeaHub voice intake

- `voice-20260902-154844-651facb3` — исходное ревью MCP/Smart Update/promo;
- `voice-20260902-163949-7ee6120d` — отдельное уведомление об изменении и
  отображение old → new на сайте;
- `voice-20260902-164447-cabb5893` — плашка «ПЕРЕНОС»/«ОТМЕНЕНО» непосредственно
  на изображении уведомления.

## 3. Что в revision 2 было правильно

1. Один EventsBot MCP runtime и одна предметная реализация при разных OAuth
   resource/audience projections для owner/operator, partner и Codex.
2. Полный Smart Update как обязательная граница создания канонического события.
3. Hard fact-consistency gate для exact и generated public text без `force`-обхода.
4. Точный `event_id` как единственный target lifecycle mutation.
5. Атомарный reschedule даты, времени и площадки.
6. Отдельная cancellation semantics с сохранением Event и истории.
7. Переиспользование существующих `JobOutbox` и promo-модели.
8. Revision-aware reconciliation и обязательные автотесты.

Эти решения сохраняются.

## 4. Найденные release-blocking пробелы

### A1. Не учтены два свежих voice intake

Revision 2 не определяла:

- отдельный social notice, когда исходная публикация уже ушла в ленте;
- правило «строго старше 24 часов»;
- видимый old → new diff на статической странице;
- графическую плашку статуса на изображении lifecycle notice.

Без этого реализация формально прошла бы тесты revision 2, но не выполнила бы
последние требования владельца.

**Решение:** включить эти требования в revision 3 и в release-blocking scenario
matrix.

### A2. Отсутствует typed POSTPONE без новой даты

Фактическая модель уже знает `lifecycle_status='postponed'`, а source parse
contract — `POSTPONE`. Но revision 2 имела только reschedule и cancel. Нельзя
подменять «перенесено, новая дата будет позже» отменой или придумывать дату.

**Решение:** добавить `event_postpone_prepare/commit`. Event сохраняет старые
логистические поля для истории, но current public schedule считается unknown;
active selectors и promo eligibility выключаются. Когда новая дата известна,
`event_reschedule_*` переводит `postponed → active` с тем же event ID.

`cancelled → active` через generic edit или reschedule запрещён. Отдельный restore
не входит в первую поставку и остаётся owner-only recovery decision.

### A3. Change history была необязательной, хотя текущий Event теряет old values

Текущая строка `Event` хранит только актуальные date/time/location/status. Без
append-only before/after ledger невозможно надёжно:

- показать «с → на» на сайте;
- создать корректный notification copy;
- доказать причину организатора;
- дедуплицировать notification jobs;
- восстановить status после timeout;
- отличить два последовательных переноса.

**Решение:** `event_change_log` становится обязательной минимальной таблицей, а
не optional fallback.

### A4. Нет авторитетной publication age и applied revision

Правило «отдельный notice, если прошло больше суток» нельзя вычислять по
`Event.added_at`, URL или времени последнего readback. Нужен подтверждённый
`first_published_at` для каждой surface/target. Для stale/up-to-date также нужен
`applied_event_revision`.

**Решение:** расширить существующий `event_publication` либо ввести строго
эквивалентный companion ledger полями:

- `publication_kind`;
- `event_change_id`;
- `first_published_at`;
- `last_published_at`;
- `applied_event_revision`;
- `provider_operation_ref`;
- `outcome_state`.

Предпочтительно расширение существующей таблицы. Если timestamp неизвестен,
система возвращает `notice_review_required` и не угадывает.

### A5. Нельзя использовать Afisha Engagement как lifecycle badge

Afisha Engagement — promo-зависимый VK CTA enhancer; Telegram в его MVP не
входит. Плашка lifecycle status является обязательной информацией, а не
промо-мотивацией.

**Решение:** deterministic derived lifecycle-notice asset поверх approved Event
Media. Исходный `EventPoster`, `photo_urls` и пиксели не меняются. Производный
артефакт связывается с `event_change_id`, event revision, source pixel SHA и
renderer/template version.

### A6. Не определены диапазоны дат и временная зона

Для multi-day события перенос start date без явной политики end date может
создать отрицательный или неверный диапазон.

**Решение:** при изменении start date у range-event обязательны либо:

- явный `end_date`, либо
- `range_policy='preserve_duration'`, показанный в preview.

Silent shift/keep запрещён. Все date/time changes нормализуются в
`Europe/Kaliningrad`, а audit хранит offset-aware effective timestamp.

### A7. Не было достаточно точной implementation map

Revision 2 описывала продуктовый контракт, но оставляла слишком много решений
кодовой реализации на последний момент: обязательность таблиц, новые JobTask,
порядок PR и граница Codex deployment.

**Решение:** revision 3 фиксирует минимальные схемы, JobTask и четыре bounded
implementation slice до release/deploy.

## 5. Принятые технические решения revision 3

### 5.1 Revision identity

Не добавлять `Event.revision INTEGER` без необходимости. Переиспользовать
существующий `static_site_release.event_public_revision(event)` как детерминированный
SHA публичного snapshot. Он уже включает date, time, location, lifecycle,
description, media и другие viewer-facing поля.

`event_change_log` хранит `base_event_revision` и `result_event_revision`.
`JobOutbox` связывается с `target_event_revision`.

### 5.2 SQLite migration

Production SQLite эволюционирует через идемпотентный `Database.init()` в `db.py`.
Изменение схемы не переносится в неиспользуемый отдельный миграционный контур.
Models синхронно обновляются в `models.py`.

### 5.3 Минимальные новые/расширенные сущности

1. `event_change_log` — append-only structured before/after, rationale,
   provenance, actor, tenant, approvals, digest, idempotency и outcome.
2. `event_publication` — authoritative publish timestamps, kind, applied revision,
   change and provider-operation binding.
3. `joboutbox` — nullable `event_change_id`, `target_event_revision`,
   `publication_kind`; индексы для status/readback. Содержимое задания остаётся в
   bounded `payload`, но identity не прячется только в JSON.
4. `JobStatus` получает terminal `superseded` и `cancelled` либо эквивалентные
   отдельные, напрямую читаемые terminal states. Маскировать их под `done` нельзя.

### 5.4 Новые JobTask внутри существующего JobOutbox

- `event_change_notice_render`;
- `tg_event_change_notice`;
- `vk_event_change_notice`.

Обычное обновление исходных managed projections продолжает использовать
существующие `telegraph_build`, `vk_sync`, `tg_event_publish`, `ics_publish`,
`tg_ics_post`, page/static-site jobs. Новый scheduler или outbox не создаётся.
MAX не объявляется поддержанным до отдельного adapter contract.

### 5.5 Notification policy

Для каждого previously published managed surface/target:

- `age > 24h` — отдельный notice планируется автоматически;
- `age <= 24h` — только reconcile/edit исходной публикации;
- age unknown — `notice_review_required`, автоматической публикации нет;
- поверхность без подтверждённой предыдущей публикации — `not_planned`;
- ровно 24 часа относится к `<= 24h`;
- один notice на `(event_change_id, surface, target)`;
- pending notice предыдущего изменения supersede-ится более новым до mutation
  boundary; уже опубликованный notice не переписывает историю;
- owner может явно `force` или `suppress` notice только с preview и audit reason;
  partner не может обходить owner review;
- `outcome_unknown` никогда не повторяется автоматически.

Prepare показывает точный список: какие managed originals будут изменены, где
будет отдельный notice, где ничего не планировалось и где нужен review.

### 5.6 Static-site policy

- stable event URL сохраняется;
- cancelled/postponed события исключаются из active discovery, но detail page
  остаётся доступна;
- detail page показывает заметный status banner;
- reschedule показывает только реально изменённые old → new поля: date/end date,
  time, venue/address/city;
- показывается optional public notice и effective timestamp;
- internal organizer comment, actor identifiers и private source metadata не
  попадают в public projection;
- SEO `EventCancelled`/`EventPostponed` сохраняется;
- `event_change_log` получает минимальную public allowlist projection.

### 5.7 Lifecycle badge policy

- labels: `ПЕРЕНОС` и `ОТМЕНЕНО`;
- артефакт производный, immutable и content-addressed;
- источник — approved Event Media;
- renderer использует существующую image geometry и safe-region evidence;
- если overlay закрывает важный объект/текст, применяется безопасная боковая
  полоса или расширение canvas, а не произвольное перекрытие;
- original poster не изменяется;
- при отсутствии безопасного renderable image provider policy решает: text-only
  только там, где это разрешено; иначе job остаётся blocked/failed truthfully;
- lifecycle badge не зависит от promo campaign и Afisha Engagement.

## 6. Implementation readiness

После переноса решений этого аудита в revision 3:

- обязательные пользовательские сценарии закрыты;
- существующие domain boundaries выбраны;
- обязательные migrations определены;
- retry/idempotency/revision semantics определены;
- public/private rationale boundary определена;
- notification and badge policies определены;
- список release-blocking tests определён.

**Дополнительного продуктового проектирования перед началом Slice A не требуется.**
Новые вопросы, обнаруженные при кодировании, должны решаться как расхождение с
контрактом или как отдельный owner decision, а не через импровизированный новый
контур.

## 7. Рекомендуемая реализация

### Slice A — structured core

- модели и идемпотентная SQLite migration;
- `event_change_log`;
- authoritative publication revision/time bindings;
- shared typed event-change service;
- fact consistency;
- create/edit/reschedule/postpone/cancel domain tests.

### Slice B — owner MCP

- owner write scopes и tool schemas;
- prepare/commit/status/idempotency;
- event/publication queue readback;
- никакого partner resource и никакого deploy на этом шаге.

### Slice C — reconciliation and viewer output

- revision-bound JobOutbox;
- original managed projection reconciliation;
- 24-hour lifecycle notice policy;
- deterministic lifecycle badge;
- static-site status/change history;
- worker/provider-fake and site tests.

### Slice D — promo and partner projection

- existing promo services through MCP;
- separate partner OAuth resource/catalog;
- tenant/organization/role policy;
- owner-review workflow and own-status readback;
- protocol/security tests.

### Release slice — Codex

Только после того как Slices A–D находятся в запушенной integration branch и все
локальные/CI тесты зелёные:

- fresh-read and independent audit;
- rebase/update against current `origin/main`;
- full relevant suite;
- exact-main merge;
- deploy;
- SQLite migration/readiness checks;
- OAuth scope/token refresh;
- authenticated `tools/list` readback;
- isolated live acceptance on test destinations;
- production smoke without user-visible test publication;
- final durable release evidence.

Codex на release slice не перепроектирует продукт и не реализует пропущенные
крупные функции. При обнаружении contract gap он останавливает release и
возвращает точную диагностику в implementation branch.
