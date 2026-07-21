# Экологичная аналитика, стабильное размещение и состояния пасхалки

> **Статус:** product/data/UX contract для discovery; production implementation
> отсутствует. Все численные target bands — предварительные canary-настройки до
> baseline, A/A, MDE/traffic check и owner acceptance.
> **Связанные документы:** [feature home](README.md),
> [критическая аналитика](product-analysis.md),
> [data ownership](../../architecture/personalization-data-ownership.md).

## Решения в одном экране

1. Пасхалка назначается пользователю/устройству **один раз** как логический
   `placement_bundle_id + placement_version` с заранее зафиксированными anchors
   для layout/accessibility paths и остаётся там до находки или expiry.
   Reload, новый визит, подсказка, dislike и изменение порядка карточек не дают
   reroll.
2. После находки она не исчезает бесследно и не становится новой целью. В том же
   месте остаётся спокойный статичный marker **«Найдено — открыть историю»** до
   expiry или явного пользовательского hide.
3. `dislike` не меняет прогресс, место, difficulty, eligibility или награду.
   `dislike`, `report` и `hide` — три разные операции.
4. До находки объект по умолчанию статичен. Halo/pulse — конечная ступень
   подсказки, не постоянный beacon. После находки motion прекращается.
5. Статистика собирается как компактные opportunity summaries и daily/campaign
   rollups по стабильным placement/type dimensions. Не хранится вечный поток
   scroll/impression events, raw URL, координаты, IP или user path.
6. Первая onboarding-пасхалка может быть заметно легче. Для обычных и сложных
   большая часть qualified пользователей не должна находить её без подсказки в
   pre-registered time window; при этом frustration, accessibility gap и
   недоставка ограничены guardrails.

## 1. Что означает «локация»

В аналитике нельзя смешивать четыре разных понятия:

| Dimension | Значение | Пример | Privacy/cardinality rule |
|---|---|---|---|
| `placement_id` | стабильное логическое место в UI | `weekend.feed.between_sections.02` | allowlist; не CSS selector и не индекс карточки |
| `page_family` | тип поверхности | `home`, `weekend`, `event_detail`, `my_collection` | фиксированный enum |
| `subject_location_id` | региональный объект, о котором рассказ | музей/маяк/район из канонического справочника | без пользовательских координат |
| `interaction_type` | способ discovery | `inline_token`, `navigation_clue`, `collection_clue`, `accessible_text_path` | фиксированный enum |

**Статистика «по локациям» по умолчанию означает `placement_id/page_family`.**
Если пасхалка рассказывает о физическом месте, допустим отдельный
`subject_location_id`, но продукт не собирает GPS пользователя, точный IP,
маршрут передвижения, click coordinates, raw URL/query/referrer или свободный
текст страницы.

`placement_id` сохраняется, пока место выполняет ту же продуктовую работу.
Изменение геометрии/копирайта/anchor создаёт новую `placement_version`; новая
работа/поверхность получает новый ID. Это позволяет сравнивать версии, не
раздувая cardinality. Уже назначенная версия и её anchors неизменяемо доступны
до expiry последних assignments либо проходят явный safety relocation.

## 2. Три независимых автомата

### 2.1. Глобальное состояние объекта

```text
DRAFT → SCHEDULED → LIVE → EXPIRED → ARCHIVED
                     └→ SUSPENDED → LIVE | EXPIRED
```

`SUSPENDED` применяется к подтверждённой ошибке факта, IP/legal blocker,
небезопасной/недоступной точке или технически сломанному placement. Уже найденное
не отбирается. Для ненайденного включается заранее проверенный equivalent fallback
либо честный paused state.

### 2.2. Назначение и прогресс пользователя

```text
UNASSIGNED
  → ASSIGNED_LOCKED
  → AVAILABLE
  → HINTED_L1 → HINTED_L2 → HINTED_L3
  → FOUND
  → FOUND_ARCHIVED
```

Альтернативы:

```text
AVAILABLE | HINTED_* → EXPIRED_UNFOUND
AVAILABLE | HINTED_* → RELOCATED_FOR_SAFETY → AVAILABLE | HINTED_*
```

`ASSIGNED_LOCKED` хранит:

- `campaign_rules_version`, `egg_id`, `egg_version`;
- `placement_bundle_id`, `placement_id`, `placement_version`, зафиксированные
  layout/accessibility anchors, `placement_pool_id`, mode/cohort;
- `assigned_at`, `available_from`, `expires_at`;
- difficulty band и accessible equivalent path;
- assignment reason/version, не behavioral score.

### 2.3. Feedback/preferences

```text
NONE | LIKED | DISLIKED(reason[]) | REPORTED(reason[]) | HIDDEN_BY_USER(scope)
```

Feedback не является состоянием прогресса. Пользователь может одновременно иметь
`FOUND + DISLIKED`; находка остаётся частью коллекции.

## 3. Пасхалка не убегает

### Assignment invariant

Выбор выполняется один раз из заранее проверенного эквивалентного пула:

```text
bucket = deterministic_hmac(
  campaign_id,
  egg_id,
  stable_audience_id,
  assignment_version
)
placement_bundle = eligible_equivalent_bundles[bucket % bundle_count]
persist bundle and all predeclared layout/accessibility anchors
```

- авторизованный пользователь получает server-owned assignment;
- anonymous/device MVP хранит подписанную/версионированную assignment record
  локально и честно предупреждает, что очистка browser state её уничтожит;
- при login merge переносит assignment/progress идемпотентно; конфликт разрешается
  в пользу уже найденного, иначе сохраняется более раннее действующее назначение;
- assignment фиксирует не один хрупкий DOM-узел, а versioned semantic bundle:
  например, одна и та же зона истории плюс заранее проверенные mobile, desktop,
  keyboard и screen-reader anchors. Они выбираются вместе, а не случайно при
  каждом устройстве;
- route/anchor задаётся семантически, а не как «после карточки №8», поэтому
  перестановка каталога не перемещает цель;
- новое окно, reload, hint, dislike, смена viewport и повторный вход не rebucket;
- signed-in cross-device получает тот же `placement_bundle_id`; каждый layout
  использует свой **уже записанный** anchor внутри этого bundle. Это одно
  продуктово объяснимое место, а не новый reroll;
- если ни один зафиксированный anchor больше не существует, target честно
  приостанавливается либо проходит `RELOCATED_FOR_SAFETY`; нельзя молча выбрать
  следующий доступный slot.

### Единственное допустимое перемещение

Только `RELOCATED_FOR_SAFETY` при недоступности, ошибке или legal/safety blocker.
Оно:

- сохраняет `egg_id`, progress, hint level, assignment time и eligibility;
- пишет `supersedes_placement_id/version` и operator reason;
- использует заранее проверенный equivalent fallback;
- сообщает нейтрально «Место находки обновлено», не обвиняя пользователя;
- никогда не используется для улучшения метрик или выравнивания A/B на ходу.

## 4. Что происходит после находки

Принято промежуточное решение **found echo**:

1. На активации — одна короткая confirmation-анимация либо статическое
   подтверждение при reduced motion.
2. Server/local authority один раз фиксирует `FOUND`; повторный click/refresh не
   создаёт второй find/claim.
3. В том же placement target превращается в спокойный marker
   **«Найдено — открыть историю»**.
4. Marker остаётся до expiry, не pulse/shimmer, не требует повторного сбора и
   позволяет открыть story/fact/share/feedback.
5. Пользователь может отдельно выбрать `Скрыть найденные на страницах`; это не
   удаляет предмет из коллекции.
6. После expiry marker можно убрать с исходной страницы; найденный объект и история
   остаются в архиве `Моё`.
7. Ненайденный target исчезает после expiry, но культурная история может стать
   доступной в публичном архиве без заднего числа созданной prize eligibility.

Почему не исчезновение сразу: оно создаёт layout/orientation loss и сомнение,
сработала ли находка. Почему не полная активная карточка до expiry: она забирает
attention budget и выглядит как banner. Found echo сохраняет место и память, но
перестаёт звать к действию.

## 5. Что делать с dislike

### Персональный dislike

После находки `Не нравится`:

- сохраняет отдельный feedback signal;
- предлагает необязательную причину:
  `too_easy | too_hard | unclear | not_interesting | intrusive |
  weak_campaign_link | inaccessible | fact_error | unsafe | other`;
- даёт `Отменить`/изменить оценку;
- **не** меняет `FOUND`, assignment, место, difficulty, eligibility или claim;
- не скрывает marker автоматически.

Для управления интерфейсом существуют отдельные команды:

- `Скрыть эту найденную пасхалку на страницах`;
- `Не показывать подсказки этой кампании`;
- `Не показывать пасхалки этой кампании`;
- позднее — global opt-out.

### Агрегированный dislike

- обычные dislike влияют только на редакционный review после достижения
  достаточного denominator; объект не перемещается и не удаляется mid-campaign;
- `fact_error|unsafe|inaccessible` идут в priority moderation и могут перевести
  объект в `SUSPENDED` после server/operator validation;
- product-quality threshold может остановить **новые assignments/hints**, но уже
  назначенные не rebucket. Полное снятие — только safety/legal/technical решение;
- партнёр не видит точные live negative counts по активному hidden placement и не
  может менять его по ходу кампании.

Reroll по dislike запрещён: иначе dislike превращается в кнопку поиска более
лёгкого места, ломает fairness и искажает эксперимент.

## 6. Visual/motion contract

Сложность создаётся контекстом, распознаванием и маршрутом, а не opacity `.05`,
маленькой click area или недоступным hover.

| State | Визуал | Motion | Interaction/a11y |
|---|---|---|---|
| `available/unfound` | compact token внутри target не меньше `44×44`; различимый силуэт/кольцо, не banner | статично | обычный `<a>`/`<button>`, keyboard order, accessible name без ответа |
| `hinted L1` | немного сильнее статическое кольцо/contrast | без обязательной motion | clue сообщает тему, не координату |
| `hinted L2` | внешний halo | один конечный slow halo sequence | не двигает focus, reduced-motion → статическое кольцо |
| `hinted L3` | видимая текстовая clue/маршрут | motion не усиливается | equivalent keyboard/screen-reader route |
| `hover` | restrained halo/border | канонический короткий transition | геометрия не меняется |
| `focus-visible` | сильный design-system focus ring и короткая подпись | короткий transition либо none | заметнее hover; Enter/Space |
| `found reveal` | полноцветный токен + progress | один short reveal | `aria-live="polite"`; focus contract dialog/inline |
| `found echo` | static marker `✓ Найдено`, story link | none | повторно открываемая история |
| `found + disliked` | тот же found echo; feedback state/Undo рядом | только короткий state transition | `aria-pressed`, находка не сереет как наказание |
| `loading` | reserved skeleton | shimmer только как loading signal | reduced-motion static; shimmer не выдаёт место |
| `expired/suspended` | честный static status или target отсутствует | none | нет ложной интерактивности |

Запрещены continuous pulse/shimmer/sweep, flashing, autoplay audio/haptics,
pointer-tracking halo, layout-changing scale/rotation, first-paint animation,
full-width pre-find card и motion после находки. CSS анимирует только дешёвые
`opacity/transform` pseudo-elements без blur/continuous `requestAnimationFrame`;
геометрия зарезервирована; вклад механики в CLS `= 0` — acceptance target, который
ещё требуется подтвердить на прототипе.

Все duration/repeat/cooldown числа остаются motion-test candidates. Near-expiry
усиливается текстовой подсказкой в `Моё`, а не более частым pulse.

## 7. Экологичная статистика

### 7.1. Source-of-truth routing

| Контур | Что хранит | Что не хранит |
|---|---|---|
| Fly/core SQLite | версии egg/placement/type, campaign binding, scheduler/kill state, aggregate report pointer/hash | visitor/profile telemetry |
| Supabase private | durable assignment/progress/find/hint current state, consent, merge/idempotency audit | weak view/impression firehose |
| YDB analytics | de-identified compact opportunity summaries с TTL и daily/campaign rollups | email, auth uid, raw anon id, IP, second profile |
| Object Storage/CDN | public manifests/assets и frozen aggregate reports | private progress/actor ids |

Browser не пишет прямо в YDB. Same-origin validator или scoped RPC принимает
bounded typed summary; authoritative `FOUND` приходит из progress mutation/outbox.
YDB outage не отменяет find и не блокирует event CTA.

### 7.2. Вместо event firehose

Default path строится из сильных server-side mutations/outbox: assignment,
explicit hint, find и feedback. Опциональный delivery summary разрешён один раз
на discovery session, если без него невозможно отличить «сложно» от «не
доставлено». Клиентский viewport observer не является обязательной аналитикой.

При включённом delivery summary браузер локально дедуплицирует его и отправляет
максимум одну компактную запись на grain:

```text
campaign_rules_version
× egg_version
× placement_version
× actor_campaign_hmac
× discovery_session
```

Поля — bounded flags/counters/buckets:

- assigned/eligible и `delivered_path_once`; отдельный `inserted_once` допустим
  только для диагностики rollout;
- opened/collected once, unassisted vs hint-assisted;
- explicit hint requested/revealed level; proactive hint — отдельный флаг;
- hide, bounded feedback/report reason и downstream meaningful-action flags;
- allowlisted dimensions: local date, treatment, page family, placement/version,
  egg type/story domain/difficulty, mode, viewport/layout/accessibility path,
  actor/trust class.

Не сохраняются каждый scroll, hover, intersection tick, found-echo reopen или
repeated rendering. Raw diagnostic events выключены по умолчанию и допускаются
только sampled, короткоживущим debug window. Клики `found echo → story` при
необходимости считаются обычной агрегированной навигацией, а не персональным
долгоживущим следом пасхалки.

### 7.3. Product denominator и delivery diagnostic

1. `assigned_eligible`: все pre-treatment eligible human actors, которым было
   назначено placement, — основной ITT denominator сложности.
2. `delivered_path_once`: actors, для которых назначенный путь хотя бы раз был
   реально доставлен, — техническая диагностика delivery, но не способ удалить
   non-finders из основного KPI.

Каждый difficulty report показывает рядом
`delivered_path_once / assigned_eligible`. Нельзя искусственно повысить find
rate, перестав показывать пасхалку сложным пользователям.

`valid_viewable`/viewport intersection не входит в default product KPI: такая
телеметрия добавляет слежение и создаёт селекцию по дожившим до slot. Для
локального usability-пилота она может быть временной диагностикой с отдельным
consent/retention, но не постоянным знаменателем production-отчёта.

### 7.4. Compact rollups

- `egg_placement_daily`: date × campaign/rules × egg/version × low-cardinality
  semantic placement/version × interaction type × treatment × layout/accessibility
  path;
- `egg_difficulty_campaign`: full-cycle survival/find/hint/frustration по
  egg/placement/type/difficulty;
- `egg_type_weekly`: type × difficulty × layout/accessibility только после
  достаточного числа разных eggs и пользователей;
- frozen campaign report artifact + hash; core хранит только pointer/hash.

Для type rollup используется equal-egg/hierarchical estimate, а не
exposure/user-weighted average, иначе одна популярная пасхалка определит весь тип.
Малые cells скрываются
или объединяются; candidate privacy suppression `k≥20` **не является** minimum
sample для продуктового вывода. Release/fairness verdict требует отдельного
предварительного power/MDE/interval calculation.

### 7.5. Retention

Предварительный экологичный budget, наследующий существующие project ranges:

- optional raw diagnostic sample: `7–14d`;
- compact opportunity summaries: `14–30d`;
- strong action/find linkage and quarantine: `30–90d` / `7–14d`;
- daily/campaign aggregates: до `12 months`, затем пересмотр/удаление;
- durable found collection: до explicit delete/account lifecycle согласно
  утверждённой retention policy;
- no-consent visitor остаётся функционален, но его remote telemetry отсутствует;
  coverage показывается в каждом отчёте.

## 8. Целевые KPI сложности

### Принцип

Первая пасхалка обучает контракту и может быть легче. Для `standard` и `hard`
«сложно» означает: большая часть qualified пользователей не находит объект без
подсказки в первые валидные возможности. Это не означает невидимый target,
сломанную страницу или accessibility gap.

Все bands ниже — **provisional canary targets**, а не обещанные SLO. Они
пересматриваются после A/A, usability pilot и полного non-prize cycle.

`qualified human` здесь определяется **только до assignment**: pre-registered
campaign eligibility, consent/telemetry cohort и заранее заданный human/trust
filter. Нельзя исключить пользователя после assignment из-за того, что он не
увидел slot, не вернулся, не нашёл объект или ухудшил KPI. Bot/suspicious
sensitivity cut задаётся до просмотра outcomes.

### KPI 1 — `unassisted_discovery_within_W` (primary, ITT)

```text
unique qualified humans who collected before any hint was revealed within the
pre-registered window W
/
all assigned_eligible qualified humans
```

`W` фиксируется до запуска относительно длины кампании; для семидневного canary
кандидат `48h`, но это не универсальный стандарт.

- Clock начинается в `assigned_at`.
- Primary cohort получает assignment только если до expiry остаётся полное `W`.
  Более поздний участник может играть, но попадает в заранее помеченный
  `late_assignment` stratum, показываемый отдельно, а не исчезает из отчёта.
- `SUSPENDED`, non-delivery и safety relocation не удаляются из ITT: отчёт
  показывает их отдельными competing/delivery outcomes и sensitivity cut.
- Explicit hint и proactive hint — разные competing outcomes. Политика/время
  proactive hints фиксируются до canary; после любого reveal последующая находка
  не считается unassisted.
- Account/consent deletion исполняется по privacy policy. Уже обезличенный rollup
  не реидентифицируется; удалённая либо потерянная anonymous record маркируется
  как administrative loss и показывается worst/best-case sensitivity, а не
  молча исключается.
- No-consent actors функционально играют локально, но не входят в remote KPI;
  доля consented/pre-treatment measurable assignments всегда показана как
  coverage limit.

| Planned band | Candidate target | Интерпретация |
|---|---:|---|
| `onboarding/first` | `55–75%` | первая доказывает механику, но не автособирается |
| `standard` | `15–35%` | большинство не найдёт быстро без подсказки |
| `hard/finale` | `5–20%` | редкая самостоятельная находка без превращения в ноль |

Repeat views/finds не умножают numerator; explicit и proactive hint attribution
показываются раздельно; любой hint reveal исключает последующий find из
`unassisted`; nonfinders и не получившие path остаются в ITT denominator;
delivery показан отдельно. Эти bands — формулировка проверяемой продуктовой
гипотезы по прямому требованию «не должно быть легко», а не release SLO.

`unassisted_discovery@K` можно оставить только как вторичную usability-диагностику
на пилоте с capped opportunities; она не заменяет ITT KPI и не отбрасывает тех,
кто не дошёл до K.

### KPI 2 — `assisted_discovery_within_A`

```text
unique qualified humans who collected within the pre-registered assistance
window A from assigned_at, including after hints
/
all assigned_eligible qualified humans
```

Primary cohort получает полный `A` до expiry; late assignments и administrative
loss показываются теми же strata/sensitivity rules, что KPI 1. Raw
`completion_by_expiry` остаётся описательным campaign snapshot по assignment-day,
но не заменяет fixed-exposure KPI.

Candidate target bands:

- onboarding: `70–90%`;
- standard: `30–55%`;
- hard/finale: `15–35%`.

Если unassisted слишком высок — объект слишком очевиден. Если assisted остаётся
низким при высокой delivery — подсказки/маршрут непонятны, а не «правильно
сложны». `delivered_path_once` всегда показан рядом, но не меняет denominator.

### KPI 3 — `difficulty_quality_guardrail`

Показывается как dashboard, не одно скрывающее проблемы число:

- `too_easy` among responders: onboarding diagnostic; standard candidate ceiling
  `25%`, hard `15%`;
- `too_hard|unclear|intrusive|hide` — отдельные rates; candidate review threshold
  `25%` standard и `35%` hard, но `unsafe|inaccessible|fact_error` никогда не ждут
  статистического порога;
- placement gap in `unassisted_discovery_within_W`: candidate `≤15pp`;
- visual vs equivalent accessibility path gap: candidate `≤10pp`;
- любой gap verdict требует minimum sample/interval; иначе `insufficient evidence`.

Response rate и denominator всегда показываются: dislike/like среди добровольно
ответивших нельзя выдавать за мнение всех участников.

### Product guardrails рядом с difficulty

- `delivered_path_once / assigned_eligible` delivery;
- core event CTA и time/cards-to-first-relevant non-inferiority;
- JS/CLS/error, hint viewability и progress drift;
- opt-out/complaints и accessibility-path parity;
- bot/quarantine/SRM/telemetry coverage.

Completion/time-on-site не становятся North Star. Downstream meaningful campaign
action vs holdout остаётся главным product outcome.

## 9. Privacy, bots и честность отчёта

- analytics actor key — rotating campaign-scoped HMAC, не email/auth uid/raw
  anon id/IP;
- crawler/preview/monitor/test/bot-likely/suspicious actors исключаются по
  заранее зарегистрированным правилам и показываются отдельным sensitivity cut;
- suspicious outcomes не удаляются post hoc только потому, что портят KPI;
- signed campaign/egg/placement/rules versions, sequence validation, idempotency,
  impossible-timing и capped counts;
- exact active hidden-placement stats недоступны public/partners до expiry;
- no-consent traffic coverage и small/insufficient cells явно видимы;
- partner/editor не может rebucket или перенести assigned users ради метрики.

## 10. Acceptance scenarios

```gherkin
Scenario: Reload не перемещает пасхалку
  Given user assigned egg E to placement P version V
  When user reloads, returns later, requests a hint or dislikes another egg
  Then E remains at P/V until found, expiry or audited safety relocation

Scenario: Находка остаётся узнаваемой до expiry
  Given egg E is found at placement P
  Then P renders a static "Найдено — открыть историю" marker
  And refresh cannot create a second find or claim
  And user may hide the marker without deleting collection progress

Scenario: Dislike does not reroll
  Given E is FOUND at P
  When user selects "Не нравится" and reason "слишком легко"
  Then feedback changes but FOUND, P, eligibility and collection remain unchanged
  And Undo is available

Scenario: Eco summary does not persist viewport noise
  Given the same egg enters viewport ten times in one discovery session
  Then no ten raw impression rows are persisted
  And at most one optional delivered_path summary exists for the session

Scenario: Difficulty report cannot hide non-delivery
  Given placement delivery falls while find rate rises
  Then the report shows both assigned_eligible and delivered_path_once
  And release verdict cannot be green on find rate alone

Scenario: Reduced motion preserves the game
  Given prefers-reduced-motion is active
  Then halo/reveal motion is disabled
  And the static token, focus ring, hint and found status remain complete
```

## 11. Решения перед implementation

1. Утвердить exact `placement_bundle/placement_id` registry и equivalent-slot
   review process.
2. Утвердить anonymous assignment promise и login-merge conflict rule.
3. Зафиксировать motion prototypes и timing/attention budget на viewport/a11y matrix.
4. Рассчитать traffic/MDE/intervals и откалибровать KPI bands/window `W`.
5. Утвердить telemetry consent, write path, TTL и small-cell threshold.
6. Назначить owner/SLA для dislike review и immediate safety reports.
7. Решить, остаётся ли found echo до expiry по умолчанию либо пользовательский
   `Скрыть найденные` включён default-on после первой сессии; текущее решение —
   echo остаётся, hide только явный.
