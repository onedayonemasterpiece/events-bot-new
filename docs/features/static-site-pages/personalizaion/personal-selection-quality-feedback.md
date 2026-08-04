# Оценка качества персональной подборки

> **Статус:** принятое продуктовое направление / предварительный implementation contract.  
> **Дата среза:** 2026-08-04.  
> **Scope:** `/dlya-menya/`, отдельные персональные продолжения и иные поверхности, где сервис честно заявляет персональную выдачу.  
> **Цель:** получать привязанную к конкретной выдаче обратную связь, чтобы понимать не только что персонализация работает недостаточно хорошо, но и на каком слое возникла ошибка: профиль, модель, constraints, каталог, surface policy, presenter или интерфейс.

## 1. Продуктовое решение

Идею следует реализовать, но **не как обычный NPS продукта**.

Нормативное название:

```text
personal_selection_quality_feedback_v1
```

Это контекстная оценка качества конкретной персональной выдачи. Внутренний
`personalization NPS` (`pNPS`) допускается как вторичная агрегатная метрика, но
не заменяет главную продуктовую метрику и не сравнивается напрямую с NPS всего
сервиса.

Главная связь с целевой системой:

```text
P(first relevant event within 30 cards) >= 0.95
cards_to_first_relevant_p95 <= 30
```

Оценка должна помогать объяснить, почему этот gate закрыт или не закрыт для
конкретной persona, surface, model revision и catalog snapshot.

## 2. Основной пользовательский сценарий

### 2.1. Первый вопрос — фактическая полезность

После достаточного просмотра персональной выдачи:

> **Где встретилось первое событие, куда вам действительно захотелось бы пойти?**

Ответы:

```text
В первых 10 карточках
В карточках 11–20
В карточках 21–30
Не встретилось
Не уверен / не хочу отвечать
```

Это первичный вопрос, потому что он напрямую связан с принятой метрикой поиска
релевантного события в пределах 30 карточек.

### 2.2. Второй вопрос — score 0–10

> **Насколько полезной была эта подборка?**  
> `0` — совсем не попала в интересы; `10` — попала очень точно.

Из него можно считать внутренний `pNPS`, но score анализируется вместе с
позицией первого релевантного события и поведением пользователя.

### 2.3. Диагностический follow-up

При `0–6`, ответе `Не встретилось` либо по добровольному раскрытию причины:

```text
Не мои темы
Слишком однообразно
Интересное оказалось слишком глубоко
Не учтены цена или бесплатность
Не учтены дата или время
Не учтены город, расстояние или транспорт
Не учтено, с кем я собираюсь
Слишком мало подходящих событий
Показали уже скрытое или неинтересное
Карточки/объяснения не помогли понять события
Другое
```

Допускается необязательный bounded comment:

> Что именно мы пока не поняли о ваших интересах?

Свободный текст не является обязательным и не должен автоматически попадать в
профиль или обучение модели.

## 3. Где и когда показывать

### 3.1. Основной trigger

- после просмотра первых 30 карточек;
- либо в конце конечной персональной выдачи, если она короче;
- только после того, как пользователь действительно получил персональную
  surface, а не static/popular fallback;
- блок является частью потока, не modal и не перекрывает карточки;
- CTA, переходы и навигация никогда не блокируются.

### 3.2. Контроль selection bias

Только end-of-list trigger создаёт смещение: довольный пользователь может быстро
найти событие и уйти, а до конца чаще доходят неудовлетворённые.

Поэтому нужен второй sampled trigger:

- после возврата со страницы события;
- после save/CTA/share outcome;
- либо после завершения персональной сессии;
- только если пользователь увидел достаточную часть served list;
- без вопроса сразу в момент сильного действия, чтобы не мешать задаче.

Отчёт всегда разделяет:

```text
trigger=end_of_list
trigger=sampled_success_session
```

и не смешивает их без bias analysis.

### 3.3. Частота

Стартовые configurable hypotheses:

- production: не более одного запроса на пользователя × surface × model
  revision в течение 14 дней;
- focus cohort: один запрос после новой существенной model revision либо
  longitudinal scenario;
- не повторять после `Не хочу отвечать` в текущей сессии;
- не спрашивать при каждом открытии `/dlya-menya/`;
- cooldown и sampling version входят в evidence.

Точные значения подтверждаются usability/focus data и не зашиваются как
неизменяемая бизнес-истина.

## 4. Где не показывать

- на календарном primary list, где порядок не персонализируется;
- до activation персонализации;
- на static/popular fallback под заголовком, создающим впечатление, что это
  персональная выдача;
- при transport/storage error, если невозможно честно связать feedback с
  served list;
- сразу после destructive/error flow;
- поверх CTA, формы авторизации, обратной связи фокус-группы или системного
  уведомления;
- детям или на sensitive surface без отдельного safety review.

## 5. Data contract

Каждый feedback относится к одной конкретной served list:

```json
{
  "schema_version": "personal_selection_quality_feedback_v1",
  "feedback_id": "uuid",
  "served_list_id": "opaque-id",
  "served_list_hash": "bounded-hash",
  "surface_id": "for_me",
  "model_version": "p13n-model-vN",
  "profile_revision": 18,
  "catalog_revision": "catalog-sha",
  "surface_policy_version": "collection-surfaces-vN",
  "experiment_id": "optional",
  "trigger": "end_of_list",
  "cards_exposed": 30,
  "cards_opened": 3,
  "first_relevant_bucket": "11_20",
  "quality_score_0_10": 7,
  "reason_codes": ["interesting_too_deep"],
  "comment": null,
  "sampling_version": "p13n-qf-sampling-v1",
  "submitted_at": "server-time"
}
```

Server определяет subject/account/device ownership из утверждённой session или
credential boundary. Browser payload не назначает `subject_id`, не передаёт
email, полный профиль, bearer token, raw history или полный каталог.

### 5.1. Served-list evidence

Для аудита сохраняется bounded evidence:

- ordered event ids до 30 либо ссылочный immutable served-list receipt;
- фактические rank;
- reason codes/model components в ограниченной диагностической форме;
- profile/model/catalog/surface versions;
- exact hides и eligibility exclusions только как sanitized counters/codes;
- DOM order hash и доказательство соответствия served evidence.

Не создаётся permanent row на каждый обычный показ. Более подробное evidence
сохраняется только при feedback, в sampled evaluation либо в longitudinal test.

## 6. Feedback не является прямым сигналом профиля

Обязательный инвариант:

```text
quality_feedback → evaluation/audit
quality_feedback != automatic interest mutation
```

Оценка `3/10` означает, что выдача оказалась плохой. Она сама по себе не
означает, что пользователь не любит жанр, город или тип события.

Допустимо:

- использовать feedback для model evaluation;
- создавать audit case;
- пополнять human judgement queue после review;
- сравнивать с фактическими действиями и анкетой;
- включать агрегированные reason codes в план калибровки.

Запрещено без отдельного review:

- понижать facet из одного низкого score;
- превращать free-text comment в профиль;
- обучать production model напрямую на raw feedback;
- считать `Не встретилось` exact hide всех показанных событий;
- использовать feedback как activation event.

## 7. Метрики

### 7.1. Primary

- `first_relevant_within_30_rate`;
- `cards_to_first_relevant_p50/p90/p95`;
- `no_relevant_event_rate`;
- lower bound 95% confidence interval для stochastic/production samples.

### 7.2. Secondary

- score distribution `0..10`;
- mean/median quality score;
- internal `pNPS = %9–10 - %0–6`;
- response rate;
- skip rate;
- low-score reason distribution;
- `interesting_too_deep_rate`;
- `not_my_topics_rate`;
- `constraint_miss_rate`;
- `catalog_supply_miss_rate`;
- feedback rate by trigger type.

### 7.3. Required cuts

- persona / focus participant cohort;
- profile maturity: cold/session/short/mid/long;
- surface;
- model, taxonomy, catalog and surface-policy revision;
- anonymous/account-linked;
- desktop/mobile/PWA;
- end-of-list vs sampled-success trigger;
- first/returning session;
- experiment/control variant.

Нельзя публиковать только общий средний score: он маскирует failure отдельных
personas и supply/context problems.

## 8. Диагностика причины

При низком результате audit сначала определяет failure layer:

| Layer | Признак |
|---|---|
| `PROFILE` | интересы/constraints пользователя представлены неверно |
| `MODEL` | профиль верен, но rank/formula ошибочны |
| `SURFACE_POLICY` | search/popular/related/calendar meaning нарушен |
| `CATALOG_SUPPLY` | подходящих eligible событий в текущем каталоге мало |
| `EVENT_FEATURES` | темы/атмосфера/constraints события извлечены неверно |
| `PRESENTER_UI` | релевантное событие было, но карточка/позиция/объяснение не сработали |
| `TRANSPORT_STATE` | применена старая/несовместимая projection |
| `TEST_DATA` | judgement/persona/fixture ошибочны или устарели |

Нельзя сразу менять weights только потому, что score низкий.

## 9. Связь с longitudinal E2E

Добавляются сценарии:

### QF0. Успешная выдача

```gherkin
Scenario: Persona находит релевантное событие в первых 10 карточках
  Given served list and profile/model/catalog revisions are recorded
  When user submits first_relevant_bucket=1_10 and score=9
  Then feedback is linked to the exact served list
  And no profile facet changes because of feedback itself
  And report counts the response in the correct persona/surface/model cut
```

### QF1. Не найдено релевантного события

```gherkin
Scenario: Не встретилось подходящее событие в top-30
  Given persona judgements contain eligible relevant events outside top-30
  When user selects no_relevant and reason interesting_too_deep
  Then audit classifies MODEL/RANK candidate failure
  And next-iteration report includes affected persona, surface and rank delta
```

### QF2. Supply failure

```gherkin
Scenario: В каталоге нет подходящего eligible события
  Given no relevant eligible event exists in the frozen catalog snapshot
  When user selects no_relevant
  Then failure is CATALOG_SUPPLY, not automatically MODEL_RELEVANCE
```

### QF3. Trigger bias

```gherkin
Scenario: End-of-list and sampled-success responses remain separated
  Given both trigger types are collected
  Then response rates and scores are reported independently
  And combined metric is not emitted without sampling/bias metadata
```

### QF4. Idempotency and ambiguous transport

```gherkin
Scenario: Repeated submission cannot duplicate one evaluation
  Given stable feedback_id and served_list_id
  When response is lost after dispatch and client reconciles/retries
  Then exactly one feedback record exists
  And UI does not claim success before durable ACK/reconcile
```

### QF5. Model revision comparison

```gherkin
Scenario: Calibration iteration improves a failing persona
  Given revision N has first_relevant_within_30_rate below target
  When revision N+1 is evaluated on the same persona/catalog judgements
  Then before/after report shows metric, reasons and guardrails
  And rollout expands only if hard invariants remain zero
```

## 10. Transport and storage

Recommended same-origin operation:

```text
POST /api/personalization/v1/selection-quality-feedback
```

или отдельный reviewed idempotent RPC behind same-origin service.

Требования:

- stable `feedback_id` idempotency;
- schema/size validation;
- bounded optional comment;
- subject/epoch binding;
- no direct browser table DML;
- ambiguous result reconciles by `feedback_id`;
- failure never blocks navigation;
- client может хранить максимум один pending feedback на served list с TTL;
- feedback ledger и model-evaluation storage отделены от current profile state;
- retention и public/privacy documents должны соответствовать фактическому
  flow до production rollout.

## 11. UX и accessibility

- feedback находится в естественном конце персональной surface;
- первый вопрос отвечает одной группой radio/buttons;
- score `0..10` доступен с клавиатуры и screen reader;
- reasons multi-select, но не обязательны;
- `Пропустить` всегда доступно;
- после отправки показывается короткое подтверждение без повторного modal;
- focus не теряется;
- mobile sticky navigation/CTA не перекрывает блок;
- no-JS выдача остаётся полной, feedback просто отсутствует;
- пользователь может открыть объяснение «Почему мы спрашиваем».

## 12. Сводный отчёт

В `personalization-test-report-template.md` добавляется секция:

```text
Selection quality feedback
- first_relevant_within_30_rate
- cards_to_first_relevant_p95
- quality score distribution
- internal pNPS
- reason-code breakdown
- trigger bias split
- persona/surface/model heatmap
- mismatch with observed actions/questionnaire
- failure-layer classification
- next calibration iteration
```

Каждый WARN/FAIL обязан содержать:

- affected personas and surfaces;
- exact model/profile/catalog revisions;
- dominant reason codes;
- failure layer;
- planned change;
- expected metric movement;
- guardrails and rollback condition.

## 13. Реализационный backlog

### P13N-QF0 — contract and fixtures

- JSON Schema;
- reason-code registry;
- trigger/sampling contract;
- fixture served lists and persona judgements;
- privacy/retention review.

### P13N-QF1 — UI preview

- end-of-list component;
- sampled-success trigger prototype;
- desktop/mobile/keyboard/screen-reader tests;
- no production write.

### P13N-QF2 — durable submission

- same-origin endpoint;
- idempotency/ACK/reconcile;
- bounded pending client state;
- private feedback/evaluation storage;
- direct/relay/both-down fault matrix.

### P13N-QF3 — evidence and reports

- join with served-list receipt;
- longitudinal E2E scenarios QF0–QF5;
- persona/model/surface scorecards;
- report integration and next-iteration generator.

### P13N-QF4 — focus cohort rollout

- higher sampling for authorized focus participants;
- bias monitoring;
- review of low-score cases;
- calibration iteration before wider rollout.

## 14. Definition of Done

- feedback относится к точной served list и model/profile/catalog revisions;
- primary first-relevant metric собирается вместе со score;
- end-of-list bias контролируется sampled-success trigger;
- one submission = one durable idempotent record;
- feedback не изменяет профиль автоматически;
- reason codes позволяют классифицировать failure layer;
- persona/surface/model report формируется автоматически;
- hard invariant violations = 0;
- no raw profile, token, email or unbounded text in evidence;
- focus cohort показывает, что вопрос понятен и не мешает поиску события;
- каждый low-score cluster приводит к явному калибровочному плану либо
  `CATALOG_SUPPLY/TEST_DATA` решению, а не к слепой правке weights.

## 15. Hard NO-GO

- называть score 0–10 единственной метрикой качества;
- смешивать pNPS с NPS всего продукта;
- показывать feedback на неперсонализированном fallback как оценку
  персонализации;
- использовать feedback как direct profile signal;
- сохранять feedback без served-list/model/profile/catalog provenance;
- смешивать end-of-list и success-sampled ответы без bias metadata;
- менять weights без failure-layer audit;
- блокировать CTA/навигацию вопросом;
- сохранять raw profile или полный clickstream ради одной оценки;
- объявлять низкий score ошибкой модели, если catalog не содержит eligible
  relevant events.

## 16. Краткий prompt для реализации

```text
Реализуй `personal_selection_quality_feedback_v1` строго по
`docs/features/static-site-pages/personalizaion/personal-selection-quality-feedback.md`.
Начни с P13N-QF0/QF1: schema, reason registry, served-list provenance и UI preview
без production writes. Главный вопрос — где встретилось первое действительно
интересное событие в пределах 30 карточек; score 0–10 и причины вторичны.
Не называй это обычным NPS, не используй feedback как прямой signal профиля,
не смешивай end-of-list и sampled-success triggers и не сохраняй оценку без
model/profile/catalog/surface versions. Обязательны idempotency, longitudinal
QF0–QF5, persona scorecards, failure-layer audit и next-iteration report.
```
