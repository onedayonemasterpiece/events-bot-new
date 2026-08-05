# Longitudinal E2E тестирование персонализации

> **Статус:** предварительный сценарный пакет для P13N-05/P13N-06 и более раннего shadow-контроля P13N-02.  
> **Дата среза:** 2026-08-03.  
> **Цель:** проверять не один удачный пример выдачи, а постепенную мутацию сервиса под пользователя: повторные визиты, изменение профиля, materialization, profile projection, трансформацию статических страниц, метрики качества и evidence для калибровки модели.

## 1. Что этот пакет закрывает

Нужен отдельный longitudinal-контур, потому что обычные unit/Playwright тесты
проверяют контракт страницы, но не отвечают на продуктовый вопрос:

> Становится ли сайт заметно лучше для разных пользователей после серии
> осмысленных взаимодействий?

Контур проверяет:

1. повторное использование сайта несколькими golden personas;
2. накопление session/short/mid/long evidence;
3. корректность materializer и периодического ETag projection refresh;
4. фактическое изменение выдачи на зарегистрированных surfaces;
5. отсутствие поломки календарной хронологии, exact hide, sensitive gates и
   static fallback;
6. метрики качества и объяснимые причины, почему модель требует калибровки.

Это не заменяет offline evaluator и A/B. Longitudinal E2E показывает, что
технический цикл работает и что модель движется в нужном направлении на
фиксированных персона-сценариях. Причинный production uplift доказывается позже
online experiment.

## 2. Главная продуктовая метрика

Верхняя метрика для персонализации:

```text
P(first relevant event within 30 cards) >= 0.95
```

Практическая форма gate:

```text
cards_to_first_relevant_p95 <= 30
```

для каждой критичной persona × surface × catalog snapshot, где relevance задана
ручными или утверждёнными judgement labels.

Правило:

- relevant event — judgement `relevance >= 2` по шкале `0..3`;
- событие должно быть eligible для surface;
- exact hidden и lifecycle-ineligible события не засчитываются;
- для `/dlya-menya/` метрика является primary;
- для тематических подборок она считается отдельно, потому что там
  personalization weak и не должна разрушать editorial meaning;
- для календарных primary lists метрика не применяется как reorder gate:
  календарь сохраняет дату/время, а персональная полка под ним измеряется как
  отдельная surface.

Если тест использует stochastic seeds/exploration, отчёт показывает не только
точечную долю, но и 95% confidence interval. Gate считается закрытым только если
нижняя граница доверительного интервала не ниже утверждённого порога либо если
детерминированный corpus полностью покрывает все обязательные persona/surface
cases.

## 3. Иерархия метрик

### 3.1. Product quality

| Метрика | Зачем нужна | Primary / guardrail |
|---|---|---|
| `cards_to_first_relevant_p50/p90/p95` | Насколько быстро пользователь встречает подходящее событие | primary |
| `first_relevant_within_30_rate` | Удобная доля для продукта | primary |
| `NDCG@10`, `NDCG@30` | Качество порядка с учётом позиции | model quality |
| `MRR@30` | Ранний релевантный hit | model quality |
| `relevant_coverage@30` | Не заужает ли модель кандидатов | guardrail |
| `serendipity@30` | Есть ли полезные неожиданные события | guardrail |
| `hide_rate_after_serving` | Модель не раздражает пользователя | guardrail |
| `save_or_cta_intent_rate` | Сильные продуктовые действия | diagnostic |
| `attendance_confirmed_rate` | Долгосрочная ценность, только trusted flow | future |

### 3.2. Model behavior

| Метрика | Что показывает |
|---|---|
| `delta_rank_after_like` | Влияет ли явный like на близкие темы |
| `delta_rank_after_hide` | Убирается ли exact event/family без жанрового overkill |
| `constraint_precision` | «дорого/далеко/не в это время» не ломают любовь к жанру |
| `short_horizon_gain` | Быстро ли session/short интерес меняет выдачу |
| `mid_horizon_stability` | Не исчезает ли устойчивый интерес после нескольких недель |
| `long_horizon_provenance_rate` | Long-term строится только из повторных сильных evidence |
| `false_expansion_rate` | Один слабый сигнал не раздувает лишние темы |
| `persona_dominance` | Persona не подавляет facet evidence |
| `unknown_mass_retained` | Профиль не делает ложный hard label |
| `exploration_rescue_rate` | Anti-bubble работает в пределах quota |

### 3.3. Hard invariants

Все ниже должны быть `0` нарушений:

- exact-hide resurrection;
- календарная primary chronology mutation;
- lifecycle/eligibility violation;
- visible/focused/acted card moved;
- server profile or weak signal before activation;
- sensitive-topic facet materialized;
- campaign/easter-egg implicit organic training;
- ordinary local rerank network request;
- per-action materializer/reprojection storm;
- cross-context profile leak;
- served DOM != served evidence.

## 4. Golden persona matrix v1

Минимальный набор не должен ограничиваться одной persona. Стартовая матрица:

| Persona key | Смысл | Главная проверка |
|---|---|---|
| `explorer_omnivore` | Любит новое и разные форматы | diversity/serendipity без хаоса |
| `evidence_planner` | Планирует заранее, доверяет фактам | стабильная выдача, объяснения, CTA/save |
| `family_weekend_curator` | Подбирает семейные выходные | age/family eligibility и price/distance constraints |
| `science_learning_local` | Научпоп, лекции, музеи, локальная идентичность | topic affinity + related knowledge events |
| `intensity_seeker` | Сильные впечатления, фестивали, unusual | novelty/intensity без опасного bubble |
| `restorative_exhibitions` | Выставки, прогулочный темп, спокойные форматы | exhibition lifecycle и low-intensity preference |
| `price_sensitive_free_seeker` | Бесплатное/недорогое | constraint не становится genre dislike |
| `music_jazz_theatre_mixed` | Смешанный вкус, например 55/45 | многомерный профиль, не один label |
| `tourist_short_window` | Турист на 2–4 дня | короткий горизонт и география важнее long history |
| `campaign_artifact_hunter` | Ищет пасхалки и открывает нерелевантное | campaign noise не обучает organic profile |
| `cold_start_unknown` | Нет данных | качественный fallback и unknown mass |
| `sensitive_interaction_control` | Взаимодействует с sensitive event | sensitive не материализуется в interest facet |

Каждая persona имеет:

- начальный профиль или `cold_start`;
- judgement labels по snapshot catalog;
- scripted browsing plan;
- allowed signals;
- expected profile timeline;
- expected rank deltas;
- anti-goals, которые не должны появиться.

## 5. Longitudinal сценарии

### L0. Cold-start baseline

**Цель:** доказать, что общий static/catalog baseline полезен до активации и не
создаёт server profile.

```gherkin
Scenario: Cold-start пользователь получает полезную выдачу без server profile
  Given новый browser context без activation
  When persona открывает главную, сегодня, подборку и страницу события
  Then server profile не создан
  And weak signals не отправлены
  And static fallback доступен
  And first_relevant_within_30 считается только как baseline
```

### L1. Short-horizon learning за одну сессию

**Цель:** показать, что явные действия дают немедленный local overlay и меняют
только разрешённые surfaces.

```gherkin
Scenario: Один визит формирует short profile без скачивания новой projection
  Given persona "science_learning_local" открывает static catalog snapshot
  When пользователь лайкает 2 научпоп-события и сохраняет 1 лекцию
  And открывает "/dlya-menya/" или personal tail после карточки события
  Then local overlay повышает близкие научпоп-кандидаты
  And календарный primary list сохраняет хронологию
  And ordinary local rerank выполнил 0 personalization network requests
  And materializer не запускался после каждого действия
```

### L2. Multi-session short → mid evolution

**Цель:** проверить, что профиль становится устойчивее после нескольких визитов,
но не застревает в пузыре.

```gherkin
Scenario: Повторные визиты за 14 дней формируют mid-horizon affinity
  Given persona "music_jazz_theatre_mixed" имеет 6 визитов в течение 14 дней
  When в каждом визите пользователь открывает 2-3 релевантные карточки,
       лайкает часть и один раз выбирает CTA
  And materializer публикует projection revisions по расписанию/threshold
  Then jazz и theatre facets растут в short и mid horizons
  And оба направления остаются представлены в top-30
  And exploration quota показывает adjacent events
  And cards_to_first_relevant_p95 <= 30
```

### L3. Constraint learning без ложного отрицания жанра

**Цель:** проверить семантику причин отказа.

```gherkin
Scenario: "Слишком дорого" меняет price constraint, а не любовь к теме
  Given persona "price_sensitive_free_seeker" лайкает джаз и лекции
  When пользователь скрывает платный jazz event с причиной "дорого"
  And проходит materialization
  Then price constraint усиливается
  And jazz facet не уменьшается только из-за этой причины
  And бесплатные jazz/lecture events не падают в выдаче
```

### L4. Long-horizon profile через 6+ месяцев evidence

**Цель:** доказать, что long-term affinity не возникает от одного действия и
формируется только повторными сильными evidence.

```gherkin
Scenario: Повторные подтверждённые посещения создают long-term affinity
  Given persona "science_learning_local" имеет trusted attendance events
        распределённые более чем на 6 месяцев
  When materializer строит новую profile revision
  Then long horizon получает science/lecture affinity
  And provenance содержит только trusted attendance или повторные strong actions
  And один share или один quick open не создаёт long-term facet
```

Для GitHub Actions время моделируется через deterministic virtual clock и
fixture timestamps. Нельзя ждать реальные месяцы; нужно проверять корректность
временного evidence и decay на замороженных данных.

### L5. Exact hide глобален, но не портит модель

```gherkin
Scenario: Exact hide исчезает со всех surfaces и не становится genre dislike
  Given persona видит event "rock-1" в тематической подборке
  When пользователь нажимает "Не интересует" и не отменяет в undo window
  Then event/family скрыт на подборке, календаре, search и "Для меня"
  And hidden collection содержит event и restore
  And related genre facets не уменьшаются без typed reason/repeated evidence
  And exact-hide resurrection count = 0
```

### L6. Campaign/easter-egg noise isolation

```gherkin
Scenario: Поиск артефактов не обучает organic interests
  Given active campaign context "artifact-hunt"
  When persona быстро открывает 15 карточек разных тем ради поиска артефактов
  Then implicit organic weights не растут
  And campaign diagnostic counters растут отдельно
  But явный like внутри campaign остаётся valid explicit signal
```

### L7. Cross-device/auth link

```gherkin
Scenario: Auth link объединяет только компактное current state
  Given anonymous browser A имеет likes/hides и projection revision 7
  And browser B авторизован тем же пользователем
  When пользователь проходит link/login flow
  Then server merge переносит explicit current state
  And raw browsing history не переносится
  And authenticated explicit state wins
  And оба devices получают новую compatible projection или refresh hint
```

### L8. Projection refresh и fallback

```gherkin
Scenario Outline: Profile projection обновляется периодически и fail-safe
  Given active profile projection revision 7 и next_refresh_at наступил
  When endpoint отвечает "<response>"
  Then local page сначала рендерит static/local rank без ожидания сети
  And refresh результат "<result>" применён только если совместим

  Examples:
    | response             | result                                  |
    | 304                  | revision 7 сохранена                    |
    | compatible_revision8 | revision 8 применена атомарно           |
    | invalid_schema       | revision 7 сохранена, diagnostic logged |
    | timeout              | revision 7 сохранена, backoff set       |
```

### L9. Factor ablation / вклад факторов

```gherkin
Scenario Outline: Вклад фактора измеряется counterfactual ablation
  Given одинаковые catalog, persona, projection, seed и surface
  When evaluator отключает фактор "<factor>"
  Then сохранён delta-rank report
  And изменение объяснимо ожидаемой причиной
  And hard invariants остаются 0

  Examples:
    | factor              |
    | session_overlay     |
    | short_horizon       |
    | mid_horizon         |
    | long_horizon        |
    | soft_persona        |
    | price_constraint    |
    | diversity_policy    |
    | exploration_quota   |
    | popularity_signal   |
```

## 6. GitHub Actions контур

Предлагаемый workflow:

```text
.github/workflows/personalization-longitudinal.yml
```

Triggers:

- `workflow_dispatch` с persona/surface/window/mode;
- nightly на frozen catalog snapshot;
- PR path filter для `site/src/lib/personalization/**`, model registry,
  surface policies и fixtures.

Jobs:

1. `build-static-site` — реальный Astro build/preview.
2. `prepare-fixtures` — catalog, event features, judgement labels, persona scripts.
3. `offline-model-eval` — ranks, NDCG/MRR/coverage/diversity/first relevant.
4. `longitudinal-browser-e2e` — Playwright sessions с virtual clock, storage,
   route interception и DOM evidence.
5. `materializer-loop` — staging/test API: action → accepted state → projection.
6. `factor-ablation` — counterfactual deltas по факторам.
7. `report` — HTML/Markdown/JSON summary + charts + artifacts.

Modes:

- `offline-only` — без browser, быстрый model gate;
- `browser-local` — local state/projection fixtures, no remote writes;
- `staging-sync` — реальный same-origin test backend, training_eligible=false;
- `fault-matrix` — backend/transport/projection failures;
- `full-nightly` — всё вместе, дольше и дороже.

## 7. Evidence artifacts

Каждый run сохраняет:

```text
artifacts/p13n-longitudinal/<run_id>/
  run-manifest.json
  catalog-snapshot.json
  event-feature-snapshot.json
  judgement-set.json
  persona-scripts.json
  surface-policy-snapshot.json
  model-registry-snapshot.json
  visit-timeline.ndjson
  action-ledger.ndjson
  served-lists.ndjson
  dom-order-diffs.ndjson
  storage-snapshots.ndjson
  projection-timeline.ndjson
  materializer-log.ndjson
  transport-redacted.ndjson
  factor-ablation.json
  metrics.json
  metrics-by-persona.csv
  screenshots/
  traces/
  report.md
  report.html
```

`run-manifest.json` содержит:

- repo SHA;
- build id;
- workflow id/run id;
- catalog/model/taxonomy/surface-policy versions;
- persona set version;
- judgement version;
- random seeds;
- browser/device matrix;
- flags/mode;
- backend route mode;
- redaction policy;
- cleanup result.

Запрещено сохранять:

- email/OTP/token/cookie/JWT;
- raw profile с идентификаторами;
- raw native hierarchy;
- full free-text history;
- service keys;
- нерезаные network headers.

## 8. DOM transformation evidence

Чтобы видеть, как статическая страница меняется под пользователя, каждый
browser scenario сохраняет:

1. `static_dom_order` — исходный server-rendered order;
2. `after_local_overlay_order` — порядок после local explicit overlay;
3. `after_projection_order` — порядок после compatible projection;
4. `frozen_prefix_ids` — карточки, которые нельзя было двигать;
5. `moved_visible_count` — должно быть `0`;
6. `moved_tail_ids` — что реально переставлено;
7. `event_rank_explanations` — короткие reason codes;
8. screenshot before/after только для зарегистрированных stable states;
9. ARIA snapshot для hidden/undo/recovery flows.

Для calendar primary ожидается `static_dom_order == personalized_dom_order` за
вычетом exact-hidden ids. Для thematic и `/dlya-menya/` ожидается объяснимый
rank delta.

## 9. Сводный отчёт по тестированию

Отчёт должен быть answer-first:

1. Verdict: `PASS / WARN / FAIL / BLOCKED`.
2. Главная метрика: `first_relevant_within_30_rate` и `cards_to_first_relevant_p95`.
3. Scorecard по personas.
4. Timeline изменения profile facets/horizons.
5. DOM transformation: что было static и что изменилось.
6. Factor ablation: какие факторы реально двигают выдачу.
7. Hard invariant ledger.
8. Storage/transport/materializer health.
9. Где модель хуже baseline и почему.
10. План ближайшей калибровки.

Пример выводов:

```text
Persona family_weekend_curator: WARN
- first_relevant_within_30_rate = 0.91, target >= 0.95
- price/distance constraint улучшил NDCG@10, но diversity упала
- next iteration: ослабить venue fatigue, добавить family-compatible exploration pool
```

## 10. Калибровочный цикл

Каждая итерация модели проходит один и тот же путь:

```text
hypothesis
  → offline evaluator
  → longitudinal golden-persona E2E
  → factor ablation
  → report
  → targeted model/feature fix
  → repeat
  → shadow/canary
  → A/B only after hard gates
```

Нельзя менять веса по одному красивому screenshot. Любое изменение должно иметь:

- affected personas;
- affected surfaces;
- expected metric movement;
- regression risk;
- before/after report;
- rollback condition.

## 11. Минимальный стартовый backlog

- `P13N-L0`: fixture format for persona scripts, judgements and visit timelines.
- `P13N-L1`: offline first-relevant/NDCG/MRR evaluator by persona × surface.
- `P13N-L2`: Playwright repeated-session harness with virtual clock.
- `P13N-L3`: DOM transformation evidence and frozen-prefix diff artifacts.
- `P13N-L4`: profile projection timeline and materializer shadow assertions.
- `P13N-L5`: factor ablation runner.
- `P13N-L6`: report generator with persona scorecards and next-iteration plan.
- `P13N-L7`: GitHub Actions workflow and artifact retention policy.

## 12. Definition of Done для longitudinal gate

Gate считается готовым, когда:

- минимум 10 golden personas имеют scripts и judgement labels;
- `/dlya-menya/`, thematic weak surface, related/search/popular и calendar exact-only
  покрыты отдельно;
- для каждого сценария есть deterministic replay;
- artifacts позволяют восстановить static order, personalized order, profile
  revision и причины ранжирования;
- primary metric считается по persona/surface и суммарно;
- factor ablation показывает вклад session/short/mid/long/persona/constraints;
- hard invariant violations = 0;
- report формирует понятный план следующей калибровки;
- workflow может запускаться вручную и nightly;
- test traffic помечен `training_eligible=false` и не загрязняет organic model.
