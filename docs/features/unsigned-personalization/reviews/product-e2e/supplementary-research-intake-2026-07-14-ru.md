# E2E-driven персонализация сайта анонсов событий

> **Status:** user-supplied supplementary research intake; not an eligible external consultant review, accepted architecture, KPI policy, release gate, or Phase B authorization.
>
> **Provenance:** supplied by the user on 2026-07-14. Provider, exact model/class, original prompt/transcript and raw provider/CLI capture were not supplied and could not be independently verified. The report says it did not inspect `feature/personalization-product-e2e-design` or the claimed task SHA `492497fe` and relied mainly on public `main` artifacts plus the task brief. Therefore it is **supplementary probe material** and satisfies neither required eligible-review slot. Recommendations remain proposals pending branch/source re-audit and accept/adapt/reject synthesis.
>
> **Integrity note:** the report body below is preserved as supplied. Project policy is stricter than some examples in it: catalog, lifecycle, supply, labels and holdout use only frozen real event records and evidenced states. Golden personas are controlled test actors without real PII; their UI actions exist only in an isolated E2E contour. Synthetic catalog worlds, invented events/dates/cancellations and artificial catalog activation are forbidden. Canonical adaptation: [Golden personas and real-data protocol](../../golden-personas-real-data-v0.md).


**Внешняя исследовательская консультация**
**Дата:** 14 июля 2026
**Репозиторий:** `onedayonemasterpiece/events-bot-new`
**Заявленная рабочая ветка:** `feature/personalization-product-e2e-design`
**Начальный SHA задачи:** `492497fe`
**Изменения в репозитории:** не выполнялись

## Область и ограничения аудита

Этот документ опирается на три группы фактов:

1. Загруженный task brief, включая описание целевого состояния и заявленный as-is.
2. Доступные публичные артефакты текущей ветки `main`: документацию `docs/features/unsigned-personalization`, перечисленные в ней Gherkin/Playwright/reference-client артефакты, документы по pgvector и generated probe.
3. Первичные исследования и официальную документацию платформ.

На момент консультации GitHub-страницы заявленной feature-ветки и точного SHA не удавалось получить через доступный read-only канал, а сетевой `git clone` из рабочей среды не проходил из-за DNS. Поэтому ниже **нет ложного утверждения о полном построчном code audit feature-ветки**. В разделе 3 применены уровни уверенности:

- **C1 — непосредственно подтверждено доступным публичным артефактом `main`;**
- **C2 — подтверждено проектной документацией и существованием названного файла, но не выполнением/прохождением кода в этой среде;**
- **C3 — заявлено task brief для feature-ветки, независимо не верифицировано.**

Это ограничение не меняет основную архитектурную рекомендацию, но перед принятием release gate потребуется повторить короткий branch-diff audit и исполняемый smoke на фактическом HEAD ветки.

## Уровни доказательности

- **E-A — established practice:** официальная документация платформ, устойчивый статистический метод либо вывод, поддержанный несколькими первичными работами.
- **E-B — strong empirical evidence:** первичная peer-reviewed/широко используемая работа с явно ограниченной переносимостью на этот домен.
- **E-C — project evidence:** фактический проектный артефакт, контракт, probe или заявленный дизайн.
- **E-D — consultant proposal:** проектное решение для этого продукта; должно пройти replay, golden evaluation и затем real-user calibration.

---

# 1. Executive conclusion

## 1.1. Итоговый вердикт

**E2E-driven подход для этой задачи правильный, но только как замкнутый продуктовый контур, а не как единственный тестовый слой.** Полный путь `UI action → accepted telemetry → dedupe → rollup → profile watermark → next-feed application → encounter outcome` действительно должен быть доказан браузером и БД. Однако массовую оценку ranker quality, Monte Carlo, confidence intervals, counterfactual replay и hyperparameter tuning нельзя переносить в Playwright: это сделает оценку медленной, хрупкой и статистически слабой.

Рекомендуемая система состоит из шести связанных слоёв:

1. детерминированные component/contract tests;
2. full-catalog offline ranker evaluation;
3. быстрый longitudinal simulator без браузера;
4. isolated DB integration для ingest/dedupe/rollup/application;
5. тонкий, но настоящий browser E2E для сквозного доказательства;
6. shadow/replay и затем controlled canary на реальных пользователях.

## 1.2. Решение по `cards_to_first_relevant`

Рекомендуется сохранить **20 карточек как основной product target**, а **30 карточек — как hard ceiling**, но отказаться от одиночного правила `cards_to_first_relevant <= 20`.

Нужна двухконтурная формулировка:

### Golden release matrix

Для заранее зарегистрированных обязательных сочетаний `persona × catalog_world × holdout × device`:

- **100% supply-eligible critical cells должны встретить релевантное событие не позже 30-й valid impression;**
- **не менее 90% — не позже 20-й;**
- ноль hard-constraint violations;
- ноль повторных показов явно скрытого события в пределах cooldown;
- все обязательные holdout-события должны быть достижимы candidate generation.

Это детерминированный regression gate, а не оценка населения.

### Population-like seeded simulation

Для фиксированного распределения personas, catalog worlds и seeds:

- `EncounterRate@20 >= 0.90`;
- `EncounterRate@30 >= 0.95`;
- односторонняя нижняя 95%-я граница Wilson должна превышать заранее заданный floor;
- `p90(cards_to_first_relevant) <= 20`;
- `p95(cards_to_first_relevant) <= 30`;
- публикуются macro-average, micro-average, worst persona и worst catalog world.

После калибровки на реальных данных целевой `EncounterRate@30` разумно поднять до 0.97, но делать это до появления наблюдаемого session-length distribution преждевременно.

## 1.3. Главная поправка к denominator

Условие «в candidate pool есть релевантное событие» допустимо **только для ranker-only диагностики**. Для основного E2E outcome denominator должен определяться так:

> В каноническом catalog snapshot после lifecycle-фильтра и жёстких ограничений пользователя существует хотя бы одно независимо размеченное релевантное активное событие.

Если релевантное событие есть в каталоге, но candidate generator его не вернул, это **product failure**, а не `not applicable`. Иначе candidate generation можно искусственно улучшать, удаляя сложные релевантные объекты из пула.

## 1.4. Что должно считаться доказанным результатом

Релиз персонализации можно считать продуктово подготовленным только когда одновременно доказаны:

| Область | Минимальное доказательство |
|---|---|
| Supply | независимая разметка показывает релевантное активное событие в каноническом snapshot |
| Collection | реальное UI-действие создаёт корректную, принятую и идемпотентную telemetry |
| Profile | rollup использует ожидаемый interaction watermark и формирует versioned snapshot |
| Application | следующая eligible выдача содержит `profile_snapshot_id/version/watermark`, соответствующий последнему завершённому rollup |
| Candidate generation | релевантный holdout не теряется до ranking |
| Ranking/presentation | событие реально получает valid impression в пределах 20/30 карточек |
| Exploration | часть полезных новых событий попадает в выдачу без нарушения primary SLO |
| Lifecycle | отменённые, завершившиеся и несовместимые по ограничениям события не засчитываются и не показываются |

**Telemetry-only pass, ranker-only pass и прямое заполнение localStorage готовым профилем не являются доказательством цели.**

---

# 2. Критическая оценка исходного E2E-driven подхода

## 2.1. Что в исходном подходе принципиально верно

### Продуктовый outcome вместо технического surrogate

В проекте уже заявлена оптимизация `time-to-interest`, а не максимального CTR. Это правильный выбор: CTR смешивает релевантность, позицию, UI salience, clickbait и доступность CTA. Для афиши важнее, увидел ли пользователь **действительно подходящее и доступное** событие достаточно рано.

### Longitudinal, а не одноразовая рекомендация

Локальный multi-horizon профиль, возвращающиеся короткие сессии и новые события требуют последовательной оценки. Односессионный `precision@K` не отвечает на вопросы:

- накопился ли профиль;
- затухают ли старые интересы;
- усваиваются ли отрицательные сигналы;
- не появляется ли feedback loop;
- применился ли профиль именно к следующей выдаче;
- обнаруживается ли новый holdout, отсутствовавший во время обучения профиля.

### Реальные UI-действия для сквозного доказательства

Требование не записывать golden profile напрямую правильно. Прямая запись обходит наиболее рискованные места: valid-impression semantics, event binding, idempotency, consent, localStorage migration, network delivery, rollup watermark и presentation.

### Virtual calendar плюс human-scale interaction delays

Разделение двух недель календарного времени и коротких человеческих задержек внутри сессии также правильно. Playwright Clock позволяет контролировать `Date`, timers и related browser time APIs, но серверный/catalog clock должен быть отдельным и явным.

## 2.2. Где исходный подход необходимо ужесточить

### Ошибка 1: один browser harness не должен быть evaluation platform

Playwright должен отвечать на вопрос «сквозной продуктовый механизм работает в настоящем браузере?», но не на вопрос «какой из 40 ranker variants статистически лучше на 10 000 episodes?». Вторая задача требует чистой функции, columnar outputs и быстрого replay.

### Ошибка 2: golden persona — не статистическое население

Десять personas полезны как regression panel, но не доказывают «практически гарантированно» для реальных пользователей. Golden panel нужен для:

- детерминированных hard cases;
- семантических инвариантов;
- lifecycle/failure coverage;
- защиты от regressions.

Population SLO требует большого числа seeded episodes, а реальная причинная ценность — controlled canary.

### Ошибка 3: неразличение encounter и action

Первичный тестовый outcome должен быть **valid exposure independently relevant event**, потому что action stochastic. Вторичный outcome — подтверждённая положительная реакция (`detail`, `save`, `ticket`, etc.). Иначе хороший ranker может «провалиться» из-за заданного случайного skip, а плохой — пройти из-за шумного клика.

### Ошибка 4: informative censoring

Пользователь, ушедший после 12 нерелевантных карточек, не является нейтрально right-censored. Его уход связан с качеством выдачи и fatigue. Для release SLO это failure; в survival analysis — competing event `abandon`, а не безопасное censoring.

### Ошибка 5: опасность circular ground truth

Нельзя выводить true relevance из тех же tags/embeddings/ranker score, которые тестируются. Нужны natural-language persona briefs, source-grounded event facts, независимые ordinal judgments, hard negatives и sealed holdouts.

### Ошибка 6: catalog supply и candidate supply нельзя смешивать

Основная цель продукта начинается с канонического Fly SQLite snapshot. Candidate pool — промежуточный output системы. Если основной denominator строится по candidate pool, один из ключевых failure modes исчезает из оценки.

## 2.3. Правильное определение E2E-driven

В этом проекте E2E-driven означает:

```text
product outcome
  ↓
metric decomposition
  ↓
served-list / profile / telemetry evidence
  ↓
layer-specific diagnosis
  ↓
minimal targeted change
  ↓
paired deterministic replay
  ↓
small browser sentinel
  ↓
shadow/canary validation
```

Это не означает «все расчёты выполняются браузером».

---

# 3. Audit фактического as-is

## 3.1. Сводная классификация

| Capability | Статус | Фактическое основание | Главный недоказанный участок | Уверенность |
|---|---|---|---|---|
| Static-first event pages, JS-independent first paint/CTA | **implemented / architectural invariant** | Проектная документация прямо требует персонализацию только после SEO/GEO HTML и fallback | Нужен executable regression на feature HEAD | C1/C2 |
| Fly SQLite как canonical event source | **implemented architecture** | Документация отделяет каталог Fly SQLite от Supabase telemetry/profile | Нужна проверка, что test snapshots воспроизводят Smart Update/lifecycle | C1/C2 |
| Local versioned profile after consent | **prototype / partial** | Описан localStorage schema; существуют reference module/demo | Не доказан production Astro integration и migration matrix | C2; C3 для ветки |
| Deterministic local reranking | **prototype implemented** | README перечисляет reference module/demo и MVP-0 related surface | Не доказан full feed и longitudinal behavior | C2 |
| Static related fallback | **implemented** | `event_detail_related` принят как первый surface, generated static preview/probe описан | Human relevance quality остаётся недоказанной | C1/C2 |
| pgvector `search_v3` / `related_v1` | **implemented infrastructure** | semantic-vector-retrieval документ заявляет two-document retrieval и RPC chain | Нужна branch/runtime validation и regression against full catalog | C1/C2 |
| LLM-verified related canaries | **implemented as reviewer stage** | Retrieval chain описывает Gemma verifier over retrieved IDs | LLM не должен быть acceptance oracle | C1/C2 |
| Golden probe | **implemented smoke** | Probe: 296 active future events, 40 anchors, 10 personas | Сам документ называет probe automated smoke, не human quality proof | C1 |
| Gherkin + Playwright contract | **implemented technical contract** | README указывает concrete feature/spec paths | Не longitudinal, не production browser→DB→rollup→next feed | C2 |
| Compact telemetry envelope | **partial / design hardened** | Поля layout/surface/served-list/profile задокументированы | Нужен authoritative schema, acceptance receipts и loss accounting | C1/C2 |
| Dedupe / served-list evidence | **partial** | `served_list_id/hash` и deduped summary описаны как must-fix | Нужен DB integration test для retries, out-of-order и hash conflicts | C1/C2 |
| Profile snapshots / rollup | **design / partial** | Server snapshots заданы как analytics/post-MVP evidence | Не доказан deterministic rollup и watermark application | C1/C3 |
| Browser → DB → rollup → next feed | **missing as product E2E** | Task brief прямо отмечает отсутствие единого доказанного процесса | Основной приоритет | C3 |
| Two-week virtual catalog | **missing** | В доступных артефактах нет longitudinal harness contract | Нужен versioned catalog world | C3 |
| Independent relevance judgments | **missing / design intent** | Документация требует human/golden acceptance, но probe heuristic | Нужен qrels-like corpus и adjudication | C1/C3 |
| Anti-bubble longitudinal metrics | **design-only** | Exploration/diversity guardrails описаны концептуально | Нет calibrated evaluation/acceptance evidence | C1/C3 |
| Mobile full longitudinal scenario | **missing** | Mobile signals/layout описаны, real usability unproven | Нужен primary browser scenario | C1/C3 |
| Desktop parity suite | **partial contract / missing parity evidence** | Desktop grid/list contract описан | Нужен compact semantic parity suite | C1/C3 |
| Shadow/replay with propensities | **missing** | Нет доступного evidence о randomized logging policy | Требуется для credible offline policy comparison | C3 |
| Controlled canary | **missing** | Проект находится до quality proof | Требуется после synthetic gates | C1/C3 |

## 3.2. Что проект уже сделал правильно

1. **Не ставит Supabase в массовый anonymous read path.** Это согласуется со static-first архитектурой и уменьшает зависимость первого экрана от backend.
2. **Разделяет event exclusions и user dislikes.** Это предотвращает семантическое загрязнение negative profile.
3. **Создаёт `served_list_id/hash` до действий.** Без этого невозможно восстановить exposure context.
4. **Оставляет LLM verifier reviewer-слоем.** В проектной документации acceptance требует deterministic assertions и human/golden evidence.
5. **Не объявляет generated probe human proof.** Это важная честная граница.
6. **Разделяет mobile feed и desktop grid/list.** Общий профиль не должен означать одинаковую presentation semantics.

## 3.3. Главный фактический разрыв

Существующие артефакты доказывают отдельные контракты, но не причинную цепочку:

```text
реальная карточка получила valid impression
→ UI action привязано к правильному event/served-list
→ payload принят ровно один раз
→ rollup включил action
→ snapshot стал current
→ следующая выдача применила snapshot
→ новый holdout вошёл в candidates
→ ranker поднял его
→ UI действительно показал его ≤ 20/30
```

Именно этот разрыв должен закрывать новый harness.

## 3.4. Required branch re-audit перед merge/release

После восстановления доступа к feature HEAD нужно автоматически собрать:

```text
branch_head_sha
merge_base_with_main
changed_files_since_492497fe
migration_files_and_checksums
playwright_specs_and_test_titles
telemetry_event_versions
profile_rollup_function/version
catalog_snapshot_builder/version
ranker/candidate algorithm_ids
CI results + artifact hashes
```

Любая таблица implemented/partial должна затем ссылаться не на README, а на конкретный файл, symbol, migration и passing test artifact.

---

# 4. Research synthesis с источниками

## 4.1. Offline ranking metrics: полный catalog важнее удобной выборки

**Established practice [E-A/E-B].** Rendle показывает, что sampled ranking metrics могут не сохранять даже относительный порядок алгоритмов по сравнению с exact metric. Поэтому release offline evaluation для небольшого событийного каталога должна работать на полном eligible catalog snapshot, а sampled negatives допустимы только для локальной разработки и должны быть явно маркированы.

**Применение к проекту [E-D].** Для каждого persona-day evaluator должен сначала сформировать полный canonical eligible set, затем отдельно сохранить candidate set и ranked set. Это позволяет различать supply, candidate recall и ranking.

## 4.2. Exposure/selection bias

**Established practice [E-A/E-B].** Логи рекомендаций содержат partial labels: пользователь может отреагировать только на то, что было показано. Schnabel et al. рассматривают рекомендации как treatment и применяют inverse propensity weighting; Li et al. показывают data-driven replay при randomized logging.

**Применение [E-D].** В real-user shadow/canary каждая exploration decision должна логировать `logging_policy_id` и propensity. Без propensity offline сравнение policy по кликам будет biased и не должно называться causal/off-policy evaluation.

## 4.3. Sequential and long-horizon recommendation

**Established practice [E-B].** Sequence-aware/session-based literature показывает, что порядок взаимодействий и session context являются отдельным объектом моделирования. RecSim формализует user latent state, item familiarity, response model и динамику состояния.

**Ограничение.** Симулятор неизбежно отражает предположения автора. Высокий synthetic score не доказывает real-user lift.

**Применение [E-D].** Harness должен разделять:

- stable latent preferences;
- short/mid/long profile state;
- session intent;
- fatigue/familiarity;
- stochastic observation/action policy;
- exogenous catalog/lifecycle process.

Ranker не должен читать latent oracle; oracle используется только evaluator.

## 4.4. Synthetic users

**Established practice [E-B].** RecSim и более новые платформы показывают пользу configurable simulation для sequential policies и failure discovery. Но synthetic agents особенно опасны, когда и simulator, и ranker используют одну ontology: система начинает оптимизироваться под собственный генератор.

**Применение [E-D].** Нужны два независимых слоя:

1. **semantic oracle** из human-authored persona/event judgments;
2. **behavior policy** вероятностно переводит скрытую relevance в observable actions.

LLM может помочь сформировать варианты поведения или проверить противоречия, но не должен одновременно генерировать labels, user actions и оценивать ranker.

## 4.5. Golden datasets и judgments

**Established practice [E-A].** TREC-подход использует test collections, pooled candidates и human judgments. Для проекта нужна qrels-подобная таблица, но с event-specific hard constraints и temporal validity.

**LLM limitation [E-B].** Исследования LLM-as-a-judge обнаруживают position bias и instability. Поэтому LLM может:

- предлагать кандидатов на review;
- находить потенциально конфликтные labels;
- составлять rationale draft;
- выполнять blinded secondary review.

Он не может быть единственным судьёй release holdout.

## 4.6. Novelty, diversity, serendipity

**Established practice [E-B].** Vargas & Castells подчёркивают, что novelty/diversity metrics должны учитывать rank и relevance; просто максимизировать расстояние между items недостаточно.

**Применение [E-D].** Для афиши полезна не «разнообразная лента сама по себе», а **useful discovery**: событие находится вне уже очевидного ядра интересов, но human judgment считает его relevant/adjacent и simulated user даёт положительную реакцию с достаточной вероятностью.

## 4.7. Browser testing and time

**Established practice [E-A].** Playwright предоставляет clean browser contexts и Clock API (`install`, `pauseAt`, `fastForward`, `runFor`). Это подходит для localStorage/session/cookie isolation и time-dependent browser behavior.

**Ограничение.** Browser clock не меняет автоматически Fly SQLite snapshot, DB timestamps, scheduled rollup или backend notion of now.

**Применение [E-D].** Нужны два clocks:

- `calendar_clock`: authoritative virtual date для catalog builder, lifecycle, backend и evaluator;
- `interaction_clock`: browser timers/dwell/read delays.

## 4.8. Statistical interpretation

**Established practice [E-A].** Time-to-event outcome естественно описывается cumulative incidence/survival curves; binary pass rates требуют confidence intervals, а сравнение rankers — paired design. Однако abandonment, вызванный плохой выдачей, является competing event, а не non-informative censoring.

**Применение [E-D].** Основные отчёты:

- empirical `F(k) = P(first relevant impression ≤ k)`;
- `F(20)`, `F(30)` с Wilson interval;
- p50/p90/p95 cards;
- cumulative incidence relevant encounter vs abandonment;
- paired delta candidate-baseline с cluster bootstrap по persona/catalog world.

## 4.9. Сводка established practice vs project proposal

| Вывод | Тип | Evidence |
|---|---|---|
| Не использовать sampled candidates как единственную offline оценку | Established | E-A/B |
| Логи текущей policy biased по exposure | Established | E-A/B |
| Sequential simulator полезен для controlled experiments, но не заменяет canary | Established | E-B |
| LLM не должен быть единственным relevance judge | Established + project-aligned | E-B/C |
| Novelty должна сохранять usefulness/relevance | Established | E-B |
| Два clocks вместо одного | Project proposal на базе Playwright | E-D |
| `F(20)≥0.90`, `F(30)≥0.95` | Project provisional target | E-D |
| Catalog-supply denominator для primary E2E | Project proposal, защищающий от gaming | E-D |

---

# 5. Рекомендуемая test architecture по слоям

## 5.1. Архитектурная пирамида

| Слой | Что проверяет | Инструмент | Масштаб | Cadence | Release role |
|---|---|---|---:|---|---|
| L0 Pure contracts | schema, scoring function, profile update, lifecycle predicates | unit/property tests | тысячи cases | каждый PR | hard gate |
| L1 Offline evaluator | full-catalog candidate/rank quality, labels, holdouts | Python/TS CLI, Parquet/JSONL | все events × personas | каждый ranker change | hard gate |
| L2 Longitudinal simulator | multi-day behavior/profile dynamics, Monte Carlo, exploration | process-level simulator, без DOM | 100–10 000 episodes | PR subset/nightly/full weekly | hard gate for tuning |
| L3 DB integration | ingest, dedupe, ordering, rollup, snapshot/watermark, RLS | isolated Postgres/Supabase schema | десятки/сотни scenarios | PR/nightly | hard gate |
| L4 Browser E2E | real UI, impression semantics, consent, storage, network, next-feed application | Playwright mobile primary; desktop parity | 10–50 critical paths | PR smoke/nightly matrix | hard gate |
| L5 Shadow/replay | behavior under real catalog/log distribution | production-like logs with propensities | continuous | pre-canary | evidence gate |
| L6 Controlled canary | real product outcome and guardrails | randomized experiment | production users | release | final decision |

## 5.2. Browser E2E: где он обязателен

Playwright должен покрыть то, что нельзя достоверно доказать pure-function тестом:

- consent gating;
- отсутствие training до consent;
- card geometry/visibility и `IntersectionObserver`-based valid impressions;
- быстрый scroll, остановка, dwell timer, navigation/back;
- mobile feed vs desktop grid/list position semantics;
- localStorage schema creation/migration/corruption/reset;
- `served_list_id/hash` до рендера и action binding;
- network request/response/receipt, retry и idempotency;
- fallback при timeout/blocked storage;
- запуск/ожидание rollup через test-control API;
- применение нового profile snapshot к следующей выдаче;
- lifecycle presentation и CTA usability;
- keyboard/back/scroll state на desktop;
- trace/screenshot/network artifacts при failure.

## 5.3. Что нельзя переносить в Playwright

Не выполнять браузером:

- full-catalog metric computation;
- 1 000+ stochastic episodes;
- confidence intervals/bootstrap;
- hyperparameter grid search;
- pairwise ranker replay;
- large-scale counterfactual IPS/DR estimators;
- exhaustive taxonomy/hard-negative checks;
- feature ablation;
- catalog stress tests;
- LLM batch judgment.

Браузер получает заранее рассчитанный scenario plan и после run отдаёт evidence. Metric engine работает отдельно.

## 5.4. Общий artifact contract

Каждый evaluation run должен быть самодостаточным:

```text
run_manifest.json
catalog_snapshots/*.json
persona_specs/*.yaml
judgments.parquet
scenario_plan.jsonl
served_lists.parquet
telemetry_sent.jsonl
telemetry_receipts.jsonl
profile_snapshots.jsonl
browser_traces/*.zip
metric_observations.parquet
summary.md
comparison.json
```

Минимальный `run_manifest.json`:

```json
{
  "run_id": "longitudinal-2026-07-14-001",
  "git_sha": "<feature-head>",
  "baseline_git_sha": "<accepted-head>",
  "initial_task_sha": "492497fe",
  "master_seed": 731904,
  "persona_set_version": "golden-personas-v1",
  "judgment_set_version": "event-qrels-v1",
  "catalog_world_version": "catalog-2w-v1",
  "telemetry_schema_version": "personalization-event-v2",
  "profile_rollup_version": "profile-rollup-v1",
  "candidate_algorithm_id": "<id>",
  "ranker_algorithm_id": "<id>",
  "browser_projects": ["mobile-chromium", "desktop-chromium"],
  "started_at_real": "2026-07-14T15:00:00Z",
  "virtual_start": "2026-08-03T08:00:00+02:00"
}
```

## 5.5. Cadence recommendation

- **PR:** pure contracts, offline golden subset, 2–3 simulator seeds/persona, 2 mobile sentinel paths, 1 desktop parity path, DB smoke.
- **Nightly:** full golden matrix, 50–100 seeds/persona, all failure/recovery scenarios, all supported viewport projects.
- **Weekly/pre-release:** sealed holdout, 500+ seeds/persona/catalog world, paired baseline comparison, complete browser critical matrix, shadow/replay report.

Повторный запуск failed seed не должен заменять его результат. Retry используется только для диагностики flakiness; release summary хранит first-attempt outcome и retry outcome отдельно.

---

# 6. Golden-persona panel

## 6.1. Design principles

Панель должна быть небольшой, но покрывать разные failure surfaces. Предлагается **10 core personas**. Они не являются демографическими стереотипами и не должны изображать «среднего реального пользователя». Это тестовые спецификации скрытой utility function, constraints и behavior policy.

Каждая persona имеет три независимых объекта:

1. `latent_preference_spec` — что объективно интересно и допустимо;
2. `behavior_policy_spec` — как скрытый интерес превращается в наблюдаемое действие;
3. `maturity_rule` — когда outcome начинает входить в mature-session SLO.

Ranker получает только telemetry-derived profile. Evaluator имеет доступ к latent spec и human judgments.

## 6.2. Persona schema

```yaml
persona_id: P01_classical_tchaikovsky
version: 1
home_city: Kaliningrad
latent_interests:
  primary:
    - concept: classical_music
      weight: 1.0
    - concept: tchaikovsky
      weight: 1.0
    - concept: symphonic_concert
      weight: 0.85
  adjacent:
    - concept: ballet
      weight: 0.55
    - concept: music_lecture
      weight: 0.35
negative_interests:
  - concept: loud_nightlife
    strength: 0.9
hard_constraints:
  cities: [Kaliningrad]
  local_time_windows:
    weekday: ["18:00-22:30"]
    weekend: ["11:00-22:30"]
  max_price_rub: 3500
unknown_zone:
  - chamber_music
  - opera_gala
behavior_policy_id: behavior-narrow-deliberate-v1
session_distribution_id: short-returning-v1
maturity_rule_id: mature-standard-v1
holdout_ids: ["H-P01-perfect-1", "H-P01-adjacent-1", "H-P01-hard-negative-1"]
```

## 6.3. Общие maturity rules

### `mature-standard-v1`

Persona становится mature в начале первой сессии, где одновременно выполнены:

- не менее 4 завершённых сессий;
- наблюдение не менее 3 разных virtual days;
- не менее 24 valid card impressions;
- не менее 6 qualified interactions;
- минимум 2 strong positive actions на 2 разных событиях;
- минимум 1 negative либо explicit preference boundary для personas, у которых она задана;
- последний rollup содержит watermark предыдущей завершённой сессии;
- next-feed request фактически использует этот snapshot.

`Qualified interaction`: detail dwell ≥ 8 s, like, save/calendar, ticket click, successful share/copy, explicit not interested. Один event не может дать больше двух maturity credits в одной сессии.

### `mature-rare-supply-v1`

Для P09 нельзя требовать несколько strong positives по редкой теме. Maturity:

- 5 сессий на ≥4 virtual days;
- ≥35 valid impressions;
- ≥4 qualified interactions по adjacent interests;
- ≥1 strong positive по rare interest **или** минимум 3 independent long-dwell/detail signals по теме;
- rollup/application requirements те же.

### `mature-drift-v1`

Для P08 maturity является interval-specific:

- phase A mature после standard rule;
- phase B evaluation начинается только после двух sessions с устойчивыми новыми signals и применения соответствующего rollup;
- одновременно проверяется controlled forgetting старого интереса.

## 6.4. P01 — узкий интерес: Чайковский / классическая музыка

| Поле | Спецификация |
|---|---|
| Latent interests | Чайковский 1.0; симфоническая классика 0.9; фортепианный/скрипичный концерт 0.8; камерная музыка 0.65 |
| Constraints | Калининград; будни после 18:00; max 3 500 ₽; cancelled/moved-outside-window недопустимы |
| Unknown/exploration | балет на музыку Чайковского; opera gala; лекция о композиторах; не знакомая камерная программа |
| Session distribution | 3–5 сессий/2 недели; `cards ~ truncated NegBin(mean=13, 5..28)`; inter-session gap 1–4 дня |
| Behavior policy | deliberate scan; stop probability высокая только при title/description evidence; detail dwell median 18 s |
| Action tendencies | relevant: detail .70, save .24, ticket .16; adjacent: detail .30, save .06; explicit negative на nightlife .16 после valid impression |
| Noise | 2% accidental detail; 1% accidental like с undo; 3% пропуск relevant из-за scan noise |
| Maturity | `mature-standard-v1` |
| Expected response | grade 3: strong; grade 2: moderate; adjacent: curiosity; irrelevant: fast skip; hard negative: hide |
| Holdouts | новый концерт с Чайковским; новый балет как adjacent; lexical hard negative «Выставка “Музыка Чайковского в плакате”» |

## 6.5. P02 — театр

| Поле | Спецификация |
|---|---|
| Latent interests | драматический театр 1.0; современная постановка 0.8; классическая пьеса 0.7; backstage экскурсия 0.45 |
| Constraints | вечер; длительность ≤3.5 h; цена ≤4 500 ₽; исключить детские утренники |
| Unknown/exploration | site-specific performance; театральная читка; опера как adjacent |
| Session distribution | 4–6 сессий; mean 16 cards; одна более длинная weekend сессия до 30 |
| Behavior policy | чаще открывает detail для состава/описания; ticket click только после detail dwell |
| Action tendencies | relevant detail .76, save .22, ticket .18; adjacent detail .35; explicit negative kids .12 |
| Noise | 4% curiosity click по известному актёру вне жанра; 2% save-then-unsave |
| Maturity | standard; минимум один positive на драму и один на современную постановку |
| Expected response | чувствителен к format/age/time, а не только category `theatre` |
| Holdouts | новая драма; экспериментальная читка; hard negative — детский спектакль с тем же театром/актёром |

## 6.6. P03 — выставки и современное искусство

| Поле | Спецификация |
|---|---|
| Latent interests | contemporary art 1.0; photography 0.8; installation/media art 0.8; museum exhibition 0.6 |
| Constraints | город/область; дневное или раннее вечернее время; цена ≤1 500 ₽ |
| Unknown/exploration | artist talk; design/architecture exhibition; performance art |
| Session distribution | 5–7 коротких сессий; mean 11 cards; высокий return rate при обновлении каталога |
| Behavior policy | чаще long dwell на карточке без detail; share/copy чуть выше остальных |
| Action tendencies | relevant stop .88, detail .52, save .18, share .07; adjacent detail .27 |
| Noise | 5% visual curiosity на нерелевантный красивый poster; 2% быстрый back после mismatch |
| Maturity | standard; long-dwell может дать один strong-positive credit только вместе с detail/share/save |
| Expected response | не путать любое `museum` с contemporary art |
| Holdouts | новая media-art выставка; архитектурная выставка adjacent; hard negative — классическая постоянная экспозиция без contemporary evidence |

## 6.7. P04 — семейные события

| Поле | Спецификация |
|---|---|
| Latent interests | family workshop 1.0; children theatre 0.85; science/interactive museum 0.8; outdoor family 0.7 |
| Constraints | ребёнок 7 лет; weekend/daytime; finish ≤19:00; ≤1 200 ₽ на человека; age fit обязателен |
| Unknown/exploration | family excursion; hands-on art; short classical concert for children |
| Session distribution | 3–5 sessions; mean 18 cards; чаще использует filters и calendar |
| Behavior policy | внимательно читает age, time, price; constraint mismatch вызывает explicit hide |
| Action tendencies | relevant detail .82, calendar/save .32, ticket .20; hard mismatch hide .34 |
| Noise | 3% click для проверки возраста; 1% accidental calendar с undo |
| Maturity | standard, включая минимум один negative constraint signal |
| Expected response | grade relevance обнуляется при age/time mismatch, даже если category совпадает |
| Holdouts | workshop 7+ на субботу; семейная экскурсия adjacent; hard negatives 12+/23:00/дорогой билет |

## 6.8. P05 — экскурсии и поездки

| Поле | Спецификация |
|---|---|
| Latent interests | local history walk 1.0; architecture excursion 0.85; regional day trip 0.8; nature trip 0.55 |
| Constraints | weekend; departure reachable from city; duration ≤10 h; price ≤5 000 ₽ |
| Unknown/exploration | industrial heritage; cemetery/history walk; boat trip |
| Session distribution | 3–4 sessions; mean 20 cards; rare search probability повышена до 5% |
| Behavior policy | detail reading для meeting point/route; copy/share для совместного планирования |
| Action tendencies | relevant detail .86, save .25, share .09, ticket .17; adjacent detail .38 |
| Noise | 6% curiosity on travel calendars; затем negative при отсутствии concrete occurrence |
| Maturity | standard |
| Expected response | template/on-demand без concrete date не считается доступным relevant event для outcome |
| Holdouts | новая архитектурная поездка; boat trip adjacent; hard negatives — другой регион/прошедшая дата/нет публичной записи |

## 6.9. P06 — смешанный профиль

| Поле | Спецификация |
|---|---|
| Latent interests | theatre .85; jazz .75; exhibitions .70; excursions .55; lectures .45 |
| Constraints | мягкие; цена ≤4 000 ₽; ближайшие 14 дней |
| Unknown/exploration | chamber music; design; food culture event |
| Session distribution | 6–8 sessions; mean 17 cards; session intent выбирается: `tonight`, `weekend`, `browse` |
| Behavior policy | высокая category switching; меньше explicit negatives; novelty seeking выше |
| Action tendencies | relevant detail .61, save .16; adjacent detail .32; share .04 |
| Noise | 7% curiosity actions; 4% contradictory feedback across days |
| Maturity | standard, positives минимум в двух категориях |
| Expected response | diversity полезна, но выдача не должна разбавлять top-20 до потери relevant encounter |
| Holdouts | по одному unseen event в 3 интересах; cross-category adjacent; repeated-venue fatigue case |

## 6.10. P07 — сильные отрицательные интересы

| Поле | Спецификация |
|---|---|
| Latent interests | jazz .8; contemporary theatre .7; exhibitions .6 |
| Negative interests | kids 1.0; stand-up .95; loud club/nightlife .9; mass festival .8 |
| Constraints | adults only; evening; city |
| Unknown/exploration | acoustic concert; lecture-performance |
| Session distribution | 5 sessions; mean 14 cards |
| Behavior policy | explicit not interested probability high on negative classes; undo 3% |
| Action tendencies | hard negative hide .55; recurrence hide .72; relevant detail .64 |
| Noise | 4% erroneous hide on adjacent followed by undo; 2% click on negative because title ambiguous |
| Maturity | standard, минимум 3 independent negative signals на ≥2 событиях |
| Expected response | negative-interest violations считаются guardrail failure даже при хорошем overall rank |
| Holdouts | positive jazz; adjacent acoustic show; hard negatives sharing venue/date/performer tokens |

## 6.11. P08 — меняющиеся интересы

| Поле | Спецификация |
|---|---|
| Phase A latent | classical music .9; museum .6 |
| Phase B latent | theatre .9; contemporary art .75; classical falls to .25 |
| Constraints | stable city/price/time |
| Unknown/exploration | ballet connects old/new interests |
| Session distribution | 8–10 sessions; explicit drift starts day 8; mean 15 cards |
| Behavior policy | phase A positives; phase B repeated skips old core + positives new core |
| Action tendencies | after drift old-classical detail drops .65→.10; new-theatre .15→.68 |
| Noise | one contradictory old-interest save after drift; prevents trivial hard switch |
| Maturity | `mature-drift-v1` |
| Expected response | system must adapt without erasing long-term signal instantly; old category concentration falls over 2–3 rollups |
| Holdouts | day-8 theatre; bridge ballet; old-classical decoy and genuinely exceptional old-interest event |

## 6.12. P09 — редкий relevant supply

| Поле | Спецификация |
|---|---|
| Latent interests | organ music 1.0; early music .9; niche historical instruments .8 |
| Constraints | city/region; next 30 days; price ≤3 000 ₽ |
| Unknown/exploration | sacred choral concert; music history lecture |
| Session distribution | 5–7 sessions; mean 22 cards; более высокий abandonment после повторов |
| Behavior policy | accepts adjacent discovery but сохраняет niche preference |
| Action tendencies | rare relevant detail .90/save .35/ticket .20; adjacent detail .40; generic concert .12 |
| Noise | 3% false positive on generic classical; 5% search after 15 unsuccessful cards |
| Maturity | `mature-rare-supply-v1` |
| Expected response | many sessions legitimately `NO_RELEVANT_CATALOG_SUPPLY`; это не ranker pass и не ranker failure |
| Holdouts | organ concert появляется day 11; choral adjacent day 8; hard negative — generic pop event in cathedral |

## 6.13. P10 — жёсткие city/time/price constraints

| Поле | Спецификация |
|---|---|
| Latent interests | broad culture: theatre, concert, exhibition, excursion |
| Constraints | только Калининград; только Fri 18:30–22:00 или Sat 11:00–18:00; total price ≤800 ₽; accessibility flag required |
| Unknown/exploration | любая новая category, если constraints соблюдены |
| Session distribution | 4–6 sessions; mean 19 cards; actively uses filters |
| Behavior policy | constraints dominate category; mismatch may still receive a detail check when data incomplete |
| Action tendencies | eligible relevant detail .70/save .26; explicit hard mismatch hide .45 |
| Noise | 5% uncertainty click when price/time absent; 2% erroneous filter toggle |
| Maturity | standard, минимум 2 constraint-confirming positives и 2 mismatch negatives |
| Expected response | filter correctness precedes semantic score; unknown constraint data cannot silently be assumed eligible |
| Holdouts | cheap accessible event in time window; category-novel eligible event; decoys wrong city/time/price/accessibility |

## 6.14. Panel acceptance

Панель считается представительной для engineering regression, если каждый из следующих axes покрыт минимум двумя personas:

- narrow vs broad interests;
- explicit negatives;
- hard constraints;
- rare supply;
- novelty-seeking;
- preference drift;
- family/audience fit;
- mobile short sessions;
- search as rare recovery;
- lifecycle-sensitive booking.

Нельзя удалять persona из release matrix из-за того, что она «редко проходит». Изменение panel требует version bump и отчёта old-vs-new, иначе возникает denominator gaming.

---

# 7. Ground-truth и holdout protocol

## 7.1. Независимая relevance model

Ground truth строится не из ranker score и не из production taxonomy alone. Источники:

1. immutable source-grounded event facts: title, description, organizers, venue, date/time, city, price, age, accessibility, booking state, lifecycle;
2. human-authored natural-language persona brief;
3. explicit constraints;
4. blinded judgments.

Taxonomy и embeddings могут показываться annotator только как навигационная помощь после первичного решения, но не как основание label.

## 7.2. Label rubric

| Grade | Смысл | Outcome eligibility |
|---:|---|---|
| 3 | strong relevant: событие явно соответствует core interest и constraints; разумно ожидать meaningful action | считается relevant |
| 2 | relevant: соответствует важному/adjacent interest и constraints; есть реальная польза | считается relevant |
| 1 | adjacent/curiosity: тематическая связь есть, но insufficient для primary outcome | novelty/serendipity, не primary relevant |
| 0 | irrelevant: нет достаточного соответствия | нет |
| -1 | hard negative / constraint violation / unavailable | guardrail failure при показе, не relevant |
| NA | невозможно решить по источнику | не используется без adjudication |

Отдельные поля:

```text
semantic_grade
constraint_status: pass | fail | unknown
availability_status: active | moved | cancelled | ended | incomplete
novelty_relation: familiar_core | adjacent_new | outside_profile_useful | outside_profile_not_useful
judgment_confidence: high | medium | low
```

Primary relevance:

```text
is_relevant = semantic_grade >= 2
              AND constraint_status = pass
              AND availability_status = active
```

## 7.3. Annotation process

1. Freeze persona brief и event source pack.
2. Pool events из:
   - current static ranker;
   - candidate new ranker;
   - pgvector retrieval;
   - lexical baseline;
   - random eligible long-tail;
   - popularity tail/head;
   - all designed holdouts/hard negatives.
3. Randomize order and remove algorithm identifiers.
4. Two independent human judgments.
5. Compute weighted/ordinal Krippendorff's alpha.
6. `alpha >= .80`: accept corpus; `.67–.80`: adjudicate disagreements and mark tentative; `<.67`: rubric/persona under-specified.
7. Third human adjudicator resolves:
   - any 0↔2/3 disagreement;
   - every hard-negative disagreement;
   - every release holdout;
   - low-confidence source facts.
8. LLM produces optional contradiction report, never final label.
9. Freeze qrels version and content hash.

## 7.4. Qrels-like schema

```sql
create table relevance_judgment (
  judgment_set_version text not null,
  persona_id text not null,
  event_id bigint not null,
  catalog_snapshot_id text not null,
  semantic_grade smallint not null check (semantic_grade between -1 and 3),
  constraint_status text not null,
  availability_status text not null,
  novelty_relation text not null,
  assessor_ids text[] not null,
  adjudicated boolean not null,
  confidence text not null,
  evidence_fact_ids text[] not null,
  rationale text not null,
  created_at timestamptz not null,
  primary key (judgment_set_version, persona_id, event_id, catalog_snapshot_id)
);
```

## 7.5. Hard negatives

Hard negative должен быть сложным для проверяемой системы, но однозначным для independent rubric. Типы:

- lexical overlap без semantic fit;
- same category, wrong audience/age;
- right theme, wrong city;
- right theme, outside available time window;
- right event, cancelled/ended;
- same venue/organizer, irrelevant format;
- high embedding similarity из-за shared boilerplate;
- «семейный» как слово в описании, но event 18+;
- «Чайковский» в выставке/лекции для persona, которой нужен concert, если human grade 1/0;
- free event с mandatory paid add-on выше budget;
- template/on-demand offer без active occurrence.

## 7.6. Holdout split

```text
Development judgments 60%
  visible to developers; used for debugging

Regression holdouts 20%
  visible labels, event IDs fixed; not used for parameter fitting

Sealed release holdouts 20%
  hidden labels and selected event IDs; opened only by CI/evaluator
```

Дополнительно минимум 30% release holdouts должны появиться **после maturity date**, чтобы исключить memorization event IDs и доказать generalization profile→new event.

## 7.7. Anti-circularity checks

Release evaluator должен падать, если:

- judgment rationale содержит ranker score/algorithm reason;
- label был автоматически скопирован из production tag;
- holdout event участвовал в profile-forming actions;
- persona behavior policy читает ranker score;
- ranker получает label/holdout marker;
- sealed set использовался в tuning run;
- event source pack изменился без judgment version bump.

---

# 8. Longitudinal session/behavior simulation

## 8.1. State model

```text
Exogenous state
  catalog snapshot, lifecycle, day/time, device, network

Latent user state
  stable interests, phase/drift, constraints, fatigue, familiarity, session intent

Observable product state
  local profile, DB profile snapshot, served lists, hidden/recent IDs

Behavior state
  current card, scan velocity, dwell, detail depth, action history, patience
```

Только observable state доступно product code. Simulator oracle знает latent state для генерации actions и оценки outcome.

## 8.2. Seed hierarchy

Один `master_seed` детерминированно порождает независимые sub-seeds:

```text
seed_catalog
seed_persona
seed_session_calendar
seed_session_length
seed_scan
seed_action
seed_noise
seed_network
seed_exploration_assignment
```

Рекомендуемый derivation:

```text
subseed = uint64(HMAC-SHA256(master_seed, namespace + entity_id)[0:8])
```

Нельзя использовать глобальный `Math.random()` без namespace. Добавление нового случайного draw в scan logic не должно менять network failure sequence.

## 8.3. Session occurrence distribution

Для каждой persona используется zero-inflated renewal process, но golden schedule фиксируется после генерации:

- inter-session gap: categorical `{0d: .08, 1d: .30, 2d: .28, 3d: .18, 4–6d: .14, 7+d: .02}`;
- session type: `{micro: .35, normal: .50, planning: .15}`;
- target valid impressions:
  - micro: truncated geometric, median 7, max 14;
  - normal: truncated negative-binomial, mean 16, max 30;
  - planning: mean 24, max 45;
- abandonment hazard grows after repeated irrelevant exposure and duplicate fatigue.

Golden E2E run uses a committed `scenario_plan.jsonl`; stochastic generator is run before browser execution, not during assertion logic.

## 8.4. Three-stage choice policy

### Stage A — scan / impression

Card becomes `valid_impression` only if all conditions hold:

```text
visible_area_ratio >= 0.50
AND continuously_visible_ms >= 800 mobile / 600 desktop
AND document.visibilityState = 'visible'
AND card is not occluded beyond threshold
AND served_list_id + position are stable
```

A fast scroll may fire intersection callbacks but remains invalid.

### Stage B — stop/read/detail

Probability depends on independent relevance grade, novelty, position, fatigue and presentation quality:

```text
logit(P(stop)) = b0
               + β_rel[grade]
               + β_novelty
               - β_position * log(1 + position)
               - β_fatigue * fatigue
               - β_repeat * exposure_count
               + ε_seeded
```

### Stage C — meaningful action

Action probabilities are conditional, not independent. Example transition:

```text
valid impression
  → stop
     → card dwell
        → detail open
           → detail dwell
              → save/calendar | ticket | share | back
     → like | not interested | continue
```

## 8.5. Default seeded probabilities

These are **initial engineering parameters [E-D]**, not real-user estimates.

| Transition | Grade 3 | Grade 2 | Grade 1 | Grade 0 | Hard negative |
|---|---:|---:|---:|---:|---:|
| valid impression → stop | .86 | .72 | .40 | .10 | .13 |
| stop → detail open | .68 | .51 | .24 | .05 | .08 |
| detail → long dwell | .78 | .61 | .34 | .12 | .10 |
| qualified view → like | .30 | .17 | .06 | .005 | .001 |
| qualified view → save/calendar | .24 | .13 | .04 | .003 | .001 |
| qualified view → ticket click | .17 | .08 | .015 | .001 | 0 |
| qualified view → successful share/copy | .045 | .025 | .012 | .002 | 0 |
| valid impression → not interested | .005 | .01 | .045 | .075 | .25 |

Modifiers:

- repeated exposure #2: stop ×0.72 unless previously positive;
- #3+: stop ×0.45, not-interested ×1.8;
- position 20+: stop ×0.85;
- fatigue >0.7: detail ×0.75, abandonment hazard ×1.6;
- novel adjacent item: stop +0.08 absolute for novelty-seeking personas;
- constraint uncertainty: detail +0.12, positive action blocked until resolved.

## 8.6. Human-like noise

Noise должен быть воспроизводимым и диагностируемым:

- accidental card tap: 1–3% valid impressions;
- accidental like/save: 0.5–1.5%, затем undo с probability .65;
- relevant skip despite exposure: 1–5%;
- curiosity detail on irrelevant: 3–8%;
- contradictory positive after negative: 1–4%;
- explicit negative on adjacent with undo: 1–3%;
- share API failure: 5%, with copy fallback success 80%;
- tab close/network interruption: 0–2% sessions in failure suite, not main quality matrix.

Каждое noise action помечается в simulator truth, но product telemetry не получает флаг `synthetic_noise` — иначе production path мог бы обходить его. Test run/tenant identifies synthetic data server-side.

## 8.7. Fatigue and repetition

```text
fatigue_0 = persona baseline
fatigue += 0.025 per valid impression
fatigue += 0.08 per consecutive grade-0 exposure
fatigue += 0.12 per repeated event after second exposure
fatigue -= 0.10 after relevant encounter
fatigue resets partially between sessions: fatigue *= exp(-gap_days / 2.5)
```

Session abandonment probability after each card:

```text
P(abandon) = sigmoid(-4.2 + 3.0*fatigue + 0.16*consecutive_irrelevant)
```

Это не production truth; параметры должны быть recalibrated. В release evaluation abandonment фиксируется как competing failure, а не скрывается.

## 8.8. Rare search

Search остаётся редким recovery behavior:

- base: 1% sessions;
- after ≥15 cards without grade≥2: +2%;
- P05 excursions: total up to 5%;
- P09 rare supply: up to 7%;
- search query создаётся из persona natural-language vocabulary, не из production tags;
- search result click может обновить профиль, но отдельный KPI должен показывать, сколько encounters достигнуто только после search.

Primary feed SLO публикуется как `organic_feed` и `any_surface`; иначе плохую ленту можно маскировать поиском.

## 8.9. Maturity and evaluation windows

Sessions до maturity используются для profile formation, но не входят в mature outcome SLO. При этом они входят в collection/reliability metrics.

Evaluation episode начинается с первой **eligible mature session**, где:

- canonical relevant supply известен;
- latest completed profile rollup применён;
- user не находится в explicit search-only intent;
- catalog snapshot/version зафиксирован.

Не допускается ретроспективно объявлять maturity после того, как ranker уже успешно показал holdout.

## 8.10. Paired baseline comparison

Candidate и baseline получают одинаковые:

- catalog snapshots;
- persona state transitions до branching point;
- session times/length budgets;
- latent utility draws;
- network schedule;
- exploration eligibility draws.

Action choice после различающихся slates естественно различается. Для сопоставимости можно использовать common random numbers per `(persona, day, event_id, action_stage)`.

---

# 9. Playwright human-behavior engine

## 9.1. Компоненты

```text
ScenarioCompiler
  → PersonaRuntime
  → CatalogTimeController
  → BrowserSessionDriver
       ├─ ConsentDriver
       ├─ FeedScanner
       ├─ DetailDriver
       ├─ ActionDriver
       ├─ NavigationDriver
       └─ FailureInjector
  → TelemetryObserver
  → DBProbe
  → RollupController
  → EvidenceCollector
```

`ScenarioCompiler` создаёт immutable plan. `BrowserSessionDriver` не принимает решений из текущего DOM ranker score; он исполняет policy на основе event ID → sealed truth lookup внутри test process.

## 9.2. Dual-clock design

### Calendar clock

Authoritative test-control contract:

```http
POST /__test__/clock
Authorization: Bearer <ephemeral-test-token>
Content-Type: application/json

{
  "run_id": "...",
  "now": "2026-08-11T17:30:00+02:00",
  "catalog_snapshot_id": "catalog-2w-v1-day-08"
}
```

Backend, catalog manifest resolver, lifecycle predicates и rollup получают virtual now по `run_id`. Production build не должен экспонировать endpoint.

### Interaction clock

Playwright Clock устанавливается **до загрузки application scripts**:

```ts
await page.clock.install({ time: new Date(session.virtualStart) });
await page.goto(session.url);

// Короткая реальная пауза только для layout/paint readiness.
await page.waitForFunction(() => document.fonts?.status === 'loaded');

// Логический dwell без секунд wall-clock ожидания.
await page.clock.runFor(1_200);
```

Не использовать `waitForTimeout(8_000)` для read delay. Реальные 20–120 ms допустимы только для browser rendering/animation settling, когда нет надёжного condition-based wait.

## 9.3. Valid impression driver

Production code должен эмитить диагностический state или event receipt, чтобы тест не дублировал скрытую логику несовместимым образом.

Рекомендуемый test hook только в test build:

```ts
type ImpressionDebug = {
  eventId: string;
  servedListId: string;
  position: number;
  visibleRatio: number;
  continuouslyVisibleMs: number;
  valid: boolean;
  invalidReason?: 'too_short' | 'occluded' | 'hidden_tab' | 'rerendered';
};
```

Test sequence:

```ts
await scanner.fastSwipePast(cardA);        // assert no valid impression
await scanner.stopOn(cardB, 1_100);        // assert exactly one valid impression
await scanner.partialExpose(cardC, 0.35);  // assert invalid
await scanner.stopOn(cardC, 900, 0.65);    // assert valid
```

## 9.4. Human interaction primitives

```ts
interface HumanDriver {
  scanNext(opts: { velocityPxPerSec: number }): Promise<void>;
  stopOn(eventId: string, logicalMs: number): Promise<void>;
  openDetail(eventId: string): Promise<void>;
  readDetail(logicalMs: number, depth: number): Promise<void>;
  backToFeed(): Promise<void>;
  like(eventId: string): Promise<void>;
  unlike(eventId: string): Promise<void>;
  notInterested(eventId: string): Promise<void>;
  undoNotInterested(eventId: string): Promise<void>;
  saveToCalendar(eventId: string): Promise<void>;
  clickTicket(eventId: string): Promise<void>;
  shareOrCopy(eventId: string): Promise<'shared'|'copied'|'failed'>;
  traverseRelated(fromId: string, toId: string): Promise<void>;
  search(query: string): Promise<void>;
}
```

Каждый primitive:

1. выполняет реальное пользовательское действие через locator;
2. ожидает observable UI state;
3. сверяет telemetry receipt;
4. записывает evidence timestamp;
5. не пишет localStorage/DB напрямую.

## 9.5. Session executor pseudocode

```ts
for (const step of session.plan) {
  switch (step.type) {
    case 'scan':
      await human.scanNext({ velocityPxPerSec: step.velocity });
      break;
    case 'dwell':
      await human.stopOn(step.eventId, step.logicalMs);
      await telemetry.expectValidImpression(step.eventId, step.expectValid);
      break;
    case 'detail':
      await human.openDetail(step.eventId);
      await human.readDetail(step.logicalMs, step.depth);
      break;
    case 'action':
      await actionDriver.execute(step.action, step.eventId);
      await telemetry.expectAcceptedAction(step.eventId, step.action);
      break;
    case 'end_session':
      await evidence.captureSessionSummary();
      await context.close();
      break;
  }
}
```

## 9.6. Storage persistence across days

Browser context isolation по умолчанию полезна, но одна persona должна переносить только разрешённое состояние:

```text
persona storageState/localStorage snapshot
  ↳ encrypted test artifact
  ↳ restored into next clean context for same persona/run
```

Нельзя переиспользовать один долгоживущий context на все две недели: это маскирует startup/migration bugs и создаёт cross-test coupling.

Procedure:

1. new context;
2. restore persona-specific storage state;
3. verify schema/version;
4. run session;
5. export state;
6. close context;
7. next virtual day uses a new context.

## 9.7. Network and DB assertions

На каждое eligible telemetry action проверяются три уровня:

```text
client intent
  request payload + event_id + idempotency_key
server receipt
  accepted | duplicate_known | rejected(reason)
durable state
  row/aggregate/profile watermark
```

`HTTP 200` без accepted receipt недостаточен. `sendBeacon` call без durable record недостаточен.

## 9.8. Mobile primary suite

Primary project: Chromium device emulation, narrow viewport, touch, reduced network profile. Полный scenario включает:

- consent;
- invalid fast-scroll impressions;
- profile-forming positives/negatives over several virtual days;
- close/reopen contexts;
- rollup;
- new holdout day;
- first relevant rank assertion;
- related traversal;
- lifecycle change;
- exploration item;
- final KPI evidence.

## 9.9. Desktop parity suite

Desktop не повторяет весь stochastic two-week run. Compact parity доказывает:

- те же semantic actions создают те же canonical signal names;
- `position`, `layout_mode=grid/list`, `viewport_class` корректны;
- filter/search context сохраняется;
- open-in-new-tab/detail dwell связывается с event;
- back preserves scroll/filter state;
- latest profile snapshot влияет на grid/list order;
- hidden item не возвращается;
- keyboard focus и CTA доступны.

## 9.10. Flakiness policy

Test считается flaky только после root-cause classification. Политика:

- никаких arbitrary sleeps;
- stable data attributes/event IDs;
- condition-based waits;
- first attempt сохраняется;
- retry не превращает first failure в green release metric;
- trace/video/network/clock state обязательны;
- stochastic plan materialized before run;
- browser run не генерирует новые random decisions при retry.

---

# 10. KPI dictionary и provisional targets

## 10.1. Outcome status taxonomy

Каждая mature evaluation session получает ровно один status:

| Status | Определение | Primary denominator |
|---|---|---|
| `SUCCESS_K20` | first independently relevant valid impression rank 1–20 | да, success@20/@30 |
| `SUCCESS_K30` | rank 21–30 | да, fail@20, success@30 |
| `LATE_SUCCESS` | rank >30 до session end | да, fail@20/@30 |
| `ABANDON_BEFORE_RELEVANT` | user policy завершила session до encounter | да, failure; competing event |
| `LIST_EXHAUSTED` | выдача закончилась, relevant catalog supply существовал | да, failure |
| `CANDIDATE_MISS` | relevant catalog supply не попал в candidate set | да, failure; diagnostic cause |
| `NO_RELEVANT_CATALOG_SUPPLY` | после lifecycle + hard constraints нет grade≥2 event | нет для rank SLO; да для supply coverage |
| `PROFILE_NOT_APPLIED` | eligible session использовала stale/no profile | reliability failure; outcome отдельно report, не смешивать |
| `ADMIN_CENSORED` | test infra/externally imposed horizon interrupted run, unrelated to rank quality | survival censoring; release run должен быть invalid |
| `INVALID_RUN` | schema mismatch, missing evidence, test failure | не outcome; блокирует release evidence |

## 10.2. Primary outcome 1 — EncounterRate@K

### Formula

Для eligible mature sessions `i`:

```text
Y_i(K) = 1[first_relevant_valid_impression_rank_i <= K]
EncounterRate@K = Σ_i Y_i(K) / N_eligible
```

`N_eligible` определяется по **canonical relevant supply**, не по candidate pool.

### Grain

`persona × catalog_snapshot/day × session × seed × device/presentation`.

### Segmentation

Обязательные срезы:

- persona;
- mobile/desktop;
- catalog world;
- supply density;
- mature age bucket;
- algorithm/profile version;
- exploration on/off assignment;
- hard-constraint complexity.

### Targets

| Context | @20 | @30 |
|---|---:|---:|
| Golden mandatory cells | ≥90%; each critical persona has no systematic miss | 100% |
| Seeded simulator provisional | point estimate ≥90%, lower one-sided 95% bound ≥85% | point estimate ≥95%, lower bound ≥92% |
| Post-calibration aspiration | lower bound ≥90% | lower bound ≥95–97% |

### Failure/no-supply

- abandonment/list exhaustion/candidate miss/late success: failure;
- no catalog supply: N/A for ranking, recorded in supply KPI;
- short list with relevant item shown: normal success at actual rank;
- short list without shown relevant but catalog supply exists: failure.

### Gaming risks

- condition on candidate pool;
- drop rare personas;
- count only long sessions;
- evaluate only catalogs with hand-placed holdouts;
- re-label clicked items as relevant;
- shrink eligible catalog via opaque filters;
- count card rendered rather than valid impression;
- ignore repeated cards in exposure count.

### Guardrails

CandidateRecall, hard-constraint violation, duplicate exposure, profile application and supply coverage are mandatory companion metrics.

## 10.3. Primary outcome 2 — distribution of cards to first relevant

For successful eligible sessions:

```text
C_i = count of valid card impressions from first actionable feed state
      through first independently relevant valid impression, inclusive
```

Repeated impressions of the same event **считаются снова**, потому что это реальная пользовательская цена. Одновременно публикуется distinct-card variant как diagnostic.

Отчёт:

- p50, p75, p90, p95;
- empirical CDF `F(k)` for k=1..30+;
- cumulative incidence of relevant encounter;
- cumulative incidence of abandonment.

Не публиковать p95 только среди successes: это удаляет failures. Для percentile table failures получают `∞`/`>horizon`; p90/p95 может быть undefined/above ceiling — это честный failure.

## 10.4. Secondary outcome — time_to_first_relevant

```text
time_to_first_relevant = timestamp(first relevant valid impression)
                         - timestamp(first feed actionable)
```

Отдельно можно хранить `page_load_to_first_relevant`, но нельзя смешивать ranking delay и static-site load latency.

Grain и denominator те же. Provisional diagnostic target для mobile:

- p90 ≤ 90 s simulated human time;
- p95 ≤ 150 s;
- при сохранении `EncounterRate@20/@30`.

Это не release primary до calibration реальными dwell/scroll distributions.

## 10.5. Supply metric — RelevantSupplyCoverage

```text
RelevantSupplyCoverage = eligible mature session opportunities
                         with ≥1 active grade≥2 event satisfying hard constraints
                         / all mature session opportunities
```

Это product/catalog health, не ranker quality.

Сегментировать по persona, city, horizon и supply density. Для P09 низкий показатель ожидаем и важен; его нельзя скрывать. Улучшить metric искусственно можно расширением relevance labels или игнорированием constraints, поэтому judgment version фиксирован.

## 10.6. Candidate generation diagnostic — CandidateRecall@Kc

```text
CandidateRecall@Kc = 1[at least one canonical relevant event appears in first Kc candidates]
```

Для multiple-relevant:

```text
RelevantItemRecall@Kc = |relevant_catalog ∩ candidate_top_Kc| / |relevant_catalog|
```

Targets:

- mandatory holdouts: 100% reachability;
- seeded episodes: session-level any-relevant recall ≥98% at candidate budget used by ranker;
- no regression >1 percentage point vs accepted baseline.

Нельзя рассчитывать только на events, уже retrieved текущим candidate generator.

## 10.7. Ranking diagnostics

### MRR

```text
MRR = mean_i(1 / rank_i_first_relevant)
```

Failures получают 0. Полезен для движения top ranks, но не заменяет @20/@30.

### Precision@20/30

```text
Precision@K = relevant valid-position items in top min(K, list_length)
              / min(K, list_length)
```

Для небольших lists denominator — actual available positions. Report coverage рядом, иначе короткие lists кажутся хорошими.

### Recall@20/30

```text
Recall@K = relevant items exposed in top K / all canonical relevant eligible items
```

Если product goal — один discovery, session-success важнее total recall; metric diagnostic.

## 10.8. Collection/reliability metrics

### DurableCollectionRate

```text
eligible_sent = telemetry intents that contract says must be persisted
success = accepted + known_idempotent_duplicate
DurableCollectionRate = success / eligible_sent
```

Target: ≥99.5% in controlled integration/E2E; unknown drop = 0.

Report reason distribution:

```text
accepted
known_duplicate
consent_blocked_expected
schema_rejected
rate_limited
network_failed
server_error
unknown_drop
```

Не считать `consent_blocked_expected` в eligible_sent; показывать отдельно.

### DuplicateIntegrity

```text
DuplicateIntegrity = retries that produce exactly one durable semantic event
                     / all retried semantic events
```

Target: 100% golden/integration.

### ProfileRollupLag

```text
profile_rollup_lag = snapshot.created_at - max(source_event.received_at in watermark)
```

Project-specific provisional target:

- local profile update: same interaction/next render;
- server rollup p95 ≤15 min in async mode;
- deterministic test-control rollup completes within configured test SLA.

Lag не должен вычисляться только для successful rollups; missing snapshot = failure/infinite.

### ProfileToNextFeedApplicationRate

```text
eligible_next_feed = feed requests after latest rollup became available
applied = request evidence references snapshot_id and watermark >= expected watermark
rate = applied / eligible_next_feed
```

Target: ≥99% seeded/integration; 100% mandatory golden cells.

## 10.9. Hard guardrails

### HardConstraintViolation@K

```text
violations among valid impressions in top K / all valid impressions in top K
```

- golden: 0;
- production canary: pre-defined near-zero ceiling, severity weighted;
- cancelled/ended event in top K is severity 1 critical.

### HiddenEventRecurrence

```text
hidden event valid impressions within cooldown / hidden-event opportunities
```

Target: 0 in golden; any recurrence must include explicit expiry/version rationale.

### ExposureFatigue

```text
RepeatExposureRate@K = repeated event valid impressions / all valid impressions
CategoryRunP95 = p95 maximum consecutive same-category run per list
VenueConcentration@K = max_venue_count / K
```

Provisional:

- no exact event >2 valid impressions in 14 days absent explicit re-engagement;
- no hidden event recurrence;
- category run ≤4 in first 20 for broad/mixed personas, unless eligible supply makes this impossible;
- always report supply-normalized expected concentration.

## 10.10. Anti-bubble guardrails

### UsefulNovelEncounterRate

```text
useful_novel = valid impression of event with
               novelty_relation in {adjacent_new, outside_profile_useful}
               AND semantic_grade >= 1
               AND no hard constraint failure
               AND positive simulated response OR grade >=2

UsefulNovelEncounterRate = sessions with ≥1 useful_novel in top 20
                           / eligible mature sessions
```

Target provisional:

- broad/mixed personas: ≥35%;
- narrow personas: ≥20%;
- rare-supply persona: ≥25% adjacent discovery;
- no degradation of EncounterRate@20 >2 pp.

### ExplorationSuccessRate

```text
successful explored items / explored valid impressions
```

Success: grade≥2 encounter or qualified positive action. Report by exploration reason.

Initial target: ≥20% broad; ≥10% narrow. Low success triggers exploration-policy diagnosis, not automatic removal of exploration.

### IntraListDiversity

Use pairwise distance over independent content representation with rank discount:

```text
ILD@K = weighted mean distance(item_i, item_j), i<j<=K
```

Не использовать как standalone gate; high ILD can be achieved by irrelevant items. Pair with relevance and calibration.

## 10.11. Decision KPI set — не десятки равноправных метрик

### Primary decision outcomes

1. `EncounterRate@20`.
2. `EncounterRate@30` hard ceiling.
3. `p90/p95 cards_to_first_relevant` plus competing-risk curve.

### Mandatory pipeline diagnostics

1. `RelevantSupplyCoverage`.
2. `CandidateRecall`.
3. `DurableCollectionRate` + reason accounting.
4. `ProfileToNextFeedApplicationRate` and rollup lag.

### Mandatory guardrails

1. hard-constraint/availability violations;
2. hidden/repeat fatigue;
3. useful novelty/exploration success;
4. worst-persona and worst-catalog regression.

MRR, precision, recall, CTR-like actions и ILD остаются диагностическими, а не равноправными release north stars.

## 10.12. Acceptance table for a ranker change

| Check | Accept | Reject |
|---|---|---|
| Golden @30 | 100% critical cells | любой miss |
| Seeded Encounter@20 | paired delta ≥0 and LCB above floor; либо non-inferior within -2 pp with guardrail gain | below floor or >2 pp loss |
| Seeded Encounter@30 | no regression, point ≥95% | any critical regression |
| Candidate recall | ≥98%, all mandatory holdouts | any mandatory holdout miss |
| Profile application | 100% golden, ≥99% simulated | stale/missing watermark |
| Hard constraints | 0 golden | any critical violation |
| Hidden recurrence | 0 | any recurrence in cooldown |
| Useful novelty | meets segment floor | collapse or primary harm >2 pp |
| Evidence completeness | all manifests/hashes/receipts present | missing evidence |


---

# 11. Anti-bubble / exploration strategy

## 11.1. Product principle

Для событийной афиши filter bubble проявляется не только как узость category. Возможны:

- повтор одного venue/organizer;
- повтор одного format/time slot;
- чрезмерная эксплуатация одного краткосрочного сигнала;
- подавление новых events без interaction history;
- невозможность проявиться изменившемуся интересу;
- скрытие adjacent supply из-за слишком жёстких negatives;
- feedback loop «показали → кликнули → показываем ещё больше того же».

Цель exploration — не максимизировать неожиданность, а создать **контролируемые возможности полезного открытия**.

## 11.2. Recommended slate composition

Для первых 20 карточек зрелой выдачи предлагается provisional composition:

```text
70–80% exploitation
  best independently eligible personalized candidates

10–15% calibrated adjacency
  adjacent categories/formats with positive semantic bridge

5–10% uncertainty reduction / freshness
  new events, under-exposed eligible items, preference probes
```

Это не жёсткий quota для каждого списка. Политика должна адаптироваться к supply и persona:

- narrow persona: exploration 8–12%;
- mixed persona: 15–20%;
- drift persona после новых signals: 15–20% targeted probes;
- hard constraints/rare supply: сначала eligibility, затем exploration.

## 11.3. Safe exploration eligibility

Event допускается в exploration slot, если:

1. lifecycle active;
2. все hard constraints pass;
3. independent relevance grade не известен production system, но offline safety rules не находят hard conflict;
4. event не hidden и не превышает fatigue cap;
5. event имеет rationale type:
   - `adjacent_semantic_bridge`;
   - `new_event_cold_start`;
   - `underexposed_quality_candidate`;
   - `preference_uncertainty_probe`;
   - `controlled_serendipity`;
6. propensity/logging evidence сохранены.

Нельзя исследовать заведомо отменённые, неподходящие по возрасту/городу/времени/цене события.

## 11.4. Exploration slots and rank protection

Чтобы exploration не разрушала основной outcome:

- позиции 1–5: максимум один exploration item и только с сильным safety score;
- позиции 6–20: 2–3 exploration opportunities;
- не ставить два high-uncertainty item подряд;
- при наличии единственного strong relevant candidate не вытеснять его из top-5;
- negative-interest candidate не получает exploration exemption;
- exact hidden item никогда не re-explore до cooldown expiry.

## 11.5. Diversification order

Применять ограничения в следующем порядке:

```text
hard eligibility
→ explicit hidden/negative suppression
→ candidate relevance/utility
→ event freshness and availability confidence
→ venue/organizer/category/format caps
→ calibrated adjacency / exploration
→ final stable tie-break
```

Если diversification выполняется до hard filters или независимо от relevance, она может повысить ILD ценой продукта.

## 11.6. Calibration, not equal-share balancing

Для mixed persona полезно сравнивать exposure distribution с target preference distribution:

```text
CalibrationError = distance(exposure_topic_distribution,
                            target_preference_distribution)
```

Но target не должен равняться historical exposure: иначе старый feedback loop замораживается. Предлагается:

```text
target = 0.70 * inferred_long_term_preferences
       + 0.20 * current_session_intent
       + 0.10 * exploration_prior
```

Weights — provisional и должны настраиваться paired replay.

## 11.7. Serendipity quality

Serendipity event должен одновременно быть:

- unexpected относительно obvious profile core;
- useful/relevant по independent judgment;
- eligible;
- не просто popular;
- не вызван тем, что persona brief уже содержит тот же exact facet.

Diagnostic:

```text
SerendipityQuality@20 =
  rank-discounted sum(unexpectedness * usefulness)
  / exploration opportunities
```

Не использовать его как самостоятельный release KPI из-за чувствительности к модели unexpectedness.

## 11.8. Explore/exploit evidence schema

```json
{
  "event_id": "7123",
  "position": 8,
  "selection_mode": "explore",
  "exploration_reason": "adjacent_semantic_bridge",
  "logging_policy_id": "epsilon-calibrated-v1",
  "propensity": 0.0831,
  "pre_exploration_rank": 37,
  "profile_snapshot_id": "ps_...",
  "constraint_gate": "pass",
  "fatigue_gate": "pass"
}
```

## 11.9. Anti-bubble acceptance table

| Scenario | Required outcome |
|---|---|
| P01 narrow classical | core relevant ≤20; хотя бы одна useful adjacent opportunity, но без nightlife violation |
| P06 mixed | first 20 не dominated одной category/venue; primary outcome не хуже baseline >2 pp |
| P07 strong negatives | exploration не обходит explicit negatives |
| P08 drift | новые interests получают controlled probes; old-interest share снижается постепенно |
| P09 rare supply | adjacent discovery сохраняется в no-core-supply days; новый rare holdout быстро поднимается |
| P10 hard constraints | diversity только внутри eligible set; никаких «разнообразных», но недоступных events |

## 11.10. Real-data calibration

После consented logging нужно калибровать:

- position bias;
- dwell distributions by surface/device;
- probability detail→save/ticket;
- repeated-exposure fatigue;
- session-length/abandonment hazard;
- inter-session gaps;
- negative/undo rates;
- exploration success by reason.

Сравнивать synthetic и real distributions через posterior predictive checks/quantile plots, transition matrices и segment-level errors. Параметры behavior simulator versioned; изменение параметров не смешивается с изменение ranker в одном comparison.

---

# 12. Двухнедельный catalog/time-travel design

## 12.1. Immutable daily snapshots

Каждый virtual day получает immutable catalog snapshot:

```text
catalog-2w-v1-day-00.json
catalog-2w-v1-day-01.json
...
catalog-2w-v1-day-14.json
```

Snapshot содержит не только список cards, но и canonical lifecycle evidence. Event IDs стабильны между days; изменение occurrence представлено новой version/revision, а не случайной сменой ID.

## 12.2. Catalog manifest schema

```json
{
  "catalog_world_id": "catalog-2w-v1",
  "snapshot_id": "catalog-2w-v1-day-08",
  "effective_at": "2026-08-11T00:00:00+02:00",
  "built_from_fly_sqlite_sha256": "...",
  "smart_update_version": "...",
  "event_count": 312,
  "active_event_ids": ["..."],
  "event_revision_hashes": {"7123": "sha256:..."},
  "previous_snapshot_id": "catalog-2w-v1-day-07",
  "change_set": {
    "new": ["H-P01-perfect-1"],
    "moved": [],
    "cancelled": [],
    "ended": []
  },
  "content_hash": "sha256:..."
}
```

## 12.3. Event revision schema

```json
{
  "event_id": "7123",
  "occurrence_id": "7123@2026-08-15T19:00+02:00",
  "revision": 3,
  "status": "scheduled",
  "starts_at": "2026-08-15T19:00:00+02:00",
  "ends_at": "2026-08-15T21:00:00+02:00",
  "city": "Калининград",
  "venue_id": "venue_44",
  "price_min_rub": 1200,
  "price_max_rub": 2200,
  "age_min": 12,
  "booking_state": "available",
  "source_fact_ids": ["fact_1", "fact_2"],
  "canonical_hash": "sha256:..."
}
```

Personalization может ранжировать это событие, но не изменять canonical fields.

## 12.4. Recommended 14-day storyline

| Virtual day | Catalog/lifecycle event | Persona behavior | What is tested |
|---:|---|---|---|
| D0 | baseline catalog; no holdouts | first visit, static useful before consent; then consent | static-first, consent gate, anon/session IDs |
| D1 | same catalog | generic browse, valid/invalid impressions | impression semantics, first signals |
| D2 | minor additions irrelevant/adjacent | no session for most personas | calendar gap, no phantom activity |
| D3 | new regular events | positive detail/save and one negative | accepted telemetry, local profile |
| D4 | one event price/time clarification | micro-session | stable IDs, revision handling |
| D5 | broader catalog refresh | stronger positive/ticket/share | served-list action binding |
| D6 | no catalog change | no session | decay uses time, not activity count only |
| D7 | maturity boundary | session, then explicit rollup | maturity, watermark, snapshot |
| D8 | perfect-match unseen holdouts appear | first mature evaluation session | profile generalization, candidate/rank/application |
| D9 | useful adjacent/serendipity holdouts | short session | exploration success/diversity |
| D10 | previously saved event moved | revisit | moved-time constraint, snapshot invalidation |
| D11 | one high-score event cancelled | session | lifecycle suppression, no stale CTA |
| D12 | old events ended; new cold-start supply | session | expiry, freshness, cold-start exploration |
| D13 | network/DB partial failure variant | micro-session | fallback, retry, dedupe, profile staleness |
| D14 | final catalog + sealed holdouts | final mature session | full KPI, drift/fatigue, ceiling @30 |

## 12.5. Catalog worlds

Минимум три независимых worlds:

1. `world_dev_visible`: labels и events видимы; быстрый debugging.
2. `world_regression`: фиксированные hard cases; tuning по ним запрещён policy, но результаты видимы.
3. `world_release_sealed`: новые event combinations/holdouts; labels открывает evaluator.

Дополнительные variants:

- sparse supply;
- high popularity concentration;
- many same-venue events;
- lifecycle churn;
- incomplete metadata;
- city/price/time constraint stress.

## 12.6. Virtual time consistency assertions

Для каждого day:

```text
browser Date.now == scenario virtual now
backend test clock == same instant
catalog snapshot effective_at <= now
no future snapshot served
rollup timestamps derived from virtual/calendar contract where appropriate
active/ended/cancelled predicates agree across UI, candidate generator and evaluator
```

Real timestamps для audit (`received_at_real`, run logs) хранятся отдельно от `occurred_at_virtual`.

## 12.7. Human delays vs calendar jumps

| Delay type | Mechanism | Typical range |
|---|---|---:|
| scroll gesture duration | real Playwright input + tiny rendering wait | 50–300 ms wall |
| valid impression dwell | Playwright Clock `runFor` | 0.8–4 s logical |
| card reading | Clock | 1–8 s logical |
| detail reading | Clock | 5–35 s logical |
| share/calendar interaction | real API/UI condition | as fast as condition permits |
| next session in 2 days | calendar controller + new context | no wall wait |
| rollup schedule | explicit test-control trigger or virtual scheduler | deterministic |

## 12.8. Lifecycle acceptance examples

### Moved event

- same stable event/occurrence identity where business contract says so;
- new revision hash;
- old served-list evidence remains historical;
- next feed uses updated time;
- if new time violates P10, event becomes ineligible even if profile score high.

### Cancelled event

- no new valid recommendation impression after cancellation snapshot;
- detail/static page may remain with cancellation state according to canonical lifecycle contract;
- ticket CTA disabled/updated;
- cannot satisfy relevance outcome.

### Ended event

- removed from active candidate set;
- profile interaction history may retain semantic signal;
- recent/hidden IDs cleanup follows retention policy, not immediate deletion of all evidence.

---

# 13. Failure, recovery, privacy и data-isolation scenarios

## 13.1. Failure matrix

| Scenario | Injection point | Expected product behavior | Required assertion |
|---|---|---|---|
| No consent | browser | static order/CTA useful; no personalization telemetry/profile training | zero eligible writes; no profile created except strictly necessary consent state |
| Consent granted mid-session | UI | only post-consent eligible signals train; prior static impressions not retroactively uploaded | watermark begins after consent timestamp |
| Consent revoked/reset | UI/storage/server | local profile removed/reset; future training stopped; server request follows retention contract | static fallback; no stale profile application |
| localStorage unavailable | browser API mock | static fallback, no crash | CTA and navigation usable; fallback reason logged only if consent allows |
| localStorage corrupt | injected value | reject incompatible schema; known-field migration or reset | no silent scoring of corrupt profile |
| Version mismatch | manifest/profile | static fallback or explicit migration | algorithm/profile version evidence |
| Telemetry timeout | network route | UI remains usable; retry/idempotency policy | no duplicate semantic event |
| 429 | ingest | bounded retry/backoff; no blocking first paint | receipt reason; durable outcome known |
| 500/503 | ingest/rollup | fallback; later recovery | current session not falsely marked applied |
| Duplicate request | transport | exactly one durable event | duplicate integrity 100% |
| Out-of-order events | DB | deterministic ordering by occurred/received/id; rollup stable | same snapshot across retry order |
| Client clock skew | browser | server bounds/records skew; no future poisoning | skew reason and normalized ordering |
| Stale catalog manifest | CDN/network | fail safe; snapshot/version visible | no cancelled event treated active |
| Rollup lag | scheduler | next feed marks stale snapshot; metric failure | no silent application claim |
| Rollup partial failure | DB | previous valid snapshot remains current; failed version not published | atomic publish semantics |
| Candidate RPC timeout | backend | static/local fallback | fallback algorithm_id and user-visible utility |
| Empty candidate list | generator | static eligible fallback or explicit empty state | no fabricated success |
| Share API denied | browser | copy fallback | success only if actual share/copy succeeded |
| Calendar blocked | browser | clear failure state, no false positive telemetry | action outcome field |
| Back navigation loses state | UI | restore scroll/filter/card context | no duplicate served-list/impression spam |
| Resize breakpoint change | UI | controlled rerender | served-list dedupe; positions consistent |
| Hidden event reappears | rank/UI | prohibited within cooldown | hard failure |
| Cancelled event cached | manifest | lifecycle state wins | hard failure if recommended as active |
| Persona cross-contamination | storage/DB | strict separation | no foreign event/profile IDs |

## 13.2. Telemetry loss accounting

Every client intent receives an auditable terminal state:

```text
NOT_ELIGIBLE_NO_CONSENT
NOT_ELIGIBLE_INVALID_IMPRESSION
ACCEPTED
DUPLICATE_KNOWN
REJECTED_SCHEMA
REJECTED_QUOTA
FAILED_NETWORK
FAILED_SERVER
UNKNOWN_TIMEOUT
```

`UNKNOWN_TIMEOUT` must be reconciled against DB by idempotency key. A run with unresolved unknowns is invalid evidence.

## 13.3. Data isolation model

Каждый run получает:

```text
test_tenant_id / schema namespace
run_id
persona-specific anon_id
persona-specific browser storage artifact
short-lived test-control token
catalog_world_id
TTL/cleanup marker
```

Prohibited:

- shared anon ID across personas;
- reuse production visitor IDs;
- running synthetic activity in production analytics without explicit test partition;
- hardcoded service role in browser;
- direct browser SELECT profile by anon ID if RLS design forbids it;
- manual DB cleanup that cannot be audited.

## 13.4. Privacy contract

Synthetic harness should store only test data. Production product should preserve project’s compact-data intent:

- no raw source texts in profile/telemetry;
- no secrets/tokens in localStorage;
- query text retention minimized/versioned;
- explicit reset control;
- consent version and timestamp;
- retention/TTL by table/entity;
- server snapshots treated as analytics/post-MVP evidence unless an authorized read contract exists;
- action payloads limited to necessary event/surface/context fields.

Правовой compliance требует отдельной jurisdiction-specific review; этот документ задаёт engineering privacy assertions, а не юридическое заключение.

## 13.5. Rollup determinism and recovery

Profile rollup должен быть:

- idempotent по `(anon_id, source_watermark, rollup_version)`;
- monotonic по published watermark;
- atomic при публикации current snapshot;
- reproducible from accepted telemetry;
- able to rebuild from source events;
- explicit about late/out-of-order events;
- versioned across taxonomy/feature changes.

Acceptance:

```text
same accepted event set + same versions + same virtual now
→ byte-equivalent semantic snapshot (excluding audit timestamps/IDs)
```

## 13.6. Failure suite segmentation

Не смешивать deliberate failure-injection episodes с quality SLO. Публиковать две панели:

- `quality_clean_room` — без инфраструктурных failures;
- `resilience_matrix` — заданные failures и recovery.

При этом real canary outcome включает фактические failures как часть пользовательского опыта.

---

# 14. E2E-driven tuning loop

## 14.1. Loop

```text
MEASURE
  ↓
CLASSIFY failure layer
  ↓
DIAGNOSE with evidence
  ↓
CHANGE one bounded component
  ↓
PAIRED REPLAY baseline vs candidate
  ↓
BROWSER SENTINEL
  ↓
ACCEPT / REJECT
  ↓
SHADOW / CANARY
```

## 14.2. Failure diagnosis order

Для каждой failed session проверять строго сверху вниз:

1. **Supply:** было ли independently relevant active event в canonical snapshot?
2. **Eligibility:** правильно ли применены city/time/price/age/lifecycle constraints?
3. **Candidate generation:** релевантный item вошёл в candidates?
4. **Profile formation:** нужные signals приняты и вошли в watermark?
5. **Profile application:** next feed сослался на ожидаемый snapshot?
6. **Ranking:** item потерян/понижен ranker score/diversification?
7. **Presentation:** фактическая позиция/viewport/hidden/occlusion?
8. **Impression:** карточка получила valid impression?
9. **Behavior:** user abandoned/skipped stochastically?
10. **Metric/label:** judgment/version/denominator корректны?

Это предотвращает бессмысленную настройку ranker weights, когда проблема в supply, stale profile или UI.

## 14.3. Evidence packet per failure

```json
{
  "episode_id": "...",
  "outcome": "CANDIDATE_MISS",
  "canonical_relevant_ids": ["7123"],
  "eligible_relevant_ids": ["7123"],
  "candidate_rank": null,
  "ranked_rank": null,
  "presented_rank": null,
  "profile_snapshot_expected": "ps_9",
  "profile_snapshot_applied": "ps_9",
  "source_watermark_expected": "evt_145",
  "source_watermark_applied": "evt_145",
  "catalog_snapshot_id": "day-08",
  "served_list_id": "sl_...",
  "root_cause_primary": "candidate_filter_city_alias",
  "root_cause_secondary": [],
  "evidence_paths": ["..."],
  "label_version": "event-qrels-v1"
}
```

## 14.4. Change discipline

Каждый tuning change объявляет:

- component and version;
- hypothesis;
- expected metric movement;
- potential harm/guardrail;
- affected personas/supply segments;
- whether it changes candidate eligibility, score, diversification, profile update or UI;
- pre-registered acceptance rule.

Пример:

```yaml
change_id: ranker-v17-negative-decay
hypothesis: explicit negative should suppress repeated same-facet items faster
expected:
  P07_negative_violation_at20: decrease
  EncounterRateAt20: non-inferior within -0.01
risk:
  P08_drift_recovery: slower
accept:
  golden_hard_negative_violations: 0
  paired_delta_encounter20_lcb: ">= -0.01"
  P08_new_interest_encounter20: no regression
```

## 14.5. Paired statistical comparison

Evaluation unit: `episode = persona × world × seed × mature_session × device`.

Report:

- paired difference `ΔEncounter@20/@30`;
- cluster bootstrap CI, clustering at least by persona and catalog world;
- paired distribution of first relevant rank;
- win/tie/loss matrix;
- worst-cell changes;
- guardrail deltas.

For proportions, Wilson intervals describe absolute SLO uncertainty; paired bootstrap/randomization tests describe change uncertainty.

## 14.6. Accept/reject logic

Change accepted only if:

1. evidence complete;
2. no golden hard failure;
3. primary target improves or is within pre-registered non-inferiority margin;
4. intended diagnostic improves in target segment;
5. no worst-persona regression beyond margin;
6. no hard constraint/privacy/reliability regression;
7. useful novelty does not collapse;
8. browser sentinel confirms product path;
9. result replicates on sealed world.

## 14.7. Tuning examples

### Failure: relevant event exists, candidate miss

Do not tune ranker weights. Inspect eligibility filters, vector document, RPC budget, city alias, lifecycle and candidate cap.

### Failure: candidate rank 4, presented rank 26

Inspect UI chunking, injected modules, sponsored/static slots, duplicates and device-specific ordering. Offline MRR can be excellent while product outcome fails.

### Failure: profile snapshot correct, rank poor after negative signals

Inspect feature weighting, decay and negative semantics. Compare P07/P08 to avoid over-suppression.

### Failure: synthetic passes, real canary fails

Likely behavior/supply calibration, UI friction or logging-policy mismatch. Do not “fix” simulator to mirror candidate result without independent data.

---

# 15. Приоритетный roadmap реализации

Roadmap задаёт порядок доказательств, не календарную оценку.

## Phase 0 — auditability foundation

**Deliverables**

- branch HEAD/diff inventory from `492497fe`;
- authoritative schema/version registry;
- `run_manifest` and artifact contract;
- stable `algorithm_id`, `profile_rollup_version`, `catalog_snapshot_id`;
- test tenant/run isolation;
- canonical outcome/status taxonomy.

**Exit gate**

Любой served list/action/profile snapshot можно связать с git SHA, catalog, algorithm, persona/run и source watermark.

## Phase 1 — independent golden data

**Deliverables**

- persona specs v1;
- source-grounded event packs;
- qrels schema;
- two-human + adjudication protocol;
- hard negatives;
- visible/regression/sealed holdouts;
- agreement report.

**Exit gate**

Labels независимы от ranker, release holdouts sealed, all hard-negative disputes adjudicated.

## Phase 2 — pure evaluator and KPI engine

**Deliverables**

- full-catalog eligibility/candidate/ranking exports;
- exact `Encounter@20/@30`, CDF, MRR/recall diagnostics;
- supply/candidate decomposition;
- confidence/paired comparison;
- anti-gaming checks.

**Exit gate**

Baseline evaluation воспроизводится byte-for-byte from immutable inputs.

## Phase 3 — DB ingest/rollup integration

**Deliverables**

- telemetry envelope v2;
- idempotency receipts;
- duplicate/drop reason accounting;
- deterministic rollup;
- atomic profile snapshot publication;
- profile application evidence;
- out-of-order/retry/rebuild tests.

**Exit gate**

UI-independent integration proves accepted events → expected snapshot and watermark.

## Phase 4 — fast longitudinal simulator

**Deliverables**

- seeded persona runtime;
- session schedule generator;
- fatigue/noise/drift;
- evolving catalog/lifecycle;
- exploration logging;
- 100+ episodes/persona nightly.

**Exit gate**

Paired baseline/candidate replay produces stable KPI and layer-specific failure packets.

## Phase 5 — mobile longitudinal Playwright

**Deliverables**

- dual clock;
- real UI primitives;
- valid/invalid impressions;
- persistent persona state across fresh contexts;
- test-control rollup;
- new holdout after maturity;
- trace/network/DB evidence.

**Exit gate**

At least one complete two-week scenario proves browser→DB→rollup→next feed→holdout encounter without profile injection.

## Phase 6 — desktop parity and resilience

**Deliverables**

- grid/list signal parity;
- filter/back/new-tab/keyboard checks;
- storage/network/DB/lifecycle failure matrix;
- cross-persona isolation.

**Exit gate**

No semantic drift mobile↔desktop and all critical recovery paths preserve static utility.

## Phase 7 — shadow/replay instrumentation

**Deliverables**

- logging policy/propensity fields;
- consented real distributions;
- simulator calibration notebook/report;
- replay estimator with overlap/variance diagnostics;
- supply monitoring.

**Exit gate**

Synthetic behavior parameters are calibrated/versioned; offline claims state support limitations.

## Phase 8 — controlled canary

**Deliverables**

- randomized assignment;
- primary real-user outcome proxy and guardrails;
- sample-size/decision rule;
- rollback/fallback;
- segment/worst-case report.

**Exit gate**

Real-user evidence confirms non-inferiority or lift without constraint/privacy/reliability harm.

## 15.1. First implementation slice

Самый полезный vertical slice:

```text
P01 + P07 + P10
× mobile
× D0/D3/D7/D8/D11
× one perfect holdout + one hard negative + one cancellation
× real UI actions
× isolated DB ingest/rollup
× next feed application
× Encounter@20/@30 report
```

Он покрывает positive learning, negatives, constraints, lifecycle и end-to-end application без преждевременного создания огромного simulator.

---

# 16. Что категорически не стоит делать

1. **Не реализовывать всю evaluation mathematics в Playwright.** Это уничтожит скорость, traceability и statistical scale.
2. **Не считать candidate-pool eligibility основным denominator.** Так candidate misses превращаются в N/A.
3. **Не записывать golden profile напрямую.** Допустим только отдельный unit test profile serializer; product E2E формирует профиль действиями.
4. **Не использовать production ranker features/score как ground truth.** Это circular validation.
5. **Не делать LLM единственным judge.** Position/order bias и shared-model bias делают это ненадёжным.
6. **Не считать CTR primary outcome.** CTR не доказывает доступность/релевантность и легко оптимизируется presentation tricks.
7. **Не использовать mean rank без percentiles/worst cells.** Среднее скрывает хвост, ради которого сформулировано «практически гарантированно».
8. **Не объявлять user abandonment neutral censoring.** Оно часто вызвано poor recommendations.
9. **Не удалять no-supply/rare persona из отчёта.** Supply coverage — отдельная продуктовая проблема.
10. **Не использовать один и тот же holdout для постоянного tuning.** Нужны sealed worlds и versioning.
11. **Не генерировать stochastic actions внутри retry.** Retry должен повторять тот же plan.
12. **Не использовать arbitrary wall sleeps.** Logical dwell контролируется clock, UI readiness — conditions.
13. **Не переиспользовать один browser context для всех personas/days.** Это маскирует isolation/startup bugs.
14. **Не считать `HTTP 200` доказательством collection.** Нужны receipts и durable reconciliation.
15. **Не считать profile existence доказательством application.** Next feed обязан ссылаться на expected snapshot/watermark.
16. **Не смешивать canonical event mutation с personalization.** Smart Update/lifecycle/facts остаются отдельным authority.
17. **Не диверсифицировать любой ценой.** Irrelevant diversity — не полезная discovery.
18. **Не разрешать exploration обходить hard constraints и hides.**
19. **Не усреднять mobile feed и desktop grid.** Presentation semantics различны.
20. **Не принимать green after retry как чистый pass.** First-attempt result и flakiness должны быть видны.
21. **Не оптимизировать simulator до идеального результата без real calibration.** Это лишь overfitting к synthetic world.
22. **Не называть replay causal, если logging propensities неизвестны или overlap отсутствует.**

---

# 17. Открытые продуктовые вопросы

1. **Что означает «действительно интересное» в production?** Valid relevant impression, detail dwell, save, ticket click или composite? Для test oracle рекомендовано exposure; для real outcome нужен заранее выбранный observable proxy.
2. **Каков реальный card budget короткой mobile session?** Target 20 имеет смысл только относительно наблюдаемого distribution и layout density.
3. **Какой horizon считать «доступным событием»?** 7/14/30 дней, и меняется ли он по session intent?
4. **Какие constraints hard, а какие soft?** Цена, city radius, time, age, accessibility и sold-out state требуют explicit semantics.
5. **Считать ли unknown price/time/accessibility eligible?** Предлагается не считать hard-pass без разрешённой fallback policy.
6. **Как обрабатывать multi-occurrence event?** Relevance может быть общей, eligibility — occurrence-specific.
7. **Какова допустимая задержка server rollup?** Если MVP local-first, что именно должен доказывать server snapshot и когда он влияет на feed?
8. **Является ли next-feed полностью browser-local или включает server candidate/ranking?** Application evidence зависит от выбранного authority.
9. **Какую долю карточек пользователь видит на экран?** `20 cards` нужно интерпретировать вместе с pixels/time, особенно desktop grid.
10. **Какие signals считаются strong positives?** Ticket click часто intent-rich, но может быть проверкой цены/наличия.
11. **Какая политика decay для explicit negatives и undo?** Persistent dislike и transient session mismatch нельзя смешивать.
12. **Какой cooldown повторного event/category/venue?** Нужны product rules до metric targets.
13. **Можно ли персонализировать static related block на event detail без изменения canonical order evidence?** Нужно сохранять fallback and reranked list separately.
14. **Каков бизнес-приоритет rare-supply users?** Низкий supply может требовать catalog acquisition, alerts/search, а не ranker tuning.
15. **Какие реальные consented logs доступны для calibration?** Без этого behavior probabilities остаются engineering hypotheses.
16. **Есть ли возможность randomized exploration logging?** Без неё off-policy evaluation ограничено.
17. **Что является rollback unit?** Algorithm ID, manifest, profile schema, rollup, client bundle должны откатываться согласованно.
18. **Какие production SLO на static-first performance?** Personalization не должна ухудшить first paint/CTA; нужен отдельный performance guardrail.
19. **Нужно ли переносить профиль между устройствами?** Текущий anonymous local-first дизайн подразумевает browser-local identity; это влияет на longitudinal interpretation.
20. **Какой режим для users without mature profile?** Static/popularity/contextual baseline должен иметь собственный quality gate.

---

# 18. Вопросы второму консультанту для взаимной критики

1. Согласны ли вы, что primary denominator должен начинаться с canonical catalog supply, а не candidate pool? В каких случаях это приведёт к unfair blame ranker team?
2. Достаточны ли `F(20)≥.90` и `F(30)≥.95`, или для заявленного «практически гарантированно» нужен более высокий p30 SLO уже на первом релизе?
3. Следует ли abandonment считать безусловным failure либо competing risk; какие cases допустимо administratively censor?
4. Не слишком ли низок/высок maturity threshold `4 sessions / 3 days / 24 impressions / 6 interactions` для этой афиши?
5. Как лучше моделировать action noise, не создавая simulator, который просто подтверждает выбранные веса сигналов?
6. Достаточен ли panel из 10 personas для engineering regression, и какой axis отсутствует?
7. Какой independent labeling protocol вы предложили бы при ограниченном human-review budget?
8. Где провести границу LLM reviewer: candidate pooling, rationale, disagreement detection, secondary blind vote?
9. Следует ли primary outcome считать по independently relevant impression или по user-confirmed action? Какие biases сильнее в каждом варианте?
10. Какой slate exploration design безопаснее: fixed slots, contextual bandit, calibrated re-ranking или constrained optimization?
11. Какие anti-bubble metrics действительно actionable для небольшого регионального каталога, а какие создадут metric theater?
12. Как доказать profile application в local-first архитектуре, если server snapshots пока analytics-only?
13. Нужен ли полный two-week browser run хотя бы для одной persona, или лучше browser session sentinels плюс process-level orchestration?
14. Какой minimum randomized traffic/propensity logging необходим, чтобы shadow replay был полезен?
15. Как защититься от repeated tuning на sealed holdout при частых релизах?
16. Какие failures должны блокировать release независимо от EncounterRate lift?
17. Как оценивать rare-supply persona: ranker SLO, supply SLO, search/alert success или composite?
18. Насколько разумен non-inferiority margin -2 percentage points @20 при улучшении useful novelty?
19. Какие project-specific assumptions в этом отчёте наиболее вероятно неверны и требуют первого empirical check?
20. Какой минимальный canary outcome вы считаете достаточно близким к «действительно интересному событию», не ожидая покупки билета?


---

# Appendix A. Concrete schemas

## A.1. Telemetry event envelope v2

```json
{
  "event_uuid": "0b4c8a19-...",
  "idempotency_key": "sha256:anon|session|event|action|served-list|ordinal",
  "schema_version": "personalization-event-v2",
  "anon_id": "uuid",
  "session_id": "uuid",
  "test_run_id": "uuid-or-null",
  "occurred_at": "2026-08-11T17:31:12.300+02:00",
  "received_at": "server-owned",
  "consent_version": "personalization-consent-v1",
  "event_name": "event_detail_view",
  "event_ref": {
    "event_id": "7123",
    "occurrence_id": "7123@2026-08-15T19:00+02:00",
    "event_revision": 3
  },
  "presentation": {
    "surface": "home_feed",
    "viewport_class": "mobile",
    "layout_mode": "feed",
    "position": 7,
    "page_cursor": "catalog-2w-v1-day-08:chunk-0",
    "served_list_id": "sl_...",
    "served_list_hash": "sha256:...",
    "catalog_snapshot_id": "catalog-2w-v1-day-08",
    "algorithm_id": "local-rerank-v4",
    "candidate_algorithm_id": "static-candidates-v3",
    "profile_snapshot_id": "ps_..."
  },
  "interaction": {
    "valid_impression": true,
    "visible_ratio": 0.72,
    "visible_ms": 1150,
    "dwell_ms": 12400,
    "action_outcome": "success",
    "related_from_event_id": null,
    "search_context_id": null
  },
  "client": {
    "bundle_version": "...",
    "profile_version": "anon-profile-v2",
    "taxonomy_version": "event-taxonomy-v1",
    "timezone": "Europe/Kaliningrad"
  }
}
```

Rules:

- `event_uuid` и `idempotency_key` создаются до send/retry;
- server owns `received_at`, acceptance code и normalized fields;
- `position` относится к фактически представленному surface, а не offline candidate rank;
- action without `served_list_id` разрешается только для явно перечисленных non-list surfaces;
- payload size bounded; no raw event description or secrets;
- every schema change increments version and migration/compatibility test.

## A.2. Ingest receipt

```json
{
  "event_uuid": "...",
  "idempotency_key": "...",
  "status": "accepted",
  "reason": "new_event",
  "server_event_id": "evt_145",
  "received_at": "2026-08-11T15:31:12.410Z",
  "normalized_occurred_at": "2026-08-11T15:31:12.300Z",
  "duplicate_of": null,
  "quota_bucket": "anon-day",
  "schema_version": "personalization-event-v2"
}
```

Allowed statuses:

```text
accepted
known_duplicate
rejected_no_consent
rejected_schema
rejected_quota
rejected_invalid_reference
failed_retryable
```

## A.3. Served-list evidence

```sql
create table personalization_served_list (
  served_list_id uuid primary key,
  served_list_hash text not null,
  anon_id uuid not null,
  session_id uuid not null,
  test_run_id uuid,
  requested_at timestamptz not null,
  catalog_snapshot_id text not null,
  surface text not null,
  viewport_class text not null,
  layout_mode text not null,
  hard_filter_hash text not null,
  profile_snapshot_id uuid,
  profile_source_watermark text,
  candidate_algorithm_id text not null,
  ranker_algorithm_id text not null,
  candidate_event_ids bigint[] not null,
  ranked_event_ids bigint[] not null,
  presented_event_ids bigint[] not null,
  exploration_modes jsonb not null,
  fallback_reason text,
  evidence_version text not null,
  unique (anon_id, session_id, served_list_hash)
);
```

`served_list_hash` должен включать ordered presented IDs, catalog snapshot, surface/layout и algorithm/profile versions. Он не должен включать resize noise.

## A.4. Profile snapshot

```json
{
  "profile_snapshot_id": "ps_...",
  "anon_id": "uuid",
  "rollup_version": "profile-rollup-v1",
  "profile_schema_version": "anon-profile-v2",
  "taxonomy_version": "event-taxonomy-v1",
  "source_watermark": {
    "max_server_event_id": "evt_145",
    "max_received_at": "2026-08-11T15:31:12.410Z",
    "event_count": 37,
    "source_hash": "sha256:..."
  },
  "virtual_effective_at": "2026-08-11T17:35:00+02:00",
  "created_at_real": "2026-07-14T15:45:00Z",
  "vectors": {
    "session": {"dim": 384, "hash": "sha256:..."},
    "short": {"dim": 384, "hash": "sha256:..."},
    "mid": {"dim": 384, "hash": "sha256:..."},
    "long": {"dim": 384, "hash": "sha256:..."},
    "negative": {"dim": 384, "hash": "sha256:..."}
  },
  "positive_facets": {"classical_music": 0.77},
  "negative_facets": {"kids": 0.83},
  "recent_event_ids": ["..."],
  "hidden_event_ids": ["..."],
  "city_affinity": {"Калининград": 1.0},
  "semantic_snapshot_hash": "sha256:...",
  "status": "published"
}
```

## A.5. Evaluation episode

```sql
create table evaluation_episode (
  episode_id text primary key,
  run_id text not null,
  persona_id text not null,
  persona_version int not null,
  seed bigint not null,
  virtual_day int not null,
  session_ordinal int not null,
  device_project text not null,
  catalog_snapshot_id text not null,
  judgment_set_version text not null,
  profile_snapshot_expected text,
  profile_snapshot_applied text,
  canonical_relevant_event_ids text[] not null,
  candidate_relevant_event_ids text[] not null,
  first_relevant_rank int,
  valid_impressions int not null,
  distinct_valid_impressions int not null,
  outcome_status text not null,
  abandonment_rank int,
  failure_layer text,
  evidence_complete boolean not null,
  started_at_virtual timestamptz not null,
  ended_at_virtual timestamptz not null
);
```

## A.6. Metric observation

```json
{
  "run_id": "...",
  "metric_id": "encounter_rate_at_20",
  "metric_version": "v1",
  "grain": "episode",
  "segment": {
    "persona_id": "P07",
    "device": "mobile",
    "catalog_world": "world_release_sealed"
  },
  "numerator": 94,
  "denominator": 100,
  "value": 0.94,
  "ci_method": "wilson-one-sided-95",
  "lower_bound": 0.889,
  "target": 0.90,
  "status": "warning",
  "input_hashes": ["sha256:..."]
}
```

## A.7. Algorithm decision trace

Не логировать огромный opaque debug payload в production, но evaluation/shadow должен иметь компактный trace:

```json
{
  "event_id": "7123",
  "candidate_rank": 6,
  "base_score": 0.74,
  "profile_score": 0.21,
  "negative_penalty": 0.0,
  "freshness_boost": 0.04,
  "fatigue_penalty": 0.0,
  "diversity_adjustment": -0.03,
  "exploration_adjustment": 0.0,
  "final_rank": 4,
  "hard_filters": {"city": "pass", "time": "pass", "price": "pass", "lifecycle": "pass"},
  "reason_codes": ["facet:classical_music", "new_event"]
}
```

Reason codes диагностические; evaluator не использует их как ground truth.

---

# Appendix B. Scenario examples

## B.1. Full mobile longitudinal scenario

```gherkin
Feature: Mature anonymous personalization finds a new relevant event

  Background:
    Given catalog world "catalog-2w-v1"
    And golden persona "P01_classical_tchaikovsky"
    And a clean isolated synthetic tenant
    And no profile is pre-seeded

  Scenario: Profile formed only by UI actions is applied to a new holdout
    Given virtual day is 0
    When the visitor opens the mobile feed without consent
    Then static cards and CTA are usable
    And no personalization telemetry is accepted

    When the visitor grants consent
    And fast-scrolls past 3 cards in less than the valid-impression threshold
    Then those cards have no valid impression

    When the visitor stops on event "classical_seed_1"
    And opens its detail, reads it, returns, and saves it
    Then the impression and actions are accepted exactly once

    Given virtual days 3 and 5 are replayed from the committed scenario plan
    And the visitor performs UI-derived positive classical signals
    And performs an explicit negative nightlife signal

    Given virtual day is 7
    When profile rollup is triggered
    Then the published snapshot watermark includes all expected accepted events
    And the profile maturity rule passes

    Given virtual day is 8
    And unseen holdout "H-P01-perfect-1" first appears in the canonical catalog
    When the visitor starts a fresh browser context with only prior legitimate storage state
    Then the feed request applies the expected profile snapshot and watermark
    And the holdout is present in the candidate evidence
    And the holdout receives a valid impression by card 20
    And no hard-negative event appears in the first 20 cards
```

## B.2. Candidate miss must fail primary E2E

```gherkin
Scenario: Relevant catalog event missing from candidate pool is a product failure
  Given a mature persona
  And canonical snapshot contains active relevant event "H-P09-organ-1"
  And all hard constraints pass
  But candidate generation omits "H-P09-organ-1"
  When the user views the available list
  Then outcome status is "CANDIDATE_MISS"
  And EncounterRateAt20 is 0 for this episode
  And the episode is not marked "NO_RELEVANT_CATALOG_SUPPLY"
```

## B.3. No relevant supply

```gherkin
Scenario: No relevant catalog supply is explicit and not a ranker success
  Given a mature rare-supply persona
  And no active event with independent grade at least 2 passes constraints
  When a session is evaluated
  Then outcome status is "NO_RELEVANT_CATALOG_SUPPLY"
  And the session is excluded from ranking EncounterRate denominator
  And it is included in RelevantSupplyCoverage denominator
  And adjacent discovery metrics are still reported
```

## B.4. Abandonment

```gherkin
Scenario: Fatigue-driven abandonment is not neutral censoring
  Given a supply-eligible mature session
  And the committed behavior plan abandons after 14 valid irrelevant impressions
  When no relevant event has been encountered
  Then outcome status is "ABANDON_BEFORE_RELEVANT"
  And EncounterRateAt20 and EncounterRateAt30 are failures
  And survival output records abandonment as a competing event at card 14
```

## B.5. Cancellation after save

```gherkin
Scenario: Cancelled event cannot remain a successful recommendation
  Given event "evt_saved" was saved on day 5
  And the event is cancelled in day-11 canonical snapshot
  When the day-11 feed is rendered
  Then "evt_saved" is not recommended as active
  And it cannot satisfy first-relevant outcome
  And its detail page exposes cancellation state according to static lifecycle contract
  And ticket CTA does not claim availability
```

## B.6. Negative and undo

```gherkin
Scenario: Explicit negative and undo update the correct event once
  Given event "hard_negative_1" has a valid impression
  When the visitor selects "not interested"
  Then exactly one negative action is accepted
  And the card is hidden
  When the visitor selects undo
  Then exactly one undo is accepted referencing the original action
  And the rollup deterministically reflects the resolved state
```

## B.7. Desktop parity

```gherkin
Scenario: Desktop grid applies the same semantic profile without mobile-feed assumptions
  Given a mature profile formed on prior sessions
  When the visitor opens desktop grid view
  Then telemetry uses viewport_class "desktop" and layout_mode "grid"
  And the expected profile snapshot is applied
  And personalized ordering changes the grid without breaking visible filters
  When the visitor opens a card in a new tab and dwells on detail
  Then the canonical event_detail_view signal is accepted
  When the visitor returns
  Then scroll and filter state are preserved
  And resize does not create duplicate served-list summaries
```

## B.8. Profile staleness

```gherkin
Scenario: Stale profile application is a reliability failure
  Given rollup publishes profile snapshot "ps_10"
  And the next feed uses snapshot "ps_09"
  When the session is evaluated
  Then ProfileToNextFeedApplication is 0
  And outcome status includes reliability failure "PROFILE_NOT_APPLIED"
  And a good rank by chance does not convert this path into an E2E pass
```

---

# Appendix C. Acceptance report template

```markdown
# Personalization evaluation — candidate vs baseline

## Identity
- candidate SHA / algorithm IDs
- baseline SHA / algorithm IDs
- persona, qrels, catalog and simulator versions
- run seed set and device projects

## Decision
ACCEPT | REJECT | INCONCLUSIVE

## Primary outcomes
| Metric | Baseline | Candidate | Paired delta | CI | Target | Status |

## Worst cells
| Persona | World | Device | Baseline outcome | Candidate outcome | Root cause |

## Pipeline diagnostics
Supply → eligibility → candidate recall → profile application → rank → presentation

## Guardrails
Hard constraints, hides, fatigue, useful novelty, concentration, privacy/reliability

## Browser evidence
Scenario IDs, first-attempt result, trace paths, DB reconciliation

## Failure taxonomy
Counts and representative evidence packets

## Decision rationale
Pre-registered rule and whether every condition passed
```

Required comparison plots, each as a separate figure:

1. `F(k)` baseline vs candidate for k=1..30;
2. cumulative incidence encounter vs abandonment;
3. per-persona Encounter@20 with confidence intervals;
4. candidate recall vs presented rank decomposition;
5. useful novelty vs primary relevance trade-off;
6. profile rollup/application latency distribution.

---

# Appendix D. Evidence map and sources

## D.1. Project evidence

### [P01] Anonymous Personalization for Static Event Pages

Project README documents static-first integration, localStorage + Supabase split, Fly SQLite source-of-truth boundary, `event_detail_related` MVP-0, mobile/desktop semantics, served-list evidence and the statement that recommendation quality and real usability remain unproven. **Evidence: E-C.**
https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/unsigned-personalization/README.md

### [P02] Semantic vector retrieval for events

Project document states two-document pgvector retrieval, `search_v3`/`related_v1`, service-role related RPC and optional Gemma verifier. **Evidence: E-C.**
https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/unsigned-personalization/semantic-vector-retrieval.md

### [P03] Event Detail Related MVP-0 Probe Report

Generated probe over a production SQLite snapshot; 296 active future events, 40 anchors and 10 rotated personas. The project itself limits this to automated smoke rather than human quality proof. **Evidence: E-C.**
https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/unsigned-personalization/event-detail-related-probe.md

### [P04] Feature brief under consultation

Defines the requested longitudinal mobile-first harness, profile formation via UI actions, two-week virtual time, KPI questions, anti-bubble guardrails and required report structure. **Evidence: E-C; feature-branch audit confidence C3 where independently unavailable.**
https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/product-e2e-research-brief.md

## D.2. Evaluation, bias and replay

### [R01] Rendle, “Evaluation Metrics for Item Recommendation under Sampling”

Shows sampled ranking metrics can be inconsistent with exact metrics and need not preserve algorithm ordering. Supports full-catalog evaluation and anti-gaming denominator design. **E-B.**
https://arxiv.org/abs/1912.02263

### [R02] Schnabel et al., “Recommendations as Treatments: Debiasing Learning and Evaluation”

Formalizes selection/exposure bias and propensity-based correction in recommendation. Supports treating observational interactions as biased exposure data. **E-B.**
https://arxiv.org/abs/1602.05352

### [R03] Li et al., “Unbiased Offline Evaluation of Contextual-bandit-based News Article Recommendation Algorithms”

Introduces randomized-log replay with unbiased evaluation guarantees under its assumptions. Supports propensity logging and a separation between replay and behavioral simulation. **E-B.**
https://arxiv.org/abs/1003.5956

### [R04] Swaminathan & Joachims, “Counterfactual Risk Minimization”

Provides counterfactual learning/evaluation methods based on logged bandit feedback and propensity scoring. **E-B.**
https://arxiv.org/abs/1502.02362

### [R05] Chen et al., “Top-K Off-Policy Correction for a REINFORCE Recommender System”

Industrial top-K recommendation work emphasizing logged-policy bias, off-policy correction and exploration. **E-B; domain/scale differ from this project.**
https://arxiv.org/abs/1812.02353

### [R06] Gao et al., “KuaiRec: A Fully-observed Dataset and Insights for Evaluating Recommender Systems”

Shows method rankings can vary with exposure bias and data density, and simulation-based imputation only partly alleviates the issue. Supports caution around partial observations. **E-B.**
https://arxiv.org/abs/2202.10842

## D.3. Sequential recommendation and simulation

### [R07] Quadrana, Cremonesi, Jannach, “Sequence-Aware Recommender Systems”

Survey/taxonomy of recommender tasks that use ordered interaction logs, supporting explicit session and longitudinal modeling. **E-B.**
https://arxiv.org/abs/1802.08452

### [R08] Ie et al., “RecSim: A Configurable Simulation Platform for Recommender Systems”

Models user latent state, familiarity, response behavior and sequential interaction. Supports configurable, interpretable simulator architecture. **E-B.**
https://arxiv.org/abs/1909.04847

### [R09] Mladenov et al., “RecSim NG: Toward Principled Uncertainty Modeling for Recommender Ecosystems”

Probabilistic multi-agent simulation platform for extended-horizon policies and uncertainty. **E-B.**
https://arxiv.org/abs/2103.08057

### [R10] Deffayet et al., “SARDINE”

Dynamic and interactive recommendation simulator emphasizing feedback effects and biased data. **E-B.**
https://arxiv.org/abs/2311.16586

### [R11] Stavinova et al., “Synthetic Data-Based Simulators for Recommender Systems: A Survey”

Reviews simulator building blocks, evaluation and simulation-to-reality gaps. Supports calibration and explicit limits on synthetic evidence. **E-B.**
https://arxiv.org/abs/2206.11338

### [R12] Ie et al., “Reinforcement Learning for Slate-based Recommender Systems: SLATEQ”

Addresses slate recommendations and long-term value; supports distinguishing immediate engagement from long-horizon effects. **E-B; not a recommendation to use RL in this MVP.**
https://arxiv.org/abs/1905.12767

### [R13] Ferrari Dacrema, Cremonesi, Jannach, “Are We Really Making Much Progress?”

Finds reproducibility/baseline problems and that simple methods often outperform reported neural methods. Supports strong baselines and replay discipline. **E-B.**
https://arxiv.org/abs/1907.06902

## D.4. Ground truth, novelty and beyond-accuracy

### [R14] NIST, Text REtrieval Conference overview

Authoritative background for reusable test collections and evaluation methodology. Supports pooled, independently judged golden collections. **E-A.**
https://trec.nist.gov/overview.html

### [R15] Vargas & Castells, “Rank and relevance in novelty and diversity metrics for recommender systems”

Formalizes novelty/diversity with rank and relevance, supporting useful rather than irrelevant diversity. **E-B.**
https://doi.org/10.1145/2043932.2043955

### [R16] Abdollahpouri et al., “Addressing the Multistakeholder Impact of Popularity Bias in Recommendation Through Calibration”

Supports monitoring popularity amplification and calibration rather than equal-share diversity. **E-B.**
https://arxiv.org/abs/2007.12230

### [R17] Shi et al., “Judging the Judges: A Systematic Study of Position Bias in LLM-as-a-Judge”

Documents position bias and instability factors in LLM judges. Supports LLM-as-reviewer, not sole release judge. **E-B.**
https://arxiv.org/abs/2406.07791

## D.5. Browser architecture

### [R18] Playwright Clock

Official API for fixed/controlled browser time, including `install`, `pauseAt`, `fastForward` and `runFor`. Supports interaction-clock design. **E-A.**
https://playwright.dev/docs/clock

### [R19] Playwright Browser Contexts / Isolation

Official clean-slate browser-context isolation model. Supports new context per persona-session with explicit storage transfer. **E-A.**
https://playwright.dev/docs/browser-contexts

### [R20] Playwright Mock APIs

Official network/API mocking and HAR facilities. Supports deterministic failure injection, not replacement of DB integration. **E-A.**
https://playwright.dev/docs/mock

### [R21] Playwright Trace Viewer

Official trace artifacts for actions, network and DOM state. Supports mandatory failure evidence. **E-A.**
https://playwright.dev/docs/trace-viewer

## D.6. Statistical methods

### [R22] Kaplan & Meier, “Nonparametric Estimation from Incomplete Observations”

Foundational survival estimator. Applicable only when censoring assumptions are respected; fatigue-driven abandonment is better modeled as competing event. **E-A.**
https://doi.org/10.1080/01621459.1958.10501452

### [R23] Wilson, “Probable Inference, the Law of Succession, and Statistical Inference”

Basis for Wilson binomial intervals used for EncounterRate SLO bounds. **E-A.**
https://doi.org/10.1080/01621459.1927.10502953

### [R24] Efron, “Bootstrap Methods: Another Look at the Jackknife”

Foundational bootstrap method. Supports paired/cluster bootstrap, with clustering adapted to persona/catalog dependence. **E-A.**
https://doi.org/10.1214/aos/1176344552

## D.7. Evidence-to-decision crosswalk

| Significant conclusion | Evidence | Project-specific step |
|---|---|---|
| Evaluate full eligible catalog, not only sampled candidates | [R01], [R06] E-B | exact canonical supply and candidate recall decomposition [E-D] |
| Observed clicks are exposure-biased | [R02]–[R05] E-B | log policy ID/propensity before shadow replay [E-D] |
| Multi-session simulation needs latent state and dynamics | [R07]–[R12] E-B | persona/state/action architecture in sections 6–9 [E-D] |
| Synthetic pass is not real-user proof | [R03], [R06], [R11] E-B | require calibration + controlled canary [E-D] |
| Strong/simple baselines and reproducibility are mandatory | [R13] E-B | paired immutable replay and manifest [E-D] |
| Human judgments must be independent; LLM is not sole judge | [R14], [R17], [P01] E-A/B/C | two assessors + adjudication + sealed holdouts [E-D] |
| Novelty/diversity must preserve relevance | [R15], [R16] E-B | useful novelty and constrained exploration [E-D] |
| Browser time can be controlled but server/catalog time is separate | [R18] E-A | dual-clock design [E-D] |
| Context isolation improves reproducibility | [R19] E-A | fresh context per persona-session [E-D] |
| `<=20` should be distributional, not a lone assertion | [R22], [R23] E-A plus product brief | F20/F30 + percentiles + bounds [E-D] |
| Abandonment should not be silently censored | [R22] E-A assumptions | competing failure for quality evaluation [E-D] |
| Project is not yet a proven longitudinal product loop | [P01]–[P04] E-C | layered architecture and roadmap [E-D] |

---

# Final recommendation

Adopt the following release contract as the concise governing statement:

> For a pre-registered, independently judged mature-persona evaluation set, whenever the canonical active catalog contains at least one event satisfying the persona’s hard constraints and relevance threshold, the personalized product must produce a valid impression of such an event within 20 cards in at least 90% of seeded eligible sessions and within 30 cards in at least 95%; every mandatory golden critical case must pass by card 30. Candidate misses, stale profile application, abandonment caused by the slate and list exhaustion are failures, not exclusions. No-supply is reported separately. The gate is valid only with complete served-list, telemetry, rollup, profile-watermark and browser-presentation evidence, while hard constraints, hidden recurrence, reliability and useful novelty remain non-negotiable guardrails.

Эта формулировка сохраняет исходную продуктовую амбицию, превращает «практически гарантированно» в проверяемый SLO и не позволяет улучшать показатель удалением сложных случаев из denominator.
