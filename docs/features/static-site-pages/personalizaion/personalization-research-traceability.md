# Трассировка исследований в реализацию персонализации

> **Статус:** нормативный guard против подмены целевой модели текущим прототипом.
> **Дата среза:** 2026-08-02.
> **Целевой источник:** [`personalization-to-be.md`](personalization-to-be.md).
> **Назначение:** доказать, что свежая консолидация исследований сохраняется при поэтапной реализации, а старый runtime используется только как объект миграции и characterization.

## 1. Главное правило

`personalization-to-be.md` определяет **какой должна стать система**:

- продуктовую механику;
- представление пользователя и события;
- смысл сигналов;
- горизонты интересов;
- Golden personas;
- модель ранжирования;
- anti-bubble/diversity/exploration;
- политику поверхностей;
- качество, эксперименты и release gates.

`personalization-implementation-contract.md` определяет **как безопасно реализовать эту целевую систему**:

- модульные границы;
- browser storage;
- wire protocol;
- физическую модель БД;
- fault handling;
- rollout и контроль PR.

Фактический `EventLayout.astro`, старые `unsigned-personalization` документы,
demo, prototype, прежние веса и текущий localStorage-профиль **не являются
источником целевой модели**. Они могут использоваться только для:

1. инвентаризации уже работающих функций;
2. сохранения пользовательского поведения во время безопасного refactor;
3. выявления миграционных рисков;
4. построения regression/characterization fixtures;
5. удаления или замены legacy без случайной поломки сайта.

## 2. Приоритет при конфликте

1. Явное актуальное решение владельца продукта в `requirements.md`.
2. Утверждённый legal/localization release contract — только в области права,
   оснований обработки, локализации и публичных документов.
3. `personalization-to-be.md` — продуктовая, исследовательская и модельная
   целевая система.
4. `personalization-implementation-contract.md` — техническая детализация,
   которая может заполнять только оставленные целевой системой инженерные
   пробелы и не может менять её смысл.
5. Этот traceability guard и `implementation-status.yml` — контроль полноты и
   фактической реализации.
6. Аудит current runtime и legacy-код — только migration evidence.

Если целевой документ оставляет вопрос открытым, запрещено автоматически брать
ответ из старого кода. Вопрос получает статус `open hypothesis`, benchmark,
product decision или legal decision. Legacy-значение допускается только как
явно названный baseline для сравнения.

## 3. Карантин legacy

### 3.1. Имена и модульная граница

Wave 0 извлекает старую формулу только в явно переходный namespace:

```text
site/src/lib/personalization/legacy/profile-v1.ts
site/src/lib/personalization/legacy/scorer-v1.ts
```

В Wave 0 запрещено создавать целевой файл
`site/src/lib/personalization/scorer.ts` путём простого переноса старой формулы.
Целевой scorer появляется позднее из target contracts, fixtures и model
bake-off, а не через переименование legacy-модуля.

Legacy module:

- импортируется только compatibility adapter и characterization tests;
- имеет `legacy` в имени экспортов, test suite и diagnostic code;
- не определяет новые surface policies;
- не получает новые веса или продуктовую логику;
- не используется для materialized profile или production experiment;
- удаляется после закрытия migration parity и включения target path.

### 3.2. Characterization не равна acceptance

Legacy characterization доказывает только:

> «refactor не изменил прежнее поведение до того, как мы намеренно заменили его».

Она не доказывает:

- качество рекомендаций;
- корректность старых весов;
- соответствие Golden personas;
- правильность временных горизонтов;
- отсутствие filter bubble;
- причинный продуктовый эффект;
- готовность durable profile loop.

После начала P13N-01/P13N-02 известные legacy expectations переводятся в
migration tests либо удаляются. Они не могут блокировать исправление поведения,
которое противоречит `personalization-to-be.md`.

## 4. Трассировка целевой модели

| Целевая наработка | Источник в `personalization-to-be.md` | Реализационный владелец | Статус до начала кода |
|---|---|---|---|
| Static-first, полезный no-JS/backend fallback | §§3, 6, 7, 14, 26, 30 | P13N-00/02, route inventory, browser E2E | зафиксировано |
| Активация первым осмысленным действием без предварительного opt-in | §§3, 6, 9.1, 9.3, 15.3, 22.7 | P13N-01/03/04 + legal gate | зафиксировано |
| Раздельные purpose consents | §§3, 9.3, 9.5 | identity/legal flows, не legacy consent | зафиксировано |
| Один durable owner профиля, YDB не второй SOR | §§7.1, 8, 9 | P13N-04 + ownership ADR | зафиксировано, localization blocked |
| Compact browser projection + immediate overlay | §§7.3, 9.4, 14.3 | P13N-01/05 | зафиксировано |
| Периодический materializer, не per-action recompute | §§3, 9.4, 22.9 | P13N-05 | зафиксировано |
| ETag/304, `next_refresh_at`, stale-compatible fallback | §§9.4, 17, 22.9 | P13N-05 | зафиксировано |
| Zero-network rerank тематических подборок | §§3, 6, 14.3, 22.9 | P13N-02 | зафиксировано |
| Менять только невидимую часть, frozen prefix | §§6, 14.3, 15.1, 22.2 | P13N-02 | зафиксировано |
| Calendar/date primary: chronological truth + exact hide only | §§6, 14.1, 22.3 | P13N-02 | зафиксировано |
| Отдельный personal tail под календарём | §§14.1–14.3 | P13N-02/06 | зафиксировано |
| Тематические подборки: слабее, чем «Для меня» | §§13.4, 14.1 | P13N-02/06 | зафиксировано |
| Search query-first, related anchor-first, popular popularity-first | §§13, 14.1 | P13N-02 | зафиксировано |
| Exact hide глобален и не rescue | §§3, 6, 12, 13.4, 15.2 | P13N-01/02/03 | зафиксировано |
| Pending hide, countdown, Undo, hidden collection | §§15.2, 22.1 | P13N-01 | стартовая UX-гипотеза 5 секунд |
| Exact hide не равен отрицанию жанра | §§10.1, 12 | P13N-01/05 | зафиксировано |
| Typed negative: цена/время/расстояние/компания отдельно от вкуса | §§10.1, 12, 22.4 | P13N-01/05 | зафиксировано |
| Like влияет на short/mid и после повторения long | §§10.2, 12, 22.4/22.6 | P13N-05 model materializer | требует benchmark весов |
| Share — сильный social/short signal, не первый activation | §§9.3, 12, 22.6 | P13N-01/05 | зафиксировано |
| CTA не равен attendance | §12 | P13N-03/05 + verified attendance flow | зафиксировано |
| Подтверждённое и повторное посещение формирует long-term evidence | §§10.2, 12, 22.6 | P13N-05, отдельный trusted flow | будущая реализация |
| Session/short/mid/long horizons; long не менее 6 месяцев | §10.2 | P13N-05 | short/mid остаются versioned hypotheses |
| Профиль многомерный, не один vector/persona | §§10.1–10.3 | P13N-05 schema/materializer | зафиксировано |
| Golden personas — soft mixture с `unknown` mass | §§10.3, 13.3 | P13N-05 model variants | не реализовывать в Wave 0 |
| Global и русскоязычный контекст — language/market overlay, не taste proxy | §§3, 10.1, 10.3 | P13N-05 feature model | зафиксировано |
| Adaptive cold-start questionnaire 6–8 вопросов, можно пропустить | §10.4 | отдельный product slice после core P13N-02 | сохранено, не MVP gate |
| Sensitive topics не создают user facets/long-term evidence | §§4.2, 10.5, 11–12, 22.10, 26.1 | все waves, schema hard gate | зафиксировано |
| События «Детям» не означают профилирование ребёнка | §10.5 | surface/event model | зафиксировано |
| Event feature snapshot: facets, motivation, atmosphere, social scenario, constraints, provenance | §11 | upstream enrichment + P13N-05 | будущая реализация |
| LLM-first enrichment только build/offline, не browser/runtime | §§11, 13 | event feature pipeline | зафиксировано |
| Eligibility до scoring | §§6, 13.1–13.2, 20 | P13N-02 pure pipeline | hard gate |
| Candidate pools: surface, semantic, editorial/popular, affinity, exploration | §13.1 | P13N-05/06 | будущая реализация |
| Facets + soft persona + session + graph + constraints | §§13.2–13.3 | P13N-05 model bake-off | кандидат, не назначенный победитель |
| Variants control/facets/hard-persona/soft-persona/hybrid | §13.3 | P13N-05 offline evaluator | обязательно до выбора модели |
| Diversity, novelty, exploration и anti-bubble | §§13.1, 13.4, 20.3 | P13N-05/06 | будущая реализация |
| Persona suppression только при высокой уверенности | §13.4 | P13N-05 | threshold открыт |
| Rescue не более 10% persona-suppressed и никогда не exact hide | §§13.4, 20.1, 26.1 | P13N-05/06 | hard invariant |
| Campaign/easter-egg interactions не обучают organic implicit profile | §§11–12, 22.5 | P13N-05/06 | зафиксировано |
| `/dlya-menya/`: strong hybrid, diversity/exploration, качественный fallback | §§14.1–14.2 | P13N-06 | будущая реализация |
| Durable «Для меня» только после подходящей identity/auth модели | §14.2 | P13N-04/06 | зафиксировано |
| Forwardable secret snapshot не является authentication | §14.2 | personal-page/email feature | сохранено отдельно |
| Interest percentage UI — только после calibration, не «вероятность» | §16 | post-P13N-06 product research | не выпускать сейчас |
| Короткие пользовательские explanations без raw history | §17 | P13N-05/06 | будущая реализация |
| Model/feature/taxonomy/experiment registry и served evidence | §§17, 23–24 | P13N-02/05/06 | зафиксировано |
| Golden fixtures/judgements/counterfactuals | §§19, 22 | P13N-02/05 | обязательный quality corpus |
| NDCG/MRR/coverage/diversity/novelty/worst-group/latency | §20.3 | P13N-05 offline evaluator | обязательный gate |
| Browser E2E доказывает correctness/sensitivity, но не causal uplift | §18 | P13N-00/02/06 | зафиксировано |
| A/B заранее регистрирует hypothesis/metric/guardrails/stop rule | §§18, 26.3 | P13N-06 | до production rollout |
| Longitudinal UI→DB→materializer→next feed canary | §§20.4, 30 | P13N-04/05/06 | обязательный live/staging proof |
| Desktop, Android и iOS critical/focus reliability | §§20–22 + release test strategy | P13N-02/03/06 | обязательный rollout gate |
| Localization/legal gate до remote profile writes | §§7.1, 9.5, 26.1, 28 | до P13N-03/04 production | blocker |

## 5. Что Wave 0 намеренно не реализует

Wave 0 не должна «приблизительно реализовать» будущую модель. Она только создаёт
безопасную заменяемую границу вокруг старого runtime.

В частности, Wave 0 не реализует и не переопределяет:

- Golden personas;
- short/mid/long materialization;
- model graph и adjacent-interest expansion;
- anti-bubble/exploration/suppression;
- target scoring weights;
- cold-start questionnaire;
- attendance evidence;
- sensitive-topic enrichment;
- interest percentages;
- offline quality winner;
- `/dlya-menya/` production model;
- A/B или causal success.

Отсутствие этих частей в первом PR означает `not started`, а не отказ от них.
Они остаются обязательными там, где `personalization-to-be.md` обозначает их как
часть целевой системы или release gate.

## 6. Обязательный research-delta review для каждого PR

Каждый P13N PR отвечает в описании на четыре вопроса:

1. Какие пункты этой таблицы затронуты?
2. Какая часть `personalization-to-be.md` реализована, не начата или намеренно
   отложена?
3. Использован ли legacy baseline? Если да, почему он не стал target default?
4. Не появилось ли новое решение, отсутствующее в целевом документе? Если
   появилось, оно должно быть оформлено как hypothesis/owner decision и внесено
   обратно в целевую документацию до production effect.

`implementation-status.yml` хранит ссылки на реализованный scope и evidence, но
не может менять смысл целевой системы.

## 7. Release NO-GO против искажения исследований

Релиз или этап блокируется, если:

- legacy scorer переименован в target scorer без model bake-off;
- старый numeric weight объявлен целевым только потому, что уже есть в коде;
- старый `consent_ok`, hide semantics или public profile RPC сохранены ради
  «совместимости» после соответствующей migration wave;
- открытый вопрос из `personalization-to-be.md` молча заполнен legacy-значением;
- Golden persona превращена в hard label;
- один exact hide создаёт genre dislike;
- weak view/scroll/dwell начинает обучать профиль до отдельного gate;
- calendar chronology изменена ради персонального score;
- sensitive interaction материализует facet;
- characterization PASS выдан за quality/production PASS;
- отсутствует research-delta review в PR, меняющем персонализацию.

## 8. Итог

Свежая исследовательская консолидация не переносится в старые скрипты и не
подстраивается под них. Старые скрипты временно помещаются в quarantine,
характеризуются и удаляются по мере включения целевых слоёв.

Целевой путь направлен только в одну сторону:

```text
requirements + research/to-be
        ↓
target contracts + fixtures
        ↓
new runtime/materializer/model
        ↓
controlled evidence and rollout
```

Путь `legacy code → inferred product truth` запрещён.
