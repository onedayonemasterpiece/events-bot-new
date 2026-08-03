# Варианты стратегии онбоардинга стандартного пользователя

> **Статус:** decision record после консолидации Gemini и ChatGPT.  
> **Выбранный baseline:** вариант A.  
> **Связанные документы:** [каноническая стратегия](README.md), [синтез исследований](research/research-synthesis-2026-08-03.md).

## Решение в одном абзаце

Исследования не дают оснований выбирать между «туром» и «контекстным онбоардингом»: тур отвергнут обоими. Реальная вариативность — насколько активно использовать Hero Talk, Page-end Talk и артефакты после первой ценности. Поэтому зафиксирован один безопасный baseline, один более выразительный challenger и один отдельный культурный extension.

## Сравнение

| Параметр | A. Сдержанный utility-first | B. Редакционно-повествовательный | C. Культурный extension |
|---|---|---|---|
| Роль | Основная стратегия MVP | Challenger после baseline | Не onboarding MVP; post-release track |
| Cold/unknown Hero | Всегда статическое service promise | То же | Не меняется |
| Dynamic Hero | Нет feature-selling; только site-wide safety | Редкие editorial/return sequences после доказанного return | Не используется для первой находки |
| Page-end Talk | Один post-value next step | Связная цепочка из редких narrative messages | Один добровольный cultural invitation |
| Inline guidance | Только error/recovery и рядом с действием | То же, плюс редкие context stories | Exact clue и accessible marker |
| Action echo | Основная обучающая механика | Основная обучающая механика | Отдельно от artifact feedback |
| Login/PWA/permissions | Только после antecedent value и release/platform eligibility | То же | Не обязательны |
| Персонализация | Только factual local/result explanation | Допускается более выразительный narrative copy, но те же facts/controls | Artifact interactions не taste signal |
| Артефакты | Нет в MVP | Первый точный hint только после mastery и отдельного gate | 3-object mini-collection либо static cultural route |
| State complexity | Низкая | Средняя/высокая | Отдельный bounded ledger |
| Риск banner blindness | Минимальный | Существенный | Низкий при отдельной surface |
| Риск CTA cannibalization | Минимальный | Требует non-inferiority experiment | Требует отдельного holdout |
| Редакционный потенциал | Умеренный | Высокий | Высокий культурный, но не utility |
| Рекомендуемый срок | MVP | После доказанного MVP | Post-release |

## Вариант A — сдержанный utility-first contextual onboarding

### Назначение

Помочь человеку получить пользу и освоить реально нужное действие, не создавая отдельный слой «обучения сайту».

### Контракт

```text
static orientation
→ real discovery/event task
→ inline help only on observable difficulty
→ action success/failure echo
→ optional single Page-end next step after value
→ capability suppression/mastery
```

### Hero Talk

- в первый и неизвестный визит остаётся стабильным service promise;
- не продвигает последовательно функции;
- не заменяется campaign или artifact;
- меняется только при site-wide lifecycle/safety incident либо после отдельного owner-approved redesign.

### Page-end Talk

Появляется не потому, что человек доскроллил, а когда есть antecedent value: event decision, successful reversible action, saved state, completed recovery или явный Help.

Разрешён один message:

- открыть результат действия;
- продолжить поиск по дате/подборке;
- объяснить фактический local rerank;
- позже — предложить PWA/identity только после release и eligibility.

### MVP capabilities

- date/route orientation — постоянный IA, без prompt;
- event facts — хороший content hierarchy, не coachmark;
- `Не интересно` — exact echo + Undo;
- like — точная реакция, не «сохранение»;
- share — различать share-sheet invocation и copy success;
- calendar/ICS — только после owner decision по семантике;
- Search recovery — только после production acceptance Search.

### Почему выбран

- полностью совместим со static/no-JS baseline;
- использует уже существующий accessible toast/live-region вместо нового движка;
- требует минимального пользовательского состояния;
- легче проверяется при небольшом трафике;
- создаёт чистую контрольную группу для будущего narrative challenger;
- не мешает параллельной разработке редакционного стиля.

### Kill/rollback

Любой proactive message отключается независимо, если:

- ухудшается first-value time или core CTA;
- появляется после permanent dismissal;
- обещает не выпущенный результат;
- создаёт duplicate announcement/focus obstruction;
- не даёт capability success в task research;
- требует недоступный backend/platform feature.

## Вариант B — редакционно-повествовательный contextual onboarding

### Назначение

Использовать Hero Talk и Page-end Talk как последовательный голос сайта: не случайные фразы, а связные небольшие narrative chains, учитывающие route, событие, фестиваль и предыдущее подтверждённое действие.

### Что остаётся неизменным

- никакого tour/checklist;
- static core и факты до сообщений;
- одна proactive message за page journey;
- action confirmation выше narrative;
- utility/promo/artifact purpose разделены;
- dismissal и accessibility равноправны;
- Golden personas не меняют давление, права и CTA.

### Дополнительные возможности

- returning Hero может показывать editorial context, если service identity остаётся видима;
- Page-end messages могут образовывать цепочку между визитами;
- event/festival context может определять содержание, но не переписывать canonical facts;
- редакционный стиль может добавлять лёгкую иронию в безопасных low-stakes состояниях;
- после mastery может появиться одна точная подсказка культурного слоя.

### Дополнительная архитектура

Требуются:

- registry narrative sequence/thread id;
- state `previous_message_id`, но без полного clickstream;
- conflict resolution с promo campaigns;
- copy/version provenance;
- route/event/festival eligibility;
- cap на sequence и явное завершение истории;
- fail-closed fallback к варианту A.

### Основные риски

- Hero превращается в рекламную «говорящую шапку»;
- route-aware текст ошибочно выглядит персональным;
- последовательность конкурирует с текущей задачей;
- campaign takeover маскируется под помощь;
- потеря local state воспроизводит начало цепочки;
- заметное усложнение editorial и QA процесса.

### Условия допуска

Вариант B не становится baseline до выполнения всех условий:

1. вариант A выпущен и имеет стабильный first-value baseline;
2. comprehension-тест подтверждает узнаваемость service promise;
3. message resolver доказывает one-message и purpose separation;
4. treatment не ухудшает event-card/date/Search outcomes;
5. owner принимает конкретные narrative chains, а не абстрактную «динамичность»;
6. есть независимый kill switch для каждого family/sequence.

## Слой C — культурное исследование

### Статус

Это не вариант первичного онбоардинга. Он подключается после основной стратегии и может быть полностью отклонён без изменения A/B.

### Допустимые формы

#### C1. Мини-коллекция

- одна тема;
- первоначальный ориентир — 3 объекта;
- первая подсказка точная;
- progress локальный или отдельный bounded ledger;
- нет login, prize, streak, share advantage;
- provenance, rights, freshness и archive state обязательны.

#### C2. Статический культурный маршрут

Обычная редакционная страница/подборка без hidden placements, progress и state. Это самый дешёвый и доступный способ проверить культурную ценность без gamification debt.

#### C3. After-action декоративный объект

Появляется после подтверждённого core action, но не является наградой, не считается progress и не влияет на профиль. Допустим только после проверки, что не отвлекает от result echo.

### Не допускается

- использовать находку как prerequisite функции;
- маскировать artifact под event badge/quality mark;
- считать find/completion product activation;
- выводить интерес к теме из находки;
- требовать hover, motion, точное наведение или второй экран;
- использовать focus-group prize model;
- ставить opaque clue вместо первого понятного пути.

## Решение владельцу

### Уже принято по умолчанию

**Вариант A** считается каноническим baseline и может переходить к детальному contract/implementation planning.

### Не требует решения сейчас

Вариант B и слой C не блокируют MVP. Они остаются challenger/post-release и возвращаются к owner review после evidence варианта A.

### Решения, которые всё ещё нужны независимо от варианта

1. Семантика `like` / `favorite_saved` / `calendar_saved` / ICS и итоговое имя `Моё`.
2. Какие capabilities реально release-ready.
3. Какой action-result copy подтверждается current implementation.
4. Когда PWA/identity/reminder имеют доказанный antecedent value.
5. Какой legal/data contract применим к target personalization.
6. Имеет ли культурный layer самостоятельную продуктовую ценность, кроме novelty.
