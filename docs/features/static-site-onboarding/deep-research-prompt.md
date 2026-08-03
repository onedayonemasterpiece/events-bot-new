# Prompt: глубокое исследование онбоардинга стандартного пользователя KenigEvents

Ниже — самостоятельный prompt для режима глубокого исследования. Его можно
передать исследовательскому агенту без дополнительной переписки.

Актуальная рамка вопроса находится в соседнем документе:
[стратегия standard-user onboarding](README.md).

---

## PROMPT START

Ты — **критический продуктовый исследователь, UX-стратег и редактор интерфейсных
коммуникаций**. Твоя специализация — contextual onboarding, progressive
disclosure, feature discovery, recommender transparency, event-discovery
products, PWA/permission prompts, accessibility и этика behavioral design.

Не подтверждай исходную идею автоматически. Для каждого механизма допускай
решения `использовать`, `сузить`, `отложить`, `заменить` или `не внедрять`.
Лучший onboarding иногда состоит в более понятном интерфейсе и отсутствии
дополнительной подсказки. Ищи не только успешные кейсы, но и evidence о tour
fatigue, banner blindness, tooltip overload, permission denial, reactance,
novelty effect, cannibalization основных CTA и dark patterns.

Отвечай по-русски.

## 1. Цель исследования

Разработать доказательную, ненасильственную и при этом эффективную стратегию
постепенного освоения KenigEvents обычным пользователем — **не участником
фокус-группы**.

Стратегия должна помочь человеку:

1. быстро понять ценность сервиса;
2. найти подходящее актуальное событие;
3. разобраться в фактах и действиях карточки;
4. постепенно освоить поиск, даты, reactions, сохранение/календарь, `Для меня`,
   объяснения персонализации, identity sync, PWA и utility reminders;
5. при желании открыть дополнительный слой региональных артефактов-пасхалок;
6. перейти от onboarding к будущей retention/return strategy без скрытой
   подписки, давления и смешения purpose.

Нужен не общий список «best practices», а repo-aware решение с точными
триггерами, состояниями, suppressions, примерами русских фраз, KPI, рисками,
экспериментами и implementation-ready decision framework.

## 2. Контекст продукта

KenigEvents / «Полюбить Калининград · Анонсы» — статическая афиша событий
Калининградской области для жителей и туристов. Основная ценность — быстро найти
жизнеспособное событие и принять решение: узнать подробности, сохранить,
добавить в календарь, поделиться, перейти к билету/регистрации либо осознанно
отказаться и продолжить поиск.

Продукт static-first:

- общая выдача и страницы событий должны быть полезны без входа, JavaScript и
  внешнего runtime;
- до первой ценности нельзя ставить регистрацию, настройку интересов, PWA,
  email или permissions;
- like, `Не интересно`, calendar/save, favorite, utility reminder, email,
  utility Push, promo Push и marketing — разные действия/purposes;
- персонализация должна быть объяснимой и управляемой;
- артефакты — конечный добровольный культурный слой, не North Star и не условие
  доступа;
- фокус-группа имеет отдельные задания, feedback и возможную prize programme;
  эти механики нельзя переносить на стандартного пользователя по умолчанию;
- трафик регионального продукта может быть недостаточен для быстрых больших
  A/B-тестов, поэтому нужен честный mixed-methods plan.

Аудитория неоднородна: новый житель, постоянный пользователь, турист, человек с
конкретным deep link, посетитель даты/выходных, пользователь Search, mobile/PWA
и desktop. Не делай демографические или психографические выводы без evidence.

## 3. Обязательный аудит репозитория

Repository:
https://github.com/onedayonemasterpiece/events-bot-new

Сначала проверь **актуальный `main` на дату исследования**, запиши exact SHA и
отдели:

- реализованное в `main`;
- noindex/lab/secret-candidate research;
- принятую целевую документацию;
- open PR / backlog / гипотезу;
- устаревший prototype.

Не считай название файла, PR body или screenshot доказательством production
behavior. Проверяй код, feature flags, release status и canonical docs.

### 3.1. Основные материалы

1. Текущий home composition:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/site/src/pages/index.astro
2. Реальный home Hero Talk:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/site/src/components/HomeHeroTalk.astro
3. Cold-start feed и local rerank:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/site/src/components/HomeColdStartFeed.astro
4. Event card actions:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/site/src/components/EventCard.astro
5. Static-site canonical overview:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/README.md
6. Целевая персонализация:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/personalizaion/personalization-to-be.md
7. Data ownership:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/architecture/personalization-data-ownership.md
8. Favorites/calendar:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/event-favorites-calendar/README.md
9. Site identity:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/site-user-identity/README.md
10. Mobile shell:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/mobile-shell.md
11. Search contract:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/unsigned-personalization/authorized-event-search.md
12. Общая механика артефактов:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-easter-eggs/README.md
13. Critical product analysis артефактов:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-easter-eggs/product-analysis.md
14. Amber artifact research:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/amber-artifact-easter-egg.md
15. Promo campaigns:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/promo-campaigns/README.md
16. Фокус-группа — только как отдельный research context:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-focus-group/README.md
17. Focus product prototype:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-focus-group/product-prototype.md
18. Focus easter-egg programme:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-focus-group/easter-egg-program.md
19. Исходный draft standard-user strategy:
    https://github.com/onedayonemasterpiece/events-bot-new/blob/agent/standard-user-onboarding-strategy/docs/features/static-site-onboarding/README.md

### 3.2. Planned context — не production truth

Проверь актуальный state и merge ancestry, прежде чем использовать:

- PR #235 — Favorites, utility/promo Push и calendar delivery:
  https://github.com/onedayonemasterpiece/events-bot-new/pull/235
- PR #270 — research-led cross-page personalization contract:
  https://github.com/onedayonemasterpiece/events-bot-new/pull/270

Если PR уже merged, используй соответствующий `main` SHA. Если открыт или
закрыт без merge, маркируй его только как planned context.

## 4. Исходные архитектурные уточнения

Не создавай отдельные сущности `Onboarding Talk` и `Campaign Talk`.

- `Hero Talk` — placement в верхней hero-зоне.
- `Page-end Talk` — тихое продолжение в конце страницы после основной ценности.
- Рассказ о функции — intent сообщения, которое может быть показано в одном из
  этих placements.
- Информация фестивальной/promo campaign — другой intent/source в тех же
  placements.
- Evergreen onboarding может потенциально быть системной campaign/activity для
  общего control plane, но это гипотеза: оцени пользу и риск смешения purpose,
  consent, приоритетов и метрик.

Разделяй минимум четыре измерения:

```text
placement     = hero_talk | page_end_talk | inline | action_echo | help
intent        = orientation | capability_guidance | confirmation |
                personalization_explanation | return | editorial |
                campaign | artifact_hint | safety
source        = system | editorial | promo_campaign | event/festival context
objective     = first_value | competence | continuity | retention | promotion |
                cultural_exploration
```

## 5. Неподвижные ограничения

1. Никакого обязательного welcome tour, checklist или tutorial wall.
2. Никакого обязательного login, email, PWA install, profile setup или
   notification permission до core value.
3. Нельзя скрывать event facts, navigation, ticket/registration, calendar или
   legal terms до «прохождения» обучения.
4. За стандартный onboarding нет points, streak, leaderboard, prize advantage,
   chance multiplier, share/invite advantage или потери прогресса.
5. Отказ, dismissal и permission denial — нормальные исходы, не ошибка
   пользователя.
6. Не учить всем функциям в первую сессию.
7. Не считать exposure, scroll, dwell или закрытие подсказки освоением.
8. Не выдавать локальный prototype/rerank за обученную модель или cross-device
   профиль.
9. Campaign/artifact interaction не становится taste signal автоматически.
10. Utility reminder и promo Push имеют отдельные purposes/consents.
11. Артефакт не является единственным способом узнать о ключевой функции и не
    конкурирует с event CTA.
12. Core site полезен при JS/storage/backend/analytics failure.
13. Touch, keyboard, screen reader, reduced motion, no-hover/no-audio и zoom
    получают эквивалентный путь.
14. Не использовать dark patterns: fake urgency, confirmshaming, nagging,
    obstruction, disguised ads, scarcity, forced continuity, preselected
    permissions.
15. Не оптимизировать onboarding completion, число prompts, время на сайте,
    permission grants или raw collection как North Star.

## 6. Главные исследовательские вопросы

### 6.1. Первая ценность и activation

1. Как для региональной афиши корректно определить `first value` и activation?
2. Достаточно ли открытия event detail, либо нужен explicit event decision,
   save/calendar, ticket intent или repeat visit?
3. Как равноценно учитывать полезный отрицательный результат: `Не интересно`,
   ноль результатов, отказ от permission, возвращение к поиску?
4. Какие метрики не должны становиться activation proxy?
5. Как различать first value у date-led, search-led, deep-link, tourist,
   festival и returning journeys?

### 6.2. Progressive learning

1. Какие функции должны быть самопонятны через IA/labels и вообще не требуют
   proactive prompt?
2. Какой один следующий шаг уместен на каждом этапе?
3. Когда использовать persistent affordance, inline hint, Hero Talk, Page-end
   Talk, action echo, coachmark, help page или ничего?
4. Какие функции безопасно учить только после успешного действия?
5. Как определить `eligible`, `attempted`, `succeeded`, `repeated`, `mastered`,
   `dismissed` и `needs_reintroduction`?
6. Когда и как прекращать подсказки навсегда либо до новой версии функции?
7. Как обращаться с новым устройством, очищенным storage и anonymous→authorized
   merge без повторного агрессивного onboarding?

### 6.3. Конкретные возможности

Для каждой функции определи trigger, prerequisite, message, placement, success
proof, cap, cooldown, suppression, fallback, analytics boundary и kill rule:

- Today / Tomorrow / Weekend / date navigation;
- Search и zero-result recovery;
- чтение event facts и lifecycle/status;
- like;
- `Не интересно` и undo;
- share;
- calendar/save;
- saved events / `Моё` / `Мои события`;
- `Для меня`;
- `Почему это` и editor интересов;
- login/identity sync;
- PWA install;
- utility reminder/email/Push;
- promo Push как отдельный non-onboarding purpose;
- артефакты и первая точная подсказка;
- help/recovery после ошибки.

Не проектируй обучение функции, которая не доказана как выпущенная в текущем
release state.

### 6.4. Hero Talk / Page-end Talk

1. Какую роль должен играть Hero Talk в первый, второй и последующие визиты?
2. Должна ли первая service orientation быть защищена от promo takeover?
3. Какие сообщения допустимы в Hero, а какие только после основной ценности в
   Page-end?
4. Как разрешать конфликт safety, direct feedback, onboarding, return,
   editorial, promo campaign и artifact hint?
5. Нужен ли единый message registry и какой minimum schema?
6. Что хранить как system campaign, а что как отдельный product state?
7. Как caps/cooldown/dismissal работают между страницами и устройствами?
8. Как избежать «говорящей шапки», которая каждый раз мешает пользователю?

### 6.5. Артефакты

1. Улучшают ли они feature discovery или отвлекают от event decision?
2. Когда человек уже достаточно понимает core UI, чтобы увидеть первую
   подсказку?
3. Стоит ли первая подсказка точно называть место, чтобы не создавать opaque
   challenge?
4. Какие page families можно исследовать через артефакты без искусственного
   pageview farming?
5. Как отделить cultural reward от hidden advertising и event semantics?
6. Какие accessibility, IP/provenance, freshness, placement-stability и fatigue
   gates обязательны?
7. Как измерять incremental downstream event value, а не completion?

### 6.6. Редакционный стиль

1. Какие характеристики copy создают ощущение дружелюбного, слегка ироничного,
   но взрослого и литературно качественного собеседника?
2. Где ирония неуместна: ошибки, permissions, отказ, accessibility, потеря
   state, отмена события?
3. Как писать `действие → результат` без канцелярита, инфантилизации и FOMO?
4. Какие фразы честно сообщают локальность, ограниченность и изменяемость
   персонализации?
5. Нужны ли lexical variants по Golden personas, либо они создадут
   фрагментацию/неравное давление?
6. Как автоматически и редакционно проверять качество generated phrases?

### 6.7. Handoff в retention

1. Где заканчивается onboarding конкретной функции?
2. Какой минимальный competency/value state можно передать retention?
3. Когда допустимо предложить возвращение, PWA, reminder или account sync?
4. Как не превратить onboarding в скрытый CRM acquisition?
5. Как отделить utility return, editorial retention, promo campaign и marketing?
6. Какие onboarding messages нельзя повторно использовать для push/email без
   отдельного purpose?

### 6.8. Измерение при низком трафике

1. Какие quantitative outcomes реалистичны для регионального продукта?
2. Как оценить MDE и необходимую выборку до запуска A/B?
3. Когда лучше moderated usability, intercept interview, task test, diary,
   sequential test или synthetic/fixture QA?
4. Как провести A/A, проверить SRM, instrumentation и event quality?
5. Как учитывать novelty и learning carryover?
6. Как избежать p-hacking и ложной причинной уверенности?

## 7. Метод исследования

### 7.1. Источники

Используй актуальные и проверяемые источники; для каждого фиксируй дату доступа.
Приоритет:

1. peer-reviewed research, systematic review, мета-анализ;
2. W3C/WCAG и официальные platform/browser guidelines;
3. официальная продуктовая документация и опубликованные эксперименты;
4. качественные case studies с методологией и численными данными;
5. экспертные статьи — только как слабое evidence, явно так помеченное.

Не используй SEO-списки «10 onboarding tips» как основу решения. Не цитируй
product marketing claim как доказанный causal result.

### 7.2. Evidence matrix

Собери минимум **18 релевантных продуктов/механик**, включая:

- не менее 6 event/local discovery, city guide, culture или ticketing products;
- не менее 3 editorial/media products;
- не менее 3 search/recommender/travel/map products;
- не менее 2 PWA/install/notification permission patterns;
- не менее 2 cultural trail/scavenger/collection patterns;
- минимум 4 отрицательных, неудачных или рискованных примера.

Для каждого:

| Поле | Требование |
|---|---|
| Product/context | страна, аудитория, device/surface, дата наблюдения |
| Observable fact | что реально показано/задокументировано |
| Evidence | direct link и strength |
| User task | какую задачу решает |
| Mechanism | placement, trigger, timing, dismissal, feedback |
| Transferable | что применимо к KenigEvents |
| Non-transferable | что нельзя переносить и почему |
| Risk/failure | давление, fatigue, accessibility, cannibalization |
| Measurement | что известно и чего не доказано |

Чётко отделяй наблюдаемый факт от своей интерпретации.

### 7.3. Литературный обзор

Отдельно сведи evidence минимум по темам:

- progressive disclosure и just-in-time guidance;
- cognitive load и interruption cost;
- user autonomy/reactance;
- habituation/banner blindness/tooltip fatigue;
- feedback loops и learning transfer;
- recommender explanations/control;
- permission priming, notification timing и denial;
- PWA install prompts;
- dark patterns/nagging/confirmshaming;
- accessibility of transient help, motion and focus;
- novelty effects и longitudinal behavior;
- low-traffic experiment design.

Не делай сильный вывод из одной лабораторной работы вне контекста. Укажи
population, setting, limitations и применимость.

### 7.4. Repo traceability

Каждое предложение связывай с:

- current component/route;
- current or target product contract;
- needed new state/data;
- dependency/feature flag;
- possible conflict;
- testable outcome;
- rollout wave.

## 8. Обязательные deliverables

### 1. Answer-first verdict

Дай общий verdict и отдельный verdict по механизмам:

- static value proposition;
- dynamic Hero Talk;
- Page-end Talk;
- inline hints;
- action confirmations;
- coachmarks/tooltips;
- checklist/progress;
- login/PWA/permission prompts;
- artifact-led discovery;
- contextual reintroduction.

Для каждого: `proceed | narrow | defer | reject`, confidence, evidence и kill
criteria.

### 2. Current-state audit

Таблица:

```text
capability / current implementation / release truth / current copy /
state ownership / telemetry / gap / risk
```

Обязательно перечисли расхождения между code, docs, labs и open PRs.

### 3. Определение первой ценности и activation

Сравни минимум 3 кандидата, например:

- event detail comprehension;
- first explicit event decision;
- calendar/save;
- ticket/registration intent;
- qualified repeat visit.

Для каждого: достоинства, bias, missing evidence, instrumentation, guardrails.
Выбери recommendation либо staged definition.

### 4. Journey maps

Отдельные карты для:

- home cold start;
- `/segodnya/`/date-led входа;
- Search-led входа;
- прямого event deep link;
- festival/collection входа;
- returning anonymous;
- authorized/saved-state return;
- mobile/PWA;
- desktop/keyboard.

Каждая карта должна содержать:

```text
user intent → current value → eligible next capability → message/placement →
success evidence → suppression → retention handoff
```

### 5. Competency state machine

Дай точную модель для каждой capability:

`unknown → eligible → exposed → attempted → succeeded → repeated → mastered`,
а также dismissal, cooldown, failure, version change, reset и
anonymous→authorized merge.

Для каждого перехода укажи доказательство и запрещённые proxy.

### 6. Feature-learning matrix

Полная таблица ключевых функций с:

- prerequisite;
- моментом обучения;
- placement;
- message intent;
- primary/secondary CTA;
- success/failure evidence;
- cap/cooldown;
- permanent suppression;
- accessibility fallback;
- storage/consent boundary;
- KPI/guardrail;
- release dependency.

### 7. Hero Talk / Page-end Talk orchestration

Разработай:

- intent taxonomy;
- source taxonomy;
- message schema;
- priority and arbitration matrix;
- caps/cooldowns/dismissal;
- campaign/artifact/retention conflicts;
- pseudocode decision engine;
- static/no-runtime fallback;
- operator/editor preview and audit needs;
- kill switch/rollback.

Отдельно ответь: стоит ли evergreen onboarding представлять как системную promo
campaign или лучше сохранить отдельный registry при общем renderer/control
plane.

### 8. Артефакты в onboarding

Дай правила:

- eligibility первой подсказки;
- точная или загадочная подсказка;
- placement;
- связь с page families;
- первая находка;
- collection/history reveal;
- no-login path;
- accessibility;
- IP/provenance;
- analytics isolation;
- guardrails и kill rules.

Предложи один bounded MVP flow и не менее двух альтернатив, включая вариант
`не использовать артефакты для обучения функциям`.

### 9. Редакционный copy framework

Нужны:

1. критерии качества фразы;
2. scorecard с минимум такими осями:
   `понятность, релевантность, правдивость, автономия, литературное качество,
   action-result clarity, терминологическая согласованность, accessibility,
   pressure risk`;
3. human review workflow;
4. автоматические lint/LLM-critic checks и их ограничения;
5. минимум **36 русскоязычных message specimens** по разным состояниям и
   placements;
6. для каждого specimen: trigger, intent, CTA, почему этично, риск неверного
   прочтения и forbidden alternative;
7. рекомендации по лёгкой персонализации тона без размножения шаблонов и
   неравного давления.

Не превращай этот раздел в случайный рекламный copy deck. Все фразы должны быть
связаны с состоянием и фактическим результатом функции.

### 10. Wireframe-level placement map

Опиши структуру, не декоративный redesign:

- home Hero Talk;
- listing top and page end;
- Search states;
- event detail;
- action echo/toast/live region;
- `Для меня`;
- saved events;
- PWA/install offer;
- artifact hint/reveal.

Покажи mobile, desktop, keyboard/screen-reader и no-JS differences.

### 11. Event/data taxonomy

Предложи минимальные события и поля, разделяющие:

- onboarding eligibility/exposure;
- capability attempt/success/mastery;
- dismissal;
- feature error;
- saved value;
- retention handoff;
- promo exposure;
- artifact exposure/find;
- experiment assignment.

Укажи privacy-preserving aggregation, TTL, sampling, dedup/idempotency и
data-quality tests. Не предлагай raw permanent clickstream по умолчанию.

### 12. KPI framework

Раздели:

- first-value outcomes;
- competence outcomes;
- continuity/return outcomes;
- guardrails;
- diagnostics;
- metrics, которые нельзя оптимизировать.

Для каждой метрики дай definition, denominator, window, source, failure modes и
интерпретацию. Негативный user decision и permission denial не должны
автоматически считаться failure.

### 13. Experiment and validation plan

Нужны:

- A/A и instrumentation validation;
- randomization unit;
- treatment isolation;
- holdout;
- sample/MDE assumptions;
- sequential/low-traffic alternative;
- novelty-aware duration;
- SRM и contamination checks;
- qualitative task plan;
- accessibility testing;
- stop/kill rules;
- interpretation template при underpowered result.

Не предлагай один эксперимент, одновременно меняющий Hero, Search, reactions,
PWA и artifacts.

### 14. Privacy, ethics, accessibility and dark-pattern threat model

Для каждого риска:

```text
threat → affected user → mechanism → prevention → detection → rollback
```

Покрой:

- nagging и repeated prompts;
- deceptive consent;
- inferred identity/persona pressure;
- loss aversion/FOMO;
- inaccessible transient UI;
- motion/hover/precision target;
- accidental event navigation/action;
- local storage loss;
- cross-device merge;
- campaign disguised as help;
- artifact disguised as event property;
- child/family and sensitive-topic safeguards;
- outage/failure messaging.

### 15. Retention handoff contract

Определи:

- момент завершения onboarding по capability;
- минимальный передаваемый state;
- что retention не получает;
- utility vs editorial vs promo boundaries;
- consent/channel boundaries;
- reintroduction rules;
- lapsed user handling;
- cancellation/reschedule context.

### 16. Roadmap

`MVP / P1 / P2` с dependencies, owner decisions, research gates, implementation
slices, tests, rollout, rollback и explicit non-goals.

MVP должен быть минимальным: не предлагай сразу строить универсальную CRM,
real-time recommender, сложный journey builder и omnichannel automation.

### 17. Owner decisions и unresolved evidence

Список решений, которые нельзя тихо принять за владельца продукта. Для каждого:

- варианты;
- recommendation;
- evidence;
- reversibility;
- deadline/dependency.

### 18. Где исходный draft ошибается или недостаточно доказан

Минимум **8 конкретных пунктов**. Критикуй численные пороги, этапы, message
priority, activation hypothesis, роль артефактов, storage model, terminology и
assumptions о трафике. Не сохраняй предложение только потому, что оно уже
записано в draft.

### 19. Repository diff-plan

Без кода предложи точный diff-plan:

- какие разделы изменить в
  `docs/features/static-site-onboarding/README.md`;
- какие отдельные contracts/schemas нужны;
- какие существующие docs должны ссылаться на стратегию;
- какие старые/фокусные решения нельзя копировать;
- какие tests/scenarios зарегистрировать;
- какие вопросы оставить `BLOCKED`.

## 9. Формат ответа

1. Executive verdict — до одной страницы.
2. Репозиторный baseline и конфликтующие truths.
3. Evidence/literature synthesis.
4. Product strategy и journey.
5. Message/control architecture.
6. Copy framework и specimens.
7. Measurement/experiments.
8. Risks/privacy/accessibility.
9. Roadmap/diff-plan/owner decisions.
10. Приложение: evidence matrix и source list.

Используй таблицы там, где они действительно упрощают сравнение. Не скрывай
неопределённость за уверенным тоном. Каждое существенное внешнее утверждение
снабжай прямой ссылкой, датой доступа и оценкой evidence strength. Для
repository claims указывай path и exact SHA/ref.

## 10. Финальная проверка качества

Перед ответом проверь:

- не превратился ли onboarding в линейный тур;
- не стало ли больше сообщений, чем полезных действий;
- остаётся ли первая ценность без входа/настройки;
- отделены ли onboarding, retention, utility и promo;
- можно ли успешно отказаться от каждого upgrade;
- не объявлена ли гипотеза production behavior;
- не оптимизируется ли permission/install/profile creation;
- не используется ли focus-group incentive для обычной аудитории;
- не выдаётся ли артефакт за обязательное обучение;
- есть ли exact suppression/mastery rules;
- доступен ли keyboard/screen-reader/reduced-motion/no-JS path;
- указан ли вариант «не показывать подсказку»;
- есть ли отрицательные evidence и kill criteria;
- не скрыта ли недостаточная выборка.

Не изменяй код и repository. Результат должен быть пригоден для последующей
редакции canonical strategy и implementation planning.

## PROMPT END
