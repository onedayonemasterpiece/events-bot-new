# Варианты стратегии онбординга стандартного пользователя

> **Статус:** owner-corrected decision record v0.4 после консолидации
> исследований и каноники Hero-talk.  
> **Выбранный baseline:** вариант A.  
> **Связанные документы:** [каноническая onboarding strategy](README.md),
> [согласование с Hero-talk](hero-talk-alignment-2026-08-03.md),
> [каноника Hero-talk](../hero-talk/README.md).  
> **Ключевая owner correction:** артефакты-пасхалки входят в стандартный
> онбординг и связаны с Клубом друзей Анонсов и регулярными розыгрышами билетов.

## 1. Что именно варьируется

Линейный tour, tutorial wall и checklist отвергнуты. Также не существует выбора
между «статическим Hero» и «настоящим Hero-talk»: semantic typed briefing,
конечный курсор, optional tile media и coherent chain являются грамматикой
самого Hero-talk.

После owner correction больше нет отдельного «культурного варианта C».
Артефактный контур присутствует и в A, и в B. Варианты различаются:

- интенсивностью и частотой Hero-talk programmes;
- длиной и памятью цепочек;
- глубиной return/personal/editorial context;
- способом продолжения artifact/club narrative между визитами;
- уровнем campaign automation;
- required Hero-talk release stage.

## 2. Общая продуктовая модель

```text
базовая event value
→ контекстное освоение полезных действий
→ первая точная подсказка об артефакте
→ находка и коллекция
→ дальнейшее самостоятельное исследование
→ Клуб друзей Анонсов
→ регулярные розыгрыши билетов по опубликованным правилам
```

Артефакты являются частью onboarding и product identity, но не admission wall:
без них доступны event facts, навигация, поиск, сохранение, календарь и внешние
CTA.

Focus-group leaderboard, NPS/feedback scoring и research missions не
переносятся. Стандартный club/raffle programme — отдельная product mechanism и
не запрещается этим правилом.

## 3. Сравнение вариантов

| Параметр | A. Сдержанный contextual baseline | B. Chain-rich contextual challenger |
|---|---|---|
| Роль | Основная стратегия запуска | Расширение после доказанного baseline |
| Hero-talk grammar | Полная каноническая grammar | Та же grammar |
| Cold/unknown home | Полезная generic static first scene; short owner-reviewed chain | Та же first scene; richer nodes только после eligibility |
| Текущий `HomeHeroTalk` | Текущая реализация и migration donor; не rollback contract | То же |
| Utility feature discovery | Одна capability/одна задача/один CTA-path | То же правило, но richer context и continuation |
| Артефакты | Входят: точная первая подсказка, доступная находка, коллекция | Входят: серии, open loops, return delta, campaign continuity |
| Клуб друзей Анонсов | Тихий handoff после реального artifact progress или explicit interest | Bounded cross-session club narrative и персональный статус после release |
| Розыгрыши билетов | Объясняются только после rules/application release; без ложных обещаний | То же; дополнительно contextual return/status chains |
| Page-end | Один точный next step после value | Bounded contextual chain с open-loop/resolution |
| Cross-session memory | Минимум: suppression, collection state, factual result | Bounded last/penultimate nodes, open loop, meaningful watermark |
| Personalization | Generic/local factual explanation | Persona packs и explicit-interest overlay после activation |
| Editorial/campaign arcs | Редкие owner-reviewed programmes | Отдельные bounded programmes после HT-7 |
| State complexity | Низкая/средняя | Выше, но bounded и versioned |
| Основной риск | Недостаточная заметность полезных механик | Narrative fatigue, false familiarity, stale open loops |
| Required stages | HT-1/2/4/5 + artifact collection gate | HT-6/7/10 поверх A |

## 4. Общие неизменяемые правила A и B

1. Нет tour, tutorial wall, checklist и глобального `onboarding_completed`.
2. Core event value доступна до login, PWA, permissions, артефактов и клуба.
3. Первый полезный Hero fragment присутствует в static HTML.
4. За page journey продвигается одна новая смысловая задача и один основной
   CTA-path; chain может содержать несколько связанных nodes.
5. Safety, lifecycle, direct error и immediate action result выше onboarding,
   artifact, club, editorial и promo messages.
6. Exposure — delivery diagnostic, не competence, find, taste или consent.
7. Immediate confirmation/Undo принадлежит action owner.
8. Dismissal/cooldown/mastery обязательны для Hero-talk compiler/runtime.
9. Persona pack не меняет urgency, CTA, refusal, consent, odds или права.
10. Runtime не вызывает LLM; используются immutable precompiled packs.
11. JS/storage/profile failure сохраняет generic static first scene и core UI.
12. Hero-talk CTR и artifact completion не являются North Star.
13. Artifact find не становится preference signal.
14. Artifact threshold не подаёт raffle application автоматически.
15. Club membership не означает consent на promo.
16. Основные CTA события не маскируются под артефакт или розыгрыш.

## 5. Вариант A — сдержанный contextual onboarding

### 5.1. Назначение

Помочь человеку быстро получить event value, освоить нужные действия и узнать
характер сервиса через первый артефакт, не превращая сайт в непрерывный разговор
или игру с обязательным прогрессом.

### 5.2. Базовый цикл

```text
useful static first scene
→ real discovery/event task
→ inline help only on observable difficulty
→ exact action success/failure echo
→ optional one-capability Hero-talk continuation
→ first-artifact invitation when contextually eligible
→ find/story/collection
→ suppression or next independent task
```

### 5.3. `home_hero`

- текущий `HomeHeroTalk.astro` — current implementation и donor для migration;
- целевой renderer сам содержит полезную static first scene;
- fallback/rollback реализуется generic served plan и kill switch внутри нового
  механизма, а не возвратом к legacy-компоненту;
- chain допускается длины один либо короткая owner-reviewed sequence;
- greeting, local identity, today/weekend, service orientation и точный
  `artifact_hint` допустимы по eligibility;
- typed motion не задерживает H1, CTA и начало ленты;
- cold/unknown user не получает сразу одновременно Search, PWA, club и raffle.

### 5.4. Utility feature discovery

Кандидат создаёт onboarding capability engine:

```yaml
intent: feature_discovery
origin: system
capability_id: <released capability>
```

Правила:

- продвигается одна capability;
- CTA ведёт к released action/help surface;
- success/mastery подавляет базовый hint;
- permanent help surface `Что умеет сайт` доступен независимо от prompts.

### 5.5. Первый артефакт

Первый артефакт — отдельная onboarding capability/product arc:

```yaml
intent: artifact_hint
origin: system | editorial_program
collection_id: <active collection>
artifact_id: <first eligible artifact>
```

Требования:

- сначала человек видит полезный сайт, но не обязан достигать абстрактного
  `mastered core`;
- первая подсказка точная: page family и понятное место;
- находка стабильна и доступна touch/keyboard/screen reader;
- после find показываются точный echo, история и ссылка на коллекцию;
- последующие подсказки могут быть менее прямыми;
- progress не блокирует афишу и не влияет на taste profile;
- пользователь может скрыть текущую artifact campaign.

### 5.6. Клуб и регулярные розыгрыши

Связь является частью стратегии, но exact rules не выдумываются.

```text
artifact progress / explicit club interest
→ factual club invitation
→ published membership/rules surface
→ explicit application or membership action
→ regular raffle lifecycle
```

До release запрещено говорить:

- что пользователь уже участвует;
- что threshold автоматически подал заявку;
- что собранная сверх threshold коллекция повышает шансы;
- сколько билетов, когда draw и на какие события, если это не опубликовано;
- что login или promo consent обязательны для самой афиши.

Текущий artifact collection contract с threshold/application является исходным
документом для согласования с моделью Клуба друзей Анонсов, а не основанием
вынести весь контур за onboarding.

### 5.7. Page-end

Допустимы:

- открыть результат действия;
- показать, где найти сохранённое;
- продолжить exact event/festival/club context;
- предложить точную подсказку первого артефакта;
- открыть коллекцию после find;
- объяснить клубный handoff после подтверждённого progress;
- после release предложить PWA/identity/reminder.

Один scroll не создаёт eligibility. Artifact hint может использовать page
context, но не перекрывает canonical recommendations или transactional CTA.

### 5.8. Почему выбран

- сохраняет static-first и event-first value;
- соответствует канонической Hero-talk grammar;
- не создаёт второй renderer;
- включает ключевую локальную механику, а не откладывает её «на потом»;
- отделяет artifact/club/raffle state от taste и utility state;
- легче проверяется при небольшом трафике;
- даёт чистый baseline для richer narrative.

## 6. Вариант B — chain-rich contextual challenger

### 6.1. Назначение

Расширить A более содержательными bounded return, personal, editorial,
artifact-series и club arcs. Это не включение narrative mode: chain-first уже
есть в A.

### 6.2. Дополнительные возможности

- продолжение нити прошлого и позапрошлого meaningful visit;
- bounded open loops и resolutions;
- `Пока вас не было` с единым `served_delta_id`;
- finite Golden-persona packs после activation;
- event/festival/club/program context;
- серии коллекций и главы artifact campaign;
- точный return status по коллекции, заявке и клубу после release;
- own editorial/campaign programmes;
- richer page-end bridges и cross-session arcs.

### 6.3. Допустимый artifact/club thread

```text
В прошлый раз вы нашли янтарного космонавта.
→ В коллекции открылась новая морская глава.
→ Показать доступную подсказку.
```

После rules release:

```text
Для заявки достаточно ещё одной находки.
→ Открыть коллекцию и правила.
```

Count, threshold, deadline и destination должны строиться из одного immutable
served snapshot. При stale rules, suspended campaign или storage/identity
uncertainty используется generic collection scene без персонального claim.

### 6.4. Условия допуска

B допускается только если:

1. вариант A имеет стабильный baseline;
2. HT-6/HT-7 contracts реализованы;
3. artifact ledger и club/raffle state имеют release truth;
4. thread/watermark/delta evidence точны;
5. owner принимает concrete chain fixtures;
6. purpose/source disclosure не смешиваются;
7. treatment non-inferior по event discovery и core CTA;
8. Day 7/14 novelty/fatigue review выполнен;
9. существуют независимые kill switches;
10. causal rollout проходит HT-10.

## 7. Kill/rollback

Отключается конкретная message/program family, если она:

- ухудшает first-value time или core CTA;
- появляется после permanent dismissal/mastery;
- обещает не выпущенный result или raffle status;
- нарушает one-task/one-CTA-path rule;
- создаёт duplicate announcement или focus obstruction;
- продолжает stale artifact/club thread;
- требует недоступный backend/platform capability;
- делает artifact похожим на event badge или рекламу;
- скрывает равноправный отказ;
- загрязняет taste profile;
- не выдерживает accessibility или novelty-aware guardrails.

Откат Hero-talk означает переход к generic static served plan. Он не означает
архитектурный возврат к legacy `HomeHeroTalk` как отдельному продукту.

## 8. Решение владельца

Принято:

- baseline — вариант A;
- артефакты-пасхалки входят в стандартный onboarding scope;
- первый артефакт должен быть явно и доступно объяснён;
- артефакты связаны с Клубом друзей Анонсов;
- клуб предусматривает регулярные розыгрыши билетов;
- focus-group-specific scoring не переносится;
- variant C отменён;
- `HomeHeroTalk` не является отдельным rollback contract.

Требуют отдельной спецификации, но не блокируют включение механики в стратегию:

- membership contract Клуба друзей Анонсов;
- frequency и exact rules регулярных draws;
- prize source/quantity/claim;
- relationship между collection threshold, application и membership;
- durable ledger, identity, privacy, legal и anti-abuse;
- release order первой публичной коллекции и первого raffle.
