# Варианты стратегии онбоардинга стандартного пользователя

> **Статус:** decision record после консолидации Gemini, ChatGPT и каноники
> Hero-talk.  
> **Выбранный baseline:** вариант A.  
> **Связанные документы:** [каноническая onboarding strategy](README.md),
> [согласование с Hero-talk](hero-talk-alignment-2026-08-03.md),
> [синтез исследований](research/research-synthesis-2026-08-03.md).  
> **Hero-talk dependency:** `docs/features/hero-talk/README.md` в stacked PR
> [#291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291).

## 1. Что именно варьируется

Исследования не дают оснований выбирать между линейным туром и контекстным
онбордингом: тур отвергнут. После изучения каноники Hero-talk также нельзя
выбирать между «статическим Hero» и «настоящим Hero-talk» как между вариантами
онбординга.

Typed briefing, конечная cursor semantics, optional tile media и coherent
narrative chain — определяющая грамматика самого Hero-talk. Поэтому варианты A
и B различаются не наличием этой грамматики, а:

- интенсивностью и частотой программ;
- допустимой длиной цепочек;
- использованием cross-session thread state;
- глубиной return/personalized/editorial context;
- campaign integration;
- required Hero-talk release stage.

Онбординг во всех вариантах владеет capability eligibility, competency,
success, dismissal, mastery и suppression. Hero-talk владеет presentation,
chain graph, page context, phrase packs и served plan.

## 2. Сравнение

| Параметр | A. Сдержанный utility-first | B. Chain-rich contextual challenger | C. Культурный extension |
|---|---|---|---|
| Роль | Основная стратегия MVP | Challenger после доказанного baseline | Не onboarding MVP; post-release track |
| Hero-talk grammar | Полная каноническая grammar | Та же grammar | Может использовать `artifact_hint`, но не определяет Hero-talk |
| `home_hero` cold/unknown | Полезная static first scene; single/short generic chain | Та же первая сцена; richer nodes только после eligibility | Не используется для первой находки |
| Greeting/local identity | Допустимы bounded `Добрый день` / `«кеска»` families | Те же, плюс связные return/editorial bridges | Только если культурно релевантно и не маскирует artifact |
| Feature discovery | Одна capability, один CTA-path, редкая system-origin chain | То же правило, но richer context and cross-visit continuation | Core capability не обучается через artifact |
| Page-end | Один точный next step после value; exact page context | Bounded contextual chain, open loop/resolution | Один добровольный cultural invitation |
| Cross-session memory | Нет либо только технический terminal/suppression state | Bounded last/penultimate nodes, open loop, meaningful watermark | Отдельный bounded artifact ledger |
| Personalization | Generic/local factual explanation | Persona packs, explicit-interest overlay, return delta после activation | Artifact action не taste signal |
| Editorial/promo arcs | Нет campaign takeover; safe editorial single scene only after gates | Own editorial/campaign arcs after HT-7 and experiment | Отдельная cultural campaign |
| Action echo | Immediate local echo; Hero may only continue confirmed result | То же | Отдельно от artifact feedback |
| Login/PWA/permissions | Только после antecedent value и release/platform eligibility | То же | Не обязательны |
| State complexity | Низкая/средняя | Высокая, но bounded | Отдельный небольшой домен |
| Основной риск | Недостаточная заметность полезной функции | Banner blindness, narrative fatigue, false familiarity | CTA cannibalization, novelty и maintenance debt |
| Required Hero-talk stages | HT-1, HT-2, HT-4, HT-5 по включённым surfaces | HT-6, HT-7, HT-10 поверх A | Отдельный artifact gate плюс Hero-talk stage, если используется hint |

## 3. Общие неизменяемые правила A и B

1. Никаких tour, checklist, missions и глобального `onboarding_completed`.
2. Core facts, navigation и event CTA доступны до Hero-talk runtime/state.
3. First useful Hero fragment присутствует в static HTML.
4. Одна новая capability за один page journey; coherent chain может содержать
   несколько связанных nodes, но сохраняет один основной CTA-path.
5. Safety, lifecycle, direct error и immediate action result не вытесняются
   onboarding/editorial/campaign сценами.
6. Exposure — delivery diagnostic, не competence и не taste.
7. Immediate confirmation/Undo принадлежит action owner; Hero-talk не дублирует
   его и может продолжать только подтверждённый результат.
8. Dismissal/cooldown/mastery приходят из onboarding state и обязательны для
   Hero-talk compiler/runtime.
9. Golden-persona pack не меняет urgency, CTA, refusal, consent, caps, права или
   result claim.
10. Runtime не вызывает LLM; используются только заранее скомпилированные packs.
11. При JS/storage/profile failure остаётся полезная generic first scene либо
   статический fallback.
12. Hero-talk CTR не является primary product outcome.

## 4. Вариант A — сдержанный utility-first contextual onboarding

### 4.1. Назначение

Помочь человеку получить пользу и освоить нужное действие, используя
канонический Hero-talk без превращения сайта в непрерывный разговор или продажу
функций.

### 4.2. Базовый цикл

```text
useful static first scene
→ real discovery/event task
→ inline help only on observable difficulty
→ immediate action success/failure echo
→ optional one-capability Hero-talk continuation
→ where-to-find-result
→ mastery/suppression
```

### 4.3. `home_hero`

- текущий static `HomeHeroTalk` сохраняется только как rollback/fallback, а не
  как второй продукт рядом с Hero-talk;
- целевой renderer показывает полезную first scene сразу;
- chain допускается длины один либо короткая owner-reviewed последовательность;
- возможны greeting, local identity, today/weekend и service-orientation
  families из HT-1/HT-2;
- до activation нет persona packs, return delta и claims о пользовательских
  интересах;
- campaign/artifact не вытесняют узнаваемую ориентацию cold/unknown visitor;
- typed motion не задерживает H1, CTA и начало ленты.

### 4.4. Feature discovery

Кандидат создаёт onboarding capability engine:

```yaml
intent: feature_discovery
origin: system
capability_id: <released capability>
```

Hero-talk compiler может оформить его как coherent chain, но:

- продвигается только одна capability;
- один CTA ведёт к реальному released action/help surface;
- подсказка не появляется без observable relevance;
- success/mastery сразу подавляет базовый hint;
- `Что умеет сайт` остаётся постоянным manual help route.

### 4.5. Page-end

Для onboarding/continuity Page-end нужен antecedent value: event decision,
successful action, saved state, completed recovery или explicit Help.

Допустимы:

- открыть результат действия;
- показать, где найти сохранённое;
- продолжить по связанной дате/подборке;
- объяснить factual local rerank;
- после release — предложить PWA/identity/reminder.

Общий editorial/event-context Page-end принадлежит Hero-talk и может быть
eligible по exact page context без onboarding capability, но не маскируется под
обучение.

### 4.6. MVP capabilities

- date/route orientation — постоянный IA, без prompt;
- event facts — content hierarchy, не coachmark;
- `Не интересно` — exact local echo + Undo;
- like — точная реакция, не «сохранение»;
- share — различать share-sheet invocation и copy success;
- calendar/ICS — только после owner decision по семантике;
- Search recovery — только после production acceptance Search;
- Hero-talk feature delivery — только после HT-5.

### 4.7. Почему выбран

- сохраняет static-first и no-JS value;
- соответствует канонической Hero-talk grammar, не создавая второй renderer;
- ограничивает narrative burden и state complexity;
- использует существующие live regions для action echo;
- легче проверяется при небольшом трафике;
- создаёт чистый baseline для richer chains.

### 4.8. Kill/rollback

Независимо отключается любая message family/chain, если:

- ухудшается first-value time или core CTA;
- появляется после permanent dismissal/mastery;
- обещает не выпущенный результат;
- нарушает one-capability/one-CTA-path rule;
- создаёт duplicate announcement или focus obstruction;
- неверно продолжает старый thread/open loop;
- требует недоступный backend/platform capability;
- не выдерживает novelty-aware guardrails.

## 5. Вариант B — chain-rich contextual challenger

### 5.1. Назначение

Расширить уже работающий вариант A более содержательными bounded return,
personalized, editorial и campaign arcs. Это не «включение narrative mode»:
chain-first grammar уже присутствует в A.

### 5.2. Дополнительные возможности

- `home_hero` продолжает нить прошлого и позапрошлого meaningful visit;
- bounded open loops и resolutions;
- `Пока вас не было` с единым `served_delta_id` для count и destination;
- finite Golden-persona packs и explicit-interest overlay после activation;
- event/festival/club/program context;
- собственные editorial/promo campaigns как кандидаты общего compiler;
- richer Page-end bridges и cross-page/cross-session arcs;
- один допустимый curiosity hook по каноническому ограничению.

### 5.3. Отдельный thread state

Hero-talk хранит только bounded доказуемое состояние, например:

```json
{
  "thread_id": "ht_...",
  "last_node_ids": ["n3", "n2"],
  "open_loop_id": "festival-kantata-education",
  "last_action": "opened_event",
  "expires_at": "..."
}
```

Это не onboarding competency, не taste profile и не полный текст разговора.
Потеря thread state ведёт к generic first scene, а не к выдуманному
«продолжению».

### 5.4. Дополнительные риски

- Hero-talk становится баннером или сериализацией ради самой сериализации;
- bounded memory ошибочно выглядит как «сайт всё помнит»;
- persona pack создаёт creepy/familiar pressure;
- campaign arc маскируется под system help;
- open loop продолжается после изменения lifecycle/catalog;
- chain compiler увеличивает editorial, QA и data-contract debt;
- return delta count расходится с destination.

### 5.5. Условия допуска

Вариант B допускается только если:

1. вариант A и соответствующие HT stages имеют стабильный baseline;
2. HT-6/HT-7 contracts реализованы и протестированы;
3. thread/watermark/served-delta имеют exact evidence;
4. owner принимает конкретные chain fixtures;
5. purpose/source disclosure не смешиваются;
6. treatment non-inferior по event discovery и core CTA;
7. Day 7/14 novelty/fatigue review выполнен;
8. существует независимый kill switch для program/chain family;
9. causal rollout проходит HT-10.

## 6. Слой C — культурное исследование

### 6.1. Статус

Это не вариант первичного онбординга. Он подключается после core value и может
быть полностью отклонён без изменения A/B.

### 6.2. Допустимые формы

#### C1. Мини-коллекция

- одна тема;
- небольшой bounded set;
- первая подсказка точная;
- progress локальный или отдельный ledger;
- нет login, prize, streak, share advantage;
- provenance, rights, freshness и archive state обязательны.

#### C2. Статический культурный маршрут

Обычная редакционная страница/подборка без hidden placements, progress и state.
Это наиболее дешёвый и доступный способ проверить культурную ценность без
gamification debt.

#### C3. After-action декоративный объект

Появляется после подтверждённого core action, но не является наградой, не
считается progress и не влияет на профиль. Допустим только после проверки, что
не отвлекает от result echo.

### 6.3. Не допускается

- использовать находку как prerequisite функции;
- маскировать artifact под event badge/quality mark;
- считать find/completion activation или mastery;
- выводить интерес к теме из находки;
- требовать hover, motion, точное наведение или второй экран;
- использовать focus-group prize model;
- ставить opaque clue вместо первого понятного пути.

## 7. Решение владельцу

### Уже принято

**Вариант A** остаётся каноническим onboarding baseline, но теперь определяется
как сдержанная программа внутри настоящего Hero-talk, а не отказ от его typed и
chain-first grammar.

### Отложено

**Вариант B** — более богатые cross-session/personal/editorial/campaign arcs,
а не сам факт narrative chains. Он возвращается к owner review после HT-6,
HT-7 и evidence HT-10.

**Слой C** не блокирует onboarding MVP.

### Решения, которые нужны независимо от варианта

1. Семантика `like` / `favorite_saved` / `calendar_saved` / ICS и имя
   `Моё`/`Мои события`.
2. Какие capabilities действительно release-ready.
3. Какой action-result copy подтверждается current implementation.
4. Когда PWA/identity/reminder имеют доказанный antecedent value.
5. Какой legal/data contract применим к target personalization.
6. Какая Hero-talk first-scene/service-orientation family принимается в HT-1.
7. Какие short chains входят в A, а какие требуют B/HT-6/HT-7.
8. Имеет ли cultural layer самостоятельную ценность помимо novelty.
