# Онбординг стандартного пользователя KenigEvents

> **Статус:** owner-corrected canonical strategy v0.4 / product decision; не
> описание уже выпущенного production behavior.
> **Дата:** 2026-08-03.
> **Проверенный research baseline:**
> `main@09fcde9012b30d0c3b4a30d35f45e3c9858b096c`; актуальный release truth
> проверяется отдельно перед implementation.
> **Аудитория:** обычный новый или вернувшийся посетитель; не участник
> фокус-группы.
> **Выбранный вариант:**
> [A — сдержанный contextual onboarding](strategy-options.md).
> **Hero-talk dependency:** [канонический Hero-talk](../hero-talk/README.md).
> **Артефакты:** [каноника пасхалок и коллекций](../static-site-easter-eggs/README.md).
> **Корректировка v0.3 → v0.4:**
> [Hero-talk, артефакты и Клуб друзей Анонсов](hero-talk-alignment-2026-08-03.md).

## 1. Итоговое решение

KenigEvents не нужен обязательный тур, welcome-wall, tutorial checklist,
missions или единый процент «завершения онбординга». Нужен контекстный способ
помочь человеку:

1. быстро получить пользу от афиши;
2. освоить конкретное действие тогда, когда оно понадобилось;
3. познакомиться с локальным характером сервиса через артефакты-пасхалки;
4. увидеть коллекцию и понять, как она связана с Клубом друзей Анонсов;
5. после собственного release gate добровольно участвовать в регулярных
   розыгрышах билетов по опубликованным правилам.

Базовый путь:

```text
доступная афиша и понятные события
→ осознанное решение по событию
→ помощь или точный результат действия
→ первая точная подсказка об артефакте
→ находка, история и коллекция
→ дальнейшее самостоятельное исследование
→ Клуб друзей Анонсов
→ регулярный raffle lifecycle по опубликованным правилам
```

Артефакты входят в стандартный onboarding scope. Они не должны становиться
admission wall: без находок, коллекции и клубного статуса по-прежнему доступны
факты событий, навигация, поиск, сохранение, календарь и переходы к
организаторам.

## 2. Две отменённые ошибки v0.3

### 2.1. `HomeHeroTalk` не является rollback contract

Текущий `HomeHeroTalk.astro` — фактическая статическая реализация Hero-зоны и
исходная точка миграции. Он не фиксируется как отдельный постоянный rollback
будущего Hero-talk.

Правильная миграция:

```text
current HomeHeroTalk.astro
→ shared Hero-talk renderer
→ useful static first scene внутри renderer
→ optional precompiled contextual chain
```

При сбое compiler, media, profile state или JavaScript новый механизм показывает
собственную generic static first scene. Откат — это kill switch или выбор
generic served plan внутри Hero-talk, а не архитектурный возврат к legacy
компоненту.

### 2.2. Нет отдельного «культурного слоя C» вне онбординга

Термин «культурные артефакты» в v0.3 был неточным. Речь идёт о существующей
механике **артефактов-пасхалок Калининградской области**.

Канонический Hero-talk уже содержит:

- intent `artifact_hint`;
- intent `club_discovery`;
- onboarding-сценарий `Первый артефакт`;
- placements `home_hero`, `collection_page_end` и `club_page_end`.

В коде и design tracks уже существуют `/artefakty/`, коллекция, локальное
состояние первого артефакта, placement в ленте и более полный registry/collection
prototype. Поэтому механика не классифицируется как необязательное приложение,
которое можно выкинуть без изменения онбординга.

## 3. Нормативная иерархия

При конфликте, в порядке убывания приоритета:

1. актуальные явные решения владельца продукта;
2. фактический код и release truth целевой ветки/сборки;
3. каноническая продуктовая модель Hero-talk;
4. каноника артефактов и коллекций;
5. эта стратегия;
6. критическая консолидация исследований;
7. исходные исследования и исторические прототипы.

Исследование может рекомендовать defer или reject, но не отменяет явное
последующее решение владельца и уже принятую продуктовую механику.

## 4. Граница ответственности

### 4.1. Onboarding strategy

Владеет:

- реестром capabilities и onboarding arcs;
- eligibility по release/route/platform/user state;
- competency, success/failure evidence;
- dismissal, cooldown, suppression и reintroduction;
- правдивостью message/result claim;
- переходом от utility onboarding к artifact/club onboarding;
- запретом taste pollution;
- KPI и guardrails.

### 4.2. Hero-talk

Владеет:

- placements `home_hero` и `*_page_end`;
- semantic typed briefing;
- конечной cursor semantics;
- optional square-tile media;
- coherent narrative graph, nodes и bridges;
- точным page/event/festival/collection/club context;
- bounded cross-session thread state;
- phrase packs и immutable served plan;
- chain-level editorial quality и frequency.

Hero-talk не является отдельным tour engine и не вызывает LLM в runtime.

### 4.3. Владелец конкретного действия

Владеет:

- фактической операцией;
- local error/recovery;
- immediate action echo;
- Undo;
- ambiguous timeout handling;
- доступным status/alert;
- reconciliation и alternate path.

Hero-talk может продолжить подтверждённый результат, но не дублирует и не
переобещает его.

### 4.4. Artifact programme

Владеет:

- `artifact_id`, collection membership и version;
- placement bundle и доступными equivalent anchors;
- find receipt, hint state и collection progress;
- provenance, rights, freshness и archive;
- campaign window, suspension и relocation;
- отдельным artifact ledger.

### 4.5. Клуб друзей Анонсов

Владеет:

- club identity и membership lifecycle;
- связью с active artifact collections;
- регулярными ticket-raffle programmes;
- eligibility/application state;
- published rules, draw snapshot и selection;
- prize/claim/alternate/cancellation lifecycle;
- privacy, legal и anti-abuse contract.

Стратегия фиксирует связь артефактов с клубом и розыгрышами, но не выдумывает
неутверждённые количества билетов, частоту, географию, возраст, selection method
или claim procedure.

## 5. Пользовательский результат

Нет одного глобального `onboarding_completed`.

```text
FV1 — qualified_event_understanding
      человек получил достаточно фактов для решения;

FV2 — explicit_event_decision
      save/calendar/hide/like/share/ticket intent или осознанное продолжение;

C1  — capability_success
      конкретное действие завершилось и результат показан;

A1  — artifact_mechanic_understood
      человек понял точную первую подсказку и доступный способ находки;

A2  — artifact_find_and_story
      находка подтверждена, история/коллекция доступны;

A3  — collection_continuity
      прогресс найден и понятен на следующей поверхности/визите;

CL1 — club_handoff
      пользователь добровольно открыл клубные правила или membership surface;

R1  — qualified_return
      следующий визит снова привёл к event value, коллекции или клубному
      действию без вытеснения основной афиши.
```

Ограничения:

- artifact find не равен mastery Search/save/date navigation;
- artifact completion не является общей activation North Star;
- club membership не равен marketing consent;
- threshold не подаёт raffle application автоматически;
- raffle application не равна выигрышу;
- exposure не равен пониманию, находке, preference или consent.

## 6. Продуктовые инварианты

1. **Static-first event value.** Facts, links, navigation и useful Hero first
   scene не ждут profile, analytics, artifacts или club state.
2. **No admission wall.** Tour, login, email, PWA, permissions и collection
   progress не стоят перед афишей.
3. **Artifacts are onboarding.** Первый артефакт и объяснение коллекции входят в
   стандартное освоение продукта.
4. **One task per chain.** За page journey продвигается одна новая смысловая
   задача и один основной CTA-path.
5. **Exact first clue.** Первая artifact hint называет понятную page family и
   доступное место; opaque challenge появляется только позже.
6. **Stable placement.** Артефакт не reroll-ится от reload, card reorder,
   dislike, viewport или запроса подсказки.
7. **Accessibility parity.** Touch, keyboard и screen reader имеют
   эквивалентный путь; motion/hover/precision/second device не обязательны.
8. **Locality first.** Error, recovery, result и Undo показываются рядом с
   действием, если это возможно.
9. **Truthful result.** Copy описывает только подтверждённый current release.
10. **Separate state domains.** Core competency, Hero thread, artifact,
    membership, raffle, taste и consent не склеиваются.
11. **No taste pollution.** Hero exposure, artifact find, collection completion
    и club click не становятся автоматически preference signal.
12. **Dismissal is valid.** Можно закрыть подсказку, скрыть campaign, отказаться
    от клуба или raffle без наказания и nagging.
13. **No hidden odds.** Скорость, share, like, покупка или отсутствие hints не
    меняют шансы без прямого опубликованного правила.
14. **No event disguise.** Артефакт не выглядит как event fact, recommendation
    badge, organizer mark или paid placement.
15. **No runtime LLM.** Runtime выбирает готовый static served plan.
16. **Bounded memory.** Hero-talk не хранит полный «разговор»; artifact/club
    state versioned и минимален.

## 7. Фактическая отправная точка

### 7.1. Inert route context/placement boundary in code

`site/src/lib/onboarding/standard-placement-context.ts` is the released typed
boundary between route families and a single `page_end` placement slot.
`StandardOnboardingPlacementContext.astro` emits one hidden, script-free marker
after page content in `EventLayout`; it performs no network/storage operation,
does not select copy, and does not alter onboarding behavior. The closed context
vocabulary is `home | listing | event_detail | search | personal | information`.

The marker explicitly keeps `artifactProgram`, `clubProgram`, and
`raffleProgram` at `disabled`. This is an inventory/placement seam, not a release
of artifact hints, club membership, applications, prizes, or draw claims.
`/fokus-gruppa/**` and `/zakrytaya-afisha/` remain a separate research product
and must not contain this standard-onboarding runtime. Generated page-runtime
inventory checks one inert marker/context/slot on each eligible standard HTML
route, zero markers on the separate focus product, and fails if any gated
programme is enabled. Labs and non-HTML JSON/ICS/SW/manifest outputs retain their
explicit inventory exclusions.

| Область | Release truth | Следствие |
|---|---|---|
| Home | `HomeHeroTalk → HomeQuickNav → HomeColdStartFeed` | Первая ценность возможна без welcome wall |
| Current `HomeHeroTalk.astro` | Статический component; нет compiler/thread/page-end | Current implementation и migration donor, не rollback contract |
| Canonical Hero-talk | Docs/release-design track; runtime gated | Delivery grammar берётся из каноники, но не выдаётся за released runtime |
| Event actions | Like, `Не интересно`, share, calendar affordances | Utility learning остаётся рядом с действием |
| Search | Point-of-intent auth и production acceptance gates | Не обещать полностью выпущенный anonymous smart search |
| Artifacts in current code | `/artefakty/`, local collection, `AmberRailArtifact`, non-production flag | Prototype уже встроен в surfaces, но production release остаётся gated |
| Artifact registry branch | Public registry/collection prototype; target `8`, threshold `5 из 8` для explicit application | Reward-enabled collection — реальный product track, не внешний cultural appendix |
| Club/regular raffles | Явное owner decision; полный membership/rules contract ещё требуется | Связь включается в onboarding architecture сейчас, exact claims — после release |
| Focus group | Отдельный research cohort и отдельный prize/scoring contract | Research scoring не копируется standard user |
| PWA/identity/reminders | Release/platform dependent | Предлагать только после antecedent value и own gates |

## 8. Journey model

### 8.1. Home cold start

```text
Hero-talk / home_hero:
useful generic first scene
→ greeting/local identity/current context when eligible
→ Today / Search / event CTA
→ HomeQuickNav
→ cold-start feed
```

Правила:

- first scene находится в static HTML;
- typed motion не задерживает CTA;
- до first value нет одновременного продвижения Search, PWA, artifacts и club;
- persona/return claims не используются без evidence;
- reduced-motion/no-JS получают полный статический смысл.

### 8.2. Date-led/listing journey

Date controls и названия страниц — основное обучение. Первый artifact может быть
размещён в стабильной точке ленты, если:

- event cards и navigation уже доступны;
- marker не выглядит как событие;
- exact accessible label объясняет действие;
- hint/placement не вытесняет event CTA;
- collection campaign active и rules version известна.

### 8.3. Direct event deep link

Сначала facts/lifecycle и event CTA. Затем action echo и canonical continuation.
Artifact marker или hint допустим только в non-conflicting stable slot и не
подменяет свойство события.

### 8.4. Первый artifact onboarding

```text
contextually eligible first hint
→ «На сайте спрятан первый артефакт»
→ точная page/placement clue
→ touch/keyboard/screen-reader find
→ «Найден артефакт …»
→ открыть source-grounded историю
→ открыть коллекцию
```

Success evidence:

- exact artifact action произошёл;
- find записан идемпотентно;
- story/collection доступны;
- first hint suppresses or changes state;
- no duplicate find/announcement.

### 8.5. Следующие находки

После A1/A2 подсказки могут быть менее прямыми. Пользователь сам запрашивает
hint; proactive hints редки и bounded. Collection progress не сгорает, found
marker остаётся спокойным и доступным.

### 8.6. Collection → Friends Club

```text
confirmed artifact progress
→ collection surface
→ factual threshold/status
→ optional club discovery
→ published membership/rules surface
```

Клуб не продвигается только по scroll. Нужны artifact progress, explicit
interest или открытая collection surface.

### 8.7. Club → regular raffle

После фактического rules/application release:

```text
published raffle window
→ eligibility explanation
→ explicit application/membership action
→ acknowledgement
→ immutable eligible snapshot
→ draw/status/claim
```

До release Hero-talk не говорит `Вы участвуете`, `Вы допущены`, `Осталась одна
находка` или `Розыгрыш состоится …`, если exact data недоступны.

### 8.8. Returning visitor

При валидном local/server state допустимы:

- factual return delta;
- продолжение artifact series;
- collection status;
- club/raffle status после release;
- lifecycle change сохранённых событий.

При потере state интерфейс fail quiet: generic first scene и честный collection
empty/unknown state, а не выдуманное продолжение.

## 9. Competency и state domains

### 9.1. Core capability state

```text
unknown → eligible → exposed → attempted → succeeded → repeated → mastered
```

Дополнительные состояния:

```text
dismissed_until
dismissed_permanently
failed_recoverable
blocked_dependency
deprecated
needs_reintroduction(version_delta)
```

### 9.2. Artifact state

```text
latent
→ hint_eligible
→ hint_exposed
→ discovered
→ found
→ story_opened
→ collection_opened
→ hidden | archived
```

`hint_exposed` не равен `found`. Find принадлежит конкретному `artifact_id`,
collection version и placement receipt.

### 9.3. Collection state

```text
DRAFT → SCHEDULED → COLLECTING → APPLICATION_GRACE
→ DRAW_LOCKED → CLAIM → CLOSED → ARCHIVED
```

Точные states и threshold/application semantics берутся из artifact collection
contract и согласуются с Friends Club rules до production.

### 9.4. Club state

Предлагаемый минимальный domain:

```text
unknown
→ discovery_eligible
→ rules_viewed
→ application_started | membership_started
→ member | declined | blocked_dependency
→ suspended | left
```

Это schema proposal, не утверждение о выпущенном backend.

### 9.5. Raffle state

```text
not_open
→ rules_published
→ application_eligible
→ application_submitted
→ snapshot_locked
→ selected | not_selected | alternate
→ claimed | expired | cancelled
```

Membership, application и draw state не выводятся из browser exposure.

## 10. Capability/onboarding registry baseline

| Capability/arc | Когда допустимо | Surface | Success evidence | Release boundary |
|---|---|---|---|---|
| Today/Tomorrow/Weekend | Route существует | Persistent IA / generic Hero | Route open | Current static routes |
| Event facts/lifecycle | Event detail | Content hierarchy | Correct task outcome | Current event pages |
| Like | Released handler | Label + echo | State toggled | Не называть save |
| `Не интересно` | Reversible behavior | Inline echo + Undo | Hide/downrank + Undo | Copy совпадает со scope |
| Share | API/copy available | Action/feature hint | Invocation/copy success separately | Не заявлять отправку |
| Calendar/save | После semantic decision | Action + exact echo | Durable save/download distinct | Acceptance gate |
| Search | После acceptance | Inline/help/Hero candidate | Valid request/result/refinement | BLOCKED acceptance |
| First artifact | Active collection + stable accessible placement | Hero/page-end/inline hint | Find + story/collection | Artifact release gate |
| Artifact collection | At least one find or explicit open | `/artefakty/`, `Для меня`, page-end | Progress retrieved | Ledger/version gate |
| Friends Club | Artifact progress or explicit interest + released rules | collection/club page-end | Rules viewed/action begun | Membership contract |
| Regular raffle | Published active programme | club/collection/status surfaces | Explicit submitted application | Rules/legal/draw backend |
| PWA | Browser eligibility + meaningful use | Page-end | `appinstalled`/decline | Platform gate |
| Identity sync | Durable local value + merge release | Page-end/settings | Auth + idempotent merge | BLOCKED linking |
| Utility reminder | Saved event + released delivery | Saved settings/page-end | Purpose choice saved | BLOCKED delivery |
| Promo | Никогда не onboarding | Separate campaign settings | Separate opt-in | Separate consent |

## 11. Интеграция с Hero-talk

### 11.1. Utility candidate

```yaml
intent: feature_discovery
origin: system
capability_id: event-share
capability_version: v1
eligibility_receipt: <bounded evidence>
success_contract: <capability-specific>
suppression_contract: <capability-specific>
```

### 11.2. Artifact candidate

```yaml
intent: artifact_hint
origin: system | editorial_program
collection_id: <active collection>
collection_version: <version>
artifact_id: <eligible artifact>
placement_bundle_id: <stable bundle>
hint_level: exact | guided | opaque
```

Для первого артефакта `hint_level=exact`.

### 11.3. Club candidate

```yaml
intent: club_discovery
origin: system
antecedent: artifact_progress | explicit_interest
club_program_version: <released version>
```

### 11.4. Raffle candidate

```yaml
intent: club_discovery
origin: lifecycle
raffle_program_id: <released programme>
rules_version: <published version>
served_status_snapshot: <immutable factual snapshot>
```

Raffle message исключается, если rules/status нельзя доказать.

### 11.5. Примеры цепочек

Первый артефакт:

```text
На сайте спрятан первый артефакт.
→ Подсказка: начните со страницы выходных.
→ Найденные истории соберутся в коллекции.
```

После находки:

```text
Янтарный космонавт найден.
→ Его история уже в коллекции.
→ Открыть коллекцию.
```

После release клубных правил:

```text
В коллекции уже пять находок.
→ Этого достаточно, чтобы открыть добровольную заявку.
→ Условия — в Клубе друзей Анонсов.
```

Последняя chain запрещена до exact threshold/rules/application release.

## 12. Варианты стратегии

### A — выбранный baseline

- полноценная grammar Hero-talk;
- useful static first scene;
- conservative single/short owner-reviewed chains;
- utility-first contextual help;
- первый artifact onboarding и collection handoff;
- Friends Club discovery после progress/explicit interest;
- raffle copy только после release;
- minimal bounded state;
- independent kill switches.

### B — challenger

Поверх A:

- bounded cross-session threads;
- return delta;
- artifact-series/open-loop/resolution;
- richer festival/collection/club context;
- finite persona packs после activation;
- editorial/campaign arcs;
- factual collection/application/raffle status chains;
- HT-6/HT-7/HT-10 gates.

Отдельного варианта C нет.

## 13. Focus group boundary

Не переносятся обычному пользователю:

- задания, обязательная обратная связь и исследовательские missions;
- баллы за NPS, feedback, likes/dislikes, Search или page-family breadth;
- leaderboard research cohort;
- advantage за количество кликов;
- обязанность тестировать сайт ради допуска к призу.

Но это ограничение не запрещает самостоятельный standard-user programme:

```text
артефактная коллекция
→ добровольная заявка/членство
→ регулярный розыгрыш билетов
```

У него отдельные rules, fairness, legal, privacy и anti-abuse contracts.

## 14. Редакционный стиль

- взрослая дружелюбная литературная речь;
- конкретное действие и фактический результат;
- локальная идентичность без искусственной фамильярности;
- равноправный отказ;
- единые термины artifact/find/collection/application/member;
- нет fake urgency, FOMO, shame, guilt и bundled consent;
- нет обещания приза или участия без release truth;
- ирония запрещена в error, state loss, permission denial, cancellation,
  disqualification, draw и claim problems.

## 15. Measurement

### 15.1. Event-first outcomes

- qualified event decision rate;
- time to first event decision;
- capability success/attempts;
- recovery success;
- saved continuity;
- qualified return;
- accessibility task success.

### 15.2. Artifact outcomes

- first-hint comprehension;
- eligible hint → find;
- find → story opened;
- find → collection retrieved;
- hint request/abandon;
- repeat find/duplicate receipt rate;
- downstream meaningful event action;
- core CTA/time-to-value non-inferiority;
- accessibility-path parity.

### 15.3. Club/raffle outcomes

После release:

- collection progress → rules viewed;
- rules viewed → explicit application/member action;
- valid application rate;
- invalid/duplicate/reconciliation rate;
- fairness/accessibility exceptions;
- claim completion;
- complaints/appeals;
- effect on qualified event return.

### 15.4. Не оптимизировать как North Star

- tour completion;
- Hero-talk CTR;
- число messages/scenes;
- dwell/scroll depth;
- raw artifact completion;
- скорость коллекции;
- число club prompts;
- raw raffle applications без event/retention guardrails;
- permission grants;
- только positive reactions.

## 16. Privacy, storage и accessibility

State хранится раздельно:

```text
onboarding capability state
Hero-talk thread state
artifact/collection state
club membership state
raffle application/draw state
personalization/taste state
consent/channel state
```

Anonymous first find может быть device-local. Prize/club eligibility не может
основываться только на изменяемом `localStorage`; нужен durable server ledger,
identity/verification и deletion contract.

Обязательные accessibility gates:

- first useful text доступен без animation;
- artifact path существует touch/keyboard/screen reader;
- first tap/click выполняет действие;
- screen reader получает связный result, не fragment spam;
- found marker имеет ясное accessible name/state;
- no hover/motion/audio/precision dependency;
- reduced motion сохраняет объект и действие;
- 200–400% zoom/reflow и 320 CSS px;
- focus не перекрыт Hero/Page-end/toast/artifact;
- action message с CTA не исчезает по timer;
- raffle rules доступны в обычном документе, а не только modal/animation.

## 17. Failure behavior

- JS/profile/compiler failure → generic static first scene;
- artifact runtime failure → core site остаётся; campaign fail-closed;
- storage reset → честный unknown/device-local state;
- duplicate find → один receipt и спокойный `Уже найден`;
- stale collection/rules → персональный claim не показывается;
- identity merge failure → local и server state не склеиваются молча;
- application timeout → reconciliation, не false submission;
- draw/snapshot uncertainty → никакого `Вы участвуете`;
- campaign pause не отключает safety/help;
- club/raffle failure не блокирует event discovery.

## 18. Rollout

### O-0 — truth correction

- убрать legacy `HomeHeroTalk` rollback wording;
- включить artifacts/club/raffle в canonical onboarding scope;
- удалить вариант C;
- зафиксировать separate state domains;
- UI не меняется.

### O-1 — action echo baseline

- exact result + Undo для одной released utility capability;
- live-region integration;
- state fixtures;
- no-JS/core fallback.

### O-2 — Hero-talk baseline

```text
HT-1 static single scene
→ HT-2 deterministic short chains
→ HT-4 contextual page-end
→ HT-5 onboarding integration
```

### O-3 — first artifact onboarding

- active collection/version;
- exact first hint;
- stable accessible placement;
- idempotent find;
- story and collection handoff;
- hide/dismiss and campaign kill switch;
- non-interference tests.

### O-4 — collection continuity

- progress surface in `/artefakty/` and approved personal surface;
- device-local honesty;
- durable ledger design;
- hint progression;
- archive/freshness/provenance.

### O-5 — Friends Club contract

- canonical membership model;
- entry/leave/suspend;
- relation to collections;
- privacy/identity/promo separation;
- club page and Hero-talk chains.

### O-6 — regular raffle release

- published rules;
- prize evidence;
- application endpoint;
- durable eligibility ledger;
- immutable draw snapshot;
- auditable selection/alternates;
- claim/cancellation/appeal;
- legal, fairness, accessibility and anti-abuse rehearsal.

### O-7 — richer challenger

- HT-6 return/personal threads;
- HT-7 editorial/artifact-series programmes;
- HT-10 controlled comparison;
- independent kill switches and novelty review.

## 19. Integration scenarios

```text
onboarding.static_first_value
onboarding.home_hero_not_legacy_rollback
onboarding.one_task_per_chain
onboarding.action_echo_undo
onboarding.no_duplicate_result_echo
onboarding.dismissal_permanent
onboarding.storage_reset_fail_quiet
onboarding.blocked_dependency_not_promoted
onboarding.no_js_core_value
onboarding.keyboard_focus_not_obscured
onboarding.screen_reader_status
onboarding.exposure_not_competence

onboarding.first_artifact_exact_hint
onboarding.artifact_stable_placement
onboarding.artifact_touch_keyboard_screen_reader_equivalence
onboarding.artifact_find_idempotent
onboarding.artifact_find_opens_story_and_collection
onboarding.artifact_found_marker_persists
onboarding.artifact_hint_suppression
onboarding.artifact_campaign_hide
onboarding.artifact_not_event
onboarding.artifact_not_taste_signal
onboarding.artifact_does_not_block_core

onboarding.collection_progress_truthful
onboarding.collection_threshold_not_auto_application
onboarding.club_discovery_requires_antecedent
onboarding.club_membership_not_promo_consent
onboarding.raffle_claim_requires_published_rules
onboarding.raffle_application_explicit
onboarding.raffle_ambiguous_submit_reconciled
onboarding.raffle_status_snapshot_consistent
onboarding.focus_group_scoring_not_standard_user
```

Имена являются strategy registry, а не утверждением о реализованном runner.

Для будущих released identity/club сценариев default test preparation —
`session_fixture`: allowlisted fixed persona, настоящая Supabase session,
per-worker/device state и нулевые product OTP/mail counters. Real-mail OTP или
Yandex OAuth запускаются отдельно только при изменении соответствующего login
contract; обычный onboarding не должен превращаться в admission wall.
Fault suites различают direct Supabase, Yandex relay и Yandex sidecars и следуют
[`yandex-dependency-resilience.md`](../../operations/yandex-dependency-resilience.md).
Это не переносит focus-group NPS/feedback/scoring в standard onboarding: ветки
остаются раздельными по разделу 13.

## 20. BLOCKED owner/release decisions

1. Семантика like/favorite/calendar/ICS и имя personal saved surface.
2. Production Search acceptance.
3. Production-complete identity merge.
4. Фактические personalization factors.
5. Utility reminder delivery.
6. Final first public artifact collection/version and placement set.
7. Durable artifact ledger and anonymous→authorized merge.
8. Canonical Клуб друзей Анонсов membership contract.
9. Связь collection threshold, club membership и raffle application.
10. Частота и календарь регулярных розыгрышей.
11. Prize source, quantity, eligible events and claim.
12. Legal organizer, geography/age, rules and personal-data purposes.
13. Selection, alternates, cancellation, appeals and anti-abuse.
14. HT-1 first-scene families и exact short chains варианта A.
15. Permanent `Что умеет сайт` IA.
16. Hero thread reset/retention/cross-device contract.
17. Baseline traffic/MDE feasibility.

`BLOCKED` означает запрет делать конкретное неподтверждённое обещание. Это не
означает исключение артефактов, клуба или розыгрышей из продуктовой стратегии.

## 21. Источники и производные документы

- [Согласование с каноникой Hero-talk и owner correction](hero-talk-alignment-2026-08-03.md).
- [Варианты A/B](strategy-options.md).
- [Канонический Hero-talk](../hero-talk/README.md).
- [Артефакты-пасхалки](../static-site-easter-eggs/README.md).
- [Контракт коллекции и допуска к заявке](../static-site-easter-eggs/collection-contract.md).
- [Критическая консолидация исследований](research/research-synthesis-2026-08-03.md).
- [Gemini — полный текст](research/gemini-deep-research-2026-08-03.txt).
- [ChatGPT — полный текст](research/chatgpt-deep-research-2026-08-03.txt).
- [Prompt глубокого исследования](deep-research-prompt.md).

Strategy v0.3 сохранена в Git history и superseded этой v0.4.
