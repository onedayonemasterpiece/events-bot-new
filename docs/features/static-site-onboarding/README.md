# Онбоардинг стандартного пользователя KenigEvents

> **Статус:** evidence-consolidated strategy v0.2 / product decision; не implementation contract и не описание уже выпущенного production behavior.  
> **Дата:** 2026-08-03.  
> **Проверенный репозиторный baseline:** `main@09fcde9012b30d0c3b4a30d35f45e3c9858b096c`.  
> **Аудитория:** обычный новый или вернувшийся посетитель; не участник фокус-группы.  
> **Выбранный вариант:** [A — сдержанный utility-first contextual onboarding](strategy-options.md).  
> **Исследования:** [Gemini](research/gemini-deep-research-2026-08-03.txt), [ChatGPT](research/chatgpt-deep-research-2026-08-03.txt), [критическая консолидация](research/research-synthesis-2026-08-03.md).  
> **Research brief:** [исходный prompt](deep-research-prompt.md).

## 1. Итоговое решение

KenigEvents не нужен отдельный «онбординг-продукт» в виде обязательного тура,
welcome-wall, checklist, missions или процента завершения. Нужна небольшая
система редких контекстных сообщений поверх уже полезного static-first сайта.

Она работает по правилу:

```text
сначала реальная пользовательская задача и доступный контент
→ затем помощь только при наблюдаемой потребности
→ после действия — точный результат и Undo/следующий путь
→ освоенное перестаёт продвигаться
→ continuity upgrade предлагается только после созданной ценности
```

Первая задача сайта — помочь найти и понять жизнеспособное событие. Онбординг
не должен превращать эту задачу в демонстрацию возможностей интерфейса.

Основные решения:

1. Cold/unknown Hero сохраняет стабильное service promise.
2. Не более одного proactive learning message на один page journey.
3. Inline recovery и action echo важнее feature promotion.
4. Page-end Talk — основной post-value placement, но не активируется одним
   scroll depth.
5. Login, identity sync, PWA, reminders и permissions появляются только после
   antecedent value, фактического release и platform eligibility.
6. Utility, editorial, promo и artifact имеют разные purpose, state и metrics.
7. Артефакты не входят в onboarding MVP и не обучают core capabilities.
8. Focus-group missions, feedback obligations, leaderboard и prize mechanics
   не переносятся обычному пользователю.

## 2. Почему основой выбрано исследование ChatGPT

Оба исследования сходятся по направлению, но различаются по качеству
репозиторного аудита и работе с неопределённостью.

ChatGPT-исследование принято основным, потому что оно:

- фиксирует точный `main` SHA и различает implemented, candidate, target, open
  PR и research;
- не выдаёт локальный rerank за production recommendation loop;
- использует staged outcomes вместо одного неоднородного activation event;
- задаёт capability-specific success evidence;
- защищает static Hero от постоянных substitutions;
- отделяет Page-end eligibility от прокрутки;
- маркирует численные thresholds как priors, а не нормы;
- учитывает low-traffic MDE, A/A, SRM и underpowered interpretation;
- включает accessibility, legal/data boundaries и release dependencies.

Из Gemini приняты concise verdict, `explicit_event_decision` как операционный
FV2 proxy, action-result copy, запрет checklist/FOMO/bundled consent и широкий
банк сценариев. Отклонены его claims об обязательном Edge SSR, уже существующем
Hero dismissal и отсутствии live-region в текущем shell.

Полный disposition: [research synthesis](research/research-synthesis-2026-08-03.md).

## 3. Фактическая отправная точка

| Область | Release truth на baseline | Следствие для стратегии |
|---|---|---|
| Home | `index.astro` строит `HomeHeroTalk → HomeQuickNav → HomeColdStartFeed` | Первая ценность уже может возникать без отдельного welcome screen |
| Текущий `HomeHeroTalk.astro` | Статический Astro-компонент с service promise, двумя CTA и одной event link; нет registry/dismissal/state | Не называть его готовым target Hero Talk engine |
| Target Hero Talk | Product concept: typed briefing с курсором/ускоренным появлением текста, иногда квадратные изображения, связные narrative chains и route/event context | Presentation grammar проектируется отдельно от message eligibility; motion не может задерживать core value |
| Cold-start feed | До 30 статических событий; local JS может переставить карточки по `ke_personalization_profile` | Никакого обязательного profile setup; copy говорит только о локальном порядке |
| Toast/live region | `MobileToastRegion.astro` уже имеет polite/assertive live regions, persistent action/error и pause при focus/pointer/touch | Переиспользовать и проверить call sites; не строить параллельный toast engine |
| Event actions | Like, `Не интересно`, share и calendar affordance присутствуют в карточках | Обучение располагается рядом с реальным действием |
| Search | Поле доступно анонимно, submit ведёт в Yandex PKCE; production UX ещё требует real immutable-candidate acceptance | Не обещать полноценный anonymous или production-ready smart search |
| Favorites/calendar | R15 candidate и deployed schema существуют; real Yandex browser acceptance остаётся gate | Не называть только planned, но не писать финальный copy до решения semantics |
| Personalization | Target static-first hybrid описан; законченного learning loop нет | Никаких сильных «мы узнали ваши интересы» и никаких SSR assumptions |
| Identity sync | Partial/design; Yandex существует, полный email/linking/merge не release-complete | Предложение sync заблокировано до фактического merge contract |
| PWA | CTA зависит от browser eligibility | Не показывать недоступную кнопку и не обещать полный offline |
| Utility/promo Push | Planned/release-gated и purpose-separated | Не обучать и не запрашивать до выпуска |
| Артефакты | Product discovery/noindex research; production-кода и закрытых rights/provenance gates нет | Отдельный post-release cultural track |
| Focus group | Отдельный исследовательский cohort | Не является шаблоном standard-user journey |

## 4. Пользовательский результат: staged model

Не выбирается один глобальный флаг `onboarding_completed` и одна универсальная
activation North Star.

```text
FV1 — qualified_event_understanding
      человек получил достаточно фактов, чтобы принять решение;

FV2 — explicit_event_decision
      save/calendar/hide/like/share/ticket intent либо осознанное продолжение;

C1  — capability_success
      конкретное действие завершилось и его реальный результат показан;

C2  — continuity_value
      результат найден или полезен за пределами текущего экрана;

R1  — qualified_return
      следующий визит снова привёл к полезному event decision.
```

### Что используется в продуктовой telemetry

`explicit_event_decision` — основной оперативный FV2 proxy. Его компоненты
хранятся и анализируются отдельно: like не равен save, share-sheet invocation не
равен отправке, ICS download не равен external calendar import, outbound ticket
click не равен покупке.

### Что нельзя выводить из telemetry автоматически

- понимание фактов из dwell time;
- attendance из save/ticket click;
- mastery из exposure;
- удовлетворённость из long session;
- consent из account creation или PWA install.

Истинная comprehension периодически проверяется task research: дата, место,
статус, цена/условия и следующий допустимый action.

## 5. Продуктовые инварианты

1. **Static-first value.** Основной контент, facts, ссылки и route navigation не
   ждут profile, analytics или onboarding state.
2. **No admission wall.** Тур, login, email, PWA и permissions не стоят перед
   первой ценностью.
3. **One proactive message.** На один page journey допускается максимум одно
   сообщение, которое продвигает следующую возможность.
4. **Locality first.** Ошибка, recovery и reversible result показываются рядом с
   действием; global toast используется только при отсутствии устойчивого
   inline места или page-wide результате.
5. **Truthful result.** Copy описывает только подтверждённый current release.
6. **Capability-specific evidence.** У разных функций разные success/failure и
   `unknown_external_outcome`.
7. **Dismissal is valid.** `Закрыть`, `Не сейчас`, `Больше не показывать` и
   permission denial имеют разные предсказуемые состояния.
8. **No nagging.** Permanent dismissal не отменяется косметическим copy change,
   новым route или promo campaign.
9. **Purpose separation.** Onboarding, utility, editorial, promo и artifact не
   наследуют consent, success, taste signals или suppression друг друга.
10. **No taste pollution.** Exposure/click onboarding, campaign или artifact не
    становится автоматически интересом пользователя.
11. **Accessibility parity.** Keyboard, screen reader, zoom/reflow, no-hover,
    reduced motion и no-JS получают эквивалентную core value.
12. **No persona pressure.** Golden persona не меняет urgency, отказ, CTA,
    обещанный результат, caps или доступность.
13. **Null treatment allowed.** Если label/IA уже решает задачу, сообщение не
    добавляется.

## 6. Journey model

### 6.1. Home cold start

```text
static service orientation
→ Today / Search / event preview
→ cold-start feed
```

Нет proactive hint, profile setup или permission prompt. Success diagnostic —
открыт task route или актуальное событие; scroll сам по себе не success.

### 6.2. Date-led journey

Постоянные date controls и названия страниц являются основным обучением. Нельзя
пульсировать share/save на первой карточке без evidence реальной проблемы.

Следующий capability предлагается только рядом с подходящим событием.

### 6.3. Search-led journey

- до submit: label и живые примеры запроса;
- при current auth gate: честное объяснение point-of-intent login;
- loading/progress/error: inline status;
- zero result: один recovery block с refinement и обычным exit;
- fallback discovery явно не называется точным search result.

До production acceptance Search не становится обязательной частью onboarding.

### 6.4. Direct event deep link

Home orientation не вставляется. Сначала факты и lifecycle, затем primary event
CTA и только потом secondary actions. Action echo появляется после действия;
Page-end может предложить связанную дату/подборку либо открыть сохранённый
результат.

Уход после получения нужных фактов является допустимым outcome.

### 6.5. Returning anonymous

При сохранённом локальном state можно показать тихий Page-end/inline переход к
`Для меня` или result surface. При потере storage интерфейс fail quiet: не
перезапускает весь proactive onboarding, а возвращается к static orientation.

### 6.6. Authorized saved-state return

Пользователь приходит за сохранённым, поэтому onboarding ограничивается
lifecycle/status/recovery. Promo и «познакомьтесь с сайтом» не вытесняют utility
задачу.

### 6.7. Mobile/PWA и desktop/keyboard

PWA offer существует только при captured browser eligibility и antecedent value.
Desktop не получает mobile install copy. Keyboard help не зависит от hover и
появляется только после keyboard intent или явного Help.

## 7. Competency state

Для каждой `capability_id` хранится отдельное compact state:

```text
unknown
  → eligible
  → exposed                 # delivery diagnostic, не competence
  → attempted
  → succeeded
  → repeated
  → mastered

из любого релевантного состояния:
  → dismissed_until
  → dismissed_permanently
  → failed_recoverable
  → blocked_dependency
  → deprecated

mastered
  → needs_reintroduction(version_delta)
```

### Переходы

| Переход | Допустимое evidence | Запрещённый proxy |
|---|---|---|
| `unknown → eligible` | Release flag, route, event/platform eligibility | Первый page view |
| `eligible → exposed` | Сообщение реально доступно по renderer contract | DOM вне viewport |
| `exposed → attempted` | User activation CTA/control | Scroll, dwell, hover |
| `attempted → succeeded` | Подтверждённая local/server mutation или завершённый browser result | Открытый permission dialog |
| `succeeded → repeated` | Другой subject/session/context | Replay одного action |
| `repeated → mastered` | Default hypothesis: два успеха без help/recovery либо explicit permanent dismiss | Закрытие подсказки |
| `mastered → needs_reintroduction` | Semantic capability/version change | Cosmetic copy change |

Порог mastery является capability-owned hypothesis и фиксируется в test fixture,
а не универсальным числом из исследования.

### Anonymous → authorized

Сливаются только explicit success/dismissal и current version state.
Authenticated explicit action имеет приоритет; raw exposure/browsing history не
переносится. До production-complete merge capability остаётся
`blocked_dependency`.

## 8. Capability registry: выбранный baseline

| Capability | Когда допустимо обучение | Placement | Success evidence | Release boundary |
|---|---|---|---|---|
| Today/Tomorrow/Weekend | Route существует | Persistent IA, без prompt | Route open/usable listing | Current static routes |
| Date navigation | Availability manifest | Inline labels | Enabled date selected | Current calendar contract |
| Event facts/lifecycle | Event detail | IA; hint только после research evidence | Correct task outcome | Current event pages |
| Like | Released handler | Label + local echo | State toggled | Не называть save |
| `Не интересно` | Reversible behavior released | Inline echo + Undo | Hide/downrank + working Undo | Copy совпадает с scope |
| Share | API/copy available | Action only | Sheet invoked или copy success раздельно | Не заявлять отправку |
| Calendar/ICS/save | После owner decision | Action + exact echo | Durable state и download различаются | BLOCKED semantics/acceptance |
| `Мои события` | Existing saved state | Echo link/Page-end | Result retrieved | Browser acceptance |
| Search | После production acceptance | Inline label/recovery | Valid request/result/refinement | BLOCKED acceptance |
| `Для меня` | Actual compatible signals | Quiet inline/Page-end | Surface/control understood | Target not production-complete |
| `Почему это` | Actual explainable factor | On-demand inline disclosure | Factor understood/changed | BLOCKED factor source |
| Identity sync | Local durable value + released merge | Page-end/settings | Auth + idempotent merge | BLOCKED linking |
| PWA | Browser eligibility + meaningful mobile use | Page-end | `appinstalled`/valid decline | No offline overclaim |
| Utility reminder | Saved eligible event + released delivery | Saved settings | Purpose choice saved | BLOCKED delivery |
| Promo Push | Никогда не onboarding | Separate campaign settings | Separate opt-in | Planned only |
| Artifact | Post-release core value | Separate cultural invitation | Find/story; downstream separate | Rights/provenance/model BLOCKED |

## 9. Message architecture

### 9.1. Hero Talk и Page-end Talk — placements

Они не являются отдельными product purposes.

```text
placement = hero_talk | page_end_talk | inline | action_echo | help
intent    = orientation | capability_guidance | confirmation |
            personalization_explanation | return | editorial |
            campaign | artifact_hint | safety
source    = system | editorial | promo_campaign | event_context
objective = first_value | competence | continuity | retention |
            promotion | cultural_exploration
```

Evergreen onboarding не моделируется как `promo_campaign`. Допустим общий
renderer/delivery control plane, но registry, purpose, attribution, consent,
caps и analytics остаются раздельными.

### 9.2. Priority

1. Site/event lifecycle и safety.
2. Direct action error/recovery.
3. Confirmation и Undo.
4. Protected service orientation.
5. On-demand personalization explanation.
6. Capability guidance.
7. Return/continuity.
8. Editorial context.
9. Promo campaign.
10. Artifact hint.

Campaign и artifact никогда не вытесняют facts, error или transactional result.

### 9.3. Minimum message record

```text
message_id
schema_version
copy_version
placement_allowlist
intent
source
objective
capability_id?
capability_version?
route_families
prerequisites
result_claim
priority_class
conflicts / exclusive_group
max_exposures
cooldown
expiry?
dismissal_mode
a11y_variant
no_js_fallback
analytics_purpose
retention_ttl
owner / reviewed_at / provenance
feature_flag / kill_switch
```

### 9.4. Resolver

```text
collect released, route-relevant candidates
→ remove candidates with unprovable result_claim
→ remove blocked dependency/platform/consent candidates
→ apply dismissal, cap, cooldown, expiry
→ protect safety, action result and cold orientation
→ remove conflicts with transactional CTA
→ require accessible equivalent
→ select at most one proactive learning message
→ fail closed to static content or nothing
```

Exposure хранится отдельно от success/mastery и никогда не записывает taste.

## 10. Hero Talk: текущий и целевой смысл

### 10.1. Текущий компонент

Current `HomeHeroTalk.astro` — статическая композиция. Его нельзя использовать как
доказательство готовой typed briefing, user-state resolver или Page-end engine.

### 10.2. Target presentation grammar

Hero Talk в продуктовой концепции имеет конкретные признаки:

- briefing-текст ускоренно появляется с курсором;
- иногда появляются квадратные изображения;
- фразы образуют последовательную narrative chain, а не случайную ротацию;
- цепочка может учитывать route, событие, фестиваль и lifecycle;
- приветствие и брендовая ориентация могут включать принятые смыслы
  `Добрый день` и `Мы говорим по-калининградски`;
- page-end вариант на странице события обязан знать контекст события, но не
  менять его canonical facts.

Presentation не меняет eligibility:

- typed motion не задерживает H1, CTA или первый контент;
- reduced-motion показывает полный текст сразу;
- screen reader получает один связный текст, а не посимвольные announcements;
- квадратное изображение декоративно/контекстно и имеет корректный alt либо
  скрыто от accessibility tree;
- цепочка имеет ограниченный конец и не требует чтения для core task.

### 10.3. Выбранный baseline

Cold/unknown Hero остаётся стабильной ориентацией. Typed/narrative динамика
относится к [варианту B](strategy-options.md) и не входит в MVP варианта A, кроме
возможного визуального представления той же неизменной service chain.

## 11. Page-end Talk

Page-end Talk предпочтителен для одного спокойного post-value шага:

- открыть сохранённый результат;
- продолжить по связанной дате/подборке;
- показать factual explanation локального rerank;
- после release — предложить PWA/identity/reminder;
- после отдельного gate — пригласить в cultural layer.

Он не активируется одним `IntersectionObserver`/scroll. Нужен antecedent:
explicit decision, successful action, saved state, completed recovery либо
explicit Help.

На event detail сообщение учитывает конкретное событие и lifecycle. На listing —
route/date context. На Search — query outcome. Общий fallback — обычная ссылка
на продолжение или отсутствие блока.

## 12. Action echo

Первый implementation slice должен переиспользовать существующий
`MobileToastRegion`, но не превращать все результаты в global toast.

### Inline предпочтителен, когда

- действие reversible и Undo относится к одной карточке;
- результат остаётся видимым возле control;
- screen-reader duplicate announcement можно исключить;
- focus должен оставаться в локальном контексте.

### Global toast допустим, когда

- результат page-wide;
- действие убрало исходный control из DOM;
- нужен переход на другую result surface;
- нет устойчивого inline slot.

### Contract

- exact action/result scope;
- `aria-live` без autofocus;
- action/error message persistent;
- timer pause при focus/pointer/touch;
- Undo — обычная доступная кнопка;
- ambiguous timeout не выдаётся за success;
- failure сохраняет core content и alternate path.

## 13. Артефакты

Артефакты не входят в onboarding MVP и не являются доказательством competency.
Они могут существовать только как отдельный культурный слой после core value.

Допустимые будущие варианты:

1. bounded мини-коллекция из небольшого числа объектов;
2. статический редакционный культурный маршрут без progress/state;
3. after-action decorative object после проверки non-interference.

Не допускается:

- prerequisite для календаря, Search, `Для меня` или аккаунта;
- event badge/quality mark disguise;
- login, prize, streak, loss, opaque first clue;
- find/completion как activation;
- artifact action как taste signal;
- hover/motion/precision/second-device dependency;
- перенос focus-group contest model.

Первая подсказка, если track будет принят, точная и добровольная. Rights,
provenance, freshness, archive и accessible equivalent являются release gates.

## 14. Редакционный стиль

Онбоардинг наследует общий редакционный стиль, но имеет более строгий semantic
contract.

### Обязательные свойства

- дружелюбно, спокойно, конкретно;
- взрослая литературная речь без канцелярита и инфантилизации;
- конструкция `действие → фактический результат`;
- равноправный отказ;
- единые термины для like/save/calendar/reminder;
- никакого «мы знаем вас» при слабых сигналах;
- никакого fake urgency, FOMO, shame, guilt и bundled consent;
- ирония запрещена в error, state loss, permission denial, cancellation,
  reschedule, accessibility и sensitive context.

### Golden personas

Допустимы только ограниченные варианты длины/формальности. Persona не меняет:

- urgency;
- CTA и доступность отказа;
- message cap/cooldown;
- обещанный результат;
- права/consent;
- safety wording.

### Copy workflow

```text
author
→ product/release fact check
→ terminology check
→ accessibility review
→ legal/purpose review для consent/channel
→ pressure/ambiguity lint или LLM critic
→ human approval
→ versioned fixture
→ canary/kill switch
```

Исходные copy banks находятся в обоих исследованиях, но ни одна фраза не
становится production copy без проверки current behavior.

## 15. Measurement

### Основные outcomes

- qualified event decision rate с отдельными component metrics;
- median/p75 time to first event decision;
- capability success / attempts;
- recovery success;
- useful negative decision и continued discovery;
- saved continuity;
- qualified return 7/28 days;
- explanation calibration в task research;
- accessibility task success.

### Guardrails

- first event/detail/facts и core CTA non-inferior;
- accidental action/undo не растут;
- prompt burden минимален;
- permission denial остаётся valid outcome;
- permanent dismissal respected;
- no-JS/backend/storage failure сохраняют core;
- campaign/artifact не вытесняют utility;
- data quality: dedup, invalid transitions, missing IDs, SRM.

### Не оптимизировать

- tour/onboarding completion;
- число подсказок;
- dwell/scroll depth;
- raw permission grants;
- PWA install без qualified return;
- profile creation;
- artifact completion;
- только положительные reactions.

### Экспериментальный порядок

```text
instrumentation-only A/A
→ SRM/dedup/missingness validation
→ moderated task/a11y study
→ один isolated treatment
→ predeclared denominator/MDE/guardrails/kill
→ sequential canary или достаточный fixed sample
→ honest underpowered conclusion
```

Assignment unit — device/subject, не page view. До production identity merge
cross-device contamination признаётся, а не скрывается.

## 16. Privacy, storage и accessibility

### Minimal state

Onboarding хранит компактное current state, а не raw clickstream:

```text
capability_id
capability_version
current_state
success_count_bucket
last_success_bucket
last_message_version
suppression_mode / dismissed_until
blocked_dependency?
strategy_version
```

Exposure diagnostics имеют короткий TTL и отдельный purpose. Retention получает
только capability state, antecedent value и explicit channel choice; не получает
raw exposures, scroll/dwell, focus status, artifact actions как taste и
inferred persona.

### Failure behavior

- localStorage reset → static orientation, не полный повтор prompts;
- JS/backend/analytics failure → dynamic layer исчезает;
- ambiguous write → fail closed/reconciliation, не false success;
- identity merge failure → auth и merge показываются раздельно;
- campaign pause не отключает safety/help;
- logout, reset personalization, reset onboarding и delete account — разные
  действия.

### Accessibility release gate

- keyboard-only journey;
- screen-reader status/alert без duplicate announcements;
- focus не скрыт Hero/Page-end/sticky toast;
- 200–400% zoom/reflow и 320 CSS px;
- reduced motion, no-hover, no-audio;
- touch target contract;
- transient help dismissible/persistent/focus-safe;
- action message с CTA не исчезает по таймеру;
- typed Hero не объявляется посимвольно.

## 17. Rollout

### Wave 0 — truth and problem evidence

- route × capability × release inventory;
- task/a11y baseline текущего UI;
- список функций, которым не нужна подсказка;
- owner decision по calendar/save semantics;
- event taxonomy и A/A design;
- UI не меняется.

### Wave 1 — один reversible action

- exact inline/global echo decision;
- Undo;
- existing live-region integration;
- state transition fixtures;
- no-JS/core fallback;
- one-capability canary и kill switch.

### Wave 2 — local recovery

- Search только после acceptance;
- zero-result/refinement;
- action failure/ambiguous outcome;
- task success и abandon guardrails.

### Wave 3 — Page-end continuity

- один renderer;
- separate system registry;
- antecedent-value eligibility;
- permanent dismissal/cross-page suppression;
- open saved result или factual local explanation.

### Wave 4 — released upgrades

Только после соответствующих release gates:

- identity sync;
- PWA eligibility copy;
- `Почему это` и interest editor;
- utility reminders;
- отдельный promo consent.

### Wave 5 — optional narrative/cultural challengers

- вариант B Hero/Page-end narrative;
- cultural route/mini-collection;
- separate ledger/holdout;
- non-inferior core CTA;
- rights/provenance/accessibility.

## 18. Регистрируемые сценарии

```text
onboarding.static_first_value
onboarding.hero_orientation_protected
onboarding.hero_typed_reduced_motion
onboarding.hero_sequence_not_random
onboarding.page_end_requires_value
onboarding.one_proactive_message
onboarding.action_echo_undo
onboarding.action_echo_existing_live_region
onboarding.no_duplicate_announcement
onboarding.dismissal_permanent
onboarding.cooldown_cross_page
onboarding.storage_reset_fail_quiet
onboarding.capability_version_reintroduction
onboarding.blocked_dependency_not_promoted
onboarding.anonymous_authorized_merge
onboarding.no_js_core_value
onboarding.keyboard_focus_not_obscured
onboarding.screen_reader_status
onboarding.reduced_motion
onboarding.permission_denial_valid
onboarding.pwa_event_eligibility
onboarding.purpose_separation
onboarding.campaign_not_help
onboarding.artifact_not_event
onboarding.artifact_not_taste_signal
onboarding.exposure_not_competence
onboarding.experiment_srm
```

Эти имена пока являются strategy registry, не утверждением, что test runner уже
реализован.

## 19. BLOCKED owner/release decisions

1. Точная семантика `like`, `favorite_saved`, `calendar_saved`, ICS и итоговое
   имя `Моё`/`Мои события`.
2. Production acceptance Search.
3. Production-complete anonymous→authorized merge.
4. Фактические personalization factors для `Почему это`.
5. Utility reminder delivery/lifecycle.
6. Promo Push consent/delivery.
7. Legal applicability и документы рекомендательных технологий/ПД.
8. Baseline traffic, conversion components и MDE feasibility.
9. Artifact purpose, owner, rights/provenance и первый curated set.
10. Допуск варианта B после baseline варианта A.

До закрытия dependency capability остаётся `blocked_dependency`; onboarding copy
не может обещать её результат.

## 20. Варианты и источники

- [Вариант A, challenger B и культурный слой C](strategy-options.md).
- [Критическая консолидация](research/research-synthesis-2026-08-03.md).
- [Индекс и hashes исходных исследований](research/README.md).
- [Gemini — полный текст](research/gemini-deep-research-2026-08-03.txt).
- [ChatGPT — полный текст](research/chatgpt-deep-research-2026-08-03.txt).
- [Prompt глубокого исследования](deep-research-prompt.md).

Предыдущий v0.1 сохранён в Git history и superseded этой стратегией.
