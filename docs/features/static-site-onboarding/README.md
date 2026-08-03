# Онбоардинг стандартного пользователя KenigEvents

> **Статус:** evidence-consolidated strategy v0.3 / product decision; не
> implementation contract и не описание уже выпущенного production behavior.  
> **Дата:** 2026-08-03.  
> **Проверенный репозиторный baseline:**
> `main@09fcde9012b30d0c3b4a30d35f45e3c9858b096c`.  
> **Аудитория:** обычный новый или вернувшийся посетитель; не участник
> фокус-группы.  
> **Выбранный вариант:**
> [A — сдержанный utility-first contextual onboarding](strategy-options.md).  
> **Hero-talk dependency:** канонический пакет `docs/features/hero-talk/` в
> stacked PR [#291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291),
> ветка
> [`agent/hero-talk-chain-research-20260803`](https://github.com/onedayonemasterpiece/events-bot-new/blob/agent/hero-talk-chain-research-20260803/docs/features/hero-talk/README.md).  
> **Согласование:**
> [корректировка стратегии по канонике Hero-talk](hero-talk-alignment-2026-08-03.md).  
> **Исследования:**
> [Gemini](research/gemini-deep-research-2026-08-03.txt),
> [ChatGPT](research/chatgpt-deep-research-2026-08-03.txt),
> [критическая консолидация](research/research-synthesis-2026-08-03.md).  
> **Research brief:** [исходный prompt](deep-research-prompt.md).

## 1. Итоговое решение

KenigEvents не нужен самостоятельный «онбординг-продукт» в виде обязательного
тура, welcome-wall, checklist, missions или процента завершения. Нужна система
редких контекстных сообщений поверх уже полезного static-first сайта.

Она работает по правилу:

```text
сначала реальная пользовательская задача и доступный контент
→ затем помощь только при наблюдаемой потребности
→ после действия — точный результат и Undo/следующий путь
→ освоенное перестаёт продвигаться
→ continuity upgrade предлагается только после созданной ценности
```

Первая задача сайта — помочь найти и понять жизнеспособное событие. Онбординг не
должен превращать эту задачу в демонстрацию возможностей интерфейса.

После изучения каноники Hero-talk уточнено важное архитектурное решение:

```text
онбординг определяет, что и когда допустимо объяснять;
Hero-talk определяет, как связно и контекстно доставить такое сообщение.
```

Основные решения:

1. Cold/unknown visitor получает полезную статическую первую сцену Hero-talk,
   доступную до profile/runtime state.
2. Typed briefing, cursor semantics, optional tile media и coherent chains —
   грамматика самого Hero-talk, а не отдельный «расширенный» вариант
   онбординга.
3. На один page journey продвигается не больше одной новой capability и
   сохраняется один основной CTA-path; Hero-talk chain при этом может содержать
   несколько связанных nodes.
4. Inline recovery и immediate action echo важнее feature promotion.
5. Page-end Hero-talk использует точный контекст страницы; onboarding/continuity
   upgrade появляется там только после antecedent value.
6. Login, identity sync, PWA, reminders и permissions появляются только после
   созданной ценности, фактического release и platform eligibility.
7. Utility, onboarding, editorial, promo и artifact имеют разные purpose,
   state, metrics и consent boundaries.
8. Артефакты не входят в onboarding MVP и не обучают core capabilities.
9. Focus-group missions, feedback obligations, leaderboard и prize mechanics не
   переносятся обычному пользователю.

## 2. Нормативная иерархия и граница ответственности

При конфликте используются, в порядке убывания приоритета:

1. актуальные явные решения владельца продукта;
2. фактический код и release truth текущего `main`;
3. каноническая продуктовая модель Hero-talk;
4. эта стратегия онбординга;
5. критическая консолидация исследований;
6. исходные исследования и исторические прототипы.

### 2.1. Что принадлежит onboarding strategy

- capability registry;
- release/route/platform prerequisites;
- eligibility конкретной capability;
- competency state;
- success/failure evidence;
- `unknown_external_outcome`;
- dismissal, cooldown и suppression;
- mastery и versioned reintroduction;
- truthfulness `result_claim`;
- retention handoff;
- запрет taste pollution;
- capability-specific KPI и guardrails.

### 2.2. Что принадлежит Hero-talk

- placements `home_hero` и семейство `*_page_end`;
- typed briefing по смысловым fragments;
- cursor semantics;
- optional square-tile media;
- narrative graph, nodes, bridges и open loops;
- exact page/event/festival/club context;
- bounded cross-session thread state;
- greeting/local-identity/current-context families;
- generation-time phrase packs и critics;
- immutable compiled served plan;
- runtime selection готового static plan;
- chain-level frequency, coherence и editorial quality.

### 2.3. Что принадлежит владельцу действия

Компонент/контур конкретной capability отвечает за:

- непосредственную операцию;
- local error/recovery;
- immediate action confirmation;
- Undo;
- ambiguous-timeout handling;
- доступный status/alert;
- reconciliation и alternate path.

Hero-talk может продолжить подтверждённый результат, но не создаёт второе
противоречащее подтверждение.

### 2.4. Что принадлежит promo/editorial origin

Editorial programme или promo campaign поставляет кандидатов в общий Hero-talk
compiler. Она не получает ownership над onboarding state, safety, consent или
первой ценностью и не создаёт отдельный `Campaign Talk`.

## 3. Почему основой оставлено исследование ChatGPT

Оба исследования сходятся по направлению. ChatGPT-исследование остаётся основным
из-за более точного repo audit и работы с неопределённостью:

- различает implemented, candidate, target, open PR и research;
- не выдаёт local rerank за production recommendation loop;
- использует staged outcomes вместо одного неоднородного activation event;
- задаёт capability-specific success evidence;
- отделяет Page-end eligibility от одного scroll depth;
- маркирует numeric thresholds как priors;
- учитывает low-traffic MDE, A/A, SRM и underpowered interpretation;
- включает accessibility, legal/data boundaries и release dependencies.

Из Gemini приняты concise verdict, `explicit_event_decision` как FV2 proxy,
action-result copy, запрет checklist/FOMO/bundled consent и сценарный банк.
Отклонены claims об обязательном Edge SSR, уже существующем Hero dismissal и
отсутствии live-region в текущем shell.

Каноника Hero-talk дополнительно исправила обе исследовательские рамки там, где
они рассматривали Hero прежде всего как placement/banner, а не как единый
chain-first product mechanism.

## 4. Фактическая отправная точка

| Область | Release truth | Следствие для стратегии |
|---|---|---|
| Home | `index.astro` строит `HomeHeroTalk → HomeQuickNav → HomeColdStartFeed` | Первая ценность достижима без welcome screen |
| Current `HomeHeroTalk.astro` | Статический Astro-компонент с двумя CTA и event preview; нет compiler/thread/page-end | Это временное наполнение целевой Hero-talk зоны и rollback, не отдельный вечный продукт |
| Canonical Hero-talk package | Docs/release-design track в stacked PR #291; runtime остаётся `NO-GO` | Presentation/chain ownership берётся из Hero-talk, но не выдаётся за production |
| Cold-start feed | До 30 статических событий; local JS может rerank по `ke_personalization_profile` | Нет profile setup wall; copy говорит только о локальном порядке |
| Toast/live region | `MobileToastRegion.astro` имеет polite/assertive regions, persistent action/error и pause на focus/pointer/touch | Переиспользовать для допустимых global echoes; не строить параллельный toast engine |
| Event actions | Like, `Не интересно`, share и calendar affordance присутствуют | Основное обучение находится рядом с действием |
| Search | Intent field виден анонимно; submit ведёт в Yandex PKCE; acceptance не завершён | Не обещать anonymous production-ready smart search |
| Favorites/calendar | R15 candidate и deployed schema; real Yandex browser acceptance остаётся gate | Не писать финальный onboarding copy до решения semantics |
| Personalization | Target static-first hybrid описан; learning loop не завершён | Никаких «мы узнали ваши интересы» без фактического evidence |
| Identity sync | Partial/design; email/linking/merge не production-complete | Sync candidate остаётся `blocked_dependency` |
| PWA | CTA зависит от browser eligibility | Не показывать недоступную кнопку и не обещать полный offline |
| Utility/promo Push | Planned/release-gated и purpose-separated | Не продвигать до выпуска |
| Артефакты | Product discovery/noindex research | Отдельный post-release cultural track |
| Focus group | Отдельный research cohort | Не является шаблоном standard-user journey |

## 5. Пользовательский результат: staged model

Не существует одного глобального `onboarding_completed` и одной универсальной
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

`explicit_event_decision` — основной оперативный FV2 proxy, но компоненты
анализируются отдельно:

- like не равен save;
- share-sheet invocation не равен отправке;
- ICS download не равен external calendar import;
- outbound ticket click не равен покупке;
- `Не интересно` может быть полезным квалифицированным решением.

Нельзя автоматически выводить:

- comprehension из dwell/scroll;
- attendance из save/ticket click;
- mastery из exposure;
- удовлетворённость из long session;
- consent из account/PWA;
- return value из notification open;
- persona из Hero-talk engagement.

## 6. Продуктовые инварианты

1. **Static-first value.** Facts, links, navigation и first useful Hero scene не
   ждут profile, analytics или onboarding state.
2. **No admission wall.** Тур, login, email, PWA и permissions не стоят перед
   первой ценностью.
3. **One capability, not one literal node.** За page journey продвигается одна
   новая capability и один основной CTA-path; coherent Hero-talk chain может
   содержать несколько смысловых nodes.
4. **Locality first.** Ошибка, recovery и reversible result показываются рядом с
   действием; global toast используется только при отсутствии устойчивого
   inline места или page-wide результате.
5. **Truthful result.** Copy описывает только подтверждённый current release.
6. **Capability-specific evidence.** У функций разные success/failure и
   `unknown_external_outcome`.
7. **Dismissal is valid.** `Закрыть`, `Не сейчас`, `Больше не показывать` и
   permission denial имеют разные состояния.
8. **No nagging.** Permanent dismissal не отменяется cosmetic copy, route или
   campaign change.
9. **Purpose separation.** Onboarding, utility, editorial, promo и artifact не
   наследуют consent, success, taste или suppression друг друга.
10. **No taste pollution.** Hero-talk exposure, campaign click и artifact find
    не становятся автоматически preference signal.
11. **Accessibility parity.** Keyboard, screen reader, zoom/reflow, no-hover,
    reduced motion и no-JS получают эквивалентную core value.
12. **No persona pressure.** Persona pack не меняет urgency, refusal, CTA,
    result claim, caps, consent или rights.
13. **Null treatment allowed.** Если IA/label решает задачу, feature-discovery
    candidate не создаётся.
14. **No runtime LLM.** Runtime выбирает готовый static served plan; генерация и
    critics работают до публикации.
15. **Bounded conversation.** Hero-talk thread state не является полным
    разговором, taste profile или CRM memory.

## 7. Journey model

### 7.1. Home cold start

```text
Hero-talk / home_hero:
useful generic first scene
→ optional short greeting/current-context node
→ Today / Search / event CTA
→ HomeQuickNav
→ cold-start feed
```

Правила:

- first scene находится в static HTML;
- chain может иметь длину один;
- допустимы daypart greeting, local identity, today/weekend и service
  orientation families;
- до activation не используются persona packs, return delta и inferred
  interests;
- нет feature-selling, profile setup или permission prompt;
- typed motion не задерживает CTA и начало ленты;
- reduced motion/no-JS получают полный статический смысл.

Success diagnostic — открыт task route или актуальное событие. Scroll и Hero
exposure сами по себе не success.

### 7.2. Date-led journey

Постоянные date controls и названия страниц являются основным обучением.
Пульсация share/save на первой карточке без evidence реальной проблемы запрещена.

Hero-talk page-end может продолжить exact date/collection context, но
feature-discovery capability предлагается только при её собственной eligibility.

### 7.3. Search-led journey

- до submit: label и живые примеры запроса;
- при current auth gate: честное point-of-intent explanation;
- loading/progress/error: inline status;
- zero result: один recovery block с refinement и normal exit;
- fallback discovery явно не называется точным result;
- Hero-talk empty/search page-end не утверждает, что запрос понят, если runtime
  завершился ошибкой.

До production acceptance Search не становится обязательной capability.

### 7.4. Direct event deep link

Home orientation не вставляется. Сначала facts/lifecycle, затем primary event
CTA и secondary actions. Immediate echo появляется после действия.

Hero-talk `event_page_end` знает exact event/festival/club/occurrence/action
context, предлагает одну следующую задачу и не дублирует `Похожие события` или
card feed.

Уход после получения нужных facts остаётся допустимым outcome.

### 7.5. Returning anonymous

При сохранённом local state может быть доступен тихий Page-end/return candidate.
При потере storage интерфейс fail quiet: generic first scene вместо полного
повтора всех prompts или выдуманного продолжения.

`Пока вас не было` разрешается только при валидном meaningful-visit watermark и
согласованном `served_delta_id`; без identity остаётся device-local.

### 7.6. Authorized saved-state return

Пользователь приходит за сохранённым. Onboarding ограничивается
lifecycle/status/recovery. Promo и «познакомьтесь с сайтом» не вытесняют utility.

Cross-device Hero thread и onboarding state имеют разные schemas/owners.

### 7.7. Mobile/PWA и desktop/keyboard

PWA offer существует только при captured browser eligibility и antecedent value.
Desktop не получает mobile install copy. Keyboard help не зависит от hover и
появляется только после keyboard intent или explicit Help.

## 8. Competency state

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

| Переход | Допустимое evidence | Запрещённый proxy |
|---|---|---|
| `unknown → eligible` | Release flag, route, event/platform prerequisites | Первый page view |
| `eligible → exposed` | Served message действительно доступен по renderer contract | Скрытый DOM node |
| `exposed → attempted` | User activation конкретного CTA/control | Scroll, dwell, hover |
| `attempted → succeeded` | Confirmed local/server/browser result | Открытый permission dialog |
| `succeeded → repeated` | Другой subject/session/context | Replay одного action |
| `repeated → mastered` | Capability-owned fixture: повтор без help либо explicit permanent suppression | Простое закрытие |
| `mastered → needs_reintroduction` | Semantic capability/version change | Cosmetic copy change |

Порог mastery — capability-owned hypothesis, не универсальное число.

### Anonymous → authorized

Сливаются explicit success/dismissal и current version state. Authenticated
explicit action имеет приоритет; raw exposure, browsing и Hero thread text не
переносятся. До production-complete merge capability остаётся
`blocked_dependency`.

## 9. Capability registry: baseline

| Capability | Когда допустимо обучение | Surface | Success evidence | Release boundary |
|---|---|---|---|---|
| Today/Tomorrow/Weekend | Route существует | Persistent IA; optional generic Hero orientation | Route open/usable listing | Current static routes |
| Date navigation | Availability manifest | Inline labels | Enabled date selected | Current calendar contract |
| Event facts/lifecycle | Event detail | IA; hint только после research evidence | Correct task outcome | Current event pages |
| Like | Released handler | Label + local echo | State toggled | Не называть save |
| `Не интересно` | Reversible behavior released | Inline echo + Undo | Hide/downrank + Undo | Copy совпадает со scope |
| Share | API/copy available | Action only; later feature-discovery candidate | Sheet invoked или copy success раздельно | Не заявлять отправку |
| Calendar/ICS/save | После owner decision | Action + exact echo | Durable state и download различаются | BLOCKED semantics/acceptance |
| `Мои события` | Existing saved state | Echo link / Hero-talk page-end | Result retrieved | Browser acceptance |
| Search | После production acceptance | Inline label/recovery; optional feature-discovery chain | Valid request/result/refinement | BLOCKED acceptance |
| `Для меня` | Actual compatible signals | Quiet inline / personal-feed page-end | Surface/control understood | Target not production-complete |
| `Почему это` | Actual explainable factor | On-demand inline disclosure | Factor understood/changed | BLOCKED factor source |
| Identity sync | Local durable value + released merge | Page-end/settings | Auth + idempotent merge | BLOCKED linking |
| PWA | Browser eligibility + meaningful mobile use | Page-end | `appinstalled`/valid decline | No offline overclaim |
| Utility reminder | Saved eligible event + released delivery | Saved settings / page-end explanation | Purpose choice saved | BLOCKED delivery |
| Promo Push | Никогда не onboarding | Separate campaign settings | Separate opt-in | Planned only |
| Artifact | Post-release core value | Separate cultural invitation / artifact hint | Find/story; downstream separate | Rights/provenance/model BLOCKED |

## 10. Интеграция с каноническим Hero-talk

### 10.1. Канонические placements и intent

Hero-talk — один продукт:

```text
home_hero
event_page_end
collection_page_end
date_listing_page_end
search_page_end
personal_feed_page_end
club_page_end
```

Onboarding candidate имеет каноническую форму:

```yaml
intent: feature_discovery
origin: system
capability_id: <capability>
capability_version: <version>
eligibility_receipt: <bounded evidence>
success_contract: <capability-specific>
suppression_contract: <capability-specific>
```

Не создаются отдельные продукты `Onboarding Talk`, `Return Talk` или
`Campaign Talk`.

### 10.2. Двухступенчатое разрешение

```text
1. onboarding capability engine
   определяет eligibility/result/suppression candidate

2. Hero-talk compiler
   объединяет candidate с greeting/current-context/editorial/campaign inputs,
   применяет page/entity/thread context,
   строит coherent chain и immutable phrase pack

3. Hero-talk runtime
   выбирает только готовый static served plan

4. capability owner
   выполняет действие и возвращает success/failure evidence

5. onboarding state
   обновляет competency/mastery/suppression
```

Hero-talk не может самостоятельно решить, что capability освоена, и не может
обойти `blocked_dependency`.

### 10.3. One-capability chain

Ограничение «одна новая возможность» применяется к смыслу chain, а не к числу
фраз:

```text
eligible
→ contextual hint
→ attempted
→ confirmed result
→ where to find result
→ mastered/suppressed
```

Chain сохраняет один topic anchor и один основной CTA-path. Нельзя в одной
последовательности учить Search, PWA и reminders.

### 10.4. Priority constraints

Полный cross-purpose arbitration принадлежит Hero-talk. Онбординг накладывает
обязательные ограничения:

1. lifecycle/safety и direct error выше feature discovery;
2. immediate confirmation/Undo выше следующей capability;
3. cold/unknown first scene остаётся полезной и generic;
4. campaign/artifact не маскируются под help;
5. feature discovery не конфликтует с transactional CTA;
6. inaccessible equivalent означает exclusion candidate;
7. unprovable result claim означает exclusion candidate.

### 10.5. Постоянный реестр «Что умеет сайт»

Контекстные подсказки не являются единственным способом узнать о функции.
Нужен постоянный accessible registry/help surface:

- список реально выпущенных capabilities;
- простое объяснение результата;
- prerequisites и ограничения;
- ссылка на настройку/помощь;
- возможность повторно запросить explanation после mastery/dismissal;
- отсутствие progress, points и completion framing.

Этот registry принадлежит onboarding track; Hero-talk может ссылаться на него.

## 11. Hero-talk presentation и варианты стратегии

### 11.1. Нормативная grammar

Любой production Hero-talk имеет:

- semantic typed briefing, не медленную посимвольную печать;
- первый полезный fragment сразу;
- finite cursor semantics;
- links, появляющиеся атомарно;
- immediate completion/pause на hover/focus/pointerdown без блокировки first tap;
- optional exact-source tile mosaic;
- text-only fallback при media failure;
- coherent chain вместо random slogan rotation;
- static no-JS/reduced-motion equivalent;
- один связный screen-reader text, не fragment-by-fragment announcement.

Это не differentiator варианта B.

### 11.2. Вариант A

Выбранный baseline использует ту же grammar, но консервативную программу:

- single scene или короткая handwritten owner-reviewed chain;
- generic greeting/local identity/current context;
- одна capability feature-discovery chain после eligibility;
- без cross-session personalized/editorial campaign arcs;
- current static `HomeHeroTalk` сохраняется как rollback.

### 11.3. Вариант B

Challenger добавляет после соответствующих HT stages:

- bounded cross-session threads;
- return delta;
- finite persona packs и explicit-interest overlay после activation;
- event/festival/club arcs;
- own editorial campaign programmes;
- richer open-loop/resolution chains;
- novelty-aware experiment and independent kill switches.

Подробное сравнение: [strategy-options.md](strategy-options.md).

## 12. Page-end policy

Hero-talk page-end является одним механизмом с разновидностями по page family.
Он знает exact page/entity/action context и не дублирует canonical continuation
cards.

### 12.1. Общий контекстный Page-end

Editorial/event/festival/club continuation может быть eligible по завершённому
page context, даже без onboarding capability:

```text
прочитано событие фестиваля
→ в программе есть связанное продолжение
→ открыть программу
```

Это не считается onboarding exposure.

### 12.2. Onboarding/continuity Page-end

Для feature discovery, identity, PWA, reminder и открытия result surface нужен
antecedent:

- explicit event decision;
- confirmed successful action;
- saved state;
- completed recovery;
- actual compatible personalization signal;
- explicit Help.

Один scroll/IntersectionObserver не создаёт eligibility. Observer может доказать
qualified visibility уже выбранного served plan, но не пользовательскую ценность.

### 12.3. Порядок на странице

```text
main page content
→ canonical recommendations/continuation
→ page-end Hero-talk
→ focus-group NPS when enabled
→ footer
```

Page-end не превращается в ещё один card feed.

## 13. Immediate action echo и Hero-talk continuation

Immediate result принадлежит action owner:

```text
user action
→ exact local/server/browser outcome
→ inline echo or existing global live region
→ Undo / alternate path
```

Hero-talk может использовать подтверждённый outcome как `result_echo` bridge:

```text
Событие сохранено.
→ Где его найти.
→ Отдельно настроить reminder, если delivery released.
```

Ограничения:

- Hero-talk не дублирует уже видимый echo без добавления нового полезного шага;
- ambiguous timeout не становится success;
- save не становится Push consent;
- ICS download не становится external import;
- failure сохраняет core content;
- result node имеет один primary CTA.

Для immediate echoes переиспользуется существующий `MobileToastRegion` только
там, где нет лучшего inline slot или result page-wide.

## 14. Hero thread state и return delta

Hero-talk thread state хранится отдельно от onboarding competency:

```json
{
  "thread_id": "ht_...",
  "last_node_ids": ["n3", "n2"],
  "open_loop_id": "festival-kantata-education",
  "last_action": "opened_event",
  "meaningful_visit_watermark": "...",
  "expires_at": "..."
}
```

Правила:

- не хранить полный свободный текст разговора;
- последние и предпоследние node IDs — bounded;
- open loop продолжать только при валидных facts/lifecycle;
- потеря state → generic first scene;
- thread interaction не становится taste signal;
- onboarding dismissal не превращается в CRM suppression другого purpose;
- anonymous state device-local;
- cross-device continuation только после production identity contract.

Сцена `Пока вас не было` не является самостоятельной onboarding capability.
Count и destination list строятся из одного `served_delta_id`, с exact hide,
lifecycle и profile projection. Background tab не обновляет meaningful watermark.

## 15. Редакционный стиль и Golden personas

Онбординг наследует общий редакционный стиль и добавляет строгий semantic
contract:

- взрослая дружелюбная литературная речь;
- конкретное `действие → фактический результат`;
- равноправный отказ;
- единые термины like/save/calendar/reminder;
- нет fake urgency, FOMO, shame, guilt и bundled consent;
- нет «мы знаем вас» при слабых signals;
- ирония запрещена в error, state loss, permission denial, cancellation,
  reschedule, accessibility и sensitive context.

### Golden-persona packs

Канонический Hero-talk допускает finite human-reviewed persona packs после
activation. Для onboarding сохраняются инварианты:

Persona pack может менять:

- литературную формулировку;
- длину/ритм;
- допустимый topic framing;
- choice из заранее утверждённых variants.

Persona pack не может менять:

- eligibility и success evidence;
- urgency и pressure;
- CTA/refusal;
- result claim;
- cap/cooldown;
- права/consent;
- safety/lifecycle wording;
- число продвигаемых capabilities.

Публичные persona labels запрещены. Уникальная LLM-реплика на человека не
создаётся.

### Copy pipeline

```text
author / generation brief
→ product/release fact lock
→ terminology check
→ accessibility review
→ legal/purpose review where applicable
→ editorial-style critic
→ chain critic
→ pressure/ambiguity lint
→ human approval
→ immutable versioned pack
→ canary / kill switch
```

## 16. Артефакты

Артефакты не входят в onboarding MVP и не являются evidence competency.
Допустимы только как отдельный cultural track после core value.

Возможные формы:

1. bounded mini-collection;
2. static editorial cultural route без progress;
3. after-action decorative object после non-interference proof.

Запрещено:

- prerequisite для calendar, Search, `Для меня` или account;
- event badge/quality mark disguise;
- login, prize, streak, loss или opaque first clue;
- find/completion как activation/mastery;
- artifact action как taste signal;
- hover/motion/precision/second-device dependency;
- перенос focus-group contest model.

Если Hero-talk показывает `artifact_hint`, первая подсказка точная и
добровольная, а cultural track имеет отдельный ledger, provenance, rights,
freshness, archive и accessible equivalent.

## 17. Measurement

### 17.1. Onboarding outcomes

- qualified event decision rate с component metrics;
- median/p75 time to first event decision;
- capability success / attempts;
- recovery success;
- useful negative decision и continued discovery;
- saved continuity;
- qualified return 7/28 days;
- explanation calibration;
- accessibility task success.

### 17.2. Hero-talk integration outcomes

Hero-talk CTR не является primary outcome. Для feature-discovery chains:

- eligible session → capability attempt;
- attempt → confirmed success;
- success → where-to-find result;
- repeated independent use;
- mastery/suppression correctness;
- downstream event detail/action through any path;
- chain dismiss/fatigue;
- first-value and core CTA non-inferiority.

Exposure считается qualified delivery diagnostic, но не comprehension,
competence, preference или consent.

### 17.3. Guardrails

- time to event/detail/facts non-inferior;
- core CTA non-inferior;
- accidental action/undo не растут;
- prompt/chain burden минимален;
- permanent dismissal respected;
- no-JS/backend/storage failure сохраняют core;
- duplicate announcements и focus obstruction отсутствуют;
- campaign/artifact не вытесняют utility;
- thread/open-loop claims точны;
- data quality: dedup, invalid transitions, missing IDs, SRM.

### 17.4. Не оптимизировать

- tour/onboarding completion;
- Hero-talk CTR;
- число scenes/prompts;
- dwell/scroll depth;
- raw permission grants;
- PWA install без qualified return;
- profile creation;
- artifact completion;
- только positive reactions.

### 17.5. Экспериментальный порядок

```text
instrumentation-only A/A
→ SRM/dedup/missingness validation
→ moderated task/a11y study
→ one isolated capability/message treatment
→ predeclared denominator/MDE/guardrails/kill
→ sequential canary or sufficient fixed sample
→ novelty-aware observation
→ honest underpowered conclusion
```

Assignment unit — device/subject, не page view.

## 18. Privacy, storage и accessibility

### 18.1. Onboarding state

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

### 18.2. Hero-talk thread state

Хранится отдельно и bounded:

```text
thread_id
last_node_ids[0..2]
open_loop_id?
last_action?
meaningful_visit_watermark?
expires_at
thread_schema_version
```

Нельзя объединять два домена в raw clickstream или «историю разговора».

Exposure diagnostics имеют короткий TTL и отдельный purpose. Retention получает
capability state, antecedent value и explicit channel choice; не получает raw
exposures, scroll/dwell, Hero thread text, artifact action как taste или inferred
persona.

### 18.3. Failure behavior

- localStorage reset → generic static first scene, не полный повтор prompts;
- JS/backend/analytics failure → dynamic candidates исчезают;
- phrase-pack/compiler failure → deterministic/last-good generic pack;
- ambiguous write → reconciliation, не false success;
- identity merge failure → auth и merge показываются отдельно;
- campaign pause не отключает safety/help;
- logout, reset personalization, reset onboarding, reset Hero thread и delete
  account — разные действия.

### 18.4. Accessibility gate

- first useful text присутствует без typed animation;
- keyboard/pointer interaction не блокируется текущей фразой;
- first tap открывает link;
- cursor terminal и finite;
- screen reader получает связный final text;
- no fragment-by-fragment announcements;
- focus не скрыт Hero/Page-end/toast;
- 200–400% zoom/reflow и 320 CSS px;
- reduced motion, no-hover, no-audio;
- optional media имеет correct alt либо исключено из tree;
- action message с CTA не исчезает по timer;
- Page-end остаётся в document order и не overlay.

Presentation-specific acceptance принадлежит Hero-talk testing track;
onboarding проверяет integration с capability state.

## 19. Rollout и зависимость от Hero-talk release track

Onboarding action echo и local recovery могут развиваться до Hero-talk engine.
Feature discovery через Hero-talk не реализуется отдельным renderer.

### O-0 — truth and problem evidence

- route × capability × release inventory;
- task/a11y baseline;
- функции, которым hint не нужен;
- owner decision calendar/save semantics;
- staged outcomes и A/A design;
- alignment с HT-0;
- UI не меняется.

### O-1 — one reversible action

- exact inline/global echo decision;
- Undo;
- existing live-region integration;
- state-transition fixtures;
- no-JS/core fallback;
- one-capability canary и kill switch.

### O-2 — local recovery

- Search только после acceptance;
- zero-result/refinement;
- action failure/ambiguous outcome;
- task success/abandon guardrails.

### O-3 — Hero-talk static and chain baseline dependency

Hero-talk track должен закрыть:

```text
HT-1 static single-scene baseline
→ HT-2 deterministic handwritten chains
→ HT-4 contextual page-end
```

Onboarding не дублирует эти implementation slices.

### O-4 — Hero-talk onboarding integration

Соответствует HT-5:

- capability eligibility/success из onboarding registry;
- one capability per chain;
- immediate result boundary;
- where-to-find-result;
- mastered suppression;
- dismissal/cooldown;
- exposure not taste/competence;
- integration fixtures and kill switch.

### O-5 — released continuity upgrades

Только после собственных product release gates:

- identity sync;
- PWA eligibility;
- `Почему это`/interest editor;
- utility reminder;
- separate promo consent.

Hero-talk персональный/return delivery требует HT-6.

### O-6 — chain-rich/editorial challenger

- variant B bounded return/personal arcs — HT-6;
- own editorial campaign arcs — HT-7;
- image mosaic only after HT-8;
- controlled comparison — HT-10;
- cultural track отдельно.

Narrative chain как таковая не является O-6: basic chain-first grammar уже
входит в Hero-talk baseline.

## 20. Регистрируемые integration scenarios

Onboarding registry хранит только capability/integration scenarios:

```text
onboarding.static_first_value
onboarding.one_capability_per_journey
onboarding.action_echo_undo
onboarding.action_echo_existing_live_region
onboarding.no_duplicate_result_echo
onboarding.dismissal_permanent
onboarding.cooldown_cross_page
onboarding.storage_reset_fail_quiet
onboarding.capability_version_reintroduction
onboarding.blocked_dependency_not_promoted
onboarding.anonymous_authorized_merge
onboarding.no_js_core_value
onboarding.keyboard_focus_not_obscured
onboarding.screen_reader_status
onboarding.permission_denial_valid
onboarding.pwa_event_eligibility
onboarding.purpose_separation
onboarding.campaign_not_help
onboarding.artifact_not_event
onboarding.artifact_not_taste_signal
onboarding.exposure_not_competence
onboarding.experiment_srm
onboarding.hero_talk_candidate_eligibility
onboarding.hero_talk_one_capability_per_chain
onboarding.hero_talk_mastery_suppression
onboarding.hero_talk_result_context_no_duplicate_echo
onboarding.hero_talk_thread_not_competency
onboarding.page_end_feature_requires_antecedent
onboarding.what_site_can_do_manual_help
```

Следующие классы принадлежат Hero-talk testing, а не onboarding:

```text
typed fragments and cursor semantics
chain graph/coherence/bridges
random-rotation prohibition
mosaic/media lifecycle
viewport compiler
phrase-pack reproducibility
page-context packets
served_delta consistency
```

Имена пока strategy registry, не утверждение о реализованном runner.

## 21. BLOCKED owner/release decisions

1. Семантика `like`, `favorite_saved`, `calendar_saved`, ICS и итоговое имя
   `Моё`/`Мои события`.
2. Production acceptance Search.
3. Production-complete anonymous→authorized merge.
4. Фактические personalization factors для `Почему это`.
5. Utility reminder delivery/lifecycle.
6. Promo Push consent/delivery.
7. Legal applicability и документы рекомендательных технологий/ПД.
8. Baseline traffic, conversion components и MDE feasibility.
9. Artifact purpose, owner, rights/provenance и first curated set.
10. Owner-approved HT-1 first-scene families.
11. Какие short feature-discovery chains входят в variant A.
12. Допуск variant B только после HT-6/HT-7/HT-10 evidence.
13. Permanent `Что умеет сайт` IA и ownership.
14. Hero thread reset/retention/cross-device contract.

До закрытия dependency capability остаётся `blocked_dependency`; onboarding copy
не может обещать её результат.

## 22. Источники и производные документы

- [Согласование с каноникой Hero-talk](hero-talk-alignment-2026-08-03.md).
- [Вариант A, challenger B и cultural layer C](strategy-options.md).
- [Hero-talk PR #291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291).
- [Критическая консолидация исследований](research/research-synthesis-2026-08-03.md).
- [Индекс и hashes исходных исследований](research/README.md).
- [Gemini — полный текст](research/gemini-deep-research-2026-08-03.txt).
- [ChatGPT — полный текст](research/chatgpt-deep-research-2026-08-03.txt).
- [Prompt глубокого исследования](deep-research-prompt.md).

Strategy v0.2 сохранена в Git history и superseded этой v0.3.
