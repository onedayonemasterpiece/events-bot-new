# Сквозной регистр UI/UX-недоработок статического сайта

> **Статус:** активный release-control register.  
> **Область:** публичный сайт, prelaunch, фокус-группа, Auth, профиль,
> персонализация, Search, email-facing UI и degraded states.  
> **Нормативный план:** [`release-plan.md`](release-plan.md).  
> **Текущий статус:** [dashboard запуска 1 сентября](../../release/2026-09-01/README.md).  
> **Вопросы до дизайна:** [`system-design-questions.md`](system-design-questions.md).

## 1. Зачем нужен отдельный регистр

UI/UX-проблемы сейчас распределены между аудитами, screenshot-обсуждениями,
feature requirements, coding-agent prompts, preview PR и историческим
[`presentation-release-checklist.md`](presentation-release-checklist.md). Из-за
этого легко получить один из ложных результатов:

- макет утверждён, но UI не собран;
- компонент собран, но не встроен в полный пользовательский путь;
- desktop выглядит приемлемо, mobile сломан;
- source/unit test зелёный, hosted candidate не проверен;
- проблема найдена на production, но не связана с пользовательским сценарием;
- новый функционал реализован без заранее спроектированных loading/error/empty и
  accessibility states.

Этот документ хранит **проблему и критерии её закрытия**. Launch checklist хранит
**статус release gate**. Feature home хранит **нормативное поведение**. Копировать
полные требования между этими тремя слоями нельзя.

## 2. Единица учёта

Каждая проблема получает стабильный ID `UIUX-NNN` и поля:

| Поле | Назначение |
|---|---|
| `Journey` | Место в сквозном пользовательском пути |
| `Surface` | Страница, компонент или cross-site pattern |
| `Problem` | Наблюдаемая недоработка, а не абстрактное пожелание |
| `Severity` | `P0`, `P1` или `P2` |
| `Status` | `OPEN`, `DESIGNING`, `OWNER_GATE`, `IMPLEMENTING`, `VERIFY`, `BLOCKED`, `DONE`, `DEFERRED` |
| `Design dependency` | `SYS-Q-*`, который надо закрыть до реализации |
| `Acceptance` | Проверяемый результат на desktop/mobile/a11y/hosted target |
| `Evidence` | PR, SHA, candidate, screenshots, test/run или production incident |
| `Owner / target` | Кто закрывает и к какому сроку |

## 3. Последовательность обработки

Работа идёт по реальному пути пользователя, а не по удобству файловой структуры:

1. prelaunch и первый вход;
2. общий shell/navigation;
3. identity/Auth/profile;
4. home/date listings/discovery;
5. collections;
6. event detail;
7. Search;
8. personalization/«Для меня»;
9. focus feedback;
10. saved/calendar/share/communications;
11. degraded/offline/error recovery;
12. keyboard/accessibility/PWA;
13. финальная cross-surface consistency.

Внутри этапа сначала закрываются P0, затем P1. Следующий этап можно проектировать
параллельно, но нельзя объявлять полный journey готовым, пока предыдущий P0
остаётся открытым.

### Definition of Done для одной поверхности

`DONE` требует всех применимых слоёв:

1. принято продуктовое решение;
2. утверждён reference/wireframe для новых или существенно изменённых UI;
3. implementation находится в актуальном `main`;
4. пройдены desktop и mobile responsive checks;
5. пройдены keyboard/focus/semantics/contrast/reduced-motion checks;
6. пройдён exact-main hosted/candidate journey;
7. loading/empty/error/degraded states не показывают false success;
8. release checklist и user story обновлены;
9. production acceptance приложена, если поверхность уже выпущена.

Screenshot, Figma/reference, source assertion или merge по отдельности не закрывают
пункт.

## 4. Начальная очередь до запуска

### Этап A — prelaunch и первый контакт

#### `UIUX-001` — Выбрать один публичный prelaunch-вариант

- **Severity:** P0
- **Status:** `OWNER_GATE`
- **Surface:** `/` до запуска
- **Design dependency:** `SYS-Q-004`
- **Problem:** PR #296, #313 и #318 задают пересекающиеся композиции и разные
  уровни реализации. Нет одного accepted reference и production owner decision.
- **Acceptance:** один exact candidate SHA; screenshots `1440×900`, `390×844` и
  reduced motion; корректный image scale/crop; единый источник света/материала;
  форма и error states; остальные варианты помечены lab/evidence.
- **Evidence:** PR #296, #313, #318.
- **Owner / target:** Product/design owner · до RC prelaunch.

#### `UIUX-002` — Согласовать публичную заглушку и focus soft gate

- **Severity:** P0
- **Status:** `DESIGNING`
- **Surface:** root, invite/QR, locked/unlocked first paint
- **Design dependency:** `SYS-Q-001`, `SYS-Q-002`
- **Problem:** публичная prelaunch surface и приглашённый focus journey не должны
  создавать два конкурирующих root UI, flash незавершённого сайта или consent wall.
- **Acceptance:** deterministic first paint; no accidental app-shell exposure;
  invite opens intended surface; no-JS behavior truthful; routes/indexing match
  launch phase; desktop/mobile browser matrix.
- **Owner / target:** Product + Frontend · до focus RC.

### Этап B — общий shell и навигация

#### `UIUX-003` — Полнота глобального navigation shell для новых функций

- **Severity:** P0
- **Status:** `OPEN`
- **Surface:** header, desktop nav, mobile drawer, bottom nav, toast region
- **Design dependency:** `SYS-Q-002`, `SYS-Q-006`
- **Problem:** новые Search/profile/collections/focus surfaces проектировались в
  разных ветках; menu может вести на blocked/missing route или иметь разные
  названия desktop/mobile.
- **Acceptance:** единый route-aware registry; одинаковые destinations/order/
  semantics при breakpoint-appropriate geometry; blocked route не кликабелен;
  no duplicate active state; route integrity после production build.
- **Owner / target:** Product + Frontend · до M3 RC.

#### `UIUX-004` — Общий набор loading/empty/error/degraded states

- **Severity:** P1
- **Status:** `OPEN`
- **Surface:** все интерактивные surfaces
- **Problem:** Search, feedback, profile, collections и transport показывают
  локально придуманные состояния; часть ошибок выдаётся как общий отказ Яндекса.
- **Acceptance:** shared vocabulary и component patterns для loading, no data,
  stale last-good, queued, partial success, retryable error, terminal error;
  truthful component receipts; screen-reader announcements; no layout jump.
- **Owner / target:** Design system + Frontend · до final UI freeze.

### Этап C — identity, Auth и профиль

#### `UIUX-005` — Спроектировать и собрать `/profil/`

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** profile page and mobile account entry
- **Design dependency:** `SYS-Q-003`, `SYS-Q-008`
- **Problem:** профиль описан в PR #328, но SOR/локализация и exact D0 scope не
  решены; logout/diagnostics/interests/reset нельзя распределять по случайным меню.
- **Acceptance:** account/session truth, local/verified state, interests projection,
  reset, logout, diagnostics copy/link, focus return, no event-collection duplication;
  zero-backend ordinary navigation; mobile/desktop/a11y/hosted tests.
- **Evidence:** PR #328.
- **Owner / target:** Product + Architecture + Frontend · после `SYS-Q-003`.

#### `UIUX-006` — Безопасный Auth CTA и возврат в исходное действие

- **Severity:** P0
- **Status:** `DESIGNING`
- **Surface:** focus feedback, Search gate, saved/personal actions
- **Design dependency:** `SYS-Q-001`, `SYS-Q-008`
- **Problem:** пользователь не должен терять page path, scroll position, выбранный
  score или текстовый черновик; anonymous/local state не должен выглядеть как
  подтверждённый вход.
- **Acceptance:** disabled state explains why; one explicit email/Yandex CTA;
  return target is allowlisted and survives callback; draft preserved locally;
  verified UI state only after protected probe; logout/account-switch safe.
- **Owner / target:** Identity + Frontend · focus RC.

#### `UIUX-007` — Mobile OTP/Yandex UI без системных и keyboard regressions

- **Severity:** P0
- **Status:** `VERIFY`
- **Surface:** Android Chrome, iOS Safari, PWA browser-tab/Auth return
- **Problem:** historical Safari first-run modal and keyboard issues требуют
  повторного exact-target evidence; source fixes не равны final acceptance.
- **Acceptance:** one issue/verify/registration where applicable; all keyboard
  fields visible and usable; no duplicate send; direct/relay route evidence;
  redaction and cleanup; final target SHA.
- **Owner / target:** Identity QA · перед focus expansion.

### Этап D — home, даты и discovery

#### `UIUX-008` — Проверить home/date listing после интеграции всех новых блоков

- **Severity:** P0
- **Status:** `VERIFY`
- **Surface:** `/`, today, tomorrow, date, weekend
- **Problem:** отдельные accepted компоненты могут создать cumulative overflow,
  повтор CTA, конфликт sticky/bottom navigation или слишком длинный first viewport.
- **Acceptance:** frozen representative content; desktop/mobile geometry; no
  horizontal overflow; useful no-JS first paint; keyboard order; core task
  «найти событие» не ухудшен; Web Vitals budget.
- **Owner / target:** Frontend QA · M3 RC.

#### `UIUX-009` — Погода/море как контекст, а не конкурирующий продукт

- **Severity:** P1
- **Status:** `OPEN`
- **Surface:** today/tomorrow/date/weekend
- **Problem:** UI контракты есть, producer/live binding не завершён; partial/stale
  values и sea threshold должны быть понятны без перегрузки листинга.
- **Acceptance:** exact freshness and attribution; water shown only by accepted
  threshold; partial/stale last-good states; no extra blocking network call;
  responsive and screen-reader acceptance.
- **Owner / target:** Weather + Frontend · default-off до live gates.

### Этап E — подборки

#### `UIUX-010` — Спроектировать полный каталог подборок

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** catalog, menu, collection pages, empty/blocked entries
- **Design dependency:** `SYS-Q-006`
- **Problem:** часть новых подборок имеет data contracts без UI; названия kids/
  family, free, clubs и editorial/semantic collections расходятся между docs.
- **Acceptance:** one registry-driven catalog; public/repair/blocked/deferred
  states; meaningful supply/empty copy; route/card design; mobile/desktop;
  sitemap/navigation parity; no link to missing HTML.
- **Owner / target:** Product + Design + Frontend · после registry decision.

#### `UIUX-011` — Гастрономическая подборка: supply и boundary UI

- **Severity:** P1
- **Status:** `BLOCKED`
- **Surface:** gastronomy hub/listing
- **Design dependency:** `SYS-Q-006`
- **Problem:** data-prep есть в PR #314, но owner-approved membership и public
  route отсутствуют; нельзя показывать ресторан-площадку как гастрособытие.
- **Acceptance:** reviewed exact IDs, lifecycle active/low-supply/dormant,
  appropriate mini-guide/empty state, occurrence dedupe, owner visual acceptance.
- **Owner / target:** Editorial collections · после data gate.

### Этап F — страница события

#### `UIUX-012` — Свести event-detail additions в одну композицию

- **Severity:** P0
- **Status:** `VERIFY`
- **Surface:** desktop Editorial/Split, mobile event detail
- **Problem:** medallions, transport, VK questions, feedback, related events,
  continuation, page-end/Hero-talk и artifacts добавлялись отдельными tracks.
- **Acceptance:** one journey map and DOM order; no duplicate CTA/handlers;
  correct card/media geometry; end-of-page sequencing; focus return; mobile
  bottom-nav clearance; exact-main browser matrix.
- **Owner / target:** Product + Frontend · M3 RC.

#### `UIUX-013` — Полный visual QA медальонов

- **Severity:** P1
- **Status:** `OPEN`
- **Surface:** все фактические static-site medallion renderers
- **Problem:** selected lab checks не доказывают artwork/ring/alpha/alias quality
  на всех bearing URLs; подробный QA contract остался в старых PR #26/#65.
- **Acceptance:** current-build inventory; every actual bearing route captured at
  mobile/desktop; zero clipping, dirty alpha, duplicate/wrong semantics, broken
  primary/fallback; owner visual disposition.
- **Owner / target:** Design QA · до final UI freeze.

#### `UIUX-014` — Транспортный блок и fallback на актуальных данных

- **Severity:** P1
- **Status:** `VERIFY`
- **Surface:** eligible event detail and transport ICS
- **Problem:** UI может быть корректен на исторических specimens, но freshness,
  unsupported locality, partial schedule и long late-return wording остаются gates.
- **Acceptance:** fresh provider manifest; exact-date eligible examples; truthful
  no-return/last-train state; no block on unsupported event; mobile maps and ICS;
  last-good/stale drill.
- **Owner / target:** Transport + QA · перед public event-page promotion.

### Этап G — умный поиск

#### `UIUX-015` — Search failure/recovery и stage diagnostics

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** Search input, progress, results, auth gate, cached/stale state
- **Design dependency:** `SYS-Q-007`
- **Problem:** production failure/root cause ещё не закрыты; UI не должен показывать
  бесконечный progress, empty result вместо transport error или повторять
  cost-bearing request через второй route.
- **Acceptance:** monotonic real stages; one in-flight; auth gate sends zero Search
  POST; cached/stale labels; direct/relay/both-down behavior; retry only where
  server contract allows; request/corpus revision diagnostics.
- **Owner / target:** Search + Frontend · P0 recovery.

### Этап H — персонализация и «Для меня»

#### `UIUX-016` — Local-first personalization truth и серверные состояния

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** home rerank, `/dlya-menya/`, profile interests, action echo
- **Design dependency:** `SYS-Q-003`, `SYS-Q-008`, `SYS-Q-009`
- **Problem:** UI не должен обещать sync/long-term profile, если state остаётся
  local; Supabase/YDB target architecture ещё не решена.
- **Acceptance:** explicit local/verified boundaries where user-visible;
  deterministic static fallback; action echo/Undo; no blank first paint; account
  switch isolation; meaningful empty state and reset behavior.
- **Owner / target:** Personalization + Product · staged by accepted wave.

### Этап I — фокус-группа и feedback

#### `UIUX-017` — Auth-gated feedback block без скрытого login wall

- **Severity:** P0
- **Status:** `DESIGNING`
- **Surface:** page score, NPS, issue text, screenshot
- **Design dependency:** `SYS-Q-001`
- **Problem:** блок должен быть виден приглашённому участнику, но controls disabled
  до identity; текущие anonymous-first docs и dashboard противоречат owner decision.
- **Acceptance:** visible disabled score/NPS/text/screenshot; concise reason and
  explicit Auth CTA; invite/share unaffected; safe callback return; after Auth
  exact controls enable; no silent anonymous session/write; browser/mobile tests.
- **Evidence:** PR #323.
- **Owner / target:** Focus + Identity + Frontend · focus RC.

#### `UIUX-018` — Revision-aware page score и общий service NPS после Auth

- **Severity:** P1
- **Status:** `BLOCKED`
- **Surface:** page Lab block and participant hub
- **Design dependency:** `SYS-Q-001`
- **Problem:** TTL-24h/current prototype не соответствует revision-aware model;
  auth-gated correction меняет pre-submit state.
- **Acceptance:** unanswered-disabled-before-auth; enabled-after-auth;
  answered-current `Ваша оценка: N`; revision-changed with previous score;
  service NPS stored separately; text/screenshot component receipts.
- **Owner / target:** Focus product + Frontend · after feedback contract update.

#### `UIUX-019` — Условия участия, прогресс и розыгрыш без ложных обещаний

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** participant hub, artifact progress, eligibility, cutoff/result
- **Design dependency:** `SYS-Q-002`, `SYS-Q-011`
- **Problem:** eligibility, exact artifact collection, cutoff, draw/reserve and
  public rules must match backend/legal truth; old 12-artifact model conflicts
  with current project collection of 7 artifacts.
- **Acceptance:** exact rules/version; server evidence; timezone cutoff;
  eligibility explanation; immutable draw status; reserve/claim states; no
  gamification pressure on feedback score; legal/a11y/anti-abuse acceptance.
- **Owner / target:** Product + Legal + Backend · before focus programme claims.

### Этап J — saved actions, email and communication UI

#### `UIUX-020` — Развести like, calendar, favorites и hidden recovery

- **Severity:** P1
- **Status:** `VERIFY`
- **Surface:** cards, event detail, Favorites, hidden collection, profile
- **Problem:** historical docs и PR #328 уже исправляли ошибочное смешение hidden
  state с Favorites; UI должен сохранять independent signals and recovery paths.
- **Acceptance:** separate action semantics, exact Undo, two-zone Favorites if
  accepted, hidden recovery only in its collection, no duplicate event inside a
  zone, profile does not render event collections.
- **Owner / target:** Product + Frontend · before personalization RC.

#### `UIUX-021` — Email templates, preferences and delivery truth

- **Severity:** P1
- **Status:** `OPEN`
- **Surface:** OTP, launch notification, transactional reminders, recommendations,
  unsubscribe/preferences, bounce/suppression states
- **Problem:** infrastructure and providers exist, but not every product stream has
  approved templates, rendering matrix, consent UX and terminal delivery states.
- **Acceptance:** versioned template/copy; text+HTML; mobile email clients;
  Reply-To/From; one-click unsubscribe where applicable; provider failure does
  not show false sent; preference/suppression UI maps to control-plane state.
- **Owner / target:** Editorial + Email + Legal · by stream before enablement.

### Этап K — degraded/offline/recovery

#### `UIUX-022` — Capability-specific diagnostic page and user copy

- **Severity:** P0
- **Status:** `IMPLEMENTING`
- **Surface:** diagnostic page, feedback bundle, error messages
- **Problem:** field evidence showed direct Supabase available while Yandex relay/
  YDB were unavailable. A generic «Яндекс не работает» is false and loses useful
  route information.
- **Acceptance:** independent receipts for direct data/Auth, relay, Yandex OAuth,
  YDB control/projection, Postbox/inbound and object delivery as applicable;
  concise screenshot-sized summary; correlation code; redacted copy action;
  no secret/PII; `CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED`-like precise state.
- **Owner / target:** Transport + UX · focus RC.

#### `UIUX-023` — Both-routes-down без false success и потери ввода

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** feedback, profile/action writes, Auth/Search where applicable
- **Problem:** UI должен отличать `queued`, `not sent`, `ambiguous` and terminal
  success; selected-once requests cannot blindly retry through another route.
- **Acceptance:** zero dispatch when no route; bounded queue only for idempotent
  operations; entered text preserved; reconnect exactly once; component-level
  partial receipts; user can inspect/retry/cancel according to contract.
- **Owner / target:** Transport + Frontend · hosted reliability gate.

### Этап L — keyboard, accessibility and PWA

#### `UIUX-024` — V8 keyboard re-entry, reading route and contextual help

- **Severity:** P1, accessibility P0 for broken core navigation
- **Status:** `VERIFY`
- **Surface:** desktop event detail Editorial/Split
- **Problem:** V7 supports strong card graph behavior, but re-entry reliability,
  reading stops, `?` help and exact artifact bridge require K0 evidence and owner
  acceptance from PR #330.
- **Acceptance:** one grammar across both families; documented key restores owner
  from every visible non-editor state; Enter semantics; gallery focus return;
  help lists enabled/disabled commands with reasons; no input/editor capture;
  Chromium/Firefox/WebKit evidence.
- **Owner / target:** Accessibility + Frontend · before keyboard rollout.

#### `UIUX-025` — Полный accessibility audit после интеграции

- **Severity:** P0
- **Status:** `OPEN`
- **Surface:** all P0 public/focus journeys
- **Problem:** component-level checks do not prove cumulative landmark, focus order,
  labels, contrast, motion and screen-reader behavior.
- **Acceptance:** keyboard-only critical journeys; visible focus; semantic headings/
  landmarks; controls names/states; contrast; zoom/reflow; reduced motion;
  screen-reader sample; no focus trap; issue disposition and regression tests.
- **Owner / target:** Accessibility QA · final RC.

#### `UIUX-026` — PWA install/relaunch on Android and iOS

- **Severity:** P0 for claimed PWA flow
- **Status:** `BLOCKED`
- **Surface:** manifest/install UI/standalone/relaunch
- **Problem:** browser viewport tests do not prove native Add to Home Screen,
  standalone scope/start URL or preservation of intended local/verified state.
- **Acceptance:** Android Launcher and iOS SpringBoard journeys; stable manifest
  id/scope/start_url; invite/local state continuity; separate session fixture where
  needed; honest network-only/offline copy; no duplicate onboarding.
- **Owner / target:** PWA + Mobile QA · before PWA promotion.

### Этап M — финальная сквозная приёмка

#### `UIUX-027` — User-story journey matrix and cumulative visual acceptance

- **Severity:** P0
- **Status:** `BLOCKED`
- **Surface:** cross-site release candidate
- **Design dependency:** `SYS-Q-012`, all preceding P0 UIUX items
- **Problem:** route checks and feature tests могут быть зелёными, хотя целая
  пользовательская задача остаётся несобранной или ломается на переходе между
  страницами/identity states.
- **Acceptance:** public and focus user-story registries; route × state × device
  matrix; exact-main immutable RC; desktop/mobile screenshots for selected
  specimens; terminal network/console evidence; no unresolved P0 visual/UX debt;
  owner sign-off tied to SHA/build.
- **Owner / target:** Product + QA + Release owner · final RC.

## 5. Как добавлять новый пункт

1. Проверить, нет ли уже ID для той же наблюдаемой проблемы.
2. Привязать к journey и конкретной surface.
3. Сформулировать наблюдаемое нарушение и влияние на пользователя.
4. Добавить `SYS-Q-*`, если дизайн зависит от нерешённого выбора.
5. Записать проверяемые acceptance criteria, включая degraded/a11y states.
6. Добавить соответствующий release checklist item или ссылку из существующего.
7. После исправления приложить exact evidence и оставить запись в истории.

Формулировки «поправить UI», «довести до ума», «посмотреть мобильную» и
«вроде готово» не являются допустимыми debt items или closure evidence.

## 6. Связь с incident и user stories

- Production incident должен указывать затронутую user story и `UIUX-*`, если
  проблема видима пользователю.
- UIUX item после production failure не закрывается только починкой конкретных
  данных: требуется regression на root cause.
- Статистика полезна, если измеряет outcome/guardrail истории; количество кликов
  само по себе не заменяет acceptance.
- Focus feedback может создавать новые UIUX items, но score/NPS не определяют
  приоритет автоматически: severity зависит от сломанной задачи и охвата.

## 7. Что делать с историческим presentation UI debt

`TD-PRESENTATION-UI-*` в
[`presentation-release-checklist.md`](presentation-release-checklist.md) остаётся
историческим evidence presentation-track. Новые общесайтовые проблемы получают
`UIUX-*`. Актуальный незакрытый `TD-*` должен быть либо:

- перенесён сюда с сохранением ссылки на старый ID;
- явно закрыт с evidence;
- помечен `HISTORICAL/SUPERSEDED`.

Поддерживать два активных параллельных UI debt register запрещено.
