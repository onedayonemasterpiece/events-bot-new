# PROMPT для кодового агента — генеральная доработка v3: стабильный focus candidate, auth-gated feedback, профиль и персонализация

Дата фиксации: **2026-08-04**  
Репозиторий: `onedayonemasterpiece/events-bot-new`  
Актуальный `main` на момент подготовки: `0d1848bc324ef8c44df146ec2a7126a116a94bf4`  
Архитектурный docs-donor: PR `#328`, branch `docs/p13n-transport-profile-20260804`, head `5c3cabe45937e29e1f7c2f3660a52419149b9e18`  
Режим задачи: **реализация, интеграция, GitHub Actions и immutable review candidate**, а не ещё один docs-only аудит.

> Этот документ полностью заменяет предыдущие версии генерального handoff.
> Кодовый агент должен начать с актуального `origin/main`, проверить, не изменились
> ли перечисленные refs, и использовать этот документ как единое задание.

---

## 0. Обязательный конечный результат

Итог — одна стабильная noindex-сборка exact SHA, которую владелец продукта может
открыть по HTTPS и проверить в нескольких браузерах.

К моменту handoff одновременно должны выполняться условия:

1. приглашённый участник фокус-группы может открыть и использовать афишу без
   обязательной авторизации;
2. блок обратной связи виден приглашённому участнику на поддерживаемых страницах;
3. без email/Яндекса feedback-контролы видимы, но заблокированы, а рядом есть
   явное рабочее действие авторизации;
4. после подтверждённого входа по email или через Яндекс тот же блок становится
   доступным без ложного anonymous write;
5. кнопка приглашения, ссылка и QR работают независимо от авторизации;
6. существует рабочая noindex-страница `/profil/` с аккаунтом, локальным
   состоянием персонализации, reset/logout и копированием redacted diagnostics;
7. обычное листание страниц при совместимом локальном состоянии даёт `0` profile
   reads в YDB и `0` profile/data reads в Supabase;
8. первая коллекция содержит ровно **7 готовых артефактов** из локального
   reference-каталога, все семь реально размещены и доступны в candidate;
9. первая подсказка, находка, история и переход в коллекцию встроены в onboarding;
10. существующие принятые механики — double-click/double-tap like, единый runtime,
    VK CTA «Остались вопросы?» — не регрессируют;
11. обязательные GitHub Actions завершились terminal PASS до публикации candidate;
12. public candidate прошёл Chromium, Firefox и WebKit smoke;
13. финальный отчёт содержит secret invitation URL, direct profile URL, direct
    collection URL, PR, exact SHA и Actions run URLs;
14. public production root и stable ICS не менялись без отдельного прямого
    разрешения владельца.

В эту итерацию **не входят**:

- production rollout новой модели персонализации;
- включение remote personalization writes;
- миграция current feedback backend в YDB;
- full raffle/draw/winner/alternate/claim lifecycle;
- включение YDB scheduler/RU или Weather producer без их отдельных canary gates.

Эти границы не разрешают завершить работу заглушкой, default-off feature,
нерабочим auth CTA, docs-only PR или candidate без ссылки.

---

## 1. Последние решения владельца — высший приоритет

### 1.1. Фокус-группа и обратная связь

Правильная модель:

```text
валидное приглашение / QR
→ focus marker
→ обычный сайт доступен без входа
```

Но:

```text
page score / текст / screenshot / structured issue / service NPS
→ только подтверждённая Supabase session после email или Яндекс
```

До авторизации участник **видит сам feedback-блок**, понимает его назначение и
видит шкалу, но не может активировать feedback-контролы.

Требуемая UX-модель:

```text
Помогла ли вам эта страница?
[0] [1] ... [10]          ← видны, disabled
[Сообщить о проблеме]     ← disabled

Чтобы отправить оценку или сообщение, войдите по email или через Яндекс.
[Войти и оставить отзыв]  ← enabled, рабочий auth handoff

[Пригласить человека]     ← enabled независимо от входа
```

Отключаются только действия, требующие verified identity. Invite/share/QR не
должны случайно попадать в disabled fieldset.

Запрещено:

- скрывать весь feedback-блок до входа;
- сначала принимать клик по числу, а затем неожиданно требовать авторизацию;
- сохранять pre-auth score как будто он отправлен;
- создавать silent anonymous Supabase Auth session ради feedback/NPS;
- отправлять anonymous feedback, NPS или screenshot;
- считать focus marker серверной идентичностью;
- блокировать сам сайт требованием войти.

### 1.2. Профиль

В этой итерации route фиксируется как:

```text
/profil/
```

Решения:

- mobile account block всегда ведёт в `Профиль`;
- direct mobile logout удаляется;
- logout находится внутри `/profil/`;
- desktop account shortcut ведёт в профиль; отдельный desktop logout в этой
  итерации не нужен;
- `Избранное` остаётся отдельной поверхностью;
- скрытые события остаются `Подборки → Помечены «не интересует»`;
- профиль не рендерит event cards, Favorites, hidden recovery или collection
  counters;
- signed-out профиль всё равно показывает локальный режим, интересы, diagnostics
  и auth CTA;
- signed-in профиль показывает session, sync/data-source state и logout;
- reset personalization и logout — разные операции.

### 1.3. Персонализация и транспорт

P13N-00 уже выполнен в `main`. Его нельзя повторно объявлять задачей или заменять
новой моделью.

Следующий безопасный slice:

```text
P13N-01A = local profile shell + diagnostic projection + zero-backend gates
```

Ограничения:

- target scorer не создаётся и legacy weights не продвигаются;
- production ranking не меняется;
- remote profile/signal writes остаются выключены;
- browser local projection — единственный default profile cache;
- Supabase не становится profile cache;
- YDB-primary и dual-plane remote writes остаются архитектурной целью после
  ownership/localization/legal gate;
- ordinary navigation не вызывает backend profile reads;
- текущий feedback backend не мигрирует в YDB как побочный эффект этой задачи.

### 1.4. Первая коллекция артефактов

Источник истины:

```text
/home/dev/projects/events-bot-new/
  docs/features/static-site-pages/references/artefact-collection-1
```

В первой коллекции ровно **7 готовых артефактов**. Они не заменяются пятью
placeholder-слотами, восемью draft-объектами или двенадцатью функциональными
`FG-E*` achievements.

Все семь должны иметь:

- immutable ID;
- public name;
- short description;
- full story;
- provenance/source note;
- image + dimensions + alt;
- desktop/mobile/accessibility placement;
- onboarding role;
- рабочий found-state;
- отображение на странице `N из 7`.

Полный draw отложен. Локальные находки доступны без входа; server receipt,
feedback и prize-related state требуют verified identity.

---

## 2. Нормативная иерархия

При конфликте использовать источники в таком порядке:

1. решения владельца из раздела 1;
2. `docs/features/static-site-pages/personalizaion/requirements.md`;
3. `docs/features/static-site-pages/personalizaion/personalization-to-be.md`;
4. `docs/features/static-site-pages/personalizaion/personalization-research-traceability.md`;
5. `docs/features/static-site-pages/personalizaion/README.md` из branch
   `agent/personalization-implementation-contract-20260802`;
6. PR `#328`:
   - `personalizaion/transport-ecology-profile-architecture.md`;
   - `user-profile.md`;
   - `testing/personalization-transport-profile-test-plan.md`;
7. exact reference-каталог первой коллекции;
8. фактический код и generated output свежего `origin/main`;
9. onboarding/Hero-talk документы;
10. исторические PR/ветки только как file-level donors.

Явно не использовать как текущую истину:

- anonymous-first feedback из PR `#250` и попавших в `main` docs/registry;
- старую Supabase-primary/YDB-analytics модель PR `#295` как wholesale merge;
- `FOCUS_EGG_DEFINITIONS` из 12 функциональных достижений как cultural artifact
  registry;
- `ARTIFACT_COLLECTION_SLOTS` с пятью ячейками;
- stale 8-item branch как owner-approved collection;
- legacy EventLayout scorer/weights как target model.

PR `#295` разрешён только как донор всё ещё актуальных Yandex capability/fault
сценариев после ручного сопоставления с dual-plane архитектурой PR `#328`.

---

## 3. Фактическая отправная точка

Перед изменениями воспроизвести и записать baseline exact SHA.

На `main@0d1848bc...` ожидаются следующие факты:

- P13N-00 имеет статус `done_behavior_preserving`;
- общий PersonalizationRuntime смонтирован, target behavior выключен;
- `FocusGroupLabPanel.astro` показывает активные score/issue controls и проверяет
  session только после клика — это неправильный UX boundary;
- `StaticSiteAuth` уже содержит resilient email OTP, verify и Yandex OAuth
  controller; новую сетевую auth-реализацию создавать не нужно;
- mobile menu ведёт signed-in account в `/dlya-menya/` и содержит direct logout;
- `/profil/` отсутствует;
- PR `#328` меняет только три документа и не является runtime proof;
- current artifact surfaces конфликтуют по counts/identity;
- session fixture и no-mail test contract уже существуют;
- double-click/double-tap, VK question CTA и page runtime уже имеют acceptance
  evidence и требуют regression, а не переписывания.

Если baseline изменился, обновить factual report, но не менять owner decisions.

---

## 4. Режим работы, ветки и интеграция

Работать из чистого worktree от свежего `origin/main`.

Рекомендуемая integration branch:

```text
agent/static-site-general-v3-profile-focus-artifacts-20260804
```

Обязательный порядок:

1. `git fetch origin`;
2. проверить текущий `origin/main` и PR `#328` head;
3. file-level перенести/ребейзнуть три docs PR `#328`;
4. синхронизировать canonical README/status/scenario registries;
5. реализовать profile + auth-gated feedback + local P13N slice;
6. реализовать exact 7-artifact collection/onboarding;
7. интегрировать на свежий main;
8. запустить обязательные GitHub Actions;
9. только после PASS публиковать immutable candidate;
10. провести public cross-browser smoke и передать ссылки.

Запрещено:

- менять грязный root checkout;
- wholesale merge старых PR `#250/#270/#287/#295`;
- создавать второй auth/transport stack;
- создавать Supabase profile cache;
- включать remote P13N writes;
- слать routine OTP email;
- считать open PR, local PASS, started background или skipped job финальным PASS;
- публиковать candidate до CI;
- продвигать public root.

Допустимы несколько PR/commits, но финальный результат обязан быть одним
интегрированным candidate exact SHA.

---

## 5. Track A — синхронизация product truth и архитектурного каркаса

### 5.1. Интегрировать PR #328 как docs-donor

Перенести все три документа без потери исправленных границ Favorites/hidden:

- `docs/features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md`;
- `docs/features/static-site-pages/user-profile.md`;
- `docs/testing/personalization-transport-profile-test-plan.md`.

Затем обновить:

- `personalizaion/README.md` — добавить три документа в canonical package;
- `personalizaion/implementation-status.yml` — новый baseline, P13N-00 evidence,
  `P13N-01A` и честные blocked remote waves;
- `docs/README.md`, `docs/routes.yml`, feature indexes;
- release/checklist документы, которые ссылаются на старую profile/storage model.

### 5.2. Исправить focus truth

Синхронно изменить prose и machine-readable truth:

- `docs/features/static-site-pages/focus-group.md`;
- `focus-group-release/README.md`;
- `focus-group-release/status.md`;
- `focus-group-release/nps-ui.md`;
- `focus-group-release/testing.md`;
- `focus-group-release/prize-rules.md`;
- `docs/testing/focus-group-release-scenarios.v1.yml`;
- launch readiness PR/registry, если он остаётся открытым.

Новая machine-readable semantics:

```yaml
access:
  invited_without_auth_can_use_site: true

feedback:
  block_visible_to_invited: true
  signed_out_state: locked
  auth_methods: [email, custom:yandex]
  anonymous_server_write: false
  page_score:
    verified_identity_required: true
  text:
    verified_identity_required: true
  screenshot:
    verified_identity_required: true
  service_nps:
    verified_identity_required: true

invite:
  link_share_requires_auth: false
  qr_share_requires_auth: false

profile:
  route: /profil/
  noindex: true
  event_collections_allowed: false

personalization:
  p13n_00: done_behavior_preserving
  current_slice: p13n_01a_local_profile
  remote_writes: false
  supabase_profile_cache: false
  ordinary_navigation_backend_calls: 0
```

Старые anonymous feedback/NPS/server receipt scenarios удалить или пометить
superseded; не оставлять одновременно две истины.

---

## 6. Track B — видимый, но заблокированный feedback-блок

### 6.1. State machine

Реализовать закрытый набор состояний:

```text
not_focus_participant
  → block absent

auth_checking
  → block visible, feedback controls disabled, status checking

locked_signed_out
  → scale/issue visible but disabled
  → auth callout visible
  → invite/share enabled

auth_handoff
  → controls disabled
  → CTA aria-busy=true

unlocked_signed_in
  → scale and issue enabled
  → auth callout hidden

transport_degraded_signed_out
  → controls disabled
  → honest retry/error copy
  → invite/share still enabled

submitting_authenticated
  → only affected feedback controls busy

pending_authenticated
  → owner-bound outbox state shown truthfully

session_expired_or_signed_out
  → immediate return to locked state
```

### 6.2. DOM and accessibility contract

- Feedback score/issue controls находятся внутри `<fieldset disabled>` или
  эквивалентной нативной disabled-границы.
- Auth CTA расположен вне disabled container.
- Invite/share расположен вне disabled feedback container.
- Disabled controls не принимают pointer/keyboard activation.
- Screen reader получает объяснение, почему controls недоступны.
- Состояния объявляются через `role=status`; ошибки — через `role=alert`.
- Visible focus обязателен для CTA, invite и unlocked controls.
- Нельзя использовать только `opacity`/CSS как блокировку.

Рекомендуемый текст:

```text
Чтобы отправить оценку или сообщение, войдите по электронной почте или через Яндекс.
```

Primary CTA:

```text
Войти и оставить отзыв
```

### 6.3. Auth handoff — переиспользовать существующий путь

Минимальный предпочтительный вариант: не копировать OTP-form в каждый feedback
block, а использовать существующий intake/auth flow с безопасным возвратом.

```text
/fokus-gruppa/priglashenie/
  ?confirm=1
  &return_to=<same-origin-path>#focus-feedback
```

Требования к `return_to`:

- только same-origin relative path;
- сохраняет candidate/base prefix;
- reject external/protocol-relative/javascript URLs;
- хранится только столько, сколько нужно для auth round-trip;
- удаляется из видимого URL после чтения;
- после успешного email/Yandex auth возвращает на исходную страницу и feedback
  anchor;
- после cancel/skip возвращает туда же в locked state;
- не содержит bearer token в logs/artifacts/diagnostics.

Использовать существующие методы `StaticSiteAuth`:

- `signInWithEmailOtp`;
- `verifyEmailOtp`;
- `signIn` для Яндекса;
- `subscribe/getSession/signOut`.

Inline auth chooser допустим только при извлечении одного reusable компонента,
который также использует `/profil/` и не дублирует transport/network logic.

### 6.4. Feedback semantics

До verified session:

- score не пишется в local feedback state;
- issue dialog не открывается;
- screenshot picker недоступен;
- outbox record не создаётся.

После verified session:

- page score, text и screenshot используют текущий idempotent feedback contract;
- diagnostic bundle прикладывается автоматически;
- component receipts правдивы и независимы;
- screenshot failure не откатывает committed text/score;
- diagnostics failure не блокирует feedback;
- pending action связан с exact auth subject и не flush-ится под другим account;
- sign-out/account switch создаёт hard barrier для pending outbox;
- no false success до durable acknowledgement.

### 6.5. Общий NPS

На `/zakrytaya-afisha/` должен быть один настоящий service NPS block:

- visible locked state для signed-out invited participant;
- same auth CTA/handoff;
- enabled только после verified session;
- отдельный `service_revision`;
- scale `0…10`;
- optional comment;
- idempotent write;
- значение NPS не влияет на шанс или результат;
- не повторяется на каждой странице;
- больше не является prototype, который «остаётся в текущей вкладке».

---

## 7. Track C — рабочий профиль `/profil/`

### 7.1. Route and navigation

Создать:

```text
site/src/pages/profil/index.astro
```

Route:

- `noindex,nofollow,noarchive`;
- входит в route/runtime inventory;
- работает под preview/candidate base path;
- не создаёт SEO/sitemap entry.

Mobile menu:

- signed-out и signed-in account entry ведёт в `/profil/`;
- signed-in entry показывает initial/name как сейчас;
- direct `Выйти` удаляется;
- `Избранное` остаётся отдельным рядом;
- hidden recovery остаётся в `Подборки`.

Desktop:

- account shortcut открывает profile;
- full management живёт на profile;
- logout в этой итерации только внутри profile.

### 7.2. Содержание

#### Account/session

Показать:

- `checking | signed-out | signed-in | degraded`;
- способ входа без лишнего раскрытия PII;
- CTA email/Yandex в signed-out state;
- `Выйти на этом устройстве` в signed-in state;
- честный offline/degraded state.

YDB не участвует в обычной session check.

#### Personalization

Показать:

- `не начата | активна | сброшена`;
- `На этом устройстве | Синхронизировано | Сохранённая копия`;
- compact top interests из совместимой local projection;
- CTA `Настроить интересы` → существующая соответствующая surface;
- CTA `Сбросить персональные рекомендации`;
- pending-action/reset-pending state.

Не показывать sensitive inference или synthetic persona label пользователю.

#### Diagnostics

Рабочая кнопка:

```text
Скопировать диагностическую информацию
```

Bundle on demand:

```text
build_id / repo_sha / release_id
page_family / route / surface_id
auth route health class
p13n route health class
profile projection revision / age class
outbox counts by lane
oldest pending age bucket
local storage budget used
service worker version
web/app mode
connectivity timing class
last error class
```

Запрещены:

- email/ФИО;
- JWT/refresh token/OTP;
- raw feedback;
- screenshot body;
- full user agent;
- IP/network identifiers;
- bearer candidate URL.

#### Management

Реализовать сейчас:

- logout on this device;
- local personalization reset;
- reset-pending state, если server command ещё не разрешён;
- copy diagnostics.

Не имитировать как готовые:

- export data;
- delete account;
- remote profile delete;
- raffle profile;
- server sync, если transport wave не включена.

### 7.3. Product boundaries

Профиль не содержит и не кэширует:

- calendar agenda;
- liked/favorite event cards;
- hidden event list;
- restore hidden controls;
- artifact collection progress;
- event manifest.

`Избранное` и hidden recovery проверяются отдельными scenarios.

### 7.4. Storage ecology

- profile читает общий bounded personalization envelope;
- не создаёт собственную копию profile/collections;
- no durable debug log;
- diagnostics generated on demand;
- aggregate KenigEvents-owned storage hard `<= 64 KiB`;
- profile projection target `<= 4 KiB`, hard `<= 8 KiB`;
- current overlay target `<= 8 KiB`, hard `<= 12 KiB`;
- outbox `<= 16 actions`, target `<= 12 KiB`;
- strong/unacknowledged/reset markers не evict-ятся молча.

---

## 8. Track D — P13N-01A local profile slice и zero-backend ecology

### 8.1. Не повторять P13N-00

В начале подтвердить существующие gates:

- legacy profile/scorer quarantined;
- target scorer отсутствует;
- route inventory полный;
- unknown surface fail-static;
- production-off behavior/network/storage unchanged.

Если они прошли, не переписывать их.

### 8.2. Scope P13N-01A

Реализовать только безопасный local slice:

- adapter чтения compact local profile projection;
- profile state labels;
- local reset marker/epoch;
- typed diagnostics projection;
- owner-bound bounded outbox metadata;
- storage budget enforcement;
- zero-backend navigation tests;
- no model/ranking changes;
- no DB migrations;
- no remote profile writes;
- no Supabase profile cache.

Не создавать target `scorer.ts` и не переносить legacy weights.

### 8.3. Zero-backend invariant

При совместимой local projection:

```text
calendar/date/listing/event/collection navigation
→ YDB requests = 0
→ Supabase data/profile requests = 0
```

Допустимы только действительно необходимые Auth token refresh requests.

Projection refresh contract для будущего:

- максимум один eligible refresh за session;
- ETag/304;
- no rewrite on 304;
- no retry storm;
- stale-compatible local state до 7d;
- incompatible state → static-only fallback.

В этой итерации remote refresh не включать без готового approved endpoint.

### 8.4. Signal separation

Не смешивать:

```text
like_state
favorite_saved
calendar_saved
hide_state
```

Проверки:

- unlike не удаляет calendar save;
- calendar remove не снимает like;
- restore hide не создаёт like/save;
- profile не меняет IA Favorites;
- hidden recovery остаётся в Collections.

### 8.5. Dual-plane architecture boundary

PR `#328` определяет target, но не разрешает production activation.

В этой итерации допустимы:

- TypeScript contracts/interfaces;
- deterministic fake transport tests;
- route/failure classification;
- diagnostic classes;
- no-op/local-only adapters.

Не допускаются без отдельного gate:

- YDB-primary migration;
- Supabase Edge bridge deploy;
- remote strong-action writes;
- production materializer;
- identity vault;
- PII migration.

Current focus feedback backend остаётся на существующем proven path до отдельной
migration task.

---

## 9. Track E — первая коллекция из exact 7 артефактов

### 9.1. Инвентаризация reference-каталога

Прочитать read-only:

```text
docs/features/static-site-pages/references/artefact-collection-1
```

Создать source-bound inventory с:

- exact seven identities;
- source paths;
- SHA-256;
- assets/dimensions;
- short/full copy;
- provenance;
- placement;
- onboarding role;
- rights/status notes.

Если файлов физически больше семи, определить supporting assets/duplicates, но
не менять owner truth `7` без прямого решения.

### 9.2. Один registry/source of truth

Registry invariants:

- exact count `7`;
- unique ID/slug/order;
- no prose classification in Astro;
- all assets exist;
- all stories/provenance non-empty;
- private placement coordinates не попадают в public projection;
- no runtime LLM;
- validators fail closed.

Удалить из current collection surfaces:

- пять placeholder slots;
- 8-item draft threshold;
- 12 `FG-E*` achievements как artifact identities;
- counts `5/8/10/12` для первой коллекции;
- demo found states;
- `coming soon` вместо готового объекта.

### 9.3. Placement and accessibility

Все семь реально достижимы:

- stable assignment by collection/placement version;
- no reroll on reload/reorder/viewport;
- mouse/touch/keyboard/screen-reader parity;
- no hover/motion/precision-only requirement;
- reduced motion retains object;
- no overlap with event facts/CTA/navigation/feedback;
- found state `Найдено — открыть историю`;
- repeat activation idempotent;
- all-seven browser journey.

### 9.4. Progress and identity

До auth:

- local progress works;
- collection page shows `N из 7`;
- share/invite works;
- no server/prize claim.

После auth:

- local finds synchronize idempotently only through existing/approved receipt
  contract;
- pending receipts bound to exact subject;
- account switch does not inherit another subject’s progress;
- sign-out preserves local progress but hides server state;
- no automatic raffle entry.

Не создавать новую backend table/RPC, если уже есть reusable receipt/outbox
contract. Если server receipt невозможно безопасно завершить без отдельной
migration, candidate обязан честно показывать local progress и auth state, а
server receipt gate — `BLOCKED` с точной причиной; это не разрешает выключить
саму коллекцию.

### 9.5. IA и onboarding

Каноническая минимальная IA:

```text
/artefakty/                         — текущая коллекция/вход
/artefakty/kollektsii/<slug>/       — progress + stories
/zakrytaya-afisha/                  — summary + CTA
/fokus-gruppa/kollektsiya/          — redirect/alias, не вторая модель
```

Onboarding chain:

```text
полезное действие в афише
→ точная первая подсказка
→ первый placement
→ находка
→ история
→ CTA в коллекцию
→ N из 7
→ следующая bounded подсказка
```

Без mandatory tour, runtime LLM, taste pollution и незакрытых raffle claims.

---

## 10. Track F — сохранить уже принятые механики

Не переписывать без воспроизведённого дефекта:

- one shared page runtime;
- double-click/double-tap → like=true once, no navigation;
- drag/nested controls/keyboard exclusions;
- VK question CTA before related events;
- current Auth resilient direct/relay transport;
- P13N-00 quarantine;
- checked collection registry;
- Weather consumer default-off;
- YDB compact code default-off.

Для них обязательны regression tests на финальном integration SHA.

Smart Update/StaticSiteBuilder:

- использовать актуальный protected read-only audit из `main`;
- перед candidate проверить builder health и отсутствие active failure storm;
- не выдавать отсутствие аудита за healthy;
- candidate build должен завершиться terminal receipt;
- root promotion не выполнять.

Collections/Weather/YDB live enablement остаются отдельными tracks и не должны
раздувать этот slice. Их незавершённость отражается в итоговом отчёте, но не
служит оправданием для нерабочих profile/focus/artifact surfaces.

---

## 11. Обязательные тесты

### 11.1. Source/unit/contracts

- PR `#328` docs присутствуют и проиндексированы;
- implementation status честно отражает P13N-00/P13N-01A;
- focus registry запрещает anonymous feedback;
- `/profil/` route policy = noindex;
- profile не импортирует/renders event collections;
- feedback controls locked before auth;
- invite remains enabled before auth;
- safe `return_to` rejects external URLs;
- fixed focus cutoff boundary;
- diagnostic redaction;
- storage budgets;
- signal separation;
- artifact count exactly 7;
- unique assets/stories/provenance;
- no legacy counts/placeholders;
- local migration/idempotency.

### 11.2. Focus browser suite

Viewport:

- desktop `1440×900`;
- mobile `390×844`;
- narrow `320px`;
- reduced motion.

Journeys:

1. invite → skip auth → site opens;
2. feedback block visible in locked state;
3. score/issue controls disabled and not focusable/activatable;
4. invite button/share/QR works while locked;
5. auth CTA opens existing email/Yandex flow;
6. cancel/skip returns to locked block;
7. session fixture auth returns and unlocks same block;
8. page score submits exactly once;
9. issue + optional screenshot produce truthful component receipts;
10. diagnostics attached and redacted;
11. session expiry/sign-out relocks block;
12. pending action never flushes under another account;
13. service NPS locked/unlocked states;
14. no anonymous feedback/outbox row;
15. no console errors/overflow.

### 11.3. Profile suite

```text
profile.route_noindex
profile.mobile_menu_links_to_profile_not_logout
profile.desktop_shortcut_opens_profile
profile.signed_out_local_mode
profile.signed_in_mode
profile.auth_handoff_returns_to_profile
profile.local_fresh_state
profile.local_stale_state
profile.static_only_state
profile.pending_actions_state
profile.copy_diagnostics_redacted
profile.reset_local_epoch
profile.logout_does_not_reset_profile
profile.no_backend_on_read_when_cached
profile.storage_budget_enforced
profile.does_not_render_event_collections
profile.does_not_copy_collection_manifests
```

### 11.4. Zero-backend suite

```text
p13n.calendar_50_pages_zero_backend
p13n.collections_20_pages_zero_backend
p13n.event_navigation_zero_backend
p13n.page_navigation_uses_cached_projection
p13n.projection_refresh_at_most_once_per_session
p13n.stale_projection_backend_down_static_usable
favorites.navigation_zero_backend_when_cached
hidden.collection_navigation_zero_backend_when_cached
```

Assertions:

- YDB calls = `0`;
- Supabase data/profile calls = `0`;
- Auth refresh only if actually due;
- storage growth bounded;
- no second collection/profile cache;
- no page-view firehose.

### 11.5. Artifact/onboarding suite

- exact seven registry;
- all seven cards/assets/stories;
- all seven placements reachable;
- `0 → 7` progress without duplicate;
- reload persists local state;
- first hint → find → story → collection;
- keyboard/screen-reader names;
- no placeholder copy;
- no runtime provider/LLM request;
- profile does not contain collection.

### 11.6. Existing regressions

- event card gesture acceptance;
- VK question CTA resolver/render;
- page runtime inventory;
- P13N production-off characterization;
- static collection registry;
- preview/production/browser release gates.

### 11.7. Auth test policy

Обычные authenticated tests:

```text
session_fixture
/auth/v1/otp = 0
external mail issue/send/receipt = 0/0/0
```

Real-mail OTP запускать только при изменении самого issue/delivery/template/code
input/callback/mobile-system path. Изменение return-to wrapper и feedback lock не
является основанием многократно рассылать OTP.

---

## 12. GitHub Actions и стабильная сборка

### Gate 1 — source/contracts

Terminal PASS:

- relevant Python/Node unit suites;
- focus scenario registry lint;
- profile/P13N contracts;
- seven-artifact content/asset contract;
- docs/routes/index lint;
- source guard.

### Gate 2 — real static build

Terminal PASS:

- Astro production/preview build;
- `check:preview`;
- page/runtime inventory;
- personalization route/source guard;
- existing browser release gate;
- focused Chromium focus/profile/artifact journeys;
- zero-backend network assertions.

Required job `SKIPPED` не считается PASS.

### Gate 3 — immutable candidate

Только после Gate 1–2:

1. build exact head SHA;
2. publish immutable noindex candidate;
3. включить profile, feedback lock, auth handoff, exact seven artifacts и
   onboarding;
4. record build ID, repo SHA, event snapshot hash, artifact registry hash;
5. independent readback;
6. production root/stable ICS unchanged.

### Gate 4 — public cross-browser smoke

На exact public candidate:

- invitation route 200;
- representative listing/event 200;
- `/profil/` 200;
- collection/results route 200;
- exact SHA marker;
- noindex/no-referrer;
- auth lock visible;
- invite active;
- one auth fixture unlock journey;
- one artifact journey;
- seven slots render;
- no overflow/broken assets/console error;
- Chromium, Firefox, WebKit terminal PASS.

WebKit не заменяет physical iOS; native mobile запускается только для реально
изменённого system-level flow.

### Gate 5 — owner handoff

Финальный ответ содержит:

- secret invitation URL;
- direct `/profil/` URL в candidate;
- direct collection URL;
- PR URL;
- exact commit SHA/build ID;
- Actions run URLs;
- browser results;
- storage/network summary;
- mail counters `0/0/0` либо точное обоснование protected OTP run;
- public root/stable ICS mutation statement;
- remaining external gates.

Bearer/token не коммитить в Git, PR, logs или Actions artifacts.

---

## 13. Рекомендуемое разбиение PR

### PR A — canonical truth sync

- PR `#328` docs integration;
- personalization README/status/indexes;
- corrected focus auth semantics;
- scenario registries;
- route/profile decision;
- launch readiness correction.

### PR B — profile + P13N-01A + feedback gate

- `/profil/`;
- mobile/desktop account links;
- diagnostics generator;
- local reset/status;
- zero-backend tests;
- feedback locked state;
- safe auth return;
- service NPS auth gate;
- component receipts.

### PR C — exact seven artifacts + onboarding

- inventory/hashes/assets/copy;
- registry;
- placements;
- collection/result pages;
- local/authenticated progress boundary;
- onboarding chain;
- accessibility/browser tests.

### PR D — integration candidate

- fresh-main integration;
- full GitHub Actions;
- immutable candidate;
- public cross-browser evidence;
- owner links.

Один PR с отдельными commits допустим, если diff остаётся управляемым. В любом
случае нужен один final candidate exact SHA.

---

## 14. Задача не завершена, если

- feedback block скрыт до auth;
- score выглядит активным, но после клика просит войти;
- controls disabled только CSS;
- auth CTA не открывает email/Yandex flow или не возвращает на страницу;
- invite/share тоже заблокирован вместе с feedback;
- anonymous feedback/outbox всё ещё создаётся;
- `/profil/` — пустая заглушка или ссылка на `/dlya-menya/`;
- mobile menu сохраняет primary logout;
- профиль показывает Favorites/hidden/artifact collections;
- profile view делает backend read на каждый вход;
- Supabase используется как default profile cache;
- legacy scorer переименован в target scorer;
- remote P13N writes включены без gate;
- registry содержит не 7 artifacts;
- часть artifacts `coming soon`/placeholder/unreachable;
- onboarding остаётся inert marker без первой рабочей chain;
- обязательные Actions красные/skipped;
- candidate опубликован до CI;
- candidate URL отсутствует или ведёт на другой SHA;
- public root промотирован без разрешения.

---

## 15. Формат итогового отчёта кодового агента

```markdown
# Итог: READY / NOT READY FOR OWNER REVIEW

## Exact identity
- base main SHA:
- branch:
- head SHA:
- PRs:
- candidate build ID:
- invitation URL:
- profile URL:
- collection URL:

## Focus feedback
- invited site without auth:
- block visible while signed out:
- feedback controls disabled:
- invite active:
- auth CTA email/Yandex:
- safe return to block:
- session fixture unlock:
- anonymous writes:
- component receipts:

## Profile
- route/noindex:
- mobile account link:
- logout location:
- local/synced/stale states:
- diagnostics redaction:
- local reset:
- backend reads when cached:
- event collections absent:

## Personalization ecology
- P13N-00 preserved:
- P13N-01A scope:
- profile projection bytes:
- aggregate storage bytes:
- navigation YDB calls:
- navigation Supabase data/profile calls:
- remote writes:
- Supabase profile cache:

## Seven artifacts
| # | artifact_id | name | source SHA | story | placement | accessibility | browser |
|---|---|---|---|---|---|---|---|
| 1 | ... | ... | ... | PASS | PASS | PASS | PASS |
...
| 7 | ... | ... | ... | PASS | PASS | PASS | PASS |

## Onboarding
- first hint:
- first placement:
- first find:
- story:
- collection handoff:

## GitHub Actions
- run URLs:
- required jobs:
- Chromium:
- Firefox:
- WebKit:
- mail issue/send/receipt:

## Publication safety
- candidate noindex:
- exact SHA marker:
- independent readback:
- public root mutation:
- stable ICS mutation:

## Remaining external gates
- only genuinely external/non-core items
```

Каждый `PASS` сопровождается exact evidence. Если Gate 1–4 не прошли или ссылки
не созданы, итоговый статус — `NOT READY FOR OWNER REVIEW`.
