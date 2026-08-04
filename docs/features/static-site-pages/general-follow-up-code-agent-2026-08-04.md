# PROMPT для кодового агента — завершить генеральную доработку статического сайта и первую коллекцию из 7 артефактов

Дата корректировки: **2026-08-04**  
Репозиторий: `onedayonemasterpiece/events-bot-new`  
Проверенный baseline на момент подготовки: `main@ccafa55a2c23e4691738bf2aefc2e5384892668b`  
Режим работы: **реализация, интеграция, автотесты и публикация immutable review candidate**, а не дополнительный docs-only анализ.

> Этот документ полностью заменяет предыдущую версию handoff из этого файла.
> Предыдущая версия ошибочно протащила модель anonymous-first feedback и ошибочно
> отложила готовую первую коллекцию артефактов. Не использовать прежние выводы.

---

## 0. Обязательный конечный результат

Нужно не ещё раз спроектировать систему и не закончить работой с выключенными
флагами. Итог этой задачи — **работающая, включённая в review candidate версия
статического сайта для фокус-группы**, которую владелец продукта сможет открыть
по HTTPS-ссылке и проверить в разных браузерах.

К моменту handoff владельцу должны одновременно выполняться условия:

1. приглашённый пользователь открывает сайт без обязательного входа;
2. обратная связь, page score, общий NPS и всё, что относится к розыгрышу,
   доступны только после авторизации по email или через Яндекс;
3. приглашённый пользователь может поделиться ссылкой или QR-кодом, чтобы другой
   человек тоже вошёл в фокус-группу;
4. первая коллекция содержит **ровно 7 готовых артефактов** из указанного ниже
   reference-каталога;
5. все 7 артефактов реально размещены, находимы, имеют рабочие состояния,
   изображения и описания; это не `coming soon`, не demo-заглушки и не пустые
   ячейки;
6. коллекция и первая находка вплетены в onboarding, а страница прогресса /
   результата доступна из фокус-hub и из onboarding-перехода;
7. описания всех 7 артефактов проверены по фактам, источникам, русскому языку и
   принятому редакционному стилю проекта;
8. базовые GitHub Actions завершились успешно **до** публикации review candidate;
9. после публикации candidate прошёл public browser smoke, и только затем
   владельцу передана секретная HTTPS-ссылка;
10. public production root и стабильные ICS не промотируются без отдельного
    прямого разрешения владельца.

**Полный розыгрыш, выбор победителя, alternate, уведомление победителя и выдача
приза в эту задачу не входят.** Это не разрешает откладывать коллекцию,
размещения, onboarding, авторизованный прогресс или страницу результата.

---

## 1. Последние решения владельца продукта — высший приоритет

При любом конфликте использовать именно эти решения.

### 1.1. Доступ и авторизация фокус-группы

Правильная модель:

```text
валидное приглашение / QR
→ локальная метка фокус-группы
→ пользователь получает обычный сайт
→ авторизация не обязательна для просмотра и использования афиши
```

Но:

```text
page score / текст / screenshot / общий NPS / участие в розыгрыше
→ только после подтверждённой авторизации по email или через Яндекс
```

Дополнительно:

```text
приглашённый пользователь
→ может открыть экран приглашения
→ может поделиться ссылкой
→ может показать или отправить QR-код
→ новый пользователь проходит тот же вход в фокус-группу
```

Запрещено:

- создавать silent anonymous Supabase Auth session ради feedback;
- принимать anonymous `auth.uid()` как достаточную identity для feedback/NPS;
- отправлять feedback, NPS или prize-related receipts до email/Yandex auth;
- выдавать local marker за серверное участие в розыгрыше;
- блокировать саму афишу требованием войти.

### 1.2. Первая коллекция артефактов

Фактический источник первой коллекции:

```text
/home/dev/projects/events-bot-new/
  docs/features/static-site-pages/references/artefact-collection-1
```

Решение владельца:

- в первой коллекции **7 артефактов**;
- они уже подготовлены и не являются будущей гипотезой;
- эта коллекция должна быть включена для фокус-группы;
- артефакты являются важной частью продукта и onboarding;
- должна быть доступна страница результата/коллекции;
- для всех артефактов должны существовать полноценные описания;
- описания должны пройти редакционную проверку;
- коллекцию нельзя заменить функциональными badges, «миссиями» или 12
  исследовательскими действиями.

Каталог reference-материалов читать **из локального root checkout только
read-only**. Если он не отслеживается Git, перенести нужные публичные assets и
контент в чистый worktree осознанно, с SHA-256 и provenance; не делать `git add -A`
в грязном root checkout.

### 1.3. Review candidate

Владельцу нужна ссылка на фактически собранный сайт, а не только PR.

Ссылка передаётся только после:

1. успешных базовых GitHub Actions;
2. immutable build exact SHA;
3. успешной публикации noindex candidate;
4. public browser smoke этого же candidate;
5. проверки, что candidate действительно содержит 7 артефактов и правильный
   focus access/auth flow.

---

## 2. Нормативная иерархия

При конфликте использовать источники в таком порядке:

1. текущие решения владельца из раздела 1;
2. содержимое
   `docs/features/static-site-pages/references/artefact-collection-1`;
3. фактический код и generated output свежего `origin/main`;
4. совмещённые требования генеральной доработки;
5. `docs/features/static-site-onboarding/README.md` и принятая связь onboarding →
   первый артефакт → история → коллекция;
6. `docs/features/hero-talk/README.md`, если он уже находится в актуальном
   целевом baseline;
7. документы фокус-группы после их исправления этой задачей;
8. старые prototype/docs branches только как доноры отдельных решений.

Следующие источники **не являются текущей продуктовой истиной**, если
противоречат разделу 1:

- PR `#250` и anonymous-first формулировки, попавшие из него в `main`;
- текущий `focus-group-release-scenarios.v1.yml` с anonymous feedback;
- `FOCUS_EGG_DEFINITIONS` на 12 функциональных «пасхалок»;
- текущий `ARTIFACT_COLLECTION_SLOTS` на 5 ячеек;
- stale branch `feature/static-site-artifacts-registry-20260727` с 8 draft
  artifacts;
- любые числа `12`, `10 из 12`, `8`, `5 из 8` применительно к первой текущей
  коллекции, если они не содержатся в owner-approved reference-каталоге.

---

## 3. Фактическая отправная точка и конфликтующие реализации

Перед изменениями воспроизвести и зафиксировать эти факты на свежем `main`.

### 3.1. Focus access

В текущем `FocusGroupInviteIntake.astro` уже есть полезная основа:

- можно выбрать email;
- можно выбрать Яндекс;
- можно нажать `Продолжить без подтверждения`;
- текст объясняет, что афиша откроется, а подтверждение понадобится позже.

Это соответствует новому решению в части доступа к сайту и должно быть
сохранено.

Текущий `FocusGroupLabPanel.astro` уже вызывает `requireSession()` перед
page score и issue submission. Это ближе к правильной модели, чем anonymous-first
документы. Его надо довести до ясного auth-gated UX, а не заменять anonymous
session.

### 3.2. Ошибочные документы и scenario registry

В `main` ошибочно записано, что anonymous user может:

- ставить page score;
- ставить общий NPS;
- отправлять текст и screenshot;
- создавать server artifact receipts.

Эти утверждения есть как минимум в:

- `docs/features/static-site-pages/focus-group.md`;
- `docs/features/static-site-pages/focus-group-release/README.md`;
- `docs/features/static-site-pages/focus-group-release/status.md`;
- `docs/features/static-site-pages/focus-group-release/nps-ui.md`;
- `docs/features/static-site-pages/focus-group-release/testing.md`;
- `docs/features/static-site-pages/focus-group-release/prize-rules.md`;
- `docs/testing/focus-group-release-scenarios.v1.yml`;
- связанных release/checklist документах;
- открытом launch-readiness PR `#324`, если он ещё не исправлен/не слит.

Все они должны быть синхронно исправлены. Нельзя исправить только prose и
оставить старую machine-readable truth.

### 3.3. Три несовместимые artifact-модели

На текущем baseline существуют три разные модели:

1. `site/src/lib/artifacts.mjs` и `/artefakty/`:
   - один `Янтарный космонавт`;
   - всего 5 ячеек;
   - четыре placeholder `future_*`;
   - feature скрыта в ordinary production;
2. `site/src/lib/focus-easter-eggs.ts` и
   `/fokus-gruppa/kollektsiya/`:
   - 12 функциональных achievements `FG-E01…FG-E12`;
   - demo states и demo scoring;
   - это не семь культурных артефактов;
3. branch `feature/static-site-artifacts-registry-20260727`:
   - 8 draft artifacts;
   - route/registry prototype;
   - источник может быть использован только как технический донор.

Нужно оставить **один канонический artifact domain для первой коллекции из 7
объектов**, а не поддерживать три конкурирующие системы.

---

## 4. Режим выполнения и безопасность worktree

Работать от свежего `origin/main` в чистом worktree.

Рекомендуемая ветка:

```text
agent/static-site-focus-artifacts-v2-20260804
```

Правила:

- не изменять грязный root checkout;
- reference-каталог из `/home/dev/projects/...` читать read-only;
- старые ветки не сливать wholesale;
- сначала `git range-diff` / `git diff` / file-level archaeology;
- переносить только актуальные компоненты и контракты;
- не запускать public-root promotion;
- не отправлять реальные OTP-письма, если код самого OTP/mail/mobile-input не
  менялся;
- session fixture использовать для обычных authenticated E2E;
- не включать live YDB scheduler/RU без отдельного canary contract;
- не выдавать started/background/skipped workflow за PASS.

Допустимо разделить код на несколько PR, но владелец должен получить **один
интегрированный candidate exact SHA**. Нельзя закончить набором несобранных
веток.

---

## 5. Фаза A — инвентаризация exact 7 артефактов

Первое действие — прочитать весь каталог:

```text
docs/features/static-site-pages/references/artefact-collection-1
```

Создать evidence-документ, например:

```text
docs/features/static-site-pages/artifacts/
  collection-1-inventory-2026-08-04.md
```

В нём для каждого из 7 объектов зафиксировать:

| Поле | Требование |
|---|---|
| `artifact_id` | immutable, kebab-case, без временного номера как identity |
| public name | точное имя из reference, без придуманного ребрендинга |
| collection order | 1…7 |
| source files | exact relative paths |
| source hashes | SHA-256 каждого входного файла |
| visual asset | format, dimensions, alpha/background, source |
| short description | для карточки/результата |
| full story | для detail/dialog/page |
| provenance | фактический источник истории |
| rights status | confirmed / focus-candidate-only / blocker for public root |
| placement | desktop/mobile/accessible anchor |
| onboarding role | first hint / continuation / completion |
| editorial result | PASS или точная правка |

Жёсткие проверки:

- найдено ровно 7 artifact identities;
- нет восьмого объекта из stale registry;
- нет 12 функциональных achievements;
- нет placeholder slots;
- один и тот же объект не продублирован из-за разных изображений/названий;
- ни один reference-файл не потерян без записанного disposition.

Если физическое содержимое каталога неожиданно не равно семи объектам, не
переключаться на старый registry. Зафиксировать точный конфликт и определить,
какой файл является duplicate/supporting asset, сохраняя owner truth `7`.

---

## 6. Фаза B — единый registry и data contract

Создать один canonical registry первой коллекции. Название файла выбирается по
актуальной структуре проекта, например:

```text
site/src/data/artifact-collection-1.json
```

Минимальная схема:

```ts
type ArtifactCollection = {
  schema_version: "artifact-collection-v1";
  collection_id: string;
  collection_version: string;
  title: string;
  summary: string;
  artifact_ids: string[]; // exact length 7
  artifacts: Array<{
    artifact_id: string;
    slug: string;
    public_name: string;
    short_description: string;
    full_story: string;
    source_label: string;
    source_url?: string;
    source_note?: string;
    image: {
      src: string;
      width: number;
      height: number;
      alt: string;
      provenance_ref: string;
    };
    placement_bundle: {
      version: string;
      mobile_anchor: string;
      desktop_anchor: string;
      accessible_anchor: string;
    };
    onboarding_role: "first" | "continuation" | "completion";
  }>;
};
```

Точные поля можно адаптировать к существующим conventions, но инварианты
обязательны:

- registry содержит 7 и только 7 элементов;
- UI ничего не классифицирует по тексту;
- все маршруты и placements используют exact IDs из registry;
- один versioned collection source of truth;
- no runtime LLM;
- public JSON, если нужен, не раскрывает секретные placement coordinates;
- private participant progress не попадает в CDN HTML/JSON;
- generated validators fail closed при missing asset, duplicate ID, unknown
  placement, пустой story или count != 7.

Миграция legacy:

- current `amber_cosmonaut` сопоставить с exact artifact identity из reference,
  если это тот же объект;
- старый localStorage мигрировать идемпотентно;
- `FOCUS_EGG_DEFINITIONS` не использовать как artifact registry;
- функциональные research achievements при необходимости оставить отдельными
  telemetry/capability signals, но не показывать как артефакты коллекции;
- старые 5/8/12 counts убрать из UI, docs, tests и scenario registry.

---

## 7. Фаза C — контент и редакционный аудит семи историй

Проверить существование описания у каждого объекта, а не только title/hint.

Для каждого из 7 нужны:

1. короткое описание карточки;
2. полная история;
3. понятная связь с Калининградской областью;
4. источник или provenance note;
5. доступный alt text изображения;
6. отсутствие недоказанных фактов;
7. отсутствие искусственного рекламного восторга;
8. естественный русский язык;
9. отсутствие канцелярита, LLM-клише и повторяющихся шаблонных вступлений;
10. ясный переход к следующему действию/коллекции без давления.

Редакционная иерархия:

1. если к моменту реализации существует owner-approved editorial style v1 или
   общий versioned editorial package — использовать его;
2. research brief `docs/editorial/README.md` сам по себе не объявлять финальным
   стандартом;
3. при отсутствии финального v1 использовать безопасное ядро: фактическая
   точность, естественный русский, спокойная дружелюбность, конкретика,
   локальная наблюдательность, минимум рекламных клише;
4. не применять иронию к auth, NPS, privacy, ошибкам, недоступности или
   prize-related состояниям.

Создать отчёт редакционной проверки с exact before/after для изменённых фраз.
Нельзя написать `style review passed` без проверки всех 7.

---

## 8. Фаза D — рабочие placements и находка

Все 7 артефактов должны быть реально достижимы в candidate.

Использовать placements из reference-каталога. Если reference содержит
отдельные desktop/mobile варианты одного объекта, это один `artifact_id`, а не
два элемента коллекции.

Инварианты placement:

- assignment стабилен в рамках collection/placement version;
- reload не reroll-ит объект;
- изменение порядка карточек не переносит объект случайно;
- viewport switch не создаёт новую находку;
- touch, mouse, keyboard и screen reader имеют эквивалентный путь;
- hover, motion, точное наведение или второй девайс не обязательны;
- `prefers-reduced-motion` не скрывает объект;
- артефакт не перекрывает event facts, ticket CTA, navigation или feedback;
- артефакт не выглядит как событие, медальон организатора или платная реклама;
- найденное состояние остаётся доступным как `Найдено — открыть историю`;
- повторная активация идемпотентна;
- all-seven browser journey не содержит unreachable slot.

Для каждого placement создать machine-readable inventory и browser test.

---

## 9. Фаза E — состояние коллекции и авторизация

Нужно различать три состояния без anonymous Supabase Auth.

### 9.1. Invited local, без email/Yandex

Доступно:

- весь сайт фокус-группы;
- placements и истории артефактов;
- локальный прогресс на этом устройстве;
- страница коллекции;
- приглашение другого пользователя через link/QR.

Недоступно как server action:

- page score;
- text feedback;
- screenshot upload;
- service NPS;
- prize eligibility/application;
- server-owned collection receipt, используемый для розыгрыша.

UI должен честно разделять:

```text
Найдено на этом устройстве
```

и

```text
Учтено в подтверждённом участии
```

До входа не показывать второе состояние.

### 9.2. Verified email/Yandex

После подтверждённого входа:

- local finds мигрируют/синхронизируются идемпотентно;
- появляется server-owned progress;
- feedback и NPS разрешаются;
- можно подготовить будущую raffle eligibility projection;
- не создаётся автоматическая заявка или выигрыш.

Минимальный server contract для artifact receipt:

- owner = текущий verified `auth.uid()`;
- unique `(collection_version, artifact_id, owner)`;
- idempotency key;
- exact placement/version;
- server timestamp;
- RLS owner-only read;
- узкий RPC для записи;
- direct/relay transport с idempotent replay;
- no raw browsing history, pointer coordinates или arbitrary text.

Перед созданием новой таблицы/RPC проверить существующие migrations и reuse
возможного общего receipt/outbox contract. Не создавать второй transport stack.

### 9.3. Sign-out и повторный вход

- выход из аккаунта не удаляет local focus marker;
- local collection остаётся доступна;
- server progress не показывается как доступный без session;
- повторный вход того же user восстанавливает server state;
- другой user на том же браузере не наследует server progress первого;
- local→server merge не создаёт duplicate receipts.

---

## 10. Фаза F — страница коллекции и страница результата

Нужно устранить дублирование между:

- `/artefakty/`;
- `/fokus-gruppa/kollektsiya/`;
- stale child route из branch на 8 объектов.

Выбрать одну понятную IA над единым registry. Предпочтительный минимальный путь:

```text
/artefakty/                         — вход и текущая коллекция
/artefakty/kollektsii/<slug>/       — прогресс и семь историй
/zakrytaya-afisha/                  — summary + CTA в коллекцию
/fokus-gruppa/kollektsiya/          — redirect/alias на canonical page
```

Адаптировать, если exact reference уже фиксирует другой route, но не оставлять
две разные коллекции.

Страница результата должна показывать:

- название первой коллекции;
- `N из 7`;
- семь exact slots;
- найденные элементы с изображением, названием и открытием истории;
- ненайденные элементы в предусмотренном reference-состоянии;
- local/server distinction;
- auth CTA для синхронизации и prize-related participation;
- отсутствие leaderboard/draw/winner simulation;
- ссылку назад в афишу;
- доступность с keyboard/screen reader;
- responsive layout без horizontal overflow.

Удалить:

- `12 маршрутов`;
- demo `2 из 11`;
- participation score `18/40` как будто это текущий результат;
- hardcoded `FG-E01`/`FG-E08` found;
- `5 из 8` threshold;
- placeholder `future_maritime`, `future_nature`, `future_city`, `future_taste`;
- `Поделиться артефактом · скоро`, если share входит в готовый reference;
  если share не предусмотрен, убрать кнопку, а не показывать сломанное обещание.

---

## 11. Фаза G — артефакты как обязательная часть onboarding

Текущий `StandardOnboardingPlacementContext` — только inert seam. Этого
недостаточно для focus candidate.

Нужно реализовать рабочую bounded chain:

```text
пользователь получил базовую пользу от афиши
→ спокойная точная подсказка о первом артефакте
→ переход/достижение exact placement
→ находка
→ короткая история
→ CTA «Открыть коллекцию»
→ страница N из 7
→ следующая подсказка без навязчивого tour
```

Требования:

- без mandatory welcome wall;
- без блокировки афиши;
- одна новая задача за раз;
- первая подсказка точная, а не загадка без ориентира;
- dismiss/suppress сохраняется;
- hint не повторяется на каждом page view;
- artifact find не становится taste/personalization signal;
- не выдавать Клуб друзей или розыгрыш за уже открытый, если их gates не
  завершены;
- использовать precompiled static copy; no runtime LLM;
- focus candidate должен включать chain, а browser test должен её пройти.

Если Hero-talk runtime ещё не реализован, не строить ради этой задачи весь HT-0…
HT-10. Использовать существующую placement boundary и минимальный reusable
renderer, совместимый с будущим Hero-talk, но реально работающий сейчас.

---

## 12. Фаза H — исправить focus flow без anonymous-first

### 12.1. Intake

Сохранить `Продолжить без подтверждения`, но исправить copy/state:

- после skip пользователь получает сайт;
- done-screen не обещает, что он уже может отправлять оценки или участвовать в
  розыгрыше;
- показать ненавязчивый путь `Подключить email или Яндекс позже`;
- verified done-screen может сообщать о доступности feedback/NPS.

### 12.2. Page score и issue feedback

На supported pages:

- local focus marker показывает Lab presence;
- без verified session показывается compact locked state и CTA входа;
- число нельзя принять локально и выдать за отправленное;
- после session открывается шкала `0–10` и issue form;
- idempotent outbox используется только для authenticated record;
- при both routes down сохраняется честное pending authenticated action;
- no anonymous user creation.

Существующий `FocusGroupLabPanel.astro` можно переиспользовать, но убрать
ложную модель, где UI сначала принимает действие, а потом неожиданно требует
подтверждение. Auth boundary должна быть понятна до submit.

### 12.3. Общий NPS

Один настоящий service NPS block на `/zakrytaya-afisha/`:

- только для verified email/Yandex participant;
- не prototype «остаётся в текущей вкладке»;
- отдельный `service_revision`;
- шкала `0–10`;
- optional comment;
- idempotent server write;
- не повторяется на каждой странице;
- значение NPS не влияет на шанс/результат;
- unverified participant видит auth CTA, но не форму отправки.

### 12.4. Invite share / QR

Должно работать и до, и после auth:

- Web Share при наличии;
- copy fallback;
- рабочий QR;
- ссылка содержит exact candidate/base path;
- hash/token очищается из адресной строки после intake;
- повторное приглашение не сбрасывает текущую сессию/прогресс;
- share не влияет на результат фокус-группы;
- browser test открывает созданную ссылку во втором context и подтверждает
  вступление.

### 12.5. Fixed cutoff

Исправить rolling `30 days` в runtime. Источник истины:

```text
2026-08-31T18:00:00+02:00
Europe/Kaliningrad
```

Проверить before/at/after boundary и PWA relaunch.

---

## 13. Фаза I — синхронно исправить документацию и machine-readable truth

Повысить версию focus scenario registry, например `v6`.

Обязательная semantics:

```yaml
identity:
  invited_local_can_use_site: true
  anonymous_supabase_auth: forbidden
  verified_provider_any_of: [email, custom:yandex]

feedback:
  page_score:
    verified_identity_required: true
  text:
    verified_identity_required: true
  screenshot:
    verified_identity_required: true
  service_nps:
    verified_identity_required: true

artifacts:
  collection_count: 7
  local_progress_before_auth: true
  server_receipt_requires_verified_identity: true
  full_collection_required_in_focus_candidate: true

raffle:
  verified_identity_required: true
  implementation_state: deferred
  winner_selection_state: deferred
```

Удалить/исправить сценарии:

- `anonymous.session_created`;
- `anonymous.feedback_allowed`;
- `anonymous_nps`;
- `anonymous artifact server receipt`;
- `anonymous_to_verified merge` как обязательную архитектуру Supabase Auth.

Добавить сценарии:

- `focus.access.invited_without_auth`;
- `focus.auth.feedback_gate`;
- `focus.auth.nps_gate`;
- `focus.invite.share_link`;
- `focus.invite.share_qr`;
- `artifacts.collection.exact_seven`;
- `artifacts.collection.all_reachable`;
- `artifacts.collection.result_page`;
- `artifacts.collection.local_progress`;
- `artifacts.collection.verified_receipt`;
- `artifacts.collection.local_to_verified_sync`;
- `artifacts.onboarding.first_hint`;
- `artifacts.onboarding.collection_handoff`;
- `artifacts.editorial.seven_descriptions`;
- `artifacts.accessibility.route_matrix`.

Синхронно обновить release readiness/checklist. Не оставлять PR `#324` с
`anonymous-first` как критическим путём.

Старый PR `#250` пометить superseded текущим owner decision, если он ещё открыт.

---

## 14. Фаза J — остальные разрывы генеральной доработки

Работу по артефактам и focus flow выполнить первой. Затем закрыть или честно
развести остальные контуры.

### 14.1. Smart Update / StaticSiteBuilder

PR `#322` пока не доказал production health: Fly probe был skipped без
`FLY_API_TOKEN`.

Нужно:

1. получить/использовать app-scoped `FLY_API_TOKEN` через protected Actions
   secret;
2. завершить read-only аудит production SQLite и runtime logs за фиксированное
   24-часовое окно;
3. классифицировать запуски/ошибки/успехи и продуктовый результат;
4. проверить отсутствие повторной failure storm/starvation;
5. связать current main SHA, Fly SHA, candidate pointer и public root release;
6. не делать root promotion в рамках аудита.

Если секрет отсутствует, назвать конкретный blocker и не писать `Smart Update
healthy`. Это не должно блокировать локальную реализацию artifact candidate, но
не позволяет объявить весь production-аудит закрытым.

### 14.2. Подборки

Сохранить checked registry, но устранить преждевременное `R2 Done`:

- привести registry к каноническому списку `podborki.md`;
- все menu/catalog/sitemap surfaces должны читать registry;
- free/clubs links не обходят registry;
- каждый visible route существует в generated tree;
- blocked/deferred entries не дают битые ссылки;
- gastronomy не публикуется из пустого owner decision store;
- актуальные части PR `#314` переносить file-level, не wholesale;
- tests проверяют completeness, route existence и lifecycle.

### 14.3. Transport / Yandex resilience

- feedback/NPS/artifact receipts используют общий resilient transport;
- no false success;
- selected-once и idempotent-replay policies не смешиваются;
- normal/direct-down/relay-down/both-down покрыты session fixture;
- hosted critical smoke выполняется на exact candidate;
- real OTP не используется для обычных feature tests.

### 14.4. Weather и YDB

Это отдельные operational tracks. Не использовать их незавершённость как повод
оставить артефакты выключенными.

- Weather candidate может оставаться вне focus core только при честном отдельном
  статусе; нельзя называть R4 complete, пока producer/canary не выполнены.
- YDB scheduler/RU не включать без documented canary; нельзя называть live
  compaction complete без server RU/observation.

---

## 15. Автотесты: обязательный минимум

### 15.1. Быстрые contract/unit tests

- registry parses;
- exact artifact count = 7;
- all IDs/slugs unique;
- all assets exist and dimensions are valid;
- all 7 short/full descriptions non-empty;
- all 7 provenance records present;
- no legacy `future_*` slots;
- no `FG-E01…FG-E12` rendered as collection items;
- no count 5/8/10/12 in current collection surfaces;
- localStorage migration idempotent;
- authenticated receipt idempotent;
- fixed cutoff boundary;
- scenario registry lint.

### 15.2. Browser tests

Desktop: `1440×900`  
Mobile: `390×844`  
Дополнительно: narrow `320px` и reduced motion.

Обязательные journeys:

1. invite → skip auth → site opens;
2. unverified participant sees site and artifact onboarding;
3. unverified participant cannot submit page score;
4. unverified participant cannot submit service NPS;
5. email/Yandex session fixture unlocks feedback/NPS;
6. invite link copy works;
7. QR encodes exact candidate invitation;
8. second browser context joins through shared link;
9. first artifact hint leads to actual placement;
10. find opens story and collection;
11. all seven placements can be reached;
12. progress changes `0 → 7` without duplicates;
13. reload preserves local progress;
14. verified sync creates exactly seven server receipts;
15. sign-out hides server state but keeps local collection;
16. result page has no overflow, broken media or placeholder copy;
17. keyboard and screen-reader names exist for all seven;
18. no console error;
19. no third-party runtime LLM/provider request;
20. no accidental public indexability of focus candidate.

### 15.3. Cross-browser GitHub Actions

После базовой suite на exact build:

- Chromium;
- Firefox;
- WebKit;

Минимум:

- invitation/access;
- collection page;
- one complete artifact journey;
- seven-card/slot rendering;
- auth gate;
- share/copy fallback;
- no horizontal overflow.

Playwright WebKit не выдавать за physical iOS acceptance, но он обязателен как
cross-browser browser-engine smoke. Native Android/iOS запускать только для
изменённых system-level flows.

### 15.4. Auth test policy

Обычные authenticated tests:

```text
session_fixture
/auth/v1/otp = 0
external mail = 0
```

Real-mail OTP запускать только если изменены:

- OTP issue;
- email delivery/routing/template;
- code input;
- callback/verify;
- mobile keyboard/system flow.

Если эти участки не менялись, итоговый receipt должен показать `0/0/0`.

---

## 16. GitHub Actions и публикация review candidate

Порядок обязателен.

### Gate 1 — source/contract

До публикации candidate должны быть terminal PASS:

- Python CI relevant scope;
- Node/unit relevant scope;
- artifact registry/content contract;
- focus scenario registry lint;
- Astro build;
- `check:preview`;
- page/runtime route inventory;
- existing static browser release gate;
- focused artifact/focus Chromium journey.

Skipped required job = не PASS.

### Gate 2 — immutable candidate

После Gate 1:

1. собрать exact head SHA;
2. опубликовать immutable noindex secret candidate;
3. включить в candidate все 7 артефактов и focus onboarding;
4. не использовать default-off flag, который превращает страницу в
   `недоступна в этой сборке`;
5. записать build ID, source SHA, artifact registry hash, event snapshot hash;
6. выполнить независимый readback;
7. production root и stable ICS не менять.

### Gate 3 — public candidate smoke

На public HTTPS candidate:

- exact SHA marker совпадает;
- root/invitation/collection routes отвечают 200;
- `noindex,nofollow,noarchive,nosnippet`;
- `no-referrer`;
- assets загружаются;
- seven-artifact contract проходит;
- browser console/network clean;
- Chromium/Firefox/WebKit smoke terminal PASS.

### Gate 4 — owner handoff

Только после Gate 3 финальный ответ владельцу содержит:

- secret invitation URL;
- direct collection/results URL в том же candidate;
- PR URL;
- exact commit SHA;
- GitHub Actions run URLs;
- short test summary;
- exact statement: public root changed or unchanged;
- known non-core live gates.

Bearer/token не коммитить в PR, docs, artifacts или logs. Передать владельцу
отдельно в финальном сообщении.

---

## 17. Запрещённые способы «закрыть» задачу

Задача не считается выполненной, если результат — любой из вариантов:

- docs-only PR;
- registry с семью строками, но без placements;
- placements есть, но candidate показывает `коллекция недоступна`;
- один рабочий артефакт и шесть placeholder;
- 12 функциональных achievements переименованы в 7 артефактов;
- stale 8-item registry выдан за owner-approved collection;
- описания не проверены;
- NPS/feedback продолжают anonymous server writes;
- site требует login до открытия афиши;
- share/QR только нарисованы и не открывают второй context;
- tests выполнены локально, но GitHub Actions красные/не запускались;
- candidate опубликован до CI;
- ссылка ведёт на старый SHA;
- candidate link отсутствует;
- `STARTED_BACKGROUND`, `SKIPPED`, open PR или mock считается PASS;
- public root промотирован без разрешения.

---

## 18. Рекомендуемое разбиение реализации

Без лишней архитектуры, но с управляемым diff.

### PR A — исправление canonical focus truth

- owner decisions;
- docs/scenario registry v6;
- fixed cutoff;
- auth-gated feedback/NPS;
- invite/share/QR tests;
- исправление launch-readiness anonymous-first пунктов.

### PR B — exact seven-artifact collection

- source inventory/hashes;
- registry/content/assets;
- legacy migration;
- placements;
- collection/results page;
- local + verified progress;
- content/editorial report;
- onboarding chain;
- focused tests.

### PR C — integration candidate

- merge fresh main;
- reconcile shared layout/navigation;
- full build/check;
- GitHub Actions;
- immutable candidate;
- public cross-browser evidence;
- owner handoff links.

Если изменения достаточно компактны, допустим один PR с отдельными commits, но
обязателен один интегрированный exact-SHA candidate.

---

## 19. Формат итогового отчёта кодового агента

Не писать общий optimistic summary. Использовать точную структуру.

```markdown
# Итог

## Exact identity
- branch:
- commit SHA:
- PR:
- candidate build ID:
- candidate invitation URL:
- candidate collection URL:

## Seven artifacts
| # | artifact_id | name | source hash | description | placement | browser |
|---|---|---|---|---|---|---|
| 1 | ... | ... | ... | PASS | PASS | PASS |
...
| 7 | ... | ... | ... | PASS | PASS | PASS |

## Focus model
- site without auth: PASS/FAIL
- feedback requires email/Yandex: PASS/FAIL
- NPS requires email/Yandex: PASS/FAIL
- invite share link: PASS/FAIL
- invite QR: PASS/FAIL
- no anonymous Supabase Auth: PASS/FAIL

## Onboarding
- first hint:
- first find:
- story:
- collection handoff:
- result page:

## GitHub Actions
- run:
- required jobs:
- mail issue/verify/receipt:
- Chromium/Firefox/WebKit:

## Publication safety
- candidate noindex:
- public root mutation:
- stable ICS mutation:

## Remaining external gates
- only genuinely external/non-core items
```

`PASS` должен сопровождаться exact evidence. Если candidate URL не создан или
Gate 1–3 не прошли, итоговый статус — `NOT READY FOR OWNER REVIEW`, а не
`готово`.
