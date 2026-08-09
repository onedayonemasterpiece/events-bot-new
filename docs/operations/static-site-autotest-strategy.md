# Стратегия автотестирования статического сайта и данных

> **Статус:** стратегический, канонический и нормативный документ проекта.
> **Область:** Astro static site, PWA, данные статического артефакта, Supabase/Yandex connectivity и критические пользовательские пути.
> **Связь с релизом:** обязательный companion к
> [`docs/features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md)
> и [`release-autotest-gates.md`](../features/static-site-pages/release-autotest-gates.md).
> **Машиночитаемый реестр:**
> [`docs/testing/static-site-autotest-scenarios.v1.yml`](../testing/static-site-autotest-scenarios.v1.yml).
> **Auth session fixture:**
> [`docs/testing/static-site-auth-session-fixture.md`](../testing/static-site-auth-session-fixture.md).
> **Yandex dependency resilience:**
> [`docs/operations/yandex-dependency-resilience.md`](yandex-dependency-resilience.md).
> **Первый implementation handoff:**
> [`docs/testing/static-site-autotest-codex-prompt.md`](../testing/static-site-autotest-codex-prompt.md).

## 1. Решение

Проект использует четыре уровня доказательства, но запускает только те уровни,
которые дают дополнительную информацию для конкретного изменения.

1. **L0 — artifact/data contracts.** Быстрые проверки файлов, manifest, catalog,
   JSON/ICS/SEO, связности данных и обязательного содержимого без браузера.
2. **L1 — browser runtime.** Playwright в Chromium/Firefox/WebKit для DOM, CSS,
   JS, сетевых ошибок, геометрии, скриншотов и массового обхода страниц.
3. **L2 — mobile system integration.** Настоящий Android Emulator с Chrome и
   iOS Simulator с Mobile Safari. Appium переключается между web и native UI для
   клавиатуры, install UI, Launcher/SpringBoard, Share Sheet и lifecycle PWA.
4. **L3 — real-device certification.** Редкий внешний canary на физическом
   Android/iPhone для push/background/OEM/performance/hardware поведения.

Мобильный viewport в desktop Chromium не считается доказательством Android или
iPhone. WebKit-проект Playwright не считается доказательством native Mobile
Safari, Share Sheet или Home Screen.

Эмуляторы не применяются ко всему статическому каталогу. Массовые проверки
выполняют L0/L1. L2 используется только для системно значимых мобильных путей и
небольшой стратифицированной выборки page families.

Для функций **после входа** default — `session_fixture`: свежая настоящая
Supabase-сессия, выпущенная trusted setup без внешнего письма и ограниченная
одним worker/job. Real-mail OTP запускается только тогда, когда предметом
проверки является OTP issue, доставка, шаблон письма, Auth transport либо
email/OTP mobile input.

## 2. Проверенный baseline на 2026-08-03

Аудит выполнен по `main`, начиная со среза
`ba8ab078ba9894ccd5810045b1b8787ecb29d743` и последующих terminal receipts.

Уже существует:

- production/preview/secret artifact checks и release manifests;
- generated-candidate Chromium browser release gate;
- component/browser contracts PWA install copy и lifecycle markers;
- защищённый ручной workflow
  `.github/workflows/external-focus-email-otp.yml`;
- единый Chromium/Android/iOS Supabase Auth OTP journey в
  `site/e2e/focus-email/run.mjs`, включая Appium UiAutomator2/XCUITest adapters;
- защищённый Yandex Mail Trigger → API Gateway WebSocket receipt без доступа CI
  к человеческому ящику или приватному inbound bucket;
- exact deployment SHA check, один OTP issue, один verify, одна idempotent
  participant registration, reload/returning state;
- focus-specific admin-issued OTP/link без внешней доставки в
  `site/scripts/issue-focus-agent-test-credentials.mjs` и
  `site/scripts/check-focus-onboarding-email-integration.mjs`;
- scenario registry, platform selector, GitHub issue `/qa run` gateway и единый
  PII-free evidence index с redaction gate.

Terminal live receipts:

- Chromium normal OTP: `30745526613`;
- Android normal OTP: `30747598046`;
- iOS native-first side-effect-free preflight: `30767191144`, attempt 2;
- Android/iOS direct-Supabase-outage: `30772062840` / `30772233868`;
- Android/iOS relay-outage: `30772957771` / `30773125445`.

Исторический iOS run `30754894934` остаётся корректно классифицированным как
`BLOCKED_SAFARI_FIRST_RUN_UI`, `0/0/0`, без keyboard verdict. Последующие
terminal runs закрыли browser-tab iOS acceptance и обе single-client-route-down
ячейки.

Реализован локальный generic harness `site/e2e/auth-session-fixture/`: он
проверяет allowlist, per-worker/device isolation, штатный `verifyOtp`,
`auth.getUser`, обязательный JWT-bound read-only protected RLS probe, ephemeral
storage state, cleanup/redaction и нулевые счётчики product OTP/mail. PASS
невозможен без одного фактически выполненного успешного probe. Рядом выполняются
registry lint и deterministic no-mail fault matrix. Это **не** заменяет live
acceptance на hosted allowlisted target.

Не существует и не должно считаться готовым:

- terminal live acceptance общего `session_fixture` на hosted target;
- native install/relaunch PWA acceptance;
- автоматизация всех перечисленных ниже будущих page/data сценариев.

Текущий service worker network-only. Пока продуктовый контракт не изменён,
release gate не требует offline content availability. Допустима проверка
регистрации worker, честного offline failure, install identity и standalone
lifecycle.

## 3. Единица тестирования

### 3.1 Сценарий

Сценарий — одна бизнес-проверка со стабильным ID, например:

- `auth.session_fixture`;
- `focus.otp.browser_tab`;
- `focus.otp.ios_keyboard_preflight` (iOS only, `0/0/0` side effects);
- `focus.pwa.install_launch`;
- `browser.route_health`;
- `data.content_minimum`;
- `event.transport_blocks`.

Сценарий не равен workflow run и не обязан иметь отдельный workflow-файл.

### 3.2 Suite

Suite — выбранный набор сценариев:

- `pr-fast`;
- `feature`;
- `catalog`;
- `visual`;
- `mobile-critical`;
- `release`;
- `post-deploy`.

### 3.3 Platform adapter

Одна бизнес-логика может иметь адаптеры:

- `browser` — Playwright;
- `android` — Appium UiAutomator2 + Chrome Android;
- `ios` — Appium XCUITest + Mobile Safari.

Платформенные действия — keyboard, browser menu, Share Sheet, Launcher,
SpringBoard — не должны размножать бизнес-сценарий на три несогласованных копии.

### 3.4 Target

Поддерживаемые targets:

- `artifact` — локально собранное дерево;
- `preview` — опубликованный noindex preview;
- `candidate` — immutable release candidate;
- `production` — stable public origin.

OTP и PWA installation проверяются против опубликованного HTTPS target с exact
repo SHA. Локальный HTTP не заменяет secure-context и real-host acceptance.

### 3.5 Auth mode

Каждый сценарий, затрагивающий identity, обязан заранее выбрать один режим из
registry:

- `anonymous` — анонимный product contract;
- `anonymous_session` — настоящая Supabase anonymous JWT/RLS session без PII,
  внешнего письма и raffle eligibility;
- `mocked_ui` — только компонентный signed-in UI, без backend claims;
- `session_fixture` — настоящий JWT/RLS без внешнего письма;
- `admin_otp_ui` — настоящий OTP UI/verify без внешней доставки;
- `real_mail_otp` — настоящий issue, письмо, receipt, ввод и verify;
- `yandex_oauth` — настоящий redirect/consent/callback.

Локальная метка, `authorized=true`, fixture user object или focus participation
marker не заменяют Supabase session. `session_fixture` не доказывает доставку
письма; `real_mail_otp` не должен повторяться в тесте Search или персонализации.
Для focus v5 приглашение и `anonymous_session` предшествуют необязательному
identity upgrade: feedback не требует email/Яндекса, а raffle eligibility —
требует подтверждённую identity.

## 4. Селектор запуска: что запускать и когда

Решение принимает scenario registry, а не память агента. Для каждого сценария
фиксируются platform, trigger tags, cost tier, side effects, evidence policy,
auth mode и blocking policy.

### 4.1 Обязательный синхронный минимум

Агент должен дождаться результата до handoff:

- релевантные L0 contracts для затронутых данных/артефактов;
- короткий L1 smoke для изменённой page family;
- unit/component regressions, добавленные или изменённые в PR;
- release-blocking scenario, если изменение непосредственно меняет его contract;
- `auth.session_fixture`, если blocking scenario зависит от авторизации;
- redaction/audit tests при изменении evidence, Auth session или OTP.

### 4.2 Фоновый advisory run

Тяжёлый run разрешено **запустить и не ждать завершения**, когда он не является
обязательным gate текущего PR:

- полный browser catalog crawl;
- расширенная visual выборка;
- Android/iOS nightly suite;
- cross-browser sample после data-only изменения;
- post-merge health sweep.

Правила фонового запуска:

1. Agent запускает workflow и возвращает run URL/ID, exact SHA, suite и target.
2. Результат в handoff обозначается `STARTED_BACKGROUND`, а не PASS.
3. Workflow всегда публикует `qa-summary.json`, `junit.xml` и безопасный artifact.
4. Красный или `BLOCKED` сигнал создаёт наблюдаемый follow-up: PR check summary,
   issue/comment либо запись в release evidence. Его нельзя silently ignore.
5. Такой run не блокирует текущий PR, пока scenario registry не назначает ему
   `blocking: true` для этого change class.
6. Перед release promotion все релевантные background signals должны иметь
   terminal outcome; незавершённый обязательный signal означает NO-GO.

### 4.3 Защищённый manual run

Real-mail OTP, fresh-user identity, destructive state reset, платный device cloud
и production write probes запускаются только явно, в защищённом Environment и с
concurrency/side-effect policy.

Trusted `session_fixture` setup также требует защищённой issuer boundary, но не
должен автоматически превращать каждый использующий его business scenario в
real-mail manual run.

### 4.4 Нельзя «запустить и забыть»

`STARTED_BACKGROUND` — допустимый промежуточный operational outcome, но не
release evidence. Если run завершился после merge, его terminal result должен
попасть в следующий release/candidate decision.

### 4.5 Auth side-effect selector

Для `session_fixture` и `admin_otp_ui` обязательны:

```text
product /auth/v1/otp = 0
external mail send/receipt = 0/0
real-mail fallback = forbidden
```

Ошибка fixture даёт `BLOCKED_AUTH_FIXTURE`, а не новый OTP. Сохранённая session в
GitHub Secret, фиксированный OTP, service-role key в browser и общий refresh
token между параллельными jobs запрещены.

## 5. Change classes и выбор платформ

| Изменение | L0 | L1 browser | Android | iOS | Auth / Real OTP |
|---|---:|---:|---:|---:|---|
| Только docs/copy без runtime contract | выборочно | нет/короткий smoke | нет | нет | не требуется |
| Data exporter, manifest, catalog, ICS, SEO | да | affected routes/full sample | нет | нет | не требуется |
| Layout/CSS/component без native integration | да | да, desktop+mobile viewport | только representative при high-risk | representative при high-risk | `mocked_ui` либо existing fixture |
| Input, focus, keyboard, viewport resize | да | да | да | да | real OTP только при Auth-flow изменении |
| PWA manifest/install/start_url/scope/service worker | да | да | да | да | fixture; real OTP только при onboarding/Auth coupling |
| Focus invite/anonymous feedback/personalization | да | да | representative при native/input risk | representative при native/input risk | `anonymous_session`; OTP/mail = 0 |
| Focus identity upgrade/Auth/Supabase relay/email hook | да | да | да | да | `real_mail_otp` или `yandex_oauth` только при изменении этого contract |
| Только backend mail routing без UI | server contracts | browser integration | background sample | background sample | protected real-mail release gate |
| Search/personalization/feedback/saved state после входа | да | authenticated journey | representative при native/input risk | representative при native/input risk | `session_fixture`, real OTP = нет |
| Все event pages / массовая непустота | да, полный каталог | да, шардированно | только specimens | только specimens | не требуется |
| Push/background/OEM/performance | частично | нет | simulator partial | simulator partial | real OTP не требуется; L3 нужен |

### Trigger tags

Минимальный набор tags:

- `static-data`;
- `static-route`;
- `visual-layout`;
- `mobile-input`;
- `pwa-system`;
- `auth-session`;
- `auth-otp`;
- `supabase-connectivity`;
- `yandex-relay`;
- `yandex-sidecar`;
- `yandex-oauth`;
- `durable-outbox`;
- `partial-delivery`;
- `provider-mail`;
- `focus-feedback`;
- `personalization`;
- `release-publisher`.

Path-based detector может предлагать tags, но scenario registry остаётся source
of truth. Ручной override допустим только в сторону усиления либо с явно
задокументированным waiver.

## 6. GitHub Actions lanes

### PR fast — blocking

Запускается на каждый PR, но только affected scope:

- lint/unit/contracts;
- preview/artifact check;
- browser smoke изменённых route families;
- changed-route screenshots на failure;
- scenario selector report;
- `session_fixture` + affected authenticated scenario, когда меняется функция
  после входа и protected setup разрешён для доверенного кода.

Эмуляторы и real-mail OTP по умолчанию не запускаются.

### PR mobile-sensitive — mixed

Запускается при tags `mobile-input`, `pwa-system`, `auth-session`, `auth-otp`:

- Android critical scenario — blocking для feature PR, если изменён его contract;
- iOS critical scenario — blocking для direct iOS contract change, иначе допускается
  background advisory до release candidate;
- real OTP — только protected/manual либо явно утверждённый release integration run;
- ordinary authenticated mobile journey получает отдельную per-device session,
  а не общий refresh token или новое письмо.

### Main/nightly — background advisory

- полный browser route crawl и content-minimum pass;
- browser visual specimens;
- Android mobile-critical suite;
- iOS mobile-critical suite;
- connectivity read-only canaries;
- authenticated read/business canaries через fixed personas + `session_fixture`;
- aggregate `qa-summary.json`.

Nightly не должен создавать новых пользователей, массово отправлять OTP или
писать production данные. Real-mail OTP в nightly запрещён; session fixture
должен доказывать `product OTP=0` и `external mail=0`.

### Release candidate — blocking

- production/candidate artifact contracts;
- generated-candidate browser release gate;
- full route health;
- Android critical journeys;
- iOS critical journeys;
- `auth.session_fixture` и affected authenticated journeys;
- protected real OTP browser-tab scenario **только** если selector связал релиз
  с Auth/onboarding/mail/OTP contract;
- PWA install/relaunch, если релиз затрагивает PWA contract;
- terminal review всех релевантных background signals.

### Post-deploy

- короткий read-only production smoke;
- manifest/service-worker canary;
- Supabase/Yandex nonce/read path;
- authenticated cached/read canary через `session_fixture`;
- fresh-device PWA canary периодически, а не после каждого data-only deploy.

## 7. Первый обязательный mobile milestone: focus OTP

### 7.1 Не создавать новый параллельный тест

Нужно модифицировать существующий isolated harness:

- `.github/workflows/external-focus-email-otp.yml`;
- `site/e2e/focus-email/run.mjs`;
- helpers/tests внутри `site/e2e/focus-email/`.

Текущий Chromium journey остаётся compatibility baseline и не удаляется до
устойчивого PASS новых adapters.

### 7.2 `focus.otp.browser_tab`

Общая семантика для browser/Android/iOS:

1. открыть exact invitation URL;
2. проверить target origin и deployed repo SHA;
3. принять приглашение;
4. пропустить PWA install;
5. открыть email step;
6. фокусировать email input и проверить реальную email keyboard на L2;
7. ввести fixed test identity;
8. выполнить конкурирующие обычные gestures и доказать ровно один OTP issue;
9. получить ровно одно подходящее real-mail message после checkpoint через
   controlled IMAPS либо dedicated Yandex Mail Trigger WebSocket;
10. фокусировать OTP input и проверить numeric/one-time-code keyboard на L2;
11. ввести OTP посимвольно;
12. доказать ровно один verify и одну participant registration;
13. увидеть membership confirmed;
14. reload/reopen и увидеть returning state без нового OTP;
15. выпустить redacted evidence.

### 7.3 Реальная клавиатура

На Android/iOS сценарий должен использовать native keyboard, а не прямую запись
значения через JS. Acceptance:

- keyboard показана;
- тип соответствует email или numeric/one-time-code intent;
- active input остаётся видимым;
- CTA/code cells не закрыты критически;
- visual viewport/scroll не оставляет пользователя в тупике;
- шестая цифра подтверждается обычным input/change path;
- native hierarchy/screenshot не содержит несанифицированных email/OTP.

Текст конкретных клавиш и layout не должен быть хрупким pixel contract. Основные
доказательства — keyboard presence/type hints, focus, viewport geometry и
успешный ordinary user input path.

### 7.4 Изоляция mailbox

Browser, Android и iOS real OTP jobs выполняются последовательно при одном fixed
mailbox. Параллельный запуск разрешён только после выдачи отдельной стабильной
identity/mailbox каждой платформе.

Сохраняются существующие ограничения:

- no service-role key in test runner/browser;
- no fixed OTP/bypass;
- no automatic resend after ambiguous request;
- one global concurrency group;
- no raw email, OTP, cookies, JWT, HAR, trace или video;
- artifact upload only after redaction gate.

### 7.5 `focus.otp.installed_pwa`

Это отдельный более дорогой scenario, не часть первого Android/iOS browser-tab
PR:

- установить через Chrome UI / Safari Share Sheet;
- запустить из Launcher / SpringBoard;
- проверить standalone и stable start identity;
- пройти Auth либо reuse returning state;
- close/relaunch;
- проверить persisted membership.

Разделение позволяет отличить Auth regression от install/lifecycle regression.

## 8. Общий Auth session fixture

Канонический детальный контракт:
[`static-site-auth-session-fixture.md`](../testing/static-site-auth-session-fixture.md).

### 8.1 Назначение

`session_fixture` нужен для сценариев, которые проверяют бизнес-функцию после
входа, а не способ получения OTP. Trusted setup выпускает свежий одноразовый
admin link/OTP без доставки, проходит штатный Supabase callback/verify и
сохраняет настоящую session в ephemeral state одного worker/device job.

### 8.2 Обязательные свойства

- настоящий `auth.getUser` и JWT, а не локальный boolean;
- обязательный один успешный read-only `/rest/v1/*` probe с этим JWT,
  publishable key и owner assertion; callback `true` без запроса запрещён;
- настоящий RLS/RPC/Edge behavior в последующем бизнес-сценарии;
- fixed allowlisted persona, а не новый email на каждый run;
- fresh credential и session на каждый parallel worker/job;
- state только в `$RUNNER_TEMP`/ephemeral storage;
- `POST /auth/v1/otp = 0`;
- external mail send/receipt `0/0`;
- no artifact/cache/job-output session transfer;
- fail-closed cleanup/redaction;
- `BLOCKED_AUTH_FIXTURE` без fallback на реальное письмо.

### 8.3 Запрещённые подмены

- `authorized=true` и fake user для live backend E2E;
- постоянная Supabase session в GitHub Secret;
- один refresh token для параллельных jobs;
- фиксированный OTP для production project;
- self-issued JWT;
- service-role/secret key в browser/Appium/localStorage/URL;
- direct token injection как platform acceptance;
- автоматическая отправка OTP при сбое fixture.

### 8.4 Scenario mapping

`session_fixture` является default для:

- `personal.for_me_page`;
- `personalization.core_journey`;
- `search.authenticated_contract`;
- `search.live_cached_journey`;
- `search.live_cold_journey`;
- `search.cache_provider_zero`;
- authenticated `search.transport_route_matrix`;
- будущих feedback/saved-event/authenticated page scenarios.

Auth issue/verify transport продолжает доказываться отдельным
`focus.otp.browser_tab`; Search не должен заново отправлять письмо.

### 8.5 Security evolution

Первый implementation может использовать минимальный issuer step в protected
GitHub Environment на доверенной ветке. Целевой контур — OIDC-protected
server-side broker, который проверяет repository/ref/workflow/run/persona/
redirect и выдаёт только одноразовый пользовательский credential, не общий
Supabase Admin API.

### 8.6 No-mail transport matrix

Routine reliability tests не используют внешний ящик. Исполняемый harness
`site/e2e/auth-session-fixture/noMailFaultMatrix.ts` проверяет четыре независимых
операции в семи профилях: `normal`, недоступность direct, недоступность relay,
недоступность обоих клиентских путей, общий отказ Supabase upstream,
неоднозначность response body после dispatch и восстановление после reload:

- Auth verify, Search и personalization — `selected-once`: маршрут выбирается
  до dispatch, при двух недоступных маршрутах dispatch равен нулю, а после
  неоднозначного ответа автоматического replay нет;
- focus feedback — `idempotent-replay`: повтор разрешён только с тем же
  idempotency key и приводит ровно к одному логическому эффекту;
- общий upstream-отказ не считается успешным восстановлением через relay;
- pending intent сериализуется, восстанавливается новым экземпляром transport и
  после reconnect создаёт один dispatch/эффект; повторный flush committed intent
  ничего не отправляет;
- для каждой ячейки product OTP issue, provider mail send и receipt равны нулю;
- receipt содержит только стабильные fault-коды, routes и счётчики; URL, request
  body, action id, токены и заголовки авторизации не экспортируются;
- отказ fixture или отсутствие allowlisted session заканчивается
  `BLOCKED_AUTH_FIXTURE`, без real-mail fallback.

Матрица доказывает transport policy локально и детерминированно. Она не выдаёт
fake session или in-memory recovery journal за live hosted/product outbox
acceptance и не закрывает provider-specific mail, OAuth, фактическую
персистентность runtime либо мобильный keyboard contract.

## 9. Критические page/data scenarios

Реестр содержит статус `implemented`, `partial` или `planned`; planned scenario
не должен делать CI красным до появления продукта и явного gate transition.

### Artifact/data

- HTML/JSON/ICS/assets не пусты;
- manifest/catalog/routes согласованы;
- event ID/slug/projection не перепутаны;
- expected content либо честный typed empty-state;
- minimum fields по page family;
- no leaked preview/bearer URLs;
- canonical/robots/sitemap/JSON-LD contracts;
- image decode либо accepted fallback;
- stale/partial batch fail-closed.

### Browser route health

- успешный HTTP status и expected origin;
- видимый ненулевой `<main>` и route marker;
- нет бесконечного skeleton/`aria-busy`;
- нет критических `pageerror`/console/network failures;
- нет horizontal overflow и перекрытия sticky shell;
- CTA и ключевые controls доступны;
- screenshot на failure, viewport screenshot для review sample.

### Event-specific content

- дата, время, место, admission и основной CTA;
- transport blocks там, где exporter обещает transport data;
- venue/source medallions без конфликтных дублей;
- people/headliner/celebrity cards после реализации;
- recommendations без self/duplicate occurrence;
- minimum meaningful copy, не generic placeholder;
- typed media family и content coverage.

### Персональные поверхности

- pre-generated `Для меня`/personal page для authenticated user после реализации;
- no unauthorized data leakage;
- deterministic fallback для immature profile;
- Supabase direct/relay read/write contract;
- Yandex relay availability и nonce/schema;
- future personalization ordering/feedback lifecycle;
- local-first actions + idempotent outbox;
- `session_fixture` как default auth preparation, `0` product OTP и `0` писем.

### Yandex capability и degraded semantics

Yandex не моделируется одним boolean. Registry разделяет relay, YDB
analytics/control, OAuth, Postbox, inbound pipeline и Object Storage/CDN. Для
каждой capability сценарий фиксирует SOR, acknowledgement boundary, operation
semantics, idempotency/replay и честное pending/partial состояние. Обязательные
fault contracts перечислены в
[`yandex-dependency-resilience.md`](yandex-dependency-resilience.md): оба
клиентских маршрута недоступны, общий Supabase upstream недоступен, YDB
projection lag, reconnect exactly-once, component-partial focus feedback,
OAuth fallback, durable Postbox outbox и inbound replay. Planned строки не
становятся PASS только из-за наличия записи в registry.

## 10. Стратифицированная мобильная выборка

Не открывать сотни event pages на двух симуляторах. Из каждой актуальной family
выбирается по одному или нескольким specimens:

- single/multiple/no image;
- portrait/wide/tall OCR;
- free/ticket/registration/calendar CTA;
- one date/date range;
- transport/no transport;
- medallion/no medallion/conflict negative;
- people card/no people;
- anonymous/authenticated/personal page;
- empty-state/underfilled data block.

Specimen identity и reason сохраняются в evidence. Expired live event не должен
удалять executable contract: для критической геометрии остаются frozen fixtures.

Authenticated mobile specimen получает отдельную per-device session. Один
browser storage state или refresh token нельзя копировать в Android и iOS jobs.

## 11. Evidence, доступный ChatGPT

Каждый job публикует единый безопасный пакет:

```text
evidence/
├── manifest.json
├── qa-summary.json
├── run.json
├── scenarios.jsonl
├── junit.xml
├── routes.jsonl
├── device.json
├── screenshots/
├── native-ui/
├── console.sanitized.jsonl
├── network.sanitized.jsonl
└── redaction-audit.json
```

Обязательные поля:

- repository и full repo SHA;
- build/snapshot/tree identity, если применимо;
- workflow run ID/attempt;
- target URL origin/path без bearer leakage;
- suite/scenario/platform;
- auth mode и session scope, если применимо;
- OS/runtime/browser/device/locale/timezone;
- blocking/advisory/manual mode;
- PASS/FAIL/BLOCKED/STARTED_BACKGROUND;
- failure domain и first failed step;
- product OTP issue count и external mail send/receipt counts;
- links/relative paths к screenshots/artifacts;
- cleanup и redaction status.

Artifact name должен быть предсказуемым:

`static-site-qa-<suite>-<platform>-<run_id>-<attempt>`.

`qa-summary.json` открывается первым. Он должен позволить ChatGPT ответить без
чтения raw logs: что запускалось, почему, что прошло, что заблокировано, какой
сценарий/route упал и какие evidence files смотреть дальше.

Для обычных UI-сценариев допустимы video/trace on failure. Для real OTP video,
trace, HAR и raw mail запрещены. Для session bootstrap trace/HAR также
запрещены; serialized auth state никогда не загружается как artifact.
Маскированные screenshots и sanitized structured events остаются обязательными.

## 12. Политика PASS/FAIL/BLOCKED

- **PASS:** все mandatory assertions выполнены на exact target identity.
- **FAIL:** продукт/данные/контракт дали неверный результат.
- **BLOCKED:** среда, secret, issuer, runner, mailbox, simulator runtime или
  target identity не позволили честно выполнить сценарий.
- **STARTED_BACKGROUND:** run создан, terminal результата ещё нет; никогда не
  эквивалентен PASS.
- **SKIPPED_NOT_APPLICABLE:** registry доказал, что scenario не относится к
  изменению.
- **NOT_IMPLEMENTED:** product/scenario planned; не маскировать как skip/pass.

`session_fixture` failure — `BLOCKED_AUTH_FIXTURE`; unexpected product OTP/mail,
identity mismatch или credential leak — FAIL. Infrastructure flake может
получить один bounded retry. Product assertion, OTP side effect и неоднозначный
verify/callback не повторяются автоматически без сценарно безопасного contract.

## 13. Экономичность

- L0/L1 сначала, L2 только после их зелёного результата.
- Browser crawl шардируется; full-page screenshots только на failure/выборке.
- Visual pixel diff только для frozen fixtures, не для постоянно меняющихся
  editorial pages.
- Android/iOS images, Appium drivers и actions pins кешируются безопасно.
- iOS/macOS не запускается на data-only PR.
- Real OTP не запускается nightly и не создаёт fresh users по умолчанию.
- Обычные authorized suites используют fixed personas + `session_fixture`.
- Fixture создаётся один раз на worker/job, а не на каждый test.
- Session state не хранится между runs и не передаётся через artifacts/cache.
- Один fixed mailbox, один sequential real-mail run, bounded timeout.
- AI visual review — advisory triage, deterministic assertions остаются gate.
- Новый тест добавляется при реальном regression risk, а не для полноты каталога.

## 14. Release integration

`release-plan.md` остаётся umbrella release truth. Этот документ определяет
способ доказательства его browser/mobile/data gates.

Release decision обязан перечислить:

- exact candidate SHA/tree;
- selected scenarios и selector reason;
- blocking terminal results;
- advisory/background results и их disposition;
- Android/iOS requirement reason либо `not_applicable` evidence;
- auth mode и session fixture receipt для authenticated scenarios;
- real OTP requirement reason и run artifact либо явный `not_applicable`;
- known planned gaps, которые не выдаются за implemented coverage.

NO-GO, если:

- изменён Auth/PWA/mobile-input contract, но отсутствует требуемый Android или
  iOS result;
- external OTP scenario FAIL/BLOCKED при релизе соответствующей Auth/mail функции;
- authenticated release scenario требует session fixture, но он отсутствует,
  отправил неожиданный OTP/mail или утёк в evidence;
- target SHA не совпадает;
- evidence не прошёл redaction;
- обязательный background run ещё не terminal;
- full catalog имеет unexplained empty/broken route;
- planned feature ошибочно отмечена implemented/pass.
- пользовательский success показан до durable SOR acknowledgement либо скрывает
  partial component failure;
- selected-once/ambiguous операция автоматически повторилась через другой
  route/provider;
- Yandex sidecar outage откатил primary action или уничтожил durable outbox.

## 15. Поступательная реализация

### M0 — документы и agent contract

Этот документ, release companion, registry, Codex handoff и scoped AGENTS.

### M1 — OTP platform-neutral extraction

Сохранить текущий Chromium PASS path, выделить shared journey/evidence и adapters.

### M2 — Android browser-tab OTP

Завершено: Android Emulator + Chrome + UiAutomator2, реальная клавиатура и
protected real mail приняты terminal run `30747598046`.

### M3 — iOS browser-tab OTP

Завершено: native-first Safari preflight принят run `30767191144` attempt 2;
полные iOS direct-outage и relay-outage journeys приняты runs `30772233868` и
`30773125445`. Исторический `30754894934` остаётся корректным blocked receipt.

### M4 — generic Auth session fixture

Локальный generic harness, persona/target allowlist, per-scope isolation,
`verifyOtp`/`auth.getUser`, обязательный JWT-bound protected RLS probe,
ephemeral Playwright state, cleanup/redaction, registry lint и no-mail matrix
реализованы. Остаются terminal live acceptance на hosted allowlisted target,
protected issuer integration и доказательство второго browser context/device
bootstrap; до этого registry честно маркирует milestone как partial, а не PASS.

### M5 — подключение authorized business scenarios

Search cached/cold/cache-zero, `Для меня`, personalization, feedback и saved
state переходят на `session_fixture` без повторной проверки доставки.

### M6 — Android/iOS session bootstrap и PWA install/relaunch

Отдельная session на device job, same-storage continuation и короткие PWA
install/relaunch scenarios без offline-first обещаний.

### M7 — generic browser/data registry runner

Affected-route health, content minimum и шардированный catalog evidence.

### M8 — новые feature scenarios

Transport, medallions, people cards, personal pages, connectivity и
personalization добавляются вместе с реализацией/audit соответствующей surface.

### M9 — Search live plane

Реализованный `site/e2e/search` сохранён, а orchestration разделена на
deterministic CI, current-target production health и manual/selective release
qualification. Старые automatic cached/cold/LLM/mobile schedules,
static-build post-deploy dispatch и generic issue reporter отключены.
Stage 2 добавляет только две bounded schedule, explicit
`search-runtime-deployed` marker и platform-scoped reporter; automatic entry
points закрыты `SEARCH_PRODUCTION_HEALTH_ENABLED` до двух terminal live proofs.
`static-site-search-canary.yml` остаётся ручным legacy debugger. Полный контракт,
trigger matrix и stage-2 activation принадлежат единственному каноническому
разделу [`smart-vector-search/README.md#16`](../features/static-site-pages/smart-vector-search/README.md#16-search-production-health-архитектурная-коррекция-этап-1).

`release_exact`, cache repeat, LLM, pagination и расширенная mobile matrix
принадлежат только release qualification. Production health принимает cache
hit/miss и движение content/index revisions, ограничен одним vector-only UI
POST на platform cell и дважды в сутки выполняет browser + Android либо browser
+ iOS с отдельной no-mail session на каждую платформу.

Нельзя откладывать Android/iOS до «когда-нибудь после общей системы», но нельзя и
заставлять каждый authenticated business test повторять дорогой real-mail OTP.
Общий exact-target resolver обязан работать и в Linux Bash, и в системном
macOS Bash 3.2: запрещено использовать `mapfile`; две проверенные строки
(bearer target и SHA) читаются через `IFS= read -r`, после чего target сразу
маскируется средствами GitHub Actions.
Appium browser capabilities на обеих платформах фиксируют
`wdio:enforceWebDriverClassic=true`: WebdriverIO не должен автоматически
переключать Chrome/Safari session на частичный BiDi transport, поскольку
Appium drivers не гарантируют `script.addPreloadScript`.
Cold simulator/WDA startup получает один WebDriver session POST с бюджетом до
300 секунд и `connectionRetryCount=0`. Search переиспользует принятую OTP
политику одного Appium restart: только закрытый receipt
`webdriver_session_create` с `auth_callback_started=false`, пустыми query cases
и нулевым traffic разрешает второй свежий WebDriver session attempt в том же
device job. Неиспользованный callback не перевыпускается и не открывался; любой
callback/Search side effect или неоднозначная стадия запрещают retry. Raw Appium
log читается только локально и сворачивается в allowlisted phase booleans,
elapsed time, attempt number и truncation flag, после чего удаляется. Обрезанный
log может объяснять отказ, но никогда не разрешает retry по отсутствующим
событиям.
Android/iOS scroll receipt строится только из реальных Appium touch gestures:
адаптер повторяет их с bounded limit до попадания последней rendered card во
viewport и проверяет положительный `scrollY` delta; DOM-scroll не допускается.
Device callback считается завершённым не при первом возврате на site origin, а
только после `is-authorized` на возвращённой `/poisk/`. Лишь после этого journey
делает обычный reload exact target и тем самым доказывает same-storage session
persistence, не прерывая одноразовый callback.
Broker admin `action_link` не открывается напрямую: его default hosted GET
возвращает implicit session fragment, который production static auth намеренно
не парсит (`detectSessionInUrl=false`). Runner fail-closed проверяет exact
`token/type/redirect_to`, строит allowlisted target callback с
`token_hash/type`, маскирует обе одноразовые ссылки, а `StaticSiteAuth.verifyOtp`
выполняется уже внутри device browser. Так session сохраняется в том же
Chrome/Safari storage без передачи access/refresh token runner-у; raw Appium
logs удаляются до завершения job и никогда не публикуются.
Android/iOS Search и real-mail OTP используют один нейтральный transport из
`site/e2e/mobile-web/`; feature adapters не имеют права копировать Appium
startup/capabilities. iOS сначала запускает `com.apple.mobilesafari` как native
application, очищает только exact allowlisted first-run dialog через один
текущий WDA alert либо одну native sheet с единственной точной allowlisted
кнопкой, затем подключает WebKit. Анализ clean-simulator source сворачивается в
закрытые счётчики; Search не сохраняет source и не имеет отдельного Safari
обработчика. Общий профиль задаёт `appium:webviewConnectTimeout=60000` и bounded
`appium:webviewConnectRetries=120`: официальный XCUITest default 5000 мс уже
привёл Search CI к отказу через 5.749 секунды. Action link и secret target не
передаются в capabilities и не входят в публикуемые evidence/Appium logs.
Критический iOS web input фокусируется общей функцией через один exact native
accessibility match и native tap; WebKit `click()` с `isKeyboardShown()` в web
context не считается доказательством software keyboard. Общий
`performNativeDocumentSwipe` прокручивает Android Chrome абсолютной W3C touch
sequence, а iOS Safari — application-level XCUITest `mobile: swipe`; обе ветки
работают в `NATIVE_APP`, читают native viewport и возвращают исходный WebView
для измерения реального `scrollY`. Safari W3C pointer source не используется:
live run `31277971410` подтвердил 24 успешных acknowledgement без доставки
жеста в WebKit. Нативные shortcuts
UiAutomator2 `mobile: scrollGesture` и XCUITest `mobile: scroll` не применяются
к browser page content: они предназначены для нативных scrollable controls или
таблиц/коллекций и могут успешно ответить без движения DOM.
Перед scroll baseline общий helper закрывает открытую IME, чтобы finger path не
попал в клавиатуру вместо browser viewport. Mobile Safari/XCUITest иногда
возвращает exact `Did not know how to dismiss the keyboard`; только этот iOS
ответ разрешает один документированный user-equivalent fallback. Search
находит ровно один caller-allowlisted non-actionable static heading вне поля и
тапает центр его native rect; произвольная координата, submit или текст
карточки запрещены. Сценарий без безопасного static target может использовать
application swipe вниз. После штатного hide или fallback helper обязан увидеть
`isKeyboardShown() == false`; иначе он завершает сценарий как
`mobile_keyboard_dismiss_unconfirmed` до document scroll. Остальные driver
errors не подавляются.
Если exact heading отсутствует или неоднозначен, failure receipt сохраняет
только `total_count`/`visible_count` для закрытого списка XCTest-типов
`StaticText`/`Other`/`Button`/`Link`. Raw hierarchy, labels и соседний текст не
собираются; диагностический тип не становится разрешением на tap автоматически.
Live receipt `31281345474` доказал конкретный случай: exact heading был
`StaticText total=1`, но `visible=0` после submit. Поэтому Search завершает
validated first-page input обычным product `blur()` до единственного POST:
native Search/Enter остаётся реальным пользовательским действием, а IME
закрывается до просмотра результатов. Harness всё равно независимо проверяет
`isKeyboardShown() == false` перед document swipe.
При отказе публикуется только закрытый route enum и numeric/boolean receipt:
native viewport, доступные start/end/duration, число жестов, `scrollY` delta и
видимость финальной карточки; hierarchy, screenshot, URL и текст страницы в
него не входят.

Для любой новой или ремонтируемой Android/iOS web-проверки обязателен проектный
skill `.codex/skills/mobile-web-e2e/SKILL.md`: сначала найти terminal receipts и
общий transport, и только затем добавлять feature journey. Это regression guard
против повторного изобретения отдельного Appium-контура.
