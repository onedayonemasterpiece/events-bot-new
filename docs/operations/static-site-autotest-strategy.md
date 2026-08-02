# Стратегия автотестирования статического сайта и данных

> **Статус:** стратегический, канонический и нормативный документ проекта.
> **Область:** Astro static site, PWA, данные статического артефакта, Supabase/Yandex connectivity и критические пользовательские пути.
> **Связь с релизом:** обязательный companion к
> [`docs/features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md)
> и [`release-autotest-gates.md`](../features/static-site-pages/release-autotest-gates.md).
> **Машиночитаемый реестр:**
> [`docs/testing/static-site-autotest-scenarios.v1.yml`](../testing/static-site-autotest-scenarios.v1.yml).
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

## 2. Проверенный baseline на 2026-08-02

Аудит выполнен по `main`, начиная со среза
`ba8ab078ba9894ccd5810045b1b8787ecb29d743`.

Уже существует:

- production/preview/secret artifact checks и release manifests;
- generated-candidate Chromium browser release gate;
- component/browser contracts PWA install copy и lifecycle markers;
- защищённый ручной workflow
  `.github/workflows/external-focus-email-otp.yml`;
- реальный Chromium + Supabase Auth + IMAPS OTP journey в
  `site/e2e/focus-email/run.mjs`;
- exact deployment SHA check, один OTP issue, один verify, одна idempotent
  participant registration, reload/returning state;
- PII-free evidence с redaction gate, пригодный для отдельного анализа ChatGPT.

Не существует и не должно считаться готовым:

- Android Emulator-вариант реального OTP;
- iOS Simulator-вариант реального OTP;
- native install/relaunch PWA acceptance;
- общий scenario registry и selector;
- единая политика blocking/background/manual запусков;
- общий evidence index для browser/Android/iOS;
- автоматизация всех перечисленных ниже будущих page/data сценариев.

Текущий service worker network-only. Пока продуктовый контракт не изменён,
release gate не требует offline content availability. Допустима проверка
регистрации worker, честного offline failure, install identity и standalone
lifecycle.

## 3. Единица тестирования

### 3.1 Сценарий

Сценарий — одна бизнес-проверка со стабильным ID, например:

- `focus.otp.browser_tab`;
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

## 4. Селектор запуска: что запускать и когда

Решение принимает scenario registry, а не память агента. Для каждого сценария
фиксируются platform, trigger tags, cost tier, side effects, evidence policy и
blocking policy.

### 4.1 Обязательный синхронный минимум

Агент должен дождаться результата до handoff:

- релевантные L0 contracts для затронутых данных/артефактов;
- короткий L1 smoke для изменённой page family;
- unit/component regressions, добавленные или изменённые в PR;
- release-blocking scenario, если изменение непосредственно меняет его contract;
- redaction/audit tests при изменении evidence или OTP.

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

### 4.4 Нельзя «запустить и забыть»

`STARTED_BACKGROUND` — допустимый промежуточный operational outcome, но не
release evidence. Если run завершился после merge, его terminal result должен
попасть в следующий release/candidate decision.

## 5. Change classes и выбор платформ

| Изменение | L0 | L1 browser | Android | iOS | Real OTP |
|---|---:|---:|---:|---:|---:|
| Только docs/copy без runtime contract | выборочно | нет/короткий smoke | нет | нет | нет |
| Data exporter, manifest, catalog, ICS, SEO | да | affected routes/full sample | нет | нет | нет |
| Layout/CSS/component без native integration | да | да, desktop+mobile viewport | только representative при high-risk | representative при high-risk | нет |
| Input, focus, keyboard, viewport resize | да | да | да | да | при Auth-flow изменении |
| PWA manifest/install/start_url/scope/service worker | да | да | да | да | при onboarding/Auth coupling |
| Focus onboarding/Auth/Supabase/Yandex relay/email hook | да | да | да | да | да |
| Только backend mail routing без UI | server contracts | browser integration | background sample | background sample | protected release gate |
| Все event pages / массовая непустота | да, полный каталог | да, шардированно | только specimens | только specimens | нет |
| Push/background/OEM/performance | частично | нет | simulator partial | simulator partial | нет; L3 нужен |

### Trigger tags

Минимальный набор tags:

- `static-data`;
- `static-route`;
- `visual-layout`;
- `mobile-input`;
- `pwa-system`;
- `auth-otp`;
- `supabase-connectivity`;
- `yandex-relay`;
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
- scenario selector report.

Эмуляторы по умолчанию не запускаются.

### PR mobile-sensitive — mixed

Запускается при tags `mobile-input`, `pwa-system`, `auth-otp`:

- Android critical scenario — blocking для feature PR, если изменён его contract;
- iOS critical scenario — blocking для direct iOS contract change, иначе допускается
  background advisory до release candidate;
- real OTP — только protected/manual либо явно утверждённый release integration run.

### Main/nightly — background advisory

- полный browser route crawl и content-minimum pass;
- browser visual specimens;
- Android mobile-critical suite;
- iOS mobile-critical suite;
- connectivity read-only canaries;
- aggregate `qa-summary.json`.

Nightly не должен создавать новых пользователей, массово отправлять OTP или
писать production данные.

### Release candidate — blocking

- production/candidate artifact contracts;
- generated-candidate browser release gate;
- full route health;
- Android critical journeys;
- iOS critical journeys;
- protected real OTP browser-tab scenario;
- PWA install/relaunch, если релиз затрагивает PWA contract;
- terminal review всех релевантных background signals.

### Post-deploy

- короткий read-only production smoke;
- manifest/service-worker canary;
- Supabase/Yandex nonce/read path;
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

- no service-role key in test runner;
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

## 8. Критические page/data scenarios

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
- local-first actions + idempotent outbox.

## 9. Стратифицированная мобильная выборка

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

## 10. Evidence, доступный ChatGPT

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
- OS/runtime/browser/device/locale/timezone;
- blocking/advisory/manual mode;
- PASS/FAIL/BLOCKED/STARTED_BACKGROUND;
- failure domain и first failed step;
- links/relative paths к screenshots/artifacts;
- redaction status.

Artifact name должен быть предсказуемым:

`static-site-qa-<suite>-<platform>-<run_id>-<attempt>`.

`qa-summary.json` открывается первым. Он должен позволить ChatGPT ответить без
чтения raw logs: что запускалось, почему, что прошло, что заблокировано, какой
сценарий/route упал и какие evidence files смотреть дальше.

Для обычных UI-сценариев допустимы video/trace on failure. Для real OTP video,
trace, HAR и raw mail запрещены. Маскированные screenshots и sanitized structured
events остаются обязательными.

## 11. Политика PASS/FAIL/BLOCKED

- **PASS:** все mandatory assertions выполнены на exact target identity.
- **FAIL:** продукт/данные/контракт дали неверный результат.
- **BLOCKED:** среда, secret, runner, mailbox, simulator runtime или target
  identity не позволили честно выполнить сценарий.
- **STARTED_BACKGROUND:** run создан, terminal результата ещё нет; никогда не
  эквивалентен PASS.
- **SKIPPED_NOT_APPLICABLE:** registry доказал, что scenario не относится к
  изменению.
- **NOT_IMPLEMENTED:** product/scenario planned; не маскировать как skip/pass.

Infrastructure flake может получить один bounded retry. Product assertion и
OTP side effect не повторяются автоматически без сценарно безопасного contract.

## 12. Экономичность

- L0/L1 сначала, L2 только после их зелёного результата.
- Browser crawl шардируется; full-page screenshots только на failure/выборке.
- Visual pixel diff только для frozen fixtures, не для постоянно меняющихся
  editorial pages.
- Android/iOS images, Appium drivers и actions pins кешируются безопасно.
- iOS/macOS не запускается на data-only PR.
- Real OTP не запускается nightly и не создаёт fresh users по умолчанию.
- Один fixed identity/mailbox, один sequential run, bounded timeout.
- AI visual review — advisory triage, deterministic assertions остаются gate.
- Новый тест добавляется при реальном regression risk, а не для полноты каталога.

## 13. Release integration

`release-plan.md` остаётся umbrella release truth. Этот документ определяет
способ доказательства его browser/mobile/data gates.

Release decision обязан перечислить:

- exact candidate SHA/tree;
- selected scenarios и selector reason;
- blocking terminal results;
- advisory/background results и их disposition;
- Android/iOS requirement reason либо `not_applicable` evidence;
- real OTP requirement reason и run artifact;
- known planned gaps, которые не выдаются за implemented coverage.

NO-GO, если:

- изменён Auth/PWA/mobile-input contract, но отсутствует требуемый Android или
  iOS result;
- external OTP scenario FAIL/BLOCKED при релизе соответствующей функции;
- target SHA не совпадает;
- evidence не прошёл redaction;
- обязательный background run ещё не terminal;
- full catalog имеет unexplained empty/broken route;
- planned feature ошибочно отмечена implemented/pass.

## 14. Поступательная реализация

### M0 — документы и agent contract

Этот документ, release companion, registry, Codex handoff и scoped AGENTS.

### M1 — OTP platform-neutral extraction

Сохранить текущий Chromium PASS path, выделить shared journey/evidence и adapters.

### M2 — Android browser-tab OTP

Android Emulator + Chrome + UiAutomator2, реальная клавиатура, protected real mail.

### M3 — iOS browser-tab OTP

iOS Simulator + Mobile Safari + XCUITest, реальная клавиатура, protected real mail.

### M4 — PWA install/relaunch

Короткие отдельные Android/iOS scenarios без offline-first обещаний.

### M5 — generic browser/data registry runner

Affected-route health, content minimum и шардированный catalog evidence.

### M6 — новые feature scenarios

Transport, medallions, people cards, personal pages, connectivity и
personalization добавляются вместе с реализацией/audit соответствующей surface.

Нельзя откладывать Android/iOS до «когда-нибудь после общей системы», но и нельзя
строить весь generic framework до доказательства первого OTP vertical slice.
