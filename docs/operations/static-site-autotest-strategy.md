# Стратегия автотестирования статического сайта и данных

> **Статус:** стратегический, канонический и нормативный документ проекта.
>
> **Область:** Astro static site, PWA, данные статического артефакта,
> Supabase/Yandex connectivity, Yandex-dependent sidecars/providers и критические
> пользовательские пути.
>
> **Связь с релизом:** обязательный companion к
> [`docs/features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md)
> и [`release-autotest-gates.md`](../features/static-site-pages/release-autotest-gates.md).
>
> **Yandex reliability contract:**
> [`yandex-dependency-resilience.md`](yandex-dependency-resilience.md).
>
> **Машиночитаемый реестр:**
> [`docs/testing/static-site-autotest-scenarios.v1.yml`](../testing/static-site-autotest-scenarios.v1.yml).
>
> **Первый implementation handoff:**
> [`docs/testing/static-site-autotest-codex-prompt.md`](../testing/static-site-autotest-codex-prompt.md).

## 1. Решение

Проект использует четыре уровня доказательства, но запускает только те уровни,
которые дают дополнительную информацию для конкретного изменения.

1. **L0 — artifact/data/contracts.** Проверки файлов, manifest, catalog,
   JSON/ICS/SEO, operation semantics, idempotency, acknowledgement и storage
   bounds без браузера.
2. **L1 — browser runtime.** Playwright в Chromium/Firefox/WebKit для DOM, CSS,
   JS, сетевых ошибок, fault injection, route selection, component receipts,
   геометрии и массового обхода страниц.
3. **L2 — mobile system integration.** Настоящий Android Emulator с Chrome и
   iOS Simulator с Mobile Safari. Appium переключается между web/native UI для
   клавиатуры, install UI, Launcher/SpringBoard, Share Sheet и PWA lifecycle.
4. **L3 — real-device/field certification.** Редкий внешний canary на физическом
   Android/iPhone для операторских сетей, DNS/IPv6, push/background/OEM,
   performance и hardware поведения.

Мобильный viewport в desktop Chromium не считается доказательством Android или
iPhone. Playwright WebKit не считается доказательством native Mobile Safari,
Share Sheet или Home Screen.

Эмуляторы не применяются ко всему статическому каталогу. Массовые проверки
выполняют L0/L1. L2 используется только для системно значимых мобильных путей и
малой стратифицированной выборки page families. Field screenshot/receipt может
быть важным L3 signal, но не заменяет воспроизводимый deterministic gate.

## 2. Проверенный baseline на 2026-08-03

Уже существует:

- production/preview/secret artifact checks и release manifests;
- generated-candidate Chromium browser release gate;
- component/browser contracts PWA install copy и lifecycle markers;
- защищённый workflow `.github/workflows/external-focus-email-otp.yml`;
- единый Chromium/Android/iOS Supabase Auth OTP journey в
  `site/e2e/focus-email/run.mjs` с Appium UiAutomator2/XCUITest adapters;
- Yandex Mail Trigger → API Gateway WebSocket receipt без доступа CI к
  человеческому mailbox/private inbound bucket;
- exact deployment SHA, один OTP issue, один verify, одна idempotent participant
  registration, registration `200`, membership и returning state;
- deterministic build-time transport fault profiles;
- scenario registry, platform selector, GitHub issue `/qa run` gateway и единый
  PII-free evidence index с redaction gate.

Terminal single-client-route matrix:

| Fault | Android | iOS | Результат |
|---|---:|---:|---|
| direct Supabase недоступен → Yandex relay | 30772062840 | 30772233868 | issue/verify/registration только relay, `1/1/1`, one mail |
| Yandex relay недоступен → direct Supabase | 30772957771 | 30773125445 | issue/verify/registration только direct, `1/1/1`, one mail |

На iOS приняты Mobile Safari/XCUITest и реальные системные email/numeric
клавиатуры. Исторический run `30754894934` остаётся
`BLOCKED_SAFARI_FIRST_RUN_UI` с `0/0/0`; он не является keyboard failure и не
заменяет последующую acceptance.

Исправлены выявленные дефекты: disposable telemetry не отравляет здоровье
общего data route, её ожидаемые failures классифицируются warning; устранён
Android keyboard-observation race; route acceptance симметрична обоим
направлениям отказа.

Полевое наблюдение 2026-08-03 на физическом iPhone показало direct Auth/Data OK,
Yandex relay Auth/Data и YDB control/API Gateway — network failure, resilient
Auth/Data — OK через direct. Это подтверждает реальность partial Yandex client
path outage и необходимость truthful degraded UX. Наблюдение не доказывает
глобальное падение всего Yandex.

Не существует и не должно считаться готовым:

- `both_client_routes_unreachable` product/degraded no-mail acceptance;
- `supabase_upstream_unavailable` product/degraded no-mail acceptance;
- YDB projection outage + durable recovery acceptance;
- focus feedback component partial-delivery acceptance;
- Yandex OAuth unavailable → email fallback acceptance;
- Postbox provider outage/reconciliation acceptance;
- inbound Yandex pipeline outage/replay acceptance;
- native install/relaunch PWA acceptance;
- автоматизация всех будущих page/data/personalization scenarios.

Текущий service worker network-only. Пока продуктовый контракт не изменён,
release gate не требует offline content availability. Проверяется честный
network failure, install identity, persisted state и standalone lifecycle.

## 3. Единица тестирования

### 3.1. Сценарий

Сценарий — одна бизнес-проверка со стабильным ID, например:

- `focus.otp.browser_tab`;
- `focus.otp.ios_keyboard_preflight`;
- `connectivity.yandex_partial_outage_truth`;
- `personalization.ydb_projection_outage`;
- `focus.feedback.partial_component_delivery`;
- `email.postbox_unavailable_durable_outbox`;
- `focus.pwa.install_launch`;
- `browser.route_health`;
- `data.content_minimum`.

Сценарий не равен workflow run и не обязан иметь отдельный workflow-файл.

### 3.2. Suite

Suite — выбранный набор сценариев:

- `pr-fast`;
- `feature`;
- `catalog`;
- `visual`;
- `mobile-critical`;
- `reliability`;
- `release`;
- `post-deploy`.

### 3.3. Platform adapter

Одна бизнес-логика может иметь адаптеры:

- `browser` — Playwright;
- `android` — Appium UiAutomator2 + Chrome Android;
- `ios` — Appium XCUITest + Mobile Safari;
- `server` — unit/integration job с durable outbox/provider fixtures.

Платформенные действия не размножают бизнес-сценарий на несогласованные копии.

### 3.4. Target

Поддерживаемые targets:

- `artifact` — локально собранное дерево;
- `preview` — опубликованный noindex preview;
- `candidate` — immutable release candidate;
- `staging` — контролируемый write-capable environment;
- `production` — stable public origin.

OTP, PWA installation и field connectivity проверяются против опубликованного
HTTPS target с exact repo SHA. Локальный HTTP не заменяет secure-context и
real-host acceptance.

### 3.5. Capability и acknowledgement

Yandex не является одной тестовой зависимостью. Scenario registry указывает
конкретный class:

- `supabase_relay`;
- `ydb_analytics`;
- `ydb_control`;
- `yandex_oauth`;
- `postbox_transactional`;
- `yandex_inbound_pipeline`;
- `object_storage_cdn`;
- `e2e_mail_trigger`.

Для write-сценария отдельно фиксируются primary-store acknowledgement,
component receipts, route/provider state и допустимый replay contract. PASS не
может основываться только на попытке dispatch.

## 4. Селектор запуска

Решение принимает scenario registry, а не память агента. Для каждого сценария
фиксируются layers, platforms, trigger tags, cost, side effects, fault profiles,
evidence и blocking policy.

### 4.1. Обязательный синхронный минимум

Агент должен дождаться результата до handoff:

- релевантные L0 contracts;
- короткий L1 smoke изменённой page family;
- unit/component regressions PR;
- release-blocking scenario, если изменение меняет его contract;
- acknowledgement/idempotency tests при изменении write/outbox/provider flow;
- redaction/audit tests при изменении evidence или OTP.

### 4.2. Background advisory

Тяжёлый run разрешено запустить и не ждать, если он не gate текущего PR:

- полный browser catalog crawl;
- расширенная visual выборка;
- Android/iOS nightly suite;
- cross-browser sample после data-only изменения;
- read-only connectivity field canary;
- post-merge health/backlog sweep.

Правила:

1. Agent возвращает run URL/ID, exact SHA, suite и target.
2. Outcome — `STARTED_BACKGROUND`, не PASS.
3. Workflow публикует `qa-summary.json`, `junit.xml` и безопасный artifact.
4. FAIL/BLOCKED создаёт наблюдаемый follow-up.
5. Перед release promotion все обязательные signals terminal и имеют
   disposition.

### 4.3. Protected manual

Real-mail OTP, fresh-user identity, destructive reset, production write probe,
provider ambiguous reconciliation и paid device cloud запускаются явно через
protected Environment с concurrency/side-effect policy.

### 4.4. Нельзя «запустить и забыть»

`STARTED_BACKGROUND` — промежуточный operational outcome. Если run завершился
после merge, terminal result попадает в следующий candidate/release decision.

## 5. Change classes и выбор платформ

| Изменение | L0 | L1/browser | Android | iOS | Protected side effect |
|---|---:|---:|---:|---:|---:|
| Docs/copy без runtime contract | выборочно | нет/короткий smoke | нет | нет | нет |
| Data exporter/manifest/ICS/SEO | да | affected/full sample | нет | нет | нет |
| Layout/CSS без native integration | да | desktop+mobile viewport | representative high-risk | representative high-risk | нет |
| Input/focus/keyboard | да | да | да | да | при Auth flow |
| PWA contract | да | да | да | да | при onboarding/Auth coupling |
| Focus Auth/Supabase relay/email hook | да | да | да | да | real OTP |
| Diagnostic acknowledgement/copy | да | full fault matrix | critical mobile sample | critical mobile sample | нет |
| YDB analytics/outbox | server contract | browser integration | representative | representative | staging write/replay |
| Focus feedback/screenshot | да | fault/component matrix | при mobile UI | при mobile UI | fixed identity/object fixture |
| Yandex OAuth | server/browser | да | sample | sample | controlled Auth identity |
| Postbox transactional routing | server/outbox | integration | background sample | background sample | protected provider gate |
| Inbound Yandex pipeline | server/outbox | нет | нет | нет | controlled mailbox fixture |
| Full catalog non-empty | full | sharded | specimens | specimens | нет |
| Push/background/OEM/performance | частично | нет | simulator partial | simulator partial | L3 |

### Trigger tags

Минимальный набор:

- `static-data`;
- `static-route`;
- `visual-layout`;
- `mobile-input`;
- `pwa-system`;
- `auth-otp`;
- `supabase-connectivity`;
- `yandex-relay`;
- `yandex-sidecar`;
- `yandex-oauth`;
- `provider-mail`;
- `durable-outbox`;
- `partial-delivery`;
- `personalization`;
- `release-publisher`.

Path detector предлагает tags, но registry остаётся source of truth. Override
разрешён только в сторону усиления или с документированным waiver.

## 6. GitHub Actions lanes

### PR fast — blocking

- lint/unit/contracts;
- preview/artifact check;
- affected browser smoke;
- operation catalog/idempotency/acknowledgement lint;
- changed-route screenshots on failure;
- scenario selector report.

Эмуляторы по умолчанию не запускаются.

### PR reliability-sensitive — mixed

Запускается при `mobile-input`, `auth-otp`, `yandex-relay`, `yandex-sidecar`,
`durable-outbox`, `provider-mail`:

- full deterministic browser/server fault matrix — blocking для изменённого
  contract;
- Android/iOS critical scenario — blocking при прямом mobile-system изменении;
- real OTP/provider write — protected/manual либо release integration;
- no-mail/no-provider fixtures применяются в both-down/upstream paths.

### Main/nightly — background advisory

- full route crawl/content minimum;
- visual specimens;
- Android/iOS mobile-critical suite;
- read-only connectivity canaries;
- YDB/provider/outbox backlog/replay fixtures без production side effects;
- aggregate `qa-summary.json`.

Nightly не создаёт новых пользователей и не рассылает real OTP массово.

### Release candidate — blocking

- production/candidate artifact contracts;
- generated-candidate browser gate;
- full route health;
- selected reliability matrix по изменённым capabilities;
- Android/iOS critical journeys;
- protected real OTP для Auth/onboarding/mail-route релиза;
- PWA install/relaunch при изменении PWA;
- terminal disposition background signals;
- durable ack/no-loss/partial-delivery gates.

### Post-deploy

- короткий read-only production smoke;
- manifest/service-worker canary;
- direct/relay + Yandex control capability receipt;
- backlog/oldest-item monitoring;
- fresh-device PWA/field canary периодически.

## 7. Закрытый mobile milestone: focus OTP

Общая семантика browser/Android/iOS:

1. открыть exact invitation URL;
2. проверить origin и deployed repo SHA;
3. принять приглашение и пропустить install;
4. сфокусировать email input и проверить native email keyboard на L2;
5. ввести fixed test identity;
6. доказать ровно один OTP issue при competing gestures;
7. получить ровно одно real-mail message после checkpoint;
8. проверить numeric/one-time-code keyboard;
9. ввести OTP обычным input path;
10. доказать один verify и одну participant registration;
11. увидеть membership confirmed;
12. reload/reopen — returning state без нового OTP;
13. проверить fault activation, exact final route, opposite-route absence;
14. выпустить redacted evidence.

Mailbox jobs последовательны при одном fixed mailbox. Запрещены service-role
key, fixed OTP/bypass, blind resend, raw mail, OTP/JWT/HAR/trace/video.

## 8. Критические page/data/reliability scenarios

### Artifact/data

- HTML/JSON/ICS/assets не пусты;
- manifest/catalog/routes согласованы;
- event ID/slug/projection не перепутаны;
- typed empty state вместо молчаливой пустоты;
- canonical/robots/sitemap/JSON-LD;
- image decode/accepted fallback;
- stale/partial batch fail-closed.

### Browser route health

- expected HTTP/origin/route marker;
- ненулевой `<main>`;
- нет infinite skeleton/`aria-busy`;
- нет critical page/console/network error;
- нет overflow/sticky overlap;
- CTA доступны;
- screenshot on failure.

### Персональные поверхности

- no unauthorized leakage;
- deterministic immature-profile fallback;
- Supabase direct/relay read/write semantics;
- local-first actions + bounded idempotent outbox;
- last-good profile projection;
- YDB analytics outage не влияет на primary action;
- reconnect/reload exactly-once;
- truthful pending/partial/ambiguous copy.

### Yandex-dependent sidecars/providers

- relay outage не заражает direct;
- YDB projection failure не откатывает core action;
- Yandex OAuth outage оставляет email fallback;
- Postbox outage сохраняет durable outbox и не показывает false sent;
- inbound pipeline outage сохраняет SpaceWeb original и допускает replay;
- Object Storage publish failure удерживает last-good;
- diagnostic summary не делает global Yandex claim по частичному endpoint
  failure.

## 9. Стратифицированная мобильная выборка

Не открывать сотни страниц на симуляторах. Выбираются specimens:

- single/multiple/no image;
- portrait/wide/OCR;
- free/ticket/registration/calendar CTA;
- transport/no transport;
- medallion/no medallion/conflict;
- anonymous/authenticated/personal;
- pending/partial/ambiguous degraded state;
- direct-only/relay-only/Yandex-degraded summary;
- empty/underfilled block.

Specimen identity/reason сохраняются. Expired live event не удаляет executable
contract: критическая геометрия имеет frozen fixtures.

## 10. Evidence, доступный ChatGPT

Каждый job публикует безопасный пакет:

```text
evidence/
├── manifest.json
├── qa-summary.json
├── run.json
├── scenarios.jsonl
├── junit.xml
├── routes.jsonl
├── device.json
├── transport-events.sanitized.jsonl
├── dependency-events.sanitized.jsonl
├── component-receipts.sanitized.json
├── outbox-summary.sanitized.json
├── screenshots/
├── native-ui/
├── console.sanitized.jsonl
├── network.sanitized.jsonl
└── redaction-audit.json
```

Обязательные поля:

- repository/full SHA/build/snapshot/tree;
- run ID/attempt/target;
- suite/scenario/platform;
- OS/browser/device/locale/timezone;
- blocking/advisory/manual mode;
- PASS/FAIL/BLOCKED/STARTED_BACKGROUND;
- failure domain/first failed step;
- capability/dependency class;
- operation semantics/selected route;
- primary-store acknowledgement;
- component receipts;
- local/server outbox state, attempts, next retry and oldest age;
- user-message class;
- redaction status.

Artifact name:

`static-site-qa-<suite>-<platform>-<run_id>-<attempt>`.

`qa-summary.json` открывается первым и позволяет определить, что запускалось,
какая capability отказала, сохранилось ли основное действие и какие files
смотреть. Для real OTP запрещены video/trace/HAR/raw mail; маскированные
screenshots и structured sanitized events обязательны.

## 11. Политика outcomes

- **PASS:** все mandatory assertions на exact target выполнены.
- **FAIL:** продукт/данные/контракт неверны.
- **BLOCKED:** среда/secret/runner/mailbox/simulator/target не позволили выполнить
  проверку честно.
- **STARTED_BACKGROUND:** run создан, terminal результата нет; не PASS.
- **SKIPPED_NOT_APPLICABLE:** registry доказал неприменимость.
- **NOT_IMPLEMENTED:** scenario/product planned.

Один bounded retry допустим только для доказанного infrastructure flake до
side effects. Product assertion, selected-once dispatch и provider send не
повторяются автоматически.

## 12. Экономичность

- L0/L1 сначала, L2 после зелёного результата;
- full crawl шардируется;
- screenshots/video on failure/selected specimen;
- iOS/macOS не запускается на data-only PR;
- real OTP не nightly и не fresh-user по умолчанию;
- no-mail deterministic faults для both-down/upstream;
- один fixed identity/mailbox и sequential run;
- AI visual review — advisory, deterministic assertions — gate;
- тест добавляется по реальному regression risk.

## 13. Release integration

`release-plan.md` остаётся umbrella release truth. Этот документ определяет
способ доказательства browser/mobile/data/reliability gates.

Release decision перечисляет:

- exact candidate SHA/tree;
- selected scenarios/selector reason;
- blocking terminal results;
- advisory results/disposition;
- Android/iOS requirement или `not_applicable` evidence;
- protected side-effect requirement/run;
- primary acknowledgement/outbox/partial-delivery result для изменённой Yandex
  capability;
- known planned gaps.

NO-GO, если:

- изменён Auth/PWA/mobile-input contract без required mobile result;
- external OTP FAIL/BLOCKED при релизе соответствующей функции;
- target SHA mismatch;
- redaction FAIL;
- mandatory background не terminal;
- full catalog имеет unexplained broken/empty route;
- planned feature отмечена pass;
- user-visible success возможен без durable acknowledgement;
- accepted action теряется при YDB/sidecar outage;
- ambiguous selected-once повторяется;
- partial component failure скрывается;
- Yandex capability conflation создаёт неверную коммуникацию;
- outbox может молча потерять payload.

## 14. Поступательная реализация

### M0 — документы и agent contract

Стратегия, release companion, registry, handoff и scoped AGENTS.

### M1 — OTP platform-neutral extraction

Завершено: shared journey/evidence/adapters при сохранённом Chromium path.

### M2 — Android browser-tab OTP

Завершено: Android Emulator + Chrome + UiAutomator2, native keyboard и protected
real mail; baseline и оба single-route fault профиля terminal PASS.

### M3 — iOS browser-tab OTP

Завершено: Mobile Safari + XCUITest, bounded first-run handling, native
email/numeric keyboards, one-mail `1/1/1`, returning state и оба single-route
fault профиля terminal PASS.

### M4 — PWA install/relaunch

Отдельные Android/iOS scenarios без ложного offline-first обещания, с проверкой
persisted membership/outbox state.

### M5 — generic browser/data registry runner

Affected route health, content minimum и sharded catalog evidence.

### M6 — Yandex dependency reliability

- truthful partial-outage diagnostics;
- both-client-routes-down и shared-upstream no-mail tests;
- YDB projection outage + durable recovery;
- focus feedback component receipts;
- OAuth fallback;
- Postbox/inbound pipeline outbox/reconciliation;
- browser full matrix + mobile representative acceptance.

### M7 — новые product scenarios

Medallions, people cards, personal pages, connectivity, personalization и другие
surfaces добавляются вместе с implementation/audit и сразу наследуют M6
acknowledgement/no-loss contract.

Android/iOS нельзя откладывать после общей системы, но и нельзя запускать full
emulator catalog вместо точных критических journeys.
