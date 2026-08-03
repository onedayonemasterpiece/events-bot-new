# Автотесты как release gate статического сайта

> **Статус:** нормативный companion к [`release-plan.md`](release-plan.md).
> Этот документ не создаёт второй release plan. Он определяет, какие
> автоматизированные доказательства нужны для закрытия соответствующих gates.
>
> Полная стратегия:
> [`../../operations/static-site-autotest-strategy.md`](../../operations/static-site-autotest-strategy.md).
>
> Обязательный reliability contract для Yandex-зависимостей:
> [`../../operations/yandex-dependency-resilience.md`](../../operations/yandex-dependency-resilience.md).

## 1. Release truth

Release truth остаётся `origin/main` + exact immutable candidate identity.
Локальный checkout, side branch, mobile viewport screenshot и незавершённый
background run не закрывают release gate.

Каждый release evidence record должен содержать:

- full repository SHA;
- build/snapshot/tree identity;
- exact target;
- suite/scenario/platform;
- selector reason;
- PASS/FAIL/BLOCKED;
- artifact/run link;
- redaction result;
- disposition для advisory/background signals;
- для Yandex-зависимой операции: capability, primary-store acknowledgement,
  component receipts, selected route и pending/ambiguous/retry state.

## 2. Обязательные gates по типу изменения

| Release surface | Обязательное доказательство |
|---|---|
| Artifact/data/exporter | L0 full affected contract + browser sample |
| Event/listing route layout | L0 + L1 affected route families + frozen geometry fixtures |
| Full catalog publication | full L0 catalog + sharded L1 route health |
| Input/focus/keyboard | L1 + Android Emulator + iOS Simulator critical scenario |
| PWA manifest/install/start URL/scope/SW | L0 + L1 + Android/iOS system integration |
| Focus onboarding/Auth/OTP | existing browser OTP + Android browser-tab OTP + iOS browser-tab OTP |
| Supabase direct/Yandex relay change | direct/relay contracts + affected browser/mobile journey + no duplicate dispatch/effect |
| YDB analytics/control change | primary action succeeds independently; durable projection outbox; backlog/replay evidence |
| Focus feedback/NPS/screenshot | component receipts, partial-delivery copy, reload/reconnect exactly-once |
| Yandex OAuth | provider failure + email-OTP fallback + callback reconciliation |
| Postbox transactional mail | durable Supabase outbox, provider-acceptance semantics, ambiguous/no-duplicate recovery |
| Yandex inbound pipeline | SpaceWeb original retained, cursor/idempotency/YMQ/DLQ replay |
| Personalization/personal pages | no-leak/data contract + authenticated browser journey; local-first/outbox fault matrix; mobile sample when UI/input changes |
| Data-only copy/facts update | no mandatory emulator unless it changes a mobile-critical component |

## 3. Blocking, background и manual

### Blocking

Агент ждёт terminal result до handoff и release не продолжается:

- affected contracts;
- changed feature browser smoke;
- Android/iOS при прямом изменении mobile-system contract;
- protected real OTP при promotion Auth/onboarding/mail-routing change;
- durable acknowledgement/idempotency gates для изменённой Yandex capability;
- evidence redaction gate.

### Background advisory

Можно запустить и не ждать в текущем PR:

- full catalog crawl после локального affected pass;
- expanded visual sample;
- Android/iOS nightly при data-only изменении;
- cross-browser extended matrix;
- read-only connectivity field canary, если он не является gate изменённого
  transport contract.

Handoff обязан назвать run как `STARTED_BACKGROUND`, указать run ID/URL, SHA и
scenario set. Такой run не является PASS. Перед release promotion все связанные
signals должны иметь terminal result и disposition.

### Protected manual

- real mailbox OTP;
- fresh-user identity;
- production write probe;
- provider reconciliation/ambiguous mail disposition;
- paid device-cloud L3.

Эти jobs используют защищённый Environment, bounded concurrency и отдельный
side-effect contract. Secrets не передаются browser catalog или visual jobs.

## 4. Первый mobile transport milestone — закрыт

Первый законченный mobile milestone был реализован как модификация существующего
isolated focus-group OTP harness, а не новый параллельный framework:

1. сохранён Chromium baseline;
2. выделен shared semantic journey;
3. Android Emulator + Chrome + системные клавиатуры приняты;
4. iOS Simulator + Mobile Safari/XCUITest + системные email/numeric клавиатуры
   приняты;
5. real-mail variants выполняются последовательно;
6. каждый terminal artifact доказывает one issue / one verify / one participant
   registration, registration `200`, membership и returning state;
7. выпущен одинаковый sanitized evidence contract;
8. fault действительно активируется, opposite route и повторные side effects
   отсутствуют.

Terminal single-route matrix:

| Fault | Android | iOS | Acceptance |
|---|---:|---:|---|
| direct Supabase недоступен → Yandex relay | 30772062840 | 30772233868 | все обязательные операции через relay |
| Yandex relay недоступен → direct Supabase | 30772957771 | 30773125445 | все обязательные операции через direct |

Исторический iOS run `30754894934` остаётся
`BLOCKED_SAFARI_FIRST_RUN_UI`, а не keyboard failure. Он был до исправления и не
заменяет последующую terminal acceptance.

Этот milestone доказывает отказ **одного клиентского маршрута**. Он не доказывает
работу при одновременном отказе обоих клиентских путей или общего Supabase
upstream; это отдельные no-mail/degraded gates.

## 5. Полевой Yandex-degraded gate

Полевой скриншот участника от 2026-08-03 подтвердил реальную конфигурацию:
прямой Supabase доступен, Yandex relay и YDB control/API Gateway не отвечают,
resilient операции выбирают direct.

Release contract требует:

- не интерпретировать это как глобальное падение всего Yandex;
- показывать `CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED`, а не общий failure;
- не просить повторять уже confirmed action;
- не использовать YDB control/analytics как acknowledgement основной операции;
- не позволять optional telemetry отравлять health прямого/relay product route;
- сохранять pending YDB/provider projection в durable outbox;
- различать relay, YDB, OAuth, Postbox, inbound и Object Storage capabilities.

Изменение диагностической страницы, acknowledgement copy или любого
Yandex-dependent write path блокируется до browser acceptance; Android/iOS
становятся blocking, если меняется mobile-critical UI/input/flow.

## 6. Page/data rollout

Сценарии добавляются поступательно вместе с реализацией или аудитом surface:

- route non-empty/content minimum;
- transport blocks;
- venue/source medallions;
- people/headliner/celebrity cards;
- authenticated pre-generated `Для меня` pages;
- Supabase direct/relay и Yandex connectivity;
- personalization ordering/feedback;
- expected block content and typed empty states;
- YDB projection outage и reconnect recovery;
- focus feedback component partial delivery;
- OAuth/provider/inbound outage recovery.

`planned` не превращается в blocking до появления product contract. При
переходе в `implemented` одновременно обновляются machine-readable registry,
реализующий test, release gate и evidence sample.

## 7. Отдельный PWA gate

После browser-tab OTP добавляется `focus.otp.installed_pwa`:

- Android Chrome install UI → Launcher → standalone → relaunch;
- iOS Safari Share Sheet → Add to Home Screen → SpringBoard → relaunch;
- stable manifest `id`, `scope`, `start_url`;
- persisted participant state;
- честное network-only поведение service worker;
- сохранение pending/outbox state после standalone relaunch.

Offline content availability не является текущим обязательством и не должна
появляться как ложный release gate.

## 8. NO-GO

Release blocked, если:

- mobile-sensitive code изменён, а required Android/iOS result отсутствует;
- OTP result FAIL/BLOCKED либо target SHA не совпал;
- mandatory background run ещё не terminal;
- evidence содержит PII/OTP/token или не прошёл redaction;
- full catalog имеет unexplained empty/broken route;
- simulator run подменён desktop mobile viewport/WebKit;
- planned test представлен как passed implementation;
- один fixed mailbox используется параллельно несколькими real OTP jobs;
- принятый strong action/feedback может исчезнуть при YDB/Yandex sidecar outage;
- UI показывает «Отправлено» до durable primary/provider acknowledgement;
- составная операция скрывает partial failure;
- selected-once автоматически повторяется после ambiguous dispatch;
- YDB analytics failure изменяет core route health или откатывает main action;
- отсутствует stable idempotency/durable replay для изменённого Yandex-dependent
  send/projection;
- pending payload может быть молча вытеснен/удалён без terminal disposition;
- диагностика сообщает «Яндекс не работает» вместо точной capability;
- last-good release/profile projection уничтожается при сетевом отказе.

## 9. Экономический guardrail

- не запускать iOS/macOS для data-only PR;
- не открывать весь каталог на эмуляторах;
- сначала L0/L1, затем L2;
- screenshots/video only-on-failure или для selected specimens;
- real OTP только явно и последовательно;
- no-mail faults используются для both-down/upstream/provider scenarios;
- один bounded retry только для доказанного инфраструктурного flake;
- product assertion, selected-once dispatch и provider send не повторяются
  автоматически;
- deterministic gates решают release, AI visual review помогает triage.
