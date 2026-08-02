# Промпт кодовому агенту: первый mobile-autotest vertical slice

Скопируй текст ниже как задачу Codex. Он предназначен для отдельного
implementation PR после принятия стратегии.

---

## Задача

Работай в репозитории `onedayonemasterpiece/events-bot-new` от свежего
`origin/main`.

Сначала прочитай и соблюдай:

1. `AGENTS.md`;
2. `site/AGENTS.md`;
3. `.github/AGENTS.md`;
4. `.codex/skills/static-site-autotest/SKILL.md`;
5. `docs/operations/static-site-autotest-strategy.md`;
6. `docs/features/static-site-pages/release-autotest-gates.md`;
7. `docs/testing/static-site-autotest-scenarios.v1.yml`;
8. `docs/testing/external-focus-email-otp.md`;
9. `docs/features/static-site-focus-group/README.md`, разделы про stable PWA
   identity, email verification и external E2E.

### Проверенный текущий baseline

Не проектируй новый OTP harness с нуля. Уже существуют:

- `.github/workflows/external-focus-email-otp.yml`;
- `site/e2e/focus-email/run.mjs`;
- helpers/tests внутри `site/e2e/focus-email/`;
- npm command `e2e:external-focus-email-otp`;
- protected Environment `external-e2e` как documented contract;
- real Chromium + IMAPS flow с exact repo SHA, one issue, one verify, one
  registration, reload/returning state и redaction-gated evidence.

Исходная реализация вошла через PR #202; отдельный harness commit —
`00f4641035fd9675c7b2777583aa9de5ee71010b`. Состояние `origin/main` важнее этих
исторических SHA: сначала проверь фактические текущие файлы.

## Цель PR

Сделай **только первый mobile OTP vertical slice**:

1. сохранить существующий Chromium real-mail journey полностью работоспособным;
2. выделить platform-neutral semantic journey и общий evidence contract;
3. добавить Android Emulator + настоящий Chrome Android + Appium UiAutomator2;
4. добавить iOS Simulator + настоящий Mobile Safari + Appium XCUITest;
5. прогнать `focus.otp.browser_tab` на Android и iOS с реальной системной
   клавиатурой и тем же защищённым IMAPS OTP;
6. оставить real-mail browser/Android/iOS последовательными, пока используется
   один fixed mailbox;
7. дать одинаковый sanitized artifact, доступный для отдельного анализа ChatGPT;
8. обновить docs/registry/changelog по факту реализации.

Не реализуй в этом PR:

- общий framework всех статических страниц;
- full catalog mobile crawl;
- people/celebrity cards;
- `Для меня` pages;
- новую personalization logic;
- PWA install через Launcher/SpringBoard;
- offline-first service worker;
- push notifications;
- физические device-cloud runs.

`focus.pwa.install_launch` — следующий отдельный PR после стабильного
browser-tab OTP на обеих мобильных платформах.

## Архитектурный contract

### Общая бизнес-логика

Вынеси из текущего `run.mjs` semantic journey, например:

```js
await runFocusOtpBrowserTab({
  ui,
  mailbox,
  networkProbe,
  releaseIdentity,
  evidence,
});
```

Точное имя и раскладка файлов могут отличаться, но должны существовать:

- shared journey;
- Playwright browser adapter;
- Android Appium adapter;
- iOS Appium adapter;
- shared IMAPS adapter;
- shared sanitized evidence writer;
- compatibility entrypoint для текущего npm/workflow path.

Семантический сценарий не должен знать Appium locators, Chrome menu labels,
XCUITest selectors или конкретный механизм screenshot.

### Общие semantic operations

Адаптер должен предоставлять смысловые операции, а не случайные CSS детали:

- `openInvite()`;
- `verifyReleaseIdentity()`;
- `waitForInstallStage()`;
- `skipInstall()`;
- `openEmailStep()`;
- `focusEmailInput()`;
- `enterEmail()`;
- `requestOtpWithCompetingGestures()`;
- `waitForCodeStep()`;
- `focusOtpInput()`;
- `enterOtpDigitByDigit()`;
- `waitForMembershipConfirmed()`;
- `reloadOrReopen()`;
- `waitForReturningMember()`;
- `captureMaskedEvidence()`.

Не создавай три копии business assertions.

## Обязательный пользовательский путь

Для browser, Android и iOS доказать:

1. exact invitation URL открыт на допустимом `kenigevents.ru` origin;
2. `preview-build.json`/release metadata содержит ожидаемый full repo SHA;
3. invitation/install stage видим;
4. установка PWA пропущена;
5. email step открыт;
6. fixed E2E email введён обычным user-input path;
7. два конкурирующих обычных gestures приводят ровно к одному
   `POST /auth/v1/otp`;
8. после pre-request IMAP checkpoint найдено ровно одно подходящее письмо;
9. six-digit OTP введён посимвольно без Enter;
10. ровно один `POST /auth/v1/verify`;
11. ровно один `POST /rpc/register_focus_group_participant_v1`;
12. registration status только из уже принятого множества;
13. UI показывает `Участие подтверждено`;
14. reload/reopen показывает returning state;
15. OTP после reload/reopen не выпускается повторно;
16. artifact проходит redaction audit.

Сохрани текущую защиту selected-once issuance. Не добавляй blind retry на
alternate route после ambiguous OTP request.

## Android

Используй GitHub-hosted Linux runner с аппаратно ускоренным Android Emulator.
Требования:

- фиксированный Android API/system image и device profile;
- Chrome Android внутри emulator;
- Appium UiAutomator2;
- web context для product DOM;
- native context для keyboard/viewport/system state;
- locale и timezone фиксированы и записаны в evidence;
- фактические Android/Chrome/Appium версии записаны в `device.json`;
- actions и зависимости pinned; не использовать floating `@main`/`latest`.

OTP тест выполняется против опубликованного HTTPS target, а не `10.0.2.2`.
Локальный `adb reverse + http://localhost` допустим только для отдельного
contract smoke, но не считается external OTP acceptance.

### Android keyboard acceptance

После focus email input:

- системная клавиатура реально показана;
- email input остаётся active и видимым;
- доступен email-oriented input path;
- CTA не превращается в недоступный тупик.

После focus OTP input:

- системная numeric/one-time-code keyboard реально показана;
- input/code cells остаются видимыми;
- обычный ввод каждой цифры вызывает product events;
- шестая цифра запускает текущий autosubmit path.

Не проверяй хрупкий pixel-perfect текст каждой клавиши. Проверяй native keyboard
presence, focus, viewport geometry и успешный ordinary input path.

## iOS

Используй GitHub-hosted macOS runner, зафиксированный runner/Xcode/iOS runtime и
конкретную модель iPhone.

Требования:

- Appium XCUITest;
- Mobile Safari;
- web context для DOM;
- native context для keyboard/viewport/system state;
- не использовать Playwright WebKit как замену;
- перед запуском проверить доступность exact simulator runtime/device;
- фактические macOS/Xcode/iOS/Safari-WebKit/Appium версии записать в evidence;
- не выбирать случайный `last available iPhone` без recorded/pinned contract.

### iOS keyboard acceptance

Аналогично Android:

- реальная system keyboard после focus;
- email-oriented input path;
- one-time-code/numeric path для OTP;
- active input и critical UI видимы;
- digit-by-digit entry проходит через product events;
- никаких прямых JS присваиваний `input.value` вместо пользовательского ввода.

Share Sheet/Add to Home Screen/SpringBoard в этот PR не входят.

## Workflow design

Не создавай по workflow-файлу на каждый semantic step.

Допустим один reusable mobile workflow плюс защищённый orchestrator либо
расширение текущего external OTP workflow. В любом случае должны быть явные
jobs/variants:

- `browser`;
- `android`;
- `ios`.

Пока один mailbox:

```text
browser -> android -> ios
```

или отдельный явно выбранный platform run. Не выполнять три real OTP jobs
параллельно.

Добавь `workflow_dispatch` inputs минимум:

- `target_url`;
- `expected_repo_sha`;
- `platform`: `browser | android | ios | all`;
- возможно `evidence_mode`, но real OTP всегда остаётся restricted.

Сохрани:

- `permissions: contents: read`;
- protected `external-e2e` Environment;
- global concurrency;
- bounded timeout;
- pinned checkout/setup/upload actions;
- отсутствие arbitrary ref checkout;
- отсутствие service-role/provider keys в runner.

Не запускай real OTP автоматически на каждом PR или nightly.

## Evidence contract

Для каждой platform создавай отдельный безопасный artifact:

`static-site-qa-focus-otp-<platform>-<run_id>-<attempt>`.

Минимум:

```text
manifest.json
qa-summary.json
result.json
steps.json
scenarios.jsonl
junit.xml
device.json
network.sanitized.jsonl
console.sanitized.jsonl
mail-delivery.sanitized.json
screenshots/
native-ui/
redaction-audit.json
.redaction-ok
```

`qa-summary.json` открывается первым и содержит:

- scenario ID `focus.otp.browser_tab`;
- platform;
- exact repo SHA expected/observed;
- target origin/path;
- PASS/FAIL/BLOCKED;
- failure domain;
- first failed step;
- issue/verify/registration counts;
- keyboard acceptance summary на Android/iOS;
- device/runtime versions;
- relative evidence paths;
- redaction status.

### Запрещено сохранять для real OTP

- email address;
- OTP;
- raw message/body/headers;
- cookies/JWT/authorization headers;
- HAR;
- Playwright/Appium trace;
- video/screen recording;
- notification banner с кодом;
- native hierarchy, если он содержит unmasked email/OTP.

Скриншоты после ввода делаются только после masking. До ввода разрешены только
явно пустые keyboard-control/product поля и safe Safari blocker frame. Перед upload выполняется fail-closed
redaction audit. `BLOCKED` artifact тоже должен быть доступен, если безопасен.

## Failure taxonomy

Сохрани/расширь минимум:

- `BLOCKED_INFRASTRUCTURE`;
- `BLOCKED_SAFARI_FIRST_RUN_UI`;
- `BLOCKED_IOS_SIMULATOR_KEYBOARD`;
- `FAIL_RELEASE_EVIDENCE`;
- `FAIL_DELIVERY`;
- `FAIL_PRODUCT`;
- `FAIL_MOBILE_KEYBOARD`;
- `FAIL_MOBILE_VIEWPORT`;
- `FAIL_BROWSER_CONTEXT`;
- `FAIL_REDACTION`.

Один bounded retry разрешён только для доказанного simulator/Appium startup
flake до side effect. Не повторяй OTP issuance автоматически после возможного
side effect.

Перед полным iOS OTP выполни три последовательных
`focus.otp.ios_keyboard_preflight`: exact allowlisted Safari first-run state,
empty control email/numeric keyboards, empty product email keyboard и строго
`issue/verify/registration=0/0/0`. Только затем запускай один real-mail iOS run.

## Tests до live run

Добавь детерминированные tests для:

- shared journey вызывает semantic steps в правильном порядке;
- browser adapter сохраняет текущие assertions;
- Android/iOS adapter config validation;
- platform selector;
- sequential mailbox/concurrency policy;
- evidence schema для всех platforms;
- masking/redaction, включая native hierarchy fixtures;
- keyboard acceptance classifier на synthetic safe fixtures;
- BLOCKED при отсутствующем simulator/runtime/config;
- current MIME parser fixtures остаются зелёными.

Не мокай real delivery и не называй fixtures external E2E PASS.

## Порядок реализации

1. Fresh branch от `origin/main`.
2. Аудит текущего OTP workflow/run/helpers/tests и package-lock.
3. Минимальный refactor shared journey без изменения observable Chromium path.
4. Полный local test existing browser harness.
5. Android adapter/job + local/config tests.
6. Один protected Android real-mail run на exact immutable target.
7. Исправить только доказанные проблемы, сохранить evidence.
8. iOS adapter/job + local/config tests.
9. Один protected iOS real-mail run на тот же exact target/SHA.
10. Повторный browser compatibility run, если refactor затронул journey.
11. Обновить registry statuses только после terminal evidence.
12. Обновить docs и `CHANGELOG.md`.
13. Открыть draft PR с exact commands, run URLs, artifact names и known limits.

Не включай Auth Send Email Hook глобально и не переноси transport на остальные
страницы только потому, что component tests зелёные. Для такого rollout нужны
terminal protected Android/iOS/browser evidence и отдельное release решение.

## Acceptance checklist

PR можно передавать на review только если:

- [ ] существующий Chromium path не ослаблен и local tests зелёные;
- [ ] shared journey не содержит platform locators;
- [ ] Android использует Chrome Android в emulator;
- [ ] iOS использует Mobile Safari в iOS Simulator;
- [ ] real system keyboards доказаны обеими platforms;
- [ ] browser/Android/iOS real-mail jobs не конкурируют за один mailbox;
- [ ] exact deployed repo SHA проверяется;
- [ ] issue=1, verify=1, registration=1;
- [ ] returning state сохраняется без reissue;
- [ ] video/trace/HAR отсутствуют;
- [ ] redaction gate fail-closed;
- [ ] ChatGPT-readable `qa-summary.json` и artifacts опубликованы;
- [ ] actions/runtime versions pinned/recorded;
- [ ] registry/docs/changelog синхронизированы;
- [ ] PWA install и общий catalog framework не попали в scope creep.

## Итоговый handoff

В PR body и финальном ответе укажи:

- branch, commit, draft PR;
- base/main SHA;
- изменённые files;
- local tests с точными результатами;
- GitHub Actions run URL/ID для browser, Android и iOS;
- exact target URL без раскрытия bearer secret;
- expected/observed repo SHA;
- artifact names и retention;
- PASS/FAIL/BLOCKED по каждой platform;
- keyboard evidence summary;
- redaction audit;
- что остаётся для отдельного `focus.pwa.install_launch` PR.

Нельзя писать «Android/iOS готовы», если существуют только workflow skeleton или
configuration tests без terminal emulator/simulator run.

---
