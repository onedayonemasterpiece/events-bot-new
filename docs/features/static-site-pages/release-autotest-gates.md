# Автотесты как release gate статического сайта

> **Статус:** нормативный companion к [`release-plan.md`](release-plan.md).
> Этот документ не создаёт второй release plan. Он определяет, какие
> автоматизированные доказательства нужны для закрытия соответствующих gates.
> Полная стратегия: [`../../operations/static-site-autotest-strategy.md`](../../operations/static-site-autotest-strategy.md).
> Auth fixture contract:
> [`../../testing/static-site-auth-session-fixture.md`](../../testing/static-site-auth-session-fixture.md).
> Yandex/degraded contract:
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
- auth mode и session scope, если применимо;
- PASS/FAIL/BLOCKED;
- product OTP issue и external mail send/receipt counts для Auth-сценариев;
- artifact/run link;
- cleanup/redaction result;
- disposition для advisory/background signals.

## 2. Обязательные gates по типу изменения

| Release surface | Обязательное доказательство |
|---|---|
| Artifact/data/exporter | L0 full affected contract + browser sample |
| Event/listing route layout | L0 + L1 affected route families + frozen geometry fixtures |
| Full catalog publication | full L0 catalog + sharded L1 route health |
| Input/focus/keyboard | L1 + Android Emulator + iOS Simulator critical scenario |
| PWA manifest/install/start URL/scope/SW | L0 + L1 + Android/iOS system integration; `session_fixture` для authenticated state |
| Focus invite/anonymous feedback | `anonymous_session` + owner-scoped feedback/component receipts; product OTP/mail `0/0/0` |
| Focus identity upgrade/Auth/OTP/mail | existing browser OTP + Android browser-tab OTP + iOS browser-tab OTP либо affected `yandex_oauth` journey |
| Supabase/Yandex Auth issue/verify route change | direct/relay contracts + affected real-mail browser/mobile journey |
| Search/personalization/feedback/saved state после входа | `auth.session_fixture` + authenticated product journey; real-mail OTP не требуется |
| Personalization/personal pages | no-leak/data contract + authenticated browser journey через `session_fixture`; mobile sample when UI/input changes |
| Data-only copy/facts update | no mandatory emulator or Auth fixture unless it changes an authenticated/mobile-critical component |
| Yandex relay/sidecar/OAuth/Postbox/inbound capability | affected deterministic fault scenario + truthful acknowledgement/degraded copy + durable replay/idempotency evidence |

## 3. Blocking, background и manual

### Blocking

Агент ждёт terminal result до handoff и release не продолжается:

- affected contracts;
- changed feature browser smoke;
- `auth.session_fixture`, если affected scenario требует настоящую identity;
- Android/iOS при прямом изменении mobile-system contract;
- protected real OTP при promotion Auth/onboarding/mail-routing change;
- evidence cleanup/redaction gate.

### Background advisory

Можно запустить и не ждать в текущем PR:

- full catalog crawl после локального affected pass;
- expanded visual sample;
- Android/iOS nightly при data-only изменении;
- cross-browser extended matrix.

Handoff обязан назвать run как `STARTED_BACKGROUND`, указать run ID/URL, SHA и
scenario set. Такой run не является PASS. Перед release promotion все связанные
signals должны иметь terminal result и disposition.

### Protected manual

- real mailbox OTP;
- fresh-user identity;
- production write probe;
- paid device-cloud L3.

Эти jobs используют защищённый Environment, bounded concurrency и отдельный
side-effect contract. Secrets не передаются browser catalog или visual jobs.

`session_fixture` также имеет trusted issuer boundary, но ordinary authenticated
business suite не должен из-за этого автоматически запускать real mailbox OTP.

## 4. Auth gate decomposition

### 4.1 `session_fixture`

Основной gate для функции после входа:

1. allowlisted fixed persona;
2. fresh admin-issued one-time credential без доставки;
3. штатный Supabase callback/verify;
4. настоящая session, `auth.getUser`/protected probe;
5. ephemeral per-worker/device state;
6. affected business journey;
7. `POST /auth/v1/otp = 0`;
8. external mail send/receipt `0/0`;
9. cleanup + redaction PASS.

Fixture failure — `BLOCKED_AUTH_FIXTURE`; fallback на реальное письмо запрещён.
Serialized session в GitHub Secret, фиксированный OTP, service-role key в
browser и общий refresh token между параллельными jobs не принимаются.

### 4.2 `admin_otp_ui`

Используется для формы кода, autosubmit, verify и callback без доказательства
доставки. Admin issuer создаёт свежий случайный credential; product OTP issue и
внешнее письмо остаются нулевыми.

### 4.3 `real_mail_otp`

Требуется только когда изменены:

- `signInWithOtp`/OTP issue UI;
- email hook/provider/routing/template;
- Auth direct/relay issue/verify semantics;
- Mail Trigger/IMAP adapter;
- email/OTP mobile keyboard/input;
- onboarding coupling, реально запускающее письмо.

Search ranking, карточки после входа, `Для меня`, feedback, saved events и
personalization сами по себе не требуют нового письма.

### 4.4 `anonymous_session`

Для focus v5 feedback default — настоящая Supabase anonymous session с JWT,
`auth.uid()` и RLS, но без email/PII и без raffle eligibility. Она не является
`session_fixture`, verified login или локальным boolean. Email/Яндекс — отдельный
upgrade scenario; отсутствие upgrade не блокирует page score, NPS, текст или
screenshot. Routine anonymous-feedback и transport fault suites требуют нулевые
product OTP/mail counters.

### 4.5 No-mail reliability matrix

Локальный deterministic runner покрывает Auth verify, Search, personalization и
focus feedback для `normal`, `direct-down`, `relay-down`, `both-down`.
Selected-once операции выбирают маршрут до dispatch; при `both-down` dispatch
равен нулю. Focus feedback допускает idempotent replay только с тем же ключом.
Matrix PASS не заменяет live fixture acceptance или provider-mail/OAuth gate.

## 5. Первый mobile release milestone

Первый законченный mobile milestone — модификация существующего isolated
focus-group OTP harness:

1. сохранён Chromium baseline;
2. выделен shared semantic journey;
3. принят Android Emulator + Chrome + реальная keyboard acceptance;
4. принят iOS native-first Safari control и full journey;
5. real-mail variants выполняются последовательно;
6. сохранены one issue / one verify / one participant registration;
7. выпускается одинаковый sanitized evidence contract;
8. PWA install/relaunch остаётся отдельным сценарием.

Terminal receipts включают Android `30747598046`, iOS preflight
`30767191144` attempt 2, direct-outage Android/iOS `30772062840` /
`30772233868` и relay-outage Android/iOS `30772957771` / `30773125445`.
Исторический iOS run `30754894934` остаётся
`BLOCKED_SAFARI_FIRST_RUN_UI`, а не keyboard failure.

## 6. Отдельный PWA gate

После browser-tab OTP добавляется `focus.pwa.install_launch`:

- Android Chrome install UI → Launcher → standalone → relaunch;
- iOS Safari Share Sheet → Add to Home Screen → SpringBoard → relaunch;
- stable manifest `id`, `scope`, `start_url`;
- persisted participant/authenticated state;
- отдельная per-device session, если нужна identity;
- честное network-only поведение service worker.

Offline content availability не является текущим обязательством и не должна
появляться как ложный release gate. Real-mail OTP не повторяется, если PWA
scenario проверяет только install/lifecycle и может использовать
`session_fixture`/returning state.

## 7. Page/data rollout

Сценарии добавляются поступательно вместе с реализацией или аудитом surface:

- route non-empty/content minimum;
- transport blocks;
- venue/source medallions;
- people/headliner/celebrity cards;
- authenticated pre-generated `Для меня` pages;
- Supabase direct/relay и Yandex connectivity;
- personalization ordering/feedback;
- expected block content and typed empty states.

Yandex reliability раскладывается по capability, а не одному статусу: relay,
YDB projection/control, OAuth, Postbox, inbound pipeline и object publication
имеют собственные SOR/acknowledgement/replay gates. Partial feedback сообщает
отдельные receipts текста и screenshot; YDB sidecar failure не отменяет primary
commit.

`planned` не превращается в blocking до появления product contract. При
переходе в `implemented` одновременно обновляются machine-readable registry,
реализующий test, release gate и evidence sample.

Authorized page/business scenarios по умолчанию зависят от
`auth.session_fixture`, а не от `focus.otp.browser_tab`.

## 8. NO-GO

Release blocked, если:

- mobile-sensitive code изменён, а required Android/iOS result отсутствует;
- real-mail OTP result FAIL/BLOCKED при релизе соответствующего Auth/mail contract;
- authenticated scenario требует fixture, но fixture отсутствует или не terminal;
- fixture неожиданно вызвал `/auth/v1/otp` или внешнее письмо;
- fixture автоматически переключился на real-mail OTP;
- session state/token попали в artifact/cache/log либо cleanup/redaction не прошли;
- target SHA не совпал;
- mandatory background run ещё не terminal;
- full catalog имеет unexplained empty/broken route;
- simulator run подменён desktop mobile viewport/WebKit;
- planned test представлен как passed implementation;
- один fixed mailbox используется параллельно несколькими real OTP jobs;
- один refresh token/session state разделён между параллельными workers/jobs.
- durable SOR acknowledgement отсутствует, но UI показал terminal success;
- составная операция скрыла partial component failure;
- selected-once или ambiguous dispatch автоматически повторён через другой
  route/provider;
- Yandex sidecar outage откатил primary action, уничтожил pending payload или
  был выдан за отказ всего Yandex.

## 9. Экономический guardrail

- не запускать iOS/macOS для data-only PR;
- не открывать весь каталог на эмуляторах;
- сначала L0/L1, затем L2;
- screenshots/video only-on-failure или для selected specimens;
- ordinary authenticated suites используют fixed personas + `session_fixture`;
- fixture создаётся один раз на worker/job, а не перед каждым test;
- real OTP только явно, по собственному change contract и последовательно;
- nightly не отправляет реальные OTP;
- один bounded retry только для доказанного инфраструктурного flake;
- deterministic gates решают release, AI visual review помогает triage.

## 10. Продуктовое качество статических подборок

Канонический сценарий `collections.product_quality` формирует один продуктовый
scorecard из трёх секций:

- `health` — наполняемость, актуальность, дубли, source/review blockers и
  концентрация выдачи;
- `semantic_sample` — живые известные positives и грубые false positives;
- `stability` — деградация относительно owner-accepted baseline и сохранение
  last-good.

Runner/workflow и facts-v3 product adapter описаны в
[`../../testing/static-collections-product-quality-autotests.md`](../../testing/static-collections-product-quality-autotests.md).
Adapter строит source-bound snapshot в существующем exporter и проверяется в
StaticSiteBuilder до Astro build. До owner-accepted baseline и terminal live run
сценарий остаётся `partial`: `FAIL` блокирует promotion, `WATCH` является
видимым неблокирующим результатом, а отсутствие baseline не выдаётся за
подтверждённое качество production-подборок.

Публичный `collections.product_page_smoke` остаётся `planned` до появления
Astro routes. В этом shadow-релизе routes, navigation, sitemap и publication
запрещены. Android/iOS для data-only изменения подборок не запускаются.
