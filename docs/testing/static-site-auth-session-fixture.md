# Auth session fixture для автотестов статического сайта

> **Статус:** нормативный companion к
> [`static-site-autotest-strategy.md`](../operations/static-site-autotest-strategy.md).
> **Область:** авторизованные browser/Android/iOS сценарии, которым нужна
> настоящая Supabase-сессия, но не требуется проверять выпуск или доставку OTP.
> **Машиночитаемый контракт:**
> [`static-site-autotest-scenarios.v1.yml`](static-site-autotest-scenarios.v1.yml).
> **Отдельный real-mail gate:**
> [`external-focus-email-otp.md`](external-focus-email-otp.md).

## 1. Решение

Для обычных авторизованных автотестов проект использует **свежую настоящую
Supabase-сессию без отправки письма**:

```text
trusted setup
  -> allowlisted fixed E2E persona
  -> Supabase Admin generateLink / одноразовый admin OTP
  -> штатный verify или callback exchange
  -> настоящий access token + refresh token
  -> ephemeral browser/device session state
  -> бизнес-сценарии
  -> cleanup/revoke
```

Это называется `session_fixture`.

`session_fixture` является способом подготовить состояние теста, а не отдельным
пользовательским способом входа. Он не заменяет проверки email OTP, magic link,
Яндекс OAuth или внешней доставки. Он устраняет их повторение в сценариях,
предметом которых являются Search, персонализация, feedback, сохранённые события,
RLS или другие функции после входа.

## 2. Почему локальный «признак авторизации» недостаточен

В проекте необходимо различать три слоя:

| Слой | Источник истины | Что подтверждает |
|---|---|---|
| Supabase Auth session | JWT + `auth.getUser` | реальную серверную identity |
| Backend state | RLS/RPC/tables | membership, quota, profile, saved state |
| Browser-local state | `localStorage`/IndexedDB | UX hints, local profile, outbox, participation marker |

Запись `authorized=true`, фиктивного пользователя или локальной participation
метки может быть допустима только в тесте `mocked_ui`. Она не даёт валидного JWT,
не проходит server-side `auth.getUser`, не доказывает RLS и не должна называться
authenticated E2E.

## 3. Поддерживаемые auth modes

Каждый сценарий в registry обязан явно выбрать один режим:

| Mode | Назначение | Внешнее письмо | Настоящий JWT |
|---|---|---:|---:|
| `anonymous` | публичный contract без server identity | 0 | нет |
| `anonymous_session` | focus v5: тихий Supabase anonymous subject с `is_anonymous=true` | 0 | да |
| `mocked_ui` | компонентный/визуальный signed-in state без backend claims | 0 | нет |
| `session_fixture` | основной режим для функций после входа | 0 | да |
| `admin_otp_ui` | настоящий OTP UI/verify без внешней доставки | 0 | да |
| `real_mail_otp` | выпуск, доставка, получение и ввод реального OTP | 1 bounded | да |
| `yandex_oauth` | настоящий redirect/consent/callback Яндекса | 0 email | да |

Режим нельзя выбирать по удобству после начала теста. Он фиксируется registry и
selector до side effects.

## 4. Текущий repository baseline

Уже существует focus-specific основа:

- `site/scripts/issue-focus-agent-test-credentials.mjs` вызывает
  `auth.admin.generateLink` для фиксированной E2E identity и получает свежие
  временные `email_otp`/`action_link` без отправки письма;
- `site/scripts/check-focus-onboarding-email-integration.mjs` вводит такой OTP в
  настоящий интерфейс, выполняет настоящий `/auth/v1/verify` и подтверждает
  membership;
- `site/src/lib/staticSiteAuth.ts` сохраняет настоящую Supabase session под
  проектным auth storage key, восстанавливает её после reload и передаёт access
  token защищённым RPC;
- внешний `.github/workflows/external-focus-email-otp.yml` отдельно доказывает
  настоящий product issue, доставку, ввод, verify и registration.

Общий issuer/fixture теперь реализован в
`site/e2e/auth-session-fixture/session-fixture.mjs`. Он:

- принимает только allowlisted persona и HTTPS target origin;
- выпускает fresh admin credential без `/auth/v1/otp` и внешней почты;
- выполняет настоящий `verifyOtp`, затем `auth.getUser` **и обязательный**
  read-only protected RLS probe с созданным access token;
- создаёт Playwright `storageState` с mode `0600` только во временной директории;
- запрещает общий active scope и real-mail fallback;
- удаляет state в `cleanup()` и возвращает только PII/token-free receipt.

Unit acceptance находится в `site/tests/auth-session-fixture.test.mjs`. Live
exact-target acceptance и OIDC broker остаются отдельными незакрытыми gates;
локальный PASS не выдаётся за live Auth acceptance.

## 5. Нормативный browser bootstrap

### 5.1 Подготовка

Trusted setup выбирает persona только из versioned allowlist. Нельзя принимать
произвольный email из PR input, issue comment или test data.

Setup получает:

- exact target URL и expected repository SHA;
- Supabase project identity;
- persona ID, но не произвольную пользовательскую identity;
- session scope (`test`, `worker` или `job`);
- scenario ID и run ID.

### 5.2 Выпуск credential

Предпочтительный путь:

1. server-side `auth.admin.generateLink({ type: 'magiclink', ... })`;
2. redirect только на allowlisted KenigEvents origin/path;
3. browser открывает одноразовую `action_link`;
4. общий `StaticSiteAuth` завершает `token_hash/type` либо code callback;
5. callback parameters удаляются из видимого URL;
6. `getSession()` и защищённый read probe подтверждают identity;
7. browser context сохраняет ephemeral `storageState`.

Если конкретная hosted callback-форма не совместима с опубликованным target,
допустим generic noindex bootstrap, который получает свежий случайный admin OTP
и вызывает существующий штатный verify. Нельзя вводить фиксированный OTP или
отдельную JWT-реализацию.

### 5.3 Использование

- fixture создаётся один раз на Playwright worker/job, а не перед каждым test;
- каждый test получает новый browser context из worker state;
- stateful tests используют отдельную persona либо сериализуются;
- после bootstrap тестовый код не получает admin/service key;
- `storageState` живёт только в `$RUNNER_TEMP` или эквивалентном ephemeral path;
- state не передаётся между workflow jobs.

### 5.4 Cleanup

В `finally` необходимо:

- закрыть contexts;
- удалить auth-state file;
- очистить временные credentials из памяти;
- при сценарной необходимости отозвать session;
- привести mutable persona data к известному состоянию идемпотентным cleanup.

## 6. Android и iOS

`session_fixture` не отменяет native platform boundary.

Для Android/iOS нужен отдельный session bootstrap на каждый device job:

- отдельный одноразовый credential;
- отдельная Supabase session;
- та же browser/app storage area, в которой выполняется продуктовый сценарий;
- отсутствие общего refresh token между browser, Android и iOS;
- exact target SHA и platform provenance в evidence.

Прямая JS-запись токенов допустима только как низкоуровневый contract test
формата storage и не считается platform acceptance. Предпочтительно пройти
штатный callback/verify в реальном Chrome Android или Mobile Safari, а затем
продолжить сценарий в той же сессии.

Если предмет теста — native email/OTP keyboard или Auth UI, выбирается
`admin_otp_ui` либо `real_mail_otp`, а не `session_fixture`.

## 7. Persona и параллельность

### 7.1 Fixed personas

Routine CI переиспользует небольшой фиксированный пул:

- `auth-readonly-*` — read-only authenticated pages;
- `search-cached-*` — cache-hit Search;
- `search-cold-*` — bounded cold Search;
- `personalization-*` — likes/hides/profile/outbox;
- `focus-member-*` — membership/feedback;
- `external-mail-*` — только real-mail gate.

Новый уникальный email на каждый run запрещён по умолчанию: он раздувает
`auth.users`, provider recipient accounting и cleanup surface.

### 7.2 Session scope

- `test`: только если test должен владеть уникальной mutable session;
- `worker`: default для browser suite;
- `job`: допустим для строго последовательного read-only suite;
- cross-job/session artifact: запрещён.

### 7.3 Refresh-token rule

Одна сериализованная session не копируется в параллельные jobs/workers. Каждый
параллельный worker получает собственную session. Общая persona допустима только
если проектная session policy и тестовый контракт это явно разрешают; безопасный
baseline — отдельная persona/session на worker.

## 8. Side-effect contract

Для `session_fixture` обязательны:

```yaml
auth_mode: session_fixture
max_product_otp_issues: 0
max_external_email_sends: 0
max_admin_credentials: 1_per_session_scope
max_auth_verifies: 1_per_session_scope
protected_rls_probe_requests: exactly_1_successful_read_only
real_mail_fallback: forbidden
```

`protectedProbe` является обязательной функцией сценария. Она получает только
созданный user access token, publishable key, expected user ID и fixture-owned
`fetchImpl`. Этот wrapper разрешает ровно read-only `GET /rest/v1/*` к тому
же Supabase origin, требует точные `Authorization: Bearer <session JWT>` и
`apikey`, а fixture выдаёт PASS только при одном выполненном успешном запросе и
явном результате callback `true`. Возврат `true` без вызова wrapper, HTTP/RLS
ошибка, запрос без session headers или вызов другого origin fail-closed.

Сам сценарий обязан выбрать allowlisted owner-scoped view/table и проверить
subject/owner в ответе. Публичный endpoint, который одинаково отвечает
без JWT или для другого пользователя, не является protected RLS probe.

Network recorder должен падать при неожиданном:

```text
POST /auth/v1/otp
```

Ошибка fixture классифицируется `BLOCKED_AUTH_FIXTURE`. Она не даёт права
отправить настоящее письмо «для восстановления теста».

Для `admin_otp_ui` также обязательно `max_product_otp_issues: 0`: credential
выпускается trusted admin setup, а UI доказывает ввод/verify/autosubmit.

Для `real_mail_otp` действуют отдельные one-issue/one-message/one-verify
ограничения внешнего harness.

## 9. Что реально проверяет `session_fixture`

Он позволяет честно проверять:

- восстановление Supabase session;
- серверный `auth.getUser`;
- настоящий access token;
- RLS и owner scoping;
- authenticated Edge Functions;
- quotas и per-user idempotency;
- direct/relay data/functions transport после входа;
- Search, feedback, saved events и personalization lifecycle;
- logout и повторное чтение local state;
- отсутствие данных другого пользователя.

Он **не** доказывает:

- product `/auth/v1/otp` issue;
- Postbox/NotiSend routing;
- письмо и его template;
- Mail Trigger/IMAP receipt;
- email keyboard;
- OTP keyboard;
- magic-link UX в почтовом клиенте;
- Яндекс consent/callback.

Эти claims закрываются `admin_otp_ui`, `real_mail_otp` и `yandex_oauth`.

## 10. Mapping сценариев

Основной `session_fixture` применяется к:

- `personal.for_me_page`;
- `personalization.core_journey`;
- `search.authenticated_contract`;
- `search.live_cached_journey`;
- `search.live_cold_journey`;
- `search.cache_provider_zero`;
- authenticated части `search.transport_route_matrix`;
- будущим saved-event, feedback и authenticated page scenarios.

Focus v5 использует отдельный `anonymous_session` для page score, service NPS,
текста, private screenshot, персонализации и artifact receipts до подтверждения
identity. Он имеет настоящий anonymous JWT/RLS owner, но не отображается как
verified login и всегда имеет raffle eligibility `false`. Email/Yandex upgrade
должен сохранить этот subject либо выполнить audited idempotent merge.

`focus.otp.browser_tab` остаётся `real_mail_otp`.

`focus.otp.ios_keyboard_preflight` остаётся side-effect-free и не создаёт
сессию.

Auth issue/verify direct/relay matrix остаётся в OTP harness. Search transport
matrix не должна повторно рассылать OTP.

Детерминированный no-mail contract для Auth verify, Search, personalization и
focus feedback реализован общим runner
`site/e2e/auth-session-fixture/noMailFaultMatrix.ts`. Он проверяет `normal`,
direct-down, relay-down и both-down с нулём product OTP/mail/provider effects;
это L0 transport proof, а не замена live browser/mobile acceptance.

## 11. Security boundary

### 11.1 Запрещено

- service-role/secret key в browser, Appium session, localStorage или page URL;
- постоянная serialized session в GitHub Secrets;
- access/refresh token в artifacts, cache, job outputs или logs;
- фиксированный OTP (`123456`) и email-specific production bypass;
- самодельный JWT;
- произвольная persona/redirect из untrusted PR input;
- admin credential в job, исполняющем непроверенный fork/PR code;
- автоматический fallback на real mail;
- один refresh token для параллельных jobs.

### 11.2 Защищённый setup

До появления broker допустим отдельный GitHub Environment:

- default/trusted branch restriction;
- required reviewer для privileged setup;
- admin secret используется только минимальным issuer step;
- одноразовый credential немедленно маскируется;
- browser получает только пользовательскую session;
- дальнейшие тестовые шаги не видят admin secret.

### 11.3 Целевой OIDC broker

Предпочтительный зрелый контур:

```text
GitHub OIDC
  -> constrained Yandex/server-side session broker
  -> repository/ref/workflow/environment/run validation
  -> allowlisted persona + redirect
  -> Supabase admin.generateLink
  -> one-time user credential
```

Broker не предоставляет общий Admin API. Он только выпускает ограниченный
credential, имеет per-run/per-persona rate limits и PII-free audit.

## 12. Storage и evidence

Auth state хранится вне checkout, например:

```text
$RUNNER_TEMP/kenigevents-auth/<run-id>/<persona>-<worker>.json
```

Обязательные ограничения:

- file mode `0600`, где применимо;
- запрет artifact/cache upload;
- удаление в `finally`;
- trace/HAR выключены во время credential bootstrap;
- screenshots только после удаления callback secret и маскировки identity;
- redaction scan на JWT, `refresh_token`, `token_hash`, action link и
  `sb-*-auth-token`.

Безопасный receipt содержит только:

- scenario/auth mode/session scope;
- persona role или salted hash, не email;
- target origin/path и exact SHA;
- project ref hash;
- bootstrap method class;
- credential/verify/session counts;
- `/auth/v1/otp` count;
- external mail send/receipt count;
- protected probe result;
- protected probe request count (`1`, без URL/body/token);
- cleanup result;
- redaction result.

## 13. Failure classes

- `BLOCKED_AUTH_FIXTURE_CONFIG` — отсутствует защищённая конфигурация;
- `BLOCKED_AUTH_FIXTURE_ISSUER` — admin credential не выпущен;
- `FAIL_AUTH_FIXTURE_REDIRECT` — credential ушёл за allowlisted target;
- `FAIL_AUTH_FIXTURE_CALLBACK` — callback завершился без session;
- `BLOCKED_AUTH_FIXTURE:PROTECTED_PROBE_REQUIRED` — сценарий не передал
  обязательный protected RLS probe;
- `BLOCKED_AUTH_FIXTURE:PROTECTED_PROBE_FAILED` — probe не выполнил один
  успешный JWT-bound read-only RLS request или owner assertion не прошёл;
- `FAIL_AUTH_FIXTURE_IDENTITY` — `getUser`/protected probe не совпал с persona;
- `FAIL_AUTH_FIXTURE_UNEXPECTED_OTP` — замечен product OTP issue;
- `FAIL_AUTH_FIXTURE_UNEXPECTED_MAIL` — замечена внешняя отправка/receipt;
- `FAIL_AUTH_FIXTURE_LEAK` — credential/session попали в evidence;
- `FAIL_AUTH_FIXTURE_CLEANUP` — состояние не удалено/не восстановлено.

Infrastructure retry разрешён только до выпуска credential либо после
доказанного отсутствия session/side effects. Нельзя повторять credential
вслепую после неоднозначного verify/callback.

## 14. Implementation milestones

### A0 — нормативный contract

- этот документ;
- стратегия;
- scenario registry;
- release companion;
- agent skill.

### A1 — generic browser fixture

- **реализовано локально:** общий issuer, fresh verify, `auth.getUser`,
  обязательный fail-closed JWT-bound protected RLS read probe, ephemeral
  Playwright storage state, active-scope isolation,
  `product OTP=0`, `external mail=0`, fail-closed cleanup/redaction;
- **реализовано в harness:** terminal exact-target `/poisk/` acceptance сверяет
  immutable candidate metadata и ожидаемый repository SHA, затем доказывает
  восстановленную browser session, `auth.getUser`, один owner-filtered RLS read,
  `product OTP=0`, `mail=0/0`, cleanup и redaction. Live hosted receipt всё ещё
  обязан быть terminal для каждого принимаемого SHA; наличие кода не равно PASS.

### A2 — подключение business scenarios

- Search cached/cold/cache-zero;
- `Для меня`;
- personalization lifecycle;
- feedback/saved-event scenarios;
- per-worker persona allocation.

### A3 — Android/iOS session bootstrap

- отдельный credential/session на device job;
- callback в реальном platform browser;
- same-storage continuation;
- platform evidence без token leakage.

### A4 — OIDC broker

- **реализовано:** GitHub OIDC broker проверяет подпись, issuer/audience,
  repository/ref/workflow/environment/event/run claims, exact persona и redirect;
- atomic service-role RPC допускает ровно один credential на
  `run_attempt + persona`, audit содержит только keyed hashes;
- broker возвращает one-time callback/OTP только защищённому caller, но runner
  никогда не сохраняет их в evidence; real-mail fallback отсутствует.

## 15. Release gates

Обычное изменение авторизованной бизнес-функции требует:

- `auth.session_fixture` terminal PASS на exact target;
- соответствующий authenticated product scenario;
- `product OTP issue=0` и `external mail=0`;
- no token/session artifact leakage.

Real-mail OTP требуется только если изменены:

- `signInWithOtp`/OTP issue UI;
- email hook/provider/routing/template;
- Auth direct/relay issue/verify semantics;
- mailbox adapter;
- email/OTP mobile input;
- onboarding coupling, которое реально запускает письмо.

Изменение Search ranking, карточек после входа, personal page, feedback или
personalization само по себе не требует нового письма.

## 16. Acceptance checklist

- [ ] сценарий имеет явный `auth_mode`;
- [ ] `session_fixture` использует fresh one-time credential;
- [ ] browser/device получил настоящую Supabase session;
- [ ] `auth.getUser` **и** один JWT-bound protected RLS probe подтверждены;
- [ ] state scoped к worker/job и не разделяется параллельно;
- [ ] `/auth/v1/otp = 0`;
- [ ] external mail send/receipt = `0/0`;
- [ ] real-mail fallback отсутствует;
- [ ] service/admin key не попал в test/browser;
- [ ] state не загружен как artifact/cache;
- [ ] cleanup и redaction PASS;
- [ ] real-mail gate запускается только по собственному change contract.
