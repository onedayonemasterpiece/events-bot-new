# Authenticated session fixture для статического сайта

> **Статус:** нормативный test contract.
> **Назначение:** получить реальную authenticated Supabase session без отправки нового письма для большинства product E2E.
> **Не является:** production login method, anonymous focus identity, фиксированным OTP или разрешением хранить refresh token в GitHub Secrets.

## 1. Решение

Большинство Search/personalization/feedback/Favorites/profile E2E проверяют
поведение **после входа**. Они не должны каждый раз расходовать письмо и provider
capacity.

Default mode:

```text
fresh admin-issued one-time fixture credential
-> ordinary supported Auth completion/callback
-> real per-worker authenticated session
-> product journey
```

Обязательные нули:

```text
/auth/v1/otp issue = 0
external email = 0
mail trigger receipts = 0
```

Fixture failure возвращает `BLOCKED_AUTH_FIXTURE`. Автоматический fallback на
real mail запрещён.

## 2. Поддерживаемые auth modes

| Mode | Назначение | Mail side effects |
|---|---|---:|
| `session_fixture` | default authenticated product journey | 0 |
| `mocked_ui` | локальная UI/source characterization, не live Auth evidence | 0 |
| `admin_otp_ui` | проверка UI без provider delivery, если разрешено сценарием | 0 external |
| `real_mail_otp` | issue/delivery/template/verify/mobile input acceptance | ровно 1 controlled mail |
| `yandex_oauth` | реальный OAuth callback/consent acceptance | provider-specific |

Удалённый режим:

```text
anonymous_session
```

Он не допускается как способ отправлять focus feedback, score, NPS, screenshot
или artifact receipts. Приглашённый пользователь может смотреть сайт без
login, но server feedback требует explicit email/Yandex authentication.

## 3. Изоляция fixture

Каждый worker/device получает собственные:

```text
fixture_subject
one-time credential
access token
refresh token
storage namespace
cleanup receipt
```

Запрещены:

- общий refresh token для parallel jobs;
- сохранённая browser session в Actions cache/artifact;
- service/admin key в browser;
- fixed OTP;
- localStorage `authorized=true` как доказательство Auth;
- reuse session между unrelated repositories/environments;
- запись token/email в screenshot/log/report.

## 4. Получение и завершение

1. Protected backend создаёт одноразовый credential с TTL и exact scenario/run
   binding.
2. Browser проходит штатную completion boundary.
3. Проверяется real `auth.uid()`/session, но ID не попадает в public evidence.
4. Product journey использует shared Auth/runtime/transport.
5. Cleanup ревокирует/удаляет fixture state по policy.
6. Evidence содержит только mode, platform, success class, exact target SHA и
   нулевые mail counters.

## 5. Focus-group usage

### Anonymous invited state

- site accessible;
- no Auth session created silently;
- feedback controls disabled;
- server feedback writes = 0.

### Authenticated focus E2E

`session_fixture` допускается для массовых deterministic проверок:

- page score;
- NPS;
- text/screenshot;
- profile;
- artifact receipt;
- exact return context.

Но он не закрывает:

- реальную доставку OTP;
- email template/routing;
- native mobile email keyboard;
- Yandex OAuth consent/callback.

Эти клетки имеют отдельные protected scenarios.

## 6. Assertions

- exact immutable target SHA;
- expected auth mode;
- one session per worker;
- no token in DOM/custom events/artifacts;
- no issue/mail for fixture mode;
- reload restores session;
- logout clears current session but not unrelated durable profile unless explicit;
- login as another account cannot inherit prior account state;
- pending outbox cannot cross identity boundary;
- cleanup is idempotent;
- fixture actor marked `training_eligible=false` and excluded from analytics.

## 7. Failure classes

```text
BLOCKED_AUTH_FIXTURE_CONFIGURATION
BLOCKED_AUTH_FIXTURE_ISSUE
FAIL_AUTH_FIXTURE_TARGET_MISMATCH
FAIL_AUTH_FIXTURE_SESSION_MISSING
FAIL_AUTH_FIXTURE_IDENTITY_CROSSOVER
FAIL_AUTH_FIXTURE_MAIL_SIDE_EFFECT
FAIL_AUTH_FIXTURE_SECRET_LEAK
FAIL_AUTH_FIXTURE_CLEANUP
```

Ни одна failure class не разрешает скрыто переключиться на real-mail OTP.
