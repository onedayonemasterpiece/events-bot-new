# Фокус-группа статического сайта

> **Статус:** owner-corrected current product contract, 2026-08-04.
> **Приоритет:** этот документ отменяет прежнюю anonymous-first модель с silent Supabase Auth и anonymous server feedback.
> **Release companions:** [`focus-group-release/README.md`](focus-group-release/README.md).
> **Аналитика:** [`analytics/README.md`](analytics/README.md).

## 1. Цель

- дать приглашённым участникам пользоваться обычным статическим сайтом до
  публичного запуска;
- проверить страницы, PWA, поиск, персонализацию и основные действия;
- получить revision-aware page score, общий NPS, текст и скриншоты;
- не превращать авторизацию в стену перед просмотром сайта;
- не создавать скрытую техническую «авторизацию», которую пользователь не
  выбирал.

## 2. Актуальный путь участника

```text
invite / QR
  -> local focus marker
  -> ordinary site is available
  -> feedback block is visible
  -> score / NPS / issue / screenshot controls are disabled
  -> explicit email or Yandex authentication CTA
  -> safe return to the same page and feedback block
  -> authenticated idempotent feedback writes
```

Приглашение, QR, просмотр сайта и sharing доступны без входа. Email/Яндекс нужны
для отправки server feedback, восстановления между устройствами и действий,
которые требуют подтверждённого субъекта.

## 3. Запрещённая прежняя модель

Не использовать:

- silent anonymous Supabase Auth;
- anonymous server `page_score`, `service_nps`, text или screenshot writes;
- UI, который называет технический anonymous subject участником с аккаунтом;
- автоматический fallback с test/session fixture на реальное OTP-письмо;
- старые условия `12 артефактов / 10 из 12`.

При Supabase/Yandex outage сайт остаётся доступен, но feedback controls честно
показывают недоступность/необходимость входа. Нельзя показывать false success.

## 4. Feedback block

До авторизации:

- блок видим на поддерживаемых страницах;
- native controls disabled;
- рядом один явный CTA `Войти, чтобы отправить отзыв`;
- после login/callback возвращаем пользователя к исходному route, anchor и
  введённому локальному draft, если он есть;
- server request count для feedback равен нулю.

После авторизации доступны:

- `page_score` 0–10;
- `service_nps` 0–10 в общем hub;
- optional text;
- optional screenshot;
- typed delivery state каждого компонента.

Текст и screenshot не попадают в общий analytics payload.

## 5. Revision-aware оценки

`page_score` относится к:

```text
account_subject + page_family + page_revision
```

`service_nps` относится к:

```text
account_subject + service_revision
```

Смена build SHA сама по себе не создаёт новую revision. После содержательного
изменения показывается прежняя оценка и просьба оценить новую версию. История не
перезаписывается.

## 6. Профиль и collections

- mobile account action ведёт в `/profil/`;
- logout находится внутри профиля;
- `Избранное` остаётся отдельной поверхностью;
- hidden recovery остаётся `Подборки -> Помечены «не интересует»`;
- профиль не рендерит event collections.

## 7. Артефакты

Для текущего focus candidate используется точная первая коллекция из **семи**
существующих артефактов в
`docs/features/static-site-pages/references/artefact-collection-1`.

- новые вымышленные артефакты не добавляются;
- pre-auth progress может быть только локальным и не считается durable receipt;
- server progress/eligibility требует явного authenticated flow;
- keyboard placement использует один настоящий артефакт из этой коллекции.

## 8. Розыгрыш

Прежний документ с `10 из 12` больше не применим. Нельзя автоматически заменить
его на `6 из 7` или другой threshold.

До отдельного owner-approved rebaseline заблокированы:

- минимальное число артефактов;
- число шансов;
- связь score/text/screenshot с eligibility;
- точный prize/claim flow;
- final immutable eligible snapshot.

Cutoff текущего prelaunch-цикла остаётся связан с release plan; любая новая
формула eligibility должна быть опубликована до сбора влияющих действий.

## 9. Надёжность

Feedback command:

- idempotent `action_id`;
- visible pending/success/failure;
- direct/relay route selected by shared transport;
- no false success;
- text/screenshot/diagnostics имеют component receipts;
- ambiguous response разрешается status lookup/replay только по durable
  idempotency contract.

## 10. Статистика

Минимально:

- invited installations;
- anonymous viewers vs authenticated focus actors;
- feedback block visible;
- auth CTA click/complete/return;
- page revision coverage;
- page score/NPS response rates после auth;
- text/screenshot delivery success;
- time-to-triage/fix/verify;
- PWA install/standalone and D1/D7;
- feature adoption и event-value metrics по общему analytics contract.

## 11. Release gate

Нельзя объявлять focus flow готовым, пока нет terminal evidence:

- anonymous invited user can browse;
- disabled feedback controls send zero requests;
- email and Yandex return to exact context;
- authenticated score/text/screenshot are stored exactly once;
- outage and ambiguous dispatch never claim success;
- seven-artifact inventory exact;
- old anonymous-session and 12-artifact assumptions absent from generated UI,
  tests and release checklist.
