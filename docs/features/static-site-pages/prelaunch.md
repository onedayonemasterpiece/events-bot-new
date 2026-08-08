# Заглушка запуска 1 сентября: форма уведомления

## Scope

До публичного запуска корень `https://kenigevents.ru/` показывает утверждённую
prelaunch-страницу. Исправления регистрации не меняют композицию, фон, visual
CSS, тексты SEO/GEO или остальные маршруты. Их принятый byte-level baseline —
production source commit `9d8fc9203a69f385407a57e23310bb47f2db4e2d`.

Форма использует существующие:

- таблицу `personalization.prelaunch_launch_subscription`;
- RPC `public.register_prelaunch_notification_v1`;
- browser transport Supabase с операцией `idempotent-replay`.

Прямой публичный `SELECT`, `INSERT`, `UPDATE` или `DELETE` таблицы запрещён.
Операторская проверка выполняется только защищённым production-доступом и не
публикует полный email в логах или артефактах.

## Нормализация и server truth

Email нормализуется одинаковым консервативным ASCII-контрактом в браузере и в
RPC: внешние пробелы удаляются, регистр приводится к нижнему; невалидные адреса
отклоняются. Проверенное production-ограничение `UNIQUE(email)` закрепляет правило «один
нормализованный email — одна строка».

RPC различает результаты:

- первая вставка: `{ "accepted": true, "status": "registered" }`;
- существующая строка или победивший concurrent insert:
  `{ "accepted": true, "status": "already_registered" }`;
- исчерпана квота новых строк: `accepted=false`,
  `status="daily_capacity_reached"`.

Финальная вставка использует `ON CONFLICT (email) DO NOTHING`; проигравший
конкурентный запрос обновляет существующую строку и возвращает
`already_registered`. Повтор обновляет `last_requested_at`, `request_count`,
`consent_version`, retention и остаётся успешным.

Миграции применяются по порядку:

1. `20260803113000_prelaunch_launch_notifications_v1.sql`;
2. `20260806163000_prelaunch_updates_consent_v2.sql`;
3. `20260808143744_prelaunch_registration_result_and_race_safe_dedup.sql`.

## Browser-state contract

После подтверждённого backend-ответа:

- `registered` скрывает форму и показывает постоянное «Готово, вы записаны»;
- `already_registered` скрывает форму и показывает «Вы уже записаны»;
- localStorage хранит только UX-hint, поэтому reload показывает
  «Вы уже записаны»; сервер остаётся источником истины дедупликации;
- только кнопка «Другой e-mail» удаляет hint и возвращает форму;
- ошибка сохраняет нормализованное введённое значение и показывает понятный
  текст;
- disabled CTA, `aria-busy` и runtime in-flight lock не допускают второй запрос
  до завершения первого.

## Release / regression gate

Перед root publish обязательны:

```bash
npm --prefix site run test:prelaunch-form
PUBLIC_PRELAUNCH_MODE=on npm --prefix site run build
npm --prefix site run check:prelaunch-form -- \
  --url http://127.0.0.1:4173/ \
  --artifact-dir ../artifacts/prelaunch-form
```

Browser gate перехватывает транспорт и проверяет first/repeat success, reload,
явный reset, сохранение email при ошибке и двойной submit. Публикация разрешена
только manual workflow из exact `origin/main` SHA. После публикации необходимы
реальная двойная отправка одного синтетического email, защищённое чтение одной
строки и удаление этой синтетической строки.

Regression contract: [`INC-2026-08-08-prelaunch-registration-confirmation-and-dedup.md`](../../reports/incidents/INC-2026-08-08-prelaunch-registration-confirmation-and-dedup.md).
