# Текущий статус фокус-группы статического сайта

> **Обновлено:** 2 августа 2026 года.  
> **Главная стратегия:** [`README.md`](README.md)  
> **NPS UI:** [`nps-ui.md`](nps-ui.md)  
> **Тестирование:** [`testing.md`](testing.md)  
> **Правила:** [`prize-rules.md`](prize-rules.md)

## Статусы

- `READY` — находится в `main`, применено и имеет terminal evidence на актуальном target.
- `PARTIAL` — существенная часть есть, но сквозной путь или финальная live-проверка не закрыты.
- `MISSING` — production-реализации нет.
- `BLOCKED` — есть конкретный незакрытый дефект.
- `OPTIONAL` — не блокирует первую волну.

## Сводка

| Область | Статус | Что это означает |
|---|---|---|
| Soft gate на всём статическом сайте | `MISSING` | Локальная метка существует только в focus flow; обычные страницы ещё не выключаются общей заглушкой |
| Актуальная заглушка | `MISSING` | Нужен экран ожидания перед первым paint |
| Invite/QR и очистка fragment | `PARTIAL` | Клиентская механика есть; нужен общий exact-target journey |
| Конец 31 августа в 18:00 | `BLOCKED` | В коде marker всё ещё использует rolling 30 days |
| NPS постоянно перед footer | `PARTIAL` | Шкала 0–10 уже была в Lab panel, но надо закрепить NPS-вопрос, общий placement и numeric saved state |
| Текст и screenshot по кнопкам | `PARTIAL` | Рабочие issue/screenshot paths есть; их надо связать с тем же NPS-блоком |
| Offline/idempotent feedback | `PARTIAL` | Outbox и v2 RPC есть; нужен live round-trip на final target |
| `page_revision` | `MISSING` | Повторная оценка после изменения страницы пока не привязана к содержательной revision |
| Browser real-mail OTP | `PARTIAL` | Workflow merged; прежний PASS есть, требуется повтор на финальном target |
| Android real-mail OTP | `PARTIAL` | Workflow merged; прежний PASS есть, требуется повтор на финальном target |
| iOS Safari identity | `BLOCKED` | Последний OTP run завершён `FAIL_MOBILE_KEYBOARD`; нужен рабочий OTP либо принятый magic-link/Яндекс fallback |
| Яндекс Auth | `PARTIAL` | Код есть; нужен свежий consent round-trip на final target |
| Verified participant + cap 200 | `PARTIAL` | Таблица/RLS/atomic cap есть; нужна live boundary-проверка и cleanup test rows |
| PWA Android install/relaunch | `PARTIAL` | Install UI и component tests есть; нужен нативный E2E |
| iPhone «На экран Домой» | `MISSING` | Нет принятого системного сценария |
| Share / Calendar / Не интересно / Для меня | `PARTIAL` | Функции есть, но не собраны в один focus acceptance journey |
| 12 артефактов | `PARTIAL` | `FG-E01…FG-E12` определены в коде; большая часть остаётся prototype/local |
| Server artifact receipts | `MISSING` | Нельзя доказуемо посчитать 10 из 12 и eligible pool |
| Правила розыгрыша | `PARTIAL` | Product rules сформированы; требуется публикация versioned copy и implementation |
| Weighted chances 1–3 | `MISSING` | Нет server projection base/text/screenshot chances |
| Automatic draw + reserve | `MISSING` | Нет immutable snapshot и защищённого draw workflow |
| Письмо победителю / 3 дня | `OPTIONAL` до seed | Логика зафиксирована; шаблон нужен до rehearsal розыгрыша |
| Благодарность всем + победитель | `OPTIONAL` до seed | Отправляется после подтверждения финального победителя |
| Извлекаемая статистика | `PARTIAL` | Auth, participant, feedback и PWA данные существуют; artifacts/chances/key-action receipts ещё неполны |
| Scheduled report | `NOT REQUIRED` | Ежедневный JSON/Markdown workflow удалён из стратегии; данные запрашиваются read-only по необходимости |
| Полный focus live workflow | `MISSING` | Нет одного synthetic exact-target journey с cleanup |

## Что уже действительно есть

### Onboarding и Auth

- focus programme/invite routes;
- удаление invite fragment из URL;
- локальная метка участия;
- email magic link и шестизначный OTP;
- Яндекс через общий Supabase Auth controller;
- resilient direct/relay transport;
- idempotent participant registration;
- atomic cap `200`.

Основные файлы:

- [`FocusGroupInviteIntake.astro`](../../../../site/src/components/FocusGroupInviteIntake.astro)
- [`focus-group-prototype.ts`](../../../../site/src/lib/focus-group-prototype.ts)
- [`staticSiteAuth.ts`](../../../../site/src/lib/staticSiteAuth.ts)
- [`20260730131051_focus_group_participant_contact_v1.sql`](../../../../supabase/migrations/20260730131051_focus_group_participant_contact_v1.sql)
- [`20260731185118_focus_group_participant_cap_and_backfill_v2.sql`](../../../../supabase/migrations/20260731185118_focus_group_participant_cap_and_backfill_v2.sql)

### Feedback

Рабочий контур уже поддерживает:

- оценку `0–10`;
- текстовое сообщение;
- private screenshot upload;
- idempotent outbox;
- повтор после возвращения сети;
- дневной anti-abuse cap.

Основные файлы:

- [`FocusGroupLabPanel.astro`](../../../../site/src/components/FocusGroupLabPanel.astro)
- [`20260729113221_focus_group_feedback_v1.sql`](../../../../supabase/migrations/20260729113221_focus_group_feedback_v1.sql)
- [`20260731193000_focus_group_feedback_idempotency_v2.sql`](../../../../supabase/migrations/20260731193000_focus_group_feedback_idempotency_v2.sql)

Нужно не создавать новые анкеты, а привести этот контур к макету
[`nps-ui.md`](nps-ui.md).

### Артефакты

[`site/src/lib/focus-easter-eggs.ts`](../../../../site/src/lib/focus-easter-eggs.ts)
уже содержит ровно 12 определений `FG-E01…FG-E12`. Следовательно, вопрос о числе
артефактов закрыт: `80% = 10 из 12`.

### OTP evidence

- [PR #245 — merged mobile real-mail OTP](https://github.com/onedayonemasterpiece/events-bot-new/pull/245)
- [Browser PASS run 30745526613](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30745526613)
- [Android PASS run 30747598046](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30747598046)
- [iOS non-acceptance run 30754894934](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30754894934)

## Короткий backlog до seed

1. `FG-01` — общий soft gate, новая заглушка, fixed cutoff `31.08 18:00`.
2. `FG-02` — постоянно видимый NPS, numeric saved state, text/screenshot buttons,
   `page_revision`.
3. `FG-03` — final browser/Android/iPhone/Yandex identity acceptance.
4. `FG-04` — native PWA return.
5. `FG-05` — Share, Calendar, «Не интересно», «Для меня» journey.
6. `FG-06` — server receipts для 12 артефактов, progress `10/12`.
7. `FG-07` — queryable participant/NPS/artifact/chance statistics.
8. `FG-08` — weighted pool, immutable draw, reserve and email lifecycle.
9. `FG-09` — full rehearsal на exact immutable target с synthetic cleanup.