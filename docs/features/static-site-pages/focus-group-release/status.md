# Текущий статус фокус-группы статического сайта

> **Обновлено:** 3 августа 2026 года.  
> **Главная стратегия:** [`README.md`](README.md)  
> **Оценки и NPS:** [`nps-ui.md`](nps-ui.md)  
> **Тестирование:** [`testing.md`](testing.md)  
> **Правила:** [`prize-rules.md`](prize-rules.md)

## Статусы

- `READY` — находится в `main`, применено и имеет terminal evidence на актуальном target.
- `PARTIAL` — существенная часть есть, но сквозной путь или финальная live-проверка не закрыты.
- `MISSING` — production-реализации нет.
- `BLOCKED` — есть конкретный незакрытый дефект.
- `PROTOTYPE` — UI/source specimen существует, но не записывает production data.
- `OPTIONAL` — не блокирует первую волну.

## Сводка

| Область | Статус | Что это означает |
|---|---|---|
| Soft gate на всём статическом сайте | `MISSING` | Локальная метка существует только в focus flow; обычные страницы ещё не выключаются общей заглушкой |
| Актуальная заглушка | `MISSING` | Нужен экран ожидания перед первым paint |
| Invite/QR и очистка fragment | `PARTIAL` | Клиентская механика есть; нужен общий exact-target journey |
| Конец 31 августа в 18:00 | `BLOCKED` | В коде marker всё ещё использует rolling 30 days |
| Route → page family mapping | `PARTIAL` | Unit-тест покрывает 16 типов URL и excluded routes, но не rendered visibility |
| Lab-блок перед footer | `PARTIAL` | Shared layout монтирует компонент в правильном месте, но отдельного DOM/browser placement test нет |
| Шкала `0–10` | `PARTIAL` | Рабочий Lab panel содержит 11 кнопок, но вопрос пока оценивает полезность и нет browser matrix |
| Контроль уже поставленной оценки | `PARTIAL` | Unit-тест хранит score по family и разрешает изменение, UI подсвечивает ячейку; явного `Ваша оценка: N` нет |
| Revision-aware повторная оценка | `MISSING` | Storage keyed только по family и TTL 24 часа; `page_revision` отсутствует в UI/RPC/DB |
| Сообщение `Страница обновилась` | `MISSING` | Нет состояния с прежней оценкой и сразу открытой шкалой новой revision |
| Общий NPS сервиса | `PROTOTYPE` | Prototype component и source assertion есть, но production mount/write/schema отсутствуют |
| Текст и screenshot по кнопкам | `PARTIAL` | Рабочие issue/screenshot paths есть; их надо связать с page score и service NPS контекстом |
| Offline/idempotent feedback | `PARTIAL` | Outbox и v2 RPC есть; нужен live round-trip на final target |
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
| Извлекаемая статистика | `PARTIAL` | Auth, participant, feedback и PWA данные существуют; page revisions, service NPS, artifacts/chances неполны |
| Scheduled report | `NOT REQUIRED` | Ежедневный JSON/Markdown workflow удалён из стратегии; данные запрашиваются read-only по необходимости |
| Полный focus live workflow | `MISSING` | Нет одного synthetic exact-target journey с cleanup |

## Что уже действительно тестируется

### Page-family mapping

[`site/src/lib/focus-group-surface.test.ts`](../../../../site/src/lib/focus-group-surface.test.ts)
проверяет:

- `home`, `today`, `tomorrow`, `calendar_date`, `weekend`, `popular`, `search`;
- `collections`, `festivals`, `clubs`, `club_detail`, `event_detail`;
- `exhibitions`, `unusual`, `favorites`, `for_me`;
- immutable candidate/preview prefixes;
- исключение `/fokus-gruppa/**`, `/zakrytaya-afisha/`, `/lab/**`, `/partners/**`.

Это доказывает классификацию URL, но не доказывает, что блок виден в реальном
браузере на каждой странице.

### Shared placement

[`site/src/layouts/EventLayout.astro`](../../../../site/src/layouts/EventLayout.astro)
монтирует один `FocusGroupLabPanel` после основного slot и до `SiteFooter`, если
URL получил focus page family.

Отдельного теста, который открывает route matrix и измеряет DOM-порядок,
видимость, footer и bottom navigation, сейчас нет.

### Сохранение score

[`site/src/lib/focus-feedback-state.test.ts`](../../../../site/src/lib/focus-feedback-state.test.ts)
проверяет:

- сохранение score при навигации;
- изменение `7 → 9`;
- очистку malformed/expired state;
- storage bounds.

Но текущая модель:

```text
key = page_family
TTL = 24 hours
```

не соответствует принятому контракту:

```text
key = page_family + page_revision
ответ действует до смены revision
```

### Delivery

[`site/tests/focus-group-reliable-feedback.test.mjs`](../../../../site/tests/focus-group-reliable-feedback.test.mjs)
source-проверкой подтверждает использование idempotent outbox, RPC v2, unique
`client_request_id`, storage limits и online flush.

Это не доказывает browser → network fault → одна production row; такой сценарий
ещё нужен.

### Общий NPS

[`site/src/components/FocusGroupFeedback.astro`](../../../../site/src/components/FocusGroupFeedback.astro)
содержит prototype общего relationship NPS. В
[`site/tests/focus-group-product-surface.test.mjs`](../../../../site/tests/focus-group-product-surface.test.mjs)
есть source assertion на наличие соответствующего текста.

Prototype явно сообщает, что ничего не отправляет. Рабочий Lab panel и feedback
RPC общего NPS не поддерживают.

## Что нужно реализовать именно для оценок

1. `page_revision` registry/prop для каждого page family.
2. Новое bounded local state с историей минимум текущей и предыдущей revision.
3. UI-состояния:
   - `unanswered`;
   - `answered_current`;
   - `revision_changed`.
4. Явный numeric state `Ваша оценка: N · Изменить`.
5. Точный Lab-текст обновлённой страницы с прежней оценкой.
6. DB/RPC contract для `page_score + page_revision`.
7. Один `service_nps` block в `/zakrytaya-afisha/`.
8. Отдельный `service_revision` и общий комментарий.
9. Contract/unit/browser tests из [`testing.md`](testing.md).

## Остальные уже существующие контуры

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

### Feedback backend

Рабочий контур уже поддерживает:

- `page_score` без revision;
- текстовое `issue`;
- private screenshot upload;
- idempotent outbox;
- повтор после возвращения сети;
- дневной anti-abuse cap.

Основные файлы:

- [`FocusGroupLabPanel.astro`](../../../../site/src/components/FocusGroupLabPanel.astro)
- [`20260729113221_focus_group_feedback_v1.sql`](../../../../supabase/migrations/20260729113221_focus_group_feedback_v1.sql)
- [`20260731193000_focus_group_feedback_idempotency_v2.sql`](../../../../supabase/migrations/20260731193000_focus_group_feedback_idempotency_v2.sql)

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
2. `FG-02` — page score: route matrix, numeric saved state, revision state и
   точный Lab-текст обновления.
3. `FG-03` — общий service NPS в participant hub и отдельное хранение.
4. `FG-04` — final browser/Android/iPhone/Yandex identity acceptance.
5. `FG-05` — native PWA return.
6. `FG-06` — Share, Calendar, «Не интересно», «Для меня» journey.
7. `FG-07` — server receipts для 12 артефактов, progress `10/12`.
8. `FG-08` — queryable participant/page-score/service-NPS/artifact/chance data.
9. `FG-09` — weighted pool, immutable draw, reserve and email lifecycle.
10. `FG-10` — full rehearsal на exact immutable target с synthetic cleanup.