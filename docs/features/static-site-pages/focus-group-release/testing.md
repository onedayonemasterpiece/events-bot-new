# Тестирование фокус-группы статического сайта

> **Цель:** проверить несколько критических пользовательских путей, а не
> прогонять весь каталог на всех мобильных системах.  
> **Стратегия:** [`README.md`](README.md)  
> **Оценки и NPS:** [`nps-ui.md`](nps-ui.md)  
> **Текущий статус:** [`status.md`](status.md)  
> **Реестр:**
> [`docs/testing/focus-group-release-scenarios.v1.yml`](../../../testing/focus-group-release-scenarios.v1.yml)

## 1. Что уже реально тестируется в `main`

Текущий `npm --prefix site run test:focus-group-product` объединяет unit/source
контракты onboarding, Auth, feedback, page-family mapping, PWA и артефактов.
Это полезный baseline, но он не является полноценным browser E2E оценки страниц.

### Фактическое покрытие оценки страниц

| Область | Что есть | Чего тест не доказывает |
|---|---|---|
| Page-family routing | `focus-group-surface.test.ts` проверяет 16 типов URL, preview/review prefixes и исключённые focus routes | Что компонент реально отрендерился и виден на опубликованной странице |
| Общее размещение | `EventLayout.astro` монтирует один `FocusGroupLabPanel` после `<slot />` и до `SiteFooter` для распознанного family | Нет теста DOM-порядка, видимости и отсутствия дубля на матрице реальных routes |
| Локально сохранённая оценка | `focus-feedback-state.test.ts` проверяет сохранение по `page_family`, изменение значения, TTL и bounds | Нет `page_revision`; TTL 24 часа противоречит требованию «до новой версии» |
| Idempotent delivery | source-тест проверяет outbox, RPC v2, unique `client_request_id` и повтор после сети | Нет deployed fault-injection с фактической одной строкой в БД |
| UI после ответа | Рабочий компонент подсвечивает выбранную кнопку и пишет общий status | Нет состояния `Ваша оценка: N · Изменить`; число не проверяется browser-тестом |
| Новая версия страницы | Нет реализации | Нет сообщения об обновлении, старой оценки, повторно открытой шкалы и отдельной DB revision |
| Общий NPS | `FocusGroupFeedback.astro` содержит prototype panel, а source-тест проверяет наличие строк | Prototype ничего не отправляет, не смонтирован в production flow и не имеет server schema |

### Критическая текущая граница

Сейчас локальный ключ — только `page_family`, а запись истекает через 24 часа.
Это создаёт два неверных поведения:

1. неизменная страница через сутки снова выглядит неоценённой;
2. новая revision в пределах суток продолжает показывать старую оценку.

Также текущий RPC допускает только `page_score` и `issue`, не принимает
`page_revision` и не имеет отдельного `service_nps` scope. Уникальность
гарантируется по `client_request_id`, а не по page/service revision.

Следовательно, автотесты из следующего раздела нельзя считать реализованными до
изменения state model, UI и schema.

## 2. Три уровня тестов

1. **Contract** — быстрые source/unit/schema проверки на затронутых PR.
2. **Deployed browser** — полный journey на exact immutable URL и repository SHA.
3. **Native mobile** — только keyboard, Auth, PWA и системные действия на
   Android/iPhone.

Массовые content/geometry проверки остаются в существующих static/browser gates.
Android Emulator и iOS Simulator не должны открывать сотни событий.

## 3. Минимальные GitHub Actions

### 3.1. `focus-group-contract.yml`

Новый быстрый workflow без секретов. Запускается на PR/push, когда изменены focus
UI, marker, page score, service NPS, feedback, artifacts, PWA, Auth или
соответствующие миграции.

Проверяет:

- существующие `test:focus-group-product`, `test:resilient-client` и OTP unit tests;
- default locked state и отсутствие flash открытой афиши без метки;
- fixed cutoff `2026-08-31 18:00 Europe/Kaliningrad`;
- route matrix всех поддерживаемых page families;
- один Lab-блок после main content и перед footer;
- ровно 11 значений `0–10`, без default selection;
- после ответа доступно `Ваша оценка: N`, а не только selected cell;
- state machine `unanswered | answered_current | revision_changed`;
- новый revision сразу открывает шкалу и сохраняет прежнюю оценку;
- неизменная revision не переоткрывается по 24-часовому TTL;
- `page_revision` не выводится автоматически из repository/build SHA;
- `page_score` и `service_nps` имеют независимые storage/database keys;
- общий NPS находится в одном каноническом hub, а не вторым вопросом на каждой
  странице;
- text/screenshot остаются optional secondary actions;
- idempotent outbox и storage bounds;
- local marker не создаёт raffle eligibility;
- collection содержит 12 IDs, а threshold равен 10;
- chances ограничены `1…3`;
- schema/RLS не открывают raw feedback и screenshot посторонним;
- machine registry и [`status.md`](status.md) не расходятся.

### 3.2. Существующий `external-focus-email-otp.yml`

Не создаётся второй OTP harness. Используется merged workflow:

[`/.github/workflows/external-focus-email-otp.yml`](../../../../.github/workflows/external-focus-email-otp.yml)

Он последовательно проверяет real-mail путь для browser, Android и iOS.

Текущая evidence:

- [Browser PASS](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30745526613)
- [Android PASS](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30747598046)
- [iOS `FAIL_MOBILE_KEYBOARD`](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30754894934)

Перед seed browser и Android повторяются на финальном target. Для iPhone нужен
один terminal поддерживаемый identity path:

- исправленный OTP с реальной keyboard/input acceptance; либо
- magic link; либо
- Яндекс.

DOM-focus без поддерживаемого способа ввести значение не считается PASS.

### 3.3. `focus-group-live.yml`

Новый protected reusable/manual workflow. Входы:

- `target_url`;
- `expected_repo_sha`;
- `mode: rehearsal | seed | post_deploy`;
- `platform: browser | android | ios | all`.

Browser проходит один synthetic journey:

```text
без метки → видна только заглушка
приглашение → метка → обычный интерфейс
reload → доступ сохраняется
матрица page families → один Lab-блок перед footer
page score 0–10 → немедленное сохранение → «Ваша оценка: N»
reload той же revision → сохранённое число
fixture новой revision → сообщение «Страница обновилась» + прежняя оценка + scale
оценка новой revision → две исторические revision, один current answer
переход в другой page family → независимый score
общий NPS в participant hub → отдельный service_nps row
общий комментарий → не меняет page score
text + screenshot по кнопкам
offline → outbox → online → ровно одна запись
Share / Calendar / Не интересно / Для меня
synthetic artifact receipts → progress 10/12
без Auth → raffle eligibility false
с test identity → eligibility/chances вычисляются
read-only запрос видит непротиворечивые данные
cleanup test participant/feedback/artifacts
```

Android/iOS проверяют только:

- usable viewport и native keyboard/input;
- install/add-to-home-screen и standalone return;
- share/calendar system handoff на одном specimen;
- шкалу page score и feedback sheet без перекрытия системной клавиатурой;
- открытие общего NPS по ссылке из Lab-блока.

Workflow не проводит реальный розыгрыш и не отправляет письма участникам.

### 3.4. `focus-group-draw.yml`

Отдельный protected workflow нужен до 31 августа, но не для ежедневной работы.

Режимы:

- `dry_run` — synthetic snapshot;
- `final` — только после cutoff и explicit environment approval;
- `promote_reserve` — только после документированного отсутствия ответа.

Проверяет:

- 12-item collection version;
- threshold `10`;
- verified participant;
- минимум один `page_score`;
- общий NPS не является дополнительным eligibility gate;
- chances: base + text + screenshot, max 3;
- immutable snapshot hash;
- winner + reserve;
- повтор того же snapshot возвращает тот же результат;
- никакой email/PII в публичном artifact.

## 4. Отдельного report workflow нет

`focus-group-report.yml`, ежедневный schedule, обязательные JSON и Markdown
artifacts не нужны.

Статистику проверяют двумя способами:

1. `focus-group-live.yml` после synthetic действий делает read-only assertions и
   cleanup;
2. владелец по необходимости просит кодового агента выполнить read-only запрос к
   production tables/views и объяснить counts.

Требуется queryability, а не оформление регулярного отчёта.

## 5. Точные тесты оценок и NPS

### 5.1. Contract/unit state machine

| ID | Вход | Ожидаемое состояние |
|---|---|---|
| `FG-SCORE-U01` | Нет ответа для `home-r3` | Шкала `0–10`, no selection |
| `FG-SCORE-U02` | Есть `home-r3=7`, открыта `home-r3` | `Ваша оценка: 7 · Изменить` |
| `FG-SCORE-U03` | Есть `home-r3=7`, открыта `home-r4` | `revision_changed`, прежняя `7`, открытая шкала |
| `FG-SCORE-U04` | Для `home-r4` выбрано `9` | `home-r3=7` сохранено, current `home-r4=9` |
| `FG-SCORE-U05` | Прошло больше 24 часов, revision та же | Ответ остаётся current, шкала не переоткрывается |
| `FG-SCORE-U06` | `event_detail-r17=8`, открыта `home-r4` | Home не считается оценённой |
| `FG-SCORE-U07` | Revision отсутствует/невалидна | Fail closed: score не отправляется без version context |
| `FG-SERVICE-U01` | Page score существует, service NPS отсутствует | Общий NPS остаётся unanswered |
| `FG-SERVICE-U02` | `service-r2=6`, page score отсутствует | Page scale остаётся unanswered |
| `FG-SERVICE-U03` | Изменение service NPS | Обновляется только текущая service revision |

### 5.2. Placement matrix

Browser-тест открывает по одному route каждого поддерживаемого family:

```text
home
today
tomorrow
calendar_date
weekend
popular
search
collections
festivals
clubs
club_detail
event_detail
exhibitions
unusual
favorites
for_me
```

На каждом route с активной focus marker:

- ровно один `[data-focus-lab-panel]`;
- он видим;
- находится после основного содержимого и перед `footer`;
- содержит `Lab`;
- содержит либо шкалу текущей revision, либо явное сохранённое число;
- не перекрывается bottom navigation;
- на excluded routes `/fokus-gruppa/**`, `/lab/**`, `/partners/**` page score не
  монтируется.

### 5.3. Revision browser journey

1. Открыть specimen `home-r3`.
2. Выбрать `7`.
3. Дождаться `Ваша оценка: 7`.
4. Reload — состояние сохраняется.
5. Открыть тот же rendered specimen с `home-r4`.
6. Увидеть:

   ```text
   Lab · Страница обновилась
   Вы уже оценивали эту страницу — ранее: 7.
   ```

7. Убедиться, что шкала доступна сразу.
8. Выбрать `9`.
9. Проверить `Ваша оценка новой версии: 9`.
10. Read-only DB assertion: одна current row для `home-r4`, историческая row
    `home-r3` сохранена, дубля одного `client_request_id` нет.

### 5.4. Общий NPS journey

1. Открыть `/zakrytaya-afisha/#obshchiy-nps`.
2. Убедиться, что виден один `Lab · Общий NPS сервиса`.
3. Выбрать значение без submit.
4. Увидеть `Ваша общая оценка: N · Изменить`.
5. Добавить текст `Что в целом неудобно или чего не хватает?`.
6. Проверить отдельную `service_nps` запись и независимость от page scores.
7. Перейти на обычную страницу: page score остаётся своим, общий NPS не
   появляется вторым обязательным вопросом.

## 6. Остальные сценарии

### A. Soft gate

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-ACCESS-01` | Без метки при первом paint видна только актуальная заглушка | browser | да |
| `FG-ACCESS-02` | Main UI hidden/inert и не достижим с keyboard/screen reader | browser/a11y | да |
| `FG-ACCESS-03` | Invite записывает метку, удаляет fragment и открывает интерфейс | browser | да |
| `FG-ACCESS-04` | Метка переживает reload/PWA и не удаляется сбросом «Для меня» | browser + Android | да |
| `FG-ACCESS-05` | В 18:00 31 августа eligibility закрывается | controlled clock + DB | да |
| `FG-ACCESS-06` | Ручной обход gate не создаёт Auth/raffle eligibility | contract | да |

### B. Auth

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-AUTH-01` | Browser real email identity | protected workflow | да |
| `FG-AUTH-02` | Android real email identity | protected workflow | да |
| `FG-AUTH-03` | iPhone real supported identity path | protected native path | да |
| `FG-AUTH-04` | Яндекс создаёт одного participant | protected journey | да |
| `FG-AUTH-05` | Marker without Auth can test but eligibility=false | browser + DB | да |
| `FG-AUTH-06` | Repeat registration idempotent; cap boundary safe | DB integration | до расширения |

### C. PWA и функции

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-PWA-01` | Android install → home icon → standalone relaunch | Android | да |
| `FG-PWA-02` | iPhone honest Add to Home Screen path | iOS/manual automation | да |
| `FG-USE-01` | Share opens supported system/native path | browser + mobile specimen | да |
| `FG-USE-02` | Calendar action produces valid handoff/ICS | browser + mobile specimen | да |
| `FG-USE-03` | «Не интересно» persists and can be undone | browser | да |
| `FG-USE-04` | «Для меня» visibly changes/clarifies ordering | browser | да |

### D. Artifacts, statistics and draw

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-EGG-01` | Frozen collection has exact 12 IDs | contract | да |
| `FG-EGG-02` | Same artifact is receipted once | DB integration | да |
| `FG-EGG-03` | 9/12 is ineligible; 10/12 is eligible | DB integration | да |
| `FG-STATS-01` | Read-only query separates page scores, service NPS, text, screenshot, artifacts and chances | operator integration | да |
| `FG-STATS-02` | Query contains no raw email/OTP/IP/User-Agent unless explicitly private | contract | да |
| `FG-DRAW-01` | One base + text + screenshot = max 3 chances | DB/unit | да |
| `FG-DRAW-02` | Dry draw is deterministic for same snapshot | protected workflow | да |
| `FG-DRAW-03` | Winner and reserve are distinct | protected workflow | да |
| `FG-DRAW-04` | 3-day response deadline and reserve promotion | workflow/unit | до final draw |
| `FG-MAIL-01` | Winner email can be sent to test mailbox | protected dry run | до final draw |
| `FG-MAIL-02` | Thank-you/final winner mailing can target verified cohort once | protected dry run | до final draw |

## 7. Evidence

Каждый blocking live run фиксирует:

- exact 40-character repository SHA;
- immutable target URL/build identity;
- scenario/platform;
- terminal outcome;
- sanitized counters;
- cleanup result.

OTP artifacts не содержат video/trace/HAR. Feedback artifacts не содержат raw
text, screenshot или participant PII. `STARTED_BACKGROUND` и `BLOCKED` не
считаются PASS.