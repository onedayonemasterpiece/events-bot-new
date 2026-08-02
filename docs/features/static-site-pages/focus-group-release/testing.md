# Тестирование фокус-группы статического сайта

> **Цель:** проверить несколько критических пользовательских путей, а не
> прогонять весь каталог на всех мобильных системах.  
> **Стратегия:** [`README.md`](README.md)  
> **NPS UI:** [`nps-ui.md`](nps-ui.md)  
> **Текущий статус:** [`status.md`](status.md)  
> **Реестр:**
> [`docs/testing/focus-group-release-scenarios.v1.yml`](../../../testing/focus-group-release-scenarios.v1.yml)

## 1. Три уровня

1. **Contract** — быстрые source/unit/schema проверки на затронутых PR.
2. **Deployed browser** — полный journey на exact immutable URL и repository SHA.
3. **Native mobile** — только keyboard, Auth, PWA и системные действия на
   Android/iPhone.

Массовые content/geometry проверки остаются в существующих static/browser gates.
Android Emulator и iOS Simulator не должны открывать сотни событий.

## 2. Минимальные GitHub Actions

### 2.1. `focus-group-contract.yml`

Новый быстрый workflow без секретов. Запускается на PR/push, когда изменены focus
UI, marker, NPS, feedback, artifacts, PWA, Auth или соответствующие миграции.

Проверяет:

- существующие `test:focus-group-product`, `test:resilient-client` и OTP unit tests;
- default locked state и отсутствие flash открытой афиши без метки;
- fixed cutoff `2026-08-31 18:00 Europe/Kaliningrad`;
- NPS всегда находится перед footer и содержит ровно 11 значений `0–10`;
- после ответа доступно `Ваша оценка: N`, а не только selected cell;
- text/screenshot остаются optional secondary actions;
- idempotent outbox и storage bounds;
- `page_revision` не меняется на каждый каталог/build автоматически;
- local marker не создаёт raffle eligibility;
- collection содержит 12 IDs, а threshold равен 10;
- chances ограничены `1…3`;
- schema/RLS не открывают raw feedback и screenshot посторонним;
- machine registry и [`status.md`](status.md) не расходятся.

### 2.2. Существующий `external-focus-email-otp.yml`

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

### 2.3. `focus-group-live.yml`

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
NPS 0–10 → немедленное сохранение → «Ваша оценка: N»
текст + screenshot по кнопкам
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
- NPS-блок и bottom sheet не перекрыты системной клавиатурой.

Workflow не проводит реальный розыгрыш и не отправляет письма участникам.

### 2.4. `focus-group-draw.yml`

Отдельный protected workflow нужен до 31 августа, но не для ежедневной работы.

Режимы:

- `dry_run` — synthetic snapshot;
- `final` — только после cutoff и explicit environment approval;
- `promote_reserve` — только после документированного отсутствия ответа.

Проверяет:

- 12-item collection version;
- threshold `10`;
- verified participant;
- минимум один NPS;
- chances: base + text + screenshot, max 3;
- immutable snapshot hash;
- winner + reserve;
- повтор того же snapshot возвращает тот же результат;
- никакой email/PII в публичном artifact.

## 3. Отдельного report workflow нет

`focus-group-report.yml`, ежедневный schedule, обязательные JSON и Markdown
artifacts не нужны.

Статистику проверяют двумя способами:

1. `focus-group-live.yml` после synthetic действий делает read-only assertions и
   cleanup;
2. владелец по необходимости просит кодового агента выполнить read-only запрос к
   production tables/views и объяснить counts.

Требуется queryability, а не оформление регулярного отчёта.

## 4. Сценарии

### A. Soft gate

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-ACCESS-01` | Без метки при первом paint видна только актуальная заглушка | browser | да |
| `FG-ACCESS-02` | Main UI hidden/inert и не достижим с keyboard/screen reader | browser/a11y | да |
| `FG-ACCESS-03` | Invite записывает метку, удаляет fragment и открывает интерфейс | browser | да |
| `FG-ACCESS-04` | Метка переживает reload/PWA и не удаляется сбросом «Для меня» | browser + Android | да |
| `FG-ACCESS-05` | В 18:00 31 августа eligibility закрывается | controlled clock + DB | да |
| `FG-ACCESS-06` | Ручной обход gate не создаёт Auth/raffle eligibility | contract | да |

### B. NPS и feedback

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-NPS-01` | NPS-блок постоянно виден перед footer | browser | да |
| `FG-NPS-02` | Видны все 0–10 и подписи крайних значений | browser/mobile | да |
| `FG-NPS-03` | Tap сразу сохраняет score без submit | browser | да |
| `FG-NPS-04` | После сохранения показано `Ваша оценка: N` + `Изменить` | browser | да |
| `FG-NPS-05` | Text/screenshot открываются только secondary buttons | browser/mobile | да |
| `FG-NPS-06` | Offline resend создаёт одну запись | browser fault injection | да |
| `FG-NPS-07` | Новая page revision снова показывает scale, старая score сохранена | contract/browser | да |
| `FG-NPS-08` | Event page автоматически прикладывает event_id | browser + DB | да |

### C. Auth

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-AUTH-01` | Browser real email identity | protected workflow | да |
| `FG-AUTH-02` | Android real email identity | protected workflow | да |
| `FG-AUTH-03` | iPhone real supported identity path | protected native path | да |
| `FG-AUTH-04` | Яндекс создаёт одного participant | protected journey | да |
| `FG-AUTH-05` | Marker without Auth can test but eligibility=false | browser + DB | да |
| `FG-AUTH-06` | Repeat registration idempotent; cap boundary safe | DB integration | до расширения |

### D. PWA и функции

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-PWA-01` | Android install → home icon → standalone relaunch | Android | да |
| `FG-PWA-02` | iPhone honest Add to Home Screen path | iOS/manual automation | да |
| `FG-USE-01` | Share opens supported system/native path | browser + mobile specimen | да |
| `FG-USE-02` | Calendar action produces valid handoff/ICS | browser + mobile specimen | да |
| `FG-USE-03` | «Не интересно» persists and can be undone | browser | да |
| `FG-USE-04` | «Для меня» visibly changes/clarifies ordering | browser | да |

### E. Artifacts, statistics and draw

| ID | Сценарий | Где | Seed blocking |
|---|---|---|---|
| `FG-EGG-01` | Frozen collection has exact 12 IDs | contract | да |
| `FG-EGG-02` | Same artifact is receipted once | DB integration | да |
| `FG-EGG-03` | 9/12 is ineligible; 10/12 is eligible | DB integration | да |
| `FG-STATS-01` | Read-only query returns participant/NPS/text/screenshot/artifact/chance counts | operator integration | да |
| `FG-STATS-02` | Query contains no raw email/OTP/IP/User-Agent unless explicitly private | contract | да |
| `FG-DRAW-01` | One base + text + screenshot = max 3 chances | DB/unit | да |
| `FG-DRAW-02` | Dry draw is deterministic for same snapshot | protected workflow | да |
| `FG-DRAW-03` | Winner and reserve are distinct | protected workflow | да |
| `FG-DRAW-04` | 3-day response deadline and reserve promotion | workflow/unit | до final draw |
| `FG-MAIL-01` | Winner email can be sent to test mailbox | protected dry run | до final draw |
| `FG-MAIL-02` | Thank-you/final winner mailing can target verified cohort once | protected dry run | до final draw |

## 5. Evidence

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