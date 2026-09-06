# Партнёрское промо

> Статус: phase A shipped (2026-05-18). Канонический ledger требований и
> поведения для партнёрского сценария промо. Любые уточнения в чате должны
> приземляться сюда в тот же ход. Базовая модель промо-кампаний и контракт
> CherryFlash описаны в соседнем
> [`docs/features/promo-campaigns/README.md`](README.md); этот файл —
> расширение для партнёрского UX и нового правила KONB.

## 1. Цель

Дать партнёрам и суперадмину единый явный механизм продвижения конкретных
событий через UI бота:

- видеоанонсы — профили `default` (завтра), `popular_review` (популярное),
  `konb` (партнёрский трек КОНБ);
- репост ВК-исходника события в общий партнёрский паблик
  `vk.com/club231828790` (тип активности заложен в схему, исполнение в
  фазе B);
- (заложено) контекстные подъёмы — например, событие научной библиотеки с
  «просто попасть в видео» промо автоматически поднимается на слот 1–2
  в видеоанонсе KONB при отсутствии более приоритетной конкуренции.

Партнёрский слой не вводит параллельную таблицу — только новые активности,
правила доступа и UI поверх общей модели `promo_campaign` /
`promo_target` / `promo_activity` / `promo_exposure`.

## 2. Состояние реализации

Phase A (готово):

- модели `Organization`, `PromoVkRepostJob`; миграции;
- idempotent seed `Научная библиотека` →
  `vk_source_group_ids=[30777579]`, `video_profile_key='konb'`,
  `sponsorship_default='Партнёрский материал · Научная библиотека'`;
- `promo.create_partner_event_promo_campaign`, `clamp_campaign_end_to_event`,
  `build_partner_campaign_title`;
- константы `PROMO_POLICY_FIRST_TWO_SLOTS`, `PROMO_SURFACE_VIDEO_GENERAL`,
  `PROMO_SURFACE_VK_REPOST`;
- `🎬` кнопка на `/events` → 6-шаговый FSM (`handlers/partner_promo_cmd.py`,
  callback prefix `ppromo:`);
- `/promo` без аргументов — кнопочное меню партнёра и админа;
  per-campaign карточка: Пауза/Запуск, Архив, Статистика, Переименовать,
  `➕ Активность` для не-архивных; у админа дополнительно строка
  приоритетов `P0..P3`;
- активности рендерятся человекопонятно: «🎬 Видеоанонс · Популярное ·
  слот 1–2 · всего показов: 3» (хелпер `_humanize_activity`, словари
  `_SURFACE_LABELS`/`_PROFILE_LABELS`/`_SLOT_POLICY_LABELS`); технические
  ключи (`video_general`, `popular_review`, `first_two_slots`) скрыты;
- multi-activity: кнопка `➕ Активность` открывает сокращённый FSM
  (поверхность → слот → количество → подтверждение), наследует
  период/режим/раскрытие; `promo.add_partner_activity_to_campaign`
  добавляет `PromoActivity` к существующей кампании, отказ на архивную;
- защищённые callback-проверки `is_superadmin or event.creator_id == user_id`;
- защитные `try/except` импорты в `main_part2.create_app` (после
  `INC-2026-05-18`);
- интерфейсный dry-run под Тикун: `scripts/partner_promo_interface_dry_run.py`
  (15 кадров — `/promo`, шаги 1–6, подтверждение, карточка кампании,
  пауза, статистика, архив);
- live E2E прогон под @The_day_of_kk через @eventsbotTestBot подтверждён:
  создание кампании через 🎬, добавление второй активности через
  ➕ Активность, обе активности видны в карточке с человекопонятными
  лейблами (artifacts/test-results/multi_activity_live.txt и
  two_activity_card.txt).

Phase B (открыто, см. §10):

- ВК-репост активности — расписание, runner, дедуп, уведомления;
- KONB auto-promote-to-slot-1-2 правило (см. §7);
- `selection_policy=first_two_slots` интеграция в видеоотборщик;
- DM-уведомления партнёру и суперадмину о публичных показах и пропусках;
- расширенная статистика с misses (`slot conflict`, `source_unavailable`,
  `dedup window`, `vk_rate_limited`, `window closed`);
- кнопка `🌐 Сайт` — пока plain alert.

### Общая транзакционная граница (#643)

`create_partner_event_promo_campaign` сохраняет campaign, event-target, выбранную
activity и стандартную TG-button activity одной транзакцией. Ошибка после
выделения campaign ID не оставляет активную пустую кампанию. Оба существующих
сервиса создания кампании и добавления activity принимают необязательный
`session`: при его передаче выполняется только flush, а commit/rollback остаётся
у application caller. Это позволяет присоединить проверку актуальных прав и
operation receipt к той же транзакции, не копируя правила promo в MCP.
Без session старые FSM callers сохраняют прежнее поведение с одним commit.
Авторизация по-прежнему обязательна на caller boundary; session не является
правом доступа, а эта подготовка не включает OAuth promo tools или выдачу прав.
Добавление activity не возобновляет paused campaign. Проверки атомарности и
rollback: `tests/test_promo_transaction_boundary.py`.

## 3. Роли и доступ

В системе реально две роли: `is_partner` и `is_superadmin`.

- `/events` доступен всем зарегистрированным незаблокированным
  пользователям; партнёр видит только свои события
  (`creator_filter = user.user_id`, `main_part2.py:6515`).
- Кнопка `🎬` появляется на любой строке `/events`, которую пользователь
  уже видит — для партнёра это автоматически только его события.
- Callback-и `ppromo:*` обязаны повторно проверять
  `is_superadmin or event.creator_id == user.user_id`. Без этого
  партнёр сможет подсунуть чужой `event_id` в `callback_data`.
- `/promo` (без аргументов) открывается партнёру и админу одинаково;
  партнёр видит в меню только кампании, где `created_by == user_id`,
  админ — все.
- `/promo report`, `/promo add ...` и другие текстовые подкоманды —
  legacy-путь, остаётся только у админа.

## 4. Точка входа: кнопка 🎬

В `/events` исторически была кнопка `🎬 <N>`
(`main_part2.py:5400`, счётчик `video_include_count`). Счётчик на практике
не использовался: даже на событиях, которые попадали в промо, в проде
оставался 0. С phase A кнопка перевешена на промо:

- лейбл `🎬 +`;
- callback `ppromo:start:{event_id}`;
- `video_include_count` как колонка остаётся (читается видеопайплайном),
  но из UI выведен; ненулевые остаточные значения в проде уже отсутствуют.

## 5. FSM создания кампании

Все шаги — inline-клавиатура; состояние в `partner_promo_sessions[user_id]`
(TTL 30 мин). На каждом шаге нижняя строка содержит `◀ Назад` и
`✕ Отмена`. Отмена очищает состояние и удаляет промежуточные сообщения.

### Шаг 0 — карточка события

Список действующих кампаний пользователя по этому событию (если есть) с
кнопкой `📊 #N <title>` на каждую, и `➕ Новая промо-кампания`,
`✕ Закрыть`. Суперадмин видит все кампании по событию, не только свои.

### Шаг 1 — поверхность

- `🎬 Видеоанонс — популярное` (профиль `popular_review`);
- `🎬 Видеоанонс — завтра` (профиль `default`);
- `🎬 Видеоанонс — КОНБ` (профиль `konb`) — только если
  `organization.video_profile_key == 'konb'`;
- `📨 Репост в партнёрский паблик` — только если у события есть
  `source_vk_post_url` и хотя бы один исходник из паблика, числящегося в
  `organization.vk_source_group_ids` (см. §8);
- `🌐 Сайт` — кнопка-плейсхолдер с alert «Размещение на сайте появится
  позже».

Для суперадмина все профили видеоанонса видны всегда.

### Шаг 2 — расположение (только для видеоанонса)

- `Любая позиция` → `selection_policy=guaranteed_any_position`;
- `Слот 1–2` → `selection_policy=first_two_slots`, занимает позицию 1
  или 2 в случайном/приоритетном порядке;
- `Только слот 1` → `selection_policy=first_slot`, `slot=1`.

Под кнопками пояснение, что слот может не гарантироваться, если уже
занят более приоритетной кампанией; попытка переносится на следующий
выпуск в окне кампании.

### Шаг 3 — количество показов

Быстрые кнопки `1, 2, 3, 5, 7, 10` + «Ввести число» (reply-ввод).
Сохраняется в `promo_campaign.total_exposure_goal` и
`promo_activity.target_exposure_goal`.

### Шаг 4 — дата окончания

Inline-клавиатура: `+7 дней`, `+14 дней`, `+30 дней`,
`До даты события (YYYY-MM-DD)`, `Ввести дату`. Любая выбранная дата
**клампится** до `min(operator_choice, event.end_date or event.date)`:
продвигать после конца события бессмысленно.

### Шаг 5 — режим (раскрытие)

- `Партнёрский / коммерческий` (по умолчанию) → требует текст-раскрытие;
  дефолт берётся из `organization.sponsorship_default` или
  «Партнёрский материал»;
- `Редакционный (бесплатный)` → `sponsorship_disclosure = NULL`,
  на зрительских поверхностях публикуется только маркер `✨`. Доступно
  и партнёру (для дружественных размещений КОНБ), и суперадмину;
  партнёр сам отвечает за корректность.

### Шаг 6 — подтверждение

Сводка: название, событие, размещение (профиль + слот), количество,
период, режим, раскрытие. Кнопки `✅ Запустить`, `✏ Переименовать`,
`✕ Отмена`, `◀ Назад`. Только после `✅ Запустить` создаётся
`PromoCampaign` со `status='active'`.

Авто-имя: `f"{org or partner_username} · {event_title[:40]} · {YYYY-MM-DD}"`
(админ: `f"editorial · ..."`). Имя показывается партнёру в подтверждении
и в карточке; можно изменить через `✏ Переименовать` (в подтверждении или
из карточки кампании).

## 6. Меню `/promo` и карточка кампании

`/promo` без аргументов открывает меню, общее для партнёра и админа:

- заголовок «Промо-кампании (партнёр|суперадмин)»;
- список свежих кампаний (до 10 строк), каждая — кнопка `#N <title>`;
- футер: `📦 Архив` или `▣ К активным`, для админа — `🌟 Seed 80`,
  `✕ Закрыть`.

Карточка кампании (`ppromo:view:{cid}`):

- статус, приоритет, период, прогресс показов, текст раскрытия;
- блоки «Цели» и «Активности» — каждая активность рендерится
  человекопонятными лейблами (`_humanize_activity`): `🎬 Видеоанонс ·
  Популярное · слот 1–2 · всего показов: 3`. Технические ключи
  (`video_general`, `popular_review`, `first_two_slots`) скрыты от
  оператора;
- кнопки: `⏸ Пауза` / `▶ Запустить` (динамически по статусу),
  `📦 Архив` / `🔄 Восстановить`, `📊 Статистика`, `✏ Переименовать`;
- кнопка `➕ Активность` для не-архивных кампаний — открывает
  сокращённый FSM (§6.1), который добавляет PromoActivity к существующей
  кампании;
- у админа дополнительная строка `P0 P1 P2 P3` — выставление приоритета;
- `◀ К списку`.

### 6.1. Добавление активности к существующей кампании

`➕ Активность` запускает условную ветвь FSM, переиспользующую шаги 1–3
полного флоу:

- Шаг 1 — выбор поверхности (тот же набор кнопок, что и при создании);
- Шаг 2 — слот (для `video_general`);
- Шаг 3 — количество показов;
- **шаги 4 (дата окончания) и 5 (режим) пропускаются** — период, режим
  и текст раскрытия наследуются от существующей кампании;
- Шаг 6 — экран подтверждения с заголовком «Добавление активности»
  показывает: к какой кампании добавляется, наследуемые период/режим,
  и новую активность. Кнопка `✅ Добавить активность` (вместо
  `✅ Запустить`); `✏ Переименовать` скрыт.

Под капотом — `promo.add_partner_activity_to_campaign(spec)` создаёт
новую `PromoActivity` для существующего `PromoCampaign`. Поверхности
`video_general` и `vk_repost` поддерживаются. Кампания в статусе
`archived` отвергает добавление активности с явным сообщением «сначала
восстановите её».

Статистика (`ppromo:stats:{cid}`) — счётчики по поверхностям и последние
показы (до 8). misses-учёт ждёт phase B (см. §10).

## 7. KONB CherryFlash — авто-промо на слот 1–2

CherryFlash KONB (`partner_konb_library_001`) уже работает и отбирает
события Научной библиотеки. Phase B добавит контекстный подъём:

- если у события научной библиотеки есть активная промо-кампания с
  `surface=video_general`, `selection_policy=guaranteed_any_position`
  («просто попасть в видеоанонс»), то в выпуске КОНБ-CherryFlash это
  событие **повышается** до слота 1–2 — независимо от естественного
  ранжирования партнёрского трека;
- правило применяется ровно один раз на релиз и не влияет на другие
  профили (`default`, `popular_review`);
- правило **подавляется**, если в этом же релизе уже есть более
  приоритетная партнёрская кампания библиотеки с
  `selection_policy IN (first_slot, first_two_slots)` и занятыми
  слотами 1–2. В этом случае «просто попасть» остаётся в обычной
  позиции трека (или попадает мимо — на общих основаниях).

Технически правило живёт в KONB-плече CherryFlash-резолвера и читает
`PromoActivity` напрямую; общий `resolve_video_promo_candidates` его не
дублирует. Приоритет внутри правила: партнёрские `first_slot`/
`first_two_slots` > KONB-авто-промо > органическое ранжирование.

## 8. ВК-репост (тип активности)

Тип активности `surface='vk_repost'` присутствует в схеме и принимается
функцией создания кампании. Текущая видимость кнопки в FSM:

- кнопка `📨 Репост в партнёрский паблик` появляется только если у
  события есть `source_vk_post_url` и хотя бы один исходник из паблика
  партнёрской организации (`organization.vk_source_group_ids`);
- при нажатии в phase A показывается alert «Появится в следующем
  релизе» — выбор не сохраняется.

Phase B добавит:

- таблица `promo_vk_repost_job` (уже в схеме): `id, campaign_id,
  activity_id, event_id, scheduled_at, source_owner_id, source_post_id,
  status, attempts, executed_at, vk_post_id, error_json`;
- расписание: окно `[now, ends_at]`, разрешённые часы
  `09:00–22:00 Europe/Kaliningrad`, равномерное распределение N
  репостов, минимальный зазор 3 ч;
- дедуп: один и тот же `(owner_id, post_id)` не репостится в
  `vk.com/club231828790` чаще раза в 72 ч;
- runner `promo_vk_repost_runner`: предполётная проверка прав бота в
  целевом клубе, retry с backoff, явные коды ошибок (`source_unavailable`,
  `vk_rate_limited`, `dedup window`);
- запись `promo_exposure` с `surface=vk_repost`, ссылка на пост,
  DM-уведомление партнёру и суперадмину.

## 9. Данные

```text
organization
  name TEXT PRIMARY KEY           -- == user.organization
  vk_source_group_ids JSON        -- паблики, считающиеся источниками орг.
  video_profile_key TEXT          -- e.g. 'konb' — открывает спец. профиль
  sponsorship_default TEXT        -- дефолтный текст раскрытия
  created_at, updated_at TIMESTAMP

promo_vk_repost_job              -- см. §8
  id, campaign_id, activity_id, event_id,
  scheduled_at, source_owner_id, source_post_id,
  status, attempts, executed_at, vk_post_id, error_json,
  created_at, updated_at
```

Связка `User → Organization` идёт по точному совпадению
`User.organization == Organization.name`. Прод-канон: имя «Научная
библиотека» — оно используется в `user.organization` у Тикун, поэтому
seed повторяет именно это имя, а `video_profile_key='konb'`
переключает Тикун на партнёрский трек.

## 10. Открытые / решённые вопросы (2026-05-18)

Уточнено в этом раунде:

- партнёр / админ авторизация — см. §3;
- слот 1–2 — `selection_policy=first_two_slots` (резолвер — phase B);
- слот может не гарантироваться (перенос на следующий выпуск);
- `ends_at` для `target=event` клампится до даты события;
- окно ВК-репоста — `09:00–22:00 Europe/Kaliningrad`;
- уведомления — в DM бота (phase B);
- источник «связан с партнёром» — точное соответствие
  `organization.vk_source_group_ids`, не эвристика;
- редакционный режим доступен партнёру и админу;
- партнёрский дефолтный приоритет `2`, админский `1`, ручной `0`;
- авто-имя кампании видимо партнёру и редактируемо;
- KONB CherryFlash auto-promote — §7;
- ВК-репост заложен как тип активности, реализация — phase B.

## 11. Gherkin-сценарии

```gherkin
Feature: Партнёрское промо для своих событий
  Партнёр продвигает свои события через кнопочный UI, не получая доступа
  к чужим событиям и без необходимости знать команды.

  Background:
    Given в БД существует партнёр @Ekaterina_Tikun с organization "Научная библиотека"
    And в Organization есть запись "Научная библиотека" с video_profile_key="konb"
    And у партнёра есть будущее активное событие #1 "Лекция о Канте" на 2026-05-25
    And в БД нет ни одной промо-кампании по этому событию

  Scenario: /events партнёра показывает только свои события и кнопку 🎬
    When @Ekaterina_Tikun вводит "/events"
    Then ответ содержит строку "#1 Лекция о Канте"
    And ответ не содержит чужих событий
    And в клавиатуре каждой строки события есть кнопка с эмодзи 🎬
    And callback_data кнопки имеет вид "ppromo:start:1"

  Scenario: 🎬 на чужом событии возвращает Not authorized
    Given в БД есть чужое событие #99, creator_id != Ekaterina_Tikun
    When @Ekaterina_Tikun кликает кнопку с callback_data "ppromo:start:99"
    Then бот отвечает alert "Not authorized"
    And меню кампаний не открывается

  Scenario: Создание видеопромо для своего события — happy path
    When @Ekaterina_Tikun кликает "ppromo:start:1"
    Then открывается шаг 0 со списком кампаний и кнопкой "➕ Новая промо-кампания"
    When @Ekaterina_Tikun кликает "Новая промо-кампания"
    Then открывается шаг 1 с кнопками "Видеоанонс — популярное", "Видеоанонс — завтра", "Видеоанонс — КОНБ"
    And кнопка "Видеоанонс — КОНБ" видима, потому что organization.video_profile_key == "konb"
    When @Ekaterina_Tikun выбирает "Видеоанонс — популярное"
    Then открывается шаг 2 с кнопками "Любая позиция", "Слот 1–2", "Только слот 1"
    When @Ekaterina_Tikun выбирает "Слот 1–2"
    Then открывается шаг 3 с пресетами 1,2,3,5,7,10 и кнопкой "Ввести число"
    When @Ekaterina_Tikun выбирает "3"
    Then открывается шаг 4 с пресетами +7/+14/+30 дней и кнопкой "До даты события (2026-05-25)"
    When @Ekaterina_Tikun выбирает "До даты события"
    Then открывается шаг 5 с режимами "Партнёрский / коммерческий" и "Редакционный (бесплатный)"
    When @Ekaterina_Tikun выбирает "Партнёрский / коммерческий"
    Then открывается шаг 6 со сводкой кампании
    And в сводке "Название: <code>Научная библиотека · Лекция о Канте · YYYY-MM-DD</code>"
    And в сводке "Раскрытие: «Партнёрский материал · Научная библиотека»"
    When @Ekaterina_Tikun кликает "✅ Запустить"
    Then в БД создаётся PromoCampaign со status=active, priority=2, total_exposure_goal=3
    And у кампании sponsorship_disclosure="Партнёрский материал · Научная библиотека"
    And создан PromoTarget(target_type='event', event_id=1)
    And создан PromoActivity(surface='video_general', profile_key='popular_review',
                              selection_policy='first_two_slots', target_exposure_goal=3)
    And бот отвечает "✅ Кампания #N активна"

  Scenario: Дата окончания клампится до даты события
    Given у партнёра есть событие #2 на 2026-05-25 без end_date
    When @Ekaterina_Tikun проходит FSM до шага 4 для события #2
    And выбирает "Ввести дату" и присылает "2026-09-01"
    Then promo_campaign.ends_at == 2026-05-25 23:59:59 UTC
    And бот не показывает ошибку

  Scenario: Редакционный режим — без раскрытия и без подписи
    When @Ekaterina_Tikun проходит FSM до шага 5
    And выбирает "Редакционный (бесплатный)"
    Then в сводке шаг 6 не содержит строки "Раскрытие:"
    When @Ekaterina_Tikun кликает "✅ Запустить"
    Then PromoCampaign.sponsorship_disclosure IS NULL

  Scenario: Отмена в середине FSM очищает состояние
    When @Ekaterina_Tikun начинает FSM и доходит до шага 3
    And кликает "✕ Отмена"
    Then запись partner_promo_sessions[user_id] удалена
    And сообщение FSM удалено

  Scenario: Кнопка "Видеоанонс — КОНБ" скрыта для организаций без konb-профиля
    Given у партнёра другого организации video_profile_key IS NULL
    When этот партнёр доходит до шага 1
    Then в клавиатуре нет кнопки "Видеоанонс — КОНБ"

  Scenario: Кнопка "Сайт" возвращает alert "появится позже"
    When @Ekaterina_Tikun на шаге 1 кликает "🌐 Сайт"
    Then показывается alert "Размещение на сайте появится в следующих релизах"
    And FSM не переходит дальше

  Scenario: ВК-репост — кнопка скрыта без подходящего источника
    Given у события #1 source_vk_post_url IS NULL
    When @Ekaterina_Tikun доходит до шага 1
    Then в клавиатуре нет кнопки "Репост в партнёрский паблик"

  Scenario: ВК-репост — alert "появится в следующем релизе" (phase A)
    Given у события #1 source_vk_post_url содержит wall-30777579_...
    And organization.vk_source_group_ids == [30777579]
    When @Ekaterina_Tikun на шаге 1 кликает "Репост в партнёрский паблик"
    Then показывается alert "Репост в партнёрский паблик появится в следующем релизе"
    And кампания не создаётся

  Scenario: /promo как партнёр показывает только свои кампании
    Given в БД есть кампания #1 created_by=Ekaterina_Tikun и кампания #2 created_by=other_partner
    When @Ekaterina_Tikun вводит "/promo"
    Then в списке только кампания #1
    And нет кнопки "Seed 80"

  Scenario: /promo как суперадмин показывает все кампании и Seed 80
    Given в БД есть кампании #1 и #2 разных партнёров
    When superadmin вводит "/promo"
    Then в списке #1 и #2
    And видна кнопка "🌟 Seed 80"

  Scenario: Карточка кампании — Пауза / Возобновление
    Given у Ekaterina_Tikun есть активная кампания #1
    When @Ekaterina_Tikun кликает "ppromo:view:1"
    Then показывается карточка #1 со статусом "активна" и кнопкой "⏸ Пауза"
    When @Ekaterina_Tikun кликает "⏸ Пауза"
    Then promo_campaign.status == "paused"
    And карточка перерисовывается с кнопкой "▶ Запустить"

  Scenario: Карточка — Архив и Восстановление
    Given кампания #1 активна
    When @Ekaterina_Tikun кликает "📦 Архив"
    Then status == "archived" и archived_at != NULL
    And в меню "Архив" появляется #1
    When @Ekaterina_Tikun кликает на #1 в архиве и затем "🔄 Восстановить"
    Then status == "active"

  Scenario: Карточка — Переименование
    When @Ekaterina_Tikun на карточке #1 кликает "✏ Переименовать"
    Then partner_promo_input_sessions[user_id].field == "rename"
    And partner_promo_input_sessions[user_id].campaign_id == 1
    When @Ekaterina_Tikun присылает текст "Кант лекция / лето"
    Then promo_campaign[1].title == "Кант лекция / лето"

  Scenario: Партнёр не может управлять чужой кампанией
    Given существует кампания #99 created_by=other_partner
    When @Ekaterina_Tikun кликает "ppromo:view:99"
    Then alert "Кампания недоступна"
    When @Ekaterina_Tikun кликает "ppromo:pause:99"
    Then alert "Кампания недоступна"

  Scenario: Админская строка приоритетов видна только суперадмину
    Given кампания #1 любая
    When суперадмин открывает карточку #1
    Then в клавиатуре есть кнопки "P0", "P1", "P2", "P3"
    When партнёр (владелец) открывает карточку #1
    Then в клавиатуре нет кнопок "P0..P3"

  Scenario: Статистика — пусто
    When @Ekaterina_Tikun на карточке #1 кликает "📊 Статистика"
    Then показывается "Публичных показов пока нет"
    And видна кнопка "◀ Назад к карточке"

  Scenario: Добавление второй активности к существующей кампании
    Given у @Ekaterina_Tikun есть активная кампания #1 с одной активностью
          (Видеоанонс — популярное, слот 1–2, 3 показа)
    When @Ekaterina_Tikun открывает карточку #1 и кликает "➕ Активность"
    Then открывается шаг 1 с тем же набором кнопок поверхности, что и в основном FSM
    When @Ekaterina_Tikun выбирает "🎬 Видеоанонс — завтра"
    Then открывается шаг 2 (слот)
    When @Ekaterina_Tikun выбирает "Только слот 1"
    Then открывается шаг 3 (количество)
    When @Ekaterina_Tikun выбирает "2"
    Then открывается экран "Добавление активности"
    And в тексте показано "К кампании: #1"
    And показаны наследуемые "Период" и "Режим"
    And шаги 4 (дата) и 5 (режим) пропущены
    And в клавиатуре есть кнопка "✅ Добавить активность"
    And в клавиатуре НЕТ кнопки "✏ Переименовать"
    When @Ekaterina_Tikun кликает "✅ Добавить активность"
    Then у PromoCampaign #1 теперь две PromoActivity
    And карточка #1 в разделе "Активности:" перечисляет обе:
          "🎬 Видеоанонс · Популярное · слот 1–2 · всего показов: 3"
          "🎬 Видеоанонс · Завтра · только слот 1 · всего показов: 2"
    And технические ключи (video_general, first_two_slots, first_slot) НЕ
        попадают в текст карточки

  Scenario: ➕ Активность скрыта на архивной кампании
    Given у @Ekaterina_Tikun есть кампания #2 в статусе "архив"
    When @Ekaterina_Tikun открывает карточку #2
    Then в клавиатуре карточки нет кнопки "➕ Активность"

  Scenario: Попытка добавить активность в архив возвращает ошибку
    Given кампания #2 заархивирована, в БД вызван add_partner_activity_to_campaign напрямую
    When add_partner_activity_to_campaign(spec, campaign_id=2)
    Then результат status="invalid"
    And в сообщении встречается "архив"
```

```gherkin
Feature: KONB CherryFlash auto-promote на слот 1–2
  Phase B. Контекст: KONB CherryFlash (`partner_konb_library_001`) уже
  отбирает события Научной библиотеки. Если у конкретного события есть
  «просто попасть в видео» промо, то в выпуске KONB-CherryFlash оно
  поднимается до слота 1–2. Партнёрские dedicated-слотные кампании
  всегда побеждают это правило.

  Background:
    Given включён партнёрский трек partner_konb_library_001
    And событие #10 — научной библиотеки, попадает в кандидатов KONB CherryFlash

  Scenario: «Просто попасть в видео» поднимается до 1–2 в KONB CherryFlash
    Given у события #10 активна кампания с PromoActivity(
            surface='video_general',
            selection_policy='guaranteed_any_position')
    And у других событий-кандидатов нет first_slot/first_two_slots кампаний
    When собирается выпуск KONB CherryFlash
    Then событие #10 размещается в позиции 1 или 2

  Scenario: Партнёрский first_two_slots побеждает авто-промоут
    Given у события #11 активна кампания с PromoActivity(
            surface='video_general',
            selection_policy='first_two_slots',
            target_exposure_goal>0)
    And у события #10 активна кампания с PromoActivity(
            surface='video_general',
            selection_policy='guaranteed_any_position')
    When собирается выпуск KONB CherryFlash
    Then событие #11 занимает слот 1 и/или 2
    And событие #10 размещается в обычной позиции (не в 1–2)

  Scenario: Только видеоанонс KONB подвержен правилу
    Given событие #10 с guaranteed_any_position-кампанией
    When собирается выпуск profile_key='popular_review' (не KONB)
    Then событие #10 размещается по общим правилам resolve_video_promo_candidates
    And никакого специального подъёма в слот 1–2 нет

  Scenario: Один подъём на релиз
    Given у двух событий научной библиотеки активны guaranteed_any_position-кампании
    When собирается выпуск KONB CherryFlash
    Then в слотах 1–2 не более одного авто-промоут-события
    And второе событие размещается органически
```

```gherkin
Feature: Стартовая устойчивость (регрессия INC-2026-05-18)
  Опциональные модули партнёрского промо не должны валить старт бота.

  Scenario: create_app стартует при отсутствии partner_promo.py
    Given partner_promo.py отсутствует на диске
    When вызывается main_part2.create_app()
    Then приложение возвращается без исключения
    And в логах есть запись "partner_promo input sessions unavailable"
    And callback ppromo:start:1 отвечает alert "Промо-кампании временно недоступны"

  Scenario: create_app стартует со всеми модулями
    Given partner_promo.py и handlers/partner_promo_cmd.py присутствуют
    When вызывается main_part2.create_app()
    Then приложение возвращается без исключения
    And dispatcher имеет зарегистрированный обработчик callback "ppromo:*"
```

## 12. Связь с MVP

- Существующий `/promo <args>` (`handlers/promo_cmd.py`) — legacy-путь
  для админа: `report`, `seed80`, `add festival/event ...`, `pause/start
  /archive`, `priority`. Партнёрские кампании попадают в тот же список и
  `/promo report`.
- `/promo` без аргументов — новая точка входа в кнопочное меню для обеих
  ролей; легаси-команда остаётся доступной с аргументами.
- 🎬 на `/events` — единственная точка входа в FSM создания.
- Все campaign-уровневые ограничения (`priority`, `total_exposure_goal`,
  `daily_exposure_cap`) и резолвер `resolve_video_promo_candidates`
  переиспользуются без изменений; KONB-правило (§7) — отдельный слой в
  KONB-плече CherryFlash, phase B.

## 13. Тесты и интерфейсный dry-run

- Unit / integration: `tests/test_partner_promo.py` (12 кейсов) —
  создание кампании, кламп даты, режимы, отказ на прошедшем событии,
  round-trip `Organization`.
- UI surface: `tests/test_partner_promo_menu.py` (9 кейсов) — фильтрация
  кампаний по роли, кнопки карточки в состояниях active/paused, наличие
  админской строки приоритетов, текст и кнопки статистики.
- Интерфейсный dry-run под Тикун:
  `scripts/partner_promo_interface_dry_run.py` — рендер 15 кадров
  всего флоу без отправки в Telegram (используется как ручная
  визуальная проверка перед деплоем).
- Существующий `tests/test_promo.py` (9 кейсов) — продолжает зелёным.
- Стартовый smoke из `INC-2026-05-18`:
  `TELEGRAM_BOT_TOKEN=<fake> python -c "from main import create_app;
  create_app()"`.
