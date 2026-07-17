# Post Metrics & Popularity (TG/VK)

Фича консолидирует сбор статистики постов (`views/likes`, а также доступные `comments/reposts`) и подсветку “популярности” единым алгоритмом **для Telegram Monitoring и VK Auto Queue**.

Это **не про “посты” как сущность**, а про то, как статистика постов становится сигналом для:

- маркировки событий в отчётах Smart Update (`⭐/👍` уровни);
- подписи популярных событий в секции ежедневного анонса `ДОБАВИЛИ В АНОНС`;
- будущих фич (см. ниже): автопубликации анонсов и приоритизации видеоанонсов.

## Канонический код (одна точка)

Единая реализация живёт в `source_parsing/post_metrics.py`:

- сохранение снапшотов: `upsert_telegram_post_metric`, `upsert_vk_post_metric`;
- вычисление бейзлайна: `load_telegram_popularity_baseline`, `load_vk_popularity_baseline`;
- вычисление маркеров: `popularity_marks`.

Обе пайплайны (TG/VK) используют **тот же код** и одинаковые ENV параметры.

## Данные (SQLite)

Снапшоты метрик хранятся отдельно для Telegram и VK:

- `telegram_post_metric` (ключ `(source_id, message_id, age_day)`): `views`, `likes`, `comments` (если у поста есть открытый discussion/replies count), `forwards`, `reactions_json`;
- `vk_post_metric` (ключ `(group_id, post_id, age_day)`): `views`, `likes`, `comments`, `reposts`.

Для статической страницы `/populyarnoe/` дополнительно используется компактная
`social_metric_snapshot` с четырьмя возрастными точками `1h`, `6h`, `24h`,
`72h`. Она не заменяет legacy-таблицы: суточные строки продолжают питать
операторские отчёты и медианные baseline, а часовые точки нужны только для
честной оценки динамики. `publication_kind` явно различает
`event_announcement`, `event_forward` и `external_event_source`.

## Пакетный сбор для статического «Популярного»

Канонический production runner: `social_metrics_kaggle.py::run_social_metrics_kaggle_batch`.

- Fly выполняет только DB-планирование точных post/message ID, запуск одного
  приватного Kaggle CPU kernel, строгую проверку результата и импорт. Все
  Telegram/VK API-чтения выполняются в
  `kaggle/SocialMetricsCollector/social_metrics_collector.py`; отдельные jobs на
  каждый пост не создаются. Manifest содержит только точные публикации, а
  Telegram читает их через `get_messages(ids=...)` без сканирования истории.
- Запросы группируются по платформе и издателю. VK `wall.getById` вызывается
  чанками максимум по 100 ID, после чего весь чанк записывается одной SQLite
  транзакцией.
- Для `klgdevents` Fly экспортирует только компактные unresolved-кандидаты, а
  Kaggle одним `wall.getById` batch и не более чем одним общим bounded wall-scan
  разрешает postponed ID в live ID. Fly повторно проверяет title/date/time/place
  evidence существующим строгим matcher перед `event_publication` и сразу
  сохраняет counters уже полученного VK item. Локальных provider reads нет.
- Вторая собственная VK-группа `231828790` подключена независимо от перегруженных
  `VK_*_GROUP_ID`: учитываются только точные single-event ledgers
  `event.vk_repost_url` и `promo_exposure(surface=vk_repost,
  publish_status=PUBLISHED_MAIN)`. Daily, дайджесты, видео, stories и фестивальные
  агрегаты исключены структурно. Точные reposts последних 90 дней подхватываются
  rolling backfill без сканирования всей стены.
- При пропущенном окне один поздний counter не копируется в несколько прошлых
  точек: самый свежий due-бакет сохраняется, более ранние получают
  `skipped_late`.
- Отсутствующее поле API хранится как `NULL`; подтверждённый ноль — как `0`.
  Сетевые ошибки получают `status=error` и повторяются следующим batch-run.
- Telegram collector по умолчанию выключен. Он принимает только отдельную
  role-scoped сессию `TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR` и никогда не заимствует
  `TELEGRAM_AUTH_BUNDLE_E2E`, `TELEGRAM_AUTH_BUNDLE_S22` или `TELEGRAM_SESSION`.
  Общими могут быть только app credentials `TG_API_ID/TG_API_HASH`, не session.
- Чтение Telethon идёт последовательно, батчами до 50 ID по умолчанию, со
  случайными bounded-паузами перед подключением, между запросами и каналами;
  никаких read receipts, typing actions или фиктивных взаимодействий нет.
- В `@kldevents` учитываются только точные `event.tg_event_post_*`. В
  `@kenigevents` — только event-forward записи, подтверждённые
  `promo_exposure(surface=tg_repost)` или `poll_repost_run.forwarded_message_id`.
  Дайджесты, опросы, ответы к ним и произвольные сообщения структурно исключены,
  без классификации по словам/хештегам.
- Внешние Telegram-источники по умолчанию остаются на существующем monitoring
  pipeline (`SOCIAL_METRICS_TG_INCLUDE_EXTERNAL=0`), чтобы новая human-like
  сессия не обходила десятки чужих каналов. Отдельное включение — только после
  оценки runtime-budget и необходимости часовых бакетов.

Флаги запуска:

- `ENABLE_SOCIAL_METRICS_KAGGLE=1` — зарегистрировать единый remote interval-job;
- `SOCIAL_METRICS_BATCH_INTERVAL_MINUTES=30`;
- `SOCIAL_METRICS_KAGGLE_TIMEOUT_SECONDS=1800`;
- `SOCIAL_METRICS_VK_OFFICIAL_GROUP_ID=231828790`;
- `SOCIAL_METRICS_VK_RESOLVE_COOLDOWN_HOURS=6`,
  `SOCIAL_METRICS_VK_RESOLVE_MAX_CANDIDATES=500`,
  `SOCIAL_METRICS_VK_WALL_SCAN_LIMIT=1000`;
- `TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR` — единственная допустимая user-session;
- `SOCIAL_METRICS_TG_STARTUP_DELAY_SECONDS=4,12`,
  `SOCIAL_METRICS_TG_BETWEEN_REQUESTS_SECONDS=2,5`,
  `SOCIAL_METRICS_TG_BETWEEN_CHANNELS_SECONDS=5,15`;
- `SOCIAL_METRICS_TG_FLOOD_SLEEP_SECONDS=60` — короткий FloodWait можно
  переждать внутри job, длинный завершает попытку и повторяется следующим
  interval-run, не удерживая role-scoped session на много минут.

Kaggle получает bundle и provider tokens только в раздельных приватных
encrypted/key datasets. Runtime обязан получить status leases
`job:social_metrics_batch` и
`telegram_session:telegram_auth_bundle_check_popular` до расшифровки и
подключения; fallback на E2E/S22/`TELEGRAM_SESSION` запрещён. Каждый
30-минутный слот имеет детерминированный `run_id` и атомарно создаётся ровно один
раз. Результат обязан покрыть все manifest targets и resolver candidates, иначе
импорт отклоняется. Временные datasets удаляются и при success, и при ошибке;
orchestration/import failure переводит ledger в terminal `error`.

Static exporter добавляет к событию объяснимые reason codes:
`fast_growth`, `frequently_shared`, `discussed`, `multi_source`. Лента остаётся
единой и ограниченной 20 событиями; сложные главы и ML-скоринг в MVP не входят.
Оба Telegram-канала и обе VK-группы считаются одной owned-family: для raw totals
и reason thresholds берётся component-wise максимум. Внутренний repost может
усилить сигнал, но не удваивает аудиторию и не создаёт ложный `multi_source`.

`age_day` означает “сколько суток прошло с публикации”:

- `0` — первые 24 часа;
- `1` — вторые 24 часа;
- `2` — третьи 24 часа (по умолчанию).

Метрики сохраняются только для `age_day <= POST_POPULARITY_MAX_AGE_DAY`, чтобы рост БД был ограничен.

Снапшоты очищаются job’ом `post_metrics_cleanup` (retention по умолчанию = `POST_POPULARITY_HORIZON_DAYS`).

## Бейзлайн (медианы)

Бейзлайн считается как **медиана** за последние `POST_POPULARITY_HORIZON_DAYS` дней отдельно:

- для каждого источника (TG: `source_id`, VK: `group_id`);
- для каждого `age_day` (с fallback на `age_day<=POST_POPULARITY_MAX_AGE_DAY`, если выборка мала).

Важно: в расчёт **попадают только посты, которые дали события**, чтобы не смешивать с “контентом без событий”:

- TG: `telegram_scanned_message.events_extracted > 0`
- VK: пост должен иметь связь с импортом через `vk_inbox_import_event`.

Минимальная выборка для маркеров управляется `POST_POPULARITY_MIN_SAMPLE` (в dev/test по умолчанию низкая, чтобы маркеры появлялись уже после первого прогона).

## Маркеры популярности (⭐/👍) и “сверх‑популярность”

Вывод: строка маркеров добавляется **к событию** в отчёте Smart Update и в блок “Популярные посты”.

- `⭐` — просмотры выше бейзлайна (медиана) внутри источника;
- `👍` — лайки выше бейзлайна внутри источника.

Маркеры поддерживают **уровни** (сверх‑популярность), например: `⭐⭐⭐` / `👍👍`.

Алгоритм:

1. Порог уровня 1: `value > median * POST_POPULARITY_*_MULT`.
2. Каждый следующий уровень требует превышения ещё на `POST_POPULARITY_*_STEP * median` сверх порога.
3. Уровни ограничены `POST_POPULARITY_MAX_LEVEL`.

ENV (общие для TG/VK):

- `POST_POPULARITY_HORIZON_DAYS` (default `90`)
- `POST_POPULARITY_MIN_SAMPLE` (default `2` в коде; можно поднять на проде)
- `POST_POPULARITY_MAX_AGE_DAY` (default `2`; для “зрелого” `/popular_posts` окна 7 суток поднимите хотя бы до `6`)
- `POST_POPULARITY_VIEWS_MULT` (default `1.0`)
- `POST_POPULARITY_LIKES_MULT` (default `1.0`)
- `POST_POPULARITY_MAX_LEVEL` (default `4`)
- `POST_POPULARITY_VIEWS_STEP` (default `0.5`)
- `POST_POPULARITY_LIKES_STEP` (default `0.5`)
- `POST_METRICS_RETENTION_DAYS` (default = `POST_POPULARITY_HORIZON_DAYS`)

## Поверхности в UI/отчётах

- Telegram Monitoring (`/tg`):
  - `🔥 Популярные посты` в конце импорта;
  - маркеры добавляются в `Smart Update (детали событий)` перед названием события;
  - если Telethon отдаёт `message.replies.replies`, счётчик комментариев сохраняется вместе с `views/likes`;
  - batch collector также сохраняет Telegram `forwards`, когда включена его
    отдельная production-сессия.
  - `/tg` → «📋 Список источников»: показывает per-channel медианы `views/likes` за окно `POST_POPULARITY_HORIZON_DAYS` и покрытие в сутках (`days/N`), чтобы оператор мог сравнить свежий пост с бейзлайном.
- VK Auto Queue (`/vk_auto_import`):
  - маркеры добавляются перед названием события в унифицированном Smart Update отчёте.
- Отбор событий по реакции аудитории (superadmin):
  - `/popular_posts [N]` — ТОП постов (TG/VK), где `views` или `likes` выше медианы внутри своего источника; выводит ссылку на исходник, созданные события (Telegraph + `id`) и цифры `median vs post`, плюс диагностическую строку по размеру выборки/скипам.
  - В отчёте остаются только события, которые идут сегодня или позже: записи, связанные только с уже завершившимися событиями, скрываются. Для многодневных событий используется `end_date`, если он заполнен.
  - Окна отчёта: `7 суток`, `3 суток`, `24 часа`.
  - Для окон `7 суток` и `3 суток` отчёт предпочитает “зрелые” снапшоты (`age_day=6` / `age_day=2`), но если их ещё нет, использует последний доступный снапшот `age_day<=target`, чтобы окно не было пустым.
  - Если нужен именно “полный” 7-дневный бакет, поднимите `POST_POPULARITY_MAX_AGE_DAY` хотя бы до `6`; для Telegram дополнительно нужен scan/rescan horizon не короче 7 суток (`TG_MONITORING_DAYS_BACK>=7`).
  - Диагностика в конце блока дополнительно показывает, сколько постов (после фильтров) оказалось выше медианы по `views/likes/оба` — это помогает понять случаи, когда “популярных по обоим” почти нет.
  - В диагностической строке блока без результатов используется HTML-безопасная запись `skip(&lt;=median)`, чтобы отчёт не падал в Telegram parse-mode.

- Ежедневный анонс (`build_daily_posts`):
  - в секции `ДОБАВИЛИ В АНОНС` популярные события получают дополнительную строку с кастомными emoji `❤️`/`🔂` из `@kenigevents adaptive pack` (`❤️=5339188899241570417`, `🔂=5336998942661975661`);
  - расчёт идёт **по сумме всех доступных distinct source posts события**: Telegram + VK, а не по одному “лучшему” VK-посту;
  - score: `likes_sum + 5 * reposts_sum`; базовый порог `DAILY_AUDIENCE_MIN_SCORE=20`;
  - если событий с label меньше `DAILY_AUDIENCE_MIN_SHARE` (default `0.15`) от секции, daily снижает только свой порог до `DAILY_AUDIENCE_RELAXED_MIN_SCORE=8` и добирает следующие лучшие строки до target;
  - верхняя граница — `DAILY_AUDIENCE_MAX_SHARE` (default `0.20`) от секции, чтобы daily не раздувался;
  - ранжирование: `score`, затем `reposts`, `likes`, `comments`, `views`.

## Будущее (roadmap, кратко)

Эта фича задумана как “база сигналов” для выбора событий по реакции аудитории.

Планируемые потребители сигналов:

1. **Автопубликация анонсов** в канал, куда публикуются `/daily`.
   - Вход: события, которые были импортированы из постов источников.
   - Сигналы: уровни `⭐/👍` относительно своего источника (пер‑канальная нормализация).
2. **Приоритизация событий для видеоанонсов** (`/v`).
   - Сигналы: уровни `⭐/👍`, плюс возможные будущие агрегаты по нескольким источникам одного события.

Важно: статистика “постов” сама по себе не цель. Цель: **маркировать и ранжировать события**, а пост‑метрики являются входным сигналом.
