# Post Metrics & Popularity (TG/VK + KenigEvents)

Фича консолидирует сбор статистики постов (`views/likes`, а также доступные `comments/reposts`) и подсветку “популярности” единым алгоритмом **для Telegram Monitoring и VK Auto Queue**. Release scope additionally requires first-party KenigEvents views/likes/shares to join the same event-level read contract; this final consolidation is not implemented yet.

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

This is currently one source-metrics implementation point, but not yet one event-engagement read point: `/popular_posts`, daily audience labels, video selection and static counters still have separate aggregation paths. The required shared source+site function, versioned output and storage contract are defined in [Consolidated event engagement](consolidated-event-engagement.md).

## Release consolidation requirement

- `/popular_posts`, daily popularity, CherryFlash/`/v`, static counter export and future rankers consume one canonical batch function for event engagement.
- The result retains TG/VK/site components and freshness while exposing compatible totals for views, likes and shares; “views” are reach observations, not unique people.
- A source post contributes once from its latest valid maturity snapshot, not once per `age_day` or duplicate mapping.
- Site likes are current state; shares and valid views are idempotently compacted and bot/reload/preview guarded.
- Fly SQLite remains source-metric owner and personalization Supabase remains first-party-state owner. Consolidation is a compact projection/read model, not a cross-database transaction or a new raw-data lake.
- Storage stays ecological: one current event aggregate, bounded source buckets, short strong-action evidence, compacted views, CDN manifest and optional de-identified YDB daily history with TTL.

## Данные (SQLite)

Снапшоты метрик хранятся отдельно для Telegram и VK:

- `telegram_post_metric` (ключ `(source_id, message_id, age_day)`): `views`, `likes`, `comments` (если у поста есть открытый discussion/replies count), `reactions_json`;
- `vk_post_metric` (ключ `(group_id, post_id, age_day)`): `views`, `likes`, `comments`, `reposts`.

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
  - если Telethon отдаёт `message.replies.replies`, счётчик комментариев сохраняется вместе с `views/likes`.
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

Before public release, the source+site event aggregate and shared consumer function move out of “future” into the mandatory [release acceptance contract](consolidated-event-engagement.md#release-acceptance).
