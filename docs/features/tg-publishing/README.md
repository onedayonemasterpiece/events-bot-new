# Telegram Event Publishing

Каноника для исходящих Telegram-публикаций отдельных событий после Smart Update.

## Event Posts

- После Smart Update событие получает редакционный пост в Telegram-канале `https://t.me/kldevents`.
- Telegram-публикация не заменяет VK, но не зависит от VK-публикации: для publish-eligible события VK (`vk_sync`) и Telegram (`tg_event_publish`) являются независимыми public surfaces. VK media/API/captcha failures must not block Telegram event announcements.
- Публикация запускается через `JobTask.tg_event_publish` и зависит только от Telegram-required prerequisites: `telegraph_build` и `tg_ics_post` при наличии валидного календарного времени. `vk_sync` не входит в `depends_on` для `tg_event_publish`.
- Если dependency ещё retry'ится с bounded `next_run_at`, ожидающий `tg_event_publish`/`tg_ics_post` не должен стареть до `expired`: он переносится к ближайшему retry dependency. Terminal Telegraph/ICS dependency failure остаётся blocker и требует requeue/repair; terminal VK failure не должен блокировать Telegram.
- Календарное время означает, что у события парсятся и `date`, и `time`; произвольные строки времени вроде `по расписанию`/`уточняется` не должны ставить `ics_publish`/`tg_ics_post` и не должны становиться dependency для `tg_event_publish`. Если такая задача уже была в очереди, она обязана завершиться как `skipped_invalid_schedule`, а не уходить в бесконечный retry.
- Повторный Smart Update/reimport должен переармировать `tg_event_publish` и `tg_ics_post` с текущим набором зависимостей, а не накапливать старые. Это важно для событий без времени: если старый импорт успел создать `tg_ics_post` и тот упал с `bad time`, свежий reimport обязан убрать зависимость `tg_ics_post:<event_id>`; старые зависимости `vk_sync:<event_id>` также должны исчезать из `tg_event_publish`.
- Если у события уже есть pending `tg_event_publish`, повторный Smart Update/reimport должен заменить его `next_run_at` на слот, рассчитанный для текущего цикла. Отложенные page rebuild jobs могут сохранять будущий слот, но Telegram event announcement не должен оставаться на завтрашнем stale slot, когда текущий publish window открыт.
- `telegraph_build` является обязательной dependency для Telegram event announcement и может включать несколько LLM-first шагов с fallback. Его runtime budget должен покрывать нормальный Telegraph render путь; slow-but-healthy render не должен помечаться `stale`, иначе `tg_event_publish` получает вечный dependency blocker.
- Runtime target по умолчанию: `@kldevents`; в production не нужно выставлять `TG_EVENT_CHANNEL=@kldevents` отдельным env. `TG_EVENT_CHANNEL` / `TG_EVENT_CHANNEL_ID` — только явный override для миграции или изолированной отладки; `ENABLE_TG_EVENT_PUBLISHING=0` выключает job handler без изменения Smart Update.
- Cancelled/postponed, silent и полностью прошедшие события не публикуются.
- Идемпотентность хранится на `event`: `tg_event_post_url`, `tg_event_post_id`, `tg_event_post_mode`, `tg_event_source_hash`. Хеш включает не только текстовый payload, но и deduped media signature; если у события позже появились изображения, старый text-only пост не считается актуальным. Если исходный event payload не изменился, повторный job ничего не публикует и не тратит новый LLM-запрос; если текст изменился, бот пытается отредактировать прежнее текстовое сообщение, а при смене режима `text`/`photo_caption` -> media group публикует новый media post и best-effort удаляет старое сообщение.
- Если Bot API даёт timeout/`Request timeout error` именно на операции отправки нового Telegram event post (`sendMessage`, `sendPhoto`, `sendMediaGroup`), результат считается неопределённым: Telegram мог уже принять сообщение, но ответ с `message_id` потерялся. Такой `tg_event_publish` не должен автоматически retry'иться, потому что retry может создать публичный дубль. Job остаётся в `error` с дальним `next_run_at`; оператор должен проверить канал через Telethon, удалить/сверить дубль или вручную reconcile/requeue.
- Чтобы не слать ночной поток уведомлений, `tg_event_publish` ставится в отложенный `JobOutbox` слот: по умолчанию не раньше `07:00` и не позже `23:00` локального `Europe/Kaliningrad`, с интервалом минимум `10` минут после последней queued/done Telegram event публикации. Runtime knobs: `TG_EVENT_PUBLISH_START_HOUR`, `TG_EVENT_PUBLISH_END_HOUR`, `TG_EVENT_PUBLISH_INTERVAL_MINUTES`.
- Spacing игнорирует `error` jobs и pending/running anchors дальше `TG_EVENT_PUBLISH_SPACING_HORIZON_HOURS` (по умолчанию `24`) от текущего времени: это защищает новые публикации от ручных cleanup-marker rows вроде `next_run_at=2036-...`. Планировщик выбирает ближайший свободный слот внутри publish window, а не слот после самого позднего pending anchor: вечерний или завтрашний backlog не должен оставлять дневную дыру в `@kldevents`, если после последней фактической публикации уже прошёл стандартный интервал. Если текущий локальный publish window уже закрыт и новый кандидат нормализован на завтрашнее `07:00`, stale anchors того же следующего дня, стоящие намного позже утреннего старта, тоже не должны сдвигать свежие импорты в вечерний кластер; нормальные соседние anchors продолжают задавать стандартный интервал.
- `tg_event_publish` различает новые объявления и edit/reconciliation jobs только для порядка очереди: no-post announcements идут раньше existing-post edit rows. Но execution spacing остаётся жёстким для каждого `tg_event_publish`, потому что existing-post job при смене режима `text`/`photo`/`album` тоже может отправить новый public message id; обход 10-минутного gate запрещён.
- Если один исходный пост/афиша (`event.source_post_url` или внешний `event_source.source_url`) породил несколько отдельных событий, `tg_event_publish` дополнительно разводит эти события минимум на `SAME_SOURCE_EVENT_PUBLISH_INTERVAL_HOURS` (default `12`) внутри `SAME_SOURCE_EVENT_PUBLISH_SPACING_HORIZON_HOURS` (default `168`). Обычная очередь при этом остаётся общей: стандартный 10-минутный spacing учитывает такие отложенные `JobOutbox` anchors, а source-specific правило только увеличивает разрыв между событиями из той же афиши.
- Пока Telegram-пост ждёт утренний слот, Smart Update-отчёты должны показывать строку `Посты: VK ... · TG ⏳`; после публикации `TG ⏳` заменяется ссылкой на пост в канале.

## Promo Campaign Surfaces

- `promo_activity.surface='tg_event_publish'` is an explicit campaign slot in a
  Telegram event-flow channel such as `@kldevents`. It is separate from the
  ordinary `JobTask.tg_event_publish` pipeline above: campaign slots may publish
  an extra full event post for selected targets and record
  `promo_exposure.publish_status='TG_PUBLISHED'`.
- The promo activity target channel is configured per activity through
  `config_json.target_chat`; it is not controlled by the global
  `TG_EVENT_CHANNEL` runtime setting.
- `promo_activity.surface='tg_repost'` forwards an already published source
  channel post, normally `@kldevents -> @kenigevents`. It uses
  `config_json.source_chat`, `config_json.target_chat`, active-window settings,
  and `dedup_hours`. If no source post exists yet (`event.tg_event_post_url` or
  a recent `tg_event_publish` exposure), the activity skips the tick instead of
  creating a new post in the daily/digest channel.
- `tg_repost` must preserve the channel-role split: event-flow channels can
  receive full event posts, while daily/digest channels receive only selected
  forwards or editorial daily blocks. A repost activity by itself must not
  change buttons on the source `@kldevents` post; button highlighting is
  controlled only by the separate `tg_button_highlight` marker activity.
- Compensation/manual repair publications in `@kldevents` must use the ordinary
  event-post publisher (`job_publish_tg_event_post` /
  `publish_tg_event_announcement`) or a fully equivalent path. A repair is not
  complete if it renders a promo-only body that drops the registration/ticket
  link or bypasses the post-publication premium emoji editor. For media groups,
  do not attach inline buttons; the registration/ticket CTA must be preserved
  as a link entity inside the caption/text.
- Canonical `tg_event_publish` schedules the Premium/custom-emoji editor after send/edit with delay/jitter. Do not run it synchronously in the publication lane: bulk catch-up can trigger Telegram `FloodWait` and must not remove the 10-minute event-post cadence.

## Формат

- Telegram-пост не наследует VK-капс: заголовок остаётся нормальным регистром и рендерится как `<b>...</b>`.
- Explicit promo activity posts (`promo_activity.surface='tg_event_publish'`) publish the full event body, but Markdown service markup from Smart Update descriptions must be normalized before send: section headings such as `### О спикере` render as bold Telegram HTML headings, bullets render as `•`, and raw `###`, `**`, or `*` markers must not be visible in the public post.
- Explicit promo activity posts with `event.photo_urls` must not silently degrade to text-only output because the full body is longer than Telegram's media caption limit. When the full promo body does not fit a media caption, the publisher sends the image/album with a concise caption and `Подробнее` button instead of dropping media; long bullet-list bodies are summarized in the caption rather than dumped as raw thesis lists.
- Инфоблок содержит дату/время, площадку/адрес/город, Пушкинскую карту и билетную строку. Если у события есть `end_date` позже `date`, дата в инфоблоке и календарной кнопке выводится диапазоном (`12–15 июня`, `30 июня–2 июля`), чтобы long-running события не выглядели как однодневные стартовые посты.
- Smart Update description не публикуется целиком: для каждого поста строится короткий Telegram intro через `TG_EVENT_REWRITE_MODEL` (по умолчанию `gemma-4-31b-it`; Lite допустим только явным override). Этот hook вызывается на каждую попытку публикации, включая retries, поэтому shared Gemini Lite lane зарезервирован для ограниченных semantic contracts Smart Update, а не projection rewriting. Intro может быть цепляющим вопросом, коротким полезным абзацем или friendly-вступлением вроде `Друзья, ...`; вопрос не обязателен, если событие утилитарное/сервисное и важнее объяснить пользу. Writer не должен начинать серией одинаковых шаблонов вроде `Хотите...`, `Готовы...`, `Что здесь стоит увидеть?`: если вопрос уместен, он должен быть конкретным к событию. Текст не должен повторять дату, место, цену, ссылки и хештеги из инфоблока, а собственные имена и названия должен брать без изменения написания. При ошибке/невалидном ответе используется уже существующий source-grounded fallback из canonical `short_description`/`search_digest`, без блокировки публикации.
- Если событие покрыто любой активной promo-кампанией (`promo_campaign` + `promo_target`), Telegram intro получает promo-режим по умолчанию для канала: до `500` символов вместо обычных `330`, 1-3 предложения и prompt на более богатый выбор 2-3 конкретных причин/деталей без рекламной воды. Для такого режима LLM получает объединённый editorial/source контекст (`description`, `short_description`, `search_digest`, `source_text`) после тех же guardrail-проверок, а не только первый доступный текст. Для этого не нужна отдельная `promo_activity.surface='tg_event_publish'`, channel target или новый env; promo-флаг входит в `tg_event_source_hash`, поэтому включение/выключение кампании переармирует пост.
- Для пунктов приёма, сбора, переработки, волонтёрских и других городских полезных акций intro должен объяснять практическую пользу, действие и важные ограничения. Если `event.description` явно противоречит utility/source text (например обещает музыкальные номера, театральную программу или билеты для события `Приём шин`), Telegram publisher использует `event.source_text` как более надёжный источник для intro.
- Та же конфликтная `event.description` не участвует в подборе type-хештегов Telegram-поста, чтобы в utility/service caption не попадали теги вроде `#спектакль` из галлюцинированного описания.
- Строка локации выводит город как хештег (`📍 Площадка, адрес, #Калининград`), поэтому нижняя hashtag line больше не дублирует городской тег и оставляет дату, компактный городской тег афиши (`#афишакалининград`), до нескольких ключевых type-тегов (`#лекция`, `#спектакль`, `#показ`, `#концерт`, ...), фестивальный тег.
- Обычный non-promo footer сохраняет текстовую ссылку `🔎 Подробнее`, а
  после неё в той же строке добавляет постоянные ссылки `Max` и `Вконтакте`
  (`https://max.ru/channel_kenigevents`,
  `https://vk.ru/im/channels/-239844596`) с визуальным отступом в 12 пробелов: `🔎 Подробнее            Max · Вконтакте`.
  Inline-кнопка `✨ Подробнее` управляется отдельной promo-активностью
  `promo_activity.surface='tg_button_highlight'` /
  `profile_key='kldevents:details-button'`: если активность включена для
  кампании, покрывающей событие, ссылка `Подробнее` уходит из текста в кнопку;
  если активности нет или она выключена, `🔎 Подробнее` остаётся текстовой
  ссылкой даже при promo-intro. Это позволяет оставить богатый promo intro от
  кампании, но отключить громоздкую кнопку для широких механик вроде
  `tg_repost`. `Подписаться` не добавляется в event captions. Social footer
  `Max · Вконтакте` подавляется только для явных button-highlight постов, где
  `Подробнее` уже вынесен в inline-кнопку `✨ Подробнее`; широкий
  `promo_highlight`/богатый intro сам по себе не убирает footer-ссылки.
- Если у события есть календарь, пост получает inline-кнопку вида `📅 <дата или диапазон> <время> · Добавить в календарь`. Публичный CTA должен вести на открываемую аудиторией ссылку: публичный Telegram calendar-post URL (`https://t.me/<username>/<id>`) предпочтительнее, но приватные внутренние `https://t.me/c/...` ссылки asset-канала не считаются публичными и заменяются на `event.ics_url`. Новые `tg_ics_post` записи сохраняют username-ссылку, если asset-канал зарегистрирован с `username`.
- Source posts that look like third-party ticket giveaways (`розыгрыш` / `разыгрываем` / `выиграть` + `билет` / `пригласительный`) are fallback content for managed Telegram/VK event publication only when Smart Update has not produced substantial cleaned non-giveaway event copy. If Smart Update extracted a real event body after removing raffle mechanics, the event still receives the normal Telegram event post and managed VK dependency even when the raw source mentioned a giveaway.
- Для бесплатных Telegram event posts в hashtag line добавляется `#бесплатно`, чтобы событие оставалось поисковым даже если post-publication premium emoji editor заменит видимую строку `🟡 Бесплатно...` на custom emoji label.
- Хештеги в Telegram event posts должны быть навигационными и поисковыми, а
  не пересказом названия события. Канонический набор: дата (`#1июля`,
  `#1_июля`), городской афишный тег (`#афишакалининград`), до трёх коротких
  type-тегов (`#лекция`, `#концерт`, ...), `#бесплатно` для бесплатных
  событий и только короткий фестивальный/брендовый тег. Presentation guardrail:
  один tag body после `#` — максимум 28 символов, строка — максимум 8 тегов и
  около 140 видимых символов. Длинные слитные title-like slug tags вроде
  `#ПоодёжкевстречаютНародныйкостюмтрадицииисмыслы` не публикуются.
- Если структурной цены, билетной ссылки, sold-out статуса и явного признака бесплатности нет, билетная строка не выводится.
- Telegram использует исходный `event.ticket_link`. `event.vk_ticket_short_url` — VK-only analytics ссылка и не должна попадать ни в текст, ни в entities/buttons Telegram-поста.
- Если `event.ticket_link` содержит телефон (`tel:+...`) или в narrative/body есть телефонный контакт, Telegram-пост должен делать его кликабельным явной Telegram `phone_number` entity при отправке/редактировании caption/text. Для самого Telegram caption видимый номер нормализуется в компактный `+74012463635`, потому что Bot API принимает `phone_number` entity только на таком phone-like payload; человекочитаемая группировка сохраняется в промежуточной HTML-сборке/Telegraph, но не должна ломать actionability в Telegram. Уже существующие HTML-ссылки не перелинковываются повторно.
- Smart Update не должен заменять уже известный canonical `event.ticket_link` на VK shortener URL (`vk.cc`, `vk.link`, `go.vk.com`, `l.vk.com`). Short URL допустим в `event.ticket_link` только если источник не дал более прямой ticket/registration URL; последующий parser/site URL обязан заменить short URL.

## Медиа

- Перед отправкой в Bot API publisher берёт уже материализованные Smart Update URL из `event.photo_urls`, скачивает их и отправляет как `BufferedInputFile`. Remote URL strings не являются штатным способом доставки медиа в Telegram event posts.
- Перед публикацией `event.photo_urls` должны быть дедуплицированы на уровне Smart Update/event storage: managed storage URL и raw CDN URL одного изображения не должны одновременно попадать в Telegram. Telegraph rebuild дополнительно best-effort rehydrate'ит Telegram/VK source media даже для одного source row, если event был сохранён без картинок; это нужно до TG/VK fanout, потому что VK guard fail-closed блокирует Telegram-origin event без медиа. Сам TG publisher дополнительно схлопывает near-duplicate Supabase/Yandex `p/dh16/...` media URLs по близкому perceptual hash и после этого берёт максимум 9 изображений.
- Если late media rehydration всё же произошёл после первого public fanout (например, video popular-review восстановил фото из исходного VK-поста), URL сначала materialize'ятся в managed storage, а затем обычные `tg_event_publish`/`vk_sync` автоматически re-arm'ятся. Сохранить `event.photo_urls`, оставив старые text-only Telegram/VK posts, запрещено.
- Если у события одно выбранное изображение, канонический пост — `sendPhoto` с uploaded file, caption, кнопкой календаря и всеми ссылками в одном сообщении.
- Если выбранных изображений несколько, канонический пост — `sendMediaGroup` с uploaded files: caption ставится на первое изображение, а календарь добавляется как текстовая ссылка в caption, потому что Telegram Bot API не поддерживает inline-кнопки у media group.
- Если у события есть выбранные `photo_urls`, text-only публикация не считается успешной деградацией: ошибка materialize/upload должна оставить `tg_event_publish` в retry/error и быть расследована. Text post допустим только для событий без изображений.
- Caption всегда удерживается в `1000` видимых символов: если LLM hook или инфоблок всё ещё длинные, deterministic safety-net режет только narrative body; заголовок, инфоблок, хештеги и footer links сохраняются.

## Проверки

- Unit: `tests/test_tg_event_publish.py`.
- Live E2E для фичи удобно гонять через `/vk_auto_import --limit=1`: событие должно появиться в `@kldevents` captioned media-постом с hook, публично открываемой календарной кнопкой/ссылкой, датным/городским/type/festival hashtag line, `Подробнее` на Telegraph, footer links и дедуплицированными изображениями; тот же Smart Update должен иметь рабочий managed VK-пост в `klgdevents`, подтверждённый не только DB URL, но и `wall.getById`. При сравнении каналов последние managed Smart Update события должны идти в одинаковом порядке; допустимы только разные времена публикации из-за VK/Telegram quiet-window spacing.
