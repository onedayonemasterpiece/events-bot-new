# Telegram Event Publishing

Каноника для исходящих Telegram-публикаций отдельных событий после Smart Update.

## Event Posts

- После Smart Update событие получает редакционный пост в Telegram-канале `https://t.me/kldevents`.
- Telegram-публикация не заменяет VK: штатный Smart Update/import считается успешным только когда для publish-eligible события есть оба видимых surface — managed VK-пост в `https://vk.com/klgdevents` и Telegram-пост в `https://t.me/kldevents`. Для событий, которым нужен managed VK surface, `tg_event_publish` зависит от `vk_sync`; если VK-публикация в `pending`/`running`/`error` retry, Telegram-пост ждёт, чтобы каналы не расходились по набору событий и порядку Smart Update.
- Публикация запускается через `JobTask.tg_event_publish` и зависит от `telegraph_build`, `tg_ics_post` при наличии календарного времени, и `vk_sync` при отсутствии уже подтверждённого managed VK-поста.
- Runtime target по умолчанию: `@kldevents`; в production не нужно выставлять `TG_EVENT_CHANNEL=@kldevents` отдельным env. `TG_EVENT_CHANNEL` / `TG_EVENT_CHANNEL_ID` — только явный override для миграции или изолированной отладки; `ENABLE_TG_EVENT_PUBLISHING=0` выключает job handler без изменения Smart Update.
- Cancelled/postponed, silent и полностью прошедшие события не публикуются.
- Идемпотентность хранится на `event`: `tg_event_post_url`, `tg_event_post_id`, `tg_event_post_mode`, `tg_event_source_hash`. Хеш включает не только текстовый payload, но и deduped media signature; если у события позже появились изображения, старый text-only пост не считается актуальным. Если исходный event payload не изменился, повторный job ничего не публикует и не тратит новый LLM-запрос; если текст изменился, бот пытается отредактировать прежнее текстовое сообщение, а при смене режима `text`/`photo_caption` -> media group публикует новый media post и best-effort удаляет старое сообщение.
- Чтобы не слать ночной поток уведомлений, `tg_event_publish` ставится в отложенный `JobOutbox` слот: по умолчанию не раньше `07:00` и не позже `23:00` локального `Europe/Kaliningrad`, с интервалом минимум `10` минут после последней queued/done Telegram event публикации. Runtime knobs: `TG_EVENT_PUBLISH_START_HOUR`, `TG_EVENT_PUBLISH_END_HOUR`, `TG_EVENT_PUBLISH_INTERVAL_MINUTES`.
- Пока Telegram-пост ждёт утренний слот, Smart Update-отчёты должны показывать строку `Посты: VK ... · TG ⏳`; после публикации `TG ⏳` заменяется ссылкой на пост в канале.

## Формат

- Telegram-пост не наследует VK-капс: заголовок остаётся нормальным регистром и рендерится как `<b>...</b>`.
- Инфоблок содержит дату/время, площадку/адрес/город, Пушкинскую карту и билетную строку.
- Smart Update description не публикуется целиком: для каждого поста строится короткий Telegram hook через `TG_EVENT_REWRITE_MODEL` (по умолчанию `gemini-3.1-flash-lite`). Hook должен начинаться с цепляющего вопроса, не повторять дату, место, цену, ссылки и хештеги из инфоблока, а собственные имена и названия брать без изменения написания.
- Внизу добавляется сдержанная hashtag line: дата, город (`#Калининград`), компактный городской тег афиши (`#афишакалининград`), до нескольких ключевых type-тегов (`#лекция`, `#спектакль`, `#показ`, `#концерт`, ...), фестивальный тег, плюс одна строка ссылок:
  `Подробнее` -> Telegraph-пост события, `Подписаться` -> `https://t.me/+MrSeuZSHv3VjMThi`, `Вконтакте` -> `https://vk.com/klgdevents`.
- Если у события есть `ics_post_url`, пост получает inline-кнопку вида `📅 <дата> <время> · Добавить в календарь`, ведущую на Telegram-пост календаря (`https://t.me/kenigeventscalendar` / private `t.me/c/...` link), а не на сырой `.ics` файл.
- Если структурной цены, билетной ссылки, sold-out статуса и явного признака бесплатности нет, билетная строка не выводится.

## Медиа

- Если у события одно уникальное изображение, канонический пост — `sendPhoto` с caption, кнопкой календаря и всеми ссылками в одном сообщении.
- Если уникальных изображений несколько, канонический пост — `sendMediaGroup`: caption ставится на первое изображение, а календарь добавляется как текстовая ссылка в caption, потому что Telegram Bot API не поддерживает inline-кнопки у media group.
- Caption всегда удерживается в `1000` видимых символов: если LLM hook или инфоблок всё ещё длинные, deterministic safety-net режет только narrative body; заголовок, инфоблок, хештеги и footer links сохраняются.
- Перед публикацией `event.photo_urls` должны быть дедуплицированы на уровне Smart Update/event storage: managed storage URL и raw CDN URL одного изображения не должны одновременно попадать в Telegram. Telegraph rebuild дополнительно best-effort rehydrate'ит Telegram/VK source media даже для одного source row, если event был сохранён без картинок; это нужно до TG/VK fanout, потому что VK guard fail-closed блокирует Telegram-origin event без медиа. Сам TG publisher дополнительно схлопывает near-duplicate Supabase `p/dh16/...` media URLs по близкому perceptual hash, чтобы уже отрендеренная Telegraph-страница с одним постером не превращалась в альбом из визуальных дублей.

## Проверки

- Unit: `tests/test_tg_event_publish.py`.
- Live E2E для фичи удобно гонять через `/vk_auto_import --limit=1`: событие должно появиться в `@kldevents` captioned media-постом с hook, календарной кнопкой/ссылкой на `ics_post_url`, датным/городским/type/festival hashtag line, `Подробнее` на Telegraph, footer links и дедуплицированными изображениями; тот же Smart Update должен иметь рабочий managed VK-пост в `klgdevents`, подтверждённый не только DB URL, но и `wall.getById`. При сравнении каналов последние managed Smart Update события должны идти в одинаковом порядке; допустимы только разные времена публикации из-за VK/Telegram quiet-window spacing.
