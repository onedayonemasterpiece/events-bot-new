# Telegram Event Publishing

Каноника для исходящих Telegram-публикаций отдельных событий после Smart Update.

## Event Posts

- После Smart Update событие получает редакционный пост в Telegram-канале `https://t.me/kldevents`.
- Публикация запускается через `JobTask.tg_event_publish` и зависит от `telegraph_build`, чтобы событие уже прошло общий Telegraph-путь.
- Runtime target по умолчанию: `@kldevents`. Его можно переопределить через `TG_EVENT_CHANNEL` или `TG_EVENT_CHANNEL_ID`; `ENABLE_TG_EVENT_PUBLISHING=0` выключает job handler без изменения Smart Update.
- Cancelled/postponed, silent и полностью прошедшие события не публикуются.
- Идемпотентность хранится на `event`: `tg_event_post_url`, `tg_event_post_id`, `tg_event_post_mode`, `tg_event_source_hash`. Если исходный event payload не изменился, повторный job ничего не публикует и не тратит новый LLM-запрос; если текст изменился, бот пытается отредактировать прежнее текстовое сообщение.

## Формат

- Telegram-пост не наследует VK-капс: заголовок остаётся нормальным регистром и рендерится как `<b>...</b>`.
- Инфоблок содержит дату/время, площадку/адрес/город, Пушкинскую карту и билетную строку.
- Smart Update description не публикуется целиком: для каждого поста строится короткий Telegram hook через `TG_EVENT_REWRITE_MODEL` (по умолчанию `gemini-3.1-flash-lite`). Hook должен начинаться с цепляющего вопроса и не повторять дату, место, цену, ссылки и хештеги из инфоблока.
- Внизу добавляется сдержанная hashtag line только с датой и фестивалем, без базового VK-набора и без городского hashtag storm, плюс одна строка ссылок:
  `Подписаться` -> `https://t.me/kldevents`, `Вконтакте` -> `https://vk.com/klgdevents`.
- Если у события есть публичный `ics_url`, текстовое сообщение получает inline-кнопку `Добавить в календарь`.

## Медиа

- Telegram Bot API не поддерживает inline-кнопки у `sendMediaGroup`, поэтому канонический пост — отдельное текстовое сообщение с кнопкой, а все `event.photo_urls` отправляются следом media groups пачками до 10 фото.
- Текстовое сообщение всегда удерживается в `1000` видимых символов: если LLM hook или инфоблок всё ещё длинные, deterministic safety-net режет только narrative body; заголовок, инфоблок, хештеги и footer links сохраняются.

## Проверки

- Unit: `tests/test_tg_event_publish.py`.
- Live E2E для фичи удобно гонять через `/vk_auto_import --limit=1`: событие должно появиться в `@kldevents` текстовым постом с hook, кнопкой календаря при наличии `ics_url`, датным/фестивальным hashtag line, footer links и всеми доступными изображениями после текста.
