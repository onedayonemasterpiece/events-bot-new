# Telegram Business Stories

Цель: безопасно публиковать сторис от имени подключённого Telegram Business аккаунта через Bot API, не сохраняя `business_connection_id` и user id в открытом виде.

## Runtime contract

- Production webhook обязан подписываться на update type `business_connection`.
- Для уже подключённых аккаунтов webhook также подписан на `business_message` и `edited_business_message`: если Telegram больше не присылает старый `business_connection`, бот восстанавливает connection через `business_connection_id` из business-message update и сразу пишет его в encrypted cache.
- Канонический список webhook updates живёт в `telegram_business.WEBHOOK_ALLOWED_UPDATES`; startup не должен собирать его вручную.
- При получении `business_connection` бот:
  - проверяет флаги подключения и права `can_manage_stories`;
  - пишет в логи только short-hash connection/user id;
  - сохраняет encrypted cache в `TELEGRAM_BUSINESS_CONNECTIONS_FILE`, либо по умолчанию в `/data/telegram_business_connections.enc.json`.
- Для внутренних ops-инструментов `telegram_business.load_cached_business_connections()` расшифровывает cache в памяти; plaintext id не должен попадать в логи, git или артефакты.
- Cache шифруется Fernet:
  - если задан `TELEGRAM_BUSINESS_FERNET_KEY`, используется он;
  - иначе ключ детерминированно выводится из `TELEGRAM_BOT_TOKEN`, поэтому файл не содержит пригодных открытых id без bot secret.
- Автопубликация из CherryFlash выбирает Business targets только из encrypted cache:
  - allowlist личных аккаунтов хранится в БД `setting.video_announce_story_business_targets` как comma/JSON list из username, `connection_hash`, `user_hash` или `username_hash`;
  - реальные username личных аккаунтов не должны попадать в repo env/docs/code;
  - `story_publish.json` содержит только hash-label вида `business:<hash>`, а реальные `business_connection_id` и bot token передаются в Kaggle только внутри encrypted story secrets dataset;
  - по умолчанию Business targets разрешены только для CherryFlash modes `popular_review,cherryflash_libsvtav1` через `VIDEO_ANNOUNCE_STORY_BUSINESS_MODES`;
  - `VIDEO_ANNOUNCE_STORY_BUSINESS_DELAY_SECONDS` задаёт паузу перед каждым Business target и по умолчанию равен `600`.

## Operator commands

- `/check_business` (только админ) — выводит inline-клавиатуру со всеми кэшированными бизнес-подключениями, у которых `is_enabled=True` и `can_manage_stories=True`. Подпись кнопки — `@username` партнёра, fallback `hash:<8>` если username не пришёл от Telegram. Клик переводит админа в session-режим на 10 минут: следующая картинка (photo или document с MIME `image/*`) уходит как тестовая сторис на этого партнёра через Bot API `postStory` с `active_period=21600` (6 ч). `/cancel` отменяет ожидание. Команда нужна для ручной проверки end-to-end без ожидания CherryFlash слота; реальный CherryFlash fanout продолжает использовать тот же encrypted cache и live `business_connection_id`.

## Operator visibility

- При получении `business_connection` (и при восстановлении через `business_message`/`edited_business_message`) бот шлёт DM суперадмину с `connection_hash`, `user_hash`, `is_enabled`, `can_manage_stories` и пометкой `🆕 NEW` / `🔄 UPDATE`. Уведомление приходит при первом кэшировании подключения, а также при изменении `is_enabled`/`can_manage_stories`. `business_connection` всегда шлёт DM; обычные `business_message` без смены состояния не спамят.
- `BusinessConnection.date` приходит из aiogram как `datetime`; перед `json.dumps` он конвертируется в unix timestamp. Сборки до этого фикса падали на каждом апдейте с `TypeError: Object of type datetime is not JSON serializable` и не создавали файл кэша вообще.
- `load_cached_business_connections` пропускает записи, которые не расшифровываются текущим Fernet-ключом (после ротации `TELEGRAM_BOT_TOKEN` / `TELEGRAM_BUSINESS_FERNET_KEY`), пишет в лог `business connection decrypt failed connection_hash=...` и просит владельца подключения переконнектить бот.

## Story publish requirements

Для `postStory` нужны:

- `business_connection_id` из Telegram update или encrypted cache;
- активное подключение (`is_enabled=True`);
- право `can_manage_stories=True`;
- фото `1080x1920`, до `10 MB`, uploaded as multipart `attach://...`;
- видео `720x1280`, streamable `H.265` в MPEG4, до `30 MB`, uploaded as multipart `attach://...`;
- `active_period`: `21600`, `43200`, `86400` или `172800`.

Если webhook не был подписан на `business_connection` в момент подключения, Telegram не отдаст старый connection update повторно. Тогда нужно временно ловить update через controlled polling или попросить пользователя пересохранить Business подключение.
