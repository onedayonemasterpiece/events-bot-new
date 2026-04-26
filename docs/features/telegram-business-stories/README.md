# Telegram Business Stories

Цель: безопасно публиковать сторис от имени подключённого Telegram Business аккаунта через Bot API, не сохраняя `business_connection_id` и user id в открытом виде.

## Runtime contract

- Production webhook обязан подписываться на update type `business_connection`.
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
  - `VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS=all` добавляет все активные подключения с `can_manage_stories`;
  - вместо `all` можно указать comma/JSON list из `connection_hash`, `user_hash` или `username_hash`;
  - `story_publish.json` содержит только hash-label вида `business:<hash>`, а реальные `business_connection_id` и bot token передаются в Kaggle только внутри encrypted story secrets dataset;
  - по умолчанию Business targets разрешены только для CherryFlash modes `popular_review,cherryflash_libsvtav1` через `VIDEO_ANNOUNCE_STORY_BUSINESS_MODES`;
  - `VIDEO_ANNOUNCE_STORY_BUSINESS_DELAY_SECONDS` задаёт паузу перед каждым Business target и по умолчанию равен `600`.

## Story publish requirements

Для `postStory` нужны:

- `business_connection_id` из Telegram update или encrypted cache;
- активное подключение (`is_enabled=True`);
- право `can_manage_stories=True`;
- фото `1080x1920`, до `10 MB`, uploaded as multipart `attach://...`;
- видео `720x1280`, streamable `H.265` в MPEG4, до `30 MB`, uploaded as multipart `attach://...`;
- `active_period`: `21600`, `43200`, `86400` или `172800`.

Если webhook не был подписан на `business_connection` в момент подключения, Telegram не отдаст старый connection update повторно. Тогда нужно временно ловить update через controlled polling или попросить пользователя пересохранить Business подключение.
