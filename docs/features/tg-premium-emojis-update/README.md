# TG premium emojis update

Status: MVP.

## Цель

Добавлять к Telegram-публикациям полезные Premium/custom emoji без изменения исходного Bot API форматтера. Первый регулярный сценарий — заменить бесплатные метки в ежедневном анонсе на компактный premium-label `Бесплатно` из четырёх custom emoji. Следующий сценарий — применять тот же редактор к Telegram event posts (`@kldevents`) после публикации/редактирования.

## Текущий контракт

- Сохранённая композиция free-label: четыре custom emoji с fallback `🆓🆓🆓🆓`.
- Free-label document ids: `5406749623865857008`, `5407072545276973461`, `5406815783542085177`, `5406927577245833438`. Single emoji ids: `👉` → `5204036388789445008`, `🎭` → `5390961951150988955`, `🤘` → `5393556708398225048`, date/calendar `🎟` → `5267071016747690521` from `Полюбить Калининград`; Tretyakov pair `🖼🖼` → `5188683852096234620`, `5188683852096234620` from `https://t.me/addemoji/lovekenigofficial`; `💰` → `5305700407874449437`, `📗` → `5339143926638996892`, `🏰` → `5305794630866989617` from `https://t.me/addemoji/MostVKenig`.
- В ежедневном анонсе заменяются:
  - `🟡 Бесплатно` → premium-label;
  - `🟡 Бесплатно по регистрации` → premium-label + ` по регистрации`;
  - в блоке `ДОБАВИЛИ В АНОНС`: `🚩 🟡` → premium-label, чтобы не тратить место на две обычные пиктограммы;
  - `👉` → custom emoji `👉` из того же набора;
  - `🎭` → custom emoji `🎭` из того же набора;
  - rock-concert title/category icon → `🤘` from the same set;
  - Telegram event-post generator fallback is independent from Telethon: `📅` for date/calendar and distinct `🎫` for tickets/registration; the editor may premiumize only date/calendar `📅` into custom `🎟`;
  - paid Telegram event-post ticket lines: `🎫 Билеты 💰 1000`, without textual `руб.`;
  - `Билеты в источнике 2200` / linked `Билеты в источнике` + price → `Билеты в источнике 💰 2200`;
  - in venue/date lines, `Научная библиотека` → `📗 Научная библиотека`;
  - in venue/date lines, `Замок Ноухайзен` → `🏰 Замок Ноухайзен`;
  - Tretyakov venue markers: venue/date `Третьяков...` → `🖼🖼`; in `ДОБАВИЛИ В АНОНС`, rows whose structured location is Tretyakov are rendered with `🖼🖼` instead of `🚩`. The marker is not inferred from titles/descriptions like `Александр Дейнека`; title-level `👉 🖼🖼 ...` leftovers are cleaned back to ordinary `🖼️`.
- Существующие entity поста (жирный/курсив, ссылки, хештеги, кнопки) сохраняются; для одиночных emoji видимый текст не меняется, добавляется только custom-emoji entity.
- Для Telegram event posts бесплатность остаётся поисковой через `#бесплатно` в hashtag line; видимая строка `🟡 Бесплатно...` после публикации может быть заменена на premium-label без потери поиска.

## Runtime

Публикация ежедневного анонса и event posts остаётся Bot API-задачей. После успешной отправки/редактирования `send_daily_announcement` и `publish_tg_event_announcement` могут запланировать Telethon-редактор на отправленные message ids.

Env:

- `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1` — включает автоматический редактор после daily send.
- `TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS=150` — базовая задержка перед правкой; по умолчанию 150 секунд (2–3 минуты).
- `TG_PREMIUM_EMOJI_EDIT_JITTER_SECONDS=45` — случайная добавка к базовой задержке для human-like поведения.
- `TG_PREMIUM_EMOJI_BETWEEN_EDITS_SECONDS=3,12` — случайный диапазон паузы между несколькими правками в одном запуске.
- `TG_PREMIUM_EMOJI_AUTH_BUNDLE` / `TG_PREMIUM_EMOJI_SESSION` — выделенная Telethon-сессия для редактора.
- `TG_PREMIUM_EMOJI_API_ID` / `TG_PREMIUM_EMOJI_API_HASH` — опциональные API credentials; если не заданы, используются `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` или `TG_API_ID`/`TG_API_HASH`.
- `TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS` — опциональный comma-separated override для free-label document ids.
- `TG_PREMIUM_EMOJI_DAILY_SINGLE_DOCUMENT_IDS_JSON` — опциональный JSON override для одиночных/inserted daily emoji, например `{"👉":5204036388789445008,"🎭":5390961951150988955,"🤘":5393556708398225048,"🎟":5267071016747690521,"💰":5305700407874449437,"📗":5339143926638996892,"🏰":5305794630866989617}`.
- `TG_PREMIUM_EMOJI_TRETYAKOV_DOCUMENT_IDS` — опциональный comma-separated override для пары `🖼🖼`.
- `TG_PREMIUM_EMOJI_ALLOW_E2E_FALLBACK=1` — только для локальных/ручных правок, разрешает `TELEGRAM_AUTH_BUNDLE_E2E` / `TELEGRAM_SESSION` fallback.

## Session control

- Автоматизация не должна брать `TELEGRAM_AUTH_BUNDLE_S22`: эта сессия зарезервирована под Kaggle/remote monitoring.
- Перед Telethon-правкой редактор запускает `remote_telegram_session` guard как `tg_premium_emoji_editor` с выбранным auth scope.
- Для production лучше завести отдельный `TG_PREMIUM_EMOJI_AUTH_BUNDLE`, чтобы редактор daily-анонса не конкурировал с локальными E2E и remote runs.

## Manual operation

Dry-run последнего ежедневного анонса:

```bash
python3 scripts/tg_premium_emoji_editor.py \
  --dotenv /path/to/.env \
  --chat kenigevents \
  --latest \
  --dry-run \
  --allow-e2e-fallback
```

Правка конкретного сообщения:

```bash
python3 scripts/tg_premium_emoji_editor.py \
  --dotenv /path/to/.env \
  --chat kenigevents \
  --message-id <id> \
  --allow-e2e-fallback
```

## Validation

После правки перечитать сообщение через Telethon и проверить:

- `🟡 Бесплатно` отсутствует;
- `🚩 🟡` отсутствует в блоке `ДОБАВИЛИ В АНОНС`;
- регулярные `👉`/`🎭`/`🤘`, date/custom `🎟`, Tretyakov pair `🖼🖼`, and inserted `💰`/`📗`/`🏰` получили `MessageEntityCustomEmoji` из соответствующих наборов;
- на каждую замену приходится 4 `MessageEntityCustomEmoji` с ожидаемыми document ids;
- ссылки/кнопки daily-поста сохранились.

## Skill

Операционная памятка для будущих premium-emoji задач: `.codex/skills/tg-premium-emojis-update/SKILL.md`.
