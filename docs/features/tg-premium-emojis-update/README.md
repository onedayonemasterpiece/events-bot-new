# TG premium emojis update

Status: MVP.

## Цель

Добавлять к Telegram-публикациям полезные Premium/custom emoji без изменения исходного Bot API форматтера. Первый регулярный сценарий — заменить бесплатные метки в ежедневном анонсе на компактный premium-label `Бесплатно` из четырёх custom emoji.

## Текущий контракт

- Сохранённая композиция free-label: четыре custom emoji с fallback `🆓🆓🆓🆓`.
- Document ids: `5406749623865857008`, `5407072545276973461`, `5406815783542085177`, `5406927577245833438`.
- В ежедневном анонсе заменяются:
  - `🟡 Бесплатно` → premium-label;
  - `🟡 Бесплатно по регистрации` → premium-label + ` по регистрации`;
  - в блоке `ДОБАВИЛИ В АНОНС`: `🚩 🟡` → premium-label, чтобы не тратить место на две обычные пиктограммы.
- Существующие entity поста (жирный/курсив, ссылки, хештеги, кнопки) сохраняются.

## Runtime

Публикация ежедневного анонса остаётся Bot API-задачей. После успешной отправки `send_daily_announcement` может запланировать Telethon-редактор на отправленные message ids.

Env:

- `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1` — включает автоматический редактор после daily send.
- `TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS=150` — задержка перед правкой; по умолчанию 150 секунд (2–3 минуты).
- `TG_PREMIUM_EMOJI_AUTH_BUNDLE` / `TG_PREMIUM_EMOJI_SESSION` — выделенная Telethon-сессия для редактора.
- `TG_PREMIUM_EMOJI_API_ID` / `TG_PREMIUM_EMOJI_API_HASH` — опциональные API credentials; если не заданы, используются `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` или `TG_API_ID`/`TG_API_HASH`.
- `TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS` — опциональный comma-separated override document ids.
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
- на каждую замену приходится 4 `MessageEntityCustomEmoji` с ожидаемыми document ids;
- ссылки/кнопки daily-поста сохранились.

## Skill

Операционная памятка для будущих premium-emoji задач: `.codex/skills/tg-premium-emojis-update/SKILL.md`.
