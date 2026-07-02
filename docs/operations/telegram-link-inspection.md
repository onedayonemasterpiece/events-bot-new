# Telegram link inspection

Status: active.

When an operator gives an events-bot task a Telegram link (`t.me/...`) and asks
to inspect, compare, or debug the post, the default read path is **Telethon** via
the local human session. Public `t.me/s/...` HTML is allowed only as fallback or
a quick non-authoritative probe.

## Why

Telegram public HTML can be incomplete, transformed, or awkward to parse. The
project already has a human Telethon session for live E2E/debugging, so exact
post text/caption/media checks should use the Telegram API through Telethon.

## Session source

Use only local E2E/human session variables:

- `TELEGRAM_AUTH_BUNDLE_E2E` preferred;
- or `TELEGRAM_SESSION`;
- plus `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` or `TG_API_ID`/`TG_API_HASH`.

Never borrow `TELEGRAM_AUTH_BUNDLE_S22` for ad-hoc local inspection; it belongs
to Kaggle/remote monitoring. Do not print session strings, API hashes, bot
tokens, or `.env` contents.

## Helper command

For exact reads:

```bash
set -a; source .env; set +a
python3 scripts/read_telegram_message.py https://t.me/kldevents/625
```

Multiple links are supported:

```bash
python3 scripts/read_telegram_message.py \
  https://t.me/kldevents/625 \
  https://t.me/kldevents/626
```

The helper prints JSON with non-secret evidence: input URL, chat id/title,
username, message id, date, text/caption, grouped id, media class, and forward
metadata when present.

## Fallbacks

If Telethon cannot access the post/chat:

1. state the exact Telethon error;
2. use production DB rows and runtime logs for events-bot state;
3. use Bot API when the bot owns/can manage the target channel;
4. use public `https://t.me/s/...` HTML only as fallback evidence and label it
   as such in the report.

For production incidents, keep Telethon evidence separate from root-cause
evidence: Telethon verifies what Telegram shows; logs/DB explain how events-bot
got there.
