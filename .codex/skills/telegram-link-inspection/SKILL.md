---
name: telegram-link-inspection
description: Use whenever the user gives a Telegram t.me link/post/channel in events-bot and the task requires reading the actual message content, comparing Telegram posts, or verifying a public Telegram incident. Uses the local human Telethon session first; public HTML is fallback only.
---

# Telegram Link Inspection

Use this skill for `https://t.me/...` / `t.me/...` links in events-bot when you
need the factual post text, caption, media, id, date, or adjacent messages.

## Non-negotiables

- Read Telegram links through Telethon first, using the local human session:
  `TELEGRAM_AUTH_BUNDLE_E2E` or `TELEGRAM_SESSION` plus
  `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` (or `TG_API_ID`/`TG_API_HASH`).
- Public HTML (`https://t.me/s/...`) is only a fallback/quick heuristic. If you
  use it, say explicitly that it is fallback evidence and do not present it as
  the preferred Telegram read path.
- Do not use `TELEGRAM_AUTH_BUNDLE_S22` for local inspection. It is reserved for
  Kaggle/remote monitoring sessions.
- Do not run the same Telethon session concurrently with a known active remote
  Kaggle/monitoring job that uses the same auth bundle.
- Do not print session strings, API hashes, bot tokens, or raw `.env` contents.
- For incident work, combine Telethon evidence with production DB/runtime-log
  evidence; Telethon answers "what is visible in Telegram", not "why it
  happened".

## Canon

- Operational doc: `docs/operations/telegram-link-inspection.md`
- Helper script: `scripts/read_telegram_message.py`
- E2E/session contract: `docs/operations/e2e-testing.md`
- Session isolation: `AGENTS.md` section `Session Boundaries`

## Workflow

1. Load local `.env` only into the current process/shell if needed:
   `set -a; source .env; set +a`.
2. Prefer the helper script for exact post reads:

   ```bash
   python3 scripts/read_telegram_message.py https://t.me/kldevents/625 https://t.me/kldevents/626
   ```

3. For private `/c/<channel>/<id>` links, the helper converts to the Telethon
   `-100...` peer automatically when possible.
4. Record only non-secret evidence: message id, chat title/username, date,
   text/caption preview or exact relevant lines, grouped id, media type.
5. If Telethon cannot access the chat/post, report the concrete failure and
   then use fallback evidence in this order: production DB/source rows, runtime
   logs, Bot API for bot-owned channel operations, public `t.me/s` HTML.

## Incident interpretation

- If two `@kldevents` links point to the same event, inspect both exact
  Telethon messages before deciding which public post to keep/delete.
- Prefer keeping the message id already stored in production DB unless Telethon
  evidence shows it is missing, edited incorrectly, or otherwise worse than the
  duplicate.
- For Telegram deletions, use Bot API only after Telethon/DB/log evidence shows
  which message is the duplicate.
