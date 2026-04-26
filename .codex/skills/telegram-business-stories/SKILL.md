---
name: telegram-business-stories
description: Use when publishing, debugging, or changing Telegram Business Stories support for this repo's bot, including Business Bot API `postStory`, `business_connection` webhook delivery, encrypted connection cache, Fly production checks, or story media preparation.
---

# Telegram Business Stories

Use this skill for repo-internal Telegram Business story work. Keep the canonical product/runtime contract in `docs/features/telegram-business-stories/README.md`; this skill is the operational playbook.

## Start

Read the canonical docs and code before acting:

- `docs/features/telegram-business-stories/README.md`
- `telegram_business.py`
- `main.py` handler `handle_business_connection`
- `main_part2.py` webhook setup and `dp.business_connection.register(...)`

If the task is live/prod-bound, also follow:

- `docs/operations/release-governance.md`
- `docs/operations/repository-workflow.md`

## Hard Rules

- Never log, commit, or paste raw `business_connection_id`, Telegram user id, bot token, or username.
- Use hash-only diagnostics from `telegram_business.secure_short_hash()`.
- Use encrypted cache only: `telegram_business.cache_business_connection()` and `load_cached_business_connections()`.
- Do not run `getUpdates` while the production webhook is active unless doing a controlled capture.
- If temporarily disabling webhook for controlled capture, always use `drop_pending_updates=false` and restore the exact webhook URL and allowed updates in `finally`.
- Do not borrow `TELEGRAM_AUTH_BUNDLE_E2E` or `TELEGRAM_AUTH_BUNDLE_S22` for Bot API Business Stories. Business Stories here are Bot API token + `business_connection_id`, not Telethon story publishing.
- Before relying on `postStory` details for a live operation, verify the current official Bot API docs if there is any doubt.

## Current Production Contract

- Fly app: `events-bot-new-wngqia`.
- Webhook allowed updates must include `business_connection`.
- Canonical allowed-updates list: `telegram_business.WEBHOOK_ALLOWED_UPDATES`.
- Encrypted cache default path on prod: `/data/telegram_business_connections.enc.json`.
- Cache key source:
  - `TELEGRAM_BUSINESS_FERNET_KEY` when set;
  - otherwise derived from `TELEGRAM_BOT_TOKEN`.

## Publishing Workflow

1. Confirm prod health:
   - `fly status -a events-bot-new-wngqia`
   - `curl -fsS --max-time 15 https://events-bot-new-wngqia.fly.dev/healthz`
2. Confirm webhook state through Bot API without printing token:
   - `getWebhookInfo.allowed_updates` contains `business_connection`;
   - `pending_update_count` and `last_error_message` are sane.
3. Load cached business connections in memory via `load_cached_business_connections()`.
4. Select only enabled connections with `can_manage_stories=True`.
5. Prepare image media:
   - photo must be `1080x1920`;
   - keep under `10 MB`;
   - write transient outputs under `artifacts/codex/...`, not repo docs/code.
6. Call `postStory` using multipart upload and `content={"type":"photo","photo":"attach://..."}`.
7. Report only `postStory_ok`, story id, and hash-only connection/user diagnostics.

## Capture Workflow

Use this only when no encrypted connection is available or a user has just reconnected Business access and the webhook did not receive the update.

1. Fetch current webhook URL and allowed updates.
2. `deleteWebhook(drop_pending_updates=false)`.
3. Poll only Business update types:
   - `business_connection`
   - `business_message`
   - `edited_business_message`
4. Extract `business_connection.id` or `business_message.business_connection_id`.
5. Call `getBusinessConnection` and require:
   - `is_enabled=True`;
   - `rights.can_manage_stories=True`.
6. Cache the connection encrypted.
7. Restore the original webhook URL and allowed updates in `finally`.
8. Re-check `getWebhookInfo`.

If no update arrives, ask the user to toggle the bot in the user's Telegram Business -> Chatbots settings or resave story rights. Toggling Business mode in BotFather only changes bot capability; it does not produce the user's `business_connection_id`.

## Code Change Workflow

For behavior changes:

1. Keep webhook update types centralized in `telegram_business.WEBHOOK_ALLOWED_UPDATES`.
2. Keep plaintext ids out of logs, docs, tests, and artifacts.
3. Update `docs/features/telegram-business-stories/README.md`.
4. Add a concise `[Unreleased]` entry to `CHANGELOG.md`.
5. Add or update focused tests, usually `tests/test_telegram_business.py`.
6. Run:
   - `pytest -q tests/test_telegram_business.py`
   - `python -m py_compile telegram_business.py main.py main_part2.py`
7. For prod-bound changes, commit, push to `origin`, ensure the commit is reachable from `origin/main`, deploy from a clean worktree, then verify `/healthz` and `getWebhookInfo`.
