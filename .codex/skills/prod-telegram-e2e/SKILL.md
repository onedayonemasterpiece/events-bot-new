---
name: prod-telegram-e2e
description: Use when running or debugging production Telegram UI E2E for this repo, especially admin bot commands such as /tg, /vk_auto_import, /fest_queue, or single-source Telegram Monitoring. Ensures the run targets @events_love39_bot, uses only the E2E human Telethon session locally, checks production DB access, and inspects Fly runtime logs early.
---

# Production Telegram UI E2E

Use this skill before any live E2E that must interact with the production
Telegram bot.

## Route

- Canonical runbook: `docs/operations/e2e-testing.md`, section
  `Production Telegram UI E2E`.
- Runtime logs: `docs/operations/runtime-logs.md`.
- Release/deploy context if code changed: `docs/operations/release-governance.md`.

## Non-negotiables

- Production bot target is `@events_love39_bot`.
- Do not infer the production bot from local `.env` `TELEGRAM_BOT_TOKEN`; local
  `.env` can point to `@eventsbotTestBot`.
- Local `.env` is only for the human Telethon client:
  `TELEGRAM_AUTH_BUNDLE_E2E` or `TELEGRAM_SESSION`, plus API id/hash.
- Never use `TELEGRAM_AUTH_BUNDLE_S22` for local UI E2E. It is reserved for
  Kaggle/remote monitoring.
- Do not grant production roles silently. If the E2E user is missing from the
  production `user` table or is not `superadmin`, ask for explicit permission.
- Temporary E2E admin access must be easy to revoke. Before enabling it, record
  whether the row existed and what its role flags were; after the debugging run,
  restore the previous state or delete the inserted E2E-only row.

## Checklist

1. Load Fly release auth for tooling only:
   `set -a; . /home/dev/.config/fly/release.env; set +a`.
2. Verify production health and log mirror:
   `flyctl status`, `/healthz`, `/data/runtime_logs/events-bot.log`.
3. Connect Telethon from the local E2E bundle and record `get_me()` id/username.
4. Check production DB `/data/db.sqlite`, table `user`, for that exact
   `user_id` and `is_superadmin=1`.
5. Send the UI command to `@events_love39_bot`, never to autodetected bot names.
6. If the bot is silent, inspect runtime logs immediately. Webhook `200` with no
   outgoing message often means the handler returned on access check.
7. Report UI evidence and log evidence together.

## Temporary Superadmin Toggle

Use only after explicit user approval. Keep the old row state in the terminal
output or an uncommitted artifact under `artifacts/codex/`.

Enable for the current E2E account:

```bash
set -a; . /home/dev/.config/fly/release.env; set +a
/home/dev/.fly/bin/flyctl ssh console -C "python -c 'import sqlite3,json; uid=8336351413; username=\"The_day_of_kk\"; con=sqlite3.connect(\"/data/db.sqlite\"); con.row_factory=sqlite3.Row; before=[dict(r) for r in con.execute(\"select user_id,username,is_superadmin,is_partner,blocked from user where user_id=?\",(uid,))]; con.execute(\"insert into user(user_id,username,is_superadmin,is_partner,blocked) values(?,?,?,?,?) on conflict(user_id) do update set username=excluded.username,is_superadmin=1,blocked=0\",(uid,username,1,0,0)); con.commit(); after=[dict(r) for r in con.execute(\"select user_id,username,is_superadmin,is_partner,blocked from user where user_id=?\",(uid,))]; print(json.dumps({\"before\":before,\"after\":after}, ensure_ascii=False))'"
```

Disable after E2E:

- If `before` was empty, delete the E2E-only row.
- If `before` existed, restore its previous `is_superadmin`, `is_partner`,
  `blocked`, and `username` values.

For the common E2E-only row case:

```bash
set -a; . /home/dev/.config/fly/release.env; set +a
/home/dev/.fly/bin/flyctl ssh console -C "python -c 'import sqlite3,json; uid=8336351413; con=sqlite3.connect(\"/data/db.sqlite\"); con.row_factory=sqlite3.Row; before=[dict(r) for r in con.execute(\"select user_id,username,is_superadmin,is_partner,blocked from user where user_id=?\",(uid,))]; con.execute(\"delete from user where user_id=?\",(uid,)); con.commit(); after=[dict(r) for r in con.execute(\"select user_id,username,is_superadmin,is_partner,blocked from user where user_id=?\",(uid,))]; print(json.dumps({\"before\":before,\"after\":after}, ensure_ascii=False))'"
```
