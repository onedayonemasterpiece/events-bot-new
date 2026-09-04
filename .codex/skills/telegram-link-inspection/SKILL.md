---
name: telegram-link-inspection
description: Read a small, explicit set of Telegram posts through the repository's role-scoped, read-only Telethon helpers. Use for t.me links, channel checks, wording review, and factual inspection of selected messages.
---

# Telegram link inspection

## Choose the narrowest reader

- For one exact message during an already-authorized local E2E session, use `scripts/read_telegram_message.py`.
- For multiple exact links or the latest bounded messages from one or more chats/channels, use `scripts/telegram_read.py`.
- In GitHub Actions, use only `.github/workflows/telegram-read.yml` and the session role `TELEGRAM_AUTH_BUNDLE_GH_ACTIONS`.

Never reuse `TELEGRAM_AUTH_BUNDLE_E2E`, `TELEGRAM_AUTH_BUNDLE_S22`, `TELEGRAM_SESSION`, publishing sessions, or monitoring sessions for the GitHub Actions reader.

A `StringSession` does not contain Telegram app credentials. Prefer `TELEGRAM_GH_ACTIONS_API_ID` and `TELEGRAM_GH_ACTIONS_API_HASH` Actions secrets when the bundle does not include `api_id` and `api_hash`. Existing shared app credentials may be reused by the workflow, but the user session must never fall back to another role.

## Build a bounded request

Use `requests/telegram-read.json`:

```json
{
  "schema_version": 1,
  "mode": "latest",
  "targets": ["https://t.me/lovekenig"],
  "limit": 2
}
```

`latest` accepts a chat/channel URL or username. `messages` accepts exact public or `/c/` message links. Stay within 10 targets, 50 messages per target, and 100 messages total. Prefer exact links whenever the task names exact posts.

## Run locally

```bash
python scripts/telegram_read.py \
  --request requests/telegram-read.json \
  --auth-bundle-env TELEGRAM_AUTH_BUNDLE_GH_ACTIONS \
  --output out/telegram-read.json
```

Do not print or inspect the secret value. Read only the output file.

## Run through GitHub Actions without an Issue

After the workflow is present on the chosen ref, use `workflow_dispatch`. When the available GitHub integration cannot dispatch workflows, create/update a branch named `agent/telegram-read-*` with a narrowly scoped `requests/telegram-read.json`; the workflow's `push` trigger will run it.

Read the short-lived `telegram-read-<run-id>` artifact, then analyze only the content needed for the user's task. Do not commit the output.

## Behavioral and security rules

The reader must remain passive. It uses randomized 4-12 second startup, 2-5 second inter-request, and 5-15 second inter-target pauses. It does not send typing indicators, read acknowledgements, reactions, or fabricated interaction, and it does not download media.

Keep the GitHub Actions session role single-use through workflow concurrency. Never log authentication data, and never include session material in a request, artifact, issue, pull request, or chat response.

The repository is public, so use the workflow only for public Telegram content unless storage and repository visibility have been deliberately changed.

For full operations guidance, see `docs/operations/telegram-link-inspection.md`.
