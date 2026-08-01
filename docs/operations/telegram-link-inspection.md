# Telegram read-only inspection

Use the repository's read-only Telethon tooling when a task requires the text or metadata of a small, explicitly selected set of Telegram messages.

## Roles and credentials

Each Telegram automation role owns a separate session. The GitHub Actions reader uses only:

```text
TELEGRAM_AUTH_BUNDLE_GH_ACTIONS
```

Do not fall back to `TELEGRAM_AUTH_BUNDLE_E2E`, `TELEGRAM_AUTH_BUNDLE_S22`, `TELEGRAM_SESSION`, publishing sessions, or monitoring sessions. Do not run two jobs against the same session concurrently.

The bundle may be raw JSON or URL-safe base64 JSON. It must contain a Telethon `StringSession` under `session`, `session_string`, `string_session`, or `stringSession`. It may also contain `api_id`, `api_hash`, and the device metadata fields used when the session was created.

A Telethon `StringSession` does not contain Telegram application credentials. If `api_id` and `api_hash` are not embedded in the bundle, configure the preferred dedicated Actions secrets:

```bash
gh secret set TELEGRAM_GH_ACTIONS_API_ID \
  --app actions \
  --repo onedayonemasterpiece/events-bot-new

gh secret set TELEGRAM_GH_ACTIONS_API_HASH \
  --app actions \
  --repo onedayonemasterpiece/events-bot-new
```

Enter each value only at the interactive prompt. Do not put it in `--body` or shell history.

For compatibility, the workflow can reuse existing Telegram *application* credentials in this order: `SOCIAL_METRICS_TG_API_ID` / `SOCIAL_METRICS_TG_API_HASH`, `TG_API_ID` / `TG_API_HASH`, then `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`. This fallback applies only to app credentials; the user session remains strictly `TELEGRAM_AUTH_BUNDLE_GH_ACTIONS`.

Never print the bundle, session, API hash, the complete environment, or a serialized client configuration.

## Universal reader

The reusable entry point is:

```bash
python scripts/telegram_read.py \
  --request requests/telegram-read.json \
  --auth-bundle-env TELEGRAM_AUTH_BUNDLE_GH_ACTIONS \
  --output out/telegram-read.json
```

The checked-in request format is:

```json
{
  "schema_version": 1,
  "mode": "latest",
  "targets": ["https://t.me/lovekenig"],
  "limit": 2
}
```

Supported modes:

- `latest`: each target is a channel/chat URL, `@username`, username, or known numeric peer ID; `limit` is applied per target.
- `messages`: each target is an exact public `https://t.me/<username>/<message-id>` or private `https://t.me/c/<internal-id>/<message-id>` link.

The reader accepts at most 10 targets, 50 messages per target, and 100 messages in total. It does not download media. The JSON output includes message text, timestamps, links, views, forwards, replies, reactions, and the media type when available.

## Human-like pacing

The reader follows the same bounded random pacing used by the repository's Telegram metrics collectors. It uses `random.SystemRandom()` and defaults to:

```text
TELEGRAM_READ_STARTUP_DELAY_SECONDS=4,12
TELEGRAM_READ_BETWEEN_REQUESTS_SECONDS=2,5
TELEGRAM_READ_BETWEEN_TARGETS_SECONDS=5,15
TELEGRAM_READ_FLOOD_SLEEP_SECONDS=60
```

This means randomized pauses before connection, between Telegram API requests, and between chats/channels. It does not imitate a user through typing indicators, read receipts, reactions, fake scrolling, or other synthetic interaction.

## GitHub Actions invocation

Workflow: `.github/workflows/telegram-read.yml`.

It can be launched in either of two ways:

1. `workflow_dispatch` with `mode`, newline-separated `targets`, and `limit`.
2. A push to a branch matching `agent/telegram-read-*` that changes `requests/telegram-read.json` or the reader implementation. This path exists so an approved GitHub automation can create a narrowly scoped request without using an Issue.

The workflow serializes all runs with one concurrency group because the secret represents one Telegram identity. It writes message content only to `out/telegram-read.json`, uploads that file as an Actions artifact with one-day retention, and puts only counts in logs and the job summary.

For CLI launch after the workflow exists on the selected ref:

```bash
gh workflow run telegram-read.yml \
  --repo onedayonemasterpiece/events-bot-new \
  --ref main \
  --field mode=latest \
  --field targets='https://t.me/lovekenig' \
  --field limit=2
```

Download the result with:

```bash
gh run download RUN_ID \
  --repo onedayonemasterpiece/events-bot-new \
  --name telegram-read-RUN_ID \
  --dir out/telegram-read-RUN_ID
```

## Security boundary

The repository is public. Use this workflow only for public Telegram content unless the repository and artifact visibility are changed appropriately. Even with one-day retention, artifact content is external storage and must be treated accordingly.

The workflow is read-only by design. It must not be extended to send, edit, delete, react, join, leave, or mark messages as read. Any future write path needs a separate session role, workflow, environment, and explicit approval gate.
