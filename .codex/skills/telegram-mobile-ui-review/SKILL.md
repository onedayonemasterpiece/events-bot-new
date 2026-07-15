---
name: telegram-mobile-ui-review
description: Use for every events-bot-new task that includes, changes, tests, reviews, screenshots, or publishes a mobile web/UI view. Before implementation, inspect the task topic and all comments/screenshots in the dedicated Telegram forum; create a topic when missing. After rendering, post mobile screenshots/video and the review URL back to that topic and verify delivery.
---

# Telegram mobile UI review

## Mandatory route

- Canonical policy: `docs/operations/mobile-ui-telegram-review.md`.
- Review forum: `KenigEvents · UI review`, `https://t.me/c/4337049383/1`, peer `-1004337049383`.
- Use only the local human E2E session from `/home/dev/projects/events-bot-new/.env` unless the operator specifies another approved session.
- Never use `TELEGRAM_AUTH_BUNDLE_S22`; never print or commit auth material.

## Gate before work

Do this before editing implementation files, building, or rendering:

1. Give the task a stable, short topic title.
2. Run `scripts/telegram_mobile_review.py inspect --download-media-dir <artifact-dir>` and review all topic titles.
3. If a matching topic exists, inspect it with `--topic-id`; read every new comment and open/download every screenshot or attachment. Treat those comments as task requirements and reconcile them with the user request before starting.
4. If no topic matches, explicitly create one with `ensure-topic --title ... --create-if-missing`, then post a short task card. An empty newly created topic satisfies the pre-work read gate because it has no comments yet.
5. If Telegram or the approved session is unavailable, report the blocker. Do not silently skip the gate.

For work longer than one turn, re-read the topic immediately before implementation resumes so comments added during the pause are not missed.

## Evidence after rendering

1. Capture at least the narrowest supported viewport and one representative phone viewport. For motion/gesture tasks also attach a short video or phase sequence.
2. Send the public/local review URL, exact build/version, what to assess, and what is intentionally out of scope.
3. Upload the actual mobile artifacts with `send`; do not send only a desktop contact sheet.
4. Verify returned message IDs and re-inspect the topic before declaring the mobile task ready for review.
5. If new feedback appeared, process it before continuing or closing.

Store inspection JSON, downloaded Telegram media, and receipts under `artifacts/codex/<task>/`; never commit them.

## Helper

If Telethon is unavailable, create an artifact-local venv and install the currently validated `telethon==1.44.0`:

```bash
python3 -m venv artifacts/codex/telegram-mobile-review/venv
artifacts/codex/telegram-mobile-review/venv/bin/pip install 'telethon==1.44.0'
```

Run the helper with that Python:

```bash
PY=artifacts/codex/telegram-mobile-review/venv/bin/python
$PY .codex/skills/telegram-mobile-ui-review/scripts/telegram_mobile_review.py \
  --output artifacts/codex/<task>/telegram-before.json inspect \
  --download-media-dir artifacts/codex/<task>/telegram-media
```

Use `--help` for `inspect`, `ensure-topic`, and `send` arguments.
