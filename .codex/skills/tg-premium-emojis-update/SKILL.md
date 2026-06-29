---
name: tg-premium-emojis-update
description: "Use for events-bot Telegram premium/custom emoji work: capturing a premium emoji composition from Saved Messages, editing channel posts via Telethon human sessions, replacing daily announcement free markers like `🟡 Бесплатно` or `🚩 🟡`, configuring automatic post-publication premium emoji editors, and validating custom emoji entities without leaking Telegram sessions."
---

# TG Premium Emojis Update

## Core rules

- Use Telethon human sessions, not Bot API formatting, when the task requires Telegram Premium/custom emoji entities.
- Respect role-scoped sessions:
  - never use `TELEGRAM_AUTH_BUNDLE_S22` for local/manual edits;
  - prefer a dedicated `TG_PREMIUM_EMOJI_AUTH_BUNDLE` for automatic production editing;
  - use `TELEGRAM_AUTH_BUNDLE_E2E` only for local/manual work with explicit fallback such as `--allow-e2e-fallback`.
- Before touching Telegram with Telethon, run the remote-session guard for `current_job_type="tg_premium_emoji_editor"` and the selected auth scope.
- Never print session strings, API hashes, bot tokens, or raw `.env` contents.

## Current free-label composition

The free-attendance label is four custom emoji with fallback text `🆓🆓🆓🆓`. Current document ids, captured from Saved Messages on 2026-06-29:

1. `5406749623865857008`
2. `5407072545276973461`
3. `5406815783542085177`
4. `5406927577245833438`

Load/override them with `TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS` as a comma-separated list when the composition changes.

## Daily announcement workflow

1. Read the latest daily announcement through Telethon, usually in `@kenigevents`, search `#ежедневныйанонс`.
2. Replace all standalone daily labels `🟡 Бесплатно` with the four custom emoji label.
   - `🟡 Бесплатно по регистрации` becomes `🆓🆓🆓🆓 по регистрации` with custom emoji entities.
   - `🟡 Бесплатно` becomes only the custom emoji label.
3. In the `ДОБАВИЛИ В АНОНС` block, replace compact marker `🚩 🟡` with the same four custom emoji label to save space.
4. Preserve existing Telegram entities: links, bold/italic, hashtags, and buttons/reply markup.
5. Verify by rereading the post and checking:
   - no `🟡 Бесплатно` remains;
   - no `🚩 🟡` remains in the added block;
   - four `MessageEntityCustomEmoji` entities exist per replacement, with the expected document ids.

## Project tooling

Use the project script instead of rewriting Telethon edit code:

```bash
python3 scripts/tg_premium_emoji_editor.py \
  --dotenv /path/to/.env \
  --chat kenigevents \
  --latest \
  --dry-run \
  --allow-e2e-fallback
```

Then edit a specific checked message:

```bash
python3 scripts/tg_premium_emoji_editor.py \
  --dotenv /path/to/.env \
  --chat kenigevents \
  --message-id 4210 \
  --allow-e2e-fallback
```

For production automation, enable the runtime editor only after configuring a dedicated session:

```env
ENABLE_TG_PREMIUM_EMOJI_EDITOR=1
TG_PREMIUM_EMOJI_AUTH_BUNDLE=<dedicated urlsafe-base64 bundle>
TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS=150
```

The scheduler edits daily announcement messages 2–3 minutes after successful Bot API publication.

## References

- Detailed operating notes live in `references/daily-free-label.md`; read it before changing replacement semantics or adding a new premium emoji composition.
