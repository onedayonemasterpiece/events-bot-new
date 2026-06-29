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

Load/override them with `TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS` as a comma-separated list when the composition changes. Also replace/insert emoji by default: `👉` → `5204036388789445008`, `🎭` → `5390961951150988955`, rock `🤘` → `5393556708398225048`, calendar `🎟` → `5267071016747690521`, Tretyakov `🖼🖼` → `5188445640325099838,5188470637034758005` (two-part composite after the small thumbnail), `💰` → `5305700407874449437`, `📗` → `5339143926638996892`, `🏰` → `5305794630866989617`; override singles with `TG_PREMIUM_EMOJI_DAILY_SINGLE_DOCUMENT_IDS_JSON` and Tretyakov pair with `TG_PREMIUM_EMOJI_TRETYAKOV_DOCUMENT_IDS` if needed.

## Daily announcement workflow

1. Read the latest daily announcement through Telethon, usually in `@kenigevents`, search `#ежедневныйанонс`.
2. Replace all standalone daily labels `🟡 Бесплатно` with the four custom emoji label.
   - `🟡 Бесплатно по регистрации` becomes `🆓🆓🆓🆓 по регистрации` with custom emoji entities.
   - `🟡 Бесплатно` becomes only the custom emoji label.
3. In the `ДОБАВИЛИ В АНОНС` block, replace compact marker `🚩 🟡` with the same four custom emoji label to save space.
4. Replace regular `👉`, `🎭`, and `🤘` with custom emoji entities where present. For Telegram event posts, keep the generator fallback semantically safe (`📅` for date/calendar, `🎫` for tickets/registration); after publication the editor may premiumize only date/calendar `📅` into custom `🎟`. Insert `💰` before daily price values after `Билеты в источнике` and before Telegram event-post ticket prices while removing textual `руб.`, insert `📗` before `Научная библиотека` venue lines, insert `🏰` before `Замок Ноухайзен` venue lines, use pair `🖼🖼` only as a Tretyakov venue marker (before visible Tretyakov location lines and in structured Tretyakov added-section rows), and use `🤘` for rock-concert event title/category icons.
5. Preserve existing Telegram entities: links, bold/italic, hashtags, and buttons/reply markup.
6. Verify by rereading the post and checking:
   - no `🟡 Бесплатно` remains;
   - no `🚩 🟡` remains in the added block;
   - four `MessageEntityCustomEmoji` entities exist per free-label replacement, with the expected document ids;
   - `👉`, `🎭`, `🤘`, date/custom `🎟`, `🖼🖼`, `💰`, `📗`, and `🏰` have their configured custom emoji document ids.

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

The scheduler edits daily announcements and Telegram event posts 2–3 minutes after successful Bot API publication, with randomized jitter (`TG_PREMIUM_EMOJI_EDIT_JITTER_SECONDS`) and between-message pauses (`TG_PREMIUM_EMOJI_BETWEEN_EDITS_SECONDS`) for human-like timing. For event posts, keep searchability by ensuring free events carry `#бесплатно` before replacing visible `🟡 Бесплатно...`.

## Compensation Telegram event posts

- Incident/compensation publications for `@kldevents` must use the standard
  Telegram event publisher (`job_publish_tg_event_post` /
  `publish_tg_event_announcement`) or an equivalent path that preserves the
  normal ticket/registration line and media-group rules.
- A short-lived one-off script must not rely only on the fire-and-forget
  `_schedule_tg_premium_emoji_editor()` task: the process can exit before the
  2–3 minute delayed editor runs. Either keep the process alive and await
  `edit_messages_with_env(..., delay_seconds≈180)` or run
  `scripts/tg_premium_emoji_editor.py --message-id ...` after publication.
- Compensation is complete only after a Telethon reread confirms the
  registration/ticket link entities are present and the premium/custom emoji
  replacement ran (or a documented blocker explains why it did not).

## References

- Detailed operating notes live in `references/daily-free-label.md`; read it before changing replacement semantics or adding a new premium emoji composition.
