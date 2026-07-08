---
name: kgd80-event-notifications
description: "Use in events-bot-new for KGD80 / «80 историй о главном» event participant notifications: finding registrants, drafting source-grounded reminder copy, sending test/batched emails from info@kgd80.ru, auditing Postbox delivery/open/click stats, and preparing VK reminders with event images."
---

# KGD80 Event Notifications

Use this skill for KGD80 / «80 историй о главном» event-specific participant notifications. Keep it focused on operational workflow; use linked project docs and transport skills for implementation details.

## Start checklist

1. Open `docs/README.md` and `docs/routes.yml`.
2. If a task mentions an incident ID or touches KGD80 registration links, open:
   - `docs/reports/incidents/README.md`
   - `docs/reports/incidents/INC-2026-06-29-kgd80-ticket-location-drift.md`
3. For kgd80.ru email sending, read and use `/home/dev/.codex/skills/kgd80-postbox-mailer/SKILL.md`.
4. For generic KenigEvents transactional calendar-follow email behavior, read `docs/features/event-email-notifications/README.md` only if the task touches that feature.
5. For VK direct/channel sending, inspect existing `vk_channel_publish` paths before introducing new code: `promo.py`, `main_part2.py`, `docs/features/promo-campaigns/README.md`.

## Safety rules

- Default to draft/dry-run. Real sends require explicit current-turn user approval.
- If the user asks for a test email first, send only the test recipient and stop for approval before any batch or VK fanout.
- Never print or commit secrets, IAM/static keys, SMTP passwords, VK tokens, `.env` values, raw recipient exports, or full request payloads with credentials.
- Do not mix `kgd80.ru` festival participant mailouts with `info@kenigevents.ru` static-site transactional notifications.
- Use official/source-grounded event facts only: title, date/time, venue, address, registration URL, organizer/source URL, and approved image URL/file.
- For KGD80 registration links, prefer event-specific `kgd80.ru/sobytiya/.../?register=1` over generic `kgd80.ru`.
- Send in small batches with a local ledger artifact under `artifacts/codex/<task>/`; do not paste full recipient lists into chat.

## Email workflow

1. Verify source event facts against kgd80.ru, production DB, or the registration backend.
2. Identify registrants and filter out cancelled/unsubscribed/suppressed records. Redact exports in operator-facing output.
3. Check Postbox status before sending:
   ```bash
   python3 /home/dev/.codex/skills/kgd80-postbox-mailer/scripts/postbox_mailer.py status
   ```
4. Draft concise Russian copy with logistics: event title, speaker if relevant, exact date/time, venue, address, registration/page link, and reply target `info@kgd80.ru`. Omit long descriptions unless requested.
5. Dry-run the payload, then send a test email first when requested.
6. After approval, send real recipients in small batches. Record for each batch: timestamp, recipient count, redacted recipient hashes/domains, MessageIds, errors, and retry state.
7. Report what was sent and where evidence is stored. Remind that replies go to the real `info@kgd80.ru` mailbox.

## Statistics workflow

- Use Postbox `ConfigurationSetName=kgd80-default` for KGD80 sends.
- Confirm event destinations before promising machine-readable delivery/open/click totals.
- Postbox delivery/bounce events are more reliable than opens; opens depend on tracking pixels and can be blocked by clients. Clicks require HTML links and engagement tracking.
- If a Yandex Data Streams/DataLens/YDB pipeline exists, query it by MessageId/batch metadata. Otherwise report console-level availability and local send ledger only.

## VK reminder workflow

1. Confirm registrants have explicit VK `peer_id`/user id and permission/consent for direct reminders. Do not infer VK identity from names/emails.
2. Use the official event poster/image from the event page or an approved local asset; do not call paid image generation.
3. Existing repo support is `vk_channel_publish` with explicit configured peer ids, not automatic per-registrant discovery. Inspect before sending:
   ```bash
   rg -n "vk_channel_publish|VK_AFISHA_CHANNEL_PEER|messages\.send|photos\.getMessagesUploadServer" promo.py main_part2.py .env.example docs/features/promo-campaigns/README.md
   ```
4. For direct messages, expect VK API permission failures when the user has not allowed community messages. Log per-recipient success/failure without exposing tokens.
5. Do not send VK fanout before the email test is approved if the user requested that gate.

## Closure checklist

- Test approval gate satisfied or explicitly blocked.
- Email and VK send ledgers saved under `artifacts/codex/<task>/`.
- MessageIds / VK response ids recorded without secrets.
- Delivery/open/click stats source stated precisely: Data Streams/YDB/DataLens/console/local ledger.
- Final report covers sent, failed, skipped, blocked, and still-pending recipients.
