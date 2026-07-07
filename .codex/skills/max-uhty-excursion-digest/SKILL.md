---
name: max-uhty-excursion-digest
description: Publish or repair excursion digest posts in the MAX channel “Ух ты, Калининград!” by mirroring Telegram/VK digest releases with the digest card image, compact numbered text, direct live links, and VK footer. Use when Codex is asked to post, edit, repair, schedule, or verify “Дайджест экскурсий” issues in MAX, especially channel “Ух ты, Калининград!”, issue numbers like №150/№152, Telegram channel @youwillsee39, or VK footer links.
---

# MAX “Ух ты, Калининград!” Excursion Digest

## Required companion skill

Use `max-web-messenger` for MAX Web login, channel navigation, rich-text editing, media upload, message context menus, and screenshots.

## Source of truth

- Telegram digest source: `@youwillsee39` compact digest post (for example, issue `№152` was Telegram message id `249` on 2026-07-07).
- Use direct Telegram message entities for event links. Do not substitute VK shorteners or `vk.cc` links unless the user asks.
- VK footer in MAX should be a direct rich link:
  - label: `Вконтакте` or exact casing from nearby MAX pattern;
  - URL: `https://vk.ru/uhtykaliningrad`.
- MAX channel name: `Ух ты, Калининград!`.

## MAX digest pattern

Mirror the compact MAX pattern used by previous posts:

```text
Дайджест экскурсий №<issue>

Коротко: <N> будущих экскурсий на <date range>. Смотрите карточку, детали и запись — ниже.

1. <linked title>
2. <linked title>
3. <linked title>
4. <linked title>
5. <linked title>

Вконтакте
```

- Include the digest card image above the text when the Telegram digest has media.
- Keep event titles as clickable rich-text links, not visible raw URLs.
- Omit Telegram-only footer fragments such as `Подписаться · Max` in MAX, unless the user explicitly requests them.
- Hashtags are not part of the established MAX compact pattern; omit them unless requested.

## Finding the current digest in Telegram

Use a human Telegram session only if already authorized and appropriate for local E2E/human use.

Minimal Telethon pattern:

```python
msg = await client.get_messages('youwillsee39', ids=<message_id>)
text = msg.message or ''
entities = msg.entities or []
media_path = await msg.download_media(file='artifacts/codex/max-web-playwright/digest_media')
```

For each `MessageEntityTextUrl`, collect:

- visible `text[offset:offset+length]` as label;
- `entity.url` as the direct link.

If the user says an issue exists in Telegram/VK, search recent `@youwillsee39` messages for `Дайджест экскурсий №<issue>` before rebuilding from production DB.

## Publishing workflow

1. Open MAX Web and channel `Ух ты, Калининград!`.
2. Inspect the previous digest in MAX to confirm pattern and casing.
3. Build the compact text from the Telegram digest.
4. Download the Telegram digest media card if present.
5. Compose rich text in the MAX Lexical editor with links from Telegram entities.
6. Attach the digest image before sending; verify a preview exists.
7. For a public channel, prefer scheduling instead of immediate sending:
   - right-click the blue send button;
   - choose `Запланировать пост`;
   - select a safely later date/time, usually tomorrow or later while preparing;
   - confirm with `Отправить завтра в HH:MM` or the equivalent button text.
8. Verify `Запланированные посты` contains the digest draft for `Ух ты, Калининград!`.
9. If the user explicitly wants immediate publication, send/save directly.
10. Verify the rendered or scheduled post:
   - image appears at top of the message bubble;
   - title is the intended issue number;
   - all event labels are links with correct hrefs;
   - footer `Вконтакте` links to `https://vk.ru/uhtykaliningrad`.
11. Save a screenshot under `artifacts/codex/max-web-playwright/`.

## Scheduled-first digest editing

For digest production work in `Ух ты, Калининград!`, use scheduled posts as the default workspace:

- Fill text, links, and the digest card image in the composer.
- Schedule the post to a later time before subscribers can see it.
- Continue proofreading and repairing the scheduled item, not a live post.
- Only move to immediate publication when the user asks for it or the scheduled draft has been fully verified.
- After any scheduled edit, re-check issue number, card image, all rich links, and the VK footer.

## Repair workflow for missing image or wrong text

1. Right-click the MAX digest post.
2. Choose `Редактировать`.
3. Confirm the composer contains the intended issue text.
4. Add the missing image through file paste/drop if `setInputFiles` does not show a preview.
5. Click the blue save/send button.
6. Verify edit mode closes and the post now has the image and correct links.

## Known issue №152 reference

Issue `№152` compact text observed from Telegram `@youwillsee39`:

```text
Дайджест экскурсий №152

Коротко: 5 будущих экскурсий на 8–10 июля. Смотрите карточку, детали и запись — ниже.

1. Лучшие локации побережья: Балтийск, Янтарный, Светлогорск, Филинская бухта
2. Калининград кинематографический: путешествие по следам советского кино
3. Путешествие по следам советского кино
4. Тильзит — Рагнит: история войны, любви и мира
5. Сквозь пыль веков из Зеленоградска

Вконтакте
```

Observed links for `№152`:

1. `https://t.me/valeravezet`
2. `https://vk.com/im?sel=-190663987`
3. `https://vk.ru/wall-190663987_9011`
4. `https://t.me/valeravezet/1264`
5. `https://t.me/valeravezet`
6. footer `https://vk.ru/uhtykaliningrad`
