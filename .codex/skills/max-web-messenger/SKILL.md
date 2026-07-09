---
name: max-web-messenger
description: Operate MAX Web Messenger through Playwright for QR login, persistent sessions, chat/channel search, reading posts, composing rich-text messages with links, attaching files/media, editing/deleting/forwarding/copying messages, and testing scheduled/postponed messages. Use when Codex is asked to use web.max.ru, messenger Max/MAX, QR authorization, MAX channels, MAX saved messages, MAX post editing, MAX media upload, or human-like Playwright automation of MAX Web.
---

# MAX Web Messenger

## Core constraints

- Use `https://web.max.ru/` and a persistent Playwright profile under `artifacts/codex/<task>/profile` unless the user names another profile.
- Treat the browser session as user-authorized human UI automation. Do not print phone numbers, private message bodies, cookies, localStorage, tokens, or QR payload internals.
- Prefer official UI flows over direct app API reverse engineering. Use DOM/Playwright only to drive the UI.
- Keep screenshots, controller scripts, downloaded media, and receipts under `artifacts/codex/<task>/`; do not commit artifacts.
- If using project Telegram sessions only to transfer QR/screenshots, follow the `telegram-human-session` skill and never reuse role-scoped bundles outside their purpose.

## Starting or restoring MAX Web

### Known local session

- Reuse the existing authorized MAX Web Chromium profile first:
  - `/home/dev/projects/events-bot-new/artifacts/codex/max-web-playwright/profile`
- Verified on 2026-07-09: opening `https://web.max.ru/` with this persistent profile showed the chat UI, not the QR login screen.
- Treat everything inside the profile as secret browser session data. Do not print or copy cookies, Local Storage, IndexedDB, or QR/auth payloads.
- A second local candidate, `artifacts/codex/max-vk-louisa-20260708/max_profile`, was checked on 2026-07-09 and showed the QR login screen; do not assume it is authorized unless re-verified.

1. Launch Chromium/Playwright with a persistent profile:
   - default profile: `/home/dev/projects/events-bot-new/artifacts/codex/max-web-playwright/profile`;
   - fallback new-task profile example: `artifacts/codex/<task>/profile`.
   - set `locale: ru-RU`, timezone `Europe/Moscow`, and a normal desktop viewport.
2. Open `https://web.max.ru/`.
3. If MAX shows `Войдите в MAX по QR-коду`, screenshot the page and show the QR to the user.
4. If the user permits Telegram delivery, decode the QR image locally and send only the extracted authorization link to Telegram Saved Messages/`Избранное` through an approved `telegram-human-session`.
   - Do not print the decoded auth link in logs or chat.
   - Treat the decoded QR payload as a short-lived secret.
   - If decoding fails, fall back to a screenshot of the QR.
5. Wait for the user to confirm scanning, then verify the page contains chat lists (`Чаты`, `Каналы`, `Избранное`) instead of the QR login page.
6. Keep the session open only while a follow-up command is expected. At the end of the task, close the browser/context/controller so the Playwright session is not left running.

## QR link handoff through Telegram

When QR authorization is needed in a headless environment:

1. Save a screenshot or cropped QR image under `artifacts/codex/<task>/`.
2. Decode the QR locally with an available tool such as `zbarimg`, `pyzbar`, or OpenCV QR detector.
3. Send the decoded MAX authorization link to Telegram Saved Messages using an approved local human Telethon session.
4. Include a short neutral caption such as `MAX Web login link`; do not include secrets in terminal output.
5. If the user reports successful scan/open, continue with MAX Web verification.

Never reuse Telegram bundles outside their scoped purpose and never send QR payloads to third parties.

## Finding chats and channels

- Use the left chat list search field (`Найти`) for named channels/chats, or click a visible chat row.
- For channels, verify the header name and subscriber count in the main pane before posting.
- If DOM click does not switch chats, click by coordinates on the visible row or dispatch mouse events on the row/button with matching text.
- The opened chat URL can include a channel id path, e.g. `https://web.max.ru/-...`; do not rely on the URL alone as proof of the target.

## Reading message patterns and links

- Use `document.body.innerText` for rough text, but inspect visible message bubbles for exact pattern.
- Extract links from message bubbles with `querySelectorAll('a')`; MAX stores rich links as `<a class="link" href="...">label</a>` in rendered posts.
- When copying a prior pattern, preserve visible text, link labels, and destination URLs. Avoid replacing direct Telegram links with VK shorteners unless the user explicitly asks.

## Composing rich text with links

MAX Web uses a Lexical editor for the composer.

1. Find the composer: `div[role="textbox"][contenteditable]` with `__lexicalEditor`.
2. For simple plain text, normal typing or `keyboard.type()` is usually enough.
3. For rich links, set the Lexical editor state rather than only changing DOM:
   - build a JSON state with one paragraph containing `text`, `linebreak`, and `link` nodes;
   - call `editor.parseEditorState(JSON.stringify(state))`;
   - call `editor.setEditorState(parsed)`;
   - dispatch an `input` event on the root element.
4. Verify before sending:
   - `root.innerText` matches intended text;
   - `root.querySelectorAll('a')` contains every intended `[label, href]`.

## Attaching files and images

- The composer normally has a visible clip button (`aria-label="Загрузить файл"`) and a hidden `input[type=file]` with `multiple=true`.
- `page.setInputFiles('input[type=file]', path)` can work for normal uploads, but if it does not show a preview in edit mode, simulate paste/drop with a `File` object and `ClipboardEvent('paste')` / `DragEvent('drop')` on the contenteditable editor.
- Verify an attachment preview before saving/sending. A successful local preview can appear as `blob:https://web.max.ru/...` with the source filename as `alt`.
- After saving, verify the rendered post contains a non-blob `https://i.oneme.ru/...` image in the target message bubble.

## Sending and editing messages

- Send/save button is the blue button near the composer, often `aria-label="Отправить сообщение"` even in edit mode.
- To edit an existing post:
  1. Right-click the message bubble.
  2. Choose `Редактировать` from the popover.
  3. Verify `Редактирование поста` appears and the composer text is the intended message.
  4. Modify text/media.
  5. Click the blue send/save button.
  6. Verify edit mode closes and the rendered post contains the expected text, links, and media.
- Message context menu actions observed: `Редактировать`, `Ответить`, `Переслать`, `Скопировать ссылку на пост`, `Закрепить`, `Отметить непрочитанным`, `Скопировать текст`, `Выбрать`, `Удалить`.

## Scheduled/postponed posts

Use scheduling for public channel drafts whenever possible: compose and inspect the post in the scheduled queue first, then publish later or edit the scheduled item. This prevents subscribers from seeing intermediate corrections.

Observed MAX Web flow for channel posts:

1. Open the target channel and fill the composer with the intended draft.
2. Verify text, links, and media preview before scheduling.
3. Right-click the blue send button, not the message body.
4. In the context menu choose `Запланировать пост`.
5. In the `Запланировать пост` dialog, choose a later calendar date/time.
   - The default time can be only a few minutes in the future.
   - Prefer tomorrow or another safely later time for work-in-progress drafts.
6. Click the confirmation button, whose text changes with the choice, for example `Отправить завтра в HH:MM`.
7. Verify that MAX opens or updates `Запланированные посты` and that the queued item shows the target channel, draft text, and scheduled time.

Notes:

- If the context menu does not open through DOM events, coordinate right-click on the visible blue send button is reliable.
- The scheduled queue is the safest place for further post filling, proofreading, and edits. Use the same right-click/context-menu discipline on scheduled items when editing or deleting them.
- Record an evidence screenshot of the dialog or resulting scheduled queue under `artifacts/codex/<task>/`.

## Safety checks before public channel actions

- Before sending to a channel, take a screenshot of the draft and inspect it.
- For live channel posts, prefer drafting/editing in scheduled/postponed messages when supported so subscribers do not see intermediate mistakes.
- If a public mistake is made, use `Редактировать` immediately rather than posting a correction unless the user asks for a separate post.
- On completion, close the Playwright browser context/controller unless the user explicitly asks to keep it open for the next command.
