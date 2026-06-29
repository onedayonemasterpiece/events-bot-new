# Daily free-label premium emoji notes

## Saved sample

- Captured from Telegram Saved Messages message `31117` on `2026-06-29T07:33:01Z`.
- Visible fallback text: `🆓🆓🆓🆓`.
- Free-label custom emoji document ids: `5406749623865857008,5407072545276973461,5406815783542085177,5406927577245833438`.
- `Полюбить Калининград` (`lovekenigofficial`) replacements: `👉` document id `5204036388789445008`; `🎭` document id `5390961951150988955`; `🤘` document id `5393556708398225048`; date/calendar `🎟` document id `5267071016747690521`; Tretyakov pair `🖼🖼` document ids `5188683852096234620,5188683852096234620` from `https://t.me/addemoji/lovekenigofficial`.
- `Most V Kёnigsberg` (`MostVKenig`, `https://t.me/addemoji/MostVKenig`) insertions: `💰` document id `5305700407874449437`; `📗` document id `5339143926638996892`; `🏰` document id `5305794630866989617` (first castle variant).

## Replacement contract

- Main daily event card label: replace the exact prefix `🟡 Бесплатно`.
- Registration suffix is preserved: `🟡 Бесплатно по регистрации` -> `<premium-free-label> по регистрации`.
- Added-announcement compact marker: after the `ДОБАВИЛИ В АНОНС` heading, replace `🚩 🟡` with `<premium-free-label>`.
- Do not replace unrelated `🚩 🟡` markers before the added block unless the user explicitly changes the contract.
- Replace regular `👉` and `🎭` anywhere in the daily text with custom emoji entities. The visible text remains `👉` / `🎭`; only entities change. Preserve overlapping bold/link formatting for these same-length substitutions.
- For daily ticket price lines, convert `Билеты в источнике 2200` (including when `Билеты в источнике` is a link entity) to `Билеты в источнике 💰 2200` and attach the `💰` custom emoji entity.
- For venue/date lines, insert `📗 ` before `Научная библиотека` and `🏰 ` before `Замок Ноухайзен`; do not insert in free prose/title lines.
- For Tretyakov venue markers, use `🖼🖼` only where the venue is visible/structured: insert before `Третьяков...` in venue/date lines and render future added-section Tretyakov rows with `🖼🖼` instead of `🚩` from structured location data. Do not infer this marker from titles/descriptions such as `Александр Дейнека`; old title-level `👉 🖼🖼 ...` markers are cleaned back to ordinary `🖼️`.
- For rock-concert events, use `🤘`: the formatter emits visible `🤘` from event data, and the Telethon editor can replace visible music/category title icons (`🎸`, `🎵`, etc.) when the post/row text indicates a rock concert.
- Telegram event-post generator fallback must be semantically safe without Telethon: date/calendar lines start with `📅`, ticket/registration lines start with distinct ordinary `🎫`, and paid ticket lines render `🎫 Билеты 💰 1000` without textual `руб.`. The premium editor may then convert only date/calendar `📅` to custom `🎟`; it must never use `🎟` for ticket rows.

## Validation evidence to collect

- Telethon read of the edited post with message id, `edit_date`, and counts of `MessageEntityCustomEmoji`.
- Boolean evidence that `🟡 Бесплатно` is absent.
- Boolean evidence that `🚩 🟡` is absent in the added block.
- The first free-label replacement's four document ids match the saved sample.
- `👉`, `🎭`, `🤘`, date/custom `🎟`, `🖼🖼`, `💰`, `📗`, and `🏰` custom entities use the ids above, and a second dry-run reports `replacements=0`.

## Event-post extension

- Apply the same custom emoji editor to `@kldevents` Telegram event posts after Bot API send/edit.
- Keep free-event searchability by adding `#бесплатно` to the event hashtag line before replacing visible `🟡 Бесплатно...`.
- Use human-like timing: base delay plus random jitter and per-message random pauses.
