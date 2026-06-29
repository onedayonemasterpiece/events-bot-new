# Daily free-label premium emoji notes

## Saved sample

- Captured from Telegram Saved Messages message `31117` on `2026-06-29T07:33:01Z`.
- Visible fallback text: `🆓🆓🆓🆓`.
- Free-label custom emoji document ids: `5406749623865857008,5407072545276973461,5406815783542085177,5406927577245833438`.
- Same custom emoji set (`Полюбить Калининград`, short name `lovekenigofficial`) single replacements: `👉` document id `5204036388789445008`; `🎭` document id `5390961951150988955`.

## Replacement contract

- Main daily event card label: replace the exact prefix `🟡 Бесплатно`.
- Registration suffix is preserved: `🟡 Бесплатно по регистрации` -> `<premium-free-label> по регистрации`.
- Added-announcement compact marker: after the `ДОБАВИЛИ В АНОНС` heading, replace `🚩 🟡` with `<premium-free-label>`.
- Do not replace unrelated `🚩 🟡` markers before the added block unless the user explicitly changes the contract.
- Replace regular `👉` and `🎭` anywhere in the daily text with custom emoji entities from the same pack. The visible text remains `👉` / `🎭`; only entities change. Preserve overlapping bold/link formatting for these same-length substitutions.

## Validation evidence to collect

- Telethon read of the edited post with message id, `edit_date`, and counts of `MessageEntityCustomEmoji`.
- Boolean evidence that `🟡 Бесплатно` is absent.
- Boolean evidence that `🚩 🟡` is absent in the added block.
- The first free-label replacement's four document ids match the saved sample.
- `👉` and `🎭` custom entities use the same-pack ids above, and a second dry-run reports `replacements=0`.

## Event-post extension

- Apply the same custom emoji editor to `@kldevents` Telegram event posts after Bot API send/edit.
- Keep free-event searchability by adding `#бесплатно` to the event hashtag line before replacing visible `🟡 Бесплатно...`.
- Use human-like timing: base delay plus random jitter and per-message random pauses.
