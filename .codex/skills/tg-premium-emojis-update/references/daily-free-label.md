# Daily free-label premium emoji notes

## Saved sample

- Captured from Telegram Saved Messages message `31117` on `2026-06-29T07:33:01Z`.
- Visible fallback text: `🆓🆓🆓🆓`.
- Custom emoji document ids: `5406749623865857008,5407072545276973461,5406815783542085177,5406927577245833438`.

## Replacement contract

- Main daily event card label: replace the exact prefix `🟡 Бесплатно`.
- Registration suffix is preserved: `🟡 Бесплатно по регистрации` -> `<premium-free-label> по регистрации`.
- Added-announcement compact marker: after the `ДОБАВИЛИ В АНОНС` heading, replace `🚩 🟡` with `<premium-free-label>`.
- Do not replace unrelated `🚩 🟡` markers before the added block unless the user explicitly changes the contract.

## Validation evidence to collect

- Telethon read of the edited post with message id, `edit_date`, and counts of `MessageEntityCustomEmoji`.
- Boolean evidence that `🟡 Бесплатно` is absent.
- Boolean evidence that `🚩 🟡` is absent in the added block.
- The first replacement's four document ids match the saved sample.
