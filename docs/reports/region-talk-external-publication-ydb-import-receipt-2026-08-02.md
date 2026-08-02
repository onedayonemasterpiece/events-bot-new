# Region Talk: YDB intake receipt — 2026-08-02

GitHub Actions run: `30737423882`. Trusted main SHA: `fb18cd45c9deb80451746aa26b789e448da0d3f0`.

Все три research-result JSON прошли строгую schema/importer validation до получения Yandex IAM token. Затем они обработаны последовательно; перед каждым execute importer перечитал live YDB identity ledger. Повторные identity записаны как replay, а не как новые intake rows.

| Request ID | Valid | Candidate pipeline | Manual/blocked | New intake | Replay | Conflicts | Input SHA-256 |
|---|---:|---:|---:|---:|---:|---:|---|
| `region-talk-external-2026-08-02-063020` | 20 | 12 | 8 | 0 | 20 | 0 | `2862d6bb2537c03376c8d347bdd07040496a597c365e5481f1059cd24183553e` |
| `region-talk-external-2026-08-02-063021` | 14 | 10 | 4 | 0 | 14 | 0 | `2f6f4f4c3e1ef63c426332edf182dc6b6851544d0d5ca9c5cefa5ac4c9c300de` |
| `region-talk-external-2026-08-01-163142` | 20 | 10 | 10 | 20 | 0 | 0 | `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7` |

Итого: **54** валидных candidate rows; **32** направляются только в штатный LLM/scoring pipeline; **22** остаются manual/blocked. Новых intake: **20**; replay: **34**; конфликтов: **0**.

Импорт не выдавал разрешение на публикацию, не повышал `manual_review_required` и не запускал Telegram/VK publishing.
