# Region Talk: результаты внешнего поиска публикаций

Status: **validated research evidence / automated YDB intake**. JSON-файлы соответствуют контракту `region_talk_external_research.v1`; чистые `candidate_report` поступают только в штатный Region Talk pipeline проверки и скоринга. `manual_review_required` остаётся непубликуемым и автоматически не повышается. Импорт в YDB не является разрешением на публикацию.

## Исторические неизменяемые пакеты

| Выполнено, UTC | JSON | Кандидаты | Результат текущего importer | SHA-256 |
|---|---|---:|---|---|
| 2026-07-31 12:50:52 | [`region-talk-external-research-result-region-talk-external-2026-07-31-125052.json`](region-talk-external-research-result-region-talk-external-2026-07-31-125052.json) | 20 | 5 semantic rejections; заменён successor `region-talk-external-2026-08-02-063020`, исторический файл не импортировать | `59b1d7cc43fff8eabe53f4f8b84b700d1c5ebc60f326b9f3f8c2d208999bc2cf` |
| 2026-07-31 17:40:33 | [`region-talk-external-research-result-region-talk-external-2026-07-31-174033.json`](region-talk-external-research-result-region-talk-external-2026-07-31-174033.json) | 14 | 1 semantic rejection; заменён successor `region-talk-external-2026-08-02-063021`, исторический файл не импортировать | `c040269f09bd72f16cf74fe2f721d9b8375ede82bd3742ce989e406747384cb0` |
| 2026-08-01 16:31:42 | [`region-talk-external-research-result-region-talk-external-2026-08-01-163142.json`](region-talk-external-research-result-region-talk-external-2026-08-01-163142.json) | 20 | clean: 20 valid / 0 rejected; допускается идемпотентный импорт или replay | `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7` |

## Актуальные импортируемые пакеты

| Request ID / выполнено UTC | JSON | Кандидаты | `candidate_report` | `manual_review_required` | Исключено | Unresolved | Проверка | SHA-256 |
|---|---|---:|---:|---:|---:|---:|---|---|
| `region-talk-external-2026-08-02-063020` / 2026-08-02 06:30:20 | [`region-talk-external-research-result-region-talk-external-2026-08-02-063020.json`](region-talk-external-research-result-region-talk-external-2026-08-02-063020.json) | 20 | 12 | 8 | 2 | 2 | strict schema + importer semantic validation: clean; successor of `region-talk-external-2026-07-31-125052` | `2862d6bb2537c03376c8d347bdd07040496a597c365e5481f1059cd24183553e` |
| `region-talk-external-2026-08-02-063021` / 2026-08-02 06:30:21 | [`region-talk-external-research-result-region-talk-external-2026-08-02-063021.json`](region-talk-external-research-result-region-talk-external-2026-08-02-063021.json) | 14 | 10 | 4 | 3 | 4 | strict schema + importer semantic validation: clean; successor of `region-talk-external-2026-07-31-174033` | `2f6f4f4c3e1ef63c426332edf182dc6b6851544d0d5ca9c5cefa5ac4c9c300de` |
| `region-talk-external-2026-08-01-163142` / 2026-08-01 16:31:42 | [`region-talk-external-research-result-region-talk-external-2026-08-01-163142.json`](region-talk-external-research-result-region-talk-external-2026-08-01-163142.json) | 20 | 10 | 10 | 4 | 1 | strict schema + importer semantic validation: clean | `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7` |

Successor-пакеты заменяют два ошибочных исторических входа и не увеличивают число уникально найденных публикаций. Перед каждым `--execute` importer перечитывает live YDB, применяет canonical URL / DOI / normalized title+authors identity guard и выполняет запись атомарно. Повторный запуск становится replay/no-op и не создаёт дубликатов.

## Автоматический импорт

После попадания чистого JSON в `main` используется workflow `Import trusted Region Talk external-publication research`. Он сначала выполняет dry validation точных committed bytes, затем через GitHub OIDC получает короткоживущий Yandex IAM token и запускает атомарный staging import. Результаты фиксируются в SHA-addressed Actions artifact с `new_intake_ids`, replay и conflict counters.

Импорт не пишет `publication_candidate_item`, не выдаёт `publication_permission`, не вызывает Telegram/VK publishing и не повышает `manual_review_required`. Новая запись начинается как `unreviewed`; только `candidate_report` направляется в существующий LLM-first Region Talk pipeline.

## Правило пополнения

Новый результат добавляется под исходным именем `region-talk-external-research-result-<request_id>.json`. Исторические JSON не редактируются: любая правка выпускается отдельным successor с новым `request_id`, фактическим SHA-256 и кратким validation summary в этой странице.
