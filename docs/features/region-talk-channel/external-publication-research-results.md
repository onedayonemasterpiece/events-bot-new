# Region Talk: результаты внешнего поиска публикаций

Status: **research evidence / import queue**. Эти файлы являются результатами внешнего поиска по контракту `region_talk_external_research.v1`. Они не являются готовыми публикационными решениями: каждый файл должен пройти штатную валидацию, live-YDB duplicate guard и импорт в staging через `scripts/region_talk_external_publication_import.py`.

## Файлы

**Текущий fail-closed статус (проверен importer из workflow commit `a9c9d43e`, 2026-08-01):** первые два исторических входа сейчас не проходят semantic validation (соответственно 5 и 1 строка); их нельзя dispatch-ить частично и нужно создать исправленные successors с новыми `request_id`. Третий вход чист: 20 valid / 0 rejected, 63 planned YDB rows в dry-run. Полные причины, PR → `main` → protected manual dispatch, OIDC variables и audit evidence описаны в [guarded import runbook](external-publication-import-runbook.md). JSON-пейлоады ниже остаются неизменяемым историческим evidence.

Хронологический порядок сохраняется для **чистых** будущих входов: ранний успешный импорт обновляет durable duplicate ledger до обработки следующего файла.

| Выполнено, UTC | JSON | Кандидаты | `candidate_report` | Ручная проверка | Исключено | Не разрешено | SHA-256 |
|---|---|---:|---:|---:|---:|---:|---|
| 2026-07-31 12:50:52 | [`region-talk-external-research-result-region-talk-external-2026-07-31-125052.json`](region-talk-external-research-result-region-talk-external-2026-07-31-125052.json) | 20 | 14 | 6 | 2 | 2 | `59b1d7cc43fff8eabe53f4f8b84b700d1c5ebc60f326b9f3f8c2d208999bc2cf` |
| 2026-07-31 17:40:33 | [`region-talk-external-research-result-region-talk-external-2026-07-31-174033.json`](region-talk-external-research-result-region-talk-external-2026-07-31-174033.json) | 14 | 10 | 4 | 3 | 4 | `c040269f09bd72f16cf74fe2f721d9b8375ede82bd3742ce989e406747384cb0` |
| 2026-08-01 16:31:42 | [`region-talk-external-research-result-region-talk-external-2026-08-01-163142.json`](region-talk-external-research-result-region-talk-external-2026-08-01-163142.json) | 20 | 10 | 10 | 4 | 1 | `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7` |

Итого в трёх исследовательских пакетах: **54 кандидата**, из них **34 `candidate_report`** и **20 `manual_review_required`**; отдельно сохранены **9 исключённых** и **7 неразрешённых** страниц.

## Быстрая проверка и импорт

Из корня репозитория:

```bash
RESULTS=(
  docs/features/region-talk-channel/region-talk-external-research-result-region-talk-external-2026-07-31-125052.json
  docs/features/region-talk-channel/region-talk-external-research-result-region-talk-external-2026-07-31-174033.json
  docs/features/region-talk-channel/region-talk-external-research-result-region-talk-external-2026-08-01-163142.json
)

mkdir -p artifacts/codex/region-talk-external-publications

for file in "${RESULTS[@]}"; do
  name="$(basename "${file}" .json)"
  python3 scripts/region_talk_external_publication_import.py "${file}" \
    --report "artifacts/codex/region-talk-external-publications/${name}.dry-run.json"
done
```

После проверки dry-run отчётов — явный staging-import, также по порядку:

```bash
for file in "${RESULTS[@]}"; do
  python3 scripts/region_talk_external_publication_import.py "${file}" --execute
done
```

Импортёр повторно проверяет схему и текущий durable ledger, поэтому строки, уже ставшие известными после исследовательского запуска, должны быть отклонены как дубликаты, а не принудительно повторно импортированы.

## Правило пополнения

Новый проверенный результат добавляется в эту же папку под исходным именем `region-talk-external-research-result-<request_id>.json`. В таблицу выше добавляется новая строка с UTC-временем, количеством строк по статусам и SHA-256 фактически закоммиченного файла. Старые JSON-файлы не редактируются задним числом; исправленный пакет получает новый `request_id`.
