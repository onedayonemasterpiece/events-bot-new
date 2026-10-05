# Smart Update Ultra — воспроизведение компонентного стенда

Это исследовательские скрипты, не production-конвейер. Нет записи в БД и публикации. Родительская ветка — draft PR #723; в ней находятся [основной отчёт](devcoveer-pilot-2026-10-05.md) и [канонический вход](../../features/smart-update-ultra/README.md).

## Сохранённый запуск

`/home/dev/artifacts/events-bot-new/20261005T073316Z-ultra-public-component-20261005`

Каталог retained с `.artifact.json`: `cases/` — исходные сообщения, `assembled/` — отдельно собранные альбомы, `media/` — изображения, `native/` — выбранные session exports без hidden reasoning, `reviews/` — content-addressed результаты проверок. Данные и credentials не коммитятся. Одноразовые bootstrap/permission-schema helpers перенесены с проверкой байтов в `source-diagnostics/20261005T082023Z`, а не оставлены в исходниках.

## Офлайн-проверки

```bash
python3 tests/test_ultra_lab_contract.py
python3 scripts/inspect/ultra_review_receipts.py --root "$LAB_ROOT" --save
python3 scripts/inspect/ultra_read_assembled.py --root "$LAB_ROOT"
```

Подтверждены **21 unittest**. Проверяются enums, типы, согласованность disposition/events/actions, сохранение неполноты evidence и границы альбомов. Это диагностический поднабор контракта: нет полной проверки семантики, OCR accuracy или domain commit. Отсутствующий evidence receipt в loose text-v1 отмечается как production readiness gap, а не нарушение поля, которое было явно задано модели.

`ultra_review_receipts.py` читает пять конкретных native trials и сохраняет review по хэшу содержимого без изменения outputs. Времена — wall time сообщений/задачи, не чистое inference time. Исходные immutable evidence digests получены через `get_task_evidence` и сохранены в отчёте.

## Публичные Telegram-источники

Используется только существующая локальная identity через `telegram-e2e-run`; не S22/VibePublish. Wrapper устанавливает только Telethon 1.42.0 с зависимостями в `deps/` managed-каталога при отсутствии, не полный application venv. Сначала проверить свободное место.

```bash
bash scripts/inspect/ultra_lab_tg.sh "$LAB_ROOT" --limit 3 agropark39 ambermuseum
bash scripts/inspect/ultra_lab_albums.sh "$LAB_ROOT" --seed tg-agropark39-2342 --seed tg-tanja_from_koenigsberg-4164 --seed tg-progulki_s_katey-812
```

Для новой выборки создать новый retained-каталог штатным `dev-artifacts`. Не заменять frozen capture более свежими данными по старому имени.

Известная граница: текущий assembly-probe требует другое сообщение до и после альбома. У последнего альбома канала правого соседа может не быть. `album_bounds_unconfirmed` здесь означает недостаточность метода подтверждения, **не доказанную потерю файлов**. Нужен следующий тест stable-frontier/quiet-window; он ещё не реализован. Нельзя ждать следующего поста бесконечно или превращать этот случай в no-event.

## OpenCode

Реальные успешные ответы получены через штатный DevCoveer `start_task`, точные бесплатные provider/model, `access=read` и явный запрет побочных эффектов. `ultra_native_lab.py` ограничен manifest task IDs и конкретными public inputs.

```bash
python3 scripts/inspect/ultra_native_lab.py inspect
python3 scripts/inspect/ultra_native_lab.py export --task <сохранённый-task-id>
```

`approve` допустим только после осмотра ожидающего запроса на exact read уже авторизованного файла; используется `once`, не глобальное разрешение. Неожиданный task/path/permission прекращает операцию.

Export-прототип сохраняет snapshot с именем task ID. **Не экспортировать поверх V1 после continue_task**: история задачи изменится. Для новой prompt-версии нужен отдельный trial/receipt. Create-only generation-aware exporter — дальнейшая доработка; не приписывать её текущему коду.

`ultra_component_lab.py run` — **непринятый direct-session эксперимент**, вернувший 403 FreeTierError. Не использовать для массового запуска, не обходить ограничение провайдера. Его наличие в исследовательском исходнике не означает рабочий batch-runner. Точные диагностические receipts сохранены отдельно.

## Открытые этапы

Согласованный production snapshot; live VK capture; независимый golden/holdout; новые vision-модели; фактические CREATE/MERGE/replay через Smart Update на shadow DB; runtime/API recovery; перенос в Kaggle. Зелёные unit tests не закрывают эти этапы. Guide-источники используются отдельным набором: profile/template/occurrence и digest eligibility не смешиваются с обычными event rows.
