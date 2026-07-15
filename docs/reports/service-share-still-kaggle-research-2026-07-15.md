# Карточка «Полюбить Калининград Анонсы»: перенос still-рендера на Kaggle

Дата проверки: 2026-07-15 (Europe/Kaliningrad)
Статус: research-only, без production deploy, публикации и изменения production БД

## Результат

- Исправлена семантика даты: у каждого события дата находится на его фото-текстуре рядом с названием; общей подписи вида `ДАННЫЕ НА …` в карточке нет.
- Зафиксирована согласованная сцена с бесшовной циклорамой, близким широкоугольным hero-кубом, цепочкой разнонаправленных кубов и бренд-биркой, приклеенной к верхнему краю без перспективного искажения.
- Локальный v7 master завершён в `1024×1024 / 256 samples`.
- Рендер перенесён в отдельный приватный script-kernel `ServiceShareStill`: быстрый gate на Kaggle GPU, затем обязательный финал на Kaggle CPU по тому же SHA-256 bundle.
- Входные события формируются воспроизводимой смесью `3 популярных + 2 промотируемых + 3 псевдослучайных`.
- Каждый новый принятый visual result отправлен в Telegram Saved Messages; production-каналы не затронуты.

## Даты на событиях

`scripts/research/prepare_service_share_faces.py` создаёт квадратную текстуру каждого события после честного кадрирования:

- одиночная дата: `24 ИЮЛЯ`;
- диапазон: `ДО 30 ИЮЛЯ`;
- название и дата наносятся после `cover/contain`, поэтому не участвуют в обрезке исходной фотографии;
- `safe_crop=true` использует центрированный cover, иначе сохраняется полное изображение поверх размытого фона;
- манифест хранит source/face SHA-256, размеры, crop mode, title и date label.

Композиционный слой `scripts/research/service_share_poster_cubes/composite_product.py` не содержит глобальной snapshot-даты. Runtime QA и launcher отдельно требуют:

```json
{
  "global_snapshot_date_present": false,
  "event_dates_on_faces": true
}
```

## Выбор событий

Источник research-прогона: локальный статический snapshot `site/src/data/preview-events.json`; дата отбора — `2026-07-15`. После фильтра `active + current/future + image` осталось `199` кандидатов.

Алгоритм `scripts/research/select_service_share_events.py`:

1. **popular** — сортировка по тому же составному сигналу реакций, репостов, просмотров и числа источников, который используется статическим сайтом;
2. **promoted** — только явный список активных promo festival/campaign, переданный вызывающей стороной; selector не создаёт и не изменяет кампании;
3. **random** — стабильная дневная ротация по `SHA-1(service_share_card|local_date|event_id)`, чтобы результат был разнообразным, но воспроизводимым;
4. media preflight удаляет недоступные изображения и пересобирает смесь до полного `3/2/3`; в проверенном прогоне так был отклонён event `5298`.

Проверенная выборка:

| Группа | Event ID | Событие | Дата/диапазон |
|---|---:|---|---|
| popular | 6466 | Посещение «Планеты Океан» и подводной лодки Б-413 | до 30.07.2026 |
| popular | 5736 | «Мещане» | 06.09.2026 |
| popular | 3216 | «Великие учителя» | до 27.09.2026 |
| promoted | 5077 | Калининград и область как кинодекорация… | до 03.08.2026 |
| promoted | 3592 | Лекция Евгения Мосийенко… | до 19.07.2026 |
| random | 6317 | «Лебединое озеро» | 02.11.2026 |
| random | 5665 | «Симфоническая пятница» | 31.07.2026 |
| random | 6333 | Дегустация сыров и вин | 26.07.2026 |

Для research-прогона явным promo-входом была активная фестивальная кампания `80 историй о главном`. У неё нет отдельного surface `service_share_card`, поэтому это не утверждение о production-таргетинге карточки. До productization нужен отдельный read-only resolver соответствующего surface; при отсутствии двух eligible promo событий рендер должен fail/underfill явно, а не выдавать случайные события за промо.

## Kaggle runtime

Канонические файлы:

- `kaggle/ServiceShareStill/service_share_still.py` — artifact-only runtime;
- `kaggle/ServiceShareStill/kernel-metadata.json` — приватный CPU-safe baseline;
- `scripts/run_service_share_still_kaggle.py` — staging, bundle hashing, private dataset, push, polling, download, QA и cleanup;
- `scripts/research/service_share_poster_cubes/` — portable Blender scene, product composite и точные бренд-ассеты.

Профили разделены:

| Profile | Kernel | Device | Resolution | Samples | Gate |
|---|---|---|---:|---:|---|
| `debug-gpu` | `service-share-still-debug` | T4 / Cycles OPTIX | 512 | 24 | actual device обязан быть GPU |
| `final-cpu` | `service-share-still-final` | CPU | 1024 | 256 | требуется успешный GPU result с тем же bundle SHA |

Bundle детерминирован: нормализованы path order, uid/gid и mtime; `.env`, Kaggle credentials и Telegram sessions внутрь не копируются. Нейтральное расширение `.tarball` не позволяет Kaggle автоматически распаковать архив и потерять проверяемую hash boundary. Run dataset удаляется после скачивания результата.

### Внутренние статусы

Runtime получает обычные `kaggle_run.json` и `kaggle_status_client.py`, пишет redacted `kaggle_status_events.jsonl` и одновременно печатает структурированные строки `[service_share_status]`. Launcher во время выполнения опрашивает Kaggle state и kernel logs, а после completion скачивает JSONL.

Business progress: `kernel_started 2% → preflight_ok 15% → render_started 20% → alive samples N/total → composite 86% → qa 94% → render_done 98% → report_written 100%`.

Research launcher намеренно передаёт пустой callback URL/token и не пишет в production `kaggle_run_ledger`: это сохраняет инфраструктурный runtime contract и полноценный внутренний polling, но не мутирует production DB. Для server-created production run тот же helper может получить подписанный callback стандартным путём Kaggle status framework.

## Live evidence

Один и тот же bundle: `1f296b61485cb4b6a6f1c62d2c91d1c470a38d02d2f36dd29b4bfb55361a4709`.

### GPU debug — принят

- run: `service-share-still-debug-gpu-20260714T235812Z`;
- actual device: `GPU`, backend `OPTIX`, `Tesla T4`;
- `512×512`, `24 samples`, `57.215 s`;
- output SHA-256: `00007563feea1a93f02cee44dd0014ddc0f5aa1981b8f90948885fc910395836`;
- black-frame guard: `near_black_fraction=0.00042343`;
- Saved Messages: message `32242`.

### CPU final — принят

- run: `service-share-still-final-cpu-20260715T000152Z`;
- actual device: `CPU`;
- `1024×1024`, `256 samples`, `192.919 s`;
- output SHA-256: `72ccf2bfc450d6d95862c8436aa83a61f54f89b486138624a137ad4ebed2a30e`;
- black-frame guard: `near_black_fraction=0.00074768`;
- Saved Messages: message `32243`.

Local v7 master был отправлен ранее как message `32239`; portable local smoke — `32240`. Первый Kaggle diagnostic с найденным compositing defect сохранён как отклонённый message `32241`, после чего порядок Pillow alpha-composite/draw исправлен и добавлен black-frame gate.

Артефакты прогонов находятся в `artifacts/codex/service-share-poster-cubes-research-v7/` и не коммитятся.

## Воспроизведение

```bash
python3 scripts/research/select_service_share_events.py \
  --events-json site/src/data/preview-events.json \
  --output artifacts/codex/service-share-poster-cubes-research-v7/selection_manifest.json \
  --local-date 2026-07-15 \
  --promo-festival '80 историй о главном' \
  --preflight-media

python3 scripts/research/prepare_service_share_faces.py \
  --selection artifacts/codex/service-share-poster-cubes-research-v7/selection_manifest.json \
  --output-dir artifacts/codex/service-share-poster-cubes-research-v7/kaggle_faces \
  --bold-font assets/fonts/Cygre-ExtraBold.ttf \
  --semibold-font assets/fonts/Cygre-SemiBold.ttf

python3 scripts/run_service_share_still_kaggle.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --bundle-dir artifacts/codex/service-share-poster-cubes-research-v7 \
  --profile debug-gpu

python3 scripts/run_service_share_still_kaggle.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --bundle-dir artifacts/codex/service-share-poster-cubes-research-v7 \
  --profile final-cpu \
  --require-debug-result artifacts/codex/service-share-poster-cubes-research-v7/kaggle/<accepted-debug-run>/service_share_render_result.json
```

## Ограничения productization

- Сейчас это исследовательский генератор и приватные Kaggle kernels, не scheduler job.
- Product counts `274` и `+75` были зафиксированы как принятая copy snapshot этой итерации; production job должен передавать заново проверенные счётчики, а не считать эти значения вечными.
- Нужен отдельный promo surface/resolver для карточки сервиса.
- Перед production enablement нужны signed callback/ledger, retention policy для готовых masters и явный delivery target; текущая задача сознательно не меняла эти поверхности.
