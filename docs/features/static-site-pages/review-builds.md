# Реестр опубликованных сборок KenigEvents

Единый машиночитаемый список именованных тестовых сборок:
[`review-builds.yaml`](review-builds.yaml). Записи отсортированы по времени
записи входного HTML-объекта в Object Storage. `latest_named_preview` указывает
на последнюю доступную **именованную** сборку, а не на самый свежий каталог
событий или принятый релиз. Начальный backfill покрывает все 96 сохранённых в
бакете `preview-*` префиксов на 26 сентября 2026 года. Сборки, удалённые до
этого backfill, нельзя восстановить из бакета; новые удаления останутся в
реестре с `present_in_bucket: false` и фактическим HTTP-статусом.

## Что означает каждая дата

- `built_at` — `generatedAt` из опубликованного `preview-build.json`, если он есть.
- `bucket_entry_modified_at` — время записи фактического HTML в бакет; это
  наблюдаемая дата публикации, но повторная загрузка может её изменить.
- `reference_date` — дата, для которой рендерили интерфейс; она не доказывает
  свежесть входного каталога.
- `repo_catalog_generated_at` — дата каталога `preview-events.json` в
  зафиксированном commit; это признак, а не доказательство идентичности с
  опубликованным HTML, если сборка использовала незакоммиченные данные.
- `catalog_snapshot_at` — дата входного каталога, подтверждённая отдельным
  источником и добавленная при ревью. Для сентябрьских UI-превью это 23 июля.

`public_url` — открываемый маршрут на `kenigevents.ru`; `bucket_url` — точный
HTML-объект в бакете. `http_status_at_sync` и `bucket_http_status_at_sync`
фиксируют ответы обоих адресов только на момент синхронизации. `source_sha`,
`source_commit`, `changes` и `review_url` позволяют
найти код и правки, ради которых выпускали сборку. Неподтверждённые подробности
не следует выводить из одного лишь имени префикса.

## Operator preflight после обновления DevCoveer

Не ищите локальный .env и не запускайте yc init, пока не проверен существующий
trusted-host контур:

    python3 scripts/sync_static_site_preview_registry.py       --operator-preflight       --fly-app events-bot-new-wngqia

ready=true означает, что DevCoveer видит штатный Fly control host, а на нём
присутствуют Kaggle и Yandex Object Storage credentials. Fly не хостит
статический сайт: конечные preview-объекты остаются в kenigevents.ru Object
Storage/CDN.

## Обновление

Локальные npm build:preview/check:preview используются только для быстрой
диагностики. Публикуемая review-сборка проходит единый rail:

    python scripts/run_static_site_builder_kaggle.py       --kernel-ref zigomaro/kenigevents-static-site-builder-review-preview       --db <immutable-production-projection.sqlite>       --repo-sha <exact-40-character-SHA>       --profile preview --preview-data-mode real --catalog-mode slice       --page-class all       --build-id <new-preview-build-id>       --asset-base-url https://static.kenigevents.ru       --astro-asset-base-url https://static.kenigevents.ru/{buildId}       --download-output --publish-preview

После успешной публикации обновите только фактическую запись нового build id и
проверьте реестр:

    python3 scripts/sync_static_site_preview_registry.py       --fly-app events-bot-new-wngqia       --build-id <new-preview-build-id>
    python3 scripts/sync_static_site_preview_registry.py --check

Registry sync читает бакет через уже настроенные права Fly, не выводит ключи и
не запускает сборку. Существующие ручные changes, review_url,
catalog_snapshot_at и notes сохраняются. Перед отправкой ссылки откройте
public_url и проверьте, что catalog_snapshot_at соответствует обещанной
свежести данных. Старый локальный npm deploy:preview не является каноническим
publisher и не должен использоваться для новой review evidence.

Полные `/_review/<token>/` кандидаты живут в отдельном безопасном контуре.
Репозиторий публичный, поэтому bearer URL не записывается в YAML. Реестр
хранит только безопасные реквизиты текущего полного кандидата; его ссылку
получают read-only командой `--show-current-review` на Fly и передают владельцу
в закрытом ревью-треде. Старые `_review` деревья могут быть удалены retention
job, поэтому ранее отправленная ссылка не заменяет текущий указатель.
