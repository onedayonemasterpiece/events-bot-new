# INC-2026-11-27: комментарии не находят опубликованный анонс

Статус: open. Владелец исправления: KenigEvents root. Источник — read-only проверка production 27 ноября12:49–12:52 UTC.

## Наблюдение

Событие11515 «Люди и судьбы: калининградское время»: Event и два frozen payload содержат отложенный16159, опубликованный receipt77828 связывает его с16180. Direct service GET подтвердил публичный16180 с тремя похожими в теле и пустым списком комментариев. Calendar171576 и related171577 сделали по11 неуспешных попыток с `vk_calendar_public_post_not_found` и `vk_relations_post_missing`.

Canonical дата27ноября, time19:00, конец18декабря; публичный заголовок содержит диапазон без времени. Время изменения canonical фактов не установлено. Отсутствие поста в первых100 или точную историческую причину header mismatch не утверждаем.

## Общая причина и исправление

Оба normal job paths обходили готовую связь stored→live из `event_publication` и заново сопоставляли mutable заголовок. Календарный ICS consumer дополнительно выбирал строки только по live URL и потерял бы события, у которых остался stored URL.

Используем единственный published receipt своего target для frozen stored URL; проверяем прямым service GET физический owner/ID, публичность и исходный postponed ID либо frozen publication timestamp. Heuristic wall_scan receipt сам по себе не разрешает публикацию. Конфликтующий receipt или public identity даёт ошибку, а не иной адрес. Без receipt работает существующий путь. Calendar принимает old/live aliases одного поста, lifecycle-фильтры сохранены; related использует ту же frozen выборку и уже прочитанный item. Canonical факты/URL, старые посты и отправленные комментарии не редактируются.

## Различающая проверка

`tests/test_vk_comment_publication_receipt.py`: actual job entrypoint с fixture DB и fake VK API. На исходном main оба positive controls падают точными production errors; negative controls не создают комментарий. После исправления проверяются одна отправка на live ID и отсутствие дубля при повторе, ICS membership по old/live aliases, отсутствие canonical URL writes, frozen выборка, conflicts/date/owner/deleted и посторонние receipts. Изолированный контейнер без сети и production DB/env. Before: 2 FAIL/10 PASS; positive controls падают точными production errors. Первый after: 64 PASS/2 FAIL из-за fixture, где конфликтующий receipt был законно исключён по canonical URL. Fixture исправлен, без расширения SQL. Итоговый expanded suite: 82 PASS/26.95s; сеть отключена, production DB/env не подключались. Отдельный simplicity guard SOURCE PASS: +27 production lines, дополнительные7 к первоначальной оценке нужны для physical identity и old/live ICS membership. Final SOURCE exact5227df353 дал CHANGES: legacy payload без publish_date ошибочно перекрывался receipt, а доказанный identity conflict повторялся без конца. Root восстановил legacy resolver для неполного payload; доказанный конфликт использует существующую terminal классификацию обеих разновидностей комментария, missing/not-public остаётся retryable. Контрпримеры на прежнем коде: 13 FAIL / 100 PASS. Исправленный expanded suite: 113 PASS / 74.04s, без сети и production DB/env. Native followup simplicity guard SOURCE PASS. Net production +29. Rollout ещё не запускался.

## Живая приёмка

Не выполнена. Требуются normal main release, продолжение существующих jobs и фактический единичный комментарий. Ручной replay, DB reset и массовый backfill не разрешены. Ранее выпущенный PR1113 и его CUTOVER_USED не повторять.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/event11515-*`.
