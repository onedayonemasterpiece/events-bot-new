# Неполная фотогалерея в опубликованных анонсах ВК

Дата 10.11.2026. Severity sev3. Реестр `inc_af96f6510a55a8ccc1d713dc`. Статус открыт до проверки в приложении ВК.

## Наблюдение и причина

Владельцу попадались обычные анонсы с одним фото. Независимая проверка качества сопоставила записи на стене, одобренные изображения событий и время последних заданий.

| Событие | Стена ВК до исправления | Одобрено | Наблюдение |
| --- | --- | ---: | --- |
| 10705, лекция об управлении финансами | [13825](https://vk.com/wall-231920894_13825), 1 фото | 2 | Второй снимок одобрен 10 ноября после последнего `vk_sync` 8 ноября |
| 10704, «Мария Стюарт» | [13856](https://vk.com/wall-231920894_13856), 1 фото | 2 | Второй снимок одобрен 10 ноября после последнего `vk_sync` 9 ноября. В общем исходном посте шесть фото |
| «Кальмания» | [13855](https://vk.com/wall-231920894_13855), 2 фото | 2 | Контрольный пример полной галереи |

Медиаревью довело по два изображения до approved projection в двух проблемных событиях. При следующем fanout `_managed_vk_post_state` принимал любое фото за полную непустую галерею. Эту проверку используют планировщик, повторная постановка завершённого задания и проверка идемпотентности `job_sync_vk_source_post`. Поэтому у планировщика не было причины поставить редактирование. При неполном количестве это происходит ещё до новой попытки загрузки. Сколько из шести кадров источника относятся к конкретному спектаклю, определяет обычное медиаревью.

У режима выкладки была отдельная причина: обычный event publisher не выбирал карусель. Общий VK upload уже поддерживал несколько фото. [Обсуждение и исходные наблюдения](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1794342920292269).

## Treat as regression guard when

При изменениях approved-галереи, полноты managed VK-поста, `vk_sync`, загрузки обложки и режима `wall.post` / `wall.edit`. Сохраняются контракты [дублей и частичной загрузки](INC-2026-06-09-event-media-duplicates.md), [разных визуально похожих снимков](INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped.md), [сбора изображений из нескольких источников](INC-2026-05-05-event-source-media-aggregation-gap.md) и [неопределённого исхода публикации](INC-2026-08-27-eventsbot-vk-image-publish-outcome-unknown.md).

## Mandatory checks before closure or deploy

`tests/test_event_media_gate.py::test_managed_vk_gallery_publish_and_edit_replay` проверяет оба layout, зеркало тех же байтов, второй approved-кадр, обновление недостающего вложения, сохранение текста и слота, единственный `wall.post`, смену формата с явным или отсутствующим режимом ВК, сохранение старых вложений и хеша при сбое замены [A,B] → [A,C], затем один edit на изменение и повтор без загрузок. Отдельные примеры проверяют прозрачную афишу и уменьшение JPEG 6000×4000 до полного декодирования. Существующие проверки медиагейта покрывают одинаковые пиксели в разных контейнерах, визуально разные снимки и исключение pending/rejected. Существующие проверки VK покрывают частичную загрузку и сохранение вложений, captcha, защищённое восстановление ID и различие между отказом до `wall.post` и неопределённым исходом самого `wall.post`.

## Required evidence и небольшой backfill

После выпуска координатор запускает этот блок на стенде. Первый запуск выбирает до двух действительно изменённых записей каждого формата из последних 100 постов. Совпадение числа фото и формата сетки отмечается отдельно как контроль по этим двум признакам. Старой карусели загружается новая первая картинка с подсказкой. Источник фото — упорядоченные approved-записи медиаревью. Исходный текст, слот и конкретный wall ID сохраняются.

Для повторной приёмки запустить тот же блок. Если в файле есть принятые записи, команда читает только эти wall ID и передаёт редактору уже принятые ID вложений, в том числе обложки. Повтор проверяет, что каждая запись всё ещё совпадает, а обычный редактор возвращает no-op.

```bash
set -o pipefail
receipt=/home/dev/artifacts/events-bot-new/20261110T203533Z-vk-multiphoto-crew/accepted-vk-posts.jsonl
touch "$receipt"
{
python3 -c 'import json, sys; print("accepted =", [json.loads(line) for line in sys.stdin])' < "$receipt" &&
cat <<'PY'
import asyncio
import json
import sys
from datetime import datetime, timezone

import main
from event_media import get_event_gallery_urls

async def wall_item(ref):
    response = await main.vk_api("wall.getById", posts=ref, _force_user_actor=True)
    return next(iter(main._vk_wall_get_by_id_items(response)), None)

def photo_ids(item):
    return [f"photo{a['photo']['owner_id']}_{a['photo']['id']}" for a in item["attachments"]]

async def check_accepted(record):
    ref, mode, ids = record["ref"], record["mode"], record["ids"]
    url = "https://vk.com/wall" + ref
    item = await wall_item(ref)
    assert item and f"{item['owner_id']}_{item['id']}" == ref
    assert (item["text"], item["date"]) == (record["text"], record["date"])
    assert item.get("primary_attachments_mode") == mode and photo_ids(item) == ids
    edited = await main.edit_vk_post(url, item["text"], attachments=ids, primary_attachments_mode=mode)
    assert not edited
    print(url, mode, len(ids), "повтор без изменения", file=sys.stderr)

async def backfill():
    if accepted:
        for record in accepted:
            await check_accepted(record)
        return

    db = main.Database("/data/db.sqlite")
    group = str(main.VK_EVENTS_GROUP_ID or main.VK_AFISHA_GROUP_ID).lstrip("-")
    owner = -int(group)
    result = await main.vk_api("wall.get", owner_id=owner, filter="owner", count=100)
    recent = main._vk_postponed_items(result)
    changed = {"carousel": 0, "grid": 0}
    controls = 0
    for post in sorted(recent, key=lambda p: p.get("date", 0), reverse=True):
        if all(n == 2 for n in changed.values()):
            break
        if post.get("owner_id") != owner or post.get("copy_history") or post.get("is_deleted"):
            continue
        if datetime.now(timezone.utc) - datetime.fromtimestamp(post["date"], timezone.utc) > main.VK_POST_MAX_EDIT_AGE:
            continue
        ref = f"{owner}_{post['id']}"
        url = "https://vk.com/wall" + ref
        async with db.get_session() as session:
            matches = (await session.execute(main.select(main.Event).where(
                main.Event.source_vk_post_url == url, main.Event.lifecycle_status == "active",
                main.Event.silent == False,
            ))).scalars().all()
            if len(matches) != 1:
                continue
            event_id = matches[0].id
            approved = await get_event_gallery_urls(session, event_id, legacy_fallback=False)
            urls = (await main._dedupe_event_photo_urls_for_publish(approved))[:main.VK_MAX_ATTACHMENTS]
        mode = main._vk_event_gallery_mode(event_id, len(urls))
        if mode is None or changed[mode] == 2:
            continue
        item = await wall_item(ref)
        if not item or item.get("can_edit") != 1 or item.get("is_deleted") or item.get("copy_history"):
            continue
        old = item.get("attachments", [])
        if any(a.get("type") != "photo" for a in old):
            continue
        if mode == "grid" and len(old) == len(urls) and item.get("primary_attachments_mode") == mode:
            controls += 1
            print(url, "контроль: совпали число фото и сетка", file=sys.stderr)
            continue
        ids, uploaded_mode = await main._upload_vk_event_gallery(group, urls, event_id, db, None)
        if len(ids) != len(urls) or uploaded_mode != mode:
            print(url, "неполная загрузка, пропуск", file=sys.stderr)
            continue
        try:
            edited = await main.edit_vk_post(url, item["text"], db, None, ids, primary_attachments_mode=mode)
        except main.VKAPIError as error:
            if error.code == 15 and "edit time expired" in str(error).lower():
                continue
            raise
        if not edited:
            controls += 1
            continue
        record = dict(ref=ref, mode=mode, ids=ids, text=item["text"], date=item["date"])
        await check_accepted(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
        changed[mode] += 1
    print("Изменено", changed, "контрольных совпадений", controls, file=sys.stderr)
    await db.engine.dispose()

async def run():
    try:
        await backfill()
    finally:
        await main.close_http_session()

asyncio.run(run())
PY
} | ssh runcoveer-production 'sudo -n docker exec -i -w /app deployment-events-1 python -' | tee -a "$receipt"
```

Если пригодных записей меньше четырёх, зафиксировать фактическое число изменений и отдельно количество контрольных совпадений. По принятым ссылкам проверить сетку, первый кадр карусели и листание до второго отличающегося фото на компьютере и в приложении ВК. Проверить читаемость надписи и сохранность нижней части исходника. Указать клиент, ссылки и результаты в общем треде. Эмулятор обозначить отдельно. После readback и независимой визуальной проверки координатор добавляет release evidence.
