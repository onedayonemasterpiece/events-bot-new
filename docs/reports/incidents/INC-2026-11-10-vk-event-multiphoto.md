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

`tests/test_event_media_gate.py::test_managed_vk_gallery_publish_and_edit_replay` проверяет оба layout, зеркало тех же байтов, второй approved-кадр, обновление недостающего вложения, сохранение текста и слота, один `wall.post`, один `wall.edit` и повтор без загрузок. Существующие проверки медиагейта покрывают одинаковые пиксели в разных контейнерах, визуально разные снимки и исключение pending/rejected. Существующие проверки VK покрывают частичную загрузку и сохранение вложений, captcha, защищённое восстановление ID и различие между отказом до `wall.post` и неопределённым исходом самого `wall.post`.

## Required evidence и небольшой backfill

После выпуска координатор выполняет команду со стенда. Она читает последние записи сообщества и текущие approved-галереи, берёт до двух редактируемых анонсов каждого формата, работает с конкретными wall ID через обычные upload и `edit_vk_post`. Текст берётся со стены. Время публикации не передаётся в edit. Удалённые записи, репосты, смешанные вложения и неоднозначные привязки пропускаются. Команда не содержит вызова создания поста.

```bash
ssh runcoveer-production 'sudo -n docker exec -i -w /app deployment-events-1 python -' <<'PY'
import asyncio
from datetime import datetime, timezone
import main
from event_media import get_event_gallery_urls

async def backfill():
    db = main.Database("/data/db.sqlite")
    group = str(main.VK_EVENTS_GROUP_ID or main.VK_AFISHA_GROUP_ID).lstrip("-")
    owner = -int(group)
    result = await main.vk_api("wall.get", owner_id=owner, filter="owner", count=100)
    recent = main._vk_postponed_items(result)
    chosen = {"carousel": 0, "grid": 0}
    for post in sorted(recent, key=lambda p: p.get("date", 0), reverse=True):
        if all(n == 2 for n in chosen.values()):
            break
        if post.get("owner_id") != owner or post.get("copy_history") or post.get("is_deleted"):
            continue
        if any(a.get("type") != "photo" for a in post.get("attachments", [])):
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
            event = matches[0]
            approved = await get_event_gallery_urls(session, event.id, legacy_fallback=False)
            urls = (await main._dedupe_event_photo_urls_for_publish(event.photo_urls))[:main.VK_MAX_ATTACHMENTS]
            if len(urls) < 2 or not set(urls).issubset(approved):
                continue
        mode = "carousel" if event.id % 2 else "grid"
        if chosen[mode] == 2:
            continue
        current = main._vk_wall_get_by_id_items(await main.vk_api(
            "wall.getById", posts=ref, _force_user_actor=True,
        ))
        if not current or current[0].get("can_edit") != 1:
            continue
        item = current[0]
        original = (item["text"], item["date"])
        old = item.get("attachments", [])
        if item.get("is_deleted") or any(a.get("type") != "photo" for a in old):
            continue
        if len(old) == len(urls) and item.get("primary_attachments_mode") == mode:
            chosen[mode] += 1
            print(event.id, url, mode, len(old), "без изменений; проверить визуально")
            continue
        ids, carousel = await main._upload_vk_event_gallery(group, urls, event.id, db, None)
        if len(ids) != len(urls):
            print(url, "неполная загрузка, пропуск")
            continue
        try:
            await main.edit_vk_post(url, original[0], db, None, ids, carousel=carousel)
        except main.VKAPIError as error:
            if error.code == 15 and "edit time expired" in str(error).lower():
                continue
            raise
        after, = main._vk_wall_get_by_id_items(await main.vk_api("wall.getById", posts=ref))
        assert (after["text"], after["date"]) == original
        assert after.get("primary_attachments_mode") == mode
        actual = [f"photo{a['photo']['owner_id']}_{a['photo']['id']}" for a in after["attachments"]]
        assert actual == ids
        assert not await main.edit_vk_post(url, original[0], db, None, ids, carousel=carousel)
        chosen[mode] += 1
        print(event.id, url, mode, len(ids), "повтор без изменения")
    print("Проверено", chosen)
    await main.close_http_session()
    await db.engine.dispose()

asyncio.run(backfill())
PY
```

Если недавних подходящих записей меньше четырёх, сохранить фактическое число. Для проверки с телефона и desktop открыть напечатанные ссылки. Снять сетку и первый кадр карусели целиком, пролистать до второго отличающегося фото, проверить читаемость «листай →» и сохранность нижней части исходника. Зафиксировать тип клиента, ссылки и результаты в общем треде. Эмулятор обозначить отдельно. После readback и независимой визуальной проверки координатор добавляет release evidence.
