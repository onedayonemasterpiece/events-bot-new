# Неполная фотогалерея в опубликованных анонсах ВК

Дата 10.11.2026. Severity sev3. Реестр `inc_af96f6510a55a8ccc1d713dc`. Статус открыт до проверки в приложении ВК.

## Наблюдение и причина

### Дополнительные системные дефекты, 12 ноября

Read-only срез живой SQLite показал, что все 100 primary calls за 12 ноября
закончились до 01:52 UTC. Из них 79 пришлись на два события, добавленных 6
ноября: 10605 (51 вызов, уже семь approved кадров) и 10628 (28, десять
approved). У новых 10838/10840/10841 дополнительные кадры остаются deferred
с `media_review_primary_budget_exhausted`, provider calls равны нулю.
Наличие исправленного publisher и исторического backfill не доказывает
системную готовность обычного потока.

Отдельно подтверждены две механические ошибки: отказ локального allowance
увеличивал число automatic attempts и навсегда переводил нерассмотренные
пары в `unresolved`; после semantic rejection оставались runnable пары с уже
исключённым изображением. Исправление сохраняет попытку при нулевом вызове
из-за allowance и отменяет устаревшие пары до скачивания/provider admission.
Cancelled evidence не блокирует approval независимого кандидата.

Эта коррекция не меняет лимиты 100/25, не увеличивает расходы и не возвращает
исторические terminal rows. Происхождение лимитов — agent commit
`4e0dda76fd836ec87382fd7c8dd6d35d034d4cfd` от 13 июля; в исследованных
документах отдельного решения владельца или расчёта этого бюджета не найдено.
Политика capacity и сбор нескольких релевантных кадров сайтов остаются
открытыми: `parse_yantarhall_html` сохраняет только одну карточку каталога,
а общие swiper banners не являются доказательством кадров спектакля.
Системный критерий не закрыт до свежего обычного intake и автоматического
VK readback с несколькими изображениями после выпуска.

Владельцу попадались обычные анонсы с одним фото. Независимая проверка качества сопоставила записи на стене, одобренные изображения событий и время последних заданий.

| Событие | Стена ВК до исправления | Одобрено | Наблюдение |
| --- | --- | ---: | --- |
| 10705, лекция об управлении финансами | [13825](https://vk.com/wall-231920894_13825), 1 фото | 2 | Второй снимок одобрен 10 ноября после последнего `vk_sync` 8 ноября |
| 10704, «Мария Стюарт» | [13856](https://vk.com/wall-231920894_13856), 1 фото | 2 | Второй снимок одобрен 10 ноября после последнего `vk_sync` 9 ноября. В общем исходном посте шесть фото |
| «Кальмания» | [13855](https://vk.com/wall-231920894_13855), 2 фото | 2 | Контрольный пример полной галереи |

Медиаревью довело по два изображения до approved projection в двух проблемных событиях. При следующем fanout `_managed_vk_post_state` принимал любое фото за полную непустую галерею. Эту проверку используют планировщик, повторная постановка завершённого задания и проверка идемпотентности `job_sync_vk_source_post`. Поэтому у планировщика не было причины поставить редактирование. При неполном количестве это происходит ещё до новой попытки загрузки. Сколько из шести кадров источника относятся к конкретному спектаклю, определяет обычное медиаревью.

У режима выкладки была отдельная причина: обычный event publisher не выбирал карусель. Общий VK upload уже поддерживал несколько фото. [Обсуждение и исходные наблюдения](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1794342920292269).

## Дополнение по живому повтору, 10 ноября

В 22:21 UTC команда обновила три записи. Журнал содержит по два успешных `edit_vk_post` для сеток 13856 и 13855. Для карусели 13825 первый edit начался в 22:21:43.993, закончился в 22:21:44.922, второй начался в 22:21:45.108 и вернул captcha 14 из проверки квитанции. Две строки старой квитанции содержат пользовательские ID, переданные редактору. В этом окне в журнале обычного `vk_sync` нет. Последнее задание `vk_sync` для события 10705 обновлено 8 ноября в 13:45:47 UTC.

На карусель передали `photo868977531_457265678, photo868977531_457265677`. Позднее оба штатных способа чтения одним пользовательским актором вернули тот же текст, дату 1794171240 и фото `photo-231920894_456252030, photo-231920894_456252031` в этом порядке. Поле верхнего уровня `primary_attachments_mode` отсутствовало. Фактическая команда запускала `python -` в контейнере Events с `PYTHONOPTIMIZE=1` и `sys.flags.optimize=1`. Поэтому из `check_accepted` Python удалил проверки `assert` для ссылки, текста, даты, режима и ID, а также завершающую `assert not edited`. Вызов обычного редактора с ID загрузки и желаемым режимом находился вне assert и исполнялся. Переменная VK_API_VERSION в контейнере отсутствовала. Публичный адаптер читал через 5.199 по умолчанию, внутренний через 5.131. Его ветка no-op требует совпадения текста, упорядоченных ID и явно запрошенного режима. Команда проверки теперь обходит редактор целиком и запоминает ID из чтения стены.

Качество открыло карусель 1/2 и перешло на второй кадр. На первом wall-фото даже полный вариант 815×1222 обрезает шапку и нижнюю часть QR-кода, полоса «листай →» отсутствует. Одобренная обложка 1264×2482 после штатного уменьшения имеет размер 815×1600. Новая копия помещает весь кадр и полосу на холст 1118×1677. Одобренная программа 1204×2482 сохранила пропорции на стене (1048×2160). Копию по пользовательскому ID ВК не вернул. Три уже изменённые записи подлежат чтению по точным ссылкам [13856](https://vk.com/wall-231920894_13856), [13855](https://vk.com/wall-231920894_13855), [13825](https://vk.com/wall-231920894_13825). Старые строки с ID загрузки оставить в исходном виде. Несовпадение при повторе передаётся координатору и качеству для визуальной сверки исходных кадров. Карусель 13825 остаётся непринятой.

## Treat as regression guard when

При изменениях approved-галереи, полноты managed VK-поста, `vk_sync`, загрузки обложки и режима `wall.post` / `wall.edit`. Сохраняются контракты [дублей и частичной загрузки](INC-2026-06-09-event-media-duplicates.md), [разных визуально похожих снимков](INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped.md), [сбора изображений из нескольких источников](INC-2026-05-05-event-source-media-aggregation-gap.md) и [неопределённого исхода публикации](INC-2026-08-27-eventsbot-vk-image-publish-outcome-unknown.md).

## Mandatory checks before closure or deploy

`tests/test_event_media_gate.py::test_managed_vk_gallery_publish_and_edit_replay` проверяет оба layout, зеркало тех же байтов, второй approved-кадр, обновление недостающего вложения, сохранение текста и слота, единственный `wall.post`, смену явно отличающегося формата и повтор при пропущенном поле режима, сохранение старых вложений и хеша при сбое замены [A,B] → [A,C], затем один edit на изменение и повтор без загрузок. Отдельные примеры проверяют прозрачную афишу и уменьшение JPEG 6000×4000 до полного декодирования. Существующие проверки медиагейта покрывают одинаковые пиксели в разных контейнерах, визуально разные снимки и исключение pending/rejected. Существующие проверки VK покрывают частичную загрузку и сохранение вложений, captcha, защищённое восстановление ID и различие между отказом до `wall.post` и неопределённым исходом самого `wall.post`.

## Required evidence и небольшой backfill

После выпуска координатор запускает этот блок на стенде. Первый запуск выбирает до двух действительно изменённых записей каждого формата из последних 100 постов. Совпадение числа фото и формата сетки отмечается отдельно как контроль по этим двум признакам. Старой карусели загружается новая первая картинка с подсказкой. Источник фото — упорядоченные approved-записи медиаревью. Исходный текст, слот и конкретный wall ID сохраняются.

Для повтора запустить тот же блок. В квитанцию попадают упорядоченные ID вложений, которые вернул сам пост после edit. Если в файле уже есть записи, команда только читает эти wall ID и сверяет текст, слот и вложения. Расхождение останавливает проверку. Если VK опускает формат, его и визуальное соответствие загруженной обложке подтверждают в клиенте. Квитанция фиксирует результат чтения, вид и содержимое кадров принимает качество.

```bash
set -o pipefail
receipt=/home/dev/artifacts/events-bot-new/20261110T203533Z-vk-multiphoto-crew/accepted-vk-posts.jsonl
accepted=None
if [ -e "$receipt" ]; then
    accepted=$(python3 -c 'import json, sys; print([json.loads(line) for line in sys.stdin])' < "$receipt") || exit 1
fi
{
printf 'accepted = %s\n' "$accepted"
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

async def wall_photos(record):
    ref = record["ref"]
    item = await wall_item(ref)
    if not item or f"{item['owner_id']}_{item['id']}" != ref:
        raise RuntimeError(f"{ref}: запись не найдена")
    if (item["text"], item["date"]) != (record["text"], record["date"]):
        raise RuntimeError(f"{ref}: изменился текст или слот")
    mode = item.get("primary_attachments_mode")
    if mode is not None and mode != record["mode"]:
        raise RuntimeError(f"{ref}: VK вернул другой формат {mode}")
    if any(a.get("type") != "photo" for a in item.get("attachments", [])):
        raise RuntimeError(f"{ref}: на стене появились другие вложения")
    return photo_ids(item)

async def backfill():
    if accepted is not None:
        if not accepted:
            raise RuntimeError("Пустая квитанция: нет записей для проверки")
        for record in accepted:
            if await wall_photos(record) != record["ids"]:
                raise RuntimeError(f"{record['ref']}: изменился состав или порядок фото")
            print("https://vk.com/wall" + record["ref"], "вложения совпали", file=sys.stderr)
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
        record = dict(ref=ref, mode=mode, text=item["text"], date=item["date"])
        wall_ids = await wall_photos(record)
        if len(wall_ids) != len(ids) or (wall_ids == photo_ids(item) and wall_ids != ids):
            raise RuntimeError(f"{ref}: замена галереи не подтверждена")
        record["ids"] = wall_ids
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

## Release verification, 2026-11-10 23:28–23:32 UTC

PR 886 was accepted by independent quality review and merged as
`2c18976b47515dd1bb03121f4dbfd3ef2993e3d7`. Events now runs image
`runcoveer/events:vk-multiphoto-replay-20261110`
(`sha256:8e40c6fc3ffae3149b2a49e54956b901d4ebd87928180403f5d076d823a109cc`).
The previous running release is its ancestor. Full tracked-tree verification
covered 4,467 files; isolated preflight made no provider calls. Live health
reported ready, database ok, scheduler ok and no issues. Kotopogoda retained
its original container. Rollback image is the preceding gallery release.

The documented backfill, using a new receipt file, updated four existing posts:
- Grid: https://vk.com/wall-231920894_13856 and https://vk.com/wall-231920894_13855.
- Carousel: https://vk.com/wall-231920894_13825 and https://vk.com/wall-231920894_13802.

Each returned two group-owned photos. Readback preserved text, publication time
and wall ID. A separate replay of the new receipt completed successfully with
four wall.getById calls, zero wall.edit, zero wall.post and zero photo uploads.
The original two receipts remain unchanged, including the recorded mismatch
for the first photo of 13856. The incident remains open for independent visual
acceptance of the actual grid, carousel, full cover, hint and second frame.

Retained evidence: central task directory
`/home/dev/artifacts/events-bot-new/20261110T203533Z-vk-multiphoto-crew`, files
`replay-cutover-receipt.json`, `accepted-vk-replay-fixed-posts.jsonl`,
`fixed-backfill.stderr` and `fixed-replay.stderr`.
