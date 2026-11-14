# Неполная фотогалерея в опубликованных анонсах ВК

Дата 10.11.2026. Severity sev3. Реестр `inc_af96f6510a55a8ccc1d713dc`. Статус открыт до проверки в приложении ВК.

## Наблюдение и причина

### Подтверждённый дубль лучшего качества, 12 ноября

После исправления выбора исходной full-size афиши остаётся отдельный
повторяемый дефект: approved миниатюра 366×450 выигрывает у pending 1800×900
даже после подтверждённого duplicate verdict. Приоритет approved стоял перед
качеством, а переход не заменял уже approved loser. Исправление существующего
duplicate transition выбирает качество после managed-origin приоритета,
повышает canonical и сохраняет другую версию как duplicate. Регрессия требует
full-size gallery → обычный queued vk_sync; более крупное unrelated изображение
остаётся rejected. До выпуска и live readback качество не считается принятым.

### Primary 503 не использовал существующую escalation, 12 ноября

После выпуска удаления pair cap обычный worker обработал пары 33095 (10833),
33096 (10838), 33099 (10841) в 14:17:59–14:19:15 UTC. Все получили настоящий
Gemini HTTP 503 UNAVAILABLE и ушли в deferred до следующего дня; поле
`escalation_model` осталось пустым. Внешний catch охватывал primary и
escalation, поэтому исключение первого вызова полностью пропускало уже
настроенную Gemma. `provider_calls=0` ошибочно скрывал состоявшуюся отправку.

Коррекция маршрутизирует retryable `ProviderError` primary, кроме 429, в
существующую escalation и сохраняет её обычную семантическую валидацию.
429/nonretryable ошибки продолжают прежний retry-путь. Учёт отправок берётся
из общего `GoogleAIClient.attempt_observer` после `mark_sent`, как в Smart
Update; локальный счётчик успешных возвратов удалён. Модели, KEY4 и строгий
shared limiter не меняются. Отдельный retry, очередь или public backfill
не добавляются. До выпуска и реального ordinary worker → VK readback это
исправление не является подтверждением системной готовности.

Регрессию усилили до обработки настоящего `vk_sync` из outbox. Она выявила
соседний механический дефект этого же успешного pair turn: создание
следующего frontier уже синхронизировало галерею, после чего worker видел
`projection_changed=False` и не запускал fanout. Факт изменения теперь
сохраняется до этой вспомогательной синхронизации. Тест требует обычный
pair review → queued vk_sync → несколько вложений, без ручного approval
или ручного enqueue VK.

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

Следующий узкий срез удаляет только локальный pair cap 100/25: capacity
остаётся у существующего атомарного Google project/model limiter. KEY4,
primary/escalation models, отсутствие local fallback и overflow сохраняются.
Число реальных inference calls может вырасти; новые тарифы или ключи не
подключаются. Это технический выбор основного окна внутри исправления
порученного потока после прямого возражения владельца против незапрошенного
ограничения, а не дословное решение владельца о конкретной реализации.
Бюджеты semantic role и geometry этим изменением не затрагиваются.

Само удаление admission не оживляет исторические terminal/deferred rows.
Компенсация после выпуска допускается только адресно: `provider_calls=0`,
`last_error=media_review_primary_budget_exhausted`, `deferred/unresolved`,
active non-silent ещё не окончившееся событие и оба изображения всё ещё
approved/pending. Сохранить исходный срез, вернуть эти пары в pending и
поставить прежний `enqueue_event_media_review_job`; решения и публикации
выполняет обычный worker. Не менять фактические uncertain, прошлые/silent
события и approved statuses; это не повторный VK backfill.

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


## November 12: initial publication overtook the remaining source photos

The owner clarified that multiple images must already be attached to a new
postponed announcement, after initial source preparation. Updating the post
later is not the acceptance criterion. A direct postponed-wall snapshot found
five entries and one photo in each. Fresh event10888 demonstrates the race:
nine source candidates, an initial one-photo post14154, then a five-photo public
post14155 as pair reviews progressed. Source/media transport can deliver several
images, but the initial preparation contract was broken.

`schedule_event_update_tasks` considered media complete once the first seed was
a CDN URL. It then omitted the media dependency, while VK's higher priority let
it publish before existing media followups. Reuse the latest media JobOutbox
and `event_media_review:<event_id>` dependency for VK and Telegraph; Telegram
already depends on Telegraph. No new readiness state, queue, limiter, polling,
retry path, media model or review policy. The existing worker owns completion
and follows the latest media job across continuations.

A related observed defect: post14154 was scheduled for20:20 UTC, but public
post14155 had provider timestamp20:04:28 UTC during its first gallery update.
`wall.edit` did not pass the already-read future publication timestamp; preserve
that value in the edit. The existing fixture had explicitly asserted absence
of publish_date on this edit and is corrected to preserve the scheduled slot.
The calendar task's added lower bound on actual public date is removed:
a new-post task must accept its exact binding even when the previous editor
released it earlier than the planned time; no historical bootstrap is added.

Distinguishing regressions on exact previous main ab7e2728: premature VK dispatch
and missing postponed edit timestamp both fail, while the ordinary public edit
control passes. With the fix, the real outbox first processes two media stages,
then invokes each of VK/Telegraph/TG exactly once with both source images.
An unchanged event's media dependency remains independent of other events.
Fixtures which invoked the pair helper outside the worker now explicitly
complete that fixture's JobOutbox before their separate VK-only assertion.

Mandatory checks: tests/test_job_worker_fresh_selection.py,
tests/test_event_media_gate.py, tests/test_vk_calendar_comments.py, ICS and
separate CAPTCHA-pause regressions. Actual normal intake must still demonstrate
a new postponed post with a complete unique gallery and preserved publication
time. Green tests or an already-updated public post do not close this incident.
Retained evidence: /home/dev/artifacts/events-bot-new/20261112T201732Z-vk-complete-gallery-before-publication-20261112.


### Optional geometry must not delay a complete gallery, November12

After PR918, ordinary new10890/10891 each had one source poster, one approved
and semantically classified ledger row, and no pair-review rows. Yet the latest
media followup waited until00:05 UTC because optional image_geometry usage
had reached its existing100-call budget at02:40 UTC. The broad dependency on
any latest media job incorrectly deferred Telegraph and VK with it. Event10892
still had genuine pending semantic work and is a negative control.

Publication now depends on the existing media job only while a source poster
is pending pair review or an approved poster is not yet semantically classified.
Optional geometry retains its normal job and budget. No extra state, flag,
queue, model, limit or provider request. Existing enqueue replacement now also
honors an empty dependency list and re-arms a pending job whose dependencies
actually changed, instead of retaining its obsolete tomorrow deferral.

The initialized two-stage worker witness fails on exact PR919: it waits for
tomorrow's optional geometry despite two ready photos. After source preparation
it must publish VK/Telegraph/TG once with both images, while geometry remains
pending. It also simulates already-deferred publication rows. This preserves
the earlier witness that a first CDN seed cannot overtake the second source.
Evidence: retained artifact20261112T204450Z-vk-no-repeated-metrics-or-docs,
single-photo-next-day-defer-audit.md and geometry test logs. A normal initial
multi-photo postponed post remains the live acceptance criterion.


### Keep the established unlinked gallery eligibility

Follow-up read-only review found10892 has linked_event_ids=[], one approved
first-source poster and a canonical gallery URL, yet the new generic semantic
prerequisite defers its VK155386/Telegraph155384 to00:05 because semantic_role
allowance is exhausted. That prerequisite was not present before PR918.
Canonical get_event_gallery_rows requires completed semantic classification
only for linked events. Event10893 is such a linked event ([7315,9351]) and
must retain its existing guard. This supersedes the universal semantic
prerequisite described above, rather than creating a new publication policy.

The dependency predicate now follows that existing linked-event distinction.
Pending source pair-review still blocks the first publication until all images
are adjudicated. Role enrichment for unlinked approved images remains in its
existing job/fanout. Initialized linked/unlinked controls reproduce the wrong
unlinked hold before the one-line correction and preserve the linked hold.
No new state, queue, flag, model or budget. Included with notification fix in
one release so import does not need another separate restart.

### Ложный конфликт обложки общей программы, 13 ноября

Обычный Telegram recovery 12057 создал событие 10897 из carrier 4271.
Pair 33232 получил одним настоящим primary вызовом distinct/confidence 1.0,
но одновременно semantic_conflict=true. Второе изображение 37530 было
отклонено до semantic-role классификации; первоначальная публикация 14167
снова получила одну картинку. Визуально исходники различаются: карточка
семейной программы 22 ноября с мастер-классом и общая обложка тех же
семейных арт-выходных 20–22 ноября. Оба исходника 1020×1280.

Прежняя инструкция называла другую дату или название конфликтом без
различения общей программы и входящего события. Уточнение существующего
pair prompt требует несовместимости именно фактов целевого события;
подтверждённая обложка программы с включающим диапазоном дат допустима.
Явно несовместимые даты, цена, место и другая программа остаются конфликтом.
Schema, модели, limiter, статусы, role policy и сохранённые review keys
не меняются. Исторические решения автоматически не инвалидируются.
Это семантический дефект: исправление LLM-first, без keyword overrides,
ручного approval или выбранного public backfill.

Проверка изменения должна включать настоящий вызов исправленного prompt
через существующий общий limiter на этих двух изображениях и отрицательный
контроль с несовместимой датой. Проверки транспорта и остальных переходов
выполняются существующими media regression tests. До результатов, выпуска
и следующей обычной initial multi-photo публикации критерий открыт.
Evidence: /home/dev/artifacts/events-bot-new/20261112T224118Z-vk-parent-program-image-relevance-20261113.

### Потеря крупного Qtickets исходника, 13 ноября

Readback 14162/14163/14164 показал маленькие фото. Исходные retained
Qtickets packets и canonical posters уже имели высоту317; это потеря
качества до VK, отдельная от Yantar desktop-poster correction.
Один public GET Impuls source page из production сети дал HTTP200.
В его JSON-LD/OG афиша317px, но тот же hero внутри poster-container
объявляет srcset2x. Адрес взят из HTML, не сконструирован. Один download
этого файла подтвердил реальные1136x634 вместо canonical568x317.

Исправлен только существующий collector: выбирается максимальная
объявленная плотность выше1x основного hero. Отсутствующий или неверный
srcset и отдельные логотипы сохраняют старый JSON-LD/OG выбор.
Данные события, occurrence identity и source import не меняются.
Никаких дополнительных page fetch, guessed CDN URLs, upscale, state,
queue, worker или selected source replay. Net production+9, budget15.

Отличающий тест разбирает реальные сохранённые declarations Impuls:
старый collector выдаёт317px JPG и падает; исправленный выдаёт declared2x
и сохраняет title/startDate/location. Отрицательные контроли проверяют
missing/invalid/1x-only/unrelated srcset. Actual browser startup и
structured-fact/occurrence regressions также обязательны.
До выпуска и обычного нового collection/import/VK readback качество
не считать полностью принятым. Evidence:
/home/dev/artifacts/events-bot-new/20261112T230543Z-qtickets-declared-poster-density-20261113.

### Готовый Telegram output остановлен перед восстановлением, 13 ноября

Обычный catchup-tg-monitoring-5233941e7fc14bc487f4282b703b093d
завершился в 23:15 UTC с ledger DONE/CLEANUP. До 23:22 нового
импорта не было. Local operation 12058 прервал выпуск; более ранний
recovery_import 12057 имел partial и считался доставкой суточного слота.
Watchdog возвращался по этому условию до существующей проверки registry.
ENABLE_KAGGLE_RECOVERY выключен, поэтому другого автоматического
продолжения данного handoff в этом runtime не было.

Существующая проверка registry перенесена перед условиями доставки
и terminal retry нового запуска. Inflight, grace/lookback и deferred cadence
сохранены. Пустой registry по-прежнему не запускает новый kernel
при прежней доставке. Production net 0: только перестановка. Нет нового
worker, состояния, таймера, retry или ручного replay исходных данных.

Initialized DB regression со старым partial и новым cancelled run:
до исправления положительный контроль падает, отрицательный проходит;
после оба проходят. Существующие TG watchdog guards также проходят.
Продуктовая проверка после выпуска — обычное продолжение сохранённого
output, новые media candidates и initial VK attachments, а не запуск
выбранного source packet. Evidence: тот же retained artifact PR 923.

### Дневной primary quota отказ обходил существующую эскалацию, 13 ноября

Обычный TG import12060 создал posters37535–37537 у10508 и новое
событие10899 с двумя source candidates. Pair33239 получил RateLimitError
rpd и был отложен до следующего дня; новый33241 также ушёл до14ноября.
Это отказ shared LLM limiter, не VK CAPTCHA или удалённый feature cap.
Код эскалировал uncertain/low confidence и retryable503, но RateLimitError
сразу попадал в внешний except с переносом на следующий день.

При rpd основной модели теперь используется уже настроенный Gemma review
с прежними schema, ключом, shared model/project limiter и attempt_observer.
Не создаются provider path, очередь, квота, retry или новое состояние.
RPM/TPM, provider429 и auth401 сохраняют прежнее поведение. Отказ резерва
не считается HTTP send; successful Gemma считается один раз.

Initialized gallery→VK grid/carousel regression до фикса падает, после
проходит. Контроли сохраняют отсутствие эскалации при minute limits и401/429,
повторное редактирование без дублей и исходный publish_date.
Production net+7 при бюджете15. Evidence:
/home/dev/artifacts/events-bot-new/20261113T001857Z-vk-existing-media-rpd-escalation-20261113.
До exact-main выпуска и обычного initial VK readback задача открыта.

Новая initial публикация10899 имела pending vk_sync155520 с явной
depends_on event_media_review:10899. В00:24 UTC её media155516 всё ещё ждало,
хотя due00:10; actual query нашёл100более ранних media reviews. Существующий
приоритет blocked_media_event_ids применялся только после failed VK attempt
с missing_materialized_media, а корректно ожидающая initial publication
туда не попадала. Расширено только существующее условие на pending VK
с той же явной media dependency. Active/future/silent eligibility сохранены.
Ранее готовые VK jobs не получают media priority. Различающий initialized
worker тест: до1fail/1negativepass, после обязательны оба и весь worker suite.
Дополнительно main.py net+4; общий production net+11 при прежнем бюджете15.
Никакой новой очереди, lane, флага, polling или capacity reservation.

### Временный отказ Gemma необоснованно переносил пару на день, 13 ноября

После выпуска924 обычный worker взял новое10899 в00:42:12UTC.
Primary rpd корректно вызвал existing Gemma escalation, actual send один.
Gemma вернула настоящий503; pair33240 deferred до14ноября00:05.
У10508 аналогичный503, у10727 timeout45секунд. Initial10899 остаётся
безVK URL и блокируется собственной media dependency; extra photo pending.

Ветка pair retry всегда использовала следующий день независимо от причины.
Semantic-role consumer уже имеет transient-error scheduler:5xx/timeout900секунд,
minute limits600секунд, rpd следующийUTC-day. Парная проверка теперь
переиспользует ровно эту функцию, одна заменённая строка, net production0
при бюджете5. Не добавлены state/queue/retry/лимит/provider path.
Attempt cap и actual-send accounting сохраняются. Семантический uncertain
без transport error сохраняет прежний следующий день; ошибка не даёт approval.

Initialized pair regression с lease текущего worker job: до исправления
503/timeout fail, rpd/uncertain negative controls pass. После обязательны
даты retry, pending extra image и существующий durable followup не позже due.
Доказательства в retained artifact:
/home/dev/artifacts/events-bot-new/20261113T004847Z-vk-pair-existing-transient-retry-20261113.
Обычный TG registry resume после924 подтверждён operation12066 с00:38:52UTC,
quiet chat NULL. Ручного source replay не было. Product result ещё открыт.

### Третий transport failure прекращал gallery review без ответа, 13 ноября

Actual33240 дважды не получил semantic verdict: Gemma503 и45s timeout.
Bounded review existing consumers показал ловушку третьей попытки:
max-attempt branch ставил unresolved раньше transient defer, selector
и followup выбирали только pending/deferred, extra photo оставалась pending.
После geometry followups initialVK мог выйти с seed, без второй картинки.
Существующий автоматический механизм эту terminal pair не возвращает.

Terminal-attempt условие теперь применяется только когда существующий
transient scheduler не определяет retry. Это одна заменённая строка,
net production0 budget3, без новых состояний/лимитов/recovery/provider paths.
Temporary timeout/5xx и дневной quota отказ сохраняют прежний deferred
механизм; counter и provider accounting не сбрасываются. Semantic uncertain
по-прежнему исчерпывается при3попытках. Ошибка не даёт image approval.

Initialized leased-pair regression перед исправлением: third503/timeout/rpd
падают, пять остальных controls проходят, включая thirdsemanticuncertain.
После необходим полный этот сценарий и gallery→VK replay controls.
Evidence retained:
/home/dev/artifacts/events-bot-new/20261113T011138Z-vk-pair-transient-liveness-20261113.
Подтверждённый ordinary update13778 с двумя разными фото и листай→
не заменяет ещё открытый критерий initial fresh10899.


## 14 November: bounded consumer copies for large canonical WebP

The ordinary job for event 10963 resolves to canonical event 10509. Its canonical
WebP is 9449×4017 (37,957,633 pixels), 3,108,668 encoded bytes. JPEG draft does
not downsample WebP, so the former 20 MP cover guard rejected this source before
VK upload, leaving an incomplete gallery. This is separate from CAPTCHA.

Reuse one Pillow consumer-copy helper for VK upload and pair-review image inputs:
1600 px longest side, JPEG quality 95, no chroma subsampling, original bytes and
raw/pixel identity preserved. Reject non-draft inputs above 40 MP before loading.
Source fingerprints already stored on immutable posters are reused; missing
fingerprints are calculated sequentially and the pair does not calculate them
again. Pair-review SSIM and provider inline images use the smaller working copies.
No semantic decision is bypassed, and generated cards retain their ordering after
all source photos.

Isolated production-runtime measurements: standard Pillow decode peaked at
640,136 KiB; three repeated decodes returned resident memory to approximately
46 MiB. Main service RSS was 234 MiB; the host had 1000 MiB available RAM and
3811 MiB free swap. Sequential decoding is material to this budget. No global
conversion setting, native library, provider path, queue or recovery mode added.
Retained evidence: /home/dev/artifacts/events-bot-new/20261114T080301Z-vk-bounded-upload-prep-20261114.
This source fix does not prove fresh multiple-source-photo delivery by itself.


### Duplicate title expansion bypassed narrative preservation, 14 November

Actual99c701f6b event10906/job155630 repeatedly attempted wall.edit14213;
11:51:54 UTC received code14. The single source poster remained unchanged.
Later imports recorded zero added facts/posters but regenerated fact-first text;
at05:21 the model also expanded the specific title. Title application preceded
the duplicate-only preservation decision, so this cosmetic change bypassed it.
The existing duplicate-only evidence is now evaluated before applying the title.
Specific titles remain stable; existing canonical, meaningful-title recovery and
generic-title upgrade rules still apply. True fact, age, ticket and schedule
changes retain their normal update path. Source-only fanout uses that same
decision to avoid rearming an existing VK publication; gallery changes still
rearm it. No state, provider, queue, retry or historical hash adoption added.

Initialized equivalent-title regression failed before and preserves title,
description, canonical hash, zero VK enqueue and zero upload/edit after. Controls
cover added facts plus changed title, tickets, visible age, age provenance,
cleanup, canonical date correction, filling missing time and generic title.
This producer fix does not undo event10906's already changed canonical payload
or prove CAPTCHA resolved. Parent owns release and ordinary product verification.
