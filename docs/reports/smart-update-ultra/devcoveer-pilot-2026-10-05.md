# Smart Update Ultra — компонентное исследование на DevCoveer, 5 октября 2026

Статус: **ЧАСТИЧНЫЙ ЭМПИРИЧЕСКИЙ РЕЗУЛЬТАТ, НЕ PRODUCTION ACCEPTANCE**.

Первый checkpoint сохранён в 08:04 UTC; обновление — после завершения пяти native tasks, сборки трёх альбомов и повторного успешного запуска 21 unit test. [Канонический вход](../../features/smart-update-ultra/README.md), [Candidate A](../../design/smart-update-ultra-to-be-2026-10-05.md). Документация — PR #723. Исследовательские скрипты — PR #724, направленный в ветку #723, **не в production main**; source commit `8e95584dd19e4d4da51a0483986a0d90f3a754ce`.

## 1. Что действительно выполнено

Продолжен прежний стенд `handoff:src_5dd70fb837000a90095bdc3d`, исходная база `627b24e2fe776a52f10448b2f955977016deef7a`. После фиксации скриптов его ветка — `chatgpt/smart-update-ultra-lab-20261005`. Чужой dirty application checkout не сбрасывался и не использовался для deployment.

Первоначально через существующий локальный Telethon launcher получены 10 публичных сообщений и 9 JPEG из четырёх каналов: `agropark39` 2342–2344, `ambermuseum` 5624–5626, `tanja_from_koenigsberg` 4164–4165, `progulki_s_katey` 812–813. Capture — 07:42 и 07:48 UTC. Затем ограниченное чтение соседних IDs добавило ранее не попавшие в выборку части двух экскурсионных альбомов: 4161–4163 и 808–811. Это чтение оригиналов сегодня, но публикации относятся к разным датам; не выдавать их за сегодняшние свежие анонсы.

Проведены пять native OpenCode tasks и один отдельный неудачный direct-session транспортный эксперимент. Preflight сервиса показал OpenCode 1.18.31. Два исторических VK-derived текста и синтетический negative control проверены двумя текстовыми моделями. **Это не live VK capture**; delivered texts были сокращены/нормализованы относительно исходных fixtures. Точный переданный prompt сохранён в session receipt.

**Не выполнены:** получение свежего production snapshot, реальные domain CREATE/MERGE/replay на shadow DB, полный независимый golden/holdout, перенос в Kaggle. Нет production event/cursor/queue mutation, публикации или deployment. Начальная комплексная Codex-задача на snapshot+лабораторию была заблокирована до создания задачи; она не объявляется работающей. Независимые публичные чтения и компонентные тесты продолжены отдельно.

## 2. Где сохранены доказательства

Private retained root:
`/home/dev/artifacts/events-bot-new/20261005T073316Z-ultra-public-component-20261005/`

- `cases/`: первоначальные source packets; `media/`: изображения и хэши.
- `assembled/`: отдельные snapshots восстановленных альбомов, с исходными IDs и границами.
- `runs/`: receipt direct-session ошибки; `native/`: пять выгрузок terminal outputs и session metadata без hidden reasoning.
- `reviews/offline-review-1456c8dba3d41166.json`: неизменяемый по содержимому результат структурной проверки.
- `source-diagnostics/20261005T082023Z`: проверенные копии одноразовых bootstrap/permission-schema helpers, удалённых из рабочего дерева после сохранения. Это лабораторные исходники, не credentials.

Оригиналы и полные receipts не коммитятся в GitHub. Пока это **DEV-корпус**, не полностью независимо размеченный золотой набор. Пять native tasks дают девять case-level выдач, потому что каждый текстовый task содержал три случая; это не девять независимых исходников.

## 3. Фактические модельные результаты

| Прогон | Модель / task ID | Результат | Ограничение оценки |
| --- | --- | --- | --- |
| T0, direct session | `opencode/mimo-v2.6-flash-free` | 403 `FreeTierError`: “OpenCode's free tier can only be used from within OpenCode”; elapsed 3.264 s, результата нет | Это ошибка транспорта/допуска, не качества извлечения. Причина различия путей не установлена; не было spoofing или платного fallback |
| Музей V1 | MiMo, `dvt_3a48ccdd43ad45489d4da555277a3209` | Прочитаны текст/фото; неизвестные даты оставлены null. В `events` попал также сезонный перерыв, disposition вне canonical enum | Семантический дефект и недостаточность первоначального loose-контракта. Enum/schema не были полностью заданы в V1; нельзя все несоответствия приписывать непослушанию модели |
| Музей V2 | MiMo, `dvt_e17a7bcc65684daa883efa0a52025066` | `MIXED`, одно недатированное посещаемое занятие; перерыв вынесен в `UPDATE_DETAILS`; даты не выдуманы | Улучшение на том же DEV-кейсе. Осталась реальная ошибка заданного типа: lifecycle `support` выдан массивом вместо строки. Не holdout и не полный pass |
| Две афиши АгроПарка | MiMo, `dvt_dcf2ee0d20bb4061a404548cbb2f20ad` | В одном финальном ответе два OCR-блока и пять children; `evidence_complete=false` сохранён для исходного partial-album пакета | Поднабор структурных проверок прошёл. Дата/время пяти занятий сверены с текстом поста; полный посимвольный OCR и все дополнительные image-only facts независимо не приняты |
| Текст V1 | `opencode/nemotron-3-ultra-free`, `dvt_fe20faaf91834e74a6566716529e59de` | Основные поля вечеринки сохранены, recap отвергнут, перенос времени обнаружен | У Rudau `EVENTS_FOUND` вместе с action; некaнонический `reschedule`. Loose-v1 не задавал всю production schema; DB lookup/commit не проверялись |
| Текст V1 | `opencode/big-pickle`, `dvt_e9dc6ea21c1447e397f94152a02ccd3b` | Основные поля вечеринки сохранены, recap отвергнут, Rudau получил `MIXED` | Некaнонический `time_changed`; нет полного production evidence receipt. Не результат materialization |

Исходные исторические fixtures: [Rudau, перенос времени](../../../tests/replays/INC-2026-05-07-vk-time-reschedule-wrong-match/sources.json), [вечеринка и negative control](../../../tests/replays/INC-2026-07-17-vk-auto-provider-quota-false-reject/sources.json).

### Подтверждённый фрагмент результата АгроПарка

По тексту `agropark39/2342`, дата 4 октября 2026: экскурсия 14:00; «От овечки до ниточки» 15:00; скачки на хобби-хорсах 15:00; «Посади растение» 16:00; String Art 16:30. Модель сохранила все пять, включая два разных занятия в 15:00. Общий праздник не продублирован как шестой child в этом ответе. Позднее отдельное live-чтение подтвердило наблюдаемые границы группы 2342–2343. Прежний partial-input и ответ модели не переписывались задним числом в complete.

Это подтверждает реализуемость **joint vision decision + OCR + children**, но не доказывает готовность автоматической записи или общую точность модели.

## 4. Время и служебные расходы

| Прогон | Assistant messages | Wall time задачи по session metadata | Последнее assistant message |
| --- | ---: | ---: | ---: |
| MiMo музей V1 | 2 | 455.179 s | 27.133 s |
| MiMo музей V2 | 2 | 158.038 s | 59.861 s |
| MiMo две афиши | 3 | 446.586 s | 61.891 s |
| Nemotron, 3 текста | 1 | 27.536 s | 27.536 s |
| Big Pickle, 3 текста | 1 | 395.465 s | 395.465 s |

Времена **не являются чистой inference latency**. Vision tasks ожидали разрешений на конкретные public-file reads. Tool intervals могут перекрываться; нельзя вычитать их сумму из общего времени. Big Pickle завершился без tool calls, но единственное наблюдение не позволяет ранжировать устойчивую скорость провайдеров. Пять задач содержали девять assistant messages; это не автоматически девять физических provider sends при неизвестных внутренних retries.

Зафиксирован значимый overhead: на три коротких текстовых случая Nemotron сообщил input=16,122 tokens, а итоговый `tokens.total`=17,400; Big Pickle total=16,254. В vision-задаче АгроПарка reported totals трёх сообщений: 15,761 / 17,330 / 23,508. Это сохранённые provider/runtime counters, не расчёт счёта. Все просмотренные assistant records содержали cost=0; независимый billing audit не выполнялся.

Следствие: для массового Ultra нужен узкий специализированный agent context вместо полного generic development-контекста и лишних чтений. Один финальный смысловой проход со зрением возможен, но схема «сначала модель решает открыть JSON, затем модель решает открыть JPEG, потом извлекает» расходует дополнительные turns и время. Уменьшать это следует штатной упаковкой входа и ролью агента, не обходом provider restrictions.

## 5. Реальный дефект выборки: обрезанные и последние альбомы

Ограниченное повторное чтение восстановило:

| Канал | Члены группы | Фото | Текст | Границы по соседним сообщениям |
| --- | --- | ---: | ---: | --- |
| agropark39 | 2342–2343 | 2 | 681 chars | До 2341, после 2344; подтверждены |
| tanja_from_koenigsberg | 4161–4165 | 5 | 2,138 chars | До 4155; справа сообщения не получено |
| progulki_s_katey | 808–813 | 6 | 210 chars | До 807; справа сообщения не получено |

Первоначальные последние два сообщения обоих guide-каналов имели пустые captions. Только после восстановления группы обнаружился исходный текст: исторический материал в первом канале и личный осенний пост во втором. По самому тексту это не датированные анонсы экскурсий; **whole-source no-event по непроверенным изображениям не выставлялся**.

Новый практический edge case: правило «ждём другое сообщение после альбома» не может само подтвердить последний альбом канала. Отсутствие правого соседа не доказывает потерю медиа. Для production это потенциальное вечное ожидание. Следующий отдельный тест должен проверить стабильный provider frontier, quiet-window и source revisions; текущий прототип консервативно оставляет `album_bounds_unconfirmed`, а не объявляет missing media или no-event.

## 6. Что теперь покрыто тестами

Повторно прошёл `python3 tests/test_ultra_lab_contract.py`: **21 unittest**. Проверены закрытые enums, согласование events/actions/disposition, ошибочный тип support из реального V2, отрицательный результат при неполном evidence, допустимый partial positive, число/уникальность OCR-блоков, текст-free photo, наблюдение границ и числовые пропуски IDs.

`ultra_review_receipts.py` затем проверил сохранённые реальные outputs. Музей V1 не проходит несколько структурных требований; V2 сохраняет ошибку support type; АгроПарк проходит диагностический поднабор; текстовые v1 выявляют alignment gaps. **21 зелёный unittest — проверка самого диагностического кода, не 21 успешный модельный кейс и не полная валидация production-контракта.**

Команды и известные ограничения: [lab-reproduction.md в source commit](https://github.com/onedayonemasterpiece/events-bot-new/blob/8e95584dd19e4d4da51a0483986a0d90f3a754ce/docs/reports/smart-update-ultra/lab-reproduction.md). Source branch содержит changelog и только исследовательские изменения; application runtime не менялся.

## 7. Уточнения требований по фактическим результатам

Первый vision-pass возвращает OCR вместе с event/lifecycle decision, как уточнил владелец. OCR=empty у текст-free photo, unreadable text и unopened file — три разных состояния. Положительное распознавание сохраняет уже извлечённый текст и факты для text-only resolver; отдельный обязательный vision/OCR pass после этого не нужен.

Нужно заранее определить schema для событий, lifecycle и evidence, а не переносить произвольные fields из conversational output в БД. Сезонный перерыв — не посещаемое событие. Единый способ хранения source spans должен исключать самодельные пересказы/многоточия, когда требуется буквальная цитата. Формальная схема и семантическая проверка оцениваются отдельно.

Permission waiting отражается как отдельная стадия, а не opaque running/provider timeout. Транспорт с 403 остановлен и не принимается как рабочий массовый маршрут. Результаты через native tasks не устанавливают причину этого 403 и не разрешают менять client identity ради обхода.

DEV-кейс музея уже использован для prompt tuning и не может позже считаться untouched holdout. Текущий набор требует независимой разметки исходников и группового разбиения репостов/сеансов. Время оценки в первых задачах — дата публикации, а не сегодняшняя дата, поэтому это as-of extraction, не текущая publication eligibility.

## 8. Экскурсии

Действующий [Guide Excursions Monitoring](../../features/guide-excursions-monitoring/README.md) использует `guide_*` и отдельные GuideProfile / ExcursionTemplate / ExcursionOccurrence / GuideFactClaim. В Ultra можно переиспользовать транспорт, vision/OCR, resource routing, durable stages и model lineage, но не сваливать guide entities в обычные `event`/`daily`.

Отдельные проверки: on-request/private против scheduled_public; несколько дат/маршрутов; common booking facts без переноса между чужими occurrences; sold-out/waitlist; перенос/отмена; profile/template-only контекст. Отсутствие датированного выхода не равно отсутствию полезных сведений для guide-product. Свежие положительные guide-анонсы и фактический guide merge ещё не проверены; случайная выборка последних фото этого не заменяет.

## 9. Открытые gates и следующий checkpoint

1. Независимая проверка полного OCR и image-only facts, размеченный DEV и отдельный HOLDOUT; frozen prompt/schema.
2. Испытание latest-album frontier без вечного ожидания и без ложного complete.
3. Live VK source acquisition через штатный read-only adapter; не смешивать с историческими text fixtures.
4. Разрешённый consistent production snapshot с provenance и достаточным диском. При подготовке свободное место DevCoveer менялось примерно с 3.4 GB до 1.8 GB; это не атрибутировано Ultra. Production env/archive с секретами не копировались, broad cleanup не выполнялся.
5. Изолированное фактическое CREATE/MERGE/replay через существующий Smart Update boundary, с отключёнными external effects и проверкой БД после commit. Не заменять игрушечным SQL. Current snapshot может уже содержать выбранные посты; явно учитывать look-ahead.
6. Узкий native OpenCode input/agent profile и безопасный versioned exporter, затем тест нескольких моделей на одном корпусе. Snapshot-export прототип не должен перезаписывать V1 после continue_task.

Все пять native tasks завершены, их повторный старт для восстановления не нужен. Продолжать с того же стенда/source commit и сохранённых артефактов. Заблокированная операция не переносится скрыто на другой механизм. Невыполненная DB-приёмка остаётся невыполненной независимо от успеха OCR.

## 10. Evidence identifiers

Immutable terminal evidence:

- Museum V1: `188f259fdb58c28a27e1df183524964ad36f4ebd2a8d8fc1524716462cd2a701`.
- Museum V2: `f059dd47b6bb7b7e3ad2a3f02db551146cccda5ca5b47f8fb0a9cc3ab4187cbc`.
- Agropark: `23e0dc920ca7efb04cc217ec5179c46d2d9ba1298598402439162b5ae27905dc`.
- Nemotron text: `c4cdc7543b5de5e696dfaf46c06fb6c4cd8d2bf57aa64a92ab3c1c2a691a6aa8`.
- Big Pickle text: `f5f1a629a04ae75ca84fa0d05379745cd2845d89b9989dcdd7384ad01f5869e2`.

Native exports have separate hashes because they are sanitized snapshots, not byte-identical copies of the immutable task-evidence envelope. Their hashes are in the saved offline review. Live album job: `job_9fbccf5a8a0f3c774bf21eb2`; source checkpoint operation: `op_9cd8bb488eaa11217b86a91a`. A failed source-patch precondition produced no mutation; the later changelog edit checked the exact previous file digest and was independently checkpointed. No failed tool call is counted as a passed test.
