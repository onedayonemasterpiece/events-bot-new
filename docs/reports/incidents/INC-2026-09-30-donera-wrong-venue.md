# INC-2026-09-30 Донэра: неверная площадка в афише

Status: mitigated
Severity: sev2
Service: events-bot production event data and Telegram/VK publications
Opened: 2026-09-30
Closed: —
Owners: events-bot operator
Related docs: `docs/operations/incident-management.md`, `docs/operations/telegram-link-inspection.md`

## Summary

События 9137 и 9379 для концерта «Донэра» 10 октября 2026 показывают «Тёрку», пл. Победы, 4. Оригинальный пост организатора `https://vk.com/wall-219175543_205` указывает клуб «СКЛАД», ул. Ялтинская, 20п, 19:00. Ошибка опубликована в Telegram, включая пересылку в `@kenigevents`.

## User / Business Impact

- Зрители могли приехать к неверной площадке.
- Ошибка продублирована в двух карточках, Telegraph, Telegram, календарных .ics и VK.

## Detection

Сообщение пользователя со скриншотом 2026-09-30. Сверка с оригинальным источником, клубом и Telegram API.

## Timeline

- 2026-09-18: опубликована карточка 9137, Telegram `@kldevents/4192`.
- 2026-09-27: добавлена карточка 9379 из оригинального VK-поста; опубликована `@kldevents/4354`.
- 2026-09-28: сообщение 4354 переслано в `@kenigevents/5141`.
- 2026-09-30 08:08 UTC: выявлено несоответствие и начата коррекция.
- 2026-09-30 08:17 UTC: исправлены два поста `@kldevents` и удалена пересылка `@kenigevents/5141`.
- 2026-09-30 08:18 UTC: опубликованы опровержения `@kldevents/4403` и `@kenigevents/5152`.
- 2026-09-30 08:19–08:22 UTC: обновлены Telegraph, VK, календарные посты и .ics.
- 2026-09-30: в календарном канале опубликовано отдельное исправление `@kenigeventscalendar/9528` для уже скачавших старый .ics.

## Root Cause

Причина восстановлена по `vk_source_packet.id=15337` и `smart_update_candidate_state.id=18658`:

1. Исходный VK пост 205 явно называл клуб «СКЛАД», ул. Ялтинская, 20п. LLM parse decision извлёк адрес `Ялтинская 20П`, но название вернул как `СКЛАD` с латинской последней буквой `D`.
2. `_vk_location_value_ungrounded` в `vk_intake.py` не распознал `СКЛАD` как совпадение с кириллическим `СКЛАД` и обнулил **и название, и правильно извлечённый адрес**. В сохранённом `parse_result_json.drafts[0]` это видно как `venue=null`, `location_address=null`.
3. `vk_auto_queue.py` подставил площадку источника при пустом `draft.venue`. У источника `vk_source.group_id=219175543` имя «Тёрка»; справочник `docs/reference/locations.md` развернул его в «Пространство Тёрка, Пл. Победы 4». В `candidate_payload` сохранена именно эта подстановка.
4. Identity adjudicator увидел конфликт между «Тёркой» в старой карточке 9137 и «СКЛАДОМ» из нового исходного поста; сохранил `FINAL_DISTINCT`, вследствие чего появилась карточка 9379 с ошибочным адресом. Публикационный конвейер распространил адрес в Telegram/VK/Telegraph/ICS.

## Contributing Factors

- Source fact 186622 сохранил неверную локацию рядом с правильным фактом 186630.
- Опубликованные пересылки не обновляются автоматически при исправлении источника.
- Проверка grounding не терпит смешения кириллицы и латиницы в названии клуба и удаляет адрес вместе с отклонённым названием; source-level fallback доверяет названию сообщества даже при явно указанной другой площадке в посте.

## Automation Contract

### Treat as regression guard when

- Меняется извлечение venue/address из источников, Smart Update, identity adjudication, публикация или исправление пересылок.

### Affected surfaces

- Production event 9137/9379, source facts, Telegram `@kldevents`, `@kenigevents` и календарь, managed VK post, Telegraph pages и .ics.

### Mandatory checks before closure or deploy

- Оригинальный текст должен побеждать устаревшую локацию; replay через source import и Smart Update с противоположным контролем.
- Проверить адрес в обеих карточках, публичных страницах и Telegram/VK после ремонта.
- Проверить наличие/удаление ошибочной пересылки и опровержение с извинением.

### Required evidence

- Source `https://vk.com/wall-219175543_205`; `@kldevents/4192`, `@kldevents/4354`, `@kenigevents/5141`; запросы к production DB; ссылки на исправления.

## Immediate Mitigation

- Production DB: адрес обеих карточек исправлен на «Клуб «СКЛАД»», ул. Ялтинская, 20п; неверные source facts 183136/186622 помечены `conflict`.
- `@kldevents/4192` и `/4354`: подписи исправлены с явным извинением; `@kenigevents/5141`: ошибочная пересылка удалена.
- В `@kldevents/4403` и `@kenigevents/5152` опубликованы отдельные опровержения и извинения.
- В оба опровержения добавлено объяснение причины: неверная подстановка площадки по умолчанию после ошибки распознавания названия.
- Telegraph страницы двух событий перестроены на прежних URL; managed VK post `wall-231920894_11506` обновлён на прежнем URL.
- Календарные документы `@kenigeventscalendar/9288` и `/9500` и публичные .ics обновлены.
- В `@kenigeventscalendar/9528` опубликовано извинение с просьбой заменить ранее скачанный .ics.
- В календарное опровержение также добавлено объяснение причины.

## Corrective Actions

Публичная коррекция выполнена. Кодовый fix mixed-script grounding и проверка неподтверждённой площадки выложены в production SHA `f5ee69253c4991c713acd288a39bfe24d6f6941a`; полный VK import → Smart Update replay остаётся открытым.

## Follow-up Actions

- [x] Исправить символьную нормализацию grounding и направить неподтверждённый source fallback на LLM review. Целевые тесты прошли, PR #708 merged, production SHA `f5ee69253` verified; полный VK import → Smart Update replay остаётся открытым.
- [ ] Выполнить полный VK import → Smart Update replay с противоположным контролем.
- [ ] Разобрать дубликат 9137/9379 и синхронизацию производных публикаций.
- [ ] Исправить `update_source_post_keyboard` при `business_connection_id` типа int; ошибка замечена при ручной регенерации календарных .ics, на изменение площадки не повлияла.

## Release And Closure Evidence

- deployed SHA: `f5ee69253c4991c713acd288a39bfe24d6f6941a`.
- deploy path: guarded production DB repair, существующие publisher functions и `scripts/deploy_fly_main.sh` после merge PR #708.
- regression checks: Telegram API readback, production DB `events_search`, публичный HTML Telegraph, содержимое .ics, VK `wall.getById`.
- post-deploy verification: в обоих event rows и Telegraph/Telegram/VK/ICS указан «СКЛАД» / Ялтинская, 20п; `@kenigevents/5141` отсутствует; два опровержения доступны. Fly machine 148eddde9b5778 проходит health check, `/healthz` HTTP 200, SHA внутри контейнера совпадает; установленный `_vk_location_value_ungrounded` принимает `СКЛАD` при наличии «СКЛАД» в тексте.

## Prevention

Запланирован replay конкретного источника и opposite control до закрытия инцидента.
