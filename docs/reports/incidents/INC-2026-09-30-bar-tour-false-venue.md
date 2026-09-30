# INC-2026-09-30 Барный тур: выдуман адрес встречи

Status: mitigated
Severity: sev2
Service: events-bot production event data and Telegram/VK publications
Opened: 2026-09-30
Closed: —
Owners: events-bot operator
Related incident: `INC-2026-09-30-donera-wrong-venue.md`

## Summary

Карточки 9281 (2 октября) и 9282 (3 октября) указывали «Пространство Тёрка, пл. Победы, 4» как место барного тура. В исходном VK-посте `https://vk.com/wall-219175543_199` указаны даты и набор участников, но не место встречи. Адрес нельзя считать подтверждённым.

## Impact and detection

Неверный адрес появился в `@kldevents/4311`, пересылке `@kenigevents/5137`, карточках, Telegraph, VK и календарях. Пользователь сообщил о посте 2026-09-30.

## Evidence and cause

- `vk_source_packet.id=14856`: исходный текст говорит о туре по барам и датах 02.10/03.10; адрес не назван.
- Сохранённый LLM parse `decision.events` уже содержит `location_name=Пространство Тёрка`, `location_address=Пл. Победы 4` для обеих дат.
- `smart_update_candidate_state.id=18386/18387` и `event_source_fact.id=185320/185324` сохранили неверный адрес. У источника есть профиль/адрес «Тёрки»; парсер перенёс адрес площадки источника в место встречи тура без подтверждения исходным постом. Проверка grounding пропустила эту подстановку, потому что считала профиль источника допустимым свидетельством и требовала явного маркера места в тексте для дополнительного LLM review.

## Mitigation and verification

- В production DB карточки 9281/9282 показывают «Место встречи уточняется у организатора» без адреса; неверные source facts 185320/185324 помечены `conflict`.
- Пост `https://t.me/kldevents/4311` исправлен на прежнем URL с извинением и объяснением. Отдельные опровержения опубликованы в `https://t.me/kldevents/4404` и `https://t.me/kenigevents/5154`.
- Пересылка `https://t.me/kenigevents/5137` остаётся доступной: Telegram ответил `message can't be edited` и `message can't be deleted` через Bot API; MTProto `channels.DeleteMessagesRequest` ответил `MessageDeleteForbiddenError`. Опровержение `/5154` закреплено в канале и ссылается на ошибочную пересылку.
- Telegraph страницы `https://telegra.ph/Barnyj-tur-09-24` и `https://telegra.ph/Barnyj-tur-09-24-2` обновлены на прежних URL. VK posts `https://vk.com/wall-231920894_11220` и `/11322` обновлены; API readback показывает отсутствие «Тёрка»/«Победы» и указание уточнить место встречи. ICS-проекции у событий отсутствуют.

## Corrective actions

- [x] Направлять VK-кандидатов с площадкой, отсутствующей в тексте и афише, на LLM review и fail closed при отсутствии подтверждения. Реализовано в `smart_event_update.py`, проверено целевыми тестами; production deploy ожидается.
- [ ] Replay сохранённого VK packet через import и Smart Update с противоположным контролем, где профиль источника действительно является площадкой.
- [ ] После релиза проверить, что повторный импорт не возвращает ложный адрес.

## Release evidence

- Source branch: `hotfix/venue-grounding-20260930` от `origin/main`.
- Local checks: 39 targeted tests passed, включая неверный барный тур, mixed-script «СКЛАD» и противоположный случай с явной площадкой.
- Deployed SHA: pending.
