# Festival Monitoring Technical Debt

Status: open
Owner: product/engineering
Created: 2026-06-08

## Summary

Фестивальный контур пока не считается production-complete. До закрытия этого долга:

- Telegram Monitoring для срочной проверки фестивальных источников запускается через обычный Smart Update pipeline, но может быть сужен до одного источника кнопкой `/tg` -> `Только @kraftmarket39`;
- агрегированные VK-посты о фестивалях выключены по умолчанию (`ENABLE_FESTIVAL_VK_POSTS` должен быть явно включён, чтобы вернуть старое поведение);
- публикация отдельных событий через Smart Update в `@kldevents` / `VK_EVENTS_GROUP_ID` остаётся рабочим контрактом.
- промо-кампании для живых фестивальных/образовательных программ нельзя
  проектировать как fixed `event_id`-only target set: пока импорт программы не
  закрыт и не проаудирован, кампания должна иметь динамический target по
  `event.festival`, `festival_series`, source/author trigger или другой
  явной programme-семантике. Ручные `event_id` списки допустимы только как
  curated publication subset для конкретной поверхности.
- Для `Кантата` canonical festival marker в источниках и публичных карточках —
  `event.festival="Кантата"`. Образовательная программа является segment/filter
  внутри фестиваля, а не отдельным festival value: eligibility строится как
  `Кантата` + динамический educational-programme filter, отделяющий лекции и
  образовательные события от концертов.

## Problems To Fix

1. `/start` -> `Добавить событие` всё ещё использует отличающийся путь сборки/публикации по сравнению с VK auto import и Telegram Monitoring через Smart Update. Нужно унифицировать его с общими компонентами: source log, Telegraph, VK/TG event fanout, linked-source handling, registration URL selection, media rehydration.
2. Фестивальная очередь и Universal Festival Parser требуют полного прогона, тестирования и E2E-отладки: разбор программы, program-only vs event split, источники, иллюстрации, Telegraph pages, индексы.
3. Упрощённый фестивальный мониторинг должен иметь отдельный E2E для queue handoff: Telegram source -> Smart Update -> `festival_queue` -> `/fest_queue` -> festival Telegraph/index without VK festival aggregate posts.
4. VK-фестивальные агрегаты нельзя возвращать без отдельного acceptance gate: они не должны попадать в устаревшие сообщества, смешивать несколько событий в один плохо заземлённый пост, давать битые ссылки или подменять event-specific registration URL общей ссылкой фестиваля.
5. Промо-дизайн фестивальных программ должен различать campaign eligibility и
   publication curation. `preferred_event_ids_by_date`, `carousel_event_ids` и
   похожие списки не могут быть единственным источником eligibility для
   программы, которая ещё пополняется импортом.
6. Нужен явный programme-segment слой для фестивалей вроде `Кантата`, где одно
   публичное festival name покрывает разные типы программы. До появления
   отдельного поля допускается LLM-first/semantic filter, но не ручной frozen
   `event_id` список как основной дизайн.

## Acceptance Gates

- `/start` add-event path and VK/TG auto-import paths share the same Smart Update publication contract for created/updated events.
- `@kraftmarket39` single-source monitoring can be launched from `/tg` and proves that only this source is present in Kaggle config/result import scope.
- Festival Queue E2E verifies a live Telegram festival source and records:
  - event rows created/merged through Smart Update;
  - festival queue item(s);
  - Telegraph festival page with images and description;
  - no VK festival aggregate post unless `ENABLE_FESTIVAL_VK_POSTS=1`.
- Full festival monitoring E2E verifies at least one VK source, one Telegram source, and one external festival URL.
- VK festival aggregate publishing has a separate reviewed implementation, tests, and release evidence before enabling in production.
- Promo campaign E2E for a live programme verifies that a newly imported event
  with the programme festival/series anchor becomes eligible for active promo
  activities without manually editing the campaign event-id list.
- `Кантата` education-programme E2E verifies both sides of the filter: a newly
  imported lecture/talk under `event.festival="Кантата"` is eligible, while a
  concert under the same festival marker is not selected by that education
  programme campaign.

## Routing

Canonical implemented docs:

- `docs/features/festivals/README.md`
- `docs/features/telegram-monitoring/README.md`
- `docs/features/smart-event-update/README.md`
- `docs/features/vk-publishing/README.md`

Related incident/regression contract:

- `docs/reports/incidents/INC-2026-06-08-festival-vk-aggregate-regression.md`
- `docs/reports/incidents/INC-2026-06-13-kantata-education-promo-id-only-design.md`
