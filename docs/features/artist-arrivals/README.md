# Приезды артистов: реестр, дайджест и Hero Talk

Статус: **реализован foundation + shadow rollout; public delivery выключен**.

Фича находит подтверждённые очные приезды артистов из других регионов России
и из-за рубежа в Калининградскую область. Один замороженный daily manifest
питает:

1. Telegram RichMessage slideshow;
2. VK carousel;
3. статическую проекцию `artist-arrivals.json` и Hero Talk preview;
4. две независимые активности промо-кампании:
   `artist_arrival_digest` и `artist_arrival_hero`.

Местный артист не получает arrival-метку, но **не исключается** из общей афиши
и других промо-механик.

## Хранилище: JSON + Fly SQLite, без YDB

Выбрана компактная гибридная схема:

- `docs/reference/data/artist_registry_batch_001.canonical.json` — versioned,
  git-diffable identity seed на 1 235 сущностей;
- Fly SQLite — только изменяемая sparse overlay: row-level locality evidence,
  appearances, замороженные issues и delivery ledger;
- `site/src/data/artist-arrivals.json` — очищенная read-only проекция для SSG;
- Object Storage/CDN — будущие проверенные изображения, но не байты в БД.

Размер полного seed: около 1,49 MiB minified / 76 KiB gzip; identity-only
проекция — около 411 KiB / 24 KiB gzip. YDB не даёт здесь выигрыша: данные
живут в event-core lifecycle, а YDB добавил бы сетевой join, IAM, отдельную
миграцию и риск рассинхронизации. YDB остаётся для собственных независимых
контуров; этот feature не переносит туда event-domain данные.

Operational tables:

- `artist_registry_entity` — sparse verified profile, freshness, evidence IDs,
  photo/rights state и отдельное rights-evidence;
- `event_artist_appearance` — artist↔event↔project, роль, visit/cancellation
  evidence, eligibility и media identity;
- `artist_digest_issue` — один frozen manifest и threshold audit;
- `artist_publication_ledger` — dedupe по surface/target/artist/project.

Миграция: `alembic/versions/20260717_artist_arrivals.py`; тот же idempotent
SQLite DDL присутствует в `Database.init()`.

## Решение о приезде

Публично допустимы только строки, где одновременно:

- event canonical, active, future/ongoing и не `silent`;
- participant identity и очное участие подтверждены source evidence;
- locality — `non_local_ru_verified` или
  `non_local_international_verified`;
- row evidence не истёк;
- appearance `eligible`, не cancelled и не tribute/recording/author-only;
- проект имеет стабильный `project_key`.

`local_verified`, `unknown` и `mobile_or_mixed` fail closed. Отсутствие имени
в seed ничего не доказывает. Широкие keyword/regex-правила не принимают
semantic decision: candidate recall остаётся вспомогательным, а дальнейшее
обогащение должно следовать LLM-first контракту из
[`artist-visit-registry.md`](../../reference/artist-visit-registry.md).

Operational downgrade имеет приоритет над seed: `review/manual_hold/rejected`,
local/unknown/mobile status, cancellation/ineligible и уже проверенные media
поля не перезаписываются очередным daily seed. Новая appearance по умолчанию
получает `review`, а не `confirmed/eligible`; planner дополнительно требует
`verification_status=verified`.

## Daily issue и dedupe

Job `artist_arrivals_daily` запускается не чаще раза в сутки и по умолчанию
строит окно 14 дней. Social digest готов только при:

- минимум 3 уникальных артиста;
- предпочтительно 4;
- минимум 2 уникальных проекта;
- максимум 8 карточек (Bot API slideshow ограничен здесь 3–10).

`build_date` и граница окна входят в manifest hash: даже при неизменном составе
каждый календарный daily run получает отдельный frozen issue, а повторный запуск
в тот же день остаётся идемпотентным.

`project_key` — стабильное семейство проекта без года/месяца; конкретный
гастрольный заезд хранится отдельно в `visit_cluster_key`. Поэтому следующий
сезон той же программы не обходит dedupe, а действительно другой проект того
же артиста допускается. Несколько дат одной связки `(artist_id, project_key)` группируются в одну
карточку. Frozen issue сохраняет все актуальные eligible items, а
`social_selected` отмечает только новые digest-карточки. Повтор блокируется
лишь после успешной доставки в настроенную пару `(surface, target)` Telegram и
VK: тестовый target не подавляет будущую production-доставку. Частичная
доставка остаётся кандидатом на недостающий channel. Другой проект того же артиста допустим.
Hero Talk использует полный список и поэтому не подавляется social ledger.

`source_revision` включает title/description/short description/search digest,
date/end date/time, venue/city, все source texts и TG/VK source URLs, ticket и
lifecycle/identity/silent state. Любое смысловое изменение события инвалидирует
старый reviewed appearance до повторной проверки.

## Telegram / VK / media gate

Telegram собирается как `InputRichMessage` с `<tg-slideshow>` и локально
отрендеренными 1080×1350 JPEG. VK использует те же карточки,
`upload_vk_photo_bytes()` и `post_to_vk(..., carousel=True)`.

Изображение допускается только когда:

- artist↔photo identity имеет `media_identity_status=verified`;
- rights status — `event_artist_verified`, `press_kit_verified` или
  `cc_verified`;
- существует отдельная запись provenance/лицензии в
  `photo_rights_evidence_json`;
- URL материализуется как изображение допустимого размера.

Built-in fetcher принимает только credential-free HTTPS URL из явного
`ARTIST_ARRIVALS_PHOTO_HOST_ALLOWLIST`, проверяет каждый redirect и DNS на
public IP и читает stream с жёстким лимитом 12 MiB. Это не заменяет rights QA,
а лишь закрывает SSRF/oversized-fetch границу.

Event poster сам по себе не доказывает, что на нём нужный артист. В shadow
review renderer даёт deterministic text-only карточку; auto publication при
непроверенных фото fail closed.

## Safety switches и rollout

Тройной public gate:

1. `ARTIST_ARRIVALS_PUBLICATION_MODE=auto`;
2. `ARTIST_ARRIVALS_ALLOW_PUBLICATION=1`;
3. activity `artist_arrival_digest` явно `enabled=true`.

`ensure_artist_arrivals_promo_campaign()` создаёт campaign как `draft`, а обе
activity — disabled с `publication_mode=shadow`. Поэтому применение миграции
или включение daily job само по себе ничего не публикует.

Рекомендуемый rollout:

1. 7 дней daily shadow issues и ручная проверка false positives/duplicates;
2. верификация artist photos и rights;
3. Telegram test target;
4. VK postponed/test group;
5. homepage promotion из preview-компонента;
6. только после gate — public auto.

Production homepage пока остаётся noindex placeholder из `origin/main`.
Компонент `ArtistArrivalsHeroTalk.astro` подключён к `/<preview>/`; перенос на
реальную главную является отдельным release gate, а не скрытым изменением
placeholder.

Static exporter дополнительно проверяет `window_end`, удаляет уже прошедшие
даты и выставляет public `eligible=true` только если campaign `active`, а Hero
activity включена. Preview использует отдельный `shadow_eligible` override и
явно маркирует shadow-состояние.

Текущий daily слой применяет evidence-reviewed curated overlay. Полностью
автоматическое LLM-first обнаружение новых participant candidates из каждого
нового event source пока не подключено: до этой отдельной стадии новые имена
добавляются через reviewed overlay и не могут попасть в auto по одному
лексическому совпадению.

### Неоднозначный network outcome

Перед каждым TG/VK send ledger атомарно получает `sending`. Если сеть оборвалась
после возможной публикации, автоматический повтор блокируется. Оператор обязан
сначала проверить **точный target** и весь набор карточек, затем вызвать
`reconcile_artist_arrival_delivery(...)` с теми же `surface`, `target` и всеми
`dedupe_keys` issue:

- `outcome="published"` — только если carousel найден; передать URL/message id;
- `outcome="not_published"` — только если подтверждено, что carousel отсутствует;
  reservations удаляются и повтор снова разрешён.

Blind cleanup `sending` строк запрещён. Утилита требует точного совпадения
набора и отказывается менять уже финализированные строки.

## Curated snapshot на 17 июля 2026

`artist_arrivals/data/curated_artist_evidence.json` содержит 26 sparse profiles
и 20 проверенных appearances. В предварительном списке:

**Международные гости:** Matteo Buonannoce (17 июля), João Neto Vieira
(24 июля), PUPO (29 июля), Can Saraç / Джан Сарач (31 июля), Teresa Voskanyan
(9 августа).

**Из других регионов России:** ЭПИДЕМИЯ (17 июля, duplicate events сводятся в
один проект), Эд Авдали (17–18 июля), Максим Аверин (18 июля), КВАТРО
(19 июля), Елена Ваенга (20 июля), Лев Чефанов (23 июля), Даниил Саямов
(24 июля), Театр танца «Шторм» (25 июля), Сергей Жилин и
«Фонограф-Джаз-Трио» (27 июля), Игорь Сидоров (30 июля), Константин Хачикян
(7 августа), Мария Макарова (7 августа), Артур Беркут и Сергей Маврин
(15 августа).

**Local suppression overlay:** HOFFMANN TRIO (состав подтверждён через текущий
оркестр Калининградского областного музыкального театра), Квартет Алексея
Маркова (Музей янтаря прямо указывает «Калининград»),
Мария Гаврилюк, Анна Ушакова, Анна Юсупова, Евгений Авраменко, Мансур Юсупов,
Камерный оркестр Калининградской областной филармонии.

Отдельно зафиксированы exclusions: tribute «АРИЯ» в исполнении Black Bulls,
Linkin Park tribute, tribute festival, author/composer-only mentions и общий
Pianissimo container. Неуверенные local club acts остаются `review/unknown`,
а не получают автоматическую arrival-метку.

## Проверки

```bash
pytest -q tests/test_artist_arrivals.py
python -m py_compile artist_arrivals/*.py models.py db.py scheduling.py promo.py
cd site && npm run build
```

Тесты покрывают idempotent seed, threshold matrix, local/unknown suppression,
artist+stable-project grouping, target-aware ledger dedupe и reconciliation,
полную source-revision invalidation, photo-rights gate, RichMessage HTML,
draft promo activities, network-safe shadow mode и public projection.
