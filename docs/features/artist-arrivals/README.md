# Приезды артистов: реестр, дайджест и Hero Talk

Статус: **реализован foundation + shadow rollout + candidate media bank;
public delivery выключен**.

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
- `artist_arrivals/data/curated_artist_media_candidates_batch_001.json` —
  лёгкий reviewed seed ссылок/атрибуции, без байтов;
- Object Storage/CDN — проверенные и материализованные изображения, но не
  байты в БД.

Размер полного seed: около 1,49 MiB minified / 76 KiB gzip; identity-only
проекция — около 411 KiB / 24 KiB gzip. YDB не даёт здесь выигрыша: данные
живут в event-core lifecycle, а YDB добавил бы сетевой join, IAM, отдельную
миграцию и риск рассинхронизации. YDB остаётся для собственных независимых
контуров; этот feature не переносит туда event-domain данные.

Operational tables:

- `artist_registry_entity` — sparse verified profile, freshness, evidence IDs,
  locality state и legacy single-photo compatibility;
- `artist_media_asset` — несколько изображений на артиста/участника,
  identity/quality/rights/storage state, content hashes и выбор preferred;
- `artist_media_provenance` — обязательные сервис, аккаунт, source page,
  original/discovery link, credit и rights-review для каждого изображения;
- `event_artist_appearance` — artist↔event↔project, роль, visit/cancellation
  evidence, eligibility, media identity и optional selected media asset;
- `artist_digest_issue` — один frozen manifest и threshold audit;
- `artist_publication_ledger` — dedupe по surface/target/artist/project.

Миграция: `alembic/versions/20260717_artist_arrivals.py`; тот же idempotent
SQLite DDL присутствует в `Database.init()`. Media bank добавляется следующей
миграцией `alembic/versions/20260717_artist_media_bank.py`.

## Продуктовое решение: банк изображений и атрибуция людей

### Где хранить ссылку и где хранить изображение

Выбран **hybrid**, а не link-only и не BLOB в SQLite:

1. remote URL хранится как discovery/provenance и позволяет доказать, где и у
   какого аккаунта найдено изображение;
2. candidate metadata компактно хранится в Fly SQLite рядом с event-domain;
3. только после identity + quality + rights review байты один раз
   материализуются по immutable SHA-пути в Yandex Object Storage и выдаются
   через `static.kenigevents.ru`;
4. public digest/site никогда не читают Pinterest/social hotlink напрямую.

Так ссылка не теряется при удалении/замене pin, внешний CDN не определяет
доступность нашего дайджеста, SQLite не раздувается изображениями, а YDB не
получает ещё один сетевой join. YDB для этого контура нецелесообразна: это
небольшая изменяемая метаинформация, непосредственно связанная с event/artist
rows и daily transaction. Perceptual hash используется только для review
near-duplicates; exact raw/pixel SHA — для безопасной дедупликации. Takedown
оставляет tombstone/hash block, чтобы спорный файл не вернулся повторно.

### Обязательная атрибуция

У каждого asset provenance обязательны:

- `service`;
- `account_handle` и, если доступно, display name/account URL;
- `source_page_url`;
- публичный `credit_text` вида `Telegram · @organizer` или
  `Pinterest · @pinner`.

Указывается именно сервис и аккаунт, **откуда взяты используемые байты**. Если
Pinterest помог найти официальный оригинал, но в bucket положен файл из
официального аккаунта/press kit, публично указывается этот оригинальный
сервис+аккаунт; Pinterest pin остаётся внутренним `discovery_url`. Если взяты
байты самого pin, credit содержит `Pinterest · @pinner`, а original link/author,
когда они известны, сохраняются дополнительно. Наличие attribution не заменяет
rights review.

### Автоматический выбор

Public selector уже fail-closed выбирает только asset, где одновременно:

- `lifecycle_status=ready`, `storage_status=ready`;
- identity и quality `verified/approved`;
- rights status входит в разрешённый список;
- provenance `approved` и содержит service+account+source page+credit;
- URL находится на managed HTTPS CDN.

Default public host contract — `ARTIST_MEDIA_CDN_HOSTS=static.kenigevents.ru`;
это отдельный allowlist от discovery/download allowlist.

Приоритет: явно выбранный asset конкретной appearance → проверенное чистое
фото из точного event announcement → preferred press kit/official artist →
organizer/venue → reviewed informational-use asset → curated Pinterest bank →
legacy profile fallback → text-only. В daily manifest сохраняются asset id,
service, account, credit и source link. Remote candidate никогда не становится
public из-за одного совпадения имени.

Producer для event announcement должен брать только уже approved CDN
`EventPoster`, LLM/VLM-first подтверждать, что изображён именно участник и что
это чистое фото без редакционной плашки, затем создавать/reuse
`artist_media_asset` с event provenance. Этим же контуром event-фото расширяет
банк для будущих проектов. **Data model и selector реализованы; автоматический
producer/materializer event-poster → artist bank ещё не подключён**, поэтому до
его canary пополнение из событий остаётся reviewed operation.

### Артисты, селебрити, спикеры и медальоны

`celebrity` — не entity type и не факт события, а редакционный признак
привлекательности. Базовая сущность должна обобщаться до
`person | collective | organization`, а связь с событием обязана хранить роль
и presence semantics: live performer/speaker/host, creative credit,
recorded cast или subject reference. Автор произведения/актёр в фильме не
считается приехавшим без отдельного подтверждения очного участия.

Людей **не нужно помещать в существующий ряд event-медальонов**. По
[контракту event token medallions](../static-site-pages/event-token-medallions.md)
медальоны
остаются быстрыми event-level фактами/брендами (организатор, площадка,
фестиваль, Пушкинская карта, доступность). Лицо без видимого имени и роли плохо
идентифицируется, а roster вытеснит факты из лимита 4–6 токенов. Для людей нужен
отдельный identity attribution:

- на event detail — видимые `имя + роль`, optional portrait 56–72 px, секции
  «Участники», «Спикеры», «Авторы и создатели», «В фильме/записи»;
- на главной и в social — отдельные narrative cards/«К нам едут»;
- позже — profile/follow surface по стабильному entity id.

Круглая фотография внутри identity card визуально может напоминать медальон,
но остаётся другим продуктовым объектом.

### Продуктовые acceptance-метрики

- 100% public images имеют service + account + кликабельный source page;
- 0 remote hotlinks и 0 candidate/review assets в public projection;
- identity precision проверяется отдельно от rights decision;
- arrival precision не смешивает live participation с author/tribute/recording;
- `media_ready coverage` считается по eligible arrivals, но отсутствие фото
  ведёт к честной text-only карточке, а не к неверному лицу;
- Pinterest funnel измеряется как collected → reviewed → identity keep →
  rights approved → bucket ready, чтобы большой search result не выдавался за
  реальное покрытие.

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

## Горизонт, Daily issue и dedupe

Job `artist_arrivals_daily` запускается не чаще раза в сутки и по умолчанию
берёт **все будущие** проверенные appearances, уже присутствующие в
каноническом каталоге событий (`ARTIST_ARRIVALS_HORIZON_DAYS=0`). Поэтому
анонс любимого артиста за полгода не теряется. Положительное значение env
остаётся только эксплуатационным bounded override, а не продуктовым правилом.

Это не копирует весь `event`: запрос идёт по sparse
`event_artist_appearance` и сохраняет ссылки/минимальный manifest. При
production probe 17 июля 2026 вся SQLite занимала около 249 MiB, а тестовый
artist overlay (26 profiles, 20 appearances и issue на 18 items) — меньше
0,1 MiB. Рост создаёт не дальность события, а бесконечное хранение ежедневных
полных preview. Поэтому `ARTIST_ARRIVALS_SHADOW_RETENTION_DAYS=45` удаляет
только старые неопубликованные issues без delivery ledger; опубликованные,
scheduled и неоднозначные `sending` issue сохраняются для аудита. При
наблюдаемом размере около 24 KiB на issue это ограничивает обычный shadow-хвост
примерно одним MiB вместо ~8,6 MiB в год.

Social digest готов только при:

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
Ближайшие приезды ранжируются первыми в общем публичном digest, но дальняя
запись не отбрасывается. Персональная формулировка «любимый артист» требует
отдельного join с пользовательскими подписками/интересами: foundation уже
сохраняет стабильный `artist_id`, однако персональные уведомления этой веткой
ещё не включены.

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
  `cc_verified`; либо оператором отдельно принят
  `informational_citation_reviewed`;
- существует отдельная запись provenance/лицензии в
  `photo_rights_evidence_json`;
- URL материализуется как изображение допустимого размера.

### Информационное использование и Pinterest

Некоммерческий характер агрегатора и отсутствие продажи билетов учитываются в
редакционной оценке, но сами по себе не превращают любое открытое изображение в
свободное. Для информационной публикации российское право допускает при
определённых условиях цитирование в том числе фотографического произведения;
при этом нужны правомерное обнародование, информационная цель, оправданный
объём, имя автора и источник. Отдельно проверяется право на изображение
гражданина и связь использования с общественно значимой деятельностью, а не с
частной жизнью. Это operational policy, а не гарантия исхода конкретного спора;
перед public auto её должен принять ответственный за юридические риски.

Практическая лестница источников:

1. фото/press kit от артиста, организатора или площадки;
2. открытая лицензия или официальный event announcement;
3. `informational_citation_reviewed` для правомерно опубликованного фото,
   непосредственно необходимого для сообщения «кто, с каким проектом и когда
   приезжает»;
4. Pinterest и другие платформы как discovery-кандидат с обязательным
   platform account; до отдельного identity/rights/storage review он не public.

Для третьей ступени gate требует в evidence: HTTPS `source_url`, автора или
правообладателя/точный `credit_text`, `service` + `account_handle`, подтверждение правомерного обнародования,
`basis=gc_rf_1274_informational_citation`,
`purpose=artist_arrival_information`, ручной `approved`, reviewer и timestamp.
`discovery_url` Pinterest хранится для аудита, но сам по себе не заменяет
rights basis. Публичная атрибуция всегда следует фактическим байтам: для файла
из найденного оригинального аккаунта — сервис+аккаунт оригинала, для файла из
pin — `Pinterest · @pinner`; оба URL можно сохранить в audit trail.

На карточке выводится нейтральное `Фото: <сервис> · @<аккаунт>`, а в тексте
TG/VK — точная кликабельная ссылка. Логотип Pinterest, Pinterest badge, формулировки
«при поддержке»/«партнёр» и стилизация под Pinterest не используются: так не
создаётся впечатление аффилированности. Если установлен только pin/account, но
не пройден отдельный rights review и не создан managed asset, карточка остаётся
text-only.

Публичная политика должна дополнительно содержать понятный канал обращения
правообладателя, оперативный takedown/correction, сохранение source URL и
редакционного решения в audit trail и запрет повторного использования
оспоренного media hash. Условие пользовательского соглашения объясняет этот
процесс пользователям, но не является лицензией от третьего лица.

Нормативные/платформенные основания, проверенные 17 июля 2026:

- [статья 1274 ГК РФ](https://www.consultant.ru/document/cons_doc_LAW_64629/84bbd636598a59112a4fe972432343dd4f51da1d/);
- [пункт 98 постановления Пленума ВС РФ № 10 от 23.04.2019](https://www.vsrf.ru/files/27771/);
- [статья 152.1 ГК РФ](https://www.consultant.ru/document/cons_doc_LAW_5142/14c6c3902cffa17ab26d330b2fd4fae28e5cd059/)
  и [пункты 43–48 постановления Пленума ВС РФ № 25 от 23.06.2015](https://www.vsrf.ru/files/14913/);
- [Pinterest Terms](https://policy.pinterest.com/en/terms-of-service) —
  запрет неавторизованного automated scraping и разделение Pinterest IP от
  user content.

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

### Pinterest photo-bank batch 001

Operator-run collection:

`/home/dev/projects/pinterest-idea-library/collections/20260717-kenigevents-artist-celebrity-photo-bank-batch-001/pins.json`

- 12 exact-name query families;
- 353 search candidates → balanced 60 downloaded thumbnails;
- Codex self-review: 5 `keep`, 2 `maybe`, 53 `reject`;
- external Gemini/Opus review не использовался;
- в project seed записаны 7 candidate links с Pinterest service, pinner account,
  pin URL, original link (когда Pinterest его сообщает), observed dimensions и
  quality blocker.

Первый банк покрывает кандидатами:

- Максим Аверин — `@natalileschenko`, `@starhit`, `@liiwrm`; отдельно
  `@mulkirx` требует clean original без небольшого watermark;
- Елена Ваенга — `@joinfonews`, `@hodunko75`;
- Сергей Жилин — `@natalasinilo34`, но доступный original всего `320×320` и не
  проходит digest quality gate.

Ни один из семи assets не public-ready: они остаются
`remote_candidate/rights=review`, без `cdn_url` и без сохранённых Pinterest
bytes. Для КВАТРО, ЭПИДЕМИИ, PUPO, Can Saraç, Артура Беркута/Сергея Маврина,
Марии Макаровой, Константина Хачикяна, Teresa Voskanyan и Даниила Саямова
exact-name Pinterest выдача оказалась визуально нерелевантной. Это важный
продуктовый результат: name search хорошо работает как ручной discovery для
части известных лиц, но непригоден как automatic identity resolver для групп,
нишевых и транслитерированных имён.

## Проверки

```bash
pytest -q tests/test_artist_arrivals.py
python -m py_compile artist_arrivals/*.py models.py db.py scheduling.py promo.py
cd site && npm run build
```

Тесты покрывают idempotent artist/media seed, link-only candidate state,
managed-CDN event-photo priority with service/account attribution, threshold matrix, local/unknown suppression,
artist+stable-project grouping, target-aware ledger dedupe и reconciliation,
полную source-revision invalidation, photo-rights gate, RichMessage HTML,
draft promo activities, network-safe shadow mode и public projection.
