# Seed sources v1 — Region Talk Channel

Status: starter seed list for MVP-1 Candidate Report Only. This is **not** a final allowlist and does not create channels, tokens, production crawlers or publications. Machine-readable file: [`seed-sources-v1.csv`](seed-sources-v1.csv).

## Rules

- `initial_status=seed_monitor_candidate` means “safe enough to consider for the first probe”, not automatic publication.
- `monitoring_enabled=true` only marks a small initial probe set; implementation may still cap/disable it through `REGION_TALK_MAX_SOURCES` and dry-run config.
- `rights_policy=unknown` blocks media reuse and autopublish. MVP-1 report can still show links/thumbnails for review.
- Catalog/discovery-hub rows are discovery inputs; they are not direct post sources unless separately accepted.
- Personal profiles remain out of MVP unless manually accepted later.

## CSV columns

`source_seed_id`, `platform`, `source_title`, `handle`, `url`, `source_kind`, `source_scope_guess`, `priority`, `discovered_from`, `discovered_from_url`, `why_seeded`, `expected_value`, `known_risks`, `initial_status`, `monitoring_enabled`, `rights_policy`, `notes`

Allowed core values follow the feature schema (`platform=telegram|vk|vkvideo|web`, `source_scope_guess=external|mixed|regional|unknown`, `rights_policy=unknown|link_only|forward_allowed|media_reuse_allowed|blocked`). For seed discovery, `source_kind` also allows `catalog` and `discovery_hub` in addition to travel/author/project kinds.

## Starter seeds

| ID | Platform | Priority | Source | Handle | Kind | Scope | Initial status | Monitoring | Why seeded | Risks |
|---:|---|---:|---|---|---|---|---|---|---|---|
| 1 | telegram | 1 | Красивые места России | @viewrussia | travel_media | mixed | seed_monitor_candidate | true | визуальный канал о красивых местах России, локациях и координатах | возможные права на пользовательские фото; media reuse только после rights check |
| 2 | telegram | 1 | РГО | @rgo1845 | official_project | mixed | seed_monitor_candidate | true | география, путешествия по России, сильный фотоархив, проектный источник | возможны новости/анонсы; media reuse требует rights check |
| 3 | telegram | 1 | Путешествуем.РФ | @puteshestvuem_rf | official_project | mixed | seed_monitor_candidate | true | главный канал о путешествиях по России, маршруты, места, карточки | официально-проектный тон, возможны промо/анонсы |
| 4 | telegram | 2 | RUSSPASS | @russpassmag | travel_service | mixed | seed_candidate | false | путешествия по России, маршруты, туристические места | промо/коммерческий контент; нужен ad_score |
| 5 | telegram | 2 | Туту | @tutu_travel | travel_service | mixed | seed_candidate | false | крупный travel channel с большим объёмом контента | промо/сервисный контент; нужен ad_score и source_novelty_score |
| 6 | telegram | 1 | Вокруг Света | @vokrugsveta1861 | travel_media | mixed | seed_monitor_candidate | true | travel/history/science media, сильный контент о местах и регионах | часть контента не travel; нужен semantic filter |
| 7 | telegram | 2 | Моя Планета | @moya_planeta | travel_media | mixed | seed_candidate | false | крупный travel/media источник | высокая новостность/происшествия; нужен newsiness_score |
| 8 | telegram | 1 | Путешествия со смыслом / Алексей Жирухин | @bepowerback_travel | author_channel | external | seed_monitor_candidate | true | авторский блог о путешествиях по России и труднодоступных местах | может редко писать о Калининграде |
| 9 | telegram | 2 | Своим Ходом — Виталик и Лиза | @svoimxodom | author_channel | external | seed_candidate | false | крупный авторский travel channel | много международного контента; нужен region relevance filter |
| 10 | telegram | 1 | Нежный Travel | @nejniy_travel | author_channel | external | seed_monitor_candidate | true | авторский travel channel с маршрутами и сильными фото/видео | много не-российского контента; нужен region relevance filter |
| 11 | telegram | 2 | Путешествия по России / паспорт, виза, два билета | @pasport_visa_tickets | travel_media | mixed | seed_candidate | false | travel channel с местами и локациями | заметная новостность/incident/travel news; строгий newsiness filter |
| 12 | telegram | 2 | Alexander Pilot | @dashapilotessa | author_channel | external | seed_candidate | false | авторский travel channel с большим числом фото/видео | много международного контента |
| 13 | telegram | 2 | Из Москвы на выходные | @izmoskvynavyhodnye | route_project | external | seed_candidate | false | weekend-route формат, полезен для source discovery | география вокруг Москвы; Калининград может встречаться редко |
| 14 | telegram | 3 | Travelhacks | @travelhacks | travel_media | external | seed_candidate | false | travel/lifehack канал из каталогов, полезен для graph discovery | может быть мало сильных фото; скорее discovery source |
| 15 | telegram | 1 | Зона комфорта | @russiamatters | travel_media | external | seed_monitor_candidate | true | канал о путешествиях по России, отелях, ресторанах и музеях | проверить фактическую визуальную силу |
| 16 | telegram | 2 | Земля приключений | @travelerdv | official_project | mixed | seed_candidate | false | проект о путешествиях и конкурсах, полезен как graph/discovery hub | фокус на Дальний Восток, Калининград может быть редким |
| 17 | telegram | 3 | ПОЕХАЛИ / Первый канал | @Poehali_1tv | travel_media | mixed | seed_candidate | false | travel TV/project source | промо-анонсы, ТВ-формат, возможна низкая частота релевантных постов |
| 18 | vkvideo | 1 | Интересные путешествия | @intravel39 | travel_media | mixed | seed_monitor_candidate | true | VK Video travel channel, в подборке упоминался контент про Калининград | проверить доступность API/постов и права на media |
| 19 | vkvideo | 2 | Коте Оганезов | @koteoganezov | author_channel | external | seed_candidate | false | travel/food blogger format, потенциально полезен для гастрономии/маршрутов | не гарантирован Калининград; нужен relevance filter |
| 20 | vkvideo | 2 | Своим Ходом — Виталик и Лиза | @svoimxodom | author_channel | external | seed_candidate | false | VK layer того же travel-блогерского проекта | связать с Telegram identity; проверить API access |
| 21 | vkvideo | 1 | Нежный Travel | @nejniy_travel | author_channel | external | seed_monitor_candidate | true | VK layer авторского travel-проекта | проверить API access и media reuse |
| 22 | vkvideo | 1 | Путешествия со смыслом / Алексей Жирухин | @bepowerbacktravel | author_channel | external | seed_monitor_candidate | true | VK layer авторского проекта о России | проверить canonical mapping с Telegram @bepowerback_travel |
| 23 | vkvideo | 1 | РГО | @rgoclub | official_project | mixed | seed_monitor_candidate | true | официальный/проектный VK Video источник РГО | media rights and official repost policy |
| 24 | vkvideo | 2 | Моя Планета | @moyaplaneta | travel_media | mixed | seed_candidate | false | крупный travel/media source | news/incident contamination; строгий newsiness filter |
| 25 | vkvideo | 3 | Лесные — жизнь в лесу | @lesnue_channel | nature_project | external | seed_candidate | false | nature/outdoor source, полезен для visual discovery | Калининград может не встречаться; скорее expansion seed |
| 26 | web | 1 | Telega.in travel/tourism catalog | — | catalog | unknown | seed_monitor_candidate | false | каталог travel/tourism Telegram channels with descriptions/subscribers/ERR | catalog data may be stale; do not auto-monitor every entry |
| 27 | web | 1 | TLGRM travel category | — | catalog | unknown | seed_monitor_candidate | false | дополнительный каталог Telegram travel channels | catalog data may be stale |
| 28 | web | 1 | TGStat travel category | — | catalog | unknown | seed_monitor_candidate | false | дополнительный источник discovery/cross-check | catalog access/rate limits; data may be stale |
| 29 | web | 1 | VK Video travel channel selections | — | catalog | unknown | seed_monitor_candidate | false | стартовый источник VK Video travel handles | manual extraction may be needed; API availability unknown |
| 30 | vk | 2 | VK Места | — | discovery_hub | mixed | seed_candidate | false | travel hub with routes/places/media-expedition materials, useful for discovery | not necessarily a direct post parsing source |
