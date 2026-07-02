# CherryFlash Partner Story Tracks

> **Status:** MVP implemented (2026-05-14) — `partner_eco_nature_001`, `partner_region_east_001`, and `partner_konb_library_001` ship through `/v`; remaining items (LLM-driven east geo classifier, per-run Kaggle isolation, source audit, scheduled runs) are tracked in the acceptance checklist below.
> **Scope:** partner-specific CherryFlash story releases for Telegram Business accounts, where each partner can have its own event filter, geo filter, schedule, render profile, and Business Story target.

## Product goal

CherryFlash must support many partner-specific story tracks, not one hardcoded personal account. A partner track is a reusable configuration that selects events for a partner audience, renders a CherryFlash-style vertical video, and publishes the result to the partner's Telegram Business Stories through the existing encrypted Business Stories pipeline.

This document is the canonical feature spec for those partner tracks. Operational details for Bot API story posting, encrypted `business_connection_id` storage, and webhook capture stay in `docs/features/telegram-business-stories/README.md`.

## Privacy and target identity

- Do not commit raw personal Telegram handles, Telegram user ids, or `business_connection_id` values to repo docs, code, env, logs, Kaggle datasets, or artifacts.
- Partner targets must be stored and matched through the existing encrypted Business connection cache plus safe selectors:
  - `connection_hash`
  - `user_hash`
  - `username_hash`
  - stable internal `partner_id`
- The requester supplied the first partner handle out-of-band in chat. In repo-visible documentation and config this partner is referred to only as `partner_eco_nature_001`.
- Runtime allowlists must use `setting.video_announce_story_business_targets` or a future structured equivalent that stores hash labels, not raw handles.

## Partner track model

Each partner track should be represented as structured config, eventually DB-backed or admin-editable:

```yaml
partner_track:
  id: partner_eco_nature_001
  status: planned
  product: cherryflash
  business_target_selector:
    type: encrypted_business_connection
    value: "<connection_hash|user_hash|username_hash>"
  content_filter_id: eco_prirodnaya
  display_name: "эко-природная"
  geo_filter_id: null
  source_policy:
    guaranteed_sources: []
    review_required_sources: []
    excluded_sources: []
  render:
    profile: popular_review
    test_gpu_first: true
    steady_state_gpu: false
  publish:
    surface: telegram_business_story
    post_to_chat_page: true
    required: true
```

Required fields for every track:

- `id` — stable internal label, safe to print.
- `business_target_selector` — hash-based target selector resolved against encrypted Business cache.
- `content_filter_id` and/or `geo_filter_id` — one or more selection contracts.
- `source_policy` — optional source allow/deny rules, especially for geo-guaranteed sources.
- `render.profile` — CherryFlash render profile.
- `publish.surface=telegram_business_story` — publication goes through Bot API Business Stories, not Telethon personal sessions.

## Rendering and Kaggle policy

- First generation, first regeneration, and visual/debug passes for a new partner track must run on Kaggle with GPU enabled.
- After the track is stable and no longer needs heavy preview/debug iteration, steady scheduled runs should be able to run without GPU.
- GPU fallback must be explicit:
  - if Kaggle rejects a GPU run because of quota, the run may retry without GPU only when the track is already marked stable or the operator explicitly asks for a CPU fallback;
  - for a new unvalidated partner track, GPU quota exhaustion should leave a clear failed/preflight state rather than silently accepting a lower-quality first proof.

### Parallel Kaggle runs

Partner tracks must be parallel-safe before more than one track is scheduled.

Current CherryFlash kernel deployment mutates the shared Kaggle kernel metadata and prunes/replaces `cherryflash-session-*` dataset sources. That model is not safe for concurrent partner runs on the same kernel slug: one run can detach or replace another run's session dataset.

Required parallel-safe contract:

- each live run has a unique `cherryflash-session-*` dataset;
- no run may mutate a shared kernel's `dataset_sources` while another run on that kernel is active;
- acceptable implementation options:
  - create a per-run Kaggle kernel copy from an immutable notebook template;
  - or allocate separate per-partner kernel slugs and serialize only within the same slug;
  - or move to a launch path where the notebook runtime receives a session dataset without editing shared kernel metadata.
- session output must include `partner_track_id`, `content_filter_id`, `geo_filter_id`, `kaggle_kernel_ref`, and the exact session dataset slug.
- `story_publish.json` and encrypted Business story secrets must remain co-located with the same session dataset as the render payload, so a parallel run cannot pick stale or cross-partner story secrets.

## Production hardening: 2026-05-15 incident

Regression contract: `docs/reports/incidents/INC-2026-05-15-cherryflash-partner-fanout-promo-filter.md`.

Partner tracks are Business-only story publications. A partner run must never
inherit the shared `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` chain used by base
CherryFlash/CrumpleVideo. In `selection_params`, an explicit
`story_targets_override=[]` means "no Telethon targets"; only
`story_business_targets` may add targets for the partner track.

Scheduled or direct one-click partner runs have no manual approval step, so
`manual_review` filter decisions are fail-closed for publication. The eco
classifier retries Gemma and then uses the configured 4o fallback for the small
classification request; if all provider paths fail, the candidate is skipped
rather than published. The watchdog may try a later run, but it must not fill
the partner video with unrelated events after classifier errors.

Partner tracks do not freely inherit base `popular_review` promo campaigns. The
eco/nature track has one explicit exception: after the selector already has at
least three partner-filtered profile matches, it may admit at most one promo
candidate that does not pass the eco filter. Promo candidates that do pass the
eco filter follow the normal promo merge rules. This lets broad editorial promo,
for example `80 историй о главном`, appear periodically without replacing the
eco/nature core of the release. Off-filter promo is always downgraded to
any-position placement for the eco track, so a base `first_slot` campaign cannot
take the first eco slot unless it also passes the eco filter. If a promo
candidate returns `manual_review` in an automatic run, it is skipped
fail-closed.

Other partner tracks resolve promo only for their exact partner `profile_key`
unless they get their own documented exception.

Partner story fanout is best-effort per configured target. A Telegram Business
or Telethon authorization failure may fail that Telegram-family target, but it
must not abort unrelated VK wall/story targets in the same CherryFlash render
when VK credentials are present. If the run reaches Kaggle handoff and then
ends as terminal `FAILED`, the same-day watchdog still treats it as retryable;
handoff alone is not delivery evidence for a partner track.

For `partner_region_east_001`, a missing Telegram Business target is treated as
a same-day defer signal after the scheduled attempt plus one watchdog retry.
When the cached Business connection still cannot be resolved on the retry, the
watchdog stops launching that track until the next scheduled local day instead
of retrying every 10 minutes up to the 22:00 deadline. The counter also treats
legacy same-day `failed` no-session attempts as attempts, so a deploy in the
middle of an already-running retry loop does not reset the budget.

## Selection policy

Partner tracks are LLM-first editorial filters. Keyword lists and geo lists below are guardrails and audit aids, not a replacement for semantic classification.

The default semantic classifier for partner tracks is `Gemma 4` (`models/gemma-4-31b-it`) through the project LLM gateway / native JSON schema path. Deterministic keyword logic may build a broad recall prefilter or provide hints to the model, but the final `matched/manual_review/exclude` decision for text meaning must be LLM-owned.

Partner filters narrow the CherryFlash candidate universe; they do not replace CherryFlash popularity priority. Within each partner track, eligible events must still be prioritized by the same popularity signals as the base CherryFlash product: views, likes, normalized popularity score, and popularity window provenance.

The selector should:

- start from future/current events that CherryFlash can render and that belong to the `/popular_posts` / CherryFlash popularity candidate pool;
- for `partner_eco_nature_001`, if the recent popularity pool underfills the
  profile, add a bounded event-date recall pass over active current/future
  events whose source posts may be older than the 1/3/7-day popularity windows;
  this recall only widens the candidate list for the same `eco_prirodnaya`
  LLM filter and must not make a deterministic semantic include decision;
- apply partner-specific semantic and/or geo filters as an eligibility layer;
- rank the remaining eligible events primarily by popularity signals (`views`, `likes`, normalized score, winning popularity window), with partner filter score used for eligibility/explanation and only as a secondary tie-breaker when popularity is otherwise comparable;
- keep source provenance and filter reasons in the selection trace;
- avoid selecting an event when the filter match is only a background mention;
- expose `needs_manual_review=true` when score or source evidence is ambiguous.

Recommended output fields for every classified candidate:

- `event_id`
- `title`
- `matched`
- `score`
- `matched_categories`
- `matched_keywords`
- `popularity_score`
- `views`
- `likes`
- `popularity_window`
- `reason`
- `needs_manual_review`

## Filter `eco_prirodnaya`

Human label: `эко-природная`.

Internal lineage: this is the compact product name for the longer editorial filter `Эко, природа, краеведение и устойчивое потребление`.

Main rule: include an event only when ecology, nature, sustainable consumption, or Kaliningrad-region local history/heritage is a primary theme, not a random word, venue background, or outdoor setting.

Local history / heritage is an equal first-class match path for this partner filter, not a weak secondary hint. The classifier must not reject a candidate only because it is "not about ecology" when the source clearly makes Kaliningrad Oblast history, museums, artifacts, castles, old routes, towns, settlements, architecture, regional identity, or cultural heritage the main promise.

Include:

- swaps, flea markets, free markets, exchanges of clothes, books, plants, toys, household items;
- recycling, separate waste collection, repair, upcycling, zero waste, circular economy, ecological everyday life;
- cleanups, planting, greening, park care, trail restoration, eco-volunteering;
- nature walks, excursions, lectures, and events about Kaliningrad Oblast landscapes, flora, fauna, birds, natural territories;
- local history broader than nature: towns, villages, architecture, estates, parks, churches, castles, fortification, water systems, cultural heritage, East Prussian / regional history;
- exhibitions, films, lectures, photography, illustration, and art where nature, ecology, landscapes, animals, plants, regional identity, or local heritage are the main theme;
- museum nights, museum routes, fund-storage excursions, local-history museum programs, and regional heritage festivals when the announcement is primarily about regional museums, collections, artifacts, restoration, local history, historical sites, or cultural heritage;
- historical reenactment / living-history events when the venue or program is tied to a real regional heritage site, castle, settlement, old route, or local-history narrative;
- plants, gardening, seedling/seed exchange, urban greening, plant-care workshops;
- animals, birds, ornithology, animal shelters, wildlife protection, bird feeders, insects, ecosystems;
- adult or children's education about ecology, nature, animals, plants, sustainable consumption, or care for the environment.
- zoo-hosted events when the main content is animals, wildlife, conservation, animal welfare, zoology, nature observation, or substantial nature education rather than the zoo being only a venue.
- vintage, second-hand, flea-market, swap, or charity resale events when reuse/resale of existing clothes, books, plants, household items, or accessories is explicit, even if the announcement is framed through fashion, style, music, or a venue party.

Exclude:

- ordinary commercial fairs, markets, shopping events, or sales of new goods without reuse, ecology, or local-heritage meaning;
- ordinary craft, food, souvenir, fashion, or designer markets when reuse/second-hand/vintage/local-heritage meaning is absent or only decorative;
- events where nature is only the venue or background;
- ordinary concerts, picnics, outdoor sports, or park festivals without a nature/ecology/local-history theme;
- animal entertainment such as petting zoos, animal shows, pony rides, or photo sessions with animals;
- ordinary zoo-hosted concerts, lotteries, games, food/market blocks, generic season-opening entertainment, or partner activities when animals/nature are only the venue context;
- ordinary concerts, sports events, philosophy lectures, generic art events, or patriotic/commemorative performances held at a museum, library, castle, zoo, park, or historic venue when the event topic itself is not regional local history, nature, ecology, sustainable consumption, or heritage interpretation;
- hunting, trophy fishing, fishing competitions, jeep tours, quad bikes, off-road racing, dune/forest racing, or other nature-damaging activities.

Zoo classification rule:

- `Калининградский зоопарк` as venue or source is not an automatic include or exclude.
- Include the specific zoo event/subevent when the announcement makes animals, nature, zoology, conservation, animal welfare, or substantial nature education the main promise.
- Exclude or mark manual review when the zoo post is a mixed program and the specific extracted subevent is mainly a concert, lottery, generic game, food/market activity, or broad season-opening block.
- For large zoo festival/day posts, classify extracted subevents separately rather than letting one nature-related line make every subevent match.

Recall guardrails for `Gemma 4` classification:

- If the title/source contains `музей`, `краеведческий`, `историко-художественный`, `история`, `наследие`, `замок`, `кирха`, `усадьба`, `фортификация`, `поселение`, `Восточная Пруссия`, or a named regional heritage site, first evaluate the `local_history` path before excluding.
- If a museum festival combines several regional museums, local-history museums, nature museums, historical routes, artifact collections, or heritage exhibitions, mark it `matched` or `manual_review`; do not reject it merely as a generic cultural festival.
- A museum, library, castle, historic gate, zoo, island, or park as venue is supporting evidence only. It is not enough by itself: the announcement must make the regional/local-history, nature, ecology, reuse, or heritage theme part of the event promise.
- If an event is military/patriotic history, disaster-themed art, or a broad museum night where the regional/local-history theme is present but mixed with unrelated entertainment, prefer `manual_review` over a confident `matched` or confident `exclude`.
- Zoo-hosted concerts remain `exclude` when the only animal/nature evidence is that the concert happens at the zoo. A note about sound being monitored by zoologists is not enough unless the event is explicitly presented as animal/nature research or education.
- Sports events tied only to an anniversary of the region are `exclude` unless the route/program includes explicit local-history interpretation, nature education, eco action, or heritage stops.
- General philosophy, fashion, body-art, costume-history, or music events are `exclude` unless their own topic is directly about Kaliningrad Oblast heritage/nature/ecology/reuse; a museum/library venue alone should not create `manual_review`.
- If a market explicitly says `барахолка`, `винтаж`, `секонд-хенд`, `second hand`, `обмен`, `своп`, `фримаркет`, or `гаражная распродажа`, first evaluate the sustainable-consumption path before excluding.

Scoring guide:

- `+3` title explicitly contains an include topic.
- `+2` description clearly explains the ecology, nature, sustainable-consumption, or local-history theme.
- `+2` organizer is a museum, library, reserve, national park, eco-center, local-history group, or conservation organization.
- `+1` venue is a museum, library, park, estate, natural territory, or similar supporting context.
- `-3` event clearly matches an exclude rule.
- `-2` theme is only a venue/background mention.
- `auto_include >= 3`, `manual_review = 2`, `exclude < 2`.

Keyword families for deterministic support:

- `swaps_fleamarkets`: `своп`, `обмен вещами`, `обмен одеждой`, `обмен книгами`, `обмен растениями`, `фримаркет`, `барахолка`, `гаражная распродажа`, `благотворительная распродажа`, `second hand`, `секонд-хенд`, `повторное использование`, `осознанное потребление`.
- `recycling_repair_upcycling`: `переработка`, `раздельный сбор`, `сбор вторсырья`, `сбор макулатуры`, `сбор батареек`, `сбор крышечек`, `сбор электроники`, `ресайклинг`, `апсайклинг`, `ремонт`, `ремонтная мастерская`, `zero waste`, `экологичный быт`, `устойчивое потребление`, `circular economy`.
- `eco_volunteering`: `субботник`, `уборка территории`, `уборка берега`, `уборка леса`, `уборка парка`, `очистка территории`, `волонтёрская акция`, `эковолонтёрство`, `посадка деревьев`, `посадка растений`, `озеленение`, `уход за парком`, `восстановление троп`, `помощь природе`, `защита природы`.
- `nature_region`: `природа`, `природа Калининградской области`, `природная экскурсия`, `ботаническая прогулка`, `орнитологическая прогулка`, `наблюдение за птицами`, `флора`, `фауна`, `ландшафт`, `лес`, `болото`, `озеро`, `река`, `побережье`, `Куршская коса`, `Виштынецкое озеро`, `Роминтская пуща`, `Балтийское море`, `дюны`, `заказник`, `заповедник`, `национальный парк`.
- `local_history`: `краеведение`, `краеведческая прогулка`, `краеведческая экскурсия`, `история края`, `история региона`, `история области`, `Восточная Пруссия`, `культурное наследие`, `историческое наследие`, `местная история`, `история города`, `история посёлка`, `усадьба`, `кирха`, `замок`, `парк`, `старый парк`, `усадебный парк`, `аллея`, `мельница`, `канал`, `водная система`, `фортификация`, `архитектурное наследие`, `немецкое наследие`.
- `nature_art_culture`: `выставка о природе`, `фотовыставка`, `природная фотография`, `ботаническая иллюстрация`, `экологическое искусство`, `документальный фильм о природе`, `кинопоказ об экологии`, `пейзаж`, `ландшафтная живопись`, `животные в искусстве`, `птицы в искусстве`, `растения в искусстве`, `выставка о регионе`, `выставка о крае`, `музейная экспозиция`.
- `plants_garden`: `растения`, `комнатные растения`, `садовые растения`, `обмен растениями`, `ботаника`, `сад`, `садоводство`, `огород`, `городское садоводство`, `озеленение`, `ландшафтный дизайн`, `мастер-класс по растениям`, `уход за растениями`, `рассада`, `семена`.
- `animals_birds`: `животные`, `птицы`, `орнитология`, `наблюдение за птицами`, `фауна`, `дикие животные`, `помощь животным`, `приют для животных`, `защита животных`, `кормушки`, `домики для птиц`, `насекомые`, `экосистема`.
- `eco_education_children`: `экопросвещение`, `экологическая лекция`, `лекция об экологии`, `мастер-класс об экологии`, `экологический урок`, `детское занятие о природе`, `детский мастер-класс о природе`, `школа экологии`, `экологическая игра`, `занятие о животных`, `занятие о растениях`, `бережное отношение к природе`.

Initial partner assignment:

- `partner_track_id`: `partner_eco_nature_001`
- `content_filter_id`: `eco_prirodnaya`
- `publish surface`: Telegram Business Story
- `target`: encrypted Business target supplied out-of-band; do not write raw handle to repo-visible files.

## Filter `konb_library`

Human label: `КОНБ / Калининградская областная научная библиотека`.

Main rule: include only future/current events that belong to the scientific library surface. This is a venue/source ownership filter, not a broad “library” topic classifier.

Include when at least one of these source-grounded facts is present:

- canonical venue/location mentions `Научная библиотека`, `Калининградская областная научная библиотека`, `КОНБ`, or `КОУНБ` (the КОНБ name itself — `Мира 9` alone is no longer sufficient, see exclusion below);
- source evidence from `https://t.me/kaliningradlibrary/...`, `https://vk.com/wall-30777579_...`, or `https://vk.com/konb39...`;
- source text explicitly identifies the event as a scientific-library event.

Exclude:

- other regional libraries, even when they are library events;
- generic book, lecture, reading, or culture events without source/venue evidence tying them to КОНБ;
- **same-address tenants at `Мира 9`** — the КОНБ shares the building with `Дом китобоя` (a separate museum). `Дом китобоя` / `Музей китобоя` events MUST be rejected by the filter even if the LLM-extracted street address is the same. The deterministic filter contains an explicit `KONB_LIBRARY_EXCLUDED_LOCATION_HINTS` list (`дом китобоя`, `доме китобоя`, `музей китобоя`, …) checked against `location_name` and title before any inclusion check, and bare-`Мира 9` matches without an explicit КОНБ name or owned source are no longer accepted (post-2026-05-17 round-1 feedback, requirement #5).

Selection policy:

- profile key: `popular_review_konb`;
- track id: `partner_konb_library_001`;
- default publish mode (post-round-3, 2026-05-17): **`prod`**. Test mode is still available via setting `partner_track_konb_publish_mode=test`, which publishes a normal Telegram channel post to `https://t.me/keniggpt` (not a channel story);
- production story fanout (post-round-4, 2026-05-30): **three independent best-effort targets** — Telegram channel story `https://t.me/kaliningradlibrary`, VK community story `https://vk.com/konb39`, and a VK community **wall post** into the same `https://vk.com/konb39` (transport `vk_wall`). **None is `required`** — success in one does not gate the others, and failure of one does not fail the overall publish. Each target is attempted on its own (`blocking=False`, `required=False`). Round-4 operator request: «Видеоанонс КОНБ нужно в Вк помимо публикации в историю опубликовать ещё и в виде поста в вк-сообщество научной библиотеки». The `vk_wall` post reuses the shipped `vk_wall` transport (`_publish_vk_wall_video`: `video.save` → `wall.post`); its caption auto-fills via `build_vk_video_announce_caption` (cities/dates) because no explicit caption is set on the target. `_dedupe_targets` keys `vk_story:konb39` and `vk_wall:konb39` distinctly, so the two VK surfaces never collapse. Production evidence from `INC-2026-05-18` confirms the VK story path can publish while Telegram channel stories for `@kaliningradlibrary` fail with `BOOSTS_REQUIRED`; Telegram story delivery therefore requires the channel boost prerequisite (or a future supported business/story target) and remains best-effort until that prerequisite is resolved;
- daily schedule (post-round-3): **`12:37 Europe/Kaliningrad`**, exactly 7 min after the eco/nature partner slot at 12:30, per operator brief. The partner-track render guard is scoped by `profile_key`, so a slow `popular_review_eco` render must not block the KОНБ slot or watchdog retries. The watchdog still retries later the same day until the shared 22:00 deadline if the KОНБ slot itself misses handoff;
- КОНБ test/prod modes must not inherit global `video_announce_story_business_targets`; the only targets are the explicit КОНБ test/prod targets above.

Ranking policy:

- base order still comes from CherryFlash popularity signals (`views`, `likes`, normalized score);
- events with `ticket_link` or a non-zero stored price receive a strong priority boost and are placed closer to the beginning when many eligible events exist;
- explicit special-guest wording (`специальный гость`, `приглашённый гость`, `встреча с`, etc.) receives a smaller priority boost;
- events due in 3 days or 1 day receive an additional boost, giving the requested extra exposure windows;
- **anti-repeat (post-round-3, 2026-05-17; incident hardening 2026-05-18)**: КОНБ events may repeat across daily videos once at least **one calendar-day boundary** has crossed in `Europe/Kaliningrad` since the last show — same-day repeats are blocked by default, but the scheduled KОНБ partner pipeline can unlock a final emergency same-day recycle layer if every fresh/future/cross-day recycle source is empty. Within a single video, the same event is still deduped (different scene = different event). The operator brief was «повторять анонс это не плохо, главное не в одном ролике и максимально не подряд» and, for the 2026-05-18 production incident, «лучше сделать повтор чем не выпустить вообще»;
- repeats are admitted but **demoted** — every recently-shown event pays `KONB_REPEAT_SCORE_PENALTY` (~220 pts) in the score, so fresh КОНБ candidates always sort ahead of repeats when both exist. A repeat only appears when the fresh pool can't fill `max_events`;
- an event that occupied slot 1 recently additionally pays the `first_position` penalty (~360 pts), so even a permitted repeat does not pin to slot 1 on the next day;
- when the popularity pool contains too few КОНБ events, the selector expands to all future КОНБ events and then applies the same renderability, calendar-day-spaced repeat allowance, and ranking rules;
- if the future fallback still underfills, the selector uses a last-resort `konb_recycle` pool from previous `popular_review_konb` video items whose events are still current/future. This is intentionally permissive across calendar days because the production contract is "prefer a repeat over no KОНБ publication";
- if even `konb_recycle` is empty and the caller passes `allow_same_day_recycle=True` (the scheduled/manual partner track pipeline does this for `partner_konb_library_001`), the selector can use the same pool one last time with `source_window=konb_same_day_recycle` and `anti_repeat_status=same_day_recycle`. This exists only to prevent a missed daily KОНБ slot; same-video duplicates remain blocked.

Outro (post-round-3 contract, 2026-05-17):

- strip color: `#780000`, text color `#FFFFFF`;
- **global outro size**: the whole composition (stripe heights, font sizes, base inter-stripe gap) is rendered **30% smaller** than round-1 via `outro.global_scale = 0.7` in `PARTNER_KONB_LIBRARY.outro`. Per-line `scale` ratios stay at 1.0 / 0.42 / 0.8;
- library name is rendered as **one word per stripe at scale 1.0**: `Калининградская` / `областная` / `научная` / `библиотека`;
- sponsor caption is a single small stripe at scale 0.42: `при поддержке`. The stripe has `extra_gap_before = 2.0` (the gap ABOVE it triples: base + 2×base = 3×base), and the next stripe (`Полюбить`) also carries `extra_gap_before = 2.0` (the gap BELOW «при поддержке» triples too), so the caption visually floats with **3× empty space above and below**;
- price/free line uses **«руб.»** instead of the `₽` glyph: the CherryFlash title font (Bebas Neue) does not include the ruble codepoint and falls back to `.notdef`, which caused render artefacts on the round-2 test. Free events still render as `БЕСПЛАТНО`; paid events render as `N руб.` / `N–M руб.` / `ОТ N руб.` / `ДО M руб.`;
- channel signature is rendered as **one word per stripe at scale 0.8** (20% smaller than the library lines): `Полюбить` / `Калининград` / `Анонсы`;
- stripes enter alternating from left/right on a staggered delay so the 8-stripe composition slides in well before the 3.5s outro card duration.

Intro contract (post-round-2, 2026-05-17):

- the 2D overlay kicker text is `НАУЧНАЯ\nБИБЛИОТЕКА`; the underline stripe under the kicker is positioned by **measuring the actual bottom of the last rendered kicker line** so it correctly sits under «БИБЛИОТЕКА» regardless of how many lines the partner kicker has;
- the 3D-phone top-screen label uses the full institutional name `Калининградская областная\nнаучная библиотека` (two lines). **Both lines must use the same font (CYGRE_BOLD) and the same size** — the size is auto-picked as the largest that fits the wider of the two lines into the available label width, so the full name reads as a single institution title, not as a count + subtitle. Round-1 used `CYGRE_SEMIBOLD` at a much smaller size for the second line, which truncated «Калининградская област…»; round-2 fixes this.

Scene card contract (post-round-2, 2026-05-17):

- venue is rendered **on one line** in the bot's canonical form `ЛОКАЦИЯ • АДРЕС • ГОРОД` (e.g. `НАУЧНАЯ БИБЛИОТЕКА • МИРА 9 • КАЛИНИНГРАД`), with the address slotted between the location name and the city. The address is sourced from `event.location_address`; if missing, the line falls back to the legacy `LOC • CITY` shape. Round-1 used a separate address line above the venue — round-2 collapses that into the single venue line per operator brief;
- between the date/time line and the venue line, each scene card still renders **a free/price line** (`БЕСПЛАТНО` when `event.is_free` is true or both `ticket_price_min/max` are explicitly zero, otherwise a `min руб.` / `min–max руб.` / `ОТ N руб.` / `ДО M руб.` label from the stored ticket prices), in the same `DETAIL_COLOR` palette as the venue line. **Important (post-2026-05-18 fix):** when there is no objective price evidence in the DB (no `is_free` flag, both prices missing), `_format_display_price` returns an empty string and the price line is skipped. The round-1 implementation defaulted to `БЕСПЛАТНО` on this branch on the assumption that library events are mostly free — that override leaked into the general non-KONB cherryflash announce on 2026-05-18 and mislabelled paid events as free, so the fallback was removed. KONB-specific «default free for library events» behaviour, if needed again, should be re-introduced upstream by setting `is_free` at the event/scene level for confirmed-free library events, not in the renderer;
- festival context (when `event.festival` is set) continues to render between the title and the date line, unchanged.

## Filter `icae_events`

Human label: `ИЦАЭ`.

Main rule: include only events whose primary venue, organizer, source, or explicit programme context is the Информационный центр по атомной энергии in Kaliningrad. The selector should generate a separate CherryFlash video announcement from ИЦАЭ events only and publish it to the partner Business Story target for `partner_icae_001`.

The filter is LLM-first. Deterministic support may pass source/channel hints, known ИЦАЭ venue aliases, and canonical location references to Gemma 4, but the final include/exclude decision must be made by a small native-schema Gemma 4 classification stage.

Include:

- quizzes, intellectual tournaments, lectures, science talks, workshops, screenings, meetings, and public programmes hosted by ИЦАЭ;
- events where ИЦАЭ is the explicit organizer or partner and the event is meant for the public;
- reposts from other sources when the source-grounded venue/organizer evidence still points to ИЦАЭ.

Exclude:

- unrelated events that merely mention science, quizzes, or education without ИЦАЭ as venue/organizer/source;
- broad digest posts where one ИЦАЭ line is mixed with unrelated events unless candidates are split per event before classification;
- private/internal organizational notices without a public event card.

Recommended native-schema output:

- `event_id`
- `title`
- `matched`
- `confidence`
- `evidence_fields`
- `canonical_location_name`
- `canonical_location_address`
- `reason`
- `needs_manual_review`

## Operator interface requirements

Partner tracks must have direct launch buttons in the CherryFlash operator UI. They must not be hidden behind a generic profile picker or require the operator to remember internal filter ids.

Required initial buttons:

- `🍒 CherryFlash` — existing base popularity track.
- `🍃 Эко-природная` — launches `partner_track_id=partner_eco_nature_001` with `content_filter_id=eco_prirodnaya`.
- `📚 КОНБ` — launches `partner_track_id=partner_konb_library_001` with `content_filter_id=konb_library` and starts in `test` publish mode until explicitly promoted to `prod`.

Future partner tracks should add their own one-click buttons with stable human labels, for example a future east-region partner button may use the `kaliningrad_region_east` geo filter once its Business target is supplied and source audit is complete.

Planned ИЦАЭ partner track:

- `partner_track_id`: `partner_icae_001`
- `content_filter_id`: `icae_events`
- `publish surface`: Telegram Business Story
- `target`: encrypted Business target supplied out-of-band; do not write raw handles to repo-visible files.
- `status`: planned; this is a product enhancement, not part of incident data repair.

The operator-facing UI should eventually expose this as a direct one-click CherryFlash partner launch, equivalent to the existing partner-button contract below.

Button behavior:

- each button must call the CherryFlash pipeline directly with its explicit `partner_track_id` / filter ids;
- launch and configuration are separate actions, so partner-specific story target setup belongs under a settings path, not inside the launch button;
- the UI must show enough preflight status to catch missing encrypted Business target, missing story rights, or unsafe parallel Kaggle state before the expensive render starts;
- manual and scheduled runs for the same partner track must share the same selection and story-publish contract.

## Filter `kaliningrad_region_east`

Human label: `Восток Калининградской области`.

Product definition: events in eastern, central-eastern, north-eastern, south-eastern, and southern Kaliningrad Oblast, excluding Kaliningrad, Guryevsk, the Kaliningrad agglomeration, and the coastal/resort zone from Baltiysk to the Curonian Spit. This is a product filter, not a strict administrative map.

Priority: `exclude_over_include`.

Include if:

- `event.location.settlement` is in included settlements;
- or `event.location.municipality` is in included municipalities.

Exclude if:

- settlement or municipality is in the excluded lists;
- `area_tag` contains `coast`, `curonian_spit`, or `kaliningrad_agglomeration`.

Included municipalities:

- `Багратионовский муниципальный округ`
- `Гвардейский муниципальный округ`
- `Гусевский муниципальный округ`
- `Краснознаменский муниципальный округ`
- `Ладушкинский городской округ`
- `Мамоновский городской округ`
- `Неманский муниципальный округ`
- `Нестеровский муниципальный округ`
- `Озёрский муниципальный округ`
- `Полесский муниципальный округ`
- `Правдинский муниципальный округ`
- `Славский муниципальный округ`
- `Советский городской округ`
- `Черняховский муниципальный округ`

Excluded municipalities:

- `Городской округ Город Калининград`
- `Гурьевский муниципальный округ`
- `Балтийский городской округ`
- `Светлогорский городской округ`
- `Зеленоградский муниципальный округ`
- `Пионерский городской округ`
- `Светловский городской округ`
- `Янтарный городской округ`

Included settlements:

- core cities: `Советск`, `Черняховск`, `Гусев`, `Гвардейск`.
- small cities: `Багратионовск`, `Краснознаменск`, `Ладушкин`, `Мамоново`, `Неман`, `Нестеров`, `Озёрск`, `Полесск`, `Правдинск`, `Славск`.
- tourist/event settlements: `Железнодорожный`, `Знаменск`, `Талпаки`, `Большаково`, `Добровольск`, `Ясная Поляна`, `Краснолесье`, `Чистые Пруды`, `Ильинское`, `Ольховатка`, `Междуречье`, `Лунино`, `Ульяново`, `Родники`, `Маяковское`, `Крылово`, `Домново`, `Дружба`, `Суворово`, `Пушкарёво`.

Excluded settlements:

- Kaliningrad and nearby: `Калининград`, `Гурьевск`, `Большое Исаково`, `Малое Исаково`, `Васильково`, `Храброво`, `Невское`, `Кутузово`, `Родники`.
- coast and resort zone: `Балтийск`, `Приморск`, `Янтарный`, `Светлогорск`, `Отрадное`, `Приморье`, `Пионерский`, `Зеленоградск`, `Лесной`, `Рыбачий`, `Морское`, `Заостровье`, `Малиновка`, `Куликово`, `Романово`, `Переславское`, `Светлый`, `Взморье`, `Люблино`.

Normalization:

- case-insensitive comparison;
- trim whitespace;
- replace `е`/`ё` variants for matching;
- normalize prefixes such as `г.`, `п.`, `пос.`;
- examples:
  - `Озерск` -> `Озёрск`
  - `пос. Железнодорожный` -> `Железнодорожный`
  - `п. Железнодорожный` -> `Железнодорожный`
  - `г. Советск` -> `Советск`

Special notes:

- `Родники` is intentionally present in both include and exclude examples; because `exclude_over_include` is mandatory, it must be excluded unless future structured geo disambiguation proves a different settlement.
- If only municipality is known, use municipality include/exclude rules.
- If the event is tourism, culture, fairs, local history, nature, or festivals and is located in an included settlement, it matches the filter.
- Do not include coastal resort events even when they are outside Kaliningrad.

## Geo-guaranteed source audit

The east-region filter may use source-level guarantees only after sources are audited.

Source-level guarantee rule:

- if a source has a stable default venue/settlement inside `kaliningrad_region_east`, and its posts are normally on-site, events from that source may auto-match the geo filter;
- if the post explicitly announces an off-site event in an excluded settlement/municipality, the event must follow the event-level location and can be excluded;
- if the source is multi-venue, touring, aggregator-like, or often posts partner events elsewhere, it must stay `review_required_sources`.

Initial source-audit evidence:

- `Дизайн-резиденция Gumbinnen` is already present in the location reference as `Ленина 29, Гусев`; because `Гусев` is included, a verified Gumbinnen source is a strong candidate for `guaranteed_sources`.
- `Ферма Тюниных` was requested as an example of a possible geo-specific source. It must be checked against the actual source catalog / production source rows before being treated as guaranteed.

Required source audit output:

```yaml
source_audit:
  source_id: "<source key>"
  source_name: "<safe public name>"
  default_location: "<normalized location>"
  settlement: "<normalized settlement>"
  municipality: "<normalized municipality>"
  geo_filter_id: kaliningrad_region_east
  guarantee: guaranteed|review_required|excluded
  reason: "<why this source does or does not auto-match>"
  evidence:
    - "<location reference row, source row, or recent post sample>"
```

## Acceptance checklist

- [x] Partner tracks are represented by stable internal ids and never require raw Telegram handles in repo-visible files. Implemented in `video_announce/partner_tracks.py`; raw handles live only in operator-supplied `Setting` rows (`partner_track_eco_business_selector`, `partner_track_east_business_selector`).
- [x] The first partner is represented as `partner_eco_nature_001` and resolves to the encrypted Business target supplied out-of-band. Resolution goes through `telegram_business.load_business_story_targets(selector_raw=<setting value>)`.
- [x] `eco_prirodnaya` can classify events with `matched_categories`, `matched_keywords`, `popularity_score`, `views`, `likes`, `popularity_window`, `reason`, and `needs_manual_review`. Implemented in `video_announce/partner_filters.py::classify_event_eco_prirodnaya`; popularity fields ride in the existing `selection_params["popular_review_trace"]`.
- [x] Partner-track semantic decisions are LLM-first, with Gemma 4/native JSON schema as the default structured classifier. `make_eco_gemma_llm_call()` ships `response_mime_type=application/json` + `response_schema` to `gemma-4-31b-it`; default model overridable via `PARTNER_FILTER_GEMMA_MODEL` env.
- [x] `kaliningrad_region_east` can classify event locations with `exclude_over_include` priority. Deterministic classifier on `event.city`/`location_*`; `Родники` is explicitly excluded per spec.
- [x] The CherryFlash operator UI has a direct `🍃 Природа и экология` launch button wired to `partner_eco_nature_001`, plus `🌾 Восток области` for `partner_region_east_001`.
- [ ] East-region source-level guarantees are backed by a source audit, including Gumbinnen and any Farm Tyuniny source before use. Deferred: today the east filter goes purely through `event.location`.
- [x] First generation/regeneration for a new partner track runs on Kaggle GPU. Inherited from the existing CherryFlash kernel deploy path; no change.
- [x] Stable steady-state partner runs can run without GPU when validated. Inherited.
- [ ] Parallel CherryFlash partner runs cannot detach or overwrite each other's Kaggle session datasets, kernels, or story secrets. Mitigated in production by scoped render locks: partner launch checks are scoped by `profile_key` so a slow eco/nature render does not block KОНБ, while same-profile duplicate renders are still blocked. Full per-run kernel isolation (separate kernel slug or per-run kernel copy) remains a follow-up for truly independent concurrent Kaggle mutation.
- [x] Successful partner runs publish to Telegram Business Stories with `post_to_chat_page=true`. Inherited from the existing Bot API `postStory` call in `kaggle/CrumpleVideo/story_publish.py`.

### What has to happen before first run

Most of the wiring is done in code — each `PartnerTrack` carries a `default_business_selector` (`@yasonneolga` for eco, `@natakkaz` for east) that is used automatically when the operator has not overridden the corresponding `Setting` row. The pipeline runs `load_business_story_targets(selector_raw=selector)` as a preflight and only proceeds when at least one cached Business connection matches.

The single thing the bot cannot do for itself is the partner's one-time consent on the Telegram side: the partner must add the bot to **Telegram → Settings → Business → Chatbots** and grant story-publishing rights. As soon as that happens, Telegram pushes a `business_connection` update, the bot caches the encrypted connection, and the partner track works without any further operator action.

Optional operator override: writing the `Setting` row `partner_track_eco_business_selector` or `partner_track_east_business_selector` replaces the in-code default — useful for testing against a different Business account without redeploying.

### Delete-bad-publication contract

`/v` → `🗑 Удалить неудачную партнёрскую публикацию` shows a per-track menu listing each partner's last persisted Telegram Business `story_id`. Selecting one asks for confirmation, then calls Bot API `deleteStory(business_connection_id, story_id)` through `main._delete_business_story`. Scope is partner-only (no channel-chain reversal). Persisted columns on `videoannounce_session`: `partner_track_id`, `partner_story_id`, `partner_story_connection_hash`, `partner_story_deleted_at`. Population happens in `video_announce/poller.py::_persist_partner_story_metadata` after the run finishes, by reading `story_publish_report.json` for the first ok `transport=telegram_business` target.

### Intro labels per track

`partner_eco_nature_001` renders intro with kicker `ПРИРОДА И ЭКОЛОГИЯ` (2D phone) / `природа и экология` (3D phone screen-top). `partner_region_east_001` uses `ВОСТОК\nКАЛИНИНГРАДСКОЙ\nОБЛАСТИ` / `восток калининградской области`. Plumbed through `selection_params["variant_overrides"]` → `selection_manifest.variant.{kicker,screen_top}` → `scripts/render_mobilefeed_intro_still.py::_default_variants`.

### Scheduling

Both partner tracks publish daily at fixed `Europe/Kaliningrad` local slots picked from a log-based analysis of existing heavy jobs:

| Track | Local slot | Why this slot |
|---|---|---|
| `partner_eco_nature_001` | `12:30` | After base CherryFlash `10:15` finishes (≈60 min runway on CPU), before `source_parsing_day` `14:15`. ~100 min clean window. |
| `partner_region_east_001` | `18:30` | After `video_tomorrow` `16:45` finishes (≈60 min runway on CPU), before the next heavy peak. ~100 min clean window. |

To clear a daily double-peak at `20:10` (`guide_excursions_full` + `kenigsberg_story_daily`), the Kenigsberg slot was moved from `20:10` to `19:30 local` on 2026-05-14.

Cron jobs `video_partner_track_eco` / `video_partner_track_konb` / `video_partner_track_east` invoke `_run_scheduled_partner_track`. A per-track watchdog (`video_partner_track_<eco|konb|east>_watchdog`) runs every 10 minutes and re-launches the partner pipeline if today's slot was missed (no confirmed Kaggle handoff for today's `target_date` under the partner `profile_key`). Retries stop at hard local deadline `22:00` so nothing publishes overnight. Anti-repeat (`POPULAR_REVIEW_ANTI_REPEAT_DAYS` scoped by `profile_key`) keeps re-runs idempotent.

The schedule is intentionally **not gated by feature flags** — the user explicitly asked to remove `ENABLE_V_*_SCHEDULED` flags as failure points. Only per-slot times can be moved without redeploy:

- `V_PARTNER_TRACK_ECO_TIME_LOCAL` (default `12:30`)
- `V_PARTNER_TRACK_KONB_TIME_LOCAL` (default `12:37`)
- `V_PARTNER_TRACK_EAST_TIME_LOCAL` (default `18:30`)

### Telegram-session isolation

Partner-track publication does not load any Telethon session. The kernel-side helper publishes only through Bot API `postStory(business_connection_id, ...)` against the encrypted Business target, so partner tracks cannot contend with the base CherryFlash channel-chain or Kenigsberg primary upload for the shared `TELEGRAM_AUTH_BUNDLE_S22`. A second Telegram session is not required for the eco/east tracks; failures of one partner connection (e.g. `@natakkaz` has not granted story rights yet) cannot block the other partner track because per-session `selection_params["story_business_targets"]` is the only selector consulted at publish time.

### Leading-emoji title fix

Event titles starting with an emoji (e.g. `🎸 Концерт …`) used to render as a `.notdef` box because the CherryFlash title font (Bebas Neue) has no emoji coverage. `scripts/render_cherryflash_full.py::_strip_leading_emoji` removes the leading emoji/pictograph run + trailing whitespace before `RenderScene.title` is built; inline emoji inside the title are preserved. A real emoji-fallback rasterizer can replace the strip later without changing the title contract.
