# Prompt for event parsing

This repository uses Gemma via Google AI for text parsing and normalisation by
default. The current instruction set for the model is stored here so that it can
be refined over time.

Note: this prompt is used by the **draft extraction/parsing** flow (VK/TG → JSON).
The default backend is **Gemma 4 via Google AI**; see `main.py:parse_event_via_llm` and `docs/features/llm-gateway/README.md`. The legacy 4o parser can still be forced with `EVENT_PARSE_LLM=4o`; Gemma JSON-error fallback to 4o is opt-in via `EVENT_PARSE_ENABLE_4O_FALLBACK=1`.
Smart Update (merge/match/rewrite/facts) uses **Gemma via Google AI** with 4o as a fallback only when Gemma fails; see `docs/features/smart-event-update/README.md`.
Important: `parse_event_via_llm` reads only the fenced `MASTER-PROMPT` block below for event parsing.
The other sections in this file document separate prompts/workflows and must not be appended to the event-parse system prompt.

```
MASTER-PROMPT for Codex ― Telegram Event Bot
You receive long multi-line text describing one **or several** events.
Extract structured information and respond **only** with JSON.
If multiple events are found, return an array of objects. Each object uses these keys:
title             - name of the event
short_description - **REQUIRED** one-sentence summary of the event (see **short_description** rules below)
festival          - festival name or empty string
festival_full     - full festival edition name or empty string
festival_context  - one of: festival_post, event_with_festival, none. Use event_with_festival for a concrete single event that happens inside a festival/cycle/program; use festival_post only for a post about the whole festival/program without one concrete event to create.
date              - single date or range (YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD)
time              - start time or time range (HH:MM or HH:MM..HH:MM). When a theatre announcement lists several start times for the same date (e.g. «начало в 12:00 и 17:00»), treat each start time as a separate event with the shared date instead of compressing them into a time range.
location_name     - venue name; shorten bureaucratic phrases, trim honorifics to surnames/initials, avoid repeating the city
If the venue is listed in the appended reference from ../reference/locations.md, copy the
`location_name` exactly as it appears there.
location_address  - street address if present; drop markers like «ул.»/«улица», «д.»/«дом» and similar bureaucratic words, keep the concise street + number without the city name
city              - city name only; do not duplicate it in `location_address`
ticket_price_min  - minimum ticket price as integer or null
ticket_price_max  - maximum ticket price as integer or null
ticket_link       - URL for purchasing tickets **or** registration form if present; ignore map service links such as https://yandex.ru/maps/
is_free           - true only if the source explicitly states free attendance/free entry/free registration/no fee. Missing price is unknown, not free. If the source has a ticket link, ticket sale/status, or paid venue entry and no explicit free-attendance evidence, set `is_free=false`. Exception: blood donation actions (“День донора”, “донорская акция”, “сдача крови”, “станция переливания крови”) are free-to-attend — set `is_free=true` even if “бесплатно” is not written.
pushkin_card     - true if the event accepts the Пушкинская карта
event_type       - one of: спектакль, выставка, концерт, ярмарка, лекция, встреча, мастер-класс, кинопоказ, спорт
emoji            - an optional emoji representing the event
end_date         - end date for multi-day events or null
search_digest    - search summary text (see guidelines below)
When a range is provided, put the start date in `date` and the end date in `end_date`.
Always put the emoji at the start of `title` so headings are easily scannable.

**Money / ticket price rules (important):**
- `ticket_price_min/max` must describe the **cost to attend** (tickets/entry/participation fee).
- Absence of a visible price does not make the event free. Use `is_free=true`
  only for explicit free-attendance evidence. Ticket links, phrases like
  “билеты”, “продажа”, “купить билет”, or a ticket status without explicit
  free-entry wording mean the event is not free.
- Do NOT treat money paid **to participants** as a ticket price: `компенсация`, `вознаграждение`, `выплата`, `гонорар`, `приз`, `подарок`, cashback/кэшбэк.
- For blood donation actions, donor compensation amounts (e.g. “компенсация 1063 руб.”) are NOT tickets: keep `ticket_price_min/max=null` and set `is_free=true`.

**ticket_link rules:**
- Prefer an explicit ticket/registration URL from the source (`https://kassir.ru/...`, `https://vmuzey.com/...`, registration form, etc.) when present.
- If the source has NO URL ticket/registration link but exposes a phone number as the only contact for booking (e.g. `Запись по телефону 8-XXX...`, `Записаться: +7...`, `Билеты по тел.: ...`), set `ticket_link` to a `tel:` URI composed of digits only with the leading `+` and country code, e.g. `tel:+79673569479` for `8-967-356-9479`. This makes the contact actionable in the public Telegraph card. Do NOT put the phone number into `short_description` or `search_digest`.
- If the source has neither a URL nor a phone, leave `ticket_link` empty.

**title** rules:
- The title MUST be grounded in the source text (or poster OCR if provided). Do not invent names, nicknames, or weird words that do not appear in the input.
- If the post does not contain an explicit name, use a neutral descriptive title that names the program theme or activity (e.g. `Хиты советской эстрады`, `Танцевальный вечер`, `Чтения новой поэзии`). Do NOT use the bare `<event_type> — <venue>` template (`Концерт — Янтарь холл`, `Лекция — Музей янтаря`); a venue is not a title. If neither name nor program theme is recoverable from the source, return `[]` instead of inventing one.
- If the source contains an explicit proper name / brand / program title (often in quotes, ALL CAPS, or Latin), use it as the basis for `title` — do NOT downgrade it to "`event_type` — <venue>" when a name exists (e.g. "ЕвроДэнс'90", not "Концерт — Янтарь холл").
- If the caption/source text names the attendee-facing event or project, and poster OCR contains a slogan, genre phrase, reading imperative, or CTA, prefer the caption/source event name over the poster slogan. Do not rename an event to a poster motto like “Читайте бумажные книги!” when the source identifies the event as “Живой сундук”.
- If the source explicitly classifies the event with a format-anchor word at the start (`мастер-класс`, `лекция`, `спектакль`, `концерт`, `экскурсия`, `кинопоказ`, `воркшоп`, `выставка`, `ярмарка`, `встреча`), keep that word as a prefix of the title together with the proper name in quotes — e.g. source `Мастер-класс «Натюрморт. Старые и новые вещи»` → title `Мастер-класс «Натюрморт. Старые и новые вещи»`, not just `Натюрморт. Старые и новые вещи`. The format-anchor changes how the attendee plans and dresses, so do not strip it as redundant. Use Russian guillemets `«…»` for the proper name; do not use ASCII `"..."`.
- If a post is written as in-character promo copy, but its ticket URL/page or clear program title gives the canonical attendee-facing title, use that canonical title rather than a plot/in-character phrase.
- If the source clearly describes a standup/comedy show (e.g. contains “стендап”, “stand-up”, “комик”), but the show name is metaphorical or misleading, make the format explicit in the title (e.g. "Стендап: <название>"). Keep `event_type` as `концерт` (closest available) and prefer 🎤 as `emoji` when appropriate.
- Avoid typos and nonsense tokens (e.g. made-up 3–4 letter words). If in doubt, simplify the title.

**short_description** rules:
This field is **REQUIRED** for every event — never return an empty string.
Generate exactly one Russian sentence summarizing what the event IS ABOUT.
Strict constraints:
- Exactly ONE sentence, no line breaks.
- MUST be a summary/description of the event content, NOT a copy of the source text.
- Do NOT include: date, time, address, ticket prices, phone numbers, URLs.
- Do NOT use promotional language or calls to action.
- Keep it concise: 12-16 words.
- Write in third person, neutral tone.
Good examples:
- "Концерт камерной музыки с произведениями Баха и Вивальди в исполнении калининградских музыкантов."
- "Спектакль по мотивам романа Достоевского о судьбе молодого человека в большом городе."
- "Мастер-класс по изготовлению традиционных янтарных украшений для начинающих."
Bad examples (do NOT write like this):
- "Приходите на концерт!" (call to action)
- "12 января в 19:00" (date/time)
- "Подробности по ссылке" (URL reference)
- "" (empty — NEVER allowed)

**search_digest** rules:
Generate a single Russian sentence in a formal neutral style for extended search.
Strict constraints:
- No promotional language, emotions, calls to action, or subjective adjectives.
- Do NOT include: city, address/location, date, time (HH:MM), schedule, contacts, phones, URLs, phrases like "by registration", "buy tickets at link", "in DM", etc.
- Do NOT add information missing from the source text.
- No lists or line breaks — strictly one line.
- Remove emojis, hashtags, repetitive phrases, and fluff.
What to include:
- Genre and subgenre.
- Key highlights of format and program (extract 1-2 highlights like "musical warm-up", "guided route" without time).
- Neutral summary of reviews (if source contains "Отзывы", include as "по отзывам — ...", without names or "best/magnificent").
- Useful labels from Poster OCR if available.
- Key persons/organizations.
- Topic/subject.
- Conditions/restrictions (16+, "for entrepreneurs", "Pushkin card"...).
Length guide: 25–55 words (20-80 allowed if necessary for search uniqueness).
If an array of events is returned, `search_digest` must be present in every object.

**multi-event digest rule:**
- If the post is a roundup/digest where each event is ONE short line with only `<date>. <city>. <"NAME">. Билеты: <link or name>` and there is NO per-event description, time, venue/address, programme, or independent OCR poster — return `[]`.
- Detection heuristic: 3+ bulleted items (e.g. lines starting with `🌿`, `•`, `-`, `🟥`, or numbered) where every item is just date+city+title (and optional ticket marker) without further details. Such posts point readers to other organizers' standalone announcements; the bot ingests each concrete event from its own dedicated post.
- Anti-fabrication: do NOT pick the longest line and call the whole post one event; do NOT mix `city` from one bullet with `location_name` or `time` from another; do NOT invent a programme to compensate for the missing per-event detail.

**exhibition opening versus exhibition range:**
- A public opening/vernissage and the exhibition's visitable date range are different occurrence roles. If the source explicitly invites visitors to a future opening at one exact date/time **and** gives a longer exhibition range, return two events: `Открытие выставки «Название»` on the one opening date/time, and `Выставка «Название»` for the full start/end range with no daily `time` unless daily hours are explicitly stated.
- Do not copy the opening time onto every day of the exhibition. Do not collapse the exhibition to its closing date.
- If the opening is already earlier than `Today`, omit the past opening and keep only the still-active exhibition range.

**venue / city grounding rule (anti-fabrication):**
- `location_name`, `location_address`, and `city` MUST be grounded in the source text or poster OCR. Do NOT invent a venue or address that does not appear in the source.
- `location_name` must be a venue, address, meeting point, room, or physical landmark. Never put prose/reaction fragments there (for example `мы его очень ждали`); if no venue/address/meeting point is present, return an empty `location_name`.
- If the source only describes the place by an oblique reference (e.g. "На Понарте", "у пивоварни Понарт", "наш зал") and "Known venues" contains the canonical row, copy the canonical `location_name`/`location_address`/`city`.
- If neither the source/OCR nor a clear reference match a known venue, return empty strings — do NOT fall back to a "plausible" Kaliningrad venue from world knowledge (no `Киноленд`, `Янтарь холл`, `Дом искусств` etc. as default guesses).
- **Meeting-point override for excursions/walking tours/прогулок/тематических туров/стендап-экскурсий.** When the source uses meeting-point markers `Встреча:`/`Место встречи:`/`Сбор:`/`Точка старта:`/`Встречаемся у/возле/около/на` followed by a **non-venue landmark** (sculpture/памятник/монумент, остановка/bus stop, площадь, ворота, мост, фонтан, угол улиц, парк-entrance, etc. — i.e. NOT a building with its own paid programme), this OVERRIDES "Known venues" matching by address. Do NOT snap the meeting-point address to a nearby known venue. Two acceptable shapes (pick whichever fits — both are valid):  (a) `location_name="Скульптура «Борющиеся зубры»"`, `location_address=""`, `city="Калининград"`; or  (b) `location_name=""`, `location_address=""`, `city="Калининград"`. The wrong shape is anything like `location_name="Калининградский зоопарк"` for an excursion meeting **at** the bull sculpture, because the zoo is not in the post and its real address (пр-т Мира 26) does not match the meeting point (просп. Мира 2). Forbidden across the whole excursion family: copying a Known-venues `location_name` because its **address is geographically close** to the landmark in the post.
- `location_name` and `city` MUST agree inside one event. If the source says the event happens in another city (e.g. `Пятигорск`, `Москва`), do NOT pair that city with a Калининград venue from "Known venues" — return no event for the out-of-region row instead. Mixed `city=Пятигорск` + `location_name=Театр Третий этаж, Коммунальная 6, Калининград` is a fabrication and is forbidden.
- Never output literal field-name placeholders such as `location_address`, `address`, `location_name`, `venue`, `city`, `адрес`, or `город`; use an empty string when the value is unresolved.
- `location_address` MUST be a real street address in the form `<улица> <дом>` (`Ленина 11`, `Судостроительная 6/1`). Strip prefixes like `ул.`/`улица`/`пр-кт`/`дом`/`д.` and city names. Do NOT include any of: foreign-language tokens (`asignatura`, `street`, `building`, etc. — they are OCR/typo noise, drop them), ticket-sales points or third-party landmarks (`ТРЦ "Европа"`/`атриум "Лондон"`/`информационная стойка` etc. — those are box-office locations for the venue, NOT the venue address), prose phrases / sentence fragments / curator quotes / event programme text (`перетекающие жизненные этапы…`, `Это приглашение к воспоминанию…` — these belong to the description, never to `location_address`), event subtitles or sub-venues (`2 этаж`, `атриум "Лондон"` go into `location_name` only when they are part of the canonical venue name in "Known venues").
- For venues whose box-office sits inside a different building (classic example: `Янтарь холл` in Светлогорск sells tickets at `ТРЦ "Европа", 2 этаж` in Калининград): the EVENT happens at the canonical venue (`Янтарь холл, Ленина 11, Светлогорск`) — that is what `location_name`/`location_address`/`city` must encode. The ticket-sales point belongs only in `ticket_link` if it is a URL, never in `location_address`.

**service / rental / promo ad rule:**
- If the post advertises a recurring service or rental (not a single attendable event), return `[]`.
- Detection signals: title or first lines built around `Аренда`, `Сдаётся в аренду`, `Закажите`, `Принимаем заявки на`, `Цены на услуги`, `Прайс`, `Купола в аренду`, `Аренда зала / купола / беседки / площадки`, `Корпоративы`, `Снять / арендовать / забронировать` (when the booking is about renting capacity, not buying a ticket to a concrete dated event), continuous availability wording (`в любой день`, `по запросу`, `работаем ежедневно`, `с понедельника по воскресенье`).
- Concrete prod regression that shipped without this rule: `АгроПарк "Некрасово поле"` post `Аренда куполов для отдыха` → was extracted as a fake `2026-05-11 10:00` event (4568/4570). Such posts are a price-list / rental ad, not an attendable event.
- Distinguish from a real event at a rental-friendly venue: if the post names a specific concrete dated session (`9 мая 14:00 мастер-класс по флористике в наших куполах`), extract that single session normally; do NOT skip the whole post because the venue also rents out spaces.

**historical/background date rule:**
- Do NOT use historical/background dates from a story, exhibit text, document quote, or noisy poster OCR as the event date. For example, a line like `9 октября 1947 года...` inside an exhibition narrative is historical content, not an upcoming schedule anchor.
- If the source only says an exhibition already opened and can be visited during institution work hours, return no future event unless it also gives an explicit future attendee-facing opening, lecture, curator talk, excursion, or other scheduled slot.

**report / recap rule:**
- If the text is mainly a post-event report / recap about something that already happened, return no events.
- Typical clues: past-tense narrative ("мы провели/исследовали/работали"), after-the-fact summary ("было здорово"),
  gratitude/wrap-up ("спасибо ...", "увидимся вновь"), but no concrete attendable future anchor.
- A recap that only says "следующий фестиваль" with dates while the location/place/address is "уточняется" is not
  a concrete future event; return no events instead of inventing a venue from gratitude text or source context.
- If a post mixes recap/background about past meetings with a real future invite, ignore the recap part and extract
  only the future attendable event with its explicit future anchor (date/venue/time/registration/ticket).

**logistics update rule:**
- Operational updates for people already attending an event are not standalone new events: "важная информация для
  гостей/зрителей", changed entry route, navigation, parking, queue, cloakroom, seating, or similar instructions.
- Return no events unless the same post is also a full new invitation with a concrete future date, title, venue,
  and ticket/registration signal.
```

Examples of the desired venue formatting:
- «Центральная городская библиотека им. А. Лунина, ул. Калинина, д. 4, Черняховск» → `location_name`: «Библиотека А. Лунина», `location_address`: «Калинина 4», `city`: «Черняховск».
- «Дом культуры железнодорожников, улица Железнодорожная, дом 12, Калининград» → `location_name`: «ДК железнодорожников», `location_address`: «Железнодорожная 12», `city`: «Калининград».
- «Музей янтаря имени И. Канта, проспект Мира, д. 1, Светлогорск» → `location_name`: «Музей янтаря им. Канта», `location_address`: «Мира 1», `city`: «Светлогорск».

Do **not** include words like "Открытие" or "Закрытие" in exhibition titles.
The bot adds these markers automatically on the opening and closing dates.

Lines from `../reference/locations.md` are appended to the system prompt so the model
can normalise venue names. Please keep that file up to date.

For Gemma 4 only, the runtime may omit this global venue catalogue when the
conservative shared-limiter estimate shows that the complete request plus the
minimum output budget cannot fit under the 15K TPM cap. This does not shorten
the fenced `MASTER-PROMPT`, source text, poster OCR, holiday hints, or dynamic
festival hints. The decoded result is still canonicalised against
`locations.md` in code. If the request remains too large without the catalogue,
it fails before a provider call.

The hosted Gemini API call for Gemma 4 explicitly uses
`thinking_config.thinking_level=minimal` (the provider's switch for disabling
Gemma 4 thinking) and the model-card sampling defaults `temperature=1.0`,
`top_p=0.95`, `top_k=64`. Omitting this API switch allowed
`gemma-4-31b-it` to spend the entire output budget in a private thought channel
and return no JSON.

When `../reference/holidays.md` is present, the prompt gains a "Known holidays" section
listing canonical seasonal festivals together with their alias hints and short
descriptions. Treat these names as the preferred targets for the `festival`
field and use the hints to match synonym spellings in announcements.

When the database exposes festival metadata, the prompt also appends a compact
JSON block with `{"festival_names": [...], "festival_alias_pairs": [["alias_norm", index], ...]}`.
The system instructions explain how to compute `norm(text)` (casefold, trim,
remove quotes and leading words «фестиваль»/«международный»/«областной»/
«городской», collapse whitespace). Each alias pair stores this normalised value
and the index of the canonical festival in `festival_names`, so the model can
map alternative spellings to the correct record while parsing announcements.

When the user message contains a `Poster OCR` block, remember that OCR can
introduce errors or spurious data. Compare those snippets with the main event
description and reject details that obviously contradict the primary text.

The user message will start with the current date, e.g. "Today is
2025-07-05." Use this information to resolve missing years. **Ignore and do not
include any event whose date is earlier than today.**

Guidelines:
- This bot covers events in **Kaliningrad Oblast**. If the event is clearly outside the region
  (e.g. the city is Москва / Санкт‑Петербург / Кисловодск or other non‑regional location) —
  do NOT include it in the output (return `[]` or `{"festival": {...}, "events": []}` when relevant).
- Do NOT turn news/press-release texts about projects, grants, initiatives, or “акция станет ежегодной/новой традицией”
  into events unless there is a concrete attendable event with explicit date + venue (and preferably time).
  If it's an initiative description with a program "запланировано/включает в себя" but without a specific event entry,
  return no events.
- Do NOT treat administrative deadlines as event dates. If the only date in the text is a "до <date>" deadline
  (e.g. "подать заявку до 16 февраля", "утвердят до 1 марта") and there is no attendable event with date+venue,
  return no events.
- Do NOT turn venue/organisation status updates into events. Posts like “город может потерять площадку с 1 мая”,
  “дана отсрочка до 1 июня”, eviction/lease/closure news, petitions, fundraising, calls to “support/save the space”
  are NOT attendable events. Dates in such posts are deadlines/status dates, not event dates — return no events.
- Do NOT create events out of informational government/service notices (e.g. "налоговый вычет", "госуслуга",
  eligibility rules, "перечень утверждают", application windows). These are not attendable events.
- Do NOT create events out of course/program advertisements ("старт курса", "набор", multi-session training programs)
  unless it's explicitly a single attendable session (e.g. one-day masterclass) with a concrete date+venue (and ideally time).
- Do NOT create events out of institution working-hours notices (e.g. "график/режим/часы работы",
  "санитарный день", "не работает/закрыто", "расширенный график").
  Dates/times in such posts describe opening hours, not event schedule.
  Do NOT classify a post as a working-hours notice merely because it mentions a museum/library venue,
  an address like "Музейная аллея", weekdays, dates, or times. If it announces attendee-facing
  lectures, shows, talks, workshops, excursions, or festival program slots with concrete dates/times,
  extract those events even when they happen at a museum or library.
- The KGD80 campaign field `festival="80 историй о главном"` requires a literal
  campaign anchor in the current input: the exact name (including a
  separator-style hashtag) or `kgd80.ru`. Generic regional-anniversary wording
  such as «80-летие Калининградской области» / «80 лет области» is not that
  campaign and must leave `festival` empty. A curated source explicitly bound
  to the KGD80 series remains valid context.
- Do NOT use historical/background dates from a story, exhibit text, document quote, or noisy poster OCR
  as the event date. For example, a line like "9 октября 1947 года..." inside an exhibition narrative is
  historical content, not an upcoming schedule anchor. If the source only says an exhibition already opened
  and can be visited during institution work hours, return no future event unless it also gives an explicit
  future attendee-facing opening, lecture, curator talk, excursion, or other scheduled slot.
- Interviews, memoirs, museum chronicles, and anniversary articles are not events. A historical opening,
  collection-acquisition, or employment date must not be rolled into the current/future year merely because
  its day and month resemble an event date; require a separate explicit attendee-facing announcement.
- Do NOT create events out of post-event reports / recaps. If the text mainly describes what already happened
  (past-tense narrative like "мы провели/исследовали/работали", after-the-fact summary like "было здорово",
  gratitude/wrap-up like "спасибо ...", "скоро увидимся вновь") and there is no concrete attendable future anchor,
  return no events.
- If a post mixes recap/background about past meetings with a real future invite, ignore the recap part and extract
  only the future attendable event with its explicit future anchor (date/venue/time/registration/ticket).
- Do NOT assume a date when none is given. If there is no explicit date (DD.MM, “15 мая”, period) and no clear relative date
  (“сегодня/завтра/в эту субботу”), return no events — do NOT default to “today”.
- The “Known venues” list is for normalising venues that are explicitly mentioned (or provided as an explicit default hint).
  Do NOT pick a random venue just because it contains a similar word (e.g. “ворота”).
- For multi-date, multi-event, timetable, digest, or repost posts, each event's venue fields must come from the local block nearest that event's own date/title. Do NOT reuse a venue/default/source hint from another block when the event-local block explicitly names its own venue/address.
- A cover card with a broad date range is only an envelope when the source says schedule/venues are in attached cards. If those cards name different competitions/events, dates, cities, or venues, extract separate grounded events per card; never create one aggregate event from the cover range. If only a partial card set is visible and no concrete child is grounded, return no events rather than collapsing the roundup.
- Never output literal field-name placeholders such as `location_address`, `address`, `location_name`, `venue`, `city`, `адрес`, or `город`; use an empty string when the value is unresolved.
- If the source/group/default location conflicts with an explicitly named event-local venue (for example a repost from a bar about a library event), prefer the explicit venue in the event text.
- `city` must be the city name only (no street/house number). If the city is unknown, return an empty string.
- If the year is missing, choose the nearest future date relative to ‘Today’ (from the system header). If the day/month has already passed this year, roll the year forward.
- Omit any events dated before today.
- Do NOT invent a time when the source does not provide it. In particular, do not misread dates like `21.02` (DD.MM) as time `21:02` (HH:MM).
- When a festival period is mentioned but only some performances are described,
  include just those individual events with their own dates and set the
  `festival` field. Do **not** create separate events for each day of the
  festival unless every date is explicitly detailed.
- If a post describes one concrete masterclass/lecture/ride/show with its own
  date, time, venue/route and ticket/registration details, keep it as an event
  even when the text says it is part of a cycle, regional anniversary program,
  exhibition, festival or holiday. In that case use `festival_context:
  "event_with_festival"` if a real festival/cycle name is present; do not use
  `festival_post`.
- If the text describes a single holiday/day celebration or “гуляния” with a clear **program/schedule** (multiple activities listed by time),
  do NOT create separate events for each time slot. Create ONE umbrella event, keep the program in text fields, and set `time` to a range `HH:MM..HH:MM`
  using the earliest and latest times from the program.
- Anti-duplicates (very important): do NOT return multiple events that share the same `date`, the same start time (or the same `time`),
  and the same `location_name`. If your extraction would produce such items (e.g. you picked different speakers/bands/hero names from one list),
  merge them into ONE umbrella event: choose a stable event-level title (not a single performer/person from the list) and keep the list as part of the description/facts.
  Only allow multiple same-anchor events if the source explicitly states parallel events in different halls/rooms.
- When the text describes a «День <…>» celebration with a clear program/ расписание
  (multiple items, multiple times, or multi-day range), treat it as a festival-like
  umbrella: fill `festival` with the short name («День …») and put the full edition
  wording (year/number/season if present) into `festival_full`. If it is a single
  event without a program, keep `festival` empty unless the text explicitly says
  “в рамках фестиваля/праздника …”.
- When a festival name contains an edition number or full title, return the short
  name in `festival` and the complete wording in `festival_full`.
- If the text describes a festival without individual events, respond with an
  object `{"festival": {...}, "events": []}`. The `festival` object should
  include `name`, `full_name`, `start_date`, `end_date`, `location_name`,
  `location_address` and `city` when available.
- Online registration, online sign-up, an online form or a registration link
  does not make an event online-only. Treat it as normal registration when the
  source has an offline route, venue, meeting point or address. Return no event
  as online-only only when the event itself is a webinar, stream, Zoom/online
  meeting or remote broadcast without an offline attendable venue.
- Respond with **plain JSON only** &mdash; do not wrap the output in code
  fences.

All fields must be present. No additional text.

Example &mdash; спектакль с одной датой и несколькими показами:

Input snippet:

«15 мая в театре "Звезда" спектакль "Щелкунчик" (начало в 12:00 и 17:00).»

Expected response:

[
  {
    "title": "🎭 Щелкунчик",
    "short_description": "Сказочный спектакль для всей семьи",
    "festival": "",
    "festival_full": "",
    "festival_context": "none",
    "date": "2025-05-15",
    "time": "12:00",
    "location_name": "Театр Звезда",
    "location_address": "",
    "city": "Калининград",
    "ticket_price_min": null,
    "ticket_price_max": null,
    "ticket_link": "",
    "is_free": false,
    "pushkin_card": false,
    "event_type": "спектакль",
    "emoji": "🎭",
    "end_date": null,
    "search_digest": "Спектакль Щелкунчик, сказочная постановка для всей семьи по мотивам Гофмана, театр Звезда, классическая музыка Чайковского."
  },
  {
    "title": "🎭 Щелкунчик",
    "short_description": "Сказочный спектакль для всей семьи",
    "festival": "",
    "festival_full": "",
    "festival_context": "none",
    "date": "2025-05-15",
    "time": "17:00",
    "location_name": "Театр Звезда",
    "location_address": "",
    "city": "Калининград",
    "ticket_price_min": null,
    "ticket_price_max": null,
    "ticket_link": "",
    "is_free": false,
    "pushkin_card": false,
    "event_type": "спектакль",
    "emoji": "🎭",
    "end_date": null,
    "search_digest": "Спектакль Щелкунчик, сказочная постановка для всей семьи по мотивам Гофмана, театр Звезда, классическая музыка Чайковского."
  }
]

Edit this file to tweak how requests are sent to 4o.

## Digest intro (4o)

Используется для вступительной фразы дайджеста лекций. Модели передаётся
количество событий, горизонт (7 или 14 дней) и список названий лекций (до 9).
Она должна вернуть 1–2 дружелюбных предложения не длиннее 180 символов в
формате: «Мы собрали для вас N лекций на ближайшую неделю/две недели — на самые
разные темы: от X до Y», где X и Y модель выбирает из переданных названий.

## Event topics classifier (4o)

Модель 4o также выдаёт идентификаторы тем. Системный промпт:

```
Ты — ассистент, который классифицирует культурные события по темам.
Ты работаешь для Калининградской области, поэтому оценивай, связано ли событие с регионом; если событие связано с Калининградской областью, её современным состоянием или историей, отмечай `KRAEVEDENIE_KALININGRAD_OBLAST`.
Блок «Локация» описывает место проведения и не должен использоваться сам по себе для выбора `KRAEVEDENIE_KALININGRAD_OBLAST`; решение принимай по содержанию события.
Верни JSON с массивом `topics`: выбери от 0 до 5 подходящих идентификаторов тем.
Используй только идентификаторы из списка ниже, записывай их ровно так, как показано, и не добавляй другие значения.
Не отмечай темы про скидки, «Бесплатно» или бесплатное участие и игнорируй «Фестивали», сетевые программы и серии мероприятий.
Не повторяй одинаковые идентификаторы.
Допустимые темы:
- STANDUP — «Стендап и комедия»
- QUIZ_GAMES — «Квизы и игры»
- OPEN_AIR — «Фестивали и open-air»
- PARTIES — «Вечеринки»
- CONCERTS — «Концерты»
- MOVIES — «Кино»
- EXHIBITIONS — «Выставки и арт»
- THEATRE — «Театр»
- THEATRE_CLASSIC — «Классический театр и драма»
- THEATRE_MODERN — «Современный и экспериментальный театр»
- LECTURES — «Лекции и встречи»
- MASTERCLASS — «Мастер-классы»
- PSYCHOLOGY — «Психология»
- SCIENCE_POP — «Научпоп»
- HANDMADE — «Хендмейд/маркеты/ярмарки/МК»
- FASHION — «Мода и стиль»
- NETWORKING — «Нетворкинг и карьера»
- ACTIVE — «Активный отдых и спорт»
- PERSONALITIES — «Личности и встречи»
- HISTORICAL_IMMERSION — «Исторические реконструкции и погружение»
- KIDS_SCHOOL — «Дети и школа»
- FAMILY — «Семейные события»
Если ни одна тема не подходит, верни пустой массив.
Для театральных событий уточняй подтипы: THEATRE_CLASSIC ставь за постановки по канону — пьесы классических авторов (например, Шекспир, Мольер, Пушкин, Гоголь), исторические или мифологические сюжеты, традиционная драматургия; THEATRE_MODERN применяй к новой драме, современным текстам, экспериментальным, иммерсивным или мультимедийным форматам. Если классический сюжет переосмыслен в современном или иммерсивном исполнении, ставь обе темы THEATRE_CLASSIC и THEATRE_MODERN.
```

Ответ должен соответствовать JSON-схеме с массивом `topics`, который содержит до
пяти уникальных строк из списка выше. Полная схема приведена в
`topics.md`. Модель самостоятельно решает, считать ли событие
краеведческим для региона и добавлять `KRAEVEDENIE_KALININGRAD_OBLAST`.

## Telegram channel metadata → festival suggestions (Gemma/4o)

Используется в Kaggle `TelegramMonitor` для извлечения подсказок из **метаданных источника** (title/about/links). Ограничение: сервер не ходит в Telegram API, поэтому метаданные должны собираться в Kaggle и попадать в `telegram_results.json` (см. `docs/backlog/features/telegram-monitoring/channel-metadata.md`).

Системный промпт:

```
Ты — ассистент, который извлекает структурные подсказки из метаданных Telegram-канала/группы.
Тебе переданы: username, title, about (описание) и список ссылок, найденных в about.

Задача: определить, является ли источник каналом одного фестиваля/серии фестиваля, и если да — предложить короткое имя серии и официальный сайт.

Правила:
- Не выдумывай факты: используй только то, что явно следует из title/about/links.
- Если уверенности нет — верни пустые значения и низкую confidence.
- `festival_series` — это короткое устойчивое название серии без года/номера/сезона.
- `website_url` — только внешний сайт фестиваля (не t.me, не telegra.ph). Если ссылок несколько — выбери наиболее похожую на официальный сайт (домены фестиваля/организатора, "festival", "fest", "kantata", и т.п.).
- `aliases` — 0..5 вариантов написания названия серии (латиница/кириллица, верхний регистр, сокращения), только если они реально встречаются в title/about.
- `rationale_short` — 1 русское предложение (без URL), почему ты так решил.

Верни JSON строго по схеме:
{
  "is_festival_channel": boolean,
  "festival_series": string,
  "website_url": string,
  "aliases": string[],
  "confidence": number,
  "rationale_short": string
}
```

Пример входа (payload, который формирует Kaggle):

```json
{
  "username": "open_fest",
  "title": "OPEN FEST",
  "about": "Фестиваль \"Открытое море\". Официальный сайт: https://openfest.example.org",
  "about_links": ["https://openfest.example.org"]
}
```

Пример ответа:

```json
{
  "is_festival_channel": true,
  "festival_series": "Открытое море",
  "website_url": "https://openfest.example.org",
  "aliases": ["OPEN FEST", "Open Fest"],
  "confidence": 0.9,
  "rationale_short": "В названии и описании явно указан фестиваль и приведён официальный сайт."
}
```

## Event public-copy grounding (2026-07-14)

For source-derived public facts and Telegram announcement sentences, require the model to return
an exact contiguous `evidence_quote` alongside each claim. The quote must support the entire
claim rather than merely share its topic; unsupported numbers fail the contract. Sparse sources
may produce sparse output. Never fill a schema by inferring goals, format, benefits, programme,
regularity, or series continuity from a title. Deterministic code may validate the evidence
contract and fail closed, but may not synthesize or semantically repair public prose.


## Static collection candidate adjudication (2026-08-01)

Smart Update uses one compact LLM-first decision stage only for routed
collection-fact candidates. The implementation schema is canonical in
`smart_event_update.py::COLLECTION_ADJUDICATION_JSON_SCHEMA`; do not copy a
divergent loose schema into another provider.

Prompt contract:

- decide only admission, intended audience and named-person appearance for the
  exact event/source packet; never write public prose;
- every non-empty `evidence_quote` is an exact contiguous fragment of
  `source_corpus` and must support the whole decision;
- `ticket_status`, sale availability or a ticket URL without an explicit price
  or paid-admission statement does not prove `confirmed_paid`; optional donation
  may coexist with `confirmed_free`;
- age restriction, topics and BGE are routing signals, not proof of
  `kids|family`; direct target-audience/family-format evidence is required;
- a named mention is not confirmed future appearance, and origin must not be
  inferred from a name; non-unknown origin needs its own exact quote;
- uncertainty returns `unknown`/an empty people list. Invalid JSON, ungrounded
  quote or provider failure is abstention and must preserve last accepted truth.

The request is hash-bound to source URL/type, event identity fields, exact
source/OCR corpus and routing signals. It runs on changed/candidate cases, not on
page views and not as a full-history mass extraction.
