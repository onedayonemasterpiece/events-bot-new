# Requirements: Recap

Status: draft  
Last reconciled: 2026-06-28

## Product intent

- `Recap` aggregates post-event evidence in one place: how an event actually happened, what organizers/visitors/media posted afterwards, and where the audience can inspect original reports.
- The product surface is a separate recap-oriented publication channel (working concept: “Recap Калининград”) plus a longer public page when the event has enough source material.
- Recaps are also reusable context for future announcements: if a new event belongs to an annual or repeating series, announcements may link to one or more previous recaps to show how earlier editions went.

## Requirements

### Source collection and provenance

- The system must monitor a shared pool of Telegram, VK and other available public sources for post-event reports, media and reactions about events already stored in the bot.
- A recap candidate must preserve source provenance: links to the original post-event reports, links to the original event announcements, and references to media assets used in the recap.
- The recap pipeline must be able to aggregate multiple source publications about the same event and treat them as evidence for one recap, not as new future-event announcements.

### Recap eligibility and editorial selection

- Recaps are not required for every event. The MVP must start with manual/LLM-assisted analysis of real past events to define selection criteria before broad automation.
- Initial eligibility signals should include: number of independent post-event publications, strength/quality of media, event scale or local significance, whether the event belongs to a repeating/annual series, and whether the recap can improve future announcements.
- Volume-based output selection must be supported:
  - a single strong source can be handled as a short native recap post when a long page would be excessive;
  - small/medium source sets require an editorial decision about whether a native post is enough or whether a long page is warranted;
  - when there are more than five distinct relevant publications, the expected output is a long recap page with all key source links and a summarized editorial body.
- The exact boundary for the “editorial decision” zone is intentionally left as a product question (`1-3` vs `1-5` sources), because the intake explicitly mentioned both variants.

### Public recap page

- Long recaps should be editorial pages, not raw link dumps: the page should combine short narrative text, selected highlights, embedded/inline media when useful, and a complete source-link section.
- Technically recommended target: build long recap pages on the project’s own static site, under the same announcements/public-site surface (for example a `/recap/<slug>` section), rather than making Telegraph the target architecture.
- Telegraph may be used only as a transitional compatibility surface while the static-site recap surface is not ready.
- Recap pages must support SEO/GEO basics consistent with the existing static event-page direction: stable URL, canonical metadata, useful HTML content, Open Graph preview, source attribution, and internal links to the related event/series pages.
- Pages with many outbound source links must still be useful indexed pages: the requirement is to avoid thin “only links” pages, not to hide the whole recap from indexing by default.

### Recap channel posts

- A recap channel post must contain a concise summary of how the event went and links to the related original event announcement(s).
- If a long recap page exists, the channel post should include one to three strongest highlights in the post body and link to the long page for the full source list and expanded narrative.
- Telegram recap posts must support media groups when multiple images/videos are selected.
- VK recap publications must support carousel-style media; for some cases a table/structured visual summary may be appropriate.
- If a recap has a strong standalone video, the video may be published as a separate recap post or grouped recap asset instead of being hidden only behind a page link.

### Media handling

- When the source set is small or editorially curated, selected media should be embedded directly into the recap page body in a blog-like reading flow: text → image/video → text → image/video.
- Links to source posts must remain available even when media is embedded directly.
- The system should avoid hiding all interesting media behind external source links when there are only a few important media objects.

### Linking recaps from future announcements

- Event announcements on the public site, Telegram, VK and other publication surfaces may link to previous recaps when the new event is clearly part of the same annual/repeating series.
- A future announcement may link to one previous recap or several relevant previous recaps, for example last year’s edition of an annual festival or recent editions of a recurring festival held every few months.
- Recap links used in announcements must be clearly contextual: they are supporting evidence/history, not a replacement for current event facts such as date, venue, tickets or schedule.

## Open questions

- Product decision: where exactly to place the threshold between “native recap post only” and “long recap page”: `1-3` source publications, `1-5`, or another rule based on source quality and media strength?
- Product decision: confirm the public channel naming/branding for the separate recap channel (the intake used the working concept “recap Калининград”).

## Technical recommendations / findings

- The existing project direction for public event pages already prefers own static HTML pages over Telegraph for SEO/GEO, metadata, sitemap, canonical URLs and design control (`docs/backlog/features/static-event-pages/README.md`). Recap should follow the same direction and use Telegraph only as a temporary compatibility layer.
- Existing Telegram Monitoring data contracts already store source links (`event_source`) and media assets (`event_media_asset`, `eventposter`, `photo_urls`), so the recap evidence model should reuse or extend those provenance/media patterns instead of storing opaque recap-only blobs.
- For SEO handling of many outbound source links, the recommended default is: index useful editorial recap pages, keep visible source attribution, and apply link attributes such as `rel="nofollow"`/`ugc` only where a specific outbound link is not an editorial endorsement. Do not make `noindex` the default for the whole page solely because it has many source links.

## Decisions log

- 2026-06-28: Initial pending intake reconciled into canonical draft requirements. No conflicts with previous requirements were found because the prior canonical sections were placeholders (`TBD`).
- 2026-06-28: Technical default recorded: long recap pages should target the project’s own static site (`/recap/<slug>`-style surface) before Telegraph, aligned with the existing static event-page backlog.

## Intake 2026-06-28T08:24:35+00:00

Status: resolved/archived (reconciled 2026-06-28)

### User notes

Итак, рекэп событий это агрегация информации о том, как событие было проведено в одном месте. Я предлагаю такую реализацию. Естественно, мониторинг, мониторинг каналов, которые об этом пишут, мониторинг разных каналов из некого общего пула. И далее составление, ну, в зависимости от количества публикаций Если буквально одна, то просто пост Если от одной до трех, то вопрос Ну, то есть это нужно принимать решение Возможно даже от одной до пяти Если их более пяти, то создается телеграф страницы, на которой будут все ссылки на телеграм и на века и так далее естественно с описанием события которые по сути всеошная и в итоге эта страница должна быть доступна также создается текст который будет в посте и

итоговая цель какая заводим отдельный канал который будет по сути recap калининград в нем будет я посты о том как прошли те или иные события если отчетов таких было много то самое интересное вытаскивается в самое интересное вытаскивается в тело поста буквально две-три не знаю может быть даже одно а далее дается ссылка к примеру на ту же самую телеграф страницу и в итоге где можно посмотреть все подробные ссылки и какое-то более подробное описание события самом же посте короткое описание события в самом посте также есть ссылки на исходные анонсы событий

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Важно, что при создании анонсов на сайте анонсов, а также в постах в телеграме, в ВК и так далее, может использоваться ссылка на рекэп предыдущих событий. Например, события могут быть ежегодными. Тогда, допустим, День молодежи 2026, тогда мы даем ссылку на День молодежи 2025. Допустим, это какой-нибудь фестиваль Кантата, тогда мы даем ссылки на рекэп за предыдущий год. Это может быть фестивали, которые многократно повторяются в течение года. Например, фестиваль Гаражка повторяется каждые несколько месяцев, наверное, даже каждые пару месяцев. Можно дать ссылку на предыдущий рекэп, а можно дать ссылку даже потенциально на несколько рекэпов.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Нужно провести анализ и выявить, целесообразно ли это делать через телеграф в страницу, или может быть стоит сделать в виде подстраниц от полюбить Калининград анонсы, ну то есть сайта, который мы сейчас делаем. Если делается внутри сайта, то что это? Отдельная подпапка или может быть отдельный домен? Нужно проанализировать и принять решение.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Нужно проанализировать с точки зрения SEO, не является ли вот такая страница с огромным количеством внешних ссылок фактором, сильно понижающим ранг сайта. Или может быть с точки зрения SEO, как бы это и можно сделать, просто закрывать их через noindex, сами ссылки. Короче, здесь нужно очень хорошо, очень тщательно проанализировать, какова правильная стратегия.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Посты, которые формируются в отдельном канале по recap, должны содержать медиафайлы. В ВК это может быть карусель, в каких-то случаях это может быть таблица, в Telegram это медиагруппа.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Если у рекэпа есть сильный видеоролик, то видеоролик потенциально может выйти неким отдельным постом, то есть такой групповой рекэп.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Также нужно проанализировать, если медиа материалов не так много, то возможно их не стоит прятать непосредственно за ссылками, ну то есть ссылки, конечно же, оставить, но и вставить эти медиа объекты внутри тела страницы. По сути это такая блогово-редакторская рекеп получается. Ты читаешь текст, смотришь картинку, читаешь текст, смотришь картинку, читаешь текст, смотришь картинку.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Нужно определить уровень создания recap'ов. Не для каждого события они скорее всего нужны. Здесь нужно тщательно выработать критерии и произвести изначально ручной анализ через нейросеть, через тебя, через кодекс, через чат GPT и так далее.

### Source files

- [source/voice_AgADNqIAAkoWAAFK.oga](source/voice_AgADNqIAAkoWAAFK.oga)
- [source/voice_AgADN6IAAkoWAAFK.oga](source/voice_AgADN6IAAkoWAAFK.oga)
- [source/voice_AgADPKIAAkoWAAFK.oga](source/voice_AgADPKIAAkoWAAFK.oga)
- [source/voice_AgADPaIAAkoWAAFK.oga](source/voice_AgADPaIAAkoWAAFK.oga)
- [source/voice_AgADPqIAAkoWAAFK.oga](source/voice_AgADPqIAAkoWAAFK.oga)
- [source/voice_AgADQqIAAkoWAAFK.oga](source/voice_AgADQqIAAkoWAAFK.oga)
- [source/voice_AgADRqIAAkoWAAFK.oga](source/voice_AgADRqIAAkoWAAFK.oga)

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution. No contradiction with previous requirements was found because the previous canonical sections were empty/TBD.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
