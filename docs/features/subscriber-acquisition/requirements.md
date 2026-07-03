# Requirements: Subscriber Acquisition

Status: reconciled draft; Discovery MVP addendum 2026-06-29

## Product intent

- Grow the subscriber/audience base for the event announcement system through social acquisition workflows in Telegram and VK.
- Acquisition actions should happen inside relevant social spaces — communities, chats, channel comment threads, and VK communities — where people are already discussing what to visit.
- The feature should help recommend events in a way that feels like a useful human reply, not like mass advertising.
- Links in recommendations should point to the appropriate canonical public surface for the platform: Telegram announcement/channel links for Telegram contexts, VK community/post links for VK contexts, and static event-site links when they are the best public destination.
- Sticker or sticker-pack generation tied to notable events is a separate acquisition/engagement idea that can be explored as a follow-up, especially for major events where a sticker reply may be more natural than text.

## Requirements

### Social-source discovery and monitoring

- The system should support collecting or configuring candidate social spaces where subscriber acquisition can happen: Telegram chats, Telegram channel comment threads, VK communities, and similar discussion surfaces.
- The system should analyze messages/comments in those candidate spaces to find moments where an event recommendation is contextually useful.
- Detection should combine LLM analysis with narrower deterministic signals/keywords; broad semantic classification of “this person may want an event recommendation” must remain LLM-first.
- Relevant signals include questions about upcoming events, requests for recommendations, discussions where a specific type of event would be useful, and similar intent-bearing comments.

### Event recommendation generation

- For a detected recommendation opportunity, the system should select one or more likely suitable events from the announcement/event corpus.
- Event selection should consider the message context, likely user intent, platform context, and available event metadata.
- The generated response should be written as a natural human-like reply, not as an obvious bot advertisement.
- The reply may mention concrete event names and, when appropriate, include links to those events.
- The reply format should support replying directly to the source message/comment when the platform allows it.


### Organizer clarification acquisition

- Publics that belong to event organizers, venues, festivals, parks or similar
  event hosts are a separate acquisition surface: the useful action may be not a
  recommendation to a third-party user, but a polite public clarification
  question to the organizer under their own event post.
- The goal is still audience acquisition: a useful, non-spammy clarification can
  draw attention to the operator/profile that contains information about the
  event service, and in some cases can later justify linking to the enriched
  canonical event page.
- The scanner should first check whether the organizer post corresponds to an
  event already known to the system. Use the existing event retrieval/vector
  search capability to match the post to a canonical event candidate before
  proposing any clarification.
- Clarification is eligible only when the matched event is missing information
  needed for an “ideal event card”. The ideal-card checklist should include, at
  minimum, date/time, venue/address, price/ticket status/link, age restriction,
  duration, registration/entry rules, accessibility/children constraints where
  relevant, and other practical organizer nuances that make the event complete.
- For Telegram organizer clarification, discovery targets organizer-owned
  channels, not ordinary chats. A channel is eligible only after resolver proof
  of an accessible linked discussion and at least one recent non-empty human
  comment/reply surface; an empty linked discussion or copied channel-post mirror
  is not enough.
- The system should learn typical clarification patterns from real public
  questions in organizer threads (for example age limits, duration, entry rules,
  whether registration is required, what is included in the ticket, weather/
  outdoor constraints), but it must not blindly ask generic questions. The
  library stores semantic question classes/evidence, not public text templates.
- Before creating a review opportunity, the system must guarantee that the same
  or materially equivalent question was not already asked earlier in the same
  thread by another participant or by us.
- LLM usage for this subfeature should be minimized: deterministic event-post
  detection, vector-search retrieval, metadata-diffing against the ideal-card
  checklist, thread dedupe, and cheap pattern clustering should happen before
  spending LLM budget on final semantic validation and wording.
- Final clarification question text must be generated only by an LLM from
  grounded facts and constraints. Deterministic code may prepare JSON facts,
  labels and safety gates, but must not concatenate templates, fixed prefixes or
  regex/synonym rewrites into `draft_question` or `published_question`.
- A separate reviewer model/stage must validate every generated and every
  actually published clarification question for naturalness, appropriateness,
  clarity, answerability, grounding, duplicate risk and spam/self-promo risk. A
  failed review drops the candidate or sends it back to the writer LLM; reviewer
  text is not published directly.


### Link targets and platform routing

- In Telegram contexts, recommendation links should prefer Telegram announcement/channel links when available and appropriate.
- In VK contexts, recommendation links should prefer VK community/post links when available and appropriate.
- If neither platform-native link is suitable, the response may link to the static event site / personalization site surface when it exists and is a better destination.
- Link selection should avoid mixing platform destinations in a confusing way; the chosen link should match the user’s current context unless there is a clear product reason to use a different surface.

### Anti-spam and safety constraints

- The feature must avoid spammy behavior: do not post frequently, do not flood a single community, and do not repeatedly answer similar prompts in the same place.
- Recommendations should be precise and sparse — “surgical” interventions in the best-fit conversations are preferred over broad campaigns in one community.
- It is preferable to make a small number of relevant recommendations across multiple communities than to mass-post in one community.
- The system should maintain rate limits, cooldowns, deduplication, and per-community/per-thread guardrails before any automatic posting is enabled.
- Automatic posting should be conservative by default; review/shadow mode is expected before broad unattended operation.

### Sticker-based follow-up idea

- The product may support generating event-related stickers or sticker packs as a separate engagement/acquisition mechanic.
- Sticker generation should be treated as a follow-up feature until its content rules, brand safety, publication flow, and platform constraints are specified.
- For major events, the system may eventually answer or promote via a sticker when a sticker is more natural than a text recommendation.

## Discovery MVP addendum (2026-06-29)

The first implementation slice is **Discovery-only shadow mode**. It should run
on Kaggle through the existing Telegram Monitoring-style infrastructure, scan
public Telegram chats/channel comment threads and VK community comment threads,
discover additional public surfaces, and put both surface candidates and
reply-opportunity candidates into manual review. Automatic replies, DMs, personal
user harvesting, VK personal-wall crawling, and sticker generation are outside
the first MVP.

Initial Telegram seeds for MVP discovery:

- `https://t.me/tg_kgd`
- `https://t.me/chatkalin`
- `https://t.me/kenig01chat`
- `https://t.me/zhest_kaliningrada`
- `https://t.me/pereezd_v_kaliningrad_legko`

VK is included in MVP scope. For the first automated discovery runs, seed VK from
all communities already present in existing VK monitoring (`vk_source`), then add
new discovered VK community links to the same frontier as candidates. The schema,
runtime config, review queue, and report must stay VK-ready from the first MVP.

For broadcast channels, discovery should inspect the linked discussion/comment
chat where available and scan comments/replies, not only top-level channel
posts. For groups/supergroups, discovery scans recent public chat messages
directly. The detailed MVP design and work estimate are in
[`mvp-discovery.md`](mvp-discovery.md).

Additional discovery topics from the 2026-07-01 static-site documentation update:

- event-site search/listing needs: people asking for a site, search, calendar, exhibitions list, popular events, or similar navigation help;
- organizer-side acquisition: people asking where to add/send/publish an event announcement or how to arrange information partnership;
- quick-filter/badge needs: Pushkin card, kids/family, charity, recording/stream, free-entry questions that map to event-token medallions and future filters;
- trip-route recommendation contexts from `trip-recomendation`: questions like where to go/sъездить for one day from Kaliningrad by train/car, where a concrete collected route should be recommended instead of a general public.
- organizer clarification acquisition: event organizer/venue/festival publics
  where a post can be matched to an existing event and a polite question can
  clarify missing details from the ideal event-card checklist.

These topics are discovery/review candidates, not automatic replies. Broad semantic
classification and final reply suitability remain LLM-first.

Region requirement:

- Discovery is Kaliningrad Oblast scoped. Deterministic surface filtering should
  reject obvious out-of-region communities/channels before adding them to the
  future scan queue; e.g. `visitNavahrudak`/Novogrudok is not a Kaliningrad
  surface. Unknown region is a diagnostic/review state, not an excuse to crawl
  arbitrary foreign/regional publics aggressively.

Storage/runtime decision for MVP:

- The current code uses core Fly SQLite as an MVP compatibility layer because it
  is already wired to the bot review UI and Kaggle status/runtime tables.
- Discovery state is a common logical base for all acquisition actions. Do not
  create separate databases/source lists for organizer clarification versus
  generic recommendation replies; store per-action eligibility/status on the same
  surface/opportunity graph.
- This is not a final physical-storage requirement. The accepted analysis in
  [`mvp-discovery.md`](mvp-discovery.md#data-ownership-analysis) allows migrating
  the common discovery store to Yandex Managed PostgreSQL once the frontier
  graph/report workload grows beyond prototype size.
- Do not use the personalization Supabase/Postgres DB for this surface; that DB
  owns anonymous site telemetry/profiles/recommendation caches.
- Do not create a separate bot for MVP; revisit only if volume/policy boundaries
  require separate credentials and deployment.
- Reuse the existing Kaggle encrypted-dataset, `kaggle_status`,
  `kaggle_registry`, `remote_telegram_session`, and `telegram_session:s22` lease
  contracts. The job must run only in idle/free time relative to heavy Kaggle/LLM
  runs and skip cleanly when the shared remote Telegram session is busy.

Review UI requirement:

- Add a separate operator menu (for example `/acq` or `/subs`) to list monitored
  acquisition surfaces, approve/reject/pause candidates, add seeds manually, and
  inspect key stats/evidence links.

Product prioritization requirement:

- Each opportunity and surface should include a conservative potential-reach
  estimate: how many additional people may realistically still see a reply. This
  estimate should use lower-bound public metrics where available (post views,
  comment activity, recent engagement) and should never assume that all channel
  subscribers/community members will see a comment. High reach cannot override
  high spam or safety risk.

Report requirement:

- Every useful non-empty discovery run should publish a Telegraph report page
  with direct Telegram/VK links, per-public monitoring-potential analytics,
  evidence snippets, reach estimates, recommended actions, and sticker-strategy
  observations. This page is the convenient artifact for manual/external review.

Sticker strategy requirement:

- Discovery should also report Telegram chats/threads where a sticker-based
  strategy may be viable: whether stickers are common, whether ordinary users
  can send them, whether the public reaction is tolerant, and whether a sticker
  whose pack/title points to the announcements channel would be natural. The MVP
  only analyzes suitability; it does not create or send stickers.

LLM requirement:

- Semantic classification of surfaces and recommendation opportunities remains
  LLM-first. Use Google/Gemma through the existing quota/rate-limit layer; Lite
  models can be a lower-confidence fallback/probe but not a replacement for
  high-confidence review decisions.
- Discovery regex/keywords may only extract links, apply hard safety/region
  gates, dedupe, and build cheap prefilter hints for LLM budget control. They
  must not be the owner of semantic suitability. In particular, post-event
  thanks/reports/praise must be rejected by the Gemma checklist gate unless there
  is a separate explicit current/future need and a clear native reply target.

## Open questions

- What exact rate limits and cooldown windows should apply per community, per thread, and globally after the first live discovery calibration?
- Who is the long-term owner for approving newly discovered acquisition surfaces after MVP shadow mode?
- Which event-link surface is canonical when multiple links exist for the same event and platform context?
- What safety policy should govern sticker generation before it becomes an implementation task?

## Decisions log

- 2026-06-27: Reconciled six Telegram voice notes into this canonical requirements draft.
- 2026-06-27: Preserved anti-spam as a primary product constraint: sparse, precise recommendations across relevant communities are preferred over repeated posting in one community.
- 2026-06-27: Treated sticker/sticker-pack generation as a related follow-up idea rather than part of the first implementation scope.
- 2026-06-27: Reconciled the 21:10 voice-note intake as an operational request to review/check requirements; no new product requirement delta was found.
- 2026-06-29: Selected Discovery-only shadow mode as the MVP slice: scan public Telegram chats/comment threads, discover new surfaces, and write manual-review candidates; no automatic posting/DMs/user harvesting.
- 2026-06-29: Used existing core Fly SQLite DB and existing bot UI as the MVP compatibility layer; personalization Supabase/Postgres and a separate bot are out of scope for MVP.
- 2026-07-01: Reopened final storage ownership as an explicit decision after operator feedback; analysis recommends Yandex Managed PostgreSQL as the target discovery-state store if the MVP graph grows beyond prototype size, but this is not a hard requirement until a migration task is accepted.
- 2026-06-29: Chose existing Kaggle Telegram Monitoring-style infrastructure with `TELEGRAM_AUTH_BUNDLE_S22`, `telegram_session:s22` lease, `kaggle_status`, `kaggle_registry`, remote-session guard, and heavy-job idle scheduling.
- 2026-06-29: Added VK communities/comment threads to MVP scope; 2026-07-01 update uses existing `vk_source` monitoring communities as the first automated seed set.
- 2026-06-29: Added conservative potential-reach scoring as a product-prioritization signal for opportunities and surfaces.
- 2026-06-29: Added Telegraph report output as the primary link-heavy manual/external review artifact.
- 2026-06-29: Added sticker-strategy suitability analysis as research-only MVP output; sticker creation/sending remains follow-up.
- 2026-07-02: Added organizer clarification acquisition as a separate
  subfeature: match organizer posts to known events through vector search,
  compare against an ideal event-card checklist, dedupe existing questions in
  the thread, and use LLM only for final validation/wording.

## Archived intake 2026-06-27T20:27:51+00:00

Status: resolved / archived 2026-06-27

Resolution: Integrated all six automatic voice transcripts into Product intent, Requirements, Open questions, and Decisions log above. The raw audio files remain linked below for auditability.

Source files:

- [source/voice_AgADE58AAkoWAAFK.oga](source/voice_AgADE58AAkoWAAFK.oga)
- [source/voice_AgADFp8AAkoWAAFK.oga](source/voice_AgADFp8AAkoWAAFK.oga)
- [source/voice_AgADGZ8AAkoWAAFK.oga](source/voice_AgADGZ8AAkoWAAFK.oga)
- [source/voice_AgADHZ8AAkoWAAFK.oga](source/voice_AgADHZ8AAkoWAAFK.oga)
- [source/voice_AgADIZ8AAkoWAAFK.oga](source/voice_AgADIZ8AAkoWAAFK.oga)
- [source/voice_AgADIp8AAkoWAAFK.oga](source/voice_AgADIp8AAkoWAAFK.oga)

Reconciliation checklist:

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-27T20:52:55+00:00

Status: resolved / archived 2026-06-27

Resolution: Recovered from the forum chat after the ArtKodex restart fallback issue. Transcript: “Можно проверять.” This is an operational confirmation/check request and does not change the product requirements above.

Source files:

- [source/voice_msg_1061.oga](source/voice_msg_1061.oga)

Reconciliation checklist:

- [x] Voice message recovered from Telegram history.
- [x] Voice message transcribed and saved in the feature event log.
- [x] Product requirements checked: no new requirement delta from this operational note.

## Archived intake 2026-06-27T21:10:03+00:00

Status: resolved / archived 2026-06-27

### User notes

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Можно проверять.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Теперь можно проверить требования.

### Resolution

Compared with the canonical requirements above. The intake is an operational confirmation/request to check the requirements and does not introduce or change product behavior, scope, safety policy, link routing, posting mode, source allowlists, rate limits, or sticker scope. No conflict with existing requirements was found, and no new user decision is required.

### Source files

- [source/voice_msg_1061-1.oga](source/voice_msg_1061-1.oga)
- [source/voice_AgADVJ8AAkoWAAFK.oga](source/voice_AgADVJ8AAkoWAAFK.oga)

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-28T07:36:42+00:00

Status: resolved / archived 2026-06-29

### User notes

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Также нужно, чтобы было отдельное меню, где можно просматривать все каналы, чаты сообщества, которые мониторятся. Чтобы там можно было добавлять эти чаты, которые мониторятся для продвижения, исключать их, чтобы можно было видеть ключевую статистику по ним.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Нужно провести анализ, делать ли это в рамках внутри имеющейся базы данных или делать отдельно, или вообще это сделать в формате отдельного небольшого бота. Нужно определить целесообразность, раздельно или внутри. То есть несколько вариантов провести тщательный анализ, спрогнозировать рост и так далее. Возможно, целесообразно наличие отдельной БД, но в рамках имеющегося бота.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Взаимодействие по анализу. Использовать в нейросети Google через контроль лимитов, скорее всего, ГЕМ-4. Можно 26B, например, для того, чтобы анализировать сообщения, принимать решения, как на них отвечать. Нужно проанализировать, сразу ли может эта нейросеть сформулировать ответ подходящий, или нужно делать через отдельный запрос. Как fallback и дополнительный вариант использовать нейросеть Google Lite.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Нужно также в ВК мониторить еще и личные стены людей. И возможно, если там большая активность и много людей приходит писать в комментарии, там тоже иногда планировать делать реплай на такие комментарии или...

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Также нужен Discovery режим, то есть каждый день делать отдельный запуск, когда из имеющихся сообществ, ВК, стен, телеграм-чатов, телеграм-каналов, чатов, привязанных к каналам, выявлять ссылки на другие стены людей, на другие сообщества, на другие каналы, где открыты комментарии, и проводить автоматическое исследование целесообразности дальнейшего мониторинга этих чатов на предмет вопросов, на предмет продвижения. То есть автоматический Discovery должен отвечать на вопрос, добавлять ли сообщество чат или что это в постоянный мониторинг или не добавлять. Такой инструмент Discovery должен просматривать на приличную глубину.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

При этом должно быть хорошо продуманное ограничение, которое не позволит уходить слишком глубоко в работу по анализу страниц. То есть будет эффективно тратить ресурсы.

### Resolution

Resolved by the 2026-06-29 Discovery MVP addendum and `mvp-discovery.md`: separate operator menu, core-DB MVP, existing Kaggle runtime/limit control, LLM-first triage, VK/personal-wall follow-up, daily discovery with depth/budget limits.

### Source files

The transcript above is sufficient for requirements review. Raw 2026-06-28 voice
artifacts are kept only as local intake materials unless they are explicitly
needed for audit.

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-28T09:40:15+00:00

Status: resolved / archived 2026-06-29

### User notes

Я думаю дискавери можно вынести как раз в MVP, проанализируй как это сделать. Т.е. основной механизм реализовать, но запустить в продакшн именно дискавери чтобы он периодически запускался, например раз в день и добавлял кандидатов, сначала ссылками на ручной отсмотр с прямыми ссылками на канал и на отдельные посты которые как раз были как кандидаты на реакцию.

Можно сделать E2E дискавери, т.е. дать задачу Codex проводить дискавери, смотреть результаты и по результатам докручивать механизм дискавери, чтобы в итоге отладить его, т.е. отладку процесса переложить на Codex

### Resolution

Resolved by the 2026-06-29 Discovery MVP addendum and `mvp-discovery.md`: Discovery is the MVP, production rollout starts in shadow/manual-review mode, direct links to candidate surfaces/posts are required, and live E2E/Codex calibration is part of rollout.

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-28T09:52:43+00:00

Status: resolved / archived 2026-06-29

### User notes

Я думаю дискавери можно вынести как раз в MVP, проанализируй как это сделать. Т.е. основной механизм реализовать, но запустить в продакшн именно дискавери чтобы он периодически запускался, например раз в день и добавлял кандидатов, сначала ссылками на ручной отсмотр с прямыми ссылками на канал и на отдельные посты которые как раз были как кандидаты на реакцию.

Можно сделать E2E дискавери, т.е. дать задачу Codex проводить дискавери, смотреть результаты и по результатам докручивать механизм дискавери, чтобы в итоге отладить его, т.е. отладку процесса переложить на Codex

Я думаю дискавери можно вынести как раз в MVP, проанализируй как это сделать. Т.е. основной механизм реализовать, но запустить в продакшн именно дискавери чтобы он периодически запускался, например раз в день и добавлял кандидатов, сначала ссылками на ручной отсмотр с прямыми ссылками на канал и на отдельные посты которые как раз были как кандидаты на реакцию.

Можно сделать E2E дискавери, т.е. дать задачу Codex проводить дискавери, смотреть результаты и по результатам докручивать механизм дискавери, чтобы в итоге отладить его, т.е. отладку процесса переложить на Codex

### Resolution

Resolved by the 2026-06-29 Discovery MVP addendum and `mvp-discovery.md`: Discovery is the MVP, production rollout starts in shadow/manual-review mode, direct links to candidate surfaces/posts are required, and live E2E/Codex calibration is part of rollout.

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
