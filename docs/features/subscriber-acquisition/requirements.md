# Requirements: Subscriber Acquisition

Status: reconciled draft

## Product intent

- Grow the subscriber/audience base for the event announcement system through social acquisition workflows in Telegram and VK.
- Acquisition actions should happen inside relevant social spaces — communities, chats, channel comment threads, VK communities, personal VK walls with meaningful public comment activity, and similar discussion surfaces — where people are already discussing what to visit.
- The feature should help recommend events in a way that feels like a useful human reply, not like mass advertising.
- Links in recommendations should point to the appropriate canonical public surface for the platform: Telegram announcement/channel links for Telegram contexts, VK community/post links for VK contexts, and static event-site links when they are the best public destination.
- The operator needs visibility and control over which social spaces are monitored for acquisition, including discovery, approval/exclusion, monitoring status, and key effectiveness/resource statistics.
- Sticker or sticker-pack generation tied to notable events is a separate acquisition/engagement idea that can be explored as a follow-up, especially for major events where a sticker reply may be more natural than text.

## Requirements

### Social-source discovery and monitoring

- The system should support collecting or configuring candidate social spaces where subscriber acquisition can happen: Telegram chats, Telegram channel comment threads, VK communities, personal VK walls, and similar discussion surfaces.
- The system should analyze messages/comments in those candidate spaces to find moments where an event recommendation is contextually useful.
- Detection should combine LLM analysis with narrower deterministic signals/keywords; broad semantic classification of “this person may want an event recommendation” must remain LLM-first.
- Relevant signals include questions about upcoming events, requests for recommendations, discussions where a specific type of event would be useful, and similar intent-bearing comments.
- VK personal-wall monitoring should be supported for public walls that have enough relevant comment activity to justify monitoring and should follow the same review, rate-limit, and anti-spam constraints as community/comment-thread monitoring.
- The feature should include a Discovery mode that periodically searches from existing monitored/known spaces for links to other relevant VK communities, personal VK walls, Telegram channels, Telegram chats, and channel-linked comment spaces.
- Discovery mode should evaluate whether a newly found space is suitable for ongoing acquisition monitoring and record a rationale/decision signal for adding, rejecting, or reviewing the space.
- Discovery crawling must be resource-bounded: it should have explicit limits for depth, number of pages/sources visited, messages/comments inspected, LLM calls/tokens, runtime, and duplicate revisits, so it does not spend unbounded resources following links.
- Discovery should prefer broad but bounded exploration across promising sources over deep unbounded crawling of a single source.

### Source inventory and operator management

- The product should provide a dedicated operator menu for acquisition monitoring sources.
- The menu should show all acquisition-monitoring sources across supported platforms/source types: Telegram chats, Telegram channels/comment threads, VK communities, personal VK walls, and future similar surfaces.
- From the menu, an operator should be able to add sources, disable/remove sources from monitoring, and explicitly exclude/reject sources so Discovery does not keep re-suggesting the same unsuitable place.
- The menu should expose each source’s monitoring state, such as discovered/candidate, approved/enabled, disabled, excluded/rejected, and last checked/last monitored timestamps.
- The menu should expose key per-source statistics useful for acquisition decisions, such as scanned messages/comments, detected recommendation opportunities, reviewed/posted replies, recent activity, errors/blocks, and resource usage or LLM-call cost indicators where available.
- Source records should keep enough metadata to support auditability: platform, source type, URL/username/ID, discovery path or manual origin, rationale for inclusion/exclusion, and source links back to the original discovery evidence when available.

### Event recommendation generation

- For a detected recommendation opportunity, the system should select one or more likely suitable events from the announcement/event corpus.
- Event selection should consider the message context, likely user intent, platform context, and available event metadata.
- The generated response should be written as a natural human-like reply, not as an obvious bot advertisement.
- The reply may mention concrete event names and, when appropriate, include links to those events.
- The reply format should support replying directly to the source message/comment when the platform allows it.
- Analysis and reply generation should use the project’s Google AI / Gemma-capable gateway with rate-limit accounting rather than direct unmanaged provider calls.
- The LLM flow should support a staged decision path: opportunity detection and event selection can be separated from final reply writing/review when that is safer or more measurable than a single one-shot response.
- The primary analysis model should be configurable; Gemma 4-class models are the expected main candidates for evaluation, with a lighter Google model fallback/low-cost lane available when the gateway and quality gates allow it.
- LLM outputs used for public replies should have structured logging of the model/lane used, decision rationale, selected event IDs/links, and failure/fallback reason.

### Link targets and platform routing

- In Telegram contexts, recommendation links should prefer Telegram announcement/channel links when available and appropriate.
- In VK contexts, recommendation links should prefer VK community/post links when available and appropriate.
- If neither platform-native link is suitable, the response may link to the static event site / personalization site surface when it exists and is a better destination.
- Link selection should avoid mixing platform destinations in a confusing way; the chosen link should match the user’s current context unless there is a clear product reason to use a different surface.

### Anti-spam and safety constraints

- The feature must avoid spammy behavior: do not post frequently, do not flood a single community, and do not repeatedly answer similar prompts in the same place.
- Recommendations should be precise and sparse — “surgical” interventions in the best-fit conversations are preferred over broad campaigns in one community.
- It is preferable to make a small number of relevant recommendations across multiple communities than to mass-post in one community.
- The system should maintain rate limits, cooldowns, deduplication, and per-community/per-thread/per-wall guardrails before any automatic posting is enabled.
- Automatic posting should be conservative by default; review/shadow mode is expected before broad unattended operation.
- Discovery and monitoring must respect explicit exclusions and should not re-add excluded sources without an operator action or a clearly recorded re-review policy.

### Technical recommendations from reconciliation

- Technical default: implement Subscriber Acquisition inside the existing events-bot/admin surface first, not as a separate small bot, unless later scale/isolation evidence proves a split is needed.
- Technical default: use the existing production database as the source of truth for acquisition source inventory, with acquisition-specific tables/states as needed; do not start with a separate database unless load, retention, or isolation requirements become concrete.
- Technical default: reuse the existing Google AI gateway (`GoogleAIClient`) for model selection, rate-limit accounting, fallback chains, and logging.
- Technical default: prefer a staged LLM pipeline for public-reply candidates — classify opportunity/select event/write reply/review — and only collapse to a single request if benchmarks show no quality/safety regression.
- Technical default: implement Discovery as a scheduled bounded crawl/research job with explicit budgets and persisted cursor/state, not as recursive unbounded real-time crawling.

### Sticker-based follow-up idea

- The product may support generating event-related stickers or sticker packs as a separate engagement/acquisition mechanic.
- Sticker generation should be treated as a follow-up feature until its content rules, brand safety, publication flow, and platform constraints are specified.
- For major events, the system may eventually answer or promote via a sticker when a sticker is more natural than a text recommendation.

## Open questions

- Which surfaces should be MVP first: Telegram chats/comment threads, VK community comments, personal VK walls, or a smaller subset?
- Should MVP responses be operator-reviewed before posting, or can any subset be published automatically after confidence/rate-limit gates?
- What is the initial source allowlist and who approves additions/removals?
- Should high-confidence Discovery candidates be auto-enabled for monitoring, or should Discovery only create operator-review candidates?
- What exact rate limits and cooldown windows should apply per community, per thread, per personal wall, and globally?
- Which event-link surface is canonical when multiple links exist for the same event and platform context?
- Which source statistics are mandatory in the first operator-menu version versus useful later?
- What safety policy should govern sticker generation before it becomes an implementation task?

## Decisions log

- 2026-06-27: Reconciled six Telegram voice notes into this canonical requirements draft.
- 2026-06-27: Preserved anti-spam as a primary product constraint: sparse, precise recommendations across relevant communities are preferred over repeated posting in one community.
- 2026-06-27: Treated sticker/sticker-pack generation as a related follow-up idea rather than part of the first implementation scope.
- 2026-06-27: Reconciled the 21:10 voice-note intake as an operational request to review/check requirements; no new product requirement delta was found.
- 2026-06-28: Integrated operator source inventory/menu, VK personal walls, bounded daily Discovery, Google AI/Gemma gateway usage, and technical defaults for in-bot/in-DB implementation from the 07:36 voice-note intake.
- 2026-06-28: Classified separate bot/database and one-shot-vs-staged LLM flow as technical implementation choices, not product conflicts; current default is existing bot + existing DB + staged LLM pipeline unless later evidence changes it.

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

Status: resolved / archived 2026-06-28

### Resolution

Compared with the canonical requirements above. No product conflict was found. The intake was integrated into the canonical sections as: source inventory/operator menu, VK personal-wall monitoring, bounded periodic Discovery mode, Google AI/Gemma gateway usage for analysis/reply decisions, staged LLM flow support, and technical defaults for using the existing bot/database first.

Separate bot/database and one-shot-vs-staged LLM execution were classified as technical implementation choices. Existing project patterns already provide an in-bot admin UI, production SQLite source tables (`telegram_source`, `vk_source` with `owner_type`), and the Google AI gateway with Gemma 4 model normalization/rate-limit/fallback support, so the requirements record the current technical default instead of asking the user to choose architecture prematurely.

### Source files

- [source/voice_AgAD3qEAAkoWAAFK.oga](source/voice_AgAD3qEAAkoWAAFK.oga)
- [source/voice_AgAD4KEAAkoWAAFK.oga](source/voice_AgAD4KEAAkoWAAFK.oga)
- [source/voice_AgAD6KEAAkoWAAFK.oga](source/voice_AgAD6KEAAkoWAAFK.oga)
- [source/voice_AgAD7KEAAkoWAAFK.oga](source/voice_AgAD7KEAAkoWAAFK.oga)
- [source/voice_AgAD8KEAAkoWAAFK.oga](source/voice_AgAD8KEAAkoWAAFK.oga)
- [source/voice_AgAD8qEAAkoWAAFK.oga](source/voice_AgAD8qEAAkoWAAFK.oga)

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
