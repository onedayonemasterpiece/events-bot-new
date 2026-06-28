# Requirements: Subscriber Acquisition

Status: reconciled draft

## Product intent

- Grow the subscriber/audience base for the event announcement system through social acquisition workflows in Telegram and VK.
- Acquisition actions should happen inside relevant social spaces — communities, chats, channel comment threads, VK communities, and public VK personal walls — where people are already discussing what to visit.
- The feature should help recommend events in a way that feels like a useful human reply, not like mass advertising.
- The MVP should be discovery-first: periodically find and rank promising acquisition sources/opportunities, then expose them for manual review before any unattended replies are enabled.
- Links in recommendations should point to the appropriate canonical public surface for the platform: Telegram announcement/channel links for Telegram contexts, VK community/post links for VK contexts, and static event-site links when they are the best public destination.
- Sticker or sticker-pack generation tied to notable events is a separate acquisition/engagement idea that can be explored as a follow-up, especially for major events where a sticker reply may be more natural than text.

## Requirements

### MVP scope: discovery-first acquisition

- Discovery mode is part of the MVP and should be the first production-visible acquisition workflow.
- The MVP production workflow should periodically run discovery, for example daily, over the configured seed sources and already-monitored social spaces.
- MVP discovery should add candidate sources/opportunities to a manual-review queue rather than automatically enabling permanent monitoring or posting unattended replies.
- Each discovery candidate shown to an operator should include direct links to the candidate channel/chat/community/wall and to the concrete post, message, or comment that made it a candidate for reaction or further monitoring.
- Discovery output should include the system’s recommendation (`add to monitoring` / `do not add` / `needs review`) with a short rationale and key evidence.
- The MVP should support an E2E/debug loop where Codex or an operator can run discovery, inspect the produced candidates, and tune the discovery mechanism based on observed results.

### Source registry and operator menu

- The system should provide a dedicated operator menu for acquisition sources.
- The menu should list monitored and candidate acquisition surfaces: Telegram chats, Telegram channels with comment threads, VK communities, public VK personal walls, and similar sources.
- Operators should be able to add sources, exclude/disable sources, and inspect the current monitoring/review status from this menu.
- The menu should show key per-source statistics, including at minimum last scan time, candidate/opportunity counts, accepted/rejected counts, recent errors, and enough activity metrics to judge whether the source is worth monitoring.
- Source records should preserve platform-specific identifiers and public links so operators can open the source and the evidence posts directly.

### Social-source discovery and monitoring

- The system should support collecting or configuring candidate social spaces where subscriber acquisition can happen: Telegram chats, Telegram channel comment threads, VK communities, public VK personal walls, and similar discussion surfaces.
- The system should analyze messages/comments in those candidate spaces to find moments where an event recommendation is contextually useful.
- Detection should combine LLM analysis with narrower deterministic signals/keywords; broad semantic classification of “this person may want an event recommendation” must remain LLM-first.
- Relevant signals include questions about upcoming events, requests for recommendations, discussions where a specific type of event would be useful, and similar intent-bearing comments.
- VK personal walls may be monitored only when they are public/accessible to the configured VK API credentials; replies on such walls should remain subject to the same relevance, safety, and rate-limit gates as community comments.

### Discovery graph traversal

- Discovery mode should crawl outward from the existing seed sources and monitored spaces to find links or mentions of other relevant sources: VK personal walls, VK communities, Telegram chats, Telegram channels, and Telegram channels with open comments.
- Discovery should evaluate whether each newly found source is suitable for ongoing monitoring for acquisition opportunities.
- Discovery should be able to look beyond a single direct link when useful, but traversal must be explicitly bounded by depth, breadth, runtime, LLM-call budget, and deduplication limits.
- Discovery should avoid repeatedly reprocessing the same source/post and should keep enough run history to make incremental daily runs efficient.

### Event recommendation generation

- For a detected recommendation opportunity, the system should select one or more likely suitable events from the announcement/event corpus.
- Event selection should consider the message context, likely user intent, platform context, and available event metadata.
- The generated response should be written as a natural human-like reply, not as an obvious bot advertisement.
- The reply may mention concrete event names and, when appropriate, include links to those events.
- The reply format should support replying directly to the source message/comment when the platform allows it.
- In the discovery-first MVP, reply text may be generated as an operator-review draft, but automatic posting is not part of the initial unattended workflow.

### LLM analysis and model routing

- Message/comment analysis, source suitability assessment, and reply-draft decisions should use the project’s Google AI gateway with centralized quota/rate-limit controls.
- Gemma 4 should be the primary model family for quality-critical source/opportunity analysis unless later benchmarks choose another configured Google model.
- Lower-cost/lite Google model fallbacks may be used for bounded low-risk triage or as an emergency fallback, but safety-critical decisions and final reply drafts should keep model identity and fallback status visible in logs/results.
- The system should support staged LLM calls when that improves quality or cost control: for example, separate source/opportunity classification from final reply drafting instead of forcing all decisions into one prompt.
- Discovery and recommendation outputs should record which model/fallback path was used so tuning and quota issues can be audited.

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
- Discovery itself must also be budgeted: daily runs should have explicit caps so they do not spend unbounded API, LLM, or operator-review resources.

### Technical recommendations / implementation defaults

- MVP should be implemented inside the existing events-bot operator/runtime surface rather than as a separate bot.
- MVP should use the existing primary application database with feature-scoped tables/fields for acquisition sources, discovery runs, candidates, opportunity evidence, and review decisions; a separate database should be reconsidered only if measured volume, retention, or operational isolation requirements outgrow the shared DB.
- Existing source concepts should be reused where practical: `telegram_source` for Telegram monitoring metadata and `vk_source.owner_type` for VK communities vs public personal walls; acquisition-specific state should not overload event-import fields when a separate review/candidate table is clearer.
- Discovery and later posting work should integrate with existing scheduler/outbox and LLM gateway patterns so retries, locks, rate limits, and audit logs are consistent with the rest of the project.

### Sticker-based follow-up idea

- The product may support generating event-related stickers or sticker packs as a separate engagement/acquisition mechanic.
- Sticker generation should be treated as a follow-up feature until its content rules, brand safety, publication flow, and platform constraints are specified.
- For major events, the system may eventually answer or promote via a sticker when a sticker is more natural than a text recommendation.

## Open questions

- What is the initial seed allowlist for MVP discovery and who approves additions/removals?
- What exact rate limits and cooldown windows should apply per community, per thread, and globally before any reply posting is enabled?
- Which event-link surface is canonical when multiple links exist for the same event and platform context?
- What review decision statuses and key statistics should be shown in the operator menu beyond the minimum listed above?
- What threshold moves a candidate source from manual-review discovery into permanent monitoring?
- When, if ever, should any subset of responses be published automatically after confidence/rate-limit gates?
- What safety policy should govern sticker generation before it becomes an implementation task?

## Decisions log

- 2026-06-27: Reconciled six Telegram voice notes into this canonical requirements draft.
- 2026-06-27: Preserved anti-spam as a primary product constraint: sparse, precise recommendations across relevant communities are preferred over repeated posting in one community.
- 2026-06-27: Treated sticker/sticker-pack generation as a related follow-up idea rather than part of the first implementation scope.
- 2026-06-27: Reconciled the 21:10 voice-note intake as an operational request to review/check requirements; no new product requirement delta was found.
- 2026-06-28: Promoted Discovery mode into the MVP, with the first production workflow producing manual-review candidates and evidence links instead of unattended replies.
- 2026-06-28: Technical default: build Subscriber Acquisition inside the existing bot/runtime and primary DB with feature-scoped state; do not split into a separate bot or DB for MVP unless measured scale requires it.
- 2026-06-28: Technical default: use the existing Google AI gateway/rate limiter for Gemma 4 analysis and keep model/fallback visibility in discovery and reply-draft outputs.

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

### User notes summary

Voice-note intake requested: a separate menu for monitored channels/chats/communities with add/exclude/statistics; analysis of whether the feature should live in the existing bot/DB or be split out; Google/Gemma model routing with quota controls and fallback; VK personal wall monitoring; daily Discovery mode that discovers other walls/communities/channels/comment-enabled spaces from existing sources; and explicit limits so Discovery does not crawl too deeply or waste resources.

### Resolution

Integrated into Product intent, MVP scope, Source registry and operator menu, Social-source discovery and monitoring, Discovery graph traversal, LLM analysis and model routing, Anti-spam and safety constraints, Technical recommendations / implementation defaults, Open questions, and Decisions log. No product conflict was found. The DB/bot split and model-call shape were classified as technical defaults from existing project patterns rather than user-choice questions.

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

## Archived intake 2026-06-28T09:40:15+00:00

Status: resolved / archived 2026-06-28

### User notes summary

Text intake proposed putting Discovery into the MVP, running it periodically in production (for example daily), adding candidates with direct links for manual review, and using an E2E/Codex loop to inspect Discovery results and tune the mechanism.

### Resolution

Integrated into MVP scope, Discovery graph traversal, and Decisions log. This narrows the previous open MVP-surface question by making Discovery the first MVP workflow; direct unattended reply posting remains outside the initial production workflow. No conflict was found.

### Source files

- Text intake in this requirements document.

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-28T09:52:43+00:00

Status: resolved / archived 2026-06-28

### User notes summary

Text intake duplicated the 2026-06-28T09:40 Discovery MVP / daily production / manual-review candidates / E2E-Codex tuning request.

### Resolution

Deduplicated against the 2026-06-28T09:40 intake and treated as confirmation of the same Discovery MVP requirement. No additional product delta and no conflict were found.

### Source files

- Text intake in this requirements document.

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
