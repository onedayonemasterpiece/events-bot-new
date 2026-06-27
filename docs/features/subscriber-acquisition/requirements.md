# Requirements: Subscriber Acquisition

Status: reconciled draft

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

## Open questions

- Which surfaces should be MVP first: Telegram chats/comment threads, VK community comments, or both?
- Should MVP responses be operator-reviewed before posting, or can any subset be published automatically after confidence/rate-limit gates?
- What is the initial source allowlist and who approves additions/removals?
- What exact rate limits and cooldown windows should apply per community, per thread, and globally?
- Which event-link surface is canonical when multiple links exist for the same event and platform context?
- What safety policy should govern sticker generation before it becomes an implementation task?

## Decisions log

- 2026-06-27: Reconciled six Telegram voice notes into this canonical requirements draft.
- 2026-06-27: Preserved anti-spam as a primary product constraint: sparse, precise recommendations across relevant communities are preferred over repeated posting in one community.
- 2026-06-27: Treated sticker/sticker-pack generation as a related follow-up idea rather than part of the first implementation scope.
- 2026-06-27: Reconciled the 21:10 voice-note intake as an operational request to review/check requirements; no new product requirement delta was found.

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
