# Requirements: Recap

Status: reconciled draft

## Product intent

- Recap is a post-event product surface that aggregates evidence of how an event actually went: reports, photos, videos, reactions, outcomes, links to original announcements, and links to source posts.
- Recap should create social proof for future events: “this already happened and was worth attention; similar or recurring events may appear again”.
- The primary user-facing surface is a separate Recap Калининград channel with short, readable posts about completed events.
- Recap should also support long-form pages for multi-source or high-value events and reusable links from future announcements, event pages, Telegram posts, VK posts, promo campaigns, and series pages.
- Recap must not be a second event-announcement pipeline. It is a post-event layer over existing event data, Smart Update facts, source monitoring, media, metrics, and series/future-event links.

## Requirements

### Scope and non-goals

- The system must automatically find and process post-event materials: reports, photo reports, videos, audience reactions, comments, reposts, UGC, partner posts, and media coverage.
- The system must connect post-event evidence to existing events, archived event snapshots, source posts, and series when applicable.
- If a post-event report describes an event absent from the event database, the system must create a `missed_event_signal` or other discovery signal, not create a backdated public event card as a recap side effect.
- MVP must not require manual premoderation for every recap. Instead it must use automated gates, post-publication diagnostics, suppress/rebuild commands, and correction signals.
- MVP must not automatically reply in external chats/comments as part of recap discovery; that belongs to Subscriber Acquisition.
- MVP must not publish weak recap content into the main announcement channel by default.
- MVP must not store unlimited raw social data without a retention policy.

### Inputs and discovery

- Recap discovery means finding post-event evidence and sources, not finding places where recap should be promoted.
- Candidate sources include existing organizers/venues, participants, partner pages, city and district publics, educational institutions, parent/student communities, photo/video communities, administrations, media/micro-media, comments under announcements, and repost chains.
- Existing Telegram Monitoring reports classified as `skipped_non_event:completed_event_report` should become recap candidates in a separate branch instead of being treated as useless noise.
- VK Auto Queue / `vk_inbox` should support a parallel recap-candidate path for reports, results, photo reports, and similar completed-event posts.
- Recap candidate detection should look for post-event signals such as “как прошло”, “состоялось”, “прошёл/прошла/прошло”, “фотоотчёт”, “итоги”, “делимся кадрами”, “благодарим участников”, and equivalent source-grounded patterns.
- New recap sources may be added automatically only through trust ramp-up: low-frequency monitoring first, stricter publication gates for new sources, promotion after successful matches, and automatic downranking/blocking on errors.

### Event matching and retention

- Each published recap must link to `event.id` or `event_archive_snapshot.id`, source posts, and `series.id` if applicable.
- Recap must not confidently describe a specific event unless event matching succeeded with sufficient confidence.
- If matching is uncertain, allowed outcomes are `evidence_only`, `missed_event_signal`, `unmatched_recap_digest`, or no publication.
- Event matching must use a deterministic shortlist before LLM adjudication: event/report date proximity, venue/address aliases, source/organizer, title, festival/series, source links, media/poster hashes where available, topic, and named entities.
- The LLM matching stage must choose only from the shortlist and must be allowed to return `no_match`.
- Recap needs durable event anchors after normal event expiry/deletion. Technically recommended default: add `event_archive_snapshot` as mandatory insurance; soft-delete/archived event statuses may be added if compatible with the existing event lifecycle.
- Archived anchors must retain minimal public facts needed for recap/future links: stable uid, title, date/end date, time if available, city, location, organizer, festival/series, canonical source URL, public URL, and facts digest.

### Fact-first and LLM processing

- Recap must reuse the existing Smart Update fact-first principle: public recap text is generated from extracted, source-grounded facts rather than directly rewriting raw source text.
- Recap must add post-event fact buckets/stages for outcomes, audience reaction, media observations, repeatability, and promo/future-event context.
- Recap must include an emotion/atmosphere extraction stage because recap value depends on how the event was perceived, not only what happened.
- Emotion and atmosphere extraction must be source-grounded: every high-confidence emotional conclusion needs evidence from source text, reactions, metrics, or media observations.
- The writer must not invent numbers, mass reactions, “everyone loved it”, future recurrence, or superlatives without evidence.
- Recap semantic analysis must remain LLM-first; deterministic rules are allowed for routing, safety, shortlist, validation, and guardrails but must not replace broad semantic decisions.
- New recap LLM stages should go through the project LLM Gateway with scoped consumer routing, fail-fast rate-limit behavior, structured JSON output, and logged `requested_model` / `provider_model` / `invoked_model`.
- Technically recommended default model split: Gemma 4 26B A4B for structured classification/matching/fact-emotion/scoring stages; Gemini Lite or the configured writer lane for final copy if testing shows better style/stability.

### Scoring and routing

- Recap selection must be score-based; not every event deserves a recap.
- The system must calculate `recap_score` for “should this material become recap?” and `event_significance_score` for “how broadly important/useful is the event?”. These scores are related but distinct.
- Scoring must consider event match confidence, source trust, media quality, emotional tone strength, event significance, audience segment fit, repeatability, future-event link value, promo fit, source discovery value, and risk.
- Post metrics must be interpreted relative to the source baseline/median, not only absolute views/likes/reposts/comments.
- Segment scoring should support at least: youth, families, children, seniors, tourists, culture lovers, creative community, education audience, local history audience, nightlife audience, sports/outdoor audience, civic audience, and general city audience.
- Each high segment score must include evidence, not just a number.
- Low-confidence event matches must block `standard_recap`, `top_recap`, and strong future-oriented CTA regardless of total score.
- Route decisions should include at least: ignore/raw log, evidence only, micro recap, standard recap, media recap, strong video recap, multi-source recap plus page, top recap, promo small recap, digest, and archive-only/noindex page.

### Publication surfaces

- MVP should have a separate recap channel. Recommended starting quota: 2–5 recap posts per day.
- Recap-channel posts should be concise enough for a feed, with a short description of the event, selected highlights, media when available, source/announcement links where useful, and a cautious future-oriented CTA.
- If a recap has many evidence posts, the post should surface only the most interesting 1–3 highlights and link to a fuller page with all relevant sources/details.
- If there is only one strong source, a micro/standard post may be enough; if there are multiple independent sources or many links, create a page/fallback page and keep the channel post short.
- Main announcement channel recap publication must be limited and gated: 0–1/day, 2–3/week, or weekly digest, only for high-score/high-repeatability/high-media/high-future-value cases.
- VK publication should support standard wall posts, carousel/album-style media recap, and separate video posts for strong videos.
- If a recap has a strong video, the video may be published as a separate post when scoring and dedup rules allow it.
- Media should not be hidden only behind source links when there are suitable assets and rights/ownership allow display. Long pages may embed selected media inline as an editorial/blog-style recap while preserving source links.
- Telegram output should support media groups for recap media; VK output should support carousel/album-like layouts where appropriate.

### Site, Telegraph, SEO, and link policy

- Technically recommended default: use the project site as the long-term canonical carrier for valuable recap pages; keep Telegraph as MVP/fallback/compatibility for thin, list-like, or temporary recap pages.
- Recommended site structure: `/recap/` archive, date archives such as `/recap/2026/06/`, individual `/recap/<slug>/` pages, `/recap/archive/<slug>/` for thin/noindex pages, and `/recap/series/<series-slug>/` for recurring formats.
- High-quality editorial recap pages and series pages may be indexable when they have enough original editorial value, structured facts, media, and future-event value.
- Thin/list-like pages whose main value is storing many external links should be `noindex` or Telegraph/fallback, not automatically indexable site content.
- External links are not by themselves a reason to avoid the site; index/noindex should depend on editorial value, duplication/thinness, and quality. UGC/social links should use a trust-based link policy such as `rel="ugc nofollow"` where appropriate.
- Recap pages should preserve links to source posts and original announcements, and future announcements/pages/posts should be able to link back to previous recaps for recurring or similar events.
- Public domain/URL for static recap pages remains a product/ops decision.

### Future-event reuse and series

- Recap links should be reusable in future announcements, event pages, Telegram posts, VK posts, and promo materials when an event is annual, recurring, or similar to a previous event.
- The system should detect and store recurring formats/series such as annual festivals, repeated markets, lecture cycles, museum programs, clubs, workshops, film screenings, and similar repeatable event patterns.
- Future event content may include “как это проходило раньше” links to one or several past recaps when evidence supports the connection.
- Recap should support series pages for recurring formats and measure CTR from future announcements to past recap pages.

### Safety, deduplication, and correction loops

- Default dedup rule: one event should have one primary recap.
- Exceptions may include multi-day festivals, a later strong-video post, weekly digests linking to an existing recap, and page updates with new sources without another channel post.
- Pre-publication automated guards must cover match confidence, risk score, source trust, duplicates, sensitive/personal data, media ownership/source policy, hallucination/coverage checks, and CTA repeatability evidence.
- Post-publication monitoring should track deleted/edited source posts, reactions/complaints, bad engagement anomalies, broken links, critical flags, and new sources for page rebuilds.
- Admin correction commands should support suppress, rebuild, source block/trust adjustment, and link-fix flows; each correction should become an evaluation/training/regression signal.

### Data model and operational architecture

- MVP data model should include `recap_candidate_post`, `recap`, `recap_event_link`, `recap_source_candidate`, `missed_event_signal`, and `event_archive_snapshot` or an equivalent retention anchor.
- Recap records should store stable recap uid, route/status, title/lead/body/CTA, emotional tone, audience segment scores, event significance, recap/risk scores, source post IDs, page/Telegraph/TG/VK URLs, noindex flag, and publication timestamps.
- Recap source candidates should store platform/source key, source URL/type, discovery reason, source discovery score, post-event frequency, media quality, trust, match success rate, evidence, status, and last seen time.
- Recap processing should run as a separate queue/batch that can be executed in Kaggle CPU notebooks, with deterministic stages, LLM API calls through the gateway, optional Astro build, object-storage upload, and publish/report artifacts.
- Recap Kaggle runs must use the Kaggle Status Framework with phase events and alive progress, not opaque long-running jobs.
- Static page publication should use a least-privilege Yandex Object Storage service account/prefix for recap build/upload if implemented through Kaggle/Astro.
- Page build failure must not block recap-channel publication; high-score recaps can use Telegraph fallback and the build should be retriable.

### MVP phases and acceptance criteria

1. **Phase 0 — schema and retention foundation**: create recap tables/anchors so post-event reports can be stored without event cards and recap links survive event archival/deletion.
2. **Phase 1 — candidate detection and matching**: route Telegram/VK completed-event reports into recap candidates, produce deterministic shortlist, LLM adjudication, and `missed_event_signal` for unmatched reports; run dry reports before publication.
3. **Phase 2 — facts, emotions, scoring**: reuse Smart Update fact extraction, add emotion/media/metrics/significance scoring, explain route decisions, and keep promo small recap separate from editorial recap.
4. **Phase 3 — auto publication to recap channel**: publish 2–5/day automatically under quotas, idempotency, suppress/retract, post-publication monitoring, source/event links, CTA, and dedup gates.
5. **Phase 4 — static pages plus Telegraph fallback**: create page payloads, build/upload pages, route index/noindex, return URL manifests, and keep channel publication independent of page build failures.
6. **Phase 5 — source discovery MVP**: discover new post-event sources, start low-frequency scanning, escalate trusted sources, and use missed-event feedback to improve monitoring.
7. **Phase 6 — future event reuse**: attach recap to future events/series/pages/posts and measure social-proof CTR.

## Technical recommendations identified during reconciliation

- Use project site pages as canonical long-term pages and Telegraph as fallback/compatibility, because the project already has site/static-page plans and Telegraph should not become the only durable carrier for quality recap content.
- Use `/recap/` paths under the existing site rather than a separate domain by default; this keeps brand/equity together unless the product later chooses a separate recap brand.
- Treat external social links with trust-based `rel`/index policy; do not route the whole feature away from the site merely because some pages contain many external links.
- Reuse Smart Update fact-first extraction and LLM Gateway instead of creating a second independent text pipeline.
- Reuse Post Metrics median/baseline logic for recap scoring; extend metrics consumers rather than adding a separate popularity subsystem.
- Route Telegram/VK completed-event reports into a separate recap branch, preserving the current rule that reports must not create normal event cards.
- Implement archival anchors (`event_archive_snapshot`) before broad publication so recap/event links do not break when old events are removed from the active event surface.

## Open questions

- What exact public channel(s) should be created for MVP: Telegram-only recap channel, VK too from day one, or both with different quotas?
- Should the recap surface have its own public brand/name or stay fully under the main афиша/«Полюбить Калининград» brand?
- What initial seed sources should be used for recap source discovery?
- Which platforms should MVP discovery prioritize first: Telegram, VK, or both?
- What media rights policy should apply to third-party photos/videos: embed/link only, repost, thumbnail, storage copy, or case-by-case trust policy?
- How long may raw source text be retained for published recaps versus rejected/noise candidates?
- Which final writer lane wins after tests: Gemini Lite, Gemma writer lane, or another configured model?
- Is a dedicated `/recap` admin command/report needed in the bot MVP?
- What public domain/base URL should static recap pages use?
- How should multiple source types be combined when one source has photos, another video, and another text: one editorial page, separate media blocks, or route-specific variants?

## Decisions log

- 2026-06-28: Reconciled initial voice/text intake and agent analysis into this canonical draft.
- 2026-06-28: Decided, as a technical default, that high-value recap should live on site pages with Telegraph as fallback/compatibility.
- 2026-06-28: Decided, as a technical default, to reuse Smart Update fact-first, LLM Gateway, Post Metrics, Telegram Monitoring, VK Auto Queue, and Kaggle Status Framework rather than build separate duplicate subsystems.
- 2026-06-28: Classified source-discovery vs subscriber-acquisition boundary: recap discovery collects post-event evidence and does not post outward in MVP.
- 2026-06-28: Classified completed-event reports as valuable recap input while preserving the existing event-import invariant that they must not create normal event cards.

## Archived intake 2026-06-28T08:24:35+00:00

Status: resolved / archived 2026-06-28

Resolution: Integrated the user text note and seven noisy voice transcripts into Product intent, Requirements, Technical recommendations, Open questions, and Decisions log. No contradiction with previous requirements was found because the canonical sections were still empty. The raw audio files remain linked below for auditability.

Source files:

- [source/voice_AgADNqIAAkoWAAFK.oga](source/voice_AgADNqIAAkoWAAFK.oga)
- [source/voice_AgADN6IAAkoWAAFK.oga](source/voice_AgADN6IAAkoWAAFK.oga)
- [source/voice_AgADPKIAAkoWAAFK.oga](source/voice_AgADPKIAAkoWAAFK.oga)
- [source/voice_AgADPaIAAkoWAAFK.oga](source/voice_AgADPaIAAkoWAAFK.oga)
- [source/voice_AgADPqIAAkoWAAFK.oga](source/voice_AgADPqIAAkoWAAFK.oga)
- [source/voice_AgADQqIAAkoWAAFK.oga](source/voice_AgADQqIAAkoWAAFK.oga)
- [source/voice_AgADRqIAAkoWAAFK.oga](source/voice_AgADRqIAAkoWAAFK.oga)

Reconciliation checklist:

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Archived intake 2026-06-28T10:17:59+00:00

Status: resolved / archived 2026-06-28

Resolution: Integrated the attached agent analysis as product/technical requirements and kept the full analysis document in `source/` for traceability.

Source files:

- [source/recap_requirements_and_analysis.md](source/recap_requirements_and_analysis.md)

Reconciliation checklist:

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.

## Intake 2026-06-29T10:43:01+00:00

Status: pending reconciliation

### User notes

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Для рекэпов событий, где много разных источников, ну допустим, несколько разных блогеров, несколько разных людей, несколько разных организаций отписались о событии, можно ввести полезную категоризацию. Например, самый эмоциональный отчет, самый полный отчет, самый профессиональный, самый необычный от сердца и так далее. Эти категории, конечно же, нужно попробовать разработать и классифицировать из будущих найденных рекэпов.

### Голосовое дополнение к требованиям

Ниже автоматическая расшифровка голосового сообщения. Распознавание может быть неточным: при сверке требований восстанови вероятный контекст, а сомнительные места вынеси в вопросы пользователю.

Собственно, хорошей темой было бы начать именно с рекэпов событий, у которых много разных людей, которые рассказали, отчитались об этом событии.

### Source files

- [source/voice_msg_1131.oga](source/voice_msg_1131.oga)
- [source/voice_msg_1132.oga](source/voice_msg_1132.oga)

### Reconciliation checklist

- [ ] Compare with previous requirements.
- [ ] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [ ] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [ ] Move resolved statements into the canonical sections above and remove/close this pending intake.
