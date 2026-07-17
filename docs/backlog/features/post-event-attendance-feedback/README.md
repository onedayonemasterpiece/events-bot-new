# Post-event attendance feedback for recurring events

> Status: **post-release product idea / not implemented / not a public-presentation GO blocker**. Earliest planning stage: after the public release, launch hypercare and the core D-1 reminder flow are stable.

## Idea

If a person saved an event to the calendar/favorites and the attended occurrence has finished, the service may send one follow-up email asking for an attendance impression:

- **green — «Понравилось»**;
- **yellow — «Нормально / смешанные впечатления»**;
- **red — «Не понравилось»**;
- reply to the email with free text about what was good or bad;
- optionally provide a URL to the person’s own public review.

Color is never the only label: every smiley/action needs text and an accessible name. An email reply or submitted URL is private by default and cannot be published automatically merely because it was sent.

## Eligibility boundary

This follow-up is allowed only for a **serial/recurring event family** where the feedback can help with another current or future occurrence, edition or program. Examples:

- a festival that is still running or has recurring editions;
- a regular concert, excursion, lecture, performance or workshop series;
- repeated screenings/sessions or the same program at later dates;
- an annual event with a confidently established cross-year lineage.

A confidently unique one-off event is excluded. An uncertain classification also fails closed and sends nothing.

For a multi-day festival or several saved sessions in one edition, the service must not send a request after every calendar item. The future product contract should select one meaningful completed attendance unit and enforce at most one request per `user + series/edition + feedback window`.

## Determining unique vs recurring

Series classification is an offline, LLM-first catalog-analysis task, not a broad title regex and not a browser decision. It analyzes current and historical canonical events and produces a versioned `series_identity` with confidence and evidence.

Candidate evidence includes:

- explicit festival/program/series identity;
- organizer and source continuity;
- normalized program concept rather than date-bearing title equality;
- venue/city continuity or an explainable touring pattern;
- past and future occurrence dates/intervals;
- canonical merge/source history and cross-year editions;
- materially different artist/program/format signals that disprove a match.

Deterministic processing may normalize titles/dates, build candidate graphs and enforce safety thresholds. A strict LLM pass decides semantic family membership over bounded candidates. Low confidence, conflicting evidence, provider/schema failure or no future/actionable family context remains unpublished/ineligible and retries offline; it never weakens the mail gate.

The classifier must distinguish:

- another occurrence of the same program;
- another edition in the same series/festival;
- merely similar events from the same venue or organizer;
- a copied/generic title that does not establish lineage.

## Trigger and email safety

Saving to the calendar still does **not** silently grant email permission. A follow-up requires a verified email, an approved consent purpose covering post-event feedback, no suppression/unsubscribe and a server-owned saved-event relation.

Before enqueue the server revalidates:

1. the canonical occurrence actually finished and was not cancelled/merged away;
2. the user saved it before completion and has not already reviewed/opted out;
3. the series classifier is high-confidence and another useful family context exists;
4. identity, consent, suppression, frequency cap and quiet-hours policy allow mail;
5. the idempotency key has not already been claimed.

Do not infer completion from event category or an invented duration. Unknown/untrusted end time needs a separately approved conservative rule; until then it sends nothing.

Each smiley action uses a signed, bounded-lifetime, single-purpose token and records one idempotent rating. Email retries and repeated clicks cannot create several reviews. A changed rating is an explicit later action, not duplicate ingestion.

## Free-text reply and public-review link

- Email replies are correlated through the retained inbound-mail pipeline using provider/message evidence or an opaque reply token; raw reply text is not placed in analytics logs.
- Free text requires PII/secrets redaction, spam/abuse filtering, retention and deletion rules before it may be used for product learning.
- A submitted public-review URL requires scheme/domain/redirect validation and a separate explicit permission decision before the service surfaces or quotes it.
- The first safe version may keep replies and URLs private for analysis. Native public reviews, quoting text, displaying a link or attributing an author are separate product/legal decisions.
- The user must be able to withdraw a review/use permission without deleting the saved event or unrelated personalization.

This visitor feedback is distinct from pre-event [discussion signals](../../../features/event-comment-feedback/README.md), which aggregate public source comments and must not claim attendance.

## Data/use boundary

Attendance feedback is user-generated opinion, not a canonical event fact. It must not automatically rewrite event title/date/location/ticket data or silently become promotional copy.

Potential later uses, each behind its own acceptance policy:

- aggregate satisfaction for later occurrences in the same series;
- private recommendation/profile signal with explicit personalization consent;
- organizer/product-quality reporting;
- public aggregate such as a bounded distribution, only when minimum-count/privacy/anti-gaming gates pass;
- surfaced public-review links, only under the separate permission and moderation policy.

Green/yellow/red must not directly boost or suppress public ranking until bias, sample-size, abuse and dissatisfied-user visibility guardrails are defined.

## Acceptance scenarios for a future task

- recurring series with a later occurrence → exactly one eligible follow-up;
- unique one-off → no follow-up;
- ambiguous family match/provider failure → no follow-up;
- multi-day festival/multiple saved sessions → no email storm;
- cancelled/rescheduled/merged occurrence → correct defer/cancel behavior;
- unknown end → fail closed under the initial contract;
- missing consent/unverified email/suppression/unsubscribe → no send;
- green/yellow/red links → accessible, single-purpose and idempotent;
- email reply → correlated without public disclosure or PII in logs;
- public-review URL → validation plus explicit publication state;
- retry, double click, cross-device click and late provider callback → one logical review;
- opt-out/withdrawal → no further request and no accidental deletion of calendar state.

## Product decisions deferred until post-release discovery

- Exact delay after a trustworthy end and quiet-hour behavior.
- Whether existing `transactional_event` consent is sufficiently explicit or a separate `post_event_feedback` purpose is required.
- What counts as “another useful occurrence”: current festival day, future session, next edition or any high-confidence historical series.
- Whether free-text replies remain private, can be summarized, or may be published with separate consent.
- Whether “public review” means submitting an external URL, publishing a native review page, or both.
- Minimum aggregate count, retention, attribution/anonymity and anti-gaming policy.
- Whether and how this signal affects personalization, series pages or organizer reporting.

## Related documentation

- [Transactional event email notifications](../../../features/event-email-notifications/README.md)
- [Favorites/calendar](../../../features/event-favorites-calendar/README.md)
- [Email delivery and inbound handling](../../../operations/email-delivery.md)
- [Site identity](../../../features/site-user-identity/README.md)
- [Discussion signals](../../../features/event-comment-feedback/README.md)
- [Static release plan, post-presentation stage](../../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-8--после-публичной-презентации)
