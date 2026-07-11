# Personal email announcements

> Status: **design v2**. The earlier YDB-owned design branch is superseded by the accepted Supabase profile/control-plane architecture. No production generator or recommendation send is enabled.

## Product contract

An explicitly subscribed user receives a personal recommendation email containing **exactly three events**. One of the three may be the visual hero. The email links to an already published personal page with a larger ranked selection and feedback actions.

This is recommendation/marketing-like mail, not a transactional reminder. It requires separate opt-in, unsubscribe, frequency/fatigue policy and deliverability evidence.

## Identity and consent

- Yandex-authenticated or verified-email Supabase identity for the first canary.
- Recommendation-email consent is purpose-specific and independently revocable.
- Calendar/favorite and transactional-email consent do not authorize this stream.
- Anonymous email subscription is a later phase after identity/abuse/linking gates.

## Data and artifact ownership

- Supabase owns subscription, consent evidence, verified email, suppression, due state, recommendation issue/cards, personal-page token hash/metadata, feedback current state, email outbox and delivery control.
- Kaggle receives a sanitized immutable profile/event snapshot and returns recommendation artifacts/evidence.
- Object Storage/CDN owns rendered personal HTML/JSON.
- YDB receives de-identified analytics/history asynchronously; it does not own profile, subscription, outbox or send eligibility.
- Fly SQLite remains canonical for event facts/lifecycle.

See [personalization data ownership](../../architecture/personalization-data-ownership.md).

## Required pipeline

1. Select due, verified, consented and non-suppressed subscriptions.
2. Materialize a sanitized profile and active/future event snapshot.
3. Generate ranked/diverse/fresh recommendations offline; fail closed on stale/cancelled events.
4. Persist one recommendation issue and exactly three email cards in Supabase.
5. Publish the larger personal page artifact.
6. Validate the artifact and token metadata.
7. Enqueue email only after publication succeeds.
8. Recheck consent/suppression/freshness immediately before send.
9. Record provider result in Supabase and analytics projection in YDB.
10. Accept feedback through a rate-limited same-origin endpoint.

## Personal page security

- high-entropy expiring/revocable token;
- only keyed token hash in Supabase;
- `noindex`, restrictive referrer policy and no token in outbound destination;
- no email, raw profile, hidden vectors/tags or internal scores in HTML/JSON;
- separate page, click, unsubscribe and feedback tokens;
- static content remains readable if analytics/feedback calls fail.

## Release phases

1. Docs/ADR and product decisions.
2. Supabase schema/RLS and verified identity/consent.
3. Dry-run generation and internal personal pages.
4. Postbox seed-list canary with exactly three events.
5. Auth-only limited beta.
6. Anonymous subscription and learning loop only after abuse/quality evidence.

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Unsigned personalization](../unsigned-personalization/README.md)
- [Email delivery](../../operations/email-delivery.md)
- [Global product decisions](../static-personal-announcements/global-product-decisions.md)
