# Personal email announcements

> Status: **design v3 / accepted provider routing**. The earlier YDB-owned design branch is superseded by the accepted Supabase profile/control-plane architecture. No production generator or recommendation send is enabled.

## Product contract

An explicitly subscribed user receives a personal recommendation email containing **exactly three events**. One of the three may be the visual hero. The email links to an already published personal page with a larger ranked selection and feedback actions.

This is recommendation/marketing-like mail, not a transactional reminder. It requires separate opt-in, unsubscribe, frequency/fatigue policy and deliverability evidence.

## Provider and address contract

- **NotiSend is the only transport for personal recommendations and personal announcements.** The intended sender is `Kenig Events <events@news.kenigevents.ru>` with `Reply-To: info@kenigevents.ru`, after the provider has verified the sender domain and all DNS alignment gates pass.
- Yandex Cloud Postbox is reserved for transactional mail. It must not silently take over recommendation delivery when NotiSend is unavailable, full or rate-limited.
- SpaceWeb owns the human mailbox `info@kenigevents.ru`; a reply to an announcement is human correspondence, not an automatic recommendation signal.
- Supabase, not NotiSend, decides whether a recipient is eligible. Provider lists, tags and campaign state are delivery projections and cannot create consent or remove a suppression.

## Launch admission cap

The launch service is available only inside the shared **200 unique NotiSend
recipient** admission set. Recommendation consent is still an independent hard
product and send-eligibility gate, not merely a UI label or a reliance on the
provider returning an over-limit error.

- Supabase must admit/activate a recommendation subscription transactionally
  only while both recommendation eligibility and the shared NotiSend unique-
  recipient capacity permit it.
- At capacity, a new user must not be marked subscribed, synchronized into a sendable NotiSend audience or sent a recommendation. A future waitlist, if introduced, is separate non-sendable state.
- Every issue build and final send claim must recheck both the Supabase admission state and the `<= 200` ceiling. Paused, revoked or suppressed users are not sendable; any slot-reuse policy must preserve consent and suppression history.
- The effective canary may be smaller than 200 when the current NotiSend plan needs capacity for seed/service contacts or imposes another lower limit. Neither a plan limit nor provider contact count permits more than 200 active consented users.
- Capacity exhaustion fails closed and raises an operator-visible condition. It does not trigger a Postbox fallback or a tariff change.

## Identity and consent

- Yandex-authenticated or verified-email Supabase identity; verified email supports both code and one-click-link completion.
- Recommendation-email consent is purpose-specific and independently revocable.
- Calendar/favorite and transactional-email consent do not authorize this stream.
- There is no parallel anonymous email-account model: entering and verifying an email creates/recovers the email-only Supabase identity.

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
8. Recheck admission-cap eligibility, consent, suppression and freshness immediately before send.
9. Send through NotiSend and record its provider result in Supabase; project only de-identified analytics to YDB.
10. Accept feedback through a rate-limited same-origin endpoint.

## Personal page security

- public forwardable high-entropy secret URL: anyone with the link may read the page without authentication;
- `noindex`/`nofollow` discovery controls, while explicitly recognizing that `noindex` is not access control;
- revocable/rotatable token with retention/expiry policy set separately;
- only keyed token hash in Supabase;
- restrictive referrer policy and no token in outbound destination;
- no email, raw profile, hidden vectors/tags or internal scores in HTML/JSON;
- separate page, click, unsubscribe and feedback tokens;
- static content remains readable if analytics/feedback calls fail.

## Release phases

1. Docs/ADR and product decisions.
2. Supabase schema/RLS and verified identity/consent.
3. Dry-run generation and internal personal pages.
4. NotiSend seed-list canary with exactly three events and a validated, already published personal page.
5. Auth-only limited beta.
6. Expand only within the hard 200-user admission ceiling after abuse, quality and deliverability evidence.

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Unsigned personalization](../unsigned-personalization/README.md)
- [Email delivery](../../operations/email-delivery.md)
- [Global product decisions](../static-personal-announcements/global-product-decisions.md)
