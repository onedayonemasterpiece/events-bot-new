# Global product decisions for the static personal announcements release

> Status: release-level product decisions originally accepted in the planning thread on 2026-07-11; retention and numerical quality thresholds still require their own evidence/approval.
> Documentation correction prepared 2026-09-05 in PR #587: activation and purpose-consent terminology is reconciled with the existing personalization requirements/TO-BE; this is not a new legal conclusion or a deployed change.
> Integration with current voice/island work: [release-integration.md](release-integration.md).

## Working decisions

1. **Three recommendations means exactly three events in the email.** A visual hero may be one of those three; it is not a fourth event. The personal page may contain a larger ranked set.
2. **Calendar save and favorite are one durable saved-event state.** Exporting an ICS is a side effect/action, not a separate user-owned entity.
3. **Calendar/favorite never implies email consent.** Transactional follow mail and recommendation mail each retain their explicit purpose-specific consent/eligibility rules.
4. **Supabase/Postgres is the current durable user/profile and email control-plane owner.** YDB is analytics/history and comment-feedback sidecar only. The existing localization/data-flow gate can block new remote-profile rollout; a store migration requires an explicit ownership revision, not a silent parallel YDB profile.
5. **Profile linking is idempotent.** Login/logout cannot silently fork two durable profiles; reset/unlink/delete remain explicit operations.
6. **Static fallback is mandatory.** Auth, personalization, telemetry, YDB, search provider or email failure must not make public event pages unusable.
7. **All F1–F17 are required for the first public release/presentation.** Technical canaries may be staged, but no capability may be removed from the release scope or presented as a future beta merely to make a smaller release green. Historical stage labels are not current proof of implementation.
8. **Verified-email login supports both a code and a one-click link.** Both complete one Supabase Auth verification flow and recover the same identity.
9. **Personal pages use a forwardable public secret link.** Anyone who has the high-entropy URL may open it without authentication. The page is `noindex` and contains no raw profile/private identity data; the token remains revocable/rotatable under retention policy.
10. **Anonymous-to-authenticated profile linking is automatic and intelligent for an eligible activated profile.** No extra merge-confirmation dialog is required after login. The merge is idempotent, deduplicates saved/action state, preserves explicit user actions over inferred interests, decays/conflict-checks inferred signals, shows the result and keeps reset/unlink available. It does not create an interest profile from mere prior browsing or grant unrelated purpose consents.
11. **Email provider routing is explicit.** SpaceWeb owns and retains the human/inbound mailbox. A read-only Yandex IMAP collector handles the production automation copy without changing `Seen`; Yandex Mail Trigger remains a direct technical canary. Postbox sends transactional mail. NotiSend sends personal recommendations and the narrow returning/repeated/fixed-test Auth route. Supabase remains the consent/suppression/admission authority; YDB remains analytics-only.
12. **NotiSend has a shared hard launch ceiling of 200 unique recipients.** Recommendation consent remains independently capped and fail-closed; provider limits do not authorize an extra recipient or a recommendation fallback to Postbox. Auth assigns over-capacity new recipients to Postbox before dispatch.
13. **Пасхалки — отдельный post-release campaign format, а не engagement North Star.** Они используют общий promo control plane, но требуют first-class egg subject/progress ledger вместо fake event ids. Первый пилот — конечная non-prize коллекция с добровольными подсказками/share, отдельным блоком в `Моё`, feedback/partner intake, admin kill switch и holdout по downstream event value; до product/legal/privacy/a11y/IP/anti-abuse acceptance production implementation запрещена.

## 2026-09-05 terminology correction: activation is not a blanket consent

Sources: the owner's [personalization requirements](../static-site-pages/personalizaion/requirements.md) and the existing [personalization blueprint](../static-site-pages/personalizaion/personalization-to-be.md), especially activation, synchronization and surface invariants. These source requirements are preserved, not rewritten here.

**Before:** the old consequence below treated eligible linking as depending on a generic “personalization-consent state”, which could be misread as a new mandatory checkbox before all personalization.

**Now:** distinguish service activation, eligibility/localization and independent purposes. `interest_profile_change`, like, `personal_feed_enabled`, or `not_interested` after the undo window can activate the personalized function under its accepted contract. Page view, scroll, dwell, closing an informational notice, share and a normal voice/search request are not first-activation events. An undone pending hide does not silently create a server profile. A valid Auth account/session alone is not evidence of an activated interest profile.

Automatic linking uses that eligible activation state and existing identity proof, preserving compact current state rather than uploading raw browsing history. Where required localization/data-flow conditions are unmet, remote-profile enablement remains blocked; adding this clarification does not declare compliance.

`product_analytics`, focus research, email recommendations, push and other communications retain their own purposes and permissions. Denial of optional analytics does not disable ordinary navigation or already-eligible personalization. Consent to analytics does not activate a profile or authorize a retrospective profile join. Existing informational recommendation notice and permanently accessible Rules remain; no new generic “I agree to personalization” popup is introduced by the voice or island features.

## Consequences

- Email-only users are authenticated identities, not a parallel anonymous-subscription account model.
- The code and link cannot create two accounts or consume each other incorrectly; replay/attempt/TTL limits apply to the shared verification transaction.
- `noindex` is discovery control, not access control. A forwarded personal-page URL intentionally grants read access to its holder.
- Personal-page artifacts must exclude email, account id, raw/inferred profile internals, hidden scores and sensitive history. The explicit forwardable personal-page contract does not make private voice conversation history publicly readable.
- Login links an eligible activated anonymous profile without another merge ceremony. Without that eligibility it does not silently persist pre-activation behavior; purpose-specific permissions are not inferred from login or linking.
- All ten release workstreams in the original readiness checklist remain obligations until explicitly reconciled by current release evidence. New voice/island work does not replace them with a UI-only acceptance claim.
- The public recommendation-email launch can reach fewer than 200 users during canary or when provider seed/service contacts reduce usable plan capacity, but it can never exceed the shared admitted ceiling without a later explicit product and infrastructure decision.
- A strong action's primary acknowledgement, asynchronous analytics delivery and later profile materialization are distinct. Failure of the analytics sidecar does not undo the primary action; a proxy response does not by itself prove either downstream outcome.
- Calendar chronology, thematic eligibility, global exact hide/undo, query-over-profile priority and non-jumping visible content remain owned by the personalization blueprint and apply to the voice answer timeline as well.

## Product decisions still required

### Retention

Product/legal owners must approve retention for current profile state, activation/consent evidence, raw telemetry, delivery events, suppressions and personal pages. Suppression evidence must outlive normal profile deletion enough to prevent accidental resend. Conversation history and browser outbox lifetime are separate from profile horizons and analytical session definitions; no feature silently overwrites all of them with one TTL.

### Event-quality stability window

Define the required canary duration and numerical “almost no defects” thresholds for duplicates, wrong location and wrong date/time. Smart Update remains prevention owner; release monitoring supplies the evidence. Historical checkmarks do not establish the current stability window.
