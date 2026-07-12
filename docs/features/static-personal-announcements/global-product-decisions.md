# Global product decisions for the static personal announcements release

> Status: release-level product decisions accepted in the planning thread on 2026-07-11 and extended on 2026-07-12; retention and numerical quality thresholds remain to be set.

## Working decisions

1. **Three recommendations means exactly three events in the email.** A visual hero may be one of those three; it is not a fourth event. The personal page may contain a larger ranked set.
2. **Calendar save and favorite are one durable saved-event state.** Exporting an ICS is a side effect/action, not a separate user-owned entity.
3. **Calendar/favorite never implies email consent.** Transactional follow mail and recommendation mail each require explicit purpose-specific consent.
4. **Supabase/Postgres is the durable user/profile and email control-plane owner.** YDB is analytics/history and comment-feedback sidecar only.
5. **Profile linking is idempotent.** Login/logout cannot silently fork two durable profiles; reset/unlink/delete remain explicit operations.
6. **Static fallback is mandatory.** Auth, personalization, telemetry, YDB, search provider or email failure must not make public event pages unusable.
7. **All F1–F17 are required for the first public release/presentation.** Technical canaries may be staged, but no capability may be removed from the release scope or presented as a future beta.
8. **Verified-email login supports both a code and a one-click link.** Both complete one Supabase Auth verification flow and recover the same identity.
9. **Personal pages use a forwardable public secret link.** Anyone who has the high-entropy URL may open it without authentication. The page is `noindex` and contains no raw profile/private identity data; the token remains revocable/rotatable under retention policy.
10. **Anonymous-to-authenticated profile linking is automatic and intelligent.** No extra merge-confirmation dialog is required after login. The merge is idempotent, deduplicates saved/action state, preserves explicit user actions over inferred interests, decays/conflict-checks inferred signals, shows the result and keeps reset/unlink available.
11. **Email providers have non-overlapping roles.** SpaceWeb owns and retains the human/inbound mailbox. Because its mailbox destination modes are mutually exclusive, a read-only Yandex IMAP collector handles the production automation copy without changing `Seen`; Yandex Mail Trigger remains a direct technical canary. Postbox sends transactional mail only, and NotiSend sends personal recommendations/announcements only. Supabase remains the consent/suppression/outbox authority; YDB remains analytics-only.
12. **Recommendation email has a hard launch ceiling of 200 actively consented users.** Capacity must be enforced in Supabase and fail closed; provider limits do not authorize an extra recipient or a fallback to Postbox.
13. **A saved event may have one explicit D-1 email reminder.** After calendar/favorite save the UI shows whether a reminder will be sent 24 hours before the canonical start and to which masked verified address. Save/calendar alone never grants transactional consent; missing email/consent is resolved inline before the promise is shown.
14. **Email acquisition always offers Yandex or manual email.** A usable verified email returned through Yandex may be reused; otherwise the user enters an address and verifies the same Supabase identity transaction by code or link. Recommendation and transactional consents remain separate.
15. **A saved successful search becomes a public tag only after normalization, deduplication and novelty proof.** Equivalent intents/result sets merge into one tag. Accepted tags are regenerated from the current catalog during static builds, are public/anonymous and disclose no creator identity or private query history.
16. **Manual email is entered once per browser.** Without Yandex, the normalized address and pending/verified UX state persist in versioned localStorage and are reused on later visits/calendar saves. Supabase remains verification/identity/consent authority; localStorage cannot independently authorize mail and has an explicit `Забыть почту` action.
17. **Personalization release acceptance is end-to-end and metric-backed.** Playwright/Gherkin must prove localStorage collection, accepted/deduped DB records, profile formation and application to a later feed. For every eligible mature golden persona, including «Чайковский», the first relevant event must be reached within at most 20 validly inspected cards.

## Consequences

- Email-only users are authenticated identities, not a parallel anonymous-subscription account model.
- The code and link cannot create two accounts or consume each other incorrectly; replay/attempt/TTL limits apply to the shared verification transaction.
- `noindex` is discovery control, not access control. A forwarded personal-page URL intentionally grants read access to its holder.
- Personal-page artifacts must exclude email, account id, raw/inferred profile internals, hidden scores and sensitive history.
- Automatic linking uses the existing personalization-consent state; without eligible consent, login does not silently persist previously local behavior until that consent exists.
- Yandex authentication is still successful when the provider supplies no usable email; email-dependent actions then require manual verification before they can be promised.
- Reminder scheduling is keyed by user, canonical event and start-version so save retries/reschedules cannot create duplicate D-1 mail.
- Public search-tag novelty is judged by normalized intent and verified result-set overlap, not by different wording alone; merged candidates point to the existing canonical tag.
- A passing localStorage test or a passing DB insert alone is insufficient personalization evidence; the same correlated run must show that the expected profile changed the next served list.
- All ten release workstreams in the readiness checklist remain blockers.
- The public recommendation-email launch can reach fewer than 200 users during canary or when provider seed/service contacts reduce usable plan capacity, but it can never exceed 200 active consented users without a later explicit product and infrastructure decision.

## Product decisions still required

### Retention

Product/legal owners must approve retention for current profile state, consent evidence, raw telemetry, delivery events, suppressions and personal pages. Suppression evidence must outlive normal profile deletion enough to prevent accidental resend.

### Event-quality stability window

Define the required canary duration and numerical “almost no defects” thresholds for duplicates, wrong location and wrong date/time. Smart Update remains prevention owner; release monitoring supplies the evidence.
