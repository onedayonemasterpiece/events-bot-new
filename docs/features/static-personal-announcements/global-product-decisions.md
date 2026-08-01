# Global product decisions for the static personal announcements release

> Status: release-level product decisions accepted in the planning thread on 2026-07-11; retention and numerical quality thresholds remain to be set.

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
11. **Email provider routing is explicit.** SpaceWeb owns and retains the
human/inbound mailbox. A read-only Yandex IMAP collector handles the production
automation copy without changing `Seen`; Yandex Mail Trigger remains a direct
technical canary. Postbox sends transactional mail. NotiSend sends personal
recommendations and the narrow returning/repeated/fixed-test Auth route.
Supabase remains the consent/suppression/admission authority; YDB remains
analytics-only.
12. **NotiSend has a shared hard launch ceiling of 200 unique recipients.**
Recommendation consent remains independently capped and fail-closed; provider
limits do not authorize an extra recipient or a recommendation fallback to
Postbox. Auth assigns over-capacity new recipients to Postbox before dispatch.

13. **Пасхалки — отдельный post-release campaign format, а не engagement North Star.** Они используют общий promo control plane, но требуют first-class egg subject/progress ledger вместо fake event ids. Первый пилот — конечная non-prize коллекция с добровольными подсказками/share, отдельным блоком в `Моё`, feedback/partner intake, admin kill switch и holdout по downstream event value; до product/legal/privacy/a11y/IP/anti-abuse acceptance production implementation запрещена.

## Consequences

- Email-only users are authenticated identities, not a parallel anonymous-subscription account model.
- The code and link cannot create two accounts or consume each other incorrectly; replay/attempt/TTL limits apply to the shared verification transaction.
- `noindex` is discovery control, not access control. A forwarded personal-page URL intentionally grants read access to its holder.
- Personal-page artifacts must exclude email, account id, raw/inferred profile internals, hidden scores and sensitive history.
- Automatic linking uses the existing personalization-consent state; without eligible consent, login does not silently persist previously local behavior until that consent exists.
- All ten release workstreams in the readiness checklist remain blockers.
- The public recommendation-email launch can reach fewer than 200 users during canary or when provider seed/service contacts reduce usable plan capacity, but it can never exceed 200 active consented users without a later explicit product and infrastructure decision.

## Product decisions still required

### Retention

Product/legal owners must approve retention for current profile state, consent evidence, raw telemetry, delivery events, suppressions and personal pages. Suppression evidence must outlive normal profile deletion enough to prevent accidental resend.

### Event-quality stability window

Define the required canary duration and numerical “almost no defects” thresholds for duplicates, wrong location and wrong date/time. Smart Update remains prevention owner; release monitoring supplies the evidence.
