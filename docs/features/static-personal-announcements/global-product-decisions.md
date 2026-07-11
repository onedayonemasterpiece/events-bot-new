# Global product decisions for the static personal announcements release

> Status: working release decisions plus questions requiring product confirmation.

## Working decisions

1. **Three recommendations means exactly three events in the email.** A visual hero may be one of those three; it is not a fourth event. The personal page may contain a larger ranked set.
2. **Calendar save and favorite are one durable saved-event state.** Exporting an ICS is a side effect/action, not a separate user-owned entity.
3. **Calendar/favorite never implies email consent.** Transactional follow mail and recommendation mail each require explicit purpose-specific consent.
4. **Supabase/Postgres is the durable user/profile and email control-plane owner.** YDB is analytics/history and comment-feedback sidecar only.
5. **Profile linking is idempotent.** Login/logout cannot silently fork two durable profiles; reset/unlink/delete remain explicit operations.
6. **Static fallback is mandatory.** Auth, personalization, telemetry, YDB, search provider or email failure must not make public event pages unusable.

## Product confirmation required

### Q1 — Release scope

Recommended: staged technical canaries are allowed, but the public presentation may promise only capabilities that passed the full release checklist. Confirm whether all F1–F17 must be public at the first presentation or whether transport/comment-feedback/admin repair may be demonstrated as controlled beta surfaces.

### Q2 — Verified-email login UX

Recommended: support both a six-digit code and a one-click link from the same Supabase Auth verification flow. Code is the robust cross-device fallback; link is the low-friction path.

### Q3 — Anonymous email subscription

Recommended: first canary is Yandex/verified-email users only. Anonymous browser profile + email subscription follows after identity linking, abuse controls and preference-center evidence are stable.

### Q4 — Personal-page access

Recommended: an expiring, revocable high-entropy link that works without a second login, contains no raw profile/private fields and can also be opened from the signed-in account. Confirm whether forwarding the link to another person is an accepted limitation or must be blocked by account binding.

### Q5 — Profile-link consent

Recommended: automatic idempotent linking is allowed only when the user has already accepted the current personalization consent; the callback shows a clear result and offers reset/unlink. Without current consent, login must not import local behavior.

### Q6 — Retention

Product/legal owners must approve retention for current profile state, consent evidence, raw telemetry, delivery events, suppressions and personal pages. Suppression evidence must outlive normal profile deletion enough to prevent accidental resend.

### Q7 — Event-quality stability window

Define the required canary duration and numerical “almost no defects” thresholds for duplicates, wrong location and wrong date/time. Smart Update remains prevention owner; release monitoring supplies the evidence.
