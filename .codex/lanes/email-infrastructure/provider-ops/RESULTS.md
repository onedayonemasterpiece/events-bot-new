# Lane provider-ops Results

## Status

Live foundation completed; outbound activation intentionally gated.

## Requirement outcome

- **R02 — Done.** Postbox temporary-key creation is fail-closed; temporary keys
  were audited and deleted without touching the existing kgd80 persistent key.
- **R04 — Done.** SpaceWeb mailboxes and exact DNS records were provisioned while
  preserving Yandex nameservers and all pre-existing site records. Encrypted
  IMAP/webmail access and public MX/SPF/DKIM/DMARC were verified.
- **R05 — Done.** The isolated Yandex inbound pipeline is live with a private
  KMS-encrypted bucket, bounded YMQ retries/DLQs, production-tagged Functions,
  direct Mail Trigger canaries and retained-mailbox IMAP collection.
- **R07 — Partial.** Postbox identity/configuration and a real-ID seed send passed,
  but the message landed in Spam and no event destination is attached. Production
  sending remains disabled.
- **R08 — Partial / external blocker.** NotiSend domain and sender are verified and
  the 200-contact cap is enforced in Supabase. API activation is still pending, so
  no provider contact import, webhook trust or send was attempted.

## Safety evidence

- No nameserver, existing website/CDN record, tariff or kgd80 resource changed.
- No mass send and no NotiSend contact import occurred.
- Mailbox/API/private-key material is stored in deletion-protected Lockbox secrets,
  never in Git or this report.
- The production SpaceWeb mailbox stays in `Mail` mode. The mutually exclusive
  forwarding mode was rejected in favor of a read-only UID-cursor collector that
  keeps messages present and unread.
- All outbound Supabase switches remain off and dry-run-only remains on.

## Live canaries

- Direct Yandex Mail Trigger canary from `info@kgd80.ru`: accepted, stored
  privately, processed once and recorded once; processing and intake DLQs empty.
- Retained SpaceWeb mailbox canary from `info@kgd80.ru`: message remained present
  and unseen after collector execution; one metadata-only Supabase receipt;
  processing and DLQs empty.
- Postbox transactional seed: real provider message id returned and message
  delivered to the retained mailbox, but Spam placement blocks activation.

Detailed redacted inventories, hashes and provider evidence remain in ignored
`artifacts/codex/email-infrastructure-20260711/`.
