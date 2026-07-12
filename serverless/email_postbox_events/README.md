# Postbox Data Streams event consumer

This Python 3.12 Yandex Function is the authenticated feedback boundary for
transactional Postbox mail:

```text
Postbox configuration destination -> YDS -> YDS trigger -> Function -> Supabase RPC
```

The function accepts only the documented YDS trigger `{"messages":[...]}` JSON,
pins the Postbox identity/configuration/from-domain, requires exactly one matching
recipient, computes a versioned HMAC of that normalized address and sends only
metadata to `email_record_postbox_event_v2`. Supabase then correlates the exact
provider `messageId` to a persisted Postbox outbox row and its DB-owned identity.

## Runtime environment

| Name | Secret | Contract |
|---|---:|---|
| `POSTBOX_EVENT_CONSUMER_ENABLED` | no | Explicit kill switch; only `1/true/yes/on` enables processing. |
| `POSTBOX_EXPECTED_IDENTITY_ID` | no | Exact Postbox identity ID from a verified live notification. |
| `POSTBOX_EXPECTED_CONFIGURATION_TAG` | no | Exact configuration ID in `mail.tags["ses:configuration-set"]`, not its display name. |
| `POSTBOX_EXPECTED_FROM_DOMAIN` | no | Must be `kenigevents.ru` in production. |
| `EMAIL_ADDRESS_HMAC_KEY` | yes | Dedicated keyed identity proof, shared with identity synchronization. |
| `EMAIL_ADDRESS_HMAC_KEY_VERSION` | yes | Positive integer version, initially `1`. |
| `PERSONALIZATION_SUPABASE_URL` | yes | Personalization project HTTPS origin. |
| `PERSONALIZATION_SUPABASE_SECRET_KEY` | yes | Backend-only key sent only as `apikey`. |

Runtime is stdlib-only. The consumer never logs provider message IDs, addresses,
headers, subjects, bodies, URLs, exception text or Supabase responses. It logs a
truncated SHA-256 of `eventId`, bounded outcome and stable error code.

## Retry and deduplication

Postbox is QoS 1 / at-least-once. The stable provider `eventId` is the dedup key.
Any invalid record or transient/correlation-pending Supabase result fails the whole
Function invocation so the YDS trigger retries it. After bounded trigger retries,
the original provider record goes to the dedicated private YMQ DLQ. Replay invokes
this same handler; an exact duplicate returns success without another transition.

`hard_bounce` and `complaint` create global suppression; `unsubscribe` creates a
transactional-only suppression. A recipient/message mismatch always fails closed.

## Build

From the repository root:

```bash
infra/yandex/email-postbox-events/build-function.sh
```

The ignored ZIP is written to `artifacts/codex/email-postbox-events/`. Deploy a
new untagged version, invoke tested fixtures, and only then move the `prod` tag.
