# Postbox event infrastructure audit (R03)

Date: 2026-07-12 UTC
Lane: `infra-audit`
Mode: read-only live audit; **no Yandex resource was created, updated, or deleted**
Folder: `b1g0v4ur96gis5kot6ku` (`kenigevents-email-prod`)
Base: `origin/main@d9ba3ad03288a923997c6626295b8a9016cf26ed`
Audit branch: `agent/postbox-events-release/infra-audit`

## Decision

Provision a dedicated serverless YDB database and a single-partition, four-hour,
on-demand YDS topic, then deliver that topic through a Python 3.12 Function and a
YDS trigger with a dedicated YMQ DLQ. Attach the Postbox destination **disabled**,
prove the consumer, duplicate and DLQ paths, and only then enable the destination
for one controlled Postbox canary. Application transactional sending stays disabled
during all of this.

This is not a fixed-capacity subscription design:

- YDB `provisioned RCU/s = 0` avoids hourly provisioned-RU billing; a 10 RU/s
  throttle limits runaway on-demand use.
- The YDS topic uses `request-units`, not `reserved-capacity`; billing is based on
  operations and actual retained bytes rather than an allocated shard subscription.
- `5GB` is a database safety **limit**, not preallocated billable storage.
- Function invocations, YMQ requests, retained log bytes, Lockbox access, and KMS
  decrypt operations remain usage-priced.

Current pricing references: [YDB serverless pricing](https://yandex.cloud/en/docs/ydb/pricing/serverless),
[Data Streams pricing](https://yandex.cloud/en/docs/data-streams/pricing), and
[Message Queue pricing](https://yandex.cloud/en/docs/message-queue/pricing).

## Redacted live inventory

### YDB / YDS

- `yc ydb database list --folder-id b1g0v4ur96gis5kot6ku`: `[]`.
- Therefore there is no YDB database, YDS stream/topic, YDS consumer trigger, or
  stream retention/metering configuration in this folder.

### Postbox (fresh IAM-token GETs, 2026-07-12)

- Identity `kenigevents.ru`: `DOMAIN`, `VerifiedForSendingStatus=true`,
  `VerificationStatus=SUCCESS`.
- BYODKIM: enabled, `EXTERNAL`, selector `postbox2026`, status `SUCCESS`.
- Identity is bound to configuration `kenigevents-transactional`.
- Configuration delivery policy is `TLS REQUIRE`.
- `GET .../event-destinations`: HTTP 200 with zero destinations (`null`/empty).
- The read used an ephemeral IAM token obtained by impersonating the existing
  `postbox.editor` service account; no static key was created and no token was
  printed or persisted.

### IAM, KMS, Lockbox, Functions and triggers

- Eight service accounts exist. Existing folder bindings are limited to
  `functions.functionInvoker`, `postbox.editor`, `postbox.sender`,
  `storage.uploader/viewer`, and `ymq.reader/writer`; no folder-wide primitive
  `viewer`, `editor`, or `admin` binding was found.
- Two customer KMS keys exist: general email secrets and inbound storage. Both are
  AES-256 with annual rotation; inbound storage has deletion protection.
- Eight Lockbox secrets exist; all are active and deletion-protected. Secret
  payloads were not read. One static access key remains only for the inbound
  intake/collector's AWS-compatible S3/YMQ calls; all other audited SAs have zero.
- Four active Python 3.12 inbound Functions are tagged `prod`: adapter, IMAP
  collector, intake, and delivery. No Postbox event consumer exists.
- Three active inbound triggers exist: two-minute IMAP timer, YMQ processing, and
  direct Mail Trigger. No YDS trigger exists.
- The default log group retains three days. No Postbox-events log group, dashboard,
  or alert is present.

Do not reuse the inbound trigger/runtime SAs, inbound DLQs, or inbound function.
The outbound provider-event trust boundary and rollback lifecycle are separate.

## Required code/data gate before provisioning

Use the code-audit v2 contract:

1. The consumer computes a versioned recipient HMAC from the provider recipient.
2. The service-only Supabase RPC correlates `provider_message_id` to the Postbox
   outbox row and DB-owned identity, and rejects any supplied HMAC mismatch.
3. Duplicate `eventId` is a successful no-op. Postbox is QoS 1 / at-least-once;
   identical retries retain the same `eventId` ([notification contract](https://yandex.cloud/en/docs/postbox/concepts/notification)).
4. Logs contain only event type, provider message ID/HMAC, event ID/hash, outcome,
   lag, and correlation IDs; never raw address, subject, or body.

The current live environment has no `EMAIL_ADDRESS_HMAC_KEY`. Create two dedicated,
deletion-protected KMS-backed secrets before deploying the consumer:

- `email-address-hmac`: random current key plus integer key version `1`; later used
  by both identity sync and this consumer.
- `email-postbox-events-supabase-api`: only the personalization Supabase URL and
  secret server key needed by this function.

The function runtime gets secret-specific `lockbox.payloadViewer` and decrypt-only
access to the KMS key. It must not receive `postbox.editor`, `postbox.sender`, YDS,
YMQ, or primitive folder roles.

## Exact provisioning order and command plan

These commands are a reviewed future runbook, not commands executed by this lane.
Always pass the explicit folder and capture only sanitized JSON under ignored
`artifacts/codex/`.

### 0. Operator gates

```bash
set -euo pipefail
export PATH=/home/dev/yandex-cloud/bin:$PATH
export FOLDER_ID=b1g0v4ur96gis5kot6ku
export DB_NAME=kenigevents-email-events
export STREAM_NAME=kenigevents-postbox-events
export FUNCTION_NAME=kenigevents-postbox-event-consumer
export TRIGGER_SA_NAME=email-postbox-event-trigger
export RUNTIME_SA_NAME=email-postbox-event-consumer
export DLQ_NAME=kenigevents-postbox-events-dlq
export LOG_GROUP_NAME=kenigevents-postbox-events
export CONFIG_SET=kenigevents-transactional
export DESTINATION_NAME=kenigevents-postbox-events-v1

test "$(git branch --show-current)" = "<approved-clean-release-branch>"
test -z "$(git status --porcelain)"
test "$(yc ydb database list --folder-id "$FOLDER_ID" --format json)" = "[]"
```

Required approvals/evidence before continuing: code/migration tests green; live DB
backup; migration applied; provider-event RPC contract verified; outbound switches
still false and dry-run true; ZIP SHA recorded; no secret in shell tracing/logs.

### 1. Serverless YDB and low-retention on-demand topic

```bash
yc ydb database create "$DB_NAME" \
  --folder-id "$FOLDER_ID" \
  --serverless \
  --deletion-protection \
  --sls-enable-throttling-rcu \
  --sls-throttling-rcu 10 \
  --sls-provisioned-rcu 0 \
  --sls-storage-size 5GB \
  --labels project=kenigevents,purpose=postbox-events,environment=prod

yc ydb database get "$DB_NAME" --folder-id "$FOLDER_ID" --format json
# From the sanitized response set:
export DB_ID='<YDB database id>'
export DB_ENDPOINT='grpcs://ydb.serverless.yandexcloud.net:2135'
export DB_PATH="/ru-central1/$FOLDER_ID/$DB_ID"

TOKEN_FILE=$(mktemp)
trap 'rm -f "$TOKEN_FILE"' EXIT
yc iam create-token >"$TOKEN_FILE"
/home/dev/ydb/bin/ydb -e "$DB_ENDPOINT" -d "$DB_PATH" \
  --iam-token-file "$TOKEN_FILE" topic create "$STREAM_NAME" \
  --partitions-count 1 \
  --retention-period 4h \
  --partition-write-speed-kbps 128 \
  --metering-mode request-units \
  --supported-codecs RAW \
  --metrics-level 2 \
  --auto-partitioning-strategy disabled

/home/dev/ydb/bin/ydb -e "$DB_ENDPOINT" -d "$DB_PATH" \
  --iam-token-file "$TOKEN_FILE" topic describe "$STREAM_NAME"
rm -f "$TOKEN_FILE"; trap - EXIT
```

Rationale: one partition is far above this stream's launch volume; autopartitioning
is irreversible in important respects and unnecessary. Four hours is the smallest
retention accepted by the documented Kinesis-compatible CreateStream API. The
native CLI permits other durations, but production should not depend on an
undocumented Postbox/Kinesis combination. Data Streams limits retention to 24 hours
and serverless YDB to 100 shards ([limits](https://yandex.cloud/en/docs/data-streams/concepts/limits)).

### 2. Dedicated service accounts and least privilege

```bash
TRIGGER_SA_ID=$(yc iam service-account create "$TRIGGER_SA_NAME" \
  --folder-id "$FOLDER_ID" \
  --description 'Reads Postbox YDS and invokes event consumer; writes trigger DLQ' \
  --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
RUNTIME_SA_ID=$(yc iam service-account create "$RUNTIME_SA_NAME" \
  --folder-id "$FOLDER_ID" \
  --description 'Runtime identity for Postbox event consumer' \
  --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# Current official YDS-trigger documentation requires yds.admin for the stream SA.
# YDS roles can only be assigned at a parent (folder/cloud), so this is the one
# known broad permission and must not be placed on the runtime SA.
yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role yds.admin --service-account-id "$TRIGGER_SA_ID"
yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role ymq.writer --service-account-id "$TRIGGER_SA_ID"
```

Official docs are inconsistent: the trigger creation page describes read/write
stream permission while the current trigger concept explicitly says `yds.admin`.
Use the documented `yds.admin` for the dedicated trigger identity rather than
silently widening an existing SA; record this as a vendor-granularity risk. See
[YDS trigger roles](https://yandex.cloud/en/docs/functions/concepts/trigger/data-streams-trigger)
and [YDS IAM](https://yandex.cloud/en/docs/data-streams/security/).

### 3. Secrets and log group

Create both secrets using stdin payloads generated from a secure operator process;
never place values in argv, Git, terminal history, or command output:

```bash
yc lockbox secret create --folder-id "$FOLDER_ID" \
  --name email-address-hmac --kms-key-id abjqsfa5omcsvdp96ct4 \
  --deletion-protection --labels project=kenigevents,purpose=email-hmac \
  --payload - <'<secure JSON payload on stdin>'

yc lockbox secret create --folder-id "$FOLDER_ID" \
  --name email-postbox-events-supabase-api --kms-key-id abjqsfa5omcsvdp96ct4 \
  --deletion-protection --labels project=kenigevents,purpose=postbox-events \
  --payload - <'<secure JSON payload on stdin>'

yc lockbox secret add-access-binding email-address-hmac \
  --role lockbox.payloadViewer --service-account-id "$RUNTIME_SA_ID"
yc lockbox secret add-access-binding email-postbox-events-supabase-api \
  --role lockbox.payloadViewer --service-account-id "$RUNTIME_SA_ID"
yc kms symmetric-key add-access-binding abjqsfa5omcsvdp96ct4 \
  --role kms.keys.decrypter --service-account-id "$RUNTIME_SA_ID"

yc logging group create --folder-id "$FOLDER_ID" \
  --name "$LOG_GROUP_NAME" --retention-period 604800s \
  --labels project=kenigevents,purpose=postbox-events,environment=prod
```

### 4. Dedicated YMQ trigger DLQ

YDS triggers support an external YMQ DLQ directly. Create a **new standard queue**
with 14-day retention; do not reuse inbound DLQs. YMQ's AWS-compatible provisioning
requires a static access key. Create a temporary key for the dedicated trigger SA,
hold it only in a `0600` temporary file/environment, create and inspect the queue,
and delete the key in a fail-closed `trap`. Never print the create-key JSON.

```bash
aws sqs create-queue \
  --endpoint-url https://message-queue.api.cloud.yandex.net \
  --queue-name "$DLQ_NAME" \
  --attributes MessageRetentionPeriod=1209600,VisibilityTimeout=60,ReceiveMessageWaitTimeSeconds=10,MaximumMessageSize=262144

DLQ_URL=$(aws sqs get-queue-url \
  --endpoint-url https://message-queue.api.cloud.yandex.net \
  --queue-name "$DLQ_NAME" --query QueueUrl --output text)
DLQ_ARN=$(aws sqs get-queue-attributes \
  --endpoint-url https://message-queue.api.cloud.yandex.net \
  --queue-url "$DLQ_URL" --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)
```

The source is YDS, so there is no YMQ source queue/redrive policy. The trigger sends
the failed invocation batch to this external DLQ after its retries. **Controlled
replay and event-level deduplication remain application-managed**: replay the saved
trigger envelope, invoke the same idempotent handler, and delete the DLQ item only
after success. Yandex documents retry range 1-5, interval 10-60 seconds, and optional
YMQ DLQ ([trigger creation](https://yandex.cloud/en/docs/functions/operations/trigger/data-streams-trigger-create)).

### 5. Python 3.12 consumer and YDS trigger

```bash
yc serverless function create "$FUNCTION_NAME" --folder-id "$FOLDER_ID" \
  --description 'Authenticated Postbox YDS event consumer' \
  --labels project=kenigevents,purpose=postbox-events,environment=prod
FUNCTION_ID=$(yc serverless function get "$FUNCTION_NAME" --folder-id "$FOLDER_ID" \
  --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

yc serverless function version create --function-id "$FUNCTION_ID" \
  --runtime python312 --entrypoint index.handler \
  --memory 256MB --execution-timeout 30s --concurrency 1 \
  --service-account-id "$RUNTIME_SA_ID" \
  --source-path '<reviewed postbox-events ZIP>' \
  --log-group-name "$LOG_GROUP_NAME" --min-log-level info \
  --metadata-options aws-v1-http-endpoint=disabled,gce-http-endpoint=enabled \
  --secret id='<email-address-hmac id>',version-id='<pinned version>',key=hmac_key,environment-variable=EMAIL_ADDRESS_HMAC_KEY \
  --secret id='<email-address-hmac id>',version-id='<pinned version>',key=hmac_key_version,environment-variable=EMAIL_ADDRESS_HMAC_KEY_VERSION \
  --secret id='<postbox-events-supabase id>',version-id='<pinned version>',key=supabase_url,environment-variable=PERSONALIZATION_SUPABASE_URL \
  --secret id='<postbox-events-supabase id>',version-id='<pinned version>',key=supabase_secret_key,environment-variable=PERSONALIZATION_SUPABASE_SECRET_KEY \
  --tags prod

yc serverless function add-access-binding "$FUNCTION_ID" \
  --role functions.functionInvoker --service-account-id "$TRIGGER_SA_ID"

yc serverless trigger create yds \
  --folder-id "$FOLDER_ID" \
  --name kenigevents-postbox-event-trigger \
  --description 'Postbox event stream to idempotent consumer' \
  --labels project=kenigevents,purpose=postbox-events,environment=prod \
  --database "$DB_PATH" --stream "$STREAM_NAME" \
  --stream-service-account-id "$TRIGGER_SA_ID" \
  --batch-size 1b --batch-cutoff 1s \
  --invoke-function-id "$FUNCTION_ID" --invoke-function-tag prod \
  --invoke-function-service-account-id "$TRIGGER_SA_ID" \
  --retry-attempts 5 --retry-interval 30s \
  --dlq-queue-id "$DLQ_ARN" --dlq-service-account-id "$TRIGGER_SA_ID"
```

`1b` deliberately isolates records: the documented trigger forwards a single
message even when it exceeds the batch threshold, so low-volume Postbox events are
not coupled into a poison batch. The trigger may take up to five minutes to start.

### 6. Postbox destination: disabled first, enabled last

Use an impersonated short-lived IAM token for the existing `postbox.editor` SA;
do not create a persistent Postbox admin key. Create with `Enabled=false`:

```bash
export YDS_KINESIS_ENDPOINT="https://yds.serverless.yandexcloud.net/ru-central1/$FOLDER_ID/$DB_ID"
export DELIVERY_STREAM_ARN="arn:yc:yds:ru-central1::$YDS_KINESIS_ENDPOINT:$STREAM_NAME"
IAM_TOKEN=$(yc iam create-token --impersonate-service-account-id ajeo2i5goc7v2h4dtbj3)

python3 - <<'PY' > /tmp/postbox-destination.json
import json, os
print(json.dumps({
  "EventDestinationName": os.environ["DESTINATION_NAME"],
  "EventDestination": {
    "Enabled": False,
    "KinesisFirehoseDestination": {
      "IamRoleArn": "arn:",
      "DeliveryStreamArn": os.environ["DELIVERY_STREAM_ARN"],
    },
  },
  "MatchingEventTypes": [
    "SEND", "BOUNCE", "COMPLAINT", "DELIVERY", "DELIVERY_DELAY",
    "SUBSCRIPTION", "RENDERING_FAILURE"
  ],
}, separators=(",", ":")))
PY
curl --fail-with-body -sS -X POST \
  -H "X-YaCloud-SubjectToken: $IAM_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @/tmp/postbox-destination.json \
  "https://postbox.cloud.yandex.net/v2/email/configuration-sets/$CONFIG_SET/event-destinations"
rm -f /tmp/postbox-destination.json; unset IAM_TOKEN
```

Yandex's current notification schema includes Complaint and Rendering Failure, and
the current AWS SESv2 model accepts both, but the Postbox create-method page's
enumeration omits them. Treat HTTP 400 for either as a vendor-documentation blocker;
do **not** silently enable a destination without complaint coverage.

After synthetic success/duplicate/failure-DLQ/replay tests pass, enable the existing
destination with `PUT` and the same ARN, then immediately GET and verify
`Enabled=true`. Do not recreate the configuration or rebind the identity.

## Monitoring and alerts

Use the seven-day dedicated log group. The handler must emit one bounded JSON line
per event without PII. Configure Monium/Yandex Monitoring alerts before destination
enablement:

| Signal | Warning / alarm |
|---|---|
| `serverless.triggers.error_per_second` for this trigger | `>0` over 5m |
| `serverless.triggers.access_error_per_second`, types `request` and `dlq` | `>0` over 5m |
| Function `functions_errors` / `serverless.functions.errors_per_second` | `>0` over 5m |
| DLQ `queue.messages.stored_count` | `>0` immediately |
| DLQ `queue.messages.oldest_age_milliseconds` (currently measured in seconds) | `>300` warning, `>900` alarm |
| Supabase submitted Postbox outbox without terminal/delivery event | `>15m` warning, `>60m` alarm via application reconciliation |

Do not grant the runtime `monitoring.editor` merely to emit a custom metric; it can
also manage dashboards. Native function/trigger/YMQ metrics plus the existing
service-side outbox reconciliation are the least-privilege choice. A no-event
period is normal at launch, so `No data` alone is not an alarm.

References: [Cloud Functions metrics](https://yandex.cloud/en/docs/monitoring/metrics-ref/functions-ref),
[YMQ metrics](https://yandex.cloud/en/docs/monitoring/metrics-ref/message-queue-ref),
and [trigger monitoring](https://yandex.cloud/en/docs/functions/operations/trigger/trigger-monitoring).

## Acceptance E2E

All checks must pass while global/transactional sends remain disabled except the
single operator canary:

1. Verify YDB is serverless, deletion-protected, provisioned RCU `0`, throttle `10`;
   topic is request-units, one partition, 128 KB/s, 4h retention, no autoscaling.
2. Inject a valid redacted fixture into YDS; prove exactly one authenticated/verified
   provider event and correct outbox transition.
3. Inject the identical `eventId` twice; prove one DB row and no duplicate state
   transition/suppression.
4. Inject hard-bounce, complaint and subscription fixtures linked to controlled
   outbox rows; prove HMAC/identity correlation and mismatch rejection. Complaint
   and hard bounce suppress `all`; apply the product-approved subscription scope.
5. Force a retriable handler failure: five bounded retries, one YMQ DLQ item, then
   controlled replay and deletion with no duplicate business event.
6. Prove malformed/oversized/unknown types fail closed without logging recipient.
7. Enable the Postbox destination, send one message to the controlled
   `info@kgd80.ru` canary, record the real MessageId, and observe provider `Send`
   plus `Delivery` events through YDS/function/Supabase with empty DLQ.
8. Confirm SPF, aligned DKIM/DMARC and recipient placement/headers. One successful
   event-consumer canary does not by itself waive gradual warm-up.
9. Exercise the application transactional kill switch and destination-disable
   rollback. Only after operator review may a bounded transactional canary switch
   be considered; no recommendation/NotiSend state changes here.

## Rollback

1. Disable the application transactional/global switches first.
2. `PUT` the Postbox destination to `Enabled=false` to stop new provider events.
3. Leave the trigger running to drain valid records within the 4h window. If the
   consumer is harmful, pause the trigger, deploy/retag the last known-good version,
   then resume before retention expires.
4. Preserve and inspect the YMQ DLQ; replay only through the idempotent handler.
5. If abandoning the feature, delete the destination, then trigger, topic and DLQ;
   remove the two dedicated SA bindings/accounts and dedicated secrets. Disable YDB
   deletion protection only after the topic is empty and evidence is retained, then
   delete the dedicated DB.
6. Never delete or recreate the verified Postbox identity/configuration, DKIM/DNS,
   existing inbound resources, general KMS keys, or SpaceWeb mailboxes as rollback.

## Risks and operator gates

- `yds.admin` is vendor-documented for a trigger but wider than read-only; isolate it
  on the new trigger SA and review it after Yandex resolves its role inconsistency.
- The Postbox notification payload contains plaintext recipients and headers. It
  exists transiently in YDS/function memory only; logging and Supabase persistence
  must remain HMAC/metadata-only.
- Four-hour retention makes alerts and operator response mandatory; DLQ retention is
  14 days for controlled recovery.
- Trigger DLQ is supported, but replay, poison-event classification, event dedup and
  semantic outbox-lag detection are application responsibilities.
- Enabling only a stream/destination without the consumer, RPC, suppression and
  alerts is prohibited. Enabling application send before the live event canary is
  prohibited.

## Audit commands executed

- Read-only `yc` list/get/access-binding/version/key-metadata operations with the
  explicit email folder.
- Read-only Postbox REST GETs using a short-lived impersonated SA IAM token.
- Official Yandex documentation and local CLI help for current limits/flags.
- One console navigation attempt hit Yandex CAPTCHA and was closed without login or
  mutation; it was not used as evidence.

No password, API/static key, IAM token, secret payload, email body, or recipient list
was printed or committed.
