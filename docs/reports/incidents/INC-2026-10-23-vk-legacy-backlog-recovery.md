# INC-2026-10-23 VK legacy backlog recovery

Status: mitigated; monitoring first scheduled automatic run and residual backlog
Severity: sev3
Service: events-bot VK Afisha / Announcements, RunCoveer
Opened: 2026-10-23
Closed: —
Registry: inc_6b3a02a987dff069834c15a0
Related incidents: INC-2026-09-24-vk-afisha-publication-recurrence; registry inc_4068c4db921f5223501b1140

## Summary and impact

Owner reported another publication stop and supplied the local replacement credential
name VK_USER_TOKEN7 if needed. Current outage was not reproduced: the existing
publisher token reads both communities and the native postponed queue successfully.
Afisha post 12702 was live at 17:03 UTC on October 23; Announcements evening daily
2208 was live at 18:12 UTC. Token rotation was not needed. The later volume investigation below identified a separate missing retry consumer and delivered a code release.

The bounded production audit found 75 active canonical event rows ending October 24
or later without a managed Afisha URL: 23 September containment rows, 49 missing-media
errors, one July media pause, and two rows without a VK job. These are event rows,
not 75 verified distinct missing posts; some describe related sessions or duplicates.
12 of the 23 September rows have Telegram publication URLs. No future event added
since October 14 with a Telegram URL lacked a managed VK URL in this audit.

## Root cause and contributing factors

Mechanical backlog state: 23 jobs retain done / contained_no_replay_pre_token_rotation_20260919,
although the future event has no managed VK publication. September flood containment
prevented blind repeats but did not subsequently recover these rows. This evidence does
not establish a new token failure. Missing-media rows are a separate delivery guard,
not authentication failures; their source/media quality was not investigated here.

## Mitigation and verification

Used the deployed job_sync_vk_source_post path for three selected events, with explicit
publish_date values in an isolated recovery process. No serving process settings changed.
Exact local Telethon reads confirmed dates, places and images; original CDN images were
visually reviewed. Authenticated native queue and service-token wall.search checks
excluded corresponding existing announcements before dispatch. Each result was read
back from wall.get(filter=postponed), requiring the exact timestamp and a photo, and
its Event URL and existing JobOutbox result were reconciled.

| Event | Native VK post | October 24, Europe/Kaliningrad |
| --- | --- | --- |
| 9080, Под каблуком Showcase & Battle | https://vk.com/wall-231920894_12703 | 10:05 |
| 9082, Под каблуком Home Edition | https://vk.com/wall-231920894_12704 | 13:05 |
| 9127, Аура Шоу | https://vk.com/wall-231920894_12705 | 16:05 |

Independent final queue readback found six posts: the three original queued items and
three recovered posts, each with one photo. Health remained ready=true. Scheduling
is verified; future public publication is not yet observed. Recovery performed no
mass replay, token replacement or application release. Existing media/publication
rules, including the enabled engagement visual behavior, were retained.

## Automation contract

Treat as regression guard when changing VK containment, backlog catch-up, token
rotation or scheduled delivery. Affected surfaces: JobOutbox.vk_sync, Event managed
VK URL/hash, native Afisha wall and postponed queue, credential selection.

Mandatory checks: verify real runtime health, existing publisher operation, exact
managed owner, active future event, source date and image identity, duplicate absence,
explicit future native time, native photo/readback, and durable Event/outbox receipt.
A done job is not publication proof if its result records manual containment. Never
repeat a recovery with an uncertain outcome; reconcile the original provider effect.

Required evidence: retained /home/dev/artifacts/events-bot-new/20261023T181930Z-vk-recovery-20261023/;
recovery.py and recovery.log record the bounded plan, previous rows and exact verified
receipts. The runtime image was runcoveer/events:kaggle-retention-20261020-v2. No code
release or regression test run was necessary for this data-only recovery.

## Follow-ups

- Review the remaining 17 September-contained future rows only with source/media and duplicate evidence.
- Investigate missing-media rows separately; do not bypass the media guard or replay duplicate sessions.
- Owner confirmed Afisha and clarified low volume, rather than total outage; see follow-up below.

## Low-volume follow-up, October 23

The initial fresh-post check did not establish pipeline throughput health. Owner
clarified that Afisha produced too few announcements and suggested upstream failure.
Using Europe/Kaliningrad days, canonical additions were 30 on October 21, 18 on
October 22, and 15 so far on October 23; 14 of the latest 15 have managed VK URLs.
The earlier UTC-day count of eight additions on October 23 must not be presented
as the Kaliningrad-day total. Current wall readback found 38/27/13 visible posts
on those days; public cleanup means this is not an immutable publication ledger.

Import impairment is confirmed independently of VK authentication. October 23 VK
import runs processed 60 carriers, reported 15 technical failures and created four
events. Nightly Telegram run 10978 processed 86 messages, 61 with typed candidates,
and reported 32 terminal-error carriers, 11 created and 28 merged events. These
counts have different grains and are not interchangeable with missed publications.

Runtime Google AI logs show provider 429, 500 and 503 failures, plus truncated
structured responses. Supported identity retries are still persisted by current
Smart Update, but SMART_UPDATE_RETRY_WORKER_ENABLED is unset and the scheduler
explicitly skips the worker. There are 91 RETRY_SCHEDULED candidate states created
since October 21, with overdue timestamps. Global backlog includes over 2,000 older
identity retries; enabling an unrestricted legacy drain is not a bounded mitigation.

Exact local Telethon reads verified meowafisha/8937 (October 25, Sutyagin meeting)
and mesto_sily_bar/2032 (October 30, Black Ball). Both lacked links from these exact source URLs and
remained first-attempt retry candidates 23655 and 23648. The first source was
subsequently matched to an existing event; source-link absence alone was not
proof of a missing canonical event. The third inspected source,
locostandup/4283, concerns an already represented event 9393; no duplicate recovery
was attempted. A single normal-facade replay for each of the two unlinked candidates
is recorded in retained replay-two.py/log, with outgoing publication jobs fenced
to October 24. No semantic guard, identity decision or forced-distinct flag is bypassed.

Follow-up remains open: provider availability and the mismatch between newly durable
identity retries and the disabled legacy worker require a scoped repair. Automatic
volume recovery has not been established, and the incident is not closed.

### Bounded replay result and health correction

Candidate 23655 merged normally into existing event 9803, «Культурные гастроли:
Что такое красота?», October 25 at 15:30, whose managed VK post is already 12260.
No duplicate was created. Candidate 23648 returned RETRY_SCHEDULED again with
identity_final_retry:distinct_not_grounded. No identity guard was bypassed and
neither replay created another native publication. The three earlier October 24
scheduled recoveries remain the only added VK posts from this task.

During this replay, the serving outbox cycle logged one SQLite OperationalError
(database is locked, autoflush UPDATE joboutbox 117355 at 18:38:58 UTC). The probe
reported HTTP503, ready=true, with the recent worker error as its sole issue;
scratch/disk/DB checks remained okay. Further replay stopped after the two planned
attempts and the standalone runner exited. This is a verification impairment,
not evidence that upstream volume was repaired. Preserve the replay and incident
open for a bounded follow-up; no unrestricted worker enablement or model switch
was attempted.

Final runtime readback after runner exit: HTTP200, ready=true, issues=[], tick age 4.9s; no restart required. Event9803 existing VK/TG refresh jobs are fenced to October24 08:05+02; no new event IDs above10070 were created. Candidate23648 remains retryable after its second attempt.

## Corrective release: bounded recent identity retry worker

The scheduler now automatically services the typed durable identity outcomes
already emitted by Smart Update, instead of silently leaving them without a
consumer. The automatic query requires a supported identity reason, creation in
the last three days, a valid active occurrence/end date and remaining retry budget.
It never selects accepted/product-rejected rows, malformed packets, context-only
intents, active leases or the older historical backlog. Claims retain the existing
transaction/lease protocol; replay uses the same Smart Update facade and semantic
identity gates. No forced-distinct creation or provider/model substitution was added.

Automatic work is capped at one candidate per 15 minutes and joins the existing
heavy-operation gate. The explicit unrestricted legacy drain remains default off.
For this rollout the first automatic run is delayed to October24 06:00+02 by the
non-secret SMART_UPDATE_IDENTITY_RETRY_START_AT setting. Future ordinary restarts
retain the existing scheduler behavior after that initial floor has passed.

Mandatory regression evidence adds recent/future/ongoing positive controls and
old/past/invalid/budget/accepted/rejected/context/active-lease negative controls,
plus default scheduler registration and timezone-aware first-run floor. An old
scope-review test fixture used August2026 and now hit the past-event gate before
its intended missing-date check; only that fixture's dates were advanced to2099.

## Delivered release and final verification

PR #750 merged with all four primary CI jobs green (run40007309816), plus the
additional contract gates. The deployed source is exact merged-main
102c12b89e02026f3a27941af81110821dbf8578. Its former running source8a971582 is a
proven ancestor. Immutable image runcoveer/events:identity-retry-20261023 has ID
sha256:ed3656c7e4e72a127cc3e80031f797eb52c454551ebeaf06d65441d2ee396ad6.
All4422 tracked files, hashes and executable bits were verified with explicit
runtime checks. Dockerfile/dependencies were unchanged. Local focused regression
suite:120 passed; final scope/scheduler checks:4 passed; network-disabled image
preflight:4 passed. Production Python optimization disables assert statements, so
release and final provider verification use explicit raising checks instead.

Events-only cutover waited until no outbox effect was running, then recreated
only Events. Kotopogoda retained containerbc2f7488d7e1a59201869be0af0f59841accdabd2ece24c4ad74479fc0e5e15a.
New Events container0cc2fc4a34bc196e3ff01b623fdd79c70439eb1b0319add34c4a49be5c4cc3d2
reported HTTP200, ok=true, ready=true, issues=[]. SQLite quick_check returned ok
in19.39s. Live startup logs prove scope=recent_future_identity, batch=1,
interval=900, registered next_run=2026-10-24 06:00:00+02:00. The unrestricted legacy
worker is explicitly off. No token/model routing change was made.

Three additional Telegram/photo/date-verified events were recovered through the
same deployed VK publisher, after wall-search duplicate checks:9142 «Люблюнемогу»
(native12706, October24 08:15+02),9084 masterclass «Под каблуком» (12707,12:05),
and9086 The Fazbear Night (12708,15:05). Independent strict native queue and DB
readback after restart verified all six task recoveries, each with one photo,
exact requested date/time, managed Event URL and done JobOutbox receipt. Native
queue total is9: three pre-existing items and six recoveries.

Retained final evidence: live-verification.json, retry-registration.log,
sqlite-check.json, cutover.log, image-build.log, image-preflight.log,
release-source-receipt.json and recovery-more-receipts.json under the incident
artifact directory. Automatic processing is now deployed and scheduled; its
first actual invocation is intentionally tomorrow and is not yet claimed as
observed. Google provider availability and the residual media/identity backlog
remain monitoring limitations; no background agent monitoring is promised.


## Owner-directed rollback of unsolicited automation

The owner requested removing the newly introduced identity retry consumer and
using VK's native postponed posts only. Implementation commit 63d52f56a is
reverted, including scheduler registration, candidate-selection additions, new
tests and configuration examples. The unrelated future-date test fixture repair
is retained. Diagnostic history above records a superseded release, not a plan
to re-enable it. The unrestricted legacy drain remains default off.

Deploy a new descendant of 102c12b89 from merged main as
`runcoveer/events:revert-identity-retry-20261023`, removing the three runtime keys
SMART_UPDATE_RETRY_WORKER_ENABLED, SMART_UPDATE_IDENTITY_RETRY_ENABLED and
SMART_UPDATE_IDENTITY_RETRY_START_AT added by this task. Verify no retry consumer
is registered, readiness, unchanged Kotopogoda and all six native VK postponed
post IDs 12703–12708. Do not delete or replay those provider-owned posts. The
processor has not run; its first invocation was scheduled for October 24.
The upstream throughput diagnosis remains separate and does not authorize more
recovery automation.
