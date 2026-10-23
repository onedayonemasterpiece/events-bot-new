# INC-2026-10-23 VK legacy backlog recovery

Status: mitigated; residual backlog open
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
2208 was live at 18:12 UTC. No token rotation, application change or deploy was needed.

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

- Review the remaining 20 September-contained future rows only with source/media and duplicate evidence.
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
