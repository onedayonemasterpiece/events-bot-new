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
- Owner clarification remains pending about which group appeared stopped; both inspected groups were live.
