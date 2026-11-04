# INC-2026-11-04 Cherryflash cross-year date loses year

Status: open
Severity: sev2
Service: Cherryflash ECO story date label
Opened: 2026-11-04
Registry: inc_7f1df38542bd1253b025d0be
Related: INC-2026-11-02-intake-quota-vk-media

## Summary and impact

Owner screenshot of today's ECO-derived Cherryflash shows «Открытие весенне-летнего сезона», «1 МАЯ • 12:00» in November, looking like a stale event. The exact event is 10315, used by published ECO session 1562, item 11025, partner story 1863. Source and canonical date are correct; the visible year was discarded. No evidence of a past event retimed by the current recovery.

## Evidence and timeline

- Actual source https://t.me/kldzoo/8103, published November 2 at 14:55:45 UTC, explicitly says the fountain will reopen May 1 at 12:00 in the spring-summer season of **2027**. Independently read through local role-safe Telethon; not inferred from canonical writer text.
- Event 10315 was added November 3 at 00:19:29 UTC, after the earlier PR761 routing deployment and before the later Pro-reviewed recovery deployments. Date 2027-05-01, time12:00, date provenance source_text, not inferred.
- Today's ECO session 1562 selected it at 11:11 UTC via popularity source window `3d`; filter matched the zoo/heritage subject. It was not selected through the ECO event-date recall fallback. Canon explicitly permits clearly future popular announcements; no upper event-date horizon exists. Do not invent a new cutoff as an incident shortcut.
- `_format_display_date` in scripts/render_cherryflash_full.py converts full ISO date to day/month and unconditionally drops the year. Actual before control produces `1 МАЯ • 12:00` for2027-05-01.
- The selector file SHA256 is 985e0dd31551c2fabdf0820631e728f324575bf5a0bc0b723feb27e77fe47515 and renderer SHA256 6aebd81435f96957ae16d10c795ab05cb09a2431eab00727ebef5d48866f9ece in both earlier routing release cdd56170713448258d81d4e05c9a2d5d3f85a70a and deployed e6bca5539de7feab4cea2e06c3e980aec0e89502. Current ingestion fixes did not change these selection/rendering files.

Retained evidence is in the active intake recovery directory: cherryflash-zoo8103-live.txt, cherryflash-eco10315-evidence.json, cherry-year-before.log, cherry-year-after.log and cherry-year-layout-control.png. The layout control uses the real scene text renderer with an explicitly synthetic solid poster; it is not the actual public video, a Blender render, or a publication.

## Root cause and classification

Mechanical information loss in the existing display formatter: a full known calendar year is removed. Event extraction/date provenance is correct. Fresh-source popularity and event occurrence date are different concepts. Broad future-date selection is existing product behaviour, not a regression introduced by this recovery. The owner was asked separately whether to change that horizon; no answer or new cutoff is assumed.

## Prevention and verification

Retain year when event year differs from rendering year. Same-year labels keep existing appearance. Two added lines in the existing formatter; no state, queue, retry, selection rule, date rewrite or renderer fork. Existing shipped runtime bundle copies this tracked script. Regression fails before, then all23 full-render/audio tests pass, including a same-year negative control. Actual existing frame layout shows `1 МАЯ 2027 • 12:00` legibly.

### Treat as regression guard when

Changing Cherryflash display dates, scene payload dates, future-event selection, or the shipped renderer bundle.

### Affected surfaces

ECO/base/partner Cherryflash primary event scene date labels; scripts/render_cherryflash_full.py and its existing runtime copy. Guide promo formatter is unchanged.

### Mandatory checks before deploy or closure

- [x] Exact live source and canonical/session provenance established.
- [x] Causal comparison to pre-recovery code and record chronology.
- [x] Differentiating next-year case fails before and passes after; same-year unchanged.
- [x] Existing render/audio suite:23 passed; real text-layout control viewed.
- [x] Required CI green, merge exact main, qualify image, safe Events-only delivery.
- [x] Verify deployed renderer/bundle and year label; preserve active intake and remote jobs.
- [ ] Resolve today's public-surface follow-up explicitly. Existing video has not been edited/deleted/reposted; no replacement or canary publication is claimed.

## Release evidence and follow-ups

DELIVERED in combined main44d183696 at17:56 UTC, immutable ca0c948dca38. Actual shipped formatter returns 1 МАЯ 2027 • 12:00 and unchanged same-year labels. Current public story remains unchanged; subsequent real render acceptance is separate. Normal guide18:10 UTC, TG21:40 UTC, media/source-parser checks and promo remain active. Owner horizon preference is optional and separate; keep existing eligibility until a decision arrives. Incident stays open.


### Chronology correction

The first commentary incorrectly said the card predates the first recovery
deployment. PR761 routing had already been delivered before Event10315 was
created. The November3 00:19 card predates the later Pro-reviewed recovery
sequence, not all routing work. No counterfactual claim is made that the
card would have existed without restored ingestion. Correct source-backed
2027 extraction and unchanged renderer/selector establish the observed
mechanical cause without that mistaken chronology.


## Qualified exact-main image, 2026-11-04 16:43 UTC

PR811 and PR812 merged after all required CI passed. Exact main
`d0518b81d92fa3d3f8d8592d930a55f644382c5f`, image
`intake-cherryflash-year-20261104`, immutable
`sha256:dfc579095e8a23afc69525c59903afcba092f35f65532aaad01233ce733c6920`
passed full 4442-file verification, actual runtime ancestry and unchanged
dependency inputs. Initialized isolated preflight preserved source/date/identity
contracts, models/cinema and scheduler behaviour; actual shipped formatter
returned cross-year2027 and unchanged same-year labels. Wrong SHA refused,
HTTP isolation passed, own preflight container removed. No provider or Event/Job
writes in the display control. No additional model call is needed for this
mechanical display-only change.

NOT DELIVERED: ordinary automatic VK11635 is active from16:30 UTC. In the
16:41 snapshot six posts are confirmed_no_event and carrier88740/source26756
is processing; parse EVENTS_FOUND is not Smart Update acceptance. Do not
interrupt healthy intake or run a parallel source CLI. Prepared guarded host
cutover/verify scripts expect current e6/ba65 and intake-date-boundary tag.
Current story and valid canonical Event10315 remain unchanged. Detailed
qualification receipt retained in cherryflash-year-release-receipt.json.


## Guarded delivery follow-up, 2026-11-04 17:09 UTC

Ordinary VK11635 is now terminal success with complete source/child/carrier
receipts, one creation and zero technical refusals. The prepared year-label
cutover was attempted once and refused BEFORE mutations because normal media
review145971 was active. Actual runtime remains healthy e6bca5539, ready=true,
DB ok. Candidate d0518b81 remains fully qualified but NOT DELIVERED; no rebuild
or repeated successful qualification required. Retained evidence:
cherryflash-year-cutover-1709.log and cherryflash-year-runtime-health-1710.txt.
Preserve healthy media work, then use the same guarded release and live verify.
Current ECO story is unchanged and no new selection horizon was adopted.


## Combined delivery verification, 2026-11-04 17:58 UTC

PR812 year-label fix delivered together with PR814 media-producer correction
from exact main44d183696ff30507b151d59b829f15614256d22a, immutable
sha256:ca0c948dca380e43f324b017e326a571f086bb5ed2a6d54da4aa772ca6686a25.
Full4442-file proof, initialized isolated preflight, actual renderer year and
same-year controls, wrong SHA and HTTP isolation passed. Live formatter and
runtime checks pass; ready=true, DB ok, models/cinema/identity unchanged,
Kotopogoda unchanged. No normal source import or MCP intake interrupted.
One durable media-only claim crosses restart under the existing recovery
contract and remains tracked by the intake incident. No public canary, story
replacement, event retiming or new selection horizon. Existing public ECO1562
story not edited; that follow-up and future actual render observation remain.
Older d051 candidate was never delivered and is superseded, no rebuild needed.
