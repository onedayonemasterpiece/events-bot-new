# INC-2026-11-09 VK photo size rejection loses complete source evidence

Status: resolved, verified in production on 2026-11-09. Severity: sev2.
Service: VK intake / poster OCR.
Opened: 2026-11-09. Registry continuation: inc_f5d0a79f3a7ffa6073158f3c.

## Summary and impact

Source monitoring of Mutilina post wall345796874_1373 ended FAILED_TECHNICAL
because one of eight photographs was rejected by the downloader. This is a
mechanical byte-budget/rendition issue, not a semantic no-event verdict.
Waiting for the unchanged oversized URL cannot recover that source.

## Detection and timeline

- November8 08:27:47 UTC: vk.download_photo_failed error=file too large.
- November9: packet26670 / inbox88635 manifest shows8 attachments,7 OCR,
  one unavailable and evidence_complete=false.
- Actual source URLs: 1920x2560 rendition5801914 bytes,1280x1707 rendition1736449
  bytes,1080x1440 rendition1284493 bytes. All returned HTTP200. The configured
  downloader limit is5242880 bytes. This is not the unrelated Golubkov404.

## Root cause and correction

The source envelope selects the largest photo and the downloader discards it
on its existing byte guard. VK advertises smaller renditions in the same URL's
`as` field and API sizes, but the downloader did not use them.

On exactly `file too large`, request one immediately smaller rendition declared
by the VK CDN URL, then apply all existing byte, MIME, JPEG and length checks.
Only `.userapi.com` URLs with declared sizes and a current size participate.
No inferred dates/venues, new state, queue, worker, global budget increase or
broad retry. HTTP404 and a second oversized result remain fail-closed.
Production diff budget35 net lines; actual25. Existing callers in VK intake,
popular review and poster backfill keep the same downloader interface.

## Automation contract

Treat as regression guard when changing VK source media collection, photo byte
limits, `_download_photo_media`, or evidence-completeness accounting.

Mandatory checks before closure/deploy:
- Distinguishing actual downloader test fails on baseline, passes after repair.
- Header and streaming limits, one fallback maximum, missing size metadata,
  non-VK host and HTTP-error controls remain fail-closed.
- Actual oversized source download passes via the declared smaller rendition.
- Re-run the exact source through normal VK intake, OCR and Smart Update after
  delivery; inspect all8 attachments and terminal receipt, without manufacturing
  a no-event decision or manually inserting an Event.
- Exact merged main, current runtime ancestry, source proof, isolated startup,
  Events-only cutover, live health and actual source receipt.

Required evidence: retained quality directory
`/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104`:
`vk-photo-fallback-tests.log`, `vk-photo-real-control.log`, subsequent release
and packet26670 source-replay receipts. Do not treat transport-only validation
as a complete source-import replay.

## Resolution and follow-ups

PR #879 passed all four CI checks and merged as
`486abac3e8cf8a6ec3902f4168fc8c89526c19df`. Delivered Events-only image
`runcoveer/events:vk-photo-size-20261109`, immutable
`sha256:efe0c1cb25584a1d428c9de3fed411b398f916e40435b4032e87f87f610e1009`.
Running `70a3d33fccf77840373116d3869ad4e793707904` was proven an ancestor;
all 4464 tracked source files matched. Initialized isolated preflight without
network, inherited behavior checks, and live readiness passed. Cutover observed
no active operations, MCP effects or background jobs. Kotopogoda stayed unchanged.
The rollback Compose and prior immutable image are retained in the release folder.

Actual ordinary source catch-up ops 11774 completed at 11:49:57 UTC. Inbox 88635
and packet 26670 now have `confirmed_no_event` /
`CONFIRMED_NO_EVENT:SERVICE_OR_RENTAL`. Attempt 12430 has eight attachments,
eight included OCR blocks, zero unavailable attachments, no truncation, and
`evidence_complete=true`. The source describes individual tour booking without
a dated invitation; no Event was fabricated. Prior failed attempt 12342 is
preserved with its seven-of-eight incomplete manifest.

The same standard handler also resolved the two long-pending December posts:
Guideva inbox 88324 / packet 26405 / ops 11772 created Event 10741; 2meterguide
inbox 88692 / packet 26724 / ops 11773 merged into that same event. Both carriers
and packets are imported / EVENTS_RESOLVED with complete evidence. Independent
SQL readback confirms both exact source anchors and the canonical event:
«Новогодняя ярмарка на острове Канта», 2026-12-24 through 2027-01-10, free,
unknown opening time left blank.

Source anchors: https://vk.com/wall-196142551_21602 and
https://vk.com/wall-208865971_151400. No date or priority was edited; no parallel
scanner, scheduler or manual Event payload was introduced. Already accepted
Boyko and Garry records were not replayed. Golubkov packet 26877 with an
unavailable HTTP 404 video thumbnail remains a distinct unresolved source case.

Additional retained evidence: `mutilina-photo-catchup-20261109.log`,
`new-sources-targeted-catchup-20261109-current.log`,
`new-source-acceptance-readback.log`, `vk-photo-inherited-preflight.log`,
`vk-photo-cutover.log`, and `vk-photo-live-health.log`. The temporary isolated
preflight container and its initialized database were removed after verification.
