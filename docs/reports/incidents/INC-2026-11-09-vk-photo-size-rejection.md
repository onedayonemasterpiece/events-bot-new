# INC-2026-11-09 VK photo size rejection loses complete source evidence

Status: open. Severity: sev2. Service: VK intake / poster OCR.
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

Pending actual-main delivery and standard source catch-up. Preserve failed
attempt history. Do not replay already accepted Boyko children or completed
Garry scan; this correction only addresses photo transport. Remaining missing
assets such as Golubkov's404 are separate and must not bypass evidence guards.
