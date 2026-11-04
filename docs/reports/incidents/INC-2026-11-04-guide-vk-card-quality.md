# INC-2026-11-04 Guide VK schedule card reduced to 320×400

Status: open
Severity: sev3
Service: guide visual digest / VK photo upload
Opened: 2026-11-04
Closed: —
Owners: Codex
Registry: inc_f5d0a79f3a7ffa6073158f3c
Related incidents: inc_9b10802ccbe05795372dd983 (promo spacing)

## Summary and impact

Readers of edition 328 saw blurred schedule text in VK. The renderer produced
1080×1350 JPEG (181054 bytes), but the published photo's largest size was
320×400. This is a mechanical media transport problem, not event semantics.

## Evidence and timeline (UTC)

- 07:05: guide light run 11617 skipped because the heavy gate was occupied.
- 08:50–08:51: a separate visual catch-up built edition 328 from saved
  occurrences 711 and 699. It did not scan the newly registered sources.
- 08:50:58 and 08:51:01: VK returned empty photo results for the original
  JPEG and first reduced variant. The 44952-byte second variant succeeded.
- 09:00:38: published wall post became https://vk.com/wall-238875824_378;
  photo-238875824_457239682 exposes maximum size 320×400.
- Investigation: the same 1080×1350 pixels re-encoded as full JPEG 4:4:4
  failed, while PNG (505256 bytes) uploaded successfully through the normal
  bytes uploader. Asset receipt: photo868977531_457264569.

Retained evidence:
`/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104/`
contains original-render.jpg, vk-published.jpg, vk-photo-evidence.json,
upload-probe-receipt.json and the exact bounded inspection/probe scripts.

## Root cause and correction

The existing upload fallback reduced pixel dimensions until VK accepted the
photo, sacrificing schedule legibility. Full-resolution PNG is accepted for
this exact card. Convert VK schedule payloads to PNG before the existing
uploader, and make its multipart MIME/extension match the attempt's format.
No new retry, queue, flag, mode, provider or schedule is needed.

## Automation contract

### Treat as regression guard when

Changing guide visual VK media serialization or the shared bytes uploader.

### Affected surfaces

`guide_excursions/visual_digest.py`, `main.upload_vk_photo_bytes`, VK wall photos.

### Mandatory checks before closure or deploy

- Distinguishing test: the actual VK publication boundary receives a PNG
  card at 1080×1350 and attaches its accepted asset to the wall request.
- Shared VK upload retry/source-image tests still pass.
- Live provider readback confirms the accepted PNG asset retains full size.
- Required CI and merged-main release/ancestry checks pass before deployment.
- Read back the affected normal publication path after deployment; a unit
  test or accepted asset alone does not prove deployed future behavior.

### Required evidence

Test results, immutable deployed SHA/image, full-size VK photo readback and
the original wall-post identity. Preserve existing publication receipts.

## Related observations and follow-up

New sources 23–30 have not been scanned yet. The next normal light slot is
13:20 Kaliningrad; owner-requested source-by-source import control is armed
for 14:20. Missing new guides in edition 328 does not mean no future tours.
Afisha repost is prepared for 13:26 local. Announcements activity has a
three-day cadence and last posted November 3; November 4 is not eligible.
Do not duplicate posts or collapse these independent schedules.
