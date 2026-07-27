# Event sharing and generated images

> Status: **preview client fallback implemented; durable production asset pipeline missing**.

## Current contract

The event page attempts native Web Share with image file, text and canonical
URL. For a source-classified `visual_only` photo it first composes a 1080×1350
PNG with the actual KenigEvents wordmark, event title, date/time, place and
admission. OCR/document and unknown media are never cropped or repainted: they
use the original image. If source fetch/composition/file sharing is unavailable,
the existing generated text-first 1080×1350 fallback is used, then text/URL
copy. Open Graph metadata supports link previews.

## Production requirement

Offline/server generation must create deterministic assets for at least:

- 1200×630 Open Graph;
- 1080×1350 vertical share;
- 1080×1080 square share.

Assets are keyed by event id plus content/media version, published before page promotion, regenerated on material event/media changes and removed/marked on lifecycle changes.

## Acceptance

- same-origin/CORS-safe source images;
- OCR-safe and focal/face-aware framing;
- brand/title/date/time/location/CTA remain readable;
- real Telegram, VK and MAX checks on Android/iOS;
- no stale image after cancellation/reschedule;
- provider/app fallback and latency/error metrics;
- share failure never blocks ordinary navigation.

The same `data-share-image-text-mode` and base-aware brand asset contract is
required on authored event/detail/card controls and cards hydrated later by the
Search/continuation runtime. A page-local share implementation or a raw
`visual_only` photo without event identity blocks release.
