# Event sharing and generated images

> Status: **preview client fallback implemented; durable production asset pipeline missing**.

## Current contract

The event page attempts native Web Share with image file, text and canonical URL; falls back to a generated 1080×1350 canvas image; finally falls back to text/URL copy. Open Graph metadata supports link previews.

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
