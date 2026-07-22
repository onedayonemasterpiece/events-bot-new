# Listing media crop evidence

The files under `site/public/assets/listing-media/` are deterministic, source-faithful still crops used only by list cards when the canonical low-resolution video thumbnail cannot satisfy the desktop raster/crop contract.

- Hufen source: public Telegram post `https://t.me/kulturnaya_chaika/7974`, original 1080×1920 video downloaded 2026-07-18 through the approved E2E human session. Runtime still is decoded frame 0, crop `[0,446,1080,720]`, 3:2. No generative editing.
- Red Tent source: public Telegram post `https://t.me/meowafisha/7913`, original 1080×1920 video downloaded 2026-07-18 through the approved E2E human session. Runtime still is decoded frame 0 before the title overlay appears, crop `[0,446,1080,720]`, 3:2. No generative editing.
- `listingMediaOverrides.json` keys the review by canonical source URL, never by event id. The same reviewed replacement therefore applies to every event using the same source bytes.
- The two still-only review entries retain their CDN source and only add bounded crop/focal evidence after manual visual inspection.

The downloaded videos and diagnostic frames remain ignored under `artifacts/codex/listing-date-v13-20260718/` and are not committed.

## Rejected V13 event-specific substitution

V13 temporarily associated event `3794` with a 1024×683 Cathedral article photo only inside the listing renderer. That was source-grounded, but it was still a manually selected event-specific result and therefore did **not** prove automatic generation from the event's existing media projection. V14 removed that association. V15 clarifies that no additional general crawler/source adapter was planned for this listing task: runtime presentation consumes the existing approved event media/Smart Update inventory and must never key alternate media by event ID. The event-relevant canonical 300×174 asset is an explicit no-upscale last resort; `no_event_relevance` still fails closed to the neutral fallback.
