# Listing media crop evidence

The files under `site/public/assets/listing-media/` are deterministic, source-faithful still crops used only by list cards when the canonical low-resolution video thumbnail cannot satisfy the desktop raster/crop contract.

- Hufen source: public Telegram post `https://t.me/kulturnaya_chaika/7974`, original 1080×1920 video downloaded 2026-07-18 through the approved E2E human session. Runtime still is decoded frame 0, crop `[0,446,1080,720]`, 3:2. No generative editing.
- Red Tent source: public Telegram post `https://t.me/meowafisha/7913`, original 1080×1920 video downloaded 2026-07-18 through the approved E2E human session. Runtime still is decoded frame 0 before the title overlay appears, crop `[0,446,1080,720]`, 3:2. No generative editing.
- `listingMediaOverrides.json` keys the review by canonical source URL, never by event id. The same reviewed replacement therefore applies to every event using the same source bytes.
- The two still-only review entries retain their CDN source and only add bounded crop/focal evidence after manual visual inspection.

The downloaded videos and diagnostic frames remain ignored under `artifacts/codex/listing-date-v13-20260718/` and are not committed.
- Organ Assemblies event `3794`: the official calendar supplied only a 300×174 still. For the desktop listing only, V13 uses the 1024×683 no-OCR rehearsal/student photo from the Cathedral's official 2026-07-14 article about the XI International Organ Assemblies; that article explicitly includes the 18 July participants' concert. Source image: `https://sobor39.ru/upload/medialibrary/061/1zccppmfa7j4v42ij0cv3ft11bi5op3x.jpg`. This is a source-grounded alternate, not generative replacement; canonical/detail media remain unchanged.
