# Surface mapping results

- Telegram: aiogram 3.29 `InputRichMessage` with `<tg-slideshow>` and 3–10
  `tg://photo?id=...` media references.
- VK: reuse `upload_vk_photo_bytes` and `post_to_vk(..., carousel=True)`.
- Static: extend the production preview exporter with
  `artist-arrivals.json`; validate through `artistArrivals.ts`; render through
  `ArtistArrivalsHeroTalk.astro` in the preview homepage.
- Promo: new `artist_arrival_digest` and `artist_arrival_hero` activities with
  separate surfaces/ledgers. Campaign starts draft and activities disabled.
- Media: event association is not person identity. Auto publication requires
  verified artist↔photo identity, an allowed rights status and explicit
  rights provenance; shadow review gets deterministic text-only cards.
- Production homepage limitation: `origin/main` still exposes a noindex
  placeholder, so this branch integrates the component into preview and leaves
  production homepage promotion as an explicit release gate.
