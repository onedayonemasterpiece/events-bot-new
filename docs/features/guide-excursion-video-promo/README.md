# Guide Excursion Video Promo Activity

> **Status:** design draft / next step is first test render  
> **Scope:** promo-campaign activity that renders a separate vertical video for one guide excursion and publishes it to VK story, VK wall/body and Telegram post.

## Reference

Canonical local reference video is stored next to this feature so it is not lost:

- `docs/features/guide-excursion-video-promo/reference/VID_20260708_142351.mp4`

Reference facts:

- 576×1280 vertical MP4, about 6.6 seconds, 30 fps, with audio.
- The captured VK/phone chrome is **not** part of the design system; use only the content motion pattern.

What we take from the reference:

1. A stack of photo cards in shallow 3D/depth perspective.
2. Cards turn/slide like a carousel, with the next/previous cards visible behind the active card.
3. The final card becomes the hero: it moves closer, grows larger and settles as the main readable CTA card.
4. A strong short headline sits above the cards and changes/settles with the card sequence.
5. The motion is lightweight, clean and social-story-native: fast enough for stories, not a long cinematic scene.

What we do **not** take from the reference:

- phone status bar / VK story UI / bottom button chrome;
- exact children/family/heart stickers;
- low-resolution screenshot artifacts;
- generic template text.

## Product goal

Create a new promo-campaign activity for guide excursions: one separate short ролик that sells a concrete excursion through its own photos, then ends on a large CTA card with date/time and booking contact.

This is different from the daily CherryFlash inserted guide scene:

- CherryFlash guide scene = one card inside the daily mixed CherryFlash video.
- Guide excursion video promo = standalone video/post dedicated to one excursion.

## Eligibility

The activity may start only when all conditions are true:

1. The guide occurrence is in the future at render time.
2. It is not sold out / unavailable / cancelled.
3. There is at least one free place (`seats_count > 0` or equivalent parsed availability).
4. It has at least **2 usable excursion photos**.
5. Photos are real content photos for the excursion/place/route, not only avatars, source logos or organization profile pictures.
6. The same occurrence is not published by this activity twice in the same local day.

Personal guide avatar is useful but is not the gate for this activity: the carousel is photo-led. If the production campaign should remain human-guide-only, add a campaign/activity config flag such as `require_personal_avatar=true` rather than hard-coding it in the renderer.

## Input data contract

A render job needs a normalized payload:

```json
{
  "occurrence_id": 342,
  "title": "Домашняя прогулка по Железнодорожному",
  "date_line": "25 июля 12:00",
  "seats_line": "10 мест",
  "contact_label": "ЗАПИСЬ В VK",
  "contact": "vk.com/natakkaz",
  "headline": "ТЁПЛАЯ ПРОГУЛКА ПО ЖЕЛЕЗНОДОРОЖНОМУ",
  "photos": ["/path/photo1.jpg", "/path/photo2.jpg", "/path/photo3.jpg"],
  "palette": "deep_wine_ivory",
  "source_url": "https://vk.com/wall..."
}
```

Contact precedence follows the existing guide promo rules:

1. phone number if present;
2. Telegram `@username` if booking/contact is Telegram;
3. compact VK label such as `vk.com/natakkaz`;
4. booking host/service/channel label.

The contact must be large enough to screenshot.

## Headline generation

The headline is a short top phrase that characterizes the excursion. It may be deterministic from the title/place, or LLM-generated from source-grounded fields.

Rules:

- Russian uppercase or strong title case, 1–2 lines.
- No invented facts: only use title, place, route, guide/source text and known event fields.
- Prefer mood + concrete place/format:
  - `ПРОГУЛКА ПО ЖЕЛЕЗНОДОРОЖНОМУ`
  - `ЗАВОРОТЬЕ БЕЗ СПЕШКИ`
  - `БИБЛИОТЕКА БФУ ИЗНУТРИ`
- If confidence is low, use the excursion title itself, shortened by deterministic line wrapping.

Cache the generated headline by `occurrence_id` + content hash so rerenders do not repeatedly call an LLM.

## Video structure

### Segment A — approved opening

The video begins with the already approved guide-intro scene:

- renderer/version: `true3d-v4-approved-2026-07-11`;
- same 720×1280 product composition and motion;
- no “по мотивам” rewrite;
- preserve SVG icon/date/contact formatting.

Implementation should reuse the tracked renderer (`scripts/render_cherryflash_guide_true3d_v4.py`) or a shared frame API around it, not duplicate the scene by hand.

### Segment B — photo-card carousel

After the approved opening, transition into the reference-inspired carousel:

1. Background continues in the same palette family, but can become lighter/airier for photo readability.
2. Photo cards appear as a depth stack: active card in front, previous/next cards offset behind with rotation, blur/opacity and smaller scale.
3. Each card flips/slides to the front with eased movement.
4. Top headline appears above the stack; it can stay fixed or subtly update once when the final card arrives.
5. If there are exactly 2 photos:
   - photo 1 = normal carousel card;
   - photo 2 = final CTA card background.
6. If there are more than 2 photos:
   - use 1–4 photo cards before the final card;
   - the last selected photo becomes the final CTA card background.

Recommended first-test duration: 10–12 seconds total:

- Segment A: existing 5.9 seconds.
- Segment B: 4–6 seconds depending on card count.

### Final CTA card

The final card becomes large and readable. It contains:

- excursion title;
- date/time with month text (`25 июля 12:00`);
- optional seats/free-place line when known;
- large contact block;
- halo/glass/light attention over contact data, not a fake hard flash.

The contact halo should start after the title/date are readable, then sweep left → right or bottom → edge as a guided attention effect.

## Visual style

- Vertical 9:16, production target 720×1280, 30 fps.
- Premium social card look: clean cards, soft shadow/depth, no phone UI chrome.
- Use the current approved CherryFlash/guide palettes as palette groups; choose palette per render unless campaign pins it.
- Photo cards should feel physical enough through rotation, z-order, scale, rim/shadow and parallax, but text must stay crisp.
- Avoid overblown stickers unless the excursion content explicitly benefits from them; the default is stylish/product-like, not meme-like.
- Maintain safe zones for story UI and Telegram/VK compression: keep headline and CTA away from extreme top/bottom edges.

## Renderer architecture draft

Preferred first implementation:

1. Build the approved intro frames by calling the existing guide true3D renderer.
2. Build carousel frames in a separate renderer module/script, likely PIL/composited 2.5D first; Blender is optional only if it improves card depth without degrading photos/text.
3. Concatenate frame sequences and encode one MP4 with audio so Telegram/VK do not treat it as GIF.
4. Store a storyboard and QA JSON per render:
   - frame count/duration;
   - first-frame motion deltas;
   - number of photos used;
   - chosen CTA contact;
   - selected palette/headline.

Do not integrate into production until a test render is accepted.

## Promo-campaign integration target

Future production activity surface proposal:

- `promo_activity.surface = 'guide_excursion_video'`
- `profile_key = 'guide_excursion_video'` or `popular_review:guide_excursion_video` depending on whether it is modelled as standalone or CherryFlash-adjacent.
- Default cadence: at most 1 guide excursion video per local day.
- Publication targets:
  - VK story;
  - VK wall/body post;
  - Telegram post.
- Exposure audit:
  - write `promo_exposure` for each successful target;
  - include `occurrence_id`, campaign/activity id, target surface, rendered video path/url, and source contact.

The production campaign can be a new guide-excursion promo campaign, or an added activity on the existing guide promotion campaign. The resolver must keep this activity independent from partner/Eco CherryFlash tracks unless explicitly configured.

## Selection strategy for first implementation

For each local day:

1. Load future guide occurrences.
2. Apply availability/free-place gates.
3. Require at least 2 usable photos.
4. Exclude occurrences already exposed by `guide_excursion_video` today.
5. Prefer nearer dates, but shuffle within the first shelf to avoid always choosing the same source.
6. Build payload and render.
7. Publish to configured surfaces; record exposure rows.

## Acceptance gate for first test render

Before sending a candidate as “готово”:

- Reference storyboard checked against `VID_20260708_142351.mp4` for carousel mechanic.
- Segment A visually matches the approved guide opening, not a new approximation.
- At least two excursion photos are used.
- Final card is large/readable and not cluttered.
- Contact data has a real-looking halo/attention pass and is screenshotable.
- Video has an audio track.
- Storyboard + MP4 are placed in Telegram Saved Messages for review.

## Open decisions before production rollout

1. Whether `require_personal_avatar=true` should be default for this standalone video activity, or whether the `2 photos + availability` gate is enough.
2. Maximum number of photo cards before CTA: proposed default is 3 photo cards + 1 CTA card.
3. Caption templates for VK wall and Telegram post.
4. Whether this activity gets a new promo campaign row by default or is added to the existing guide-excursion campaign.
