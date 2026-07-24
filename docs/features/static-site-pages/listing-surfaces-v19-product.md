# Listing surfaces V19: chronological now, immediate media discovery and bounded Popular continuation

> **Status:** desktop preview candidate, 2026-07-18. Mobile remains a separate research pass.
> **Surfaces:** `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`.
> **Supersedes:** V18 only where this document explicitly changes current-time placement, overlay strength, image loading/cache behavior and the Popular personalization boundary. All V18 media-truth, crop, packing and evidence rules remain in force.

## Product decisions

### `Сейчас` is a position in the schedule, not a page notice

The marker is hidden in the static first frame and inserted into the Today exact-time timeline after the Kaliningrad clock is known. It appears after every exact start earlier than now, before the first exact start equal to or later than now, and before `Время уточняется`. It is never rendered between the header and the whole event list.

Minute updates may change `Завершилось` / `Началось ранее` styling, but must not move a card into the collapsed `Завершились` disclosure while the user is reading. A full rebuild/navigation may repartition only events with a truthful explicit end.

### Overlay recognition is clear; auxiliary evidence stays quiet

A medallion admitted by the existing fail-closed reviewed-photo/fallback gate and placed directly over the preview is a recognition mark, so it uses `opacity:1` and no saturation filter. External identity medallions remain quiet until hover/focus. Non-zero Share/Like remains translucent evidence, not a listing CTA.

### Cache was not the principal Weekend failure

V18 public profiling at `1536×864`:

- 67 event images, 65 unique selected objects, about 1.23 MB cold bodies;
- loaded count grew monotonically `6 → 11 → 15 → 23 → 31 → 51 → 67`; no URL removal or actual unload was reproduced;
- warm reload and warm scroll transferred 0 image body bytes;
- a visible non-priority card received `src` only around 916ms and became ready around 1.15s because application JS withheld its URL behind a `200px` observer;
- 15/67 cards selected a 256w raster for an actual 257–317px rendered frame at DPR 1.

V19 therefore:

1. emits real `src/srcset/sizes` in parser-visible HTML;
2. delegates offscreen scheduling to native `loading=lazy` instead of using JS as a URL gate;
3. makes the first four real Weekend cards in global chronological order eager/high, including cards in a second sparse early hour;
4. declares Weekend at 320px / 340px wide desktop (44vw mobile provisional) so DPR 1 never undershoots the measured frame;
5. keeps intrinsic dimensions and decode-gated skeleton removal;
6. uploads build-prefixed `_astro/**` and `assets/**` as one-year immutable while HTML/ICS remain short-cache.

The HTML is still large and CDN `Timing-Allow-Origin` is not configured; both are follow-up performance/RUM topics, not explanations for the V18 late-scroll flash.

Public V19 acceptance on the same viewport found 67/67 parser-visible `src` and
`srcset`, 4 eager + 63 native-lazy images, 13 ready at the cold initial state
(versus 6 in V18), all 67 retained and ready after down/up scroll, and all 67
immediately complete after warm reload. Warm reload again transferred 0 image
body bytes (75 image resources including medallions, p95 4.5ms). Minimum selected
raster density was 1.024 for the measured maximum 317.19px frame. The trade-off
is explicit: correcting the old 256w undershoot increased cold selected image
bodies; a future 384w derivative may reduce that cost without restoring blur.

## Popular: personalized continuation is an experiment, not a generic feed

The accepted four-to-five behavioral shelves remain the complete default Popular page. The previous generic `PersonalFeedSlot` is removed because it can drift from the Popular scoring contract and has no whole-page family exclusion.

A future experiment may append one fail-closed wrapper:

1. **`Вам может быть интересно`** — 5 candidates from normalized popularity × compatible personal affinity;
2. **`Откройте новое`** — 3–5 credible popular candidates outside the profile's two strongest categories.

Minimum gate:

- explicit personalization consent and a meaningful compatible profile;
- experiment allocation and fresh/version-compatible normalized Popular score;
- exact event plus normalized family exclusion across every behavioral shelf and both continuation lanes;
- personal diversity: maximum two of one category, maximum one venue, at least three categories;
- exploration diversity: maximum one per category/venue and at least three categories;
- at least five qualified personal and three qualified exploration candidates; otherwise hide the entire wrapper;
- success measured by meaningful detail/calendar/ticket activity and category breadth, with a holdout—not CTR alone.

This is **research-only in V19**. No fake fixture or generic response is shown as the feature.

## External critical review

Gemini 3.1 Pro High reviewed the four decisions and returned `PASS` with two cautions already incorporated: do not dynamically relocate cards while a person reads, and validate native loading with selected `sizes`, LCP/CLS and cache evidence. The Popular continuation was accepted only as an experiment with the complete gate above.

## Acceptance references

- `ADD-LIST-03`: chronological current marker.
- `ADD-LIST-11`: parser-discoverable responsive media and cache profile.
- `ADD-LIST-16`: behavioral Popular plus experiment boundary.
- `ADD-LIST-17`: opaque overlay versus quiet external/proof evidence.
- Profile evidence: `artifacts/codex/listing-surfaces-v19-current-media-20260718/perf/` (not committed).
- Public preview: `https://kenigevents.ru/preview-20260718-date-listings-v19/`.
- Visual review thread: `https://t.me/c/4337049383/122`, V19 messages `362–367`.

## R11 retained date context

A desktop date page keeps one discovery plane. Before it is pinned, the large
page header owns the date. After that header leaves the viewport, the existing
sticky discovery rail reveals a compact date at its left (`8 августа` plus the
weekday), followed by the city and time controls. It must not create a second
sticky bar, cover the leather brand reservation, shift the page, or appear on
mobile where the accepted rail shell owns context.
