# Optional gallery card «Как добраться»

> Status: **optional release candidate / prototype not implemented**. It is an additional presentation of validated F11 transport data, not a replacement for the accessible transport schedule block and not a public-release blocker by itself.

## Product intent

For an eligible event with validated rail/bus guidance, the image gallery may contain at most one non-photo informational slide titled **«Как добраться»**. The purpose is to expose the most useful route summary at the same moment a visitor browses event media, while the canonical full schedule and transport-calendar actions remain in normal page content.

The release can ship the core transport feature without this gallery slide if the prototype does not pass visual, accessibility, freshness or comprehension acceptance. The decision to omit the optional slide does not waive F11 schedule/refresh requirements.

## Build and data contract

- Generate the card at static-build time from the same immutable validated transport snapshot and selector output as the full block. It cannot call journey providers in the browser or implement a second route-selection algorithm.
- Preferred delivery is a deterministic safe SVG informational card, with a lightweight WebP derivative only where the gallery implementation requires raster media. It must pass the site-wide CDN and asset-budget gates.
- Include only compact source-backed facts: mode, Kaliningrad origin, destination/target stop, selected outbound option or time window, return status/caveat, last-mile warning where material, and snapshot freshness.
- Add a terse “расписание может измениться” warning and an in-page action to the full **«Как добраться»** block. Do not overload the slide with every timetable row.
- Fail closed: do not generate/show the slide for stale, partial, ambiguous-date, unsupported-locality or no-usable-option cases.
- The card is derived content. It is not event photography, must not enter event `image[]` structured data as source media and must never become the default hero, OG/share image or focal-crop candidate.

## Gallery behavior

- Place at most one card after at least one genuine event poster/photo; never before the hero/first event image.
- Visually identify it as an informational transport card, not as a venue/event photo.
- Preserve ordinary swipe, keyboard and pagination behavior. Auto-advance must not make the card unreadable and must respect `prefers-reduced-motion`.
- The gallery and the full transport block must not expose competing “save” semantics. A transport-leg calendar action continues to save the selected leg; the event calendar/favorite remains a separate event action.

## Accessibility and static fallback

Information cannot exist only as pixels in the gallery card:

- provide a concise useful alt/accessible name;
- expose the same facts in structured HTML in the normal transport section;
- ensure the in-page “Подробнее о маршруте” destination works with keyboard and no JavaScript;
- keep source/freshness and uncertainty visible in the full block even when shortened on the slide.

If the gallery or card fails to hydrate, the event content and full transport block remain usable.

## Prototype acceptance

The release-UI task compares “no gallery card” with the single-card candidate on the immutable baseline. Acceptance covers:

- enabled rail event, enabled bus event and explicit-return/unknown-end variants;
- unsupported locality, stale snapshot and partial-provider failure (no slide rendered);
- `375`, `768`, `1366` and `1440` CSS px, keyboard/no-JS/reduced-motion and slow media;
- exactly one derived slide, correct placement after real media, no use as hero/OG/JSON-LD event image;
- visible route facts equal the canonical selector result and refresh manifest hash;
- SVG/WebP weight and CDN checks, no PNG/JPEG fallback.

Final inclusion and exact composition require project-owner sign-off together with the frozen release UI.

## Related documentation

- [Event transport feature and production refresh gate](README.md)
- [Detailed rail/bus renderer contract](../static-site-pages/event-transport-schedule.md)
- [Release UI contract](../static-site-pages/release-ui-contract.md)
- [CDN and asset delivery](../static-site-pages/cdn-asset-delivery.md)
