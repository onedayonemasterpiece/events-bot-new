# Listing surfaces V20: evidence-ranked Popular and mobile dual density

> **Status:** immutable preview candidate, 2026-07-18.
> **Surface in focus:** `/populyarnoe/`; desktop V19 geometry remains the regression baseline.
> **Supersedes:** V19 only for Popular shelf labels, per-shelf ranking and phone presentation.

## Product problem and outcome

Popular is a short explanation of *why* events attract attention, not a filter constructor. Two former headings were too elliptical, so the consumer labels are now:

- `Быстро набирают популярность`;
- `Встречается во множестве источников`.

The row allocation stays page-wide and idempotent: one event/program family appears in only one priority shelf.

### Why BREAK SUMMER FEST 5130 disappeared

The event was present and eligible with 69 shares, 52 likes, 7,212 source views and reason `frequently_shared`. It was not removed by lifecycle or family deduplication. The defect was coarse scoring: every event with that reason received the same categorical `2.5`, after which date/id ordering won and the shelf was sliced to five items. Thus three-share events could precede a 69-share event.

V20 does not add an event-ID exception. After earlier priority shelves allocate their families, the `frequently_shared` shelf sorts by actual `shares_count`, then likes, views and the stable incoming rank. Event 5130 therefore enters the shelf by the same evidence rule as every other event.

The current projection does not expose trustworthy magnitude for growth, discussion or independent publisher-family count. Those shelves retain their stable V19 order. A later exporter version should project reason-specific magnitudes; `source_engagement_sources_count` must not be misrepresented as independent source count because it can include owned surfaces.

## Mobile duality

Both modes use exactly the same `ListingEventCard` elements, shelf assignment, event order and media semantics.

### `Крупно` — default

- one horizontal shelf per behavioral reason;
- a card uses up to 86vw and leaves a visible continuation toward the next card;
- reviewed visual-only media may use the existing safe crop;
- OCR/unknown/poster media preserves its authored ratio and receives no synthetic fields;
- only existing quiet non-zero social proof and identity medallions remain.

### `Компактно`

- each shelf becomes an ordinary vertical scan list, not a smaller carousel;
- media stays on the left and copy on the right;
- the same crop/medallion/proof gates apply;
- the goal is fast comparison, not a separate ranking or reduced candidate set.

An explicit bottom safe-area radio group is the only density input. The preference is local presentation state (`ke_listing_density_v1`), not a personalization profile, and is applied before the shelf content is painted when possible. Switching preserves the nearest visible event as the viewport anchor.

Native pinch zoom remains enabled and is not intercepted. A hidden pinch-to-density gesture was rejected because it conflicts with browser accessibility zoom and is undiscoverable.

The generic floating `Для меня / Показать всё` prototype is not rendered on Popular: it is a different axis and would create two competing bottom pills. A future personalized `Вам может быть интересно` plus anti-bubble continuation remains a separate fail-closed experiment and may inherit the same density state.

## Critical review

The mobile product lane inspected the public V19 page at `390×844` and found a real cascade conflict: a roughly 304px card could receive only a 48–72px-wide preview. Gemini 3.1 Pro High returned `PASS` for the chosen large-default carousel, compact vertical list, one-DOM contract, explicit switch, native zoom and viewport-anchor preservation, with safe-area/browser-chrome validation as a required gate.

## Acceptance

- exact shelf labels above are visible;
- event 5130 is in `frequently_shared` because of 69 shares, with no ID override;
- each event ID and normalized family appears once page-wide;
- default phone mode is `large`; saved `compact` is restored locally;
- `360×800`, `390×844`, `430×932`: no document horizontal overflow;
- large shelves alone own horizontal overflow and show the next-card peek;
- compact shelves have no horizontal scrolling and retain identical ordered IDs;
- switching keeps the same visible event within 16px of its former vertical anchor;
- radio semantics, roving tabindex, arrows/Home/End, 48px targets and visible focus work;
- native zoom is not disabled; reduced motion removes dock movement;
- OCR/unknown media retains ratio 1.0, medallions/proof are not clipped, and the two modes do not duplicate images or event DOM.

- Public preview: `https://kenigevents.ru/preview-20260718-date-listings-v20-mobile/populyarnoe/`.
- Telegram visual review: `https://t.me/c/4337049383/122`, messages `368–372` (`369` large, `370` compact, `371` desktop ranking evidence).
