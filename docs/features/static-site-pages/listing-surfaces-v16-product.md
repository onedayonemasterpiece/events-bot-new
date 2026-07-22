# Listing surfaces V16: compact evidence and trustworthy time context

> **Status:** desktop preview candidate, 2026-07-18. Mobile remains outside this acceptance pass.
> **Surfaces:** `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`.
> **Supersedes for current behavior:** the V15 decisions recorded in [`listing-surfaces-v14-product.md`](listing-surfaces-v14-product.md); media/OCR fail-closed rules remain unchanged.

## Product job

Date listings help a person answer **when can I still go, what starts at that exact time, and which event is worth opening**. They do not attempt to close the attendance decision on the listing itself. Popular answers **what has current audience evidence and can I continue scanning without a horizontal dead end**.

This keeps the hierarchy:

1. exact time and day availability;
2. event identity (media, title, place);
3. quiet trust/social evidence;
4. detail page for the final decision and actions.

## Calendar evidence: documented decision

The canonical [favorites/calendar contract](../event-favorites-calendar/README.md) plans a durable, idempotent `saved event` row per user and event. It lists save/calendar UI as an eligible card surface, but it **does not specify a public aggregate count**. The current static site only has an ICS artifact/download route; an ICS request is not a unique person and does not prove that an external calendar accepted or retained the event.

Therefore V16 does not render a calendar icon/count on listings. A future calendar proof is allowed only when all of the following are true:

- the number is a deduplicated aggregate of durable saved-event state, not raw `ics_download` or link clicks;
- it is privacy-thresholded and non-zero;
- its label means “saved”, not “people will attend”;
- it uses the same quiet, detail-linked proof treatment as non-zero Share/Like;
- zero or unavailable evidence consumes no DOM/width.

This is not a rejection of saving from cards; it prevents a transport metric from being presented as social proof.

## V16 decisions

### Compact identity and audience evidence

- A card uses one `60px` side rail instead of separate `60px + 36px` rails. This reduces the reserved tail from `104px` to `64px` including the gap.
- With non-zero social proof, the rail shows at most two `52px` medallions at the top and up to two quiet `24px` proof rows at the bottom. Without proof it may show three medallions.
- A safe wide classified photo may keep one medallion over the lower-right image corner. A true no-image fallback may place the medallion inside the neutral media field. OCR/unknown media never gains an overlay merely to save width.
- Share/Like remain static non-zero orientation signals that link to detail. They are not CTAs. Calendar is absent until a trustworthy saved-event aggregate exists.

### City and time navigation

- Expanded city selection uses readable direct link-like toggles with more spacing. The rail measures its actual wrapped rows rather than assuming one viewport-dependent height.
- Once sticky, typography/gaps/counts compact so the schedule receives more vertical space. There is no dropdown and no horizontal scrolling.
- Today/Tomorrow can fit cities and periods in one measured surface. Weekend retains two semantic rows when the city list wraps; removing exact-time context merely to force one row was rejected.
- Daypart links and direct exact-time hashes land below the complete sticky stack. Weekend adds its persistent day-head height plus `12px`; a post-font double-`requestAnimationFrame` correction handles direct-load layout races.

### Weekend time, past state and dates

- The exact-time marker remains sticky below header, discovery surface and day heads. Under the time it shows only the filled weekday chip (`сб`/`вс`); the count is light normal text, for example `сб 2 события`, `вс 3 события`.
- Explicit `end_at <= now` is truly completed. Where end time is unknown, a start older than one hour is labelled **«Началось ранее»**, not “completed” or “unavailable”; only media is muted. This avoids falsely closing long concerts/festivals. Current/future events retain normal treatment.
- Weekend navigation is a visible cloud containing the selected/current weekend plus five future smart ranges. Only the terminal continuation uses the selected elongated SVG Repo arrow. The same control is repeated after the schedule.
- Sunday auto-position remains conservative: only a fresh visit with no hash, history scroll, input or reduced-motion preference may move to the first not-earlier Sunday group.

### Packing and sparse rows

- Events preserve source/relevance order. An exhaustive permutation check showed that swaps reduce one row only at some widths and can demote stronger social/relevance order; Sunday `11:00` had no stable gain at the current lane. V16 therefore does not disguise layout efficiency as ranking.
- Packing uses order-preserving intrinsic flex flow, a `1600px` global listing token and finite media heights independent of viewport height. More horizontal room may fit more cards; it must never make row count worse merely because the screen is taller/wider.
- A singleton is centered by its complete painted envelope (media + side rail + title/meta), not by the image alone. The envelope can use up to `420px` of readable copy space without changing another row.
- Only the actual last item of a multi-card rendered row may extend copy into unused right tail; this remains lower priority than legitimate media geometry and is removed if it exceeds four lines or collides.

### Personalization and Popular

- Full inventory is the default. `Для меня` appears only when a compatible consented profile produces a real different set. V16 includes a preview-only qualified state (`?personalization=qualified`) so composition and filtering can be inspected without claiming that production statistics already exist.
- The switch changes client visibility only; canonical HTML, sitemap and JSON-LD retain the full set and all counts recompute.
- Popular remains one continuous score-ordered intrinsic stream. Earlier category rows were a Home-page exploration, not a committed Popular taxonomy. Adding category shelves now would require incomplete heuristic labels, duplicate events and a false sense of coverage. A later optional category filter is allowed only after controlled taxonomy coverage and behavior evidence.

## Measured browser evidence

Local immutable build id: `preview-20260718-date-listings-v16`.

- `1366/1536/1920`: no document horizontal overflow; city controls wrap without horizontal scrolling.
- At `1536`, discovery rail is `88px`; Weekend marker at a direct `#weekend-time-17:00` load sits `12px` below the persistent day heads and stays visible while the group scrolls.
- Side-rail overflow is `0px`; singleton painted-envelope centre offset is `0px`.
- Current Saturday runtime classification found explicit completed rows only when end data existed; unknown-end starts older than one hour use `Началось ранее`.
- Order-preserving row counts do not regress when widening from `1536` to `1920`; the container may expand up to `1600px` while media height uses finite desktop tokens.

The exact generated screenshot/geometry records live under the non-committed `artifacts/codex/listing-surfaces-v16-product-20260718/` directory.

## Critical consultant gate

A first Gemini 3.1 Pro review was challenged because it proposed Grid, CSS-only hash landing, an incomplete sticky offset and calendar visibility derived from unavailable evidence. The corrected Pro addendum accepted:

- flex/intrinsic order-preserving packing;
- sticky top = complete dynamic stack + Weekend day heads + `12px`;
- post-font hash realignment;
- calendar proof only from durable saved-event semantics;
- preview-only qualified personalization;
- day-specific lightweight counts and media-only past muting;
- a long-arrow continuation asset.

The recommendation to horizontally scroll cities was rejected against the direct-access/no-horizontal-dead-end product contract; measured wrapping and compact sticky typography are used instead. Review artifacts: `artifacts/codex/listing-surfaces-v16-product-20260718/gemini-pro-product-review.md` and `gemini-pro-corrected-addendum.md`.

The final visual review then found a real singleton feedback loop: a settled OCR
card could re-measure its already expanded flex width and create UI side fields.
V16 now derives the visual width from the canonical `mediaHeight × sourceRatio +
tail` geometry; control `6869` settles at `221×221` media, `285px` visual and a
`420px` copy envelope. After that correction, Gemini's challenged final verdict
is **PASS WITH P2**: no P0/P1 remains; only further visual lightening of the
explicit city/weekend-link navigation is a non-blocking P2. The Weekend two-lane
criticism was withdrawn after applying the locked comparison/day-attribution
contract. Final artifacts: `gemini-pro-final-visual-review.md` and
`gemini-pro-final-challenge-review.md` in the same artifact directory.

## Deferred measurement

- implement and validate privacy-safe `saved_event_count` before calendar social proof;
- measure whether detail-open rate and time-to-first-detail improve with medallions/proof without reducing city/event diversity;
- validate optional Popular taxonomy filters against controlled coverage before shelves;
- test mobile navigation/rail collapse separately rather than shrinking the desktop composition by analogy;
- compare Today+Tomorrow split mode only as an explicit reversible experiment.
