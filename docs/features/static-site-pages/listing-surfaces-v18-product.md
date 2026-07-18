# Listing surfaces V18: ecological desktop density and behavioral Popular

> **Status:** desktop immutable preview candidate, 2026-07-18. Mobile remains a separate research pass.
> **Surfaces:** `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`.
> **Supersedes:** the layout decisions of V17. Its data-truth, no-fields, OCR fail-closed, non-zero social-proof and truthful Calendar-count rules remain in force.

## Product job

A date listing is a fast scanning surface, not the place where a person must make the final attendance decision. Its order of evidence is **time → recognizable image/identity → title → place → quiet audience evidence**. The detail page owns full consideration, save/share actions and complete facts. The listing therefore maximizes useful choices visible without turning the schedule into a spreadsheet or a fixed card grid.

`Популярное` has a different job: explain *why the audience is paying attention* and let a person leave for a detail page quickly. It is not a deterministic filter constructor.

## Consolidated V12–V18 visual contract

### Source-truthful media

1. Every desktop surface uses a common media height per density: `221px` regular (`232px` at wide desktop) and `178px` Weekend (`188px` wide). Low-resolution media no longer changes the row rhythm.
2. A known low-resolution canonical asset remains visible when it is the best real event media. Event `3794` keeps its `300×174` source and is bounded-upscaled to the common `221px` height (`1.270×` by height). It is neither removed nor replaced by a manually found image.
3. OCR, identity posters and unknown documents keep authored ratio and `vertical_retention=1`. No `contain` fields, repeated edges, blurred backdrops or invented canvas filler are allowed.
4. Only a source-specific reviewed `visual_only` event photo with safe crop/focal evidence may grow or shrink inside a bounded aspect envelope. This classification may authorize a crop of an existing source asset; it is not a crawler and does not replace content.
5. Copy never determines packing width. A rightmost card may use spare copy width only after media packing settles. A singleton starts at the common left scan edge and may receive a bounded `420px` copy envelope; it is never optically centered by image alone.

### Recognition and audience evidence

- A venue/festival/Free medallion is a quiet recognition aid. There is no universal ring or shadow; the default listing treatment is reduced saturation/opacity with full strength on hover/focus.
- One medallion may overlay at `right:10px; bottom:10px` only on a reviewed safe no-OCR photo or a true no-image fallback. Controls: Tretyakovka `6950` and Zoo fallback `6957`. OCR/unknown media fail closed.
- Up to three identities are retained. At regular `221px`, three `51px` identities and two proof rows share one `64px` vertical rail. At short Weekend height the same evidence uses a `56px + 36px` split rail (`96px` total). Control: `6811`.
- Share/Like numbers are links to detail and decision-support evidence, not listing CTA. Zero values occupy no DOM or width; there are no listing Share/Like/Calendar buttons. A public Calendar count remains forbidden until a privacy-safe deduplicated durable `saved_event_count` exists.

### Time and Weekend packing

- The time marker has transparent background and stays pinned below the common site header and the single discovery plane; Weekend also clears its day heads. Accepted tops are `121px` and `182px` respectively at the measured desktop stack.
- Explicitly ended starts are collapsed above the Today flow. An unknown-end current-day start older than one hour is `Началось ранее`, not falsely declared completed. Known future end stays available.
- Weekend keeps one temporal axis and two day lanes. Packing is based on the actual client lane width, never a fixed `530px` guess. Reordering is legal only inside one day + exact hour and only if the projected row count decreases. It never crosses a day/time boundary.
- At `1536×864` a lane is about `641.6px`: two events per lane row is normal when protected media permits. At `1920×1080`, several real groups fit three. Protected OCR/unknown cards may still force fewer; density never overrides data truth.
- Past/started-earlier main media is muted while title/place/proof remain readable. Neutral `сб`/`вс` chips and light per-day counts preserve temporal context without competing with cards.

### One stable discovery plane

- Cities and dayparts share one `52px` sticky plane under the real `57px` site header. There is no second stacked subheader, dropdown or observer-driven expanded/compact state.
- The geometry is CSS-first and identical before and after scroll; no `IntersectionObserver`, `ResizeObserver`, `data-stuck` or delayed typography jump participates in header compaction.
- Direct city links remain horizontally available. Counts are hidden at the `1536px` CSS viewport and visible at `1920px`, where there is enough space.
- Movement uses the shared `220ms cubic-bezier(.2,.8,.2,1)` transition and respects `prefers-reduced-motion`.

## Behavioral Popular

The exporter projects explainable codes: `fast_growth`, `multi_source`, `discussed`, `frequently_shared`. `multi_source` requires two independent publisher families; the service's owned Telegram/VK repost family cannot create it.

The page allocates an event/program family once in this priority order:

1. `Быстро набирают`;
2. `В разных источниках`;
3. `Активно обсуждают`;
4. `Часто делятся`;
5. `Популярное сейчас` fallback.

Each shelf contains at most five cards and exactly one desktop row. A behavioral shelf with fewer than three candidates is omitted; fallback remains. Allocation uses normalized title + event type + venue/city, so another occurrence of the same program cannot return in a lower-priority shelf. V18 production snapshot yields five shelves × five cards = 25 unique discovery objects at both 1536 and 1920 without horizontal overflow.

## Product questions and decisions

| Question | V18 decision | Why / risk |
| --- | --- | --- |
| Should a singleton be centered? | No; align the complete card envelope to the common left edge. | Centering destroys temporal scanning and looked accidental. |
| Should safe photos expand when space exists? | Yes, inside bounded reviewed crop ratios; copy expansion is secondary. | Uses width without allowing titles to push later cards. |
| Should protected posters compress to force density? | No beyond the accepted retention bound. | A dense but unreadable poster is lower value than one fewer card. |
| Should the city row morph on scroll? | No; keep a stable 52px CSS plane. | Removes lag, layout shift and script failure mode. |
| Should personalization live in the header? | No. V18 tests a quiet bottom-center lens; full list is default. | It is optional context, not primary navigation. Risk: viewport occlusion, so it yields at the footer and remains a research prototype. |
| Should listings expose actions? | No; only non-zero social proof. | Attendance/share decisions belong to detail; listing numbers only support opening it. |
| Should Popular expose event-type filters? | No in this overview. | Behavioral shelves answer curiosity faster and avoid a complex constructor. Search/type discovery remains a separate surface. |
| Should exhibitions fill Today? | No when enough start-based events exist. | Month-long availability is a different task from “what starts today”; short festivals remain occurrence-like. |
| Should current/past Weekend events disappear? | No. | Muted history preserves orientation; on current Sunday a future production may auto-scroll after a stable time contract. |

## Personalization research prototype

V18 moves `Для меня / Показать всё` out of the discovery plane into one bottom-center radio group. It is shown only by the controlled preview fixture; production does not synthesize eligibility. `Показать всё` is selected by default, fewer than five personal results fail closed to the full list, and the control leaves when the footer becomes visible. This is an acceptance prototype, not a final mobile decision.

The mobile follow-up must offer an explicit accessible `Комфортно / Компактно` density control using the same DOM and event order. Native browser zoom must remain enabled; pinch must not silently replace an accessible control.

## Measured desktop acceptance

Playwright evidence is stored outside Git in `artifacts/codex/listing-surfaces-v18-density-popular-20260718/final/`.

- `1536×864`: regular height `221px`; Weekend lane `641.59px`; no document overflow; singleton left equals flow left; `3794` uses the real `300×174` asset at common height; `6950`/`6957` overlays are exactly bottom-right.
- `1920×1080`: content width `1720px`, city counts visible, actual three-card Weekend rows exist.
- Sticky context: discovery stays `52px` with identical attributes/typography before and after scroll; date marker top `121px`; Weekend marker top `182px` below day heads.
- Popular: reason order is behavioral, 25 event IDs are unique, and every `1459px` row has `scrollWidth == clientWidth` at 1536.
- `6811`: all three medallions and both non-zero proof rows remain present; regular density uses one compact rail, Weekend uses the height-safe split rail.

## External critical review

`a-gemini` / `Gemini 3.1 Pro (High)` first returned `FAIL`: it correctly found
the brand-red current marker, visually strong proof and oversized floating
prototype. V18 accepted those findings: the marker is neutral, passive proof is
`opacity:.58`, and the floating control is `44px` high with a softer shadow.
The repeat review returned `PASS WITH CHANGES` for the desktop preview and
`RESEARCH-ONLY` for personalization; it accepted the data-driven sparse hours,
measured Weekend density and absence of universal CSS medallion rings.

The repeat review's remaining production blocker (“ragged Popular image/title
baseline”) is rejected by browser evidence, not by opinion: all five first-shelf
media boxes are exactly `top=373.28125`, `height=180`, `bottom=553.28125`, and all
five titles start at `top=563.28125`. Different source aspect ratios do not
change the V18 shelf baseline. Promoting this preview to production remains a
separate owner/release decision regardless of that false-positive finding.

## Review delivery evidence

The immutable public candidate is available at
[`Сегодня`](https://kenigevents.ru/preview-20260718-date-listings-v18/segodnya/),
[`Завтра`](https://kenigevents.ru/preview-20260718-date-listings-v18/zavtra/),
[`Выходные`](https://kenigevents.ru/preview-20260718-date-listings-v18/vyhodnye/)
and
[`Популярное`](https://kenigevents.ru/preview-20260718-date-listings-v18/populyarnoe/).
Public Playwright captures use the same prefix rather than a local server.

The consumer screenshots and the four concrete media/evidence controls were
published to the existing Telegram UI-review topic: the
[V18 introduction](https://t.me/c/4337049383/350),
[Today](https://t.me/c/4337049383/351),
[Weekend at 1536px](https://t.me/c/4337049383/352),
[Weekend at 1920px](https://t.me/c/4337049383/353),
[behavioral Popular](https://t.me/c/4337049383/354),
[`3794` common-height control](https://t.me/c/4337049383/355),
[`6950` safe overlay](https://t.me/c/4337049383/356),
[`6957` no-media overlay](https://t.me/c/4337049383/357),
[`6811` identity/proof packing](https://t.me/c/4337049383/358) and the
[acceptance facts](https://t.me/c/4337049383/359).
The redacted delivery receipt is retained outside Git under the V18 artifact
directory.
