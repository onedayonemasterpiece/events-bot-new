# Mobile Weekend editorial review

Status: **linked preview prototype; automated generation not implemented**.
Owner request: 2026-09-06. This is not a new collection, rating, shell or sticky island.

## Product contract

- Show a concise, pre-generated review **at the end of the mobile Weekend page**,
  after the listings, never in the fixed navigation. Do not display it on desktop.
- Title: «На что обратить внимание». Explain a few notable or unusual choices
  rather than declaring an unsupported “best” ranking or pretending attendance.
- Mentioned events are **internal links to their real detailed event pages**,
  resolved by the existing `eventHref`, preserving the active preview base path.
- The prototype is explicitly an all-region overview, independent of city filters.
  A production city-specific policy needs a separate decision; do not silently
  present the all-region summary as matching the selected city.
- Text is prepared before publication, not generated during a visit. Upcoming
  weekends use forward-looking wording; «стоило посетить» belongs to retrospectives.
- Keep normal card behavior and the existing lower date/navigation island intact.

## Current prototype / provenance

`site/src/data/weekend-editorial-prototype.json` is an assistant-prepared AI draft,
not an output from a wired production LLM job. It references the repository's
historical snapshot `2026-07-23T06:55:06.777309+00:00`, weekend25–26July2026.
The prose was prepared from the existing titles, schedules and descriptions:

- 6941: КиВиН / Летний кубок, Янтарь-холл, Saturday17:00; teams/program grounding.
- 5663: туба и орган, Кафедральный собор, Saturday18:00; unusual instrument pairing.
- 6954: Зелёный фестиваль, зоопарк, Sunday12:00; recycling and listed workshops.

No popularity or attendance statistic is inferred from these descriptions.
The two Saturday choices are alternatives, not a feasible sequential itinerary.

The real `/vyhodnye/` page renders the block only in preview mode, for the exact
B7/B9/B10 review builds and matching weekend, with all referenced IDs present. The footer is
hidden at widths above720px; no production/footer fallback is invented.
All three `/sobytiya/<slug>/` targets are part of the focused review artifact.
Review entry is a **direct Weekend URL**, not a design-system catalog:
`https://kenigevents.ru/preview-islands-20260906-archetypes-date-b7/vyhodnye/`.

## Implementation plan

- [x] Record product contract and proposal in the release planning index.
- [x] Build a mobile-only footer prototype with source-bound draft and real links.
- [ ] Obtain owner approval of placement, length, labels and editorial tone.
- [ ] Define schema: weekend range, snapshot/hash, generated_at, provider/model,
  prompt version, source event/occurrence IDs, sentence-level evidence and status.
- [ ] Add pre-generation to the **existing** snapshot/StaticSiteBuilder pipeline,
  with existing provider admission/limiter and LLM-first semantic selection. No
  separate notebook, on-open LLM request or title-keyword “best events” classifier.
- [ ] Validate grounded claims, schedule conflicts, cancellations and internal
  targets before publishing. Escape prose; resolve links from IDs, never trust
  arbitrary LLM-generated URLs or raw HTML.
- [ ] Define stale/missing/failed policy: hide mismatched editorial content;
  never attach last week's summary to a new weekend or block the event listing.
- [ ] Reuse existing analytics for review exposure and event-link clicks; no
  new personalization switch or independent tracking identity.
- [ ] Run representative live-preview freshness and mobile acceptance before
  separately authorized production rollout. This prototype is not promotion.

## Validation

`site/tests/weekend-editorial.playwright.mjs`: mobile footer after event rows,
no overlay/overflow at page end, three real same-preview detail navigations,
no footer on desktop or a different weekend. Native Android/iOS, current-event
accuracy and automated LLM/provider execution are **not claimed**.
Evidence: `artifacts/codex/mobile-city-native-20260906/`.

## Date specimen extension — 2026-09-06

The same product contract now has a source-bound one-day prototype at
`/preview-islands-review-20260906/date-2026-07-23/`, after the event lists on
mobile only. `date-editorial-prototype.json` is an assistant-prepared AI draft,
not a production provider run. It is statically included only for the exact
review prefix, matching day, snapshot timestamp and available referenced IDs.
No browser/LLM request is made to invent recommendations on opening the page.

Grounding in the same23July historical snapshot:
- 6986: botanical cyanotype at «Ворота», gathering plants, emulsion and printing.
- 6997: Alina Zhurina's local-play reading, followed by an announced discussion.
- 5662: marimba, vibraphone and balafon with chamber orchestra.

The block explicitly says these are alternative evenings, not a sequential
itinerary. All-region scope is visible and does not pretend to follow the city
filter. Each real link is resolved by `eventHref`; all three detail routes must
be part of the focused preview. A different day or snapshot hides the draft.

Plan extension: the pending shared editorial schema/pipeline above must support
one-day scope as well as weekend ranges; generation remains before publication
through existing LLM admission/limiter, with event-ID evidence and stale-data
checks. Wiring automated day/weekend generation and fresh production data is
still pending, not silently completed by this visual prototype.
