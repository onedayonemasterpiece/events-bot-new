# Listing surfaces V28 — desktop Popular

**Scope:** the desktop presentation remains V28, while eligibility is one
cross-representation truth contract for desktop and mobile `/populyarnoe/`.

## Product contract

Popular is a bounded answer, not a filter constructor. It shows five evidence
shelves in this order:

1. `Быстро набирают популярность`;
2. `Встречается во множестве источников`;
3. `Активно обсуждают`;
4. `Часто делятся`;
5. `Популярное сейчас`.

Each shelf is one row with at most five cards. An evidence shelf is omitted
below three truthful candidates; it is never padded with unrelated
"evergreen" cards merely to make the geometry symmetrical. There is no
load-more control and no card-level explanation/chip: the shelf heading already
states the reason. Existing quiet non-zero like/share evidence remains inside
the shared `ListingEventCard`; a zero count has no DOM node.

## Eligibility and repeated events

Both projections are calculated against the same explicit build reference:

- cancelled, postponed, merged, duplicate, deleted and inactive events fail
  closed;
- an elapsed one-off is removed and a future one-off remains eligible;
- only a semantically projected exhibition (`event_type=выставка|экспозиция`
  or topic `EXHIBITIONS`) remains eligible through its `end_date`, even when
  the opening-day `end_at` is already past. An ordinary event with a stale or
  broad date range is excluded after its start day; the renderer never
  reclassifies it from title keywords.

Family identity is normalized title + event type + venue/city. A family is
allocated once across all five shelves. Its highest-ranked upcoming occurrence
is the visible card; sibling occurrence IDs are folded into `other_date_ids`
and the quiet lifecycle line says, for example,
`24 июля · 19:00 · ещё 1 показ`. Engagement is not summed across occurrences,
because source exports may already contain aggregated values.
The final selector is finite: linked occurrences and exact repeated IDs are
collapsed before the limit, and a short truthful inventory yields fewer cards
instead of repeated filler.

The static-site calendar rollover already requests a rebuild at midnight in
`Europe/Kaliningrad`, with startup catch-up. The build-time cutoff is still
required: a daily rebuild alone cannot distinguish an event that started
earlier on the same date.

## Cross-cutting personalization

After the five global shelves, desktop may reveal one sixth shelf:
`Вам может быть интересно`. It is an application of the existing anonymous
site-wide profile, not a Popular-specific profile or a separate personal page.

- no consent, incompatible schema or fewer than three strong signals: the
  shelf remains absent;
- a warm profile: exactly four positive-affinity candidates plus one
  anti-bubble candidate;
- hidden/not-interested IDs and all families already shown above are excluded;
- if four honest affinity candidates plus one exploration candidate cannot be
  formed, the whole shelf remains absent;
- selection runs once on load and does not reorder visible cards after a
  feedback action.

The profile lives only in localStorage, so server-side personalized HTML is not
possible without changing the privacy architecture. The static HTML therefore
contains a bounded hidden, family-deduplicated candidate pool rendered with the
same `ListingEventCard`; the client reveals only the selected five. The shelf
is last, so revealing it cannot move any of the already visible global cards.

## Isolation and gates

- `getPopularEvents()` and `getPopularDesktopEvents()` share one eligibility
  predicate; only grouping and presentation differ.
- the generated-output gate rejects an ineligible event ID in either tree.
- a stale exported `current_date` must not silently define a newer preview:
  the builder supplies an explicit deterministic reference or refreshes the
  export before generation.
- `ListingEventCard` gained optional `temporalLabel` / `familyKey` props; callers
  that do not pass them keep the previous DOM behavior.

Release checks require the five shelf order, 3–5 truthful cards per rendered
evidence shelf, global family uniqueness, lifecycle labels, repeat folding,
Break Summer Fest regression ID `5130`, cold-start absence of the sixth shelf,
4+1 warm-profile selection and identical mobile large/adaptive ranking.
Desktop browser geometry remains checked at 1366×768, 1536×864 (FHD at 125%)
and 1920×1080.

Every desktop shelf remains `nowrap`, but the shelf itself owns any intrinsic
width excess with horizontal scrolling at every desktop width. The document
must not become wider than the viewport. This does not shrink cards, change
media ratios, wrap the five-card answer, or introduce a second presentation.
In addition to the established presentation contours, browser acceptance
checks `961`, `1022`, `1023`, `1024` and `1440` px.

The 30 July presentation snapshot fitting without document overflow is not
contradictory evidence: the fault was data-dependent. A later real catalogue
combined enough wide 16:9 cards and recognition rails to exceed the shelf at
desktop seams. The repair assigns overflow to the existing shelf and preserves
the accepted V28 composition rather than reviving or redesigning an older UI.

Gemini 3.1 Pro's critical review supplied the global-family, time-boundary,
mobile-isolation and warm/cold invariants. Two suggestions were intentionally
rejected: server-side selection cannot read the local-only profile, and false
category fillers would make the shelf headings dishonest.

## Review build

The immutable candidate uses prefix
`preview-20260720-popular-desktop-v28`. The checked source fixture was generated
on 18 July 2026 and is acceptance data, not a claim that the public production
catalog has already been refreshed for 20 July.
