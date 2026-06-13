# INC-2026-06-13 Kantata Education Promo ID-Only Design

Status: open
Severity: sev2
Service: promo campaigns, festival/programme modelling
Opened: 2026-06-13
Closed: —
Owners: product/engineering
Related incidents: `INC-2026-06-13-kraftmarket-promo-zero-events`, `INC-2026-06-08-festival-vk-aggregate-regression`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/backlog/features/festival-monitoring-debt/README.md`

## Summary

The promo campaign for the educational programme of `Кантата` was modelled too narrowly as a fixed set of event ids. That design is unsafe because the programme was not fully imported yet: newly imported education events under the same festival marker would remain outside promo eligibility until an operator manually edited the campaign.

## User / Business Impact

- Promoted `Кантата` educational events can be imported correctly and still miss campaign promotion.
- Operators cannot trust that "promote the educational programme of `Кантата`" means the whole live programme; it may mean only the events known at setup time.
- Campaign reports may look healthy for the initial ids while silently ignoring newly discovered programme events.

## Detection

- Detected during the 2026-06-13 red incident for missing `@kraftmarket39/287`.
- Production campaign audit showed `Кантата` festival campaigns plus a separate event-targeted educational campaign with a fixed event-id set.
- Operator clarified the intended model: public/source marker is the festival `Кантата`; inside it, events are either concerts or educational programme items.

## Timeline

- 2026-06-13 UTC: missing `@kraftmarket39/287` exposed that a newly imported education event would need to become promo-eligible.
- 2026-06-13 UTC: audit found an educational programme campaign tied to fixed ids rather than a dynamic `Кантата` + education segment filter.
- 2026-06-13 UTC: design incident opened and docs updated to forbid ID-only live programme eligibility.

## Root Cause

1. Campaign eligibility and publication curation were conflated. Explicit event ids are useful for a specific carousel/post, but they are not a safe eligibility model for an open programme.
2. The promo data model lacked a first-class programme-segment target such as `festival=Кантата` plus `programme=education`.
3. `Кантата` was treated as if the educational programme could be represented by a closed list of known events, even though the import pipeline was still discovering future programme items.

## Contributing Factors

- `Кантата` uses one public festival marker for different programme types: concerts and educational events.
- The docs did not explicitly state that an educational programme is a segment inside the festival, not a replacement `event.festival` value.
- Existing surfaces (`preferred_event_ids_by_date`, `carousel_event_ids`, `celebrity_event_ids`) made fixed id lists convenient, which hid the eligibility risk.

## Automation Contract

### Treat as regression guard when

- creating or editing promo campaigns for a live festival/programme;
- changing promo target types or campaign resolvers;
- changing `Кантата` import, classification, or campaign activities;
- adding per-surface event-id lists to festival/programme campaigns.

### Affected surfaces

- promo campaign target design and `/promo` operator flows;
- campaign reports and activity resolvers;
- `Кантата` event import from Telegram/VK/external sources;
- `vk_festival_carousel`, `afishaengagement`, `tg_event_publish`, `vk_publication`, and related promo surfaces.

### Mandatory checks before closure or deploy

- A live programme campaign must have dynamic eligibility, not only fixed event ids.
- For `Кантата`, `event.festival` remains exactly `Кантата`.
- The education campaign applies a dynamic programme filter that admits lecture/talk/education events and rejects concerts under the same festival marker.
- A newly imported `Кантата` education event becomes eligible without editing the campaign id list.
- Per-surface id lists are allowed only as curated publication subsets and must not be the whole campaign eligibility mechanism.

### Required evidence

- docs/tests showing the contract is pinned;
- production campaign rows after repair showing dynamic `Кантата` + education eligibility;
- production check with at least one imported education event and one concert under `Кантата`;
- campaign report evidence that the education event is eligible and the concert is not.

## Immediate Mitigation

- Keep `event.festival="Кантата"` for all relevant `Кантата` programme items.
- After the import hotfix, repair the production educational campaign so newly imported `Кантата` education events are eligible through a dynamic filter, not by manually appending ids as the primary mechanism.

## Corrective Actions

- Documented the rule in `docs/features/promo-campaigns/README.md`: live programme campaigns cannot be `event.id`-only.
- Documented the `Кантата`-specific model in `docs/backlog/features/festival-monitoring-debt/README.md`: festival marker first, education segment filter second.
- Added a regression test that pins the live-programme promo design contract.

## Follow-up Actions

- [ ] Add a first-class promo target/filter for `festival=Кантата` + education programme segment.
- [ ] Convert the current production education campaign away from fixed-id-only eligibility.
- [ ] Add E2E that imports a new `Кантата` lecture and proves campaign eligibility without editing event ids.
- [ ] Add a negative E2E/control case for a `Кантата` concert under the same festival marker.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `.venv/bin/python -m pytest tests/test_promo.py::test_promo_docs_forbid_event_id_only_live_programme_campaigns -q` -> `1 passed`
  - `git diff --check` -> ok
- post-deploy verification: pending

## Prevention

Future festival programme campaigns must explicitly separate:

- festival identity used in public/source communication (`event.festival`, for example `Кантата`);
- programme segment selection (`education`, `concerts`, or another auditable classifier);
- publication curation (`carousel_event_ids`, `preferred_event_ids_by_date`, etc.).

Only the last layer may be a fixed id list for a live programme.
