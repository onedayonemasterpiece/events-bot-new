# Requirements: VK location marker

Status: reconciled draft

## Product intent

- VK event wall posts should carry a safe VK location marker when the event city is confidently known, so users see the relevant city/location context in VK without adding manual work for operators. Source: [source/voice_AgADTaEAAoIEsEk.oga](source/voice_AgADTaEAAoIEsEk.oga), [source/voice_AgADT6EAAoIEsEk.oga](source/voice_AgADT6EAAoIEsEk.oga).
- The bot currently operates for Kaliningrad Oblast; location markers must not point outside Kaliningrad Oblast even if an event/source contains an ambiguous or conflicting city. Source: [source/voice_AgADTaEAAoIEsEk.oga](source/voice_AgADTaEAAoIEsEk.oga).
- Location marker enrichment is optional and must be fail-open for publication: inability to resolve a safe marker must never block or degrade the VK post itself. Source: [source/voice_AgADT6EAAoIEsEk.oga](source/voice_AgADT6EAAoIEsEk.oga).

## Requirements

### VK publication behavior

- When creating an automatic VK event publication and the event has a confident city/location in Kaliningrad Oblast, the publisher should add the corresponding VK location marker to the `wall.post` request.
- If the required location marker cannot be found, cannot be validated as Kaliningrad Oblast, or has low confidence, the publisher must omit the marker and still publish the VK post normally.
- The feature must use the event's structured location data first (`event.city`, then `location_name`/`location_address` as supporting context) rather than trying to infer a new city from the rendered VK caption.
- Publication surfaces without event/city context must skip the marker instead of guessing.

### Region and confidence rules

- A marker is eligible only when the resolved city/place belongs to Kaliningrad Oblast.
- Ambiguous names must be treated conservatively: if the resolver cannot distinguish the Kaliningrad Oblast place from an out-of-region namesake with high confidence, no marker is sent.
- Existing region filtering primitives should be reused where applicable: the current project already has Kaliningrad Oblast allowlist/cache logic in `geo_region.py`, `geo_city_region_cache`, and `docs/features/geo-region-filter/README.md`.

### Internal cache / directory

- Resolved VK marker data must be cached by normalized city/place key so the bot does not repeat external/API lookups for the same city on every publication.
- The cache entry should store enough data to reuse the marker safely: normalized query, display title/city, Kaliningrad Oblast decision, VK/API marker payload (`lat`/`long` and/or `place_id` if available), confidence/provenance, and timestamps.
- Negative or ambiguous results may also be cached with conservative TTL/refresh semantics to avoid repeated failed lookups while allowing future correction.

### Technical integration notes

- Technical finding: VK `wall.post` supports `lat`, `long`, and `place_id` parameters for a post location marker; there is no `city_id` parameter on `wall.post`. Implementation should therefore translate an event city/place into the safe marker payload accepted by `wall.post`.
- Technical recommendation: integrate through the shared VK wall-post creation path where possible, but keep the marker optional per call so existing VK posts/reposts/digests without event context are unchanged.
- Technical recommendation: add observability for marker decisions (`applied`, `skipped_no_city`, `skipped_not_region`, `skipped_low_confidence`, `lookup_error`) without exposing noisy details to VK users.

## Open questions

- None from the reconciled intake. Product/UX behavior is clear: apply only confident Kaliningrad Oblast markers and skip otherwise.

## Decisions log

- 2026-06-20: Initial voice intake reconciled into canonical requirements. No conflicts with previous canonical requirements because prior sections were placeholders.
- 2026-06-20: Technical analysis intake classified as implementation guidance rather than a product question. VK API marker primitive is `lat`/`long`/`place_id` on `wall.post`; existing project Kaliningrad Oblast cache/filter should be reused or extended rather than asking the user to choose storage/API details.

## Intake archive

### Intake 2026-06-20T08:34:39+00:00

Status: resolved/archived 2026-06-20

Resolution: integrated into Product intent, VK publication behavior, Region and confidence rules, and Internal cache / directory.

#### User notes

Voice transcript summary: events have a location and city; VK publication can include a city/location marker; the bot should try to add the city during automatic VK post generation; resolved city data should be saved in an internal directory/cache to avoid repeated searches; the current operating region is Kaliningrad Oblast; if the required location cannot be found or confidence is low, omit it.

#### Source files

- [source/voice_AgADTaEAAoIEsEk.oga](source/voice_AgADTaEAAoIEsEk.oga)
- [source/voice_AgADT6EAAoIEsEk.oga](source/voice_AgADT6EAAoIEsEk.oga)

### Intake 2026-06-20T10:22:49+00:00

Status: resolved/archived 2026-06-20

Resolution: classified as a request for technical reconciliation/system analysis, not a conflicting product requirement. Integrated as Technical integration notes.

#### User notes

Systemic requirements review requested, including implementation-relevant analysis.
