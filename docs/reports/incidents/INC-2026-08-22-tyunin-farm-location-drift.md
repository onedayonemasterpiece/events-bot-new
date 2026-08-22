# INC-2026-08-22 Tyunin Farm location drift

Status: investigating
Severity: sev2
Service: Smart Update location normalization / public event projections
Opened: 2026-08-22
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-08-22-sos-dedup-veto-location-tyunin-farm`, `INC-2026-05-09-event-location-alias-free-dup-regressions`
Related docs: `docs/reference/locations.md`, `docs/reference/location-aliases.md`

## Summary

Event `7717`, «Прогулка с Фонарщиком», retained `Ферма у камина`
without an address instead of resolving the operator-expected canonical venue
`Ферма Тюниных` in Знаменск. The VK source is
`https://vk.com/wall-96427382_6194` and contains `#ферматюниных`.

This location-reference defect is linked to, but is not a cause of, the August
Smart Update deduplication regression.

## User / Business Impact

- The public venue is an uncanonicalized sub-location phrase.
- Address and venue identity are missing from discovery/projections.

## Detection

Operator review detected the defect on 2026-08-22. The maintained locations
and aliases currently contain neither the expected canonical row nor these
source forms.

## Root Cause

The maintained reference layer has no verified Tyunin Farm row or aliases.
Whether it was lost during migration or never committed remains open. This
incident must not invent an address.

## Automation Contract

### Treat as regression guard when

- maintained locations, aliases, hashtag normalization or location fallback
  changes.

### Affected surfaces

- `docs/reference/locations.md`, `docs/reference/location-aliases.md`;
- `find_known_venue_in_text` and event `7717` projections.

### Mandatory checks before closure or deploy

- recover authoritative canonical name/address evidence;
- add prose/hashtag aliases plus a focused resolver regression;
- repair `7717` without losing the original wording;
- rebuild and verify its public projections.

### Required evidence

- authoritative venue/address source or repository-history proof;
- test receipt, before/after production row, exact-main SHA and public checks.

## Immediate Mitigation

No guessed reference or production mutation was applied during the dedup
incident response.

## Corrective Actions

- [ ] Verify `Ферма Тюниных` and its Знаменск address.
- [ ] Add verified row and aliases for `ферма у камина`,
  `кафе ферма у камина`, `ферматюниных` and hashtag form.
- [ ] Repair `7717`, retain source wording as provenance/fact, rebuild surfaces.

## Release And Closure Evidence

- authoritative reference: pending
- test/deploy SHA: pending
- production repair/public verification: pending

## Prevention

Location additions require authoritative evidence and a resolver fixture for
prose and hashtag forms; incident response never infers an address from an
operator label alone.
