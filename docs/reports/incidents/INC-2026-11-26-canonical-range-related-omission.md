# INC-2026-11-26: canonical date ranges omitted from related catalog

Status: prevention implemented locally; deployment and live publication open.
DevCoveer context: inc_af96f6510a55a8ccc1d713dc.

## Observed impact

Event 11502 had canonical date `2026-12-03..2026-12-25`, explicit end
`2026-12-25` and time `08:00..17:00`. Its pending first VK job 170977 repeatedly
deferred with `vk_initial_waiting_related`. The normal completed build 163030
requested event 11502, but the exporter omitted it from the candidate catalog.

## Cause and prevention

The app supports canonical date ranges; the export SQL and public gate accepted
only one ISO date. Their omission removed the anchor before verification.
This is a mechanical syntax mismatch, not a change of date interpretation.
The exporter now uses shared start/end expressions for selection, ledger and
archive, and shared validated bounds for exported events and club consumers.
Canonical rows and the strict first-publication requirement are unchanged.

## Treat as regression guard when

Changing catalog eligibility, date normalization, related input or club export.

## Mandatory checks and release evidence

The offline fixture reproduces the missing anchor before the fix. Verify full
fetch, start/end/hours, ledger parity, ongoing final day, archive and unchanged
raw data; reject malformed and reversed embedded ranges and invalid explicit
ends. Existing single-day, club, projection and related-scope checks must pass.
Release merged main through the Events-only runbook; verify a normal completed
build, first publication and one paced frozen comment. Live verification remains
open until those ordinary results occur; no SQL repair or replay is prescribed.
