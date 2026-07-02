# INC-2026-06-24 replay fixtures

Minimal source artifacts for the future-event date/default-venue incident.

These fixtures are not a passing automated replay yet; they preserve the exact source texts and expected outcomes needed for the closure-grade test required by `docs/operations/incident-management.md`:

1. Run Telegram Monitoring server import boundary on these payloads against a prod snapshot/shadow DB.
2. Run Smart Update for each extracted candidate.
3. Assert `sources.json[].expected` against resulting `event` rows and skipped/non-event rows.
4. Include a pre/post DB diff in the incident evidence before closing `INC-2026-06-24-future-event-date-default-venue-regressions`.

The final fixture is an explicit negative control: a home-venue source with no offsite venue evidence may keep `source.default_location`.
