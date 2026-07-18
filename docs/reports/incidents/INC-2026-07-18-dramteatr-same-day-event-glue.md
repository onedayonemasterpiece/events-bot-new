# INC-2026-07-18-dramteatr-same-day-event-glue

Status: open
Severity: sev2
Service: Smart Update event identity, public event data and static event pages
Opened: 2026-07-18
Closed: —
Owners: events-bot / static-site
Related incidents: `INC-2026-07-02-boyko-exhibition-smart-update-glue.md`, `INC-2026-07-16-static-event-media-action-regressions.md`
Related docs: `docs/llm/request-guide.md`, `docs/features/static-site-pages/astro-preview.md`, `docs/operations/incident-management.md`

## Summary

Smart Update attached the distinct `14:30` Dramatic Theatre backstage tour to
the `18:00` performance rows for events `5754`–`5757`. The existing LLM merge
identity gate repeatedly classified these pairs as `related_but_distinct` and
returned `skip_merge_side_effects`, but production still ran the gate in
`shadow` mode. The correct verdict was therefore recorded without preventing
source, description, type and media contamination.

The static candidate amplified the canonical-data defect. Event `5756` was
presented as an excursion named «Женитьба», and a generic OCR ticket advert was
allowed to own/crop the hero in an artifact generated before the latest image
classification was projected.

## User / Business Impact

- the same-date theatre excursion and four different performances were mixed;
- event `5756` had a play title/ticket/time but excursion type and copy;
- unrelated/shared theatre media entered event galleries and an OCR advert was
  cropped as if it were a visual photo;
- generated desktop/mobile pages could be internally contradictory, so a user
  could misunderstand what they were booking.

## Detection

- detected by user visual review of the secret static candidate on 2026-07-18;
- production SQLite investigation correlated `event`, `event_source`,
  `eventposter` and `event_identity_decision_log` rows;
- no automated release gate rejected a canonical row whose exact occurrence
  fields disagreed with an attached source or whose shadow identity verdict
  said to skip side effects.

## Timeline

- 2026-07-02: merge identity gate introduced after an earlier sibling-event
  glue incident, with `off|shadow|enforce` rollout modes.
- 2026-07-03..2026-07-18: production shadow audit repeatedly recorded correct
  `skip_merge_side_effects` verdicts for Dramatic Theatre source/event pairs,
  but shadow mode continued the mutation path.
- 2026-07-17: immutable static candidate snapshot captured the polluted rows.
- 2026-07-18: user reported the contradictory «Женитьба» page and cropped OCR
  advert; investigation expanded the affected set to `5754`–`5757`.

## Root Cause

1. The post-match LLM identity gate correctly detected different occurrences,
   but `SMART_UPDATE_MERGE_IDENTITY_GATE=shadow` made the verdict non-blocking.
2. Smart Update continued merge side effects after the correct shadow verdict,
   attaching the tour source and shared media to performance rows.
3. Static export trusted the polluted aggregate too broadly; the stale
   candidate also failed to preserve stored `ocr_text` classification when OCR
   text itself was empty.

## Contributing Factors

- rollout of the existing safety gate was left in shadow after its precision
  review window;
- same venue/date created high lexical and contextual similarity despite
  conflicting time, title, ticket URL and event kind;
- shared theatre gallery/advert assets made filename/order-based hero selection
  unsafe;
- candidate acceptance tested page geometry but not exact occurrence/source
  consistency.

## Automation Contract

### Treat as regression guard when

- changing Smart Update match/merge side effects or identity-gate modes;
- importing multiple same-date events from one organizer/venue;
- projecting event source text/type/media into static pages;
- repairing or regenerating events `5754`–`5757`.

### Affected surfaces

- Smart Update final-match merge path and
  `SMART_UPDATE_MERGE_IDENTITY_GATE` runtime configuration;
- `event`, `event_source`, `eventposter`, identity decision log and publication
  hashes/outbox rows;
- Telegraph, managed Telegram/VK event publications and static-site export;
- static hero/media classification and content projection.

### Mandatory checks before closure or deploy

- exact replay: a `14:30` backstage tour and an `18:00` play at the same venue
  and date remain distinct and produce no merge side effects;
- positive control: a genuine same-event source update still merges;
- audit/review of shadow decisions before promoting `enforce`, with a documented
  rollback to `shadow` and no broad deterministic title/date merge rule;
- production backup and source-grounded repair for all affected rows, including
  creation/reconciliation of the real tour occurrence rather than deleting it;
- authenticated verification of every already-published Telegraph, Telegram
  and VK surface touched by the repair;
- fresh immutable production snapshot, `quick_check=ok`, production-rail Kaggle
  generation and desktop/mobile Playwright of the repaired pages;
- source commit reachable from `origin/main` before any production config or
  deploy change.

### Required evidence

- before/after production rows and named backup table(s);
- identity replay test output plus shadow-decision audit artifact;
- deployed SHA and runtime mode evidence (without secrets);
- public-surface URLs and authenticated/browser verification;
- immutable snapshot SHA, Kaggle result, secret candidate manifest and
  Playwright screenshots/results.

## Immediate Mitigation

- keep all regenerated pages behind a secret noindex candidate; do not promote
  the production root;
- add a static projection guard for exact structured occurrence conflicts and
  preserve stored OCR classification so the current aggregate cannot be shown
  as a cropped excursion hero;
- do not mutate production until the affected set is frozen and backed up.

## Corrective Actions

- [ ] add exact negative and positive identity replay coverage;
- [ ] promote the proven identity gate to an enforcing production path with a
  bounded rollout/rollback after review;
- [ ] repair canonical rows/sources/media for `5754`–`5757` and reconcile the
  separate `14:30` tour occurrence;
- [ ] repair already-published public surfaces and publication hashes;
- [ ] regenerate and publish a new immutable secret candidate through Kaggle.

## Follow-up Actions

- [ ] add an alert/metric when a high-confidence shadow
  `skip_merge_side_effects` verdict is ignored;
- [ ] run event-local LLM/VLM reconciliation for the mixed theatre media ledger
  instead of bulk SQL classification by filename;
- [ ] add release acceptance for source occurrence consistency, not geometry
  alone.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

This record is a mandatory regression contract for Smart Update merge-gate
configuration, same-date organizer imports and static projection of mixed
source/media aggregates.
