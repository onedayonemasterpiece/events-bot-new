# INC-2026-08-04 Smart Update identity/source/replay corruption

Status: open
Severity: sev1
Service: Smart Update event import and affected public event surfaces
Opened: 2026-08-04
Closed: —
Owners: Smart Update service owner / production incident owner
Related incidents: `INC-2026-07-02-boyko-exhibition-smart-update-glue`, `INC-2026-07-18-dramteatr-same-day-event-glue`, `INC-2026-07-02-exhibition-duplicates-static-site`, `INC-2026-07-10-future-event-semantic-audit`, `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-07-31-poster-candidate-url`
Related docs: `docs/features/smart-event-update/README.md`, `docs/operations/release-smoke-smart-update.md`, `docs/operations/sqlite-db-init.md`, issue `#297`

## Summary

Production evidence from GitHub Actions run `30889892515` confirmed that Smart
Update could merge distinct events, bind a direct ticket source to a different
event identity, and mutate event prose on an identical Telegram/VK packet
replay. Events `3864`, `7024`, `7244`, and `7435` require a narrowly scoped,
transactional repair. Event `7151` is a negative control and must remain
unchanged unless new direct evidence appears.

## User / Business Impact

- An event card could contain facts, media, or a public page from another event.
- Two same-place/same-day screenings could share the wrong direct ticket source.
- An identical upstream packet could cause provider work and repeated prose
  drift rather than a no-op.
- Telegraph, ICS, and static projections derived from corrupt identity data can
  expose the same errors publicly.

## Detection

- Protected read-only audit run `30889892515` produced classification `FAIL`.
- Evidence artifact:
  `smart-update-prod-audit-0d1848bc324ef8c44df146ec2a7126a116a94bf4-30889892515`;
  supplied SHA-256 `aecdbc13013ce14c0b8c9a0dcbe995ee36757980f3393c3e2fcd592df6dd3fb6`.
- Follow-up investigation is limited to the affected graph and exact source
  evidence; a second broad audit is explicitly out of scope.

## Timeline

- 2026-08-04: protected audit evidence accepted as incident input.
- 2026-08-04: direct Tretyakov ticket identities for slots `46315`, `48801`,
  and `48636` independently confirmed without production mutation.
- 2026-08-04: incident opened; code, replay, snapshot, release, and repair gates
  defined below.

## Root Cause

1. Production configured the general identity gate in `enforce` mode but left
   the separate merge identity gate at its `off` default.
2. `EventSource` uniqueness was scoped to raw `(event_id, source_url)` and did
   not distinguish a direct identity-bearing source from a reusable programme
   context source or protect a canonical direct URL across event identities.
3. Smart Update did not stop an exact source-packet replay before LLM calls and
   writes, allowing warm replay prose drift tracked by issue `#297`.

## Contributing Factors

- Ticket SPA fragment variants (`#buy` and `#/buy`) and Telegram URL variants
  were not represented by one persistence-grade canonical identity.
- Merge-gate shadow decisions did not protect the production write path.
- Historical sources include both legitimate shared programme context and
  corrupt direct bindings, so a blanket legacy uniqueness migration is unsafe.

## Automation Contract

### Treat as regression guard when

- changing Smart Update candidate matching, merge gates, source persistence,
  packet replay, EventSource schema, or affected publication scheduling;
- changing URL canonicalization for Telegram, VK, or direct ticket sources.

### Affected surfaces

- `smart_event_update.py`, `smart_update_identity.py`, `db.py`, `models.py`;
- Fly Smart Update identity-gate configuration;
- Event/EventSource/EventSourceFact/EventPoster and narrowly related outbox rows;
- affected Telegraph, ICS, month/weekend/festival projections and immutable
  static candidate output.

### Mandatory checks before closure or deploy

- Boyko and Pianissimo/exhibition distinct-identity regression replays;
- same-day Tretyakov `48801`/`48636` source-separation replay;
- shared festival post as `context_only` without identity mutations;
- exact Telegram and VK warm replay with zero provider/domain/outbox writes;
- edited packet at the same URL proceeds with a different fingerprint;
- enforce-mode identity-gate error fails closed before all domain side effects;
- repair dry-run/apply/second-apply/verify on a disposable production snapshot;
- `PRAGMA quick_check`, whitelist diff, and negative control `7151` unchanged;
- clean exact-`origin/main` deploy and in-container SHA/config verification.

### Required evidence

Ignored receipts live under
`artifacts/codex/INC-2026-08-04-smart-update-identity-source-replay-corruption/`:

- evidence manifest and supplied audit digest;
- sanitized source-binding ledger;
- minimal before snapshot and compact repair diffs;
- snapshot migration/replay and rollback-drill receipts;
- deploy, repair, verification, and public-surface receipts.

No receipt may contain secrets, source text, prompts/completions, Telegram user
identifiers, user data, or a full production database.

## Immediate Mitigation

- Keep the affected production graph quarantined from broad republishing during
  repair.
- Do not mutate event `7151`.
- Do not run a broad audit, outbox cleanup, media retry, or limiter change.

## Corrective Actions

- [ ] Enable `SMART_UPDATE_MERGE_IDENTITY_GATE=enforce` in production config.
- [ ] Fail closed on unsafe/review/error gate results before domain side effects.
- [ ] Add canonical source URL, source role, and stable packet fingerprint.
- [ ] Protect identity-bearing canonical source ownership transactionally and at
  DB level after historical conflicts are explicitly repaired/classified.
- [ ] Return an exact-packet no-op before the first LLM call or write.
- [ ] Apply the idempotent whitelisted production repair only after exact-main
  code deploy and production dry-run approval.

## Follow-up Actions

- [ ] Close issue `#297` with immutable Telegram/VK replay evidence.
- [ ] Verify only affected supported public surfaces; production static-root
  promotion remains disabled and must not be enabled by this incident.

## Release And Closure Evidence

- PR: pending
- merge/deployed SHA: pending
- Fly version/image: pending
- regression checks: pending
- production dry-run/apply/second-apply/verify: pending
- source-transfer ledger and before/after public URLs: pending
- post-deploy verification: pending

## Rollback

- A failed repair rolls back its single transaction.
- Post-commit rollback restores only whitelisted rows with compare-and-swap
  checks against recorded after-state hashes, removes only whitelisted rows/jobs
  created by the repair, and rebuilds the same affected surfaces.
- Additive source-role columns and the uniqueness invariant remain in place.
- Code rollback is a revert merged to `main` and an exact-main deploy. An
  emergency `enforce` to `shadow` change requires pausing affected automatic
  imports and leaves this incident open; the gate must never silently be turned
  off.

## Prevention

Canonical role-aware source ownership, a database invariant, fail-closed merge
gating, exact-packet replay idempotency, and incident-specific replay fixtures
become mandatory regression contracts for future Smart Update changes.
