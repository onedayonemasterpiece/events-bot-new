# INC-2026-08-22 SOS dedup veto override and Tyunin Farm location drift

Status: investigating
Severity: sev1
Service: Telegram Monitoring / Smart Update identity / public Telegram and Telegraph projections / location reference
Opened: 2026-08-22
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-06-30-prose-location-non-event-daily-duplicate`, `INC-2026-07-07-new-event-quality-degradation`, `INC-2026-08-01-kldevents-event-quality`, `INC-2026-08-10-smart-update-identity-terminal-loss`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/smart-event-update/identity-state-machine.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/smart-update-prod-audit.md`

## Summary

Operator review of the public 22 August digest found five active/public rows covering the same 22 August 21:00 `Барн / SOS` slot:

| Event ID | Public title | Added at | Ticket | Notes |
| --- | --- | --- | --- | --- |
| `8242` | `Праздничный SOS` | `2026-08-22 01:51:55` | `https://barn.timepad.ru/event/4147114` | Newly created fragment from `https://t.me/kldevents/3619`; no linked events and no identity decisions on the created row |
| `8117` | `Тройной день рождения: Барн, Chipi Clo и SOS` | `2026-08-20 03:33:12` | same Timepad URL | Umbrella festival row; the source `https://t.me/kldevents/3619` was already attached here |
| `8115` | `SOS — легендарная вечеринка` | `2026-08-20 03:27:53` | same Timepad URL | 21:00 party child; linked to `7881` and `7885` |
| `7881` | `SOS — легендарная вечеринка` | `2026-08-18 01:49:58` | same Timepad URL | Exact-title 21:00 duplicate; linked to `7885` and `8115` |
| `7885` | `Вечеринка SOS` | `2026-08-18 02:02:30` | — | 21:00 party variant; linked to `7881` and `8115` |

The decisive failure is reproducible from durable identity evidence. For candidate source `https://t.me/kldevents/3619`, Smart Update recorded an enforced deterministic `veto_create` on event `8117` at `2026-08-22 01:51:35.740895`, confidence `0.92`, reason `deterministic_same_ticket_slot`: same ticket URL, date/time slot and related title/location. About twenty seconds later the same candidate became new active event `8242`.

The operator also reported a venue normalization defect for event `7717`, `Прогулка с Фонарщиком`:

- original VK source: `https://vk.com/wall-96427382_6194`;
- source text says `ждём вас в кафе Ферма у камина` and contains `#ферматюниных`;
- stored public venue: `Ферма у камина`;
- stored address: `NULL`;
- expected canonical venue: `Ферма Тюниных`, Знаменск.

On current `main` at repository SHA `a68c7f23c4e014c6e9f66e95f394656e9cb0f411`, root `LOCATIONS.md` is only a redirect to `docs/reference/locations.md`. The active canonical file contains neither `Ферма Тюниных` nor `Ферма у камина`; `docs/reference/location-aliases.md` contains no `ферма`/`ферматюниных` alias. Therefore the current reference layer cannot canonicalize this source even though the hashtag is a strong venue identity signal.

## User / Business Impact

- One real 21:00 programme slot occupies five positions in a reader-facing digest and has several Telegraph pages.
- Exact and near-exact duplicates dilute discovery, damage trust and can fan out independently to Telegram, VK, ICS, static pages, vector search and future digests.
- A deterministic fail-closed identity decision is contradicted by the committed database state; this means the invariant cannot currently be trusted for other imports.
- Venue identity is lost for event `7717`; the public card has a local sub-venue phrase without the canonical place/address.
- The same source/publication can be attached to an existing event and later create another event, indicating an idempotency or re-ingestion boundary failure.

## Detection

- Detected by operator review of the 22 August public event list.
- Canonical production evidence was read through eventsBot MCP.
- Current repository code and reference data were inspected through GitHub.
- No existing open GitHub issue was found for this exact SOS/Tyunin Farm case.

## Timeline

- `2026-08-18 01:49–02:02` — events `7881` and `7885` are created for the SOS party/programme.
- `2026-08-20 03:27–03:33` — events `8115` and `8117` are created.
- `2026-08-22 01:50:03` — Smart Update records an `allow_merge` decision for the later `kldevents/3619` update path on event `8117`.
- `2026-08-22 01:51:35` — Smart Update records enforced deterministic `veto_create`, reason `deterministic_same_ticket_slot`, matched event `8117`.
- `2026-08-22 01:51:55` — event `8242` is nevertheless inserted as a new active event from `https://t.me/kldevents/3619`.
- `2026-08-22` — operator reports five public SOS rows and the `Ферма у камина` location drift.
- `2026-08-22 08:55 UTC` — operations snapshot reports the separate `exhibition_duplicate_audit` run `6878` failed while listing `89` potential duplicate pairs. This audit has a different scope and is only a signal for a broader census, not proof that all 89 pairs are genuine duplicates.

## Confirmed Technical Cause: create-gate verdict can be overridden

Current `smart_event_update.py` evaluates the deterministic identity gate, records `veto_create`, loads the matched event, and then passes that event into the widened LLM dedup adjudicator.

In the adjudicator branch:

1. any non-null typed decision sets `identity_gate_adjudicated = True`;
2. `_dedup_adjudicator_accept_merge(...)` may reject merge and return `no_merge`;
3. the code logs the rejection and continues;
4. the later fail-closed guard runs only when `not identity_gate_adjudicated`;
5. the create path is therefore still reachable after an enforced deterministic veto.

This orchestration contradicts the create-gate contract: a strong `VETO_CREATE` currently means “ask the LLM once more”, while the durable log still presents it as an enforced veto. A model decision can authorize CREATE without a persisted final override record.

The exact SOS sequence is consistent with this path: deterministic same-ticket-slot veto is logged against `8117`, then the candidate is created as `8242`. The final LLM distinct/no-merge response is not visible in `event_get(8242)` or the identity-decision log, which is a second observability defect.

## Additional Root-Cause Hypotheses To Verify

### Same-source re-ingestion / replay race

`https://t.me/kldevents/3619` is already present as identity-bearing evidence on event `8117`, but the same URL is the `source_post_url` of new event `8242`. Verify whether this was:

- self-re-ingestion of a managed `@kldevents` publication;
- concurrent processing of one candidate with stale shortlist state;
- replay/retry after a successful merge without source-level idempotency;
- or a combination of those paths.

Regardless of the producer, a source URL already attached as identity-bearing evidence must not create a second event without an explicit, durable and justified split decision.

### Location reference data loss

The canonical Tyunin Farm row/aliases expected by the operator are absent from current `main`. Determine whether they:

- existed only in an older/local `locations.md`;
- were lost during the redirect/migration to `docs/reference/locations.md`;
- live in an unmerged branch;
- or were never committed.

Do not invent the address. Recover or verify the canonical line from authoritative source/history before data repair.

## Immediate Mitigation

- This incident record was committed before prevention/data mutation.
- No production rows or public posts were deleted or merged during evidence collection.
- Treat event `8242` and its pending/future publication jobs as suspect until the canonical cluster is resolved.
- Do not run destructive bulk dedup solely from `exhibition_duplicate_audit`; its list contains parent/child and exhibition/excursion siblings that require typed identity review.

## Corrective Actions

### P0 — restore the identity invariant

- [ ] Make `IdentityGateAction.VETO_CREATE` in `ENFORCE` mode a hard transactional barrier: the same attempt may end only in `MERGED`, a durable `RETRY_SCHEDULED`/review outcome, or an explicit typed override with stronger contradictory identity evidence. It must never fall through to ordinary CREATE.
- [ ] For `deterministic_same_ticket_slot`, disallow automatic `distinct/create` override when ticket URL, date, exact start time and canonical venue all agree. A genuine multi-session/sibling exception must expose a concrete blocking difference and use a separate auditable decision type.
- [ ] Persist the final adjudicator result after a gate veto: candidate fingerprint, source URL, gate match id, action, relation, confidence, reason code, blocking conflicts, model/schema version and whether CREATE remained reachable.
- [ ] Add a final transaction-bound source-identity check: if the normalized identity-bearing source URL is already attached to an active event, lock/re-read that owner and merge/retry instead of inserting a new row.
- [ ] Verify candidate-attempt idempotency and concurrent-worker serialization for the same source URL/fingerprint.

### P0 — exact replay and tests

- [ ] Add an incident replay fixture for `https://t.me/kldevents/3619` using the pre-existing rows `7881`, `7885`, `8115`, `8117`.
- [ ] Regression: same Timepad URL + `2026-08-22` + `21:00` + `Барн` + related SOS title produces no new event.
- [ ] Regression: a deterministic veto followed by LLM `distinct/no_merge` cannot reach `_create_from_prepared_candidate()`.
- [ ] Regression: the final identity outcome is durable and queryable after every gate/adjudicator path.
- [ ] Concurrency regression: two simultaneous attempts for the same source/fingerprint result in one canonical owner.
- [ ] Positive opposite control: two genuinely distinct same-venue events with different ticket/session identity remain separate.

### P1 — public projection and data repair

- [ ] Define and test the product policy for umbrella programme versus child events. For this case, the reader-facing 21:00 slot must appear once even if one umbrella row and one child row are retained internally.
- [ ] Build an explicit remediation map for `7881`, `7885`, `8115`, `8117`, `8242`; preserve all source evidence, posters, ticket facts and publication provenance before deactivating/merging rows.
- [ ] Rebuild Telegraph, Telegram daily/weekend, VK, ICS, static and vector projections after repair; verify that stale URLs do not remain in current lists.
- [ ] Audit events created in the last 7 days for exact normalized ticket URL + overlapping date + exact time + canonical venue, and separately for one identity-bearing source URL attached to multiple active events.

### P1 — Tyunin Farm canonicalization

- [ ] Recover/verify the canonical `Ферма Тюниных` location line and address for Знаменск.
- [ ] Add the verified canonical row to `docs/reference/locations.md`.
- [ ] Add curated aliases at minimum for `ферма у камина`, `кафе ферма у камина`, `ферматюниных` and `#ферматюниных` normalization semantics.
- [ ] Regression: `find_known_venue_in_text("ждём вас в кафе Ферма у камина ... #ферматюниных", city="Знаменск")` resolves one canonical venue and fills the verified address/city.
- [ ] Repair event `7717`, preserve the original sub-venue wording as a fact when useful, and rebuild its public projections.

## Acceptance Criteria

1. Replaying the exact SOS source against a production-like snapshot creates zero additional active rows.
2. An enforced deterministic veto cannot be followed by an unlogged CREATE.
3. The source URL `https://t.me/kldevents/3619` has one canonical identity owner after repair.
4. The public 22 August 21:00 Barn/SOS slot is represented once according to documented umbrella/child policy.
5. Event `7717` resolves to the verified canonical Tyunin Farm venue and address.
6. A bounded 7-day census and pre/post repair report are attached to the incident.
7. Targeted tests, changed-module `py_compile`, `git diff --check`, release-governance checks and post-deploy production verification pass.

## Required Evidence Before Closure

- Exact source/candidate replay and pre/post event/source/identity-decision snapshots.
- Trace proving the final identity action after deterministic veto.
- Concurrency/idempotency test output.
- Canonical location source/history evidence for Tyunin Farm.
- Data repair backup and row/publication mapping.
- Final public URLs/screenshots or API checks for Telegram, Telegraph, VK and current digest.
- Deployed SHA reachable from `origin/main`, `/healthz`, DB quick check and relevant runtime logs.

## Release And Closure Evidence

- prevention PR/SHA: pending
- data repair run: pending
- deploy path: pending
- post-deploy verification: pending
- closure decision: pending
