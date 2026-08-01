# Static collections data-prep integration report

Date: 2026-08-01
Branch: `integration/static-collections-data-prep-20260801`
Scope: data collection/preparation only; no Astro routes/navigation/sitemap, no
public rollout, no cinema-source work, no festival extraction/page work.

## Source of truth and production audit

Core data remains the single immutable Fly SQLite snapshot already supplied to
the existing StaticSiteBuilder. The new collection/venue/club path records
`supabase_core_reads=0`; it does not start another notebook or transfer a second
snapshot. Production was inspected read-only on Fly at 2026-08-01 15:43 UTC
(DB mtime 15:33 UTC, size 312,098,816 bytes, `integrity_check=ok`). No production
writes, migrations or deploy were performed.

Control evidence used by the implementation:

- 6 approved club identities and 13 exact accepted relation events in the
  inclusive 2026-02-01..2026-08-01 activity window; 2 shadow identities remain
  non-public;
- 8 exact official theatre organizations and 6 venue-page candidates; venue
  current/future event counts were 80/29/23/23/20/14 on the audit snapshot;
- admission review controls include 5370, 7145, 7244, 7246, 7247 and 7287;
  the code supplies correction provenance but does not mutate those live rows;
- current/future audience/people supply has topics and source text but lacks a
  canonical grounded audience/person relation, justifying the candidate-only
  Smart Update stage.

Counts are fixtures/audit controls, never runtime hardcodes.

## Lane integration

| Lane | Requirement | Integrated commits | Disposition |
|---|---|---|---|
| W_REGISTRY | exact theatre/venue registry | `9377df6b`, `3901ea8f` | Done |
| W_FACTS | admission/audience/people facts | `5375da2b`, `a79d6f4a` | Done in code; live targeted backfill pending |
| W_SEMANTIC | debounce/shared BGE/batch handoff | `bb7fc3b9`, `eae81e0b`, `4e564392` | Done in code; real Kaggle cold/warm acceptance pending |
| W_CLUBS | durable club update + six-month projection | `a16e49f1`, `4be9020d` | Done in code; production migration/promotion pending |
| Integrator | exporter wiring/docs/reconciliation | `d9be593e`, `a5f36229`, `7fa53572`, `870f155c`, `ee15beff` | Done |

Individual lane evidence is stored under
`.codex/lanes/static-collections-W_*/RESULTS.md`. `origin/main@416d17e6` was merged
without losing concurrent Region Talk/PWA/VK changes; the only textual conflict
was `CHANGELOG.md`, resolved by retaining both sides. The original requirement
file `podborki.md` remains byte-identical to analysis commit `c01e0ade`
(SHA-256 `e0c295cc372e311e6d989de5fad35a962af31906f883a9f2b99d1fc533ad0872`).

## Delivered contracts

1. **Clubs:** durable `JobTask.interest_club_relation`, immutable running owner
   plus one successor, retry/history that preserves compatible accepted truth,
   bounded shadow discovery and `interest-clubs-static-v2.json` with inclusive
   six-calendar-month visibility.
2. **Entities:** checked-in place/organization registry, exact structured
   source/organizer/venue reasons, 8 theatre entities and 6 venue pilots with
   organization/venue roles kept separate.
3. **Facts:** additive nullable `Event.collection_decisions`, strict source-bound
   admission/audience/people schemas, atomic accepted source attachment,
   unknown/failure preservation and `Event.is_free` materialization.
4. **Semantic batch:** evidence-only `collection_semantics_v1`, pinned BGE-M3,
   physically validated float32 cache, prototype-independent event reuse, one
   namespaced matrix pass and per-label compute/quality/publication state in
   `collection-batch-v1.json`.
5. **Reliability:** production-candidate requires compute independent of legacy
   Unusual/public flags; strict trailing Smart Update +15m scheduling retains
   one-running/one-follow-up recovery semantics.
6. **Astro boundary:** ID-only `collection-batch-v1.json`,
   `venue-pages-v1.json`, `interest-clubs-static-v2.json`; Astro may render but
   must not recalculate membership from prose.

Exact shelves are shadow-ready. Semantic heads, including Unusual on the new
document, remain explicitly blocked until gold; this is intentional, not a
missing configuration.

## Verification

- collection/semantic/export/release: `123 passed`;
- club/outbox/Smart Update integration: `63 passed`;
- facts/DB/ticket/participant/May-incident replay: `85 passed` (one unrelated
  Pydantic deprecation warning);
- Kaggle status/handoff/unusual/outbox incidents: `116 passed`;
- post-lint registry/semantic/club smoke: `57 passed`;
- lane registry tests: `26 passed`; lane facts tests: `77 passed`; lane semantic
  tests: `92 passed`; lane club tests: `41 passed, 1 stale test deselected` plus
  `62 passed`; the stale debounce test was corrected by the integrator and is
  included in the green 123-test run;
- `py_compile` passed for all modified runtime/migration/exporter/runner files;
- focused Ruff passed after import/style cleanup; JSON fixtures/policies parse;
- migration graph check: `20260731_festival_web_research ->
  20260801_static_collection_facts -> 20260801_club_eval_history`; W_CLUBS
  standalone SQLite upgrade preservation probe returned
  `migration-upgrade-ok rows=2`;
- `git diff --check` passed.

The repository has no Alembic configuration containing `script_location`, so a
plain `alembic heads` command is not a valid project check. The graph was
validated directly and the actual table-copy upgrade was exercised by the lane
probe instead of inventing a configuration.

## Requirement disposition

| ID | Status | Notes |
|---|---|---|
| R01 clubs | Done | implementation/tests complete; production rollout pending |
| R02 registry | Done | exact entities/reasons/fixtures complete |
| R03 scheduling | Done | local contract complete; live build exercise pending |
| R04 admission | Partial release | code complete; targeted production review/backfill/rebuild not run |
| R05 audience/people | Partial release | strict stage complete; owner benchmark/backfill not run |
| R06 BGE/batch | Partial release | compute/cache/batch complete; real cold/warm Kaggle and owner gold absent; no semantic label can publish |
| R07 integration | Done | docs/CHANGELOG/release plan/reconciliation complete |

## External review blocker

Required Opus-class consultation could not run: `a-opus` rejected the location
and Claude project alias `Opus` was not authenticated. Evidence is in ignored
`artifacts/codex/static-collections-data-prep-20260801/`. No lower-class model
was represented as external consultant review.

## Required next release steps

1. Merge this branch through the normal main-based review; do not cherry-pick
   only exporter/UI fragments.
2. Apply both additive migrations first to a verified production copy, then in
   an approved production change window.
3. Run targeted admission/audience/people review/backfill; do not scan the full
   historical archive.
4. Run real current-catalog pinned Kaggle cold/warm builds and require complete
   coverage, `provider_calls=0`, unchanged-event re-encode 0 and exact receipts.
5. Obtain owner gold/recalibration for each semantic head; Unusual incident stays
   open until a current accepted route exists.
6. Hand the same branch/manifests to the separate Astro integration window for
   pages/navigation/indexability; public promotion remains governed by
   `docs/features/static-site-pages/release-plan.md`.
