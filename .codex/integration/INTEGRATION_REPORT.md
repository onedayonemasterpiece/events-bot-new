# Integration Report — Smart Update Vector Identity Gate

Base: `origin/main` / `f44a3f3db3112e03e2cbf6ba4e24fff44cd1afc8`
Integration branch: `integration/smart-update-vector-identity-gate`
Head: updated after reviewer fixes

| Lane | Requirement IDs | Branch/commit | Status | Evidence |
|---|---|---|---|---|
| schema-core | R01, R10 | `4f72962a` | merged | event identity/date provenance fields; `event_identity_decision_log`; `event_identity_lock`; py_compile + sqlite smoke in lane |
| vector-rpc | R02, R03, R11 | `07e16fee` + integration fixes | merged | `event_identity.py`; service-role RPC migration; `identity_candidate_v1` document includes model/dim metadata; related_v1/search_v3 union recall; tests |
| smart-update-core | R04, R05, R06, R11, R12, R14 | `df4f9a9b` + integration fixes | merged | identity gate flags, structured verdicts, deterministic/vector guard, vector recall evidence, insertion before create, low-risk vector-failure fallback, final pre-insert duplicate probe |
| date-media | R07, R08 | `86ba2db2` + integration fixes | merged | date provenance trust helpers; newly created events populate date/end_date provenance/confidence; poster dedup helpers integrated into Smart Update |
| public-gate / monitor | R09, G03 | `c0cf3ffe` + continuation | partial/merged | exporter-side public projection gate remains partial because `origin/main` lacks the static-site caller; added repo-native read-only SQLite `/vystavki/` monitor `scripts/inspect/audit_public_exhibition_duplicates.py` with JSON/Prometheus metrics, real `event.added_at`-based 14-day window semantics, and fail-on-high-confidence mode |
| scheduled-ops-audit | G03, G04 | continuation | merged | optional `ENABLE_EXHIBITION_DUPLICATE_AUDIT=1` APScheduler job records `ops_run(kind='exhibition_duplicate_audit')`, alerts admin on high-confidence pairs, and raises after persisted `failed` status; env/docs/tests added |
| tests-replay | R13, G01, G02 | integration fixes + continuation | merged | import-boundary Smart Update replay tests cover all listed exhibition clusters, recurring high-similarity control, and same-source multi-session control; pure replay-contract tests remain as constants/evidence |

## Integration tests

- `python3 -m compileall -q event_identity.py smart_update_identity.py smart_event_update.py db.py models.py site/scripts/export-production-preview-data.py tests/test_event_identity.py tests/test_smart_update_identity_gate.py tests/test_smart_event_update_date_media_helpers.py tests/test_static_site_public_gate.py`
- `git diff --check`
- `PYTHONDONTWRITEBYTECODE=1 uv run --isolated --with-requirements requirements.txt --with pytest pytest -q -p no:cacheprovider tests/test_event_identity.py tests/test_smart_update_identity_gate.py tests/test_smart_update_identity_replay_contracts.py tests/test_smart_update_identity_persistence.py tests/test_smart_update_identity_incident_replay.py tests/test_exhibition_identity_duplicate_monitor.py tests/test_exhibition_duplicate_audit_scheduler.py tests/test_smart_event_update_date_media_helpers.py tests/test_static_site_public_gate.py tests/test_dedup_adjudicator.py tests/test_genai_dump_and_poster_dedup.py` → 80 passed
- `uv run --isolated --with-requirements requirements.txt --with pytest pytest -q tests/test_pre_create_duplicate_probe.py tests/test_dedup_adjudicator.py` → 22 passed

## Known verification issue

Broader pre-existing/adjacent suite command `tests/test_pre_create_duplicate_probe.py tests/test_smart_event_update_duplicate_guards.py tests/test_smart_event_update_non_event_guards.py` has one failure: `test_smart_update_rejects_reaction_text_location_candidate` currently returns `skipped_non_event` instead of expected `invalid` because LLM-disabled eventness review fires before the later prose-location invalid branch. This is not in the new identity-gate code path, but should be resolved before merging if full suite cleanliness is required.

## Reviewer follow-up status

- Persisted decision log: addressed for enabled identity gate invocations and final duplicate veto.
- Candidate doc metadata: addressed with `embedding_model` and `embedding_dim`.
- Date provenance population: addressed on create path.
- Failure policy: adjusted so vector infra errors fail safe for parser/weak-date candidates but allow low-risk source-grounded non-parser creates through existing Smart Update behavior.
- Concurrency: partially addressed with final pre-insert duplicate probe; full application lock/negative pair lock usage remains follow-up.
- Public/static gate: partial because the chosen base did not contain the active static site exporter/caller; mitigated with repo-native read-only `/vystavki/` duplicate monitor plus optional scheduled ops audit for the 14-day enforce acceptance window.
- 14-day metric semantics: `events_public_exhibition_duplicate_pairs_since_total` now uses `event.added_at` where available; schemas without `added_at` fail closed by counting pairs in the window.
- Incident replay: import-boundary Smart Update tests now prove 5/5 known exhibition clusters do not create a second canonical row, while recurring and same-source multi-session controls create distinct occurrences.
