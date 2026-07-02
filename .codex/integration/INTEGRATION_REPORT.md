# Integration Report — Smart Update Vector Identity Gate

Base: `origin/main` / `f44a3f3db3112e03e2cbf6ba4e24fff44cd1afc8`
Integration branch: `integration/smart-update-vector-identity-gate`
Head: `152b3d26`

| Lane | Requirement IDs | Branch/commit | Status | Evidence |
|---|---|---|---|---|
| schema-core | R01, R10 | `4f72962a` | merged | event identity/date provenance fields; `event_identity_decision_log`; `event_identity_lock`; py_compile + sqlite smoke in lane |
| vector-rpc | R02, R03, R11 | `07e16fee` + integration `152b3d26` | merged | `event_identity.py`; service-role RPC migration; related_v1/search_v3 union recall; 6 event identity tests |
| smart-update-core | R04, R05, R06, R11, R12, R14 | `df4f9a9b` + integration `152b3d26` | merged | identity gate flags, structured verdicts, deterministic/vector guard, insertion before create; 9 identity gate tests |
| date-media | R07, R08 | `86ba2db2` | merged | date provenance trust helpers and poster dedup helpers integrated into Smart Update; 27 lane tests |
| public-gate | R09 | `c0cf3ffe` | merged | exporter-side public projection gate; include/control ids covered; static public gate tests |
| tests-replay | R13 | integration `152b3d26` | partial/merged | replay-contract tests for listed clusters and recurring negative controls; not full production import boundary replay fixtures |

## Integration tests

- `python3 -m compileall -q event_identity.py smart_update_identity.py smart_event_update.py db.py models.py site/scripts/export-production-preview-data.py tests/test_event_identity.py tests/test_smart_update_identity_gate.py tests/test_smart_event_update_date_media_helpers.py tests/test_static_site_public_gate.py`
- `git diff --check`
- `uv run --isolated --with-requirements requirements.txt --with pytest pytest -q tests/test_event_identity.py tests/test_smart_update_identity_gate.py tests/test_smart_update_identity_replay_contracts.py tests/test_smart_event_update_date_media_helpers.py tests/test_static_site_public_gate.py tests/test_dedup_adjudicator.py tests/test_genai_dump_and_poster_dedup.py` → 63 passed
- `uv run --isolated --with-requirements requirements.txt --with pytest pytest -q tests/test_pre_create_duplicate_probe.py tests/test_dedup_adjudicator.py` → 22 passed

## Known verification issue

Broader pre-existing/adjacent suite command `tests/test_pre_create_duplicate_probe.py tests/test_smart_event_update_duplicate_guards.py tests/test_smart_event_update_non_event_guards.py` has one failure: `test_smart_update_rejects_reaction_text_location_candidate` currently returns `skipped_non_event` instead of expected `invalid` because LLM-disabled eventness review fires before the later prose-location invalid branch. This is not in the new identity-gate code path, but should be resolved before merging if full suite cleanliness is required.
