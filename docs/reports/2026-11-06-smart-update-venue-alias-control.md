# Second-source venue control for Event 10619

## Result

The enabled production engine does not create the duplicate seen in supported LLM-disabled mode. It recalls the existing owner through widened adjudication, then rejects the same-address update because a lexical location difference becomes an unconditional blocking conflict. This is an isolated acceptance failure, not an observed live duplicate.

The opposite control is essential: the current model also incorrectly authorizes merging house 30 into house 3. The existing blocker prevents that false merge. Removing location blockers or trusting `allow_merge` alone is disproved by the same experiment.

## Exact scope and evidence

- Runtime: b86aa845d2bbe77c0410cb168da37fbc1741cb96, independent PR 846.
- Real owner fields: Event 10619, opening of the mural, November 6 at 14:00, Sovetsk. Stored location_name: `фасаде дома № 3 по улице Горького`; location_address is null.
- Candidate is a synthetic second-source Telegram message with the same title/date/time/city and either `ул. Горького, 3` or `ул. Горького, 30`. This is not replay of a real source or compensation.
- Each case uses a fresh initialized SQLite shadow with only that owner. Actual LLM and vector functions run unchanged. Topic enrichment and eventness are fixtures; region filtering is disabled only in the disposable process. Publication scheduling is disabled and external projections deferred.
- Shared provider audit passes: allowlisted_debt=0, unapproved=0. Actual Gemini embedding and Gemma calls use the existing atomic limiter. No raw-key call, quota change, fallback addition or production configuration update.
- Vector query succeeds but returns no candidates. This experiment proves widened LLM recall, not vector recovery of this owner.
- Production Event and Job writes: zero. Each shadow retains one seeded Event and zero JobOutbox rows. Main process and healthy guide run are not restarted.

## Outcomes

| Input venue | Final identity adjudicator | Merge-model response | Actual terminal |
| --- | --- | --- | --- |
| Same house, abbreviated address | FINAL_MATCH, same_event, owner 10619 | allow_merge, same_event, confidence 1.0; no model blocking conflicts | FINAL_RETRY / merge_identity_blocking_conflict |
| Different house 30 | FINAL_MATCH, same_event, owner 10619 | allow_merge, source_update, confidence 0.95; treats house difference as correction | FINAL_RETRY / merge_identity_blocking_conflict |

For the same-house case the model explicitly says both wordings identify the same address and opening slot. The persisted merge verdict nevertheless contains `location_name: ул. Горького, 3` as a blocker added outside the model. The different-house case demonstrates why that blocker cannot simply be deleted.

Earlier full-pipeline LLM-disabled controls: verbatim venue MERGED; abbreviated house 3 CREATED a second local Event; house 30 CREATED. Those results must not be presented as enabled-runtime behavior. Local duplicate ID 10620 is unrelated to actual production Event 10620.

## Mechanism and requested review

`smart_event_update.py` computes `conflicting["location_name"]` from `_location_matches`, then passes all `conflicting` fields as `blocking_conflicts` to the existing merge gate. `smart_update_identity.py::build_merge_identity_gate_verdict` unions those conflicts with model conflicts and fails closed even when the model says same_event. The persisted shadow decision proves that path ran; this is not source inference alone.

Please recommend the smallest correction to the existing semantic/grounding contract that accepts equivalent venue wording while retaining actual different-house protection. Assess whether prompt output and existing fields are sufficient. Do not propose a new normalizer, worker, retry, state, broad title/date dedup or generic removal of reason guards. A naive rule that accepts every model `allow_merge` fails the house-30 control. No implementation is claimed in this report.

Mandatory contrasting controls before implementation acceptance: same house with abbreviated/full address, different house, actual different venue, native ticket/date/time conflicts, and decision 19099 remaining FINAL_RETRY for its date contradiction. Preserve canonical owner/source history; no production rebind, retime or replay is authorized by this diagnostic.

## Retained evidence

Directory: `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103`.

- `auditor10619-enabled-shadow.py` and `.log`: exact harness and provider stages.
- `auditor10619-enabled-shadow-result.json`: terminal cases and unchanged shadow rows.
- `auditor10619-enabled-decisions.json`: exact persisted model and final decision payloads.
- `auditor10619-provider-audit-1133.log`: actual runtime provider audit.
- `auditor10619-isolated-pipeline-control.py/.json`: earlier LLM-disabled contrast.

Remote shadow evidence remains under `/data/incident_recovery/auditor10619-enabled-20261106`. The driver refuses an existing run directory and must not be rerun blindly. No production code change, deployment, public callback or source intake was performed.


## 2026-11-17: retained same-address venue-label recurrence

Candidate30403 provides the Geographic Dictation at the Museum of the World
Ocean, Petra Velikogo1V, Kaliningrad. Existing owner11003 names its educational
centre OKEANiYA at the same structured address and city. Retained decision23069
contains an affirmative model source_update with no model blocking conflicts;
the caller's lexical location_name conflict overrides it to FINAL_RETRY in23070.
The source was not attached. The carrier terminal FAILED_TECHNICAL preserves its
accepted sibling; the identity retry worker remains disabled by the owner policy.

The narrow correction filters only this deterministic name blocker when both
cities are present and equal and the existing complete-token address comparator
matches. The original conflicting map is retained as identity-model context and
as prose do-not-use input. Other caller/model blockers and the global fail-closed
builder are unchanged. Equal addresses do not themselves assert event identity;
the existing semantic decision must still allow the merge. Non-parser updates
preserve the populated canonical venue/address. allowed_fields is logged and is
not an application whitelist; it is not the basis of this protection.

The caller-level regression uses a retained-shaped source update plus different
house, missing address, different city/date/time and model-conflict controls.
It checks source attachment and preservation of the owner logistics. Recall and
the identity model are fixtures; this is not a new live inference or intake replay.
Historical10619 has no structured owner address and retains its blocker. The old
candidate30403 and existing VK posts are not replayed or rewritten by this change.
