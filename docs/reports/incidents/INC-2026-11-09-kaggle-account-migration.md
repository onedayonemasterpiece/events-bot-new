# INC-2026-11-09-kaggle-account-migration

Status: mitigated
Severity: sev2
Service: Events Bot / RunCoveer / Kaggle
Opened: 2026-11-09
Registry: inc_bcc270b783404a2b6458aa54
Related incidents: INC-2026-04-16-cherryflash-kaggle-save-kernel-drift

## Summary and impact

Rotating the API username/key did not migrate notebook IDs, dataset sources or video-lane refs. The new account richss777 could upload inputs, but a configured BGE artifact remained under the previous owner. SaveKernel returned created versions with invalid dataset sources. Retrying every ten seconds created versions 1–16, exhausted five CPU sessions and blocked social metrics. A successful notebook exit did not prove a result was imported by Events.

## Owner decisions

2026-11-09, current task chat: finish the migration, remove previous-owner operational references, and make no requests to the old account or its resources. Use local assets and the new account only. Historical evidence is preserved.

## Root cause

- Credentials were changed without auditing metadata, defaults, environment refs and recovery records.
- The invalid-source retry treated a created notebook version as an unapplied request.
- The generic runner read its polling/ledger owner from repository metadata while push could override it.

## Automation contract

Treat as a regression guard for Kaggle account migration, kernel push, dataset binding and recovery.

Affected surfaces: kaggle/*/kernel-metadata.json, source parser defaults, video lane refs, accelerator preferences, BGE dataset configuration, Kaggle registry and run ledger.

Mandatory checks:
- No previous-owner reference in operational source, live environment or active recovery registry.
- One SaveKernel invocation when an invalid-source response contains a created version; explicit failure retained.
- Existing GPU-quota CPU fallback and nonempty SaveKernel.error rejection still pass.
- BGE waits for dataset readiness with the existing helper before submitting a notebook.
- Focused Kaggle/guide/video regression tests, main ancestry and Events-only release.
- One real new-account task completes and its output is imported.

## Timeline and evidence

- 18:43 UTC: credentials rotated; API access and Events readiness verified, full pipeline not yet qualified.
- 19:08–19:26 UTC: job 152116 retries; versions 11–16 at 19:25–19:26, then maximum five CPU sessions reached.
- 19:15 UTC: social metrics launch failed at the same CPU-session limit.
- Evidence retained under /home/dev/artifacts/events-bot-new/20261109T193558Z-kaggle-account-20261109.

## Corrective actions

- Replace operational account references in metadata, defaults, preferences and fixtures.
- Remove invalid-source push retries. Keep explicit errors and existing GPU-quota fallback.
- Reuse dataset-readiness polling before BGE submission.
- Migrate live refs and retire old recovery records without querying the old account.

## Release and verification

- PR #881 merged at 19:51 UTC after all four required CI jobs passed.
- Exact main/source SHA: `cdad5f743c61910891e05e6fcbe91682af6ba1ee`.
- Running image: `runcoveer/events:kaggle-account-20261109`, ID `sha256:bcc49d17611198f346a57aa5d01905e6c414599b0857ff481a5e395e626f100a`.
- Prior running SHA `486abac3e8cf8a6ec3902f4168fc8c89526c19df` is an ancestor. Dependency inputs are unchanged; all 4,465 tracked files passed candidate-image verification.
- Offline real-handler startup passed with network disabled and an isolated fresh fixture DB (`quick_check=ok`). Before scheduler readiness, HTTP health intentionally reports DB `skipped`; this is not a failed DB connection.
- Events-only cutover at 19:56 UTC; Kotopogoda container unchanged. Live readiness: `ok=true`, `ready=true`, DB `ok`, issues empty, required tasks healthy.
- Runtime audit: zero retired-account references in operational source/environment/active registry/nonterminal ledger. Three stale registry entries and 1,784 stale nonterminal ledger records retired locally; completed history preserved. No requests or remote cancellation to the retired account after the owner's prohibition.
- Real credentials/product-flow proof: `social-metrics:996807`, `richss777/kenigevents-social-metrics-collector`, exactly one pushed version (1), status `done`, phase `imported` at 19:47:56 UTC. Collected/imported count 101, collection errors 0, resolution errors 0. This run predates the source cutover; it qualifies the new credentials and unchanged successful path, not unavailable BGE assets.
- Tests: 803 tests across 20 changed test files; 796 initially passed and seven environment/date-fixture failures were covered by a successful 10-test recheck. Additional focused client/accelerator and BGE/notebook groups passed (31 and 13 tests). Full CI is green.
- Evidence directory: `/home/dev/artifacts/events-bot-new/20261109T193558Z-kaggle-account-20261109` (retained), including build, preflight, cutover, live health, migration, account audit and import receipts.

## Remaining data blockers

The new-account inventory lacks seven configured prepared datasets:

- `richss777/event-age-bge-approved-v3-20260715`
- `richss777/video-announce-assets`
- `richss777/afisha-dataset-2`
- `richss777/koenigsberg19191940`
- `richss777/koenigsberg-winter`
- `richss777/koenigsberg-music`
- `richss777/kenigsberg-audio`

Exact-name searches across local project worktrees, retained artifacts and available backups found no prepared BGE classifier/evaluation/vector bundle. The owner does not know its location. Do not treat repository test media as an authoritative replacement for missing production assets. BGE now fails before input creation/notebook submission when its prepared dataset is unavailable, leaving the numeric quality contract intact. Video consumers of the missing static datasets remain unqualified. This incident is mitigated, not closed; the data migration is not complete.

## Follow-ups

Prepared private datasets must exist under the new account before their consumers can be qualified. Missing assets must remain explicit failures; do not remove classifier quality checks or silently downgrade BGE assessments.

## November 10 video recovery and nested mount failure

The owner reported missing ordinary Cherryflash and CrumpleVideo publications after the credential migration. Authentication and social metrics were insufficient video acceptance. Only the new account was contacted. Private `richss777/video-announce-assets` was restored once from the deployed production fonts, music and helper code: 37 files, 18.1 MB. Existing normal input/push pipelines provisioned previously absent targets with real event selections.

- Cherryflash session 1599 / handoff operation 11861: `richss777/cherryflash` version 1, dataset `richss777/cherryflash-session-1599-1794343824`. At 21:24 UTC it was still publishing, without terminal/public acceptance. Do not rerun it.
- CrumpleVideo session 1601 / handoff operation 11863: `richss777/crumple-video-video1` version 1, dataset `richss777/video-afisha-session-1601`. Failed at 21:11:51 UTC before creating an MP4. Its actual notebook log found payload and helper under `/kaggle/input/datasets/richss777/video-afisha-session-1601`, then inspected only `/kaggle/input/datasets` for assets and raised `Required CrumpleVideo intro font not found: Benzin-Bold.ttf`. Required fonts were present in the session bundle.

The existing notebook asset loader enumerated only immediate input directories. Change that enumeration to recursive directory discovery, retaining its existing copying, font requirements and failure behavior. The actual loader test fails on the retained nested layout before the fix; legacy layout and absent-font rejection already pass. After the fix all three controls pass, including static helper/music copying. These small path fixtures do not qualify font rendering or public publication. No new retry, state, worker, semantic rule or missing-asset bypass is added. Net production change: one replaced line (net zero), budget two lines.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261110T204306Z-video-migration-completion-20261110`, especially `nested-assets-before.log` and `progress-2130.json`; exact terminal provider log is retained privately on RunCoveer under `/data/incident_recovery/video-migration-20261110/crumple1601-terminal/`. Candidate code is not a delivered release or a successful compensating render. Remaining configured video lanes and ordinary scheduled recurrence still need verification.

## November 12 guide monitoring target provisioning

The migrated account had no `richss777/guide-excursions-monitor` notebook.
Normal guide full/light runs, including 11958 and 11968, failed at the exact
pre-push GetKernel revision lookup with HTTP 403 and empty source reports.
This was a missing operational target, not a successful scan without findings.
Authenticated new-account inventory contained nine notebooks and omitted this
target; an exact new-account GetKernel returned `kernels.get` denied. No retired
account or resource was queried.

One operator provisioning run used the deployed notebook and a real normal full
input: ops 11969, run `45cd5f944173`, exact runtime
`2c18976b47515dd1bb03121f4dbfd3ef2993e3d7`. Existing config encryption, provider
limiter, prepared notebook, output identity matching and service import were
retained. No invented positive revision or zero baseline was supplied to the
normal launch-intent contract. An existing UNKNOWN recovery job/session barrier
was registered before one SaveKernel, with an exclusive durable operator receipt;
unknown submission must be reconciled by this exact run and target before any
repeat. No product code or scheduler was changed and no parallel S22 session
was started. Offline provider audit passed with unapproved=0 and
allowlisted_debt=0.

Kaggle accepted version 1 with no invalid dataset sources. All 30 sources emitted
source_done, and the matching complete report contains 81 posts and 11 total
occurrences. Remote completion alone does not qualify host materialization.
The ordinary host import and occurrence enrichment completed successfully at
10:26:50 UTC: 30 source reports, 81 posts, 34 successful LLM analyses, no errors
or deferrals, five created records and five updates; one past occurrence skipped.
Created records are 731, 732, 733, 734 and 735. Record 733 has no date and remains
unknown; do not describe it as a scheduled future excursion. The accepted future
new records are 731 (November 15), 732 (November 15), 734 (November 13), and 735
(November 15). These belong to preexisting sources, not the newly onboarded eight.
No compensating publication was created. Prior running SHA 486abac3 remains an
ancestor of the actual runtime. No runtime cutover was needed for provisioning.

All eight newly onboarded guide sources have source_status=ok and no remote
errors. In the five-day full window, Garry and Boyko each had two posts without
qualifying invitations, 2meterguide had nine posts and seven successful LLM
analyses without an occurrence, and the other five had no posts. None of the
11 remote occurrences belongs to those eight sources. Independent database
readback confirms all eight last_scan_at values updated on November 12, all
source reports accepted, and zero future occurrences through both primary source
and guide_occurrence_source links for sources 23–30.

Retained evidence is under
`/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104`:
`guide-bootstrap-20261112.log`, `guide-account-current-20261112.json`, and
`guide-materialization-readback-20261112.json`. The scoped operator receipt is
`/data/incident_recovery/guide-account-bootstrap-20261112/bootstrap-receipt.json`.
An already accepted guide source or event must not be replayed to create a
nonzero count. The exact new-account revision lookup now returns 1; the normal recovery registry
is empty after successful import. Verify the next ordinary scheduled run through
the existing positive-revision path, without this operator provisioning adapter.

### Ordinary scheduled recurrence accepted

On November 12 the 13:20 Kaliningrad light entry 11978 skipped while Telegram
monitoring held the heavy operation gate. The ordinary automatic full entry
11979 (trigger=scheduled, run `e6e6df92290e`) started at 13:20:02 and imported
successfully at 14:09:24. Its original launcher obtained the positive existing
revision and submitted version 2 once, with no invalid dataset sources or
operator provisioning adapter. The exact callback run has 30 source_done events;
all 30 source reports were materialized, 81 posts processed, 34 LLM analyses
successful, ten occurrence updates and zero errors or new occurrences. All eight
onboarded sources again returned ok and their last_scan_at values advanced;
primary and post-source joins confirm no future occurrences from those eight.
This completes the ordinary guide recurrence check. Evidence:
`guide-recurrence-readback-20261112.json` in the retained source-onboarding
directory. Actual runtime `9a782e4d01368cc117f3125e610bf41ebba1b401` contains prior
`2c18976b47515dd1bb03121f4dbfd3ef2993e3d7`; health is ready with no issues.
No deployment, repeated scan, source replay, or extra publication was performed
for this verification. Other account migration data blockers remain separate.
