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
