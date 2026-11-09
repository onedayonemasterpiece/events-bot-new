# INC-2026-11-09-kaggle-account-migration

Status: open
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

Pending.

## Follow-ups

Prepared private datasets must exist under the new account before their consumers can be qualified. Missing assets must remain explicit failures; do not remove classifier quality checks or silently downgrade BGE assessments.
