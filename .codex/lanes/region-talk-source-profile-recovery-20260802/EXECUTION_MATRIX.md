# Region Talk source-profile recovery execution matrix

Base: `origin/main@ba8ab078ba9894ccd5810045b1b8787ecb29d743`
Integration: `integration/region-talk-source-profile-recovery-20260802`

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R01 | P0 regression tests first | QA/contracts | merged #221 | medium | INTEGRATOR-QA | all 12 regressions fail before/fix after and are represented in focused suites |
| R02 | P1 bounded social capture | Telegram/VK acquisition | role-scoped readers | high | SOCIAL | description+pinned+30–80 capture, classification/digest/fingerprint, durable projection |
| R03 | P2 readiness/order | finalizer/profile lifecycle | R02,R04 | high | PROFILE-RUNTIME | reusable profile precedes Writer, explicit needs_source_profile, separate budget |
| R04 | P3 publisher importer/Action | schema/YDB/OIDC | merged sidecars | high | PUBLISHER | dry-run/execute, strong atomic idempotence/conflict, correction queue, guarded Action |
| R05 | P4 future publisher evidence merge | external importer | R04 | high | PUBLISHER | same publisher profile enriched monotonically on new/replay intake without candidate reopen |
| R06 | P5 Writer vNext | prompts/renderer | stable profile projection | high | WRITER | hook-first/source-second, paragraph-2 contract, deterministic CTA, validators/versioning |
| R07 | P6 supplied profiles and RG correction | importer/review | R04 | high | PUBLISHER | 3 packages accepted; RG queued fail-closed for live re-adjudication |
| R08 | P7 live backfill/operator delivery | production ops | R02–R07 merged/deployed | critical | LIVE-INTEGRATOR | guarded import/readback, profiles, RG block, unpublished regen, fresh review messages |
| R09 | P8 evidence/tests/audit | verification | all lanes | critical | INTEGRATOR-QA | full Region Talk suite, schemas/replay/conflict/zero-provider, 20-copy audit, zero-autopublish |

