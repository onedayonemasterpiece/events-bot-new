# iOS OTP harness correction — integration report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| ios_harness_map | R01–R05 support | read-only | completed | n/a | n/a | screenshot/workflow/adapter map |
| control_evidence_map | R06–R08 support | read-only | completed | n/a | n/a | control/evidence/diagnostics map |
| docs_skill_map | R09–R10 support | read-only | completed | n/a | n/a | canonical correction inventory |
| serial_integrator | R01–R10 primary | integration/ios-otp-harness-correction-20260802 | committed / PR #264 | `2e1597f8` | pending merge | 48 OTP + 82 focus + 46 resilient tests pass; CI 2/2 PASS |
| ios_otp_checklist_review | R01–R10 review | read-only | GO | n/a | n/a | all reported P0/P1 blockers resolved |

R10 deliberately remains open until three protected side-effect-free iOS preflights and one full protected real-mail iOS OTP run finish through issue #253.
