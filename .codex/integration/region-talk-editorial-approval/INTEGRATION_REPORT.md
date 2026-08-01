# Region Talk editorial approval — integration report

Integration branch: `integration/region-talk-editorial-approval`  
Rebased base: `origin/main@d08ecf320d231e6b934715a5930b3d6d92c771ac`

| Lane | Requirements | Status | Integrated evidence |
|---|---|---|---|
| prompt | R04 | merged | external Gemini review prompt and results ledger |
| cafe-tobe | R07 | merged | separate café-review product hypothesis in backlog |
| reaction-code | R03 | merged | exact reviewer/revision reaction ledger and planner gate |
| visual-code | R05, R06 | merged | associated source hero/gallery/video evidence and deterministic materialization refs |
| runtime-code | R01, R02 | merged | five safe slots, durable cursors, revisit reserve, autonomous sync |
| writer-regeneration | R04, R08 | merged | Strategy → Writer → validators → Critic v8 and media-first operator renderer |
| browser-code | R05, R06 | merged | bounded Chromium materializer, SSRF guard, finite retry and orchestrator hook |

## Integration gates

- Gemini Pro consultation is preserved at
  `docs/features/region-talk-channel/onboarding-prompt-consultation.md` and its
  accepted staged pattern is implemented by writer v8.
- Region Talk functional Telegram access is restricted to DISCOVERY1/DISCOVERY2;
  neither generic `TELEGRAM_SESSION` nor E2E is part of the pipeline.
- Public target-channel publishing stays disabled. Operator delivery is the
  controlled approval surface.
- Browser-wait article rows are a distinct local queue and become
  ImageDiagnostic work only after direct source-media materialization.
- Documentation and `[Unreleased]` changelog entries accompany every behavior
  change.

## Verification

- Focused browser/orchestrator suite: **107 passed**.
- Full Region Talk suite before final rebase: **678 passed**.
- Full Region Talk suite after the final `origin/main` rebase: **678 passed in 45.91s**.
