# F18 service-share footer lane map

```yaml
mode: worktree_worker_then_serial_integrator
repo: onedayonemasterpiece/events-bot-new
base_ref: origin/main@926dad8a91fc7f1070126d32a05281aa92ff1666
visual_reference: preview-20260714t-desktop-focus-v11
base_branch: origin/main
integration_branch: integration/f18-service-share-footer-v11-20260715
global_constraints:
  - preview/test only; no production pointer switch
  - footer only; header/mobile menu Deferred until V12
  - one canonical URL and one payload/controller
  - D0 desktop default; D1/D2 research only
  - no OpenAI image generation
  - no stable ICS/object mutation during preview deploy
verification_owner: root integrator
stop_conditions:
  - production write/deploy path not proven preview-only
  - Kaggle auth/status contract cannot be safely isolated
  - asset claims do not match source snapshot
lanes:
  - id: ui
    role: worker
    requirement_ids: [R01, R02, R03, R04, R07]
    target: shared footer component/controller/lab/Playwright
    depends_on: [product contract]
    execution_mode: parallel
    branch: agent/f18-service-share/ui
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-ui
    writable_files: [site/src/components/ServiceShareAction.astro, site/src/lib/service-share/**, site/src/pages/lab/service-share/**, site/src/layouts/EventLayout.astro, site/scripts/check-preview.mjs, site/src/env.d.ts, tests/playwright/**, site/package.json, site/package-lock.json]
    forbidden_files: [scripts/research/**, kaggle/**, scheduling.py, docs/**, CHANGELOG.md]
    expected_output: committed UI vertical slice and focused tests
    verification_scope: targeted
    status: planned
  - id: renderer
    role: worker
    requirement_ids: [R05, R06]
    target: snapshot metrics/event selection, daily card assets, Kaggle/status/schedule contract
    depends_on: [product manifest contract]
    execution_mode: parallel
    branch: agent/f18-service-share/renderer
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-renderer
    writable_files: [scripts/service_share/**, scripts/research/service_share_poster_cubes/**, scripts/research/prepare_service_share_faces.py, scripts/research/select_service_share_events.py, scripts/run_service_share_still_kaggle.py, kaggle/ServiceShareStill/**, tests/test_service_share**, scheduling.py, main.py, .env.example]
    forbidden_files: [site/src/**, site/scripts/**, docs/**, CHANGELOG.md]
    expected_output: committed production-ready-but-disabled daily pipeline and tests
    verification_scope: targeted
    status: planned
  - id: docs
    role: worker
    requirement_ids: [R08]
    target: canonical F18 docs/routes/e2e/manual matrix/runbook/changelog
    depends_on: [ui, renderer]
    execution_mode: serial_after_dependency
    branch: agent/f18-service-share/docs
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-docs
    writable_files: [docs/**, CHANGELOG.md, .codex/lanes/docs/RESULTS.md]
    forbidden_files: [site/**, scripts/**, kaggle/**, tests/**, scheduling.py, main.py]
    expected_output: committed synchronized documentation
    verification_scope: inspection_only
    status: planned
  - id: integration
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08]
    target: merge, asset generation, Kaggle GPU/CPU run, preview-only publish, public HTTPS acceptance, PR
    depends_on: [ui, renderer, docs]
    execution_mode: serial_after_dependency
    branch: integration/f18-service-share-footer-v11-20260715
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-service-share-footer
    writable_files: [integration conflict resolutions, artifacts/codex/f18-service-share/**]
    forbidden_files: [production/current pointer]
    expected_output: test implementation ready or evidence-backed blocker
    verification_scope: full_local_and_public_preview
    status: planned
```
