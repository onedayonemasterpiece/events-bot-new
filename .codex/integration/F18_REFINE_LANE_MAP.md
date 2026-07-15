# F18 refinement lane map

```yaml
mode: worktree_worker
repo: events-bot-new
base_ref: 46751dcb4096abb354877417d18227727b91a07b
base_branch: integration/f18-service-share-footer-v11-20260715
integration_branch: integration/f18-service-share-footer-v11-20260715
global_constraints:
  - footer only; do not change the legacy header
  - preserve current daily dynamic metrics and event selection
  - no paid OpenAI image generation
verification_owner: /root
stop_conditions:
  - consultant is not Gemini Pro class and no valid fallback review exists
  - worker scope conflicts with unrelated changes
lanes:
  - id: R01-design-review
    role: reviewer
    requirement_ids: [R01]
    target: critique current footer and define restrained two-action hierarchy
    depends_on: []
    execution_mode: parallel
    branch: none
    worktree: integration worktree (read-only)
    writable_files: []
    forbidden_files: ['*']
    expected_output: consultant artifact with model evidence and concrete recommendations
    verification_scope: inspection_only
    effort: high
    status: completed_supplementary_probe
  - id: R02-ui
    role: worker
    requirement_ids: [R02]
    target: restrained footer and separate image vs text-link desktop actions
    depends_on: [R01-design-review]
    execution_mode: read_only_until_dependency
    branch: agent/f18-service-share-refine/ui
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-refine-ui
    writable_files:
      - site/src/components/ServiceShareAction.astro
      - site/src/layouts/EventLayout.astro
      - site/src/lib/service-share/controller.js
      - site/src/pages/lab/service-share/index.astro
      - tests/playwright/service-share.spec.ts
      - tests/js/service-share-controller.test.mjs
    forbidden_files:
      - scripts/research/service_share_poster_cubes/**
      - docs/**
      - CHANGELOG.md
    expected_output: committed UI/controller/tests and lane RESULTS.md
    verification_scope: targeted
    effort: high
    status: completed
  - id: R03-renderer
    role: worker
    requirement_ids: [R03]
    target: cover crop for non-OCR photo faces without changing OCR poster treatment
    depends_on: []
    execution_mode: parallel
    branch: agent/f18-service-share-refine/renderer
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-refine-renderer
    writable_files:
      - scripts/research/prepare_service_share_faces.py
      - scripts/research/service_share_poster_cubes/**
      - tests/test_service_share_renderer.py
      - tests/test_service_share_face_preparation.py
    forbidden_files:
      - site/src/**
      - docs/**
      - CHANGELOG.md
    expected_output: committed renderer/tests and lane RESULTS.md
    verification_scope: targeted
    effort: high
    status: completed
  - id: R04-integration
    role: merge_reviewer
    requirement_ids: [R04]
    target: integrate, update canonical docs/changelog, build, browser QA, render, Telegram Saved Messages
    depends_on: [R01-design-review, R02-ui, R03-renderer]
    execution_mode: serial_after_dependency
    branch: integration/f18-service-share-footer-v11-20260715
    worktree: /home/dev/.codex/worktrees/events-bot-new/f18-service-share-footer
    writable_files: ['docs/**', CHANGELOG.md, '.codex/integration/**']
    forbidden_files: []
    expected_output: validated clean pushed integration commit and preview evidence
    verification_scope: full_local
    effort: extra-high
    status: completed
```

## Integration outcome

- R01: no evidence was found that the previous dominant footer treatment had
  Gemini approval. The Antigravity response is retained only as supplementary
  probe material because its CLI display alias did not prove the canonical
  provider model ID; the allowed Opus fallback was quota-blocked.
- R02: integrated as `b5c36ad8`; desktop image and text-link intents are
  isolated and the footer is visually de-escalated.
- R03: integrated as `d0eede4b` + `9cde8a58`; explicit OCR posters remain
  protected while non-OCR/photo and conservative landscape fallbacks use cover.
- R04: verified with controller, renderer and browser suites plus an exact-bundle
  Kaggle GPU -> CPU render. Final CPU result was sent to Telegram Saved Messages
  as message `32270`.
