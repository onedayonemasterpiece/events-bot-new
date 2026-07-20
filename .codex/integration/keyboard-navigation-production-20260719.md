# Keyboard navigation production integration — 2026-07-19

## Execution matrix

| ID | Requirement | Area | Dependencies | Lane | Done when |
|---|---|---|---|---|---|
| R01 | Transfer reviewed V7 behavior exactly from `db5310e8` into production, not generated HTML | keyboard router | reviewed prototype merged at `bd972507` | R01-router | production module reuses V7 command logic and semantic DOM contracts |
| R02 | `Смотрите дальше` and the finite broader desktop continuation reuse canonical large `EventCard` DOM and behavior | cards/personal feed | current main parity baseline | R02-cards | common card controller semantics, stable feedback, desktop-only hydration, tests |
| R03 | Explicit `init()`/`destroy()`, listener/observer cleanup, desktop-only feature flag, no prototype autofocus | lifecycle | R01 | R01-router | reversible lifecycle and secret-candidate 100% enablement without root promotion |
| R04 | Port V7 regression and cover production families/IME/layout/latch/focus/CTA/card paths | browser tests | R01,R02 | R01-router + integrator | targeted Node and Chromium/Firefox/WebKit checks |
| R05 | Main-reachable source and Smart Update production handoff | release | R01–R04 | integrator | PR/CI merge to `origin/main`, exact deployed SHA configured |
| R06 | Regenerate every event page from production data only under immutable `/_review/` | secret publication | R05 | integrator/ops | checked candidate receipt advances; root/current/stable ICS unchanged |
| R07 | Report actual static build diagnostics for last 24h | observability | R06 | integrator/ops | redacted DB/log/object report with outcomes/counts/times |
| R08 | Canonical docs, scenario index, incident regression evidence and CHANGELOG | docs | R01–R07 | integrator | no duplicate docs; release limitations explicit |
| R09 | Never promote production root; candidate remains noindex/no-referrer | release safety | R06 | integrator/ops | secret checks and negative root mutation evidence pass |
| R10 | Recover recurring Fly root-overlay exhaustion; bound static outputs and detect root/temp failure before Smart Update retries | incident/static/health | production evidence | R10-static-disk | root writable, static output retention + health/tempfile regression guarded |
| R11 | Bound terminal videoannounce frame trees without deleting active/recoverable output | incident/video | R10 evidence | R11-video-cleanup | published/terminal cleanup and recovery preservation tests |

## Lane map

```yaml
mode: worktree_worker_then_serial_integrator
repo: /home/dev/projects/events-bot-new
base_ref: bd972507
base_branch: integration/keyboard-navigation-production-20260719
integration_branch: integration/keyboard-navigation-production-20260719
global_constraints:
  - exact V7 behavior source is d0027a53/db5310e8
  - no generated HTML copied into production
  - no production-root promotion
  - all publication remains immutable noindex /_review
verification_owner: /root
stop_conditions:
  - dirty/unreproducible deploy source
  - root mutation or missing noindex
  - source SHA mismatch
lanes:
  - id: R01-router
    role: worker
    requirement_ids: [R01, R03, R04]
    target: production keyboard module, mount, regression harness
    depends_on: [reviewed prototype merge bd972507]
    execution_mode: parallel
    branch: agent/keyboard-navigation-production/R01-router
    worktree: /home/dev/.codex/worktrees/events-bot-new/keyboard-nav-prod-r01
    writable_files:
      - site/src/components/KeyboardEventNavigationPrototype.astro
      - site/src/components/KeyboardEventNavigation.astro
      - site/src/lib/keyboardEventNavigation.mjs
      - site/src/pages/sobytiya/[slug].astro
      - site/scripts/check-keyboard-event-navigation-playwright.sh
      - site/tests/keyboard-event-navigation-production.test.mjs
      - .codex/lanes/R01-router/RESULTS.md
    forbidden_files: [site/src/layouts/EventLayout.astro, site/src/components/PersonalFeedSlot.astro, docs, CHANGELOG.md]
    expected_output: committed exact-source production extraction plus targeted tests
    verification_scope: targeted
    status: implemented_and_review_hardened
  - id: R02-cards
    role: worker
    requirement_ids: [R02]
    target: canonical card-controller behavior parity and desktop-only broader hydration
    depends_on: []
    execution_mode: parallel
    branch: agent/keyboard-navigation-production/R02-cards
    worktree: /home/dev/.codex/worktrees/events-bot-new/keyboard-nav-prod-r02
    writable_files:
      - site/src/layouts/EventLayout.astro
      - site/src/components/PersonalFeedSlot.astro
      - site/tests/personal-feed-surface.test.mjs
      - site/tests/event-continuation-contract.test.mjs
      - .codex/lanes/R02-cards/RESULTS.md
    forbidden_files: [site/src/components/KeyboardEventNavigationPrototype.astro, site/src/pages/sobytiya/[slug].astro, docs, CHANGELOG.md]
    expected_output: committed controller parity fix and targeted tests
    verification_scope: targeted
    status: implemented_and_review_hardened
  - id: R10-static-disk
    role: worker
    requirement_ids: [R10]
    target: static output retention and root scratch health/preflight
    depends_on: []
    execution_mode: parallel
    branch: agent/keyboard-navigation-production/R10-static-disk
    worktree: /home/dev/.codex/worktrees/events-bot-new/keyboard-nav-prod-r10-static
    writable_files: [main.py, main_part2.py, runtime_disk.py, scripts/run_static_site_builder_kaggle.py, static_site_release.py, static_site_diagnostics.py, tests, .env.example, fly.toml, .codex/lanes/R10-static-disk/RESULTS.md]
    forbidden_files: [site/src, docs, CHANGELOG.md, video_announce/poller.py]
    expected_output: bounded recognized-only artifact cleanup plus dual-disk/tempfile readiness
    verification_scope: targeted
    status: implemented_and_review_hardened
  - id: R11-video-cleanup
    role: worker
    requirement_ids: [R11]
    target: terminal video output cleanup/preservation
    depends_on: []
    execution_mode: parallel
    branch: agent/keyboard-navigation-production/R11-video-cleanup
    worktree: /home/dev/.codex/worktrees/events-bot-new/keyboard-nav-prod-r11-video
    writable_files: [video_announce/poller.py, tests/test_video_announce_poller.py, tests/test_video_announce_v_pipeline.py, .codex/lanes/R11-video-cleanup/RESULTS.md]
    forbidden_files: [site/src, docs, CHANGELOG.md, main.py, static_site_release.py]
    expected_output: assertion-safe terminal cleanup with recovery preservation
    verification_scope: targeted
    status: implemented_and_review_hardened
  - id: integration-ops
    role: merge_reviewer
    requirement_ids: [R05, R06, R07, R08, R09]
    target: reconcile, docs, CI, deploy, Smart Update secret build, diagnostics
    depends_on: [R01-router, R02-cards]
    execution_mode: serial_after_dependency
    branch: integration/keyboard-navigation-production-20260719
    worktree: /home/dev/.codex/worktrees/events-bot-new/keyboard-navigation-production-20260719
    writable_files: [docs, CHANGELOG.md, .codex/integration, release operations]
    forbidden_files: []
    expected_output: main-reachable checked secret candidate and redacted 24h report
    verification_scope: full_local_and_live_secret
    status: in_progress
```
