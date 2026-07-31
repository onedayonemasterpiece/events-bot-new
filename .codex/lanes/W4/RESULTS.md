# W4 Results — atomic static-site production root

## Scope

- Lane: `W4`
- Requirements: `R04`, `R06`
- Base SHA: `45be3557583aad05898e1767c5526b56efbcf847`
- Implementation/head SHA under review: `d1fed03280678cc40351b266a808276a261ebd44`
- Branch: `agent/static-site-resilient-egress/W4`
- Live infrastructure/DNS/deploy: **not changed**

## Delivered

- Added `static_site_atomic_root.py`, a default-off two-root-bucket/Yandex ALB
  publication state machine.
- The checked `production_root` archive is safely extracted on configured
  persistent scratch, then bound to build/run/repo/snapshot identity.
- Every manifest file is verified for local SHA-256, size and derived MIME;
  required root browser, route, canonical/indexing and PWA gates are mandatory.
- Inventory requires exactly two configured ALB bucket backends and a separate
  shared media/ICS/review bucket. Every positive-weight root is proven complete.
- Only the inactive page-only bucket can be reconciled or stale-cleaned. The
  active bucket and separate shared asset/ICS bucket are not writable through
  the publisher.
- The target tree is uploaded/reused, byte-read back and verified before ALB
  convergence. The ALB adapter enables the new complete backend before it
  disables the old complete backend, so bounded convergence serves old/new
  complete trees only.
- Stable exact-hash/MIME smoke covers `/`, `/segodnya/`, `/poisk/` and the PWA
  manifest. Smoke failure switches weights back and records `rolled_back`.
- Durable atomic receipt records operation ID(s), control-plane revision,
  current/previous identities and inactive cleanup counts. Retry adopts
  already-staged or already-active exact releases without rewriting active.
- `_finish_static_site_candidate` invokes root publication only after checked
  candidate/root result validation and successful immutable review publication,
  and only with explicit `ENABLE_STATIC_SITE_ROOT_PROMOTION`. Disabled is a
  no-op; `plan` performs no bucket write/delete or ALB update.
- Added the exact setup/plan/apply/rollback/SWS runbook, default-off env contract,
  release/test documentation and changelog entry.
- Preserved incident regression contracts
  `INC-2026-07-19-static-site-stale-builder-lease` and
  `INC-2026-07-08-prod-root-overlay-disk-full`.

## Evidence and commands

```text
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_static_site_atomic_root.py \
  tests/test_static_site_release.py \
  tests/test_static_site_build_handoff.py

57 passed in 3.02s
```

Additional validation:

```text
python3 -m py_compile static_site_atomic_root.py static_site_release.py main.py
git diff --check
```

Both completed successfully.

Focused acceptance coverage includes:

- active bucket never written;
- pre-switch failure preserves active;
- weight convergence exposes complete old/new trees only;
- idempotent retry;
- stale cleanup inactive only;
- stable-smoke rollback;
- current/previous retained and never GC targets;
- plan/disabled non-mutating behavior;
- real Yandex CLI adapter command order and operation IDs.

## External contract research

Implementation and runbook were checked against current official Yandex Cloud
contracts for ALB Object Storage backends/weights, HTTP backend updates, and
Smart Web Security/ARL/WAF virtual-host attachment:

- <https://yandex.cloud/en/docs/application-load-balancer/concepts/backend-group>
- <https://yandex.cloud/en/docs/application-load-balancer/operations/backend-group-update>
- <https://yandex.cloud/en/docs/smartwebsecurity/operations/host-connect>
- <https://yandex.cloud/en/docs/smartwebsecurity/>

## Risks / blockers

- The two page-only buckets, ALB backend group/router/listener, SWS/ARL/WAF and
  DNS route do not exist yet. No live apply/rollback drill has been run.
- Bucket backends require public object read for ALB. Provisioning must retain
  anonymous listing denial, strict page-only contents and SWS/rate-limit
  protection at the virtual host.
- The CLI adapter is unit-tested with the documented command contract but awaits
  a real read-only `plan` against provisioned inventory before activation.
- `ENABLE_STATIC_SITE_ROOT_PROMOTION` must remain `0`; there is no production GO
  until the runbook's pre-DNS inventory, browser/PWA, SWS and rollback gates pass.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `main.py`
- `static_site_release.py`
- `static_site_atomic_root.py`
- `tests/test_static_site_atomic_root.py`
- `docs/README.md`
- `docs/routes.yml`
- `docs/operations/static-site-atomic-root.md`
- `docs/operations/kaggle-static-site-builder.md`
- `docs/features/static-site-pages/README.md`
- `docs/features/static-site-pages/astro-preview.md`
- `docs/features/static-site-pages/festival-timeline.md`
- `docs/features/static-site-pages/presentation-release-checklist.md`
- `docs/features/static-site-pages/release-plan.md`
- `docs/features/static-site-pages/test-scenarios.md`
