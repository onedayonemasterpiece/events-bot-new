# R13 / r02-source-binding results

## Lane contract

- Lane ID: `R13` / `r02-source-binding`
- Requirement IDs: Stage-1 source binding correction (`R13`)
- Status: Done
- Branch: `agent/search-production-health-stage1/source-binding`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/search-stage1-source-binding`
- Base SHA: `9642c7d56a34f647a6f725eba0d5d3b4b8256554`
- Implementation head SHA: `8cd7746abd2deeace1475349a543d821e270ff77`
- Push/deploy/live execution: not performed

## Outcome

- Docker now writes a build-time source manifest beside `.static-site-repo-sha`. The receipt binds the full Fly image revision to a deterministic digest of every static-site/Kaggle source input copied into the image.
- The Fly handoff includes the baked manifest digest/tree digest in the static input fingerprint and durable remote handoff.
- The runner recomputes and verifies the current image source bytes before creating a private dataset, then writes a per-run source manifest binding the image receipt, exact staged payload tree, and exact `site_source.tarball` bytes.
- Kaggle verifies the mounted source-manifest digest, archive digest, repo SHA, and extracted payload-tree digest before export/install/build.
- The downloaded result and generated secret-candidate manifest carry the same complete source identity. Fly validates the result against the current image receipt and validates the candidate manifest against the result before publication.
- Recovery refuses an old remote handoff when its repo SHA or baked source identity differs from the currently deployed image; it cannot publish a candidate from another deploy.
- Mutable `STATIC_SITE_REPO_SHA` fallback is rejected in Fly. It remains available only outside Fly for explicit local/dev operation.

## Deterministic negative coverage

1. **Source claim mismatch:** mutating one source file after writing the image receipt is rejected before dataset staging.
2. **Missing production marker/fallback:** Fly cannot use the legacy environment-only repo SHA when the baked revision file is absent.
3. **Cross-deploy recovery:** an old handoff repo SHA is rejected before snapshot validation or subprocess/adoption.
4. **Result/candidate mismatch:** publication rejects a candidate manifest whose payload source identity differs from the checked result.
5. Additional boundary: Kaggle rejects a mounted archive whose bytes do not match the source manifest.

## Validation evidence

Commands run from the lane worktree:

```text
python3 -m py_compile main.py static_site_release.py scripts/run_static_site_builder_kaggle.py kaggle/StaticSiteBuilder/static_site_builder.py
# PASS

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_build_debounce.py
# 63 passed in 7.02s

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_site_release.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_build_debounce.py
# 87 passed in 9.10s

git diff --check
# PASS
```

A local, side-effect-free manifest probe hashed 885 source files and successfully revalidated the written manifest against current bytes. No Docker build, Kaggle run, static build, deploy, production database, Search request, or publication was executed.

## Risks / integration gates

- The canonical deploy script remains the trust anchor proving the Docker build argument is exact clean `origin/main`; `.git` is deliberately absent from the image. This lane makes that claimed SHA inseparable from the source bytes subsequently sent to Kaggle, but does not add another Git service or build.
- A deploy during an unfinished older Kaggle run now fails closed instead of adopting cross-deploy output. Operations/integration must let the old owner be reconciled or explicitly superseded; publication of its output is intentionally forbidden.
- Docker image construction was prohibited by lane scope. CI/integration must exercise the Dockerfile receipt-writing step once before release.
- Local/dev execution without Fly markers creates and checks an in-process source receipt from the current checkout; production always requires the baked manifest.
- Canonical docs and `CHANGELOG.md` were intentionally not edited because the integrator owns them.

## Changed files

- `Dockerfile`
- `main.py`
- `static_site_release.py`
- `scripts/run_static_site_builder_kaggle.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `tests/test_static_site_build_handoff.py`
- `tests/test_static_site_build_debounce.py`
- `.codex/lanes/search-production-health-stage2/r02-source-binding/RESULTS.md`
