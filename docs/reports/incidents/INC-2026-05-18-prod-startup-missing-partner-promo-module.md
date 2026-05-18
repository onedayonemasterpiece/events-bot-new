# INC-2026-05-18 Prod Startup Missing Partner Promo Module

Status: open
Severity: sev0
Service: Fly production bot startup / webhook serving
Opened: 2026-05-18
Closed: —
Owners: events-bot runtime / release owner
Related incidents: `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke`
Related docs: `docs/operations/incident-management.md`, `docs/operations/release-governance.md`, `docs/features/digests/README.md`

## Summary

The 2026-05-18 deploy of `c7dc458b` restarted the Fly machine, but production failed during `create_app()` with `ModuleNotFoundError: No module named 'partner_promo'`. The serving bot repeatedly restarted and `/healthz` stayed critical.

## User / Business Impact

- Production webhook serving was unavailable while the machine restarted.
- The operator could not safely run the planned manual `/daily` rerun until startup was restored.
- The requested `/daily` VK fix and Kenigsberg VK Stories rollout were built into the image but not usable until the startup crash was contained.

## Detection

- Fly deploy health checks did not turn green after rollout.
- `flyctl status` showed machine `48e42d5b714228` at version `1118` with `1 total, 1 critical`.
- `flyctl logs --no-tail` showed the crash at `main_part2.py:create_app`, importing `partner_promo_input_sessions`.

## Timeline

- 2026-05-18 06:51 UTC: deploy image `deployment-01KRWXFD9PE9ZPX0DHEDKY63WA` started rolling out.
- 2026-05-18 06:51-06:52 UTC: production machine repeatedly exited with `ModuleNotFoundError: partner_promo`.
- 2026-05-18 06:52 UTC: incident workflow started from failed health checks and Fly logs.

## Root Cause

1. `origin/main` contained partner-promo callback/input registration in `main_part2.py`.
2. The supporting `partner_promo.py` module was still only present as local uncommitted work in another checkout.
3. The startup path imported `partner_promo` unconditionally, so a non-critical unfinished feature could prevent the whole bot from booting.

## Contributing Factors

- The deploy bundled multiple recent production changes from `origin/main`; a partial feature integration was already present before the `/daily` hotfix.
- There was no startup regression test that calls `create_app()` with optional partner-promo modules absent.

## Automation Contract

### Treat as regression guard when

- changing `main_part2.py::create_app` handler registration;
- adding optional feature handlers or callback prefixes;
- deploying from `origin/main` after partial feature work touched startup imports.

### Affected surfaces

- `main_part2.py::create_app`
- `main_part2.py` partner-promo callback/reply wrappers
- Fly production startup and `/healthz`
- release/deploy workflow from `origin/main`

### Mandatory checks before closure or deploy

- `python -m py_compile main.py main_part2.py`
- A startup smoke that calls `create_app()` with the current production env shape and no local-only modules.
- Targeted tests already relevant to the intended release (`tests/test_vk_daily.py`, `tests/test_daily_format.py`, `tests/test_kenigsberg_stories.py`).
- Production `/healthz` after deploy.
- Confirm deployed SHA is reachable from `origin/main`.

### Required evidence

- deployed SHA reachable from `origin/main`;
- Fly image/deployment id;
- `flyctl status` showing healthy checks;
- `/healthz` response;
- regression test output.

## Immediate Mitigation

- Make partner-promo startup/callback imports fail closed: the bot starts without the optional module, logs the missing feature, and partner-promo callbacks/replies answer that promo campaigns are temporarily unavailable instead of crashing startup.

## Corrective Actions

- Add a startup guard around `partner_promo_input_sessions` import in `create_app()`.
- Add guarded lazy imports in partner-promo callback/reply wrappers.
- Add `.venv/` to `.dockerignore` so local test environments cannot inflate emergency deploy contexts.

## Follow-up Actions

- [ ] Finish or revert the partner-promo feature branch so `ppromo:*` buttons are not half-wired in production.
- [ ] Add a focused startup smoke test for optional handler imports.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Optional feature modules must not be imported unconditionally from the production startup path unless their files are already committed and covered by startup smoke.
