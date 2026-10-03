# RunCoveer deployment

Host: `78.111.90.203`, Serverspace `l72s1332671`, Ubuntu 26.04,
1 CPU / 3 GB RAM / 25 GB boot volume. SSH alias `runcoveer-production` uses
`deploy` and DevCoveer-only `~/.ssh/prod_78_111_91_149_ed25519`.

Scope: Events Bot and Kotopogoda application runtimes. Yandex Object Storage,
CDN, Supabase, Kaggle and other managed dependencies stay external.

## Build and restore

Events source baseline is `8105d069f2d91306c13db02b976e5fa2921226f6`.
Copy its archive to `/opt/runcoveer/events/source`, supply the preserved
`ai_resource_control-0.1.4-py3-none-any.whl` in `vendor-private`, then build:
`sudo docker build --build-arg STATIC_SITE_IMAGE_REPO_SHA=8105d069f2d91306c13db02b976e5fa2921226f6 -t runcoveer/events:8105d069f .`.
Kotopogoda rebuilds its preserved deployed snapshot `951065bf` with
`Dockerfile.runtime-snapshot` and `requirements.runtime.lock`; current repository
HEAD is not substituted for deployed source. Build image
`runcoveer/kotopogoda:runtime-951065bf`.

Authoritative recovery sources on DevCoveer:
- `/home/dev/artifacts/events-bot-dr-20261001-192142`: code provenance, wheel,
  environment and extra preserved SQLite databases.
- `/home/dev/artifacts/kotopogoda-dr-20261001-1935`: exact runtime snapshot,
  environment and asset cache.
- `/home/dev/secure-backups/flyio/2026-10-02`: newer consistent SQLite snapshots
  and non-SQLite state.

Restore only while containers are stopped, into `/var/lib/runcoveer/{events,kotopogoda}`.
Extract Events non-SQLite archive into `events/data`, then overwrite corresponding
SQLite files with consistent snapshots (strip `.backup` suffix); use the older
consistent snapshot only for DB files absent from the newer collection.
Extract Kotopogoda volume to `kotopogoda/data`, replace `bot.db` with latest
consistent snapshot and remove restored stale `bot.db-wal`/`bot.db-shm`.
Restore asset cache to `data/bot_assets`, app data to `kotopogoda/app-data`.
Never restore a live copied WAL over a consistent database.
Check `pragma quick_check`, key table counts and checksums before startup.
Keep all secret env files at mode 0600 and state directories private.

## Preflight and cutover

Install Compose files under `/opt/runcoveer/deployment`. `.env` contains
`DEPLOY_PREFLIGHT=1` before verification. The adapters boot actual HTTP handlers
without webhook registration, schedules, workers or background publication.
Health probes: `http://127.0.0.1:8081/healthz` and
`http://127.0.0.1:8082/v1/health`. Check actual production integration results,
not only container state. Events full readiness can require its production
heartbeat and therefore must be verified again after controlled cutover.

Temporary HTTPS origins: `events.78.111.90.203.sslip.io` and
`weather.78.111.90.203.sslip.io`. These depend on public sslip.io DNS; replace
with owner-controlled domains when available, updating callback consumers as well.

Caddy terminates TLS; application ports bind localhost only. Validate public TLS
before switching webhooks. Cordon old machines before stopping them (`flyctl machine cordon`): proxy
traffic must not reactivate old writers. Events autostart is also explicitly
disabled; its old command uses `sleep infinity` for final capture. Kotopogoda
cannot be updated because both its GHCR tag and manifest are unavailable;
cordon removes its routes without revalidating the image. Preserve that
machine/volume. Rollback uncordons it only after the new writer is stopped.
For Events rollback also restore `python main.py` before activating the old writer.
Capture/checkpoint final changed SQLite files and only changed non-SQLite state,
then confirm old machines remain stopped even after an old-origin request.
After data and product preflight, set `DEPLOY_PREFLIGHT=0` and recreate containers, then
verify Telegram `getWebhookInfo`, real Telegram `/start` and representative
read-only product routes. Check external callers using old Fly URLs separately.

## Repeat deploy, persistence and backup

`deploy.sh` copies configuration and runs Compose; it does not restore or replace
data. Keep images fixed to the verified release; build/tag a new image explicitly
for an application upgrade. Docker and Caddy start with systemd; containers use
`unless-stopped`. Logs rotate at 10 MB x 3 per container; existing Events runtime
file rotation remains enabled. Firewall allows 22/80/443 only.

Install `backup.service`/`backup.timer` to `/etc/systemd/system/runcoveer-backup.*`
and enable the timer. `sudo python3 /opt/runcoveer/deployment/backup.py` makes
consistent SQLite backups and copies non-SQLite persistent state with mode 0700.
Keep the last three complete local backups; pruning applies only to snapshots
with this script’s receipt and timestamp format, after the next backup succeeds.
Copy backups off-host to a retained DevCoveer artifact directory and verify
checksums. Local backup alone does not protect against host loss. Secrets have
separate preserved recovery copies and are not printed or committed.

Verify individual restart, Docker restart, repeat deploy and server reboot,
then repeat health checks and key row counts. Restore procedure: stop containers,
retain current state, restore the selected backup into persistent directories,
verify SQLite, start and recheck product behavior.

## Rollback

Stop new containers first and preserve their latest writes via `backup.py`.
For rollback to Fly, propagate new data into the stopped old volume before
starting its single writer; restore previous webhook/endpoints. Never run old
and new writers concurrently or silently restore an older database over new
writes. The earlier 50 GB server has no running production application and is
not itself a traffic rollback target.

## Verified cutover, 2026-10-03

Both workloads run in production mode on this host with new Telegram webhooks.
Real Telegram smoke, public TLS/health, container restart, repeat deploy, Docker
restart and host reboot passed. Final changed snapshots supersede Oct 2 state:
retained evidence is `/home/dev/artifacts/runcoveer/20261003T081708Z-production-migration`.
Post-reboot counts: Events 9,123, Kotopogoda 154 assets; SQLite integrity is ok.
Daily backup timer is enabled and a post-cutover backup completed. Legacy Fly
machines remain cordoned/stopped. Temporary public domains and administrative
publishing checks remain explicit operational limitations.
