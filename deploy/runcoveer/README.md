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
`unless-stopped`. Docker logs rotate at 50 MB x 10 per container with compression. Persistent
project logs rotate at 16 MB per file, retain up to 7 days and have a 512 MB
cap per project with a 1 GB free-space floor. Kotopogoda reuses the existing
stdlib bounded handler mounted from Events source and preserves its JSON
formatter/redaction. Persistent logs survive container replacement; Docker
stdout logs belong to the container and do not guarantee a time window. Firewall allows 22/80/443 only.

The production host has an active 4 GB `/swapfile-runcoveer`, persisted in `/etc/fstab`.
Check `swapon --show` and `free -h` after host changes. Swap helps absorb
short memory spikes; continue monitoring available RAM and OOM events.

Events includes the private MCP HTTP/OAuth service in its existing container.
Its public base is `https://events.78.111.90.203.sslip.io`. Existing connectors
must replace the old Fly origin while preserving their private endpoint path
and authorize again: OAuth resources/tokens are bound to the public origin.
Verified over HTTPS: OAuth metadata, unauthenticated rejection, a complete
Codex OAuth/PKCE authorization, authenticated initialize (2025-06-18) and
tools/list (7 tools). No tool calls were issued. This server verification does
not update an existing client connector configuration.

Install `backup.service`/`backup.timer` to `/etc/systemd/system/runcoveer-backup.*`
and enable the timer. `sudo python3 /opt/runcoveer/deployment/backup.py` makes
consistent SQLite backups and copies non-SQLite persistent state with mode 0700.
Keep only the latest complete local backup; pruning applies only to snapshots
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

## External integration verification

A 35-byte create-only probe from the production Events container wrote/read
the existing `kenigevents.ru` Object Storage bucket with HTTP 200 and matching
bytes, then deleted its own object. Existing publication credentials and CDN
origins remain in place. Kaggle callback derives from the new webhook origin.
CDN root and an 82,732-byte static asset passed strict TLS from the new container;
other CDN workers intermittently still served the default certificate during
post-billing propagation. Track this separately as `inc_e748d11ecc8bce50126485e1`;
one successful response is not proof of global CDN recovery.

Kotopogoda external RAG PostgreSQL still returns tenant/user not found, as
already recorded in its pre-migration external-data inventory. This deployment
restores the existing configuration; it does not repair the missing provider
tenant. Basic Telegram runtime and local data are independently verified.

## Encrypted private Kaggle recovery

The daily timer at 03:00 UTC (05:00 Europe/Kaliningrad) runs `backup-kaggle.py`:
consistent SQLite snapshots and persistent application files are archived with
production env files, deployment configuration and Caddy configuration. No SSH
private key or backup decryption key is included. `age` encrypts the streaming
archive using `/etc/runcoveer/backup-age.recipient` before upload.

Only DevCoveer holds `/home/dev/.config/runcoveer/backup-age.key` (0600).
Losing this identity makes the dataset unrecoverable; protect it separately.
Production only receives the public recipient. Install `age` on production;
the Kaggle SDK is supplied by the existing Events image and uses its preserved
production Kaggle identity, without starting an application worker.

Dataset: `<KAGGLE_USERNAME>/runcoveer-production-backup`, always private.
First upload explicitly uses `public=False`; updates refuse a public dataset
and preserve version history (`delete_old_versions=False`). The fixed encrypted
filename and manifest replace the current version without creating new datasets.
Kaggle’s official private-dataset quota explanation counts the most recent
version, so history is retained while the current archive remains bounded:
https://www.kaggle.com/discussions/product-feedback/50755. Local snapshot retention is also one completed
snapshot; an incomplete next upload never causes a blind repeated mutation.
A pending intent records the exact encrypted checksum before upload. The next
run reconciles that same remote manifest before generating another snapshot;
unknown outcomes require inspection instead of creating duplicate versions.

Manual run: `sudo systemctl start runcoveer-backup.service`.
Receipt: `/var/backups/runcoveer/kaggle/receipt.json`; inspect the systemd unit
result and receipt timestamp for off-host backup freshness. An upload failure
leaves the local consistent backup and pending intent intact.

Restore: download `backup.tar.gz.age` and `manifest.json` with authenticated
Kaggle access, verify the encrypted SHA-256, decrypt with the DevCoveer age
identity, and extract into a separate private directory. Verify all SQLite
files with quick_check and key row counts before stopping applications and
restoring state/env/config. Never extract a test restore over running data.
Use streaming over SSH when DevCoveer lacks room for large archives. Preserve
current state before a real restore and recheck health, webhooks and Telegram.

## Host capacity control

Install `journald-runcoveer.conf` as `/etc/systemd/journald.conf.d/runcoveer.conf`
and restart journald: persistent system journals have a 256 MB cap, 14-day
retention and a 2 GB free-space floor. Install `logrotate-rsyslog.conf` as `/etc/logrotate.d/rsyslog` (daily,
16 MB maximum size trigger, seven compressed rotations). Install
`logrotate-timer.conf` as `/etc/systemd/system/logrotate.timer.d/runcoveer.conf`
to check logrotate rules every 15 minutes. OS tmpfiles cleanup remains enabled.
These size triggers are evaluated at checks; log bursts can exceed a trigger
between checks. Run `disk-maintenance.py` for a dry run, then install/enable
`disk-maintenance.service` and `.timer` under the `runcoveer-` prefix. Every
15 minutes they check both disk bytes and inodes and prune only Docker build
cache older than seven days. Under pressure they also clear the APT package
cache. Service data, Docker images used for rollback and backups are preserved.
Warning thresholds: 80% full (bytes or inodes) or less than 2 GB free. Critical:
90% full or less than 1 GB free; the service fails visibly in systemd. Latest
state: `/run/runcoveer/disk-status.json`, with results also in journalctl. No
external notification integration is implied by these local checks.

Before creating a backup, reserve twice the current data size plus 2 GiB for
snapshot/encryption; otherwise defer with a clear error, retaining the previous
complete backup. A failed snapshot removes only its own incomplete directory.
Encrypted staging uses one fixed filename and pending uploads are reconciled
before creating another archive. Persistent data growth requires capacity
planning; the guard does not delete database rows or user assets.
