# RunCoveer deployment

Host: `78.111.90.203`, Serverspace `l72s1332671`, Ubuntu 26.04,
1 CPU / 3 GB RAM / 25 GB boot volume. SSH alias `runcoveer-production` uses
`deploy` and DevCoveer-only `~/.ssh/prod_78_111_91_149_ed25519`.

Scope: Events Bot and Kotopogoda application runtimes. Yandex Object Storage,
CDN, Supabase, Kaggle and other managed dependencies stay external.

## Build and restore

The 2026-10-06 Events production baseline, retained for rollback, was
`4b23f991842abb53c26bb95429444d4e5d741e3a`, packaged as immutable image
`runcoveer/events:4b23f991-slim`. This rollout reuses the verified runtime and
dependency layers from `runcoveer/events:c344d63d` and replaces `/app` from an
exact Git archive. Before using that disk-safe source-layer build, the release
verified that both `Dockerfile` and `requirements.txt` have zero diff between
`c344d63d` and `4b23f991`; image provenance then matched the exact source SHA
and source-file checksums. For ordinary upgrades, keep the full exact-archive
build path: provide the exact `ai_resource_control-0.1.4-py3-none-any.whl` in
`vendor-private` and run Dockerfile with the intended 40-character
`STATIC_SITE_IMAGE_REPO_SHA`. Source-layer reuse is allowed only when dependency
inputs are unchanged and the resulting image provenance is explicitly checked.
Do not overwrite `/opt/runcoveer/events/source` during an Events-only upgrade:
Kotopogoda mounts `runtime_logging.py` from that preserved source tree.
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

## MAX recovery release (2026-10-10 13:12 UTC)

Production Events uses `runcoveer/events:maxrecovery-c4fa01923bd9`, exact source
`c4fa01923bd9de5a5ba329f9876b7e3a97b2cfce` (PR #740, four CI checks passed),
image ID `sha256:31442ea2c7da93f7a25e51fd6be440cc6f3d73a339457d4de71a36e9904e76df`.
The disk-bounded source delta over the qualified full-source image below changed
seven files with no deletions or renames. All 4,412 tracked file hashes, complete
tree membership, file types and Git executable bits were verified against the
exact commit; generated provenance was refreshed. Dependency inputs and the
CherryFlash module hash were unchanged.

The previously established isolated consistent DB copy from today's initial
rollout was reused, with its provenance retained. SQLite `quick_check`, both
lazy MAX schemas, and network-none candidate startup passed. No live DB was
mounted into that preflight. Events-only cutover completed at 13:12:04 UTC:
`ok=true`, `ready=true`, `issues=[]`, MAX remained enabled, and the original
operation/request/payload identity was unchanged. Events container became
`d3280ff574d8f8d16bb58307bf6ae301663be33d1631f1e77b1942a3f4007a51`;
Kotopogoda retained its exact container identity recorded below.
VibePublish prerequisite source `5879a32a7dd3a8b2deff0458c3fe8cdde5f4edd0`
was already verified live. The ordinary maintenance scheduler owns observation
and bounded recovery; this release receipt does not claim MAX publication.

Build, preflight and cutover receipts are retained under
`/opt/runcoveer/releases/events-c4fa01923bd9de5a5ba329f9876b7e3a97b2cfce/`.
The immediate image rollback is `runcoveer/events:maxdigest-6bb8c8bde960`,
ID `sha256:0509c59e6b23bf965a53337def17344f0a0075a92da3467339f1b85a1f6ee264`.
Rollback changes only the Events image pin, preserving enabled binding and all
current data; use Events-only Compose `up -d --no-deps --no-build --pull never events`
with `DEPLOY_PREFLIGHT=0`. Do not restore a DB or invoke the two-service deploy
script for this image-only rollback. Incident `inc_ef32a78d9c053a6dd1fc9a32`
tracks provider acceptance separately.

## MAX digest implementation rollout (2026-10-10)

The initial MAX implementation rollout used `runcoveer/events:maxdigest-6bb8c8bde960`, exact full
source `6bb8c8bde960f3ae0eb0458635ef9ba3f1420a04` (PR #736), image ID
`sha256:0509c59e6b23bf965a53337def17344f0a0075a92da3467339f1b85a1f6ee264`.
All four CI jobs passed on final head `be7358845d1c11b0d8432b889dd8f7bc221cf134`.
The dependency inputs remained unchanged against qualified `4b23f991-slim`;
the full `/app` exact archive replaced old source and regenerated provenance.
The CherryFlash module hash below is preserved.

Isolated no-network consistent-DB preflight passed, including both lazy MAX
ledger/snapshot schemas. Events-only cutover at 2026-10-10 11:42:34 UTC returned
`ok=true`, `ready=true`, `issues=[]`; new Events container is
`918a0dd28cc92fdf565491b9f789be7fd50c1e68a0e6c4fb9e321dd7bb9230b5`.
Kotopogoda container `05f91f1e423b757b6d0e8938b5da19cc6cb4017cda6b5a79c2a5a160b3a20784`
was unchanged. At that checkpoint `ENABLE_GUIDE_VISUAL_DIGEST_MAX=0` was
explicit: the cutover receipt proves implementation deployment, not a MAX
publication or activated binding.
That disabled rollout was followed by verified activation at 2026-10-10
12:22:16 UTC. The owner-resolved binding is `max_224b98c2579885727f4c`, native
`-76480585669200`, revision 1, canonical URL
`https://max.ru/channel_uh_kaliningrad`. RunCoveer's installed service identity
passed bootstrap with exactly that sole destination before enabling MAX. The
service grants only bootstrap, publish and status; no Telegram read grant.
Events-only recreation passed readiness with Kotopogoda unchanged. The source
image was unchanged at activation. The canonical Compose now carries the nonsecret binding
and enabled flag; the credential remains only in protected deployment config.
Publication acceptance is tracked separately by edition 289 and its stable
per-target delivery ledger; activation alone is not publication success.
The nonsecret cutover receipt and previous Compose are retained under
`/opt/runcoveer/releases/events-6bb8c8bde960f3ae0eb0458635ef9ba3f1420a04/`.
At that initial checkpoint rollback was the CherryFlash image below. The current
recovery release above instead rolls back to this full-source MAX image.

## CherryFlash scene-selection hotfix (2026-10-09)

Previous production Events was pinned to immutable local image
`runcoveer/events:cherry9671-a9abd662` (image ID
`sha256:72c83df980d5c3baaa40445c49516b570616afabfec99e52b06791578c425abd`).
This is an intentionally **limited source-layer overlay**, not a full-source
build claiming that all code matches `a9abd662`. It starts from the previously
qualified `runcoveer/events:4b23f991-slim` and replaces only
`/app/video_announce/poster_overlay.py` with its exact version from merged
commit `a9abd662aee20edfc63688921e38ae4cc7c29e09` (PR #733).
The deployed Python module SHA-256 is
`46a86e5448184f5ae3c323e0a1bb43671a44f000db49a7e10f6275d210991c06`.
The Docker labels record the base commit, patch commit, module hash and scope.
`Dockerfile` and `requirements.txt` did not change from baseline to patch.

This repairs incident `inc_bfc5ed606c0999b7875f35ec`: CherryFlash discarded
already selected event scenes lacking OCR even when they had canonical
title/date/location and CDN poster. Acceptance in production used the six
actual `READY` items from session #1411: six scenes entered the selection
manifest, including forum event #9671 in second position. That acceptance
was read-only and did not trigger Kaggle or republish the October 9 video.
The production health endpoint returned `ok=true`, `ready=true`, `issues=[]`;
only Events was recreated, with Kotopogoda container identity unchanged.

To rebuild the *same patch image* on this host, obtain that exact Git commit
and its `video_announce/poster_overlay.py` blob, verify the SHA-256 above, and
use the already-present `runcoveer/events:4b23f991-slim` image as Docker
`FROM`. Copy that one file into `/app/video_announce/poster_overlay.py`, apply
the provenance labels above and tag `runcoveer/events:cherry9671-a9abd662`.
Never replace the blob with an unverified later working-tree file. The existing
`docker compose -f /opt/runcoveer/deployment/compose.yaml up -d --no-deps events`
activates that image without replacing Kotopogoda or persistent `/data`.
Rollback uses the preserved `runcoveer/events:4b23f991-slim` image and the
same Events-only Compose command. Production and version-controlled Compose
must keep the **same** image pin before a repeat `deploy.sh`.

The next full-source upgrade should replace the scoped overlay with a verified
exact-archive image, preserving the fix and production data. The incident stays
open until the scheduled October 10 CherryFlash release proves actual external
video delivery, not only a correct event-selection manifest.

## Repeat deploy, persistence and backup

`deploy.sh` copies configuration and runs Compose; it does not restore or replace
data. The checked-in Compose file pins the verified Events image
`runcoveer/events:maxrecovery-c4fa01923bd9`; an application upgrade must build/tag the next
immutable image and update that pin before repeat deploy. Docker and Caddy start
with systemd; containers use
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

### Partner MCP rollout, 2026-10-05

Canonical main `e679853bc328aece113163e30667cfd4bbeab70a` was built as
`runcoveer/events:e679853bc` (image
`sha256:c2855e5c8015131881843d8f6601db094f441a89873f3e213fb6020b3d2f0612`)
using exact `ai-resource-control` 0.1.4. A separate preflight container ran
`Database.init()` with scheduler/webhook disabled; SQLite `quick_check` passed
and the additive promo outcome table was present. Events-only cutover preserved
the Kotopogoda container and the old `runcoveer/events:8105d069f` rollback
image. After cutover, public health, scheduler/tasks, Telegram webhook origin,
owner/Codex/partner OAuth metadata and unauthenticated rejection were verified.
Owner event create/assets/typed operations and partner event/promo gates are
enabled. No real partner grant or public provider mutation was used for
acceptance, so isolated-provider live verification remains a separate gate.

### Owner MCP scope-upgrade rollout, 2026-10-06

PR #727 merged to canonical main
`c344d63d992b64aa0722230bbf7080d05bc8e43c` and was deployed as
`runcoveer/events:c344d63d` (image
`sha256:0d52ec6b552ff494ffbc1feca5845c099c0c81db7fc2c350bea6a19d8036a7cf`).
The exact image reported the same source SHA and the bounded owner discovery
scope set `events:write, partners:manage, promo:read, promo:write`.

Before cutover, production SQLite returned `PRAGMA quick_check=ok`.
A separate `DEPLOY_PREFLIGHT=1` candidate boot returned HTTP 200 with
`issues=[]` while webhook registration and schedulers stayed disabled.
After the Events-only cutover, health returned `ready=true`, `db=ok`, all
critical scheduler/task checks were `ok`, and the previous
`runcoveer/events:e679853bc` image remained available for rollback.

A live OAuth check issued a temporary read-only owner token. `tools/list`
returned 44 tools and included owner event create/edit/reschedule/cancel,
promo create/update/state and partner administration. Calling
`event_create_prepare` with that read-only token returned the expected
OAuth `insufficient_scope` challenge rather than executing a mutation.
The partner protected-resource metadata also remained available. No event,
partner grant, promo campaign or provider publication was created by this
acceptance check.


### Partner authority-scoped create rollout, 2026-10-06

PR #729 introduced server-owned partner event-create authority bindings and PR
#730 hardened `series_operator` with grounded series/project evidence. Canonical
main `4b23f991842abb53c26bb95429444d4e5d741e3a` passed all required GitHub CI
checks. The final source gate covered the entire private-events MCP suite plus
event-operation receipts; the authority-specific targeted suite passed 94
tests, and the final series hardening reported 939 private-MCP/receipt tests.

Production uses `runcoveer/events:4b23f991-slim` (image
`sha256:d835802889fedf6636648c39cb74f6c56b98cb0701a26eb941ad87ba30fc83e8`).
The image embeds the exact `4b23f991842abb53c26bb95429444d4e5d741e3a` source
SHA; `private_events_mcp/partner_access.py` and `event_operation_receipts.py`
matched the exact release context byte-for-byte.

An isolated preflight used a consistent copy of production `db.sqlite`, never
the live `/data` volume. The candidate returned HTTP 200 with `ok=true` and
`issues=[]`; `ready=false`/`db=skipped` was the expected preflight state because
the deployment wrapper intentionally disables scheduler/heartbeat readiness.
The copied database returned `PRAGMA quick_check=ok`, initialized
`mcp_partner_authority`, and contained zero authority rows.

After the Events-only cutover, production returned `ready=true`, `ok=true`,
`db=ok`, `issues=[]` and `PRAGMA quick_check=ok`. No partner or authority grant
was created for acceptance (`mcp_partner=0`, `mcp_partner_authority=0`), no
provider/publication mutation was used, and the first five minutes contained no
`ERROR`, `CRITICAL` or `Traceback` records. `runcoveer/events:c344d63d` and
`runcoveer/events:e679853bc` remain available as rollback images.

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
