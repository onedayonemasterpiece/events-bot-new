# Atomic root provisioning — review only, 28.11.2026

**Decision package, not an apply instruction. No cloud writes, DNS, production env or deployment changes.** The original direct shared-bucket backend is incompatible with its required LIST=false. The amended IaC is an explicit PE+HTTPS candidate, not an assertion that YC accepts a PE address as an ALB target. No paid resources were created. Current source inspected at main 5b1d1f95d0d1c0e1b982ce639d4a43b304b9e45e; UI1125 merged; builder integration PR1128 is separate. Use only a checked full production candidate built from accepted merged main with running ancestry. Do not merge this proposal as an approved contract or apply it before the decision and acceptance below.

## Evidence and minimum remediation

Reader opens canonical root after normal Smart Update: September prelaunch, Today/Search 404. Current related-only build can emit related_ready,published=false without Astro. Separate full-builder and UI PRs remedy candidate side. Root delivery failure: contracted page buckets + ALB absent. At 10:35Z coordinator thread reports root A 213.180.193.247 and static A 188.72.112.1, strict TLS valid until Jan 2027; raw kenigevents.ru/index.html=200 10261 B identical to root, segodnya/index.html=404, prelaunch receipt prelaunch-main-35568442999-1, sha313a72ebd280d93f5366dca8a3060029091f435a, 21 Sep 06:29:56Z; R7 preview intact; clubs CSS missing. At 11:24Z authenticated inventory in b1g5tck18cgqtjb7rn3s: ALB list=[], backend-group list=[], buckets kenigevents and kenigevents.ru, not root-blue/green. These are coordinator's observations, not a new authenticated inventory. No retry of lost Root job. [Thread](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1795861173544759), [prelaunch receipt](https://kenigevents.ru/_static/prelaunch-root/current.json).

Contracts: [atomic runbook](../../../docs/operations/static-site-atomic-root.md), [release plan](../../../docs/features/static-site-pages/release-plan.md), [CDN delivery](../../../docs/features/static-site-pages/cdn-asset-delivery.md), [publisher](../../../static_site_atomic_root.py), [runtime](../../../deploy/runcoveer/compose.yaml). Repository tree has no ALB provisioning module; email-inbound IaC is unrelated. No old PR43 publisher, no new queue or service. Existing incident inc_6511a56fcd9438abf39b87b8 remains open.

## Provider contract, provenance and smallest correction

The original runbook step 1 (introduced in [b163a4c5](https://github.com/onedayonemasterpiece/events-bot-new/commit/b163a4c57681eab6bd0876dee418ddd48b23e147)) says versioning, encryption, anonymous listing off. [W4 results](../../../.codex/lanes/W4/RESULTS.md) describe that as a provisioning risk/implementation assumption, not a quoted owner mandate; [lane map](../../../.codex/integration/static-site-resilient-egress-20260731/LANE_MAP.md) maps W4 to R04/R06, not an explicit public-root no-LIST or customer-KMS demand. No independent owner decision to those specific controls for page-only buckets was located in examined W4 sources and current incident thread. Do not attribute the assumption to the owner; it remains current repository contract until the proposed correction is reviewed. In contrast, the shared bucket's no-LIST is explicitly required: [secret publisher](../../../static_site_release.py) calls _candidate_public_list_disabled before uploading bearer _review links; it is not an optional recommendation.

| Object class | Current/premise | Proposed smallest change |
|---|---|---|
| New public page-only blue/green | Anonymous READ=true, LIST=false; optional customer SSE-KMS inferred from “encryption” | READ=true, LIST=true, config_read=false, versioning=true; no customer KMS config. Public-only manifest, provider's system-key at-rest encryption, HTTPS public ingress. No sensitive inputs, tokens, user-specific/private data, review or release-control objects. |
| Existing shared kenigevents.ru | Verified READ=true, LIST=false, config_read=false, versioning disabled; known GET 200, anonymous ListObjectsV2 max-keys=0 returns 403 | No ACL, policy, versioning, encryption, DNS/CDN-origin or existing object change. Never public LIST. Use separate HTTPS path candidate below; review bearer URLs stay same origin/path/query. |
| Existing legacy kenigevents and static CDN | Already live | Unchanged; no import, migration, copy or delete. |

Primary source: [ALB BackendGroup.Create](https://yandex.cloud/en/docs/application-load-balancer/api-ref/BackendGroup/create), [AddBackend](https://yandex.cloud/en/docs/application-load-balancer/api-ref/BackendGroup/addBackend) and [UpdateBackend](https://yandex.cloud/en/docs/application-load-balancer/api-ref/BackendGroup/updateBackend) require **both** public object access and public list for the storageBucket backend. The [concept page](https://yandex.cloud/en/docs/application-load-balancer/concepts/backend-group) only mentions read; the explicit API condition governs. [Separate anonymous flags](https://yandex.cloud/en/docs/storage/operations/buckets/bucket-availability) confirm that READ alone is not LIST. Native storageBucket cannot satisfy shared's unchanged restriction under this documented contract; no speculative public-prefix listing or global shared ACL change.

[YC security standard §4.2](https://yandex.cloud/en/docs/security/standard/encryption) states system keys encrypt stored data by default; customer SSE-KMS is enhanced protection. [KMS tutorial](https://yandex.cloud/en/docs/storage/tutorials/server-side-encryption) requires an authenticated user/SA with kms.keys.decrypter and storage.configurer for download. [Hosting restrictions](https://yandex.cloud/en/docs/storage/operations/hosting/setup) explicitly exclude customer-encrypted buckets. Native ALB bucket backend has no KMS reader identity field in the API. Remove only the new roots' customer-key block, not at-rest encryption; no public KMS grants. Existing KMS key inventory is empty. If owner requires customer KMS or no public root enumeration, this native bucket backend topology cannot meet it as documented; authenticated backend/proxy is a different design. Root LIST means object names/manifest and any public version history may be discoverable directly (even inactive during staging), bypassing SWS; confirm all bytes and metadata intended public before initial and routine publication. SWS/ARL/WAF protect ingress, not raw anonymous S3.

### Shared route: documented incompatibility and exact minimal alternative

The original IaC's shared storage_bucket="kenigevents.ru" is removed. No native FQDN target: [TargetGroup.Create](https://yandex.cloud/en/docs/application-load-balancer/api-ref/TargetGroup/create), [target group operations](https://yandex.cloud/ru/docs/application-load-balancer/operations/target-group-create) and [concept](https://yandex.cloud/ru/docs/application-load-balancer/concepts/target-group) define IP/subnet targets or external RFC1918 targets over connected networks. External-address flags are not a documented DNS resolver or authorization to pin today's changing Object Storage public address. Existing folder has no VM/PE. Native LIST=false bucket support is not established; no direct supported route under the original exact topology was found.

Minimal alternative without an application proxy or CDN/origin redirect: [VPC service connection](https://yandex.cloud/ru/docs/vpc/concepts/private-endpoint) for Object Storage is regional and provides a private IP from a subnet to resources in its network. Provider [vpc_private_endpoint](https://yandex.cloud/ru/docs/terraform/resources/vpc_private_endpoint) exposes the allocated endpoint_address.address; [alb_target_group](https://yandex.cloud/ru/docs/terraform/resources/alb_target_group) accepts an IP in its subnet. Proposed IaC attaches that provider-returned private IP, not a guessed/hard-coded external IP. private_dns_records_enabled=false avoids changing the primary service DNS (the PE's own special internal record is created as part of the service). HTTPS target backend port 443, SNI=storage.yandexcloud.net, independently verified CA chain, fixed Host rewrite to storage.yandexcloud.net; upstream path /kenigevents.ru/<key>, query carried through, public origin/path/query unchanged. [S3 API URL forms](https://yandex.cloud/ru/docs/storage/s3/) require path-style HTTPS for bucket names containing dots; do not use a virtual-hosted dotted hostname. [Backend TLS/targetGroups](https://yandex.cloud/en/docs/application-load-balancer/api-ref/BackendGroup/addBackend), [HTTP route schema](https://yandex.cloud/ru/docs/terraform/resources/alb_virtual_host). No shared bucket resource or new IAM policy. No anonymous authorization to LIST; no credentials sent upstream.

**Evidence boundary:** components and IP connectivity are documented; the docs do not explicitly identify a VPC PE as an ALB target, while the target-group concept names VMs and explicitly supports internal NLB in a migration example. Provider schema validation is not control-plane acceptance or verified transport. The PE route is therefore a concrete candidate, not a supported end-to-end guarantee. It is an extra paid networking resource and needs explicit decision, not a silent topology expansion. Before any paid action obtain YC confirmation that the same-network PE private IP can be registered as ALB target and that unsigned HTTPS known-key GET/HEAD/OPTIONS, Host/path rewrite and TLS validation work. If accepted, run the pre-DNS drill below after owner authorizes paid resources. If YC confirms native bucket backend can serve LIST=false despite the API text, remove PE + target group, use native shared bucket backend with no policy change and prove it with the same drill; no listing relaxation. If both routes unavailable, the original constraints are incompatible: retaining exact same-origin shared URLs requires an explicitly approved real HTTPS target (e.g. managed reverse proxy on private IP, with HA/operational cost), or a separately approved URL/origin contract migration to existing CDN that cannot silently break bearer links or PWA origin. Neither is implemented or authorized; no new application service, proxy, CDN topology or redirects proposed as automatic fallback.

[ALB](https://yandex.cloud/ru/docs/terraform/resources/alb_load_balancer), [SWS](https://yandex.cloud/ru/docs/terraform/resources/sws_security_profile), [WAF](https://yandex.cloud/ru/docs/terraform/resources/sws_waf_profile), [ARL](https://yandex.cloud/ru/docs/terraform/resources/sws_advanced_rate_limiter_profile). Provider pin [0.250.0](https://github.com/yandex-cloud/terraform-provider-yandex/releases/tag/v0.250.0).

## Exact proposed resources and ownership

main.tf.json proposes two STANDARD versioned page-only buckets, one two-backend root group at 100/0, one PE/target group and HTTPS shared group referencing the same existing bucket by fixed S3 path, router/vhost, SWS/WAF/ARL, protected IPv4, a dedicated ALB SG, two existing subnets in distinct zones. Two zones are proposed HA minimum, not a new owner requirement. No new VPC, DNS resource, existing bucket import, credentials, application proxy or daemon. Root group ignores http_backend drift because the existing publisher owns weights: review membership/bucket map/weights explicitly before future Terraform changes. Never reapply initial weights, never destroy. State local/isolated and protected by operator, not Git. No force_destroy or lifecycle purge. Initial SWS/WAF/ARL dry-run is for measured baseline; existing runbook requires enforce and measured acceptance before production.

Routes, ordered (regexes and replacement strings in IaC):
| Incoming path | Action |
|---|---|
| /_review/<token>/, nested directory /, /preview-.../.../, existing /_static/ directory | HTTPS shared, rewrite to /kenigevents.ru/<key>/index.html |
| Slashless review/preview/_static directory with extensionless leaf | HTTPS shared, append /index.html internally |
| /p/..., /ics/..., /_review/..., /_static/..., /preview-.../..., /_astro/<hash>.* | HTTPS shared, prepend /kenigevents.ru; no external redirect |
| / and root .../ | Root weights, append index.html |
| Root slashless extensionless leaf | Root weights, append /index.html internally |
| Other root files (manifest/SW/robots/sitemap) | Root weights, exact file |

GET/HEAD/OPTIONS are explicit on all six routes; other methods have no match. Query untouched by proposed path rewrite; verify actual ALB query handling and response parity, including Range/ETag, percent encoding, duplicate query keys, encoded dot/slash/traversal, invalid Host, and no upstream bucket-root or other bucket exposure. No generic proxy. [OPTIONS](https://yandex.cloud/ru/docs/storage/s3/api-ref/object/options) returns 200 with CORS headers if allowed, 403 without if disallowed; do not invent permissive CORS. Raw S3 missing key with no LIST may return 403, not 404; compare old public behavior, no SPA fallback, no fake 200. Astro trailingSlash='always' (site/astro.config.mjs), but a slashless internal rewrite may change relative link resolution: test actual slashless old/new HTML, relative URL behavior and canonical metadata; only retain rewrite when parity shown, otherwise a reviewed canonical slash redirect is a distinct change. HTTP 80 existing plan redirects HTTPS, not shared path redirects. static.kenigevents.ru DNS/CDN and existing media/ICS remain unchanged; exact apex only (www before-state separately verify, don't cut it over implicitly).

### Cross-slot immutable assets: actual counterexample, necessary correction

site/astro.config.mjs uses local /_astro unless PUBLIC_ASTRO_ASSET_BASE_URL is explicitly supplied; site/scripts/build-production.mjs sets media CDN base but not an independently published Astro assets namespace. site/scripts/release-contract.mjs marks _astro immutable. static_site_atomic_root.py:418-439 enables the other slot before disabling the first; :916-929 exact inventory; :932-972 deletes stale inactive keys; :531-551 stable HTML smoke does not fetch every linked asset. Thus A HTML references /_astro/A.js, B has only /_astro/B.js: during overlap B may serve A's asset request; after switch an old tab still needs A, after rollback a B tab still needs B. Exact complete trees do NOT imply complete multi-request page loads. [ALB session affinity](https://yandex.cloud/en/docs/application-load-balancer/concepts/backend-group#session-affinity) is restricted to single active target-group backend with MAGLEV_HASH, not two weighted bucket backends. Do not add affinity, duplicate active tree writes, route updates per release, or retain unmanifested keys inside slots.

Smallest proposed fix, separate from concurrent builder integration: reserve same-origin /_astro/ for append-only immutable assets in existing shared bucket (route included in candidate IaC). Before the very first bootstrap and every existing automatic root promotion, publish from the SAME checked production_root release only manifest-bound content-hashed _astro files to existing shared /_astro keys. Reuse existing S3 conditional-create + exact readback pattern from static_site_release.py:2850-2878, not the secret-candidate prefix itself: IfNoneMatch="*", same SHA-256, bytes, size, MIME, cache policy; on conflict compare exact existing bytes/MIME/cache and adopt only if equal, otherwise fail before changing weights. Existing root publisher retains its exact root tree (including _astro) and inactive-only reconciliation unchanged. Use separate narrowly scoped shared-asset SA with GetObject/PutObject only on arn:aws:s3:::kenigevents.ru/_astro/*, no LIST/Delete/overwrite path, no new feature flag or manual step per update; no modifications to existing review publisher credentials. Existing shared root-only contract requires explicit correction for this append-only prefix; never delete existing assets or change review/media/ICS objects. Assets must remain available for previous, failed/rolled-back and cached browser sessions; no GC policy introduced. Other mutable /assets, PWA/SW and /data routes remain on root, test actual file hash/reference compatibility and avoid claims of full session atomicity for those. If artifact emits non-content-addressed or incompatible assets, the proposed subset is not sufficient: inspect manifest and actual dependency closure and correct existing build packaging, not blindly copy. Alternative using already-existing static CDN as asset origin would require verified content-hash publication and explicit origin decision, not a fallback. No runtime patch in this PR: exact existing publisher code and concurrent builder untouched, this is a required implementation handoff before promotion. Merely provisioning the IaC or setting the flag is insufficient. Offline test demonstrates negative using actual two consecutive publisher calls and models append-only address resolution; it is not a real cloud/browser acceptance.

## Before-state / scopes (authenticated read-only, 28 Nov 12:36Z)

Do not repeat known empty ALB/bucket inventory. Retained evidence (no tokens, keys, certificate private material or secret review keys):
- /home/dev/artifacts/events-bot-new/20261128T102033Z-kenigevents-site-recovery/atomic-plan-1126-2beb43ea-readonly.json (op_658db3ba8b5cee856fa33824)
- /home/dev/artifacts/events-bot-new/20261128T102033Z-kenigevents-site-recovery/atomic-plan-1126-2beb43ea-narrow.json (op_60024413d86340d14abb88fe)

| Input | Actual |
|---|---|
| folder | b1g5tck18cgqtjb7rn3s (ACTIVE), network enpefopfcgibi1igmjt2 |
| zone_a / subnet_a | ru-central1-a / e9bq5u6gbejfhpi1lce3 (10.128.0.0/24) |
| zone_b / subnet_b | ru-central1-b / e2l8mn482utf9e8t83u5 (10.129.0.0/24) |
| optional third zone | ru-central1-d / fl87sofhjfgdaf48mtml (10.130.0.0/24) |
| existing SG | enpocps1v12mli8fu6av default allows ANY in/out from 0.0.0.0/0; do NOT import/change/reuse as restricted SG |
| certificate_id | fpqi91sau05ifdvfsft4, managed ISSUED, apex/www, 23 Oct 2026–21 Jan 2027 |
| static cert | fpqs4u9csqlo3qlldgjb, valid through Jan 24; unchanged |
| KMS / VPC PE / compute VM | each inventory empty in folder |
| shared known key | anonymous raw /kenigevents.ru/index.html=200, 10261 bytes, text/html; ListObjectsV2 max-keys=0=403 without reading response body |
| restricted_log_group_id | yc logging group list returned RPC Unavailable twice, entitlement/ID NOT established |
| publisher_storage_sa_id | none of the observed candidates qualifies as least privilege; existing storage.editor at folder scope (sa-bot-storage, flyaccount, kaggle-publish, release-codex); release-codex also editor and unrelated roles |

Default SG untouched. Proposed new SG permits listener TCP80/443, healthcheck TCP30080 from documented health subnets, egress TCP80/443 only (native bucket path is managed; no dynamic public IP pin). Review provider connectivity and narrow PE egress further if supported; [ALB SG](https://yandex.cloud/en/docs/application-load-balancer/concepts/application-load-balancer#security-groups), [health-check ranges](https://yandex.cloud/en/docs/network-load-balancer/concepts/health-check). No guessed certificate or KMS key. Independently validate storage CA PEM from trust store / actual CA chain; no insecure TLS. Unresolved log entitlement: a single native read-only operation yc logging group list --folder-id b1g5tck18cgqtjb7rn3s --format json (return group id/name/status/retention or exact service error; already Unavailable from this window). [Logging quickstart](https://yandex.cloud/ru/docs/logging/quickstart) says no new customers since Oct22; [SWS logging](https://yandex.cloud/ru/docs/smartwebsecurity/operations/configure-logging) documents Monium for SWS, Cloud Logging for ALB. Verify an existing restricted group or obtain YC-supported ALB logging destination; don't assume Monium replacement for ALB from SWS-only docs or silently disable logging. Bearer paths may be logged; access/retention restricted and raw logs not copied to reports. Actual ARL request rate, WAF inbound score and traffic baseline not measured; no guessed defaults.

Capture authoritative DNS A/AAAA/CNAME/ALIAS/TTL/NS for apex, www if exists, static, API/OAuth/focus/preview, exact previous RRsets, not merely resolved IP. Read-only examples: dig +noall +answer kenigevents.ru A / AAAA / CNAME; dig +short NS kenigevents.ru; dig +noall +answer static.kenigevents.ru A. DNS provider not assumed; certificate reuse avoids issuance and DNS mutation. No DNS write until explicit owner cutover approval.

Principals: provisioning operator separately scoped storage admin/configurer on new buckets, alb.editor, vpc.publicAdmin/network editor as required for SG/PE, sws.editor, appropriate CM use and log writer. Runtime root SA bucket policy CanonicalUser ListBucket/GetObject/PutObject/DeleteObject of current new-root keys only; verify authenticated HeadBucket; no DeleteObjectVersion/bucket configuration/delete; no shared permissions, no customer KMS grants. Existing folder storage.editor SAs have wider rights and should not be repurposed as this least-privilege identity. Proposed separate append-only asset SA above with prefix Get/Put, no LIST or delete. Runtime ALB needs get/updateBackend/operation wait; documented alb.editor at folder scope has create/delete too; no proven narrower update-only grant, review resource binding or dedicated folder scope as smallest alternative, do not pretend scoped identity exists or assign broad editor silently. No runtime DNS/CM/SWS/VPC writes. No keys in repo/tfstate/Kaggle; credentials supplied via existing secret channel after authorization. [Storage actions](https://yandex.cloud/ru/docs/storage/s3/api-ref/policy/actions), [ALB IAM](https://yandex.cloud/ru/docs/application-load-balancer/security/).

Actual runtime Events container, not Fly. Dockerfile does not install yc and compose only mounts /data and launch script. Before promotion verify executable inside container and supported auth path without printing tokens. If absent, operator selects existing managed YC CLI mount or smallest pinned packaging change separately reviewed. Persistent /data scratch minimum max(512 MiB, 3 * archive file bytes); use CI/prod scratch, not devserver's low root disk. Before rollout check /data and root (July8 overlay regression), /tmp write, health and SQLite quick check via approved protocol.

## Validation / immutable plan — not run against cloud

Run on isolated CI or provision operator workspace, not devserver checkout:

    cd infra/yandex/static-site-atomic-root
    python -m json.tool main.tf.json >/dev/null
    terraform init -backend=false -input=false
    terraform validate -no-color

With actual inventory-bound variables held outside repo and isolated NEW state (no import), review plan after resolving provider compatibility, logging and publisher prerequisites. This is future read-only plan, not an apply approval. No authenticated Terraform plan run here (inputs missing and PE target composition still unconfirmed):

    terraform plan -input=false -refresh=false -var-file="$VARS" -target=yandex_storage_bucket.root -target=yandex_alb_backend_group.root -out="$EVIDENCE/buckets.tfplan"
    terraform show -no-color "$EVIDENCE/buckets.tfplan" > "$EVIDENCE/buckets.plan.txt"
    sha256sum main.tf.json .terraform.lock.hcl "$EVIDENCE/buckets.tfplan" "$EVIDENCE/buckets.plan.txt"

The first stage is fresh buckets plus root backend group with no attached listener; group ID needed by publisher config. After owner approves paid creation and both new buckets seeded, separate full ALB plan (do not execute apply here):

    terraform plan -input=false -var-file="$VARS" -out="$EVIDENCE/alb.tfplan"
    terraform show -no-color "$EVIDENCE/alb.tfplan" > "$EVIDENCE/alb.plan.txt"
    sha256sum main.tf.json .terraform.lock.hcl "$EVIDENCE/alb.tfplan" "$EVIDENCE/alb.plan.txt"

Plan MUST show adds only, no changes/deletes of existing resources; review redacted output + immutable plan hash, credentials never in evidence. Targeting exceptional one-time bootstrap. Successful schema validate not proof of API-level shared PE target registration or live access. Plan may contain unknown allocated IP; do not preselect a dynamic or occupied IP.

## Bootstrap existing publisher / rollback drill

After UI1125 and the reviewed builder integration accepted and merged, plus the required append-only asset publishing correction, produce checked production-root.tar.gz and static_site_build_result.json from exact merged main and normal full pipeline. Don't rebuild from prelaunch, preview or partial sidecar. Retain archive/result hashes and exact manifest identity. In RunCoveer existing env file /opt/runcoveer/events/.env.production (not modified here), configure non-secret mapping from .env.example:

    ENABLE_STATIC_SITE_ROOT_PROMOTION=0
    STATIC_SITE_ROOT_PROMOTION_MODE=plan
    STATIC_SITE_ROOT_BLUE_BUCKET=kenigevents-root-blue
    STATIC_SITE_ROOT_GREEN_BUCKET=kenigevents-root-green
    STATIC_SITE_ROOT_BLUE_BACKEND=root-blue
    STATIC_SITE_ROOT_GREEN_BACKEND=root-green
    STATIC_SITE_ROOT_SHARED_ASSET_BUCKET=kenigevents.ru
    STATIC_SITE_ROOT_ALB_BACKEND_GROUP_ID=<actual-root-group-id>
    STATIC_SITE_ROOT_YC_FOLDER_ID=b1g5tck18cgqtjb7rn3s
    STATIC_SITE_ROOT_ACTIVE_WEIGHT=100
    STATIC_SITE_ROOT_PUBLIC_BASE_URL=https://kenigevents.ru
    STATIC_SITE_ROOT_RECEIPT_PATH=/data/static_site_builder/atomic-root-receipt.json
    STATIC_SITE_ROOT_SCRATCH_DIR=/data/static_site_builder/atomic-root-tmp
    STATIC_SITE_ROOT_YC_ENDPOINT=https://storage.yandexcloud.net
    STATIC_SITE_ROOT_YC_REGION=ru-central1
    STATIC_SITE_ROOT_YC_CLI=<verified-container-path>

Secrets STATIC_SITE_ROOT_YC_ACCESS_KEY_ID / STATIC_SITE_ROOT_YC_SECRET_ACCESS_KEY via existing secret channel; retain existing ENABLE_STATIC_SITE_SECRET_PUBLISH and KENIGEVENTS_SITE_* settings. Root group without listener first. Seed and verify immutable asset subset via the reviewed append-only publication path before attaching the ALB routes. One-time seed using existing functions (not new publisher), both slots newly created, empty, authenticated, no old data. On partial failure retain bytes, explicitly revalidate exact keys/hashes, no blanket delete.

    # Future owner-authorized bootstrap, NOT executed. Run inside existing env + checked source.
    # ROOT_ARCHIVE and BUILD_RESULT are absolute paths to real candidate files.
    python - <<'PY'
    import json, os, shutil
    from pathlib import Path
    from static_site_atomic_root import RootPublisherConfig, S3RootObjectStore, validate_root_archive, _verify_remote_tree
    cfg = RootPublisherConfig.from_env()
    assert cfg.mode == "plan"
    assert {s.bucket for s in cfg.slots} == {"kenigevents-root-blue","kenigevents-root-green"}
    assert cfg.shared_asset_bucket == "kenigevents.ru"
    store = S3RootObjectStore(cfg)
    for slot in cfg.slots:
        store.check_bucket(slot.bucket)
        assert not store.list_keys(slot.bucket), "bootstrap requires empty new bucket"
    result = json.loads(Path(os.environ["BUILD_RESULT"]).read_text())
    release = validate_root_archive(os.environ["ROOT_ARCHIVE"], build_result=result,
        extraction_root=cfg.extraction_root, expected_site_origin=cfg.public_base_url)
    try:
        for slot in cfg.slots:
            for item in release.files:
                store.write(slot.bucket, item)
            _verify_remote_tree(store, slot, release)
            print(slot.name, release.manifest_sha256, release.tree_sha256)
    finally:
        shutil.rmtree(release.root, ignore_errors=True)
    PY

No actual execution. Never use old bucket; publisher rejects empty active tree (source lines 1027-1034). Plan CLI after seed, planned_noop for A, planned for distinct checked B; verifies active manifest and hashes/metadata inventory, no cloud writes and local receipt:

    python3 static_site_atomic_root.py --mode plan --archive "$ROOT_ARCHIVE" --result "$BUILD_RESULT"
    yc alb backend-group get "$GROUP_ID" --folder-id b1g5tck18cgqtjb7rn3s --format json

Pre-DNS strict smoke uses canonical host/SNI and only local resolver override (no public DNS change):

    curl --fail --silent --show-error --resolve "kenigevents.ru:443:$ALB_IP" -H 'Accept-Encoding: identity' -H 'Cache-Control: no-cache' "https://kenigevents.ru/segodnya/?root-release=$NONCE" -o "$EVIDENCE/today.html"
    sha256sum "$EVIDENCE/today.html"

Compare actual body and MIME to manifest for all required routes, including /, Today, Search, root/focus manifests and focus invitation, all catalog routes and current event/club details, sitemap/robots/SW, ICS, CDN media and CSS/JS, R7 and real secret review (redact token), actual PWA install standalone Android and identity retention. Test anonymous direct new root GET/LIST expected allowed (public-only inventory, no secrets), existing shared GET 200 and LIST 403 (do not print listing body). Browser artifact gate runs in existing checked artifact pipeline; live ALB browser needs canonical hostname local resolver, don't change STATIC_SITE_ROOT_PUBLIC_BASE_URL or certificate verification. Check browser network errors, DOM/link navigation, source asset manifest. Test both old/new browser sessions during switch, no forced old release replay.

Distinguishing pre-DNS drill (requires paid provisioning authorization; NOT executed):

1. With VPC PE provisioned under approved exact plan, read PE allocated address + subnet/status and ALB target/group membership. From an allowed same-network test context, curl --resolve storage.yandexcloud.net:443:$PE_IP https://storage.yandexcloud.net/kenigevents.ru/index.html (normal TLS validation, 200 + SHA/MIME same as baseline); raw shared LIST with max-keys=0 remains 403. No test VM/proxy automatically created. If no suitable VPC test context use ALB pre-DNS end-to-end directly.
2. Via curl --resolve kenigevents.ru:443:$ALB_IP request one existing known shared public key (e.g. /_static/prelaunch-root/current.json), one owner-held review URL (never log token), media and stable ICS with query ?v=probe, GET/HEAD with Range/ETag and OPTIONS with Origin+Access-Control-Request-Method. Compare exact old vs new status/MIME/cache headers, body hash, CORS allow/deny and final URL/origin; test slashless + slash paths, root catalog/event routes and no unexpected redirect. Probe missing known prefix, path traversal/encoded dots/slashes and non-read method against an isolated synthetic route; no unintended listing, other bucket or write path. Direct shared ListObjectsV2 max-keys=0 is still 403; do not reveal list body. No broad bucket enumeration. If TLS, path/query, known-key GET or CORS parity fails, no cutover.
3. Prepare two checked distinct A/B manifests and browser dependency closure with different _astro hashes. While A serves and B stages, request A asset; during weight overlap, request both A/B hashed assets repeatedly with cold cache, including dynamic chunks; after successful B and forced rollback, open old A and B pages and verify each immutable hash and MIME remains reachable. Validate manifest identities on both roots, no stale/partial tree and no shared key overwrite/delete. Browser tests include stable nonhashed assets, SW, focus install/identity, missing routes and actual navigation. No session atomicity claim until observed. If either asset set unavailable, do not enable root promotion.

Existing offline regression:
    pytest -q tests/test_static_site_atomic_root.py tests/test_static_site_release.py tests/test_static_site_build_handoff.py
    pytest -q tests/test_static_site_atomic_root.py -k 'stable_smoke_failure_rolls_back or weight_convergence or plan_mode or active_bucket_is_never_written'
    npm --prefix site run test:static-release
    npm --prefix site run test:browser-release-gate

Forced rollback acceptance (after owner approval, isolated ALB canonical resolver, no public DNS): start A sole active, B distinct fully checked. Run existing public function with injected smoke, no new production code. Expected rolled_back, current=A previous=B unhealthy, switch + rollback operation receipts, revision and weights 100/0, exact A public hash, shared before-state untouched. The injection tests the same branch as real HTTP error, not fake successful promotion.

    python - <<'PY'
    import json, os
    from pathlib import Path
    from dataclasses import replace
    from static_site_atomic_root import RootPublisherConfig,YandexAlbCliAdapter,S3RootObjectStore,publish_atomic_root_archive
    class ForcedSmoke:
        def verify(self, *, release, nonce):
            raise RuntimeError("forced_smoke_failure")
    cfg = replace(RootPublisherConfig.from_env(), mode="apply")
    assert cfg.mode == "apply" and cfg.public_base_url == "https://kenigevents.ru"
    result=json.loads(Path(os.environ["BUILD_RESULT_B"]).read_text())
    receipt=publish_atomic_root_archive(os.environ["ROOT_ARCHIVE_B"],build_result=result,
        config=cfg,control=YandexAlbCliAdapter(cfg),store=S3RootObjectStore(cfg),smoke=ForcedSmoke())
    assert receipt["status"]=="rolled_back", receipt["status"]
    print(receipt["status"],receipt["revision"],receipt["operation_ids"])
    PY

No forced real-cloud run here; exact source tests plus explicit manual future procedure. Normal controlled B apply (existing CLI --mode apply) only owner-approved, before DNS with canonical resolver and verified complete A/B. Compare identities and public SHA/MIME; CLI exit 2 on rolled_back. Publisher enables target before disabling source, waits operations/readback; rollback same converge. If interrupted with ambiguous weights or rollback failure: stop existing flag, read group, only restore known complete former backend first, then set other weight 0 using documented existing adapter exact commands (yc alb backend-group update-http-backend "$GROUP_ID" --name root-blue --weight 100 --async --folder-id b1g5tck18cgqtjb7rn3s --format json; yc operation wait "$OP_ID" --folder-id b1g5tck18cgqtjb7rn3s --format json; repeat other to zero; then get). No bucket deletes. Only after accepted drill authorize canonical DNS RRset move to ALB IP, lower TTL beforehand only with approval, wait previous TTL; preserve old origin/cert/data, restore exact prior authoritative RRset on DNS rollback (not blindly set historical resolved A if ALIAS/CNAME). No static DNS changes. Verify both resolvers and real clients until old TTL elapsed, then normal natural CREATE + UPDATE and following checked full builds -> receipt promoted/noop + public SHA and actual changed events (not replay); verify VK related latency and publication. If failed, disable existing root promotion flag, restore known good backend/old RRset as appropriate. Do not destroy resources or old data.

Monitoring: existing Monitoring metrics service=application-load-balancer, load_balancer.requests_count_per_second code label for 4xx/5xx, requests_latency_milliseconds, scaling.requests_per_second/active_connections; SWS outcomes/ARL/blocked legit. Configure alert recipients in existing channel, thresholds from measured baseline, hard fail on known-route/hash/strict TLS failure. Keep receipt age, failed publish, no successful root receipt after successful full candidate in existing incident observation. No new background service. Existing incident guards July8 disk and July19 stale-builder lease included.

## Release gates: existing only

[Atomic runbook](../../../docs/operations/static-site-atomic-root.md) explicitly requires owner approval for apply/flag/DNS; [release plan](../../../docs/features/static-site-pages/release-plan.md) and [release autotest gates](../../../docs/features/static-site-pages/release-autotest-gates.md) require checked manifest, browser/real PWA and production acceptance including daily blocking data.critical_pipelines_daily (3 consecutive scheduled PASS, failed-refresh isolation, downstream consumption, no-change evidence, 72h). Exact main search found data.critical_pipelines_daily only in docs; no evidence of pass. Older Missing rows may be historical (current atomic publisher exists; full builder exists but related-only profile, builder integration pending; UI1125 merged but not live). No claim all historical Missing rows still missing/resolved. Separate canonical D0/D10 Telegraph/public-link rollout and owner product approval; don't switch existing Telegram/VK URLs. Real OAuth/search/collections/focus testing and public noindex/indexing contract remain owner release plan, not new gates. This draft cannot release, incident remains open.

## Cost — actual authenticated SKU versions, RUB incl VAT, 720 h

28 Nov 2026 12:36 UTC read-only Billing Service.List/Sku.List for bound billing account, folder cloud association verified. Returned latest effective STREET_PRICE versions; no CONTRACT_PRICE override returned (not proof of absent account discount). Full SKU IDs/unit/effectiveTime/rates retained in read-only JSON above. Account quota usage and real RPS/traffic not measured. [Billing SKU API](https://yandex.cloud/ru/docs/billing/api-ref/Sku/list); [ALB](https://yandex.cloud/ru/docs/application-load-balancer/pricing), [VPC](https://yandex.cloud/ru/docs/vpc/pricing), [SWS](https://yandex.cloud/ru/docs/smartwebsecurity/pricing), [Storage](https://yandex.cloud/ru/docs/storage/pricing), [Logging](https://yandex.cloud/ru/docs/logging/pricing).

| SKU ID | Actual rate |
|---|---|
| dn2tadkmnr78hd4cp1l6 ALB resource unit | 2.63/unit-hour, effective 2026-04-30T21:00Z; min 2 units per zone |
| dn26d8ndqtdh51aais5h ALB active IPv4 | 0.26352/h (effective 2025-12-31T21:00Z); inactive dn2vtcr86atnrm5her6c 0.34038/h |
| dn2uhjmenr743xxdxj2r VPC PE | 0.8424/h effective 2026-06-30T21:00Z |
| dn26ir5ccmrrtqfjz773 / dn2atdqtzmzak2gqq4lm PE ingress/egress | 0.0648/GiB each direction, same effective date |
| dn28ml7sjbb5v98jkuj3 VPC outgoing | first 100 GiB free, then 1.42/GiB effective 2026-04-30T21:00Z |
| dn2c0dto33dte0qg9hpe DDoS-filtered incoming | 2.26/GiB effective 2026-04-30T21:00Z |
| dn2mq25rnvkhqml3c5nq SWS / dn240oicjob6k1q1isf2 WAF | each first 10k free, 27,450/million through 1m, 3,843/million next through 10m, then 219.6/million through 100m; effective 2025-12-31T21:00Z |
| dn2blgm6egk7u97t0mr0 STANDARD | 720 GiB-hours free, then .0033/GiB-hour (2.376/GiB per 720h), effective 2026-04-30T21:00Z |
| dn2c17otu20ij2eu4k18 PUT, dn2rcmep94te6ps49spp LIST, dn2runrjuiiq807rb18s POST, dn2clegl6fmj0sedcn4b PATCH | 10k free then .5692/1k each SKU, effective 2026-04-30T21:00Z |
| dn2tsb3bn2sgk6tict7r GET, dn2l7lp6fkm81ge7p8uc HEAD, dn2064t09vdjeuu973hm OPTIONS | 100k free then .46/10k each SKU, effective 2026-04-30T21:00Z |
| dn2fghbb6nhc0qgc03ta external Storage egress | first 100 GiB free, then 1.67994/GiB through 1024, 1.40544 through 51200, 1.3176 through 102400, 1.2627 after; effective 2025-12-31T21:00Z |
| dn26jj76b9gbq9uhgrel Object Storage to cloud networks | 0/GiB |
| dn2idprquhacf32r0509 Cloud Logging writes | 5 GiB free, then 25.55046/GiB |
| dn2v7uqqdp73iekn9oih Cloud Logging storage | 720 GiB-hour free, then .015494/GiB-hour (11.15568/GiB/720h) |
| dn2v26ez3jp4ztahlu7u Monium log writes | 4.4/GiB; not assumed ALB-compatible |
| CM existing cert / SG / customer KMS for proposed roots | no incremental cert/SG charge, no KMS key/operations planned; system-key at-rest encryption |

Pricing tiers above are returned SKU thresholds, not a claim that free quota remains unused or that each free allowance is independent under service billing. No annual SWS commitment. ALB min resource units not measured peak. Fixed cost = 720*(zones*2*2.63+.26352) plus .8424*720 only if PE chosen:
| Zones | ALB + active IPv4 | with candidate PE |
|---:|---:|---:|
| 1 | 3,976.93 | 4,583.46 |
| 2 | 7,764.13 | 8,370.66 |
| 3 | 11,551.33 | 12,157.86 |

One zone theoretical floor, 2 proposed, 3 optional; not owner requirements. PE incremental 606.53/mo + both-direction PE traffic, not a free network feature. For 2 zones with PE, 10k/100k/1m/10m legitimate requests through BOTH SWS and WAF, assuming free quotas available: 8,370.66 / 13,311.66 / 62,721.66 / 131,895.66. Dry-run still billed for processed allowed requests. Requests include assets, previews and accepted bots, not people. No additional ARL SKU observed for PAYG. Actual no-PE totals 7,764.13 / 12,705.13 / 62,115.13 / 131,289.13; only applicable if YC confirms native shared route.

Variable-cost example: 10 GiB aggregate root trees+version history -> 21.38 assuming 1GiB free; add shared immutable assets actual growth at 2.376/GiB-mo. 300 GiB public ALB egress -> 284 if first 100 free; 20 GiB legitimate ingress through DDoS -> 45.20; 300 GiB direct external S3 publisher readback -> 335.99 separately if same tier and free quota available; 100 GiB traffic through PE each direction -> 12.96, count actual ingress and egress separately. Internal Object Storage-to-cloud is free, do not double-count as external S3 egress. 1.5m GET alone -> 64.40 with free quota; 100k PUT -> 51.228; LIST separately at same SKU rate. Cloud Logging example 15 GiB writes -> 255.50 if entitled and 5GiB quota free, retention billed separately. Log entitlement not verified. Actual traffic and quota baseline needed for budget, not fabricated; all prices use authenticated SKU data, not illustrative web examples. No paid tests or provisioned resources.

## §6 / validation status and decisions

| Item | Why needed | Existing mechanism reused / alternative rejected | Production delta |
|---|---|---|---|
| Public root LIST + system encryption | API requires public list, KMS conflicts with unsigned reads | existing two native buckets/versioning; no public KMS, no private data | change one flag, delete customer-KMS var/block |
| Candidate VPC PE + ALB target | no supported native shared LIST=false in API contract | fixed S3 Host/path/HTTPS, existing network/cert; no FQDN/dynamic public-IP pin, no proxy | 2 declarative resources; conditional on provider/live proof |
| Dedicated ALB SG | existing SG permits ANY all ports | known listener/health ports, restrict egress ports; no default SG edits | 1 resource |
| Shared/static routing + slashless/OPTIONS | same-origin shared keys and public pages must work | ordered existing ALB routes; no automatic redirects or CDN topology changes | 2 extra routes, Host/path rewrites and methods |
| Cross-release _astro | actual complete-slot counterexample | existing checked artifact/publisher/create-only readback; append-only shared prefix proposed; no new queue or manual per build | plan/acceptance only; no runtime patch |
| CI | existing workflow must run exact root regression | add existing root test file to incident test list; delete branch-only workflow after candidate schema validate | one CI line, tests separate |

No new queues, flags, timers, background services, secrets, customer KMS, public shared LIST, active bucket writes, DNS or cloud changes. This PR changes declarative config and documentation/tests only. Builder integration/production not touched. User-visible improvement only when accepted full candidate reaches stable public root. Existing release-plan gates and incident contracts preserved; do not substitute fixture or review candidate for canonical. Provider validate is schema-only, ordinary CI offline; neither establishes PE registration, public GET or live rollback.

Owner-only decisions requested by coordinator after technical review: (1) accept corrected page-only public LIST/system-key contract (including exposure of manifest/object names/versions and inactive staging; no sensitive bytes), (2) accept extra PE resource if supported and scoped append-only shared _astro contract (or explicitly choose different topology if PE unsupported), (3) budget authorization after actual request/traffic estimate plus logging/IAM choices and immutable plan, (4) existing runbook authorization of paid provisioning, apply/flag and DNS cutover after pre-DNS acceptance. No extra per-update approval. Remaining technical facts: PE target eligibility, TLS/route and known shared key/no LIST live acceptance, logging entitlement, exact new publisher principal/role scoping, asset append-only implementation and complete dependency test, checked full candidate and existing release gates. If PE unsupported and native shared LIST=false not supported, retain old DNS, no apply. Incident stays open until actual public release. 

