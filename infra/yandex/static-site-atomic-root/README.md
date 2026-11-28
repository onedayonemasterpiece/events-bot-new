# Atomic root provisioning — review only, 28.11.2026

**NO-GO / conditional draft. No apply, DNS, bucket policy, IAM, production env or deployment changes.** Do not merge as a release instruction until provider compatibility is resolved. Existing publisher unchanged. Exact source base 6f492189abe226de37e65246467c05ae6580d086; backend running ba37eb9cd619f2ae6b02c923676808700ed49876 per thread. Final candidate only from merged and accepted PR1124+1125 main with running ancestry; not from this branch.

## Evidence and minimum remediation

Reader opens canonical root after normal Smart Update: September prelaunch, Today/Search 404. Current related-only build can emit related_ready,published=false without Astro. Separate full-builder and UI PRs remedy candidate side. Root delivery failure: contracted page buckets + ALB absent. At 10:35Z coordinator thread reports root A 213.180.193.247 and static A 188.72.112.1, strict TLS valid until Jan 2027; raw kenigevents.ru/index.html=200 10261 B identical to root, segodnya/index.html=404, prelaunch receipt prelaunch-main-35568442999-1, sha313a72ebd280d93f5366dca8a3060029091f435a, 21 Sep 06:29:56Z; R7 preview intact; clubs CSS missing. At 11:24Z authenticated inventory in b1g5tck18cgqtjb7rn3s: ALB list=[], backend-group list=[], buckets kenigevents and kenigevents.ru, not root-blue/green. These are coordinator's observations, not a new authenticated inventory. No retry of lost Root job. [Thread](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1795861173544759), [prelaunch receipt](https://kenigevents.ru/_static/prelaunch-root/current.json).

Contracts: [atomic runbook](../../../docs/operations/static-site-atomic-root.md), [release plan](../../../docs/features/static-site-pages/release-plan.md), [CDN delivery](../../../docs/features/static-site-pages/cdn-asset-delivery.md), [publisher](../../../static_site_atomic_root.py), [runtime](../../../deploy/runcoveer/compose.yaml). Repository tree has no ALB provisioning module; email-inbound IaC is unrelated. No old PR43 publisher, no new queue or service. Existing incident inc_6511a56fcd9438abf39b87b8 remains open.

## Provider contradiction: resolve before provisioning

- Atomic runbook requires anonymous READ true, LIST false, versioning/encryption. Current [ALB BackendGroup.Update](https://yandex.cloud/ru/docs/application-load-balancer/api-ref/BackendGroup/update) and [UpdateBackend](https://yandex.cloud/ru/docs/application-load-balancer/api-ref/BackendGroup/updateBackend) require public list AND public objects. [Bucket availability](https://yandex.cloud/en/docs/storage/operations/buckets/bucket-availability) confirms flags independent. No proof LIST=false works. main.tf.json intentionally keeps it false. Do not silently relax. Existing shared bucket contains _review bearer-token keys; never open its LIST, never log or enumerate keys in report. Separate shared group with unchanged bucket policy is conditional on provider confirming compatibility; root-only relaxation does not solve same-origin shared routes. Obtain YC supported private-list access answer; if unavailable owner must decide documented topology/route contract change. No redirect of bearer review links to different origin, single-bucket rewrite or new proxy as implicit fallback.
- [Encryption](https://yandex.cloud/ru/docs/storage/concepts/encryption) and [download](https://yandex.cloud/ru/docs/storage/operations/objects/download) require KMS decrypter for encrypted objects. Verify ALB reads SSE-KMS without public key grants, and public-read contract supported. Never grant KMS rights publicly. Reuse an appropriate existing dedicated key if possible. No claim anonymous GET works.
- Direct public Object Storage origin bypasses SWS; L7 protection covers ALB ingress only. No claim of origin protection.
- [Routes](https://yandex.cloud/ru/docs/application-load-balancer/operations/manage-routes), [storage](https://yandex.cloud/ru/docs/terraform/resources/storage_bucket), [ALB](https://yandex.cloud/ru/docs/terraform/resources/alb_load_balancer), [backend group](https://yandex.cloud/ru/docs/terraform/resources/alb_backend_group), [SWS](https://yandex.cloud/ru/docs/terraform/resources/sws_security_profile), [WAF](https://yandex.cloud/ru/docs/terraform/resources/sws_waf_profile), [ARL](https://yandex.cloud/ru/docs/terraform/resources/sws_advanced_rate_limiter_profile). Provider pin [0.250.0](https://github.com/yandex-cloud/terraform-provider-yandex/releases/tag/v0.250.0), 17 Nov. Pin is a review choice, not a runtime release.

## Exact proposed resources and ownership

main.tf.json creates two STANDARD versioned page-only buckets, one two-backend root group at 100/0, separate shared-routes group referencing the existing bucket without managing it, router/vhost, SWS/WAF/ARL profiles in logging-only initial mode, reserved protected IPv4, ALB with 2 existing subnets in distinct zones. 2 zones proposed HA minimum. No new VPC, DNS resource, existing bucket import, credentials. Terraform ignores root http_backend block changes because publisher owns live weights; every later review independently compare exact membership, bucket map, weights with yc get. Never reapply initial weights, never destroy. State local/isolated and protected by operator, not Git. No force_destroy or lifecycle purge. Version growth cost measured, not cleaned automatically. Required non-secret inputs: actual network/subnet A/B IDs and zones (same VPC), SG IDs, issued root cert ID, appropriate KMS key ID, root-only storage SA ID, restricted existing log group ID, measured ARL req/min and reviewed WAF anomaly score. No guessed identity or threshold. SWS/ARL dry_run only for pre-DNS baseline; enforce after review before production acceptance. Existing log group access restricted: logs can contain bearer URLs. Do not attach raw paths to reports. Since 22 Oct YC Logging closed to new customers, if no entitlement investigate official Monium path, don't add it silently.

Routes ordered: shared directory regex (append index.html), shared file regex, root directory regex (append index.html), root fallback. GET/HEAD; HTTP 80 redirects HTTPS; exact authority kenigevents.ru, issued certificate via CM. Shared paths p/, ics/, _review/, _static/, preview-* preserve same hostname/path/query, no new publication; compare before-state for known links incl prelaunch receipt/R7. Root /, catalog/event/page families, _astro if local, manifest, SW, focus invitation and focus manifest from checked root. Missing URL should remain real 404, never SPA fallback. Verify provider slashless directory, query, S3 error MIME and CORS/OPTIONS where actually used. Do not expose control pointers beyond existing exposure. static.kenigevents.ru DNS/CDN origin unchanged. Existing media/ICS CDN and secret publisher stay as-is; API/RPC/telemetry/OAuth actual origins captured from final HTML, subdomains unchanged. Test cold/warm cache + PWA, all versioned assets during 100/100 transient overlap (atomic per request, not across page requests). If CSS/JS are local root-only and old/new hashes differ, switching may serve HTML referencing a missing asset on other slot; no session-atomicity claim until browser drill proves existing layout handles it.

## Before-state / scopes

Capture authoritative DNS A/AAAA/CNAME/ALIAS/TTL/NS for apex, www if exists, static, API/OAuth/focus/preview, exact old certificate IDs/SAN/validity; keep private DNS export in approved evidence directory. Read-only examples:

    dig +noall +answer kenigevents.ru A
    dig +noall +answer kenigevents.ru AAAA
    dig +noall +answer kenigevents.ru CNAME
    dig +short NS kenigevents.ru
    dig +noall +answer static.kenigevents.ru A
    yc alb backend-group list --folder-id b1g5tck18cgqtjb7rn3s --format json
    yc alb load-balancer list --folder-id b1g5tck18cgqtjb7rn3s --format json
    yc vpc network list --folder-id b1g5tck18cgqtjb7rn3s --format json
    yc vpc subnet list --folder-id b1g5tck18cgqtjb7rn3s --format json
    yc vpc security-group list --folder-id b1g5tck18cgqtjb7rn3s --format json
    yc certificate-manager certificate list --folder-id b1g5tck18cgqtjb7rn3s --format json

No authoritative DNS export, TTL, network/SG, cert/SA/KMS IDs or traffic baseline verified here. Don't assume DNS provider is Cloud DNS. Existing SG must permit 443/80 ingress, required YC health/network egress per official ALB SG guidance; restrict control plane separately; no broad SG edits. TLS reuse actual issued root certificate; otherwise request managed DNS validation under owner-authorized phase, keep old record/cert; don't move public DNS for issuance.

Principals: provisioning operator separately scoped storage admin/configurer on new buckets, alb.editor and vpc.publicAdmin on network, sws.editor, appropriate CM use and KMS admin only if key creation/assignment required; verify [ALB IAM](https://yandex.cloud/ru/docs/application-load-balancer/security/) and SWS/CM scopes. Runtime storage principal gets new root bucket policy CanonicalUser only: ListBucket (+verify authenticated HeadBucket), GetObject/PutObject/DeleteObject of current keys only, no DeleteObjectVersion, bucket configuration/delete, shared access; [policy/actions](https://yandex.cloud/ru/docs/storage/s3/api-ref/policy/actions). KMS encrypterDecrypter only on chosen key if compatible; docs mention storage.configurer for encrypted objects, broader than desired: confirm precise combination, don't grant broadly to bypass. Runtime ALB principal needs get/updateBackend/operation wait; documented alb.editor at folder scope includes delete/create, not just weight; require scope review, no assumed narrow role/custom IAM. No runtime DNS/CM/SWS/VPC writes. Independent credentials not in repo/tfstate/Kaggle. Verify negative checks by IAM policy inspection, no production write probes.

Actual runtime Events container, not Fly. Dockerfile does not install yc and compose only mounts /data and launch script. Before promotion verify executable inside container and supported auth path without printing tokens. If absent, operator selects existing managed YC CLI mount or smallest pinned packaging change separately reviewed; no image dependencies installed on devserver. Persistent /data scratch minimum max(512 MiB, 3 * archive file bytes), sufficient extracted tree/readback overhead. Existing disk 68 MiB root inadequate. Use CI scratch and existing persistent volume. Before later release check both root and /data (July8 overlay regression), /tmp write, health and SQLite quick check via approved operator protocol; do not dump env.

## Validation / immutable plan — not run against cloud

Run on isolated CI or provision operator workspace, not devserver checkout:

    cd infra/yandex/static-site-atomic-root
    python -m json.tool main.tf.json >/dev/null
    terraform init -backend=false -input=false
    terraform validate -no-color

With actual inventory-bound variables file held outside repo and isolated NEW state (no import), read-only authenticated plan commands after resolving provider compatibility:

    terraform plan -input=false -refresh=false -var-file="$VARS" -target=yandex_storage_bucket.root -target=yandex_alb_backend_group.root -out="$EVIDENCE/buckets.tfplan"
    terraform show -no-color "$EVIDENCE/buckets.tfplan" > "$EVIDENCE/buckets.plan.txt"
    sha256sum main.tf.json .terraform.lock.hcl "$EVIDENCE/buckets.tfplan" "$EVIDENCE/buckets.plan.txt"

The first stage is fresh buckets plus root backend group with no attached listener; group ID needed by publisher config. After owner approves paid creation and both new buckets seeded, separate full ALB plan (do not execute apply here):

    terraform plan -input=false -var-file="$VARS" -out="$EVIDENCE/alb.tfplan"
    terraform show -no-color "$EVIDENCE/alb.tfplan" > "$EVIDENCE/alb.plan.txt"
    sha256sum main.tf.json .terraform.lock.hcl "$EVIDENCE/alb.tfplan" "$EVIDENCE/alb.plan.txt"

Plan MUST show adds only, no changes/deletes of existing resources; review redacted output + immutable plan hash, credentials never in evidence. Targeting exceptional one-time bootstrap. Successful schema validate not proof of API-level public-list/KMS compatibility.

## Bootstrap existing publisher / rollback drill

After both #1124 and #1125 accepted and merged, produce checked production-root.tar.gz and static_site_build_result.json from exact merged main and normal full pipeline. Don't rebuild from prelaunch, preview or partial sidecar. Retain archive/result hashes and exact manifest identity. In RunCoveer existing env file /opt/runcoveer/events/.env.production (not modified here), configure non-secret mapping from .env.example:

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

Secrets STATIC_SITE_ROOT_YC_ACCESS_KEY_ID / STATIC_SITE_ROOT_YC_SECRET_ACCESS_KEY via existing secret channel; retain existing ENABLE_STATIC_SITE_SECRET_PUBLISH and KENIGEVENTS_SITE_* settings. Root group without listener first. One-time seed using existing functions (not new publisher), both slots newly created, empty, authenticated, no old data. On partial failure retain bytes, explicitly revalidate exact keys/hashes, no blanket delete.

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

Compare actual body and MIME to manifest for all required routes, including /, Today, Search, root/focus manifests and focus invitation, all catalog routes and current event/club details, sitemap/robots/SW, ICS, CDN media and CSS/JS, R7 and real secret review (redact token), actual PWA install standalone Android and identity retention. Test anonymous direct bucket GET and LIST denial (do not print listing body). Browser artifact gate runs in existing checked artifact pipeline; live ALB browser needs canonical hostname local resolver, don't change STATIC_SITE_ROOT_PUBLIC_BASE_URL or certificate verification. Check browser network errors, DOM/link navigation, source asset manifest. Test both old/new browser sessions during switch, no forced old release replay.

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

[Atomic runbook](../../../docs/operations/static-site-atomic-root.md) explicitly requires owner approval for apply/flag/DNS; [release plan](../../../docs/features/static-site-pages/release-plan.md) and [release autotest gates](../../../docs/features/static-site-pages/release-autotest-gates.md) require checked manifest, browser/real PWA and production acceptance including daily blocking data.critical_pipelines_daily (3 consecutive scheduled PASS, failed-refresh isolation, downstream consumption, no-change evidence, 72h). Exact main search found data.critical_pipelines_daily only in docs; no evidence of pass. Older Missing rows may be historical (current atomic publisher exists; full builder exists but related-only profile, PR1124 pending; UI donor not proof of current release, PR1125 pending). No claim all historical Missing rows still missing/resolved. Separate canonical D0/D10 Telegraph/public-link rollout and owner product approval; don't switch existing Telegram/VK URLs. Real OAuth/search/collections/focus testing and public noindex/indexing contract remain owner release plan, not new gates. This draft cannot release, incident remains open.

## Cost (RUB incl VAT, ru-central1, 28 Nov 2026, 720 h)

Official [ALB](https://yandex.cloud/ru/docs/application-load-balancer/pricing), [VPC](https://yandex.cloud/ru/docs/vpc/pricing), [SWS](https://yandex.cloud/ru/docs/smartwebsecurity/pricing), [Storage](https://yandex.cloud/ru/docs/storage/pricing), [KMS](https://yandex.cloud/ru/docs/kms/pricing), [CM](https://yandex.cloud/ru/docs/certificate-manager/pricing), [Logging](https://yandex.cloud/ru/docs/logging/pricing), [Monitoring](https://yandex.cloud/ru/docs/monitoring/pricing), [Monium](https://yandex.cloud/ru/docs/monium/pricing). Rates not account invoice; free quotas may be consumed. No SWS subscription, external-domain proxy, new VM, extra CDN or data duplication. ALB min 2 units/zone at 2.63 per unit-hour, actual hourly peak based on RPS/connections/traffic. Single zone theoretical floor 3,787.20; 2 zones 7,574.40; 3 zones 11,361.60. Active public IPv4 0.26352/h = 189.73; reserved unused 0.6039/h = 434.81. TLS CM 0 additional. SG 0. Basic DDoS L3/L4 charged legitimate incoming GiB; separate VPC SKU price table not accessible in text, request exact quote from calculator, do not substitute SWS domain proxy rate. VPC egress first 100 GB shared free, overage price from actual VPC calculator; storage-to-ALB internal traffic not billable internet; no double counting. Optional KMS new active version 0.00439/h = 3.16, 3.16/10k ops; reuse existing suitable key if possible, no public grants.

Monthly fixed ALB+active IP floors:
- 1 zone 3,976.93 (not HA recommendation);
- 2 zones 7,764.13;
- 3 zones 11,551.33.
If 31 days, multiply hourly components by 744/720; 2 zones 8,022.94. One-time provisioning fee not listed; every active ALB hour bills from creation, so a 2-zone isolated 24h acceptance floor 258.80 plus storage/SWS/traffic/logs/KMS. No tests created paid resources.

SWS PAYG per legitimate request that reaches resource, separate security and WAF processing; first 10k each free at account-level, then 27,450/million per profile through 1m; next 9m 3,843/million per profile; ARL no separate request charge. Full WAF+SWS:
| requests/month both processed | SWS+WAF | 2-zone ALB+IP+SWS/WAF |
|---:|---:|---:|
| 10,000 | 0 | 7,764.13 |
| 100,000 | 4,941.00 | 12,705.13 |
| 1,000,000 | 54,351.00 | 62,115.13 |
| 10,000,000 | 123,525.00 | 131,289.13 |
Dry run still billable for allowed requests; static CDN direct requests excluded only if not routed through ALB. Request count not visitors; HTML + local JS/CSS/manifest + preview traffic and accepted bots. Get actual usage; avoid annual commitment unless owner explicitly chooses.

Storage: standard 2.376/GiB-month, 1 GiB shared free. Example 2 GiB live trees each plus 6 GiB retained old versions = 10 GiB -> 21.38 if free available, 23.76 if not. Size not measured; no deleting versions. First 10k PUT/POST/PATCH/LIST and first 100k GET/HEAD/OPTIONS free; GET/HEAD/OPTIONS overage 0.46/10k, e.g. 1.5m total GET/HEAD -> 64.40 with quota. PUT/LIST overage numeric tariff not exposed in text view; exact current SKU calculator required; estimate formula billable ops/10k * current rate. DELETE free, versions still stored. Storage egress after free 100 GiB through 1 TiB 1.67994/GiB, e.g. 300 GiB direct external readback -> 335.99 with quota; in-cloud internal 0. Publisher reads staged tree twice plus HEAD/list; count normal rebuilds, changed PUTs, verification bytes and version retention, not just visitor traffic. KMS example 100k ops 31.60 + new active version 3.16. Existing static CDN/media/ICS charges unchanged, excluding additional same-origin forwarded traffic. Logging/Monitoring incremental usage separate; existing Cloud Logging rates not accessible in rendered text, Monium if already available 4.40/GiB logs (15 GiB => 66), 1.5/1000 alert-hours (4 single alerts*720h => 4.32), automatic YC resource metric writes 0. No claim those resources already exist. Price quote incomplete for VPC egress/DDoS, PUT/LIST and legacy log volume; obtain calculator SKU rates with actual usage before owner budget decision. Not a fixed full invoice.

## §6 / validation status

User-visible improvement only when checked full candidate reaches stable public root, after paid/topology decision and existing release gates. Added one isolated declarative review file (167 production config lines) + this operational document; no runtime code, old publisher, data or cloud state modified. Deliberately not added: new publisher, single-bucket fallback, new queue/daemon/feature flags, public listing, credentials, DNS resources, destructive lifecycle, paid probes. No checkout or dependency installs on devserver. Syntax JSON round-trip passed; provider schema validation and authenticated plan not yet run (sandbox lacks terraform and network, server has no scratch); no live provisioning acceptance. Crew project/state absolute-path reads denied by typed probe, used exact requirements thread/inventory instead; devserver-artifacts skill path not present in public skill tree, do not claim read. AGENTS home/repo and simplicity/incident skill read. Next action: coordinator review draft, obtain YC compatibility answer on no-list/SSE-KMS/same-origin shared route, inventory remaining IDs and run CI provider validation; owner decides paid topology/contract only once constraints and actual request rates known. No reconstruction complete claim.
