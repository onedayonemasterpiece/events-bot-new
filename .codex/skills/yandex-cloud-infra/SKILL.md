---
name: yandex-cloud-infra
description: "Use in events-bot-new for Yandex Cloud infrastructure tasks: Object Storage static website buckets, CDN resources, Certificate Manager certificates, DNS zones, static.kenigevents.ru TLS checks, kenigevents.ru bucket/CDN publishing, and `yc` CLI troubleshooting. Trigger when Codex must inspect or change YC CDN/Object Storage/DNS/cert settings, verify static.kenigevents.ru, or locate the existing local YC CLI profile/token cache without re-authenticating."
---

# Yandex Cloud Infra for KenigEvents

Use the existing local `yc` installation and user-level auth cache. Do not start a new browser auth flow unless the user explicitly asks and the current cache is unusable.

## Local CLI/auth

1. Add CLI to PATH:
   ```bash
   export PATH="/home/dev/yandex-cloud/bin:$PATH"
   ```
2. Existing profile/session location:
   - CLI: `/home/dev/yandex-cloud/bin/yc`
   - User config/cache: `/home/dev/.config/yandex-cloud/`
   - Current profile file: `/home/dev/.config/yandex-cloud/config.yaml`
   - Credential cache includes `/home/dev/.config/yandex-cloud/credentials/artkoder`
3. Never print tokens, refresh tokens, IAM tokens, callback URLs, service-account keys, or raw credential files. Redact output:
   ```bash
   yc config list | sed -E 's/(token|secret|key|Authorization|Cookie)([^[:alnum:]_-]*)([^[:space:]]+)/\1\2<hidden>/Ig'
   ```
4. Verify auth with a real API call:
   ```bash
   yc config profile list
   yc config list | sed -E 's/(token|secret|key)([^[:alnum:]_-]*)([^[:space:]]+)/\1\2<hidden>/Ig'
   yc resource-manager folder list
   ```

Known active user profile on this machine: `artkoder` (`art.koder@yandex.ru`). The profile may default to cloud `b1goifscr17duurhullj`, folder `b1g57eelh8ih40eiqlvj`, but KenigEvents CDN/DNS resources are in a different folder below.

## KenigEvents folders/resources

Always pass explicit folder ids for CDN/DNS checks; do not rely on the active default folder.

- KenigEvents CDN/DNS folder: `b1g5tck18cgqtjb7rn3s` (`cloud-art-koder/default`).
- Static CDN resource: `bc8rani5q2j4yfpl7oge`, CNAME `static.kenigevents.ru`.
- Static CDN provider CNAME observed: `d1dbb74e06652ac7.topology.gslb.yccdn.ru`.
- Static CDN Certificate Manager cert: `fpqs4u9csqlo3qlldgjb`, expected domain `static.kenigevents.ru`.
- KenigEvents DNS zone: `dnsbhbtvj0l1lf8jpefb`, zone `kenigevents.ru.`.
- Static website origin currently used by CDN: `kenigevents.ru.website.yandexcloud.net`.

Useful commands:

```bash
FOLDER=b1g5tck18cgqtjb7rn3s
yc cdn resource list --folder-id "$FOLDER" --format json
yc cdn resource get bc8rani5q2j4yfpl7oge --folder-id "$FOLDER" --format json
yc certificate-manager certificate get fpqs4u9csqlo3qlldgjb --folder-id "$FOLDER" --format json
yc dns zone list-records dnsbhbtvj0l1lf8jpefb --folder-id "$FOLDER" --format json
```

## TLS/CDN acceptance checks

Check both YC control plane and public edge behavior. YC saying `ssl_certificate.status=READY` is not enough; verify the served certificate.

```bash
dig +short CNAME static.kenigevents.ru
curl -I https://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/__preview/
echo | openssl s_client -servername static.kenigevents.ru -connect static.kenigevents.ru:443 2>/dev/null \
  | openssl x509 -noout -subject -issuer -ext subjectAltName
curl -I https://static.kenigevents.ru/ics/5878.ics
```

Expected public TLS certificate must include `static.kenigevents.ru` in SAN. If the edge serves `*.yccdn.cloud.yandex.net`, browser subresources from `https://static.kenigevents.ru/...` will fail strict TLS even when the main `kenigevents.ru` bucket works.


## CDN certificate repair playbook

If YC control plane shows Certificate Manager `ISSUED` and CDN `ssl_certificate.status=READY`, but public `openssl s_client -servername static.kenigevents.ru` still serves `*.yccdn.cloud.yandex.net`, re-apply the certificate binding and purge the resource cache, then re-check public TLS:

```bash
FOLDER=b1g5tck18cgqtjb7rn3s
RESOURCE=bc8rani5q2j4yfpl7oge
CERT=fpqs4u9csqlo3qlldgjb
yc cdn resource update "$RESOURCE" --folder-id "$FOLDER" --active=true --cert-manager-ssl-cert-id "$CERT"
yc cdn cache purge --resource-id "$RESOURCE" --folder-id "$FOLDER" --all
echo | openssl s_client -servername static.kenigevents.ru -connect static.kenigevents.ru:443 2>/dev/null \
  | openssl x509 -noout -subject -issuer -ext subjectAltName
curl -I https://static.kenigevents.ru/ics/5878.ics
```

Acceptance: public certificate subject/SAN includes `static.kenigevents.ru`, and strict `curl` returns `HTTP/2 200` without `-k`. On 2026-07-01 this repair changed the public certificate from the default `*.yccdn.cloud.yandex.net` cert to a Let's Encrypt cert for `static.kenigevents.ru`.

## Safety

- Do not borrow credentials from other projects. Use only `/home/dev/.config/yandex-cloud` for this machine unless the user gives new project-specific credentials.
- Do not commit artifacts from `~/.config/yandex-cloud` or YC logs.
- For resource changes, first capture current resource JSON to `artifacts/codex/<task>/` if the change is non-trivial; do not include secrets.
