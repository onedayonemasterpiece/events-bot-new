# Product Atlas — stable Penpot delivery

> **Status:** public delivery verified; real Penpot host import remains a separate acceptance gate.  
> **Published:** 2026-08-07.  
> **GitHub Actions run:** `31215123101`.

## Stable installation URL

`https://static.kenigevents.ru/plugins/product-atlas/manifest.json`

This project-owned URL is the durable installation entry point. The manifest is mutable only as a version pointer; it references an immutable release directory.

## Current immutable release

- Release ID: `20260807-d87e6d55`.
- Source: `onedayonemasterpiece/lovekgd-design-system@d87e6d55181242867c05b7fa9cc233f247215162`.
- Release base: `https://static.kenigevents.ru/plugins/product-atlas/releases/20260807-d87e6d55/`.
- Manifest SHA-256: `5a2c3317deac5fe57693faa0d854103d81f33d48e4588342f6c48d5c897166b0`.
- Plugin SHA-256: `9ce3540a59a0cc710a2920a2802b1e2ca168c1e6070d5161226689ff483d14b8`.
- UI SHA-256: `fa2f02d5ea22aa824215ba435ee2d9668633ba38b94fcd05aeaf8ad83a29e990`.
- Catalog SHA-256: `19fb5816825bfbbc088d66af8c2910584ae9a3f9404ebbbcddea5e83f8f2e9eb`.
- Manifest schema version: `2`.
- Third-party CDN dependency in plugin/UI/catalog delivery: none.
- Exact public HTTP body, MIME and CORS verification: PASS.

## Acceptance boundary

Publication does not claim a real Penpot host import. Host acceptance must still prove initial import, a zero-change second preflight, comment preservation, systemic prompt construction and both wrong-file guards.
