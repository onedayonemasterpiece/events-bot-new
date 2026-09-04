# Independent V0 — product routes only

Continue the independent V0 role; do not become an R0/source implementer and do not delegate to Codex/DevCoveer.

Audit exact immutable target:

- hub: `https://kenigevents.ru/preview-real-5862a4ec6-normalized-20260904-v1/__preview/`
- manifest: `https://kenigevents.ru/preview-real-5862a4ec6-normalized-20260904-v1/preview-build.json`
- expected SHA: `5862a4ec6728359548e6c4af76f97f1d9b94fb27`

Fresh-read #621 from comments `5542976599` and `5542976781`. Verify the manifest first. Discover the exact 18 owner product-archetype links from the hub, then audit the **targets**, not the hub design. Explicitly exclude `/lab/**`, the `/__preview/` shell, 404/500 and internal catalog/demo pages from product verdict.

Using a callable browser, personally navigate every product archetype at 390×844 and 1440×900. Capture DOM/accessibility, computed styles and box geometry. Check:

- HTTP/render/runtime errors and horizontal document overflow;
- responsive shell/header/footer seams;
- page/section/card/body/metadata typography consistency as semantic mobile/desktop roles, not merely literal duplication;
- canonical semantic icon identity and small size vocabulary;
- share/like/save/calendar icon-only, icon+label and icon+counter composites, accessible names and ≥44px targets;
- EventCard server and hydrated/client-created forms, order and grid/rail remainders;
- MediaFrame loaded/no-source/broken ownership and clipping;
- Date versus Weekend and Popular Large/Compact distinctions;
- event-detail desktop/mobile actions without turning one local defect into blanket failure.

Publish a concise independent `[VERDICT] V0 PRODUCT ROUTES` comment to #621 containing exact SHA/URL, 36 viewport result count, PASS/DRIFT per Foundations, Typography, Icons/Action composites, MediaFrame, EventCard/Grid and Shell/Routes, and every defect with exact route, viewport, selector, computed/box evidence and screenshot reference. Do not report lab-page drift as product drift. Do not change repository source.

After the verdict, fresh-read the current trunk/result. If a newer exact full real Preview is already published, audit it in the same run; otherwise stop cleanly after the factual verdict.
