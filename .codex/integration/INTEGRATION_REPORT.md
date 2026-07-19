# Integration report — static-event-v12-fidelity-idempotency

## Integrated scope

| Requirement | Status | Integration evidence |
|---|---|---|
| R01 | Done in source | Exact Telegram `261–264` A/B/C hierarchy, arm-specific last-mile/return copy, shared icons, `на Кауп`; one secret-only noindex QA route survives elapsed event `4671`. |
| R02 | Done in source | Unicode alias boundaries, evidence-ranked one-venue ceiling and fail-closed conflicts; `6796` resolves only KAUP and `5295` resolves no disputed venue mark. |
| R03 | Done in source | Footer uses canonical inline wide-`о` wordmark in `Понравились Анонсы? Поделитесь`. |
| R04 | Done locally; awaiting public candidate | One Europe/Kaliningrad clock across scheduler/export/runner/receipt; boundary tests pass. Fresh-snapshot local candidate `/segodnya/` is 18 July; a 17–19 July ongoing event remains correctly visible. |
| R05 | Done in source | Durable claim/history, canonical public fingerprint/no-op, one follow-up, operator force, remote liveness and exact completed-orphan adoption before any replacement Kaggle push. |
| R06 | Done in source | Ticket/telephone variants keep a three-control bottom row; committed Playwright CLI geometry gate covers `6551/5374` at `1536×864`. |
| R07 | Done after incident correction | Supplied typed fallback art is presentation-only. Incorrect independent repair of `6774` was reversed per known `6774→2884`: survivor/source/public surfaces reconciled and duplicate tombstoned. |
| R08 | Done locally; awaiting release refresh | Read-only reason/type inventory uses Kaliningrad local day. Fresh post-repair snapshot has exactly two `no_ledger` rows: `5663` (concert) and `6890` (meeting); both receive typed presentation-only art. |

## Integration decisions

- Accepted transport screenshots are a normative design-system reference, not
  an inspiration board. Shared primitives may not collapse treatment-specific
  content.
- The safe secret-candidate QA route is allowlisted exactly; all other lab
  routes remain forbidden and it stays absent from sitemap/indexing.
- Build idempotency is server-side and crash-safe. A local file lock or stale
  callback alone cannot authorize another fixed-kernel push.
- Known incident duplicate mappings are mandatory context for event-local
  semantic repair. Source-fact correctness alone is not identity correctness.
- Fallback artwork is presentation, never canonical event media.

## Verification completed before release

- `pytest -q tests/test_static_site_*.py tests/test_static_no_image_inventory.py tests/test_smart_update_merge_identity_gate.py`: `78 passed`.
- All static-site Node behavior suites against `preview-v12-final`: `27 passed`; content/media suite: `6 passed`; `check:preview` passed for `303` events and the preview build produced `379` pages.
- Fresh production-snapshot contract build: `292` event pages / `1062` files; secret candidate: `1063` files with exactly one allowlisted noindex lab route.
- Playwright: both CTA specimens passed the committed `1536×864` geometry gate; transport A/B/C each selected exactly one visible forced arm with no horizontal overflow at `1536×864` and `390×844`. Six element screenshots reproduce the accepted hierarchy/copy.
- Local candidate checks: `6796` emits only structured KAUP evidence and no MMO text; `2884` has the verified Cathedral/Kant identity and image; fallback assets are `concert-symphonic.webp` for `5663` and `lecture-meeting.webp` for `6890`; footer exposes the branded `Понравились Анонсы? Поделитесь` prompt.
- Production duplicate repair: `PRAGMA quick_check=ok`; survivor `2884`
  active/canonical, duplicate `6774` merged/silent; Telegram duplicate `2152`
  absent, rich survivor `2531` present; authenticated VK duplicate `7717`
  `is_deleted=true`, survivor `7412` present; duplicate Telegraph redirects.
- External consultation: `a-opus` was blocked by individual quota and is not
  represented as completed. Gemini 3.1 Pro (High) completed the UI and
  idempotency reviews and endorsed exact visual reproduction plus server-side
  claim/fingerprint/adoption boundaries.

## Remaining release gates

- main-reachable commit, production deploy/health/schema verification;
- fresh current-date production-Kaggle secret candidate, public checks and
  refreshed no-image inventory;
- Telegram review-topic link handoff/readback and user visual acceptance.
