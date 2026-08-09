# Current UI Behavioral Decoder v1.1 — closure integration report

## Result

**Final status:** `READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS`

The Current UI Behavioral Decoder v1.1 evidence gap is closed without any
production UI change. Every one of the 293 breakpoint/container probes has a
terminal browser disposition, the rail semantic contract is reconciled, all
new rasters were inspected at full resolution, and the final package has
durable Actions/Release provenance plus an independent audit PASS.

## Identity

- Integration base after PR #455: `1f449af361e586da509d0199cfe059d620fb42d6`
- Exact pinned UI source: `ef7aa62e45c60f7a12da6160f490719c0721ec03`
- PR: <https://github.com/onedayonemasterpiece/events-bot-new/pull/456>
- Capture workflow: `.github/workflows/current-ui-behavioral-decoder-v1-1.yml`
- Capture head: `14be44b108ab4bd0b20d6dd95a20bcc4250adb95`
- Review materializer head audited: `44606917fb399479f3dd9b525a48edf62e9da5b6`
- Independent audit commit: `26697c4164ec67b804b66ee89f7b459dfbc34e76`
- Prior reviewed supplement manifest SHA-256:
  `c6c62cee8bea4e9440ff85bc75c46bc85cf5abf3e2fdcd4c7357c6ece916436f`
- Immutable Decoder v1 tree:
  `e77fc2457fadfdffb46ed2d90304ebb91e89a715`

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Rail Home/End semantics and keyboard evidence | Done | Ordinary focusable overflow list, not a composite. Home/End no-op is non-required/nonblocking. Fresh packet records Tab/Shift+Tab, visible rail/Like focus, Like Space/Enter, Arrow boundaries, link skip, and drag-only negative feedback. |
| R02 | 293 real-browser breakpoint/container probes | Done | 293/293 terminal: 236 PASS, 39 MISMATCH, 18 UNREACHABLE_WITH_REASON; 32 source paths, 272 media, 21 container, 273 numeric, 20 nonnumeric, zero planned. |
| R03 | Reconcile ledgers/manifests/receipt | Done | Matrix/probe/automation are 1:1; 294 automation rows; 134 observations/page verifications/reviews; 87 unique findings and zero readiness blockers. |
| R04 | Verify published research synthesis | Done | `lovekgd-design-system` main `f9cb3c931d6f2200f0a4221f5130b3a6299f7005`; direct R-07 path and research README entry verified. R-01…R-06 unchanged. |
| R05 | Independent final audit | Done | `INDEPENDENT_AUDIT.md`: PASS; no Critical/High/Medium findings. |

## Actions and durable evidence

- Successful capture:
  <https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31327863197>
- Actions artifact `9042097413`:
  - name `current-ui-behavioral-decoder-v1-1-closure-31327863197`
  - 3,015,654 bytes
  - SHA-256 `8bb8712effaa0ba3b08a672a784d9e1b90d876c6ca6d039a417bfc0617723523`
- Permanent Release:
  <https://github.com/onedayonemasterpiece/events-bot-new/releases/tag/current-ui-behavioral-decoder-v1-1-closure-run-31327863197>
- Raw capture asset `507763470`: same bytes and SHA-256 as the Actions artifact.
- Reviewed compact supplement asset `507776086`:
  - 1,511,707 bytes
  - SHA-256 `cd89640e1d808a293fda8704c7294160a1d918009b3b21fa3562fde66900f4ab`
- Durable receipt asset `507776555`:
  - 2,899 bytes
  - SHA-256 `4f292848430683aaff537567a1307cdd4a4a8dfe6d62931794d1c9ba4405e3e7`
- Final compact hashes:
  - manifest `c676be4f2ad956b8a58c7707c8f71b7bb33afd771e506457309597e76d67d9a1`
  - receipt `d981ad23280dd177d1fef8a59674fe754c5887c76a0981cd722a59c604780d9f`
  - manual visual ledger `8dafd73a26c14aa6229fdd9d25eb82f14e8639cd47245666ee0cbe792a5e6864`

The first Actions attempt `31327604777` failed closed because Playwright's
internal screenshot path waited on an optional remote font after the CSSOM
probe was already terminal. The decoder now uses its existing bounded
fallback-font capture contract; regression coverage was added, and the second
exact run succeeded.

## Full-resolution review

Ten new PNGs from the successful Actions artifact were opened individually at
original resolution:

- 8 bounded breakpoint mismatch signatures;
- rail root focus;
- Like focus after the observed sequential-link skip.

Sparse/root-fallback images were retained and described as truthful mismatch
evidence; none was relabelled PASS. The new review ledger has ten exact
path/SHA rows and is incorporated into the 134-row canonical ledger.

## Research synthesis publication

- Design main: `f9cb3c931d6f2200f0a4221f5130b3a6299f7005`
- PR: <https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/28>
- Direct path:
  `docs/research/ui-normalization-2026-08/07-cross-research-synthesis-and-adoption.md`
- R-07 SHA-256:
  `cc1997ec4ab024a6fcba3e9b6d5c7632e0a367ed15b80ea2347e4f5bac01d944`
- Direct research index:
  `docs/research/ui-normalization-2026-08/README.md`

No design-system repository edit was required by this closure.

## Validation

- strict final `closure-validate.mjs`: PASS
- independent post-binding `POST_BINDING_AUDIT.md`: PASS
- terminal records: 293/293
- matrix/probe/automation referential integrity: PASS
- 134/134 visual review rows: PASS
- ZIP archive integrity: PASS
- secret scan: 76 text evidence files, 0 matches
- PR CI: Python, static browser release gate, facts/contract gate PASS
- production `site/src` / `site/public` diff: zero files
- all extracted decisions: `NOT_MERGED`

## Strict STOP

No rail implementation was fixed. No component was merged, split, deleted, or
defragmented. No experiment winner was selected. No token or Penpot resource
was created. No normalization was started. The final status authorizes only the
next project-level normalization synthesis discussion.
