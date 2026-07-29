# Lane results: outro_scene

- **Status:** Done
- **Requirement IDs:** R05, R06
- **Branch:** `agent/autopresenter-continuous-outro/outro-scene`
- **Worktree:** `/home/dev/projects/events-bot-new-autopresenter-outro-scene`
- **Base SHA:** `49115f6cdcb5fc6c2ccdbf4a6533f6c7c6aca171`
- **Implementation head SHA:** `16652bf0ff43f4f783da1b2684f27786fdb43461`
- **Effort:** high

`Implementation head SHA` is the reviewed implementation commit. This results
record is added by a follow-up evidence-only commit; its final SHA is reported
to the integrator.

## Requirement outcome

| Requirement | Status | Evidence |
|---|---|---|
| R05 | Done | The presenter stage keeps `live-site` as its default and switches to one explicit `outro-qr` scene only through `presenter:scene` with `detail.id`. The outro is a genuine fullscreen scene, while its hidden live iframe remains the existing real mobile site. |
| R06 | Done | The scene uses the exact immutable CDN survey asset, intrinsic `1155×1155` dimensions, eager loading plus image preload, reserved square geometry, large scan size, premium dark/coral typography and transform/opacity entrance motion with reduced-motion handling. |

## Delivered

- Added the concise Russian presentation thought `Как вам?` and a restrained
  one-line evaluation prompt.
- Added one strong fullscreen composition with a large QR, dark brand field and
  coral accent. The outro contains no phone frame, status card, dashboard
  language, instructions or explanatory side panel.
- Added an interruptible, re-triggerable scene switch that changes only between
  `live-site` and `outro-qr`; it is not an editor, DSL or queue abstraction.
- Added the standalone `outro-contract.mjs` with scene IDs, immutable asset
  metadata, visual acceptance rules and strict scene-ID resolution.
- Added focused stage/source and contract tests.

## Validation evidence

```text
node --test \
  site/tests/presenter-stage.test.mjs \
  tools/autopresenter/agent/test/outro-contract.test.mjs
# PASS: 6/6

npm test --prefix tools/autopresenter/agent
# PASS: 20/20

git diff --check
# PASS
```

CDN verification on 2026-07-29:

```text
curl -fsSI <immutable CDN URL>
# HTTP/2 200
# content-type: image/png
# cache-control: public, max-age=31536000, immutable

curl -fsS <immutable CDN URL> | sha256sum
# 916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9

curl -fsS <immutable CDN URL> | file -
# PNG image data, 1155 x 1155, 8-bit/color RGB, non-interlaced
```

Astro syntax/build validation was attempted but this isolated worktree has no
site dependency tree. `astro check` requested interactive installation of
`@astrojs/check`/TypeScript, and `npm run build --prefix site` stopped before
compilation because `astro/config` could not be resolved. No dependency files
were changed and no install was attempted inside the bounded lane.

## Changed files

- `site/src/pages/internal/presenter-stage/index.astro`
- `site/tests/presenter-stage.test.mjs`
- `tools/autopresenter/agent/outro-contract.mjs`
- `tools/autopresenter/agent/test/outro-contract.test.mjs`
- `.codex/lanes/outro_scene/RESULTS.md` (lane evidence only)

## Risks / integration notes

- The integration owner must bridge the runtime/control selector to dispatch
  `new CustomEvent('presenter:scene', { detail: { id: 'outro-qr' } })`; those
  files were explicitly forbidden in this lane.
- Integration must run the Astro build and final 1920×1080 browser/screenshot
  acceptance with installed site dependencies.
- Canonical documentation and `CHANGELOG.md` were forbidden here and remain an
  integration completion gate.
- The user-owned scenario source was read only and was not edited.
