import { constants, cpSync, existsSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { isAbsolute, relative, resolve, join, parse } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { loadPreviewEventCatalog, resolvePreviewEventFixture } from './fixtures.mjs';
import { buildSpecimenRegistry } from './registry.mjs';
import { assertSpecimenRegistry, stableHash } from './validate.mjs';

function safeJson(value) { return JSON.stringify(value).replaceAll('<', '\\u003c'); }
function componentImport(renderer) {
  return ({
    button: 'components/design-system/Button.astro', 'focus-egg': 'components/FocusEggArtifact.astro',
    amber: 'components/listings/AmberRailArtifact.astro', 'artifact-collection': 'components/artifacts/ArtifactCollection.astro',
    rail: 'components/EventTransportSchedule.astro', kaup: 'components/KaupTransportSchedule.astro',
    medallions: 'components/EventTokenMedallions.astro', 'event-hero': 'components/EventHero.astro',
    'event-media-rail': 'components/EventMediaRail.astro',
  })[renderer];
}
function renderExpression(row) {
  const props = row.props || {};
  if (row.renderer === 'button') return `<Component variant=${safeJson(props.variant)} size=${safeJson(props.size)} state=${safeJson(props.state)}>Evidence action</Component>`;
  if (row.renderer === 'focus-egg') return `<Component eggId=${safeJson(props.eggId)} title=${safeJson(props.title)} state=${safeJson(props.state)} compact={${Boolean(props.compact)}} />`;
  if (row.renderer === 'amber') return `<Component eventId={${Number(props.eventId)}} placement=${safeJson(props.placement)} />`;
  if (row.renderer === 'artifact-collection') return '<Component />';
  if (row.renderer === 'kaup') return `<Component event={event} compact={${Boolean(props.compact)}} />`;
  if (row.renderer === 'medallions') return `<Component event={event} layout=${safeJson(props.layout)} allowTopSlot={${Boolean(props.allowTopSlot)}} />`;
  if (row.renderer === 'event-media-rail') return `<Component assets={event.image_assets || []} galleryId="controlled-event-media" eventTitle={event.title} maxVisible={${Number(props.maxVisible)}} />`;
  return '<Component event={event} />';
}

export function renderSpecimenPage(row, fixture = null) {
  const source = componentImport(row.renderer);
  if (!source) throw new Error(`Unsupported specimen renderer: ${row.renderer}`);
  const eventLine = fixture ? `const event = ${safeJson(fixture.event)};` : '';
  return `---
import '../../../upstream/styles/design-system.css';
import Component from '../../../upstream/${source}';
${eventLine}
const trace = ${safeJson({ plan_id: row.id, source_paths: row.source_paths, fixture_trace: fixture?.trace || null })};
---
<!doctype html><html lang="ru"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width"/><title>${row.id}</title></head>
<body data-specimen-id=${safeJson(row.id)}><main data-specimen-root data-specimen-trace={JSON.stringify(trace)} style=${safeJson(`width:${row.container.width}px;max-width:100%;min-height:1px;margin:24px auto`)}>
${renderExpression(row)}
</main></body></html>
<style is:global>html{background:#ece8e1;color:#241c17;font-family:var(--ke-font-sans,system-ui)}body{margin:0}.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}</style>
`;
}

function reflinkCopy(source, target) {
  const command = spawnSync('cp', ['-a', '--reflink=auto', source, target], { encoding: 'utf8' });
  if (command.status === 0) return;
  cpSync(source, target, { recursive: true, mode: constants.COPYFILE_FICLONE });
}

export function materializeSpecimenHarness({ candidateSite, harnessRoot, nodeModules = null, registry = buildSpecimenRegistry() }) {
  assertSpecimenRegistry(registry);
  const site = resolve(candidateSite); const root = resolve(harnessRoot);
  if (!existsSync(join(site, 'src/data/preview-events.json'))) throw new Error('candidateSite is not an exact static-site source tree');
  const siteFromRoot = relative(root, site);
  if (root === parse(root).root || root.length < 12 || siteFromRoot === '' || (!siteFromRoot.startsWith('..') && !isAbsolute(siteFromRoot))) {
    throw new Error('Harness root must be a specific disposable path outside candidate source');
  }
  rmSync(root, { recursive: true, force: true }); mkdirSync(join(root, 'src/pages/specimens'), { recursive: true });
  reflinkCopy(join(site, 'src'), join(root, 'upstream'));
  const modules = resolve(nodeModules || join(site, 'node_modules'));
  if (!existsSync(modules)) throw new Error('Exact candidate node_modules is missing');
  symlinkSync(modules, join(root, 'node_modules'), 'dir');
  symlinkSync(join(site, 'public'), join(root, 'public'), 'dir');
  writeFileSync(join(root, 'package.json'), `${JSON.stringify({ name: 'current-ui-decoder-specimens', private: true, type: 'module', scripts: { build: 'astro build' } }, null, 2)}\n`);
  writeFileSync(join(root, 'astro.config.mjs'), "import { defineConfig } from 'astro/config';\nexport default defineConfig({ output:'static', trailingSlash:'always', vite:{ server:{ fs:{ strict:false } } } });\n");
  const catalog = loadPreviewEventCatalog(site); const pages = [];
  for (const row of registry.controlled_specimens) {
    const fixture = row.fixture_ref ? resolvePreviewEventFixture(catalog, row.fixture_ref, row.fixture_delta) : null;
    const target = join(root, 'src/pages/specimens', `${row.id}.astro`);
    const content = renderSpecimenPage(row, fixture); writeFileSync(target, content); pages.push({ id: row.id, path: `src/pages/specimens/${row.id}.astro`, sha256: stableHash(content) });
  }
  const receipt = {
    schema_version: registry.schema_version, status: 'materialized-not-built', candidate_sha: registry.pinned_candidate_sha,
    source_copy_mode: 'exact-src-reflink-or-copy', source_symlinked: false, generated_wrapper_count: pages.length,
    production_source_mutated: false, normalization_allowed: false, pages,
  };
  writeFileSync(join(root, 'specimen-materialization-receipt.json'), `${JSON.stringify(receipt, null, 2)}\n`);
  return receipt;
}

export function buildSpecimenHarness({ harnessRoot, env = {} }) {
  const root = resolve(harnessRoot); const astro = join(root, 'node_modules/astro/bin/astro.mjs');
  if (!existsSync(astro)) throw new Error('Astro CLI missing from exact node_modules');
  const result = spawnSync(process.execPath, [astro, 'build'], {
    cwd: root, encoding: 'utf8', env: { ...process.env, PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE: 'off', ...env },
  });
  return { ok: result.status === 0, status: result.status, stdout_tail: result.stdout.slice(-4000), stderr_tail: result.stderr.slice(-4000), dist: join(root, 'dist') };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [candidateSite, harnessRoot, nodeModules] = process.argv.slice(2);
  const receipt = materializeSpecimenHarness({ candidateSite, harnessRoot, nodeModules });
  const build = buildSpecimenHarness({ harnessRoot });
  process.stdout.write(`${JSON.stringify({ receipt, build }, null, 2)}\n`);
  if (!build.ok) process.exitCode = 1;
}
