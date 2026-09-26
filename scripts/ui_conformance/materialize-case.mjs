#!/usr/bin/env node
import { constants, cpSync, existsSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, parse, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { renderSpecimenPage } from '../current_ui_resource_graph/v1/specimens/materialize.mjs';

function parseArgs(argv) { const out = {}; for (let i = 0; i < argv.length; i += 2) out[argv[i].replace(/^--/u, '')] = argv[i + 1]; return out; }
const args = parseArgs(process.argv.slice(2));
if (!args.resolved || !args.site || !args.harness) throw new Error('--resolved, --site and --harness are required');
const data = JSON.parse(await (await import('node:fs/promises')).readFile(resolve(args.resolved), 'utf8'));
const site = resolve(args.site); const root = resolve(args.harness); const rel = relative(root, site);
if (root === parse(root).root || root.length < 12 || rel === '') throw new Error('Harness root must be a specific disposable path outside candidate source');
rmSync(root, { recursive: true, force: true }); mkdirSync(join(root, 'src/pages/specimens'), { recursive: true });
const copy = spawnSync('cp', ['-a', '--reflink=auto', join(site, 'src'), join(root, 'upstream')], { encoding: 'utf8' });
if (copy.status !== 0) cpSync(join(site, 'src'), join(root, 'upstream'), { recursive: true, mode: constants.COPYFILE_FICLONE });
const modules = resolve(args['node-modules'] || join(site, 'node_modules')); if (!existsSync(modules)) throw new Error('Exact candidate node_modules is missing');
symlinkSync(modules, join(root, 'node_modules'), 'dir'); symlinkSync(join(site, 'public'), join(root, 'public'), 'dir');
writeFileSync(join(root, 'package.json'), `${JSON.stringify({ name: 'ui-conformance-specimen', private: true, type: 'module', scripts: { build: 'astro build' } }, null, 2)}\n`);
writeFileSync(join(root, 'astro.config.mjs'), "import { defineConfig } from 'astro/config';\nexport default defineConfig({ output:'static', trailingSlash:'always', vite:{ server:{ fs:{ strict:false } } } });\n");
const row = {
  id: data.case_id, renderer: data.component_id === 'core.button' ? 'button' : 'event-card', source_paths: [],
  props: data.resolved_props || {}, root_selector: data.component_id === 'core.button' ? '[data-specimen-root] .ke-button' : '[data-event-card]',
  container: { width: data.viewport.container_width, height: 'auto' }, viewport: { width: data.viewport.width, height: data.viewport.height },
};
const fixture = data.event ? { event: data.event, trace: data.fixture_trace } : null;
const target = join(root, 'src/pages/specimens', `${data.case_id}.astro`); writeFileSync(target, renderSpecimenPage(row, fixture));
const astro = join(root, 'node_modules/astro/bin/astro.mjs'); const build = spawnSync(process.execPath, [astro, 'build'], { cwd: root, encoding: 'utf8', env: { ...process.env, TZ: 'Europe/Kaliningrad', LANG: 'ru_RU.UTF-8' } });
const receipt = { schema_version: 'ui_conformance_specimen_materialization_v1', case_id: data.case_id, resolved_render_case_sha256: data.resolved_render_case_sha256, production_source_mutated: false, source_copy_mode: 'exact-src-reflink-or-copy', route: `/specimens/${data.case_id}/`, root_selector: row.root_selector, build_ok: build.status === 0, stdout_tail: build.stdout.slice(-3000), stderr_tail: build.stderr.slice(-3000) };
writeFileSync(join(root, 'specimen-materialization-receipt.json'), `${JSON.stringify(receipt, null, 2)}\n`); process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`); if (build.status !== 0) process.exitCode = 1;
