#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import {
  cpSync, existsSync, mkdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync,
} from 'node:fs';
import { dirname, isAbsolute, join, parse, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '../..');
const IMPLEMENTED = new Set(['event-detail', 'date-listing', 'today', 'tomorrow', 'weekend', 'favorites']);

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    if (!key.startsWith('--')) throw new Error(`Unexpected argument: ${key}`);
    const value = argv[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`${key} requires a value`);
    args[key.slice(2)] = value;
    index += 1;
  }
  return args;
}

const json = (path) => JSON.parse(readFileSync(path, 'utf8'));
const pad = (value) => String(value).padStart(2, '0');
const isoDate = (date) => `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
const addDays = (value, count) => { const date = new Date(`${value}T00:00:00Z`); date.setUTCDate(date.getUTCDate() + count); return isoDate(date); };

export function isExhibitionLike(event) {
  const type = String(event.event_type || '').trim().toLowerCase();
  if (type === 'выставка' || type === 'экспозиция') return true;
  if (type) return false;
  return (event.topics || []).some((topic) => String(topic).trim().toUpperCase() === 'EXHIBITIONS');
}

function sourceOrder(events) {
  return [...events].sort((a, b) => String(a.starts_at || a.start_date).localeCompare(String(b.starts_at || b.start_date)) || a.id - b.id);
}

// This is an L0 placement oracle only. It mirrors selectors and family collapse; it does not render UI.
export function collapseLinkedSessions(events) {
  const ordered = sourceOrder(events);
  const byId = new Map(ordered.map((event) => [event.id, event]));
  const parent = new Map(ordered.map((event) => [event.id, event.id]));
  const find = (id) => { let root = id; while (parent.get(root) !== root) root = parent.get(root); while (parent.get(id) !== id) { const next = parent.get(id); parent.set(id, root); id = next; } return root; };
  const union = (left, right) => { const a = find(left); const b = find(right); if (a !== b) parent.set(b, a); };
  for (const event of ordered) for (const linked of event.other_date_ids || []) if (byId.has(linked)) union(event.id, linked);
  const seen = new Set();
  return ordered.filter((event) => { const root = find(event.id); if (seen.has(root)) return false; seen.add(root); return true; });
}

export function expectedSurfaceOrder(events, surface, route, clock) {
  const eligible = sourceOrder(events).filter((event) => !isExhibitionLike(event));
  if (surface === 'event-detail') {
    const slug = route.match(/^\/sobytiya\/([^/]+)\/$/u)?.[1];
    return sourceOrder(events).filter((event) => event.slug === slug).map((event) => event.id);
  }
  let selected = [];
  if (surface === 'date-listing') {
    const date = route.match(/^\/date-(\d{4}-\d{2}-\d{2})\/$/u)?.[1];
    selected = eligible.filter((event) => event.start_date === date);
  } else if (surface === 'today') {
    selected = eligible.filter((event) => event.start_date === clock.current_date);
  } else if (surface === 'tomorrow') {
    selected = eligible.filter((event) => event.start_date === addDays(clock.current_date, 1));
  } else if (surface === 'weekend') {
    const now = new Date(`${clock.current_date}T00:00:00Z`);
    const day = now.getUTCDay();
    const saturday = addDays(clock.current_date, day === 0 ? -1 : day === 6 ? 0 : 6 - day);
    const sunday = addDays(saturday, 1);
    selected = eligible.filter((event) => event.start_date >= saturday && event.start_date <= sunday);
  }
  return collapseLinkedSessions(selected).map((event) => event.id);
}

function tagsWithAttribute(html, attribute) {
  const result = [];
  const tagPattern = /<([a-z][\w:-]*)\b[^>]*>/giu;
  for (const match of html.matchAll(tagPattern)) {
    const tag = match[0];
    if (!new RegExp(`\\s${attribute}(?:\\s|=|>|/)`, 'iu').test(tag)) continue;
    const attrs = {};
    for (const attr of tag.matchAll(/\s([:\w-]+)(?:=(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/gu)) attrs[attr[1]] = attr[2] ?? attr[3] ?? attr[4] ?? '';
    result.push(attrs);
  }
  return result;
}

export function extractSurfaceMarkers(html, surface) {
  if (surface === 'event-detail') return tagsWithAttribute(html, 'data-desktop-clean-event').map((attrs) => ({ id: Number(attrs['data-event-id']), component: 'event.detail', state: `surface=${attrs['data-event-surface'] || 'event-detail'};family=${attrs['data-desktop-family'] || 'unknown'}` }));
  return tagsWithAttribute(html, 'data-ds-component')
    .filter((attrs) => attrs['data-ds-component'] === 'ListingEventCard')
    .map((attrs) => ({ id: Number(attrs['data-event-id']), component: 'listing.event-card', state: `density=${attrs['data-listing-density'] || 'unknown'};media=${attrs['data-listing-media-treatment'] || 'unknown'}` }));
}

function extractMobileMarkers(html) {
  return tagsWithAttribute(html, 'data-mobile-listing-row').map((attrs) => ({ id: Number(attrs['data-event-id']), component: 'listing.rail-row', state: `gallery=${attrs['data-mobile-rail-gallery-count'] || '0'}` }));
}

function verifyCorpus(corpusRoot) {
  const corpusPath = join(corpusRoot, 'corpus.json');
  const corpus = json(corpusPath);
  const expectations = json(join(corpusRoot, corpus.surface_expectations_path));
  if (corpus.schema_version !== 'ui-reference-event-corpus.v1' || !corpus.immutable) throw new Error('Corpus must be immutable ui-reference-event-corpus.v1');
  if (expectations.corpus_id !== corpus.corpus_id || JSON.stringify(expectations.reference_clock) !== JSON.stringify(corpus.reference_clock)) throw new Error('Surface expectations/corpus identity or clock mismatch');
  const events = [];
  for (const fixture of corpus.fixtures) {
    const payloadPath = join(corpusRoot, fixture.payload_path);
    const wrapper = json(payloadPath);
    const hashRun = spawnSync('python3', ['-c', 'import json,hashlib,sys; d=json.load(open(sys.argv[1]))[\"preview_event\"]; print(hashlib.sha256((json.dumps(d,ensure_ascii=False,sort_keys=True,separators=(\",\",\":\"))+\"\\n\").encode()).hexdigest())', payloadPath], { encoding: 'utf8' });
    if (hashRun.status !== 0) throw new Error(`Unable to validate frozen payload: ${hashRun.stderr}`);
    const actualHash = hashRun.stdout.trim();
    if (wrapper.fixture_id !== fixture.fixture_id || wrapper.event_id !== fixture.event_id || wrapper.preview_event.id !== fixture.event_id) throw new Error(`Fixture identity mismatch: ${fixture.fixture_id}`);
    if (wrapper.preview_event_sha256 !== fixture.preview_event_sha256 || actualHash !== fixture.preview_event_sha256) throw new Error(`Fixture payload hash mismatch: ${fixture.fixture_id}`);
    events.push(wrapper.preview_event);
  }
  return { corpus, expectations, events };
}

function safeHarness(harness, site) {
  if (!isAbsolute(harness)) throw new Error('Harness path must be absolute');
  const parsed = parse(harness);
  if (harness === parsed.root || harness.length < 16 || harness === site || !relative(site, harness).startsWith('..')) throw new Error('Harness must be a specific disposable path outside candidate site');
}

function materializeHarness(site, harness, events, clock, nodeModules) {
  rmSync(harness, { recursive: true, force: true });
  mkdirSync(harness, { recursive: true });
  cpSync(join(site, 'src'), join(harness, 'src'), { recursive: true });
  cpSync(join(site, 'public'), join(harness, 'public'), { recursive: true });
  const modules = resolve(nodeModules || join(site, 'node_modules'));
  if (!existsSync(modules)) throw new Error(`Exact candidate dependencies missing: ${modules}`);
  symlinkSync(realpathSync(modules), join(harness, 'node_modules'), 'dir');
  writeFileSync(join(harness, 'package.json'), `${JSON.stringify({ name: 'ui-l0-placement-harness', private: true, type: 'module' }, null, 2)}\n`);
  writeFileSync(join(harness, 'astro.config.mjs'), "import { defineConfig } from 'astro/config';\nexport default defineConfig({site:'https://kenigevents.ru',output:'static',trailingSlash:'always'});\n");
  writeFileSync(join(harness, 'src/data/preview-events.json'), `${JSON.stringify({ build: { generated_at: clock.reference_iso, source: 'ui-reference-event-corpus.v1', current_date: clock.current_date, current_datetime: clock.reference_iso, effective_current_date: clock.current_date, catalog_mode: 'golden-corpus-v1', notes: ['frozen L0 placement harness; not production data'] }, events }, null, 2)}\n`);
  writeFileSync(join(harness, 'src/data/preview-event-archive.json'), `${JSON.stringify({ build: { generated_at: clock.reference_iso, source: 'ui-reference-event-corpus.v1', current_date: clock.current_date }, events: [] }, null, 2)}\n`);
}

function buildHarness(site, harness, clock, nodeModules) {
  const astro = join(resolve(nodeModules || join(site, 'node_modules')), 'astro/bin/astro.mjs');
  const build = spawnSync(process.execPath, [astro, 'build'], {
    cwd: harness,
    encoding: 'utf8',
    env: { ...process.env, TZ: clock.timezone, LANG: 'ru_RU.UTF-8', PUBLIC_STATIC_SITE_CURRENT_DATE: clock.current_date, PUBLIC_STATIC_SITE_REFERENCE_ISO: clock.reference_iso, PUBLIC_PRELAUNCH_MODE: '0', PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE: 'off' },
  });
  return { ok: build.status === 0, status: build.status, stdout_tail: build.stdout.slice(-5000), stderr_tail: build.stderr.slice(-5000) };
}

function htmlPath(harness, route) {
  if (route === '/') return join(harness, 'dist/index.html');
  return join(harness, 'dist', route.replace(/^\//u, '').replace(/\/$/u, ''), 'index.html');
}

async function verifyFavorites(site, events, scenario, clock) {
  const module = await import(`${pathToFileURL(join(site, 'src/lib/favorites.mjs')).href}?l0=${Date.now()}`);
  const fixtureIds = scenario.context.saved_event_fixture || [];
  const byFixture = new Map(events.map((event) => [`event.real.${event.id}`, event]));
  const calendarIds = fixtureIds.map((id) => String(byFixture.get(id)?.id || '')).filter(Boolean);
  const refs = module.mergeSavedEventRefs({ remoteRows: scenario.context.cloud_rows || [], calendarIds, likedEventIds: [] });
  const payload = { related_static: events.map((event) => ({ event_id: event.id, candidate: { event_id: event.id, date: event.start_date, title: event.title } })) };
  const joined = module.joinFutureSavedEvents(payload, refs, clock.current_date);
  return joined.map(({ saved, item }) => ({ id: Number(item.event_id), component: 'event.card', state: `surface=favorites;layout=split-actions;source=${saved.source}` }));
}

export async function runPlacementVerification({ corpusRoot, site, harness, output, nodeModules }) {
  corpusRoot = resolve(corpusRoot); site = resolve(site); harness = resolve(harness);
  safeHarness(harness, site);
  const before = spawnSync('git', ['status', '--porcelain', '--', 'site/src'], { cwd: resolve(site, '..'), encoding: 'utf8' }).stdout;
  const sourceSha = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: resolve(site, '..'), encoding: 'utf8' }).stdout.trim();
  const { corpus, expectations, events } = verifyCorpus(corpusRoot);
  materializeHarness(site, harness, events, corpus.reference_clock, nodeModules);
  const build = buildHarness(site, harness, corpus.reference_clock, nodeModules);
  const scenarios = [];
  const routeChecks = new Map();
  if (build.ok) {
    for (const scenario of expectations.scenarios) {
      if (scenario.expected_presence === 'not_implemented') {
        scenarios.push({ scenario_id: scenario.scenario_id, surface_id: scenario.surface_id, status: 'SKIPPED_DECLARED_GAP', reason: scenario.reason });
        continue;
      }
      const fixture = corpus.fixtures.find((row) => row.fixture_id === scenario.fixture_id);
      let markers;
      if (scenario.surface_id === 'favorites') markers = await verifyFavorites(site, events, scenario, corpus.reference_clock);
      else {
        const path = htmlPath(harness, scenario.route);
        const html = existsSync(path) ? readFileSync(path, 'utf8') : '';
        markers = extractSurfaceMarkers(html, scenario.surface_id);
        if (scenario.surface_id !== 'event-detail' && !routeChecks.has(scenario.route)) {
          routeChecks.set(scenario.route, { route: scenario.route, surface_id: scenario.surface_id, expected_ordered_event_ids: expectedSurfaceOrder(events, scenario.surface_id, scenario.route, corpus.reference_clock), actual_desktop_ordered_event_ids: markers.map((row) => row.id), actual_mobile_ordered_event_ids: extractMobileMarkers(html).map((row) => row.id) });
        }
      }
      const found = markers.find((row) => row.id === fixture.event_id);
      const expectedPresent = scenario.expected_presence === 'present';
      const componentOk = !expectedPresent || found?.component === scenario.expected_component;
      const stateOk = !expectedPresent || !scenario.expected_state_key || found?.state?.startsWith(scenario.expected_state_key);
      const ok = Boolean(found) === expectedPresent && componentOk && stateOk;
      scenarios.push({ scenario_id: scenario.scenario_id, surface_id: scenario.surface_id, route: scenario.route, expected_presence: scenario.expected_presence, expected_component: scenario.expected_component, expected_state_key: scenario.expected_state_key, actual_marker: found || null, status: ok ? 'PASS' : 'FAIL' });
    }
  }
  for (const check of routeChecks.values()) check.status = JSON.stringify(check.actual_desktop_ordered_event_ids) === JSON.stringify(check.expected_ordered_event_ids) && JSON.stringify(check.actual_mobile_ordered_event_ids) === JSON.stringify(check.expected_ordered_event_ids) ? 'PASS' : 'FAIL';
  const after = spawnSync('git', ['status', '--porcelain', '--', 'site/src'], { cwd: resolve(site, '..'), encoding: 'utf8' }).stdout;
  const failed = scenarios.filter((row) => row.status === 'FAIL').length + [...routeChecks.values()].filter((row) => row.status === 'FAIL').length;
  const report = { schema_version: 'ui_surface_placement_receipt_v1', status: build.ok && failed === 0 && before === after ? 'PASS' : 'FAIL', corpus_id: corpus.corpus_id, corpus_sha256: corpus.corpus_sha256, frozen_clock: corpus.reference_clock, astro_repository_sha: sourceSha, implemented_surfaces: [...IMPLEMENTED], declared_gap_surfaces: expectations.surface_classes.filter((surface) => !IMPLEMENTED.has(surface)), production_source_mutated: before !== after, build, route_checks: [...routeChecks.values()], scenarios, summary: { pass: scenarios.filter((row) => row.status === 'PASS').length, failed, declared_gaps: scenarios.filter((row) => row.status === 'SKIPPED_DECLARED_GAP').length } };
  if (output) { mkdirSync(dirname(resolve(output)), { recursive: true }); writeFileSync(resolve(output), `${JSON.stringify(report, null, 2)}\n`); }
  return report;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  for (const key of ['corpus-root', 'site', 'harness']) if (!args[key]) throw new Error(`--${key} is required`);
  const report = await runPlacementVerification({ corpusRoot: args['corpus-root'], site: args.site, harness: args.harness, output: args.output, nodeModules: args['node-modules'] });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (report.status !== 'PASS') process.exitCode = 1;
}
