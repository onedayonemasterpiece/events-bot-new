#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import {
  cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync,
  realpathSync, rmSync, statSync, writeFileSync,
} from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { spawn, spawnSync } from 'node:child_process';
import {
  FOCUSED_PREVIEW_SUPPORT_ROUTES,
  normalizeStaticSitePageClasses,
  pageClassForComponent,
  STATIC_SITE_PAGE_CLASSES,
} from './page-class-build-filter.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(scriptDir, '..');
const repoRoot = resolve(siteDir, '..');
const DEFAULT_LIMIT = 24;

function usage() {
  return `Canonical local focused static-site workflow\n\nUsage:\n  npm --prefix site run local:focused -- --route /segodnya/ [options]\n  npm --prefix site run local:focused -- --page-class date [options]\n\nSelection:\n  --route <route>            Exact owner route; materializes only it plus required companions\n  --page-class <class>       One canonical class-wide slice; never aliases --route\n\nData:\n  --fixture                  Use committed deterministic site/src/data (default without --db)\n  --db <events.sqlite>       Use canonical production-preview exporter\n  --entity-id <id>           Exact entity id for a real event-detail route\n  --entity-slug <slug>       Validate event route slug\n  --limit <n>                Canonical exporter limit (default ${DEFAULT_LIMIT})\n  --current-date <YYYY-MM-DD>\n  --current-datetime <ISO>\n  --skip-image-probes\n\nExecution:\n  --build-id <preview-...>\n  --output-root <dir>        Owned output root; defaults outside repository\n  --result-json <path>       Optional second copy of receipt\n  --no-smoke                 Skip Playwright smoke\n  --offline                  Block third-party requests during smoke\n  --no-serve                 Exit after checks (CI)\n  --open                     Open local URL while serving\n  --help\n`;
}

export function parseLocalFocusedArgs(argv) {
  const options = {
    route: null, pageClass: null, fixture: false, db: null, entityId: null,
    entitySlug: null, limit: DEFAULT_LIMIT, currentDate: null, currentDatetime: null,
    skipImageProbes: false, buildId: null, outputRoot: null, resultJson: null,
    smoke: true, serve: true, open: false, offline: false, help: false,
  };
  const values = new Map([
    ['--route', 'route'], ['--page-class', 'pageClass'], ['--db', 'db'],
    ['--entity-id', 'entityId'], ['--entity-slug', 'entitySlug'], ['--limit', 'limit'],
    ['--current-date', 'currentDate'], ['--current-datetime', 'currentDatetime'],
    ['--build-id', 'buildId'], ['--output-root', 'outputRoot'], ['--result-json', 'resultJson'],
  ]);
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (values.has(token)) {
      const value = argv[++i];
      if (!value || value.startsWith('--')) throw new Error(`${token} requires a value`);
      options[values.get(token)] = value;
    } else if (token === '--fixture') options.fixture = true;
    else if (token === '--skip-image-probes') options.skipImageProbes = true;
    else if (token === '--no-smoke') options.smoke = false;
    else if (token === '--offline') options.offline = true;
    else if (token === '--no-serve') options.serve = false;
    else if (token === '--open') { options.open = true; options.serve = true; }
    else if (token === '--help' || token === '-h') options.help = true;
    else throw new Error(`Unknown argument: ${token}`);
  }
  options.limit = Number(options.limit);
  if (!Number.isInteger(options.limit) || options.limit < 1 || options.limit > 300) {
    throw new Error('--limit must be an integer from 1 to 300');
  }
  if (options.entityId !== null) {
    options.entityId = Number(options.entityId);
    if (!Number.isInteger(options.entityId) || options.entityId < 1) {
      throw new Error('--entity-id must be a positive integer');
    }
  }
  if (options.route && options.pageClass) throw new Error('--route and --page-class are mutually exclusive');
  if (!options.route && !options.pageClass && !options.help) throw new Error('Provide --route or --page-class');
  if (options.db && options.fixture) throw new Error('--db and --fixture are mutually exclusive');
  if (!options.db) options.fixture = true;
  return options;
}

export function normalizeOwnerRoute(value) {
  let route = String(value || '').trim();
  if (!route.startsWith('/')) route = `/${route}`;
  route = route.replace(/\/+/gu, '/');
  if (!route.endsWith('/')) route += '/';
  if (!route || route.includes('..') || route.includes('?') || route.includes('#')) {
    throw new Error(`Route must be a clean pathname: ${value}`);
  }
  return route;
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

export function astroPageSourceToRoute(sourcePath) {
  const normalized = sourcePath.replaceAll('\\', '/');
  if (!normalized.startsWith('src/pages/') || !normalized.endsWith('.astro')) return null;
  const relativePage = normalized.slice('src/pages/'.length, -'.astro'.length);
  const segments = relativePage.split('/');
  if (segments.at(-1) === 'index') segments.pop();
  let dynamicCount = 0;
  let catchAllCount = 0;
  let staticCharacters = 0;
  const regexSegments = segments.filter(Boolean).map((segment) => {
    let cursor = 0;
    let pattern = '';
    for (const match of segment.matchAll(/\[(\.\.\.)?([^\]]+)\]/gu)) {
      const prefix = segment.slice(cursor, match.index);
      pattern += escapeRegex(prefix);
      staticCharacters += prefix.length;
      dynamicCount += 1;
      if (match[1]) { catchAllCount += 1; pattern += '.+'; } else pattern += '[^/]+';
      cursor = match.index + match[0].length;
    }
    const tail = segment.slice(cursor);
    staticCharacters += tail.length;
    return pattern + escapeRegex(tail);
  });
  return {
    sourcePath: normalized,
    regex: new RegExp(`^/${regexSegments.join('/')}${regexSegments.length ? '/' : ''}$`, 'u'),
    dynamicCount, catchAllCount, staticCharacters,
  };
}

function walkFiles(root, current = root, out = []) {
  if (!existsSync(current)) return out;
  for (const entry of readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const absolute = join(current, entry.name);
    if (entry.isDirectory()) walkFiles(root, absolute, out);
    else if (entry.isFile()) out.push(absolute);
  }
  return out;
}

export function resolveOwnerRoute(route, pagesRoot = join(siteDir, 'src', 'pages')) {
  const normalized = normalizeOwnerRoute(route);
  const matches = walkFiles(pagesRoot)
    .filter((file) => file.endsWith('.astro'))
    .map((file) => astroPageSourceToRoute(`src/pages/${relative(pagesRoot, file).replaceAll(sep, '/')}`))
    .filter((candidate) => candidate?.regex.test(normalized))
    .sort((a, b) => a.dynamicCount - b.dynamicCount
      || a.catchAllCount - b.catchAllCount
      || b.staticCharacters - a.staticCharacters
      || a.sourcePath.localeCompare(b.sourcePath));
  if (!matches.length) throw new Error(`No Astro owner page matches route ${normalized}`);
  const selected = matches[0];
  const pageClass = pageClassForComponent(selected.sourcePath);
  if (!pageClass || pageClass === 'shell') {
    throw new Error(`Route ${normalized} has no selectable canonical page class`);
  }
  return { ...selected, route: normalized, pageClass };
}

export function resolvePageClass(value) {
  const selected = normalizeStaticSitePageClasses(value || '');
  if (selected.length !== 1 || selected[0] === 'all' || !STATIC_SITE_PAGE_CLASSES.includes(selected[0])) {
    throw new Error(`--page-class must be exactly one canonical class: ${STATIC_SITE_PAGE_CLASSES.join(', ')}`);
  }
  return selected[0];
}

function sha256Bytes(value) {
  return createHash('sha256').update(value).digest('hex');
}
function sha256File(path) {
  return sha256Bytes(readFileSync(path));
}
export function hashDirectory(root) {
  const hash = createHash('sha256');
  for (const file of walkFiles(root)) {
    hash.update(relative(root, file).replaceAll(sep, '/'));
    hash.update('\0');
    hash.update(readFileSync(file));
    hash.update('\0');
  }
  return hash.digest('hex');
}
function git(args, cwd = repoRoot) {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${result.stderr || result.stdout}`);
  return result.stdout.trim();
}
function run(command, args, { cwd = repoRoot, env = process.env, capture = false } = {}) {
  const started = performance.now();
  const result = spawnSync(command, args, {
    cwd, env, encoding: capture ? 'utf8' : undefined, stdio: capture ? 'pipe' : 'inherit',
    shell: process.platform === 'win32',
  });
  const durationMs = Math.round(performance.now() - started);
  if (result.status !== 0) {
    const detail = capture ? `\n${result.stdout || ''}\n${result.stderr || ''}` : '';
    throw new Error(`${command} ${args.join(' ')} failed with ${result.status}${detail}`);
  }
  return { durationMs, stdout: capture ? result.stdout : '' };
}
function ensureCleanSource() {
  if (git(['status', '--porcelain=v1', '--untracked-files=all'])) {
    throw new Error('Local focused builds require a clean committed checkout');
  }
}
function defaultOutputRoot() {
  const cache = process.env.XDG_CACHE_HOME
    || (process.platform === 'win32' ? process.env.LOCALAPPDATA : join(homedir(), '.cache'))
    || tmpdir();
  return join(cache, 'kenigevents', 'local-focused');
}
function fixtureClock(stagedSite) {
  const data = JSON.parse(readFileSync(join(stagedSite, 'src', 'data', 'preview-events.json'), 'utf8'));
  return { currentDate: data?.build?.current_date || null, currentDatetime: data?.build?.generated_at || null };
}
function dateInKaliningrad(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Kaliningrad', year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(now);
  const part = (name) => parts.find((item) => item.type === name)?.value || '';
  return `${part('year')}-${part('month')}-${part('day')}`;
}
function safeBuildId(value) {
  if (!/^preview-[a-zA-Z0-9._-]+$/u.test(value || '') || value.includes('/')) {
    throw new Error(`Invalid build id: ${value || '(empty)'}`);
  }
  return value;
}
function safeSlug(value) {
  return String(value || '').replace(/[^a-zA-Z0-9._-]+/gu, '-').replace(/^-+|-+$/gu, '').slice(0, 28) || 'slice';
}
function installDependencies(stagedSite, smoke) {
  const npm = run('npm', ['ci', '--no-audit', '--no-fund'], { cwd: stagedSite });
  let browser = { durationMs: 0 };
  const configuredBrowser = String(process.env.PLAYWRIGHT_EXECUTABLE_PATH || '').trim();
  if (smoke && !(configuredBrowser && existsSync(configuredBrowser))) {
    browser = run('npm', ['exec', '--', 'playwright', 'install', 'chromium'], { cwd: stagedSite });
  }
  return { npmCiMs: npm.durationMs, playwrightInstallMs: browser.durationMs };
}

export function buildCanonicalExporterArgs({
  stagedSite, db, outputDir, limit, entityId, buildId, sourceSha,
  snapshotIdentity, snapshotSize, currentDate, currentDatetime, pageClass, skipImageProbes,
}) {
  const args = [
    join(stagedSite, 'scripts', 'export-production-preview-data.py'),
    '--db', db, '--output-dir', outputDir, '--catalog-mode', 'slice', '--limit', String(limit),
    '--include-ids', entityId ? String(entityId) : '', '--related-mode', 'sparse',
    '--base-path', `/${buildId}`, '--repo-sha', sourceSha, '--run-id', `local-focused:${buildId}`,
    '--build-id', buildId, '--snapshot-id', `local-db-${snapshotIdentity.slice(0, 16)}`,
    '--snapshot-sha256', snapshotIdentity, '--snapshot-size', String(snapshotSize),
    '--snapshot-frozen-db', db, '--current-date', currentDate, '--current-datetime', currentDatetime,
    '--page-classes', pageClass,
  ];
  if (skipImageProbes) args.push('--skip-image-probes');
  return args;
}

function eventSupportRoutes(dataDir, selectedRoute, explicitId) {
  const match = /^\/sobytiya\/([^/]+)\/$/u.exec(selectedRoute || '');
  if (!match) return [];
  const slug = match[1];
  let id = explicitId;
  if (!id) {
    for (const name of ['preview-events.json', 'preview-event-archive.json']) {
      const path = join(dataDir, name);
      if (!existsSync(path)) continue;
      const parsed = JSON.parse(readFileSync(path, 'utf8'));
      const found = (parsed?.events || []).find((event) => event.slug === slug);
      if (found) { id = Number(found.id); break; }
    }
  }
  if (!Number.isInteger(id) || id < 1) {
    throw new Error(`Cannot resolve canonical event id for focused route ${selectedRoute}`);
  }
  return [`/sobytiya/${slug}/event.ics`, `/data/discovery/${id}.json`];
}

function routeFromFile(root, file) {
  const rel = relative(root, file).replaceAll(sep, '/');
  if (['_astro/', 'assets/', 'service-share/'].some((prefix) => rel.startsWith(prefix))) return null;
  if (rel === 'preview-build.json') return null;
  if (rel.endsWith('/index.html')) return `/${rel.slice(0, -'index.html'.length)}`;
  if (rel === 'index.html') return '/';
  if (rel.endsWith('.html')) return `/${rel.slice(0, -5)}/`;
  if (rel.endsWith('.json') || rel.endsWith('.ics') || rel.endsWith('.txt')) return `/${rel}`;
  return null;
}
export function listProducedPaths(buildRoot) {
  return [...new Set(walkFiles(buildRoot).map((file) => routeFromFile(buildRoot, file)).filter(Boolean))].sort();
}

export function resolvePlaywrightApi(module) {
  const api = module?.chromium ? module : module?.default;
  if (!api?.chromium) throw new Error('Staged Playwright module does not expose chromium');
  return api;
}

export function localFocusedBrowserEpoch(currentDate) {
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(currentDate || '')) {
    throw new Error(`Invalid local focused browser date: ${currentDate || '(empty)'}`);
  }
  const epoch = Date.parse(`${currentDate}T12:00:00+02:00`);
  const calendarProbe = new Date(`${currentDate}T00:00:00Z`);
  if (!Number.isFinite(epoch) || Number.isNaN(calendarProbe.valueOf()) || calendarProbe.toISOString().slice(0, 10) !== currentDate) {
    throw new Error(`Invalid local focused browser date: ${currentDate}`);
  }
  return epoch;
}

async function loadStagedBrowserModules(stagedSite) {
  const require = createRequire(join(stagedSite, 'package.json'));
  const playwrightEntry = require.resolve('playwright');
  const [playwrightModule, releaseGate] = await Promise.all([
    import(`${pathToFileURL(playwrightEntry).href}?focused=${Date.now()}`),
    import(`${pathToFileURL(join(stagedSite, 'scripts', 'check-browser-release-gate.mjs')).href}?focused=${Date.now()}`),
  ]);
  return [resolvePlaywrightApi(playwrightModule), releaseGate];
}

export async function smokeFocusedRoute({ stagedSite, buildRoot, buildId, route, offline, currentDate }) {
  const [{ chromium }, { browserLaunchOptions, startReleaseServer }] = await loadStagedBrowserModules(stagedSite);
  const server = await startReleaseServer(buildRoot, `/${buildId}`);
  const browser = await chromium.launch(browserLaunchOptions(process.env.PLAYWRIGHT_EXECUTABLE_PATH));
  const observations = [];
  const browserEpoch = localFocusedBrowserEpoch(currentDate);
  try {
    for (const viewport of [{ id: 'desktop', width: 1440, height: 900 }, { id: 'mobile', width: 390, height: 844 }]) {
      const context = await browser.newContext({ viewport });
      const page = await context.newPage();
      // Immutable fixtures may intentionally be older than the host clock.
      // Exercise the artifact at its declared build date so /segodnya/ tests
      // the requested owner instead of taking the live stale-date redirect.
      await page.addInitScript((epoch) => {
        const NativeDate = Date;
        class FixedDate extends NativeDate {
          constructor(...args) { super(...(args.length ? args : [epoch])); }
          static now() { return epoch; }
        }
        globalThis.Date = FixedDate;
      }, browserEpoch);
      const failures = [];
      if (offline) {
        await page.route('**/*', async (requestRoute) => {
          const target = new URL(requestRoute.request().url());
          if (target.origin === server.origin) await requestRoute.continue();
          else await requestRoute.abort('blockedbyclient');
        });
      }
      page.on('response', (response) => {
        if (response.url().startsWith(server.origin) && response.status() >= 400) failures.push(`${response.status()} ${response.url()}`);
      });
      page.on('pageerror', (error) => failures.push(`pageerror ${error.message}`));
      const expectedPath = `/${buildId}${route}`.replace(/\/+/gu, '/');
      const response = await page.goto(`${server.origin}${expectedPath}`, { waitUntil: 'domcontentloaded', timeout: 60_000 });
      await page.waitForTimeout(400);
      const geometry = await page.evaluate(() => ({
        pathname: location.pathname,
        h1Count: [...document.querySelectorAll('h1')].filter((node) => {
          const r = node.getBoundingClientRect();
          const s = getComputedStyle(node);
          return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
        }).length,
        main: Boolean(document.querySelector('main')),
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: Math.max(document.documentElement.scrollWidth, document.body?.scrollWidth || 0),
      }));
      const overflow = Math.max(0, geometry.scrollWidth - geometry.clientWidth);
      if (response?.status() !== 200 || geometry.pathname !== expectedPath || !geometry.main || geometry.h1Count < 1 || overflow > 1 || failures.length) {
        throw new Error(`Browser smoke failed: ${JSON.stringify({ viewport, status: response?.status(), geometry, failures })}`);
      }
      observations.push({ viewport: viewport.id, status: 200, pathname: geometry.pathname, horizontalOverflowPx: overflow });
      await context.close();
    }
  } finally {
    await browser.close();
    await server.close();
  }
  return observations;
}

function writeReceipt(outputPath, baseReceipt, resultJson) {
  const payloadFiles = walkFiles(outputPath).map((file) => ({
    path: relative(outputPath, file).replaceAll(sep, '/'),
    sha256: sha256File(file),
    bytes: statSync(file).size,
  }));
  const payloadTreeSha256 = hashDirectory(outputPath);
  const receiptBase = {
    ...baseReceipt,
    objectCount: payloadFiles.length + 1,
    fileCount: payloadFiles.length + 1,
    hashes: { payloadTreeSha256, files: payloadFiles },
  };
  const receiptSha256 = sha256Bytes(`${JSON.stringify(receiptBase)}\n`);
  const receipt = { ...receiptBase, receiptSha256 };
  const receiptPath = join(outputPath, '__preview', 'local-focused-receipt.json');
  mkdirSync(dirname(receiptPath), { recursive: true });
  writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`);
  if (resultJson) {
    const external = resolve(resultJson);
    mkdirSync(dirname(external), { recursive: true });
    writeFileSync(external, `${JSON.stringify(receipt, null, 2)}\n`);
  }
  return { receipt, receiptPath };
}

async function serve(stagedSite, outputPath, buildId, route, shouldOpen) {
  const { startReleaseServer } = await import(`${pathToFileURL(join(stagedSite, 'scripts', 'check-browser-release-gate.mjs')).href}?serve=${Date.now()}`);
  const server = await startReleaseServer(outputPath, `/${buildId}`);
  const url = `${server.origin}/${buildId}${route}`.replace(/([^:]\/)\/+/gu, '$1');
  console.log(`[local-focused] local_url=${url}`);
  if (shouldOpen) {
    const command = process.platform === 'darwin' ? ['open', [url]]
      : process.platform === 'win32' ? ['cmd', ['/c', 'start', '', url]]
        : ['xdg-open', [url]];
    const child = spawn(command[0], command[1], { detached: true, stdio: 'ignore' });
    child.unref();
  }
  await new Promise((done) => {
    const stop = async () => { await server.close(); done(); };
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
  });
}

export async function runLocalFocusedPreview(raw) {
  ensureCleanSource();
  const sourceSha = git(['rev-parse', 'HEAD']);
  const sourceTree = git(['rev-parse', 'HEAD^{tree}']);
  const resolution = raw.route ? resolveOwnerRoute(raw.route) : null;
  const selectedRoute = resolution?.route || null;
  const pageClass = resolution?.pageClass || resolvePageClass(raw.pageClass);
  const eventSlug = /^\/sobytiya\/([^/]+)\/$/u.exec(selectedRoute || '')?.[1] || null;
  if (raw.entitySlug && eventSlug && raw.entitySlug !== eventSlug) throw new Error('--entity-slug does not match --route');
  if (raw.db && eventSlug && !raw.entityId) throw new Error('Real event-detail route requires --entity-id');

  const tempRoot = mkdtempSync(join(tmpdir(), 'kenigevents-local-focused-'));
  const stagedRepo = join(tempRoot, 'repo');
  const durations = {};
  let worktreeAdded = false;
  try {
    durations.stageSource = run('git', ['worktree', 'add', '--detach', stagedRepo, sourceSha], { cwd: repoRoot }).durationMs;
    worktreeAdded = true;
    const stagedSite = join(stagedRepo, 'site');
    const deps = installDependencies(stagedSite, raw.smoke);
    durations.dependencies = deps.npmCiMs;
    durations.playwrightInstall = deps.playwrightInstallMs;

    const clock = raw.fixture ? fixtureClock(stagedSite) : {};
    const currentDate = raw.currentDate || clock.currentDate || dateInKaliningrad();
    const currentDatetime = raw.currentDatetime || clock.currentDatetime || new Date().toISOString();
    const dataDir = join(stagedSite, 'src', 'data');
    let snapshotIdentity;
    let snapshotSize;
    let dataIdentity;
    if (raw.fixture) {
      snapshotIdentity = hashDirectory(dataDir);
      snapshotSize = walkFiles(dataDir).reduce((sum, file) => sum + statSync(file).size, 0);
      dataIdentity = snapshotIdentity;
      durations.exportData = 0;
    } else {
      const db = realpathSync(resolve(raw.db));
      snapshotIdentity = sha256File(db);
      snapshotSize = statSync(db).size;
      const buildIdForExport = safeBuildId(raw.buildId || `preview-local-${safeSlug(pageClass)}-${sourceSha.slice(0, 8)}-${Date.now().toString(36)}`);
      const args = buildCanonicalExporterArgs({
        stagedSite, db, outputDir: dataDir, limit: raw.limit, entityId: raw.entityId,
        buildId: buildIdForExport, sourceSha, snapshotIdentity, snapshotSize,
        currentDate, currentDatetime, pageClass, skipImageProbes: raw.skipImageProbes,
      });
      durations.exportData = run('python3', args, { cwd: stagedRepo }).durationMs;
      dataIdentity = hashDirectory(dataDir);
      raw = { ...raw, buildId: buildIdForExport };
    }

    const buildId = safeBuildId(raw.buildId || `preview-local-${safeSlug(pageClass)}-${sourceSha.slice(0, 8)}-${Date.now().toString(36)}`);
    const supportRoutes = selectedRoute ? eventSupportRoutes(dataDir, selectedRoute, raw.entityId) : [];
    const focusedRoutes = selectedRoute
      ? [selectedRoute, ...supportRoutes, '/__preview/', ...FOCUSED_PREVIEW_SUPPORT_ROUTES]
      : [];
    const env = {
      ...process.env,
      PREVIEW_BUILD_ID: buildId,
      PREVIEW_DATA_MODE: 'real',
      STATIC_SITE_REPO_SHA: sourceSha,
      STATIC_SITE_PAGE_CLASSES: pageClass,
      ...(focusedRoutes.length ? { STATIC_SITE_FOCUSED_ROUTES: JSON.stringify(focusedRoutes) } : {}),
      STATIC_SITE_CURRENT_DATE: currentDate,
      STATIC_SITE_CURRENT_DATETIME: currentDatetime,
      STATIC_SITE_SNAPSHOT_ID: raw.fixture ? `fixture-${snapshotIdentity.slice(0, 16)}` : `local-db-${snapshotIdentity.slice(0, 16)}`,
      STATIC_SITE_SNAPSHOT_SHA256: snapshotIdentity,
      STATIC_SITE_SNAPSHOT_SIZE: String(snapshotSize),
      PUBLIC_ASTRO_ASSET_BASE_URL: '',
      PUBLIC_ASSET_BASE_URL: '',
    };
    durations.build = run('npm', ['run', 'build:preview'], { cwd: stagedSite, env }).durationMs;
    durations.checkPreviewSlice = run('npm', ['run', 'check:preview-slice'], { cwd: stagedSite, env }).durationMs;

    const stagedBuildRoot = join(stagedSite, 'dist', buildId);
    const producedPaths = listProducedPaths(stagedBuildRoot);
    if (selectedRoute) {
      const expected = new Set(focusedRoutes);
      const unexpected = producedPaths.filter((route) => !expected.has(route));
      const missing = focusedRoutes.filter((route) => !producedPaths.includes(route));
      if (unexpected.length || missing.length) {
        throw new Error(`Exact route isolation failed: ${JSON.stringify({ missing, unexpected, producedPaths })}`);
      }
    }

    const outputRoot = resolve(raw.outputRoot || defaultOutputRoot());
    const outputPath = join(outputRoot, buildId);
    if (existsSync(outputPath)) throw new Error(`Refusing to replace existing output: ${outputPath}`);
    mkdirSync(outputRoot, { recursive: true });
    const copyStart = performance.now();
    cpSync(stagedBuildRoot, outputPath, { recursive: true, dereference: true });
    durations.copyOutput = Math.round(performance.now() - copyStart);

    const smokeStart = performance.now();
    const browserSmoke = raw.smoke
      ? await smokeFocusedRoute({
        stagedSite, buildRoot: outputPath, buildId,
        route: selectedRoute || '/__preview/', offline: raw.offline, currentDate,
      })
      : [];
    durations.browserSmoke = Math.round(performance.now() - smokeStart);

    const baseReceipt = {
      schema: 'kenigevents.local-focused-preview-result.v2',
      sourceSha, sourceTree, buildId,
      selection: {
        kind: selectedRoute ? 'route' : 'page-class',
        exactRoute: selectedRoute,
        pageClass,
        ownerSource: resolution?.sourcePath || null,
        supportRoutes,
      },
      dataIdentity: {
        mode: raw.fixture ? 'fixture' : 'real-db',
        snapshotSha256: snapshotIdentity,
        snapshotSize,
        generatedSha256: dataIdentity,
        currentDate,
        currentDatetime,
      },
      producedPaths,
      elapsedStagesMs: durations,
      browserSmoke,
    };
    const { receipt, receiptPath } = writeReceipt(outputPath, baseReceipt, raw.resultJson);
    console.log(`[local-focused] source_sha=${sourceSha}`);
    console.log(`[local-focused] page_class=${pageClass}`);
    console.log(`[local-focused] exact_route=${selectedRoute || '(class-wide)'}`);
    console.log(`[local-focused] produced_paths=${JSON.stringify(producedPaths)}`);
    console.log(`[local-focused] object_count=${receipt.objectCount}`);
    console.log(`[local-focused] elapsed_ms=${JSON.stringify(durations)}`);
    console.log(`[local-focused] receipt_sha256=${receipt.receiptSha256}`);
    console.log(`[local-focused] receipt=${receiptPath}`);
    console.log(`LOCAL_FOCUSED_PREVIEW_RESULT=${JSON.stringify(receipt)}`);

    if (raw.serve) await serve(stagedSite, outputPath, buildId, selectedRoute || '/__preview/', raw.open);
    return receipt;
  } finally {
    if (worktreeAdded) spawnSync('git', ['worktree', 'remove', '--force', stagedRepo], { cwd: repoRoot, stdio: 'ignore' });
    rmSync(tempRoot, { recursive: true, force: true });
  }
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  try {
    const options = parseLocalFocusedArgs(process.argv.slice(2));
    if (options.help) console.log(usage());
    else await runLocalFocusedPreview(options);
  } catch (error) {
    console.error(`[local-focused] ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
