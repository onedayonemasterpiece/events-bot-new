import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import {
  createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, statSync,
  writeFileSync,
} from 'node:fs';
import { once } from 'node:events';
import { basename, dirname, extname, join, relative, resolve, sep } from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

export const SCHEMA = 'current_ui_resource_graph_v0';
export const REQUIRED_FILES = [
  'manifest.json', 'summary.md', 'source-components.jsonl',
  'observed-ui-families.jsonl', 'runtime-observations.jsonl',
  'page-families.jsonl', 'desktop-mobile-analysis.jsonl',
  'style-observations.jsonl', 'fragmentation-report.jsonl',
  'candidate-component-graph.jsonl', 'unresolved-questions.md',
  'coverage-report.md', 'screenshots-index.jsonl',
];

export const DEFAULT_IDENTITIES = Object.freeze({
  snapshot_id: 'snapshot-20260808T124842-4786ac53bc',
  snapshot_time: '2026-08-08T12:48:42Z',
  latest_checked_kaggle_candidate: {
    role: 'current_durable_candidate',
    source_sha: 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
    build_id: 'production-secret-20260808T144842-5472a382',
    run_id: 'static-site:production-secret-20260808T144842-5472a382:acbd40ef5203',
    snapshot_id: 'snapshot-20260808T124842-4786ac53bc',
    manifest_sha256: 'd615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322',
    astro_version: '6.4.8',
    node_version: '22.12.0',
    publication: 'immutable_noindex_candidate',
    production_root: false,
    root_promotion_enabled: false,
    manifest_facts: { html_count: 1266, page_count: 3021, file_count: 3323, bytes: 686610720 },
  },
  current_root_prelaunch: {
    role: 'current_public_root',
    source_sha: '5a9d804438377f65fe4b26bd7019e73626529864',
    release_id: 'prelaunch-main-31263560430-1',
    actions_run_id: '31263560430',
    artifact_id: '9023507736',
    published_at: '2026-08-08T15:09:02Z',
    runtime_url: 'https://kenigevents.ru/',
  },
});

export const FAMILY_SEEDS = Object.freeze([
  { id: 'family.event-representations', label: 'Event representations', source: /event(card|list|item|detail|hero|page)/iu, runtime: (x) => x.eventLike > 0 },
  { id: 'family.event-actions', label: 'Event actions', source: /(calendarlink|eventaction|ticket|share)/iu, runtime: (x) => x.actionLike > 0 && x.eventLike > 0 },
  { id: 'family.button-like-actions', label: 'Button-like actions', source: /(button|cta|action|link)/iu, runtime: (x) => x.actionLike > 0 },
  { id: 'family.listing-surfaces', label: 'Listing surfaces', source: /(listing|list|feed|search|calendar)/iu, runtime: (x) => x.listLike > 0 },
  { id: 'family.media-treatments', label: 'Media treatments', source: /(image|media|hero|gallery|poster)/iu, runtime: (x) => x.mediaLike > 0 },
  { id: 'family.medallions', label: 'Medallions', source: /medallion/iu, runtime: (x) => x.medallionLike > 0 },
  { id: 'family.dates', label: 'Dates and occurrence labels', source: /(date|occurrence|calendar)/iu, runtime: (x) => x.timeLike > 0 },
  { id: 'family.transport', label: 'Transport', source: /(transport|bus|rail|route)/iu, runtime: (x) => x.transportLike > 0 },
  { id: 'family.navigation', label: 'Navigation', source: /(nav|header|footer|breadcrumb)/iu, runtime: (x) => x.navLike > 0 },
  { id: 'family.labels-badges', label: 'Labels and badges', source: /(label|badge|chip|tag)/iu, runtime: (x) => x.badgeLike > 0 },
  { id: 'family.breadcrumbs', label: 'Breadcrumbs', source: /breadcrumb/iu, runtime: (x) => x.breadcrumbLike > 0 },
  { id: 'family.async-states', label: 'Async states', source: /(loading|error|empty|skeleton|async)/iu, runtime: (x) => x.asyncLike > 0 },
  { id: 'family.brand', label: 'Brand', source: /(brand|logo|lockup)/iu, runtime: (x) => x.brandLike > 0 },
  { id: 'family.collections', label: 'Collections', source: /(collection|podbork|gastronom)/iu, runtime: (x) => x.collectionLike > 0 },
  { id: 'family.headers', label: 'Headers', source: /header/iu, runtime: (x) => x.navLike > 0 },
  { id: 'family.footers', label: 'Footers', source: /footer/iu, runtime: (x) => x.navLike > 0 },
  { id: 'family.form-controls', label: 'Form controls', source: /(form|input|select|textarea)/iu, runtime: (x) => x.actionLike > 1 },
  { id: 'family.personal-feed', label: 'Personal feed', source: /(PersonalFeed|dlya-menya)/iu, runtime: (x) => x.eventLike > 0 && x.listLike > 0 },
  { id: 'family.search-results', label: 'Search results', source: /search/iu, runtime: (x) => x.eventLike > 0 && x.listLike > 0 },
  { id: 'family.onboarding', label: 'Onboarding', source: /onboarding/iu, runtime: (x) => x.actionLike > 0 && x.asyncLike > 0 },
]);

export const COVERAGE_HYPOTHESES = Object.freeze([
  { id: 'home', label: 'Home', source: /(?:\/pages\/index\.astro$|Home)/iu, route: /^\/$/u },
  { id: 'event-detail', label: 'Event Detail', source: /(?:EventDetail|DesktopEventPage|\/sobytiya\/)/iu, route: /^\/sobytiya\//u },
  { id: 'day-listing', label: 'Day Listing', source: /(?:segodnya|zavtra|DayListing)/iu, route: /^\/(?:segodnya|zavtra)\//u },
  { id: 'weekend-listing', label: 'Weekend Listing', source: /(?:vyhodnye|Weekend)/iu, route: /^\/vyhodnye\//u },
  { id: 'search', label: 'Search (mobile noted)', source: /(?:poisk|Search)/iu, route: /^\/poisk\//u },
  { id: 'popular', label: 'Popular', source: /(?:populyarnoe|Popular)/iu, route: /^\/populyarnoe\//u },
  { id: 'collections', label: 'Collections', source: /(?:podborki|Collection)/iu, route: /^\/podborki\//u },
  { id: 'festivals', label: 'Festivals', source: /(?:festivali|Festival)/iu, route: /^\/festivali\//u },
  { id: 'interest-clubs', label: 'Interest Clubs', source: /(?:kluby-po-interesam|InterestClub)/iu, route: /^\/kluby-po-interesam\//u },
  { id: 'editorial-collections', label: 'Editorial Collections', source: /EditorialCollection/iu, route: /^\/editorial-collections\//u },
  { id: 'partners', label: 'Partners', source: /(?:\/partners\/|\/partnerstvo\/|Partner)/iu, route: /^\/(?:partners|partnerstvo)\//u },
  { id: 'favorites', label: 'Favorites', source: /(?:izbrannoe|Favorite)/iu, route: /^\/izbrannoe\//u },
  { id: 'for-me', label: 'For Me / personal feed', source: /(?:dlya-menya|PersonalFeed)/iu, route: /^\/dlya-menya\//u },
  { id: 'legal-documents', label: 'Legal documents', source: /\/pages\/(?:legal|privacy|terms|politika|soglasie|oferta)(?:\/|\.astro$)/iu, route: /^\/(?:legal|privacy|terms|politika|soglasie|oferta)\//u },
  { id: 'exhibitions', label: 'Exhibitions', source: /(?:vystavki|ExhibitionsPersonalSurface)/iu, route: /^\/vystavki\//u },
  { id: 'hero-talk', label: 'Hero-talk', source: /HomeHeroTalk/iu, route: /^\/$/u, marker: 'home_hero_talk' },
  { id: 'hero-talk-page-end', label: 'Hero-talk page-end', source: /(?:PageEndHeroTalk|HeroTalkPageEnd)/iu, route: /$a/u, marker: 'hero_talk_page_end' },
]);

export function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function stableObject(value) {
  if (Array.isArray(value)) return value.map(stableObject);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableObject(value[key])]));
  }
  return value;
}

export function stableJson(value, pretty = false) {
  return `${JSON.stringify(stableObject(value), null, pretty ? 2 : 0)}\n`;
}

export function walk(root, predicate = () => true) {
  const found = [];
  function visit(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile() && predicate(path)) found.push(path);
    }
  }
  visit(root);
  return found;
}

export function redactFactory(secrets = []) {
  const needles = [...new Set(secrets.filter(Boolean).map(String))].sort((a, b) => b.length - a.length);
  return (input) => {
    let text = String(input ?? '');
    for (const needle of needles) text = text.split(needle).join('[REDACTED]');
    text = text.replace(/\/_review\/[A-Za-z0-9_-]{20,}(?=\/|\b)/gu, '/_review/[REDACTED]');
    text = text.replace(/(?:authorization|bearer)(?:%20|\s|:)+[A-Za-z0-9._~+\/-]{8,}/giu, 'authorization:[REDACTED]');
    return text;
  };
}

export class Budget {
  constructor(limit) { this.limit = limit; this.used = 0; }
  claim(bytes, label) {
    if (this.used + bytes > this.limit) throw new Error(`Snapshot byte budget exceeded while writing ${label}`);
    this.used += bytes;
  }
}

export class JsonlWriter {
  constructor(path, budget) {
    this.path = path; this.budget = budget; this.count = 0;
    this.stream = createWriteStream(path, { encoding: 'utf8' });
  }
  async write(record) {
    const line = stableJson(record);
    this.budget.claim(Buffer.byteLength(line), basename(this.path));
    if (!this.stream.write(line)) await once(this.stream, 'drain');
    this.count += 1;
  }
  async close() { this.stream.end(); await once(this.stream, 'close'); }
}

export function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) throw new Error(`Unexpected argument: ${token}`);
    const equal = token.indexOf('=');
    const key = token.slice(2, equal > 0 ? equal : undefined).replaceAll('-', '_');
    const value = equal > 0 ? token.slice(equal + 1) : argv[++index];
    if (value === undefined || value.startsWith('--')) throw new Error(`Missing value for --${key.replaceAll('_', '-')}`);
    result[key] = value;
  }
  return result;
}

export function git(args, cwd) {
  return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

export function validateSourcePin(sourceRoot, expectedSha, expectedTreeHash = '') {
  const root = resolve(sourceRoot);
  if (!existsSync(root) || !statSync(root).isDirectory()) throw new Error('Pinned source root does not exist');
  if (!/^[0-9a-f]{40}$/u.test(expectedSha)) throw new Error('Pinned source SHA must be a full Git SHA');
  let repository;
  try { repository = git(['rev-parse', '--show-toplevel'], root); } catch { repository = null; }
  if (repository) {
    const head = git(['rev-parse', 'HEAD'], repository);
    git(['cat-file', '-e', `${expectedSha}^{commit}`], repository);
    const sourceRelative = relative(repository, root).split(sep).join('/') || '.';
    const headTree = git(['rev-parse', sourceRelative === '.' ? 'HEAD^{tree}' : `HEAD:${sourceRelative}`], repository);
    const pinnedTree = git(['rev-parse', sourceRelative === '.' ? `${expectedSha}^{tree}` : `${expectedSha}:${sourceRelative}`], repository);
    if (head !== expectedSha && headTree !== pinnedTree) throw new Error('Pinned source mismatch: checked source tree differs from requested SHA');
    return { checked_out_sha: head, requested_sha: expectedSha, match: head === expectedSha ? 'exact_commit' : 'exact_tree_equivalent', tree_hash: headTree };
  }
  const hash = hashSourceTree(root);
  if (!expectedTreeHash || hash !== expectedTreeHash) throw new Error('Non-Git source requires an exact matching --source-tree-hash');
  return { checked_out_sha: null, requested_sha: expectedSha, match: 'exact_tree_hash', tree_hash: hash };
}

export function hashSourceTree(root) {
  const lines = walk(root).map((path) => `${relative(root, path).split(sep).join('/')}\0${sha256(readFileSync(path))}\n`);
  return sha256(lines.join(''));
}

function nodeName(path) { return basename(path, extname(path)); }
function componentType(rel) {
  if (rel.includes('/pages/')) return 'page';
  if (rel.includes('/layouts/')) return 'layout';
  if (rel.includes('/components/')) return 'component';
  return 'controller_or_module';
}
function lineAt(text, offset) { return text.slice(0, offset ?? 0).split('\n').length; }

export async function loadParsers(siteRoot) {
  const requireFromSite = createRequire(join(resolve(siteRoot), 'package.json'));
  const astroPath = requireFromSite.resolve('@astrojs/compiler');
  const astro = await import(pathToFileURL(astroPath).href);
  const postcss = requireFromSite('postcss');
  const htmlparser2 = requireFromSite('htmlparser2');
  const esm = requireFromSite('es-module-lexer');
  await esm.init;
  return { astro, postcss, htmlparser2, esm };
}

function astWalk(node, visit) {
  if (!node || typeof node !== 'object') return;
  visit(node);
  for (const child of node.children || []) astWalk(child, visit);
}

function extractFrontmatterFacts(frontmatter, esm) {
  let parsed = [];
  try { parsed = esm.parse(frontmatter)[0]; } catch { /* invalid standalone TS is represented as unknown below */ }
  const imports = parsed.map((entry) => frontmatter.slice(entry.s, entry.e)).filter(Boolean).sort();
  const propMatch = frontmatter.match(/(?:const|let)\s*\{([^}]+)\}\s*=\s*Astro\.props/su);
  const props = propMatch ? propMatch[1].split(',').map((item) => item.trim().split(/[:=]/u)[0]?.trim()).filter((item) => /^[A-Za-z_$][\w$]*$/u.test(item)).sort() : [];
  const data = imports.filter((item) => /(?:data|lib|api|client|server|supabase|json)/iu.test(item));
  const css = imports.filter((item) => /\.css(?:\?|$)/iu.test(item));
  return { imports, props, data, css };
}

export async function inventorySource(sourceRoot, parsers) {
  const files = walk(sourceRoot, (path) => ['.astro', '.ts', '.js', '.mjs'].includes(extname(path)) && !/\.(?:test|spec)\.[^.]+$/u.test(path));
  const records = [];
  for (const path of files) {
    const rel = relative(dirname(sourceRoot), path).split(sep).join('/');
    const source = readFileSync(path, 'utf8');
    const hash = sha256(source);
    const type = componentType(`/${rel}`);
    let imports = [], exports = [], children = [], props = [], slots = [], conditions = [], data = [], css = [], clientDirectives = [], parser = 'es_module_lexer', parserStatus = 'parsed';
    if (extname(path) === '.astro') {
      try {
        const parsed = await parsers.astro.parse(source, { position: true });
        const frontmatter = parsed.ast.children.find((child) => child.type === 'frontmatter')?.value || '';
        const facts = extractFrontmatterFacts(frontmatter, parsers.esm);
        ({ imports, props, data, css } = facts);
        astWalk(parsed.ast, (node) => {
          if (node.type === 'component') children.push({ name: node.name, line: node.position?.start?.line ?? null });
          for (const attribute of node.attributes || []) if (attribute.name?.startsWith('client:')) clientDirectives.push(`${node.name || node.type}:${attribute.name}`);
          if (node.type === 'element' && node.name === 'slot') {
            const named = node.attributes?.find((attr) => attr.name === 'name');
            slots.push({ name: named?.value || 'default', line: node.position?.start?.line ?? null });
          }
          if (node.type === 'expression') conditions.push({ kind: 'template_expression', line: node.position?.start?.line ?? null });
          if (node.type === 'element' && node.name === 'style') css.push('inline:style');
        });
        parser = '@astrojs/compiler';
      } catch (error) {
        parserStatus = 'parse_failed';
        conditions = [{ kind: 'unknown', reason_hash: sha256(error.message) }];
      }
    } else {
      try {
        const parsedModule = parsers.esm.parse(source);
        imports = parsedModule[0].map((entry) => source.slice(entry.s, entry.e)).filter(Boolean).sort();
        exports = parsedModule[1].map((entry) => entry.n).filter(Boolean).sort();
      }
      catch { parserStatus = 'parse_failed'; }
      data = imports.filter((item) => /(?:data|api|client|server|supabase|json)/iu.test(item));
      css = imports.filter((item) => /\.css(?:\?|$)/iu.test(item));
    }
    records.push({
      id: `source.${sha256(rel).slice(0, 16)}`,
      path: rel,
      export: extname(path) === '.astro' ? ['default'] : exports,
      name: nodeName(path), type,
      imports: [...new Set(imports)].sort(), consumers: [],
      children: children.sort((a, b) => a.name.localeCompare(b.name) || (a.line ?? 0) - (b.line ?? 0)),
      props: props.map((name) => ({ name, status: 'observed', confidence: 'medium' })),
      slots: slots.sort((a, b) => a.name.localeCompare(b.name)),
      conditions, data_dependencies: [...new Set(data)].sort(), css_dependencies: [...new Set(css)].sort(),
      client_dependencies: [...new Set([...imports.filter((item) => /(?:client|browser|supabase)/iu.test(item)), ...clientDirectives])].sort(),
      content_sha256: hash, status: parserStatus === 'parsed' ? 'observed' : 'unknown',
      confidence: parserStatus === 'parsed' ? (extname(path) === '.astro' ? 'high' : 'medium') : 'low',
      evidence: { parser, parser_status: parserStatus, source_line_count: source.split('\n').length },
    });
  }
  const bySuffix = new Map();
  for (const record of records) bySuffix.set(record.path.replace(/^site\/src\//u, ''), record);
  for (const consumer of records) {
    for (const imported of consumer.imports) {
      if (!imported.startsWith('.')) continue;
      const base = resolve(dirname(join(dirname(sourceRoot), consumer.path)), imported);
      const target = records.find((candidate) => {
        const absolute = join(dirname(sourceRoot), candidate.path);
        return absolute === base || absolute === `${base}.astro` || absolute === `${base}.ts` || absolute === `${base}.js` || absolute === join(base, 'index.astro');
      });
      if (target) target.consumers.push(consumer.id);
    }
  }
  for (const record of records) record.consumers.sort();
  return records.sort((a, b) => a.path.localeCompare(b.path));
}

export function astroRouteTemplate(pageRoot, path) {
  let rel = relative(pageRoot, path).split(sep).join('/').replace(/\.astro$/u, '');
  if (rel.endsWith('/index')) rel = rel.slice(0, -6);
  if (rel === 'index') rel = '';
  const parts = rel.split('/').filter(Boolean).map((part) => part.replace(/\[\.\.\.([^\]]+)\]/gu, ':$1*').replace(/\[([^\]]+)\]/gu, ':$1'));
  return `/${parts.join('/')}${parts.length ? '/' : ''}`;
}

function pageFamilyFor(template) {
  if (template === '/') return 'page-family.home';
  if (template.includes(':') && /sobytiya|event/iu.test(template)) return 'page-family.event-detail';
  if (template.includes(':')) return `page-family.dynamic-${template.split('/').filter(Boolean)[0]?.replace(/[^a-z0-9-]/giu, '') || 'root'}`;
  if (/search|poisk/iu.test(template)) return 'page-family.search';
  if (/podbork|collection|gastronom/iu.test(template)) return 'page-family.collections';
  if (/segodnya|zavtra|vyhodnye|calendar|populyar/iu.test(template)) return 'page-family.listing';
  if (/lab|example/iu.test(template)) return 'page-family.special-lab';
  return `page-family.special-${template.split('/').filter(Boolean)[0] || 'root'}`;
}

export function pageFamiliesFromSource(sourceRoot, sourceRecords, runtimeObservations) {
  const pageRoot = join(sourceRoot, 'pages');
  const templates = existsSync(pageRoot) ? walk(pageRoot, (path) => extname(path) === '.astro').map((path) => ({ template: astroRouteTemplate(pageRoot, path), source: `source.${sha256(relative(dirname(sourceRoot), path).split(sep).join('/')).slice(0, 16)}` })) : [];
  const grouped = new Map();
  for (const item of templates) {
    const id = pageFamilyFor(item.template);
    if (!grouped.has(id)) grouped.set(id, { id, source_templates: [], source_pages: [], runtime_route_hashes: [], clustering_basis: ['source_page_template'] });
    grouped.get(id).source_templates.push(item.template); grouped.get(id).source_pages.push(item.source);
  }
  for (const observation of runtimeObservations) {
    const id = observation.page_family;
    if (!grouped.has(id)) grouped.set(id, { id, source_templates: [], source_pages: [], runtime_route_hashes: [], clustering_basis: ['runtime_structure'] });
    grouped.get(id).runtime_route_hashes.push(observation.route_hash);
    if (!grouped.get(id).structure_hashes) grouped.get(id).structure_hashes = [];
    grouped.get(id).structure_hashes.push(observation.structure_hash);
  }
  for (const family of grouped.values()) {
    family.source_templates.sort(); family.source_pages.sort(); family.runtime_route_hashes.sort();
    family.structure_hashes = [...new Set(family.structure_hashes || [])].sort();
    family.layouts = sourceRecords.filter((record) => record.type === 'layout' && record.consumers.some((id) => family.source_pages.includes(id))).map((record) => record.id).sort();
    family.major_regions = ['unknown'];
    family.desktop_structure = 'independent observation required';
    family.mobile_structure = 'independent observation required';
    family.status = family.runtime_route_hashes.length ? 'observed' : 'candidate';
  }
  return [...grouped.values()].sort((a, b) => a.id.localeCompare(b.id));
}

function runtimeFeatures() {
  return { actionLike: 0, asyncLike: 0, badgeLike: 0, brandLike: 0, breadcrumbLike: 0, collectionLike: 0, eventLike: 0, listLike: 0, mediaLike: 0, medallionLike: 0, navLike: 0, timeLike: 0, transportLike: 0 };
}

export function structuralScan(html, htmlparser2) {
  const tags = new Map(); const regions = new Map(); const features = runtimeFeatures(); const tokens = [];
  const surfaceMarkers = new Set();
  let elementCount = 0, maxDepth = 0, depth = 0;
  const parser = new htmlparser2.Parser({
    onopentag(name, attributes) {
      depth += 1; maxDepth = Math.max(maxDepth, depth); elementCount += 1;
      tags.set(name, (tags.get(name) || 0) + 1);
      if (tokens.length < 10000) tokens.push(`<${name}>`);
      const marker = `${name} ${attributes.class || ''} ${attributes.id || ''} ${Object.keys(attributes).join(' ')}`.toLowerCase();
      if (/home-hero-talk|data-home-hero-talk/u.test(marker)) surfaceMarkers.add('home_hero_talk');
      if (/page-end-hero-talk|hero-talk-page-end/u.test(marker)) surfaceMarkers.add('hero_talk_page_end');
      if (/^(header|main|nav|aside|footer|section)$/u.test(name)) regions.set(name, (regions.get(name) || 0) + 1);
      if (/^(a|button|input|select|textarea)$/u.test(name) || /button|cta|action/u.test(marker)) features.actionLike += 1;
      if (/event|sobyt/u.test(marker)) features.eventLike += 1;
      if (/^(ul|ol)$/u.test(name) || /list|listing|feed/u.test(marker)) features.listLike += 1;
      if (/^(img|picture|video|figure)$/u.test(name) || /media|gallery|poster|hero/u.test(marker)) features.mediaLike += 1;
      if (/medallion/u.test(marker)) features.medallionLike += 1;
      if (name === 'time' || /date|calendar|occurrence/u.test(marker)) features.timeLike += 1;
      if (/transport|bus|rail/u.test(marker)) features.transportLike += 1;
      if (name === 'nav' || /navigation|header|footer/u.test(marker)) features.navLike += 1;
      if (/badge|label|chip|tag/u.test(marker)) features.badgeLike += 1;
      if (/breadcrumb/u.test(marker)) features.breadcrumbLike += 1;
      if (/loading|error|empty|skeleton|aria-busy/u.test(marker)) features.asyncLike += 1;
      if (/brand|logo|lockup/u.test(marker)) features.brandLike += 1;
      if (/collection|podbork|gastronom/u.test(marker)) features.collectionLike += 1;
    },
    onclosetag(name) { if (tokens.length < 10000) tokens.push(`</${name}>`); depth = Math.max(0, depth - 1); },
  }, { decodeEntities: true });
  parser.write(html); parser.end();
  return {
    element_count: elementCount, max_depth: maxDepth,
    tag_counts: Object.fromEntries([...tags].sort(([a], [b]) => a.localeCompare(b))),
    major_regions: Object.fromEntries([...regions].sort(([a], [b]) => a.localeCompare(b))),
    structure_hash: sha256(tokens.join('')), features, surface_markers: [...surfaceMarkers].sort(),
  };
}

function routeFromKey(key) {
  if (key === 'index.html') return '/';
  if (key.endsWith('/index.html')) return `/${key.slice(0, -10)}/`;
  return `/${key.replace(/\.html$/u, '')}`;
}
function runtimePageFamily(route) {
  const generic = route.replace(/\d+/gu, ':id').replace(/[0-9a-f]{8,}/giu, ':slug');
  return pageFamilyFor(generic);
}

export function manifestHtmlFiles(manifest) {
  if (!manifest || !Array.isArray(manifest.files)) throw new Error('Runtime manifest must contain an exact files inventory');
  return manifest.files.filter((file) => typeof file.key === 'string' && file.key.endsWith('.html')).sort((a, b) => a.key.localeCompare(b.key));
}

export async function scanLocalRuntime(runtimeRoot, manifest, parsers, maxHtmlBytes) {
  const observations = [];
  for (const file of manifestHtmlFiles(manifest)) {
    const path = resolve(runtimeRoot, file.key);
    if (!path.startsWith(`${resolve(runtimeRoot)}${sep}`) && path !== resolve(runtimeRoot)) throw new Error('Unsafe runtime manifest key');
    const bytes = readFileSync(path);
    if (bytes.length > maxHtmlBytes) throw new Error(`HTML input exceeds per-route budget: ${file.key}`);
    if (file.sha256 && sha256(bytes) !== file.sha256) throw new Error(`Runtime file hash mismatch: ${file.key}`);
    observations.push(runtimeRecord(routeFromKey(file.key), structuralScan(bytes.toString('utf8'), parsers.htmlparser2), bytes.length, file.sha256 || sha256(bytes)));
  }
  return observations;
}

export async function scanRemoteRuntime(candidateBase, manifest, parsers, maxHtmlBytes, redact) {
  const observations = [];
  const base = new URL(candidateBase.endsWith('/') ? candidateBase : `${candidateBase}/`);
  for (const file of manifestHtmlFiles(manifest)) {
    const response = await fetch(new URL(file.key, base), { redirect: 'error', headers: { accept: 'text/html' } });
    if (!response.ok) throw new Error(redact(`Runtime fetch failed (${response.status}) for ${file.key}`));
    const expectedLength = Number(response.headers.get('content-length') || 0);
    if (expectedLength > maxHtmlBytes) throw new Error(`HTML input exceeds per-route budget: ${file.key}`);
    const chunks = []; let size = 0;
    for await (const chunk of response.body) {
      size += chunk.length;
      if (size > maxHtmlBytes) throw new Error(`HTML input exceeds per-route budget: ${file.key}`);
      chunks.push(chunk);
    }
    const bytes = Buffer.concat(chunks);
    if (file.sha256 && sha256(bytes) !== file.sha256) throw new Error(`Runtime file hash mismatch: ${file.key}`);
    observations.push(runtimeRecord(routeFromKey(file.key), structuralScan(bytes.toString('utf8'), parsers.htmlparser2), bytes.length, file.sha256 || sha256(bytes)));
  }
  return observations;
}

function runtimeRecord(route, scan, bytes, contentHash) {
  const routeHash = sha256(route);
  const hypotheses = COVERAGE_HYPOTHESES.filter((item) => item.route.test(route) || (item.marker && scan.surface_markers.includes(item.marker))).map((item) => item.id).sort();
  return {
    id: `runtime.${routeHash.slice(0, 16)}`, route_hash: routeHash,
    route: `route:${routeHash.slice(0, 16)}`, page_family: runtimePageFamily(route),
    viewport: 'structural_all_routes', component_candidate: 'unmapped', source_mapping: 'unresolved',
    state: 'rendered_static_html', content_fixture: { html_bytes: bytes, content_sha256: contentHash },
    media_fixture: { element_count: scan.features.mediaLike }, screenshot: null,
    structure_hash: scan.structure_hash, element_count: scan.element_count, max_depth: scan.max_depth,
    major_regions: scan.major_regions, tag_counts: scan.tag_counts, feature_counts: scan.features,
    surface_hypotheses: hypotheses, surface_markers: scan.surface_markers,
    evidence: ['exact_manifest_file', 'streaming_structural_parse'],
  };
}

export async function styleObservations(sourceRoot, sourceRecords, parsers) {
  const result = [];
  for (const record of sourceRecords.filter((item) => item.path.endsWith('.astro'))) {
    const path = join(dirname(sourceRoot), record.path);
    const source = readFileSync(path, 'utf8');
    let parsed;
    try { parsed = await parsers.astro.parse(source, { position: true }); } catch { continue; }
    const blocks = [];
    astWalk(parsed.ast, (node) => {
      if (node.type === 'element' && node.name === 'style') blocks.push({ css: (node.children || []).filter((child) => child.type === 'text').map((child) => child.value).join(''), start_line: node.position?.start?.line ?? null });
    });
    for (let index = 0; index < blocks.length; index += 1) {
      let root;
      try { root = parsers.postcss.parse(blocks[index].css, { from: record.path }); } catch { continue; }
      root.walkDecls((decl) => {
        const contexts = []; let parent = decl.parent;
        while (parent && parent.type !== 'root') { if (parent.type === 'atrule') contexts.push(`@${parent.name} ${parent.params}`.trim()); parent = parent.parent; }
        const selector = decl.parent?.type === 'rule' ? decl.parent.selector : null;
        result.push({
          id: `style.${sha256(`${record.path}\0${index}\0${decl.source?.start?.line}\0${decl.prop}`).slice(0, 16)}`,
          source_id: record.id, source_path: record.path, kind: 'source_literal_usage', property: decl.prop,
          value: decl.value, selector_sha256: selector ? sha256(selector) : null,
          at_rule_context: contexts.reverse(), style_block_index: index,
          style_block_start_line: blocks[index].start_line, line_in_style_block: decl.source?.start?.line ?? null,
          computed_inconsistency: 'unknown', confidence: 'high', evidence: ['postcss_ast'], recommendation: 'unresolved',
        });
      });
    }
  }
  return result.sort((a, b) => a.source_path.localeCompare(b.source_path) || a.style_block_index - b.style_block_index || (a.line_in_style_block ?? 0) - (b.line_in_style_block ?? 0) || a.property.localeCompare(b.property));
}

export function computedStyleObservations(viewportEvidence) {
  const groups = new Map();
  for (const item of viewportEvidence) {
    if (!groups.has(item.family)) groups.set(item.family, []);
    groups.get(item.family).push(item);
  }
  const result = [];
  for (const [family, observations] of [...groups].sort(([a], [b]) => a.localeCompare(b))) {
    for (const property of ['background_color', 'color', 'font_family', 'font_size']) {
      const instances = observations.map((item) => ({ route_hash: item.route_hash, viewport: item.viewport, value: item.computed.body[property] })).sort((a, b) => a.route_hash.localeCompare(b.route_hash) || a.viewport.width - b.viewport.width);
      const values = [...new Set(instances.map((item) => item.value))].sort();
      result.push({
        id: `style.computed.${sha256(`${family}\0${property}`).slice(0, 16)}`, family,
        kind: 'computed_runtime_observation', property: `body.${property}`, instances,
        computed_inconsistency: values.length > 1 ? 'observed_inconsistency' : 'observed_consistent',
        confidence: 'high', evidence: ['playwright_computed_style', 'exact_candidate_representative'], recommendation: 'unresolved',
      });
    }
  }
  return result;
}

export function observedFamilies(sourceRecords, runtimeObservations) {
  return FAMILY_SEEDS.map((seed) => {
    const sources = sourceRecords.filter((record) => seed.source.test(`${record.name} ${record.path}`)).map((record) => record.id).sort();
    const runtime = runtimeObservations.filter((record) => seed.runtime(record.feature_counts)).map((record) => record.id).sort();
    const status = runtime.length ? 'observed' : sources.length ? 'candidate' : 'unknown';
    return { id: seed.id, label: seed.label, implementations: sources, runtime_observations: runtime, status, confidence: runtime.length && sources.length ? 'high' : runtime.length || sources.length ? 'medium' : 'low', evidence_channels: [...(sources.length ? ['source_ast'] : []), ...(runtime.length ? ['runtime_structure'] : [])] };
  });
}

export function fragmentationReport(families, styles) {
  return families.map((family) => ({
    id: `fragmentation.${family.id.slice(7)}`, family: family.id,
    observations: [...family.implementations, ...family.runtime_observations].sort(),
    evidence_channels: family.evidence_channels,
    similarity: 'not_scored', decision: 'NOT_MERGED', reason: family.evidence_channels.length >= 2 ? 'multiple channels show a review candidate; semantic analysis remains required' : 'insufficient independent evidence for a merge decision',
    recommendation: 'unresolved', status: family.implementations.length > 1 ? 'fragmented' : family.status === 'observed' ? 'candidate' : 'unknown',
  }));
}

export function candidateGraph(families, sourceRecords) {
  return families.map((family) => ({
    id: `candidate.${family.id.slice(7)}`, family: family.id, sources: family.implementations,
    consumers: [...new Set(family.implementations.flatMap((id) => sourceRecords.find((record) => record.id === id)?.consumers || []))].sort(),
    runtime_observations: family.runtime_observations, unknowns: ['semantic contract', 'independent mobile behaviour', 'computed style consistency'],
    status: family.implementations.length > 1 ? 'fragmented' : family.status,
    decision: 'NOT_MERGED', recommendation: 'unresolved',
  }));
}

export function desktopMobile(families, runtimeObservations, viewportEvidence = []) {
  return families.map((family) => {
    const desktop = viewportEvidence.filter((item) => item.family === family.id && item.viewport?.width >= 1000);
    const mobile = viewportEvidence.filter((item) => item.family === family.id && item.viewport?.width < 600);
    let relation = 'unknown';
    if (desktop.length && mobile.length) relation = sha256(stableJson(desktop.map((x) => x.structure))) === sha256(stableJson(mobile.map((x) => x.structure))) ? 'shared_structure_observed' : 'divergent_structure_observed';
    return {
      id: `desktop-mobile.${family.id.slice(7)}`, family: family.id,
      desktop: desktop.length ? desktop : { viewport: { width: 1728, height: 900 }, status: 'not_observed' },
      mobile: mobile.length ? mobile : { viewport: { width: 390, height: 844 }, status: 'not_observed' },
      relation, interpretation: 'independent_observations_not_responsive_variants',
      optional_breakpoint_evidence: [{ width: 430, height: 932 }, { width: 768, height: 1024 }, { width: 1280, height: 800 }],
    };
  });
}

export function screenshotIndex(pageFamilies) {
  return pageFamilies.map((family) => ({
    id: `screenshot.${family.id.slice(12)}`, page_family: family.id,
    selection: family.runtime_route_hashes.length ? 'deterministic_representative' : 'not_available',
    route_hash: family.runtime_route_hashes[0] || null, screenshot_path: null,
    viewport_status: 'not_captured_structural_scan_only', reason: 'screenshots are bounded to representatives/outliers/conflicts in an explicit browser pass',
  }));
}

export async function captureBrowserEvidence({ candidateBase, manifest, runtimeObservations, families, siteRoot, outputDir, budget, maxPages = 20 }) {
  if (!candidateBase) throw new Error('Browser evidence requires the candidate base URL');
  const requireFromSite = createRequire(join(resolve(siteRoot), 'package.json'));
  const { chromium } = requireFromSite('playwright');
  const files = manifestHtmlFiles(manifest);
  if (files.length !== runtimeObservations.length) throw new Error('Browser selection cannot map runtime inventory to manifest');
  const byFamily = new Map();
  for (let index = 0; index < runtimeObservations.length; index += 1) {
    const observation = runtimeObservations[index];
    if (!byFamily.has(observation.page_family)) byFamily.set(observation.page_family, []);
    byFamily.get(observation.page_family).push({ file: files[index], observation });
  }
  const selected = [];
  for (const [pageFamily, rows] of [...byFamily].sort(([a], [b]) => a.localeCompare(b))) {
    const structures = new Set();
    for (const row of rows) {
      const selection = structures.size === 0 ? 'representative' : structures.has(row.observation.structure_hash) ? null : 'structural_outlier';
      structures.add(row.observation.structure_hash);
      if (selection) selected.push({ ...row, pageFamily, selection });
    }
  }
  selected.splice(maxPages);
  const screenshotDir = join(outputDir, 'screenshots'); mkdirSync(screenshotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const viewportEvidence = []; const screenshots = [];
  try {
    for (const selectedPage of selected) {
      for (const viewport of [{ width: 390, height: 844 }, { width: 1728, height: 900 }]) {
        const page = await browser.newPage({ viewportSize: viewport, deviceScaleFactor: 1, reducedMotion: 'reduce' });
        const target = new URL(selectedPage.file.key, candidateBase.endsWith('/') ? candidateBase : `${candidateBase}/`);
        await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 20_000 });
        await page.evaluate(async () => { if (document.fonts?.ready) await document.fonts.ready; });
        const computed = await page.evaluate(() => {
          const geometry = (selector) => {
            const node = document.querySelector(selector); if (!node) return null;
            const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
            return { display: style.display, height: Math.round(rect.height), width: Math.round(rect.width), x: Math.round(rect.x), y: Math.round(rect.y) };
          };
          const body = getComputedStyle(document.body);
          const tags = [...document.querySelectorAll('*')].slice(0, 20_000).map((node) => node.tagName.toLowerCase()).join('>');
          return {
            body: { background_color: body.backgroundColor, color: body.color, font_family: body.fontFamily, font_size: body.fontSize, scroll_height: document.documentElement.scrollHeight, scroll_width: document.documentElement.scrollWidth },
            regions: { footer: geometry('footer'), header: geometry('header'), main: geometry('main'), nav: geometry('nav') },
            structure_hash: tags,
          };
        });
        computed.structure_hash = sha256(computed.structure_hash);
        const safeFamily = selectedPage.pageFamily.replace(/[^a-z0-9-]+/giu, '-');
        const filename = `${safeFamily}-${selectedPage.observation.route_hash.slice(0, 12)}-${viewport.width}x${viewport.height}.jpg`;
        const path = join(screenshotDir, filename);
        await page.screenshot({ path, type: 'jpeg', quality: 65, fullPage: false, animations: 'disabled' });
        await page.close();
        const size = statSync(path).size; budget.claim(size, `screenshots/${filename}`);
        const relativePath = `screenshots/${filename}`;
        screenshots.push({ id: `screenshot.${sha256(`${selectedPage.observation.route_hash}\0${viewport.width}`).slice(0, 16)}`, page_family: selectedPage.pageFamily, selection: selectedPage.selection, route_hash: selectedPage.observation.route_hash, screenshot_path: relativePath, viewport, screenshot_sha256: sha256(readFileSync(path)), screenshot_bytes: size, source: 'exact_candidate_browser' });
        for (const family of families.filter((item) => item.runtime_observations.includes(selectedPage.observation.id))) viewportEvidence.push({ family: family.id, route_hash: selectedPage.observation.route_hash, viewport, structure: computed.structure_hash, computed, screenshot_path: relativePath, selection: selectedPage.selection });
      }
    }
  } finally { await browser.close(); }
  return { screenshots, viewportEvidence };
}

export function coverageRows(sourceRecords, runtimeObservations, pageFamilies) {
  const rows = COVERAGE_HYPOTHESES.map((hypothesis) => {
    const source = sourceRecords.filter((record) => hypothesis.source.test(record.path) || hypothesis.source.test(record.name)).map((record) => record.id).sort();
    const runtime = runtimeObservations.filter((record) => record.surface_hypotheses?.includes(hypothesis.id)).map((record) => record.id).sort();
    let status = source.length && runtime.length ? 'FOUND' : source.length || runtime.length ? 'AMBIGUOUS' : 'MISSING';
    let note = 'source and runtime evidence are independently reported';
    if (hypothesis.id === 'hero-talk-page-end' && !source.length && !runtime.length) note = 'No component/consumer/runtime marker; the independent onboarding page_end slot is not Hero-talk evidence.';
    if (hypothesis.id === 'legal-documents' && !source.length && !runtime.length) note = 'No exact legal page route; footer text or future documentation is not a current surface.';
    return { id: hypothesis.id, label: hypothesis.label, status, source_evidence: source, runtime_evidence: runtime, note };
  });
  const known = new Set(runtimeObservations.flatMap((record) => record.surface_hypotheses || []));
  for (const family of pageFamilies) {
    if (family.status !== 'observed') continue;
    const matchesNamed = rows.some((row) => row.runtime_evidence.some((id) => family.runtime_route_hashes.some((hash) => id.endsWith(hash.slice(0, 16)))));
    if (!matchesNamed && !['page-family.home'].includes(family.id)) rows.push({ id: `discovered-${family.id}`, label: family.id, status: 'DISCOVERED', source_evidence: family.source_pages, runtime_evidence: family.runtime_route_hashes.map((hash) => `runtime.${hash.slice(0, 16)}`), note: 'Observed outside the supplied hypothesis set; no semantic merge was attempted.' });
  }
  return rows.sort((a, b) => a.id.localeCompare(b.id));
}

export function outputHashes(outputDir, names) {
  return Object.fromEntries(names.filter((name) => existsSync(join(outputDir, name))).sort().map((name) => {
    const bytes = readFileSync(join(outputDir, name)); return [name, { bytes: bytes.length, sha256: sha256(bytes) }];
  }));
}

export function assertGraphInvariants(outputDir) {
  for (const name of REQUIRED_FILES) {
    const path = join(outputDir, name);
    if (!existsSync(path) || statSync(path).size === 0) throw new Error(`Required output is missing or empty: ${name}`);
  }
  for (const name of REQUIRED_FILES.filter((item) => item.endsWith('.jsonl'))) {
    const rows = readFileSync(join(outputDir, name), 'utf8').trim().split('\n').map((line) => JSON.parse(line));
    if (!rows.length) throw new Error(`Required JSONL has no records: ${name}`);
  }
  for (const name of ['fragmentation-report.jsonl', 'candidate-component-graph.jsonl']) {
    for (const line of readFileSync(join(outputDir, name), 'utf8').trim().split('\n')) {
      const record = JSON.parse(line);
      if (record.decision !== 'NOT_MERGED' || record.recommendation !== 'unresolved') throw new Error(`Merge invariant violated in ${name}`);
    }
  }
}

export function writeDeterministic(path, content, budget) {
  const bytes = Buffer.from(content);
  budget.claim(bytes.length, basename(path)); writeFileSync(path, bytes);
}

export function ensureOutput(path) { mkdirSync(path, { recursive: true }); return resolve(path); }
