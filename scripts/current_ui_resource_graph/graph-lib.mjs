import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import {
  createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, statSync,
  writeFileSync,
} from 'node:fs';
import { once } from 'node:events';
import { basename, dirname, extname, join, relative, resolve, sep } from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { extractAstroStateFacts, extractStateAwareFacts, inlineScriptImports } from './v1/source-state.mjs';
import { captureComponentScopedEvidence } from './v1/evidence.mjs';

export const SCHEMA = 'current_ui_resource_graph_v0';
const MAX_SCREENSHOT_BYTES = 192 * 1024;
export const REQUIRED_FILES = [
  'manifest.json', 'summary.md', 'source-components.jsonl',
  'observed-ui-families.jsonl', 'runtime-observations.jsonl',
  'page-families.jsonl', 'event-presentation-formats.jsonl', 'desktop-mobile-analysis.jsonl',
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
    generated_at: '2026-08-08T13:32:56.163Z',
    manifest_sha256: 'd615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322',
    tree_sha256: '0aad3919fccd996a5d32bcc760af8ee9b72249742c9db53196b009759bd0e7f4',
    production_manifest_sha256: 'baa0f29da3205ac81ddd4804bf6ff8e22b4585abb58d7d378e8dd87b9d395e45',
    production_tree_sha256: '47df3798686dfbdde43589ba6a6498effd82f6fd091de6883a4899b7b4e57769',
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
    artifact_name: 'prelaunch-root-release-31263560430',
    artifact_digest: 'sha256:d9d6ec1e5c5e291a598dfdcef6ca90ce049196f3f3484d0d89351d04bc2bb855',
    artifact_bytes: 6076603,
    artifact_expires_at: '2026-09-07T15:09:03Z',
    published_at: '2026-08-08T15:09:02Z',
    runtime_url: 'https://kenigevents.ru/',
    html_sha256: '1c31504d10d9ec66c7fa84ad52c94e6019a741f0ee01826f219578963e0ea21e',
  },
});

export const REQUIRED_CANDIDATE_CHECKS = Object.freeze([
  'astro_build', 'browser_visual', 'candidate_contract', 'catalog_parity',
  'no_referrer', 'noindex', 'prefix_containment', 'root_isolation',
]);

export const PRIORITIZED_PAGE_FAMILIES = Object.freeze([
  'page-family.home', 'page-family.event-detail', 'page-family.day-listing',
  'page-family.weekend-listing', 'page-family.search', 'page-family.popular',
  'page-family.collections', 'page-family.festivals', 'page-family.interest-clubs',
  'page-family.partners-partnership', 'page-family.favorites', 'page-family.for-me',
  'page-family.exhibitions', 'page-family.artifacts', 'page-family.unusual',
  'page-family.focus-group', 'page-family.closed-poster', 'page-family.labs-preview-special',
]);

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
  if (extname(rel) === '.css') return 'stylesheet';
  if (rel.includes('/pages/') && extname(rel) === '.astro') return 'page';
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
  const babel = requireFromSite('@babel/parser');
  await esm.init;
  return { astro, postcss, htmlparser2, esm, babel };
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

export async function inventorySource(sourceRoot, parsers, { plane = 'latest_checked_kaggle_candidate' } = {}) {
  const files = walk(sourceRoot, (path) => ['.astro', '.ts', '.js', '.mjs', '.css'].includes(extname(path)) && !/\.(?:test|spec)\.[^.]+$/u.test(path));
  const records = [];
  for (const path of files) {
    const rel = relative(dirname(sourceRoot), path).split(sep).join('/');
    const source = readFileSync(path, 'utf8');
    const hash = sha256(source);
    const type = componentType(`/${rel}`);
    let imports = [], exports = [], children = [], props = [], slots = [], conditions = [], data = [], css = [], clientDirectives = [], parser = 'es_module_lexer', parserStatus = 'parsed';
    let sourceState = { parser: '@babel/parser', parser_status: 'not_attempted' };
    if (extname(path) === '.astro') {
      try {
        const parsed = await parsers.astro.parse(source, { position: true });
        const frontmatter = parsed.ast.children.find((child) => child.type === 'frontmatter')?.value || '';
        const facts = extractFrontmatterFacts(frontmatter, parsers.esm);
        ({ imports, props, data, css } = facts);
        imports = [...new Set([...imports, ...inlineScriptImports(source, parsers.esm)])].sort();
        sourceState = extractStateAwareFacts(frontmatter, parsers.babel);
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
        sourceState = extractAstroStateFacts(source, sourceState, parsed.ast, parsers.babel);
        parser = '@astrojs/compiler';
      } catch (error) {
        parserStatus = 'parse_failed';
        conditions = [{ kind: 'unknown', reason_hash: sha256(error.message) }];
      }
    } else if (extname(path) === '.css') {
      try {
        const root = parsers.postcss.parse(source, { from: rel });
        const declarations = []; const atRules = []; const selectors = [];
        root.walkDecls((decl) => declarations.push({ property: decl.prop, value_hash: sha256(decl.value), line: decl.source?.start?.line ?? null, custom_property: decl.prop.startsWith('--') }));
        root.walkAtRules((rule) => atRules.push({ name: rule.name, params_hash: sha256(rule.params || ''), line: rule.source?.start?.line ?? null }));
        root.walkRules((rule) => selectors.push({ selector_hash: sha256(rule.selector || ''), line: rule.source?.start?.line ?? null, pseudo_state: /:(?:hover|focus|focus-visible|active|disabled|checked|open|has)\b/u.test(rule.selector || '') }));
        imports = root.nodes.filter((node) => node.type === 'atrule' && node.name === 'import').map((node) => node.params.replace(/^['"]|['"].*$/gu, '')).filter(Boolean).sort();
        sourceState = { parser: 'postcss', parser_status: declarations.length || atRules.length || selectors.length ? 'parsed' : 'empty', declarations, at_rules: atRules, selectors };
        parser = 'postcss';
      } catch (error) {
        parserStatus = 'parse_failed'; sourceState = { parser: 'postcss', parser_status: 'parse_failed', reason_hash: sha256(error.message) };
      }
    } else {
      try {
        const parsedModule = parsers.esm.parse(source);
        imports = parsedModule[0].map((entry) => source.slice(entry.s, entry.e)).filter(Boolean).sort();
        exports = parsedModule[1].map((entry) => entry.n).filter(Boolean).sort();
        sourceState = extractStateAwareFacts(source, parsers.babel);
      }
      catch { parserStatus = 'parse_failed'; }
      data = imports.filter((item) => /(?:data|api|client|server|supabase|json)/iu.test(item));
      css = imports.filter((item) => /\.css(?:\?|$)/iu.test(item));
    }
    const routeTemplate = type === 'page' ? astroRouteTemplate(join(sourceRoot, 'pages'), path) : null;
    records.push({
      id: `source.${plane}.${sha256(rel).slice(0, 16)}`,
      plane, path: rel, route_template: routeTemplate,
      export: extname(path) === '.astro' ? ['default'] : exports,
      name: nodeName(path), type,
      imports: [...new Set(imports)].sort(), consumers: [],
      children: children.sort((a, b) => a.name.localeCompare(b.name) || (a.line ?? 0) - (b.line ?? 0)),
      props: props.map((name) => ({ name, status: 'observed', confidence: 'medium' })),
      slots: slots.sort((a, b) => a.name.localeCompare(b.name)),
      conditions, data_dependencies: [...new Set(data)].sort(), css_dependencies: [...new Set(css)].sort(),
      client_dependencies: [...new Set([...imports.filter((item) => /(?:client|browser|supabase)/iu.test(item)), ...clientDirectives])].sort(),
      content_sha256: hash, status: parserStatus === 'parsed' ? 'observed' : 'unknown',
      confidence: parserStatus === 'parsed' ? (['.astro', '.css'].includes(extname(path)) ? 'high' : 'medium') : 'low',
      evidence: { parser, parser_status: parserStatus, source_line_count: source.split('\n').length },
      source_state: sourceState,
    });
  }
  for (const consumer of records) {
    for (const imported of consumer.imports) {
      if (!imported.startsWith('.')) continue;
      const base = resolve(dirname(join(dirname(sourceRoot), consumer.path)), imported);
      const target = records.find((candidate) => {
        const absolute = join(dirname(sourceRoot), candidate.path);
        return absolute === base || absolute === `${base}.astro` || absolute === `${base}.ts` || absolute === `${base}.js` || absolute === `${base}.css` || absolute === join(base, 'index.astro');
      });
      if (target) target.consumers.push(consumer.id);
    }
  }
  for (const record of records) record.consumers.sort();
  const byId = new Map(records.map((record) => [record.id, record]));
  for (const record of records) {
    record.direct_dependencies = records.filter((candidate) => candidate.consumers.includes(record.id)).map((candidate) => candidate.id).sort();
  }
  for (const record of records) {
    const direct = record.direct_dependencies;
    const visited = new Set(); const pending = [...direct];
    while (pending.length) {
      const id = pending.shift();
      if (visited.has(id)) continue;
      visited.add(id);
      for (const nested of byId.get(id)?.direct_dependencies || []) pending.push(nested);
    }
    record.transitive_dependencies = [...visited].sort();
  }
  return records.sort((a, b) => a.path.localeCompare(b.path));
}

export function astroRouteTemplate(pageRoot, path) {
  let rel = relative(pageRoot, path).split(sep).join('/').replace(/\.astro$/u, '');
  if (rel.endsWith('/index')) rel = rel.slice(0, -6);
  if (rel === 'index') rel = '';
  const parts = rel.split('/').filter(Boolean).map((part) => part.replace(/\[\.\.\.([^\]]+)\]/gu, ':$1*').replace(/\[([^\]]+)\]/gu, ':$1'));
  return `/${parts.join('/')}${parts.length ? '/' : ''}`;
}

export function pageFamilyFor(template) {
  const path = template.replace(/\/+/gu, '/').toLowerCase();
  if (path === '/') return 'page-family.home';
  if (/^\/sobytiya\//u.test(path)) return 'page-family.event-detail';
  if (/^\/(?:segodnya|zavtra)(?:\/|$)/u.test(path) || /^\/date-(?::[^/]+|\d{4}-\d{2}-\d{2})(?:\/|$)/u.test(path)) return 'page-family.day-listing';
  if (/^\/vyhodnye(?:\/|$)/u.test(path)) return 'page-family.weekend-listing';
  if (/^\/(?:poisk|search)(?:\/|$)/u.test(path)) return 'page-family.search';
  if (/^\/populyarnoe(?:\/|$)/u.test(path)) return 'page-family.popular';
  if (/^\/(?:podborki|collections?)(?:\/|$)/u.test(path)) return 'page-family.collections';
  if (/^\/festivali(?:\/|$)/u.test(path)) return 'page-family.festivals';
  if (/^\/kluby-po-interesam(?:\/|$)/u.test(path)) return 'page-family.interest-clubs';
  if (/^\/(?:partners|partnerstvo)(?:\/|$)/u.test(path)) return 'page-family.partners-partnership';
  if (/^\/izbrannoe(?:\/|$)/u.test(path)) return 'page-family.favorites';
  if (/^\/dlya-menya(?:\/|$)/u.test(path)) return 'page-family.for-me';
  if (/^\/vystavki(?:\/|$)/u.test(path)) return 'page-family.exhibitions';
  if (/^\/artefakt(?:y|i)?(?:\/|$)/u.test(path)) return 'page-family.artifacts';
  if (/^\/neobychnoe(?:\/|$)/u.test(path)) return 'page-family.unusual';
  if (/^\/(?:focus|fokus-gruppa)(?:\/|$)/u.test(path)) return 'page-family.focus-group';
  if (/^\/(?:closed-poster|zakrytaya-afisha)(?:\/|$)/u.test(path)) return 'page-family.closed-poster';
  if (/^\/(?:lab|__preview|preview)(?:\/|$)/u.test(path)) return 'page-family.labs-preview-special';
  const first = path.split('/').filter(Boolean)[0]?.replace(/[^a-z0-9-]/gu, '') || 'root';
  return `page-family.special-${first}`;
}

export function pageFamiliesFromSource(sourceRecords, runtimeObservations) {
  const templates = sourceRecords.filter((record) => record.type === 'page' && record.route_template).map((record) => ({ template: record.route_template, source: record.id, plane: record.plane, composition: record.children.map((child) => child.name).sort() }));
  const grouped = new Map();
  for (const item of templates) {
    const id = pageFamilyFor(item.template);
    if (!grouped.has(id)) grouped.set(id, { id, source_templates: [], source_pages: [], source_planes: [], top_level_compositions: [], runtime_route_hashes: [], clustering_basis: ['source_page_template', 'top_level_composition'] });
    grouped.get(id).source_templates.push(item.template); grouped.get(id).source_pages.push(item.source);
    grouped.get(id).source_planes.push(item.plane); grouped.get(id).top_level_compositions.push(item.composition);
  }
  for (const observation of runtimeObservations) {
    const id = observation.page_family;
    if (!grouped.has(id)) grouped.set(id, { id, source_templates: [], source_pages: [], source_planes: [], top_level_compositions: [], runtime_route_hashes: [], clustering_basis: ['runtime_structure'] });
    grouped.get(id).runtime_route_hashes.push(observation.route_hash);
    if (!grouped.get(id).structure_hashes) grouped.get(id).structure_hashes = [];
    grouped.get(id).structure_hashes.push(observation.structure_hash);
  }
  for (const family of grouped.values()) {
    family.source_templates.sort(); family.source_pages.sort(); family.runtime_route_hashes.sort();
    family.source_planes = [...new Set(family.source_planes)].sort();
    family.top_level_compositions = [...new Map(family.top_level_compositions.map((value) => [stableJson(value), value])).values()].sort((a, b) => stableJson(a).localeCompare(stableJson(b)));
    family.structure_hashes = [...new Set(family.structure_hashes || [])].sort();
    family.layouts = sourceRecords.filter((record) => record.type === 'layout' && record.consumers.some((id) => family.source_pages.includes(id))).map((record) => record.id).sort();
    family.major_regions = [...new Set(runtimeObservations.filter((record) => family.runtime_route_hashes.includes(record.route_hash)).flatMap((record) => Object.keys(record.major_regions || {})))].sort();
    family.desktop_structure = 'independent observation required';
    family.mobile_structure = 'independent observation required';
    family.status = family.runtime_route_hashes.length && family.source_pages.length ? 'FOUND' : family.runtime_route_hashes.length ? 'DISCOVERED' : 'AMBIGUOUS';
    family.evidence = {
      source_template_count: family.source_templates.length,
      composition_count: family.top_level_compositions.length,
      runtime_structure_count: family.structure_hashes.length,
    };
  }
  return [...grouped.values()].sort((a, b) => a.id.localeCompare(b.id));
}

const EVENT_PRESENTATION_RESOURCE_SPECS = Object.freeze([
  {
    id: 'event-format.desktop.editorial-landscape', category: 'layout', label: 'Desktop event — editorial landscape',
    sourceNames: ['DesktopEventPage', 'desktopEventPresentation'], desktopFamily: 'editorial',
    sourceBoundary: "DesktopEventPage candidate === 'editorial' branch",
    structure: 'wide landscape hero with independent editorial side column',
  },
  {
    id: 'event-format.desktop.split-portrait-poster', category: 'layout', label: 'Desktop event — split portrait/poster',
    sourceNames: ['DesktopEventPage', 'desktopEventPresentation'], desktopFamily: 'split',
    sourceBoundary: "DesktopEventPage candidate === 'split' branch",
    structure: 'sticky portrait/poster media column plus independent content column',
  },
  {
    id: 'event-format.desktop.split-portrait-visual', category: 'layout_state', label: 'Desktop event — split portrait/square visual state',
    sourceNames: ['DesktopEventPage', 'desktopEventPresentation'], desktopFamily: 'split',
    presentationReasons: ['split-portrait-or-square-visual', 'split-low-resolution-portrait-viewer'],
    sourceBoundary: 'desktopEventPresentation portrait/square or low-resolution portrait resolver states',
    structure: 'portrait-oriented visual is retained in the split media column rather than promoted to editorial landscape',
  },
  {
    id: 'event-format.desktop.no-image-fallback', category: 'layout', label: 'Desktop event — no-image fallback',
    sourceNames: ['DesktopEventPage', 'desktopEventPresentation', 'EventFallbackArt'], desktopFamily: 'split', presentationReason: 'split-no-image-fallback',
    sourceBoundary: 'DesktopEventPage split fallback branch with EventFallbackArt or generic fallback',
    structure: 'typed or generic fallback occupies the primary media frame when no event image exists',
  },
  {
    id: 'event-format.cta.editorial-side-stacked', category: 'cta', label: 'Editorial desktop CTA — side/stacked',
    sourceNames: ['DesktopEventPage', 'DesktopEventActionPanel'], desktopFamily: 'editorial', actionFamily: 'editorial', actionLayout: 'stacked', marker: 'desktop_action_panel',
    sourceBoundary: 'DesktopEventActionPanel family=editorial rendered in desktop-editorial-side',
    structure: 'CTA belongs to the editorial side column',
  },
  {
    id: 'event-format.cta.split-inline', category: 'cta', label: 'Split desktop CTA — inline',
    sourceNames: ['DesktopEventPage', 'DesktopEventActionPanel'], desktopFamily: 'split', actionFamily: 'split', actionLayout: 'inline', marker: 'desktop_action_panel',
    sourceBoundary: 'DesktopEventActionPanel family=split rendered in continuous content flow',
    structure: 'CTA is an inline/sticky bar in the split content column',
  },
  {
    id: 'event-format.media.primary-large-frame', category: 'media', label: 'Event media — primary large frame',
    sourceNames: ['DesktopEventPage'], marker: 'primary_media_frame',
    sourceBoundary: 'DesktopEventPage primary media figure',
    structure: 'large primary event image or poster frame',
  },
  {
    id: 'event-format.media.split-small-photo-rail', category: 'media', label: 'Split event media — small remaining-photo rail',
    sourceNames: ['DesktopEventPage'], desktopFamily: 'split', marker: 'split_poster_thumbnail_rail',
    sourceBoundary: 'desktop-prototype__media-rail--poster',
    structure: 'small aspect-aware thumbnails below the large split poster',
  },
  {
    id: 'event-format.media.editorial-small-photo-rail', category: 'media', label: 'Editorial event media — small hero thumbnail rail',
    sourceNames: ['DesktopEventPage'], desktopFamily: 'editorial', marker: 'editorial_hero_thumbnail_rail',
    sourceBoundary: 'desktop-prototype__media-rail--hero',
    structure: 'small photo selectors beside the large editorial hero',
  },
  {
    id: 'event-format.media.editorial-large-poster-companion', category: 'media', label: 'Editorial event media — large poster companion',
    sourceNames: ['DesktopEventPage'], desktopFamily: 'editorial', marker: 'editorial_poster_companion_large',
    sourceBoundary: 'desktop-editorial-companion / companion board',
    structure: 'large contain-fit identity poster kept separate from the photo hero',
  },
  {
    id: 'event-format.media.editorial-small-companion-previews', category: 'media', label: 'Editorial event media — small companion photo previews',
    sourceNames: ['DesktopEventPage'], desktopFamily: 'editorial', marker: 'editorial_companion_photo_preview_small',
    sourceBoundary: 'desktop-editorial-companion-board__previews',
    structure: 'small remaining-photo previews adjacent to the large poster companion',
  },
]);

function eventResourceMatch(observation, spec) {
  if (observation.page_family !== 'page-family.event-detail') return false;
  const resources = observation.event_resources || {};
  if (spec.desktopFamily && !resources.desktop_families?.[spec.desktopFamily]) return false;
  if (spec.actionFamily && !resources.action_families?.[spec.actionFamily]) return false;
  if (spec.actionLayout && !resources.action_layouts?.[spec.actionLayout]) return false;
  if (spec.presentationReason && !resources.presentation_reasons?.[spec.presentationReason]) return false;
  if (spec.presentationReasons && !spec.presentationReasons.some((reason) => resources.presentation_reasons?.[reason])) return false;
  if (spec.marker && !resources.markers?.[spec.marker]) return false;
  return true;
}

export function eventPresentationFormats(sourceRecords, runtimeObservations, screenshots = []) {
  const candidateSources = sourceRecords.filter((record) => record.plane === 'latest_checked_kaggle_candidate');
  return EVENT_PRESENTATION_RESOURCE_SPECS.map((spec) => {
    const sources = candidateSources.filter((record) => spec.sourceNames.includes(record.name)).map((record) => record.id).sort();
    const runtime = runtimeObservations.filter((record) => eventResourceMatch(record, spec));
    const routeHashes = new Set(runtime.map((record) => record.route_hash));
    const screenshotEvidence = screenshots.filter((record) => routeHashes.has(record.route_hash) && record.screenshot_path).map((record) => ({
      route_hash: record.route_hash, screenshot_path: record.screenshot_path, selection: record.selection, viewport: record.viewport,
    })).sort((left, right) => left.route_hash.localeCompare(right.route_hash) || left.viewport.width - right.viewport.width);
    const reasonCounts = new Map();
    for (const observation of runtime) for (const [reason, count] of Object.entries(observation.event_resources?.presentation_reasons || {})) reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + count);
    return {
      id: spec.id, category: spec.category, label: spec.label,
      boundary_kind: spec.category === 'cta' ? 'component_family_variant' : 'internal_conditional_resource',
      source_boundary: spec.sourceBoundary, source_component_ids: sources,
      desktop_family: spec.desktopFamily || 'shared', action_family: spec.actionFamily || null, action_layout: spec.actionLayout || null,
      runtime_route_count: runtime.length,
      runtime_route_hash_samples: runtime.map((record) => record.route_hash).sort().slice(0, 20),
      presentation_reasons: Object.fromEntries([...reasonCounts].sort(([a], [b]) => a.localeCompare(b))),
      screenshot_evidence: screenshotEvidence,
      observed_structure: spec.structure,
      status: sources.length && runtime.length ? 'observed' : sources.length ? 'source_only' : runtime.length ? 'runtime_only' : 'missing',
      decision: 'NOT_MERGED', recommendation: 'unresolved',
      confidence: sources.length && runtime.length ? 'high' : 'medium',
      unknowns: ['semantic equivalence is not asserted', 'normalization target is intentionally undefined'],
    };
  }).sort((left, right) => left.id.localeCompare(right.id));
}

function runtimeFeatures() {
  return { actionLike: 0, asyncLike: 0, badgeLike: 0, brandLike: 0, breadcrumbLike: 0, collectionLike: 0, eventLike: 0, listLike: 0, mediaLike: 0, medallionLike: 0, navLike: 0, timeLike: 0, transportLike: 0 };
}

export function structuralScan(html, htmlparser2) {
  const tags = new Map(); const regions = new Map(); const features = runtimeFeatures(); const tokens = [];
  const surfaceMarkers = new Set();
  const eventResourceMarkers = new Map();
  const componentStateAttributes = new Map();
  const desktopFamilies = new Map(); const actionFamilies = new Map(); const actionLayouts = new Map(); const presentationReasons = new Map(); const presentationFallbacks = new Map();
  const countValue = (target, value, { maxValues = 32, maxLength = 96 } = {}) => {
    if (typeof value !== 'string' || !value || value.length > maxLength) return;
    if (!target.has(value) && target.size >= maxValues) return;
    target.set(value, (target.get(value) || 0) + 1);
  };
  const resourceAttributes = Object.freeze({
    'data-media-frame': 'primary_media_frame',
    'data-split-media-rail': 'split_poster_thumbnail_rail',
    'data-hero-rail': 'editorial_hero_thumbnail_rail',
    'data-editorial-ocr-companion': 'editorial_poster_companion_large',
    'data-companion-preview-item': 'editorial_companion_photo_preview_small',
    'data-desktop-action-panel': 'desktop_action_panel',
  });
  const exactStateAttributes = Object.freeze([
    'data-event-transport-schedule', 'data-event-city', 'data-outbound-count', 'data-return-count', 'data-event-end-basis',
    'data-return-schedule-cutoff', 'data-last-same-day-return',
    'data-event-bus-schedule', 'data-bus-route', 'data-bus-outbound', 'data-bus-return',
    'data-kaup-transport', 'data-kaup-compact', 'data-kaup-official-transfer', 'data-kaup-public-bus', 'data-transport-treatment',
    'data-medallion-layout', 'data-main-medallion-slug', 'data-top-slot-enabled', 'data-medallion-slot',
    'data-medallion-role', 'data-medallion-category', 'data-identity-resolution', 'data-identity-conflict',
    'data-focus-egg-artifact', 'data-egg-state', 'data-artifact-collection', 'data-artifact-slot',
    'data-artifact-state', 'data-artifact-collection-unavailable', 'data-amber-artifact', 'aria-pressed',
  ]);
  let elementCount = 0, maxDepth = 0, depth = 0;
  const parser = new htmlparser2.Parser({
    onopentag(name, attributes) {
      depth += 1; maxDepth = Math.max(maxDepth, depth); elementCount += 1;
      tags.set(name, (tags.get(name) || 0) + 1);
      if (tokens.length < 10000) tokens.push(`<${name}>`);
      const marker = `${name} ${attributes.class || ''} ${attributes.id || ''} ${Object.keys(attributes).join(' ')}`.toLowerCase();
      if (/home-hero-talk|data-home-hero-talk/u.test(marker)) surfaceMarkers.add('home_hero_talk');
      if (/page-end-hero-talk|hero-talk-page-end/u.test(marker)) surfaceMarkers.add('hero_talk_page_end');
      countValue(desktopFamilies, attributes['data-desktop-family']);
      countValue(actionFamilies, attributes['data-action-family']);
      countValue(actionLayouts, attributes['data-action-layout']);
      countValue(presentationReasons, attributes['data-presentation-reason']);
      countValue(presentationFallbacks, attributes['data-presentation-fallback']);
      for (const [attribute, resource] of Object.entries(resourceAttributes)) {
        if (Object.hasOwn(attributes, attribute)) countValue(eventResourceMarkers, resource);
      }
      for (const attribute of exactStateAttributes) if (Object.hasOwn(attributes, attribute)) {
        if (!componentStateAttributes.has(attribute)) componentStateAttributes.set(attribute, new Map());
        const raw = attributes[attribute];
        const value = raw === '' ? 'present' : raw;
        if (/^(?:present|true|false|explicit|forecast|schedule_cutoff|inline|desktop-slots|top|main|secondary|organizer|source|program|pushkin|badge|pill|resolved|conflicting_source_identity|ambiguous_venue_identity|locked|eligible|found|unavailable|empty|tail|departure_board_v1|route_strips_v1|next_departure_queue_v1|[0-9]{1,3}|[a-z0-9_-]{1,48})$/u.test(value)) countValue(componentStateAttributes.get(attribute), value, { maxValues: 24, maxLength: 64 });
      }
      if (/^(header|main|nav|aside|footer|section)$/u.test(name)) regions.set(name, (regions.get(name) || 0) + 1);
      if (/^(a|button|input|select|textarea)$/u.test(name) || /button|cta|action/u.test(marker)) features.actionLike += 1;
      if (/event|sobyt/u.test(marker)) features.eventLike += 1;
      if (/^(ul|ol)$/u.test(name) || /list|listing|feed/u.test(marker)) features.listLike += 1;
      if (/^(img|picture|video|figure)$/u.test(name) || /media|gallery|poster|hero/u.test(marker)) features.mediaLike += 1;
      if (/medallion|event-token-layout|data-medallion-layout/u.test(marker)) features.medallionLike += 1;
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
    event_resources: {
      desktop_families: Object.fromEntries([...desktopFamilies].sort(([a], [b]) => a.localeCompare(b))),
      action_families: Object.fromEntries([...actionFamilies].sort(([a], [b]) => a.localeCompare(b))),
      action_layouts: Object.fromEntries([...actionLayouts].sort(([a], [b]) => a.localeCompare(b))),
      presentation_reasons: Object.fromEntries([...presentationReasons].sort(([a], [b]) => a.localeCompare(b))),
      presentation_fallbacks: Object.fromEntries([...presentationFallbacks].sort(([a], [b]) => a.localeCompare(b))),
      markers: Object.fromEntries([...eventResourceMarkers].sort(([a], [b]) => a.localeCompare(b))),
    },
    component_states: Object.fromEntries([...componentStateAttributes].sort(([a], [b]) => a.localeCompare(b)).map(([attribute, values]) => [attribute, Object.fromEntries([...values].sort(([a], [b]) => a.localeCompare(b)))])),
  };
}

function routeFromKey(key) {
  if (key === 'index.html') return '/';
  if (key.endsWith('/index.html')) return `/${key.slice(0, -11)}/`;
  return `/${key.replace(/\.html$/u, '')}`;
}
function runtimePageFamily(route) {
  const generic = route.replace(/\d{4}-\d{2}-\d{2}/gu, ':date').replace(/[0-9a-f]{8,}/giu, ':slug');
  return pageFamilyFor(generic);
}

export function manifestHtmlFiles(manifest) {
  if (!manifest || !Array.isArray(manifest.files)) throw new Error('Runtime manifest must contain an exact files inventory');
  return manifest.files.filter((file) => typeof file.key === 'string' && file.key.endsWith('.html')).sort((a, b) => a.key.localeCompare(b.key));
}

export function safeRelativeKey(key) {
  return typeof key === 'string' && key.length > 0 && Buffer.byteLength(key) <= 4096
    && !key.startsWith('/') && !key.includes('\\') && !/[\u0000-\u001f\u007f]/u.test(key)
    && key.split('/').every((part) => part && part !== '.' && part !== '..');
}

export function relativeKeyUrl(base, key) {
  if (!safeRelativeKey(key)) throw new Error('Runtime manifest contains an unsafe relative key');
  const normalizedBase = new URL(base.endsWith('/') ? base : `${base}/`);
  const encoded = key.split('/').map((part) => encodeURIComponent(part)).join('/');
  const target = new URL(encoded, normalizedBase);
  if (target.origin !== normalizedBase.origin || !target.pathname.startsWith(normalizedBase.pathname)) {
    throw new Error('Runtime manifest key escapes the configured base URL');
  }
  return target;
}

export function treeHash(files) {
  return sha256([...files].sort((a, b) => a.key.localeCompare(b.key)).map((file) => `${file.key}\0${file.sha256}\0${file.size}\n`).join(''));
}

export function validateManifestInventory(manifest) {
  if (!manifest || !Array.isArray(manifest.files) || manifest.files.length === 0) throw new Error('Runtime manifest must contain a non-empty exact files inventory');
  const keys = new Set();
  for (const file of manifest.files) {
    if (!safeRelativeKey(file.key)) throw new Error('Runtime manifest contains an unsafe relative key');
    if (keys.has(file.key)) throw new Error(`Runtime manifest contains a duplicate key: ${file.key}`);
    keys.add(file.key);
    if (!/^[0-9a-f]{64}$/u.test(file.sha256 || '') || !Number.isSafeInteger(file.size) || file.size < 0) throw new Error(`Runtime manifest file metadata is invalid: ${file.key}`);
  }
  const actual = {
    file_count: manifest.files.length,
    html_count: manifest.files.filter((file) => file.key.endsWith('.html')).length,
    page_count: manifest.files.filter((file) => /(?:\.html|\.json|\.xml|\.txt|\.ics)$/u.test(file.key)).length,
    bytes: manifest.files.reduce((sum, file) => sum + file.size, 0),
    tree_sha256: treeHash(manifest.files),
  };
  for (const field of ['file_count', 'html_count', 'page_count', 'bytes']) {
    if (manifest.counts?.[field] !== actual[field]) throw new Error(`Runtime manifest count mismatch: ${field}`);
  }
  if (manifest.tree_sha256 !== actual.tree_sha256) throw new Error('Runtime manifest tree SHA-256 mismatch');
  return actual;
}

export async function withRetry(label, operation, { attempts = 3, baseDelayMs = 100, redact = String } = {}) {
  if (!Number.isSafeInteger(attempts) || attempts < 1 || attempts > 5) throw new Error('Retry attempts must be an integer from 1 through 5');
  if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs < 0 || baseDelayMs > 5000) throw new Error('Retry base delay must be an integer from 0 through 5000ms');
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try { return await operation(attempt); } catch (error) {
      lastError = error;
      if (attempt === attempts) break;
      await new Promise((resolveDelay) => setTimeout(resolveDelay, baseDelayMs * (2 ** (attempt - 1))));
    }
  }
  throw new Error(redact(`${label} failed after ${attempts} attempts: ${lastError?.message || lastError}`));
}

async function fetchBounded(url, { accept, maxBytes, redact, label }) {
  return withRetry(label, async () => {
    const response = await fetch(url, { redirect: 'error', headers: { accept } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const declared = Number(response.headers.get('content-length') || 0);
    if (declared > maxBytes) throw new Error('content-length exceeds byte budget');
    const chunks = []; let size = 0;
    for await (const chunk of response.body) {
      size += chunk.length;
      if (size > maxBytes) throw new Error('body exceeds byte budget');
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }, { redact });
}

export async function scanLocalRuntime(runtimeRoot, manifest, parsers, maxHtmlBytes) {
  const observations = [];
  const realRoot = realpathSync(resolve(runtimeRoot));
  for (const file of manifestHtmlFiles(manifest)) {
    if (!safeRelativeKey(file.key)) throw new Error('Runtime manifest contains an unsafe relative key');
    const path = resolve(runtimeRoot, file.key);
    if (!path.startsWith(`${resolve(runtimeRoot)}${sep}`) && path !== resolve(runtimeRoot)) throw new Error('Unsafe runtime manifest key');
    const realPath = realpathSync(path);
    if (!realPath.startsWith(`${realRoot}${sep}`)) throw new Error('Runtime manifest key resolves outside the runtime root');
    const bytes = readFileSync(path);
    if (bytes.length > maxHtmlBytes) throw new Error(`HTML input exceeds per-route budget: ${file.key}`);
    if (file.sha256 && sha256(bytes) !== file.sha256) throw new Error(`Runtime file hash mismatch: ${file.key}`);
    observations.push(runtimeRecord(routeFromKey(file.key), structuralScan(bytes.toString('utf8'), parsers.htmlparser2), bytes.length, file.sha256 || sha256(bytes), { plane: 'latest_checked_kaggle_candidate', relativePath: file.key }));
  }
  return observations;
}

export async function scanRemoteRuntime(candidateBase, manifest, parsers, maxHtmlBytes, redact) {
  const observations = [];
  for (const file of manifestHtmlFiles(manifest)) {
    const bytes = await fetchBounded(relativeKeyUrl(candidateBase, file.key), { accept: 'text/html', maxBytes: maxHtmlBytes, redact, label: `Runtime route ${sha256(file.key).slice(0, 12)}` });
    if (file.sha256 && sha256(bytes) !== file.sha256) throw new Error(`Runtime file hash mismatch: ${file.key}`);
    observations.push(runtimeRecord(routeFromKey(file.key), structuralScan(bytes.toString('utf8'), parsers.htmlparser2), bytes.length, file.sha256 || sha256(bytes), { plane: 'latest_checked_kaggle_candidate', relativePath: file.key }));
  }
  return observations;
}

export async function scanPublicRoot(runtimeUrl, expectedHash, parsers, maxHtmlBytes, redact) {
  const bytes = await fetchBounded(new URL(runtimeUrl), { accept: 'text/html', maxBytes: maxHtmlBytes, redact, label: 'Public root runtime observation' });
  const actualHash = sha256(bytes);
  if (actualHash !== expectedHash) throw new Error(`Public root HTML SHA-256 mismatch: ${actualHash}`);
  return runtimeRecord('/', structuralScan(bytes.toString('utf8'), parsers.htmlparser2), bytes.length, actualHash, { plane: 'current_root_prelaunch', relativePath: '' });
}

function runtimeRecord(route, scan, bytes, contentHash, { plane, relativePath }) {
  const routeHash = sha256(route);
  const hypotheses = COVERAGE_HYPOTHESES.filter((item) => item.marker ? scan.surface_markers.includes(item.marker) : item.route.test(route)).map((item) => item.id).sort();
  return {
    id: `runtime.${plane}.${routeHash.slice(0, 16)}`, plane, route_hash: routeHash,
    route_relative_path: relativePath, route: `route:${routeHash.slice(0, 16)}`, page_family: runtimePageFamily(route),
    viewport: 'structural_all_routes', component_candidates: [], source_page_ids: [], source_mapping: 'unresolved',
    state: 'rendered_static_html', content_fixture: { html_bytes: bytes, content_sha256: contentHash },
    media_fixture: { element_count: scan.features.mediaLike }, screenshot: null,
    structure_hash: scan.structure_hash, element_count: scan.element_count, max_depth: scan.max_depth,
    major_regions: scan.major_regions, tag_counts: scan.tag_counts, feature_counts: scan.features,
    surface_hypotheses: hypotheses, surface_markers: scan.surface_markers, event_resources: scan.event_resources,
    component_states: scan.component_states,
    evidence: ['exact_manifest_file', 'streaming_structural_parse'],
  };
}

function templateRegex(template) {
  if (template === '/') return /^\/$/u;
  const escaped = template.split('/').map((part) => {
    if (!part) return '';
    if (/^:[^/]+\*$/u.test(part)) return '.*';
    if (/^:[^/]+$/u.test(part)) return '[^/]+';
    return part.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  }).join('/');
  return new RegExp(`^${escaped}$`, 'u');
}

export function mapRuntimeToSource(runtimeObservations, sourceRecords) {
  const pages = sourceRecords.filter((record) => record.type === 'page' && record.route_template);
  for (const observation of runtimeObservations) {
    const route = observation.route_relative_path ? routeFromKey(observation.route_relative_path) : '/';
    const mapped = pages.filter((page) => page.plane === observation.plane && templateRegex(page.route_template).test(route));
    observation.source_page_ids = mapped.map((page) => page.id).sort();
    observation.component_candidates = [...new Set(mapped.flatMap((page) => page.transitive_dependencies || []))].sort();
    observation.source_mapping = mapped.length === 1 ? 'exact_route_template' : mapped.length > 1 ? 'ambiguous_route_templates' : 'unresolved';
    observation.evidence.push(...(mapped.length ? ['source_route_template', 'transitive_import_graph'] : []));
  }
  return runtimeObservations;
}

function semanticCohorts(selector, property) {
  const cohorts = [];
  if (/(?:button|\bcta\b|\baction\b|\[role=["']?button)/iu.test(selector)) cohorts.push('button-action');
  if (/(?:card|listing|list-item|event-row)/iu.test(selector)) cohorts.push('card-listing');
  if (/(?:badge|label|chip|tag)/iu.test(selector)) cohorts.push('badge-label');
  if (/(?:media|hero|poster|gallery|\bimg\b|\bpicture\b|\bvideo\b)/iu.test(selector)) cohorts.push('media-hero');
  if (/^(?:font|line-height|letter-spacing|text-)/iu.test(property)) cohorts.push('typography');
  if (/(?:^|-)color$/iu.test(property)) cohorts.push('color');
  if (/^(?:margin|padding|gap|inset|top|right|bottom|left)(?:-|$)/iu.test(property)) cohorts.push('spacing');
  return [...new Set(cohorts)].sort();
}

function appendCssDeclarations(result, root, { sourceId, sourcePath, plane, blockIndex, blockStartLine }) {
  let declarationIndex = 0;
  root.walkDecls((decl) => {
    declarationIndex += 1;
    const contexts = []; let parent = decl.parent;
    while (parent && parent.type !== 'root') { if (parent.type === 'atrule') contexts.push(`@${parent.name} ${parent.params}`.trim()); parent = parent.parent; }
    const selector = decl.parent?.type === 'rule' ? decl.parent.selector : '';
    result.push({
      id: `style.${plane}.${sha256(`${sourcePath}\0${blockIndex}\0${declarationIndex}\0${decl.source?.start?.line}\0${decl.source?.start?.column}\0${decl.prop}`).slice(0, 16)}`,
      plane, source_id: sourceId, source_path: sourcePath, kind: 'source_literal_usage', property: decl.prop,
      value: decl.value, selector: selector.slice(0, 512), selector_sha256: selector ? sha256(selector) : null,
      pseudo_states: [...selector.matchAll(/:(hover|focus|focus-visible|active|disabled|checked|open|has)\b/gu)].map((match) => match[1]),
      custom_property_dependencies: [...new Set([...decl.value.matchAll(/var\(\s*(--[A-Za-z0-9_-]+)/gu)].map((match) => match[1]))].sort(),
      semantic_cohorts: semanticCohorts(selector, decl.prop), at_rule_context: contexts.reverse(), style_block_index: blockIndex,
      style_block_start_line: blockStartLine, line_in_style_block: decl.source?.start?.line ?? null,
      source_divergence: 'literal_usage_only', computed_inconsistency: 'unknown', confidence: 'high', evidence: ['postcss_ast'], recommendation: 'unresolved',
    });
  });
}

export async function styleObservations(sourceRoots, sourceRecords, parsers) {
  const result = [];
  for (const record of sourceRecords.filter((item) => item.path.endsWith('.astro'))) {
    const sourceRoot = sourceRoots[record.plane];
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
      appendCssDeclarations(result, root, { sourceId: record.id, sourcePath: record.path, plane: record.plane, blockIndex: index, blockStartLine: blocks[index].start_line });
    }
  }
  for (const [plane, sourceRoot] of Object.entries(sourceRoots).sort(([a], [b]) => a.localeCompare(b))) {
    for (const path of walk(sourceRoot, (item) => extname(item) === '.css')) {
      const rel = relative(dirname(sourceRoot), path).split(sep).join('/');
      let root;
      try { root = parsers.postcss.parse(readFileSync(path, 'utf8'), { from: rel }); } catch { continue; }
      const stylesheet = sourceRecords.find((record) => record.plane === plane && record.path === rel && record.type === 'stylesheet');
      appendCssDeclarations(result, root, { sourceId: stylesheet?.id || null, sourcePath: rel, plane, blockIndex: 0, blockStartLine: 1 });
    }
  }
  const cohorts = new Map();
  for (const observation of result) for (const cohort of observation.semantic_cohorts) {
    const key = `${observation.plane}\0${cohort}\0${observation.property}`;
    if (!cohorts.has(key)) cohorts.set(key, []);
    cohorts.get(key).push(observation);
  }
  for (const [key, observations] of cohorts) {
    const [plane, cohort, property] = key.split('\0');
    const values = [...new Set(observations.map((item) => item.value))].sort();
    result.push({
      id: `style.cohort.${sha256(key).slice(0, 16)}`, plane, kind: 'source_semantic_cohort', semantic_cohort: cohort,
      property, instances: observations.map((item) => item.id).sort(), literal_values: values,
      source_divergence: values.length > 1 ? 'distinct_literals_observed' : 'single_literal_observed',
      computed_inconsistency: 'unknown', confidence: 'medium', evidence: ['postcss_ast', 'conservative_selector_property_cohort'], recommendation: 'unresolved',
    });
  }
  return result.sort((a, b) => String(a.source_path || '').localeCompare(String(b.source_path || '')) || (a.style_block_index ?? 0) - (b.style_block_index ?? 0) || (a.line_in_style_block ?? 0) - (b.line_in_style_block ?? 0) || String(a.property || '').localeCompare(String(b.property || '')));
}

export function computedStyleObservations(viewportEvidence) {
  const groups = new Map();
  for (const item of viewportEvidence) {
    const scopes = [
      ...(item.page_family ? [{ kind: 'page_family', family: item.page_family }] : []),
      ...(item.ui_families || []).map((family) => ({ kind: 'ui_family', family })),
    ];
    for (const scope of scopes) {
      const key = `${scope.kind}\0${scope.family}`;
      if (!groups.has(key)) groups.set(key, { ...scope, observations: [] });
      groups.get(key).observations.push(item);
    }
  }
  const result = [];
  for (const [scopeKey, group] of [...groups].sort(([a], [b]) => a.localeCompare(b))) {
    const { family, kind: observationScope, observations } = group;
    for (const cohort of ['button-action', 'card-listing', 'badge-label', 'media-hero', 'typography']) {
      const instances = observations.flatMap((item) => (item.computed.cohorts?.[cohort] || []).map((value) => ({ route_hash: item.route_hash, viewport: item.viewport, ...value }))).sort((a, b) => a.route_hash.localeCompare(b.route_hash) || a.viewport.width - b.viewport.width || a.order - b.order);
      if (!instances.length) continue;
      const fingerprints = [...new Set(instances.map((item) => stableJson({ visible: item.visible, display: item.display, color: item.color, background_color: item.background_color, font_family: item.font_family, font_size: item.font_size, padding: item.padding, margin: item.margin, gap: item.gap, border_radius: item.border_radius, object_fit: item.object_fit })))];
      result.push({
        id: `style.computed.${sha256(`${scopeKey}\0${cohort}`).slice(0, 16)}`, family,
        kind: 'computed_runtime_cohort_observation', observation_scope: observationScope,
        semantic_cohort: cohort, instances,
        computed_inconsistency: fingerprints.length > 1 ? 'observed_divergence' : 'observed_consistent',
        confidence: 'high', evidence: ['playwright_computed_style', 'family_specific_element_cohort'], recommendation: 'unresolved',
      });
    }
  }
  return result;
}

const FAMILY_STYLE_COHORTS = Object.freeze({
  'family.event-representations': ['card-listing', 'media-hero'],
  'family.event-actions': ['button-action'],
  'family.button-like-actions': ['button-action'],
  'family.listing-surfaces': ['card-listing'],
  'family.media-treatments': ['media-hero'],
  'family.labels-badges': ['badge-label'],
  'family.search-results': ['card-listing'],
  'family.personal-feed': ['card-listing'],
});

function styleEvidenceForFamily(family, styles) {
  const cohorts = new Set(FAMILY_STYLE_COHORTS[family.id] || []);
  return styles.filter((style) => (
    (style.source_id && family.implementations.includes(style.source_id))
    || (style.semantic_cohort && cohorts.has(style.semantic_cohort))
    || (style.semantic_cohorts || []).some((cohort) => cohorts.has(cohort))
  ));
}

export function observedFamilies(sourceRecords, runtimeObservations) {
  return FAMILY_SEEDS.map((seed) => {
    const sources = sourceRecords.filter((record) => seed.source.test(`${record.name} ${record.path}`)).map((record) => record.id).sort();
    const runtime = runtimeObservations.filter((record) => record.component_candidates.some((id) => sources.includes(id))).map((record) => record.id).sort();
    const heuristicRuntime = runtimeObservations.filter((record) => seed.runtime(record.feature_counts)).map((record) => record.id).sort();
    const status = runtime.length && sources.length ? 'observed' : sources.length || heuristicRuntime.length ? 'candidate' : 'unknown';
    const implementationGroups = logicalImplementationGroups(sources, sourceRecords);
    const implementationsByPlane = Object.fromEntries([...new Set(sourceRecords.map((record) => record.plane))].sort().map((plane) => [plane, sources.filter((id) => sourceRecords.find((record) => record.id === id)?.plane === plane)]));
    return {
      id: seed.id, label: seed.label, implementations: sources,
      implementations_by_plane: implementationsByPlane,
      implementation_groups: implementationGroups,
      logical_implementation_count: implementationGroups.length,
      runtime_observations: runtime, heuristic_runtime_candidates: heuristicRuntime,
      status, confidence: runtime.length && sources.length ? 'high' : sources.length ? 'medium' : 'low',
      evidence_channels: [...(sources.length ? ['source_ast'] : []), ...(runtime.length ? ['exact_runtime_source_mapping'] : [])],
    };
  });
}

function logicalImplementationGroups(sourceIds, sourceRecords) {
  const byPath = new Map();
  for (const id of sourceIds) {
    const source = sourceRecords.find((record) => record.id === id);
    if (!source) continue;
    const canonicalPath = source.path.includes('/') ? source.path.slice(source.path.indexOf('/') + 1) : source.path;
    if (!byPath.has(canonicalPath)) byPath.set(canonicalPath, { path: canonicalPath, plane_paths: [], source_ids: [], planes: [], content_sha256s: [] });
    const group = byPath.get(canonicalPath);
    group.plane_paths.push({ plane: source.plane, path: source.path });
    group.source_ids.push(source.id); group.planes.push(source.plane); group.content_sha256s.push(source.content_sha256);
  }
  return [...byPath.values()].map((group) => ({
    ...group,
    plane_paths: [...new Map(group.plane_paths.map((item) => [`${item.plane}\0${item.path}`, item])).values()].sort((a, b) => a.plane.localeCompare(b.plane) || a.path.localeCompare(b.path)),
    source_ids: [...new Set(group.source_ids)].sort(),
    planes: [...new Set(group.planes)].sort(),
    content_sha256s: [...new Set(group.content_sha256s)].sort(),
    cross_plane_drift: new Set(group.content_sha256s).size > 1,
  })).sort((a, b) => a.path.localeCompare(b.path));
}

export function fragmentationReport(families, styles, sourceRecords, runtimeObservations) {
  return families.map((family) => {
    const sources = family.implementations.map((id) => sourceRecords.find((record) => record.id === id)).filter(Boolean);
    const runtime = family.runtime_observations.map((id) => runtimeObservations.find((record) => record.id === id)).filter(Boolean);
    const implementationGroups = family.implementation_groups || logicalImplementationGroups(family.implementations, sourceRecords);
    const styleIds = styleEvidenceForFamily(family, styles).map((style) => style.id).sort();
    const evidenceChannels = [...new Set([...family.evidence_channels, ...(styleIds.length ? ['source_style'] : []), ...(sources.some((source) => source.children.length) ? ['source_composition'] : [])])].sort();
    const crossPlaneDrift = implementationGroups.some((group) => group.cross_plane_drift);
    const distinctImplementationContent = new Set(implementationGroups.flatMap((group) => group.content_sha256s)).size > 1;
    return {
      id: `fragmentation.${family.id.slice(7)}`, family: family.id,
      observations: [...family.implementations, ...family.runtime_observations, ...styleIds].sort(),
      implementations_by_plane: family.implementations_by_plane,
      implementation_groups: implementationGroups,
      logical_implementation_count: implementationGroups.length,
      evidence_channels: evidenceChannels,
      similarities: implementationGroups.length > 1 ? ['seed semantic cohort', 'source naming/path evidence'] : [],
      differences: [...(distinctImplementationContent && implementationGroups.length > 1 ? ['distinct implementation content'] : []), ...(crossPlaneDrift ? ['cross-plane source drift'] : []), ...(new Set(runtime.map((item) => item.structure_hash)).size > 1 ? ['distinct runtime structures'] : [])],
      counterevidence: implementationGroups.length > 1 ? ['distinct implementations are not proof of interchangeable semantics'] : sources.length > 1 ? ['same logical source path is repeated only across independent identity planes'] : ['no duplicate source set observed'],
      unknowns: ['semantic intent', 'state equivalence', 'accessibility equivalence'], confidence: evidenceChannels.length >= 3 ? 'medium' : 'low',
      decision: 'NOT_MERGED', reason: evidenceChannels.length >= 2 ? 'multiple evidence channels identify a review candidate; counterevidence and semantic unknowns prevent merging' : 'insufficient independent evidence for a merge decision',
      recommendation: 'unresolved', status: implementationGroups.length > 1 ? 'fragmented' : family.status === 'observed' ? 'candidate' : 'unknown',
    };
  });
}

export function candidateGraph(families, sourceRecords, runtimeObservations, styles) {
  return families.map((family) => {
    const implementationGroups = family.implementation_groups || logicalImplementationGroups(family.implementations, sourceRecords);
    return {
      id: `candidate.${family.id.slice(7)}`, family: family.id, sources: family.implementations,
      implementations_by_plane: family.implementations_by_plane,
      implementation_groups: implementationGroups,
      logical_implementation_count: implementationGroups.length,
      consumers: [...new Set(family.implementations.flatMap((id) => sourceRecords.find((record) => record.id === id)?.consumers || []))].sort(),
      runtime_observations: family.runtime_observations,
      evidence_channels: [...new Set([...family.evidence_channels, ...(styleEvidenceForFamily(family, styles).length ? ['source_style'] : []), ...(family.implementations.some((id) => sourceRecords.find((record) => record.id === id)?.children.length) ? ['source_composition'] : [])])].sort(),
      similarities: implementationGroups.length > 1 ? ['bounded semantic seed match'] : [],
      differences: new Set(implementationGroups.flatMap((group) => group.content_sha256s)).size > 1 && implementationGroups.length > 1 ? ['distinct implementation content'] : implementationGroups.some((group) => group.cross_plane_drift) ? ['cross-plane source drift'] : [],
      counterevidence: ['no interchangeability contract observed'], unknowns: ['semantic contract', 'independent mobile behaviour', 'computed style consistency'], confidence: family.status === 'observed' ? 'medium' : 'low',
      status: implementationGroups.length > 1 ? 'fragmented' : family.status,
      decision: 'NOT_MERGED', recommendation: 'unresolved',
    };
  });
}

export function desktopMobile(pageFamilies, uiFamilies, viewportEvidence = []) {
  const comparisonFingerprint = (item) => stableJson({
    structure: item.structure,
    regions: Object.fromEntries(Object.entries(item.computed?.regions || {}).sort(([a], [b]) => a.localeCompare(b)).map(([name, value]) => [name, value ? { display: value.display } : null])),
    cohorts: Object.fromEntries(Object.entries(item.computed?.cohorts || {}).sort(([a], [b]) => a.localeCompare(b)).map(([cohort, values]) => [cohort, values.map((value) => ({
      order: value.order, visible: value.visible, display: value.display, font_size: value.font_size,
      padding: value.padding, margin: value.margin, gap: value.gap, border_radius: value.border_radius,
      object_fit: value.object_fit,
    }))])),
  });
  const compare = ({ id, scope, pageFamily = null, uiFamily = null, sourceImplementations = [], evidence }) => {
    const ordered = (items) => items.sort((a, b) => a.route_hash.localeCompare(b.route_hash) || a.viewport.width - b.viewport.width);
    const desktop = ordered(evidence.filter((item) => item.viewport?.width >= 1000));
    const mobile = ordered(evidence.filter((item) => item.viewport?.width < 600));
    let relation = 'unknown';
    if (desktop.length && mobile.length) {
      const desktopFingerprints = desktop.map(comparisonFingerprint);
      const mobileFingerprints = mobile.map(comparisonFingerprint);
      relation = stableJson(desktopFingerprints) === stableJson(mobileFingerprints) ? 'shared_structure_observed' : 'divergent_structure_observed';
    }
    return {
      id, scope, page_family: pageFamily, ui_family: uiFamily,
      source_implementations: sourceImplementations,
      observed_host_page_families: [...new Set(evidence.map((item) => item.page_family))].sort(),
      desktop: desktop.length ? desktop : { viewport: { width: 1728, height: 900 }, status: 'not_observed' },
      mobile: mobile.length ? mobile : { viewport: { width: 390, height: 844 }, status: 'not_observed' },
      relation, interpretation: 'independent_observations_not_responsive_variants',
      boundary_status: scope === 'ui_family' ? 'source_mapped_host_page_without_synthetic_component_wrapper' : 'page_family_observation',
      optional_breakpoint_evidence: [{ width: 430, height: 932 }, { width: 768, height: 1024 }, { width: 1280, height: 800 }],
    };
  };
  const pageRecords = pageFamilies.map((family) => compare({
    id: `desktop-mobile.page-family.${family.id.slice(12)}`, scope: 'page_family', pageFamily: family.id,
    sourceImplementations: family.source_pages || [], evidence: viewportEvidence.filter((item) => item.page_family === family.id),
  }));
  const uiRecords = uiFamilies.map((family) => compare({
    id: `desktop-mobile.ui-family.${family.id.slice(7)}`, scope: 'ui_family', uiFamily: family.id,
    sourceImplementations: family.implementations || [], evidence: viewportEvidence.filter((item) => item.ui_families?.includes(family.id)),
  }));
  return [...pageRecords, ...uiRecords].sort((a, b) => a.id.localeCompare(b.id));
}

export function screenshotIndex(pageFamilies) {
  return pageFamilies.map((family) => ({
    id: `screenshot.${family.id.slice(12)}`, page_family: family.id,
    selection: family.runtime_route_hashes.length ? 'deterministic_representative' : 'not_available',
    route_hash: family.runtime_route_hashes[0] || null, screenshot_path: null,
    viewport_status: 'not_captured_structural_scan_only', reason: 'screenshots are bounded to representatives/outliers/conflicts in an explicit browser pass',
  }));
}

export function selectScreenshotPages(byFamily, maxPages) {
  if (!Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > 200) throw new Error('browser-max-pages must be an integer from 1 through 200');
  const order = [...byFamily.keys()].sort((a, b) => {
    const ai = PRIORITIZED_PAGE_FAMILIES.indexOf(a); const bi = PRIORITIZED_PAGE_FAMILIES.indexOf(b);
    return (ai < 0 ? 999 : ai) - (bi < 0 ? 999 : bi) || a.localeCompare(b);
  });
  const representativeByFamily = new Map(); const outliersByFamily = new Map();
  for (const pageFamily of order) {
    const rows = [...byFamily.get(pageFamily)].sort((a, b) => a.observation.route_hash.localeCompare(b.observation.route_hash) || a.file.key.localeCompare(b.file.key));
    const clusters = new Map();
    for (const row of rows) {
      const hash = row.observation.structure_hash;
      if (!clusters.has(hash)) clusters.set(hash, []);
      clusters.get(hash).push(row);
    }
    const clusterOrder = [...clusters.entries()].sort(([hashA, rowsA], [hashB, rowsB]) => rowsB.length - rowsA.length || hashA.localeCompare(hashB));
    const representative = clusterOrder[0][1][0];
    representativeByFamily.set(pageFamily, { ...representative, pageFamily, selection: 'family_representative' });
    outliersByFamily.set(pageFamily, clusterOrder.slice(1).map(([, clusterRows]) => ({ ...clusterRows[0], pageFamily, selection: 'structural_outlier' })));
  }
  const selected = order.slice(0, maxPages).map((family) => representativeByFamily.get(family));
  if (selected.length < maxPages && byFamily.has('page-family.event-detail')) {
    const alreadySelected = new Set(selected.filter((row) => row.pageFamily === 'page-family.event-detail').flatMap((row) => Object.keys(row.observation.event_resources?.desktop_families || {})));
    const rows = [...byFamily.get('page-family.event-detail')].sort((a, b) => a.observation.route_hash.localeCompare(b.observation.route_hash) || a.file.key.localeCompare(b.file.key));
    const formats = new Map();
    for (const row of rows) for (const format of Object.keys(row.observation.event_resources?.desktop_families || {})) {
      if (!formats.has(format)) formats.set(format, []);
      formats.get(format).push(row);
    }
    for (const [format, formatRows] of [...formats].sort(([left], [right]) => left.localeCompare(right))) {
      if (selected.length === maxPages) break;
      if (alreadySelected.has(format)) continue;
      const clusters = new Map();
      for (const row of formatRows) {
        if (!clusters.has(row.observation.structure_hash)) clusters.set(row.observation.structure_hash, []);
        clusters.get(row.observation.structure_hash).push(row);
      }
      const modal = [...clusters.entries()].sort(([hashA, rowsA], [hashB, rowsB]) => rowsB.length - rowsA.length || hashA.localeCompare(hashB))[0][1][0];
      selected.push({ ...modal, pageFamily: 'page-family.event-detail', selection: 'resource_format_representative' });
      alreadySelected.add(format);
    }
    const selectedRouteHashes = new Set(selected.map((row) => row.observation.route_hash));
    const requiredSpecimens = [
      {
        selection: 'portrait_visual_format_representative',
        matches: (row) => Boolean(row.observation.event_resources?.presentation_reasons?.['split-portrait-or-square-visual'])
          || Boolean(row.observation.event_resources?.presentation_reasons?.['split-low-resolution-portrait-viewer']),
      },
      {
        selection: 'no_image_format_representative',
        matches: (row) => Boolean(row.observation.event_resources?.presentation_reasons?.['split-no-image-fallback']),
      },
      {
        selection: 'poster_companion_format_representative',
        matches: (row) => Boolean(row.observation.event_resources?.markers?.editorial_poster_companion_large)
          && Boolean(row.observation.event_resources?.markers?.editorial_companion_photo_preview_small),
      },
    ];
    for (const specimen of requiredSpecimens) {
      if (selected.length === maxPages) break;
      if (selected.some(specimen.matches)) continue;
      const row = rows.find((candidate) => specimen.matches(candidate) && !selectedRouteHashes.has(candidate.observation.route_hash));
      if (!row) continue;
      selected.push({ ...row, pageFamily: 'page-family.event-detail', selection: specimen.selection });
      selectedRouteHashes.add(row.observation.route_hash);
    }
  }
  for (let round = 0; selected.length < maxPages; round += 1) {
    let added = false;
    for (const family of order) {
      const row = outliersByFamily.get(family)[round];
      if (!row) continue;
      selected.push(row); added = true;
      if (selected.length === maxPages) break;
    }
    if (!added) break;
  }
  const selectedFamilies = new Set(selected.map((item) => item.pageFamily));
  const uncaptured = order.filter((family) => !selectedFamilies.has(family)).map((family) => ({
    id: `screenshot.uncaptured.${sha256(family).slice(0, 16)}`, page_family: family,
    selection: 'uncaptured', route_hash: representativeByFamily.get(family).observation.route_hash,
    screenshot_path: null, viewport_status: 'not_captured',
    reason: 'browser-max-pages is smaller than the number of page families; deterministic family representatives are selected before any outlier',
  }));
  return { selected, uncaptured };
}

export async function captureBrowserEvidence({ candidateBase, manifest, runtimeObservations, families, siteRoot, outputDir, budget, maxPages = 20, snapshotTime }) {
  if (!candidateBase) throw new Error('Browser evidence requires the candidate base URL');
  const fixedEpochMs = Date.parse(snapshotTime);
  if (!Number.isFinite(fixedEpochMs)) throw new Error('Browser evidence requires an exact snapshot time');
  const requireFromSite = createRequire(join(resolve(siteRoot), 'package.json'));
  const { chromium } = requireFromSite('playwright');
  const sharp = requireFromSite('sharp');
  const files = manifestHtmlFiles(manifest);
  if (files.length !== runtimeObservations.length) throw new Error('Browser selection cannot map runtime inventory to manifest');
  const byFamily = new Map();
  for (let index = 0; index < runtimeObservations.length; index += 1) {
    const observation = runtimeObservations[index];
    if (!byFamily.has(observation.page_family)) byFamily.set(observation.page_family, []);
    byFamily.get(observation.page_family).push({ file: files[index], observation });
  }
  const { selected, uncaptured } = selectScreenshotPages(byFamily, maxPages);
  const screenshotDir = join(outputDir, 'screenshots'); mkdirSync(screenshotDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const viewportEvidence = []; const screenshots = []; const componentEvidence = [];
  try {
    for (const selectedPage of selected) {
      for (const viewport of [{ width: 390, height: 844 }, { width: 1728, height: 900 }]) {
        const page = await browser.newPage({ viewport, deviceScaleFactor: 1, reducedMotion: 'reduce' });
        const actualViewport = page.viewportSize();
        if (actualViewport?.width !== viewport.width || actualViewport?.height !== viewport.height) {
          throw new Error(`Browser viewport contract mismatch: expected ${viewport.width}x${viewport.height}`);
        }
        const deterministicSeed = Number.parseInt(selectedPage.observation.route_hash.slice(0, 8), 16) ^ viewport.width ^ viewport.height;
        await page.addInitScript(({ fixedEpochMs: epoch, deterministicSeed: initialSeed }) => {
          const NativeDate = globalThis.Date;
          class FrozenDate extends NativeDate {
            constructor(...args) { super(...(args.length ? args : [epoch])); }
            static now() { return epoch; }
          }
          globalThis.Date = FrozenDate;
          let state = initialSeed >>> 0 || 0x9e3779b9;
          Math.random = () => {
            state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
            return (state >>> 0) / 0x100000000;
          };
        }, { fixedEpochMs, deterministicSeed });
        const target = relativeKeyUrl(candidateBase, selectedPage.file.key);
        await withRetry(`Browser route ${selectedPage.observation.route_hash.slice(0, 12)}`, () => page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 20_000 }), { attempts: 3, redact: redactFactory([candidateBase]) });
        await page.waitForLoadState('networkidle', { timeout: 20_000 });
        await page.evaluate(async () => { if (document.fonts?.ready) await document.fonts.ready; });
        await page.evaluate(async () => {
          const images = [...document.images].filter((image) => {
            const rect = image.getBoundingClientRect();
            return rect.bottom >= -innerHeight && rect.top <= innerHeight * 2;
          });
          const results = await Promise.all(images.map(async (image) => {
            image.loading = 'eager';
            return Promise.race([
              (async () => {
                if (!image.complete) await new Promise((resolve) => {
                  image.addEventListener('load', resolve, { once: true }); image.addEventListener('error', resolve, { once: true });
                });
                if (typeof image.decode === 'function') await image.decode().catch(() => undefined);
                return 'settled';
              })(),
              new Promise((resolve) => setTimeout(() => resolve('timeout'), 5_000)),
            ]);
          }));
          if (results.includes('timeout')) throw new Error('near-viewport media did not settle');
        });
        await page.addStyleTag({ content: '*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}' });
        const layoutStable = await page.evaluate(async () => {
          const fingerprint = () => JSON.stringify([
            document.documentElement.scrollWidth, document.documentElement.scrollHeight,
            ...[...document.querySelectorAll('header,main,nav,footer,button,a,img,picture,video,[class*="card"],[class*="listing"],[class*="hero"]')].slice(0, 500).flatMap((node) => {
              const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
              return [style.display, style.visibility, Math.round(rect.x), Math.round(rect.y), Math.round(rect.width), Math.round(rect.height)];
            }),
          ]);
          let previous = ''; let stableFrames = 0;
          for (let frame = 0; frame < 120; frame += 1) {
            await new Promise((resolveFrame) => requestAnimationFrame(resolveFrame));
            const current = fingerprint();
            if (current === previous) stableFrames += 1; else stableFrames = 0;
            if (stableFrames >= 5) return true;
            previous = current;
          }
          return false;
        });
        if (!layoutStable) throw new Error(`Browser layout did not stabilize: ${selectedPage.observation.route_hash.slice(0, 12)}`);
        const computed = await page.evaluate(() => {
          const geometry = (selector) => {
            const node = document.querySelector(selector); if (!node) return null;
            const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
            return { display: style.display, height: Math.round(rect.height), width: Math.round(rect.width), x: Math.round(rect.x), y: Math.round(rect.y) };
          };
          const details = (node, order) => {
            const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
            return { order, visible: style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0,
              display: style.display, geometry: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) },
              color: style.color, background_color: style.backgroundColor, font_family: style.fontFamily, font_size: style.fontSize,
              padding: style.padding, margin: style.margin, gap: style.gap, border_radius: style.borderRadius, object_fit: style.objectFit };
          };
          const cohorts = Object.fromEntries(Object.entries({
            'button-action': 'button,a,[role="button"],[class*="cta"],[class*="action"]',
            'card-listing': '[class*="card"],[class*="listing"],[class*="list-item"]',
            'badge-label': '[class*="badge"],[class*="label"],[class*="chip"],[class*="tag"]',
            'media-hero': 'img,picture,video,[class*="media"],[class*="hero"],[class*="poster"]',
            typography: 'h1,h2,h3,p',
          }).map(([cohort, selector]) => [cohort, [...document.querySelectorAll(selector)].slice(0, 12).map(details)]));
          const body = getComputedStyle(document.body);
          const tags = [...document.querySelectorAll('*')].slice(0, 20_000).map((node) => node.tagName.toLowerCase()).join('>');
          return {
            body: { background_color: body.backgroundColor, color: body.color, font_family: body.fontFamily, font_size: body.fontSize, scroll_height: document.documentElement.scrollHeight, scroll_width: document.documentElement.scrollWidth },
            regions: { footer: geometry('footer'), header: geometry('header'), main: geometry('main'), nav: geometry('nav') }, cohorts,
            structure_hash: tags,
          };
        });
        computed.structure_hash = sha256(computed.structure_hash);
        const safeFamily = selectedPage.pageFamily.replace(/[^a-z0-9-]+/giu, '-');
        const filename = `${safeFamily}-${selectedPage.observation.route_hash.slice(0, 12)}-${viewport.width}x${viewport.height}.jpg`;
        const path = join(screenshotDir, filename);
        const screenshotOptions = { type: 'jpeg', quality: 65, fullPage: false, animations: 'disabled', caret: 'hide', scale: 'css' };
        const firstScreenshot = await page.screenshot(screenshotOptions);
        const stableScreenshot = await page.screenshot(screenshotOptions);
        const differenceHash = async (bytes) => {
          const { data } = await sharp(bytes).greyscale().resize(9, 8, { fit: 'fill', kernel: 'lanczos3' }).raw().toBuffer({ resolveWithObject: true });
          let bits = '';
          for (let y = 0; y < 8; y += 1) for (let x = 0; x < 8; x += 1) bits += data[y * 9 + x] > data[y * 9 + x + 1] ? '1' : '0';
          return BigInt(`0b${bits}`).toString(16).padStart(16, '0');
        };
        const firstPerceptualDhash = await differenceHash(firstScreenshot);
        const perceptualDhash = await differenceHash(stableScreenshot);
        if (firstPerceptualDhash !== perceptualDhash) throw new Error(`Browser pixels failed perceptual two-frame stability: ${selectedPage.observation.route_hash.slice(0, 12)}`);
        writeFileSync(path, stableScreenshot);
        const size = statSync(path).size;
        if (size > MAX_SCREENSHOT_BYTES) throw new Error(`Screenshot exceeds deterministic byte reservation: ${filename}`);
        budget.claim(MAX_SCREENSHOT_BYTES, `screenshots/${filename}`);
        const relativePath = `screenshots/${filename}`;
        screenshots.push({ id: `screenshot.${sha256(`${selectedPage.observation.route_hash}\0${viewport.width}`).slice(0, 16)}`, page_family: selectedPage.pageFamily, selection: selectedPage.selection, route_hash: selectedPage.observation.route_hash, screenshot_path: relativePath, viewport,
          first_sha256: sha256(firstScreenshot), screenshot_sha256: sha256(stableScreenshot), perceptual_dhash_64: perceptualDhash,
          first_perceptual_dhash_64: firstPerceptualDhash, pixel_exact_stable: firstScreenshot.equals(stableScreenshot),
          pixel_stability: 'two_consecutive_equal_perceptual_dhash_64', raw_raster_role: 'noncanonical_visual_evidence', source: 'exact_candidate_browser' });
        componentEvidence.push(...await captureComponentScopedEvidence({
          page, pageFamily: selectedPage.pageFamily, routeHash: selectedPage.observation.route_hash,
          viewport, outputDir, budget, sharp, plane: 'latest_checked_kaggle_candidate',
        }));
        const uiFamilies = families.filter((item) => item.runtime_observations.includes(selectedPage.observation.id)).map((item) => item.id).sort();
        viewportEvidence.push({ page_family: selectedPage.pageFamily, ui_families: uiFamilies, route_hash: selectedPage.observation.route_hash, viewport, structure: computed.structure_hash, computed, screenshot_path: relativePath, selection: selectedPage.selection });
        await page.close();
      }
    }
  } finally { await browser.close(); }
  return { screenshots: [...screenshots, ...uncaptured], viewportEvidence, componentEvidence };
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
  for (const family of pageFamilies) {
    if (!family.runtime_route_hashes.length) continue;
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
  for (const name of ['fragmentation-report.jsonl', 'candidate-component-graph.jsonl', 'event-presentation-formats.jsonl']) {
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
