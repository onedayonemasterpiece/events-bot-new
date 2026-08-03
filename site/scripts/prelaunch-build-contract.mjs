import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';

export const PRELAUNCH_LAUNCH_DATE = '2026-09-01';
export const PRELAUNCH_ROBOTS_DIRECTIVE = 'noindex,nofollow,noarchive,nosnippet';

const ENABLED = /^(?:1|true|yes|on)$/iu;
const DISABLED = /^(?:0|false|no|off)$/iu;
const ROBOTS_META = /<meta\b[^>]*\bname=(['"])robots\1[^>]*>/iu;

export function resolvePrelaunchMode(env = process.env) {
  const raw = String(env.PUBLIC_PRELAUNCH_MODE ?? '').trim();
  // The public launch is scheduled for 2026-09-01. Until the explicit launch
  // release flips this flag off, production artifacts default to the holder.
  if (!raw) return true;
  if (ENABLED.test(raw)) return true;
  if (DISABLED.test(raw)) return false;
  throw new Error('PUBLIC_PRELAUNCH_MODE must be one of on/off, true/false or 1/0');
}

function normalizeOrigin(value) {
  const origin = new URL(value || 'https://kenigevents.ru');
  if (origin.protocol !== 'https:' || origin.pathname !== '/' || origin.search || origin.hash) {
    throw new Error('prelaunch siteOrigin must be one HTTPS origin');
  }
  return origin.origin;
}

function listHtmlFiles(root) {
  const result = [];
  const visit = (directory) => {
    for (const item of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, item.name);
      if (item.isDirectory()) visit(path);
      else if (item.isFile() && item.name.endsWith('.html')) result.push(path);
    }
  };
  visit(root);
  return result.sort();
}

function normalizedKey(root, path) {
  return relative(root, path).split(sep).join('/');
}

function setRobotsMeta(source, directive) {
  const replacement = `<meta name="robots" content="${directive}">`;
  if (ROBOTS_META.test(source)) return source.replace(ROBOTS_META, replacement);
  if (!/<head(?:\s[^>]*)?>/iu.test(source)) throw new Error('Cannot apply prelaunch robots policy: missing <head>');
  return source.replace(/<head(?:\s[^>]*)?>/iu, (head) => `${head}${replacement}`);
}

function rootOnlySitemap(siteOrigin) {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n  <url><loc>${siteOrigin}/</loc></url>\n</urlset>\n`;
}

function rootOnlyRobots(siteOrigin) {
  return `User-agent: *\nAllow: /$\nDisallow: /\nSitemap: ${siteOrigin}/sitemap.xml\n`;
}

export function applyPrelaunchArtifactPolicy(distDir, { enabled, siteOrigin }) {
  if (!enabled) return { enabled: false, htmlCount: 0, hiddenHtmlCount: 0 };
  const origin = normalizeOrigin(siteOrigin);
  const htmlFiles = listHtmlFiles(distDir);
  let hiddenHtmlCount = 0;

  for (const path of htmlFiles) {
    const key = normalizedKey(distDir, path);
    const root = key === 'index.html';
    const source = readFileSync(path, 'utf8');
    const next = setRobotsMeta(
      source,
      root ? 'index,follow' : PRELAUNCH_ROBOTS_DIRECTIVE,
    );
    if (next !== source) writeFileSync(path, next);
    if (!root) hiddenHtmlCount += 1;
  }

  writeFileSync(join(distDir, 'robots.txt'), rootOnlyRobots(origin));
  writeFileSync(join(distDir, 'sitemap.xml'), rootOnlySitemap(origin));
  return { enabled: true, htmlCount: htmlFiles.length, hiddenHtmlCount };
}

export function assertPrelaunchArtifactPolicy(distDir, { siteOrigin }) {
  const origin = normalizeOrigin(siteOrigin);
  const htmlFiles = listHtmlFiles(distDir);
  if (!htmlFiles.length) throw new Error('Prelaunch artifact contains no HTML');

  for (const path of htmlFiles) {
    const key = normalizedKey(distDir, path);
    const source = readFileSync(path, 'utf8');
    if (key === 'index.html') {
      if (!source.includes('data-prelaunch-page')) throw new Error('Prelaunch root marker is missing');
      if (!/<meta\s+name="robots"\s+content="index,follow">/iu.test(source)) {
        throw new Error('Prelaunch root must remain indexable');
      }
      continue;
    }
    if (!source.includes(`<meta name="robots" content="${PRELAUNCH_ROBOTS_DIRECTIVE}">`)) {
      throw new Error(`Prelaunch noindex policy missing from ${key}`);
    }
  }

  const expectedRobots = rootOnlyRobots(origin);
  if (readFileSync(join(distDir, 'robots.txt'), 'utf8') !== expectedRobots) {
    throw new Error('Prelaunch robots.txt does not allow only the root');
  }
  const sitemap = readFileSync(join(distDir, 'sitemap.xml'), 'utf8');
  const locations = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/gu)].map((match) => match[1]);
  if (locations.length !== 1 || locations[0] !== `${origin}/`) {
    throw new Error('Prelaunch sitemap must contain only the root');
  }

  return { htmlCount: htmlFiles.length, hiddenHtmlCount: Math.max(0, htmlFiles.length - 1) };
}
