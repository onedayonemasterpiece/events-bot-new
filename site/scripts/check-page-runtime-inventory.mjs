#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(siteRoot, '..');

function inventoryFiles(root) {
  const out = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes:true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) {
        const relativePath = relative(root, path).split(sep).join('/');
        let kind = null;
        if (entry.name.endsWith('.html')) kind = 'html';
        else if (/^(?:data\/.+|service-share\/.+\/manifest)\.json$/u.test(relativePath)) kind = 'json-route';
        else if (entry.name.endsWith('.ics')) kind = 'ics-route';
        else if (relativePath === 'pwa-sw.js' || relativePath === 'service-worker.js') kind = 'service-worker-route';
        else if (relativePath === 'manifest.webmanifest') kind = 'webmanifest-route';
        if (kind) out.push({ file:path, kind });
      }
    }
  };
  walk(root);
  return out.sort((left, right) => left.file.localeCompare(right.file));
}

function publicPath(value) {
  const path = `/${value.split(sep).join('/')}`.replace(/\/index\.html$/u, '/').replace(/\.html$/u, '/');
  return path.replace(/^\/preview-[A-Za-z0-9._-]+(?=\/)/u, '') || '/';
}

function markupOnly(html) {
  return html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/giu, '').replace(/<style\b[^>]*>[\s\S]*?<\/style>/giu, '');
}

function attrs(markup, name) {
  const expression = new RegExp(`${name}(?:=(?:"([^"]*)"|'([^']*)'))?`, 'giu');
  return Array.from(markup.matchAll(expression), (match) => match[1] ?? match[2] ?? '');
}

function exclusion(path, html, kind) {
  if (kind !== 'html') return `explicit-non-html-${kind}`;
  if (path.startsWith('/lab/')) return 'isolated-lab-html';
  if (!/^\s*(?:<!doctype\s+html|<html\b)/iu.test(html)) return 'generated-non-html-route-json-ics-or-feed';
  return null;
}

export function buildPageRuntimeInventory(distRoot) {
  const pages = inventoryFiles(distRoot).map(({ file, kind }) => {
    const relativePath = relative(distRoot, file);
    const path = publicPath(relativePath);
    const html = readFileSync(file, 'utf8');
    const markup = markupOnly(html);
    const exclusionReason = exclusion(path, html, kind);
    const p13n = attrs(markup, 'data-p13n-runtime-marker').length;
    const auth = attrs(markup, 'data-static-site-auth-runtime').length;
    const diagnostics = attrs(markup, 'data-focus-connectivity').length;
    const staticOnly = attrs(markup, 'data-p13n-static-only-reason').filter(Boolean);
    let context = 'unclassified';
    if (exclusionReason) context = 'excluded';
    else if (auth === 1) context = 'shared-auth-resilient-transport';
    else if (diagnostics === 1) context = 'specialized-read-only-diagnostic-transport';
    else if (staticOnly.length === 1) context = 'explicit-static-only-no-auth';
    const failures = [];
    if (!exclusionReason && p13n !== 1) failures.push(`personalization_runtime_count=${p13n}`);
    if (!exclusionReason && auth > 1) failures.push(`auth_runtime_count=${auth}`);
    if (!exclusionReason && diagnostics > 1) failures.push(`diagnostic_transport_count=${diagnostics}`);
    if (!exclusionReason && context === 'unclassified') failures.push('runtime_context_unclassified');
    return {
      relative_path: relativePath.split(sep).join('/'), public_path:path,
      route_kind:kind,
      context, p13n_runtime_count:p13n, auth_runtime_count:auth,
      diagnostic_transport_count:diagnostics,
      static_only_reason:staticOnly[0] || null,
      excluded:Boolean(exclusionReason), exclusion_reason:exclusionReason,
      status:failures.length ? 'fail' : (exclusionReason ? 'excluded' : 'ok'), failures,
    };
  });
  return {
    schema_version:'page-runtime-inventory-v1',
    counts:{
      html_ok:pages.filter((page) => page.status === 'ok').length,
      shared_auth_transport:pages.filter((page) => page.context === 'shared-auth-resilient-transport').length,
      specialized_diagnostic_transport:pages.filter((page) => page.context === 'specialized-read-only-diagnostic-transport').length,
      explicit_static_only:pages.filter((page) => page.context === 'explicit-static-only-no-auth').length,
      excluded_lab_html:pages.filter((page) => page.exclusion_reason === 'isolated-lab-html').length,
      excluded_generated_non_html_html:pages.filter((page) => page.exclusion_reason === 'generated-non-html-route-json-ics-or-feed').length,
      excluded_non_html_json:pages.filter((page) => page.route_kind === 'json-route').length,
      excluded_non_html_ics:pages.filter((page) => page.route_kind === 'ics-route').length,
      excluded_service_worker:pages.filter((page) => page.route_kind === 'service-worker-route').length,
      excluded_webmanifest:pages.filter((page) => page.route_kind === 'webmanifest-route').length,
      excluded_lab_or_non_html:pages.filter((page) => page.excluded).length,
      failures:pages.filter((page) => page.status === 'fail').length,
    },
    pages,
  };
}

function valueAfter(flag, fallback) {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function main() {
  const dist = resolve(valueAfter('--dist', join(siteRoot, 'dist')));
  const output = resolve(valueAfter('--out', join(repoRoot, 'artifacts/page-runtime-inventory.json')));
  if (!existsSync(dist)) throw new Error(`runtime inventory dist not found: ${dist}`);
  const inventory = buildPageRuntimeInventory(dist);
  mkdirSync(dirname(output), { recursive:true });
  writeFileSync(output, `${JSON.stringify(inventory, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(inventory.counts)}\nartifact=${relative(repoRoot, output).split(sep).join('/')}\n`);
  if (inventory.counts.failures) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) main();
