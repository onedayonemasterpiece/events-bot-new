#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const SITE_ROOT = process.cwd();
const CONTRACT_PATH = path.join(SITE_ROOT, 'src/data/design-system-iconography-contract.v1.json');
const args = process.argv.slice(2);

function valueAfter(flag) {
  const index = args.indexOf(flag);
  return index >= 0 ? args[index + 1] : null;
}

const strict = args.includes('--strict') || args.includes('--strict-production');
const outPath = path.resolve(SITE_ROOT, valueAfter('--out') || 'artifacts/design-system/iconography-inventory.json');
const distArg = valueAfter('--dist');
const distRoot = distArg ? path.resolve(SITE_ROOT, distArg) : null;

const normalize = (value) => value.split(path.sep).join('/');
const rel = (absolutePath) => normalize(path.relative(SITE_ROOT, absolutePath));
const uniqueSorted = (values) => [...new Set(values)].sort((a, b) => a.localeCompare(b, 'en'));
const sha256 = (value) => createHash('sha256').update(value).digest('hex');

async function walk(root) {
  if (!root || !existsSync(root)) return [];
  const output = [];
  const entries = await readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(root, entry.name);
    if (entry.isDirectory()) output.push(...await walk(full));
    else if (entry.isFile()) output.push(full);
  }
  return output;
}

function extractLiteralUnion(source, propertyName) {
  const match = source.match(new RegExp(`${propertyName}\\s*:\\s*([^;]+);`, 'u'));
  if (!match) return [];
  return uniqueSorted([...match[1].matchAll(/['"]([^'"]+)['"]/gu)].map((item) => item[1]));
}

function staticComponentNames(source, tagName) {
  const names = [];
  const dynamic = [];
  const tagPattern = new RegExp(`<${tagName}\\b[\\s\\S]*?>`, 'gu');
  for (const tag of source.matchAll(tagPattern)) {
    const text = tag[0];
    const literal = text.match(/\bname\s*=\s*(?:['"]([^'"]+)['"]|\{\s*['"]([^'"]+)['"]\s*\})/u);
    if (literal) names.push(literal[1] || literal[2]);
    else if (/\bname\s*=/u.test(text)) dynamic.push(text.replace(/\s+/gu, ' ').slice(0, 240));
  }
  return { names, dynamic };
}

function referencedSvgPaths(source) {
  const values = [];
  const pattern = /(?:['"(])([^'"()\s]+\.svg(?:\?[^'"()\s]*)?)/gu;
  for (const match of source.matchAll(pattern)) values.push(match[1]);
  return values;
}

function collectionContains(collection, siteRelativePath) {
  const root = normalize(collection.root).replace(/^site\//u, '');
  if (!siteRelativePath.startsWith(`${root}/`)) return false;
  const local = siteRelativePath.slice(root.length + 1);
  return collection.expected_svg_files.includes(local);
}

function linkedBrandAsset(siteRelativePath) {
  return siteRelativePath === 'public/favicon.svg'
    || siteRelativePath === 'public/brand-mark.svg'
    || siteRelativePath.startsWith('public/brand/')
    || siteRelativePath.startsWith('public/assets/pwa/');
}

const failures = [];
const warnings = [];

const contractText = await readFile(CONTRACT_PATH, 'utf8');
const contract = JSON.parse(contractText);
const canonicalById = new Map(contract.canonical_components.map((component) => [component.id, component]));
const uiContract = canonicalById.get('icon.ui');
const socialContract = canonicalById.get('icon.social');
if (!uiContract || !socialContract) failures.push('Contract must define icon.ui and icon.social canonical components.');

const uiSourcePath = path.join(SITE_ROOT, uiContract?.source?.replace(/^site\//u, '') || 'src/components/Icon.astro');
const socialSourcePath = path.join(SITE_ROOT, socialContract?.source?.replace(/^site\//u, '') || 'src/components/SocialIcon.astro');
const uiSource = await readFile(uiSourcePath, 'utf8');
const socialSource = await readFile(socialSourcePath, 'utf8');
const uiNames = extractLiteralUnion(uiSource, 'name');
const socialNames = extractLiteralUnion(socialSource, 'name');
const expectedUiNames = uniqueSorted(uiContract?.names || []);
const expectedSocialNames = uniqueSorted(socialContract?.names || []);

if (JSON.stringify(uiNames) !== JSON.stringify(expectedUiNames)) {
  failures.push(`Icon.astro name union differs from contract. source=${JSON.stringify(uiNames)} contract=${JSON.stringify(expectedUiNames)}`);
}
if (JSON.stringify(socialNames) !== JSON.stringify(expectedSocialNames)) {
  failures.push(`SocialIcon.astro name union differs from contract. source=${JSON.stringify(socialNames)} contract=${JSON.stringify(expectedSocialNames)}`);
}

const expectedAssetPaths = [];
const attributionPaths = [];
for (const collection of contract.asset_collections) {
  for (const file of collection.expected_svg_files || []) {
    const expected = normalize(path.join(collection.root.replace(/^site\//u, ''), file));
    expectedAssetPaths.push(expected);
    if (!existsSync(path.join(SITE_ROOT, expected))) failures.push(`Missing icon asset: site/${expected}`);
  }
  if (collection.attribution) {
    const attribution = normalize(collection.attribution.replace(/^site\//u, ''));
    attributionPaths.push(attribution);
    if (!existsSync(path.join(SITE_ROOT, attribution))) failures.push(`Missing attribution/provenance file: site/${attribution}`);
  }
}

const sourceFiles = (await walk(path.join(SITE_ROOT, 'src')))
  .filter((file) => /\.(astro|css|js|mjs|ts|tsx|jsx|json)$/u.test(file));
const sourceRecords = [];
const uiConsumers = new Map(expectedUiNames.map((name) => [name, []]));
const socialConsumers = new Map(expectedSocialNames.map((name) => [name, []]));
const dynamicUiConsumers = [];
const dynamicSocialConsumers = [];
const inlineSvgOutsideCanonical = [];
const svgReferences = [];

for (const file of sourceFiles) {
  const source = await readFile(file, 'utf8');
  const relative = rel(file);
  const inlineSvgCount = (source.match(/<svg\b/gu) || []).length;
  const uiUsage = staticComponentNames(source, 'Icon');
  const socialUsage = staticComponentNames(source, 'SocialIcon');
  const fileSvgReferences = referencedSvgPaths(source);

  for (const name of uiUsage.names) {
    if (!uiConsumers.has(name)) uiConsumers.set(name, []);
    uiConsumers.get(name).push(relative);
  }
  for (const name of socialUsage.names) {
    if (!socialConsumers.has(name)) socialConsumers.set(name, []);
    socialConsumers.get(name).push(relative);
  }
  if (uiUsage.dynamic.length) dynamicUiConsumers.push({ source: relative, expressions: uiUsage.dynamic });
  if (socialUsage.dynamic.length) dynamicSocialConsumers.push({ source: relative, expressions: socialUsage.dynamic });
  if (inlineSvgCount && file !== uiSourcePath && file !== socialSourcePath) {
    inlineSvgOutsideCanonical.push({ source: relative, count: inlineSvgCount });
  }
  for (const reference of fileSvgReferences) svgReferences.push({ source: relative, reference });
  sourceRecords.push({
    source: relative,
    inlineSvgCount,
    uiIcons: uniqueSorted(uiUsage.names),
    socialIcons: uniqueSorted(socialUsage.names),
    svgReferences: uniqueSorted(fileSvgReferences),
  });
}

const publicFiles = await walk(path.join(SITE_ROOT, 'public'));
const publicSvgFiles = publicFiles.filter((file) => file.endsWith('.svg')).map(rel);
const iconCandidateSvgFiles = publicSvgFiles.filter((file) =>
  file.startsWith('public/assets/icons/')
  || file.startsWith('public/assets/transport/')
  || file.startsWith('public/assets/social/'));
const classifiedAssetFiles = iconCandidateSvgFiles.filter((file) =>
  contract.asset_collections.some((collection) => collectionContains(collection, file)));
const unclassifiedIconAssets = iconCandidateSvgFiles.filter((file) => !classifiedAssetFiles.includes(file));
const linkedBrandAssets = publicSvgFiles.filter(linkedBrandAsset);
const otherSvgVisualAssets = publicSvgFiles.filter((file) =>
  !iconCandidateSvgFiles.includes(file) && !linkedBrandAssets.includes(file));

const usedUiNames = uniqueSorted([...uiConsumers.entries()].filter(([, consumers]) => consumers.length).map(([name]) => name));
const unusedUiNames = expectedUiNames.filter((name) => !usedUiNames.includes(name));
const usedSocialNames = uniqueSorted([...socialConsumers.entries()].filter(([, consumers]) => consumers.length).map(([name]) => name));
const unusedSocialNames = expectedSocialNames.filter((name) => !usedSocialNames.includes(name));

const distTextFiles = distRoot
  ? (await walk(distRoot)).filter((file) => /\.(html|css|js|json|webmanifest|xml|txt)$/u.test(file))
  : [];
const distText = (await Promise.all(distTextFiles.map(async (file) => {
  try { return await readFile(file, 'utf8'); } catch { return ''; }
}))).join('\n');
const productionReferencedAssets = expectedAssetPaths.filter((assetPath) => {
  const publicPath = `/${assetPath.replace(/^public\//u, '')}`;
  return distText.includes(publicPath) || distText.includes(path.basename(assetPath));
});

for (const record of inlineSvgOutsideCanonical) {
  warnings.push(`Inline SVG outside canonical icon components: ${record.source} (${record.count})`);
}
for (const asset of unclassifiedIconAssets) warnings.push(`Unclassified icon candidate asset: site/${asset}`);
for (const name of unusedUiNames) warnings.push(`Icon.astro name has no static source consumer: ${name}`);
for (const name of unusedSocialNames) warnings.push(`SocialIcon.astro name has no static source consumer: ${name}`);
if (dynamicUiConsumers.length) warnings.push(`${dynamicUiConsumers.length} files contain dynamic <Icon name=...> usage.`);
if (dynamicSocialConsumers.length) warnings.push(`${dynamicSocialConsumers.length} files contain dynamic <SocialIcon name=...> usage.`);

const gaps = {
  inlineSvgOutsideCanonical,
  unclassifiedIconAssets,
  unusedUiNames,
  unusedSocialNames,
  dynamicUiConsumers,
  dynamicSocialConsumers,
};

if (strict) {
  if (inlineSvgOutsideCanonical.length) failures.push(`${inlineSvgOutsideCanonical.length} source files contain raw inline SVG outside Icon.astro/SocialIcon.astro.`);
  if (unclassifiedIconAssets.length) failures.push(`${unclassifiedIconAssets.length} public icon assets are not classified by the iconography contract.`);
  if (unusedUiNames.length) failures.push(`${unusedUiNames.length} canonical UI icons have no statically detected source consumer.`);
  if (unusedSocialNames.length) failures.push(`${unusedSocialNames.length} canonical social icons have no statically detected source consumer.`);
  if (distRoot && productionReferencedAssets.length === 0) failures.push('No classified external icon asset was detected in the supplied production artifact.');
}

const report = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  strict,
  contract: {
    path: rel(CONTRACT_PATH),
    id: contract.contract_id,
    sha256: sha256(contractText),
  },
  source: {
    uiComponent: rel(uiSourcePath),
    socialComponent: rel(socialSourcePath),
    uiNames,
    socialNames,
  },
  counts: {
    uiIconNames: uiNames.length,
    socialIconNames: socialNames.length,
    expectedExternalSvgAssets: expectedAssetPaths.length,
    publicSvgFiles: publicSvgFiles.length,
    classifiedIconAssets: classifiedAssetFiles.length,
    unclassifiedIconAssets: unclassifiedIconAssets.length,
    inlineSvgOutsideCanonicalFiles: inlineSvgOutsideCanonical.length,
    sourceFilesScanned: sourceFiles.length,
    productionReferencedExternalAssets: productionReferencedAssets.length,
  },
  consumers: {
    ui: Object.fromEntries([...uiConsumers.entries()].map(([name, consumers]) => [name, uniqueSorted(consumers)])),
    social: Object.fromEntries([...socialConsumers.entries()].map(([name, consumers]) => [name, uniqueSorted(consumers)])),
  },
  assets: {
    expected: uniqueSorted(expectedAssetPaths),
    classified: uniqueSorted(classifiedAssetFiles),
    unclassifiedIconCandidates: uniqueSorted(unclassifiedIconAssets),
    linkedBrandAssets: uniqueSorted(linkedBrandAssets),
    otherSvgVisualAssets: uniqueSorted(otherSvgVisualAssets),
    productionReferenced: uniqueSorted(productionReferencedAssets),
    attribution: uniqueSorted(attributionPaths),
  },
  gaps,
  warnings: uniqueSorted(warnings),
  failures: uniqueSorted(failures),
  pass: failures.length === 0,
};

await mkdir(path.dirname(outPath), { recursive: true });
await writeFile(outPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

console.log(`Iconography contract: ${report.pass ? 'PASS' : 'FAIL'}`);
console.log(`UI icons: ${report.counts.uiIconNames}; social icons: ${report.counts.socialIconNames}; classified SVG assets: ${report.counts.classifiedIconAssets}`);
console.log(`Gaps: inline SVG files=${report.counts.inlineSvgOutsideCanonicalFiles}; unclassified icon assets=${report.counts.unclassifiedIconAssets}`);
console.log(`Report: ${outPath}`);
for (const warning of report.warnings) console.warn(`WARN: ${warning}`);
for (const failure of report.failures) console.error(`ERROR: ${failure}`);

if (!report.pass) process.exit(1);
