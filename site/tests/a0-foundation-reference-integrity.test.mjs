import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const srcRoot = path.join(siteRoot, 'src');

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(absolute));
    else if (/\.(?:astro|css|mjs|ts)$/u.test(entry.name)) files.push(absolute);
  }
  return files;
}

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

const normalizedConsumers = [
  'src/components/CollectionCatalog.astro',
  'src/components/FocusConnectivityDiagnostic.astro',
  'src/components/FocusEggArtifact.astro',
  'src/components/FocusEggCollectionCard.astro',
  'src/components/FocusEggSavedListDemo.astro',
  'src/components/FocusGroupInviteIntake.astro',
  'src/components/FocusGroupThankYou.astro',
  'src/components/InterestClubCard.astro',
  'src/components/InterestProfile.astro',
  'src/components/OptimizedEventCardGrid.astro',
  'src/components/artifacts/ArtifactCollection.astro',
  'src/pages/fokus-gruppa/index.astro',
  'src/pages/partners/index.astro',
  'src/pages/podborki/index.astro',
];

test('every A0 foundation reference is defined by actual branch source', async () => {
  const files = await sourceFiles(srcRoot);
  const definitions = new Set();
  for (const file of files) {
    const source = await readFile(file, 'utf8');
    for (const match of source.matchAll(/(--ke-[a-z0-9-]+)\s*:/giu)) definitions.add(match[1]);
  }

  const missing = [];
  for (const relativePath of normalizedConsumers) {
    const source = await read(relativePath);
    const references = new Set([...source.matchAll(/var\(\s*(--ke-[a-z0-9-]+)/giu)].map((match) => match[1]));
    for (const reference of references) {
      if (!definitions.has(reference)) missing.push(`${relativePath}: ${reference}`);
    }
  }

  assert.deepEqual(missing, [], `undefined foundation references:\n${missing.join('\n')}`);
});

test('normalized consumers use one product-contour entry point when they own visible foundation roles', async () => {
  const requiredEntryPoint = normalizedConsumers.filter((path) => !path.endsWith('OptimizedEventCardGrid.astro'));
  for (const relativePath of requiredEntryPoint) {
    const source = await read(relativePath);
    assert.match(source, /product-contour-foundations\.css/u, `${relativePath} bypasses the product-contour entry point`);
  }
});
