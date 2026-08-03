import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fileInventory, treeHash } from './release-contract.mjs';
import { assertPrelaunchArtifactPolicy } from './prelaunch-build-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const root = join(siteDir, 'dist');
const manifest = JSON.parse(readFileSync(join(root, 'static-release-manifest.json'), 'utf8'));
const build = JSON.parse(readFileSync(join(root, 'production-build.json'), 'utf8'));

function fail(message) {
  throw new Error(`Prelaunch production check failed: ${message}`);
}

if (build.prelaunch_mode !== true || build.public_surface !== 'prelaunch') {
  fail('build metadata does not identify the prelaunch surface');
}
if (manifest.prelaunch_mode !== true || manifest.public_surface !== 'prelaunch') {
  fail('manifest does not identify the prelaunch surface');
}
if (manifest.build_id !== build.build_id || manifest.run_id !== build.run_id || manifest.repo_sha !== build.repo_sha) {
  fail('build/manifest identity mismatch');
}
for (const key of ['production_contract', 'catalog_parity', 'fixture_isolation', 'canonical_and_indexing', 'tree_hashes']) {
  if (manifest.checks?.[key] !== 'ok') fail(`full-catalog precursor check is not terminal: ${key}`);
}

const files = fileInventory(root, { exclude: ['static-release-manifest.json'] });
const manifestByKey = new Map(manifest.files.map((file) => [file.key, file]));
if (files.length !== manifest.files.length || manifestByKey.size !== files.length) {
  fail('tree/manifest file count mismatch');
}
for (const file of files) {
  const expected = manifestByKey.get(file.key);
  if (!expected || expected.sha256 !== file.sha256 || expected.size !== file.size || expected.content_type !== file.content_type) {
    fail(`file inventory mismatch: ${file.key}`);
  }
}
if (manifest.tree_sha256 !== treeHash(files)) fail('tree hash mismatch');
if (manifest.counts.file_count !== files.length) fail('manifest file count mismatch');
if (manifest.counts.bytes !== files.reduce((sum, file) => sum + file.size, 0)) fail('manifest byte count mismatch');

const rootHtml = readFileSync(join(root, 'index.html'), 'utf8');
if (!rootHtml.includes('data-production-root-home') || !rootHtml.includes('data-home-page') || !rootHtml.includes('data-prelaunch-page')) {
  fail('root prelaunch markers are incomplete');
}
if (!rootHtml.includes('1 сентября 2026') || !rootHtml.includes('data-prelaunch-form')) {
  fail('root launch date or notification form is missing');
}

const receipt = assertPrelaunchArtifactPolicy(root, { siteOrigin: build.site_origin });
if (receipt.htmlCount < 2 || receipt.hiddenHtmlCount !== receipt.htmlCount - 1) {
  fail('not every non-root HTML document is hidden');
}

console.log(
  `Prelaunch production check passed: ${manifest.build_id}, root indexed, ${receipt.hiddenHtmlCount} HTML pages noindex`,
);
