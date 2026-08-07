import { copyFileSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteDir = dirname(scriptDir);
const repoRoot = dirname(siteDir);
const source = join(
  repoRoot,
  'docs',
  'features',
  'static-site-pages',
  'prelaunch-handoff',
  'reference',
  'PWA-icon.webp',
);
const targetDir = join(siteDir, 'public', 'assets', 'prelaunch');
const target = join(targetDir, 'PWA-icon.webp');

if (!existsSync(source)) {
  throw new Error(`Prelaunch artwork source is missing: ${source}`);
}
if (statSync(source).size < 10_000) {
  throw new Error(`Prelaunch artwork source is unexpectedly small: ${source}`);
}

mkdirSync(targetDir, { recursive: true });
copyFileSync(source, target);
console.log(`Prepared prelaunch artwork: ${target}`);
