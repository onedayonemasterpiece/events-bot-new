#!/usr/bin/env node
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(siteRoot, '..');
const layoutPath = join(siteRoot, 'src/layouts/EventLayout.astro');
const personalizationRoot = join(siteRoot, 'src/lib/personalization');

export const LEGACY_INLINE_BASELINE_V1 = Object.freeze({
  rankScoreFunctions: Object.freeze([
    'rankAdjacentContinuationCandidates',
    'rankEventDetailRelated',
    'rankPersonalFeedCandidates',
    'rankPopularFallbackCandidates',
    'scoreRelatedCandidate',
  ]),
  personalizationKeys: Object.freeze([
    'ke_personalization_last_reset_at_v1',
    'ke_personalization_profile',
  ]),
  consentOkOccurrences: 2,
  localStorageSetItemOccurrences: 5,
  directRpcPathOccurrences: 1,
});

function filesBelow(root) {
  if (!existsSync(root)) return [];
  const result = [];
  for (const entry of readdirSync(root)) {
    const path = join(root, entry);
    if (statSync(path).isDirectory()) result.push(...filesBelow(path));
    else result.push(path);
  }
  return result;
}

function occurrences(source, expression) {
  return Array.from(source.matchAll(expression)).length;
}

export function personalizationSourceGuardFailures() {
  const failures = [];
  const layout = readFileSync(layoutPath, 'utf8');
  const rankScoreFunctions = Array.from(layout.matchAll(/function\s+((?:rank|score)[A-Za-z0-9_]*)\s*\(/gu), (match) => match[1]).sort();
  if (JSON.stringify(rankScoreFunctions) !== JSON.stringify([...LEGACY_INLINE_BASELINE_V1.rankScoreFunctions].sort())) {
    failures.push(`EventLayout rank/score baseline changed: ${rankScoreFunctions.join(',')}`);
  }
  const keys = Array.from(new Set(Array.from(layout.matchAll(/['"](ke_personalization_[A-Za-z0-9:_-]+)['"]/gu), (match) => match[1]))).sort();
  if (JSON.stringify(keys) !== JSON.stringify([...LEGACY_INLINE_BASELINE_V1.personalizationKeys].sort())) {
    failures.push(`EventLayout personalization key baseline changed: ${keys.join(',')}`);
  }
  const consentCount = occurrences(layout, /\bconsent_ok\b/gu);
  if (consentCount !== LEGACY_INLINE_BASELINE_V1.consentOkOccurrences) failures.push(`inline consent_ok baseline changed: ${consentCount}`);
  const localWrites = occurrences(layout, /localStorage\.setItem\s*\(/gu);
  if (localWrites !== LEGACY_INLINE_BASELINE_V1.localStorageSetItemOccurrences) failures.push(`inline localStorage.setItem baseline changed: ${localWrites}`);
  const rpcPaths = occurrences(layout, /\/rest\/v1\/rpc\//gu);
  if (rpcPaths !== LEGACY_INLINE_BASELINE_V1.directRpcPathOccurrences) failures.push(`inline personalization RPC baseline changed: ${rpcPaths}`);

  const targetScorer = join(personalizationRoot, 'scorer.ts');
  const targetModel = join(personalizationRoot, 'model.ts');
  if (existsSync(targetScorer)) failures.push('target scorer.ts is reserved and must not exist in P13N-00');
  if (existsSync(targetModel)) failures.push('target model.ts is reserved and must not exist in P13N-00');

  for (const file of filesBelow(personalizationRoot)) {
    if (!/\.(?:ts|js|mjs)$/u.test(file)) continue;
    const relativePath = relative(repoRoot, file).split(sep).join('/');
    const source = readFileSync(file, 'utf8');
    const legacyImport = /(?:from\s+|import\s*\()['"][^'"]*legacy\/scorer-v1(?:\.ts)?['"]/u.test(source);
    const allowed = relativePath.endsWith('/legacy/scorer-v1.ts') || /legacy_characterization/u.test(relativePath);
    if (legacyImport && !allowed) failures.push(`legacy scorer imported from target path: ${relativePath}`);
    if (!relativePath.includes('/legacy/') && !relativePath.endsWith('/contract.ts') && /\bconsent_ok\b/u.test(source)) failures.push(`consent_ok leaked outside legacy namespace: ${relativePath}`);
    if (!relativePath.includes('/legacy/') && /ke_personalization_[A-Za-z0-9:_-]+/u.test(source)) failures.push(`legacy storage key leaked outside legacy namespace: ${relativePath}`);
    if (!relativePath.includes('/legacy/') && /(?:localStorage\.setItem|\/rest\/v1\/rpc\/|supabase|\bfetch\s*\()/iu.test(source)) {
      failures.push(`target P13N-00 module gained storage/network behavior: ${relativePath}`);
    }
  }
  return failures;
}

const failures = personalizationSourceGuardFailures();
if (failures.length) {
  process.stderr.write(`${failures.join('\n')}\n`);
  process.exitCode = 1;
} else {
  process.stdout.write('personalization source guard: PASS\n');
}
