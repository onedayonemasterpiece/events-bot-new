import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
const failures = [];
const evidence = {};

function check(condition, message) {
  if (!condition) failures.push(message);
}

function readJson(name) {
  const path = resolve(artifactDir, name);
  check(existsSync(path), `missing visual evidence: ${name}`);
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    failures.push(`cannot parse ${name}: ${String(error?.message || error)}`);
    return null;
  }
}

const browser = readJson('prelaunch-browser-summary.json');
const fit = readJson('prelaunch-viewport-fit-summary.json');
const light = readJson('prelaunch-light-model-summary.json');
const experience = readJson('prelaunch-experience-summary.json');

if (browser) {
  check(browser.ok === true, `browser failures: ${(browser.failures || []).join(' | ')}`);
}
if (fit) {
  check(fit.ok === true, `viewport-fit failures: ${(fit.failures || []).join(' | ')}`);
}
if (light) {
  check(light.ok === true, `light/material failures: ${(light.failures || []).join(' | ')}`);
}
if (experience) {
  check(experience.ok === true, `form-state failures: ${(experience.failures || []).join(' | ')}`);
}

const requiredScreenshots = [
  'prelaunch-reference-square-1200x1200.png',
  'prelaunch-desktop-1440x900.png',
  'prelaunch-mobile-390x844.png',
  'prelaunch-mobile-small-320x568.png',
  'prelaunch-experience-idle-390x844.png',
  'prelaunch-experience-registered-390x844.png',
];
for (const name of requiredScreenshots) {
  check(existsSync(resolve(artifactDir, name)), `missing review screenshot: ${name}`);
}

const referenceFiles = [
  'reference/generated-lighting-desktop-v1.png',
  'reference/generated-lighting-mobile-v1.png',
  'reference/target-desktop.webp',
  'reference/PWA-icon.webp',
];
for (const name of referenceFiles) {
  check(existsSync(resolve(artifactDir, name)), `missing bundled reference: ${name}`);
}

evidence.browser = browser?.ok ?? null;
evidence.viewportFit = fit?.ok ?? null;
evidence.lightMaterial = light?.ok ?? null;
evidence.formStates = experience?.ok ?? null;
evidence.requiredScreenshots = requiredScreenshots;
evidence.referenceFiles = referenceFiles;

const result = {
  schema_version: 'prelaunch_product_visual_policy_v4',
  ok: failures.length === 0,
  artifact_dir: artifactDir,
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-product-visual-policy.json'), `${JSON.stringify(result, null, 2)}\n`);
console.log(JSON.stringify(result, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch product visual policy failed:\n- ${failures.join('\n- ')}`);
}
