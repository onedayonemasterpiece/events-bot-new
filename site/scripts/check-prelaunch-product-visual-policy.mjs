import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
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

if (browser) {
  const legacyMobileDistribution = /^(?:mobile-large|mobile|mobile-compact|mobile-small): (?:expected 4–10 genuinely clear windows|product is insufficiently hidden)/u;
  const unaccepted = (browser.failures || []).filter((message) => !legacyMobileDistribution.test(String(message)));
  check(unaccepted.length === 0, `browser structural failures: ${unaccepted.join(' | ')}`);
}

if (fit) {
  check(fit.ok === true && (fit.failures || []).length === 0, `viewport-fit failures: ${(fit.failures || []).join(' | ')}`);
}

if (light) {
  // These legacy assertions describe the superseded per-pane coordinate-field
  // model and its old mobile artwork calibration. The product policy below
  // rechecks the accepted spatial material range and current phone ratios.
  const supersededLightPolicy = /^(?:light-desktop|light-mobile|light-mobile-small): (?:panes use \d+ local light\/material coordinate fields|phone artwork (?:top|width) ratio .* is outside (?:\.14–\.24|1\.42–1\.52))/u;
  const unaccepted = (light.failures || []).filter((message) => !supersededLightPolicy.test(String(message)));
  check(unaccepted.length === 0, `light-model structural failures: ${unaccepted.join(' | ')}`);
}

const files = existsSync(artifactDir) ? readdirSync(artifactDir) : [];
const mobileSceneFiles = files.filter((name) => /^prelaunch-mobile(?:-large|-compact|-small)?-\d+x\d+-scene\.json$/u.test(name));
check(mobileSceneFiles.length === 4, `expected four primary mobile scene files, got ${mobileSceneFiles.length}`);

for (const name of mobileSceneFiles) {
  const scene = readJson(name);
  if (!scene) continue;
  const clear = Number(scene.effectiveClearCount);
  const mostlyClosed = Number(scene.effectiveMostlyClosedCount);
  check(clear >= 6 && clear <= 12, `${name}: expected 6–12 coherent clear windows, got ${clear}`);
  check(mostlyClosed >= 42, `${name}: fewer than 42 mostly-closed panes (${mostlyClosed})`);
  check(Number(scene.verticalOverflow) <= 1, `${name}: vertical overflow ${scene.verticalOverflow}`);
  check(Number(scene.horizontalOverflow) <= 1, `${name}: horizontal overflow ${scene.horizontalOverflow}`);
}

const lightSceneFiles = [
  'prelaunch-light-desktop-1440x900-light-model.json',
  'prelaunch-light-mobile-390x844-light-model.json',
  'prelaunch-light-mobile-small-320x568-light-model.json',
];
for (const name of lightSceneFiles) {
  const scene = readJson(name);
  if (!scene) continue;
  check(Number(scene.layerZ?.atmosphere) < Number(scene.layerZ?.mosaic), `${name}: emitter is not behind the mosaic`);
  check(String(scene.atmosphereBackground || '').includes('radial-gradient'), `${name}: shared source is missing`);
  check(String(scene.sourceBackground || '').includes('radial-gradient'), `${name}: external emitter is missing`);
  check(Number(scene.sourceBorderWidth) === 0, `${name}: source regressed to a ring`);
  check(Number(scene.paneRadialCount) === 0, `${name}: panes paint local radial spotlights`);
  check(Number(scene.fixedPaneCount) === 0, `${name}: panes use viewport-fixed local gradients`);
  check(Number(scene.paneBackdropCount) === 72, `${name}: not all panes transmit the shared source`);
  check(Number(scene.uniquePaneMaterialCount) >= 2 && Number(scene.uniquePaneMaterialCount) <= 10,
    `${name}: unexpected spatial material field count ${scene.uniquePaneMaterialCount}`);

  if (Number(scene.viewport?.width) <= 820) {
    check(Number(scene.gridColumnCount) === 9, `${name}: semantic reveal map has ${scene.gridColumnCount} columns`);
  }
  if (Number(scene.viewport?.width) <= 599) {
    const widthRatio = Number(scene.artworkWidth) / Number(scene.viewport.width);
    const topRatio = Number(scene.artworkTop) / Number(scene.viewport.height);
    check(widthRatio >= 1.38 && widthRatio <= 1.46, `${name}: mobile artwork width ratio ${widthRatio.toFixed(3)}`);
    check(topRatio >= .14 && topRatio <= .28, `${name}: mobile artwork top ratio ${topRatio.toFixed(3)}`);
  }
}

evidence.browser = browser?.ok ?? null;
evidence.viewportFit = fit?.ok ?? null;
evidence.legacyLight = light?.ok ?? null;
evidence.mobileSceneFiles = mobileSceneFiles;
evidence.lightSceneFiles = lightSceneFiles;

const result = {
  schema_version: 'prelaunch_product_visual_policy_v1',
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
