#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const m0Root = join(scriptRoot, "..");
const candidatesRoot = join(m0Root, "candidates");
const releaseRoot = join(m0Root, "release-m0");

const expected = {
  "current-control": {
    node: "22.12.0",
    playwright: "1.61.1",
    browserRevision: "1228",
    browserVersion: "149.0.7827.55",
    executable: "browsers/chromium-1228/chrome-win64/chrome.exe",
    afterBoundary: true,
  },
  "pre-cft-compat": {
    node: "22.12.0",
    playwright: "1.54.2",
    browserRevision: "1181",
    browserVersion: "139.0.7258.5",
    executable: "browsers/chromium-1181/chrome-win/chrome.exe",
    afterBoundary: false,
  },
};

const expectedNodeArchiveSha256 =
  "2b8f2256382f97ad51e29ff71f702961af466c4616393f767455501e6aece9b8";

async function json(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

async function sha256(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

const candidateIds = (
  await readdir(candidatesRoot, { withFileTypes: true })
)
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort();
assert.deepEqual(candidateIds, Object.keys(expected).sort());

for (const candidateId of candidateIds) {
  const wanted = expected[candidateId];
  const candidateRoot = join(candidatesRoot, candidateId);
  const manifest = await json(join(candidateRoot, "candidate.json"));
  const packageJson = await json(join(candidateRoot, "package.json"));
  const packageLock = await json(join(candidateRoot, "package-lock.json"));

  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.id, candidateId);
  assert.equal(manifest.target.os, "windows");
  assert.equal(manifest.target.architecture, "x64");
  assert.equal(manifest.node.version, wanted.node);
  assert.equal(manifest.node.distribution, "win-x64");
  assert.equal(manifest.node.sha256, expectedNodeArchiveSha256);
  assert.equal(manifest.playwright.version, wanted.playwright);
  assert.equal(
    manifest.playwright.packageLockSha256,
    await sha256(join(candidateRoot, "package-lock.json")),
  );
  assert.equal(manifest.playwright.corePackage.version, wanted.playwright);
  assert.equal(
    manifest.playwright.boundary.playwright157OrNewer,
    wanted.afterBoundary,
  );
  assert.equal(manifest.managedBrowser.name, "chromium");
  assert.equal(manifest.managedBrowser.revision, wanted.browserRevision);
  assert.equal(manifest.managedBrowser.version, wanted.browserVersion);
  assert.equal(
    manifest.managedBrowser.executableRelativePath,
    wanted.executable,
  );
  assert.deepEqual(manifest.managedBrowser.executableSha256, {
    resolution: "computed-from-packaged-executable-at-build",
    recordedIn: "VERSIONS.json",
  });
  assert.deepEqual(manifest.launch, {
    headless: false,
    browserChannel: null,
    arguments: [],
    profileModes: ["fresh", "persistent"],
    viewport: { width: 430, height: 932 },
  });

  assert.equal(manifest.runtimePolicy.playwrightBrowsersPath, "browsers");
  assert.equal(manifest.runtimePolicy.browserSource, "packaged-playwright-managed");
  assert.equal(manifest.runtimePolicy.downloadAllowedAtRuntime, false);
  assert.equal(manifest.runtimePolicy.allowGlobalBrowserCache, false);
  assert.equal(manifest.runtimePolicy.allowSystemBrowser, false);
  assert.equal(manifest.runtimePolicy.allowBrowserChannel, false);
  assert.equal(manifest.runtimePolicy.allowNpmOrNpxAtRuntime, false);
  assert.equal(manifest.runtimePolicy.requiresAdministrator, false);
  assert.equal(manifest.runtimePolicy.changesExecutionPolicy, false);

  assert.equal(packageJson.engines.node, wanted.node);
  assert.deepEqual(Object.keys(packageJson.dependencies), ["playwright"]);
  assert.equal(packageJson.dependencies.playwright, wanted.playwright);
  assert.equal(packageLock.lockfileVersion, 3);
  assert.equal(
    packageLock.packages[""].dependencies.playwright,
    wanted.playwright,
  );
  assert.equal(
    packageLock.packages["node_modules/playwright"].version,
    wanted.playwright,
  );
  assert.equal(
    packageLock.packages["node_modules/playwright"].integrity,
    manifest.playwright.package.integrity,
  );
  assert.equal(
    packageLock.packages["node_modules/playwright-core"].version,
    wanted.playwright,
  );
  assert.equal(
    packageLock.packages["node_modules/playwright-core"].integrity,
    manifest.playwright.corePackage.integrity,
  );
}

for (const templateName of ["start.cmd.in", "self-test.cmd.in"]) {
  const template = await readFile(
    join(releaseRoot, "templates", templateName),
    "utf8",
  );
  assert.match(template, /set "AP_ROOT=%~dp0"/);
  assert.match(
    template,
    /set "PLAYWRIGHT_BROWSERS_PATH=%AP_ROOT%browsers"/,
  );
  assert.match(template, /set "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1"/);
  assert.match(template, /if not exist "%AP_BROWSER%"/);
  assert.match(template, /--portable-root "%AP_ROOT%"/);
  assert.match(template, /--browser-executable "%AP_BROWSER_REL%"/);
  assert.match(template, /--portable-browsers-root "browsers"/);
  assert.doesNotMatch(template, /\b(?:npm|npx)\b/i);
  assert.doesNotMatch(template, /--channel|\bmsedge\b/i);
  assert.doesNotMatch(template, /powershell|executionpolicy|runas/i);
}

const pathMatrixTemplate = await readFile(
  join(releaseRoot, "templates", "path-matrix.cmd.in"),
  "utf8",
);
assert.match(pathMatrixTemplate, /set "AP_ROOT=%~dp0"/);
assert.match(pathMatrixTemplate, /set "PLAYWRIGHT_BROWSERS_PATH=%AP_ROOT%browsers"/);
assert.match(pathMatrixTemplate, /set "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1"/);
assert.match(pathMatrixTemplate, /app\\reporting\\run-path-matrix\.js/);
assert.doesNotMatch(pathMatrixTemplate, /\b(?:npm|npx)\b/i);

const startTemplate = await readFile(
  join(releaseRoot, "templates", "start.cmd.in"),
  "utf8",
);
assert.match(startTemplate, /app\\src\\run-suite\.js/);
assert.match(startTemplate, /--candidate-id "%AP_CANDIDATE_ID%"/);
assert.match(startTemplate, /--output-dir "logs\\m0"/);
assert.match(startTemplate, /--profile-root "data\\m0-profiles"/);
assert.match(startTemplate, /--system-info "evidence\\SYSTEM-INFO\.json"/);
assert.match(startTemplate, /set \/p "AP_LIVE_URL=/);
assert.match(startTemplate, /--live-url "%AP_LIVE_URL%"/);
assert.match(startTemplate, /--live-click-selector "%AP_CLICK_SELECTOR%"/);
assert.match(startTemplate, /--live-success-selector "%AP_SUCCESS_SELECTOR%"/);

const selfTestTemplate = await readFile(
  join(releaseRoot, "templates", "self-test.cmd.in"),
  "utf8",
);
assert.match(selfTestTemplate, /app\\src\\self-test\.js/);
assert.match(selfTestTemplate, /--versions-file "VERSIONS\.json"/);
assert.match(selfTestTemplate, /--data-dir "data"/);
assert.match(selfTestTemplate, /--logs-dir "logs"/);

const builder = await readFile(join(scriptRoot, "build-candidate.ps1"), "utf8");
assert.match(builder, /\$env:PLAYWRIGHT_BROWSERS_PATH = \$releaseBrowsersRoot/);
assert.match(
  builder,
  /Copy-Item -LiteralPath \$fixtureSourcePath -Destination \(Join-Path \$releaseAppRoot 'fixture'\) -Recurse/,
);
assert.match(
  builder,
  /Copy-Item -LiteralPath \$reportingSourcePath -Destination \(Join-Path \$releaseAppRoot 'reporting'\) -Recurse/,
);
assert.match(builder, /playwright-core\\cli\.js/);
assert.match(builder, /install chromium --no-shell/);
assert.match(builder, /PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = '1'/);
assert.match(builder, /npm\.cmd/);
assert.doesNotMatch(builder, /&\s+npx(?:\.cmd)?\b/im);
assert.doesNotMatch(builder, /Set-ExecutionPolicy|ExecutionPolicy Bypass|RunAs/i);
assert.doesNotMatch(builder, /install-deps|--with-deps|--channel|\bmsedge\b/i);

const releaseEntries = await readdir(releaseRoot, {
  recursive: true,
  withFileTypes: true,
});
const releaseFiles = releaseEntries
  .filter((entry) => entry.isFile())
  .map((entry) => relative(releaseRoot, join(entry.parentPath, entry.name)))
  .sort();
assert.deepEqual(releaseFiles, [
  ".gitignore",
  "templates/finalize-evidence.cmd.in",
  "templates/path-matrix.cmd.in",
  "templates/prepare-evidence.cmd.in",
  "templates/self-test.cmd.in",
  "templates/start.cmd.in",
  "templates/system-info.cmd.in",
]);

console.log(
  `Packaging contract OK: ${candidateIds.join(", ")}; hermetic launchers and build policy verified.`,
);
