"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { ContractError } = require("./errors");

function normalizedForComparison(value) {
  const normalized = path.resolve(value);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function isPathInside(root, candidate) {
  const relative = path.relative(
    normalizedForComparison(root),
    normalizedForComparison(candidate),
  );
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function realDirectory(directory, code) {
  let resolved;
  try {
    resolved = fs.realpathSync.native(directory);
  } catch {
    throw new ContractError(code, `Required directory does not exist: ${directory}`);
  }
  if (!fs.statSync(resolved).isDirectory()) {
    throw new ContractError(code, `Expected a directory: ${directory}`);
  }
  return resolved;
}

function realFile(filePath, code) {
  let resolved;
  try {
    resolved = fs.realpathSync.native(filePath);
  } catch {
    throw new ContractError(code, `Required file does not exist: ${filePath}`);
  }
  if (!fs.statSync(resolved).isFile()) {
    throw new ContractError(code, `Expected a regular file: ${filePath}`);
  }
  return resolved;
}

function resolvePortableRoot(value) {
  if (!value || !path.isAbsolute(value)) {
    throw new ContractError(
      "PORTABLE_ROOT_ABSOLUTE_REQUIRED",
      "--portable-root must be an absolute path so launch never depends on cwd",
    );
  }
  return realDirectory(value, "PORTABLE_ROOT_MISSING");
}

function resolvePortablePath(portableRoot, value, description) {
  if (!value) {
    throw new ContractError(
      "PORTABLE_PATH_REQUIRED",
      `${description} path is required`,
    );
  }
  const resolved = path.isAbsolute(value)
    ? path.resolve(value)
    : path.resolve(portableRoot, value);
  if (!isPathInside(portableRoot, resolved)) {
    throw new ContractError(
      "PORTABLE_PATH_ESCAPE",
      `${description} must remain within the portable release root`,
    );
  }
  return resolved;
}

function ensurePortableDirectory(portableRoot, value, description) {
  const resolved = resolvePortablePath(portableRoot, value, description);
  fs.mkdirSync(resolved, { recursive: true });
  const real = realDirectory(resolved, "PORTABLE_DIRECTORY_MISSING");
  if (!isPathInside(portableRoot, real)) {
    throw new ContractError(
      "PORTABLE_SYMLINK_ESCAPE",
      `${description} resolves outside the portable release root`,
    );
  }
  return real;
}

function validatePortableLayout({ portableRoot, appRoot, nodeExecutable }) {
  const runtimeRoot = realDirectory(
    path.join(portableRoot, "runtime"),
    "PORTABLE_RUNTIME_MISSING",
  );
  const realNode = realFile(nodeExecutable, "PORTABLE_NODE_MISSING");
  if (!isPathInside(runtimeRoot, realNode)) {
    throw new ContractError(
      "SYSTEM_NODE_FORBIDDEN",
      "The runner must be launched by the portable runtime/node executable",
    );
  }

  const expectedAppRoot = realDirectory(
    path.join(portableRoot, "app"),
    "PORTABLE_APP_MISSING",
  );
  const realAppRoot = realDirectory(appRoot, "APP_ROOT_MISSING");
  if (
    realAppRoot !== expectedAppRoot &&
    !isPathInside(expectedAppRoot, realAppRoot)
  ) {
    throw new ContractError(
      "APP_OUTSIDE_PORTABLE_RELEASE",
      "Runtime source and modules must be loaded from the portable app directory",
    );
  }

  return {
    appRoot: realAppRoot,
    nodeExecutable: realNode,
    runtimeRoot,
  };
}

function resolveManagedBrowser({
  portableRoot,
  portableBrowsersRootValue,
  browserExecutableValue,
}) {
  const browsersRootPath = resolvePortablePath(
    portableRoot,
    portableBrowsersRootValue,
    "portable browsers root",
  );
  const portableBrowsersRoot = realDirectory(
    browsersRootPath,
    "PORTABLE_BROWSERS_MISSING",
  );
  if (path.basename(portableBrowsersRoot).toLowerCase() !== "browsers") {
    throw new ContractError(
      "PORTABLE_BROWSERS_NAME_INVALID",
      "The managed browser root must be the release browsers/ directory",
    );
  }

  const executablePath = realFile(
    resolvePortablePath(
      portableRoot,
      browserExecutableValue,
      "managed browser executable",
    ),
    "MANAGED_BROWSER_MISSING",
  );
  if (!isPathInside(portableBrowsersRoot, executablePath)) {
    throw new ContractError(
      "SYSTEM_BROWSER_FORBIDDEN",
      "Browser executable must resolve beneath the portable browsers/ directory",
    );
  }

  return {
    executablePath,
    executableRelative: toPortableRelative(portableRoot, executablePath),
    portableBrowsersRoot,
  };
}

function loadPlaywrightCore({ appRoot, modulePath }) {
  let entry;
  try {
    if (modulePath) {
      const candidate = fs.statSync(modulePath).isDirectory()
        ? require.resolve("playwright-core", { paths: [modulePath] })
        : modulePath;
      entry = realFile(candidate, "PLAYWRIGHT_CORE_MISSING");
    } else {
      entry = realFile(
        require.resolve("playwright-core", { paths: [appRoot] }),
        "PLAYWRIGHT_CORE_MISSING",
      );
    }
  } catch (error) {
    if (error instanceof ContractError) {
      throw error;
    }
    throw new ContractError(
      "PLAYWRIGHT_CORE_MISSING",
      "Local app/node_modules/playwright-core is required; runtime install/download fallback is forbidden",
    );
  }
  if (!isPathInside(appRoot, entry)) {
    throw new ContractError(
      "GLOBAL_PLAYWRIGHT_FORBIDDEN",
      "playwright-core must resolve from within the portable app directory",
    );
  }

  // This is intentionally playwright-core only: never fall back to a globally
  // installed playwright package and never ask Playwright to install a browser.
  const playwright = require(entry);
  if (!playwright || !playwright.chromium) {
    throw new ContractError(
      "PLAYWRIGHT_CORE_INVALID",
      "The packaged playwright-core module does not expose chromium",
    );
  }
  return { entry, playwright };
}

function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  const data = fs.readFileSync(filePath);
  hash.update(data);
  return hash.digest("hex");
}

function toPortableRelative(portableRoot, value) {
  return path.relative(portableRoot, value).split(path.sep).join("/");
}

function verifyVersionsManifest({
  manifest,
  portableRoot,
  nodeExecutable,
  browserExecutable,
}) {
  const expectedNodePath = manifest?.node?.executableRelativePath;
  const expectedNodeHash = manifest?.node?.executableSha256;
  const expectedBrowserPath = manifest?.browser?.executableRelativePath;
  const expectedBrowserHash = manifest?.browser?.executableSha256;
  const missing = [
    ["node.executableRelativePath", expectedNodePath],
    ["node.executableSha256", expectedNodeHash],
    ["browser.executableRelativePath", expectedBrowserPath],
    ["browser.executableSha256", expectedBrowserHash],
  ]
    .filter(([, value]) => typeof value !== "string" || value.length === 0)
    .map(([name]) => name);
  if (missing.length) {
    throw new ContractError(
      "VERSIONS_MANIFEST_INCOMPLETE",
      `VERSIONS.json is missing exact executable fields: ${missing.join(", ")}`,
    );
  }

  const actual = {
    node: {
      executableRelativePath: toPortableRelative(portableRoot, nodeExecutable),
      executableSha256: sha256File(nodeExecutable),
    },
    browser: {
      executableRelativePath: toPortableRelative(portableRoot, browserExecutable),
      executableSha256: sha256File(browserExecutable),
    },
  };
  const comparisons = [
    [
      "node executable path",
      expectedNodePath.replaceAll("\\", "/"),
      actual.node.executableRelativePath,
    ],
    ["node SHA-256", expectedNodeHash.toLowerCase(), actual.node.executableSha256],
    [
      "browser executable path",
      expectedBrowserPath.replaceAll("\\", "/"),
      actual.browser.executableRelativePath,
    ],
    [
      "browser SHA-256",
      expectedBrowserHash.toLowerCase(),
      actual.browser.executableSha256,
    ],
  ];
  const mismatches = comparisons
    .filter(([, expected, observed]) => expected !== observed)
    .map(([label, expected, observed]) => ({ label, expected, observed }));
  if (mismatches.length) {
    throw new ContractError(
      "VERSIONS_MANIFEST_MISMATCH",
      `Executable manifest mismatch: ${mismatches
        .map(({ label }) => label)
        .join(", ")}`,
    );
  }
  return actual;
}

function verifyExactCandidate({
  appRoot,
  browserExecutable,
  candidateId,
  portableRoot,
}) {
  const candidate = JSON.parse(
    fs.readFileSync(realFile(path.join(portableRoot, "CANDIDATE.json"), "CANDIDATE_MANIFEST_MISSING"), "utf8"),
  );
  const versions = JSON.parse(
    fs.readFileSync(realFile(path.join(portableRoot, "VERSIONS.json"), "VERSIONS_MANIFEST_MISSING"), "utf8"),
  );
  const packageLock = realFile(
    path.join(appRoot, "package-lock.json"),
    "PACKAGE_LOCK_MISSING",
  );
  const corePackage = JSON.parse(
    fs.readFileSync(
      realFile(
        path.join(appRoot, "node_modules", "playwright-core", "package.json"),
        "PLAYWRIGHT_CORE_MISSING",
      ),
      "utf8",
    ),
  );
  const errors = [];
  if (candidate.id !== candidateId || versions.candidateId !== candidateId) {
    errors.push("candidate identity");
  }
  if (candidate.target?.architecture !== "x64" || process.arch !== "x64") {
    errors.push("target architecture");
  }
  if (`v${candidate.node?.version}` !== process.version) errors.push("Node version");
  if (
    versions.node?.executableRelativePath !==
      toPortableRelative(portableRoot, process.execPath) ||
    versions.node?.executableSha256 !== sha256File(process.execPath)
  ) {
    errors.push("portable Node path/hash");
  }
  if (
    candidate.playwright?.version !== corePackage.version ||
    versions.playwright?.coreVersion !== corePackage.version
  ) {
    errors.push("Playwright version");
  }
  if (
    candidate.playwright?.packageLockSha256 !== sha256File(packageLock) ||
    versions.packageLock?.sha256 !== sha256File(packageLock)
  ) {
    errors.push("package-lock hash");
  }
  const relativeBrowser = toPortableRelative(portableRoot, browserExecutable);
  const browserHash = sha256File(browserExecutable);
  if (
    candidate.managedBrowser?.executableRelativePath !== relativeBrowser ||
    versions.browser?.executableRelativePath !== relativeBrowser ||
    versions.browser?.executableSha256 !== browserHash
  ) {
    errors.push("managed browser path/hash");
  }
  if (
    candidate.launch?.headless !== false ||
    candidate.launch?.browserChannel !== null ||
    candidate.launch?.arguments?.length !== 0 ||
    candidate.launch?.profileModes?.join(",") !== "fresh,persistent"
  ) {
    errors.push("headed launch/profile policy");
  }
  if (errors.length) {
    throw new ContractError(
      "EXACT_CANDIDATE_MISMATCH",
      `Packaged candidate contract mismatch: ${errors.join(", ")}`,
    );
  }
  return { candidate, versions };
}

module.exports = {
  ensurePortableDirectory,
  isPathInside,
  loadPlaywrightCore,
  realFile,
  resolveManagedBrowser,
  resolvePortablePath,
  resolvePortableRoot,
  sha256File,
  toPortableRelative,
  validatePortableLayout,
  verifyVersionsManifest,
  verifyExactCandidate,
};
