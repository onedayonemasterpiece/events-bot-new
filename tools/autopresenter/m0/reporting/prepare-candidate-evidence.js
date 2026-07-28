#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const { validateCandidateEvidence, validateRunRecord } = require('./validate');
const {
  assertTargetMachineProvenance,
  queryMachineProvenance,
} = require('../src/machine-provenance');

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function parseArgs(argv) {
  const result = {};
  for (let i = 0; i < argv.length; i += 2) result[argv[i]?.slice(2)] = argv[i + 1];
  for (const key of ['portable-root', 'zip', 'evidence-root']) {
    if (!result[key]) throw new Error(`--${key} is required`);
  }
  return result;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function findRunFiles(root) {
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const value = path.join(root, entry.name);
    return entry.isDirectory() ? findRunFiles(value) : entry.name === 'run.json' ? [value] : [];
  });
}

function verifyReleaseChecksums(root) {
  const lines = fs.readFileSync(path.join(root, 'SHA256SUMS.txt'), 'utf8').trim().split(/\r?\n/);
  for (const line of lines) {
    const match = /^([a-f0-9]{64})  (.+)$/.exec(line);
    if (!match) throw new Error(`Invalid release checksum line: ${line}`);
    const file = path.join(root, ...match[2].split('/'));
    if (!fs.existsSync(file) || sha256(file) !== match[1]) {
      throw new Error(`Release checksum mismatch: ${match[2]}`);
    }
  }
}

function verifyZipMatchesRelease(zip, portableRoot) {
  if (process.platform !== 'win32') {
    throw new Error('ZIP_RELEASE_BINDING_TARGET_REQUIRED: prepare evidence on target Windows');
  }
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'autopresenter-m0-zip-'));
  try {
    const command =
      "Expand-Archive -LiteralPath $env:AP_M0_ZIP -DestinationPath $env:AP_M0_DEST -Force";
    const encoded = Buffer.from(command, 'utf16le').toString('base64');
    const result = spawnSync(
      'powershell.exe',
      ['-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded],
      {
        encoding: 'utf8',
        env: { ...process.env, AP_M0_ZIP: zip, AP_M0_DEST: temporary },
        timeout: 180000,
        windowsHide: true,
      },
    );
    if (result.error || result.status !== 0) {
      throw new Error(`Cannot inspect candidate ZIP: ${String(result.error?.message || result.stderr).slice(0, 1000)}`);
    }
    const roots = fs.readdirSync(temporary, { withFileTypes: true }).filter((entry) => entry.isDirectory());
    if (roots.length !== 1) throw new Error('Candidate ZIP must contain one release root');
    const extracted = path.join(temporary, roots[0].name);
    for (const name of ['CANDIDATE.json', 'VERSIONS.json', 'RELEASE-MANIFEST.json', 'SHA256SUMS.txt']) {
      if (
        !fs.existsSync(path.join(extracted, name)) ||
        sha256(path.join(extracted, name)) !== sha256(path.join(portableRoot, name))
      ) {
        throw new Error(`Tested portable folder does not match ZIP member: ${name}`);
      }
    }
    verifyReleaseChecksums(extracted);
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

function copyRun(runFile, sourceRunsRoot, evidenceRoot) {
  const record = readJson(runFile);
  validateRunRecord(record);
  const suite = record.target === 'local-fixture' ? 'compatibility' : 'live';
  const destination = path.join(
    evidenceRoot, 'runs', record.candidateId, suite,
    `run-${String(record.run).padStart(3, '0')}`,
  );
  fs.mkdirSync(destination, { recursive: true });
  const sourceDirectory = path.dirname(runFile);
  for (const name of ['run.json', 'runtime-result.json', 'worker.log', 'screenshot.png', 'trace.zip']) {
    const source = path.join(sourceDirectory, name);
    if (fs.existsSync(source)) fs.copyFileSync(source, path.join(destination, name));
  }
  const prefix = path.relative(evidenceRoot, destination).split(path.sep).join('/');
  record.screenshot = fs.existsSync(path.join(destination, 'screenshot.png')) ? `${prefix}/screenshot.png` : null;
  record.trace = fs.existsSync(path.join(destination, 'trace.zip')) ? `${prefix}/trace.zip` : null;
  record.log = `${prefix}/worker.log`;
  validateRunRecord(record);
  fs.writeFileSync(path.join(destination, 'run.json'), `${JSON.stringify(record, null, 2)}\n`);
  return record;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const portableRoot = path.resolve(args['portable-root']);
  const evidenceRoot = path.resolve(args['evidence-root']);
  const zip = path.resolve(args.zip);
  const candidate = readJson(path.join(portableRoot, 'CANDIDATE.json'));
  const versions = readJson(path.join(portableRoot, 'VERSIONS.json'));
  const selfTestPath = path.join(portableRoot, 'logs', 'self-test.json');
  const suitePath = path.join(portableRoot, 'logs', 'm0', 'm0-runtime-suite.json');
  const systemInfoPath = path.join(portableRoot, 'evidence', 'SYSTEM-INFO.json');
  if (!fs.existsSync(systemInfoPath)) {
    throw new Error('Run system-info.cmd for this candidate before preparing evidence');
  }
  const systemInfo = readJson(systemInfoPath);
  assertTargetMachineProvenance(
    systemInfo.provenance?.machineAccountFingerprint,
    systemInfo.os?.build,
    queryMachineProvenance(),
  );
  const selfTestReport = fs.existsSync(selfTestPath) ? readJson(selfTestPath) : null;
  const suite = fs.existsSync(suitePath) ? readJson(suitePath) : null;
  if (
    candidate.id !== versions.candidateId ||
    (suite && candidate.id !== suite.candidateId)
    || systemInfo.provenance?.sourceCandidateId !== candidate.id
  ) {
    throw new Error('Candidate identity mismatch across release and runtime evidence');
  }
  verifyReleaseChecksums(portableRoot);
  if (!fs.existsSync(zip)) throw new Error(`Candidate ZIP is absent: ${zip}`);
  verifyZipMatchesRelease(zip, portableRoot);
  const sourceRoot = path.join(evidenceRoot, 'sources', candidate.id);
  fs.mkdirSync(sourceRoot, { recursive: true });
  for (const name of ['CANDIDATE.json', 'VERSIONS.json', 'RELEASE-MANIFEST.json', 'SHA256SUMS.txt']) {
    fs.copyFileSync(path.join(portableRoot, name), path.join(sourceRoot, name));
  }
  fs.copyFileSync(systemInfoPath, path.join(sourceRoot, 'SYSTEM-INFO.json'));
  fs.writeFileSync(
    path.join(sourceRoot, 'M0-RUNTIME-SUITE.json'),
    `${JSON.stringify(suite || {
      schemaVersion: 1,
      candidateId: candidate.id,
      runtimeChecksPassed: false,
      suites: {
        localCompatibility: { metTarget: false },
        liveSmoke: { metTarget: false },
      },
      processCleanup: { passed: false },
      error: 'runtime suite did not emit m0-runtime-suite.json',
    }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(sourceRoot, 'SELF-TEST.json'),
    `${JSON.stringify(selfTestReport || {
      selfTest: {
        passed: false,
        offline: true,
        targetSiteAccessed: false,
        localFixtureClick: false,
        probeFilesCreatedAndDeleted: false,
        nodeExitCode: 1,
        browserStarted: false,
        browserClosed: false,
        usedSystemBrowser: false,
        browserDownloadAttempted: false,
        adminRequired: false,
        installRequired: false,
        orphanProcesses: [],
        error: 'self-test did not emit logs/self-test.json',
      },
    }, null, 2)}\n`,
  );
  fs.copyFileSync(path.join(portableRoot, 'app', 'package-lock.json'), path.join(sourceRoot, 'package-lock.json'));
  const zipHash = sha256(zip);
  fs.writeFileSync(
    path.join(sourceRoot, 'ZIP.sha256'),
    `${zipHash}  ${path.basename(zip)}\n`,
  );
  const runs = findRunFiles(path.join(portableRoot, 'logs', 'm0', 'runs'))
    .map((file) => copyRun(file, path.join(portableRoot, 'logs', 'm0', 'runs'), evidenceRoot));
  const record = {
    schemaVersion: 1,
    candidateId: candidate.id,
    machineAccountFingerprint:
      systemInfo.provenance.machineAccountFingerprint,
    suiteChecksPassed: Boolean(
      suite?.runtimeChecksPassed &&
      suite?.suites?.localCompatibility?.metTarget &&
      suite?.suites?.liveSmoke?.metTarget &&
      suite?.processCleanup?.passed,
    ),
    pathMatrixPassed:
      systemInfo.pathVariants.length === 3 &&
      systemInfo.pathVariants.every((entry) => entry.passed),
    stack: {
      nodeVersion: versions.node.version,
      nodeArchitecture: versions.node.architecture,
      playwrightVersion: versions.playwright.version,
      browserName: versions.browser.name,
      browserVersion: versions.browser.version,
      browserRevision: versions.browser.revision,
      browserExecutableSha256: versions.browser.executableSha256,
    },
    release: {
      zipFile: path.basename(zip),
      zipSha256: zipHash,
      packageLockFile: `sources/${candidate.id}/package-lock.json`,
      packageLockSha256: versions.packageLock.sha256,
    },
    requirements: {
      administrator: candidate.runtimePolicy.requiresAdministrator,
      installedNode: false,
      installedBrowser: false,
      installStep: false,
      downloadAtRuntime: candidate.runtimePolicy.downloadAllowedAtRuntime,
      systemBrowser: candidate.runtimePolicy.allowSystemBrowser,
    },
    extraRequirements: [],
    stabilitySignals: runs.filter((run) => run.error).map((run) => `${run.target}:${run.run}:${run.error.code || 'error'}`),
    selfTest: selfTestReport?.selfTest || {
      passed: false,
      offline: true,
      targetSiteAccessed: false,
      localFixtureClick: false,
      probeFilesCreatedAndDeleted: false,
      nodeExitCode: 1,
      browserStarted: false,
      browserClosed: false,
      usedSystemBrowser: false,
      browserDownloadAttempted: false,
      adminRequired: false,
      installRequired: false,
      orphanProcesses: [],
      error: 'self-test did not emit logs/self-test.json',
    },
  };
  validateCandidateEvidence(record);
  fs.mkdirSync(path.join(evidenceRoot, 'candidates'), { recursive: true });
  fs.writeFileSync(
    path.join(evidenceRoot, 'candidates', `${candidate.id}.json`),
    `${JSON.stringify(record, null, 2)}\n`,
  );
  process.stdout.write(`${candidate.id}: copied ${runs.length} run records\n`);
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 2;
}
