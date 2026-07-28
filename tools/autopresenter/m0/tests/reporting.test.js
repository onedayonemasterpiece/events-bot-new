'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const crypto = require('node:crypto');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const {
  EvidenceValidationError,
  aggregateEvidence,
  aggregateEvidenceDirectory,
  renderMarkdown,
  validateCandidateEvidence,
  validateRunRecord,
  writeReports,
} = require('../reporting');

const FIXTURES = path.join(__dirname, 'fixtures');
const SCHEMAS = path.join(__dirname, '..', 'schemas');
const GENERATED_AT = '2026-07-28T12:00:00.000Z';

function fixture(name) {
  return JSON.parse(fs.readFileSync(path.join(FIXTURES, name), 'utf8'));
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function hash(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function candidates() {
  const compat = fixture('candidate-base.json');
  compat.release.packageLockFile = 'sources/pre-cft-compat/package-lock.json';
  compat.release.packageLockSha256 = hash('lock:pre-cft-compat');
  const current = clone(compat);
  current.candidateId = 'current-control';
  current.stack.playwrightVersion = '1.61.1';
  current.stack.browserVersion = '149.0.7827.55';
  current.stack.browserRevision = '1228';
  current.stack.browserExecutableSha256 = 'd'.repeat(64);
  current.release.zipFile = 'releases/current-control.zip';
  current.release.zipSha256 = 'e'.repeat(64);
  current.release.packageLockFile = 'sources/current-control/package-lock.json';
  current.release.packageLockSha256 = hash('lock:current-control');
  return [compat, current];
}

function runsFor(candidateId) {
  const localTemplate = fixture('run-local.json');
  const liveTemplate = fixture('run-live.json');
  const runs = [];
  for (let run = 1; run <= 20; run += 1) {
    const record = clone(localTemplate);
    record.candidateId = candidateId;
    record.run = run;
    record.profileMode = run <= 10 ? 'fresh' : 'persistent';
    record.screenshot = `runs/${candidateId}/local-${String(run).padStart(2, '0')}.png`;
    record.trace = `runs/${candidateId}/local-${String(run).padStart(2, '0')}.zip`;
    runs.push(record);
  }
  for (let run = 1; run <= 5; run += 1) {
    const record = clone(liveTemplate);
    record.candidateId = candidateId;
    record.run = run;
    record.screenshot = `runs/${candidateId}/live-${String(run).padStart(2, '0')}.png`;
    record.trace = `runs/${candidateId}/live-${String(run).padStart(2, '0')}.zip`;
    runs.push(record);
  }
  return runs;
}

function completeEvidence() {
  const candidateRecords = candidates();
  return {
    systemInfo: fixture('system-info.json'),
    candidates: candidateRecords,
    runs: candidateRecords.flatMap((candidate) => runsFor(candidate.candidateId)),
    inventory: {
      complete: true,
      requiredFiles: {
        'SYSTEM-INFO.json': true,
        'VERSIONS.json': true,
        'RELEASE-MANIFEST.json': true,
        'SHA256SUMS.txt': true,
        'M0-REPORT.json': true,
        'M0-REPORT.md': true,
      },
    },
    generatedAt: GENERATED_AT,
  };
}

function writeEvidenceTree(evidence) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autopresenter-m0-evidence-'));
  fs.mkdirSync(path.join(root, 'candidates'));
  fs.mkdirSync(path.join(root, 'runs'));
  fs.writeFileSync(path.join(root, 'SYSTEM-INFO.json'), JSON.stringify(evidence.systemInfo));
  for (const variant of evidence.systemInfo.pathVariants) {
    const file = path.join(root, ...variant.selfTest.split('/'));
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const payload = variant.kind;
    fs.writeFileSync(file, payload);
    variant.selfTestSha256 = hash(payload);
  }
  fs.writeFileSync(path.join(root, 'SYSTEM-INFO.json'), JSON.stringify(evidence.systemInfo));
  fs.writeFileSync(path.join(root, 'VERSIONS.json'), '{}\n');
  fs.writeFileSync(path.join(root, 'RELEASE-MANIFEST.json'), '{}\n');
  fs.writeFileSync(path.join(root, 'SHA256SUMS.txt'), 'fixture\n');
  evidence.candidates.forEach((candidate) => {
    fs.writeFileSync(
      path.join(root, 'candidates', `${candidate.candidateId}.json`),
      JSON.stringify(candidate),
    );
    const sourceRoot = path.join(root, 'sources', candidate.candidateId);
    fs.mkdirSync(sourceRoot, { recursive: true });
    const versions = {
      schemaVersion: 1,
      candidateId: candidate.candidateId,
      node: { version: candidate.stack.nodeVersion },
      playwright: { version: candidate.stack.playwrightVersion },
      browser: {
        revision: candidate.stack.browserRevision,
        executableSha256: candidate.stack.browserExecutableSha256,
      },
      packageLock: { sha256: candidate.release.packageLockSha256 },
    };
    fs.writeFileSync(path.join(sourceRoot, 'VERSIONS.json'), JSON.stringify(versions));
    fs.writeFileSync(path.join(sourceRoot, 'CANDIDATE.json'), JSON.stringify({ id: candidate.candidateId }));
    const sourceSystem = clone(evidence.systemInfo);
    sourceSystem.provenance.sourceCandidateId = candidate.candidateId;
    fs.writeFileSync(path.join(sourceRoot, 'SYSTEM-INFO.json'), JSON.stringify(sourceSystem));
    fs.writeFileSync(path.join(sourceRoot, 'M0-RUNTIME-SUITE.json'), JSON.stringify({
      candidateId: candidate.candidateId,
      runtimeChecksPassed: candidate.suiteChecksPassed,
      suites: {
        localCompatibility: { metTarget: candidate.suiteChecksPassed },
        liveSmoke: { metTarget: candidate.suiteChecksPassed },
      },
      processCleanup: { passed: candidate.suiteChecksPassed },
    }));
    fs.writeFileSync(
      path.join(sourceRoot, 'SELF-TEST.json'),
      JSON.stringify({ selfTest: candidate.selfTest }),
    );
    fs.writeFileSync(path.join(sourceRoot, 'package-lock.json'), `lock:${candidate.candidateId}`);
    fs.writeFileSync(
      path.join(sourceRoot, 'ZIP.sha256'),
      `${candidate.release.zipSha256}  ${candidate.release.zipFile}\n`,
    );
    fs.writeFileSync(path.join(sourceRoot, 'RELEASE-MANIFEST.json'), '{}');
    fs.writeFileSync(path.join(sourceRoot, 'SHA256SUMS.txt'), 'fixture');
  });
  evidence.runs.forEach((record) => {
    const suite = record.target === 'local-fixture' ? 'compatibility' : 'live';
    const runDirectory = path.join(
      root,
      'runs',
      record.candidateId,
      suite,
      `run-${String(record.run).padStart(3, '0')}`,
    );
    fs.mkdirSync(runDirectory, { recursive: true });
    const prefix = path.relative(root, runDirectory).split(path.sep).join('/');
    record.log = `${prefix}/worker.log`;
    record.screenshot = `${prefix}/screenshot.png`;
    record.trace = `${prefix}/trace.zip`;
    for (const name of ['worker.log', 'screenshot.png', 'trace.zip', 'runtime-result.json']) {
      fs.writeFileSync(path.join(runDirectory, name), `${record.candidateId}:${record.target}:${record.run}:${name}`);
    }
    fs.writeFileSync(
      path.join(runDirectory, 'run.json'),
      JSON.stringify(record),
    );
  });
  const versions = Object.fromEntries(evidence.candidates.map((candidate) => [
    candidate.candidateId,
    JSON.parse(fs.readFileSync(path.join(root, 'sources', candidate.candidateId, 'VERSIONS.json'))),
  ]));
  fs.writeFileSync(path.join(root, 'VERSIONS.json'), JSON.stringify({ schemaVersion: 1, candidates: versions }));
  const excluded = new Set(['M0-REPORT.json', 'M0-REPORT.md', 'RELEASE-MANIFEST.json', 'SHA256SUMS.txt']);
  const walk = (directory) => fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const value = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(value) : [value];
  });
  const files = walk(root).filter((file) => !excluded.has(path.basename(file))).map((file) => ({
    path: path.relative(root, file).split(path.sep).join('/'),
    sha256: hash(fs.readFileSync(file)),
    sizeBytes: fs.statSync(file).size,
  }));
  fs.writeFileSync(path.join(root, 'RELEASE-MANIFEST.json'), JSON.stringify({
    schemaVersion: 1,
    targetMachineOnly: true,
    candidateIds: ['current-control', 'pre-cft-compat'],
    files,
  }));
  const manifestHash = hash(fs.readFileSync(path.join(root, 'RELEASE-MANIFEST.json')));
  fs.writeFileSync(
    path.join(root, 'SHA256SUMS.txt'),
    `${files.map((file) => `${file.sha256}  ${file.path}`).join('\n')}\n${manifestHash}  RELEASE-MANIFEST.json\n`,
  );
  return root;
}

test('all four JSON schemas parse and close their root objects', () => {
  const names = [
    'candidate-evidence.schema.json',
    'run-record.schema.json',
    'system-info.schema.json',
    'm0-report.schema.json',
  ];
  for (const name of names) {
    const schema = JSON.parse(fs.readFileSync(path.join(SCHEMAS, name), 'utf8'));
    assert.equal(schema.$schema, 'https://json-schema.org/draft/2020-12/schema');
    assert.equal(schema.type, 'object');
    assert.equal(schema.additionalProperties, false);
  }
});

test('20/20 local plus separate 5/5 live for both selects newer on equal stability', () => {
  const report = aggregateEvidence(completeEvidence());
  assert.equal(report.verdict.status, 'PASS');
  assert.equal(report.verdict.selectedCandidateId, 'current-control');
  assert.match(report.verdict.reason, /newer Playwright version/);
  assert.deepEqual(
    report.candidates.map((candidate) => [
      candidate.status,
      candidate.localCold.passed,
      candidate.liveSmoke.passed,
    ]),
    [['PASS', 20, 5], ['PASS', 20, 5]],
  );
});

test('19/20 is FAIL and cannot win against a complete candidate', () => {
  const evidence = completeEvidence();
  evidence.runs = evidence.runs.filter((record) => !(
    record.candidateId === 'current-control'
    && record.target === 'local-fixture'
    && record.run === 20
  ));
  const report = aggregateEvidence(evidence);
  const current = report.candidates.find((candidate) => candidate.candidateId === 'current-control');
  assert.equal(current.status, 'FAIL');
  assert.equal(current.localCold.observed, 19);
  assert.equal(report.verdict.selectedCandidateId, 'pre-cft-compat');
});

test('both failures produce the exact Windows 10 no-go verdict', () => {
  const evidence = completeEvidence();
  for (const candidateId of ['pre-cft-compat', 'current-control']) {
    evidence.runs.find((record) => (
      record.candidateId === candidateId && record.target === 'local-fixture'
    )).successMarkerVisible = false;
  }
  const report = aggregateEvidence(evidence);
  assert.deepEqual(report.verdict, {
    status: 'FAIL',
    code: 'PLAYWRIGHT_ON_TARGET_WIN10_NO_GO',
    selectedCandidateId: null,
    reason: 'Both exact candidates failed the target Windows 10 M0 gate.',
  });
});

test('local successes cannot substitute for separate live route and content checks', () => {
  const evidence = completeEvidence();
  const live = evidence.runs.find((record) => (
    record.candidateId === 'current-control' && record.target === 'live-site'
  ));
  live.liveContentSuccess = false;
  const report = aggregateEvidence(evidence);
  const current = report.candidates.find((candidate) => candidate.candidateId === 'current-control');
  assert.equal(current.liveSmoke.passed, 4);
  assert.equal(current.status, 'FAIL');
});

test('admin, install, download, system browser, self-test, and orphan evidence fail closed', () => {
  const mutators = [
    (evidence) => { evidence.candidates[0].requirements.administrator = true; },
    (evidence) => { evidence.runs[0].installRequired = true; },
    (evidence) => { evidence.runs[0].browserDownloadAttempted = true; },
    (evidence) => { evidence.runs[0].usedSystemBrowser = true; },
    (evidence) => { evidence.candidates[0].selfTest.passed = false; },
    (evidence) => { evidence.runs[0].orphanProcesses = ['chrome.exe:1234']; },
  ];
  for (const mutate of mutators) {
    const evidence = completeEvidence();
    mutate(evidence);
    const report = aggregateEvidence(evidence);
    assert.equal(report.candidates[0].status, 'FAIL');
  }
});

test('suite, path matrix, and target machine/account provenance fail closed', () => {
  for (const mutation of [
    (candidate) => { candidate.suiteChecksPassed = false; },
    (candidate) => { candidate.pathMatrixPassed = false; },
    (candidate) => { candidate.machineAccountFingerprint = 'a'.repeat(64); },
  ]) {
    const evidence = completeEvidence();
    mutation(evidence.candidates[0]);
    const report = aggregateEvidence(evidence);
    assert.equal(report.candidates[0].status, 'FAIL');
  }
});

test('directory aggregation rejects a run copied from another machine/account', () => {
  const evidence = completeEvidence();
  const root = writeEvidenceTree(evidence);
  try {
    const run = path.join(
      root, 'runs', 'pre-cft-compat', 'compatibility', 'run-001', 'run.json',
    );
    const record = JSON.parse(fs.readFileSync(run, 'utf8'));
    record.machineAccountFingerprint = 'a'.repeat(64);
    fs.writeFileSync(run, JSON.stringify(record));
    assert.throws(
      () => aggregateEvidenceDirectory(root, { generatedAt: GENERATED_AT }),
      /Manifest file mismatch|Run provenance mismatch/,
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('both pass but different stability selects the more stable candidate, not the newer one', () => {
  const evidence = completeEvidence();
  evidence.candidates[1].stabilitySignals.push('one non-fatal headed launch delay');
  const report = aggregateEvidence(evidence);
  assert.equal(report.verdict.selectedCandidateId, 'pre-cft-compat');
  assert.match(report.verdict.reason, /fewer recorded stability signals/);
});

test('schemas reject absolute executable paths and unknown candidate fields', () => {
  const badRun = fixture('run-local.json');
  badRun.browserExecutable = 'C:\\Program Files\\Google\\Chrome\\chrome.exe';
  assert.throws(() => validateRunRecord(badRun), EvidenceValidationError);

  const badCandidate = fixture('candidate-base.json');
  badCandidate.browserChannel = 'chrome';
  assert.throws(() => validateCandidateEvidence(badCandidate), EvidenceValidationError);
});

test('directory aggregation requires full inventory and writes JSON and Markdown reports', () => {
  const evidence = completeEvidence();
  const root = writeEvidenceTree(evidence);
  try {
    const report = aggregateEvidenceDirectory(root, { generatedAt: GENERATED_AT });
    assert.equal(report.verdict.status, 'PASS');
    const output = writeReports(root, report);
    assert.equal(fs.existsSync(output.jsonPath), true);
    assert.equal(fs.existsSync(output.markdownPath), true);
    assert.deepEqual(JSON.parse(fs.readFileSync(output.jsonPath, 'utf8')), report);
    const markdown = fs.readFileSync(output.markdownPath, 'utf8');
    assert.match(markdown, /^# M0 Compatibility Report/m);
    assert.match(markdown, /20\/20/);
    assert.match(markdown, /5\/5/);
    assert.equal(markdown, renderMarkdown(report));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('missing required release evidence fails closed before report generation', () => {
  const evidence = completeEvidence();
  const root = writeEvidenceTree(evidence);
  try {
    fs.unlinkSync(path.join(root, 'SHA256SUMS.txt'));
    assert.throws(
      () => aggregateEvidenceDirectory(root, { generatedAt: GENERATED_AT }),
      /SHA256SUMS/,
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
