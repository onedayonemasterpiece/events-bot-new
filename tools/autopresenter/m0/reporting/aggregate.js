'use strict';

const fs = require('node:fs');
const path = require('node:path');
const {
  validateCandidateEvidence,
  validateReport,
  validateRunRecord,
  validateSystemInfo,
} = require('./validate');

const SOURCE_EVIDENCE_FILES = [
  'SYSTEM-INFO.json',
  'VERSIONS.json',
  'RELEASE-MANIFEST.json',
  'SHA256SUMS.txt',
];

const REPORT_FILES = ['M0-REPORT.json', 'M0-REPORT.md'];

function parseSemver(version) {
  const match = /^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(version);
  if (!match) throw new Error(`Invalid semantic version: ${version}`);
  return match.slice(1).map(Number);
}

function compareSemver(left, right) {
  const leftParts = parseSemver(left);
  const rightParts = parseSemver(right);
  for (let index = 0; index < 3; index += 1) {
    if (leftParts[index] !== rightParts[index]) return leftParts[index] - rightParts[index];
  }
  return 0;
}

function requirementFree(candidate) {
  return Object.values(candidate.requirements).every((required) => required === false)
    && candidate.extraRequirements.length === 0;
}

function selfTestPassed(candidate) {
  const selfTest = candidate.selfTest;
  return selfTest.passed === true
    && selfTest.offline === true
    && selfTest.targetSiteAccessed === false
    && selfTest.localFixtureClick === true
    && selfTest.probeFilesCreatedAndDeleted === true
    && selfTest.nodeExitCode === 0
    && selfTest.browserStarted === true
    && selfTest.browserClosed === true
    && selfTest.usedSystemBrowser === false
    && selfTest.browserDownloadAttempted === false
    && selfTest.adminRequired === false
    && selfTest.installRequired === false
    && selfTest.orphanProcesses.length === 0
    && (selfTest.error === undefined || selfTest.error === null);
}

function runPassed(run) {
  return run.coldStart === true
    && run.nodeProcessFresh === true
    && run.browserProcessFresh === true
    && run.nodeExitCode === 0
    && run.browserStarted === true
    && run.usedSystemBrowser === false
    && run.browserDownloadAttempted === false
    && run.adminRequired === false
    && run.installRequired === false
    && run.action === 'locator.click'
    && run.successMarkerVisible === true
    && run.orphanProcesses.length === 0
    && run.error === null
    && (run.target !== 'live-site'
      || (run.liveRouteSuccess === true && run.liveContentSuccess === true));
}

function suiteSummary(runs, expected) {
  return {
    expected,
    observed: runs.length,
    passed: runs.filter(runPassed).length,
    fresh: runs.filter((run) => run.profileMode === 'fresh').length,
    persistent: runs.filter((run) => run.profileMode === 'persistent').length,
  };
}

function hasExactRunNumbers(runs, expected) {
  const numbers = runs.map((run) => run.run).sort((left, right) => left - right);
  return numbers.length === expected
    && numbers.every((runNumber, index) => runNumber === index + 1);
}

function systemGateIssues(systemInfo) {
  const issues = [];
  if (!systemInfo.user.standardUser || systemInfo.user.administrator) {
    issues.push('target execution was not recorded for a non-administrator standard user');
  }
  if (Object.values(systemInfo.baseline).some(Boolean)) {
    issues.push('target baseline included installed Node, Chrome, or Playwright');
  }
  const failedPaths = systemInfo.pathVariants.filter((entry) => !entry.passed).map((entry) => entry.kind);
  if (failedPaths.length > 0) issues.push(`path matrix failed: ${failedPaths.join(', ')}`);
  return issues;
}

function evaluateCandidate(candidate, allRuns, systemInfo, inventoryComplete) {
  const runs = allRuns.filter((run) => run.candidateId === candidate.candidateId);
  const localRuns = runs.filter((run) => run.target === 'local-fixture');
  const liveRuns = runs.filter((run) => run.target === 'live-site');
  const localCold = suiteSummary(localRuns, 20);
  const liveSmoke = suiteSummary(liveRuns, 5);
  const reasons = [];

  if (!inventoryComplete) reasons.push('required evidence inventory is incomplete');
  reasons.push(...systemGateIssues(systemInfo));
  if (!hasExactRunNumbers(localRuns, 20)) {
    reasons.push(`local cold cycle set must be exactly runs 1..20; observed ${localRuns.length}`);
  }
  if (localCold.passed !== 20) reasons.push(`local cold cycles must pass 20/20; passed ${localCold.passed}/20`);
  if (localCold.fresh !== 10 || localCold.persistent !== 10) {
    reasons.push(`local profile split must be 10 fresh + 10 persistent; observed ${localCold.fresh} + ${localCold.persistent}`);
  }
  if (!hasExactRunNumbers(liveRuns, 5)) {
    reasons.push(`live smoke set must be exactly runs 1..5; observed ${liveRuns.length}`);
  }
  if (liveSmoke.passed !== 5) reasons.push(`live smoke must pass 5/5; passed ${liveSmoke.passed}/5`);
  if (!selfTestPassed(candidate)) reasons.push('offline self-test did not satisfy every required check');
  if (!requirementFree(candidate)) reasons.push('candidate requires a prohibited or extra target-machine dependency');

  const allOrphans = runs.flatMap((run) => run.orphanProcesses)
    .concat(candidate.selfTest.orphanProcesses);
  if (allOrphans.length > 0) reasons.push(`orphan processes recorded: ${allOrphans.length}`);

  const prohibitedRun = runs.some((run) => (
    run.usedSystemBrowser
    || run.browserDownloadAttempted
    || run.adminRequired
    || run.installRequired
  ));
  if (prohibitedRun) reasons.push('one or more runs used a prohibited browser/download/admin/install path');

  return {
    candidateId: candidate.candidateId,
    playwrightVersion: candidate.stack.playwrightVersion,
    status: reasons.length === 0 ? 'PASS' : 'FAIL',
    localCold,
    liveSmoke,
    selfTestPassed: selfTestPassed(candidate),
    prohibitedRequirementsAbsent: requirementFree(candidate) && !prohibitedRun,
    orphanFree: allOrphans.length === 0,
    stabilitySignalCount: candidate.stabilitySignals.length,
    reasons: [...new Set(reasons)],
  };
}

function chooseWinner(candidateResults) {
  const passing = candidateResults.filter((candidate) => candidate.status === 'PASS');
  if (passing.length === 0) {
    return {
      status: 'FAIL',
      code: 'PLAYWRIGHT_ON_TARGET_WIN10_NO_GO',
      selectedCandidateId: null,
      reason: 'Both exact candidates failed the target Windows 10 M0 gate.',
    };
  }
  if (passing.length === 1) {
    return {
      status: 'PASS',
      code: 'M0_COMPATIBILITY_PASS',
      selectedCandidateId: passing[0].candidateId,
      reason: 'Only one exact candidate satisfied every M0 gate.',
    };
  }

  const stableOrder = [...passing].sort((left, right) => {
    if (left.stabilitySignalCount !== right.stabilitySignalCount) {
      return left.stabilitySignalCount - right.stabilitySignalCount;
    }
    return compareSemver(right.playwrightVersion, left.playwrightVersion);
  });
  const best = stableOrder[0];
  const next = stableOrder[1];
  if (best.stabilitySignalCount < next.stabilitySignalCount) {
    return {
      status: 'PASS',
      code: 'M0_COMPATIBILITY_PASS',
      selectedCandidateId: best.candidateId,
      reason: 'Both candidates passed; the candidate with fewer recorded stability signals was selected.',
    };
  }
  if (compareSemver(best.playwrightVersion, next.playwrightVersion) === 0) {
    return {
      status: 'FAIL',
      code: 'PLAYWRIGHT_ON_TARGET_WIN10_NO_GO',
      selectedCandidateId: null,
      reason: 'Passing candidates had equal stability and indistinguishable Playwright versions; selection is ambiguous.',
    };
  }
  return {
    status: 'PASS',
    code: 'M0_COMPATIBILITY_PASS',
    selectedCandidateId: best.candidateId,
    reason: 'Both candidates passed with equal stability and no extra requirements; the newer Playwright version was selected.',
  };
}

function aggregateEvidence({
  systemInfo,
  candidates,
  runs,
  inventory,
  generatedAt = new Date().toISOString(),
}) {
  validateSystemInfo(systemInfo);
  if (!Array.isArray(candidates) || candidates.length !== 2) {
    throw new Error(`Exactly two candidate evidence records are required; received ${candidates?.length ?? 0}`);
  }
  candidates.forEach((candidate, index) => validateCandidateEvidence(candidate, `candidate evidence ${index + 1}`));
  runs.forEach((run, index) => validateRunRecord(run, `run record ${index + 1}`));

  const ids = candidates.map((candidate) => candidate.candidateId);
  if (new Set(ids).size !== ids.length) throw new Error('Candidate IDs must be unique');
  const unknownRun = runs.find((run) => !ids.includes(run.candidateId));
  if (unknownRun) throw new Error(`Run record references unknown candidate: ${unknownRun.candidateId}`);

  const expectedInventoryNames = [...SOURCE_EVIDENCE_FILES, ...REPORT_FILES];
  const inventoryComplete = inventory.complete === true
    && expectedInventoryNames.every((name) => inventory.requiredFiles?.[name] === true)
    && Object.keys(inventory.requiredFiles || {}).every((name) => expectedInventoryNames.includes(name));
  const results = candidates.map((candidate) => (
    evaluateCandidate(candidate, runs, systemInfo, inventoryComplete)
  ));
  const report = {
    schemaVersion: 1,
    generatedAt,
    targetMachineOnly: true,
    system: {
      productName: systemInfo.os.productName,
      build: systemInfo.os.build,
      architecture: systemInfo.os.architecture,
      standardUser: systemInfo.user.standardUser && !systemInfo.user.administrator,
    },
    evidenceInventory: {
      complete: inventoryComplete,
      requiredFiles: inventory.requiredFiles,
      candidateRecordCount: candidates.length,
      runRecordCount: runs.length,
    },
    candidates: results,
    verdict: chooseWinner(results),
  };
  validateReport(report);
  return report;
}

function readJson(filePath) {
  let value;
  try {
    value = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (error) {
    throw new Error(`Cannot read JSON evidence ${filePath}: ${error.message}`);
  }
  return value;
}

function listJsonFiles(directory) {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory)
    .filter((name) => name.toLowerCase().endsWith('.json'))
    .sort()
    .map((name) => path.join(directory, name));
}

function readEvidenceDirectory(evidenceDirectory) {
  const requiredFiles = Object.fromEntries(
    [...SOURCE_EVIDENCE_FILES, ...REPORT_FILES].map((name) => [
      name,
      REPORT_FILES.includes(name)
        ? true
        : fs.existsSync(path.join(evidenceDirectory, name))
          && fs.statSync(path.join(evidenceDirectory, name)).isFile(),
    ]),
  );
  const candidateFiles = listJsonFiles(path.join(evidenceDirectory, 'candidates'));
  const runFiles = listJsonFiles(path.join(evidenceDirectory, 'runs'));
  const sourceComplete = SOURCE_EVIDENCE_FILES.every((name) => requiredFiles[name]);
  const inventory = {
    complete: sourceComplete && candidateFiles.length === 2,
    requiredFiles,
  };
  if (!requiredFiles['SYSTEM-INFO.json']) {
    throw new Error('Missing required evidence file: SYSTEM-INFO.json');
  }
  return {
    systemInfo: readJson(path.join(evidenceDirectory, 'SYSTEM-INFO.json')),
    candidates: candidateFiles.map(readJson),
    runs: runFiles.map(readJson),
    inventory,
  };
}

function renderMarkdown(report) {
  const lines = [
    '# M0 Compatibility Report',
    '',
    `- Verdict: **${report.verdict.status}**`,
    `- Code: \`${report.verdict.code}\``,
    `- Selected candidate: ${report.verdict.selectedCandidateId ? `\`${report.verdict.selectedCandidateId}\`` : 'none'}`,
    `- Target: ${report.system.productName} build ${report.system.build} ${report.system.architecture}`,
    `- Generated: ${report.generatedAt}`,
    '- Scope: exact target-machine evidence only; this is not a general Windows 10 support claim.',
    '',
    '## Candidate results',
    '',
    '| Candidate | Playwright | Gate | Local cold | Live smoke | Self-test | Orphan-free |',
    '|---|---:|---:|---:|---:|---:|---:|',
  ];
  for (const candidate of report.candidates) {
    lines.push(
      `| ${candidate.candidateId} | ${candidate.playwrightVersion} | ${candidate.status} `
      + `| ${candidate.localCold.passed}/${candidate.localCold.expected} `
      + `| ${candidate.liveSmoke.passed}/${candidate.liveSmoke.expected} `
      + `| ${candidate.selfTestPassed ? 'PASS' : 'FAIL'} `
      + `| ${candidate.orphanFree ? 'yes' : 'no'} |`,
    );
  }
  lines.push('', '## Selection', '', report.verdict.reason);
  for (const candidate of report.candidates) {
    if (candidate.reasons.length > 0) {
      lines.push('', `### ${candidate.candidateId} failures`, '');
      candidate.reasons.forEach((reason) => lines.push(`- ${reason}`));
    }
  }
  lines.push('', '## Evidence inventory', '');
  for (const [name, present] of Object.entries(report.evidenceInventory.requiredFiles)) {
    lines.push(`- ${present ? '[x]' : '[ ]'} \`${name}\``);
  }
  lines.push('');
  return `${lines.join('\n')}\n`;
}

function writeReports(evidenceDirectory, report) {
  fs.mkdirSync(evidenceDirectory, { recursive: true });
  const jsonPath = path.join(evidenceDirectory, 'M0-REPORT.json');
  const markdownPath = path.join(evidenceDirectory, 'M0-REPORT.md');
  const token = `${process.pid}-${Date.now()}`;
  const temporaryJson = `${jsonPath}.${token}.tmp`;
  const temporaryMarkdown = `${markdownPath}.${token}.tmp`;
  fs.writeFileSync(temporaryJson, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  fs.writeFileSync(temporaryMarkdown, renderMarkdown(report), 'utf8');
  fs.renameSync(temporaryJson, jsonPath);
  fs.renameSync(temporaryMarkdown, markdownPath);
  return { jsonPath, markdownPath };
}

function aggregateEvidenceDirectory(evidenceDirectory, { generatedAt } = {}) {
  const evidence = readEvidenceDirectory(evidenceDirectory);
  return aggregateEvidence({ ...evidence, generatedAt });
}

module.exports = {
  REPORT_FILES,
  SOURCE_EVIDENCE_FILES,
  aggregateEvidence,
  aggregateEvidenceDirectory,
  chooseWinner,
  compareSemver,
  readEvidenceDirectory,
  renderMarkdown,
  runPassed,
  writeReports,
};
