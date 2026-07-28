'use strict';

const CANDIDATE_KEYS = [
  'schemaVersion',
  'candidateId',
  'stack',
  'release',
  'requirements',
  'extraRequirements',
  'stabilitySignals',
  'selfTest',
];

const RUN_KEYS = [
  'schemaVersion',
  'candidateId',
  'run',
  'startedAt',
  'finishedAt',
  'coldStart',
  'profileMode',
  'nodeProcessFresh',
  'browserProcessFresh',
  'nodeExitCode',
  'browserStarted',
  'browserExecutable',
  'usedSystemBrowser',
  'browserDownloadAttempted',
  'adminRequired',
  'installRequired',
  'target',
  'locator',
  'action',
  'successMarkerVisible',
  'liveRouteSuccess',
  'liveContentSuccess',
  'orphanProcesses',
  'screenshot',
  'trace',
  'error',
];

class EvidenceValidationError extends Error {
  constructor(label, errors) {
    super(`${label} is invalid:\n- ${errors.join('\n- ')}`);
    this.name = 'EvidenceValidationError';
    this.errors = errors;
  }
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function checkExactKeys(value, required, allowed, path, errors) {
  if (!isObject(value)) {
    errors.push(`${path} must be an object`);
    return false;
  }
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      errors.push(`${path}.${key} is required`);
    }
  }
  for (const key of Object.keys(value)) {
    if (!allowed.includes(key)) {
      errors.push(`${path}.${key} is not allowed`);
    }
  }
  return true;
}

function checkString(value, path, errors, pattern = null) {
  if (typeof value !== 'string' || value.length === 0) {
    errors.push(`${path} must be a non-empty string`);
  } else if (pattern && !pattern.test(value)) {
    errors.push(`${path} has an invalid format`);
  }
}

function checkBoolean(value, path, errors) {
  if (typeof value !== 'boolean') {
    errors.push(`${path} must be a boolean`);
  }
}

function checkInteger(value, path, errors, minimum = null, maximum = null) {
  if (!Number.isInteger(value)) {
    errors.push(`${path} must be an integer`);
  } else {
    if (minimum !== null && value < minimum) errors.push(`${path} must be >= ${minimum}`);
    if (maximum !== null && value > maximum) errors.push(`${path} must be <= ${maximum}`);
  }
}

function checkTimestamp(value, path, errors) {
  checkString(value, path, errors);
  if (typeof value === 'string' && (!/^\d{4}-\d{2}-\d{2}T/.test(value) || Number.isNaN(Date.parse(value)))) {
    errors.push(`${path} must be an RFC 3339 date-time`);
  }
}

function checkEnum(value, values, path, errors) {
  if (!values.includes(value)) {
    errors.push(`${path} must be one of: ${values.join(', ')}`);
  }
}

function checkStringArray(value, path, errors) {
  if (!Array.isArray(value)) {
    errors.push(`${path} must be an array`);
    return;
  }
  const seen = new Set();
  value.forEach((item, index) => {
    checkString(item, `${path}[${index}]`, errors);
    if (seen.has(item)) errors.push(`${path} must not contain duplicates`);
    seen.add(item);
  });
}

function isRelativeEvidencePath(value) {
  if (typeof value !== 'string' || value.length === 0) return false;
  if (/^[A-Za-z]:[\\/]/.test(value) || /^[\\/]/.test(value)) return false;
  return !value.split(/[\\/]/).includes('..');
}

function checkRelativePath(value, path, errors) {
  if (!isRelativeEvidencePath(value)) {
    errors.push(`${path} must be a safe relative path`);
  }
}

function finish(label, errors) {
  if (errors.length > 0) throw new EvidenceValidationError(label, errors);
}

function validateRunRecord(value, label = 'run record') {
  const errors = [];
  const required = RUN_KEYS.filter((key) => !['screenshot', 'trace'].includes(key));
  if (!checkExactKeys(value, required, RUN_KEYS, '$', errors)) finish(label, errors);

  if (value.schemaVersion !== 1) errors.push('$.schemaVersion must equal 1');
  checkString(value.candidateId, '$.candidateId', errors, /^[a-z0-9][a-z0-9._-]{0,63}$/);
  checkInteger(value.run, '$.run', errors, 1, value.target === 'live-site' ? 5 : 20);
  checkTimestamp(value.startedAt, '$.startedAt', errors);
  checkTimestamp(value.finishedAt, '$.finishedAt', errors);
  if (!Number.isNaN(Date.parse(value.startedAt)) && !Number.isNaN(Date.parse(value.finishedAt))
      && Date.parse(value.finishedAt) < Date.parse(value.startedAt)) {
    errors.push('$.finishedAt must not precede $.startedAt');
  }
  checkBoolean(value.coldStart, '$.coldStart', errors);
  checkEnum(value.profileMode, ['fresh', 'persistent'], '$.profileMode', errors);
  checkBoolean(value.nodeProcessFresh, '$.nodeProcessFresh', errors);
  checkBoolean(value.browserProcessFresh, '$.browserProcessFresh', errors);
  checkInteger(value.nodeExitCode, '$.nodeExitCode', errors);
  checkBoolean(value.browserStarted, '$.browserStarted', errors);
  checkRelativePath(value.browserExecutable, '$.browserExecutable', errors);
  checkBoolean(value.usedSystemBrowser, '$.usedSystemBrowser', errors);
  checkBoolean(value.browserDownloadAttempted, '$.browserDownloadAttempted', errors);
  checkBoolean(value.adminRequired, '$.adminRequired', errors);
  checkBoolean(value.installRequired, '$.installRequired', errors);
  checkEnum(value.target, ['local-fixture', 'live-site'], '$.target', errors);
  checkString(value.locator, '$.locator', errors);
  if (value.action !== 'locator.click') errors.push('$.action must equal locator.click');
  checkBoolean(value.successMarkerVisible, '$.successMarkerVisible', errors);
  if (value.target === 'live-site') {
    checkBoolean(value.liveRouteSuccess, '$.liveRouteSuccess', errors);
    checkBoolean(value.liveContentSuccess, '$.liveContentSuccess', errors);
  } else if (value.liveRouteSuccess !== null || value.liveContentSuccess !== null) {
    errors.push('local-fixture runs must set liveRouteSuccess and liveContentSuccess to null');
  }
  checkStringArray(value.orphanProcesses, '$.orphanProcesses', errors);
  for (const field of ['screenshot', 'trace']) {
    if (value[field] !== undefined && value[field] !== null) {
      checkRelativePath(value[field], `$.${field}`, errors);
    }
  }
  if (value.error !== null) {
    if (checkExactKeys(value.error, ['message'], ['name', 'message', 'code'], '$.error', errors)) {
      checkString(value.error.message, '$.error.message', errors);
      if (value.error.name !== undefined) checkString(value.error.name, '$.error.name', errors);
      if (value.error.code !== undefined && value.error.code !== null
          && typeof value.error.code !== 'string' && !Number.isInteger(value.error.code)) {
        errors.push('$.error.code must be a string, integer, or null');
      }
    }
  }
  finish(label, errors);
  return value;
}

function validateCandidateEvidence(value, label = 'candidate evidence') {
  const errors = [];
  if (!checkExactKeys(value, CANDIDATE_KEYS, CANDIDATE_KEYS, '$', errors)) finish(label, errors);
  if (value.schemaVersion !== 1) errors.push('$.schemaVersion must equal 1');
  checkString(value.candidateId, '$.candidateId', errors, /^[a-z0-9][a-z0-9._-]{0,63}$/);

  const stackKeys = [
    'nodeVersion', 'nodeArchitecture', 'playwrightVersion', 'browserName',
    'browserVersion', 'browserRevision', 'browserExecutableSha256',
  ];
  if (checkExactKeys(value.stack, stackKeys, stackKeys, '$.stack', errors)) {
    checkString(value.stack.nodeVersion, '$.stack.nodeVersion', errors, /^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/);
    if (value.stack.nodeArchitecture !== 'x64') errors.push('$.stack.nodeArchitecture must equal x64');
    checkString(value.stack.playwrightVersion, '$.stack.playwrightVersion', errors, /^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/);
    checkString(value.stack.browserName, '$.stack.browserName', errors);
    checkString(value.stack.browserVersion, '$.stack.browserVersion', errors);
    checkString(value.stack.browserRevision, '$.stack.browserRevision', errors, /^\d+$/);
    checkString(value.stack.browserExecutableSha256, '$.stack.browserExecutableSha256', errors, /^[a-f0-9]{64}$/);
  }

  const releaseKeys = ['zipFile', 'zipSha256', 'packageLockFile', 'packageLockSha256'];
  if (checkExactKeys(value.release, releaseKeys, releaseKeys, '$.release', errors)) {
    checkRelativePath(value.release.zipFile, '$.release.zipFile', errors);
    checkString(value.release.zipSha256, '$.release.zipSha256', errors, /^[a-f0-9]{64}$/);
    checkRelativePath(value.release.packageLockFile, '$.release.packageLockFile', errors);
    checkString(value.release.packageLockSha256, '$.release.packageLockSha256', errors, /^[a-f0-9]{64}$/);
  }

  const requirementKeys = [
    'administrator', 'installedNode', 'installedBrowser',
    'installStep', 'downloadAtRuntime', 'systemBrowser',
  ];
  if (checkExactKeys(value.requirements, requirementKeys, requirementKeys, '$.requirements', errors)) {
    for (const key of requirementKeys) checkBoolean(value.requirements[key], `$.requirements.${key}`, errors);
  }
  checkStringArray(value.extraRequirements, '$.extraRequirements', errors);
  checkStringArray(value.stabilitySignals, '$.stabilitySignals', errors);

  const selfTestKeys = [
    'passed', 'offline', 'targetSiteAccessed', 'localFixtureClick',
    'probeFilesCreatedAndDeleted', 'nodeExitCode', 'browserStarted', 'browserClosed',
    'usedSystemBrowser', 'browserDownloadAttempted', 'adminRequired',
    'installRequired', 'orphanProcesses', 'error',
  ];
  const selfTestRequired = selfTestKeys.filter((key) => key !== 'error');
  if (checkExactKeys(value.selfTest, selfTestRequired, selfTestKeys, '$.selfTest', errors)) {
    for (const key of [
      'passed', 'offline', 'targetSiteAccessed', 'localFixtureClick',
      'probeFilesCreatedAndDeleted', 'browserStarted', 'browserClosed',
      'usedSystemBrowser', 'browserDownloadAttempted', 'adminRequired', 'installRequired',
    ]) {
      checkBoolean(value.selfTest[key], `$.selfTest.${key}`, errors);
    }
    checkInteger(value.selfTest.nodeExitCode, '$.selfTest.nodeExitCode', errors);
    checkStringArray(value.selfTest.orphanProcesses, '$.selfTest.orphanProcesses', errors);
    if (value.selfTest.error !== undefined && value.selfTest.error !== null) {
      checkString(value.selfTest.error, '$.selfTest.error', errors);
    }
  }
  finish(label, errors);
  return value;
}

function validateSystemInfo(value, label = 'system info') {
  const errors = [];
  const rootKeys = ['schemaVersion', 'collectedAt', 'os', 'user', 'baseline', 'pathVariants'];
  if (!checkExactKeys(value, rootKeys, rootKeys, '$', errors)) finish(label, errors);
  if (value.schemaVersion !== 1) errors.push('$.schemaVersion must equal 1');
  checkTimestamp(value.collectedAt, '$.collectedAt', errors);

  const osKeys = ['productName', 'version', 'build', 'architecture'];
  if (checkExactKeys(value.os, osKeys, osKeys, '$.os', errors)) {
    if (value.os.productName !== 'Windows 10') errors.push('$.os.productName must equal Windows 10');
    checkString(value.os.version, '$.os.version', errors);
    checkString(value.os.build, '$.os.build', errors, /^\d+(?:\.\d+)*$/);
    if (value.os.architecture !== 'x64') errors.push('$.os.architecture must equal x64');
  }

  const userKeys = ['standardUser', 'administrator'];
  if (checkExactKeys(value.user, userKeys, userKeys, '$.user', errors)) {
    checkBoolean(value.user.standardUser, '$.user.standardUser', errors);
    checkBoolean(value.user.administrator, '$.user.administrator', errors);
  }
  const baselineKeys = ['installedNode', 'installedChrome', 'installedPlaywright'];
  if (checkExactKeys(value.baseline, baselineKeys, baselineKeys, '$.baseline', errors)) {
    for (const key of baselineKeys) checkBoolean(value.baseline[key], `$.baseline.${key}`, errors);
  }
  if (!Array.isArray(value.pathVariants) || value.pathVariants.length < 3) {
    errors.push('$.pathVariants must contain at least three entries');
  } else {
    const kinds = [];
    value.pathVariants.forEach((entry, index) => {
      const path = `$.pathVariants[${index}]`;
      if (checkExactKeys(entry, ['kind', 'path', 'passed'], ['kind', 'path', 'passed'], path, errors)) {
        checkEnum(entry.kind, ['plain', 'spaces', 'unicode'], `${path}.kind`, errors);
        checkString(entry.path, `${path}.path`, errors);
        checkBoolean(entry.passed, `${path}.passed`, errors);
        kinds.push(entry.kind);
      }
    });
    for (const kind of ['plain', 'spaces', 'unicode']) {
      if (kinds.filter((valueKind) => valueKind === kind).length !== 1) {
        errors.push(`$.pathVariants must contain exactly one ${kind} entry`);
      }
    }
  }
  finish(label, errors);
  return value;
}

function validateReport(value, label = 'M0 report') {
  const errors = [];
  const rootKeys = [
    'schemaVersion', 'generatedAt', 'targetMachineOnly', 'system',
    'evidenceInventory', 'candidates', 'verdict',
  ];
  if (!checkExactKeys(value, rootKeys, rootKeys, '$', errors)) finish(label, errors);
  if (value.schemaVersion !== 1) errors.push('$.schemaVersion must equal 1');
  checkTimestamp(value.generatedAt, '$.generatedAt', errors);
  if (value.targetMachineOnly !== true) errors.push('$.targetMachineOnly must equal true');
  if (!isObject(value.system)) errors.push('$.system must be an object');
  if (!isObject(value.evidenceInventory)) errors.push('$.evidenceInventory must be an object');
  if (!Array.isArray(value.candidates) || value.candidates.length !== 2) {
    errors.push('$.candidates must contain exactly two candidate results');
  }
  if (!isObject(value.verdict)) {
    errors.push('$.verdict must be an object');
  } else {
    checkEnum(value.verdict.status, ['PASS', 'FAIL'], '$.verdict.status', errors);
    checkEnum(
      value.verdict.code,
      ['M0_COMPATIBILITY_PASS', 'PLAYWRIGHT_ON_TARGET_WIN10_NO_GO'],
      '$.verdict.code',
      errors,
    );
    if (value.verdict.selectedCandidateId !== null) {
      checkString(value.verdict.selectedCandidateId, '$.verdict.selectedCandidateId', errors);
    }
    checkString(value.verdict.reason, '$.verdict.reason', errors);
  }
  finish(label, errors);
  return value;
}

module.exports = {
  EvidenceValidationError,
  isRelativeEvidencePath,
  validateCandidateEvidence,
  validateReport,
  validateRunRecord,
  validateSystemInfo,
};
