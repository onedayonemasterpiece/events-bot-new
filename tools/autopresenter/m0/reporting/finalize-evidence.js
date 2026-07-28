#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { aggregateEvidenceDirectory, writeReports } = require('./aggregate');

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function walk(root) {
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const value = path.join(root, entry.name);
    return entry.isDirectory() ? walk(value) : [value];
  });
}

function main() {
  if (process.argv.length !== 3) throw new Error('Usage: finalize-evidence.js EVIDENCE_ROOT');
  const root = path.resolve(process.argv[2]);
  const ids = ['current-control', 'pre-cft-compat'];
  const versions = Object.fromEntries(ids.map((id) => [
    id,
    JSON.parse(fs.readFileSync(path.join(root, 'sources', id, 'VERSIONS.json'), 'utf8')),
  ]));
  const systems = Object.fromEntries(ids.map((id) => [
    id,
    JSON.parse(fs.readFileSync(path.join(root, 'sources', id, 'SYSTEM-INFO.json'), 'utf8')),
  ]));
  const fingerprints = new Set(
    ids.map((id) => systems[id].provenance?.machineAccountFingerprint),
  );
  const osBuilds = new Set(ids.map((id) => systems[id].os?.build));
  if (
    fingerprints.size !== 1 ||
    osBuilds.size !== 1 ||
    ids.some((id) => systems[id].provenance?.sourceCandidateId !== id)
  ) {
    throw new Error('Both candidates must be tested on the same target machine/account');
  }
  fs.writeFileSync(
    path.join(root, 'SYSTEM-INFO.json'),
    `${JSON.stringify(systems[ids[0]], null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(root, 'VERSIONS.json'),
    `${JSON.stringify({ schemaVersion: 1, candidates: versions }, null, 2)}\n`,
  );
  const excluded = new Set(['M0-REPORT.json', 'M0-REPORT.md', 'RELEASE-MANIFEST.json', 'SHA256SUMS.txt']);
  const files = walk(root)
    .filter((file) => !excluded.has(path.basename(file)))
    .sort()
    .map((file) => ({
      path: path.relative(root, file).split(path.sep).join('/'),
      sha256: sha256(file),
      sizeBytes: fs.statSync(file).size,
    }));
  const manifest = {
    schemaVersion: 1,
    scope: 'autopresenter-m0-target-evidence',
    generatedAt: new Date().toISOString(),
    targetMachineOnly: true,
    candidateIds: ids,
    files,
  };
  fs.writeFileSync(path.join(root, 'RELEASE-MANIFEST.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  const checksummed = [...files, {
    path: 'RELEASE-MANIFEST.json',
    sha256: sha256(path.join(root, 'RELEASE-MANIFEST.json')),
  }];
  fs.writeFileSync(
    path.join(root, 'SHA256SUMS.txt'),
    `${checksummed.map((entry) => `${entry.sha256}  ${entry.path}`).join('\n')}\n`,
  );
  const report = aggregateEvidenceDirectory(root);
  const output = writeReports(root, report);
  process.stdout.write(`${JSON.stringify({ verdict: report.verdict, ...output }, null, 2)}\n`);
  process.exitCode = report.verdict.status === 'PASS' ? 0 : 1;
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 2;
}
