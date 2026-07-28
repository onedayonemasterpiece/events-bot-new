#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function parseArgs(argv) {
  if (argv.length !== 4 || argv[0] !== '--portable-root' || argv[2] !== '--output') {
    throw new Error('Usage: run-path-matrix.js --portable-root ROOT --output JSON');
  }
  return {
    portableRoot: path.resolve(argv[1]),
    output: path.resolve(argv[3]),
  };
}

function main() {
  if (process.platform !== 'win32' || process.arch !== 'x64') {
    throw new Error('PATH_MATRIX_TARGET_REQUIRED: run on target Windows x64');
  }
  const { portableRoot, output } = parseArgs(process.argv.slice(2));
  const parent = path.dirname(portableRoot);
  const cases = [
    ['plain', path.join(parent, 'APM0Plain')],
    ['spaces', path.join(parent, 'AP M0 path with spaces')],
    ['unicode', path.join(parent, 'Автопрезентатор-M0')],
  ];
  const records = [];
  for (const [kind, destination] of cases) {
    fs.rmSync(destination, { recursive: true, force: true });
    fs.cpSync(portableRoot, destination, {
      recursive: true,
      filter(source) {
        const relative = path.relative(portableRoot, source);
        return !relative || !['data', 'logs'].includes(relative.split(path.sep)[0]);
      },
    });
    const result = spawnSync('cmd.exe', ['/d', '/s', '/c', 'self-test.cmd'], {
      cwd: destination,
      encoding: 'utf8',
      timeout: 120000,
      windowsHide: false,
    });
    const selfTestPath = path.join(destination, 'logs', 'self-test.json');
    let selfTestPassed = false;
    if (fs.existsSync(selfTestPath)) {
      const selfTest = JSON.parse(fs.readFileSync(selfTestPath, 'utf8'));
      selfTestPassed =
        result.status === 0 &&
        selfTest.selfTest?.passed === true &&
        path.resolve(selfTest.portablePath) === path.resolve(destination);
    }
    const retainedDirectory = path.join(portableRoot, 'logs', 'path-matrix');
    fs.mkdirSync(retainedDirectory, { recursive: true });
    const retainedPath = path.join(retainedDirectory, `${kind}-self-test.json`);
    if (fs.existsSync(selfTestPath)) {
      fs.copyFileSync(selfTestPath, retainedPath);
    } else {
      fs.writeFileSync(
        retainedPath,
        `${JSON.stringify({
          schemaVersion: 1,
          kind: 'autopresenter-m0-path-self-test-failure',
          pathKind: kind,
          portablePath: destination,
          exitCode: result.status,
          error: String(result.error?.message || result.stderr || 'self-test did not emit JSON').slice(0, 2000),
        }, null, 2)}\n`,
      );
    }
    records.push({
      kind,
      path: destination,
      passed: selfTestPassed,
      selfTest: path.relative(portableRoot, retainedPath).split(path.sep).join('/'),
      selfTestSha256: sha256(retainedPath),
    });
    fs.rmSync(destination, { recursive: true, force: true });
  }
  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, `${JSON.stringify(records, null, 2)}\n`, 'utf8');
  if (records.some((record) => !record.passed)) process.exitCode = 1;
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 2;
}
