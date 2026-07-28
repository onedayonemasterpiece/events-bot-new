#!/usr/bin/env node
'use strict';

const path = require('node:path');
const { aggregateEvidenceDirectory, writeReports } = require('./aggregate');

function main(argv) {
  if (argv.length !== 1 || ['-h', '--help'].includes(argv[0])) {
    process.stderr.write('Usage: node reporting/cli.js <evidence-directory>\n');
    return 2;
  }
  const evidenceDirectory = path.resolve(argv[0]);
  const report = aggregateEvidenceDirectory(evidenceDirectory);
  const output = writeReports(evidenceDirectory, report);
  process.stdout.write(`${JSON.stringify({
    verdict: report.verdict,
    reportJson: output.jsonPath,
    reportMarkdown: output.markdownPath,
  }, null, 2)}\n`);
  return report.verdict.status === 'PASS' ? 0 : 1;
}

try {
  process.exitCode = main(process.argv.slice(2));
} catch (error) {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 2;
}
