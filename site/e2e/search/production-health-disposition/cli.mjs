#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import process from 'node:process';

import { buildSearchHealthReportPlan } from './report-plan.mjs';

const usage = 'usage: node cli.mjs [--input <json-file>]';

const parseArgs = (argv) => {
  if (argv.length === 0) return { input: null };
  if (argv.length === 2 && argv[0] === '--input' && argv[1]) return { input: argv[1] };
  throw new Error(usage);
};

const readStdin = async () => {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
};

const assertEnvelope = (value) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('search_health_report_input_invalid:record');
  }
  const fields = Object.keys(value).sort();
  if (!fields.every((field) => ['history', 'summary'].includes(field)) || !fields.includes('summary')) {
    throw new Error('search_health_report_input_invalid:field_allowlist');
  }
};

export async function runSearchHealthReportPlanCli(argv = process.argv.slice(2)) {
  const { input } = parseArgs(argv);
  const source = input ? await readFile(input, 'utf8') : await readStdin();
  const envelope = JSON.parse(source);
  assertEnvelope(envelope);
  return buildSearchHealthReportPlan({
    summary: envelope.summary,
    history: envelope.history || [],
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const plan = await runSearchHealthReportPlanCli();
    process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
