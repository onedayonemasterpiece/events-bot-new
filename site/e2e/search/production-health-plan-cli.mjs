#!/usr/bin/env node

import { planProductionHealthRun } from './production-health-planner.mjs';

const parseArgs = (argv) => {
  const parsed = { changedPaths: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--plane' || arg === '--trigger' || arg === '--changed-path') {
      const value = argv[index + 1];
      if (!value || value.startsWith('--')) throw new Error(`search_health_cli_value_missing:${arg.slice(2)}`);
      index += 1;
      if (arg === '--changed-path') parsed.changedPaths.push(value);
      else parsed[arg.slice(2)] = value;
      continue;
    }
    if (arg.startsWith('--plane=') || arg.startsWith('--trigger=') || arg.startsWith('--changed-path=')) {
      const [name, ...parts] = arg.slice(2).split('=');
      const value = parts.join('=');
      if (!value) throw new Error(`search_health_cli_value_missing:${name}`);
      if (name === 'changed-path') parsed.changedPaths.push(value);
      else parsed[name] = value;
      continue;
    }
    throw new Error(`search_health_cli_argument_unknown:${arg.slice(0, 64)}`);
  }
  return parsed;
};

try {
  const plan = planProductionHealthRun(parseArgs(process.argv.slice(2)));
  process.stdout.write(`${JSON.stringify(plan)}\n`);
} catch (error) {
  const code = String(error?.message || 'search_health_cli_failed').replace(/[^a-z0-9:_-]/giu, '_').slice(0, 160);
  process.stdout.write(`${JSON.stringify({
    schema_version: 'search_production_health_stage1_cli_error_v1',
    error: code,
  })}\n`);
  process.exitCode = 1;
}
