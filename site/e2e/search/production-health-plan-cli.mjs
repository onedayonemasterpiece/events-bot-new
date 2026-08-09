#!/usr/bin/env node

import { planProductionHealthRun } from './production-health-planner.mjs';

const MULTI_VALUE_ARGS = new Set(['changed-path', 'changed-surface']);
const KNOWN_ARGS = new Set([
  'plane', 'trigger', 'profile', 'changed-path', 'changed-surface',
  'site-runtime-sha', 'search-backend-revision', 'validation-profile', 'deployment-run-id',
]);

const parseArgs = (argv) => {
  const raw = { 'changed-path': [], 'changed-surface': [] };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    let name;
    let value;
    if (arg.startsWith('--') && arg.includes('=')) {
      [name, ...value] = arg.slice(2).split('=');
      value = value.join('=');
    } else if (arg.startsWith('--')) {
      name = arg.slice(2);
      value = argv[index + 1];
      if (value && !value.startsWith('--')) index += 1;
    }
    if (!KNOWN_ARGS.has(name)) throw new Error(`search_health_cli_argument_unknown:${String(name || arg).slice(0, 64)}`);
    if (!value || value.startsWith('--')) throw new Error(`search_health_cli_value_missing:${name}`);
    if (MULTI_VALUE_ARGS.has(name)) raw[name].push(value);
    else if (Object.hasOwn(raw, name)) throw new Error(`search_health_cli_argument_duplicate:${name}`);
    else raw[name] = value;
  }
  const deploymentFields = [
    'site-runtime-sha', 'search-backend-revision', 'validation-profile', 'deployment-run-id',
  ];
  const hasDeploymentMarker = deploymentFields.some((name) => Object.hasOwn(raw, name))
    || raw['changed-surface'].length > 0;
  return {
    plane: raw.plane,
    trigger: raw.trigger,
    profile: raw.profile,
    changedPaths: raw['changed-path'],
    ...(hasDeploymentMarker ? {
      deploymentMarker: {
        site_runtime_sha: raw['site-runtime-sha'],
        search_backend_revision: raw['search-backend-revision'],
        validation_profile: raw['validation-profile'],
        changed_surfaces: raw['changed-surface'],
        deployment_run_id: raw['deployment-run-id'],
      },
    } : {}),
  };
};

try {
  const plan = planProductionHealthRun(parseArgs(process.argv.slice(2)));
  process.stdout.write(`${JSON.stringify(plan)}\n`);
} catch (error) {
  const code = String(error?.message || 'search_health_cli_failed').replace(/[^a-z0-9:_-]/giu, '_').slice(0, 160);
  process.stdout.write(`${JSON.stringify({
    schema_version: 'search_production_health_stage2_cli_error_v1',
    error: code,
  })}\n`);
  process.exitCode = 1;
}
