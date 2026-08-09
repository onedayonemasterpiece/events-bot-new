#!/usr/bin/env node

import { validateSearchRuntimeDeployPayload } from '../site/e2e/search/production-health-planner.mjs';

const parseArgs = (argv) => {
  const parsed = { changed_surfaces: [] };
  const names = new Map([
    ['site-runtime-sha', 'site_runtime_sha'],
    ['search-backend-revision', 'search_backend_revision'],
    ['validation-profile', 'validation_profile'],
    ['changed-surface', 'changed_surfaces'],
    ['deployment-run-id', 'deployment_run_id'],
  ]);
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    let name;
    let value;
    if (token.startsWith('--') && token.includes('=')) {
      [name, ...value] = token.slice(2).split('=');
      value = value.join('=');
    } else if (token.startsWith('--')) {
      name = token.slice(2);
      value = argv[index + 1];
      if (value && !value.startsWith('--')) index += 1;
    }
    const field = names.get(name);
    if (!field) throw new Error(`search_runtime_dispatch_argument_unknown:${String(name || token).slice(0, 64)}`);
    if (!value || value.startsWith('--')) throw new Error(`search_runtime_dispatch_value_missing:${name}`);
    if (field === 'changed_surfaces') parsed.changed_surfaces.push(value);
    else if (Object.hasOwn(parsed, field)) throw new Error(`search_runtime_dispatch_argument_duplicate:${name}`);
    else parsed[field] = value;
  }
  return parsed;
};

export function buildSearchRuntimeDeployDispatch(payload) {
  return Object.freeze({
    event_type: 'search-runtime-deployed',
    client_payload: validateSearchRuntimeDeployPayload(payload),
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    process.stdout.write(`${JSON.stringify(buildSearchRuntimeDeployDispatch(parseArgs(process.argv.slice(2))))}\n`);
  } catch (error) {
    process.stderr.write(`${String(error?.message || 'search_runtime_dispatch_failed').replace(/https?:\/\/\S+/gu, '<redacted-url>')}\n`);
    process.exitCode = 1;
  }
}
