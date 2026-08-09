#!/usr/bin/env node
import { waitForActiveSearchBackend } from './search-backend-release-probe.mjs';

const required = (name) => {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`search_backend_probe_env_missing:${name.toLowerCase()}`);
  return value;
};

try {
  const receipt = await waitForActiveSearchBackend({
    supabaseUrl: required('PERSONALIZATION_SUPABASE_URL'),
    publishableKey: required('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'),
    expectedRevision: required('E2E_EXPECTED_SEARCH_BACKEND_REVISION'),
  });
  process.stdout.write(`${JSON.stringify({
    schema_version: receipt.schema_version, active: receipt.active,
    observed_revision: receipt.observed_revision,
    observed_contract_version: receipt.observed_contract_version || null,
    attempts: receipt.attempts, product_search_posts: 0, auth_requests: 0,
  })}\n`);
  if (!receipt.active) process.exitCode = 3;
} catch (error) {
  process.stderr.write(`${String(error?.message || 'search_backend_release_probe_failed').split(':')[0]}\n`);
  process.exitCode = 2;
}
