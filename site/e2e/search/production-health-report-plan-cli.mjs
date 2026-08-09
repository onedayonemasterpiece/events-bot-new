#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';
import process from 'node:process';

import { applySearchHealthReportPlan } from '../../../.github/scripts/apply-search-health-report-plan.mjs';
import { classifyProductionHealthOutcome } from './production-health-contract.mjs';
import { buildSearchHealthReportPlan } from './production-health-disposition/report-plan.mjs';

const sha = (value) => createHash('sha256').update(String(value), 'utf8').digest('hex');
const platforms = ['browser', 'android', 'ios'];
const safeFallbackFailureClasses = new Set([
  'UNKNOWN_AUTH_BROKER', 'UNKNOWN_RUNNER_BROWSER', 'UNKNOWN_ANDROID_INFRA', 'UNKNOWN_IOS_INFRA',
  'BLOCKED_RELEASE_NOT_ACTIVE', 'EVIDENCE_REDACTION_FAILED',
]);

async function resultsBelow(root) {
  const found = [];
  async function walk(directory) {
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile() && entry.name === 'result.json') {
        const value = JSON.parse(await readFile(path, 'utf8'));
        if (value?.schema_version === 'search_production_health_evidence_v1') found.push(value);
      }
    }
  }
  if (root) await walk(resolve(root));
  return found;
}

async function aggregateSummariesBelow(root) {
  const found = [];
  async function walk(directory) {
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile() && entry.name === 'summary.json') {
        let value;
        try { value = JSON.parse(await readFile(path, 'utf8')); } catch { continue; }
        if (value?.schema_version !== 'search_production_health_workflow_summary_v1'
          || !Array.isArray(value.platforms)) continue;
        for (const item of value.platforms) {
          found.push({
            schema_version: 'search_production_health_summary_v1',
            platform: item.platform,
            product_health: item.product_health,
            execution_status: item.execution_status,
            failure_class: item.failure_class,
            target_superseded: item.target_superseded === true,
            target_fingerprint: item.target_fingerprint,
            runtime_fingerprint: item.runtime_fingerprint,
            run_id: item.run_id,
            run_url: item.run_url,
          });
        }
      }
    }
  }
  if (root) await walk(resolve(root));
  return found;
}

function summaryFromRecord(record, env) {
  const runId = String(env.GITHUB_RUN_ID || '');
  const repository = String(env.GITHUB_REPOSITORY || '');
  return {
    schema_version: 'search_production_health_summary_v1',
    platform: record.platform,
    product_health: record.product_health,
    execution_status: record.execution_status,
    failure_class: record.failure_class,
    target_superseded: record.target?.target_superseded === true,
    target_fingerprint: record.target?.target_url_sha256,
    runtime_fingerprint: sha(JSON.stringify({
      repo_sha: record.target?.target_repo_sha,
      search_contract_version: record.search?.response?.search_contract_version,
      search_backend_revision: record.search?.response?.search_backend_revision,
      expected_backend_revision: record.search?.expected_backend_revision,
    })),
    run_id: runId,
    run_url: `https://github.com/${repository}/actions/runs/${runId}`,
  };
}

function fallbackSummary(platform, env) {
  const defaultFailure = {
    browser: 'UNKNOWN_RUNNER_BROWSER', android: 'UNKNOWN_ANDROID_INFRA', ios: 'UNKNOWN_IOS_INFRA',
  }[platform];
  const supplied = String(env[`${platform.toUpperCase()}_FAILURE_CLASS`] || '');
  let failure = defaultFailure;
  try {
    if (supplied && safeFallbackFailureClasses.has(supplied)) {
      classifyProductionHealthOutcome({ failureClass: supplied, platform });
      failure = supplied;
    }
  } catch { /* an untrusted workflow output cannot widen the fixed taxonomy */ }
  const outcome = classifyProductionHealthOutcome({ failureClass: failure, platform });
  const runId = String(env.GITHUB_RUN_ID || '');
  return {
    schema_version: 'search_production_health_summary_v1', platform,
    product_health: outcome.product_health, execution_status: outcome.execution_status, failure_class: failure,
    target_superseded: false,
    target_fingerprint: sha(`target-unavailable:${platform}`),
    runtime_fingerprint: sha(`${env.GITHUB_SHA || 'unknown'}:${platform}`),
    run_id: runId,
    run_url: `https://github.com/${env.GITHUB_REPOSITORY || ''}/actions/runs/${runId}`,
  };
}

function aggregatePlatformSummary(summary, record) {
  if (!record) return {
    ...summary,
    evidence_available: false,
    tested_at: null,
    target_url_sha256: null,
    target_superseded: false,
    site_runtime_sha: null,
    search_backend_revision: null,
    content_generation_id: null,
    search_index_generation_id: null,
    search_contract_version: null,
    request_id: null,
    search_post_count: 0,
    result_count: 0,
    rendered_card_count: 0,
    opened_route_status: null,
    latency_ms: null,
    cache_status: null,
    provider_attempt_counts: { embedding: 0, vector: 0, llm: 0 },
    pagination_requests: 0,
    client_observed_supabase_bytes: 0,
  };
  return {
    ...summary,
    evidence_available: true,
    tested_at: record.tested_at,
    target_url_sha256: record.target?.target_url_sha256,
    target_superseded: record.target?.target_superseded === true,
    site_runtime_sha: record.target?.target_repo_sha,
    search_backend_revision: record.search?.response?.search_backend_revision,
    content_generation_id: record.search?.response?.catalog_revision,
    search_index_generation_id: record.search?.response?.corpus_revision,
    search_contract_version: record.search?.response?.search_contract_version,
    request_id: record.search?.response?.request_id,
    search_post_count: Number(record.search?.physical_post_count || 0),
    result_count: Number(record.search?.response_id_count || 0),
    rendered_card_count: Number(record.search?.card_count || 0),
    opened_route_status: Number(record.search?.event_route?.http_status || 0),
    latency_ms: Number(record.search?.latency_ms || 0),
    cache_status: record.search?.cache_state,
    provider_attempt_counts: record.search?.provider_attempts,
    pagination_requests: Number(record.search?.forbidden_activity?.pagination_requests || 0),
    client_observed_supabase_bytes: Number(record.supabase_observed_bytes?.total_bytes || 0),
  };
}

function parse(argv) {
  const options = { evidenceRoot: '', historyRoot: '', aggregateOutput: '', apply: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--apply') options.apply = true;
    else if (argv[i] === '--evidence-root' && argv[i + 1]) options.evidenceRoot = argv[++i];
    else if (argv[i] === '--history-root' && argv[i + 1]) options.historyRoot = argv[++i];
    else if (argv[i] === '--aggregate-output' && argv[i + 1]) options.aggregateOutput = argv[++i];
    else throw new Error('search_health_report_arguments_invalid');
  }
  if (!options.evidenceRoot) throw new Error('search_health_report_evidence_root_missing');
  return options;
}

export async function runProductionHealthReporter(argv = process.argv.slice(2), env = process.env) {
  const options = parse(argv);
  const currentRunId = String(env.GITHUB_RUN_ID || '');
  const currentRecords = (await resultsBelow(options.evidenceRoot))
    .filter((item) => String(item.workflow_run_id || '') === currentRunId)
    .sort((left, right) => String(left.tested_at || '').localeCompare(String(right.tested_at || '')));
  const priorRecords = await resultsBelow(options.historyRoot);
  const priorAggregates = await aggregateSummariesBelow(options.historyRoot);
  // GitHub's "rerun failed jobs" does not recreate artifacts for successful
  // jobs. Current evidence can therefore contain browser attempt 1 and iOS
  // attempts 1+2; sorted Map replacement deterministically keeps the latest
  // sanitized receipt for each platform without treating an earlier success as
  // missing.
  const currentByPlatform = new Map(currentRecords.map((item) => [item.platform, item]));
  const priorEvidence = priorRecords.map((item) => summaryFromRecord(item, {
    ...env,
    GITHUB_RUN_ID: String(item.workflow_run_id || env.GITHUB_RUN_ID || ''),
  }));
  // Aggregate summaries preserve typed pre-runner UNKNOWN cells that have no
  // platform result.json. Prefer the platform evidence when both are present.
  const priorByIdentity = new Map(priorAggregates.map((item) => [`${item.run_id}:${item.platform}`, item]));
  for (const item of priorEvidence) priorByIdentity.set(`${item.run_id}:${item.platform}`, item);
  const prior = [...priorByIdentity.values()].filter((item) => item.run_id !== String(env.GITHUB_RUN_ID || '')).sort((left, right) => {
    const a = BigInt(left.run_id);
    const b = BigInt(right.run_id);
    return a < b ? -1 : a > b ? 1 : 0;
  });
  const outputs = [];
  const aggregatePlatforms = [];
  for (const platform of platforms) {
    const result = String(env[`${platform.toUpperCase()}_RESULT`] || 'skipped');
    if (result === 'skipped') continue;
    const summary = currentByPlatform.has(platform)
      ? summaryFromRecord(currentByPlatform.get(platform), env)
      : fallbackSummary(platform, env);
    aggregatePlatforms.push(aggregatePlatformSummary(summary, currentByPlatform.get(platform)));
    const history = prior.filter((item) => item.platform === platform);
    const plan = buildSearchHealthReportPlan({ summary, history });
    const applied = options.apply
      ? await applySearchHealthReportPlan(plan, { token: env.GH_TOKEN || env.GITHUB_TOKEN })
      : { action: plan.operation.action, dry_run: true };
    outputs.push({ platform, action: applied.action });
  }
  if (options.aggregateOutput && !aggregatePlatforms.some((item) => item.failure_class === 'EVIDENCE_REDACTION_FAILED')) {
    const output = resolve(options.aggregateOutput);
    await mkdir(resolve(output, '..'), { recursive: true, mode: 0o700 });
    const totals = aggregatePlatforms.reduce((total, item) => ({
      search_post_count: total.search_post_count + Number(item.search_post_count || 0),
      llm_attempts: total.llm_attempts + Number(item.provider_attempt_counts?.llm || 0),
      pagination_requests: total.pagination_requests + Number(item.pagination_requests || 0),
      client_observed_supabase_bytes: total.client_observed_supabase_bytes
        + Number(item.client_observed_supabase_bytes || 0),
    }), {
      search_post_count: 0,
      llm_attempts: 0,
      pagination_requests: 0,
      client_observed_supabase_bytes: 0,
    });
    await writeFile(output, `${JSON.stringify({
      schema_version: 'search_production_health_workflow_summary_v1',
      workflow_run_id: String(env.GITHUB_RUN_ID || ''),
      platforms: aggregatePlatforms,
      platform_count: aggregatePlatforms.length,
      aggregate: totals,
    }, null, 2)}\n`, { mode: 0o600 });
  }
  return outputs;
}

if (process.argv[1] && basename(process.argv[1]) === basename(new URL(import.meta.url).pathname)) {
  runProductionHealthReporter().then((value) => process.stdout.write(`${JSON.stringify(value)}\n`))
    .catch((error) => { process.stderr.write(`${String(error?.message || 'search_health_report_failed')}\n`); process.exitCode = 1; });
}
