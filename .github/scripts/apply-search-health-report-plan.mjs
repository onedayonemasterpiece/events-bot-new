#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import process from 'node:process';

import {
  SEARCH_HEALTH_REPORT_ACTIONS,
  normalizeSearchHealthReportPlan,
} from '../../site/e2e/search/production-health-disposition/report-plan.mjs';

const API_VERSION = '2022-11-28';
const HEALTH_LABEL = 'search-production-health';
const MAX_ISSUE_PAGES = 10;
const PRODUCT_FINGERPRINT_PATTERN = /^search-product:(browser|android|ios):BROKEN_[A-Z_]+$/u;

const fail = (reason) => {
  throw new Error(`search_health_report_apply_failed:${reason}`);
};

const normalizeRepository = (value) => {
  const normalized = String(value || '');
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u.test(normalized)) fail('repository');
  return normalized;
};

const normalizeApiBase = (value) => {
  let url;
  try {
    url = new URL(value || 'https://api.github.com');
  } catch {
    fail('api_base');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) fail('api_base');
  return url.toString().replace(/\/$/u, '');
};

const markerFingerprint = (body) => {
  if (typeof body !== 'string') return null;
  const match = body.match(/<!-- search-health-fingerprint:([^\s<>]+) -->/u);
  return match?.[1] || null;
};

const safeIssueNumber = (value) => {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1) fail('issue_number');
  return number;
};

const apiRequest = async ({ fetchImpl, token, method = 'GET', url, body }) => {
  let response;
  try {
    response = await fetchImpl(url, {
      method,
      headers: {
        accept: 'application/vnd.github+json',
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
        'user-agent': 'events-bot-search-health-reporter',
        'x-github-api-version': API_VERSION,
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  } catch {
    fail(`github_api_transport:${method}`);
  }
  if (!response || typeof response.ok !== 'boolean') fail('fetch_response');
  if (!response.ok) {
    let endpoint = 'github_api';
    try {
      endpoint = new URL(url).pathname;
    } catch {
      // Keep the error fixed and sanitized.
    }
    fail(`github_api:${method}:${endpoint}:status_${Number(response.status) || 0}`);
  }
  if (response.status === 204) return null;
  try {
    return await response.json();
  } catch {
    fail('github_api_json');
  }
};

const listHealthIssues = async ({ fetchImpl, token, repository, apiBase }) => {
  const issues = [];
  for (let page = 1; page <= MAX_ISSUE_PAGES; page += 1) {
    const query = new URLSearchParams({
      state: 'all',
      labels: HEALTH_LABEL,
      per_page: '100',
      page: String(page),
    });
    const batch = await apiRequest({
      fetchImpl,
      token,
      url: `${apiBase}/repos/${repository}/issues?${query}`,
    });
    if (!Array.isArray(batch)) fail('issues_response');
    issues.push(...batch.filter((issue) => issue && typeof issue === 'object' && !issue.pull_request));
    if (batch.length < 100) return issues;
  }
  fail('issue_page_limit');
};

const exactFingerprintMatches = (issues, fingerprint) => issues.filter(
  (issue) => markerFingerprint(issue.body) === fingerprint,
);

const openOrUpdate = async ({ plan, issues, context, dryRun }) => {
  const { operation } = plan;
  const matches = exactFingerprintMatches(issues, operation.fingerprint);
  if (matches.length > 1) fail('duplicate_fingerprint');
  const payload = {
    title: operation.title,
    body: operation.body,
    labels: operation.labels,
  };
  if (matches.length === 0) {
    let issueNumbers = [];
    if (!dryRun) {
      const created = await apiRequest({
        ...context,
        method: 'POST',
        url: `${context.apiBase}/repos/${context.repository}/issues`,
        body: payload,
      });
      issueNumbers = [safeIssueNumber(created?.number)];
    }
    return Object.freeze({ action: 'create', fingerprint: operation.fingerprint, issue_numbers: issueNumbers, dry_run: dryRun });
  }
  const number = safeIssueNumber(matches[0].number);
  if (!dryRun) {
    await apiRequest({
      ...context,
      method: 'PATCH',
      url: `${context.apiBase}/repos/${context.repository}/issues/${number}`,
      body: { ...payload, state: 'open' },
    });
  }
  return Object.freeze({ action: 'update', fingerprint: operation.fingerprint, issue_numbers: [number], dry_run: dryRun });
};

const closeMatchingProducts = async ({ plan, issues, context, dryRun }) => {
  const { operation } = plan;
  const matches = issues.filter((issue) => {
    const fingerprint = markerFingerprint(issue.body);
    return issue.state === 'open'
      && PRODUCT_FINGERPRINT_PATTERN.test(fingerprint || '')
      && fingerprint.startsWith(operation.fingerprint_prefix);
  });
  const numbers = matches.map((issue) => safeIssueNumber(issue.number)).sort((a, b) => a - b);
  if (!dryRun) {
    for (const number of numbers) {
      await apiRequest({
        ...context,
        method: 'POST',
        url: `${context.apiBase}/repos/${context.repository}/issues/${number}/comments`,
        body: { body: operation.close_comment },
      });
      await apiRequest({
        ...context,
        method: 'PATCH',
        url: `${context.apiBase}/repos/${context.repository}/issues/${number}`,
        body: { state: 'closed', state_reason: 'completed' },
      });
    }
  }
  return Object.freeze({
    action: 'close_matching',
    fingerprint_prefix: operation.fingerprint_prefix,
    issue_numbers: numbers,
    dry_run: dryRun,
  });
};

/** Applies an already-built strict report plan. No plan/body/token is logged. */
export async function applySearchHealthReportPlan(rawPlan, {
  fetchImpl = globalThis.fetch,
  token = process.env.GITHUB_TOKEN,
  repository = process.env.GITHUB_REPOSITORY,
  apiBaseUrl = process.env.GITHUB_API_URL || 'https://api.github.com',
  dryRun = false,
} = {}) {
  const plan = normalizeSearchHealthReportPlan(rawPlan);
  if (plan.operation.action === SEARCH_HEALTH_REPORT_ACTIONS.NONE) {
    return Object.freeze({ action: 'none', reason: plan.operation.reason, issue_numbers: [], dry_run: Boolean(dryRun) });
  }
  if (typeof fetchImpl !== 'function') fail('fetch');
  if (typeof token !== 'string' || token.length < 1) fail('token');
  const context = {
    fetchImpl,
    token,
    repository: normalizeRepository(repository),
    apiBase: normalizeApiBase(apiBaseUrl),
  };
  const issues = await listHealthIssues(context);
  if (plan.operation.action === SEARCH_HEALTH_REPORT_ACTIONS.OPEN_OR_UPDATE) {
    return openOrUpdate({ plan, issues, context, dryRun: Boolean(dryRun) });
  }
  if (plan.operation.action === SEARCH_HEALTH_REPORT_ACTIONS.CLOSE_MATCHING) {
    return closeMatchingProducts({ plan, issues, context, dryRun: Boolean(dryRun) });
  }
  fail('action');
}

const parseArgs = (argv) => {
  let input = null;
  let dryRun = false;
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === '--dry-run') {
      dryRun = true;
    } else if (argv[index] === '--input' && argv[index + 1]) {
      input = argv[index + 1];
      index += 1;
    } else {
      fail('arguments');
    }
  }
  return { input, dryRun };
};

const readStdin = async () => {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
};

export async function runApplySearchHealthReportPlanCli(argv = process.argv.slice(2)) {
  const { input, dryRun } = parseArgs(argv);
  const source = input ? await readFile(input, 'utf8') : await readStdin();
  const result = await applySearchHealthReportPlan(JSON.parse(source), { dryRun });
  return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const result = await runApplySearchHealthReportPlanCli();
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
