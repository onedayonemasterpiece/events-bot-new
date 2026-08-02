import { appendFile, readFile } from 'node:fs/promises';

import { selectedPlatforms } from '../e2e/focus-email/helpers/platform.mjs';
import { validateFocusE2eTarget } from '../e2e/focus-email/helpers/target-url.mjs';

const COMMAND = /^\/qa run scenario=(focus\.otp\.browser_tab) platform=(browser|android|ios|all) target_url=(\S+) expected_repo_sha=([0-9a-f]{40}) mode=(blocking)$/u;

export function parseQaRunCommand(body) {
  const raw = String(body || '').trim();
  if (raw.length > 500 || raw.includes('\n')) throw new Error('command_format_invalid');
  const match = raw.match(COMMAND);
  if (!match) throw new Error('command_format_invalid');
  const [, scenario, platform, targetUrl, expectedRepoSha, mode] = match;
  const target = validateFocusE2eTarget(targetUrl);
  selectedPlatforms(platform);
  return { scenario, platform, target_url: target.href, expected_repo_sha: expectedRepoSha, mode,
    target_origin: target.origin, target_path: target.pathname };
}

export async function validateIssueEvent(event, { controlIssueNumber }) {
  if (Number(event?.issue?.number) !== Number(controlIssueNumber)) throw new Error('control_issue_mismatch');
  if (!(event?.issue?.labels || []).some((label) => label?.name === 'static-site-qa-control')) throw new Error('control_issue_label_missing');
  return parseQaRunCommand(event?.comment?.body);
}

async function main() {
  const event = JSON.parse(await readFile(process.env.GITHUB_EVENT_PATH, 'utf8'));
  const value = await validateIssueEvent(event, { controlIssueNumber: process.env.STATIC_SITE_QA_CONTROL_ISSUE_NUMBER });
  if (process.env.GITHUB_OUTPUT) {
    for (const [name, item] of Object.entries(value)) await appendFile(process.env.GITHUB_OUTPUT, `${name}=${item}\n`);
  }
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) main().catch((error) => { process.stderr.write(`${error.message}\n`); process.exitCode = 1; });
