import { readFile, readdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const SAFE = /^[A-Za-z0-9_.:/ -]{0,160}$/u;
const safe = (value, fallback = 'unreported') => {
  const text = String(value ?? fallback);
  return SAFE.test(text) ? text : fallback;
};
const keyboard = (value) => value == null ? 'n/a' : value.passed === true ? 'pass' : 'fail';
const count = (value) => value == null ? 'unknown' : String(Number(value));

export function formatTerminalReceipt({ scenario, platform, summaries, runUrl }) {
  const prefix = `TERMINAL · ${safe(scenario)} · ${safe(platform)} ·`;
  if (!summaries.length) return `${prefix} status=FAIL · domain=FAIL_CONTROL_PLANE_NO_QA_SUMMARY · counts=unknown/unknown/unknown · keyboards=n/a · warnings=none · redaction=unverified · artifacts=${safe(runUrl)}`;
  const expectedPlatforms = platform === 'all' ? ['browser', 'android', 'ios'] : [platform];
  const presentPlatforms = new Set(summaries.map((item) => String(item.platform)));
  const missingPlatforms = expectedPlatforms.filter((item) => !presentPlatforms.has(item));
  const overall = missingPlatforms.length || summaries.some((item) => item.status === 'FAIL') ? 'FAIL'
    : summaries.some((item) => item.status === 'BLOCKED') ? 'BLOCKED' : 'PASS';
  const lines = summaries.sort((a, b) => String(a.platform).localeCompare(String(b.platform))).map((summary) => {
    const counts = summary.counts || {};
    const keys = summary.keyboard_acceptance || {};
    const warningCodes = (summary.warnings || []).map((item) => safe(item?.code)).filter(Boolean);
    const sha = summary.provenance || {};
    return `${safe(summary.platform)}:${safe(summary.status)}/${safe(summary.failure_domain, 'none')}`
      + ` failed_step=${safe(summary.first_failed_step, 'none')}`
      + ` counts=${count(counts.issue)}/${count(counts.verify)}/${count(counts.registration)}`
      + ` registration_status=${count(summary.registration_status)} mail_count=${count(summary.matching_mail_count)}`
      + ` final=${safe(summary.final_ui_state, 'none')} reload=${safe(summary.reload_state, 'none')}`
      + ` keyboards=email:${keyboard(keys.email)},otp:${keyboard(keys.otp)},control-email:${keyboard(keys.control_email)},control-numeric:${keyboard(keys.control_numeric)}`
      + ` warnings=${warningCodes.join(',') || 'none'} redaction=${safe(summary.redaction_status)}`
      + ` artifact=${safe(sha.artifact_name)}`
      + ` sha=harness:${safe(sha.harness_repo_sha)},tested:${safe(sha.tested_repo_sha)},observed:${safe(sha.observed_preview_sha)}`;
  });
  const missing = missingPlatforms.length
    ? ` status=FAIL · domain=FAIL_CONTROL_PLANE_MISSING_SUMMARY · missing=${missingPlatforms.join(',')} ·`
    : '';
  return `${prefix} overall=${overall} ·${missing} ${lines.join(' · ')} · artifacts=${safe(runUrl)}`;
}

async function findSummaries(root) {
  const found = [];
  async function walk(dir) {
    for (const entry of await readdir(dir, { withFileTypes: true }).catch(() => [])) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (entry.name === 'qa-summary.json') found.push(JSON.parse(await readFile(path, 'utf8')));
    }
  }
  await walk(root);
  return found;
}

async function main() {
  const summaries = await findSummaries(process.env.STATIC_SITE_QA_ARTIFACT_ROOT);
  const body = formatTerminalReceipt({ scenario: process.env.STATIC_SITE_QA_SCENARIO,
    platform: process.env.STATIC_SITE_QA_PLATFORM, summaries, runUrl: process.env.STATIC_SITE_QA_RUN_URL });
  await writeFile(process.env.STATIC_SITE_QA_TERMINAL_BODY, `${body}\n`, 'utf8');
}

if (import.meta.url === `file://${process.argv[1]}`) main().catch((error) => { console.error(error.message); process.exitCode = 1; });
