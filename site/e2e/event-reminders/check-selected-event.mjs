import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';

import { chromium } from 'playwright';

import { parseFirstVevent } from './resolve-current-event.mjs';

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function xmlEscape(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

async function fetchBytes(url, accept) {
  const response = await fetch(url, {
    headers: { Accept: accept, 'User-Agent': 'KenigEvents-Calendar-E2E/1.0' },
    signal: AbortSignal.timeout(15_000),
  });
  if (!response.ok) throw new Error(`http_${response.status}:${new URL(url).pathname}`);
  return { response, bytes: Buffer.from(await response.arrayBuffer()) };
}

async function main() {
  const startedAt = Date.now();
  const selectedPath = resolve(String(process.env.E2E_SELECTED_EVENT_PATH || process.argv[2] || 'artifacts/event-reminders/selected-event.json'));
  const evidenceRoot = resolve(String(process.env.E2E_EVIDENCE_DIR || process.argv[3] || 'artifacts/event-reminders/browser'));
  await mkdir(join(evidenceRoot, 'screenshots'), { recursive: true });
  const selected = JSON.parse(await readFile(selectedPath, 'utf8'));
  const failures = [];
  let browser = null;
  let page = null;
  const consoleEvents = [];
  const networkEvents = [];
  let observed = null;
  try {
    const now = new Date();
    if (!selected.event_url || !selected.ics_url) throw new Error('selected_event_urls_missing');
    if (selected.expected_repo_sha && selected.observed_repo_sha !== selected.expected_repo_sha) {
      throw new Error('repo_sha_mismatch');
    }
    const { response: icsResponse, bytes: icsBytes } = await fetchBytes(
      selected.ics_url,
      'text/calendar,text/plain;q=0.9,*/*;q=0.1',
    );
    const parsed = parseFirstVevent(icsBytes.toString('utf8'));
    observed = {
      event_url: selected.event_url,
      ics_url: selected.ics_url,
      ics_content_type: icsResponse.headers.get('content-type'),
      ics_sha256: sha256(icsBytes),
      uid: parsed.uid,
      summary: parsed.summary,
      location: parsed.location,
      starts_at: parsed.startsAt?.toISOString() || null,
      ends_at: parsed.endsAt?.toISOString() || null,
      status: parsed.status || null,
    };
    for (const [name, actual, expected] of [
      ['ics_sha256', observed.ics_sha256, selected.ics_sha256],
      ['uid', observed.uid, selected.uid],
      ['summary', observed.summary, selected.summary],
      ['location', observed.location, selected.location],
      ['starts_at', observed.starts_at, selected.starts_at],
      ['ends_at', observed.ends_at, selected.ends_at],
    ]) {
      if (actual !== expected) failures.push(`event_changed:${name}`);
    }
    if (parsed.status === 'CANCELLED') failures.push('event_cancelled');
    if (!parsed.startsAt || parsed.startsAt <= now) failures.push('event_started_or_invalid');

    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ viewport: { width: 390, height: 844 }, locale: 'ru-RU' });
    page = await context.newPage();
    page.on('console', (message) => {
      if (message.type() === 'error') consoleEvents.push({ type: message.type(), text: message.text().slice(0, 300) });
    });
    page.on('pageerror', (error) => consoleEvents.push({ type: 'pageerror', text: String(error.message || error).slice(0, 300) }));
    page.on('response', (response) => {
      if (response.status() >= 400) {
        const url = new URL(response.url());
        networkEvents.push({ status: response.status(), origin: url.origin, path: url.pathname });
      }
    });
    const response = await page.goto(selected.event_url, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    if (!response?.ok()) failures.push(`event_page_http:${response?.status() ?? 'none'}`);
    const route = await page.evaluate(() => {
      const main = document.querySelector('main');
      const text = String(main?.textContent || '').replace(/\s+/gu, ' ').trim();
      return {
        href: location.href,
        title: document.title,
        main_present: Boolean(main),
        main_text_length: text.length,
        horizontal_overflow: document.documentElement.scrollWidth > document.documentElement.clientWidth + 2,
      };
    });
    if (!route.main_present || route.main_text_length < 40) failures.push('event_page_empty');
    if (route.horizontal_overflow) failures.push('event_page_horizontal_overflow');
    if (new URL(route.href).origin !== new URL(selected.event_url).origin) failures.push('event_page_origin_changed');
    if (consoleEvents.length) failures.push('event_page_console_error');
    await page.screenshot({ path: join(evidenceRoot, 'screenshots', 'selected-event.png'), fullPage: false });
    await context.close();
    observed.route = route;
  } catch (error) {
    failures.push(String(error?.message || error));
  } finally {
    await page?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }

  const status = failures.length ? 'FAIL' : 'PASS';
  const result = {
    schema_version: 1,
    scenario_id: 'event.current_event.selection',
    platform: 'browser',
    status,
    failure_domain: failures[0] || null,
    expected_repo_sha: selected.expected_repo_sha || null,
    observed_repo_sha: selected.observed_repo_sha || null,
    selected_event: {
      event_url: selected.event_url,
      ics_url: selected.ics_url,
      starts_at: selected.starts_at,
      ends_at: selected.ends_at,
      uid_sha256: sha256(String(selected.uid || '')),
      selection_reason: selected.selection_reason,
    },
    observed,
    console_error_count: consoleEvents.length,
    failed_response_count: networkEvents.length,
    failures,
    elapsed_ms: Date.now() - startedAt,
  };
  const summary = {
    schema_version: 1,
    scenario_id: result.scenario_id,
    status,
    failure_domain: result.failure_domain,
    expected_repo_sha: result.expected_repo_sha,
    observed_repo_sha: result.observed_repo_sha,
    event_url: result.selected_event.event_url,
    event_start: result.selected_event.starts_at,
    evidence: {
      result: 'result.json',
      selected_event: 'selected-event.json',
      screenshots: 'screenshots/',
      network: 'network.sanitized.jsonl',
      console: 'console.sanitized.jsonl',
      junit: 'junit.xml',
    },
  };
  await Promise.all([
    writeFile(join(evidenceRoot, 'selected-event.json'), `${JSON.stringify(selected, null, 2)}\n`),
    writeFile(join(evidenceRoot, 'result.json'), `${JSON.stringify(result, null, 2)}\n`),
    writeFile(join(evidenceRoot, 'qa-summary.json'), `${JSON.stringify(summary, null, 2)}\n`),
    writeFile(join(evidenceRoot, 'console.sanitized.jsonl'), `${consoleEvents.map((item) => JSON.stringify(item)).join('\n')}${consoleEvents.length ? '\n' : ''}`),
    writeFile(join(evidenceRoot, 'network.sanitized.jsonl'), `${networkEvents.map((item) => JSON.stringify(item)).join('\n')}${networkEvents.length ? '\n' : ''}`),
    writeFile(join(evidenceRoot, 'scenarios.jsonl'), `${JSON.stringify({ scenario_id: result.scenario_id, status, failure_domain: result.failure_domain })}\n`),
    writeFile(
      join(evidenceRoot, 'junit.xml'),
      `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="event-reminders-calendar" tests="1" failures="${failures.length ? 1 : 0}" time="${((Date.now() - startedAt) / 1000).toFixed(3)}"><testcase classname="browser" name="event.current_event.selection">${failures.length ? `<failure type="${xmlEscape(result.failure_domain)}" message="${xmlEscape(failures.join(';'))}"/>` : ''}</testcase></testsuite>\n`,
    ),
  ]);
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  if (failures.length) process.exitCode = 1;
}

main().catch((error) => {
  process.stderr.write(`selected_event_check_failed:${String(error?.message || error)}\n`);
  process.exitCode = 1;
});
