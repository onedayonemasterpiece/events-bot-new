import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import process from 'node:process';

const DEFAULT_LISTINGS = Object.freeze([
  'zavtra/',
  'segodnya/',
  'vyhodnye/',
  'populyarnoe/',
  '',
]);

function positiveInteger(value, fallback, { minimum = 1, maximum = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isInteger(parsed)) return fallback;
  if (parsed < minimum || parsed > maximum) throw new Error('resolver_config_integer_out_of_range');
  return parsed;
}

function cleanUrl(value) {
  const url = new URL(value);
  url.username = '';
  url.password = '';
  url.search = '';
  url.hash = '';
  return url;
}

export function deriveDeploymentBase(target) {
  const url = target instanceof URL ? new URL(target.href) : new URL(target);
  const segments = url.pathname.split('/').filter(Boolean);
  let basePath = '/';
  if (segments[0]?.startsWith('preview-')) {
    basePath = `/${segments[0]}/`;
  } else if (segments[0] === '_review' && segments[1]) {
    basePath = `/_review/${segments[1]}/`;
  }
  return { origin: url.origin, basePath, baseUrl: new URL(basePath, url.origin) };
}

function decodeHtmlAttribute(value) {
  return String(value)
    .replaceAll('&amp;', '&')
    .replaceAll('&quot;', '"')
    .replaceAll('&#39;', "'")
    .replaceAll('&#x2F;', '/');
}

export function extractEventLinks(html, listingUrl, basePath) {
  const links = [];
  const expectedPrefix = `${basePath}sobytiya/`.replaceAll('//', '/');
  for (const match of String(html).matchAll(/<a\b[^>]*\bhref\s*=\s*["']([^"']+)["']/giu)) {
    try {
      const url = cleanUrl(new URL(decodeHtmlAttribute(match[1]), listingUrl));
      if (url.origin !== listingUrl.origin) continue;
      if (!url.pathname.startsWith(expectedPrefix)) continue;
      if (!/\/sobytiya\/[^/]+\/$/u.test(url.pathname)) continue;
      links.push(url.href);
    } catch {
      // Invalid editorial hrefs are ignored; route-health tests report them separately.
    }
  }
  return [...new Set(links)].sort();
}

export function unfoldIcs(text) {
  const normalized = String(text).replaceAll('\r\n', '\n').replaceAll('\r', '\n');
  const output = [];
  for (const line of normalized.split('\n')) {
    if (/^[ \t]/u.test(line) && output.length) output[output.length - 1] += line.slice(1);
    else output.push(line);
  }
  return output;
}

function unescapeIcsText(value) {
  return String(value)
    .replaceAll('\\n', '\n')
    .replaceAll('\\N', '\n')
    .replaceAll('\\,', ',')
    .replaceAll('\\;', ';')
    .replaceAll('\\\\', '\\');
}

function parseOffsetDate(value) {
  const match = String(value).match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})([+-])(\d{2})(\d{2})$/u);
  if (!match) return null;
  const [, year, month, day, hour, minute, second, sign, offsetHour, offsetMinute] = match;
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}${sign}${offsetHour}:${offsetMinute}`);
}

export function parseIcsDate(value, parameters = {}) {
  const raw = String(value || '').trim();
  if (/^\d{8}$/u.test(raw)) {
    const [year, month, day] = [raw.slice(0, 4), raw.slice(4, 6), raw.slice(6, 8)];
    return { date: new Date(`${year}-${month}-${day}T00:00:00Z`), allDay: true, source: 'date' };
  }
  if (/^\d{8}T\d{6}Z$/u.test(raw)) {
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}Z`;
    return { date: new Date(iso), allDay: false, source: 'utc' };
  }
  const offsetDate = parseOffsetDate(raw);
  if (offsetDate) return { date: offsetDate, allDay: false, source: 'offset' };
  if (/^\d{8}T\d{6}$/u.test(raw) && parameters.TZID === 'Europe/Kaliningrad') {
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}+02:00`;
    return { date: new Date(iso), allDay: false, source: 'Europe/Kaliningrad' };
  }
  return null;
}

function propertyFromLine(line) {
  const colon = line.indexOf(':');
  if (colon <= 0) return null;
  const descriptor = line.slice(0, colon);
  const value = line.slice(colon + 1);
  const [rawName, ...rawParameters] = descriptor.split(';');
  const parameters = {};
  for (const item of rawParameters) {
    const equal = item.indexOf('=');
    if (equal > 0) parameters[item.slice(0, equal).toUpperCase()] = item.slice(equal + 1);
  }
  return { name: rawName.toUpperCase(), parameters, value };
}

export function parseFirstVevent(text) {
  const lines = unfoldIcs(text);
  const startIndex = lines.findIndex((line) => line.trim().toUpperCase() === 'BEGIN:VEVENT');
  const endIndex = lines.findIndex((line, index) => index > startIndex && line.trim().toUpperCase() === 'END:VEVENT');
  if (startIndex < 0 || endIndex < 0) throw new Error('event_ics_vevent_missing');
  const properties = new Map();
  for (const line of lines.slice(startIndex + 1, endIndex)) {
    const property = propertyFromLine(line);
    if (property && !properties.has(property.name)) properties.set(property.name, property);
  }
  const start = properties.get('DTSTART');
  const end = properties.get('DTEND');
  const parsedStart = start ? parseIcsDate(start.value, start.parameters) : null;
  const parsedEnd = end ? parseIcsDate(end.value, end.parameters) : null;
  return {
    uid: unescapeIcsText(properties.get('UID')?.value || '').trim(),
    summary: unescapeIcsText(properties.get('SUMMARY')?.value || '').trim(),
    location: unescapeIcsText(properties.get('LOCATION')?.value || '').trim(),
    description: unescapeIcsText(properties.get('DESCRIPTION')?.value || '').trim(),
    canonicalUrl: unescapeIcsText(properties.get('URL')?.value || '').trim(),
    status: String(properties.get('STATUS')?.value || '').trim().toUpperCase(),
    startsAt: parsedStart?.date || null,
    endsAt: parsedEnd?.date || null,
    allDay: Boolean(parsedStart?.allDay),
    datetimeSource: parsedStart?.source || null,
  };
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

async function fetchWithTimeout(url, { timeoutMs = 15_000, accept = '*/*' } = {}) {
  const response = await fetch(url, {
    redirect: 'follow',
    headers: { Accept: accept, 'User-Agent': 'KenigEvents-Calendar-E2E/1.0' },
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) throw new Error(`http_${response.status}:${url.pathname}`);
  return response;
}

async function releaseIdentity(baseUrl, expectedRepoSha) {
  const names = ['preview-build.json', 'production-build.json', 'secret-candidate-build.json'];
  const failures = [];
  for (const name of names) {
    const url = new URL(name, baseUrl);
    try {
      const response = await fetchWithTimeout(url, { accept: 'application/json' });
      const body = await response.json();
      const repoSha = String(body?.repo_sha || '').trim().toLowerCase();
      if (!/^[0-9a-f]{40}$/u.test(repoSha)) throw new Error('repo_sha_missing');
      if (expectedRepoSha && repoSha !== expectedRepoSha) throw new Error('repo_sha_mismatch');
      return { metadataUrl: url.href, repoSha, buildId: String(body?.build_id || body?.preview_id || '').trim() || null };
    } catch (error) {
      failures.push(`${name}:${String(error?.message || error)}`);
    }
  }
  throw new Error(`target_metadata_unavailable:${failures.join('|')}`);
}

export function selectCurrentCandidate(candidates, now, { minLeadMinutes, maxLeadDays }) {
  const minimum = now.getTime() + minLeadMinutes * 60_000;
  const maximum = now.getTime() + maxLeadDays * 86_400_000;
  const eligible = candidates.filter((candidate) => {
    const start = candidate.startsAt?.getTime();
    const end = candidate.endsAt?.getTime();
    return Boolean(
      candidate.uid
      && candidate.summary
      && candidate.location
      && Number.isFinite(start)
      && Number.isFinite(end)
      && end > start
      && start >= minimum
      && start <= maximum
      && !candidate.allDay
      && candidate.status !== 'CANCELLED'
    );
  });
  eligible.sort((left, right) => left.startsAt - right.startsAt || left.eventUrl.localeCompare(right.eventUrl));
  return eligible[0] || null;
}

export async function resolveCurrentEvent({
  targetUrl,
  expectedRepoSha = '',
  minLeadMinutes = 90,
  maxLeadDays = 30,
  now = new Date(),
  listings = DEFAULT_LISTINGS,
} = {}) {
  if (!targetUrl) throw new Error('target_url_required');
  const target = cleanUrl(targetUrl);
  if (target.protocol !== 'https:') throw new Error('target_https_required');
  const { basePath, baseUrl } = deriveDeploymentBase(target);
  const identity = await releaseIdentity(baseUrl, String(expectedRepoSha).trim().toLowerCase());
  const candidates = [];
  const listingFailures = [];
  for (const listingPath of listings) {
    const listingUrl = new URL(listingPath, baseUrl);
    try {
      const response = await fetchWithTimeout(listingUrl, { accept: 'text/html' });
      const html = await response.text();
      for (const eventHref of extractEventLinks(html, listingUrl, basePath)) {
        const eventUrl = cleanUrl(eventHref);
        const icsUrl = new URL('event.ics', eventUrl);
        try {
          const [eventResponse, icsResponse] = await Promise.all([
            fetchWithTimeout(eventUrl, { accept: 'text/html' }),
            fetchWithTimeout(icsUrl, { accept: 'text/calendar,text/plain;q=0.9,*/*;q=0.1' }),
          ]);
          await eventResponse.text();
          const icsBytes = Buffer.from(await icsResponse.arrayBuffer());
          const parsed = parseFirstVevent(icsBytes.toString('utf8'));
          candidates.push({
            ...parsed,
            eventUrl: eventUrl.href,
            icsUrl: icsUrl.href,
            icsSha256: sha256(icsBytes),
            sourceListingUrl: listingUrl.href,
          });
        } catch (error) {
          listingFailures.push(`event:${eventUrl.pathname}:${String(error?.message || error)}`);
        }
      }
    } catch (error) {
      listingFailures.push(`listing:${listingUrl.pathname}:${String(error?.message || error)}`);
    }
  }
  const unique = [...new Map(candidates.map((candidate) => [candidate.eventUrl, candidate])).values()];
  const selected = selectCurrentCandidate(unique, now, { minLeadMinutes, maxLeadDays });
  if (!selected) {
    throw new Error(`no_current_complete_event:candidates=${unique.length}:failures=${listingFailures.slice(0, 8).join('|')}`);
  }
  const startMs = selected.startsAt.getTime();
  const revalidateMs = Math.min(now.getTime() + 10 * 60_000, startMs - 60 * 60_000);
  return {
    schema_version: 1,
    selected_at: now.toISOString(),
    selection_reason: 'earliest_complete_timed_current_event',
    source_listing_url: selected.sourceListingUrl,
    event_url: selected.eventUrl,
    ics_url: selected.icsUrl,
    ics_sha256: selected.icsSha256,
    uid: selected.uid,
    summary: selected.summary,
    location: selected.location,
    starts_at: selected.startsAt.toISOString(),
    ends_at: selected.endsAt.toISOString(),
    status: selected.status || null,
    datetime_source: selected.datetimeSource,
    expected_repo_sha: String(expectedRepoSha).trim().toLowerCase() || null,
    observed_repo_sha: identity.repoSha,
    preview_build_id: identity.buildId,
    release_metadata_url: identity.metadataUrl,
    deployment_base_path: basePath,
    revalidate_before: new Date(Math.max(now.getTime(), revalidateMs)).toISOString(),
    considered_event_count: unique.length,
    nonblocking_probe_failure_count: listingFailures.length,
  };
}

async function main() {
  const targetUrl = String(process.env.E2E_TARGET_URL || process.argv[2] || '').trim();
  const expectedRepoSha = String(process.env.E2E_EXPECTED_REPO_SHA || process.argv[3] || '').trim().toLowerCase();
  const outputPath = resolve(String(process.env.E2E_SELECTED_EVENT_PATH || process.argv[4] || 'artifacts/event-reminders/selected-event.json'));
  const result = await resolveCurrentEvent({
    targetUrl,
    expectedRepoSha,
    minLeadMinutes: positiveInteger(process.env.E2E_MIN_LEAD_MINUTES, 90, { minimum: 1, maximum: 10_080 }),
    maxLeadDays: positiveInteger(process.env.E2E_MAX_LEAD_DAYS, 30, { minimum: 1, maximum: 366 }),
  });
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  process.stdout.write(`${JSON.stringify({ ok: true, output_path: outputPath, selected: result }, null, 2)}\n`);
}

const invokedPath = process.argv[1] ? pathToFileURL(resolve(process.argv[1])).href : '';
if (invokedPath && import.meta.url === invokedPath) {
  main().catch((error) => {
    process.stderr.write(`current_event_resolver_failed:${String(error?.message || error)}\n`);
    process.exitCode = 1;
  });
}
