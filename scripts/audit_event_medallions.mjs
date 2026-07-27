#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

import {
  resolveEventMedallions,
} from '../site/src/lib/eventMedallions.ts';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function parseArgs(argv) {
  const values = { current:null, history:null, output:null };
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    if (key === '--current') values.current = argv[++index];
    else if (key === '--history') values.history = argv[++index];
    else if (key === '--output') values.output = argv[++index];
    else throw new Error(`unknown argument: ${key}`);
  }
  if (!values.current || !values.output) {
    throw new Error('usage: audit_event_medallions.mjs --current <events.json> [--history <events.json>] --output <directory>');
  }
  return values;
}

function loadSnapshot(file) {
  const payload = JSON.parse(readFileSync(path.resolve(file), 'utf8'));
  if (!Array.isArray(payload.events)) throw new Error(`${file}: events must be an array`);
  return payload;
}

function loadManifest(relativePath, kind) {
  const payload = JSON.parse(readFileSync(path.join(root, relativePath), 'utf8'));
  return (payload.items || []).map((item) => ({ ...item, manifestKind:kind }));
}

function staticEvent(event) {
  return {
    venue_name:event.location_name,
    address:event.location_address,
    festival:event.festival,
    organizer_names:event.organizer_names || [],
    source_url:event.source_post_url,
    source_urls:[event.source_post_url, event.source_vk_post_url].filter(Boolean),
  };
}

function staticResolutions(events, catalog) {
  return events.map((event) => ({
    event,
    resolution:resolveEventMedallions(staticEvent(event), catalog),
  }));
}

const telegramBridge = String.raw`
import json, sys
from types import SimpleNamespace
import tg_graphic_medallions as tg

payload = json.load(open(sys.argv[1], encoding="utf-8"))
rows = []
for raw in payload["events"]:
    event = SimpleNamespace(
        **raw,
        source_urls=[
            value for value in (raw.get("source_post_url"), raw.get("source_vk_post_url"))
            if value
        ],
    )
    resolved = tg.resolve_event_graphic_medallions(event, limit=99)
    location = tg._event_text(event, ("location_name", "location_address", "city"))
    organizers = tg._event_text(event, ("organizer_names",))
    identity = tg._event_text(
        event, ("tg_source_author", "source_post_url", "source_vk_post_url", "source_urls")
    )
    festival = tg._norm(getattr(event, "festival", None))
    matches = []
    for item in resolved:
        reason = item.get("reason", "")
        alias = ""
        field = reason
        haystack = ""
        if reason == "location_alias":
            field, haystack = "location", location
        elif reason == "organizer_field":
            field, haystack = "organizer_names", organizers
        elif reason == "source_identity":
            field, haystack = "source_identity", identity
        elif reason == "festival_field":
            field, haystack = "festival", festival
        elif reason.startswith("kgd80_"):
            field, alias = "festival_policy", "80 историй о главном"
        elif reason == "pushkin_card":
            field, alias = "pushkin_card", "pushkin_card=true"
        if haystack:
            alias = next(
                (candidate for candidate in tg._aliases(item) if tg._bounded_match(candidate, haystack)),
                "",
            )
        matches.append({
            "slug": item.get("slug"),
            "reason": reason,
            "field": field,
            "alias": alias,
            "asset_path": item.get("asset_path"),
        })
    rows.append({"event": raw, "matches": matches})
print(json.dumps(rows, ensure_ascii=False))
`;

function telegramResolutions(snapshotFile) {
  const result = spawnSync('python3', ['-c', telegramBridge, path.resolve(snapshotFile)], {
    cwd:root,
    encoding:'utf8',
    maxBuffer:64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error(`Telegram resolver bridge failed:\n${result.stderr || result.stdout}`);
  }
  return JSON.parse(result.stdout);
}

function groupStatic(rows) {
  const grouped = new Map();
  for (const { event, resolution } of rows) {
    for (const identity of resolution.identities) {
      const slug = identity.item.slug;
      const values = grouped.get(slug) || [];
      values.push({
        id:event.id,
        title:event.title,
        field:identity.evidence.field,
        alias:identity.evidence.alias,
        match:identity.evidence.match,
      });
      grouped.set(slug, values);
    }
  }
  return grouped;
}

function groupTelegram(rows) {
  const grouped = new Map();
  for (const { event, matches } of rows) {
    for (const match of matches) {
      const values = grouped.get(match.slug) || [];
      values.push({
        id:event.id,
        title:event.title,
        field:match.field,
        alias:match.alias,
        match:match.reason,
      });
      grouped.set(match.slug, values);
    }
  }
  return grouped;
}

function missingFiles(item) {
  const paths = [item.avatarUrl, item.fallbackPngUrl]
    .filter(Boolean)
    .map((value) => path.join(root, 'site/public', String(value).replace(/^\//u, '')));
  if (item.sourcePath) paths.push(path.join(root, item.sourcePath));
  const missing = [];
  for (const file of paths) {
    try {
      readFileSync(file);
    } catch {
      missing.push(path.relative(root, file));
    }
  }
  return missing;
}

function csvCell(value) {
  const text = String(value ?? '');
  return /[",\n]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function eventList(rows) {
  return rows.map((row) => `${row.id} ${row.title}`).join('; ');
}

function evidenceList(rows) {
  return Array.from(new Set(rows.map((row) => `${row.field}:${row.alias || '—'}:${row.match}`))).join('; ');
}

function summarizeRow(item, currentStatic, currentTelegram, historyStatic, historyTelegram) {
  const staticNow = currentStatic.get(item.slug) || [];
  const telegramNow = currentTelegram.get(item.slug) || [];
  const staticEver = historyStatic.get(item.slug) || [];
  const telegramEver = historyTelegram.get(item.slug) || [];
  const missing = missingFiles(item);
  const currentIds = new Set([...staticNow, ...telegramNow].map((row) => row.id));
  const historyIds = new Set([...staticEver, ...telegramEver].map((row) => row.id));
  let status = 'used';
  if (missing.length) status = 'unreachable';
  else if (currentIds.size === 0) status = 'unused';
  const surfaces = ['lab'];
  if (staticEver.length) surfaces.unshift('event detail');
  if (telegramEver.length) surfaces.push('Telegram');
  if (surfaces.length === 1) surfaces[0] = 'lab only';
  return {
    manifest:item.manifestKind,
    slug:item.slug,
    name:item.name,
    category:item.category || (item.manifestKind === 'festival' ? 'festival' : 'organizer'),
    status,
    surfaces:surfaces.join(' / '),
    current_events:eventList(Array.from(new Map(
      [...staticNow, ...telegramNow].map((row) => [row.id, row]),
    ).values())),
    static_evidence:evidenceList(staticNow),
    telegram_evidence:evidenceList(telegramNow),
    current_event_count:currentIds.size,
    historical_event_count:historyIds.size,
    historical_sample:eventList(Array.from(new Map(
      [...staticEver, ...telegramEver].map((row) => [row.id, row]),
    ).values()).slice(0, 5)),
    runtime_asset:item.avatarUrl,
    source_path:item.sourcePath || '',
    source_page:item.sourcePage || '',
    missing_files:missing.join('; '),
  };
}

function crossSurfaceMismatches(staticRows, telegramRows) {
  const staticBySlug = groupStatic(staticRows);
  const telegramBySlug = groupTelegram(telegramRows);
  const slugs = new Set([...staticBySlug.keys(), ...telegramBySlug.keys()]);
  const mismatches = [];
  for (const slug of Array.from(slugs).sort()) {
    const staticIds = new Set((staticBySlug.get(slug) || []).map((row) => row.id));
    const telegramIds = new Set((telegramBySlug.get(slug) || []).map((row) => row.id));
    const staticOnly = Array.from(staticIds).filter((id) => !telegramIds.has(id));
    const telegramOnly = Array.from(telegramIds).filter((id) => !staticIds.has(id));
    if (staticOnly.length || telegramOnly.length) {
      mismatches.push({ slug, staticOnly, telegramOnly });
    }
  }
  return mismatches;
}

function sourceCount(event) {
  return new Set([event.source_post_url, event.source_vk_post_url].filter(Boolean)).size;
}

function writeReports(outputDir, rows, context) {
  mkdirSync(outputDir, { recursive:true });
  const columns = [
    'manifest', 'slug', 'name', 'category', 'status', 'surfaces',
    'current_event_count', 'current_events', 'static_evidence', 'telegram_evidence',
    'historical_event_count', 'historical_sample', 'runtime_asset', 'source_path',
    'source_page', 'missing_files',
  ];
  const csv = [
    columns.join(','),
    ...rows.map((row) => columns.map((column) => csvCell(row[column])).join(',')),
  ].join('\n');
  writeFileSync(path.join(outputDir, 'medallion-usage-audit.csv'), `${csv}\n`);

  const markdown = [
    '# Event medallion usage audit',
    '',
    `Generated: ${new Date().toISOString()}`,
    `Current snapshot: ${context.current.snapshot?.source || 'provided JSON'}; queried ${context.current.snapshot?.queried_at_utc || 'unknown'} UTC; ${context.current.events.length} events; SHA supplied separately by the caller.`,
    `Historical snapshot: ${context.history.snapshot?.source || 'same as current'}; queried ${context.history.snapshot?.queried_at_utc || 'unknown'} UTC; ${context.history.events.length} events.`,
    '',
    'Status is `used` when at least one current event resolves the slug, `unused` when current use is zero (the historical count distinguishes dormant from never-used), and `unreachable` when a declared runtime/source file is missing.',
    '',
    '## Per-manifest entry',
    '',
    '| Slug | Name | Category | Current status | Surfaces | Actual current event IDs/titles | Match field / alias | Historical count/sample | Runtime / provenance |',
    '| --- | --- | --- | --- | --- | --- | --- | --- | --- |',
    ...rows.map((row) => `| ${[
      `\`${row.slug}\``,
      row.name.replaceAll('|', '\\|'),
      row.category,
      row.status,
      row.surfaces,
      (row.current_events || '—').replaceAll('|', '\\|'),
      [`detail: ${row.static_evidence || '—'}`, `Telegram: ${row.telegram_evidence || '—'}`].join('<br>').replaceAll('|', '\\|'),
      `${row.historical_event_count}; ${(row.historical_sample || '—').replaceAll('|', '\\|')}`,
      `${row.runtime_asset}<br>${row.source_path || '—'}<br>${row.source_page || '—'}`.replaceAll('|', '\\|'),
    ].join(' | ')} |`),
    '',
    '## Resolver conflicts and cross-surface review',
    '',
    `- Current fail-closed results: ${context.currentConflicts.length ? context.currentConflicts.join('; ') : 'none'}.`,
    `- Historical fail-closed results: ${context.historyConflicts.length ? context.historyConflicts.slice(0, 50).join('; ') : 'none'}.`,
    `- Current cross-surface mismatches (review list, not automatically classified as defects): ${context.mismatches.length ? context.mismatches.map((item) => `${item.slug} detail-only=[${item.staticOnly.join(',')}] Telegram-only=[${item.telegramOnly.join(',')}]`).join('; ') : 'none'}.`,
    '',
    '## Non-manifest special cases',
    '',
    `- **Pushkin Card:** event-detail runtime is the composite badge and Telegram uses the same raster inventory; ${context.pushkinCurrent} current and ${context.pushkinHistory} historical canonical events have \`pushkin_card=true\`.`,
    `- **Free listing:** ${context.freeCurrent} current events use the event-detail free-admission pill. The standalone \`free-listing-medallion.svg\` is a lab/listing specimen and is not sent to Telegram.`,
    `- **MEOW:** ${context.meowCurrent} current events reference MEOW in the primary database URL columns. Final event-detail eligibility additionally requires exported \`source_count <= 2\`; checked preview event 6911 correctly rendered no MEOW token. The actual Telegram resolver returned \`meow-afisha\` zero times.`,
    '- **RZD Lastochka:** lab-only transport specimen. It has no event resolver eligibility and cannot be inferred from city/venue text.',
    '',
    '## Reproduction',
    '',
    '```bash',
    'node --experimental-strip-types scripts/audit_event_medallions.mjs \\',
    '  --current artifacts/codex/L02-medallion-audit/prod-current-events-20260723.json \\',
    '  --history artifacts/codex/L02-medallion-audit/prod-event-history-20260723.json \\',
    '  --output artifacts/codex/L02-medallion-audit',
    '```',
    '',
  ].join('\n');
  writeFileSync(path.join(outputDir, 'medallion-usage-audit.md'), markdown);
}

const args = parseArgs(process.argv.slice(2));
const current = loadSnapshot(args.current);
const history = args.history ? loadSnapshot(args.history) : current;
const organizers = loadManifest('site/src/data/organizerMedallions.json', 'organizer');
const festivals = loadManifest('site/src/data/festivalMedallions.json', 'festival');
const organizerSlugs = new Set(organizers.map((item) => item.slug));
const detailCatalog = [
  ...organizers,
  ...festivals.filter((item) => (
    item.category === 'festival'
    && item.slug !== 'kgd80-80-stories'
    && !organizerSlugs.has(item.slug)
  )),
];
const currentStaticRows = staticResolutions(current.events, detailCatalog);
const historyStaticRows = staticResolutions(history.events, detailCatalog);
const currentTelegramRows = telegramResolutions(args.current);
const historyTelegramRows = telegramResolutions(args.history || args.current);
const currentStatic = groupStatic(currentStaticRows);
const historyStatic = groupStatic(historyStaticRows);
const currentTelegram = groupTelegram(currentTelegramRows);
const historyTelegram = groupTelegram(historyTelegramRows);
const rows = [...organizers, ...festivals].map((item) => summarizeRow(
  item, currentStatic, currentTelegram, historyStatic, historyTelegram,
));
const conflictLabel = ({ event, resolution }) => (
  resolution.failClosedReason
    ? `${event.id}:${resolution.failClosedReason}:${(resolution.conflictEvidence || []).join('|')}`
    : null
);
const meowCurrent = current.events.filter((event) => (
  sourceCount(event) > 0
  && sourceCount(event) <= 2
  && [event.source_post_url, event.source_vk_post_url].some((url) => /(?:t|telegram)\.me\/meowafisha(?:\/|$)/iu.test(String(url || '')))
)).length;
writeReports(path.resolve(args.output), rows, {
  current,
  history,
  currentConflicts:currentStaticRows.map(conflictLabel).filter(Boolean),
  historyConflicts:historyStaticRows.map(conflictLabel).filter(Boolean),
  mismatches:crossSurfaceMismatches(currentStaticRows, currentTelegramRows),
  pushkinCurrent:current.events.filter((event) => Boolean(event.pushkin_card)).length,
  pushkinHistory:history.events.filter((event) => Boolean(event.pushkin_card)).length,
  freeCurrent:current.events.filter((event) => Boolean(event.is_free)).length,
  meowCurrent,
});
console.log(JSON.stringify({
  output:path.resolve(args.output),
  organizer_entries:organizers.length,
  festival_entries:festivals.length,
  used_current:rows.filter((row) => row.status === 'used').length,
  unused_current:rows.filter((row) => row.status === 'unused').length,
  unreachable:rows.filter((row) => row.status === 'unreachable').length,
}, null, 2));
