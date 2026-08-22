#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { constants, cpSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, parse, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { renderSpecimenPage } from '../current_ui_resource_graph/v1/specimens/materialize.mjs';
import { assertImmutableCheckout } from './immutable-checkout.mjs';
import { resolveEventCardSemantics } from './event-card-semantics.mjs';

const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const readJson = (path) => JSON.parse(readFileSync(resolve(path), 'utf8'));

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith('--') || !value || value.startsWith('--')) throw new Error(`Invalid argument near ${key || '<end>'}`);
    out[key.slice(2)] = value;
  }
  return out;
}

function copyTree(source, target) {
  const result = spawnSync('cp', ['-a', '--reflink=auto', source, target], { encoding: 'utf8' });
  if (result.status !== 0) cpSync(source, target, { recursive: true, mode: constants.COPYFILE_FICLONE });
}

function decodeHtml(value = '') {
  return value
    .replace(/&#(\d+);/gu, (_, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/giu, (_, code) => String.fromCodePoint(Number.parseInt(code, 16)))
    .replaceAll('&quot;', '"').replaceAll('&#39;', "'").replaceAll('&apos;', "'")
    .replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&amp;', '&');
}

function plainText(value = '') {
  return decodeHtml(value.replace(/<[^>]*>/gu, '')).replace(/\s+/gu, ' ').trim();
}

function attribute(fragment, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const match = new RegExp(`(?:^|\\s)${escaped}(?:=(?:"([^"]*)"|'([^']*)'|([^\\s>]+)))?`, 'u').exec(fragment);
  return match ? decodeHtml(match[1] ?? match[2] ?? match[3] ?? '') : null;
}

function startTagWith(html, marker) {
  const escaped = marker.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  return new RegExp(`<[^>]+(?:^|\\s)${escaped}(?=[\\s=>])[^>]*>`, 'u').exec(html)?.[0] ?? null;
}

function elementText(html, marker) {
  const escaped = marker.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const match = new RegExp(`<([a-z][a-z0-9-]*)[^>]*(?:^|\\s)${escaped}(?=[\\s=>])[^>]*>([\\s\\S]*?)<\\/\\1>`, 'iu').exec(html);
  return match ? plainText(match[2]) : null;
}

function actionState(html, marker, countMarker = null) {
  const tag = startTagWith(html, marker);
  const countTag = countMarker ? startTagWith(html, countMarker) : null;
  return {
    present: Boolean(tag),
    aria_label: tag ? attribute(tag, 'aria-label') : null,
    base_count: tag && attribute(tag, marker === 'data-native-share' ? 'data-share-base-count' : 'data-base-count') !== null
      ? Number(attribute(tag, marker === 'data-native-share' ? 'data-share-base-count' : 'data-base-count'))
      : null,
    count_label: countTag ? elementText(html, countMarker) : null,
  };
}

export function extractGeneratedChipState(html, fixture) {
  const cardMatch = /<article[^>]*\sdata-event-card(?=[\s=>])[^>]*>[\s\S]*?<\/article>/u.exec(html);
  const cardHtml = cardMatch?.[0] ?? '';
  const article = startTagWith(cardHtml, 'data-event-card');
  if (!article) throw new Error(`Generated EventCard is absent for ${fixture.fixture_id}`);
  const occurrence = startTagWith(cardHtml, 'data-occurrence-label');
  const calendar = startTagWith(cardHtml, 'data-calendar-action');
  const negative = startTagWith(cardHtml, 'data-feedback-action="not_interested"');
  const share = actionState(cardHtml, 'data-native-share', 'data-share-count');
  const like = actionState(cardHtml, 'data-feedback-action="like"', 'data-feedback-count');
  const eventTypeLabel = elementText(cardHtml, 'data-card-type');
  const admissionLabel = elementText(cardHtml, 'data-card-status');
  return {
    fixture_id: fixture.fixture_id,
    event_id: fixture.event_id,
    fixture_payload_sha256: fixture.preview_event_sha256,
    event_type: { present: eventTypeLabel !== null, label: eventTypeLabel },
    admission: { present: admissionLabel !== null, label: admissionLabel },
    occurrence: {
      present: Boolean(occurrence),
      label: occurrence ? elementText(cardHtml, 'data-occurrence-label') : null,
      aria_label: occurrence ? attribute(occurrence, 'aria-label') : null,
      variant: occurrence ? attribute(occurrence, 'data-occurrence-label-variant') : null,
      complex: occurrence ? attribute(occurrence, 'data-occurrence-complex') === 'true' : null,
    },
    calendar: {
      eligible: attribute(article, 'data-calendar-eligible') === 'true',
      present: Boolean(calendar),
      label: calendar ? elementText(cardHtml, 'data-calendar-label') : null,
    },
    actions: {
      not_interested: { present: Boolean(negative), label: negative ? elementText(cardHtml, 'data-feedback-action="not_interested"') : null },
      calendar: { present: Boolean(calendar), label: calendar ? elementText(cardHtml, 'data-calendar-label') : null },
      share: { ...share, label: elementText(cardHtml, 'data-share-label') },
      like,
    },
    branch_families: [
      `event-type:${eventTypeLabel === null ? 'absent' : 'present'}`,
      `admission:label:${admissionLabel}`,
      `occurrence:${attribute(occurrence || '', 'data-occurrence-complex') === 'true' ? 'complex' : 'simple'}`,
      `calendar:${calendar ? 'present' : 'absent'}`,
      `like-count:${like.base_count > 0 ? 'nonzero' : 'zero'}`,
      `share-count:${share.base_count > 0 ? 'nonzero' : 'zero'}`,
      'actions:split-actions',
    ],
  };
}

function unique(values) {
  return [...new Set(values)].sort();
}

export function summarizeInventory(rows) {
  const semantic = (row) => row.semantic || (row.event_type?.semantic_value ? {
    event_type:row.event_type,
    admission:row.admission,
    actions:row.actions,
    social_proof:row.social_proof,
    anomalies:row.semantic_anomalies || [],
  } : {
    event_type:row.event_type,
    admission:{...row.admission,state:row.admission?.label ? 'legacy-label' : 'absent'},
    actions:row.actions,
    social_proof:{like:{count:row.actions.like.base_count},share:{count:row.actions.share.base_count}},
    anomalies:[],
  });
  return {
    event_type_semantic_values: unique(rows.map((row) => semantic(row).event_type.semantic_value).filter(Boolean)),
    event_type_labels: unique(rows.map((row) => semantic(row).event_type.label).filter(Boolean)),
    admission_states: unique(rows.map((row) => semantic(row).admission.state).filter(Boolean)),
    admission_labels: unique(rows.map((row) => semantic(row).admission.visible === false ? null : semantic(row).admission.label).filter(Boolean)),
    source_rendered_admission_labels: unique(rows.map((row) => row.source_rendered?.admission?.label || row.admission?.label).filter(Boolean)),
    occurrence_labels: unique(rows.map((row) => row.occurrence.label).filter(Boolean)),
    action_labels: unique(rows.flatMap((row) => Object.values(semantic(row).actions).map((action) => action.label)).filter(Boolean)),
    like_count_values: unique(rows.map((row) => semantic(row).social_proof.like.count)),
    share_count_values: unique(rows.map((row) => semantic(row).social_proof.share.count)),
    semantic_anomalies: unique(rows.flatMap((row) => semantic(row).anomalies)),
    branch_families: unique(rows.flatMap((row) => row.branch_families)),
  };
}

export function buildInventory(args) {
  for (const key of ['astro-source-site', 'astro-source-sha', 'tooling-root', 'tooling-sha', 'corpus-root', 'semantic-census', 'harness', 'output']) {
    if (!args[key]) throw new Error(`--${key} is required`);
  }
  const site = resolve(args['astro-source-site']);
  const toolingRoot = resolve(args['tooling-root']);
  const corpusRoot = resolve(args['corpus-root']);
  const harness = resolve(args.harness);
  const output = resolve(args.output);
  const astroSource = assertImmutableCheckout({ root: resolve(site, '..'), expectedSha: args['astro-source-sha'], label: 'Astro source checkout' });
  const tooling = assertImmutableCheckout({ root: toolingRoot, expectedSha: args['tooling-sha'], label: 'Conformance tooling checkout' });
  const corpus = readJson(join(corpusRoot, 'corpus.json'));
  const semanticCensus = readJson(args['semantic-census']);
  if (semanticCensus.schema_version !== 'event_card_large_production_semantic_census.v1' || semanticCensus.public_projection?.event_count !== 703) throw new Error('Semantic production census binding is invalid');
  if (corpus.corpus_id !== 'ui-reference-events.v1' || corpus.version !== 'v1' || corpus.fixtures.length !== 8) {
    throw new Error('Chip inventory is bounded to the exact eight-fixture Golden Event Corpus v1');
  }
  const sourceRootRelation = relative(harness, site);
  if (harness === parse(harness).root || harness.length < 12 || sourceRootRelation === '') throw new Error('Harness must be a disposable path outside Astro source');
  rmSync(harness, { recursive: true, force: true });
  mkdirSync(join(harness, 'src/pages/chip-inventory'), { recursive: true });
  copyTree(join(site, 'src'), join(harness, 'upstream'));
  copyTree(join(site, 'public'), join(harness, 'public'));
  const nodeModules = resolve(args['node-modules'] || join(site, 'node_modules'));
  if (!existsSync(join(nodeModules, 'astro/bin/astro.mjs'))) throw new Error('Lockfile-compatible Astro node_modules is missing');
  symlinkSync(nodeModules, join(harness, 'node_modules'), 'dir');
  writeFileSync(join(harness, 'package.json'), `${JSON.stringify({ name: 'event-card-chip-inventory', private: true, type: 'module', scripts: { build: 'astro build' } }, null, 2)}\n`);
  writeFileSync(join(harness, 'astro.config.mjs'), "import { defineConfig } from 'astro/config';\nexport default defineConfig({output:'static',trailingSlash:'always',vite:{server:{fs:{strict:false}}}});\n");
  const fixtureEvents = new Map();
  for (const fixture of corpus.fixtures) {
    const wrapper = readJson(join(corpusRoot, fixture.payload_path));
    if (wrapper.preview_event_sha256 !== fixture.preview_event_sha256) throw new Error(`Fixture identity mismatch: ${fixture.fixture_id}`);
    fixtureEvents.set(fixture.fixture_id, wrapper.preview_event);
    const row = { id: fixture.fixture_id, renderer: 'event-card', source_paths: ['src/components/EventCard.astro'], props: { variant: 'split-actions', desktopRelatedCrop: false, mobileFlowMedia: false }, container: { width: 474 } };
    writeFileSync(join(harness, 'src/pages/chip-inventory', `${fixture.fixture_id}.astro`), renderSpecimenPage(row, { event: wrapper.preview_event, trace: { corpus_id: corpus.corpus_id, fixture_id: fixture.fixture_id } }));
  }
  const astro = join(harness, 'node_modules/astro/bin/astro.mjs');
  const build = spawnSync(process.execPath, [astro, 'build'], {
    cwd: harness,
    encoding: 'utf8',
    env: { ...process.env, TZ: corpus.reference_clock.timezone, LANG: 'ru_RU.UTF-8', PUBLIC_STATIC_SITE_CURRENT_DATE: corpus.reference_clock.current_date, PUBLIC_STATIC_SITE_REFERENCE_ISO: corpus.reference_clock.reference_iso, PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE: 'off' },
  });
  if (build.status !== 0) throw new Error(`Astro chip inventory build failed:\n${build.stderr.slice(-4000)}\n${build.stdout.slice(-4000)}`);
  const cases = corpus.fixtures.map((fixture) => {
    const sourceRendered = extractGeneratedChipState(readFileSync(join(harness, 'dist/chip-inventory', fixture.fixture_id, 'index.html'), 'utf8'), fixture);
    const semantics = resolveEventCardSemantics(fixtureEvents.get(fixture.fixture_id), { calendarEligible:sourceRendered.calendar.eligible, defaultCurrency:semanticCensus.admission.current_renderer_currency_default });
    const { event_type:sourceEventType, admission:sourceAdmission, actions:sourceActions, ...generatedFacts } = sourceRendered;
    return {
      ...generatedFacts,
      event_type:semantics.event_type,
      admission:semantics.admission,
      domain_evidence:semantics.domain_evidence,
      actions:semantics.actions,
      social_proof:semantics.social_proof,
      semantic_anomalies:semantics.anomalies,
      source_rendered:{ event_type:sourceEventType, admission:sourceAdmission, actions:sourceActions },
      branch_families:unique([...sourceRendered.branch_families, `event-type-value:${semantics.event_type.semantic_value}`, `admission-state:${semantics.admission.state}`, `admission-visible:${semantics.admission.visible}`, `like-proof:${semantics.social_proof.like.visible ? 'present' : 'absent'}`, `share-proof:${semantics.social_proof.share.visible ? 'present' : 'absent'}`]),
    };
  });
  const componentPath = join(site, 'src/components/EventCard.astro');
  const occurrencePath = join(site, 'src/components/EventOccurrenceLabel.astro');
  const report = {
    schema_version: 'event_card_large_chip_inventory.v2',
    corpus_id: corpus.corpus_id,
    corpus_sha256: corpus.corpus_sha256,
    reference_clock: corpus.reference_clock,
    astro_source_repository_sha: astroSource.sha,
    conformance_tooling_repository_sha: tooling.sha,
    generation_mode: 'real-astro-static-build/exact-event-card/split-actions',
    production_source_mutated: false,
    semantic_projection_mode: 'adapter-only/pre-penpot-acceptance',
    production_census:{ schema_version:semanticCensus.schema_version, exact_source_artifact_sha256:semanticCensus.authoritative_exact_census.sha256, event_count:semanticCensus.public_projection.event_count, rendered_event_type_label_count:semanticCensus.event_type.rendered_label_count, rendered_price_label_count:semanticCensus.admission.rendered_price_label_count, component_variant_count:semanticCensus.event_type.component_variant_count },
    source_files: [
      { path: 'site/src/components/EventCard.astro', sha256: sha256(readFileSync(componentPath)) },
      { path: 'site/src/components/EventOccurrenceLabel.astro', sha256: sha256(readFileSync(occurrencePath)) },
    ],
    fixture_count: cases.length,
    cases,
    summary: summarizeInventory(cases),
  };
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  return { output, report_sha256: sha256(readFileSync(output)), fixture_count: cases.length, summary: report.summary };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.stdout.write(`${JSON.stringify(buildInventory(parseArgs(process.argv.slice(2))), null, 2)}\n`);
}
