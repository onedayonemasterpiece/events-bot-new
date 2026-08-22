#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const sha256 = (value) => createHash('sha256').update(value).digest('hex');

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function semanticIconName(filename) {
  return filename
    .replace(/^\d+-/u, '')
    .replace(/\.svg$/u, '')
    .replace(/-2$/u, '');
}

export function extractFestivalCategoryIcons(source) {
  const match = /const festivalCategoryIcons:\s*Record<string, string\[\]>\s*=\s*(\{[\s\S]*?\n\});/u.exec(source);
  if (!match) throw new Error('festivalCategoryIcons is absent from the actual Astro renderer');
  // Normalize values out of the VM realm so strict read-back comparisons do not
  // accidentally depend on the parser implementation's Array prototype.
  const raw = JSON.parse(JSON.stringify(vm.runInNewContext(`(${match[1]})`, Object.create(null), { timeout: 1000 })));
  return Object.fromEntries(Object.entries(raw).map(([category, icons]) => [
    category,
    icons.map(semanticIconName),
  ]));
}

function canonicalCategoryRows(mapping, timeline) {
  const counts = new Map();
  for (const item of timeline.festivals) counts.set(item.category, (counts.get(item.category) || 0) + 1);
  const missing = [...counts.keys()].filter((category) => !mapping[category]);
  if (missing.length) throw new Error(`Actual generated categories lack an explicit icon mapping: ${missing.join(', ')}`);
  return Object.entries(mapping).map(([label, icons]) => ({
    label,
    semantic_value: label.toLocaleLowerCase('ru-RU').replace(/\s+/gu, '-'),
    occurrence_count: counts.get(label) || 0,
    icons,
    icon_slot_count: icons.length,
  }));
}

export function buildFestivalSemanticCensus({ root = resolve(import.meta.dirname, '../..') } = {}) {
  const rendererPath = resolve(root, 'site/src/pages/festivali/index.astro');
  const timelinePath = resolve(root, 'site/src/data/festival-timeline.json');
  const previewPath = resolve(root, 'site/src/data/preview-events.json');
  const rendererSource = readFileSync(rendererPath, 'utf8');
  const timelineSource = readFileSync(timelinePath, 'utf8');
  const previewSource = readFileSync(previewPath, 'utf8');
  const timeline = JSON.parse(timelineSource);
  const preview = JSON.parse(previewSource);
  const categoryIcons = extractFestivalCategoryIcons(rendererSource);
  const events = new Map(preview.events.map((event) => [event.id, event]));
  const categories = canonicalCategoryRows(categoryIcons, timeline);
  const festivals = timeline.festivals.map((item) => {
    const event = item.internalEventId === null ? null : events.get(item.internalEventId);
    if (item.internalEventId !== null && !event) throw new Error(`Festival ${item.slug} references missing preview event ${item.internalEventId}`);
    const baseCount = event ? Number(event.likes_count || 0) : 0;
    return {
      slug: item.slug,
      internal_event_id: item.internalEventId,
      theme: {
        component_id: 'festival.meta.theme',
        component_variant: 'default',
        semantic_value: item.category.toLocaleLowerCase('ru-RU').replace(/\s+/gu, '-'),
        label: item.category.toLocaleLowerCase('ru-RU'),
        icons: categoryIcons[item.category],
        secondary_icon_present: categoryIcons[item.category].length > 1,
      },
      like: {
        action_component_id: 'event.action.like',
        proof_component_id: 'event.social-proof.like',
        base_count: baseCount,
        count_label: baseCount > 0 ? String(baseCount) : '',
        proof_present: baseCount > 0,
      },
    };
  });
  return {
    schema_version: 'festival_card_semantic_census.v1',
    evidence_mode: 'actual-astro-renderer-source-plus-generated-projection-data',
    source_files: [
      { path: 'site/src/pages/festivali/index.astro', sha256: sha256(rendererSource) },
      { path: 'site/src/data/festival-timeline.json', sha256: sha256(timelineSource) },
      { path: 'site/src/data/preview-events.json', sha256: sha256(previewSource) },
    ],
    production_source_mutated: false,
    festival_count: festivals.length,
    category_count: categories.length,
    joined_event_count: festivals.filter((row) => row.internal_event_id !== null).length,
    source_facts: {
      theme_model: 'one-label/one-or-two-category-icons',
      responsive_secondary_icon: 'hidden-at-820px-through-1000px',
      renderer_like_model: 'festival-local-favorite/no-aggregate-count',
    },
    target_model: {
      owner_override: 'shared-like-action/aggregate-social-proof',
      reverse_integration_state: 'pre-acceptance/no-production-mutation',
    },
    component_contract: {
      theme: {
        component_id: 'festival.meta.theme',
        component_variant: 'default',
        label_role: 'content',
        icon_slots: ['primary', 'secondary'],
        secondary_icon_presence: 'optional',
      },
      like: {
        action_component_id: 'event.action.like',
        proof_component_id: 'event.social-proof.like',
        local_selection_state: 'behavior-only',
        festival_specific_component: false,
      },
    },
    categories,
    festivals,
  };
}

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 2) {
    if (!argv[index]?.startsWith('--') || !argv[index + 1]) throw new Error(`Invalid argument near ${argv[index] || '<end>'}`);
    out[argv[index].slice(2)] = argv[index + 1];
  }
  return out;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  if (!args.output) throw new Error('--output is required');
  const report = buildFestivalSemanticCensus({ root: resolve(args.root || resolve(import.meta.dirname, '../..')) });
  const output = resolve(args.output);
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ output, sha256: sha256(readFileSync(output)), festival_count: report.festival_count, category_count: report.category_count }, null, 2)}\n`);
}
