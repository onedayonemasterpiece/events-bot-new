import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => readFile(path.join(siteRoot, relative), 'utf8');

function occurrences(source, pattern) {
  return Array.from(source.matchAll(pattern)).length;
}

test('EventLayout mounts the target personalization runtime exactly once', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  assert.equal(occurrences(layout, /<PersonalizationRuntime\b/gu), 1);
  assert.equal(occurrences(layout, /import PersonalizationRuntime from/gu), 1);
  assert.doesNotMatch(layout, /personalization\/legacy\//u);
});

test('EventLayout exposes one inert standard-onboarding page-end context after content', async () => {
  const [layout, component] = await Promise.all([
    read('src/layouts/EventLayout.astro'),
    read('src/components/onboarding/StandardOnboardingPlacementContext.astro'),
  ]);
  assert.equal(occurrences(layout, /<StandardOnboardingPlacementContext\b/gu), 1);
  assert.equal(occurrences(layout, /import StandardOnboardingPlacementContext from/gu), 1);
  assert.ok(layout.indexOf('<slot />') < layout.indexOf('<StandardOnboardingPlacementContext'));
  assert.doesNotMatch(component, /<script|fetch\s*\(|localStorage|sessionStorage|indexedDB/iu);
  assert.match(component, /data-standard-onboarding-artifact-program=\{context\.artifactProgram\}/u);
  assert.match(component, /data-standard-onboarding-club-program=\{context\.clubProgram\}/u);
  assert.match(component, /data-standard-onboarding-raffle-program=\{context\.raffleProgram\}/u);
});

test('standard card click arbitration is delegated and excludes controls, rails and drag', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(layout, /const CARD_TAP_WINDOW_MS = 280/u);
  assert.match(layout, /\[data-event-card\]\[data-card-href\]/u);
  assert.match(layout, /card\.closest\('\[data-mobile-listing-rails\]'\)/u);
  assert.match(layout, /isCardInteractiveTarget\(event\.target\)/u);
  assert.match(layout, /Math\.hypot[\s\S]*CARD_DRAG_SLOP_PX/u);
  assert.match(layout, /if \(pending && now - pending\.startedAt <= CARD_TAP_WINDOW_MS\)[\s\S]*setCardLikeTrue\(card\)/u);
  assert.match(layout, /like\.getAttribute\('aria-pressed'\) === 'true'/u);
  assert.match(layout, /like\.click\(\)/u);
  assert.match(layout, /cardTapState\.delete\(card\)[\s\S]*location\.href = href/u);
});

test('question CTA is build-time typed, fail-closed and rendered before related blocks', async () => {
  const [types, cta, desktop, mobile, exporter] = await Promise.all([
    read('src/lib/types.ts'),
    read('src/components/EventQuestionCta.astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/pages/sobytiya/[slug].astro'),
    read('scripts/export-production-preview-data.py'),
  ]);
  assert.match(types, /provider: 'vk'/u);
  assert.match(types, /url: string/u);
  assert.match(types, /source: 'partner_post' \| 'managed_afisha_post'/u);
  assert.match(types, /question_cta\?: EventQuestionCta \| null/u);
  assert.match(cta, /rel="noopener noreferrer nofollow"/u);
  assert.match(cta, /Остались вопросы\?/u);
  assert.match(cta, /Задайте их в комментариях ВКонтакте/u);
  assert.ok(desktop.indexOf('<EventQuestionCta') < desktop.indexOf('class="desktop-clean-related"'));
  assert.ok(mobile.indexOf('<EventQuestionCta') < mobile.indexOf('id="discovery-feed"'));
  assert.match(exporter, /status='published'/u);
  assert.match(exporter, /target='klgdevents'/u);
  assert.doesNotMatch(cta, /source_urls|source_url/u);
});

test('question and VK action icons retain CC0 SVG Repo provenance', async () => {
  const [question, vk, provenance] = await Promise.all([
    read('public/assets/icons/question-cta/question-bubble-390478.svg.metadata.json'),
    read('public/assets/icons/question-cta/vk-repost-348786.svg.metadata.json'),
    read('public/assets/icons/question-cta/PROVENANCE.md'),
  ]);
  assert.equal(JSON.parse(question).license, 'CC0 License');
  assert.equal(JSON.parse(vk).license, 'CC0 License');
  assert.match(provenance, /svgrepo-390478/u);
  assert.match(provenance, /svgrepo-348786/u);
});
