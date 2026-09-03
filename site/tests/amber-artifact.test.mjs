import assert from 'node:assert/strict';
import { access, readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');
const assembler = await read('../../scripts/assemble_mobile_search_calendar_preview.py');
const search = await read('../src/components/AuthorizedEventSearch.astro');
const layout = await read('../src/layouts/EventLayout.astro');
const learning = await read('../src/components/SearchCollectionLinks.astro');
const artifactPage = await read('../src/pages/artefakty/index.astro');

test('standalone Search restores the accepted flat large textarea without replacing runtime hooks', () => {
  assert.match(search, /standalone \? \(\s*<textarea[^>]*rows="3"[^>]*data-search-input/u);
  assert.match(search, /Что хочется сделать\?/u);
  assert.match(search, /data-search-submit/u);
  assert.match(layout, /\.authorized-search--standalone \{[\s\S]*?border:\s*0;[\s\S]*?background:\s*transparent;[\s\S]*?box-shadow:\s*none;/u);
  assert.match(layout, /\.authorized-search--standalone \.authorized-search__form textarea \{[\s\S]*?min-height:\s*82px;[\s\S]*?border-bottom:\s*2px solid/u);
  assert.match(layout, /\.authorized-search--standalone \.authorized-search__submit \{[\s\S]*?min-height:\s*50px;[\s\S]*?width:\s*100%/u);
  assert.match(layout, /\.authorized-search--standalone \.authorized-search__submit::before \{[\s\S]*?background:\s*#98401f/u);
  assert.match(layout, /@keyframes authorized-search-submit-indeterminate \{\s*from \{ transform: translateX\(-70%\); \}\s*to \{ transform: translateX\(180%\); \}/u);
  assert.match(learning, /input instanceof HTMLInputElement \|\| input instanceof HTMLTextAreaElement/u);
});

test('artifact route keeps one normalized research composition and central unavailable text role', () => {
  assert.match(artifactPage, /import '\.\.\/\.\.\/components\/design-system\/product-contour-foundations\.css'/u);
  assert.match(artifactPage, /data-ds-family="ArtifactCollectionRouteComposition"/u);
  assert.match(artifactPage, /data-ds-version="1"/u);
  assert.match(artifactPage, /data-ds-variant="research-collection"/u);
  assert.match(artifactPage, /data-ds-state=\{artifactResearchEnabled \? 'enabled' : 'unavailable'\}/u);
  assert.match(artifactPage, /color:var\(--ke-color-text-muted\)/u);
  assert.doesNotMatch(artifactPage, /color:\s*#76645a/u);
});

test('amber prototype builds two isolated placements without nesting a button inside the event link', () => {
  assert.match(assembler, /\("artifact-tail", "tail", candidates\[0\]\)/u);
  assert.match(assembler, /\("artifact-after-medallion", "after-medallion", candidates\[1\]\)/u);
  assert.match(assembler, /row\.replace\(needle, f'<\/a>\{artifact\}<button/u);
  assert.match(assembler, /f'<\/button>\{artifact\}<\/span><\/div><\/article>'/u);
  assert.match(assembler, /data-artifact-placement/u);
  assert.match(assembler, /artifact-prototypes\.json/u);
  assert.match(assembler, /source = output \/ ARTIFACT_SOURCE_PAGE \/ "index\.html"/u);
  assert.match(assembler, /Artifact A\/B must inherit the accepted Reference4 menu/u);
});

test('amber artifact is retina-ready, bounded to the rail and motion-accessible', async () => {
  for (const scale of [1, 2, 3]) {
    await access(new URL(`../public/assets/gamification/amber-cosmonaut-${scale}x.webp`, import.meta.url));
  }
  assert.match(assembler, /width="74" height="96"/u);
  assert.match(assembler, /-1x\.webp 1x,[^\n]+-2x\.webp 2x,[^\n]+-3x\.webp 3x/u);
  assert.match(assembler, /height:\s*112px/u);
  assert.match(assembler, /IntersectionObserver/u);
  assert.match(assembler, /intersectionRatio < \.72/u);
  assert.match(assembler, /amber-float 3000ms 440ms ease-in-out infinite alternate/u);
  assert.match(assembler, /amber-shine-cycle 5200ms 760ms linear infinite/u);
  assert.match(assembler, /amber-artifact__rays/u);
  assert.match(assembler, /prefers-reduced-motion:\s*reduce/u);
  assert.match(assembler, /kenigevents:artifact-collected/u);
  assert.match(assembler, /aria-live="polite"/u);
});
