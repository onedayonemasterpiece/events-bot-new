import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const read=p=>readFileSync(new URL(p,import.meta.url),'utf8');
const css=read('../src/components/design-system/foundations.css');
const layout=read('../src/layouts/EventLayout.astro');
const authority=JSON.parse(read('../src/components/design-system/f0-typography-authority.v1.json'));
test('card hierarchy is readable and centralized without changing page heading roles',()=>{
 for(const [role,weight] of Object.entries({title:700,metadata:500,status:600,action:600})){
  assert.match(css,new RegExp(`--ke-type-card-${role}-weight: ${weight};`));
  assert.equal(authority.weights.core_roles[`--ke-type-card-${role}-weight`],weight);
 }
 assert.match(css,/--ke-type-h1-weight: 950;/);
});
test('shared EventCard selectors consume card roles, no per-route heavy weights',()=>{
 for(const [sel,role] of [['title','title'],['meta','metadata'],['footer','metadata'],['tag','status'],['status','status'],['other-times','status']]){
  const block=layout.match(new RegExp(`\\.event-card__${sel} \\{([^}]+)\\}`))[1];
  assert.ok(block.includes(`font-weight: var(--ke-type-card-${role}-weight)`));
  assert.doesNotMatch(block,/font-weight:\s*\d/);
 }
 assert.match(layout,/\.feedback-button \{[^}]*font-weight: var\(--ke-type-card-action-weight\)/);
});

test('typography checker follows rejected-baseline status and only the actual mobile heading alias scope',()=>{
 const checker=read('../src/components/design-system/check-f0-typography-authority.mjs');
 assert.ok(checker.includes("assert.equal(authority.status, 'ACTIVE_OWNER_CORRECTION_NOT_VISUAL_ACCEPTANCE'"));
 assert.ok(checker.includes('&& insideMobileScope'));
 assert.ok(checker.includes('assert.ok(!previous || responsiveHeadingAlias'));
});
