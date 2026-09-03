import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  goldenActionContract,
  goldenActionHref,
  goldenExpectedActionLabel,
  goldenExpectedAdmissionLabel,
} from './golden-review-actions.mjs';
import { loadGoldenCorpus } from './golden-review-corpus.mjs';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID
  || readdirSync(distDir).filter((name) => name.startsWith('preview-golden-')).sort().at(-1);
if (!buildId || !/^preview-golden-[a-zA-Z0-9._-]+$/u.test(buildId)) {
  throw new Error(`Golden action checker requires preview-golden-* build id, received ${buildId || '(empty)'}`);
}
const root = join(distDir, buildId);
const { corpus } = loadGoldenCorpus();
const evidencePath = join(root, 'data', 'golden', 'evidence.json');
if (!existsSync(evidencePath)) throw new Error('Golden action evidence is missing');
const evidence = JSON.parse(readFileSync(evidencePath, 'utf8'));
const expectedContract = goldenActionContract(corpus);
if (JSON.stringify(evidence.action_contract) !== JSON.stringify(expectedContract)) {
  throw new Error('Golden action evidence differs from the source contract');
}

for (const spec of corpus.events) {
  const detailPath = join(root, 'sobytiya', spec.slug, 'index.html');
  if (!existsSync(detailPath)) throw new Error(`Golden action detail page is missing for ${spec.id}`);
  const html = readFileSync(detailPath, 'utf8');
  const expectedHref = goldenActionHref(spec);
  const expectedActionLabel = goldenExpectedActionLabel(spec);
  const expectedAdmissionLabel = goldenExpectedAdmissionLabel(spec);
  const fixturePrefix = 'https://example.invalid/kenigevents-golden/';
  const eventFixtureNeedle = `${fixturePrefix}${spec.admission.kind}/${spec.id}`;

  if (expectedHref) {
    if (!html.includes(expectedHref)) throw new Error(`Golden event ${spec.id} does not render its ${spec.admission.kind} action href`);
    if (!html.includes(expectedActionLabel)) throw new Error(`Golden event ${spec.id} misses canonical action label: ${expectedActionLabel}`);
  } else if (html.includes(eventFixtureNeedle)) {
    throw new Error(`Golden event ${spec.id} unexpectedly renders an active external action`);
  }

  if (!html.includes(expectedAdmissionLabel)) {
    throw new Error(`Golden event ${spec.id} misses canonical admission label: ${expectedAdmissionLabel}`);
  }
  if (spec.admission.kind === 'phone' && !html.includes('tel:+74012000000')) {
    throw new Error(`Golden phone event ${spec.id} misses its tel action`);
  }
  if (spec.lifecycle_status === 'cancelled' && !html.includes('https://schema.org/EventCancelled')) {
    throw new Error(`Golden cancelled event ${spec.id} misses EventCancelled structured state`);
  }
}

console.log(JSON.stringify({
  status:'PASS',
  buildId,
  action_contract_entries:expectedContract.length,
  admission_kinds:[...new Set(expectedContract.map((item) => item.kind))].sort(),
  canonical_action_labels:[...new Set(expectedContract.map((item) => item.expected_action_label))].sort(),
  canonical_admission_labels:[...new Set(expectedContract.map((item) => item.expected_admission_label))].sort(),
  deterministic_external_origin:'https://example.invalid',
  phone_action:'tel:+74012000000',
  free_and_cancelled_external_actions:false,
}, null, 2));
