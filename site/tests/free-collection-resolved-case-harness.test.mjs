import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { inspectResolvedRenderCase } from '../scripts/inspect-resolved-render-case.mjs';

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]));
  return value;
}
const digest = (document) => createHash('sha256').update(`${JSON.stringify(canonicalize({ ...document, content_sha256: null }))}\n`).digest('hex');

test('Astro evidence harness reads an externally supplied resolved case without owning its payload', async () => {
  const document = {
    schema: 'kenigevents.resolved-render-case.v1',
    case_id: 'nonvisual.contract-test',
    control_generation: 3,
    authority: { input_bindings: {}, generator: 'external' },
    payload: { fixture_order: [], groups: {} },
    content_sha256: null,
  };
  document.content_sha256 = digest(document);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'astro-resolved-case-'));
  const file = path.join(directory, 'case.json');
  fs.writeFileSync(file, `${JSON.stringify(document)}\n`);
  const result = await inspectResolvedRenderCase(file, document.content_sha256);
  assert.equal(result.resolved_case_content_sha256, document.content_sha256);
  fs.writeFileSync(file, `${JSON.stringify({ ...document, case_id: 'tampered' })}\n`);
  await assert.rejects(inspectResolvedRenderCase(file, document.content_sha256), /RESOLVED_CASE_HASH_MISMATCH/u);
});
