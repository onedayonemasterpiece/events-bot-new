#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import process from 'node:process';

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]));
  return value;
}
const canonicalJson = (value) => `${JSON.stringify(canonicalize(value))}\n`;
const sha256 = (value) => createHash('sha256').update(value).digest('hex');

export async function inspectResolvedRenderCase(casePath, expectedContentSha256) {
  const document = JSON.parse(await readFile(casePath, 'utf8'));
  if (document.schema !== 'kenigevents.resolved-render-case.v1') throw new Error('RESOLVED_CASE_SCHEMA_MISMATCH');
  const actual = sha256(canonicalJson({ ...document, content_sha256: null }));
  if (document.content_sha256 !== actual || actual !== expectedContentSha256) throw new Error('RESOLVED_CASE_HASH_MISMATCH');
  return {
    schema: 'kenigevents.astro-evidence-harness-readback.v1',
    consumer: 'Astro evidence harness',
    case_id: document.case_id,
    resolved_case_content_sha256: actual,
    fixture_order: document.payload.fixture_order,
    groups: document.payload.groups,
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [casePath, expectedContentSha256] = process.argv.slice(2);
  if (!casePath || !expectedContentSha256) throw new Error('usage: case-path expected-content-sha256');
  inspectResolvedRenderCase(casePath, expectedContentSha256).then((result) => {
    process.stdout.write(`${JSON.stringify(result)}\n`);
  }).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
