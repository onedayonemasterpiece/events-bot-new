import { createHash } from 'node:crypto';
import { readFile, readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';

const EMAIL = /[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}/giu;
const JWT = /\beyJ[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\b/giu;
const AUTH = /\b(?:authorization\s*:|bearer\s+)[^\s"']+/giu;
const TOKEN_VALUE = /\b(?:access_token|refresh_token|token_hash)\b\s*[:=]\s*["']?[^\s,"'}]+/giu;

export function shortHash(value) {
  return createHash('sha256').update(String(value || '')).digest('hex').slice(0, 16);
}

export function redactText(value, secrets = []) {
  let result = String(value || '');
  for (const secret of secrets.filter(Boolean).sort((a, b) => String(b).length - String(a).length)) {
    result = result.split(String(secret)).join('[redacted]');
  }
  return result
    .replace(EMAIL, '[email]')
    .replace(JWT, '[jwt]')
    .replace(AUTH, '[authorization]')
    .replace(TOKEN_VALUE, '[token]');
}

export function scanUnsafeText(value, secrets = []) {
  const text = String(value || '');
  const findings = [];
  if (secrets.some((secret) => secret && text.includes(String(secret)))) findings.push('exact_secret');
  if (EMAIL.test(text)) findings.push('email');
  EMAIL.lastIndex = 0;
  if (JWT.test(text)) findings.push('jwt');
  JWT.lastIndex = 0;
  if (AUTH.test(text)) findings.push('authorization');
  AUTH.lastIndex = 0;
  if (TOKEN_VALUE.test(text)) findings.push('token_value');
  TOKEN_VALUE.lastIndex = 0;
  return [...new Set(findings)];
}

async function filesUnder(root) {
  const result = [];
  for (const name of await readdir(root)) {
    const path = join(root, name);
    const info = await stat(path);
    if (info.isDirectory()) result.push(...await filesUnder(path));
    else result.push(path);
  }
  return result;
}

export async function auditEvidenceDirectory(root, secrets = []) {
  const unsafe = [];
  for (const path of await filesUnder(root)) {
    if (/\.(?:png|jpg|jpeg|webp)$/iu.test(path)) continue;
    const findings = scanUnsafeText(await readFile(path, 'utf8'), secrets);
    if (findings.length) unsafe.push({ file: path.slice(root.length + 1), findings });
  }
  return { passed: unsafe.length === 0, unsafe };
}
