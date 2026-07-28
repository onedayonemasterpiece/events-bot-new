import { existsSync, readFileSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const PUBLIC_SEARCH_KEYS = Object.freeze({
  PUBLIC_PERSONALIZATION_SUPABASE_URL: [
    'PUBLIC_PERSONALIZATION_SUPABASE_URL',
    'STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL',
    'PERSONALIZATION_SUPABASE_URL',
  ],
  PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: [
    'PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
    'STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
    'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
  ],
  PUBLIC_YANDEX_AUTH_PROVIDER: [
    'PUBLIC_YANDEX_AUTH_PROVIDER',
    'STATIC_SITE_PUBLIC_YANDEX_AUTH_PROVIDER',
  ],
  PUBLIC_AUTHORIZED_SEARCH_TRANSPORT: [
    'PUBLIC_AUTHORIZED_SEARCH_TRANSPORT',
    'STATIC_SITE_PUBLIC_AUTHORIZED_SEARCH_TRANSPORT',
  ],
});

function parseDotEnv(text) {
  const result = {};
  for (const rawLine of String(text || '').split(/\r?\n/u)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const match = line.match(/^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/u);
    if (!match) continue;
    let value = match[2].trim();
    if (
      value.length >= 2
      && ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'")))
    ) {
      value = value.slice(1, -1);
    }
    result[match[1]] = value;
  }
  return result;
}

function primaryCheckoutRoot(siteDir) {
  const repoRoot = resolve(siteDir, '..');
  const commonDirResult = spawnSync(
    'git',
    ['rev-parse', '--path-format=absolute', '--git-common-dir'],
    { cwd: repoRoot, encoding: 'utf8' },
  );
  if (commonDirResult.status !== 0) return '';
  const commonDirValue = commonDirResult.stdout.trim();
  if (!commonDirValue) return '';
  const commonDir = isAbsolute(commonDirValue) ? commonDirValue : resolve(repoRoot, commonDirValue);
  return dirname(commonDir);
}

export function previewEnvCandidates(siteDir, runtimeEnv = process.env) {
  const repoRoot = resolve(siteDir, '..');
  const candidates = [];
  if (runtimeEnv.STATIC_SITE_PREVIEW_ENV_FILE) {
    candidates.push(resolve(repoRoot, runtimeEnv.STATIC_SITE_PREVIEW_ENV_FILE));
  }
  candidates.push(join(repoRoot, '.env'));
  const primaryRoot = primaryCheckoutRoot(siteDir);
  if (primaryRoot) candidates.push(join(primaryRoot, '.env'));
  return [...new Set(candidates)];
}

export function loadPreviewPublicConfig(siteDir, runtimeEnv = process.env) {
  const fileValues = {};
  let source = '';
  for (const candidate of previewEnvCandidates(siteDir, runtimeEnv)) {
    if (!existsSync(candidate)) continue;
    Object.assign(fileValues, parseDotEnv(readFileSync(candidate, 'utf8')));
    source ||= candidate;
  }
  const values = {};
  for (const [publicName, aliases] of Object.entries(PUBLIC_SEARCH_KEYS)) {
    const value = aliases
      .map((name) => runtimeEnv[name] || fileValues[name] || '')
      .find((candidate) => String(candidate).trim());
    if (value) values[publicName] = String(value).trim();
  }
  if (!values.PUBLIC_YANDEX_AUTH_PROVIDER) values.PUBLIC_YANDEX_AUTH_PROVIDER = 'custom:yandex';
  if (
    values.PUBLIC_AUTHORIZED_SEARCH_TRANSPORT
    && !/^(?:json|ndjson)$/u.test(values.PUBLIC_AUTHORIZED_SEARCH_TRANSPORT)
  ) {
    throw new Error('PUBLIC_AUTHORIZED_SEARCH_TRANSPORT must be json or ndjson');
  }
  const configured = Boolean(
    values.PUBLIC_PERSONALIZATION_SUPABASE_URL
    && values.PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY,
  );
  return {
    values,
    configured,
    source: source ? 'dotenv' : 'process',
  };
}

export function requirePreviewAuthorizedSearch(config, runtimeEnv = process.env) {
  const required = /^(?:1|true|yes|on)$/iu.test(String(runtimeEnv.PREVIEW_REQUIRE_AUTHORIZED_SEARCH || ''));
  if (required && !config.configured) {
    throw new Error(
      'Authorized Search is required for this preview, but no browser-safe Supabase URL/publishable key were found. '
      + 'Set PUBLIC_*, STATIC_SITE_PUBLIC_* or PERSONALIZATION_SUPABASE_URL/PUBLISHABLE_KEY in the explicit preview env file.',
    );
  }
}
