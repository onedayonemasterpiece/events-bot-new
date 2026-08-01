import process from 'node:process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { createClient } from '@supabase/supabase-js';

export const FOCUS_AGENT_TEST_EMAIL = 'focus-agent-e2e@kenigevents.ru';
const INVITE = 'focus-group-2026-announcements';

function argument(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

function configuredEntryUrl({ origin, basePath, entryUrl }) {
  const cleanOrigin = String(origin || 'https://kenigevents.ru').replace(/\/+$/u, '');
  const cleanBasePath = String(basePath || '').replace(/^\/+|\/+$/gu, '');
  const target = entryUrl
    ? new URL(entryUrl)
    : new URL(`${cleanOrigin}${cleanBasePath ? `/${cleanBasePath}` : ''}/fokus-gruppa/priglashenie/`);
  target.searchParams.set('launch', 'pwa');
  target.searchParams.set('agent_test_email', FOCUS_AGENT_TEST_EMAIL);
  target.hash = `invite=${INVITE}`;
  return target;
}

export async function issueFocusAgentTestCredentials(options = {}) {
  const supabaseUrl = String(
    options.supabaseUrl
    || process.env.PERSONALIZATION_SUPABASE_URL
    || process.env.STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL
    || '',
  ).replace(/\/+$/u, '');
  const serviceKey = String(
    options.serviceKey
    || process.env.PERSONALIZATION_SUPABASE_SECRET_KEY
    || process.env.PERSONALIZATION_SUPABASE_SERVICE_KEY
    || process.env.PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY
    || '',
  );
  if (!supabaseUrl || !serviceKey) {
    throw new Error('Set PERSONALIZATION_SUPABASE_URL and a server-only Supabase service key.');
  }

  const entryUrl = configuredEntryUrl(options);
  const client = createClient(supabaseUrl, serviceKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const { data, error } = await client.auth.admin.generateLink({
    type: 'magiclink',
    email: FOCUS_AGENT_TEST_EMAIL,
    options: { redirectTo: entryUrl.href },
  });
  if (error) throw error;

  const otp = String(data?.properties?.email_otp || '');
  const magicLink = String(data?.properties?.action_link || '');
  if (!/^\d{6}$/u.test(otp) || !magicLink.startsWith('https://')) {
    throw new Error('Supabase did not return the expected temporary OTP/link pair.');
  }
  return { email: FOCUS_AGENT_TEST_EMAIL, otp, entryUrl: entryUrl.href, magicLink };
}

async function main() {
  const credentials = await issueFocusAgentTestCredentials({
    origin: argument('--origin', 'https://kenigevents.ru'),
    basePath: argument('--base-path', ''),
    entryUrl: argument('--entry-url', ''),
  });
  if (process.env.GITHUB_ACTIONS === 'true') {
    console.log(`::add-mask::${credentials.otp}`);
    console.log(`::add-mask::${credentials.magicLink}`);
  }
  console.log(`Email: ${credentials.email}`);
  console.log(`Temporary OTP: ${credentials.otp}`);
  console.log(`OTP entry: ${credentials.entryUrl}`);
  console.log(`One-time link: ${credentials.magicLink}`);
  console.log('These credentials are temporary. Do not commit or paste them into public logs.');
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  await main();
}
