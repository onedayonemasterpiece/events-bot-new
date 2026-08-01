import process from 'node:process';

import { createClient } from '@supabase/supabase-js';

const TEST_EMAIL = 'focus-agent-e2e@kenigevents.ru';
const INVITE = 'focus-group-2026-announcements';

function argument(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const supabaseUrl = String(process.env.PERSONALIZATION_SUPABASE_URL || '').replace(/\/+$/u, '');
const serviceKey = String(
  process.env.PERSONALIZATION_SUPABASE_SECRET_KEY
  || process.env.PERSONALIZATION_SUPABASE_SERVICE_KEY
  || '',
);
const origin = argument('--origin', 'https://kenigevents.ru').replace(/\/+$/u, '');
const basePath = `/${argument('--base-path', '').replace(/^\/+|\/+$/gu, '')}`.replace(/^\/$/u, '');

if (!supabaseUrl || !serviceKey) {
  throw new Error('Set PERSONALIZATION_SUPABASE_URL and a server-only Supabase service key.');
}

const entryUrl = new URL(`${origin}${basePath}/fokus-gruppa/priglashenie/`);
entryUrl.searchParams.set('launch', 'pwa');
entryUrl.searchParams.set('agent_test_email', TEST_EMAIL);
entryUrl.hash = `invite=${INVITE}`;

const client = createClient(supabaseUrl, serviceKey, {
  auth: { autoRefreshToken: false, persistSession: false },
});
const { data, error } = await client.auth.admin.generateLink({
  type: 'magiclink',
  email: TEST_EMAIL,
  options: { redirectTo: entryUrl.href },
});
if (error) throw error;

const otp = String(data?.properties?.email_otp || '');
const magicLink = String(data?.properties?.action_link || '');
if (!/^\d{6}$/u.test(otp) || !magicLink.startsWith('https://')) {
  throw new Error('Supabase did not return the expected temporary OTP/link pair.');
}

console.log(`Email: ${TEST_EMAIL}`);
console.log(`Temporary OTP: ${otp}`);
console.log(`OTP entry: ${entryUrl.href}`);
console.log(`One-time link: ${magicLink}`);
console.log('These credentials are temporary. Do not commit or paste them into public logs.');
