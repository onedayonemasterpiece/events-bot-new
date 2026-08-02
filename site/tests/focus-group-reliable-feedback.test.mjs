import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('focus onboarding is sequential and offers link plus six-digit mobile OTP', async () => {
  const [intake, helper] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../src/lib/emailOtp.ts'),
  ]);
  assert.match(intake, /data-intake-stage="install"/u);
  assert.match(intake, /data-intake-stage="identity"/u);
  assert.match(intake, /data-intake-stage="done"/u);
  assert.match(intake, /Получить код и ссылку/u);
  assert.match(intake, /inputmode="numeric"/u);
  assert.match(intake, /autocomplete="one-time-code"/u);
  assert.match(intake, /pattern="\[0-9\]\{6\}"/u);
  assert.equal((intake.match(/<span data-focus-otp-digit><\/span>/g) || []).length, 6);
  assert.match(intake, /navigator\.clipboard\.readText/u);
  assert.match(intake, /permission\.state !== 'granted'/u);
  assert.match(intake, /token\.length === EMAIL_OTP_LENGTH[\s\S]*void verifyOtp\(\)/u);
  assert.match(intake, /addEventListener\('change', handleOtpMutation\)/u);
  assert.match(intake, /data-focus-email-otp-status/u);
  assert.match(intake, /otpStatus\.textContent = 'Проверяем код…'/u);
  assert.match(intake, /try \{[\s\S]*auth\.verifyEmailOtp[\s\S]*await finish\('email_intent', true\)[\s\S]*catch[\s\S]*finally/u);
  assert.match(intake, /otpInput\.removeAttribute\('aria-busy'\)/u);
  assert.doesNotMatch(intake, /otpInput\.disabled = true/u);
  assert.match(intake, /if \(result\.accepted\) \{[\s\S]*showEmailCode\(\)/u);
  assert.match(intake, /result\.status === 'ambiguous'[\s\S]*showEmailCode\(\)[\s\S]*startResendTimer\(\)/u);
  assert.match(intake, /result\.status === 'ambiguous'[\s\S]*60_000/u);
  assert.match(helper, /EMAIL_OTP_LENGTH = 6/u);
  assert.doesNotMatch(intake, /Вставить код/u);
  assert.match(intake, /data-focus-email-resend hidden/u);
});

test('confirmed identity and participant persistence have separate recovery copy', async () => {
  const intake = await read('../src/components/FocusGroupInviteIntake.astro');
  assert.match(intake, /Вход подтверждён\. Участие ещё не сохранилось/u);
  assert.match(intake, /Повторить сохранение/u);
  assert.match(intake, /signedInName\.textContent = staticAuthDisplayName\(snapshot\.user\)[\s\S]*void finish/u);
});

test('external code testing uses a dedicated temporary Supabase identity without a public bypass', async () => {
  const [intake, issuer] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../scripts/issue-focus-agent-test-credentials.mjs'),
  ]);
  assert.match(intake, /agent_test_email/u);
  assert.match(intake, /focus-agent-e2e@kenigevents\.ru/u);
  assert.match(intake, /Письмо не требуется/u);
  assert.match(issuer, /auth\.admin\.generateLink/u);
  assert.match(issuer, /email_otp/u);
  assert.match(issuer, /PERSONALIZATION_SUPABASE_(?:SECRET|SERVICE)_KEY/u);
  assert.match(issuer, /export async function issueFocusAgentTestCredentials/u);
  assert.doesNotMatch(intake, /service[_-]?role|SECRET_KEY|SERVICE_KEY/iu);
});

test('focus page rating is durable for a day and replays through an idempotent outbox', async () => {
  const [panel, state, migration] = await Promise.all([
    read('../src/components/FocusGroupLabPanel.astro'),
    read('../src/lib/focus-feedback-state.ts'),
    read('../../supabase/migrations/20260731193000_focus_group_feedback_idempotency_v2.sql'),
  ]);
  assert.match(panel, /getIdempotentOutbox/u);
  assert.match(panel, /channel: 'focus_feedback_v2'/u);
  assert.match(panel, /submit_focus_group_feedback_v2/u);
  assert.match(panel, /dataClient\.request/u);
  assert.match(panel, /return 'skip' as const/u);
  assert.match(panel, /window\.addEventListener\('online'/u);
  assert.match(state, /24 \* 60 \* 60 \* 1000/u);
  assert.match(state, /FOCUS_FEEDBACK_SCORE_MAX_BYTES = 2_048/u);
  assert.match(migration, /unique index if not exists focus_group_feedback_user_request_uidx/u);
  assert.match(migration, /feedback_daily_limit_exceeded/u);
  assert.match(migration, /grant execute[\s\S]*to authenticated/u);
  assert.match(migration, /revoke all[\s\S]*from public, anon, authenticated/u);
});
