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
  assert.match(intake, /token\.length === EMAIL_OTP_LENGTH[\s\S]*void verifyOtp\(\)/u);
  assert.match(helper, /EMAIL_OTP_LENGTH = 6/u);
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
  assert.match(panel, /return 'skip' as const/u);
  assert.match(panel, /window\.addEventListener\('online'/u);
  assert.match(state, /24 \* 60 \* 60 \* 1000/u);
  assert.match(state, /FOCUS_FEEDBACK_SCORE_MAX_BYTES = 2_048/u);
  assert.match(migration, /unique index if not exists focus_group_feedback_user_request_uidx/u);
  assert.match(migration, /feedback_daily_limit_exceeded/u);
  assert.match(migration, /grant execute[\s\S]*to authenticated/u);
  assert.match(migration, /revoke all[\s\S]*from public, anon, authenticated/u);
});
