/**
 * Platform-neutral focus-group OTP journey. UI adapters own selectors and
 * browser/native mechanics; this module owns the one-side-effect contract.
 */
export async function runFocusOtpBrowserTab({ ui, mailbox, recipient, timeoutMs, step }) {
  await mailbox.connect();
  const checkpoint = await mailbox.checkpoint();
  step('mailbox_checkpoint');

  await ui.openInvite();
  const observedRepoSha = await ui.verifyReleaseIdentity();
  step('release_identity_checked');
  await ui.waitForInstallStage();
  await ui.captureMaskedEvidence('01-invite-accepted');
  step('invite_accepted');

  await ui.skipInstall();
  await ui.openEmailStep();
  const emailKeyboard = await ui.focusEmailInput();
  await ui.enterEmail(recipient);
  await ui.captureMaskedEvidence('02-email-step');
  // Masking is destructive by design, so ordinary input is repeated before
  // the sole issuance attempt. There is never an automatic resend.
  await ui.enterEmail(recipient);
  step('email_entered');

  await ui.requestOtpWithCompetingGestures();
  await ui.waitForCodeStep();
  await ui.captureMaskedEvidence('03-mail-accepted-ui');
  step('mail_request_accepted_ui');

  const mail = await mailbox.waitForSingleOtp({ checkpoint, recipient, timeoutMs });
  step('single_inbox_message_received');
  ui.setOtpSecret?.(mail.otp);
  const otpKeyboard = await ui.focusOtpInput();
  await ui.enterOtpDigitByDigit(mail.otp);
  await ui.waitForMembershipConfirmed();
  step('otp_autosubmit_confirmed');
  await ui.captureMaskedEvidence('04-membership-confirmed');

  const counts = await ui.requestCounts();
  if (counts.issue !== 1) throw new Error(`otp_issue_count:${counts.issue}`);
  if (counts.verify !== 1) throw new Error(`otp_verify_count:${counts.verify}`);
  if (counts.registration !== 1) throw new Error(`participant_registration_count:${counts.registration}`);
  if (![200, 204, 409].includes(counts.registrationStatus)) {
    throw new Error(`participant_registration_status:${counts.registrationStatus}`);
  }

  await ui.reloadOrReopen();
  await ui.waitForReturningMember();
  const afterReload = await ui.requestCounts();
  if (afterReload.issue !== 1) throw new Error('otp_reissued_after_reload');
  step('returning_state_persisted');
  await ui.captureMaskedEvidence('05-returning-state');

  return {
    observedRepoSha,
    mail,
    counts,
    keyboardAcceptance: { email: emailKeyboard, otp: otpKeyboard },
  };
}
