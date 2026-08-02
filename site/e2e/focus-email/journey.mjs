/**
 * Platform-neutral focus-group OTP journey. UI adapters own selectors and
 * browser/native mechanics; this module owns the one-side-effect contract.
 */
export async function runFocusOtpBrowserTab({ ui, mailbox, recipient, timeoutMs, step, onSecret = () => {} }) {
  await ui.openInvite();
  const observedRepoSha = await ui.verifyReleaseIdentity();
  step('release_identity_checked');
  await ui.waitForInstallStage();
  await ui.captureMaskedEvidence('01-invite-accepted');
  step('invite_accepted');

  if (ui.preflightMobileKeyboards) {
    const preflight = await ui.preflightMobileKeyboards();
    if (preflight) step('side_effect_free_keyboard_controls_passed');
  }

  await ui.skipInstall();
  await ui.openEmailStep();
  const emailKeyboard = await ui.focusEmailInput();
  await ui.captureEmptyKeyboardEvidence?.('01b-product-email-empty-keyboard');

  await mailbox.connect();
  const checkpoint = await mailbox.checkpoint();
  step('mailbox_checkpoint');
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
  onSecret(mail.otp);
  ui.setOtpSecret?.(mail.otp);
  const otpKeyboard = await ui.focusOtpInput();
  await ui.captureEmptyKeyboardEvidence?.('03b-product-otp-empty-keyboard');
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
    keyboardAcceptance: { ...ui.keyboard, email: emailKeyboard, otp: otpKeyboard },
  };
}

/** iOS-only, side-effect-free keyboard/control-plane preflight. */
export async function runIosKeyboardPreflight({ ui, step }) {
  await ui.openInvite();
  const observedRepoSha = await ui.verifyReleaseIdentity();
  step('release_identity_checked');
  await ui.waitForInstallStage();
  await ui.captureMaskedEvidence('01-invite-accepted');
  await ui.preflightMobileKeyboards();
  step('side_effect_free_keyboard_controls_passed');
  await ui.skipInstall();
  await ui.openEmailStep();
  const email = await ui.focusEmailInput();
  await ui.captureEmptyKeyboardEvidence('01b-product-email-empty-keyboard');
  const counts = await ui.requestCounts();
  if (counts.issue !== 0 || counts.verify !== 0 || counts.registration !== 0) {
    throw new Error(`preflight_side_effect_detected:${counts.issue}:${counts.verify}:${counts.registration}`);
  }
  step('product_email_keyboard_passed');
  return { observedRepoSha, counts, keyboardAcceptance: { ...ui.keyboard, email }, keyboardPreflight: ui.keyboardPreflight };
}
