import { normalizePrelaunchEmail } from '../lib/prelaunchEmail';

function initializePrelaunchEmailGuard(): void {
  const form = document.querySelector<HTMLFormElement>('[data-prelaunch-form]');
  if (!form || form.dataset.emailGuardReady === 'true') return;
  form.dataset.emailGuardReady = 'true';

  const email = form.elements.namedItem('email') as HTMLInputElement | null;
  const status = form.querySelector<HTMLElement>('[data-prelaunch-status]');
  if (!email) return;

  // This mirrors the conservative parser, while the TypeScript validator stays
  // authoritative for normalization and error handling.
  email.pattern = "[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+";
  email.autocapitalize = 'none';
  email.spellcheck = false;

  form.addEventListener('submit', (event) => {
    const result = normalizePrelaunchEmail(email.value);
    if (result.ok) {
      email.value = result.email;
      email.setCustomValidity('');
      form.dataset.emailValidation = 'accepted';
      return;
    }

    event.preventDefault();
    event.stopImmediatePropagation();
    email.setCustomValidity('Проверьте адрес электронной почты.');
    form.dataset.emailValidation = result.reason;
    form.dataset.experienceState = 'error';
    if (status) {
      status.hidden = false;
      status.dataset.kind = 'error';
      status.textContent = 'Проверьте адрес электронной почты.';
    }
    email.focus({ preventScroll: true });
  }, { capture: true });

  email.addEventListener('input', () => {
    email.setCustomValidity('');
    if (form.dataset.experienceState !== 'error') return;
    form.dataset.emailValidation = 'pending';
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchEmailGuard, { once: true });
} else {
  initializePrelaunchEmailGuard();
}
