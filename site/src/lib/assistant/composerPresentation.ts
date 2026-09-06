/** One composer, opened by a launcher positioned by EventLayout (not a second shell).
 * Local persistence is recovery UI; answer sections stay in document flow. */
export function mountComposerPresentation(root: HTMLElement) {
  const dialog = root.querySelector<HTMLDialogElement>('[data-assistant-composer]')!;
  const launcher = root.querySelector<HTMLButtonElement>('[data-assistant-launcher]')!;
  const status = root.querySelector<HTMLElement>('[data-assistant-processing]')!;
  let activate = () => { status.textContent = 'Поиск ещё подключается. Попробуйте через несколько секунд.'; };
  let beforeClose: () => Promise<void> = async () => {};
  const open = () => { if (!dialog.open) dialog.showModal(); launcher.setAttribute('aria-expanded', 'true'); };
  const close = async () => { await beforeClose(); dialog.close(); };
  launcher.addEventListener('click', () => { open(); activate(); });
  root.querySelector('[data-assistant-close]')!.addEventListener('click', () => { void close().catch(() => { status.textContent = 'Не удалось завершить запись. Не закрывайте вкладку: аудио остаётся в памяти.'; }); });
  dialog.addEventListener('cancel', event => { event.preventDefault(); void close().catch(() => { status.textContent = 'Не удалось завершить запись. Не закрывайте вкладку: аудио остаётся в памяти.'; }); });
  dialog.addEventListener('close', () => { launcher.setAttribute('aria-expanded', 'false'); launcher.focus({preventScroll:true}); });
  return { open, close, launcher, bind: (action: () => void, stop: () => Promise<void>) => { activate = action; beforeClose = stop; },
    fail: () => { status.textContent = 'Голосовой поиск не подключился. Обновите страницу или воспользуйтесь обычным поиском.'; activate = () => {}; root.querySelector<HTMLButtonElement>('[data-assistant-record]')!.disabled = true; } };
}
