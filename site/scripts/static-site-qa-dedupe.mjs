import { readFile } from 'node:fs/promises';

function timestamp(value) {
  const parsed = Date.parse(String(value || ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

function isBotReceipt(comment, prefix) {
  return comment?.user?.type === 'Bot' && String(comment?.body || '').startsWith(prefix);
}

export function classifyDuplicateActiveRequest({ current, comments }) {
  const currentCreated = timestamp(current?.created_at);
  const currentBody = String(current?.body || '');
  const currentId = Number(current?.id || 0);
  const prior = comments
    .filter((comment) => Number(comment?.id || 0) !== currentId
      && String(comment?.body || '') === currentBody
      && timestamp(comment?.created_at) < currentCreated)
    .sort((a, b) => timestamp(b.created_at) - timestamp(a.created_at))[0];
  if (!prior) return { duplicate: false, runId: null };

  const accepted = comments
    .filter((comment) => isBotReceipt(comment, 'ACCEPTED · focus.otp.browser_tab ·')
      && timestamp(comment?.created_at) >= timestamp(prior.created_at))
    .sort((a, b) => timestamp(a.created_at) - timestamp(b.created_at))
    .find((comment) => /\/actions\/runs\/([0-9]+)/u.test(String(comment.body)));
  const runId = String(accepted?.body || '').match(/\/actions\/runs\/([0-9]+)/u)?.[1] || null;
  if (!runId) return { duplicate: false, runId: null };

  const terminal = comments.find((comment) => isBotReceipt(comment, 'TERMINAL · focus.otp.browser_tab ·')
    && String(comment.body).includes(`/actions/runs/${runId}`));
  return {
    duplicate: Boolean(terminal && timestamp(terminal.created_at) > currentCreated),
    runId,
  };
}

async function main() {
  const event = JSON.parse(await readFile(process.env.GITHUB_EVENT_PATH, 'utf8'));
  const pages = JSON.parse(await readFile(process.env.STATIC_SITE_QA_COMMENTS_FILE, 'utf8'));
  const comments = Array.isArray(pages?.[0]) ? pages.flat() : pages;
  const result = classifyDuplicateActiveRequest({ current: event.comment, comments });
  await import('node:fs/promises').then(({ appendFile }) => appendFile(
    process.env.GITHUB_OUTPUT,
    `duplicate=${result.duplicate}\nprior_run_id=${result.runId || ''}\n`,
  ));
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  main().catch((error) => {
    console.error(`Static Site QA dedupe failed: ${String(error?.message || error)}`);
    process.exitCode = 1;
  });
}
