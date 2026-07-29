export function abortError() {
  const error = new Error("scenario stopped");
  error.name = "AbortError";
  return error;
}

export function assertNotAborted(signal) {
  if (signal?.aborted) throw abortError();
}

export function abortableDelay(milliseconds, signal) {
  assertNotAborted(signal);
  return new Promise((resolve, reject) => {
    let timer;
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(abortError());
    };
    timer = setTimeout(() => {
      cleanup();
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
