const count = (value) => (
  Number.isSafeInteger(Number(value)) && Number(value) >= 0 ? Number(value) : 0
);

const hasOwn = (value, key) => (
  value && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, key)
);

/**
 * Project the three cost-bearing provider counters from the production Edge
 * Function contract.  `request_counters` is authoritative; the older aliases
 * remain accepted only for the manual legacy harness.
 */
export function searchProviderCounters(payload = {}) {
  const actual = payload?.request_counters && typeof payload.request_counters === 'object'
    ? payload.request_counters
    : null;
  const legacy = payload?.provider_attempt_counters ?? payload?.provider_attempts ?? {};
  const values = actual || legacy;
  const authoritative = actual !== null;
  return Object.freeze({
    embedding: count(authoritative
      ? values.embedding_provider_attempts
      : values.embedding ?? values.embedding_provider ?? payload?.embedding_provider_attempts),
    vector: count(authoritative
      ? values.vector_rpc_attempts
      : values.vector ?? values.vector_rpc ?? payload?.vector_attempts),
    llm: count(authoritative
      ? values.llm_provider_attempts
      : values.llm ?? values.verifier ?? payload?.llm_provider_attempts),
    present: authoritative
      ? ['embedding_provider_attempts', 'vector_rpc_attempts', 'llm_provider_attempts']
        .every((key) => hasOwn(values, key))
      : Boolean(
        (values && typeof values === 'object' && Object.keys(values).length > 0)
        || payload?.embedding_provider_attempts != null
        || payload?.vector_attempts != null
        || payload?.llm_provider_attempts != null
      ),
    source: authoritative ? 'request_counters' : 'legacy_alias',
  });
}
