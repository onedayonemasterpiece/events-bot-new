export function previewBuildUrl(target) {
  const [prefix] = target.pathname.split('/').filter(Boolean);
  return new URL(prefix?.startsWith('preview-') ? `/${prefix}/preview-build.json` : '/preview-build.json', target.origin);
}

export async function observedRepoSha(target, expected, fetchImpl = fetch, expectedFaultProfile = 'normal') {
  const response = await fetchImpl(previewBuildUrl(target), { signal: AbortSignal.timeout(15_000) });
  if (!response.ok) {
    if (expected) throw new Error(`release_evidence_metadata_status:${response.status}`);
    return null;
  }
  let body;
  try { body = await response.json(); } catch { throw new Error('release_evidence_metadata_not_json'); }
  const value = String(body?.repo_sha || '').trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(value)) {
    if (expected) throw new Error('release_evidence_repo_sha_missing');
    return null;
  }
  if (expected && value !== expected) throw new Error('release_evidence_repo_sha_mismatch');
  const observedFaultProfile = String(body?.transportFaultProfile || 'normal');
  if (observedFaultProfile !== expectedFaultProfile) throw new Error('release_evidence_fault_profile_mismatch');
  if (expectedFaultProfile !== 'normal' && !/^[0-9a-f]{64}$/u.test(String(body?.transportFaultRegistryDigest || ''))) {
    throw new Error('release_evidence_fault_registry_digest_missing');
  }
  return value;
}
