"""Replay the published two VK controls locally. Needs Python 3 and the adjacent JSON."""
import json
from pathlib import Path

evidence = json.loads(Path(__file__).with_name('vk-reader-evidence.json').read_text())
for aid, result in evidence['reader']['result'].items():
    chain = evidence['frozen_top20'][aid]
    observed = evidence['packets'][aid]
    expected = {peer for peer, _ in chain}
    assert len(chain) == len(expected) == 20
    unique = {}
    for packet in observed:
        allowed = {str(peer) for peer in packet['ids']}
        assert allowed.issubset({str(peer) for peer in expected})
        assert set(packet['judged']).issubset(allowed)
        for key, values in packet['judged'].items():
            assert key not in unique
            unique[key] = values

    strong = []
    for peer, similarity in chain:
        if str(peer) in unique:
            score, reject, confidence, kind = unique[str(peer)]
            if score >= 0.72 and not reject and kind not in {'same_occurrence_group', 'duplicate_same_slot'}:
                weight = round(0.82 * score + 0.12 * similarity + 0.06 * confidence, 4)
                strong.append((-weight, -similarity, peer))
    pure = [peer for _, _, peer in sorted(strong)]
    assert pure == result['pure']
    basis = result['reader_input']
    candidates = {row['id']: row for row in basis['candidates'] if row['eligible']}
    groups = []
    for peer, row in candidates.items():
        group = {peer, *row['links']}
        for known in groups[:]:
            if group & known:
                groups.remove(known)
                group |= known
        groups.append(group)
    seen_urls = {basis['anchor_url']}
    seen_groups, related, repeats = set(), [], []
    for peer in pure:
        row = candidates.get(peer)
        if not row or peer in basis['linked'] or not row['public'] or row['url'] in seen_urls:
            continue
        seen_urls.add(row['url'])
        group_id = next(i for i, group in enumerate(groups) if peer in group)
        (repeats if group_id in seen_groups else related).append(peer)
        seen_groups.add(group_id)
    shown = (related + repeats)[:5] if basis['has_photo'] else []
    assert shown == [item['id'] for item in result['archived_vk_rows']]
    assert len(unique) == result['unambiguous_verdicts']
    assert result['strict_e2e'] == ({int(i) for i in unique} == expected and bool(result['strict']) and not result['visibility_unknown'])
    print(f"{aid}: {len(unique)}/20; strong {pure}; VK {shown}; complete {result['strict_e2e']}")
