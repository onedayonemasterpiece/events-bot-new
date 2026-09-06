import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  collapseOccurrenceFamilies,
  occurrenceFamilyKey,
  paginateOccurrenceFamilies,
} from "./occurrence-families.ts";

const candidate = (eventId, memberIds, score) => ({
  event_id: eventId,
  static_score: score,
  occurrence_member_ids: memberIds,
  display: { occurrence_member_ids: memberIds },
});

test("mutual projected family collapses to the highest-ranked representative", () => {
  const highest = candidate(9, [2, 9], 0.98);
  const sibling = candidate(2, [2, 9], 0.94);
  const result = collapseOccurrenceFamilies([highest, sibling]);

  assert.deepEqual(result.map((item) => item.event_id), [9]);
  assert.equal(occurrenceFamilyKey(highest), "family:2,9");
});

test("asymmetric and lookalike payloads fail closed instead of collapsing", () => {
  const oneWay = candidate(1, [1, 2], 0.99);
  const noReverse = candidate(2, [2], 0.97);
  const sameTitleWithoutLinks = candidate(3, [], 0.95);

  assert.deepEqual(
    collapseOccurrenceFamilies([oneWay, noReverse, sameTitleWithoutLinks])
      .map((item) => item.event_id),
    [1, 2, 3],
  );
  assert.equal(occurrenceFamilyKey(noReverse), "event:2");
});

test("family pagination is stable and never resurfaces a later sibling", () => {
  const ranked = [
    candidate(9, [2, 9], 0.99),
    candidate(7, [7], 0.98),
    candidate(2, [2, 9], 0.97),
    candidate(8, [8], 0.96),
  ];

  const pageOne = paginateOccurrenceFamilies(ranked, 0, 2);
  const pageTwo = paginateOccurrenceFamilies(ranked, pageOne.nextOffset, 2);

  assert.deepEqual(pageOne.items.map((item) => item.event_id), [9, 7]);
  assert.deepEqual(pageTwo.items.map((item) => item.event_id), [8]);
  assert.equal(pageOne.hasMore, true);
  assert.equal(pageTwo.hasMore, false);
});

test("fallback can share the main result family seen-set", () => {
  const main = candidate(9, [2, 9], 0.99);
  const fallbackSibling = candidate(2, [2, 9], 0.5);
  const fallbackOther = candidate(7, [7], 0.4);
  const seen = new Set([occurrenceFamilyKey(main)]);

  assert.deepEqual(
    collapseOccurrenceFamilies([fallbackSibling, fallbackOther], seen)
      .map((item) => item.event_id),
    [7],
  );
});

test("Edge handler wires collapse before pagination, after rerank and into fallback", () => {
  const source = readFileSync(
    fileURLToPath(new URL("./index.ts", import.meta.url)),
    "utf8",
  );

  assert.match(source, /p_match_count:\s*60,[\s\S]*p_offset_count:\s*0,/u);
  assert.match(source, /paginateOccurrenceFamilies\(\s*rankedCandidates,\s*offset,\s*verificationWindow,/u);
  assert.match(source, /items\s*=\s*collapseOccurrenceFamilies\(\s*assistantIntent \? llmResult\.exact : llmResult\.used/u);
  assert.match(
    source,
    /collapseOccurrenceFamilies\(\s*llmResult\.possible,\s*new Set\(items\.map\(occurrenceFamilyKey\)\),?\s*\)/u,
  );
  assert.ok(source.includes("fallbackItems = collapseOccurrenceFamilies("));
  assert.ok(source.includes("(Array.isArray(fallbackRows) ? fallbackRows : [])"));
});
